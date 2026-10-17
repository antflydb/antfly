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
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
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

pub fn ac_dump(arg_buf: [*c]const i16, arg_w: c_int, arg_h: c_int, arg_what: [*c]const u8) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var what = arg_what;
    _ = &what;
    _ = @as(c_int, 0);
    while ((blk: {
        const ref = &h;
        const tmp = ref.*;
        ref.* -= 1;
        break :blk tmp;
    }) != 0) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                _ = @as(c_int, 0);
            }
        }
        buf += @as(usize, @bitCast(@as(isize, @intCast(w))));
        _ = @as(c_int, 0);
    }
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
pub fn apply_sign64(v: c_int, s: i64) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(i64, 0)) -v else v;
}
pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
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
pub const DAV1D_WM_TYPE_TRANSLATION = common.DAV1D_WM_TYPE_TRANSLATION;
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
const struct_unnamed_22 = extern struct {
    pal_idx: [*c]u8 = null,
    cbi: [*c]i16 = null,
    cf: [*c]coef = null,
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
pub const DAV1D_INLOOPFILTER_DEBLOCK = common.DAV1D_INLOOPFILTER_DEBLOCK;
pub const DAV1D_INLOOPFILTER_CDEF = common.DAV1D_INLOOPFILTER_CDEF;
pub const DAV1D_INLOOPFILTER_RESTORATION = common.DAV1D_INLOOPFILTER_RESTORATION;
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
pub const BS_32x32 = common.BS_32x32;
pub const enum_BlockSize = common.enum_BlockSize;
pub const EDGE_I444_TOP_HAS_RIGHT = common.EDGE_I444_TOP_HAS_RIGHT;
pub const EDGE_I420_TOP_HAS_RIGHT = common.EDGE_I420_TOP_HAS_RIGHT;
pub const EDGE_I444_LEFT_HAS_BOTTOM = common.EDGE_I444_LEFT_HAS_BOTTOM;
pub const EDGE_I420_LEFT_HAS_BOTTOM = common.EDGE_I420_LEFT_HAS_BOTTOM;
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
    cf: [*c]coef = null,
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
pub const TX_4X4 = common.TX_4X4;
pub const TX_8X8 = common.TX_8X8;
pub const TX_16X16 = common.TX_16X16;
pub const TX_32X32 = common.TX_32X32;
pub const TX_64X64 = common.TX_64X64;
pub const RTX_4X8 = common.RTX_4X8;
pub const RTX_8X4 = common.RTX_8X4;
pub const RTX_8X16 = common.RTX_8X16;
pub const RTX_16X8 = common.RTX_16X8;
pub const RTX_16X32 = common.RTX_16X32;
pub const RTX_32X16 = common.RTX_32X16;
pub const RTX_32X64 = common.RTX_32X64;
pub const RTX_64X32 = common.RTX_64X32;
pub const RTX_4X16 = common.RTX_4X16;
pub const RTX_16X4 = common.RTX_16X4;
pub const RTX_8X32 = common.RTX_8X32;
pub const RTX_32X8 = common.RTX_32X8;
pub const RTX_16X64 = common.RTX_16X64;
pub const RTX_64X16 = common.RTX_64X16;
pub const enum_RectTxfmSize = common.enum_RectTxfmSize;
pub const DCT_DCT = common.DCT_DCT;
pub const IDTX = common.IDTX;
pub const V_ADST = common.V_ADST;
pub const H_ADST = common.H_ADST;
pub const V_FLIPADST = common.V_FLIPADST;
pub const H_FLIPADST = common.H_FLIPADST;
pub const WHT_WHT = common.WHT_WHT;
pub const enum_TxfmType = common.enum_TxfmType;
pub const TX_CLASS_2D = common.TX_CLASS_2D;
pub const TX_CLASS_H = common.TX_CLASS_H;
pub const TX_CLASS_V = common.TX_CLASS_V;
pub const enum_TxClass = common.enum_TxClass;
pub const DC_PRED = common.DC_PRED;
pub const SMOOTH_PRED = common.SMOOTH_PRED;
pub const SMOOTH_V_PRED = common.SMOOTH_V_PRED;
pub const SMOOTH_H_PRED = common.SMOOTH_H_PRED;
pub const CFL_PRED = common.CFL_PRED;
pub const FILTER_PRED = common.FILTER_PRED;
pub const enum_IntraPredMode = common.enum_IntraPredMode;
pub const II_SMOOTH_PRED = common.II_SMOOTH_PRED;
pub const GLOBALMV = common.GLOBALMV;
pub const GLOBALMV_GLOBALMV = common.GLOBALMV_GLOBALMV;
pub const COMP_INTER_NONE = common.COMP_INTER_NONE;
pub const COMP_INTER_WEIGHTED_AVG = common.COMP_INTER_WEIGHTED_AVG;
pub const COMP_INTER_AVG = common.COMP_INTER_AVG;
pub const COMP_INTER_SEG = common.COMP_INTER_SEG;
pub const COMP_INTER_WEDGE = common.COMP_INTER_WEDGE;
pub const INTER_INTRA_BLEND = common.INTER_INTRA_BLEND;
pub const MM_OBMC = common.MM_OBMC;
pub const MM_WARP = common.MM_WARP;
pub extern fn antfly_codec_llabs(c_longlong) c_longlong;
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
pub extern const antfly_av1_dav1d_txtp_from_uvmode: [14]u8;
pub extern const antfly_av1_dav1d_tx_types_per_set: [40]u8;
pub extern const antfly_av1_dav1d_filter_mode_to_y_mode: [5]u8;
pub extern var antfly_av1_dav1d_lo_ctx_offsets: [3][5][5]u8;
pub extern var antfly_av1_dav1d_skip_ctx: [5][5]u8;
pub extern const antfly_av1_dav1d_tx_type_class: [17]u8;
pub extern var antfly_av1_dav1d_filter_2d: [4][4]u8;
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
pub fn get_uv_inter_txtp(uvt_dim: [*c]const TxfmInfo, ytxtp: enum_TxfmType) callconv(.c) enum_TxfmType {
    _ = &uvt_dim;
    _ = &ytxtp;
    if (@as(c_int, uvt_dim.*.max) == TX_32X32) return @bitCast(if (ytxtp == @as(enum_TxfmType, IDTX)) IDTX else DCT_DCT);
    if ((@as(c_int, uvt_dim.*.min) == TX_16X16) and (((@as(c_int, 1) << @intCast(@as(c_int, @bitCast(ytxtp)))) & ((((@as(c_int, 1) << @intCast(H_FLIPADST)) | (@as(c_int, 1) << @intCast(V_FLIPADST))) | (@as(c_int, 1) << @intCast(H_ADST))) | (@as(c_int, 1) << @intCast(V_ADST)))) != 0)) {
        return DCT_DCT;
    }
    return ytxtp;
}
pub extern fn antfly_av1_dav1d_msac_decode_symbol_adapt_c(s: [*c]MsacContext, cdf: [*c]u16, n_symbols: usize) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_bool_adapt_c(s: [*c]MsacContext, cdf: [*c]u16) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_bool_equi_c(s: [*c]MsacContext) c_uint;
pub extern fn antfly_av1_dav1d_msac_decode_hi_tok_c(s: [*c]MsacContext, cdf: [*c]u16) c_uint;
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
pub extern fn antfly_av1_dav1d_cdef_brow_16bpc(tc: [*c]Dav1dTaskContext, p: [*c]const [*c]pixel, lflvl: [*c]const Av1Filter, by_start: c_int, by_end: c_int, sbrow_start: c_int, sby: c_int) void;
pub const union_alias64 = common.union_alias64;
pub const union_alias32 = common.union_alias32;
pub const union_alias16 = common.union_alias16;
pub const union_alias8 = common.union_alias8;
pub const dav1d_memset_pow2_fn = common.dav1d_memset_pow2_fn;
pub extern const antfly_av1_dav1d_memset_pow2: [6]dav1d_memset_pow2_fn;
pub fn dav1d_memset_likely_pow2(ptr: ?*anyopaque, value: c_int, n: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    _ = &n;
    while (true) {
        if (!((n >= @as(c_int, 1)) and (n <= @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    if ((n & (n - @as(c_int, 1))) == @as(c_int, 0)) {
        antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, n))))))].?(ptr, value);
    } else {
        _ = antfly_codec_afz_memset(ptr, value, @bitCast(@as(c_long, n)));
    }
}
pub extern fn antfly_av1_dav1d_prepare_intra_edges_16bpc(x: c_int, have_left: c_int, y: c_int, have_top: c_int, w: c_int, h: c_int, edge_flags: enum_EdgeFlags, dst: [*c]const pixel, stride: ptrdiff_t, prefilter_toplevel_sb_edge: [*c]const pixel, mode: enum_IntraPredMode, angle: [*c]c_int, tw: c_int, th: c_int, filter_edge: c_int, topleft_out: [*c]pixel, bitdepth_max: c_int) enum_IntraPredMode;
pub fn sm_flag(b: [*c]const BlockContext, idx: c_int) callconv(.c) c_int {
    _ = &b;
    _ = &idx;
    if (!(@as(c_int, b.*.intra[@bitCast(@as(isize, @intCast(idx)))]) != 0)) return 0;
    const m: enum_IntraPredMode = b.*.mode[@bitCast(@as(isize, @intCast(idx)))];
    _ = &m;
    return if (((m == @as(enum_IntraPredMode, SMOOTH_PRED)) or (m == @as(enum_IntraPredMode, SMOOTH_H_PRED))) or (m == @as(enum_IntraPredMode, SMOOTH_V_PRED))) @as(c_int, 512) else @as(c_int, 0);
}
pub fn sm_uv_flag(b: [*c]const BlockContext, idx: c_int) callconv(.c) c_int {
    _ = &b;
    _ = &idx;
    const m: enum_IntraPredMode = b.*.uvmode[@bitCast(@as(isize, @intCast(idx)))];
    _ = &m;
    return if (((m == @as(enum_IntraPredMode, SMOOTH_PRED)) or (m == @as(enum_IntraPredMode, SMOOTH_H_PRED))) or (m == @as(enum_IntraPredMode, SMOOTH_V_PRED))) @as(c_int, 512) else @as(c_int, 0);
}
pub extern fn antfly_av1_dav1d_loopfilter_sbrow_cols_16bpc(f: [*c]const Dav1dFrameContext, p: [*c]const [*c]pixel, lflvl: [*c]Av1Filter, sby: c_int, start_of_tile_row: c_int) void;
pub extern fn antfly_av1_dav1d_loopfilter_sbrow_rows_16bpc(f: [*c]const Dav1dFrameContext, p: [*c]const [*c]pixel, lflvl: [*c]Av1Filter, sby: c_int) void;
pub extern fn antfly_av1_dav1d_copy_lpf_16bpc(f: [*c]Dav1dFrameContext, src: [*c]const [*c]pixel, sby: c_int) void;
pub extern fn antfly_av1_dav1d_lr_sbrow_16bpc(f: [*c]Dav1dFrameContext, dst: [*c]const [*c]pixel, sby: c_int) void;
pub extern const antfly_av1_dav1d_scans: [19][*c]const u16;
const struct_unnamed_62 = extern struct {
    wedge: [2][16]u16 = @import("std").mem.zeroes([2][16]u16),
    ii: [4]u16 = @import("std").mem.zeroes([4]u16),
};
pub const Dav1dMasks = extern struct {
    offsets: [3][11]struct_unnamed_62 align(64) = @import("std").mem.zeroes([3][11]struct_unnamed_62),
    wedge_444_32x32: [16384]u8 align(64) = @import("std").mem.zeroes([16384]u8),
    wedge_444_32x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_444_32x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_16x32: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_444_16x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_16x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_444_8x32: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_444_8x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_444_8x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_422_16x32: [16384]u8 = @import("std").mem.zeroes([16384]u8),
    wedge_422_16x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_422_16x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_8x32: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_422_8x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_8x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_422_4x32: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_422_4x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_422_4x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_16x16: [8192]u8 = @import("std").mem.zeroes([8192]u8),
    wedge_420_16x8: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_420_16x4: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_8x16: [4096]u8 = @import("std").mem.zeroes([4096]u8),
    wedge_420_8x8: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_8x4: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_4x16: [2048]u8 = @import("std").mem.zeroes([2048]u8),
    wedge_420_4x8: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    wedge_420_4x4: [512]u8 = @import("std").mem.zeroes([512]u8),
    ii_dc: [1024]u8 = @import("std").mem.zeroes([1024]u8),
    ii_nondc_32x32: [3072]u8 = @import("std").mem.zeroes([3072]u8),
    ii_nondc_16x32: [1536]u8 = @import("std").mem.zeroes([1536]u8),
    ii_nondc_16x16: [768]u8 = @import("std").mem.zeroes([768]u8),
    ii_nondc_8x32: [768]u8 = @import("std").mem.zeroes([768]u8),
    ii_nondc_8x16: [384]u8 = @import("std").mem.zeroes([384]u8),
    ii_nondc_8x8: [192]u8 = @import("std").mem.zeroes([192]u8),
    ii_nondc_4x16: [192]u8 = @import("std").mem.zeroes([192]u8),
    ii_nondc_4x8: [96]u8 = @import("std").mem.zeroes([96]u8),
    ii_nondc_4x4: [48]u8 = @import("std").mem.zeroes([48]u8),
};
pub extern var antfly_av1_dav1d_masks: Dav1dMasks;
pub fn read_golomb(msac: [*c]MsacContext) callconv(.c) c_uint {
    _ = &msac;
    var len: c_int = 0;
    _ = &len;
    var val: c_uint = 1;
    _ = &val;
    while (!(antfly_av1_dav1d_msac_decode_bool_equi_c(msac) != 0) and (len < @as(c_int, 32))) {
        len += 1;
    }
    while ((blk: {
        const ref = &len;
        const tmp = ref.*;
        ref.* -= 1;
        break :blk tmp;
    }) != 0) {
        val = (val << @intCast(@as(c_uint, 1))) +% antfly_av1_dav1d_msac_decode_bool_equi_c(msac);
    }
    return val -% @as(c_uint, 1);
}
pub fn get_skip_ctx(t_dim: [*c]const TxfmInfo, bs: enum_BlockSize, a: [*c]const u8, l: [*c]const u8, chroma: c_int, layout: enum_Dav1dPixelLayout) callconv(.c) c_uint {
    _ = &t_dim;
    _ = &bs;
    _ = &a;
    _ = &l;
    _ = &chroma;
    _ = &layout;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    if (chroma != 0) {
        const ss_ver: c_int = @as(c_int, @intFromBool(layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver;
        const ss_hor: c_int = @as(c_int, @intFromBool(layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
        _ = &ss_hor;
        const not_one_blk: c_int = @as(c_int, @intFromBool(((@as(c_int, b_dim[@as(c_int, 2)]) - @intFromBool(!!(@as(c_int, b_dim[@as(c_int, 2)]) != 0) and (ss_hor != 0))) > @as(c_int, t_dim.*.lw)) or ((@as(c_int, b_dim[@as(c_int, 3)]) - @intFromBool(!!(@as(c_int, b_dim[@as(c_int, 3)]) != 0) and (ss_ver != 0))) > @as(c_int, t_dim.*.lh))));
        _ = &not_one_blk;
        var ca: c_uint = undefined;
        _ = &ca;
        var cl: c_uint = undefined;
        _ = &cl;
        blk: {
            switch (@as(c_int, t_dim.*.lw)) {
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    ca = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, a.*) != @as(c_int, 64)))));
                    break :blk;
                },
                TX_4X4 => {
                    ca = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, a.*) != @as(c_int, 64)))));
                    break :blk;
                },
                TX_8X8 => {
                    ca = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*) != @as(c_int, 16448)))));
                    break :blk;
                },
                TX_16X16 => {
                    ca = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as([*c]const u32, @ptrCast(@alignCast(a))).* != @as(c_uint, 1077952576)))));
                    break :blk;
                },
                TX_32X32 => {
                    ca = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as([*c]const u64, @ptrCast(@alignCast(a))).* != @as(c_ulonglong, 4629771061636907072)))));
                    break :blk;
                },
            }
        }
        blk: {
            switch (@as(c_int, t_dim.*.lh)) {
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    cl = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, l.*) != @as(c_int, 64)))));
                    break :blk;
                },
                TX_4X4 => {
                    cl = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, l.*) != @as(c_int, 64)))));
                    break :blk;
                },
                TX_8X8 => {
                    cl = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*) != @as(c_int, 16448)))));
                    break :blk;
                },
                TX_16X16 => {
                    cl = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as([*c]const u32, @ptrCast(@alignCast(l))).* != @as(c_uint, 1077952576)))));
                    break :blk;
                },
                TX_32X32 => {
                    cl = @bitCast(@as(c_int, @as(c_int, @intFromBool(@as([*c]const u64, @ptrCast(@alignCast(l))).* != @as(c_ulonglong, 4629771061636907072)))));
                    break :blk;
                },
            }
        }
        return (@as(c_uint, @bitCast(@as(c_int, @as(c_int, 7) + (not_one_blk * @as(c_int, 3))))) +% ca) +% cl;
    } else if ((@as(c_int, b_dim[@as(c_int, 2)]) == @as(c_int, t_dim.*.lw)) and (@as(c_int, b_dim[@as(c_int, 3)]) == @as(c_int, t_dim.*.lh))) {
        return 0;
    } else {
        var la: c_uint = undefined;
        _ = &la;
        var ll: c_uint = undefined;
        _ = &ll;
        blk: {
            switch (@as(c_int, t_dim.*.lw)) {
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    if (TX_4X4 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = a.*;
                    }
                    if (TX_4X4 == TX_32X32) {
                        la |= @bitCast(@as(c_int, @as(c_int, (&a[@sizeOf(u8)]).*)));
                    }
                    if (TX_4X4 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_4X4 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_4X4 => {
                    if (TX_4X4 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = a.*;
                    }
                    if (TX_4X4 == TX_32X32) {
                        la |= @bitCast(@as(c_int, @as(c_int, (&a[@sizeOf(u8)]).*)));
                    }
                    if (TX_4X4 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_4X4 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_8X8 => {
                    if (TX_8X8 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = @as([*c]const u16, @ptrCast(@alignCast(a))).*;
                    }
                    if (TX_8X8 == TX_32X32) {
                        la |= @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(&a[@sizeOf(u16)]))).*)));
                    }
                    if (TX_8X8 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_8X8 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_16X16 => {
                    if (TX_16X16 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = @as([*c]const u32, @ptrCast(@alignCast(a))).*;
                    }
                    if (TX_16X16 == TX_32X32) {
                        la |= @as([*c]const u32, @ptrCast(@alignCast(&a[@sizeOf(u32)]))).*;
                    }
                    if (TX_16X16 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_16X16 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_32X32 => {
                    if (TX_32X32 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = @as([*c]const u32, @ptrCast(@alignCast(a))).*;
                    }
                    if (TX_32X32 == TX_32X32) {
                        la |= @as([*c]const u32, @ptrCast(@alignCast(&a[@sizeOf(u32)]))).*;
                    }
                    if (TX_32X32 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_32X32 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_64X64 => {
                    if (TX_64X64 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).*;
                        la = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        la = @as([*c]const u32, @ptrCast(@alignCast(a))).*;
                    }
                    if (TX_64X64 == TX_32X32) {
                        la |= @as([*c]const u32, @ptrCast(@alignCast(&a[@sizeOf(u32)]))).*;
                    }
                    if (TX_64X64 >= TX_16X16) {
                        la |= la >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_64X64 >= TX_8X8) {
                        la |= la >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
            }
        }
        blk: {
            switch (@as(c_int, t_dim.*.lh)) {
                else => {
                    while (true) {
                        if (!(@as(c_int, 0) != 0)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    if (TX_4X4 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = l.*;
                    }
                    if (TX_4X4 == TX_32X32) {
                        ll |= @bitCast(@as(c_int, @as(c_int, (&l[@sizeOf(u8)]).*)));
                    }
                    if (TX_4X4 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_4X4 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_4X4 => {
                    if (TX_4X4 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = l.*;
                    }
                    if (TX_4X4 == TX_32X32) {
                        ll |= @bitCast(@as(c_int, @as(c_int, (&l[@sizeOf(u8)]).*)));
                    }
                    if (TX_4X4 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_4X4 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_8X8 => {
                    if (TX_8X8 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = @as([*c]const u16, @ptrCast(@alignCast(l))).*;
                    }
                    if (TX_8X8 == TX_32X32) {
                        ll |= @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(&l[@sizeOf(u16)]))).*)));
                    }
                    if (TX_8X8 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_8X8 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_16X16 => {
                    if (TX_16X16 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = @as([*c]const u32, @ptrCast(@alignCast(l))).*;
                    }
                    if (TX_16X16 == TX_32X32) {
                        ll |= @as([*c]const u32, @ptrCast(@alignCast(&l[@sizeOf(u32)]))).*;
                    }
                    if (TX_16X16 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_16X16 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_32X32 => {
                    if (TX_32X32 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = @as([*c]const u32, @ptrCast(@alignCast(l))).*;
                    }
                    if (TX_32X32 == TX_32X32) {
                        ll |= @as([*c]const u32, @ptrCast(@alignCast(&l[@sizeOf(u32)]))).*;
                    }
                    if (TX_32X32 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_32X32 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
                TX_64X64 => {
                    if (TX_64X64 == TX_64X64) {
                        var tmp: u64 = @as([*c]const u64, @ptrCast(@alignCast(l))).*;
                        _ = &tmp;
                        tmp |= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).*;
                        ll = @as(c_uint, @truncate(tmp >> @intCast(@as(u64, 32)))) | @as(c_uint, @truncate(tmp));
                    } else {
                        ll = @as([*c]const u32, @ptrCast(@alignCast(l))).*;
                    }
                    if (TX_64X64 == TX_32X32) {
                        ll |= @as([*c]const u32, @ptrCast(@alignCast(&l[@sizeOf(u32)]))).*;
                    }
                    if (TX_64X64 >= TX_16X16) {
                        ll |= ll >> @intCast(@as(c_uint, 16));
                    }
                    if (TX_64X64 >= TX_8X8) {
                        ll |= ll >> @intCast(@as(c_uint, 8));
                    }
                    break :blk;
                },
            }
        }
        return antfly_av1_dav1d_skip_ctx[umin(la & @as(c_uint, 63), 4)][umin(ll & @as(c_uint, 63), 4)];
    }
    return undefined;
}
pub fn get_dc_sign_ctx(tx: c_int, a: [*c]const u8, l: [*c]const u8) callconv(.c) c_uint {
    _ = &tx;
    _ = &a;
    _ = &l;
    var mask: u64 = 13889313184910721216;
    _ = &mask;
    var mul: u64 = 72340172838076673;
    _ = &mul;
    var s: c_int = undefined;
    _ = &s;
    blk: {
        switch (tx) {
            else => {
                while (true) {
                    if (!(@as(c_int, 0) != 0)) {
                        unreachable;
                    }
                    if (!false) break;
                }
                {
                    var t: c_int = @as(c_int, a.*) >> @intCast(@as(c_int, 6));
                    _ = &t;
                    t += @as(c_int, l.*) >> @intCast(@as(c_int, 6));
                    s = (t - @as(c_int, 1)) - @as(c_int, 1);
                    break :blk;
                }
            },
            TX_4X4 => {
                {
                    var t: c_int = @as(c_int, a.*) >> @intCast(@as(c_int, 6));
                    _ = &t;
                    t += @as(c_int, l.*) >> @intCast(@as(c_int, 6));
                    s = (t - @as(c_int, 1)) - @as(c_int, 1);
                    break :blk;
                }
            },
            TX_8X8 => {
                {
                    var t: u32 = @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*)))) & @as(u32, @truncate(mask));
                    t *%= 67372036;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 2)) - @as(c_int, 2);
                    break :blk;
                }
            },
            TX_16X16 => {
                {
                    var t: u32 = (@as([*c]const u32, @ptrCast(@alignCast(a))).* & @as(u32, @truncate(mask))) >> @intCast(@as(u32, 6));
                    _ = &t;
                    t +%= (@as([*c]const u32, @ptrCast(@alignCast(l))).* & @as(u32, @truncate(mask))) >> @intCast(@as(u32, 6));
                    t *%= @truncate(mul);
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 4)) - @as(c_int, 4);
                    break :blk;
                }
            },
            TX_32X32 => {
                {
                    var t: u64 = (@as([*c]const u64, @ptrCast(@alignCast(a))).* & mask) >> @intCast(@as(u64, 6));
                    _ = &t;
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(l))).* & mask) >> @intCast(@as(u64, 6));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 8)) - @as(c_int, 8);
                    break :blk;
                }
            },
            TX_64X64 => {
                {
                    var t: u64 = (@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    _ = &t;
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6));
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 16)) - @as(c_int, 16);
                    break :blk;
                }
            },
            RTX_4X8 => {
                {
                    var t: u32 = @as(u32, @bitCast(@as(c_int, @as(c_int, a.*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*)))) & @as(u32, @truncate(mask));
                    t *%= 67372036;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 1)) - @as(c_int, 2);
                    break :blk;
                }
            },
            RTX_8X4 => {
                {
                    var t: u32 = @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, l.*)))) & @as(u32, @truncate(mask));
                    t *%= 67372036;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 2)) - @as(c_int, 1);
                    break :blk;
                }
            },
            RTX_8X16 => {
                {
                    var t: u32 = @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as([*c]const u32, @ptrCast(@alignCast(l))).* & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u32, 6))) *% @as(u32, @truncate(mul));
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 2)) - @as(c_int, 4);
                    break :blk;
                }
            },
            RTX_16X8 => {
                {
                    var t: u32 = @as([*c]const u32, @ptrCast(@alignCast(a))).* & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*)))) & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u32, 6))) *% @as(u32, @truncate(mul));
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 4)) - @as(c_int, 2);
                    break :blk;
                }
            },
            RTX_16X32 => {
                {
                    var t: u64 = @as([*c]const u32, @ptrCast(@alignCast(a))).* & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as([*c]const u64, @ptrCast(@alignCast(l))).* & mask;
                    t = (t >> @intCast(@as(u64, 6))) *% mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 4)) - @as(c_int, 8);
                    break :blk;
                }
            },
            RTX_32X16 => {
                {
                    var t: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).* & mask;
                    _ = &t;
                    t +%= @as([*c]const u32, @ptrCast(@alignCast(l))).* & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u64, 6))) *% mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 8)) - @as(c_int, 4);
                    break :blk;
                }
            },
            RTX_32X64 => {
                {
                    var t: u64 = (@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    _ = &t;
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 8)) - @as(c_int, 16);
                    break :blk;
                }
            },
            RTX_64X32 => {
                {
                    var t: u64 = (@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    _ = &t;
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6));
                    t +%= (@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 0)]))).* & mask) >> @intCast(@as(u64, 6));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 16)) - @as(c_int, 8);
                    break :blk;
                }
            },
            RTX_4X16 => {
                {
                    var t: u32 = @as(u32, @bitCast(@as(c_int, @as(c_int, a.*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as([*c]const u32, @ptrCast(@alignCast(l))).* & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u32, 6))) *% @as(u32, @truncate(mul));
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 1)) - @as(c_int, 4);
                    break :blk;
                }
            },
            RTX_16X4 => {
                {
                    var t: u32 = @as([*c]const u32, @ptrCast(@alignCast(a))).* & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, l.*)))) & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u32, 6))) *% @as(u32, @truncate(mul));
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u32, 24)))))) - @as(c_int, 4)) - @as(c_int, 1);
                    break :blk;
                }
            },
            RTX_8X32 => {
                {
                    var t: u64 = @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(a))).*)))) & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as([*c]const u64, @ptrCast(@alignCast(l))).* & mask;
                    t = (t >> @intCast(@as(u64, 6))) *% mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 2)) - @as(c_int, 8);
                    break :blk;
                }
            },
            RTX_32X8 => {
                {
                    var t: u64 = @as([*c]const u64, @ptrCast(@alignCast(a))).* & mask;
                    _ = &t;
                    t +%= @as(u32, @bitCast(@as(c_int, @as(c_int, @as([*c]const u16, @ptrCast(@alignCast(l))).*)))) & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u64, 6))) *% mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 8)) - @as(c_int, 2);
                    break :blk;
                }
            },
            RTX_16X64 => {
                {
                    var t: u64 = @as([*c]const u32, @ptrCast(@alignCast(a))).* & @as(u32, @truncate(mask));
                    _ = &t;
                    t +%= @as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 0)]))).* & mask;
                    t = (t >> @intCast(@as(u64, 6))) +% ((@as([*c]const u64, @ptrCast(@alignCast(&l[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6)));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 4)) - @as(c_int, 16);
                    break :blk;
                }
            },
            RTX_64X16 => {
                {
                    var t: u64 = @as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 0)]))).* & mask;
                    _ = &t;
                    t +%= @as([*c]const u32, @ptrCast(@alignCast(l))).* & @as(u32, @truncate(mask));
                    t = (t >> @intCast(@as(u64, 6))) +% ((@as([*c]const u64, @ptrCast(@alignCast(&a[@as(c_int, 8)]))).* & mask) >> @intCast(@as(u64, 6)));
                    t *%= mul;
                    s = (@as(c_int, @bitCast(@as(c_uint, @truncate(t >> @intCast(@as(u64, 56)))))) - @as(c_int, 16)) - @as(c_int, 4);
                    break :blk;
                }
            },
        }
    }
    return @bitCast(@as(c_int, @as(c_int, @intFromBool(s != @as(c_int, 0))) + @as(c_int, @intFromBool(s > @as(c_int, 0)))));
}
pub fn get_lo_ctx(levels: [*c]const u8, tx_class: enum_TxClass, hi_mag: [*c]c_uint, ctx_offsets: [*c][5]u8, x: c_uint, y: c_uint, stride: ptrdiff_t) callconv(.c) c_uint {
    _ = &levels;
    _ = &tx_class;
    _ = &hi_mag;
    _ = &ctx_offsets;
    _ = &x;
    _ = &y;
    _ = &stride;
    var mag: c_uint = @bitCast(@as(c_int, @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * stride) + @as(ptrdiff_t, 1))))]) + @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * stride) + @as(ptrdiff_t, 0))))])));
    _ = &mag;
    var offset: c_uint = undefined;
    _ = &offset;
    if (tx_class == @as(enum_TxClass, TX_CLASS_2D)) {
        mag +%= @bitCast(@as(c_int, @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * stride) + @as(ptrdiff_t, 1))))])));
        hi_mag.* = mag;
        mag +%= @bitCast(@as(c_int, @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * stride) + @as(ptrdiff_t, 2))))]) + @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 2) * stride) + @as(ptrdiff_t, 0))))])));
        offset = ctx_offsets[umin(y, 4)][umin(x, 4)];
    } else {
        mag +%= @bitCast(@as(c_int, @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * stride) + @as(ptrdiff_t, 2))))])));
        hi_mag.* = mag;
        mag +%= @bitCast(@as(c_int, @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * stride) + @as(ptrdiff_t, 3))))]) + @as(c_int, levels[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * stride) + @as(ptrdiff_t, 4))))])));
        offset = @as(c_uint, 26) +% (if (y > @as(c_uint, 1)) @as(c_uint, 10) else y *% @as(c_uint, 5));
    }
    return offset +% (if (mag > @as(c_uint, 512)) @as(c_uint, 4) else (mag +% @as(c_uint, 64)) >> @intCast(@as(c_uint, 7)));
}
pub fn decode_coefs(t: [*c]Dav1dTaskContext, a: [*c]u8, l: [*c]u8, tx: enum_RectTxfmSize, bs: enum_BlockSize, b: [*c]const Av1Block, intra: c_int, plane: c_int, arg_cf: [*c]coef, txtp: [*c]enum_TxfmType, arg_res_ctx: [*c]u8) callconv(.c) c_int {
    _ = &t;
    _ = &a;
    _ = &l;
    _ = &tx;
    _ = &bs;
    _ = &b;
    _ = &intra;
    _ = &plane;
    var cf = arg_cf;
    _ = &cf;
    _ = &txtp;
    var res_ctx = arg_res_ctx;
    _ = &res_ctx;
    var afz_dq_sat_85: c_int = undefined;
    _ = &afz_dq_sat_85;
    var afz_tok_83: c_uint = undefined;
    _ = &afz_tok_83;
    var afz_dq_84: c_uint = undefined;
    _ = &afz_dq_84;
    var afz_rc_tok_82: c_uint = undefined;
    _ = &afz_rc_tok_82;
    var afz_sign_81: c_int = undefined;
    _ = &afz_sign_81;
    var afz_ac_dq_80: c_uint = undefined;
    _ = &afz_ac_dq_80;
    var afz_dq_90: c_int = undefined;
    _ = &afz_dq_90;
    var afz_tok_89: c_uint = undefined;
    _ = &afz_tok_89;
    var afz_rc_tok_88: c_uint = undefined;
    _ = &afz_rc_tok_88;
    var afz_sign_87: c_int = undefined;
    _ = &afz_sign_87;
    var afz_ac_dq_86: c_uint = undefined;
    _ = &afz_ac_dq_86;
    var afz_dc_dq_79: c_int = undefined;
    _ = &afz_dc_dq_79;
    var afz_dc_sign_78: c_int = undefined;
    _ = &afz_dc_sign_78;
    var afz_dc_sign_cdf_77: [*c]u16 = undefined;
    _ = &afz_dc_sign_cdf_77;
    var afz_dc_sign_ctx_76: c_int = undefined;
    _ = &afz_dc_sign_ctx_76;
    var afz_cul_level_74: c_uint = undefined;
    _ = &afz_cul_level_74;
    var afz_dc_sign_level_75: c_uint = undefined;
    _ = &afz_dc_sign_level_75;
    var afz_cf_max_73: c_int = undefined;
    _ = &afz_cf_max_73;
    var afz_dq_shift_72: c_int = undefined;
    _ = &afz_dq_shift_72;
    var afz_qm_tbl_71: [*c]u8 = undefined;
    _ = &afz_qm_tbl_71;
    var afz_dq_tbl_70: [*c]u16 = undefined;
    _ = &afz_dq_tbl_70;
    var afz_rc_i_68: c_uint = undefined;
    _ = &afz_rc_i_68;
    var afz_i_67: c_int = undefined;
    _ = &afz_i_67;
    var afz_level_66: [*c]u8 = undefined;
    _ = &afz_level_66;
    var afz_x_64: c_uint = undefined;
    _ = &afz_x_64;
    var afz_y_65: c_uint = undefined;
    _ = &afz_y_65;
    var afz_mask_63: c_uint = undefined;
    _ = &afz_mask_63;
    var afz_shift_61: c_uint = undefined;
    _ = &afz_shift_61;
    var afz_shift2_62: c_uint = undefined;
    _ = &afz_shift2_62;
    var afz_stride_60: ptrdiff_t = undefined;
    _ = &afz_stride_60;
    var afz_lo_ctx_offsets_59: [*c][5]u8 = undefined;
    _ = &afz_lo_ctx_offsets_59;
    var afz_rc_i_58: c_uint = undefined;
    _ = &afz_rc_i_58;
    var afz_i_57: c_int = undefined;
    _ = &afz_i_57;
    var afz_level_56: [*c]u8 = undefined;
    _ = &afz_level_56;
    var afz_x_54: c_uint = undefined;
    _ = &afz_x_54;
    var afz_y_55: c_uint = undefined;
    _ = &afz_y_55;
    var afz_mask_53: c_uint = undefined;
    _ = &afz_mask_53;
    var afz_shift_51: c_uint = undefined;
    _ = &afz_shift_51;
    var afz_shift2_52: c_uint = undefined;
    _ = &afz_shift2_52;
    var afz_stride_50: ptrdiff_t = undefined;
    _ = &afz_stride_50;
    var afz_lo_ctx_offsets_49: [*c][5]u8 = undefined;
    _ = &afz_lo_ctx_offsets_49;
    var afz_rc_i_48: c_uint = undefined;
    _ = &afz_rc_i_48;
    var afz_i_47: c_int = undefined;
    _ = &afz_i_47;
    var afz_level_46: [*c]u8 = undefined;
    _ = &afz_level_46;
    var afz_x_44: c_uint = undefined;
    _ = &afz_x_44;
    var afz_y_45: c_uint = undefined;
    _ = &afz_y_45;
    var afz_mask_43: c_uint = undefined;
    _ = &afz_mask_43;
    var afz_shift_41: c_uint = undefined;
    _ = &afz_shift_41;
    var afz_shift2_42: c_uint = undefined;
    _ = &afz_shift2_42;
    var afz_stride_40: ptrdiff_t = undefined;
    _ = &afz_stride_40;
    var afz_lo_ctx_offsets_39: [*c][5]u8 = undefined;
    _ = &afz_lo_ctx_offsets_39;
    var afz_nonsquare_tx_38: c_uint = undefined;
    _ = &afz_nonsquare_tx_38;
    var afz_scan_37: [*c]u16 = undefined;
    _ = &afz_scan_37;
    var afz_mag_36: c_uint = undefined;
    _ = &afz_mag_36;
    var afz_level_tok_35: c_int = undefined;
    _ = &afz_level_tok_35;
    var afz_tok_34: c_int = undefined;
    _ = &afz_tok_34;
    var afz_eob_tok_33: c_int = undefined;
    _ = &afz_eob_tok_33;
    var afz_ctx_32: c_uint = undefined;
    _ = &afz_ctx_32;
    var afz_levels_31: [*c]u8 = undefined;
    _ = &afz_levels_31;
    var afz_lo_cdf_30: [*c][4]u16 = undefined;
    _ = &afz_lo_cdf_30;
    var afz_tok_br_69: c_int = undefined;
    _ = &afz_tok_br_69;
    var afz_rc_28: c_uint = undefined;
    _ = &afz_rc_28;
    var afz_dc_tok_29: c_uint = undefined;
    _ = &afz_dc_tok_29;
    var afz_hi_cdf_27: [*c][4]u16 = undefined;
    _ = &afz_hi_cdf_27;
    var afz_eob_cdf_26: [*c][4]u16 = undefined;
    _ = &afz_eob_cdf_26;
    var afz_eob_hi_bit_25: c_int = undefined;
    _ = &afz_eob_hi_bit_25;
    var afz_eob_hi_bit_cdf_24: [*c]u16 = undefined;
    _ = &afz_eob_hi_bit_cdf_24;
    var afz_eob_23: c_int = undefined;
    _ = &afz_eob_23;
    var afz_eob_bin_cdf_22: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_22;
    var afz_eob_bin_cdf_21: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_21;
    var afz_eob_bin_cdf_20: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_20;
    var afz_eob_bin_cdf_19: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_19;
    var afz_eob_bin_cdf_18: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_18;
    var afz_eob_bin_cdf_17: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_17;
    var afz_eob_bin_cdf_16: [*c]u16 = undefined;
    _ = &afz_eob_bin_cdf_16;
    var afz_is_1d_15: c_int = undefined;
    _ = &afz_is_1d_15;
    var afz_tx_class_14: enum_TxClass = undefined;
    _ = &afz_tx_class_14;
    var afz_tx2dszctx_13: c_int = undefined;
    _ = &afz_tx2dszctx_13;
    var afz_slw_11: c_int = undefined;
    _ = &afz_slw_11;
    var afz_slh_12: c_int = undefined;
    _ = &afz_slh_12;
    var afz_eob_bin_10: c_int = undefined;
    _ = &afz_eob_bin_10;
    var afz_y_mode_nofilt_9: enum_IntraPredMode = undefined;
    _ = &afz_y_mode_nofilt_9;
    var afz_idx_8: c_uint = undefined;
    _ = &afz_idx_8;
    var afz_all_skip_7: c_int = undefined;
    _ = &afz_all_skip_7;
    var afz_sctx_6: c_int = undefined;
    _ = &afz_sctx_6;
    var afz_dbg_5: c_int = undefined;
    _ = &afz_dbg_5;
    var afz_t_dim_4: [*c]TxfmInfo = undefined;
    _ = &afz_t_dim_4;
    var afz_lossless_3: c_int = undefined;
    _ = &afz_lossless_3;
    var afz_f_2: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_2;
    var afz_chroma_1: c_int = undefined;
    _ = &afz_chroma_1;
    var afz_ts_0: [*c]Dav1dTileState = undefined;
    _ = &afz_ts_0;
    var afz_pc: c_uint = 436;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return afz_eob_23;
            },
            2 => {
                res_ctx.* = @truncate(umin(afz_cul_level_74, 63) | afz_dc_sign_level_75);
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_28 != 0) @as(c_int, 25) else @as(c_int, 2)));
                continue;
            },
            4 => {
                afz_rc_28 = afz_rc_tok_82 & @as(c_uint, 1023);
                afz_pc = 3;
                continue;
            },
            5 => {
                cf[afz_rc_28] = if (afz_sign_81 != 0) -afz_dq_sat_85 else afz_dq_sat_85;
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_dq_sat_85 = @bitCast(@as(c_uint, @truncate(umin(afz_dq_84, @bitCast(@as(c_int, afz_cf_max_73 + afz_sign_81))))));
                afz_pc = 5;
                continue;
            },
            7 => {
                afz_dq_84 >>= @intCast(@as(c_uint, @bitCast(@as(c_int, afz_dq_shift_72))));
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_cul_level_74 +%= afz_tok_83;
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_dq_84 = (afz_dq_84 *% afz_tok_83) & @as(c_uint, 16777215);
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_tok_83 &= 1048575;
                afz_pc = 9;
                continue;
            },
            11 => {
                _ = @as(c_int, 0);
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 11) else @as(c_int, 10)));
                continue;
            },
            13 => {
                afz_tok_83 = read_golomb(&afz_ts_0.*.msac) +% @as(c_uint, 15);
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 16) else @as(c_int, 8);
                continue;
            },
            15 => {
                unreachable;
            },
            16 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_dq_84 <= @as(c_uint, 16777215))) @as(c_int, 15) else @as(c_int, 14)));
                continue;
            },
            17 => {
                afz_dq_84 *%= afz_tok_83;
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_tok_83 = afz_rc_tok_82 >> @intCast(@as(c_uint, 11));
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_tok_82 >= @as(c_uint, @bitCast(@as(c_int, @as(c_int, 15) << @intCast(@as(c_int, 11)))))) @as(c_int, 13) else @as(c_int, 18)));
                continue;
            },
            20 => {
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_dq_84 = ((afz_ac_dq_80 *% @as(c_uint, @bitCast(@as(c_int, @as(c_int, afz_qm_tbl_71[afz_rc_28]))))) +% @as(c_uint, 16)) >> @intCast(@as(c_uint, 5));
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_rc_tok_82 = @bitCast(@as(c_int, cf[afz_rc_28]));
                afz_pc = 21;
                continue;
            },
            23 => {
                _ = @as(c_int, 0);
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 23) else @as(c_int, 22)));
                continue;
            },
            25 => {
                afz_sign_81 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_equi_c(&afz_ts_0.*.msac))));
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_ac_dq_80 = afz_dq_tbl_70[@as(c_int, 1)];
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_28 != 0) @as(c_int, 26) else @as(c_int, 2)));
                continue;
            },
            28 => {
                cf[@as(c_int, 0)] = if (afz_dc_sign_78 != 0) -afz_dc_dq_79 else afz_dc_dq_79;
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_dc_dq_79 = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, afz_dc_dq_79)), @bitCast(@as(c_int, afz_cf_max_73 + afz_dc_sign_78))))));
                afz_pc = 28;
                continue;
            },
            30 => {
                afz_dc_dq_79 >>= @intCast(afz_dq_shift_72);
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_cul_level_74 = afz_dc_tok_29;
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_dc_dq_79 = @bitCast(@as(c_uint, @truncate((@as(c_uint, @bitCast(@as(c_int, afz_dc_dq_79))) *% afz_dc_tok_29) & @as(c_uint, 16777215))));
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_dc_tok_29 &= 1048575;
                afz_pc = 32;
                continue;
            },
            34 => {
                _ = @as(c_int, 0);
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 34) else @as(c_int, 33)));
                continue;
            },
            36 => {
                afz_dc_tok_29 = read_golomb(&afz_ts_0.*.msac) +% @as(c_uint, 15);
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 39) else @as(c_int, 31);
                continue;
            },
            38 => {
                unreachable;
            },
            39 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_dc_dq_79 <= @as(c_int, 16777215))) @as(c_int, 38) else @as(c_int, 37)));
                continue;
            },
            40 => {
                {
                    const ref = &afz_dc_dq_79;
                    ref.* = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, ref.*))) *% afz_dc_tok_29)));
                }
                afz_pc = 39;
                continue;
            },
            41 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dc_tok_29 == @as(c_uint, 15)) @as(c_int, 36) else @as(c_int, 40)));
                continue;
            },
            42 => {
                afz_dc_dq_79 = ((afz_dc_dq_79 * @as(c_int, afz_qm_tbl_71[@as(c_int, 0)])) + @as(c_int, 16)) >> @intCast(@as(c_int, 5));
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_28 != 0) @as(c_int, 64) else @as(c_int, 2)));
                continue;
            },
            44 => {
                afz_rc_28 = afz_rc_tok_88 & @as(c_uint, 1023);
                afz_pc = 43;
                continue;
            },
            45 => {
                cf[afz_rc_28] = if (afz_sign_87 != 0) -afz_dq_90 else afz_dq_90;
                afz_pc = 44;
                continue;
            },
            46 => {
                afz_cul_level_74 +%= afz_tok_89;
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_dq_90 = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, afz_dq_90)), @bitCast(@as(c_int, afz_cf_max_73 + afz_sign_87))))));
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_dq_90 = @bitCast(@as(c_uint, @truncate(((afz_ac_dq_86 *% afz_tok_89) & @as(c_uint, 16777215)) >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_dq_shift_72)))))));
                afz_pc = 47;
                continue;
            },
            49 => {
                afz_tok_89 &= 1048575;
                afz_pc = 48;
                continue;
            },
            50 => {
                _ = @as(c_int, 0);
                afz_pc = 49;
                continue;
            },
            51 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 50) else @as(c_int, 49)));
                continue;
            },
            52 => {
                afz_tok_89 = read_golomb(&afz_ts_0.*.msac) +% @as(c_uint, 15);
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 55) else @as(c_int, 46);
                continue;
            },
            54 => {
                unreachable;
            },
            55 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_dq_90 <= afz_cf_max_73)) @as(c_int, 54) else @as(c_int, 53)));
                continue;
            },
            56 => {
                afz_dq_90 = @bitCast(@as(c_uint, @truncate((afz_ac_dq_86 *% afz_tok_89) >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_dq_shift_72)))))));
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_tok_89 = afz_rc_tok_88 >> @intCast(@as(c_uint, 11));
                afz_pc = 56;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_tok_88 >= @as(c_uint, @bitCast(@as(c_int, @as(c_int, 15) << @intCast(@as(c_int, 11)))))) @as(c_int, 52) else @as(c_int, 57)));
                continue;
            },
            59 => {
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_rc_tok_88 = @bitCast(@as(c_int, cf[afz_rc_28]));
                afz_pc = 60;
                continue;
            },
            62 => {
                _ = @as(c_int, 0);
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 62) else @as(c_int, 61)));
                continue;
            },
            64 => {
                afz_sign_87 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_equi_c(&afz_ts_0.*.msac))));
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_ac_dq_86 = afz_dq_tbl_70[@as(c_int, 1)];
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_pc = @bitCast(@as(c_int, if (afz_rc_28 != 0) @as(c_int, 65) else @as(c_int, 2)));
                continue;
            },
            67 => {
                cf[@as(c_int, 0)] = if (afz_dc_sign_78 != 0) -afz_dc_dq_79 else afz_dc_dq_79;
                afz_pc = 66;
                continue;
            },
            68 => {
                afz_cul_level_74 = afz_dc_tok_29;
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_dc_dq_79 = @bitCast(@as(c_uint, @truncate(umin(@bitCast(@as(c_int, afz_dc_dq_79)), @bitCast(@as(c_int, afz_cf_max_73 + afz_dc_sign_78))))));
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_dc_dq_79 = @bitCast(@as(c_uint, @truncate(((@as(c_uint, @bitCast(@as(c_int, afz_dc_dq_79))) *% afz_dc_tok_29) & @as(c_uint, 16777215)) >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_dq_shift_72)))))));
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_dc_tok_29 &= 1048575;
                afz_pc = 70;
                continue;
            },
            72 => {
                _ = @as(c_int, 0);
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 72) else @as(c_int, 71)));
                continue;
            },
            74 => {
                afz_dc_tok_29 = read_golomb(&afz_ts_0.*.msac) +% @as(c_uint, 15);
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 77) else @as(c_int, 68);
                continue;
            },
            76 => {
                unreachable;
            },
            77 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_dc_dq_79 <= afz_cf_max_73)) @as(c_int, 76) else @as(c_int, 75)));
                continue;
            },
            78 => {
                afz_dc_dq_79 = @bitCast(@as(c_uint, @truncate((@as(c_uint, @bitCast(@as(c_int, afz_dc_dq_79))) *% afz_dc_tok_29) >> @intCast(@as(c_uint, @bitCast(@as(c_int, afz_dq_shift_72)))))));
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dc_tok_29 == @as(c_uint, 15)) @as(c_int, 74) else @as(c_int, 78)));
                continue;
            },
            80 => {
                afz_pc = @bitCast(@as(c_int, if (afz_qm_tbl_71 != null) @as(c_int, 42) else @as(c_int, 79)));
                continue;
            },
            81 => {
                afz_dc_sign_level_75 = @bitCast(@as(c_int, (afz_dc_sign_78 - @as(c_int, 1)) & (@as(c_int, 2) << @intCast(@as(c_int, 6)))));
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_dc_dq_79 = afz_dq_tbl_70[@as(c_int, 0)];
                afz_pc = 81;
                continue;
            },
            83 => {
                _ = @as(c_int, 0);
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 83) else @as(c_int, 82)));
                continue;
            },
            85 => {
                afz_dc_sign_78 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&afz_ts_0.*.msac, afz_dc_sign_cdf_77))));
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_dc_sign_cdf_77 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.dc_sign[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_dc_sign_ctx_76)))]));
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_dc_sign_ctx_76 = @bitCast(@as(c_uint, @truncate(get_dc_sign_ctx(@bitCast(tx), a, l))));
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_pc = 65;
                continue;
            },
            89 => {
                afz_pc = 26;
                continue;
            },
            90 => {
                afz_pc = @bitCast(@as(c_int, if (afz_qm_tbl_71 != null) @as(c_int, 89) else @as(c_int, 88)));
                continue;
            },
            91 => {
                afz_dc_sign_level_75 = @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, 6))));
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_cul_level_74 = 0;
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_dc_tok_29 != 0)) @as(c_int, 92) else @as(c_int, 87)));
                continue;
            },
            94 => {
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_cf_max_73 = @bitCast(@as(c_uint, @truncate(~(~@as(c_uint, 127) << @intCast(@as(c_uint, @bitCast(@as(c_int, if (@as(c_int, 16) == @as(c_int, 8)) @as(c_int, 8) else afz_f_2.*.cur.p.bpc))))))));
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_dq_shift_72 = imax(0, @as(c_int, afz_t_dim_4.*.ctx) - @as(c_int, 2));
                afz_pc = 95;
                continue;
            },
            97 => {
                afz_qm_tbl_71 = @ptrCast(@alignCast(@constCast(if (txtp.* < @as(enum_TxfmType, IDTX)) @as(?*const anyopaque, @ptrCast(@alignCast(afz_f_2.*.qm[tx][@bitCast(@as(isize, @intCast(plane)))]))) else @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))))));
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_dq_tbl_70 = @ptrCast(@alignCast(@constCast(@as([*c]const u16, @ptrCast(@alignCast(&afz_ts_0.*.dq[b.*.seg_id][@bitCast(@as(isize, @intCast(plane)))]))))));
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 101) else @as(c_int, 98);
                continue;
            },
            100 => {
                unreachable;
            },
            101 => {
                afz_pc = if (!(@as(c_int, 0) != 0)) @as(c_int, 100) else @as(c_int, 99);
                continue;
            },
            102 => {
                afz_pc = 98;
                continue;
            },
            103 => {
                _ = @as(c_int, 0);
                afz_pc = 102;
                continue;
            },
            104 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 103) else @as(c_int, 102)));
                continue;
            },
            105 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32])));
                afz_pc = 104;
                continue;
            },
            106 => {
                afz_ctx_32 = if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1));
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_mag_36 &= 63;
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_mag_36 = @bitCast(@as(c_int, (@as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * afz_stride_60) + @as(ptrdiff_t, 1))))]) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_60) + @as(ptrdiff_t, 0))))])) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_60) + @as(ptrdiff_t, 1))))])));
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_2D) @as(c_int, 108) else @as(c_int, 107);
                continue;
            },
            110 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dc_tok_29 == @as(c_uint, 3)) @as(c_int, 109) else @as(c_int, 102)));
                continue;
            },
            111 => {
                _ = @as(c_int, 0);
                afz_pc = 110;
                continue;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 111) else @as(c_int, 110)));
                continue;
            },
            113 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3);
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_ctx_32 = if (TX_CLASS_V == TX_CLASS_2D) @as(c_uint, 0) else get_lo_ctx(afz_levels_31, TX_CLASS_V, &afz_mag_36, afz_lo_ctx_offsets_59, 0, 0, afz_stride_60);
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_67 > @as(c_int, 0)) @as(c_int, 149) else @as(c_int, 114)));
                continue;
            },
            116 => {
                afz_i_67 -= 1;
                afz_pc = 115;
                continue;
            },
            117 => {
                afz_rc_28 = afz_rc_i_68;
                afz_pc = 116;
                continue;
            },
            118 => {
                cf[afz_rc_i_68] = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 << @intCast(@as(c_int, 11))))) | afz_rc_28)));
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_level_66.* = @bitCast(@as(i8, @truncate(afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6))))));
                afz_pc = 118;
                continue;
            },
            120 => {
                _ = @as(c_int, 0);
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 120) else @as(c_int, 119)));
                continue;
            },
            122 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 121;
                continue;
            },
            123 => {
                afz_ctx_32 = @as(c_uint, @bitCast(@as(c_int, if (afz_y_65 > @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(TX_CLASS_V == TX_CLASS_2D)))))) @as(c_int, 14) else @as(c_int, 7)))) +% (if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1)));
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_mag_36 &= 63;
                afz_pc = 123;
                continue;
            },
            125 => {
                cf[afz_rc_i_68] = afz_tok_34;
                afz_pc = 116;
                continue;
            },
            126 => {
                afz_rc_28 = afz_rc_i_68;
                afz_pc = 125;
                continue;
            },
            127 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 != 0) @as(c_int, 126) else @as(c_int, 125)));
                continue;
            },
            128 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 >> @intCast(@as(c_int, 9))))) & (afz_rc_28 +% ~@as(c_uint, 2047)))));
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_level_66.* = @bitCast(@as(i8, @truncate(afz_tok_34)));
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_tok_34 *= 1572673;
                afz_pc = 129;
                continue;
            },
            131 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 == @as(c_int, 3)) @as(c_int, 124) else @as(c_int, 130)));
                continue;
            },
            132 => {
                _ = @as(c_int, 0);
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 132) else @as(c_int, 131)));
                continue;
            },
            134 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3))));
                afz_pc = 133;
                continue;
            },
            135 => {
                afz_y_65 |= afz_x_64;
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_2D) @as(c_int, 135) else @as(c_int, 134);
                continue;
            },
            137 => {
                afz_ctx_32 = get_lo_ctx(afz_level_66, TX_CLASS_V, &afz_mag_36, afz_lo_ctx_offsets_59, afz_x_64, afz_y_65, afz_stride_60);
                afz_pc = 136;
                continue;
            },
            138 => {
                afz_level_66 = afz_levels_31 + afz_rc_28;
                afz_pc = 137;
                continue;
            },
            139 => {
                afz_level_66 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_64) * afz_stride_60))))) + afz_y_65;
                afz_pc = 137;
                continue;
            },
            140 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 138) else @as(c_int, 139);
                continue;
            },
            141 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 143) else @as(c_int, 140);
                continue;
            },
            142 => {
                unreachable;
            },
            143 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_x_64 < @as(c_uint, 32)) and (afz_y_65 < @as(c_uint, 32)))) @as(c_int, 142) else @as(c_int, 141)));
                continue;
            },
            144 => {
                afz_rc_i_68 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_i_67)))];
                afz_x_64 = afz_rc_i_68 >> @intCast(afz_shift_61);
                afz_y_65 = afz_rc_i_68 & afz_mask_63;
                afz_pc = 143;
                continue;
            },
            145 => {
                afz_x_64 = @as(c_uint, @bitCast(@as(c_int, afz_i_67))) & afz_mask_63;
                afz_y_65 = @as(c_uint, @bitCast(@as(c_int, afz_i_67))) >> @intCast(afz_shift_61);
                afz_rc_i_68 = @bitCast(@as(c_int, afz_i_67));
                afz_pc = 143;
                continue;
            },
            146 => {
                afz_x_64 = @as(c_uint, @bitCast(@as(c_int, afz_i_67))) & afz_mask_63;
                afz_y_65 = @as(c_uint, @bitCast(@as(c_int, afz_i_67))) >> @intCast(afz_shift_61);
                afz_rc_i_68 = (afz_x_64 << @intCast(afz_shift2_62)) | afz_y_65;
                afz_pc = 143;
                continue;
            },
            147 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_H) @as(c_int, 145) else @as(c_int, 146);
                continue;
            },
            148 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_2D) @as(c_int, 144) else @as(c_int, 147);
                continue;
            },
            149 => {
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_i_67 = afz_eob_23 - @as(c_int, 1);
                afz_pc = 115;
                continue;
            },
            151 => {
                afz_level_66.* = @bitCast(@as(i8, @truncate(afz_level_tok_35)));
                afz_pc = 150;
                continue;
            },
            152 => {
                afz_level_66 = afz_levels_31 + afz_rc_28;
                afz_pc = 151;
                continue;
            },
            153 => {
                afz_level_66 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_64) * afz_stride_60))))) + afz_y_65;
                afz_pc = 151;
                continue;
            },
            154 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 152) else @as(c_int, 153);
                continue;
            },
            155 => {
                cf[afz_rc_28] = afz_tok_34 << @intCast(@as(c_int, 11));
                afz_pc = 154;
                continue;
            },
            156 => {
                _ = @as(c_int, 0);
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 156) else @as(c_int, 155)));
                continue;
            },
            158 => {
                afz_level_tok_35 = afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6)));
                afz_pc = 157;
                continue;
            },
            159 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 158;
                continue;
            },
            160 => {
                afz_ctx_32 = @bitCast(@as(c_int, if ((if (TX_CLASS_V == TX_CLASS_2D) @as(c_int, @intFromBool((afz_x_64 | afz_y_65) > @as(c_uint, 1))) else @as(c_int, @intFromBool(afz_y_65 != @as(c_uint, 0)))) != 0) @as(c_int, 14) else @as(c_int, 7)));
                afz_pc = 159;
                continue;
            },
            161 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_tok_33 == @as(c_int, 2)) @as(c_int, 160) else @as(c_int, 155)));
                continue;
            },
            162 => {
                _ = @as(c_int, 0);
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 162) else @as(c_int, 161)));
                continue;
            },
            164 => {
                afz_rc_28 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_eob_23)))];
                afz_x_64 = afz_rc_28 >> @intCast(afz_shift_61);
                afz_y_65 = afz_rc_28 & afz_mask_63;
                afz_pc = 163;
                continue;
            },
            165 => {
                afz_x_64 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_63;
                afz_y_65 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_61);
                afz_rc_28 = @bitCast(@as(c_int, afz_eob_23));
                afz_pc = 163;
                continue;
            },
            166 => {
                afz_x_64 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_63;
                afz_y_65 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_61);
                afz_rc_28 = (afz_x_64 << @intCast(afz_shift2_62)) | afz_y_65;
                afz_pc = 163;
                continue;
            },
            167 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_H) @as(c_int, 165) else @as(c_int, 166);
                continue;
            },
            168 => {
                afz_pc = if (TX_CLASS_V == TX_CLASS_2D) @as(c_int, 164) else @as(c_int, 167);
                continue;
            },
            169 => {
                afz_pc = 168;
                continue;
            },
            170 => {
                afz_pc = 169;
                continue;
            },
            171 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_levels_31)), 0, @bitCast(@as(c_long, afz_stride_60 * @as(ptrdiff_t, (@as(c_int, 4) << @intCast(afz_slw_11)) + @as(c_int, 2)))));
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_mask_63 = @bitCast(@as(c_int, (@as(c_int, 4) << @intCast(afz_slw_11)) - @as(c_int, 1)));
                afz_pc = 171;
                continue;
            },
            173 => {
                afz_shift_61 = @bitCast(@as(c_int, afz_slw_11 + @as(c_int, 2)));
                afz_shift2_62 = @bitCast(@as(c_int, afz_slh_12 + @as(c_int, 2)));
                afz_pc = 172;
                continue;
            },
            174 => {
                afz_stride_60 = 16;
                afz_pc = 173;
                continue;
            },
            175 => {
                afz_lo_ctx_offsets_59 = null;
                afz_pc = 174;
                continue;
            },
            176 => {
                afz_pc = 98;
                continue;
            },
            177 => {
                _ = @as(c_int, 0);
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 177) else @as(c_int, 176)));
                continue;
            },
            179 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32])));
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_ctx_32 = if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1));
                afz_pc = 179;
                continue;
            },
            181 => {
                afz_mag_36 &= 63;
                afz_pc = 180;
                continue;
            },
            182 => {
                afz_mag_36 = @bitCast(@as(c_int, (@as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * afz_stride_50) + @as(ptrdiff_t, 1))))]) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_50) + @as(ptrdiff_t, 0))))])) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_50) + @as(ptrdiff_t, 1))))])));
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_2D) @as(c_int, 182) else @as(c_int, 181);
                continue;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dc_tok_29 == @as(c_uint, 3)) @as(c_int, 183) else @as(c_int, 176)));
                continue;
            },
            185 => {
                _ = @as(c_int, 0);
                afz_pc = 184;
                continue;
            },
            186 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 185) else @as(c_int, 184)));
                continue;
            },
            187 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3);
                afz_pc = 186;
                continue;
            },
            188 => {
                afz_ctx_32 = if (TX_CLASS_H == TX_CLASS_2D) @as(c_uint, 0) else get_lo_ctx(afz_levels_31, TX_CLASS_H, &afz_mag_36, afz_lo_ctx_offsets_49, 0, 0, afz_stride_50);
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_57 > @as(c_int, 0)) @as(c_int, 223) else @as(c_int, 188)));
                continue;
            },
            190 => {
                afz_i_57 -= 1;
                afz_pc = 189;
                continue;
            },
            191 => {
                afz_rc_28 = afz_rc_i_58;
                afz_pc = 190;
                continue;
            },
            192 => {
                cf[afz_rc_i_58] = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 << @intCast(@as(c_int, 11))))) | afz_rc_28)));
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_level_56.* = @bitCast(@as(i8, @truncate(afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6))))));
                afz_pc = 192;
                continue;
            },
            194 => {
                _ = @as(c_int, 0);
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 194) else @as(c_int, 193)));
                continue;
            },
            196 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_ctx_32 = @as(c_uint, @bitCast(@as(c_int, if (afz_y_55 > @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(TX_CLASS_H == TX_CLASS_2D)))))) @as(c_int, 14) else @as(c_int, 7)))) +% (if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1)));
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_mag_36 &= 63;
                afz_pc = 197;
                continue;
            },
            199 => {
                cf[afz_rc_i_58] = afz_tok_34;
                afz_pc = 190;
                continue;
            },
            200 => {
                afz_rc_28 = afz_rc_i_58;
                afz_pc = 199;
                continue;
            },
            201 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 != 0) @as(c_int, 200) else @as(c_int, 199)));
                continue;
            },
            202 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 >> @intCast(@as(c_int, 9))))) & (afz_rc_28 +% ~@as(c_uint, 2047)))));
                afz_pc = 201;
                continue;
            },
            203 => {
                afz_level_56.* = @bitCast(@as(i8, @truncate(afz_tok_34)));
                afz_pc = 202;
                continue;
            },
            204 => {
                afz_tok_34 *= 1572673;
                afz_pc = 203;
                continue;
            },
            205 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 == @as(c_int, 3)) @as(c_int, 198) else @as(c_int, 204)));
                continue;
            },
            206 => {
                _ = @as(c_int, 0);
                afz_pc = 205;
                continue;
            },
            207 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 206) else @as(c_int, 205)));
                continue;
            },
            208 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3))));
                afz_pc = 207;
                continue;
            },
            209 => {
                afz_y_55 |= afz_x_54;
                afz_pc = 208;
                continue;
            },
            210 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_2D) @as(c_int, 209) else @as(c_int, 208);
                continue;
            },
            211 => {
                afz_ctx_32 = get_lo_ctx(afz_level_56, TX_CLASS_H, &afz_mag_36, afz_lo_ctx_offsets_49, afz_x_54, afz_y_55, afz_stride_50);
                afz_pc = 210;
                continue;
            },
            212 => {
                afz_level_56 = afz_levels_31 + afz_rc_28;
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_level_56 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_54) * afz_stride_50))))) + afz_y_55;
                afz_pc = 211;
                continue;
            },
            214 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 212) else @as(c_int, 213);
                continue;
            },
            215 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 217) else @as(c_int, 214);
                continue;
            },
            216 => {
                unreachable;
            },
            217 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_x_54 < @as(c_uint, 32)) and (afz_y_55 < @as(c_uint, 32)))) @as(c_int, 216) else @as(c_int, 215)));
                continue;
            },
            218 => {
                afz_rc_i_58 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_i_57)))];
                afz_x_54 = afz_rc_i_58 >> @intCast(afz_shift_51);
                afz_y_55 = afz_rc_i_58 & afz_mask_53;
                afz_pc = 217;
                continue;
            },
            219 => {
                afz_x_54 = @as(c_uint, @bitCast(@as(c_int, afz_i_57))) & afz_mask_53;
                afz_y_55 = @as(c_uint, @bitCast(@as(c_int, afz_i_57))) >> @intCast(afz_shift_51);
                afz_rc_i_58 = @bitCast(@as(c_int, afz_i_57));
                afz_pc = 217;
                continue;
            },
            220 => {
                afz_x_54 = @as(c_uint, @bitCast(@as(c_int, afz_i_57))) & afz_mask_53;
                afz_y_55 = @as(c_uint, @bitCast(@as(c_int, afz_i_57))) >> @intCast(afz_shift_51);
                afz_rc_i_58 = (afz_x_54 << @intCast(afz_shift2_52)) | afz_y_55;
                afz_pc = 217;
                continue;
            },
            221 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_H) @as(c_int, 219) else @as(c_int, 220);
                continue;
            },
            222 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_2D) @as(c_int, 218) else @as(c_int, 221);
                continue;
            },
            223 => {
                afz_pc = 222;
                continue;
            },
            224 => {
                afz_i_57 = afz_eob_23 - @as(c_int, 1);
                afz_pc = 189;
                continue;
            },
            225 => {
                afz_level_56.* = @bitCast(@as(i8, @truncate(afz_level_tok_35)));
                afz_pc = 224;
                continue;
            },
            226 => {
                afz_level_56 = afz_levels_31 + afz_rc_28;
                afz_pc = 225;
                continue;
            },
            227 => {
                afz_level_56 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_54) * afz_stride_50))))) + afz_y_55;
                afz_pc = 225;
                continue;
            },
            228 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 226) else @as(c_int, 227);
                continue;
            },
            229 => {
                cf[afz_rc_28] = afz_tok_34 << @intCast(@as(c_int, 11));
                afz_pc = 228;
                continue;
            },
            230 => {
                _ = @as(c_int, 0);
                afz_pc = 229;
                continue;
            },
            231 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 230) else @as(c_int, 229)));
                continue;
            },
            232 => {
                afz_level_tok_35 = afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6)));
                afz_pc = 231;
                continue;
            },
            233 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_ctx_32 = @bitCast(@as(c_int, if ((if (TX_CLASS_H == TX_CLASS_2D) @as(c_int, @intFromBool((afz_x_54 | afz_y_55) > @as(c_uint, 1))) else @as(c_int, @intFromBool(afz_y_55 != @as(c_uint, 0)))) != 0) @as(c_int, 14) else @as(c_int, 7)));
                afz_pc = 233;
                continue;
            },
            235 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_tok_33 == @as(c_int, 2)) @as(c_int, 234) else @as(c_int, 229)));
                continue;
            },
            236 => {
                _ = @as(c_int, 0);
                afz_pc = 235;
                continue;
            },
            237 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 236) else @as(c_int, 235)));
                continue;
            },
            238 => {
                afz_rc_28 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_eob_23)))];
                afz_x_54 = afz_rc_28 >> @intCast(afz_shift_51);
                afz_y_55 = afz_rc_28 & afz_mask_53;
                afz_pc = 237;
                continue;
            },
            239 => {
                afz_x_54 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_53;
                afz_y_55 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_51);
                afz_rc_28 = @bitCast(@as(c_int, afz_eob_23));
                afz_pc = 237;
                continue;
            },
            240 => {
                afz_x_54 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_53;
                afz_y_55 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_51);
                afz_rc_28 = (afz_x_54 << @intCast(afz_shift2_52)) | afz_y_55;
                afz_pc = 237;
                continue;
            },
            241 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_H) @as(c_int, 239) else @as(c_int, 240);
                continue;
            },
            242 => {
                afz_pc = if (TX_CLASS_H == TX_CLASS_2D) @as(c_int, 238) else @as(c_int, 241);
                continue;
            },
            243 => {
                afz_pc = 242;
                continue;
            },
            244 => {
                afz_pc = 243;
                continue;
            },
            245 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_levels_31)), 0, @bitCast(@as(c_long, afz_stride_50 * @as(ptrdiff_t, (@as(c_int, 4) << @intCast(afz_slh_12)) + @as(c_int, 2)))));
                afz_pc = 244;
                continue;
            },
            246 => {
                afz_mask_53 = @bitCast(@as(c_int, (@as(c_int, 4) << @intCast(afz_slh_12)) - @as(c_int, 1)));
                afz_pc = 245;
                continue;
            },
            247 => {
                afz_shift_51 = @bitCast(@as(c_int, afz_slh_12 + @as(c_int, 2)));
                afz_shift2_52 = 0;
                afz_pc = 246;
                continue;
            },
            248 => {
                afz_stride_50 = 16;
                afz_pc = 247;
                continue;
            },
            249 => {
                afz_lo_ctx_offsets_49 = null;
                afz_pc = 248;
                continue;
            },
            250 => {
                afz_pc = 98;
                continue;
            },
            251 => {
                _ = @as(c_int, 0);
                afz_pc = 250;
                continue;
            },
            252 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 251) else @as(c_int, 250)));
                continue;
            },
            253 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32])));
                afz_pc = 252;
                continue;
            },
            254 => {
                afz_ctx_32 = if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1));
                afz_pc = 253;
                continue;
            },
            255 => {
                afz_mag_36 &= 63;
                afz_pc = 254;
                continue;
            },
            256 => {
                afz_mag_36 = @bitCast(@as(c_int, (@as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 0) * afz_stride_40) + @as(ptrdiff_t, 1))))]) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_40) + @as(ptrdiff_t, 0))))])) + @as(c_int, afz_levels_31[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 1) * afz_stride_40) + @as(ptrdiff_t, 1))))])));
                afz_pc = 255;
                continue;
            },
            257 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_2D) @as(c_int, 256) else @as(c_int, 255);
                continue;
            },
            258 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dc_tok_29 == @as(c_uint, 3)) @as(c_int, 257) else @as(c_int, 250)));
                continue;
            },
            259 => {
                _ = @as(c_int, 0);
                afz_pc = 258;
                continue;
            },
            260 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 259) else @as(c_int, 258)));
                continue;
            },
            261 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3);
                afz_pc = 260;
                continue;
            },
            262 => {
                afz_ctx_32 = if (TX_CLASS_2D == TX_CLASS_2D) @as(c_uint, 0) else get_lo_ctx(afz_levels_31, TX_CLASS_2D, &afz_mag_36, afz_lo_ctx_offsets_39, 0, 0, afz_stride_40);
                afz_pc = 261;
                continue;
            },
            263 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_47 > @as(c_int, 0)) @as(c_int, 297) else @as(c_int, 262)));
                continue;
            },
            264 => {
                afz_i_47 -= 1;
                afz_pc = 263;
                continue;
            },
            265 => {
                afz_rc_28 = afz_rc_i_48;
                afz_pc = 264;
                continue;
            },
            266 => {
                cf[afz_rc_i_48] = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 << @intCast(@as(c_int, 11))))) | afz_rc_28)));
                afz_pc = 265;
                continue;
            },
            267 => {
                afz_level_46.* = @bitCast(@as(i8, @truncate(afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6))))));
                afz_pc = 266;
                continue;
            },
            268 => {
                _ = @as(c_int, 0);
                afz_pc = 267;
                continue;
            },
            269 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 268) else @as(c_int, 267)));
                continue;
            },
            270 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 269;
                continue;
            },
            271 => {
                afz_ctx_32 = @as(c_uint, @bitCast(@as(c_int, if (afz_y_45 > @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(TX_CLASS_2D == TX_CLASS_2D)))))) @as(c_int, 14) else @as(c_int, 7)))) +% (if (afz_mag_36 > @as(c_uint, 12)) @as(c_uint, 6) else (afz_mag_36 +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1)));
                afz_pc = 270;
                continue;
            },
            272 => {
                afz_mag_36 &= 63;
                afz_pc = 271;
                continue;
            },
            273 => {
                cf[afz_rc_i_48] = afz_tok_34;
                afz_pc = 264;
                continue;
            },
            274 => {
                afz_rc_28 = afz_rc_i_48;
                afz_pc = 273;
                continue;
            },
            275 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 != 0) @as(c_int, 274) else @as(c_int, 273)));
                continue;
            },
            276 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, afz_tok_34 >> @intCast(@as(c_int, 9))))) & (afz_rc_28 +% ~@as(c_uint, 2047)))));
                afz_pc = 275;
                continue;
            },
            277 => {
                afz_level_46.* = @bitCast(@as(i8, @truncate(afz_tok_34)));
                afz_pc = 276;
                continue;
            },
            278 => {
                afz_tok_34 *= 1572673;
                afz_pc = 277;
                continue;
            },
            279 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_34 == @as(c_int, 3)) @as(c_int, 272) else @as(c_int, 278)));
                continue;
            },
            280 => {
                _ = @as(c_int, 0);
                afz_pc = 279;
                continue;
            },
            281 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 280) else @as(c_int, 279)));
                continue;
            },
            282 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_lo_cdf_30[afz_ctx_32])), 3))));
                afz_pc = 281;
                continue;
            },
            283 => {
                afz_y_45 |= afz_x_44;
                afz_pc = 282;
                continue;
            },
            284 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_2D) @as(c_int, 283) else @as(c_int, 282);
                continue;
            },
            285 => {
                afz_ctx_32 = get_lo_ctx(afz_level_46, TX_CLASS_2D, &afz_mag_36, afz_lo_ctx_offsets_39, afz_x_44, afz_y_45, afz_stride_40);
                afz_pc = 284;
                continue;
            },
            286 => {
                afz_level_46 = afz_levels_31 + afz_rc_28;
                afz_pc = 285;
                continue;
            },
            287 => {
                afz_level_46 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_44) * afz_stride_40))))) + afz_y_45;
                afz_pc = 285;
                continue;
            },
            288 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 286) else @as(c_int, 287);
                continue;
            },
            289 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 291) else @as(c_int, 288);
                continue;
            },
            290 => {
                unreachable;
            },
            291 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_x_44 < @as(c_uint, 32)) and (afz_y_45 < @as(c_uint, 32)))) @as(c_int, 290) else @as(c_int, 289)));
                continue;
            },
            292 => {
                afz_rc_i_48 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_i_47)))];
                afz_x_44 = afz_rc_i_48 >> @intCast(afz_shift_41);
                afz_y_45 = afz_rc_i_48 & afz_mask_43;
                afz_pc = 291;
                continue;
            },
            293 => {
                afz_x_44 = @as(c_uint, @bitCast(@as(c_int, afz_i_47))) & afz_mask_43;
                afz_y_45 = @as(c_uint, @bitCast(@as(c_int, afz_i_47))) >> @intCast(afz_shift_41);
                afz_rc_i_48 = @bitCast(@as(c_int, afz_i_47));
                afz_pc = 291;
                continue;
            },
            294 => {
                afz_x_44 = @as(c_uint, @bitCast(@as(c_int, afz_i_47))) & afz_mask_43;
                afz_y_45 = @as(c_uint, @bitCast(@as(c_int, afz_i_47))) >> @intCast(afz_shift_41);
                afz_rc_i_48 = (afz_x_44 << @intCast(afz_shift2_42)) | afz_y_45;
                afz_pc = 291;
                continue;
            },
            295 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_H) @as(c_int, 293) else @as(c_int, 294);
                continue;
            },
            296 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_2D) @as(c_int, 292) else @as(c_int, 295);
                continue;
            },
            297 => {
                afz_pc = 296;
                continue;
            },
            298 => {
                afz_i_47 = afz_eob_23 - @as(c_int, 1);
                afz_pc = 263;
                continue;
            },
            299 => {
                afz_level_46.* = @bitCast(@as(i8, @truncate(afz_level_tok_35)));
                afz_pc = 298;
                continue;
            },
            300 => {
                afz_level_46 = afz_levels_31 + afz_rc_28;
                afz_pc = 299;
                continue;
            },
            301 => {
                afz_level_46 = (afz_levels_31 + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, afz_x_44) * afz_stride_40))))) + afz_y_45;
                afz_pc = 299;
                continue;
            },
            302 => {
                afz_pc = if (TX_CLASS_2D != 0) @as(c_int, 300) else @as(c_int, 301);
                continue;
            },
            303 => {
                cf[afz_rc_28] = afz_tok_34 << @intCast(@as(c_int, 11));
                afz_pc = 302;
                continue;
            },
            304 => {
                _ = @as(c_int, 0);
                afz_pc = 303;
                continue;
            },
            305 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 304) else @as(c_int, 303)));
                continue;
            },
            306 => {
                afz_level_tok_35 = afz_tok_34 + (@as(c_int, 3) << @intCast(@as(c_int, 6)));
                afz_pc = 305;
                continue;
            },
            307 => {
                afz_tok_34 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[afz_ctx_32]))))));
                afz_pc = 306;
                continue;
            },
            308 => {
                afz_ctx_32 = @bitCast(@as(c_int, if ((if (TX_CLASS_2D == TX_CLASS_2D) @as(c_int, @intFromBool((afz_x_44 | afz_y_45) > @as(c_uint, 1))) else @as(c_int, @intFromBool(afz_y_45 != @as(c_uint, 0)))) != 0) @as(c_int, 14) else @as(c_int, 7)));
                afz_pc = 307;
                continue;
            },
            309 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_tok_33 == @as(c_int, 2)) @as(c_int, 308) else @as(c_int, 303)));
                continue;
            },
            310 => {
                _ = @as(c_int, 0);
                afz_pc = 309;
                continue;
            },
            311 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 310) else @as(c_int, 309)));
                continue;
            },
            312 => {
                afz_rc_28 = afz_scan_37[@bitCast(@as(isize, @intCast(afz_eob_23)))];
                afz_x_44 = afz_rc_28 >> @intCast(afz_shift_41);
                afz_y_45 = afz_rc_28 & afz_mask_43;
                afz_pc = 311;
                continue;
            },
            313 => {
                afz_x_44 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_43;
                afz_y_45 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_41);
                afz_rc_28 = @bitCast(@as(c_int, afz_eob_23));
                afz_pc = 311;
                continue;
            },
            314 => {
                afz_x_44 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) & afz_mask_43;
                afz_y_45 = @as(c_uint, @bitCast(@as(c_int, afz_eob_23))) >> @intCast(afz_shift_41);
                afz_rc_28 = (afz_x_44 << @intCast(afz_shift2_42)) | afz_y_45;
                afz_pc = 311;
                continue;
            },
            315 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_H) @as(c_int, 313) else @as(c_int, 314);
                continue;
            },
            316 => {
                afz_pc = if (TX_CLASS_2D == TX_CLASS_2D) @as(c_int, 312) else @as(c_int, 315);
                continue;
            },
            317 => {
                afz_pc = 316;
                continue;
            },
            318 => {
                afz_pc = 317;
                continue;
            },
            319 => {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(afz_levels_31)), 0, @bitCast(@as(c_long, afz_stride_40 * @as(ptrdiff_t, (@as(c_int, 4) << @intCast(afz_slw_11)) + @as(c_int, 2)))));
                afz_pc = 318;
                continue;
            },
            320 => {
                afz_mask_43 = @bitCast(@as(c_int, (@as(c_int, 4) << @intCast(afz_slh_12)) - @as(c_int, 1)));
                afz_pc = 319;
                continue;
            },
            321 => {
                afz_shift_41 = @bitCast(@as(c_int, afz_slh_12 + @as(c_int, 2)));
                afz_shift2_42 = 0;
                afz_pc = 320;
                continue;
            },
            322 => {
                afz_stride_40 = @as(c_int, 4) << @intCast(afz_slh_12);
                afz_pc = 321;
                continue;
            },
            323 => {
                afz_scan_37 = @ptrCast(@alignCast(@constCast(antfly_av1_dav1d_scans[tx])));
                afz_pc = 322;
                continue;
            },
            324 => {
                afz_lo_ctx_offsets_39 = @ptrCast(@alignCast(&antfly_av1_dav1d_lo_ctx_offsets[afz_nonsquare_tx_38 +% (tx & afz_nonsquare_tx_38)]));
                afz_pc = 323;
                continue;
            },
            325 => {
                afz_nonsquare_tx_38 = @bitCast(@as(c_int, @as(c_int, @intFromBool(tx >= @as(enum_RectTxfmSize, RTX_4X8)))));
                afz_pc = 324;
                continue;
            },
            326 => {
                blk: {
                    switch (afz_tx_class_14) {
                        @as(enum_TxClass, TX_CLASS_V) => {
                            afz_pc = 175;
                            continue;
                        },
                        @as(enum_TxClass, TX_CLASS_H) => {
                            afz_pc = 249;
                            continue;
                        },
                        @as(enum_TxClass, TX_CLASS_2D) => {
                            afz_pc = 325;
                            continue;
                        },
                        else => {
                            afz_pc = 101;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            327 => {
                afz_pc = 326;
                continue;
            },
            328 => {
                afz_pc = 327;
                continue;
            },
            329 => {
                afz_level_tok_35 = afz_tok_34 * @as(c_int, 65);
                afz_pc = 328;
                continue;
            },
            330 => {
                afz_tok_34 = afz_eob_tok_33 + @as(c_int, 1);
                afz_pc = 329;
                continue;
            },
            331 => {
                afz_eob_tok_33 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_eob_cdf_26[afz_ctx_32])), 2))));
                afz_pc = 330;
                continue;
            },
            332 => {
                afz_ctx_32 = @bitCast(@as(c_int, (@as(c_int, 1) + @as(c_int, @intFromBool(afz_eob_23 > (@as(c_int, 2) << @intCast(afz_tx2dszctx_13))))) + @as(c_int, @intFromBool(afz_eob_23 > (@as(c_int, 4) << @intCast(afz_tx2dszctx_13))))));
                afz_pc = 331;
                continue;
            },
            333 => {
                afz_levels_31 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_0.levels));
                afz_pc = 332;
                continue;
            },
            334 => {
                afz_lo_cdf_30 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.base_tok[afz_t_dim_4.*.ctx][@bitCast(@as(isize, @intCast(afz_chroma_1)))]));
                afz_pc = 333;
                continue;
            },
            335 => {
                afz_rc_28 = 0;
                afz_pc = 98;
                continue;
            },
            336 => {
                _ = @as(c_int, 0);
                afz_pc = 335;
                continue;
            },
            337 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 336) else @as(c_int, 335)));
                continue;
            },
            338 => {
                afz_dc_tok_29 = antfly_av1_dav1d_msac_decode_hi_tok_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_hi_cdf_27[@as(c_int, 0)])));
                afz_pc = 337;
                continue;
            },
            339 => {
                afz_pc = @bitCast(@as(c_int, if (afz_tok_br_69 == @as(c_int, 2)) @as(c_int, 338) else @as(c_int, 335)));
                continue;
            },
            340 => {
                _ = @as(c_int, 0);
                afz_pc = 339;
                continue;
            },
            341 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 340) else @as(c_int, 339)));
                continue;
            },
            342 => {
                afz_dc_tok_29 = @bitCast(@as(c_int, @as(c_int, 1) + afz_tok_br_69));
                afz_pc = 341;
                continue;
            },
            343 => {
                afz_tok_br_69 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_eob_cdf_26[@as(c_int, 0)])), 2))));
                afz_pc = 342;
                continue;
            },
            344 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_23 != 0) @as(c_int, 334) else @as(c_int, 343)));
                continue;
            },
            345 => {
                afz_pc = 344;
                continue;
            },
            346 => {
                afz_hi_cdf_27 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.br_tok[@bitCast(@as(isize, @intCast(imin(afz_t_dim_4.*.ctx, 3))))][@bitCast(@as(isize, @intCast(afz_chroma_1)))]));
                afz_pc = 345;
                continue;
            },
            347 => {
                afz_eob_cdf_26 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_base_tok[afz_t_dim_4.*.ctx][@bitCast(@as(isize, @intCast(afz_chroma_1)))]));
                afz_pc = 346;
                continue;
            },
            348 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 350) else @as(c_int, 347);
                continue;
            },
            349 => {
                unreachable;
            },
            350 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_eob_23 >= @as(c_int, 0))) @as(c_int, 349) else @as(c_int, 348)));
                continue;
            },
            351 => {
                _ = @as(c_int, 0);
                afz_pc = 350;
                continue;
            },
            352 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 351) else @as(c_int, 350)));
                continue;
            },
            353 => {
                afz_eob_23 = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, (afz_eob_hi_bit_25 | @as(c_int, 2)) << @intCast(afz_eob_bin_10 - @as(c_int, 2))))) | dav1d_msac_decode_bools(&afz_ts_0.*.msac, @bitCast(@as(c_int, afz_eob_bin_10 - @as(c_int, 2)))))));
                afz_pc = 352;
                continue;
            },
            354 => {
                _ = @as(c_int, 0);
                afz_pc = 353;
                continue;
            },
            355 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 354) else @as(c_int, 353)));
                continue;
            },
            356 => {
                afz_eob_hi_bit_25 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&afz_ts_0.*.msac, afz_eob_hi_bit_cdf_24))));
                afz_pc = 355;
                continue;
            },
            357 => {
                afz_eob_hi_bit_cdf_24 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_hi_bit[afz_t_dim_4.*.ctx][@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_eob_bin_10)))]));
                afz_pc = 356;
                continue;
            },
            358 => {
                afz_eob_23 = afz_eob_bin_10;
                afz_pc = 350;
                continue;
            },
            359 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_bin_10 > @as(c_int, 1)) @as(c_int, 357) else @as(c_int, 358)));
                continue;
            },
            360 => {
                afz_pc = 359;
                continue;
            },
            361 => {
                _ = @as(c_int, 0);
                afz_pc = 360;
                continue;
            },
            362 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 361) else @as(c_int, 360)));
                continue;
            },
            363 => {
                afz_pc = 362;
                continue;
            },
            364 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_22, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 6)))))));
                afz_pc = 363;
                continue;
            },
            365 => {
                afz_eob_bin_cdf_22 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_1024[@bitCast(@as(isize, @intCast(afz_chroma_1)))]));
                afz_pc = 364;
                continue;
            },
            366 => {
                afz_pc = 362;
                continue;
            },
            367 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_21, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 5)))))));
                afz_pc = 366;
                continue;
            },
            368 => {
                afz_eob_bin_cdf_21 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_512[@bitCast(@as(isize, @intCast(afz_chroma_1)))]));
                afz_pc = 367;
                continue;
            },
            369 => {
                afz_pc = 362;
                continue;
            },
            370 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_20, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 4)))))));
                afz_pc = 369;
                continue;
            },
            371 => {
                afz_eob_bin_cdf_20 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_256[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_is_1d_15)))]));
                afz_pc = 370;
                continue;
            },
            372 => {
                afz_pc = 362;
                continue;
            },
            373 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_19, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 3)))))));
                afz_pc = 372;
                continue;
            },
            374 => {
                afz_eob_bin_cdf_19 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_128[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_is_1d_15)))]));
                afz_pc = 373;
                continue;
            },
            375 => {
                afz_pc = 362;
                continue;
            },
            376 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_18, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 2)))))));
                afz_pc = 375;
                continue;
            },
            377 => {
                afz_eob_bin_cdf_18 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_64[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_is_1d_15)))]));
                afz_pc = 376;
                continue;
            },
            378 => {
                afz_pc = 362;
                continue;
            },
            379 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_17, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 1)))))));
                afz_pc = 378;
                continue;
            },
            380 => {
                afz_eob_bin_cdf_17 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_32[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_is_1d_15)))]));
                afz_pc = 379;
                continue;
            },
            381 => {
                afz_pc = 362;
                continue;
            },
            382 => {
                afz_eob_bin_10 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, afz_eob_bin_cdf_16, @bitCast(@as(c_long, @as(c_int, 4) + @as(c_int, 0)))))));
                afz_pc = 381;
                continue;
            },
            383 => {
                afz_eob_bin_cdf_16 = @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.eob_bin_16[@bitCast(@as(isize, @intCast(afz_chroma_1)))][@bitCast(@as(isize, @intCast(afz_is_1d_15)))]));
                afz_pc = 382;
                continue;
            },
            384 => {
                blk: {
                    switch (afz_tx2dszctx_13) {
                        @as(c_int, 6) => {
                            afz_pc = 365;
                            continue;
                        },
                        @as(c_int, 5) => {
                            afz_pc = 368;
                            continue;
                        },
                        @as(c_int, 4) => {
                            afz_pc = 371;
                            continue;
                        },
                        @as(c_int, 3) => {
                            afz_pc = 374;
                            continue;
                        },
                        @as(c_int, 2) => {
                            afz_pc = 377;
                            continue;
                        },
                        @as(c_int, 1) => {
                            afz_pc = 380;
                            continue;
                        },
                        @as(c_int, 0) => {
                            afz_pc = 383;
                            continue;
                        },
                        else => {
                            afz_pc = 362;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            385 => {
                afz_is_1d_15 = @as(c_int, @intFromBool(afz_tx_class_14 != @as(enum_TxClass, TX_CLASS_2D)));
                afz_pc = 384;
                continue;
            },
            386 => {
                afz_tx_class_14 = antfly_av1_dav1d_tx_type_class[txtp.*];
                afz_pc = 385;
                continue;
            },
            387 => {
                afz_tx2dszctx_13 = afz_slw_11 + afz_slh_12;
                afz_pc = 386;
                continue;
            },
            388 => {
                afz_slw_11 = imin(afz_t_dim_4.*.lw, TX_32X32);
                afz_slh_12 = imin(afz_t_dim_4.*.lh, TX_32X32);
                afz_pc = 387;
                continue;
            },
            389 => {
                afz_pc = 388;
                continue;
            },
            390 => {
                txtp.* = WHT_WHT;
                afz_pc = 389;
                continue;
            },
            391 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 393) else @as(c_int, 390);
                continue;
            },
            392 => {
                unreachable;
            },
            393 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_t_dim_4.*.max) == TX_4X4)) @as(c_int, 392) else @as(c_int, 391)));
                continue;
            },
            394 => {
                txtp.* = DCT_DCT;
                afz_pc = 389;
                continue;
            },
            395 => {
                txtp.* = @bitCast(if (intra != 0) @as(c_int, antfly_av1_dav1d_txtp_from_uvmode[b.*.unnamed_0.unnamed_0.uv_mode]) else @as(c_int, @bitCast(get_uv_inter_txtp(afz_t_dim_4, txtp.*))));
                afz_pc = 389;
                continue;
            },
            396 => {
                txtp.* = DCT_DCT;
                afz_pc = 389;
                continue;
            },
            397 => {
                _ = @as(c_int, 0);
                afz_pc = 389;
                continue;
            },
            398 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 397) else @as(c_int, 389)));
                continue;
            },
            399 => {
                txtp.* = antfly_av1_dav1d_tx_types_per_set[afz_idx_8 +% @as(c_uint, 0)];
                afz_pc = 398;
                continue;
            },
            400 => {
                afz_idx_8 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.m.txtp_intra2[afz_t_dim_4.*.min][afz_y_mode_nofilt_9])), 4);
                afz_pc = 399;
                continue;
            },
            401 => {
                txtp.* = antfly_av1_dav1d_tx_types_per_set[afz_idx_8 +% @as(c_uint, 5)];
                afz_pc = 398;
                continue;
            },
            402 => {
                afz_idx_8 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.m.txtp_intra1[afz_t_dim_4.*.min][afz_y_mode_nofilt_9])), 6);
                afz_pc = 401;
                continue;
            },
            403 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_2.*.frame_hdr.*.reduced_txtp_set) != 0) or (@as(c_int, afz_t_dim_4.*.min) == TX_16X16)) @as(c_int, 400) else @as(c_int, 402)));
                continue;
            },
            404 => {
                afz_y_mode_nofilt_9 = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_0.y_mode) == FILTER_PRED) @as(c_int, antfly_av1_dav1d_filter_mode_to_y_mode[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_0.y_angle)))]) else @as(c_int, b.*.unnamed_0.unnamed_0.y_mode));
                afz_pc = 403;
                continue;
            },
            405 => {
                _ = @as(c_int, 0);
                afz_pc = 389;
                continue;
            },
            406 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 405) else @as(c_int, 389)));
                continue;
            },
            407 => {
                txtp.* = (afz_idx_8 -% @as(c_uint, 1)) & @as(c_uint, IDTX);
                afz_pc = 406;
                continue;
            },
            408 => {
                afz_idx_8 = antfly_av1_dav1d_msac_decode_bool_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.m.txtp_inter3[afz_t_dim_4.*.min])));
                afz_pc = 407;
                continue;
            },
            409 => {
                txtp.* = antfly_av1_dav1d_tx_types_per_set[afz_idx_8 +% @as(c_uint, 12)];
                afz_pc = 406;
                continue;
            },
            410 => {
                afz_idx_8 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.m.txtp_inter2)), 11);
                afz_pc = 409;
                continue;
            },
            411 => {
                txtp.* = antfly_av1_dav1d_tx_types_per_set[afz_idx_8 +% @as(c_uint, 24)];
                afz_pc = 406;
                continue;
            },
            412 => {
                afz_idx_8 = antfly_av1_dav1d_msac_decode_symbol_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.m.txtp_inter1[afz_t_dim_4.*.min])), 15);
                afz_pc = 411;
                continue;
            },
            413 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, afz_t_dim_4.*.min) == TX_16X16) @as(c_int, 410) else @as(c_int, 412)));
                continue;
            },
            414 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_f_2.*.frame_hdr.*.reduced_txtp_set) != 0) or (@as(c_int, afz_t_dim_4.*.max) == TX_32X32)) @as(c_int, 408) else @as(c_int, 413)));
                continue;
            },
            415 => {
                afz_pc = @bitCast(@as(c_int, if (intra != 0) @as(c_int, 404) else @as(c_int, 414)));
                continue;
            },
            416 => {
                afz_pc = 415;
                continue;
            },
            417 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, afz_f_2.*.frame_hdr.*.segmentation.qidx[b.*.seg_id]) != 0)) @as(c_int, 396) else @as(c_int, 416)));
                continue;
            },
            418 => {
                afz_pc = @bitCast(@as(c_int, if (afz_chroma_1 != 0) @as(c_int, 395) else @as(c_int, 417)));
                continue;
            },
            419 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, afz_t_dim_4.*.max) + intra) >= TX_64X64) @as(c_int, 394) else @as(c_int, 418)));
                continue;
            },
            420 => {
                afz_pc = @bitCast(@as(c_int, if (afz_lossless_3 != 0) @as(c_int, 393) else @as(c_int, 419)));
                continue;
            },
            421 => {
                return -@as(c_int, 1);
            },
            422 => {
                txtp.* = @bitCast(afz_lossless_3 * WHT_WHT);
                afz_pc = 421;
                continue;
            },
            423 => {
                res_ctx.* = 64;
                afz_pc = 422;
                continue;
            },
            424 => {
                afz_pc = @bitCast(@as(c_int, if (afz_all_skip_7 != 0) @as(c_int, 423) else @as(c_int, 420)));
                continue;
            },
            425 => {
                _ = @as(c_int, 0);
                afz_pc = 424;
                continue;
            },
            426 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 425) else @as(c_int, 424)));
                continue;
            },
            427 => {
                afz_all_skip_7 = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_msac_decode_bool_adapt_c(&afz_ts_0.*.msac, @ptrCast(@alignCast(&afz_ts_0.*.cdf.coef.skip[afz_t_dim_4.*.ctx][@bitCast(@as(isize, @intCast(afz_sctx_6)))]))))));
                afz_pc = 426;
                continue;
            },
            428 => {
                afz_sctx_6 = @bitCast(@as(c_uint, @truncate(get_skip_ctx(afz_t_dim_4, bs, a, l, afz_chroma_1, afz_f_2.*.cur.p.layout))));
                afz_pc = 427;
                continue;
            },
            429 => {
                _ = @as(c_int, 0);
                afz_pc = 428;
                continue;
            },
            430 => {
                afz_pc = @bitCast(@as(c_int, if (afz_dbg_5 != 0) @as(c_int, 429) else @as(c_int, 428)));
                continue;
            },
            431 => {
                afz_dbg_5 = @as(c_int, @intFromBool(((((((false and (@as(c_int, afz_f_2.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and (plane != 0)) and false));
                afz_pc = 430;
                continue;
            },
            432 => {
                afz_t_dim_4 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_txfm_dimensions[tx])));
                afz_pc = 431;
                continue;
            },
            433 => {
                afz_lossless_3 = afz_f_2.*.frame_hdr.*.segmentation.lossless[b.*.seg_id];
                afz_pc = 432;
                continue;
            },
            434 => {
                afz_f_2 = @ptrCast(@alignCast(@constCast(t.*.f)));
                afz_pc = 433;
                continue;
            },
            435 => {
                afz_chroma_1 = @as(c_int, @intFromBool(!!(plane != 0)));
                afz_pc = 434;
                continue;
            },
            436 => {
                afz_ts_0 = t.*.ts;
                afz_pc = 435;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub fn read_coef_tree(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block, ytx: enum_RectTxfmSize, depth: c_int, tx_split: [*c]const u16, x_off: c_int, y_off: c_int, arg_dst: [*c]pixel) callconv(.c) void {
    _ = &t;
    _ = &bs;
    _ = &b;
    _ = &ytx;
    _ = &depth;
    _ = &tx_split;
    _ = &x_off;
    _ = &y_off;
    var dst = arg_dst;
    _ = &dst;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[ytx];
    _ = &t_dim;
    const txw: c_int = t_dim.*.w;
    _ = &txw;
    const txh: c_int = t_dim.*.h;
    _ = &txh;
    if (((depth < @as(c_int, 2)) and (@as(c_int, tx_split[@bitCast(@as(isize, @intCast(depth)))]) != 0)) and ((@as(c_int, tx_split[@bitCast(@as(isize, @intCast(depth)))]) & (@as(c_int, 1) << @intCast((y_off * @as(c_int, 4)) + x_off))) != 0)) {
        const sub: enum_RectTxfmSize = t_dim.*.sub;
        _ = &sub;
        const sub_t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[sub];
        _ = &sub_t_dim;
        const txsw: c_int = sub_t_dim.*.w;
        _ = &txsw;
        const txsh: c_int = sub_t_dim.*.h;
        _ = &txsh;
        read_coef_tree(t, bs, b, sub, depth + @as(c_int, 1), tx_split, (x_off * @as(c_int, 2)) + @as(c_int, 0), (y_off * @as(c_int, 2)) + @as(c_int, 0), dst);
        t.*.bx += txsw;
        if ((txw >= txh) and (t.*.bx < f.*.bw)) {
            read_coef_tree(t, bs, b, sub, depth + @as(c_int, 1), tx_split, (x_off * @as(c_int, 2)) + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 0), @ptrCast(@alignCast(if (dst != null) @as(?*anyopaque, @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(@as(c_int, 4) * txsw)))]))) else @as(?*anyopaque, null))));
        }
        t.*.bx -= txsw;
        t.*.by += txsh;
        if ((txh >= txw) and (t.*.by < f.*.bh)) {
            if (dst != null) {
                dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 4) * txsh) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)])))));
            }
            read_coef_tree(t, bs, b, sub, depth + @as(c_int, 1), tx_split, (x_off * @as(c_int, 2)) + @as(c_int, 0), (y_off * @as(c_int, 2)) + @as(c_int, 1), dst);
            t.*.bx += txsw;
            if ((txw >= txh) and (t.*.bx < f.*.bw)) {
                read_coef_tree(t, bs, b, sub, depth + @as(c_int, 1), tx_split, (x_off * @as(c_int, 2)) + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 1), @ptrCast(@alignCast(if (dst != null) @as(?*anyopaque, @ptrCast(@alignCast(&dst[@bitCast(@as(isize, @intCast(@as(c_int, 4) * txsw)))]))) else @as(?*anyopaque, null))));
            }
            t.*.bx -= txsw;
        }
        t.*.by -= txsh;
    } else {
        const bx4: c_int = t.*.bx & @as(c_int, 31);
        _ = &bx4;
        const by4: c_int = t.*.by & @as(c_int, 31);
        _ = &by4;
        var txtp: enum_TxfmType = undefined;
        _ = &txtp;
        var cf_ctx: u8 = undefined;
        _ = &cf_ctx;
        var eob: c_int = undefined;
        _ = &eob;
        var cf: [*c]coef = undefined;
        _ = &cf;
        if (t.*.frame_thread.pass != 0) {
            const p: c_int = t.*.frame_thread.pass & @as(c_int, 1);
            _ = &p;
            while (true) {
                if (!(ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].cf != null)) {
                    unreachable;
                }
                if (!false) break;
            }
            cf = ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].cf;
            ts.*.frame_thread[@bitCast(@as(isize, @intCast(p)))].cf += @as(usize, @bitCast(@as(isize, @intCast((imin(t_dim.*.w, 8) * imin(t_dim.*.h, 8)) * @as(c_int, 16)))));
        } else {
            cf = @ptrCast(@alignCast(&t.*.unnamed_0.cf_16bpc));
        }
        if (t.*.frame_thread.pass != @as(c_int, 2)) {
            eob = decode_coefs(t, &t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(bx4)))], &t.*.l.lcoef[@bitCast(@as(isize, @intCast(by4)))], ytx, bs, b, 0, 0, cf, &txtp, &cf_ctx);
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
            dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(bx4)))])), cf_ctx, imin(txw, f.*.bw - t.*.bx));
            dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(by4)))])), cf_ctx, imin(txh, f.*.bh - t.*.by));
            var txtp_map: [*c]u8 = &t.*.scratch.unnamed_1.unnamed_1.txtp_map[@bitCast(@as(isize, @intCast((by4 * @as(c_int, 32)) + bx4)))];
            _ = &txtp_map;
            blk: {
                switch (@as(c_int, t_dim.*.lw)) {
                    @as(c_int, 0) => {
                        {
                            var y: c_int = 0;
                            _ = &y;
                            while (y < txh) : (y += 1) {
                                @as([*c]union_alias8, @ptrCast(@alignCast(&txtp_map[@as(c_int, 0)]))).*.u8 = @truncate(txtp *% @as(enum_TxfmType, 1));
                                txtp_map += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                            }
                        }
                        break :blk;
                    },
                    @as(c_int, 1) => {
                        {
                            var y: c_int = 0;
                            _ = &y;
                            while (y < txh) : (y += 1) {
                                @as([*c]union_alias16, @ptrCast(@alignCast(&txtp_map[@as(c_int, 0)]))).*.u16 = @truncate(txtp *% @as(enum_TxfmType, 257));
                                txtp_map += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                            }
                        }
                        break :blk;
                    },
                    @as(c_int, 2) => {
                        {
                            var y: c_int = 0;
                            _ = &y;
                            while (y < txh) : (y += 1) {
                                @as([*c]union_alias32, @ptrCast(@alignCast(&txtp_map[@as(c_int, 0)]))).*.u32 = txtp *% @as(c_uint, 16843009);
                                txtp_map += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                            }
                        }
                        break :blk;
                    },
                    @as(c_int, 3) => {
                        {
                            var y: c_int = 0;
                            _ = &y;
                            while (y < txh) : (y += 1) {
                                @as([*c]union_alias64, @ptrCast(@alignCast(&txtp_map[@as(c_int, 0)]))).*.u64 = @as(c_ulonglong, txtp) *% @as(c_ulonglong, 72340172838076673);
                                txtp_map += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                            }
                        }
                        break :blk;
                    },
                    @as(c_int, 4) => {
                        {
                            var y: c_int = 0;
                            _ = &y;
                            while (y < txh) : (y += 1) {
                                while (true) {
                                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&txtp_map[@as(c_int, 0)])), @bitCast(txtp), 16);
                                    if (!false) break;
                                }
                                txtp_map += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
            if (t.*.frame_thread.pass == @as(c_int, 1)) {
                (blk: {
                    const ref = &ts.*.frame_thread[@as(c_int, 1)].cbi;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).* = @truncate((eob * (@as(c_int, 1) << @intCast(@as(c_int, 5)))) + @as(c_int, @bitCast(txtp)));
            }
        } else {
            const cbi: c_int = (blk: {
                const ref = &ts.*.frame_thread[@as(c_int, 0)].cbi;
                const tmp = ref.*;
                ref.* += 1;
                break :blk tmp;
            }).*;
            _ = &cbi;
            eob = cbi >> @intCast(@as(c_int, 5));
            txtp = @bitCast(cbi & @as(c_int, 31));
        }
        if (!((t.*.frame_thread.pass & @as(c_int, 1)) != 0)) {
            while (true) {
                if (!(dst != null)) {
                    unreachable;
                }
                if (!false) break;
            }
            if (eob >= @as(c_int, 0)) {
                if ((((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) {
                    _ = @as(u8, 0);
                }
                dsp.*.itx.itxfm_add[ytx][txtp].?(dst, f.*.cur.stride[@as(c_int, 0)], cf, eob, f.*.bitdepth_max);
                if ((((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) {
                    _ = @as(u8, 0);
                }
            }
        }
    }
}
pub export fn antfly_av1_dav1d_read_coef_blocks_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) void {
    _ = &t;
    _ = &bs;
    _ = &b;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const bx4: c_int = t.*.bx & @as(c_int, 31);
    _ = &bx4;
    const by4: c_int = t.*.by & @as(c_int, 31);
    _ = &by4;
    const cbx4: c_int = bx4 >> @intCast(ss_hor);
    _ = &cbx4;
    const cby4: c_int = by4 >> @intCast(ss_ver);
    _ = &cby4;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bw4: c_int = b_dim[@as(c_int, 0)];
    _ = &bw4;
    const bh4: c_int = b_dim[@as(c_int, 1)];
    _ = &bh4;
    const cbw4: c_int = (bw4 + ss_hor) >> @intCast(ss_hor);
    _ = &cbw4;
    const cbh4: c_int = (bh4 + ss_ver) >> @intCast(ss_ver);
    _ = &cbh4;
    const has_chroma: c_int = @as(c_int, @intFromBool(((f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) and ((bw4 > ss_hor) or ((t.*.bx & @as(c_int, 1)) != 0))) and ((bh4 > ss_ver) or ((t.*.by & @as(c_int, 1)) != 0))));
    _ = &has_chroma;
    if (@as(c_int, b.*.skip) != 0) {
        const a: [*c]BlockContext = t.*.a;
        _ = &a;
        antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 2)]].?(@ptrCast(@alignCast(&a.*.lcoef[@bitCast(@as(isize, @intCast(bx4)))])), 64);
        antfly_av1_dav1d_memset_pow2[b_dim[@as(c_int, 3)]].?(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(by4)))])), 64);
        if (has_chroma != 0) {
            var memset_cw: dav1d_memset_pow2_fn = antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbw4))))))];
            _ = &memset_cw;
            var memset_ch: dav1d_memset_pow2_fn = antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, cbh4))))))];
            _ = &memset_ch;
            memset_cw.?(@ptrCast(@alignCast(&a.*.ccoef[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cbx4)))])), 64);
            memset_cw.?(@ptrCast(@alignCast(&a.*.ccoef[@as(c_int, 1)][@bitCast(@as(isize, @intCast(cbx4)))])), 64);
            memset_ch.?(@ptrCast(@alignCast(&t.*.l.ccoef[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cby4)))])), 64);
            memset_ch.?(@ptrCast(@alignCast(&t.*.l.ccoef[@as(c_int, 1)][@bitCast(@as(isize, @intCast(cby4)))])), 64);
        }
        return;
    }
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const w4: c_int = imin(bw4, f.*.bw - t.*.bx);
    _ = &w4;
    const h4: c_int = imin(bh4, f.*.bh - t.*.by);
    _ = &h4;
    const cw4: c_int = (w4 + ss_hor) >> @intCast(ss_hor);
    _ = &cw4;
    const ch4: c_int = (h4 + ss_ver) >> @intCast(ss_ver);
    _ = &ch4;
    while (true) {
        if (!(t.*.frame_thread.pass == @as(c_int, 1))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!!(@as(c_int, b.*.skip) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    const uv_t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[b.*.uvtx];
    _ = &uv_t_dim;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[@bitCast(@as(isize, @intCast(if (b.*.intra != 0) @as(c_int, b.*.unnamed_0.unnamed_0.tx) else @as(c_int, b.*.unnamed_0.unnamed_1.max_ytx))))];
    _ = &t_dim;
    const tx_split: [2]u16 = [2]u16{
        b.*.unnamed_0.unnamed_1.tx_split0,
        b.*.unnamed_0.unnamed_1.tx_split1,
    };
    _ = &tx_split;
    {
        var init_y: c_int = 0;
        _ = &init_y;
        while (init_y < h4) : (init_y += 16) {
            const sub_h4: c_int = imin(h4, @as(c_int, 16) + init_y);
            _ = &sub_h4;
            {
                var init_x: c_int = 0;
                _ = &init_x;
                while (init_x < w4) : (init_x += 16) {
                    const sub_w4: c_int = imin(w4, init_x + @as(c_int, 16));
                    _ = &sub_w4;
                    var y_off: c_int = @as(c_int, @intFromBool(!!(init_y != 0)));
                    _ = &y_off;
                    var y: c_int = undefined;
                    _ = &y;
                    var x: c_int = undefined;
                    _ = &x;
                    {
                        y = init_y;
                        t.*.by += init_y;
                        while (y < sub_h4) : ({
                            y += t_dim.*.h;
                            t.*.by += t_dim.*.h;
                            y_off += 1;
                        }) {
                            var x_off: c_int = @as(c_int, @intFromBool(!!(init_x != 0)));
                            _ = &x_off;
                            {
                                x = init_x;
                                t.*.bx += init_x;
                                while (x < sub_w4) : ({
                                    x += t_dim.*.w;
                                    t.*.bx += t_dim.*.w;
                                    x_off += 1;
                                }) {
                                    if (!(@as(c_int, b.*.intra) != 0)) {
                                        read_coef_tree(t, bs, b, b.*.unnamed_0.unnamed_1.max_ytx, 0, @ptrCast(@alignCast(&tx_split)), x_off, y_off, null);
                                    } else {
                                        var cf_ctx: u8 = 64;
                                        _ = &cf_ctx;
                                        var txtp: enum_TxfmType = undefined;
                                        _ = &txtp;
                                        const eob: c_int = decode_coefs(t, &t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(bx4 + x)))], &t.*.l.lcoef[@bitCast(@as(isize, @intCast(by4 + y)))], b.*.unnamed_0.unnamed_0.tx, bs, b, 1, 0, ts.*.frame_thread[@as(c_int, 1)].cf, &txtp, &cf_ctx);
                                        _ = &eob;
                                        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                                            _ = @as(c_int, 0);
                                        }
                                        (blk: {
                                            const ref = &ts.*.frame_thread[@as(c_int, 1)].cbi;
                                            const tmp = ref.*;
                                            ref.* += 1;
                                            break :blk tmp;
                                        }).* = @truncate((eob * (@as(c_int, 1) << @intCast(@as(c_int, 5)))) + @as(c_int, @bitCast(txtp)));
                                        ts.*.frame_thread[@as(c_int, 1)].cf += @as(usize, @bitCast(@as(isize, @intCast((imin(t_dim.*.w, 8) * imin(t_dim.*.h, 8)) * @as(c_int, 16)))));
                                        dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(bx4 + x)))])), cf_ctx, imin(t_dim.*.w, f.*.bw - t.*.bx));
                                        dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(by4 + y)))])), cf_ctx, imin(t_dim.*.h, f.*.bh - t.*.by));
                                    }
                                }
                            }
                            t.*.bx -= x;
                        }
                    }
                    t.*.by -= y;
                    if (!(has_chroma != 0)) continue;
                    const sub_ch4: c_int = imin(ch4, (init_y + @as(c_int, 16)) >> @intCast(ss_ver));
                    _ = &sub_ch4;
                    const sub_cw4: c_int = imin(cw4, (init_x + @as(c_int, 16)) >> @intCast(ss_hor));
                    _ = &sub_cw4;
                    {
                        var pl: c_int = 0;
                        _ = &pl;
                        while (pl < @as(c_int, 2)) : (pl += 1) {
                            {
                                y = init_y >> @intCast(ss_ver);
                                t.*.by += init_y;
                                while (y < sub_ch4) : ({
                                    y += uv_t_dim.*.h;
                                    t.*.by += @as(c_int, uv_t_dim.*.h) << @intCast(ss_ver);
                                }) {
                                    {
                                        x = init_x >> @intCast(ss_hor);
                                        t.*.bx += init_x;
                                        while (x < sub_cw4) : ({
                                            x += uv_t_dim.*.w;
                                            t.*.bx += @as(c_int, uv_t_dim.*.w) << @intCast(ss_hor);
                                        }) {
                                            var cf_ctx: u8 = 64;
                                            _ = &cf_ctx;
                                            var txtp: enum_TxfmType = undefined;
                                            _ = &txtp;
                                            if (!(@as(c_int, b.*.intra) != 0)) {
                                                txtp = t.*.scratch.unnamed_1.unnamed_1.txtp_map[@bitCast(@as(isize, @intCast((((by4 + (y << @intCast(ss_ver))) * @as(c_int, 32)) + bx4) + (x << @intCast(ss_hor)))))];
                                            }
                                            const eob: c_int = decode_coefs(t, &t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(cbx4 + x)))], &t.*.l.ccoef[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(cby4 + y)))], b.*.uvtx, bs, b, b.*.intra, @as(c_int, 1) + pl, ts.*.frame_thread[@as(c_int, 1)].cf, &txtp, &cf_ctx);
                                            _ = &eob;
                                            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                                                _ = @as(c_int, 0);
                                            }
                                            (blk: {
                                                const ref = &ts.*.frame_thread[@as(c_int, 1)].cbi;
                                                const tmp = ref.*;
                                                ref.* += 1;
                                                break :blk tmp;
                                            }).* = @truncate((eob * (@as(c_int, 1) << @intCast(@as(c_int, 5)))) + @as(c_int, @bitCast(txtp)));
                                            ts.*.frame_thread[@as(c_int, 1)].cf += @as(usize, @bitCast(@as(isize, @intCast((@as(c_int, uv_t_dim.*.w) * @as(c_int, uv_t_dim.*.h)) * @as(c_int, 16)))));
                                            var ctw: c_int = imin(uv_t_dim.*.w, ((f.*.bw - t.*.bx) + ss_hor) >> @intCast(ss_hor));
                                            _ = &ctw;
                                            var cth: c_int = imin(uv_t_dim.*.h, ((f.*.bh - t.*.by) + ss_ver) >> @intCast(ss_ver));
                                            _ = &cth;
                                            dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(cbx4 + x)))])), cf_ctx, ctw);
                                            dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.ccoef[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(cby4 + y)))])), cf_ctx, cth);
                                        }
                                    }
                                    t.*.bx -= x << @intCast(ss_hor);
                                }
                            }
                            t.*.by -= y << @intCast(ss_ver);
                        }
                    }
                }
            }
        }
    }
}
pub fn mc(t: [*c]Dav1dTaskContext, dst8: [*c]pixel, dst16: [*c]i16, dst_stride: ptrdiff_t, bw4: c_int, bh4: c_int, bx: c_int, by: c_int, pl: c_int, mv_1: mv, refp: [*c]const Dav1dThreadPicture, refidx: c_int, filter_2d: enum_Filter2d) callconv(.c) c_int {
    _ = &t;
    _ = &dst8;
    _ = &dst16;
    _ = &dst_stride;
    _ = &bw4;
    _ = &bh4;
    _ = &bx;
    _ = &by;
    _ = &pl;
    _ = &mv_1;
    _ = &refp;
    _ = &refidx;
    _ = &filter_2d;
    while (true) {
        if (!((@as(c_int, @intFromBool(@as(?*anyopaque, @ptrCast(@alignCast(dst8))) != @as(?*anyopaque, null))) ^ @as(c_int, @intFromBool(@as(?*anyopaque, @ptrCast(@alignCast(dst16))) != @as(?*anyopaque, null)))) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ss_ver: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
    _ = &ss_hor;
    const h_mul: c_int = @as(c_int, 4) >> @intCast(ss_hor);
    _ = &h_mul;
    const v_mul: c_int = @as(c_int, 4) >> @intCast(ss_ver);
    _ = &v_mul;
    const mvx: c_int = mv_1.unnamed_0.x;
    _ = &mvx;
    const mvy: c_int = mv_1.unnamed_0.y;
    _ = &mvy;
    const mx: c_int = mvx & (@as(c_int, 15) >> @intCast(@as(c_int, @intFromBool(!(ss_hor != 0)))));
    _ = &mx;
    const my: c_int = mvy & (@as(c_int, 15) >> @intCast(@as(c_int, @intFromBool(!(ss_ver != 0)))));
    _ = &my;
    var ref_stride: ptrdiff_t = refp.*.p.stride[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))];
    _ = &ref_stride;
    var ref: [*c]const pixel = undefined;
    _ = &ref;
    if ((refp.*.p.p.w == f.*.cur.p.w) and (refp.*.p.p.h == f.*.cur.p.h)) {
        const dx: c_int = (bx * h_mul) + (mvx >> @intCast(@as(c_int, 3) + ss_hor));
        _ = &dx;
        const dy: c_int = (by * v_mul) + (mvy >> @intCast(@as(c_int, 3) + ss_ver));
        _ = &dy;
        var w: c_int = undefined;
        _ = &w;
        var h: c_int = undefined;
        _ = &h;
        if (refp.*.p.data[@as(c_int, 0)] != f.*.cur.data[@as(c_int, 0)]) {
            w = (f.*.cur.p.w + ss_hor) >> @intCast(ss_hor);
            h = (f.*.cur.p.h + ss_ver) >> @intCast(ss_ver);
        } else {
            w = (f.*.bw * @as(c_int, 4)) >> @intCast(ss_hor);
            h = (f.*.bh * @as(c_int, 4)) >> @intCast(ss_ver);
        }
        if ((((dx < (@as(c_int, @intFromBool(!!(mx != 0))) * @as(c_int, 3))) or (dy < (@as(c_int, @intFromBool(!!(my != 0))) * @as(c_int, 3)))) or (((dx + (bw4 * h_mul)) + (@as(c_int, @intFromBool(!!(mx != 0))) * @as(c_int, 4))) > w)) or (((dy + (bh4 * v_mul)) + (@as(c_int, @intFromBool(!!(my != 0))) * @as(c_int, 4))) > h)) {
            const emu_edge_buf: [*c]pixel = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_1.emu_edge_16bpc));
            _ = &emu_edge_buf;
            f.*.dsp.*.mc.emu_edge.?((bw4 * h_mul) + (@as(c_int, @intFromBool(!!(mx != 0))) * @as(c_int, 7)), (bh4 * v_mul) + (@as(c_int, @intFromBool(!!(my != 0))) * @as(c_int, 7)), w, h, dx - (@as(c_int, @intFromBool(!!(mx != 0))) * @as(c_int, 3)), dy - (@as(c_int, @intFromBool(!!(my != 0))) * @as(c_int, 3)), emu_edge_buf, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 192) *% @sizeOf(pixel)))), @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))])), ref_stride);
            ref = &emu_edge_buf[@bitCast(@as(isize, @intCast(((@as(c_int, 192) * @as(c_int, @intFromBool(!!(my != 0)))) * @as(c_int, 3)) + (@as(c_int, @intFromBool(!!(mx != 0))) * @as(c_int, 3)))))];
            ref_stride = @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 192) *% @sizeOf(pixel))));
        } else {
            ref = (@as([*c]pixel, @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(ref_stride) * @as(ptrdiff_t, dy)))))) + @as(usize, @bitCast(@as(isize, @intCast(dx))));
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(dst8))) != @as(?*anyopaque, null)) {
            f.*.dsp.*.mc.mc[filter_2d].?(dst8, dst_stride, ref, ref_stride, bw4 * h_mul, bh4 * v_mul, mx << @intCast(@as(c_int, @intFromBool(!(ss_hor != 0)))), my << @intCast(@as(c_int, @intFromBool(!(ss_ver != 0)))), f.*.bitdepth_max);
        } else {
            f.*.dsp.*.mc.mct[filter_2d].?(dst16, ref, ref_stride, bw4 * h_mul, bh4 * v_mul, mx << @intCast(@as(c_int, @intFromBool(!(ss_hor != 0)))), my << @intCast(@as(c_int, @intFromBool(!(ss_ver != 0)))), f.*.bitdepth_max);
        }
    } else {
        while (true) {
            if (!(refp != (&f.*.sr_cur))) {
                unreachable;
            }
            if (!false) break;
        }
        const orig_pos_y: c_int = ((by * v_mul) << @intCast(@as(c_int, 4))) + (mvy * (@as(c_int, 1) << @intCast(@as(c_int, @intFromBool(!(ss_ver != 0))))));
        _ = &orig_pos_y;
        const orig_pos_x: c_int = ((bx * h_mul) << @intCast(@as(c_int, 4))) + (mvx * (@as(c_int, 1) << @intCast(@as(c_int, @intFromBool(!(ss_hor != 0))))));
        _ = &orig_pos_x;
        var pos_y: c_int = undefined;
        _ = &pos_y;
        var pos_x: c_int = undefined;
        _ = &pos_x;
        while (true) {
            const tmp: i64 = (@as(i64, orig_pos_x) * @as(i64, f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 0)].scale)) + @as(i64, (f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 0)].scale - @as(c_int, 16384)) * @as(c_int, 8));
            _ = &tmp;
            pos_x = apply_sign64(@truncate((antfly_codec_llabs(tmp) + @as(c_longlong, 128)) >> @intCast(@as(c_longlong, 8))), tmp) + @as(c_int, 32);
            if (!false) break;
        }
        while (true) {
            const tmp: i64 = (@as(i64, orig_pos_y) * @as(i64, f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 1)].scale)) + @as(i64, (f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 1)].scale - @as(c_int, 16384)) * @as(c_int, 8));
            _ = &tmp;
            pos_y = apply_sign64(@truncate((antfly_codec_llabs(tmp) + @as(c_longlong, 128)) >> @intCast(@as(c_longlong, 8))), tmp) + @as(c_int, 32);
            if (!false) break;
        }
        const left: c_int = pos_x >> @intCast(@as(c_int, 10));
        _ = &left;
        const top: c_int = pos_y >> @intCast(@as(c_int, 10));
        _ = &top;
        const right: c_int = ((pos_x + (((bw4 * h_mul) - @as(c_int, 1)) * f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 0)].step)) >> @intCast(@as(c_int, 10))) + @as(c_int, 1);
        _ = &right;
        const bottom: c_int = ((pos_y + (((bh4 * v_mul) - @as(c_int, 1)) * f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 1)].step)) >> @intCast(@as(c_int, 10))) + @as(c_int, 1);
        _ = &bottom;
        if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
            _ = @as(c_int, 0);
        }
        const w: c_int = (refp.*.p.p.w + ss_hor) >> @intCast(ss_hor);
        _ = &w;
        const h: c_int = (refp.*.p.p.h + ss_ver) >> @intCast(ss_ver);
        _ = &h;
        if ((((left < @as(c_int, 3)) or (top < @as(c_int, 3))) or ((right + @as(c_int, 4)) > w)) or ((bottom + @as(c_int, 4)) > h)) {
            const emu_edge_buf: [*c]pixel = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_1.emu_edge_16bpc));
            _ = &emu_edge_buf;
            f.*.dsp.*.mc.emu_edge.?((right - left) + @as(c_int, 7), (bottom - top) + @as(c_int, 7), w, h, left - @as(c_int, 3), top - @as(c_int, 3), emu_edge_buf, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 320) *% @sizeOf(pixel)))), @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))])), ref_stride);
            ref = &emu_edge_buf[(@as(c_int, 320) * @as(c_int, 3)) + @as(c_int, 3)];
            ref_stride = @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 320) *% @sizeOf(pixel))));
            if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
                _ = @as(c_int, 0);
            }
        } else {
            ref = (@as([*c]pixel, @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(ref_stride) * @as(ptrdiff_t, top)))))) + @as(usize, @bitCast(@as(isize, @intCast(left))));
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(dst8))) != @as(?*anyopaque, null)) {
            f.*.dsp.*.mc.mc_scaled[filter_2d].?(dst8, dst_stride, ref, ref_stride, bw4 * h_mul, bh4 * v_mul, pos_x & @as(c_int, 1023), pos_y & @as(c_int, 1023), f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 0)].step, f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 1)].step, f.*.bitdepth_max);
        } else {
            f.*.dsp.*.mc.mct_scaled[filter_2d].?(dst16, ref, ref_stride, bw4 * h_mul, bh4 * v_mul, pos_x & @as(c_int, 1023), pos_y & @as(c_int, 1023), f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 0)].step, f.*.svc[@bitCast(@as(isize, @intCast(refidx)))][@as(c_int, 1)].step, f.*.bitdepth_max);
        }
    }
    return 0;
}
pub fn obmc(t: [*c]Dav1dTaskContext, dst: [*c]pixel, dst_stride: ptrdiff_t, b_dim: [*c]const u8, pl: c_int, bx4: c_int, by4: c_int, w4: c_int, h4: c_int) callconv(.c) c_int {
    _ = &t;
    _ = &dst;
    _ = &dst_stride;
    _ = &b_dim;
    _ = &pl;
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
    const lap: [*c]pixel = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_0.lap_16bpc));
    _ = &lap;
    const ss_ver: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
    _ = &ss_hor;
    const h_mul: c_int = @as(c_int, 4) >> @intCast(ss_hor);
    _ = &h_mul;
    const v_mul: c_int = @as(c_int, 4) >> @intCast(ss_ver);
    _ = &v_mul;
    var res: c_int = undefined;
    _ = &res;
    if ((t.*.by > t.*.ts.*.tiling.row_start) and (!(pl != 0) or (((@as(c_int, b_dim[@as(c_int, 0)]) * h_mul) + (@as(c_int, b_dim[@as(c_int, 1)]) * v_mul)) >= @as(c_int, 16)))) {
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
                const step4: c_int = iclip(a_b_dim[@as(c_int, 0)], 2, 16);
                _ = &step4;
                if (@as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) {
                    const ow4: c_int = imin(step4, b_dim[@as(c_int, 0)]);
                    _ = &ow4;
                    const oh4: c_int = imin(b_dim[@as(c_int, 1)], 16) >> @intCast(@as(c_int, 1));
                    _ = &oh4;
                    res = mc(t, lap, null, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, @bitCast(@as(c_long, ow4 * h_mul))) *% @sizeOf(pixel)))), ow4, ((oh4 * @as(c_int, 3)) + @as(c_int, 3)) >> @intCast(@as(c_int, 2)), t.*.bx + x, t.*.by, pl, a_r.*.mv.mv[@as(c_int, 0)], &f.*.refp[@bitCast(@as(isize, @intCast(@as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], @as(c_int, a_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1), antfly_av1_dav1d_filter_2d[t.*.a.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast((bx4 + x) + @as(c_int, 1))))]][t.*.a.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast((bx4 + x) + @as(c_int, 1))))]]);
                    if (res != 0) return res;
                    f.*.dsp.*.mc.blend_h.?(&dst[@bitCast(@as(isize, @intCast(x * h_mul)))], dst_stride, lap, h_mul * ow4, v_mul * oh4);
                    i += 1;
                }
                x += step4;
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
            const step4: c_int = iclip(l_b_dim[@as(c_int, 1)], 2, 16);
            _ = &step4;
            if (@as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) {
                const ow4: c_int = imin(b_dim[@as(c_int, 0)], 16) >> @intCast(@as(c_int, 1));
                _ = &ow4;
                const oh4: c_int = imin(step4, b_dim[@as(c_int, 1)]);
                _ = &oh4;
                res = mc(t, lap, null, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, @bitCast(@as(c_long, h_mul * ow4))) *% @sizeOf(pixel)))), ow4, oh4, t.*.bx, t.*.by + y, pl, l_r.*.mv.mv[@as(c_int, 0)], &f.*.refp[@bitCast(@as(isize, @intCast(@as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], @as(c_int, l_r.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1), antfly_av1_dav1d_filter_2d[t.*.l.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast((by4 + y) + @as(c_int, 1))))]][t.*.l.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast((by4 + y) + @as(c_int, 1))))]]);
                if (res != 0) return res;
                f.*.dsp.*.mc.blend_v.?(&dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y * v_mul) * PXSTRIDE(dst_stride))))], dst_stride, lap, h_mul * ow4, v_mul * oh4);
                i += 1;
            }
            y += step4;
        }
    }
    return 0;
}
pub fn warp_affine(t: [*c]Dav1dTaskContext, arg_dst8: [*c]pixel, arg_dst16: [*c]i16, dstride: ptrdiff_t, b_dim: [*c]const u8, pl: c_int, refp: [*c]const Dav1dThreadPicture, wmp: [*c]const Dav1dWarpedMotionParams) callconv(.c) c_int {
    _ = &t;
    var dst8 = arg_dst8;
    _ = &dst8;
    var dst16 = arg_dst16;
    _ = &dst16;
    _ = &dstride;
    _ = &b_dim;
    _ = &pl;
    _ = &refp;
    _ = &wmp;
    while (true) {
        if (!((@as(c_int, @intFromBool(@as(?*anyopaque, @ptrCast(@alignCast(dst8))) != @as(?*anyopaque, null))) ^ @as(c_int, @intFromBool(@as(?*anyopaque, @ptrCast(@alignCast(dst16))) != @as(?*anyopaque, null)))) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const dsp: [*c]const Dav1dDSPContext = f.*.dsp;
    _ = &dsp;
    const ss_ver: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(!!(pl != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
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
    const width: c_int = (refp.*.p.p.w + ss_hor) >> @intCast(ss_hor);
    _ = &width;
    const height: c_int = (refp.*.p.p.h + ss_ver) >> @intCast(ss_ver);
    _ = &height;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < (@as(c_int, b_dim[@as(c_int, 1)]) * v_mul)) : (y += 8) {
            const src_y: c_int = (t.*.by * @as(c_int, 4)) + ((y + @as(c_int, 4)) << @intCast(ss_ver));
            _ = &src_y;
            const mat3_y: i64 = (@as(i64, mat[@as(c_int, 3)]) * @as(i64, src_y)) + @as(i64, mat[@as(c_int, 0)]);
            _ = &mat3_y;
            const mat5_y: i64 = (@as(i64, mat[@as(c_int, 5)]) * @as(i64, src_y)) + @as(i64, mat[@as(c_int, 1)]);
            _ = &mat5_y;
            {
                var x: c_int = 0;
                _ = &x;
                while (x < (@as(c_int, b_dim[@as(c_int, 0)]) * h_mul)) : (x += 8) {
                    const src_x: c_int = (t.*.bx * @as(c_int, 4)) + ((x + @as(c_int, 4)) << @intCast(ss_hor));
                    _ = &src_x;
                    const mvx: i64 = ((@as(i64, mat[@as(c_int, 2)]) * @as(i64, src_x)) + mat3_y) >> @intCast(@as(i64, ss_hor));
                    _ = &mvx;
                    const mvy: i64 = ((@as(i64, mat[@as(c_int, 4)]) * @as(i64, src_x)) + mat5_y) >> @intCast(@as(i64, ss_ver));
                    _ = &mvy;
                    const dx: c_int = @as(c_int, @truncate(mvx >> @intCast(@as(i64, 16)))) - @as(c_int, 4);
                    _ = &dx;
                    const mx: c_int = (((@as(c_int, @truncate(mvx)) & @as(c_int, 65535)) - (@as(c_int, wmp.*.u.p.alpha) * @as(c_int, 4))) - (@as(c_int, wmp.*.u.p.beta) * @as(c_int, 7))) & ~@as(c_int, 63);
                    _ = &mx;
                    const dy: c_int = @as(c_int, @truncate(mvy >> @intCast(@as(i64, 16)))) - @as(c_int, 4);
                    _ = &dy;
                    const my: c_int = (((@as(c_int, @truncate(mvy)) & @as(c_int, 65535)) - (@as(c_int, wmp.*.u.p.gamma) * @as(c_int, 4))) - (@as(c_int, wmp.*.u.p.delta) * @as(c_int, 4))) & ~@as(c_int, 63);
                    _ = &my;
                    var ref_ptr: [*c]const pixel = undefined;
                    _ = &ref_ptr;
                    var ref_stride: ptrdiff_t = refp.*.p.stride[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))];
                    _ = &ref_stride;
                    if ((((dx < @as(c_int, 3)) or (((dx + @as(c_int, 8)) + @as(c_int, 4)) > width)) or (dy < @as(c_int, 3))) or (((dy + @as(c_int, 8)) + @as(c_int, 4)) > height)) {
                        const emu_edge_buf: [*c]pixel = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_1.emu_edge_16bpc));
                        _ = &emu_edge_buf;
                        f.*.dsp.*.mc.emu_edge.?(15, 15, width, height, dx - @as(c_int, 3), dy - @as(c_int, 3), emu_edge_buf, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 32) *% @sizeOf(pixel)))), @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))])), ref_stride);
                        ref_ptr = &emu_edge_buf[(@as(c_int, 32) * @as(c_int, 3)) + @as(c_int, 3)];
                        ref_stride = @bitCast(@as(c_ulong, @truncate(@as(c_ulong, 32) *% @sizeOf(pixel))));
                    } else {
                        ref_ptr = (@as([*c]pixel, @ptrCast(@alignCast(refp.*.p.data[@bitCast(@as(isize, @intCast(pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(PXSTRIDE(ref_stride) * @as(ptrdiff_t, dy)))))) + @as(usize, @bitCast(@as(isize, @intCast(dx))));
                    }
                    if (@as(?*anyopaque, @ptrCast(@alignCast(dst16))) != @as(?*anyopaque, null)) {
                        dsp.*.mc.warp8x8t.?(&dst16[@bitCast(@as(isize, @intCast(x)))], dstride, ref_ptr, ref_stride, @ptrCast(@alignCast(&wmp.*.u.abcd)), mx, my, f.*.bitdepth_max);
                    } else {
                        dsp.*.mc.warp8x8.?(&dst8[@bitCast(@as(isize, @intCast(x)))], dstride, ref_ptr, ref_stride, @ptrCast(@alignCast(&wmp.*.u.abcd)), mx, my, f.*.bitdepth_max);
                    }
                }
            }
            if (dst8 != null) {
                dst8 += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * PXSTRIDE(dstride)))));
            } else {
                dst16 += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * dstride))));
            }
        }
    }
    return 0;
}
pub export fn antfly_av1_dav1d_recon_b_intra_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, intra_edge_flags: enum_EdgeFlags, b: [*c]const Av1Block) void {
    _ = &t;
    _ = &bs;
    _ = &intra_edge_flags;
    _ = &b;
    var afz_cbi_89: c_int = undefined;
    _ = &afz_cbi_89;
    var afz_p_88: c_int = undefined;
    _ = &afz_p_88;
    var afz_cth_92: c_int = undefined;
    _ = &afz_cth_92;
    var afz_ctw_91: c_int = undefined;
    _ = &afz_ctw_91;
    var afz_cf_ctx_90: u8 = undefined;
    _ = &afz_cf_ctx_90;
    var afz_cf_87: [*c]coef = undefined;
    _ = &afz_cf_87;
    var afz_eob_86: c_int = undefined;
    _ = &afz_eob_86;
    var afz_txtp_85: enum_TxfmType = undefined;
    _ = &afz_txtp_85;
    var afz_m_84: enum_IntraPredMode = undefined;
    _ = &afz_m_84;
    var afz_ystart_83: c_int = undefined;
    _ = &afz_ystart_83;
    var afz_xstart_82: c_int = undefined;
    _ = &afz_xstart_82;
    var afz_xpos_80: c_int = undefined;
    _ = &afz_xpos_80;
    var afz_ypos_81: c_int = undefined;
    _ = &afz_ypos_81;
    var afz_uv_mode_79: enum_IntraPredMode = undefined;
    _ = &afz_uv_mode_79;
    var afz_sby_78: c_int = undefined;
    _ = &afz_sby_78;
    var afz_top_sb_edge_77: [*c]pixel = undefined;
    _ = &afz_top_sb_edge_77;
    var afz_edge_flags_76: enum_EdgeFlags = undefined;
    _ = &afz_edge_flags_76;
    var afz_angle_75: c_int = undefined;
    _ = &afz_angle_75;
    var afz_dst_74: [*c]pixel = undefined;
    _ = &afz_dst_74;
    var afz_pl_73: c_int = undefined;
    _ = &afz_pl_73;
    var afz_sub_cw4_72: c_int = undefined;
    _ = &afz_sub_cw4_72;
    var afz_uv_sb_has_bl_71: c_int = undefined;
    _ = &afz_uv_sb_has_bl_71;
    var afz_uv_sb_has_tr_70: c_int = undefined;
    _ = &afz_uv_sb_has_tr_70;
    var afz_sm_uv_fl_69: c_int = undefined;
    _ = &afz_sm_uv_fl_69;
    var afz_m_64: enum_IntraPredMode = undefined;
    _ = &afz_m_64;
    var afz_ystart_63: c_int = undefined;
    _ = &afz_ystart_63;
    var afz_xstart_62: c_int = undefined;
    _ = &afz_xstart_62;
    var afz_xpos_60: c_int = undefined;
    _ = &afz_xpos_60;
    var afz_ypos_61: c_int = undefined;
    _ = &afz_ypos_61;
    var afz_sby_59: c_int = undefined;
    _ = &afz_sby_59;
    var afz_top_sb_edge_58: [*c]pixel = undefined;
    _ = &afz_top_sb_edge_58;
    var afz_angle_57: c_int = undefined;
    _ = &afz_angle_57;
    var afz_pl_56: c_int = undefined;
    _ = &afz_pl_56;
    var afz_furthest_b_55: c_int = undefined;
    _ = &afz_furthest_b_55;
    var afz_furthest_r_54: c_int = undefined;
    _ = &afz_furthest_r_54;
    var afz_uv_dst_53: [2][*c]pixel = undefined;
    _ = &afz_uv_dst_53;
    var afz_uv_off_52: ptrdiff_t = undefined;
    _ = &afz_uv_off_52;
    var afz_y_src_51: [*c]pixel = undefined;
    _ = &afz_y_src_51;
    var afz_ac_50: [*c]i16 = undefined;
    _ = &afz_ac_50;
    var afz_p_68: c_int = undefined;
    _ = &afz_p_68;
    var afz_pal_idx_67: [*c]u8 = undefined;
    _ = &afz_pal_idx_67;
    var afz_pal_66: [*c][8]pixel = undefined;
    _ = &afz_pal_66;
    var afz_uv_dstoff_65: ptrdiff_t = undefined;
    _ = &afz_uv_dstoff_65;
    var afz_stride_49: ptrdiff_t = undefined;
    _ = &afz_stride_49;
    var afz_cbi_47: c_int = undefined;
    _ = &afz_cbi_47;
    var afz_p_46: c_int = undefined;
    _ = &afz_p_46;
    var afz_cf_ctx_48: u8 = undefined;
    _ = &afz_cf_ctx_48;
    var afz_txtp_45: enum_TxfmType = undefined;
    _ = &afz_txtp_45;
    var afz_eob_44: c_int = undefined;
    _ = &afz_eob_44;
    var afz_cf_43: [*c]coef = undefined;
    _ = &afz_cf_43;
    var afz_m_42: enum_IntraPredMode = undefined;
    _ = &afz_m_42;
    var afz_sby_41: c_int = undefined;
    _ = &afz_sby_41;
    var afz_top_sb_edge_40: [*c]pixel = undefined;
    _ = &afz_top_sb_edge_40;
    var afz_edge_flags_39: enum_EdgeFlags = undefined;
    _ = &afz_edge_flags_39;
    var afz_angle_38: c_int = undefined;
    _ = &afz_angle_38;
    var afz_dst_37: [*c]pixel = undefined;
    _ = &afz_dst_37;
    var afz_sub_w4_36: c_int = undefined;
    _ = &afz_sub_w4_36;
    var afz_y_34: c_int = undefined;
    _ = &afz_y_34;
    var afz_x_35: c_int = undefined;
    _ = &afz_x_35;
    var afz_sb_has_bl_33: c_int = undefined;
    _ = &afz_sb_has_bl_33;
    var afz_sb_has_tr_32: c_int = undefined;
    _ = &afz_sb_has_tr_32;
    var afz_intra_flags_31: c_int = undefined;
    _ = &afz_intra_flags_31;
    var afz_pal_30: [*c]pixel = undefined;
    _ = &afz_pal_30;
    var afz_p_29: c_int = undefined;
    _ = &afz_p_29;
    var afz_pal_idx_28: [*c]u8 = undefined;
    _ = &afz_pal_idx_28;
    var afz_dst_27: [*c]pixel = undefined;
    _ = &afz_dst_27;
    var afz_init_x_26: c_int = undefined;
    _ = &afz_init_x_26;
    var afz_sub_ch4_25: c_int = undefined;
    _ = &afz_sub_ch4_25;
    var afz_sub_h4_24: c_int = undefined;
    _ = &afz_sub_h4_24;
    var afz_init_y_23: c_int = undefined;
    _ = &afz_init_y_23;
    var afz_intra_edge_filter_flag_22: c_int = undefined;
    _ = &afz_intra_edge_filter_flag_22;
    var afz_cbw4_20: c_int = undefined;
    _ = &afz_cbw4_20;
    var afz_cbh4_21: c_int = undefined;
    _ = &afz_cbh4_21;
    var afz_edge_19: [*c]pixel = undefined;
    _ = &afz_edge_19;
    var afz_uv_t_dim_18: [*c]TxfmInfo = undefined;
    _ = &afz_uv_t_dim_18;
    var afz_t_dim_17: [*c]TxfmInfo = undefined;
    _ = &afz_t_dim_17;
    var afz_has_chroma_16: c_int = undefined;
    _ = &afz_has_chroma_16;
    var afz_cw4_14: c_int = undefined;
    _ = &afz_cw4_14;
    var afz_ch4_15: c_int = undefined;
    _ = &afz_ch4_15;
    var afz_w4_12: c_int = undefined;
    _ = &afz_w4_12;
    var afz_h4_13: c_int = undefined;
    _ = &afz_h4_13;
    var afz_bw4_10: c_int = undefined;
    _ = &afz_bw4_10;
    var afz_bh4_11: c_int = undefined;
    _ = &afz_bh4_11;
    var afz_b_dim_9: [*c]u8 = undefined;
    _ = &afz_b_dim_9;
    var afz_cbx4_7: c_int = undefined;
    _ = &afz_cbx4_7;
    var afz_cby4_8: c_int = undefined;
    _ = &afz_cby4_8;
    var afz_ss_hor_6: c_int = undefined;
    _ = &afz_ss_hor_6;
    var afz_ss_ver_5: c_int = undefined;
    _ = &afz_ss_ver_5;
    var afz_bx4_3: c_int = undefined;
    _ = &afz_bx4_3;
    var afz_by4_4: c_int = undefined;
    _ = &afz_by4_4;
    var afz_dsp_2: [*c]Dav1dDSPContext = undefined;
    _ = &afz_dsp_2;
    var afz_f_1: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_1;
    var afz_ts_0: [*c]Dav1dTileState = undefined;
    _ = &afz_ts_0;
    var afz_pc: c_uint = 218;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return;
            },
            1 => {
                afz_pc = @bitCast(@as(c_int, if (afz_init_y_23 < afz_h4_13) @as(c_int, 200) else @as(c_int, 0)));
                continue;
            },
            2 => {
                afz_init_y_23 += 16;
                afz_pc = 1;
                continue;
            },
            3 => {
                afz_pc = @bitCast(@as(c_int, if (afz_init_x_26 < afz_w4_12) @as(c_int, 197) else @as(c_int, 2)));
                continue;
            },
            4 => {
                afz_init_x_26 += 16;
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_73 < @as(c_int, 2)) @as(c_int, 66) else @as(c_int, 4)));
                continue;
            },
            6 => {
                afz_pl_73 += 1;
                afz_pc = 5;
                continue;
            },
            7 => {
                t.*.by -= afz_y_34 << @intCast(afz_ss_ver_5);
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_34 < afz_sub_ch4_25) @as(c_int, 65) else @as(c_int, 7)));
                continue;
            },
            9 => {
                afz_y_34 += afz_uv_t_dim_18.*.h;
                t.*.by += @as(c_int, afz_uv_t_dim_18.*.h) << @intCast(afz_ss_ver_5);
                afz_pc = 8;
                continue;
            },
            10 => {
                t.*.bx -= afz_x_35 << @intCast(afz_ss_hor_6);
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_pc = @bitCast(@as(c_int, if (afz_x_35 < afz_sub_cw4_72) @as(c_int, 63) else @as(c_int, 10)));
                continue;
            },
            12 => {
                afz_x_35 += afz_uv_t_dim_18.*.w;
                t.*.bx += @as(c_int, afz_uv_t_dim_18.*.w) << @intCast(afz_ss_hor_6);
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_dst_74 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, afz_uv_t_dim_18.*.w) * @as(c_int, 4)))));
                afz_pc = 12;
                continue;
            },
            14 => {
                _ = @as(u8, 0);
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 14) else @as(c_int, 13)));
                continue;
            },
            16 => {
                afz_dsp_2.*.itx.itxfm_add[b.*.uvtx][afz_txtp_85].?(afz_dst_74, afz_stride_49, afz_cf_87, afz_eob_86, afz_f_1.*.bitdepth_max);
                afz_pc = 15;
                continue;
            },
            17 => {
                _ = @as(u8, 0);
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 17) else @as(c_int, 16)));
                continue;
            },
            19 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_86 >= @as(c_int, 0)) @as(c_int, 18) else @as(c_int, 13)));
                continue;
            },
            20 => {
                afz_txtp_85 = @bitCast(afz_cbi_89 & @as(c_int, 31));
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_eob_86 = afz_cbi_89 >> @intCast(@as(c_int, 5));
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_88)))].cf += @as(usize, @bitCast(@as(isize, @intCast((@as(c_int, afz_uv_t_dim_18.*.w) * @as(c_int, afz_uv_t_dim_18.*.h)) * @as(c_int, 16)))));
                afz_pc = 21;
                continue;
            },
            23 => {
                afz_cf_87 = afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_88)))].cf;
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_cbi_89 = (blk: {
                    const ref = &afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_88)))].cbi;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).*;
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_p_88 = t.*.frame_thread.pass & @as(c_int, 1);
                afz_pc = 24;
                continue;
            },
            26 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cby4_8 + afz_y_34)))])), afz_cf_ctx_90, afz_cth_92);
                afz_pc = 19;
                continue;
            },
            27 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cbx4_7 + afz_x_35)))])), afz_cf_ctx_90, afz_ctw_91);
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_cth_92 = imin(afz_uv_t_dim_18.*.h, ((afz_f_1.*.bh - t.*.by) + afz_ss_ver_5) >> @intCast(afz_ss_ver_5));
                afz_pc = 27;
                continue;
            },
            29 => {
                afz_ctw_91 = imin(afz_uv_t_dim_18.*.w, ((afz_f_1.*.bw - t.*.bx) + afz_ss_hor_6) >> @intCast(afz_ss_hor_6));
                afz_pc = 28;
                continue;
            },
            30 => {
                _ = @as(c_int, 0);
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_pc = @bitCast(@as(c_int, if (((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) @as(c_int, 30) else @as(c_int, 29)));
                continue;
            },
            32 => {
                afz_eob_86 = decode_coefs(t, &t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cbx4_7 + afz_x_35)))], &t.*.l.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cby4_8 + afz_y_34)))], b.*.uvtx, bs, b, 1, @as(c_int, 1) + afz_pl_73, afz_cf_87, &afz_txtp_85, &afz_cf_ctx_90);
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_cf_87 = @ptrCast(@alignCast(&t.*.unnamed_0.cf_16bpc));
                afz_pc = 32;
                continue;
            },
            34 => {
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if (t.*.frame_thread.pass != 0) @as(c_int, 25) else @as(c_int, 34)));
                continue;
            },
            36 => {
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_pc = 36;
                continue;
            },
            38 => {
                afz_pc = 37;
                continue;
            },
            39 => {
                antfly_av1_dav1d_memset_pow2[afz_uv_t_dim_18.*.lh].?(@ptrCast(@alignCast(&t.*.l.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cby4_8 + afz_y_34)))])), 64);
                afz_pc = 13;
                continue;
            },
            40 => {
                antfly_av1_dav1d_memset_pow2[afz_uv_t_dim_18.*.lw].?(@ptrCast(@alignCast(&t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(afz_pl_73)))][@bitCast(@as(isize, @intCast(afz_cbx4_7 + afz_x_35)))])), 64);
                afz_pc = 39;
                continue;
            },
            41 => {
                afz_pc = @bitCast(@as(c_int, if (!(t.*.frame_thread.pass != 0)) @as(c_int, 40) else @as(c_int, 13)));
                continue;
            },
            42 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, b.*.skip) != 0)) @as(c_int, 38) else @as(c_int, 41)));
                continue;
            },
            43 => {
                _ = @as(u8, 0);
                afz_pc = 42;
                continue;
            },
            44 => {
                _ = @as(u8, 0);
                afz_pc = 43;
                continue;
            },
            45 => {
                _ = @as(u8, 0);
                afz_pc = 44;
                continue;
            },
            46 => {
                _ = @as(u8, 0);
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 46) else @as(c_int, 42)));
                continue;
            },
            48 => {
                afz_dsp_2.*.ipred.intra_pred[afz_m_84].?(afz_dst_74, afz_stride_49, afz_edge_19, @as(c_int, afz_uv_t_dim_18.*.w) * @as(c_int, 4), @as(c_int, afz_uv_t_dim_18.*.h) * @as(c_int, 4), afz_angle_75 | afz_sm_uv_fl_69, (((@as(c_int, 4) * afz_f_1.*.bw) + afz_ss_hor_6) - (@as(c_int, 4) * (t.*.bx & ~afz_ss_hor_6))) >> @intCast(afz_ss_hor_6), (((@as(c_int, 4) * afz_f_1.*.bh) + afz_ss_ver_5) - (@as(c_int, 4) * (t.*.by & ~afz_ss_ver_5))) >> @intCast(afz_ss_ver_5), afz_f_1.*.bitdepth_max);
                afz_pc = 47;
                continue;
            },
            49 => {
                afz_angle_75 |= afz_intra_edge_filter_flag_22;
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_m_84 = antfly_av1_dav1d_prepare_intra_edges_16bpc(afz_xpos_80, @as(c_int, @intFromBool(afz_xpos_80 > afz_xstart_82)), afz_ypos_81, @as(c_int, @intFromBool(afz_ypos_81 > afz_ystart_83)), afz_ts_0.*.tiling.col_end >> @intCast(afz_ss_hor_6), afz_ts_0.*.tiling.row_end >> @intCast(afz_ss_ver_5), afz_edge_flags_76, afz_dst_74, afz_stride_49, afz_top_sb_edge_77, afz_uv_mode_79, &afz_angle_75, afz_uv_t_dim_18.*.w, afz_uv_t_dim_18.*.h, afz_f_1.*.seq_hdr.*.intra_edge_filter, afz_edge_19, afz_f_1.*.bitdepth_max);
                afz_pc = 49;
                continue;
            },
            51 => {
                afz_ystart_83 = afz_ts_0.*.tiling.row_start >> @intCast(afz_ss_ver_5);
                afz_pc = 50;
                continue;
            },
            52 => {
                afz_xstart_82 = afz_ts_0.*.tiling.col_start >> @intCast(afz_ss_hor_6);
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_xpos_80 = t.*.bx >> @intCast(afz_ss_hor_6);
                afz_ypos_81 = t.*.by >> @intCast(afz_ss_ver_5);
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_uv_mode_79 = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) == CFL_PRED) DC_PRED else @as(c_int, b.*.unnamed_0.unnamed_0.uv_mode));
                afz_pc = 53;
                continue;
            },
            55 => {
                afz_top_sb_edge_77 += @as(usize, @bitCast(@as(isize, @intCast((afz_f_1.*.sb128w * @as(c_int, 128)) * (afz_sby_78 - @as(c_int, 1))))));
                afz_pc = 54;
                continue;
            },
            56 => {
                afz_sby_78 = t.*.by >> @intCast(afz_f_1.*.sb_shift);
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_top_sb_edge_77 = afz_f_1.*.ipred_edge[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_73)))];
                afz_pc = 56;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if (!(((t.*.by & ~afz_ss_ver_5) & (afz_f_1.*.sb_step - @as(c_int, 1))) != 0)) @as(c_int, 57) else @as(c_int, 54)));
                continue;
            },
            59 => {
                afz_top_sb_edge_77 = null;
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_edge_flags_76 = @bitCast((if (((afz_y_34 > (afz_init_y_23 >> @intCast(afz_ss_ver_5))) or !(afz_uv_sb_has_tr_70 != 0)) and ((afz_x_35 + @as(c_int, afz_uv_t_dim_18.*.w)) >= afz_sub_cw4_72)) @as(c_int, 0) else EDGE_I444_TOP_HAS_RIGHT) | (if ((afz_x_35 > (afz_init_x_26 >> @intCast(afz_ss_hor_6))) or (!(afz_uv_sb_has_bl_71 != 0) and ((afz_y_34 + @as(c_int, afz_uv_t_dim_18.*.h)) >= afz_sub_ch4_25))) @as(c_int, 0) else EDGE_I444_LEFT_HAS_BOTTOM));
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_angle_75 = b.*.unnamed_0.unnamed_0.uv_angle;
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_pc = 42;
                continue;
            },
            63 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) == CFL_PRED) and (@as(c_int, b.*.unnamed_0.unnamed_0.cfl_alpha[@bitCast(@as(isize, @intCast(afz_pl_73)))]) != 0)) or (@as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)]) != 0)) @as(c_int, 62) else @as(c_int, 61)));
                continue;
            },
            64 => {
                afz_x_35 = afz_init_x_26 >> @intCast(afz_ss_hor_6);
                t.*.bx += afz_init_x_26;
                afz_pc = 11;
                continue;
            },
            65 => {
                afz_dst_74 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_73)))]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * ((@as(ptrdiff_t, t.*.by >> @intCast(afz_ss_ver_5)) * PXSTRIDE(afz_stride_49)) + @as(ptrdiff_t, (t.*.bx + afz_init_x_26) >> @intCast(afz_ss_hor_6)))))));
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_y_34 = afz_init_y_23 >> @intCast(afz_ss_ver_5);
                t.*.by += afz_init_y_23;
                afz_pc = 8;
                continue;
            },
            67 => {
                afz_pl_73 = 0;
                afz_pc = 5;
                continue;
            },
            68 => {
                afz_sub_cw4_72 = imin(afz_cw4_14, (afz_init_x_26 + @as(c_int, 16)) >> @intCast(afz_ss_hor_6));
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_uv_sb_has_bl_71 = if (afz_init_x_26 != 0) @as(c_int, 0) else if (((afz_init_y_23 + @as(c_int, 16)) >> @intCast(afz_ss_ver_5)) < afz_ch4_15) @as(c_int, 1) else @as(c_int, @bitCast(intra_edge_flags & @as(enum_EdgeFlags, @bitCast(EDGE_I420_LEFT_HAS_BOTTOM >> @intCast(@as(c_int, @bitCast(afz_f_1.*.cur.p.layout -% @as(enum_Dav1dPixelLayout, 1))))))));
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_uv_sb_has_tr_70 = if (((afz_init_x_26 + @as(c_int, 16)) >> @intCast(afz_ss_hor_6)) < afz_cw4_14) @as(c_int, 1) else if (afz_init_y_23 != 0) @as(c_int, 0) else @as(c_int, @bitCast(intra_edge_flags & @as(enum_EdgeFlags, @bitCast(EDGE_I420_TOP_HAS_RIGHT >> @intCast(@as(c_int, @bitCast(afz_f_1.*.cur.p.layout -% @as(enum_Dav1dPixelLayout, 1))))))));
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_sm_uv_fl_69 = sm_uv_flag(t.*.a, afz_cbx4_7) | sm_uv_flag(&t.*.l, afz_cby4_8);
                afz_pc = 70;
                continue;
            },
            72 => {
                _ = @as(u8, 0);
                afz_pc = 71;
                continue;
            },
            73 => {
                _ = @as(u8, 0);
                afz_pc = 72;
                continue;
            },
            74 => {
                ac_dump(afz_ac_50, @as(c_int, 4) * afz_cbw4_20, @as(c_int, 4) * afz_cbh4_21, "ac");
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 74) else @as(c_int, 71)));
                continue;
            },
            76 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_56 < @as(c_int, 2)) @as(c_int, 90) else @as(c_int, 75)));
                continue;
            },
            77 => {
                afz_pl_56 += 1;
                afz_pc = 76;
                continue;
            },
            78 => {
                afz_dsp_2.*.ipred.cfl_pred[afz_m_64].?(afz_uv_dst_53[@bitCast(@as(isize, @intCast(afz_pl_56)))], afz_stride_49, afz_edge_19, @as(c_int, afz_uv_t_dim_18.*.w) * @as(c_int, 4), @as(c_int, afz_uv_t_dim_18.*.h) * @as(c_int, 4), afz_ac_50, b.*.unnamed_0.unnamed_0.cfl_alpha[@bitCast(@as(isize, @intCast(afz_pl_56)))], afz_f_1.*.bitdepth_max);
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_m_64 = antfly_av1_dav1d_prepare_intra_edges_16bpc(afz_xpos_60, @as(c_int, @intFromBool(afz_xpos_60 > afz_xstart_62)), afz_ypos_61, @as(c_int, @intFromBool(afz_ypos_61 > afz_ystart_63)), afz_ts_0.*.tiling.col_end >> @intCast(afz_ss_hor_6), afz_ts_0.*.tiling.row_end >> @intCast(afz_ss_ver_5), 0, afz_uv_dst_53[@bitCast(@as(isize, @intCast(afz_pl_56)))], afz_stride_49, afz_top_sb_edge_58, DC_PRED, &afz_angle_57, afz_uv_t_dim_18.*.w, afz_uv_t_dim_18.*.h, 0, afz_edge_19, afz_f_1.*.bitdepth_max);
                afz_pc = 78;
                continue;
            },
            80 => {
                afz_ystart_63 = afz_ts_0.*.tiling.row_start >> @intCast(afz_ss_ver_5);
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_xstart_62 = afz_ts_0.*.tiling.col_start >> @intCast(afz_ss_hor_6);
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_xpos_60 = t.*.bx >> @intCast(afz_ss_hor_6);
                afz_ypos_61 = t.*.by >> @intCast(afz_ss_ver_5);
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_top_sb_edge_58 += @as(usize, @bitCast(@as(isize, @intCast((afz_f_1.*.sb128w * @as(c_int, 128)) * (afz_sby_59 - @as(c_int, 1))))));
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_sby_59 = t.*.by >> @intCast(afz_f_1.*.sb_shift);
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_top_sb_edge_58 = afz_f_1.*.ipred_edge[@bitCast(@as(isize, @intCast(afz_pl_56 + @as(c_int, 1))))];
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_pc = @bitCast(@as(c_int, if (!(((t.*.by & ~afz_ss_ver_5) & (afz_f_1.*.sb_step - @as(c_int, 1))) != 0)) @as(c_int, 85) else @as(c_int, 82)));
                continue;
            },
            87 => {
                afz_top_sb_edge_58 = null;
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_angle_57 = 0;
                afz_pc = 87;
                continue;
            },
            89 => {
                afz_pc = 77;
                continue;
            },
            90 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, b.*.unnamed_0.unnamed_0.cfl_alpha[@bitCast(@as(isize, @intCast(afz_pl_56)))]) != 0)) @as(c_int, 89) else @as(c_int, 88)));
                continue;
            },
            91 => {
                afz_pl_56 = 0;
                afz_pc = 76;
                continue;
            },
            92 => {
                afz_dsp_2.*.ipred.cfl_ac[afz_f_1.*.cur.p.layout -% @as(enum_Dav1dPixelLayout, 1)].?(afz_ac_50, afz_y_src_51, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_cbw4_20 - (afz_furthest_r_54 >> @intCast(afz_ss_hor_6)), afz_cbh4_21 - (afz_furthest_b_55 >> @intCast(afz_ss_ver_5)), afz_cbw4_20 * @as(c_int, 4), afz_cbh4_21 * @as(c_int, 4));
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_furthest_b_55 = (((afz_ch4_15 << @intCast(afz_ss_ver_5)) + @as(c_int, afz_t_dim_17.*.h)) - @as(c_int, 1)) & ~(@as(c_int, afz_t_dim_17.*.h) - @as(c_int, 1));
                afz_pc = 92;
                continue;
            },
            94 => {
                afz_furthest_r_54 = (((afz_cw4_14 << @intCast(afz_ss_hor_6)) + @as(c_int, afz_t_dim_17.*.w)) - @as(c_int, 1)) & ~(@as(c_int, afz_t_dim_17.*.w) - @as(c_int, 1));
                afz_pc = 93;
                continue;
            },
            95 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][*c]pixel, @ptrCast(@alignCast(&afz_uv_dst_53))))), @ptrCast(@alignCast(@as([*c][*c]pixel, @ptrCast(@alignCast(@constCast(&@as([2][*c]pixel, [2][*c]pixel{
                    @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 1)]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_off_52)))),
                    @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 2)]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_off_52)))),
                }))))))), @sizeOf(@TypeOf(afz_uv_dst_53)));
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_uv_off_52 = @as(ptrdiff_t, 4) * (@as(ptrdiff_t, t.*.bx >> @intCast(afz_ss_hor_6)) + (@as(ptrdiff_t, t.*.by >> @intCast(afz_ss_ver_5)) * PXSTRIDE(afz_stride_49)));
                afz_pc = 95;
                continue;
            },
            97 => {
                afz_y_src_51 = (@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * (t.*.bx & ~afz_ss_hor_6)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 4) * (t.*.by & ~afz_ss_ver_5)) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)])))));
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_ac_50 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_1.ac));
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 101) else @as(c_int, 98);
                continue;
            },
            100 => {
                unreachable;
            },
            101 => {
                afz_pc = @bitCast(@as(c_int, if (!(!(afz_init_x_26 != 0) and !(afz_init_y_23 != 0))) @as(c_int, 100) else @as(c_int, 99)));
                continue;
            },
            102 => {
                _ = @as(u8, 0);
                afz_pc = 71;
                continue;
            },
            103 => {
                _ = @as(u8, 0);
                afz_pc = 102;
                continue;
            },
            104 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 103) else @as(c_int, 71)));
                continue;
            },
            105 => {
                afz_f_1.*.dsp.*.ipred.pal_pred.?(@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 2)]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_dstoff_65)))), afz_f_1.*.cur.stride[@as(c_int, 1)], @ptrCast(@alignCast(&afz_pal_66[@as(c_int, 2)])), afz_pal_idx_67, afz_cbw4_20 * @as(c_int, 4), afz_cbh4_21 * @as(c_int, 4));
                afz_pc = 104;
                continue;
            },
            106 => {
                afz_f_1.*.dsp.*.ipred.pal_pred.?(@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 1)]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uv_dstoff_65)))), afz_f_1.*.cur.stride[@as(c_int, 1)], @ptrCast(@alignCast(&afz_pal_66[@as(c_int, 1)])), afz_pal_idx_67, afz_cbw4_20 * @as(c_int, 4), afz_cbh4_21 * @as(c_int, 4));
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_68)))].pal_idx += @as(usize, @bitCast(@as(isize, @intCast((afz_cbw4_20 * afz_cbh4_21) * @as(c_int, 8)))));
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_pal_idx_67 = afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_68)))].pal_idx;
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_pal_66 = @ptrCast(@alignCast(&afz_f_1.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (afz_f_1.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))]));
                afz_pc = 108;
                continue;
            },
            110 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 112) else @as(c_int, 109);
                continue;
            },
            111 => {
                unreachable;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_68)))].pal_idx != null)) @as(c_int, 111) else @as(c_int, 110)));
                continue;
            },
            113 => {
                afz_p_68 = t.*.frame_thread.pass & @as(c_int, 1);
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_pal_idx_67 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.pal_idx_uv));
                afz_pc = 106;
                continue;
            },
            115 => {
                afz_pal_66 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc));
                afz_pc = 114;
                continue;
            },
            116 => {
                afz_pc = @bitCast(@as(c_int, if (t.*.frame_thread.pass != 0) @as(c_int, 113) else @as(c_int, 115)));
                continue;
            },
            117 => {
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_uv_dstoff_65 = @as(ptrdiff_t, 4) * (@as(ptrdiff_t, t.*.bx >> @intCast(afz_ss_hor_6)) + (@as(ptrdiff_t, t.*.by >> @intCast(afz_ss_ver_5)) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)])));
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)] != 0) @as(c_int, 119) else @as(c_int, 71)));
                continue;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, b.*.unnamed_0.unnamed_0.uv_mode) == CFL_PRED) @as(c_int, 101) else @as(c_int, 120)));
                continue;
            },
            122 => {
                afz_stride_49 = afz_f_1.*.cur.stride[@as(c_int, 1)];
                afz_pc = 121;
                continue;
            },
            123 => {
                afz_pc = 4;
                continue;
            },
            124 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_has_chroma_16 != 0)) @as(c_int, 123) else @as(c_int, 122)));
                continue;
            },
            125 => {
                t.*.by -= afz_y_34;
                afz_pc = 124;
                continue;
            },
            126 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_34 < afz_sub_h4_24) @as(c_int, 176) else @as(c_int, 125)));
                continue;
            },
            127 => {
                afz_y_34 += afz_t_dim_17.*.h;
                t.*.by += afz_t_dim_17.*.h;
                afz_pc = 126;
                continue;
            },
            128 => {
                t.*.bx -= afz_x_35;
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_pc = @bitCast(@as(c_int, if (afz_x_35 < afz_sub_w4_36) @as(c_int, 174) else @as(c_int, 128)));
                continue;
            },
            130 => {
                afz_x_35 += afz_t_dim_17.*.w;
                t.*.bx += afz_t_dim_17.*.w;
                afz_pc = 129;
                continue;
            },
            131 => {
                afz_dst_37 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * @as(c_int, afz_t_dim_17.*.w)))));
                afz_pc = 130;
                continue;
            },
            132 => {
                _ = @as(u8, 0);
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 132) else @as(c_int, 131)));
                continue;
            },
            134 => {
                afz_dsp_2.*.itx.itxfm_add[b.*.unnamed_0.unnamed_0.tx][afz_txtp_45].?(afz_dst_37, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_cf_43, afz_eob_44, afz_f_1.*.bitdepth_max);
                afz_pc = 133;
                continue;
            },
            135 => {
                _ = @as(u8, 0);
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 135) else @as(c_int, 134)));
                continue;
            },
            137 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_44 >= @as(c_int, 0)) @as(c_int, 136) else @as(c_int, 131)));
                continue;
            },
            138 => {
                afz_txtp_45 = @bitCast(afz_cbi_47 & @as(c_int, 31));
                afz_pc = 137;
                continue;
            },
            139 => {
                afz_eob_44 = afz_cbi_47 >> @intCast(@as(c_int, 5));
                afz_pc = 138;
                continue;
            },
            140 => {
                afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_46)))].cf += @as(usize, @bitCast(@as(isize, @intCast((imin(afz_t_dim_17.*.w, 8) * imin(afz_t_dim_17.*.h, 8)) * @as(c_int, 16)))));
                afz_pc = 139;
                continue;
            },
            141 => {
                afz_cf_43 = afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_46)))].cf;
                afz_pc = 140;
                continue;
            },
            142 => {
                afz_cbi_47 = (blk: {
                    const ref = &afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_46)))].cbi;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).*;
                afz_pc = 141;
                continue;
            },
            143 => {
                afz_p_46 = t.*.frame_thread.pass & @as(c_int, 1);
                afz_pc = 142;
                continue;
            },
            144 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(afz_by4_4 + afz_y_34)))])), afz_cf_ctx_48, imin(afz_t_dim_17.*.h, afz_f_1.*.bh - t.*.by));
                afz_pc = 137;
                continue;
            },
            145 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(afz_bx4_3 + afz_x_35)))])), afz_cf_ctx_48, imin(afz_t_dim_17.*.w, afz_f_1.*.bw - t.*.bx));
                afz_pc = 144;
                continue;
            },
            146 => {
                _ = @as(c_int, 0);
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_pc = @bitCast(@as(c_int, if (((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) @as(c_int, 146) else @as(c_int, 145)));
                continue;
            },
            148 => {
                afz_eob_44 = decode_coefs(t, &t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(afz_bx4_3 + afz_x_35)))], &t.*.l.lcoef[@bitCast(@as(isize, @intCast(afz_by4_4 + afz_y_34)))], b.*.unnamed_0.unnamed_0.tx, bs, b, 1, 0, afz_cf_43, &afz_txtp_45, &afz_cf_ctx_48);
                afz_pc = 147;
                continue;
            },
            149 => {
                afz_cf_43 = @ptrCast(@alignCast(&t.*.unnamed_0.cf_16bpc));
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_pc = 149;
                continue;
            },
            151 => {
                afz_pc = @bitCast(@as(c_int, if (t.*.frame_thread.pass != 0) @as(c_int, 143) else @as(c_int, 150)));
                continue;
            },
            152 => {
                afz_pc = 151;
                continue;
            },
            153 => {
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_pc = 153;
                continue;
            },
            155 => {
                antfly_av1_dav1d_memset_pow2[afz_t_dim_17.*.lh].?(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(afz_by4_4 + afz_y_34)))])), 64);
                afz_pc = 131;
                continue;
            },
            156 => {
                antfly_av1_dav1d_memset_pow2[afz_t_dim_17.*.lw].?(@ptrCast(@alignCast(&t.*.a.*.lcoef[@bitCast(@as(isize, @intCast(afz_bx4_3 + afz_x_35)))])), 64);
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_pc = @bitCast(@as(c_int, if (!(t.*.frame_thread.pass != 0)) @as(c_int, 156) else @as(c_int, 131)));
                continue;
            },
            158 => {
                afz_pc = @bitCast(@as(c_int, if (!(@as(c_int, b.*.skip) != 0)) @as(c_int, 154) else @as(c_int, 157)));
                continue;
            },
            159 => {
                _ = @as(u8, 0);
                afz_pc = 158;
                continue;
            },
            160 => {
                _ = @as(u8, 0);
                afz_pc = 159;
                continue;
            },
            161 => {
                _ = @as(u8, 0);
                afz_pc = 160;
                continue;
            },
            162 => {
                _ = @as(u8, 0);
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 162) else @as(c_int, 158)));
                continue;
            },
            164 => {
                afz_dsp_2.*.ipred.intra_pred[afz_m_42].?(afz_dst_37, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_edge_19, @as(c_int, afz_t_dim_17.*.w) * @as(c_int, 4), @as(c_int, afz_t_dim_17.*.h) * @as(c_int, 4), afz_angle_38 | afz_intra_flags_31, (@as(c_int, 4) * afz_f_1.*.bw) - (@as(c_int, 4) * t.*.bx), (@as(c_int, 4) * afz_f_1.*.bh) - (@as(c_int, 4) * t.*.by), afz_f_1.*.bitdepth_max);
                afz_pc = 163;
                continue;
            },
            165 => {
                afz_m_42 = antfly_av1_dav1d_prepare_intra_edges_16bpc(t.*.bx, @as(c_int, @intFromBool(t.*.bx > afz_ts_0.*.tiling.col_start)), t.*.by, @as(c_int, @intFromBool(t.*.by > afz_ts_0.*.tiling.row_start)), afz_ts_0.*.tiling.col_end, afz_ts_0.*.tiling.row_end, afz_edge_flags_39, afz_dst_37, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_top_sb_edge_40, b.*.unnamed_0.unnamed_0.y_mode, &afz_angle_38, afz_t_dim_17.*.w, afz_t_dim_17.*.h, afz_f_1.*.seq_hdr.*.intra_edge_filter, afz_edge_19, afz_f_1.*.bitdepth_max);
                afz_pc = 164;
                continue;
            },
            166 => {
                afz_top_sb_edge_40 += @as(usize, @bitCast(@as(isize, @intCast((afz_f_1.*.sb128w * @as(c_int, 128)) * (afz_sby_41 - @as(c_int, 1))))));
                afz_pc = 165;
                continue;
            },
            167 => {
                afz_sby_41 = t.*.by >> @intCast(afz_f_1.*.sb_shift);
                afz_pc = 166;
                continue;
            },
            168 => {
                afz_top_sb_edge_40 = afz_f_1.*.ipred_edge[@as(c_int, 0)];
                afz_pc = 167;
                continue;
            },
            169 => {
                afz_pc = @bitCast(@as(c_int, if (!((t.*.by & (afz_f_1.*.sb_step - @as(c_int, 1))) != 0)) @as(c_int, 168) else @as(c_int, 165)));
                continue;
            },
            170 => {
                afz_top_sb_edge_40 = null;
                afz_pc = 169;
                continue;
            },
            171 => {
                afz_edge_flags_39 = @bitCast((if (((afz_y_34 > afz_init_y_23) or !(afz_sb_has_tr_32 != 0)) and ((afz_x_35 + @as(c_int, afz_t_dim_17.*.w)) >= afz_sub_w4_36)) @as(c_int, 0) else EDGE_I444_TOP_HAS_RIGHT) | (if ((afz_x_35 > afz_init_x_26) or (!(afz_sb_has_bl_33 != 0) and ((afz_y_34 + @as(c_int, afz_t_dim_17.*.h)) >= afz_sub_h4_24))) @as(c_int, 0) else EDGE_I444_LEFT_HAS_BOTTOM));
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_angle_38 = b.*.unnamed_0.unnamed_0.y_angle;
                afz_pc = 171;
                continue;
            },
            173 => {
                afz_pc = 158;
                continue;
            },
            174 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)] != 0) @as(c_int, 173) else @as(c_int, 172)));
                continue;
            },
            175 => {
                afz_x_35 = afz_init_x_26;
                t.*.bx += afz_init_x_26;
                afz_pc = 129;
                continue;
            },
            176 => {
                afz_dst_37 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * (((@as(ptrdiff_t, t.*.by) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)])) + @as(ptrdiff_t, t.*.bx)) + @as(ptrdiff_t, afz_init_x_26))))));
                afz_pc = 175;
                continue;
            },
            177 => {
                afz_y_34 = afz_init_y_23;
                t.*.by += afz_init_y_23;
                afz_pc = 126;
                continue;
            },
            178 => {
                afz_sub_w4_36 = imin(afz_w4_12, afz_init_x_26 + @as(c_int, 16));
                afz_pc = 177;
                continue;
            },
            179 => {
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_sb_has_bl_33 = if (afz_init_x_26 != 0) @as(c_int, 0) else if ((afz_init_y_23 + @as(c_int, 16)) < afz_h4_13) @as(c_int, 1) else @as(c_int, @bitCast(intra_edge_flags & @as(enum_EdgeFlags, EDGE_I444_LEFT_HAS_BOTTOM)));
                afz_pc = 179;
                continue;
            },
            181 => {
                afz_sb_has_tr_32 = if ((afz_init_x_26 + @as(c_int, 16)) < afz_w4_12) @as(c_int, 1) else if (afz_init_y_23 != 0) @as(c_int, 0) else @as(c_int, @bitCast(intra_edge_flags & @as(enum_EdgeFlags, EDGE_I444_TOP_HAS_RIGHT)));
                afz_pc = 180;
                continue;
            },
            182 => {
                afz_intra_flags_31 = (sm_flag(t.*.a, afz_bx4_3) | sm_flag(&t.*.l, afz_by4_4)) | afz_intra_edge_filter_flag_22;
                afz_pc = 181;
                continue;
            },
            183 => {
                _ = @as(u8, 0);
                afz_pc = 182;
                continue;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 183) else @as(c_int, 182)));
                continue;
            },
            185 => {
                afz_f_1.*.dsp.*.ipred.pal_pred.?(afz_dst_27, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_pal_30, afz_pal_idx_28, afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4));
                afz_pc = 184;
                continue;
            },
            186 => {
                afz_pal_30 = if (t.*.frame_thread.pass != 0) @as([*c]pixel, @ptrCast(@alignCast(&afz_f_1.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (afz_f_1.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))][@as(c_int, 0)]))) else @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc[@as(c_int, 0)])));
                afz_pc = 185;
                continue;
            },
            187 => {
                afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_29)))].pal_idx += @as(usize, @bitCast(@as(isize, @intCast((afz_bw4_10 * afz_bh4_11) * @as(c_int, 8)))));
                afz_pc = 186;
                continue;
            },
            188 => {
                afz_pal_idx_28 = afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_29)))].pal_idx;
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 191) else @as(c_int, 188);
                continue;
            },
            190 => {
                unreachable;
            },
            191 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_29)))].pal_idx != null)) @as(c_int, 190) else @as(c_int, 189)));
                continue;
            },
            192 => {
                afz_p_29 = t.*.frame_thread.pass & @as(c_int, 1);
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_pal_idx_28 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.pal_idx_y));
                afz_pc = 186;
                continue;
            },
            194 => {
                afz_pc = @bitCast(@as(c_int, if (t.*.frame_thread.pass != 0) @as(c_int, 192) else @as(c_int, 193)));
                continue;
            },
            195 => {
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_dst_27 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * ((@as(ptrdiff_t, t.*.by) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)])) + @as(ptrdiff_t, t.*.bx))))));
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 0)] != 0) @as(c_int, 196) else @as(c_int, 182)));
                continue;
            },
            198 => {
                afz_init_x_26 = 0;
                afz_pc = 3;
                continue;
            },
            199 => {
                afz_sub_ch4_25 = imin(afz_ch4_15, (afz_init_y_23 + @as(c_int, 16)) >> @intCast(afz_ss_ver_5));
                afz_pc = 198;
                continue;
            },
            200 => {
                afz_sub_h4_24 = imin(afz_h4_13, @as(c_int, 16) + afz_init_y_23);
                afz_pc = 199;
                continue;
            },
            201 => {
                afz_init_y_23 = 0;
                afz_pc = 1;
                continue;
            },
            202 => {
                afz_intra_edge_filter_flag_22 = @as(c_int, afz_f_1.*.seq_hdr.*.intra_edge_filter) << @intCast(@as(c_int, 10));
                afz_pc = 201;
                continue;
            },
            203 => {
                afz_cbw4_20 = (afz_bw4_10 + afz_ss_hor_6) >> @intCast(afz_ss_hor_6);
                afz_cbh4_21 = (afz_bh4_11 + afz_ss_ver_5) >> @intCast(afz_ss_ver_5);
                afz_pc = 202;
                continue;
            },
            204 => {
                afz_edge_19 = @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.edge_16bpc))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 128)))));
                afz_pc = 203;
                continue;
            },
            205 => {
                afz_uv_t_dim_18 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_txfm_dimensions[b.*.uvtx])));
                afz_pc = 204;
                continue;
            },
            206 => {
                afz_t_dim_17 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_txfm_dimensions[b.*.unnamed_0.unnamed_0.tx])));
                afz_pc = 205;
                continue;
            },
            207 => {
                afz_has_chroma_16 = @as(c_int, @intFromBool(((afz_f_1.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) and ((afz_bw4_10 > afz_ss_hor_6) or ((t.*.bx & @as(c_int, 1)) != 0))) and ((afz_bh4_11 > afz_ss_ver_5) or ((t.*.by & @as(c_int, 1)) != 0))));
                afz_pc = 206;
                continue;
            },
            208 => {
                afz_cw4_14 = (afz_w4_12 + afz_ss_hor_6) >> @intCast(afz_ss_hor_6);
                afz_ch4_15 = (afz_h4_13 + afz_ss_ver_5) >> @intCast(afz_ss_ver_5);
                afz_pc = 207;
                continue;
            },
            209 => {
                afz_w4_12 = imin(afz_bw4_10, afz_f_1.*.bw - t.*.bx);
                afz_h4_13 = imin(afz_bh4_11, afz_f_1.*.bh - t.*.by);
                afz_pc = 208;
                continue;
            },
            210 => {
                afz_bw4_10 = afz_b_dim_9[@as(c_int, 0)];
                afz_bh4_11 = afz_b_dim_9[@as(c_int, 1)];
                afz_pc = 209;
                continue;
            },
            211 => {
                afz_b_dim_9 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]))))));
                afz_pc = 210;
                continue;
            },
            212 => {
                afz_cbx4_7 = afz_bx4_3 >> @intCast(afz_ss_hor_6);
                afz_cby4_8 = afz_by4_4 >> @intCast(afz_ss_ver_5);
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_ss_hor_6 = @as(c_int, @intFromBool(afz_f_1.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
                afz_pc = 212;
                continue;
            },
            214 => {
                afz_ss_ver_5 = @as(c_int, @intFromBool(afz_f_1.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_bx4_3 = t.*.bx & @as(c_int, 31);
                afz_by4_4 = t.*.by & @as(c_int, 31);
                afz_pc = 214;
                continue;
            },
            216 => {
                afz_dsp_2 = @ptrCast(@alignCast(@constCast(afz_f_1.*.dsp)));
                afz_pc = 215;
                continue;
            },
            217 => {
                afz_f_1 = @ptrCast(@alignCast(@constCast(t.*.f)));
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_ts_0 = t.*.ts;
                afz_pc = 217;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
}
pub export fn antfly_av1_dav1d_recon_b_inter_16bpc(t: [*c]Dav1dTaskContext, bs: enum_BlockSize, b: [*c]const Av1Block) c_int {
    _ = &t;
    _ = &bs;
    _ = &b;
    var afz_cbi_83: c_int = undefined;
    _ = &afz_cbi_83;
    var afz_p_82: c_int = undefined;
    _ = &afz_p_82;
    var afz_cth_86: c_int = undefined;
    _ = &afz_cth_86;
    var afz_ctw_85: c_int = undefined;
    _ = &afz_ctw_85;
    var afz_cf_ctx_84: u8 = undefined;
    _ = &afz_cf_ctx_84;
    var afz_txtp_81: enum_TxfmType = undefined;
    _ = &afz_txtp_81;
    var afz_eob_80: c_int = undefined;
    _ = &afz_eob_80;
    var afz_cf_79: [*c]coef = undefined;
    _ = &afz_cf_79;
    var afz_x_78: c_int = undefined;
    _ = &afz_x_78;
    var afz_uvdst_77: [*c]pixel = undefined;
    _ = &afz_uvdst_77;
    var afz_pl_76: c_int = undefined;
    _ = &afz_pl_76;
    var afz_x_74: c_int = undefined;
    _ = &afz_x_74;
    var afz_x_off_75: c_int = undefined;
    _ = &afz_x_off_75;
    var afz_y_off_72: c_int = undefined;
    _ = &afz_y_off_72;
    var afz_y_73: c_int = undefined;
    _ = &afz_y_73;
    var afz_init_x_71: c_int = undefined;
    _ = &afz_init_x_71;
    var afz_init_y_70: c_int = undefined;
    _ = &afz_init_y_70;
    var afz_tx_split_69: [2]u16 = undefined;
    _ = &afz_tx_split_69;
    var afz_ytx_68: [*c]TxfmInfo = undefined;
    _ = &afz_ytx_68;
    var afz_uvtx_67: [*c]TxfmInfo = undefined;
    _ = &afz_uvtx_67;
    var afz_memset_ch_66: dav1d_memset_pow2_fn = undefined;
    _ = &afz_memset_ch_66;
    var afz_memset_cw_65: dav1d_memset_pow2_fn = undefined;
    _ = &afz_memset_cw_65;
    var afz_a_64: [*c]BlockContext = undefined;
    _ = &afz_a_64;
    var afz_cw4_62: c_int = undefined;
    _ = &afz_cw4_62;
    var afz_ch4_63: c_int = undefined;
    _ = &afz_ch4_63;
    var afz_pl_21: c_int = undefined;
    _ = &afz_pl_21;
    var afz_pl_39: c_int = undefined;
    _ = &afz_pl_39;
    var afz_pl_38: c_int = undefined;
    _ = &afz_pl_38;
    var afz_top_filter_2d_37: enum_Filter2d = undefined;
    _ = &afz_top_filter_2d_37;
    var afz_pl_36: c_int = undefined;
    _ = &afz_pl_36;
    var afz_left_filter_2d_35: enum_Filter2d = undefined;
    _ = &afz_left_filter_2d_35;
    var afz_pl_34: c_int = undefined;
    _ = &afz_pl_34;
    var afz_h_off_32: ptrdiff_t = undefined;
    _ = &afz_h_off_32;
    var afz_v_off_33: ptrdiff_t = undefined;
    _ = &afz_v_off_33;
    var afz_sby_50: c_int = undefined;
    _ = &afz_sby_50;
    var afz_top_sb_edge_49: [*c]pixel = undefined;
    _ = &afz_top_sb_edge_49;
    var afz_uvdst_48: [*c]pixel = undefined;
    _ = &afz_uvdst_48;
    var afz_angle_47: c_int = undefined;
    _ = &afz_angle_47;
    var afz_m_46: enum_IntraPredMode = undefined;
    _ = &afz_m_46;
    var afz_tl_edge_45: [*c]pixel = undefined;
    _ = &afz_tl_edge_45;
    var afz_tmp_44: [*c]pixel = undefined;
    _ = &afz_tmp_44;
    var afz_pl_43: c_int = undefined;
    _ = &afz_pl_43;
    var afz_ii_mask_42: [*c]u8 = undefined;
    _ = &afz_ii_mask_42;
    var afz_pl_40: c_int = undefined;
    _ = &afz_pl_40;
    var afz_pl_41: c_int = undefined;
    _ = &afz_pl_41;
    var afz_r_31: [*c][*c]refmvs_block = undefined;
    _ = &afz_r_31;
    var afz_is_sub8x8_30: c_int = undefined;
    _ = &afz_is_sub8x8_30;
    var afz_sby_29: c_int = undefined;
    _ = &afz_sby_29;
    var afz_top_sb_edge_28: [*c]pixel = undefined;
    _ = &afz_top_sb_edge_28;
    var afz_angle_27: c_int = undefined;
    _ = &afz_angle_27;
    var afz_tmp_26: [*c]pixel = undefined;
    _ = &afz_tmp_26;
    var afz_m_25: enum_IntraPredMode = undefined;
    _ = &afz_m_25;
    var afz_tl_edge_24: [*c]pixel = undefined;
    _ = &afz_tl_edge_24;
    var afz_filter_2d_23: enum_Filter2d = undefined;
    _ = &afz_filter_2d_23;
    var afz_refp_22: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_refp_22;
    var afz_uvdst_61: [*c]pixel = undefined;
    _ = &afz_uvdst_61;
    var afz_refp_60: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_refp_60;
    var afz_i_59: c_int = undefined;
    _ = &afz_i_59;
    var afz_pl_58: c_int = undefined;
    _ = &afz_pl_58;
    var afz_refp_57: [*c]Dav1dThreadPicture = undefined;
    _ = &afz_refp_57;
    var afz_i_56: c_int = undefined;
    _ = &afz_i_56;
    var afz_mask_55: [*c]u8 = undefined;
    _ = &afz_mask_55;
    var afz_seg_mask_54: [*c]u8 = undefined;
    _ = &afz_seg_mask_54;
    var afz_jnt_weight_53: c_int = undefined;
    _ = &afz_jnt_weight_53;
    var afz_tmp_52: [*c][16384]i16 = undefined;
    _ = &afz_tmp_52;
    var afz_filter_2d_51: enum_Filter2d = undefined;
    _ = &afz_filter_2d_51;
    var afz_uvdstoff_20: ptrdiff_t = undefined;
    _ = &afz_uvdstoff_20;
    var afz_dst_19: [*c]pixel = undefined;
    _ = &afz_dst_19;
    var afz_cbh4_17: c_int = undefined;
    _ = &afz_cbh4_17;
    var afz_cbw4_18: c_int = undefined;
    _ = &afz_cbw4_18;
    var afz_res_16: c_int = undefined;
    _ = &afz_res_16;
    var afz_chr_layout_idx_15: c_int = undefined;
    _ = &afz_chr_layout_idx_15;
    var afz_has_chroma_14: c_int = undefined;
    _ = &afz_has_chroma_14;
    var afz_w4_12: c_int = undefined;
    _ = &afz_w4_12;
    var afz_h4_13: c_int = undefined;
    _ = &afz_h4_13;
    var afz_bw4_10: c_int = undefined;
    _ = &afz_bw4_10;
    var afz_bh4_11: c_int = undefined;
    _ = &afz_bh4_11;
    var afz_b_dim_9: [*c]u8 = undefined;
    _ = &afz_b_dim_9;
    var afz_cbx4_7: c_int = undefined;
    _ = &afz_cbx4_7;
    var afz_cby4_8: c_int = undefined;
    _ = &afz_cby4_8;
    var afz_ss_hor_6: c_int = undefined;
    _ = &afz_ss_hor_6;
    var afz_ss_ver_5: c_int = undefined;
    _ = &afz_ss_ver_5;
    var afz_bx4_3: c_int = undefined;
    _ = &afz_bx4_3;
    var afz_by4_4: c_int = undefined;
    _ = &afz_by4_4;
    var afz_dsp_2: [*c]Dav1dDSPContext = undefined;
    _ = &afz_dsp_2;
    var afz_f_1: [*c]Dav1dFrameContext = undefined;
    _ = &afz_f_1;
    var afz_ts_0: [*c]Dav1dTileState = undefined;
    _ = &afz_ts_0;
    var afz_pc: c_uint = 286;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return 0;
            },
            1 => {
                return 0;
            },
            2 => {
                afz_pc = @bitCast(@as(c_int, if (afz_init_y_70 < afz_bh4_11) @as(c_int, 65) else @as(c_int, 1)));
                continue;
            },
            3 => {
                afz_init_y_70 += 16;
                afz_pc = 2;
                continue;
            },
            4 => {
                afz_pc = @bitCast(@as(c_int, if (afz_init_x_71 < afz_bw4_10) @as(c_int, 64) else @as(c_int, 3)));
                continue;
            },
            5 => {
                afz_init_x_71 += 16;
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_76 < @as(c_int, 2)) @as(c_int, 46) else @as(c_int, 5)));
                continue;
            },
            7 => {
                afz_pl_76 += 1;
                afz_pc = 6;
                continue;
            },
            8 => {
                t.*.by -= afz_y_73 << @intCast(afz_ss_ver_5);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_73 < imin(afz_ch4_63, (afz_init_y_70 + @as(c_int, 16)) >> @intCast(afz_ss_ver_5))) @as(c_int, 44) else @as(c_int, 8)));
                continue;
            },
            10 => {
                afz_y_73 += afz_uvtx_67.*.h;
                afz_pc = 9;
                continue;
            },
            11 => {
                t.*.by += @as(c_int, afz_uvtx_67.*.h) << @intCast(afz_ss_ver_5);
                afz_pc = 10;
                continue;
            },
            12 => {
                t.*.bx -= afz_x_78 << @intCast(afz_ss_hor_6);
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_uvdst_77 += @as(usize, @bitCast(@as(isize, @intCast((PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)]) * @as(ptrdiff_t, 4)) * @as(ptrdiff_t, @as(c_int, afz_uvtx_67.*.h))))));
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_pc = @bitCast(@as(c_int, if (afz_x_78 < imin(afz_cw4_62, (afz_init_x_71 + @as(c_int, 16)) >> @intCast(afz_ss_hor_6))) @as(c_int, 42) else @as(c_int, 13)));
                continue;
            },
            15 => {
                afz_x_78 += afz_uvtx_67.*.w;
                afz_pc = 14;
                continue;
            },
            16 => {
                t.*.bx += @as(c_int, afz_uvtx_67.*.w) << @intCast(afz_ss_hor_6);
                afz_pc = 15;
                continue;
            },
            17 => {
                _ = @as(u8, 0);
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 17) else @as(c_int, 16)));
                continue;
            },
            19 => {
                afz_dsp_2.*.itx.itxfm_add[b.*.uvtx][afz_txtp_81].?(&afz_uvdst_77[@bitCast(@as(isize, @intCast(@as(c_int, 4) * afz_x_78)))], afz_f_1.*.cur.stride[@as(c_int, 1)], afz_cf_79, afz_eob_80, afz_f_1.*.bitdepth_max);
                afz_pc = 18;
                continue;
            },
            20 => {
                _ = @as(u8, 0);
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 20) else @as(c_int, 19)));
                continue;
            },
            22 => {
                afz_pc = @bitCast(@as(c_int, if (afz_eob_80 >= @as(c_int, 0)) @as(c_int, 21) else @as(c_int, 16)));
                continue;
            },
            23 => {
                afz_txtp_81 = @bitCast(afz_cbi_83 & @as(c_int, 31));
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_eob_80 = afz_cbi_83 >> @intCast(@as(c_int, 5));
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_82)))].cf += @as(usize, @bitCast(@as(isize, @intCast((@as(c_int, afz_uvtx_67.*.w) * @as(c_int, afz_uvtx_67.*.h)) * @as(c_int, 16)))));
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_cf_79 = afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_82)))].cf;
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_cbi_83 = (blk: {
                    const ref = &afz_ts_0.*.frame_thread[@bitCast(@as(isize, @intCast(afz_p_82)))].cbi;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).*;
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_p_82 = t.*.frame_thread.pass & @as(c_int, 1);
                afz_pc = 27;
                continue;
            },
            29 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.l.ccoef[@bitCast(@as(isize, @intCast(afz_pl_76)))][@bitCast(@as(isize, @intCast(afz_cby4_8 + afz_y_73)))])), afz_cf_ctx_84, afz_cth_86);
                afz_pc = 22;
                continue;
            },
            30 => {
                dav1d_memset_likely_pow2(@ptrCast(@alignCast(&t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(afz_pl_76)))][@bitCast(@as(isize, @intCast(afz_cbx4_7 + afz_x_78)))])), afz_cf_ctx_84, afz_ctw_85);
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_cth_86 = imin(afz_uvtx_67.*.h, ((afz_f_1.*.bh - t.*.by) + afz_ss_ver_5) >> @intCast(afz_ss_ver_5));
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_ctw_85 = imin(afz_uvtx_67.*.w, ((afz_f_1.*.bw - t.*.bx) + afz_ss_hor_6) >> @intCast(afz_ss_hor_6));
                afz_pc = 31;
                continue;
            },
            33 => {
                _ = @as(c_int, 0);
                afz_pc = 32;
                continue;
            },
            34 => {
                afz_pc = @bitCast(@as(c_int, if (((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) @as(c_int, 33) else @as(c_int, 32)));
                continue;
            },
            35 => {
                afz_eob_80 = decode_coefs(t, &t.*.a.*.ccoef[@bitCast(@as(isize, @intCast(afz_pl_76)))][@bitCast(@as(isize, @intCast(afz_cbx4_7 + afz_x_78)))], &t.*.l.ccoef[@bitCast(@as(isize, @intCast(afz_pl_76)))][@bitCast(@as(isize, @intCast(afz_cby4_8 + afz_y_73)))], b.*.uvtx, bs, b, 0, @as(c_int, 1) + afz_pl_76, afz_cf_79, &afz_txtp_81, &afz_cf_ctx_84);
                afz_pc = 34;
                continue;
            },
            36 => {
                afz_txtp_81 = t.*.scratch.unnamed_1.unnamed_1.txtp_map[@bitCast(@as(isize, @intCast((((afz_by4_4 + (afz_y_73 << @intCast(afz_ss_ver_5))) * @as(c_int, 32)) + afz_bx4_3) + (afz_x_78 << @intCast(afz_ss_hor_6)))))];
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_cf_79 = @ptrCast(@alignCast(&t.*.unnamed_0.cf_16bpc));
                afz_pc = 36;
                continue;
            },
            38 => {
                afz_pc = 37;
                continue;
            },
            39 => {
                afz_pc = @bitCast(@as(c_int, if (t.*.frame_thread.pass != 0) @as(c_int, 28) else @as(c_int, 38)));
                continue;
            },
            40 => {
                afz_pc = 39;
                continue;
            },
            41 => {
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_x_78 = afz_init_x_71 >> @intCast(afz_ss_hor_6);
                t.*.bx += afz_init_x_71;
                afz_pc = 14;
                continue;
            },
            44 => {
                afz_pc = 43;
                continue;
            },
            45 => {
                afz_y_73 = afz_init_y_70 >> @intCast(afz_ss_ver_5);
                t.*.by += afz_init_y_70;
                afz_pc = 9;
                continue;
            },
            46 => {
                afz_uvdst_77 = (@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_76)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))))) + @as(usize, @bitCast(@as(isize, @intCast(((PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)]) * @as(ptrdiff_t, afz_init_y_70)) * @as(ptrdiff_t, 4)) >> @intCast(@as(ptrdiff_t, afz_ss_ver_5))))));
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_pl_76 = 0;
                afz_pc = 6;
                continue;
            },
            48 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 47) else @as(c_int, 5)));
                continue;
            },
            49 => {
                t.*.by -= afz_y_73;
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_dst_19 -= @as(usize, @bitCast(@as(isize, @intCast((PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)]) * @as(ptrdiff_t, 4)) * @as(ptrdiff_t, afz_y_73)))));
                afz_pc = 49;
                continue;
            },
            51 => {
                afz_pc = @bitCast(@as(c_int, if (afz_y_73 < imin(afz_h4_13, afz_init_y_70 + @as(c_int, 16))) @as(c_int, 61) else @as(c_int, 50)));
                continue;
            },
            52 => {
                afz_y_73 += afz_ytx_68.*.h;
                afz_y_off_72 += 1;
                afz_pc = 51;
                continue;
            },
            53 => {
                t.*.by += afz_ytx_68.*.h;
                afz_pc = 52;
                continue;
            },
            54 => {
                t.*.bx -= afz_x_74;
                afz_pc = 53;
                continue;
            },
            55 => {
                afz_dst_19 += @as(usize, @bitCast(@as(isize, @intCast((PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)]) * @as(ptrdiff_t, 4)) * @as(ptrdiff_t, @as(c_int, afz_ytx_68.*.h))))));
                afz_pc = 54;
                continue;
            },
            56 => {
                afz_pc = @bitCast(@as(c_int, if (afz_x_74 < imin(afz_w4_12, afz_init_x_71 + @as(c_int, 16))) @as(c_int, 59) else @as(c_int, 55)));
                continue;
            },
            57 => {
                afz_x_74 += afz_ytx_68.*.w;
                afz_x_off_75 += 1;
                afz_pc = 56;
                continue;
            },
            58 => {
                t.*.bx += afz_ytx_68.*.w;
                afz_pc = 57;
                continue;
            },
            59 => {
                read_coef_tree(t, bs, b, b.*.unnamed_0.unnamed_1.max_ytx, 0, @ptrCast(@alignCast(&afz_tx_split_69)), afz_x_off_75, afz_y_off_72, &afz_dst_19[@bitCast(@as(isize, @intCast(afz_x_74 * @as(c_int, 4))))]);
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_x_74 = afz_init_x_71;
                t.*.bx += afz_init_x_71;
                afz_pc = 56;
                continue;
            },
            61 => {
                afz_x_off_75 = @as(c_int, @intFromBool(!!(afz_init_x_71 != 0)));
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_y_73 = afz_init_y_70;
                t.*.by += afz_init_y_70;
                afz_pc = 51;
                continue;
            },
            63 => {
                afz_dst_19 += @as(usize, @bitCast(@as(isize, @intCast((PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)]) * @as(ptrdiff_t, 4)) * @as(ptrdiff_t, afz_init_y_70)))));
                afz_pc = 62;
                continue;
            },
            64 => {
                afz_y_off_72 = @as(c_int, @intFromBool(!!(afz_init_y_70 != 0)));
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_init_x_71 = 0;
                afz_pc = 4;
                continue;
            },
            66 => {
                afz_init_y_70 = 0;
                afz_pc = 2;
                continue;
            },
            67 => {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&afz_tx_split_69))))), @ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(@constCast(&@as([2]u16, [2]u16{
                    b.*.unnamed_0.unnamed_1.tx_split0,
                    b.*.unnamed_0.unnamed_1.tx_split1,
                }))))))), @sizeOf(@TypeOf(afz_tx_split_69)));
                afz_pc = 66;
                continue;
            },
            68 => {
                afz_ytx_68 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_txfm_dimensions[b.*.unnamed_0.unnamed_1.max_ytx])));
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_uvtx_67 = @ptrCast(@alignCast(@constCast(&antfly_av1_dav1d_txfm_dimensions[b.*.uvtx])));
                afz_pc = 68;
                continue;
            },
            70 => {
                return 0;
            },
            71 => {
                afz_memset_ch_66.?(@ptrCast(@alignCast(&t.*.l.ccoef[@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_cby4_8)))])), 64);
                afz_pc = 70;
                continue;
            },
            72 => {
                afz_memset_ch_66.?(@ptrCast(@alignCast(&t.*.l.ccoef[@as(c_int, 0)][@bitCast(@as(isize, @intCast(afz_cby4_8)))])), 64);
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_memset_cw_65.?(@ptrCast(@alignCast(&afz_a_64.*.ccoef[@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_cbx4_7)))])), 64);
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_memset_cw_65.?(@ptrCast(@alignCast(&afz_a_64.*.ccoef[@as(c_int, 0)][@bitCast(@as(isize, @intCast(afz_cbx4_7)))])), 64);
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_memset_ch_66 = antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, afz_cbh4_17))))))];
                afz_pc = 74;
                continue;
            },
            76 => {
                afz_memset_cw_65 = antfly_av1_dav1d_memset_pow2[@bitCast(@as(isize, @intCast(ulog2(@bitCast(@as(c_int, afz_cbw4_18))))))];
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 76) else @as(c_int, 70)));
                continue;
            },
            78 => {
                antfly_av1_dav1d_memset_pow2[afz_b_dim_9[@as(c_int, 3)]].?(@ptrCast(@alignCast(&t.*.l.lcoef[@bitCast(@as(isize, @intCast(afz_by4_4)))])), 64);
                afz_pc = 77;
                continue;
            },
            79 => {
                antfly_av1_dav1d_memset_pow2[afz_b_dim_9[@as(c_int, 2)]].?(@ptrCast(@alignCast(&afz_a_64.*.lcoef[@bitCast(@as(isize, @intCast(afz_bx4_3)))])), 64);
                afz_pc = 78;
                continue;
            },
            80 => {
                afz_a_64 = t.*.a;
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.skip != 0) @as(c_int, 80) else @as(c_int, 69)));
                continue;
            },
            82 => {
                afz_cw4_62 = (afz_w4_12 + afz_ss_hor_6) >> @intCast(afz_ss_hor_6);
                afz_ch4_63 = (afz_h4_13 + afz_ss_ver_5) >> @intCast(afz_ss_ver_5);
                afz_pc = 81;
                continue;
            },
            83 => {
                _ = @as(u8, 0);
                afz_pc = 82;
                continue;
            },
            84 => {
                _ = @as(u8, 0);
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 84) else @as(c_int, 82)));
                continue;
            },
            86 => {
                _ = @as(u8, 0);
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_pc = @bitCast(@as(c_int, if ((((((false and (@as(c_int, afz_f_1.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) and false) @as(c_int, 86) else @as(c_int, 82)));
                continue;
            },
            88 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_21 < @as(c_int, 3)) @as(c_int, 92) else @as(c_int, 87)));
                continue;
            },
            89 => {
                afz_pl_21 += 1;
                afz_pc = 88;
                continue;
            },
            90 => {
                return afz_res_16;
            },
            91 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 90) else @as(c_int, 89)));
                continue;
            },
            92 => {
                afz_res_16 = mc(t, @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(afz_pl_21)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10 << @intCast(@as(c_int, @intFromBool(afz_bw4_10 == afz_ss_hor_6))), afz_bh4_11 << @intCast(@as(c_int, @intFromBool(afz_bh4_11 == afz_ss_ver_5))), t.*.bx & ~afz_ss_hor_6, t.*.by & ~afz_ss_ver_5, afz_pl_21, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], &afz_f_1.*.sr_cur, 0, FILTER_2D_BILINEAR);
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_pl_21 = 1;
                afz_pc = 88;
                continue;
            },
            94 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 93) else @as(c_int, 87)));
                continue;
            },
            95 => {
                return afz_res_16;
            },
            96 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 95) else @as(c_int, 94)));
                continue;
            },
            97 => {
                afz_res_16 = mc(t, afz_dst_19, null, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by, 0, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], &afz_f_1.*.sr_cur, 0, FILTER_2D_BILINEAR);
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 100) else @as(c_int, 97);
                continue;
            },
            99 => {
                unreachable;
            },
            100 => {
                afz_pc = @bitCast(@as(c_int, if (!!(@as(c_int, afz_f_1.*.frame_hdr.*.super_res.enabled) != 0)) @as(c_int, 99) else @as(c_int, 98)));
                continue;
            },
            101 => {
                t.*.tl_4x4_filter = afz_filter_2d_23;
                afz_pc = 87;
                continue;
            },
            102 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_39 < @as(c_int, 2)) @as(c_int, 106) else @as(c_int, 101)));
                continue;
            },
            103 => {
                afz_pl_39 += 1;
                afz_pc = 102;
                continue;
            },
            104 => {
                return afz_res_16;
            },
            105 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 104) else @as(c_int, 103)));
                continue;
            },
            106 => {
                afz_res_16 = mc(t, ((@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_39)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))))) + @as(usize, @bitCast(@as(isize, @intCast(afz_h_off_32))))) + @as(usize, @bitCast(@as(isize, @intCast(afz_v_off_33)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by, @as(c_int, 1) + afz_pl_39, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], afz_refp_22, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], afz_filter_2d_23);
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_pl_39 = 0;
                afz_pc = 102;
                continue;
            },
            108 => {
                afz_v_off_33 = @as(ptrdiff_t, 2) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)]);
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_38 < @as(c_int, 2)) @as(c_int, 113) else @as(c_int, 108)));
                continue;
            },
            110 => {
                afz_pl_38 += 1;
                afz_pc = 109;
                continue;
            },
            111 => {
                return afz_res_16;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 111) else @as(c_int, 110)));
                continue;
            },
            113 => {
                afz_res_16 = mc(t, (@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_38)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))))) + @as(usize, @bitCast(@as(isize, @intCast(afz_h_off_32)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by - @as(c_int, 1), @as(c_int, 1) + afz_pl_38, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))].mv.mv[@as(c_int, 0)], &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(@as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], @as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1), if (t.*.frame_thread.pass != @as(c_int, 2)) afz_top_filter_2d_37 else @as(enum_Filter2d, @bitCast(@as(c_int, afz_f_1.*.frame_thread.b[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, t.*.by - @as(c_int, 1)) * afz_f_1.*.b4_stride) + @as(ptrdiff_t, t.*.bx))))].unnamed_0.unnamed_1.filter2d))));
                afz_pc = 112;
                continue;
            },
            114 => {
                afz_pl_38 = 0;
                afz_pc = 109;
                continue;
            },
            115 => {
                afz_top_filter_2d_37 = antfly_av1_dav1d_filter_2d[t.*.a.*.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_bx4_3)))]][t.*.a.*.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(afz_bx4_3)))]];
                afz_pc = 114;
                continue;
            },
            116 => {
                afz_pc = @bitCast(@as(c_int, if (afz_bh4_11 == afz_ss_ver_5) @as(c_int, 115) else @as(c_int, 107)));
                continue;
            },
            117 => {
                afz_h_off_32 = 2;
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_36 < @as(c_int, 2)) @as(c_int, 122) else @as(c_int, 117)));
                continue;
            },
            119 => {
                afz_pl_36 += 1;
                afz_pc = 118;
                continue;
            },
            120 => {
                return afz_res_16;
            },
            121 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 120) else @as(c_int, 119)));
                continue;
            },
            122 => {
                afz_res_16 = mc(t, (@as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_36)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))))) + @as(usize, @bitCast(@as(isize, @intCast(afz_v_off_33)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10, afz_bh4_11, t.*.bx - @as(c_int, 1), t.*.by, @as(c_int, 1) + afz_pl_36, afz_r_31[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].mv.mv[@as(c_int, 0)], &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(@as(c_int, afz_r_31[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], @as(c_int, afz_r_31[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1), if (t.*.frame_thread.pass != @as(c_int, 2)) afz_left_filter_2d_35 else @as(enum_Filter2d, @bitCast(@as(c_int, afz_f_1.*.frame_thread.b[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, t.*.by) * afz_f_1.*.b4_stride) + @as(ptrdiff_t, t.*.bx)) - @as(ptrdiff_t, 1))))].unnamed_0.unnamed_1.filter2d))));
                afz_pc = 121;
                continue;
            },
            123 => {
                afz_pl_36 = 0;
                afz_pc = 118;
                continue;
            },
            124 => {
                afz_left_filter_2d_35 = antfly_av1_dav1d_filter_2d[t.*.l.filter[@as(c_int, 1)][@bitCast(@as(isize, @intCast(afz_by4_4)))]][t.*.l.filter[@as(c_int, 0)][@bitCast(@as(isize, @intCast(afz_by4_4)))]];
                afz_pc = 123;
                continue;
            },
            125 => {
                afz_pc = @bitCast(@as(c_int, if (afz_bw4_10 == @as(c_int, 1)) @as(c_int, 124) else @as(c_int, 116)));
                continue;
            },
            126 => {
                afz_h_off_32 = 2;
                afz_pc = 125;
                continue;
            },
            127 => {
                afz_v_off_33 = @as(ptrdiff_t, 2) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)]);
                afz_pc = 126;
                continue;
            },
            128 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_34 < @as(c_int, 2)) @as(c_int, 132) else @as(c_int, 127)));
                continue;
            },
            129 => {
                afz_pl_34 += 1;
                afz_pc = 128;
                continue;
            },
            130 => {
                return afz_res_16;
            },
            131 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 130) else @as(c_int, 129)));
                continue;
            },
            132 => {
                afz_res_16 = mc(t, @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_34)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10, afz_bh4_11, t.*.bx - @as(c_int, 1), t.*.by - @as(c_int, 1), @as(c_int, 1) + afz_pl_34, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].mv.mv[@as(c_int, 0)], &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(@as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], @as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) - @as(c_int, 1), if (t.*.frame_thread.pass != @as(c_int, 2)) t.*.tl_4x4_filter else @as(enum_Filter2d, @bitCast(@as(c_int, afz_f_1.*.frame_thread.b[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, t.*.by - @as(c_int, 1)) * afz_f_1.*.b4_stride) + @as(ptrdiff_t, t.*.bx)) - @as(ptrdiff_t, 1))))].unnamed_0.unnamed_1.filter2d))));
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_pl_34 = 0;
                afz_pc = 128;
                continue;
            },
            134 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_bw4_10 == @as(c_int, 1)) and (afz_bh4_11 == afz_ss_ver_5)) @as(c_int, 133) else @as(c_int, 125)));
                continue;
            },
            135 => {
                afz_h_off_32 = 0;
                afz_v_off_33 = 0;
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 138) else @as(c_int, 135);
                continue;
            },
            137 => {
                unreachable;
            },
            138 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ss_hor_6 == @as(c_int, 1))) @as(c_int, 137) else @as(c_int, 136)));
                continue;
            },
            139 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_43 < @as(c_int, 2)) @as(c_int, 153) else @as(c_int, 101)));
                continue;
            },
            140 => {
                afz_pl_43 += 1;
                afz_pc = 139;
                continue;
            },
            141 => {
                afz_dsp_2.*.mc.blend.?(afz_uvdst_48, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_tmp_44, afz_cbw4_18 * @as(c_int, 4), afz_cbh4_17 * @as(c_int, 4), afz_ii_mask_42);
                afz_pc = 140;
                continue;
            },
            142 => {
                afz_dsp_2.*.ipred.intra_pred[afz_m_46].?(afz_tmp_44, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, @bitCast(@as(c_long, afz_cbw4_18 * @as(c_int, 4)))) *% @sizeOf(pixel)))), afz_tl_edge_45, afz_cbw4_18 * @as(c_int, 4), afz_cbh4_17 * @as(c_int, 4), 0, 0, 0, afz_f_1.*.bitdepth_max);
                afz_pc = 141;
                continue;
            },
            143 => {
                afz_m_46 = antfly_av1_dav1d_prepare_intra_edges_16bpc(t.*.bx >> @intCast(afz_ss_hor_6), @as(c_int, @intFromBool((t.*.bx >> @intCast(afz_ss_hor_6)) > (afz_ts_0.*.tiling.col_start >> @intCast(afz_ss_hor_6)))), t.*.by >> @intCast(afz_ss_ver_5), @as(c_int, @intFromBool((t.*.by >> @intCast(afz_ss_ver_5)) > (afz_ts_0.*.tiling.row_start >> @intCast(afz_ss_ver_5)))), afz_ts_0.*.tiling.col_end >> @intCast(afz_ss_hor_6), afz_ts_0.*.tiling.row_end >> @intCast(afz_ss_ver_5), 0, afz_uvdst_48, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_top_sb_edge_49, afz_m_46, &afz_angle_47, afz_cbw4_18, afz_cbh4_17, 0, afz_tl_edge_45, afz_f_1.*.bitdepth_max);
                afz_pc = 142;
                continue;
            },
            144 => {
                afz_top_sb_edge_49 += @as(usize, @bitCast(@as(isize, @intCast((afz_f_1.*.sb128w * @as(c_int, 128)) * (afz_sby_50 - @as(c_int, 1))))));
                afz_pc = 143;
                continue;
            },
            145 => {
                afz_sby_50 = t.*.by >> @intCast(afz_f_1.*.sb_shift);
                afz_pc = 144;
                continue;
            },
            146 => {
                afz_top_sb_edge_49 = afz_f_1.*.ipred_edge[@bitCast(@as(isize, @intCast(afz_pl_43 + @as(c_int, 1))))];
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_pc = @bitCast(@as(c_int, if (!((t.*.by & (afz_f_1.*.sb_step - @as(c_int, 1))) != 0)) @as(c_int, 146) else @as(c_int, 143)));
                continue;
            },
            148 => {
                afz_top_sb_edge_49 = null;
                afz_pc = 147;
                continue;
            },
            149 => {
                afz_uvdst_48 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_43)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))));
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_angle_47 = 0;
                afz_pc = 149;
                continue;
            },
            151 => {
                afz_m_46 = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode) == II_SMOOTH_PRED) SMOOTH_PRED else @as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode));
                afz_pc = 150;
                continue;
            },
            152 => {
                afz_tl_edge_45 = @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.edge_16bpc))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                afz_pc = 151;
                continue;
            },
            153 => {
                afz_tmp_44 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.interintra_16bpc));
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_pl_43 = 0;
                afz_pc = 139;
                continue;
            },
            155 => {
                afz_ii_mask_42 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrFromInt(@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks))) +% (@as(usize, @bitCast(@as(c_long, if (@as(c_int, b.*.unnamed_0.unnamed_1.interintra_type) == INTER_INTRA_BLEND) @as(c_int, antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(afz_chr_layout_idx_15)))][bs -% @as(enum_BlockSize, BS_32x32)].ii[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode]) else @as(c_int, antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(afz_chr_layout_idx_15)))][bs -% @as(enum_BlockSize, BS_32x32)].wedge[@as(c_int, 0)][b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx])))) *% @as(usize, 8)))))));
                afz_pc = 154;
                continue;
            },
            156 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.unnamed_0.unnamed_1.interintra_type != 0) @as(c_int, 155) else @as(c_int, 101)));
                continue;
            },
            157 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_40 < @as(c_int, 2)) @as(c_int, 161) else @as(c_int, 156)));
                continue;
            },
            158 => {
                afz_pl_40 += 1;
                afz_pc = 157;
                continue;
            },
            159 => {
                return afz_res_16;
            },
            160 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 159) else @as(c_int, 158)));
                continue;
            },
            161 => {
                afz_res_16 = warp_affine(t, @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_40)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_b_dim_9, @as(c_int, 1) + afz_pl_40, afz_refp_22, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) &t.*.warpmv else &afz_f_1.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]);
                afz_pc = 160;
                continue;
            },
            162 => {
                afz_pl_40 = 0;
                afz_pc = 157;
                continue;
            },
            163 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_41 < @as(c_int, 2)) @as(c_int, 171) else @as(c_int, 156)));
                continue;
            },
            164 => {
                afz_pl_41 += 1;
                afz_pc = 163;
                continue;
            },
            165 => {
                return afz_res_16;
            },
            166 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 165) else @as(c_int, 164)));
                continue;
            },
            167 => {
                afz_res_16 = obmc(t, @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_41)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20)))), afz_f_1.*.cur.stride[@as(c_int, 1)], afz_b_dim_9, @as(c_int, 1) + afz_pl_41, afz_bx4_3, afz_by4_4, afz_w4_12, afz_h4_13);
                afz_pc = 166;
                continue;
            },
            168 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_OBMC) @as(c_int, 167) else @as(c_int, 164)));
                continue;
            },
            169 => {
                return afz_res_16;
            },
            170 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 169) else @as(c_int, 168)));
                continue;
            },
            171 => {
                afz_res_16 = mc(t, @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_41)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20)))), null, afz_f_1.*.cur.stride[@as(c_int, 1)], afz_bw4_10 << @intCast(@as(c_int, @intFromBool(afz_bw4_10 == afz_ss_hor_6))), afz_bh4_11 << @intCast(@as(c_int, @intFromBool(afz_bh4_11 == afz_ss_ver_5))), t.*.bx & ~afz_ss_hor_6, t.*.by & ~afz_ss_ver_5, @as(c_int, 1) + afz_pl_41, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], afz_refp_22, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], afz_filter_2d_23);
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_pl_41 = 0;
                afz_pc = 163;
                continue;
            },
            173 => {
                afz_pc = @bitCast(@as(c_int, if ((imin(afz_cbw4_18, afz_cbh4_17) > @as(c_int, 1)) and (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV) and (@as(c_int, afz_f_1.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]) != 0)) or ((@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) and (t.*.warpmv.type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION))))) @as(c_int, 162) else @as(c_int, 172)));
                continue;
            },
            174 => {
                afz_pc = @bitCast(@as(c_int, if (afz_is_sub8x8_30 != 0) @as(c_int, 138) else @as(c_int, 173)));
                continue;
            },
            175 => {
                afz_is_sub8x8_30 &= @as(c_int, @intFromBool(@as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                afz_pc = 174;
                continue;
            },
            176 => {
                afz_pc = @bitCast(@as(c_int, if ((afz_bw4_10 == @as(c_int, 1)) and (afz_bh4_11 == afz_ss_ver_5)) @as(c_int, 175) else @as(c_int, 174)));
                continue;
            },
            177 => {
                afz_is_sub8x8_30 &= @as(c_int, @intFromBool(@as(c_int, afz_r_31[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@bitCast(@as(isize, @intCast(t.*.bx)))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = @bitCast(@as(c_int, if (afz_bh4_11 == afz_ss_ver_5) @as(c_int, 177) else @as(c_int, 176)));
                continue;
            },
            179 => {
                afz_is_sub8x8_30 &= @as(c_int, @intFromBool(@as(c_int, afz_r_31[@as(c_int, 0)][@bitCast(@as(isize, @intCast(t.*.bx - @as(c_int, 1))))].ref.ref[@as(c_int, 0)]) > @as(c_int, 0)));
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_pc = @bitCast(@as(c_int, if (afz_bw4_10 == @as(c_int, 1)) @as(c_int, 179) else @as(c_int, 178)));
                continue;
            },
            181 => {
                afz_r_31 = &t.*.rt.r[@bitCast(@as(isize, @intCast((t.*.by & @as(c_int, 31)) + @as(c_int, 5))))];
                afz_pc = 180;
                continue;
            },
            182 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 184) else @as(c_int, 181);
                continue;
            },
            183 => {
                unreachable;
            },
            184 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ss_hor_6 == @as(c_int, 1))) @as(c_int, 183) else @as(c_int, 182)));
                continue;
            },
            185 => {
                afz_pc = @bitCast(@as(c_int, if (afz_is_sub8x8_30 != 0) @as(c_int, 184) else @as(c_int, 174)));
                continue;
            },
            186 => {
                afz_pc = 185;
                continue;
            },
            187 => {
                afz_is_sub8x8_30 = @as(c_int, @intFromBool((afz_bw4_10 == afz_ss_hor_6) or (afz_bh4_11 == afz_ss_ver_5)));
                afz_pc = 186;
                continue;
            },
            188 => {
                afz_pc = 101;
                continue;
            },
            189 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_has_chroma_14 != 0)) @as(c_int, 188) else @as(c_int, 187)));
                continue;
            },
            190 => {
                afz_dsp_2.*.mc.blend.?(afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_tmp_26, afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), @as([*c]const u8, @ptrFromInt(@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks))) +% (@as(usize, @bitCast(@as(c_long, if (@as(c_int, b.*.unnamed_0.unnamed_1.interintra_type) == INTER_INTRA_BLEND) @as(c_int, antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][bs -% @as(enum_BlockSize, BS_32x32)].ii[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode]) else @as(c_int, antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][bs -% @as(enum_BlockSize, BS_32x32)].wedge[@as(c_int, 0)][b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx])))) *% @as(usize, 8)))));
                afz_pc = 189;
                continue;
            },
            191 => {
                afz_dsp_2.*.ipred.intra_pred[afz_m_25].?(afz_tmp_26, @bitCast(@as(c_ulong, @truncate(@as(c_ulong, @bitCast(@as(c_long, @as(c_int, 4) * afz_bw4_10))) *% @sizeOf(pixel)))), afz_tl_edge_24, afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), 0, 0, 0, afz_f_1.*.bitdepth_max);
                afz_pc = 190;
                continue;
            },
            192 => {
                afz_m_25 = antfly_av1_dav1d_prepare_intra_edges_16bpc(t.*.bx, @as(c_int, @intFromBool(t.*.bx > afz_ts_0.*.tiling.col_start)), t.*.by, @as(c_int, @intFromBool(t.*.by > afz_ts_0.*.tiling.row_start)), afz_ts_0.*.tiling.col_end, afz_ts_0.*.tiling.row_end, 0, afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_top_sb_edge_28, afz_m_25, &afz_angle_27, afz_bw4_10, afz_bh4_11, 0, afz_tl_edge_24, afz_f_1.*.bitdepth_max);
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_top_sb_edge_28 += @as(usize, @bitCast(@as(isize, @intCast((afz_f_1.*.sb128w * @as(c_int, 128)) * (afz_sby_29 - @as(c_int, 1))))));
                afz_pc = 192;
                continue;
            },
            194 => {
                afz_sby_29 = t.*.by >> @intCast(afz_f_1.*.sb_shift);
                afz_pc = 193;
                continue;
            },
            195 => {
                afz_top_sb_edge_28 = afz_f_1.*.ipred_edge[@as(c_int, 0)];
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_pc = @bitCast(@as(c_int, if (!((t.*.by & (afz_f_1.*.sb_step - @as(c_int, 1))) != 0)) @as(c_int, 195) else @as(c_int, 192)));
                continue;
            },
            197 => {
                afz_top_sb_edge_28 = null;
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_angle_27 = 0;
                afz_pc = 197;
                continue;
            },
            199 => {
                afz_tmp_26 = @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.interintra_16bpc));
                afz_pc = 198;
                continue;
            },
            200 => {
                afz_m_25 = @bitCast(if (@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode) == II_SMOOTH_PRED) SMOOTH_PRED else @as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.interintra_mode));
                afz_pc = 199;
                continue;
            },
            201 => {
                afz_tl_edge_24 = @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.edge_16bpc))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
                afz_pc = 200;
                continue;
            },
            202 => {
                afz_pc = @bitCast(@as(c_int, if (b.*.unnamed_0.unnamed_1.interintra_type != 0) @as(c_int, 201) else @as(c_int, 189)));
                continue;
            },
            203 => {
                return afz_res_16;
            },
            204 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 203) else @as(c_int, 202)));
                continue;
            },
            205 => {
                afz_res_16 = warp_affine(t, afz_dst_19, null, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_b_dim_9, 0, afz_refp_22, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) &t.*.warpmv else &afz_f_1.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]);
                afz_pc = 204;
                continue;
            },
            206 => {
                return afz_res_16;
            },
            207 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 206) else @as(c_int, 202)));
                continue;
            },
            208 => {
                afz_res_16 = obmc(t, afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_b_dim_9, 0, afz_bx4_3, afz_by4_4, afz_w4_12, afz_h4_13);
                afz_pc = 207;
                continue;
            },
            209 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_OBMC) @as(c_int, 208) else @as(c_int, 202)));
                continue;
            },
            210 => {
                return afz_res_16;
            },
            211 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 210) else @as(c_int, 209)));
                continue;
            },
            212 => {
                afz_res_16 = mc(t, afz_dst_19, null, afz_f_1.*.cur.stride[@as(c_int, 0)], afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by, 0, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@as(c_int, 0)], afz_refp_22, b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)], afz_filter_2d_23);
                afz_pc = 211;
                continue;
            },
            213 => {
                afz_pc = @bitCast(@as(c_int, if ((imin(afz_bw4_10, afz_bh4_11) > @as(c_int, 1)) and (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV) and (@as(c_int, afz_f_1.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))]) != 0)) or ((@as(c_int, b.*.unnamed_0.unnamed_1.motion_mode) == MM_WARP) and (t.*.warpmv.type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION))))) @as(c_int, 205) else @as(c_int, 212)));
                continue;
            },
            214 => {
                afz_filter_2d_23 = b.*.unnamed_0.unnamed_1.filter2d;
                afz_pc = 213;
                continue;
            },
            215 => {
                afz_refp_22 = &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))];
                afz_pc = 214;
                continue;
            },
            216 => {
                afz_pc = @bitCast(@as(c_int, if (afz_pl_58 < @as(c_int, 2)) @as(c_int, 236) else @as(c_int, 87)));
                continue;
            },
            217 => {
                afz_pl_58 += 1;
                afz_pc = 216;
                continue;
            },
            218 => {
                afz_pc = 217;
                continue;
            },
            219 => {
                afz_dsp_2.*.mc.mask.?(afz_uvdst_61, afz_f_1.*.cur.stride[@as(c_int, 1)], @ptrCast(@alignCast(&afz_tmp_52[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign])), @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign) != 0))))))])), (afz_bw4_10 * @as(c_int, 4)) >> @intCast(afz_ss_hor_6), (afz_bh4_11 * @as(c_int, 4)) >> @intCast(afz_ss_ver_5), afz_mask_55, afz_f_1.*.bitdepth_max);
                afz_pc = 218;
                continue;
            },
            220 => {
                afz_pc = 217;
                continue;
            },
            221 => {
                afz_dsp_2.*.mc.w_avg.?(afz_uvdst_61, afz_f_1.*.cur.stride[@as(c_int, 1)], @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 0)])), @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 1)])), (afz_bw4_10 * @as(c_int, 4)) >> @intCast(afz_ss_hor_6), (afz_bh4_11 * @as(c_int, 4)) >> @intCast(afz_ss_ver_5), afz_jnt_weight_53, afz_f_1.*.bitdepth_max);
                afz_pc = 220;
                continue;
            },
            222 => {
                afz_pc = 217;
                continue;
            },
            223 => {
                afz_dsp_2.*.mc.avg.?(afz_uvdst_61, afz_f_1.*.cur.stride[@as(c_int, 1)], @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 0)])), @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 1)])), (afz_bw4_10 * @as(c_int, 4)) >> @intCast(afz_ss_hor_6), (afz_bh4_11 * @as(c_int, 4)) >> @intCast(afz_ss_ver_5), afz_f_1.*.bitdepth_max);
                afz_pc = 222;
                continue;
            },
            224 => {
                blk: {
                    switch (@as(c_int, b.*.unnamed_0.unnamed_1.comp_type)) {
                        COMP_INTER_SEG => {
                            afz_pc = 219;
                            continue;
                        },
                        COMP_INTER_WEDGE => {
                            afz_pc = 219;
                            continue;
                        },
                        COMP_INTER_WEIGHTED_AVG => {
                            afz_pc = 221;
                            continue;
                        },
                        COMP_INTER_AVG => {
                            afz_pc = 223;
                            continue;
                        },
                        else => {
                            afz_pc = 217;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            225 => {
                afz_uvdst_61 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + afz_pl_58)))]))) + @as(usize, @bitCast(@as(isize, @intCast(afz_uvdstoff_20))));
                afz_pc = 224;
                continue;
            },
            226 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_59 < @as(c_int, 2)) @as(c_int, 235) else @as(c_int, 225)));
                continue;
            },
            227 => {
                afz_i_59 += 1;
                afz_pc = 226;
                continue;
            },
            228 => {
                return afz_res_16;
            },
            229 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 228) else @as(c_int, 227)));
                continue;
            },
            230 => {
                afz_res_16 = warp_affine(t, null, @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(afz_i_59)))])), (afz_bw4_10 * @as(c_int, 4)) >> @intCast(afz_ss_hor_6), afz_b_dim_9, @as(c_int, 1) + afz_pl_58, afz_refp_60, &afz_f_1.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_59)))])))]);
                afz_pc = 229;
                continue;
            },
            231 => {
                return afz_res_16;
            },
            232 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 231) else @as(c_int, 227)));
                continue;
            },
            233 => {
                afz_res_16 = mc(t, null, @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(afz_i_59)))])), 0, afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by, @as(c_int, 1) + afz_pl_58, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@bitCast(@as(isize, @intCast(afz_i_59)))], afz_refp_60, b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_59)))], afz_filter_2d_51);
                afz_pc = 232;
                continue;
            },
            234 => {
                afz_pc = @bitCast(@as(c_int, if (((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV_GLOBALMV) and (imin(afz_cbw4_18, afz_cbh4_17) > @as(c_int, 1))) and (@as(c_int, afz_f_1.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_59)))])))]) != 0)) @as(c_int, 230) else @as(c_int, 233)));
                continue;
            },
            235 => {
                afz_refp_60 = &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_59)))])))];
                afz_pc = 234;
                continue;
            },
            236 => {
                afz_i_59 = 0;
                afz_pc = 226;
                continue;
            },
            237 => {
                afz_pl_58 = 0;
                afz_pc = 216;
                continue;
            },
            238 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 237) else @as(c_int, 87)));
                continue;
            },
            239 => {
                afz_pc = 238;
                continue;
            },
            240 => {
                afz_mask_55 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrFromInt(@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks))) +% (@as(usize, antfly_av1_dav1d_masks.offsets[@bitCast(@as(isize, @intCast(afz_chr_layout_idx_15)))][bs -% @as(enum_BlockSize, BS_32x32)].wedge[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign][b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx]) *% @as(usize, 8)))))));
                afz_pc = 239;
                continue;
            },
            241 => {
                afz_pc = @bitCast(@as(c_int, if (afz_has_chroma_14 != 0) @as(c_int, 240) else @as(c_int, 239)));
                continue;
            },
            242 => {
                afz_dsp_2.*.mc.mask.?(afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], @ptrCast(@alignCast(&afz_tmp_52[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign])), @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign) != 0))))))])), afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), afz_mask_55, afz_f_1.*.bitdepth_max);
                afz_pc = 241;
                continue;
            },
            243 => {
                afz_mask_55 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrFromInt(@as(usize, @intCast(@intFromPtr(&antfly_av1_dav1d_masks))) +% (@as(usize, antfly_av1_dav1d_masks.offsets[@as(c_int, 0)][bs -% @as(enum_BlockSize, BS_32x32)].wedge[@as(c_int, 0)][b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.wedge_idx]) *% @as(usize, 8)))))));
                afz_pc = 242;
                continue;
            },
            244 => {
                afz_pc = 238;
                continue;
            },
            245 => {
                afz_mask_55 = afz_seg_mask_54;
                afz_pc = 244;
                continue;
            },
            246 => {
                afz_dsp_2.*.mc.w_mask[@bitCast(@as(isize, @intCast(afz_chr_layout_idx_15)))].?(afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], @ptrCast(@alignCast(&afz_tmp_52[b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign])), @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!(@as(c_int, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign) != 0))))))])), afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), afz_seg_mask_54, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mask_sign, afz_f_1.*.bitdepth_max);
                afz_pc = 245;
                continue;
            },
            247 => {
                afz_pc = 238;
                continue;
            },
            248 => {
                afz_dsp_2.*.mc.w_avg.?(afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 0)])), @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 1)])), afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), afz_jnt_weight_53, afz_f_1.*.bitdepth_max);
                afz_pc = 247;
                continue;
            },
            249 => {
                afz_jnt_weight_53 = afz_f_1.*.jnt_weights[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 0)])))][@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@as(c_int, 1)])))];
                afz_pc = 248;
                continue;
            },
            250 => {
                afz_pc = 238;
                continue;
            },
            251 => {
                afz_dsp_2.*.mc.avg.?(afz_dst_19, afz_f_1.*.cur.stride[@as(c_int, 0)], @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 0)])), @ptrCast(@alignCast(&afz_tmp_52[@as(c_int, 1)])), afz_bw4_10 * @as(c_int, 4), afz_bh4_11 * @as(c_int, 4), afz_f_1.*.bitdepth_max);
                afz_pc = 250;
                continue;
            },
            252 => {
                blk: {
                    switch (@as(c_int, b.*.unnamed_0.unnamed_1.comp_type)) {
                        COMP_INTER_WEDGE => {
                            afz_pc = 243;
                            continue;
                        },
                        COMP_INTER_SEG => {
                            afz_pc = 246;
                            continue;
                        },
                        COMP_INTER_WEIGHTED_AVG => {
                            afz_pc = 249;
                            continue;
                        },
                        COMP_INTER_AVG => {
                            afz_pc = 251;
                            continue;
                        },
                        else => {
                            afz_pc = 238;
                            break :blk;
                        },
                    }
                }
                continue;
            },
            253 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_56 < @as(c_int, 2)) @as(c_int, 262) else @as(c_int, 252)));
                continue;
            },
            254 => {
                afz_i_56 += 1;
                afz_pc = 253;
                continue;
            },
            255 => {
                return afz_res_16;
            },
            256 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 255) else @as(c_int, 254)));
                continue;
            },
            257 => {
                afz_res_16 = warp_affine(t, null, @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(afz_i_56)))])), afz_bw4_10 * @as(c_int, 4), afz_b_dim_9, 0, afz_refp_57, &afz_f_1.*.frame_hdr.*.gmv[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_56)))])))]);
                afz_pc = 256;
                continue;
            },
            258 => {
                return afz_res_16;
            },
            259 => {
                afz_pc = @bitCast(@as(c_int, if (afz_res_16 != 0) @as(c_int, 258) else @as(c_int, 254)));
                continue;
            },
            260 => {
                afz_res_16 = mc(t, null, @ptrCast(@alignCast(&afz_tmp_52[@bitCast(@as(isize, @intCast(afz_i_56)))])), 0, afz_bw4_10, afz_bh4_11, t.*.bx, t.*.by, 0, b.*.unnamed_0.unnamed_1.unnamed_0.unnamed_0.mv[@bitCast(@as(isize, @intCast(afz_i_56)))], afz_refp_57, b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_56)))], afz_filter_2d_51);
                afz_pc = 259;
                continue;
            },
            261 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(c_int, b.*.unnamed_0.unnamed_1.inter_mode) == GLOBALMV_GLOBALMV) and (@as(c_int, afz_f_1.*.gmv_warp_allowed[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_56)))])))]) != 0)) @as(c_int, 257) else @as(c_int, 260)));
                continue;
            },
            262 => {
                afz_refp_57 = &afz_f_1.*.refp[@bitCast(@as(isize, @intCast(b.*.unnamed_0.unnamed_1.ref[@bitCast(@as(isize, @intCast(afz_i_56)))])))];
                afz_pc = 261;
                continue;
            },
            263 => {
                afz_i_56 = 0;
                afz_pc = 253;
                continue;
            },
            264 => {
                afz_pc = 263;
                continue;
            },
            265 => {
                afz_seg_mask_54 = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_0.unnamed_0.seg_mask));
                afz_pc = 264;
                continue;
            },
            266 => {
                afz_pc = 265;
                continue;
            },
            267 => {
                afz_tmp_52 = @ptrCast(@alignCast(&t.*.scratch.unnamed_0.unnamed_0.unnamed_0.compinter));
                afz_pc = 266;
                continue;
            },
            268 => {
                afz_filter_2d_51 = b.*.unnamed_0.unnamed_1.filter2d;
                afz_pc = 267;
                continue;
            },
            269 => {
                afz_pc = @bitCast(@as(c_int, if (@as(c_int, b.*.unnamed_0.unnamed_1.comp_type) == COMP_INTER_NONE) @as(c_int, 215) else @as(c_int, 268)));
                continue;
            },
            270 => {
                afz_pc = @bitCast(@as(c_int, if (!((afz_f_1.*.frame_hdr.*.frame_type & @as(enum_Dav1dFrameType, 1)) != 0)) @as(c_int, 100) else @as(c_int, 269)));
                continue;
            },
            271 => {
                afz_uvdstoff_20 = @as(ptrdiff_t, 4) * (@as(ptrdiff_t, t.*.bx >> @intCast(afz_ss_hor_6)) + (@as(ptrdiff_t, t.*.by >> @intCast(afz_ss_ver_5)) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 1)])));
                afz_pc = 270;
                continue;
            },
            272 => {
                afz_dst_19 = @as([*c]pixel, @ptrCast(@alignCast(afz_f_1.*.cur.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * ((@as(ptrdiff_t, t.*.by) * PXSTRIDE(afz_f_1.*.cur.stride[@as(c_int, 0)])) + @as(ptrdiff_t, t.*.bx))))));
                afz_pc = 271;
                continue;
            },
            273 => {
                afz_cbh4_17 = (afz_bh4_11 + afz_ss_ver_5) >> @intCast(afz_ss_ver_5);
                afz_cbw4_18 = (afz_bw4_10 + afz_ss_hor_6) >> @intCast(afz_ss_hor_6);
                afz_pc = 272;
                continue;
            },
            274 => {
                afz_pc = 273;
                continue;
            },
            275 => {
                afz_chr_layout_idx_15 = if (afz_f_1.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) @as(c_int, 0) else DAV1D_PIXEL_LAYOUT_I444 - @as(c_int, @bitCast(afz_f_1.*.cur.p.layout));
                afz_pc = 274;
                continue;
            },
            276 => {
                afz_has_chroma_14 = @as(c_int, @intFromBool(((afz_f_1.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) and ((afz_bw4_10 > afz_ss_hor_6) or ((t.*.bx & @as(c_int, 1)) != 0))) and ((afz_bh4_11 > afz_ss_ver_5) or ((t.*.by & @as(c_int, 1)) != 0))));
                afz_pc = 275;
                continue;
            },
            277 => {
                afz_w4_12 = imin(afz_bw4_10, afz_f_1.*.bw - t.*.bx);
                afz_h4_13 = imin(afz_bh4_11, afz_f_1.*.bh - t.*.by);
                afz_pc = 276;
                continue;
            },
            278 => {
                afz_bw4_10 = afz_b_dim_9[@as(c_int, 0)];
                afz_bh4_11 = afz_b_dim_9[@as(c_int, 1)];
                afz_pc = 277;
                continue;
            },
            279 => {
                afz_b_dim_9 = @ptrCast(@alignCast(@constCast(@as([*c]const u8, @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]))))));
                afz_pc = 278;
                continue;
            },
            280 => {
                afz_cbx4_7 = afz_bx4_3 >> @intCast(afz_ss_hor_6);
                afz_cby4_8 = afz_by4_4 >> @intCast(afz_ss_ver_5);
                afz_pc = 279;
                continue;
            },
            281 => {
                afz_ss_hor_6 = @as(c_int, @intFromBool(afz_f_1.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
                afz_pc = 280;
                continue;
            },
            282 => {
                afz_ss_ver_5 = @as(c_int, @intFromBool(afz_f_1.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
                afz_pc = 281;
                continue;
            },
            283 => {
                afz_bx4_3 = t.*.bx & @as(c_int, 31);
                afz_by4_4 = t.*.by & @as(c_int, 31);
                afz_pc = 282;
                continue;
            },
            284 => {
                afz_dsp_2 = @ptrCast(@alignCast(@constCast(afz_f_1.*.dsp)));
                afz_pc = 283;
                continue;
            },
            285 => {
                afz_f_1 = @ptrCast(@alignCast(@constCast(t.*.f)));
                afz_pc = 284;
                continue;
            },
            286 => {
                afz_ts_0 = t.*.ts;
                afz_pc = 285;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_filter_sbrow_deblock_cols_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void {
    _ = &f;
    _ = &sby;
    if (!((f.*.c.*.inloop_filters & @as(enum_Dav1dInloopFilterType, DAV1D_INLOOPFILTER_DEBLOCK)) != 0) or (!(@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) and !(@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0))) {
        return;
    }
    const y: c_int = (sby * f.*.sb_step) * @as(c_int, 4);
    _ = &y;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const p: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)]))))),
        f.*.lf.p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &p;
    var mask: [*c]Av1Filter = f.*.lf.mask + @as(usize, @bitCast(@as(isize, @intCast((sby >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))))) * f.*.sb128w))));
    _ = &mask;
    antfly_av1_dav1d_loopfilter_sbrow_cols_16bpc(f, @ptrCast(@alignCast(&p)), mask, sby, f.*.lf.start_of_tile_row[@bitCast(@as(isize, @intCast(sby)))]);
}
pub export fn antfly_av1_dav1d_filter_sbrow_deblock_rows_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void {
    _ = &f;
    _ = &sby;
    const y: c_int = (sby * f.*.sb_step) * @as(c_int, 4);
    _ = &y;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const p: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)]))))),
        f.*.lf.p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &p;
    var mask: [*c]Av1Filter = f.*.lf.mask + @as(usize, @bitCast(@as(isize, @intCast((sby >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))))) * f.*.sb128w))));
    _ = &mask;
    if (((f.*.c.*.inloop_filters & @as(enum_Dav1dInloopFilterType, DAV1D_INLOOPFILTER_DEBLOCK)) != 0) and (@as(enum_Dav1dInloopFilterType, @intCast(@as(c_int, @intFromBool((@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) or (@as(c_int, f.*.frame_hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0))))) != 0)) {
        antfly_av1_dav1d_loopfilter_sbrow_rows_16bpc(f, @ptrCast(@alignCast(&p)), mask, sby);
    }
    if ((@as(c_int, f.*.seq_hdr.*.cdef) != 0) or (f.*.lf.restore_planes != 0)) {
        antfly_av1_dav1d_copy_lpf_16bpc(f, @ptrCast(@alignCast(&p)), sby);
    }
}
pub export fn antfly_av1_dav1d_filter_sbrow_cdef_16bpc(tc: [*c]Dav1dTaskContext, sby: c_int) void {
    _ = &tc;
    _ = &sby;
    const f: [*c]const Dav1dFrameContext = tc.*.f;
    _ = &f;
    if (!((f.*.c.*.inloop_filters & @as(enum_Dav1dInloopFilterType, DAV1D_INLOOPFILTER_CDEF)) != 0)) return;
    const sbsz: c_int = f.*.sb_step;
    _ = &sbsz;
    const y: c_int = (sby * sbsz) * @as(c_int, 4);
    _ = &y;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const p: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)]))))),
        f.*.lf.p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &p;
    var prev_mask: [*c]Av1Filter = f.*.lf.mask + @as(usize, @bitCast(@as(isize, @intCast(((sby - @as(c_int, 1)) >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))))) * f.*.sb128w))));
    _ = &prev_mask;
    var mask: [*c]Av1Filter = f.*.lf.mask + @as(usize, @bitCast(@as(isize, @intCast((sby >> @intCast(@as(c_int, @intFromBool(!(@as(c_int, f.*.seq_hdr.*.sb128) != 0))))) * f.*.sb128w))));
    _ = &mask;
    const start: c_int = sby * sbsz;
    _ = &start;
    if (sby != 0) {
        const ss_ver_1: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver_1;
        var p_up: [3][*c]pixel = [3][*c]pixel{
            p[@as(c_int, 0)] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)]))))),
            p[@as(c_int, 1)] - @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, 8) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver_1)))))),
            p[@as(c_int, 2)] - @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, 8) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver_1)))))),
        };
        _ = &p_up;
        antfly_av1_dav1d_cdef_brow_16bpc(tc, @ptrCast(@alignCast(&p_up)), prev_mask, start - @as(c_int, 2), start, 1, sby);
    }
    const n_blks: c_int = sbsz - (@as(c_int, 2) * @as(c_int, @intFromBool((sby + @as(c_int, 1)) < f.*.sbh)));
    _ = &n_blks;
    const end: c_int = imin(start + n_blks, f.*.bh);
    _ = &end;
    antfly_av1_dav1d_cdef_brow_16bpc(tc, @ptrCast(@alignCast(&p)), mask, start, end, 0, sby);
}
pub export fn antfly_av1_dav1d_filter_sbrow_resize_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void {
    _ = &f;
    _ = &sby;
    const sbsz: c_int = f.*.sb_step;
    _ = &sbsz;
    const y: c_int = (sby * sbsz) * @as(c_int, 4);
    _ = &y;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const p: [3][*c]const pixel = [3][*c]const pixel{
        f.*.lf.p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)]))))),
        f.*.lf.p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &p;
    const sr_p: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.sr_p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 0)]))))),
        f.*.lf.sr_p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.sr_p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &sr_p;
    const has_chroma: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)));
    _ = &has_chroma;
    {
        var pl: c_int = 0;
        _ = &pl;
        while (pl < (@as(c_int, 1) + (@as(c_int, 2) * has_chroma))) : (pl += 1) {
            const ss_ver_1: c_int = @as(c_int, @intFromBool((pl != 0) and (f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420))));
            _ = &ss_ver_1;
            const h_start: c_int = (@as(c_int, 8) * @as(c_int, @intFromBool(!!(sby != 0)))) >> @intCast(ss_ver_1);
            _ = &h_start;
            const dst_stride: ptrdiff_t = f.*.sr_cur.p.stride[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))];
            _ = &dst_stride;
            var dst: [*c]pixel = sr_p[@bitCast(@as(isize, @intCast(pl)))] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, h_start) * PXSTRIDE(dst_stride)))));
            _ = &dst;
            const src_stride: ptrdiff_t = f.*.cur.stride[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))];
            _ = &src_stride;
            var src: [*c]const pixel = p[@bitCast(@as(isize, @intCast(pl)))] - @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, h_start) * PXSTRIDE(src_stride)))));
            _ = &src;
            const h_end: c_int = (@as(c_int, 4) * (sbsz - (@as(c_int, 2) * @as(c_int, @intFromBool((sby + @as(c_int, 1)) < f.*.sbh))))) >> @intCast(ss_ver_1);
            _ = &h_end;
            const ss_hor: c_int = @as(c_int, @intFromBool((pl != 0) and (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444))));
            _ = &ss_hor;
            const dst_w: c_int = (f.*.sr_cur.p.p.w + ss_hor) >> @intCast(ss_hor);
            _ = &dst_w;
            const src_w: c_int = ((@as(c_int, 4) * f.*.bw) + ss_hor) >> @intCast(ss_hor);
            _ = &src_w;
            const img_h: c_int = ((f.*.cur.p.h - ((sbsz * @as(c_int, 4)) * sby)) + ss_ver_1) >> @intCast(ss_ver_1);
            _ = &img_h;
            f.*.dsp.*.mc.resize.?(dst, dst_stride, src, src_stride, dst_w, imin(img_h, h_end) + h_start, src_w, f.*.resize_step[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))], f.*.resize_start[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(!!(pl != 0))))))], f.*.bitdepth_max);
        }
    }
}
pub export fn antfly_av1_dav1d_filter_sbrow_lr_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void {
    _ = &f;
    _ = &sby;
    if (!((f.*.c.*.inloop_filters & @as(enum_Dav1dInloopFilterType, DAV1D_INLOOPFILTER_RESTORATION)) != 0)) return;
    const y: c_int = (sby * f.*.sb_step) * @as(c_int, 4);
    _ = &y;
    const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const sr_p: [3][*c]pixel = [3][*c]pixel{
        f.*.lf.sr_p[@as(c_int, 0)] + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 0)]))))),
        f.*.lf.sr_p[@as(c_int, 1)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
        f.*.lf.sr_p[@as(c_int, 2)] + @as(usize, @bitCast(@as(isize, @intCast((@as(ptrdiff_t, y) * PXSTRIDE(f.*.sr_cur.p.stride[@as(c_int, 1)])) >> @intCast(@as(ptrdiff_t, ss_ver)))))),
    };
    _ = &sr_p;
    antfly_av1_dav1d_lr_sbrow_16bpc(f, @ptrCast(@alignCast(&sr_p)), sby);
}
pub export fn antfly_av1_dav1d_filter_sbrow_16bpc(f: [*c]Dav1dFrameContext, sby: c_int) void {
    _ = &f;
    _ = &sby;
    antfly_av1_dav1d_filter_sbrow_deblock_cols_16bpc(f, sby);
    antfly_av1_dav1d_filter_sbrow_deblock_rows_16bpc(f, sby);
    if (@as(c_int, f.*.seq_hdr.*.cdef) != 0) {
        antfly_av1_dav1d_filter_sbrow_cdef_16bpc(f.*.c.*.tc, sby);
    }
    if (f.*.frame_hdr.*.width[@as(c_int, 0)] != f.*.frame_hdr.*.width[@as(c_int, 1)]) {
        antfly_av1_dav1d_filter_sbrow_resize_16bpc(f, sby);
    }
    if (f.*.lf.restore_planes != 0) {
        antfly_av1_dav1d_filter_sbrow_lr_16bpc(f, sby);
    }
}
pub export fn antfly_av1_dav1d_backup_ipred_edge_16bpc(t: [*c]Dav1dTaskContext) void {
    _ = &t;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const sby: c_int = t.*.by >> @intCast(f.*.sb_shift);
    _ = &sby;
    const sby_off: c_int = (f.*.sb128w * @as(c_int, 128)) * sby;
    _ = &sby_off;
    const x_off: c_int = ts.*.tiling.col_start;
    _ = &x_off;
    const y: [*c]const pixel = (@as([*c]const pixel, @ptrCast(@alignCast(f.*.cur.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(x_off * @as(c_int, 4)))))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, ((t.*.by + f.*.sb_step) * @as(c_int, 4)) - @as(c_int, 1)) * PXSTRIDE(f.*.cur.stride[@as(c_int, 0)])))));
    _ = &y;
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&f.*.ipred_edge[@as(c_int, 0)][@bitCast(@as(isize, @intCast(sby_off + (x_off * @as(c_int, 4)))))])), @ptrCast(@alignCast(y)), @bitCast(@as(c_long, (@as(c_int, 4) * (ts.*.tiling.col_end - x_off)) << @intCast(@as(c_int, 1)))));
    if (f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) {
        const ss_ver: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver;
        const ss_hor: c_int = @as(c_int, @intFromBool(f.*.cur.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
        _ = &ss_hor;
        const uv_off: ptrdiff_t = @as(ptrdiff_t, (x_off * @as(c_int, 4)) >> @intCast(ss_hor)) + (@as(ptrdiff_t, (((t.*.by + f.*.sb_step) * @as(c_int, 4)) >> @intCast(ss_ver)) - @as(c_int, 1)) * PXSTRIDE(f.*.cur.stride[@as(c_int, 1)]));
        _ = &uv_off;
        {
            var pl: c_int = 1;
            _ = &pl;
            while (pl <= @as(c_int, 2)) : (pl += 1) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&f.*.ipred_edge[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(sby_off + ((x_off * @as(c_int, 4)) >> @intCast(ss_hor)))))])), @ptrCast(@alignCast(&@as([*c]const pixel, @ptrCast(@alignCast(f.*.cur.data[@bitCast(@as(isize, @intCast(pl)))])))[@bitCast(@as(isize, @intCast(uv_off)))])), @bitCast(@as(c_long, ((@as(c_int, 4) * (ts.*.tiling.col_end - x_off)) >> @intCast(ss_hor)) << @intCast(@as(c_int, 1)))));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_copy_pal_block_y_16bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void {
    _ = &t;
    _ = &bx4;
    _ = &by4;
    _ = &bw4;
    _ = &bh4;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const pal: [*c]pixel = if (t.*.frame_thread.pass != 0) @as([*c]pixel, @ptrCast(@alignCast(&f.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (f.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))][@as(c_int, 0)]))) else @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc[@as(c_int, 0)])));
    _ = &pal;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < bw4) : (x += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4 + x)))][@as(c_int, 0)]))))), @ptrCast(@alignCast(pal)), @as(c_ulong, 8) *% @sizeOf(pixel));
        }
    }
    {
        var y: c_int = 0;
        _ = &y;
        while (y < bh4) : (y += 1) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4 + y)))][@as(c_int, 0)]))))), @ptrCast(@alignCast(pal)), @as(c_ulong, 8) *% @sizeOf(pixel));
        }
    }
}
pub export fn antfly_av1_dav1d_copy_pal_block_uv_16bpc(t: [*c]Dav1dTaskContext, bx4: c_int, by4: c_int, bw4: c_int, bh4: c_int) void {
    _ = &t;
    _ = &bx4;
    _ = &by4;
    _ = &bw4;
    _ = &bh4;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const pal: [*c][8]pixel = if (t.*.frame_thread.pass != 0) @as([*c][8]pixel, @ptrCast(@alignCast(&f.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (f.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))]))) else @as([*c][8]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc)));
    _ = &pal;
    {
        var pl: c_int = 1;
        _ = &pl;
        while (pl <= @as(c_int, 2)) : (pl += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < bw4) : (x += 1) {
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4 + x)))][@bitCast(@as(isize, @intCast(pl)))]))))), @ptrCast(@alignCast(@as([*c]const pixel, @ptrCast(@alignCast(&pal[@bitCast(@as(isize, @intCast(pl)))]))))), @as(c_ulong, 8) *% @sizeOf(pixel));
                }
            }
            {
                var y: c_int = 0;
                _ = &y;
                while (y < bh4) : (y += 1) {
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4 + y)))][@bitCast(@as(isize, @intCast(pl)))]))))), @ptrCast(@alignCast(@as([*c]const pixel, @ptrCast(@alignCast(&pal[@bitCast(@as(isize, @intCast(pl)))]))))), @as(c_ulong, 8) *% @sizeOf(pixel));
                }
            }
        }
    }
}
pub export fn antfly_av1_dav1d_read_pal_plane_16bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, pl: c_int, sz_ctx: c_int, bx4: c_int, by4: c_int) void {
    _ = &t;
    _ = &b;
    _ = &pl;
    _ = &sz_ctx;
    _ = &bx4;
    _ = &by4;
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const pal_sz: c_int = blk: {
        const tmp = @as(u8, @truncate(antfly_av1_dav1d_msac_decode_symbol_adapt_c(&ts.*.msac, @ptrCast(@alignCast(&ts.*.cdf.m.pal_sz[@bitCast(@as(isize, @intCast(pl)))][@bitCast(@as(isize, @intCast(sz_ctx)))])), 6) +% @as(c_uint, 2)));
        b.*.unnamed_0.unnamed_0.pal_sz[@bitCast(@as(isize, @intCast(pl)))] = tmp;
        break :blk tmp;
    };
    _ = &pal_sz;
    var cache: [16]pixel = undefined;
    _ = &cache;
    var used_cache: [8]pixel = undefined;
    _ = &used_cache;
    var l_cache: c_int = if (pl != 0) @as(c_int, t.*.pal_sz_uv[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4)))]) else @as(c_int, t.*.l.pal_sz[@bitCast(@as(isize, @intCast(by4)))]);
    _ = &l_cache;
    var n_cache: c_int = 0;
    _ = &n_cache;
    var a_cache: c_int = if ((by4 & @as(c_int, 15)) != 0) if (pl != 0) @as(c_int, t.*.pal_sz_uv[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))]) else @as(c_int, t.*.a.*.pal_sz[@bitCast(@as(isize, @intCast(bx4)))]) else @as(c_int, 0);
    _ = &a_cache;
    var l: [*c]const pixel = @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4)))][@bitCast(@as(isize, @intCast(pl)))]));
    _ = &l;
    var a: [*c]const pixel = @ptrCast(@alignCast(&t.*.unnamed_1.al_pal_16bpc[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))][@bitCast(@as(isize, @intCast(pl)))]));
    _ = &a;
    while ((l_cache != 0) and (a_cache != 0)) {
        if (@as(c_int, l.*) < @as(c_int, a.*)) {
            if (!(n_cache != 0) or (@as(c_int, cache[@bitCast(@as(isize, @intCast(n_cache - @as(c_int, 1))))]) != @as(c_int, l.*))) {
                cache[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &n_cache;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = l.*;
            }
            l += 1;
            l_cache -= 1;
        } else {
            if (@as(c_int, a.*) == @as(c_int, l.*)) {
                l += 1;
                l_cache -= 1;
            }
            if (!(n_cache != 0) or (@as(c_int, cache[@bitCast(@as(isize, @intCast(n_cache - @as(c_int, 1))))]) != @as(c_int, a.*))) {
                cache[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &n_cache;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = a.*;
            }
            a += 1;
            a_cache -= 1;
        }
    }
    if (l_cache != 0) {
        while (true) {
            if (!(n_cache != 0) or (@as(c_int, cache[@bitCast(@as(isize, @intCast(n_cache - @as(c_int, 1))))]) != @as(c_int, l.*))) {
                cache[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &n_cache;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = l.*;
            }
            l += 1;
            if (!((blk: {
                const ref = &l_cache;
                ref.* -= 1;
                break :blk ref.*;
            }) > @as(c_int, 0))) break;
        }
    } else if (a_cache != 0) {
        while (true) {
            if (!(n_cache != 0) or (@as(c_int, cache[@bitCast(@as(isize, @intCast(n_cache - @as(c_int, 1))))]) != @as(c_int, a.*))) {
                cache[
                    @bitCast(@as(isize, @intCast(blk: {
                        const ref = &n_cache;
                        const tmp = ref.*;
                        ref.* += 1;
                        break :blk tmp;
                    })))
                ] = a.*;
            }
            a += 1;
            if (!((blk: {
                const ref = &a_cache;
                ref.* -= 1;
                break :blk ref.*;
            }) > @as(c_int, 0))) break;
        }
    }
    var i: c_int = 0;
    _ = &i;
    {
        var n: c_int = 0;
        _ = &n;
        while ((n < n_cache) and (i < pal_sz)) : (n += 1) if (antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac) != 0) {
            used_cache[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &i;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = cache[@bitCast(@as(isize, @intCast(n)))];
        };
    }
    const n_used_cache: c_int = i;
    _ = &n_used_cache;
    const pal: [*c]pixel = if (t.*.frame_thread.pass != 0) @as([*c]pixel, @ptrCast(@alignCast(&f.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (f.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))][@bitCast(@as(isize, @intCast(pl)))]))) else @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc[@bitCast(@as(isize, @intCast(pl)))])));
    _ = &pal;
    if (i < pal_sz) {
        const bpc: c_int = if (@as(c_int, 16) == @as(c_int, 8)) @as(c_int, 8) else f.*.cur.p.bpc;
        _ = &bpc;
        var prev: c_int = blk: {
            const tmp = @as(pixel, @truncate(dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, bpc)))));
            pal[
                @bitCast(@as(isize, @intCast(blk_1: {
                    const ref = &i;
                    const tmp_2 = ref.*;
                    ref.* += 1;
                    break :blk_1 tmp_2;
                })))
            ] = tmp;
            break :blk tmp;
        };
        _ = &prev;
        if (i < pal_sz) {
            var bits: c_int = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, bpc - @as(c_int, 3)))) +% dav1d_msac_decode_bools(&ts.*.msac, 2))));
            _ = &bits;
            const max: c_int = (@as(c_int, 1) << @intCast(bpc)) - @as(c_int, 1);
            _ = &max;
            while (true) {
                const delta: c_int = @bitCast(@as(c_uint, @truncate(dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, bits))))));
                _ = &delta;
                prev = blk: {
                    const tmp = @as(pixel, @bitCast(@as(c_short, @truncate(imin((prev + delta) + @as(c_int, @intFromBool(!(pl != 0))), max)))));
                    pal[
                        @bitCast(@as(isize, @intCast(blk_1: {
                            const ref = &i;
                            const tmp_2 = ref.*;
                            ref.* += 1;
                            break :blk_1 tmp_2;
                        })))
                    ] = tmp;
                    break :blk tmp;
                };
                if ((prev + @as(c_int, @intFromBool(!(pl != 0)))) >= max) {
                    while (i < pal_sz) : (i += 1) {
                        pal[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(c_short, @truncate(max)));
                    }
                    break;
                }
                bits = imin(bits, @as(c_int, 1) + ulog2(@bitCast(@as(c_int, (max - prev) - @as(c_int, @intFromBool(!(pl != 0)))))));
                if (!(i < pal_sz)) break;
            }
        }
        var n: c_int = 0;
        _ = &n;
        var m: c_int = n_used_cache;
        _ = &m;
        {
            i = 0;
            while (i < pal_sz) : (i += 1) {
                if ((n < n_used_cache) and ((m >= pal_sz) or (@as(c_int, used_cache[@bitCast(@as(isize, @intCast(n)))]) <= @as(c_int, pal[@bitCast(@as(isize, @intCast(m)))])))) {
                    pal[@bitCast(@as(isize, @intCast(i)))] = used_cache[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref = &n;
                            const tmp = ref.*;
                            ref.* += 1;
                            break :blk tmp;
                        })))
                    ];
                } else {
                    while (true) {
                        if (!(m < pal_sz)) {
                            unreachable;
                        }
                        if (!false) break;
                    }
                    pal[@bitCast(@as(isize, @intCast(i)))] = pal[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref = &m;
                            const tmp = ref.*;
                            ref.* += 1;
                            break :blk tmp;
                        })))
                    ];
                }
            }
        }
    } else {
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(pal)), @ptrCast(@alignCast(@as([*c]pixel, @ptrCast(@alignCast(&used_cache))))), @as(c_ulong, @bitCast(@as(c_long, n_used_cache))) *% @sizeOf(@TypeOf(@as([*c]pixel, @ptrCast(@alignCast(&used_cache))).*)));
    }
    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
        _ = @as(c_int, 0);
        {
            var n: c_int = 0;
            _ = &n;
            while (n < n_cache) : (n += 1) {
                _ = @as(c_int, 0);
            }
        }
        _ = @as(c_int, 0);
        {
            var n: c_int = 0;
            _ = &n;
            while (n < pal_sz) : (n += 1) {
                _ = @as(c_int, 0);
            }
        }
        _ = @as(c_int, 0);
    }
}
pub export fn antfly_av1_dav1d_read_pal_uv_16bpc(t: [*c]Dav1dTaskContext, b: [*c]Av1Block, sz_ctx: c_int, bx4: c_int, by4: c_int) void {
    _ = &t;
    _ = &b;
    _ = &sz_ctx;
    _ = &bx4;
    _ = &by4;
    antfly_av1_dav1d_read_pal_plane_16bpc(t, b, 1, sz_ctx, bx4, by4);
    const ts: [*c]Dav1dTileState = t.*.ts;
    _ = &ts;
    const f: [*c]const Dav1dFrameContext = t.*.f;
    _ = &f;
    const pal: [*c]pixel = if (t.*.frame_thread.pass != 0) @as([*c]pixel, @ptrCast(@alignCast(&f.*.frame_thread.pal[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, (t.*.by >> @intCast(@as(c_int, 1))) + (t.*.bx & @as(c_int, 1))) * (f.*.b4_stride >> @intCast(@as(ptrdiff_t, 1)))) + @as(ptrdiff_t, (t.*.bx >> @intCast(@as(c_int, 1))) + (t.*.by & @as(c_int, 1))))))][@as(c_int, 2)]))) else @as([*c]u16, @ptrCast(@alignCast(&t.*.scratch.unnamed_1.unnamed_2.unnamed_1.pal_16bpc[@as(c_int, 2)])));
    _ = &pal;
    const bpc: c_int = if (@as(c_int, 16) == @as(c_int, 8)) @as(c_int, 8) else f.*.cur.p.bpc;
    _ = &bpc;
    if (antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac) != 0) {
        const bits: c_int = @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, bpc - @as(c_int, 4)))) +% dav1d_msac_decode_bools(&ts.*.msac, 2))));
        _ = &bits;
        var prev: c_int = blk: {
            const tmp = @as(pixel, @truncate(dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, bpc)))));
            pal[@as(c_int, 0)] = tmp;
            break :blk tmp;
        };
        _ = &prev;
        const max: c_int = (@as(c_int, 1) << @intCast(bpc)) - @as(c_int, 1);
        _ = &max;
        {
            var i: c_int = 1;
            _ = &i;
            while (i < @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)])) : (i += 1) {
                var delta: c_int = @bitCast(@as(c_uint, @truncate(dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, bits))))));
                _ = &delta;
                if ((@as(c_uint, @bitCast(@as(c_int, delta))) != 0) and (antfly_av1_dav1d_msac_decode_bool_equi_c(&ts.*.msac) != 0)) {
                    delta = -delta;
                }
                prev = blk: {
                    const tmp = @as(pixel, @bitCast(@as(c_short, @truncate((prev + delta) & max))));
                    pal[@bitCast(@as(isize, @intCast(i)))] = tmp;
                    break :blk tmp;
                };
            }
        }
    } else {
        {
            var i: c_int = 0;
            _ = &i;
            while (i < @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)])) : (i += 1) {
                pal[@bitCast(@as(isize, @intCast(i)))] = @truncate(dav1d_msac_decode_bools(&ts.*.msac, @bitCast(@as(c_int, bpc))));
            }
        }
    }
    if (((((false and (@as(c_int, f.*.frame_hdr.*.frame_offset) == @as(c_int, 2))) and (t.*.by >= @as(c_int, 0))) and (t.*.by < @as(c_int, 4))) and (t.*.bx >= @as(c_int, 8))) and (t.*.bx < @as(c_int, 12))) {
        _ = @as(c_int, 0);
        {
            var n: c_int = 0;
            _ = &n;
            while (n < @as(c_int, b.*.unnamed_0.unnamed_0.pal_sz[@as(c_int, 1)])) : (n += 1) {
                _ = @as(c_int, 0);
            }
        }
        _ = @as(c_int, 0);
    }
}
