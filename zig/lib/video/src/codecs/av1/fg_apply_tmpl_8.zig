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
//  * Copyright © 2018, Niklas Haas
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
pub const ptrdiff_t = common.ptrdiff_t;
pub const struct_Dav1dRef_1 = opaque {};
pub const struct_Dav1dUserData = extern struct {
    data: [*c]const u8 = null,
    ref: ?*struct_Dav1dRef_1 = null,
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
pub const DAV1D_PIXEL_LAYOUT_I400 = common.DAV1D_PIXEL_LAYOUT_I400;
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const DAV1D_MC_IDENTITY = common.DAV1D_MC_IDENTITY;
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
    frame_hdr_ref: ?*struct_Dav1dRef_1 = null,
    seq_hdr_ref: ?*struct_Dav1dRef_1 = null,
    content_light_ref: ?*struct_Dav1dRef_1 = null,
    mastering_display_ref: ?*struct_Dav1dRef_1 = null,
    itut_t35_ref: ?*struct_Dav1dRef_1 = null,
    reserved_ref: [4]usize = @import("std").mem.zeroes([4]usize),
    ref: ?*struct_Dav1dRef_1 = null,
    allocator_data: ?*anyopaque = null,
};
pub const Dav1dPicture = struct_Dav1dPicture;
pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const pixel = u8;
pub const entry = i8;
pub const generate_grain_y_fn = ?*const fn (buf: [*c][82]entry, data: [*c]const Dav1dFilmGrainData) callconv(.c) void;
pub const generate_grain_uv_fn = ?*const fn (buf: [*c][82]entry, buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize) callconv(.c) void;
pub const fgy_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int) callconv(.c) void;
pub const fguv_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, luma_row: [*c]const pixel, luma_stride: ptrdiff_t, uv_pl: c_int, is_id: c_int) callconv(.c) void;
pub const struct_Dav1dFilmGrainDSPContext = extern struct {
    generate_grain_y: generate_grain_y_fn = null,
    generate_grain_uv: [3]generate_grain_uv_fn = @import("std").mem.zeroes([3]generate_grain_uv_fn),
    fgy_32x32xn: fgy_32x32xn_fn = null,
    fguv_32x32xn: [3]fguv_32x32xn_fn = @import("std").mem.zeroes([3]fguv_32x32xn_fn),
};
pub const Dav1dFilmGrainDSPContext = struct_Dav1dFilmGrainDSPContext;
pub fn generate_scaling(bitdepth: c_int, arg_points: [*c][2]u8, num: c_int, arg_scaling: [*c]u8) callconv(.c) void {
    _ = &bitdepth;
    var points = arg_points;
    _ = &points;
    _ = &num;
    var scaling = arg_scaling;
    _ = &scaling;
    const shift_x: c_int = 0;
    _ = &shift_x;
    const scaling_size: c_int = 256;
    _ = &scaling_size;
    if (num == @as(c_int, 0)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(scaling)), 0, @bitCast(@as(c_long, scaling_size)));
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(scaling)), points[@as(c_int, 0)][@as(c_int, 1)], @bitCast(@as(c_long, @as(c_int, points[@as(c_int, 0)][@as(c_int, 0)]) << @intCast(shift_x))));
    {
        var i: c_int = 0;
        _ = &i;
        while (i < (num - @as(c_int, 1))) : (i += 1) {
            const bx: c_int = points[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)];
            _ = &bx;
            const by: c_int = points[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)];
            _ = &by;
            const ex: c_int = points[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))][@as(c_int, 0)];
            _ = &ex;
            const ey: c_int = points[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))][@as(c_int, 1)];
            _ = &ey;
            const dx: c_int = ex - bx;
            _ = &dx;
            const dy: c_int = ey - by;
            _ = &dy;
            while (true) {
                if (!(dx > @as(c_int, 0))) {
                    unreachable;
                }
                if (!false) break;
            }
            const delta: c_int = dy * @divTrunc(@as(c_int, 65536) + (dx >> @intCast(@as(c_int, 1))), dx);
            _ = &delta;
            {
                var x: c_int = 0;
                _ = &x;
                var d: c_int = 32768;
                _ = &d;
                while (x < dx) : (x += 1) {
                    scaling[@bitCast(@as(isize, @intCast((bx + x) << @intCast(shift_x))))] = @bitCast(@as(i8, @truncate(by + (d >> @intCast(@as(c_int, 16))))));
                    d += delta;
                }
            }
        }
    }
    const n: c_int = @as(c_int, points[@bitCast(@as(isize, @intCast(num - @as(c_int, 1))))][@as(c_int, 0)]) << @intCast(shift_x);
    _ = &n;
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&scaling[@bitCast(@as(isize, @intCast(n)))])), points[@bitCast(@as(isize, @intCast(num - @as(c_int, 1))))][@as(c_int, 1)], @bitCast(@as(c_long, scaling_size - n)));
}
pub export fn antfly_av1_dav1d_prep_grain_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, arg_scaling: [*c][256]u8, arg_grain_lut: [*c][74][82]entry) void {
    _ = &dsp;
    _ = &out;
    _ = &in;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    const data: [*c]const Dav1dFilmGrainData = &out.*.frame_hdr.*.film_grain.data;
    _ = &data;
    dsp.*.generate_grain_y.?(@ptrCast(@alignCast(&grain_lut[@as(c_int, 0)])), data);
    if ((data.*.num_uv_points[@as(c_int, 0)] != 0) or (data.*.chroma_scaling_from_luma != 0)) {
        dsp.*.generate_grain_uv[in.*.p.layout -% @as(enum_Dav1dPixelLayout, 1)].?(@ptrCast(@alignCast(&grain_lut[@as(c_int, 1)])), @ptrCast(@alignCast(&grain_lut[@as(c_int, 0)])), data, 0);
    }
    if ((data.*.num_uv_points[@as(c_int, 1)] != 0) or (data.*.chroma_scaling_from_luma != 0)) {
        dsp.*.generate_grain_uv[in.*.p.layout -% @as(enum_Dav1dPixelLayout, 1)].?(@ptrCast(@alignCast(&grain_lut[@as(c_int, 2)])), @ptrCast(@alignCast(&grain_lut[@as(c_int, 0)])), data, 1);
    }
    if ((data.*.num_y_points != 0) or (data.*.chroma_scaling_from_luma != 0)) {
        generate_scaling(in.*.p.bpc, @ptrCast(@alignCast(@constCast(&data.*.y_points))), data.*.num_y_points, @ptrCast(@alignCast(&scaling[@as(c_int, 0)])));
    }
    if (data.*.num_uv_points[@as(c_int, 0)] != 0) {
        generate_scaling(in.*.p.bpc, @ptrCast(@alignCast(@constCast(&data.*.uv_points[@as(c_int, 0)]))), data.*.num_uv_points[@as(c_int, 0)], @ptrCast(@alignCast(&scaling[@as(c_int, 1)])));
    }
    if (data.*.num_uv_points[@as(c_int, 1)] != 0) {
        generate_scaling(in.*.p.bpc, @ptrCast(@alignCast(@constCast(&data.*.uv_points[@as(c_int, 1)]))), data.*.num_uv_points[@as(c_int, 1)], @ptrCast(@alignCast(&scaling[@as(c_int, 2)])));
    }
    while (true) {
        if (!(out.*.stride[@as(c_int, 0)] == in.*.stride[@as(c_int, 0)])) {
            unreachable;
        }
        if (!false) break;
    }
    if (!(data.*.num_y_points != 0)) {
        const stride: ptrdiff_t = out.*.stride[@as(c_int, 0)];
        _ = &stride;
        const sz: ptrdiff_t = @as(ptrdiff_t, out.*.p.h) * stride;
        _ = &sz;
        if (sz < @as(ptrdiff_t, 0)) {
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(out.*.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(in.*.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @bitCast(@as(c_long, -sz)));
        } else {
            _ = antfly_codec_afz_memcpy(out.*.data[@as(c_int, 0)], in.*.data[@as(c_int, 0)], @bitCast(@as(c_long, sz)));
        }
    }
    if ((in.*.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I400)) and !(data.*.chroma_scaling_from_luma != 0)) {
        while (true) {
            if (!(out.*.stride[@as(c_int, 1)] == in.*.stride[@as(c_int, 1)])) {
                unreachable;
            }
            if (!false) break;
        }
        const ss_ver: c_int = @as(c_int, @intFromBool(in.*.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
        _ = &ss_ver;
        const stride: ptrdiff_t = out.*.stride[@as(c_int, 1)];
        _ = &stride;
        const sz: ptrdiff_t = @as(ptrdiff_t, (out.*.p.h + ss_ver) >> @intCast(ss_ver)) * stride;
        _ = &sz;
        if (sz < @as(ptrdiff_t, 0)) {
            if (!(data.*.num_uv_points[@as(c_int, 0)] != 0)) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(out.*.data[@as(c_int, 1)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(in.*.data[@as(c_int, 1)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @bitCast(@as(c_long, -sz)));
            }
            if (!(data.*.num_uv_points[@as(c_int, 1)] != 0)) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(out.*.data[@as(c_int, 2)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @ptrCast(@alignCast((@as([*c]u8, @ptrCast(@alignCast(in.*.data[@as(c_int, 2)]))) + @as(usize, @bitCast(@as(isize, @intCast(sz))))) - @as(usize, @bitCast(@as(isize, @intCast(stride)))))), @bitCast(@as(c_long, -sz)));
            }
        } else {
            if (!(data.*.num_uv_points[@as(c_int, 0)] != 0)) {
                _ = antfly_codec_afz_memcpy(out.*.data[@as(c_int, 1)], in.*.data[@as(c_int, 1)], @bitCast(@as(c_long, sz)));
            }
            if (!(data.*.num_uv_points[@as(c_int, 1)] != 0)) {
                _ = antfly_codec_afz_memcpy(out.*.data[@as(c_int, 2)], in.*.data[@as(c_int, 2)], @bitCast(@as(c_long, sz)));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_apply_grain_row_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture, arg_scaling: [*c][256]u8, arg_grain_lut: [*c][74][82]entry, row: c_int) void {
    _ = &dsp;
    _ = &out;
    _ = &in;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    _ = &row;
    const data: [*c]const Dav1dFilmGrainData = &out.*.frame_hdr.*.film_grain.data;
    _ = &data;
    const ss_y: c_int = @as(c_int, @intFromBool(in.*.p.layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_y;
    const ss_x: c_int = @as(c_int, @intFromBool(in.*.p.layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_x;
    const cpw: c_int = (out.*.p.w + ss_x) >> @intCast(ss_x);
    _ = &cpw;
    const is_id: c_int = @as(c_int, @intFromBool(out.*.seq_hdr.*.mtrx == @as(enum_Dav1dMatrixCoefficients, DAV1D_MC_IDENTITY)));
    _ = &is_id;
    const luma_src: [*c]pixel = @as([*c]pixel, @ptrCast(@alignCast(in.*.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, row * @as(c_int, 32)) * in.*.stride[@as(c_int, 0)]))));
    _ = &luma_src;
    if (data.*.num_y_points != 0) {
        const bh: c_int = imin(out.*.p.h - (row * @as(c_int, 32)), 32);
        _ = &bh;
        dsp.*.fgy_32x32xn.?(@as([*c]pixel, @ptrCast(@alignCast(out.*.data[@as(c_int, 0)]))) + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, row * @as(c_int, 32)) * out.*.stride[@as(c_int, 0)])))), luma_src, out.*.stride[@as(c_int, 0)], data, @bitCast(@as(c_long, out.*.p.w)), @ptrCast(@alignCast(&scaling[@as(c_int, 0)])), @ptrCast(@alignCast(&grain_lut[@as(c_int, 0)])), bh, row);
    }
    if ((!(data.*.num_uv_points[@as(c_int, 0)] != 0) and !(data.*.num_uv_points[@as(c_int, 1)] != 0)) and !(data.*.chroma_scaling_from_luma != 0)) {
        return;
    }
    const bh: c_int = (imin(out.*.p.h - (row * @as(c_int, 32)), 32) + ss_y) >> @intCast(ss_y);
    _ = &bh;
    if ((out.*.p.w & ss_x) != 0) {
        var ptr: [*c]pixel = luma_src;
        _ = &ptr;
        {
            var y: c_int = 0;
            _ = &y;
            while (y < bh) : (y += 1) {
                ptr[@bitCast(@as(isize, @intCast(out.*.p.w)))] = ptr[@bitCast(@as(isize, @intCast(out.*.p.w - @as(c_int, 1))))];
                ptr += @as(usize, @bitCast(@as(isize, @intCast(in.*.stride[@as(c_int, 0)] << @intCast(@as(ptrdiff_t, ss_y))))));
            }
        }
    }
    const uv_off: ptrdiff_t = (@as(ptrdiff_t, row * @as(c_int, 32)) * out.*.stride[@as(c_int, 1)]) >> @intCast(@as(ptrdiff_t, ss_y));
    _ = &uv_off;
    if (data.*.chroma_scaling_from_luma != 0) {
        {
            var pl: c_int = 0;
            _ = &pl;
            while (pl < @as(c_int, 2)) : (pl += 1) {
                dsp.*.fguv_32x32xn[in.*.p.layout -% @as(enum_Dav1dPixelLayout, 1)].?(@as([*c]pixel, @ptrCast(@alignCast(out.*.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(uv_off)))), @as([*c]const pixel, @ptrCast(@alignCast(in.*.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(uv_off)))), in.*.stride[@as(c_int, 1)], data, @bitCast(@as(c_long, cpw)), @ptrCast(@alignCast(&scaling[@as(c_int, 0)])), @ptrCast(@alignCast(&grain_lut[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))])), bh, row, luma_src, in.*.stride[@as(c_int, 0)], pl, is_id);
            }
        }
    } else {
        {
            var pl: c_int = 0;
            _ = &pl;
            while (pl < @as(c_int, 2)) : (pl += 1) if (data.*.num_uv_points[@bitCast(@as(isize, @intCast(pl)))] != 0) {
                dsp.*.fguv_32x32xn[in.*.p.layout -% @as(enum_Dav1dPixelLayout, 1)].?(@as([*c]pixel, @ptrCast(@alignCast(out.*.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(uv_off)))), @as([*c]const pixel, @ptrCast(@alignCast(in.*.data[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))]))) + @as(usize, @bitCast(@as(isize, @intCast(uv_off)))), in.*.stride[@as(c_int, 1)], data, @bitCast(@as(c_long, cpw)), @ptrCast(@alignCast(&scaling[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))])), @ptrCast(@alignCast(&grain_lut[@bitCast(@as(isize, @intCast(@as(c_int, 1) + pl)))])), bh, row, luma_src, in.*.stride[@as(c_int, 0)], pl, is_id);
            };
        }
    }
}
pub export fn antfly_av1_dav1d_apply_grain_8bpc(dsp: [*c]const Dav1dFilmGrainDSPContext, out: [*c]Dav1dPicture, in: [*c]const Dav1dPicture) void {
    _ = &dsp;
    _ = &out;
    _ = &in;
    var grain_lut: [3][74][82]entry align(8) = undefined;
    _ = &grain_lut;
    var scaling: [3][256]u8 = undefined;
    _ = &scaling;
    const rows: c_int = @divTrunc((out.*.p.h + @as(c_int, 32)) - @as(c_int, 1), @as(c_int, 32));
    _ = &rows;
    antfly_av1_dav1d_prep_grain_8bpc(dsp, out, in, @ptrCast(@alignCast(&scaling)), @ptrCast(@alignCast(&grain_lut)));
    {
        var row: c_int = 0;
        _ = &row;
        while (row < rows) : (row += 1) {
            antfly_av1_dav1d_apply_grain_row_8bpc(dsp, out, in, @ptrCast(@alignCast(&scaling)), @ptrCast(@alignCast(&grain_lut)), row);
        }
    }
}
