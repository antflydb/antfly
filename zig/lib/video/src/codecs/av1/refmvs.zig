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
//  * Copyright © 2021, VideoLAN and dav1d authors
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
pub extern fn antfly_codec_afz_free(?*anyopaque) void;
pub extern fn antfly_codec_afz_posix_memalign(__memptr: [*c]?*anyopaque, __alignment: usize, __size: usize) c_int;
pub extern fn antfly_codec_abs(c_int) c_int;
pub const ptrdiff_t = common.ptrdiff_t;
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
pub fn apply_sign(v: c_int, s: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(c_int, 0)) -v else v;
}
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
pub const enum_Dav1dAdaptiveBoolean = common.enum_Dav1dAdaptiveBoolean;
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
pub const DAV1D_WM_TYPE_IDENTITY = common.DAV1D_WM_TYPE_IDENTITY;
pub const DAV1D_WM_TYPE_TRANSLATION = common.DAV1D_WM_TYPE_TRANSLATION;
pub const DAV1D_WM_TYPE_ROT_ZOOM = common.DAV1D_WM_TYPE_ROT_ZOOM;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_4 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_3 = extern union {
    p: struct_unnamed_4,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_3 = @import("std").mem.zeroes(union_unnamed_3),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const enum_Dav1dColorPrimaries = common.enum_Dav1dColorPrimaries;
pub const enum_Dav1dTransferCharacteristics = common.enum_Dav1dTransferCharacteristics;
pub const enum_Dav1dMatrixCoefficients = common.enum_Dav1dMatrixCoefficients;
pub const enum_Dav1dChromaSamplePosition = common.enum_Dav1dChromaSamplePosition;
pub const struct_Dav1dSequenceHeaderOperatingPoint_5 = common.struct_Dav1dSequenceHeaderOperatingPoint_5;
pub const struct_Dav1dSequenceHeaderOperatingParameterInfo_6 = common.struct_Dav1dSequenceHeaderOperatingParameterInfo_6;
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
    operating_points: [32]struct_Dav1dSequenceHeaderOperatingPoint_5 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingPoint_5),
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
    operating_parameter_info: [32]struct_Dav1dSequenceHeaderOperatingParameterInfo_6 = @import("std").mem.zeroes([32]struct_Dav1dSequenceHeaderOperatingParameterInfo_6),
};
pub const Dav1dSequenceHeader = struct_Dav1dSequenceHeader;
pub const Dav1dSegmentationDataSet = common.Dav1dSegmentationDataSet;
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_7 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_8 = common.struct_Dav1dFrameHeaderOperatingPoint_8;
const struct_unnamed_9 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
const struct_unnamed_10 = extern struct {
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
const struct_unnamed_11 = extern struct {
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
const struct_unnamed_12 = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    temporal: u8 = 0,
    update_data: u8 = 0,
    seg_data: Dav1dSegmentationDataSet = @import("std").mem.zeroes(Dav1dSegmentationDataSet),
    lossless: [8]u8 = @import("std").mem.zeroes([8]u8),
    qidx: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_14 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
};
const struct_unnamed_15 = extern struct {
    present: u8 = 0,
    res_log2: u8 = 0,
    multi: u8 = 0,
};
const struct_unnamed_13 = extern struct {
    q: struct_unnamed_14 = @import("std").mem.zeroes(struct_unnamed_14),
    lf: struct_unnamed_15 = @import("std").mem.zeroes(struct_unnamed_15),
};
const struct_unnamed_16 = extern struct {
    level_y: [2]u8 = @import("std").mem.zeroes([2]u8),
    level_u: u8 = 0,
    level_v: u8 = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    mode_ref_deltas: Dav1dLoopfilterModeRefDeltas = @import("std").mem.zeroes(Dav1dLoopfilterModeRefDeltas),
    sharpness: u8 = 0,
};
const struct_unnamed_17 = extern struct {
    damping: u8 = 0,
    n_bits: u8 = 0,
    y_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
    uv_strength: [8]u8 = @import("std").mem.zeroes([8]u8),
};
const struct_unnamed_18 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
pub const struct_Dav1dFrameHeader = extern struct {
    film_grain: struct_unnamed_7 = @import("std").mem.zeroes(struct_unnamed_7),
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
    operating_points: [32]struct_Dav1dFrameHeaderOperatingPoint_8 = @import("std").mem.zeroes([32]struct_Dav1dFrameHeaderOperatingPoint_8),
    refresh_frame_flags: u8 = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    super_res: struct_unnamed_9 = @import("std").mem.zeroes(struct_unnamed_9),
    have_render_size: u8 = 0,
    allow_intrabc: u8 = 0,
    frame_ref_short_signaling: u8 = 0,
    refidx: [7]i8 = @import("std").mem.zeroes([7]i8),
    hp: u8 = 0,
    subpel_filter_mode: enum_Dav1dFilterMode = @import("std").mem.zeroes(enum_Dav1dFilterMode),
    switchable_motion_mode: u8 = 0,
    use_ref_frame_mvs: u8 = 0,
    refresh_context: u8 = 0,
    tiling: struct_unnamed_10 = @import("std").mem.zeroes(struct_unnamed_10),
    quant: struct_unnamed_11 = @import("std").mem.zeroes(struct_unnamed_11),
    segmentation: struct_unnamed_12 = @import("std").mem.zeroes(struct_unnamed_12),
    delta: struct_unnamed_13 = @import("std").mem.zeroes(struct_unnamed_13),
    all_lossless: u8 = 0,
    loopfilter: struct_unnamed_16 = @import("std").mem.zeroes(struct_unnamed_16),
    cdef: struct_unnamed_17 = @import("std").mem.zeroes(struct_unnamed_17),
    restoration: struct_unnamed_18 = @import("std").mem.zeroes(struct_unnamed_18),
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
pub const enum_BlockSize = common.enum_BlockSize;
const struct_unnamed_19 = extern struct {
    y: i16 = 0,
    x: i16 = 0,
};
pub const union_mv = extern union {
    unnamed_0: struct_unnamed_19,
    n: u32,
};
pub const mv = union_mv;
pub const EDGE_I444_TOP_HAS_RIGHT = common.EDGE_I444_TOP_HAS_RIGHT;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
pub extern var antfly_av1_dav1d_block_dimensions: [22][4]u8;
pub const struct_refmvs_temporal_block = extern struct {
    mv: mv align(1) = @import("std").mem.zeroes(mv),
    ref: i8 = 0,
};
pub const refmvs_temporal_block = struct_refmvs_temporal_block;
comptime {
    if (!(@as(c_ulong, 5) == @sizeOf(refmvs_temporal_block))) @compileError("static assertion failed \"refmvs_temporal_block\"");
}
pub const union_refmvs_refpair = common.union_refmvs_refpair;
pub const refmvs_refpair = common.refmvs_refpair;
comptime {
    if (!(@as(c_ulong, 2) == @sizeOf(refmvs_refpair))) @compileError("static assertion failed \"refmvs_refpair\"");
}
pub const union_refmvs_mvpair = extern union {
    mv: [2]mv,
    n: u64,
};
pub const refmvs_mvpair = union_refmvs_mvpair;
comptime {
    if (!(@as(c_ulong, 8) == @sizeOf(refmvs_mvpair))) @compileError("static assertion failed \"refmvs_mvpair\"");
}
pub const struct_refmvs_block = extern struct {
    mv: refmvs_mvpair align(1) = @import("std").mem.zeroes(refmvs_mvpair),
    ref: refmvs_refpair align(1) = @import("std").mem.zeroes(refmvs_refpair),
    bs: u8 = 0,
    mf: u8 = 0,
};
pub const refmvs_block = struct_refmvs_block;
comptime {
    if (!(@as(c_ulong, 12) == @sizeOf(refmvs_block))) @compileError("static assertion failed \"refmvs_block\"");
}
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
pub const struct_refmvs_candidate = extern struct {
    mv: refmvs_mvpair = @import("std").mem.zeroes(refmvs_mvpair),
    weight: c_int = 0,
};
pub const refmvs_candidate = struct_refmvs_candidate;
pub const load_tmvs_fn = ?*const fn (rf: [*c]const refmvs_frame, tile_row_idx: c_int, col_start8: c_int, col_end8: c_int, row_start8: c_int, row_end8: c_int) callconv(.c) void;
pub const save_tmvs_fn = ?*const fn (rp: [*c]refmvs_temporal_block, stride: ptrdiff_t, rr: [*c]const [*c]refmvs_block, ref_sign: [*c]const u8, col_end8: c_int, row_end8: c_int, col_start8: c_int, row_start8: c_int) callconv(.c) void;
pub const splat_mv_fn = ?*const fn (rr: [*c][*c]refmvs_block, rmv: [*c]const refmvs_block, bx4: c_int, bw4: c_int, bh4: c_int) callconv(.c) void;
pub const struct_Dav1dRefmvsDSPContext = extern struct {
    load_tmvs: load_tmvs_fn = null,
    save_tmvs: save_tmvs_fn = null,
    splat_mv: splat_mv_fn = null,
};
pub const Dav1dRefmvsDSPContext = struct_Dav1dRefmvsDSPContext;
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
                        .unnamed_0 = struct_unnamed_19{
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
                        .unnamed_0 = struct_unnamed_19{
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
                        .unnamed_0 = struct_unnamed_19{
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
                    .unnamed_0 = struct_unnamed_19{
                        .y = 0,
                        .x = 0,
                    },
                };
            },
        }
    }
}
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
pub fn add_spatial_candidate(mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, weight: c_int, b: [*c]const refmvs_block, ref: union_refmvs_refpair, arg_gmv: [*c]const mv, have_newmv_match: [*c]c_int, have_refmv_match: [*c]c_int) callconv(.c) void {
    _ = &mvstack;
    _ = &cnt;
    _ = &weight;
    _ = &b;
    _ = &ref;
    var gmv = arg_gmv;
    _ = &gmv;
    _ = &have_newmv_match;
    _ = &have_refmv_match;
    if (b.*.mv.mv[@as(c_int, 0)].n == @as(c_uint, 2147516416)) return;
    if (@as(c_int, ref.ref[@as(c_int, 1)]) == -@as(c_int, 1)) {
        {
            var n: c_int = 0;
            _ = &n;
            while (n < @as(c_int, 2)) : (n += 1) {
                if (@as(c_int, b.*.ref.ref[@bitCast(@as(isize, @intCast(n)))]) == @as(c_int, ref.ref[@as(c_int, 0)])) {
                    const cand_mv: mv = if (((@as(c_int, b.*.mf) & @as(c_int, 1)) != 0) and (gmv[@as(c_int, 0)].n != @as(c_uint, 2147516416))) gmv[@as(c_int, 0)] else b.*.mv.mv[@bitCast(@as(isize, @intCast(n)))];
                    _ = &cand_mv;
                    have_refmv_match.* = 1;
                    have_newmv_match.* |= @as(c_int, b.*.mf) >> @intCast(@as(c_int, 1));
                    const last: c_int = cnt.*;
                    _ = &last;
                    {
                        var m: c_int = 0;
                        _ = &m;
                        while (m < last) : (m += 1) if (mvstack[@bitCast(@as(isize, @intCast(m)))].mv.mv[@as(c_int, 0)].n == cand_mv.n) {
                            mvstack[@bitCast(@as(isize, @intCast(m)))].weight += weight;
                            return;
                        };
                    }
                    if (last < @as(c_int, 8)) {
                        mvstack[@bitCast(@as(isize, @intCast(last)))].mv.mv[@as(c_int, 0)] = cand_mv;
                        mvstack[@bitCast(@as(isize, @intCast(last)))].weight = weight;
                        cnt.* = last + @as(c_int, 1);
                    }
                    return;
                }
            }
        }
    } else if (@as(c_int, b.*.ref.pair) == @as(c_int, ref.pair)) {
        const cand_mv: refmvs_mvpair = refmvs_mvpair{
            .mv = [2]mv{
                if (((@as(c_int, b.*.mf) & @as(c_int, 1)) != 0) and (gmv[@as(c_int, 0)].n != @as(c_uint, 2147516416))) gmv[@as(c_int, 0)] else b.*.mv.mv[@as(c_int, 0)],
                if (((@as(c_int, b.*.mf) & @as(c_int, 1)) != 0) and (gmv[@as(c_int, 1)].n != @as(c_uint, 2147516416))) gmv[@as(c_int, 1)] else b.*.mv.mv[@as(c_int, 1)],
            },
        };
        _ = &cand_mv;
        have_refmv_match.* = 1;
        have_newmv_match.* |= @as(c_int, b.*.mf) >> @intCast(@as(c_int, 1));
        const last: c_int = cnt.*;
        _ = &last;
        {
            var n: c_int = 0;
            _ = &n;
            while (n < last) : (n += 1) if (mvstack[@bitCast(@as(isize, @intCast(n)))].mv.n == cand_mv.n) {
                mvstack[@bitCast(@as(isize, @intCast(n)))].weight += weight;
                return;
            };
        }
        if (last < @as(c_int, 8)) {
            mvstack[@bitCast(@as(isize, @intCast(last)))].mv = cand_mv;
            mvstack[@bitCast(@as(isize, @intCast(last)))].weight = weight;
            cnt.* = last + @as(c_int, 1);
        }
    }
}
pub fn scan_row(mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, ref: union_refmvs_refpair, arg_gmv: [*c]const mv, arg_b: [*c]const refmvs_block, bw4: c_int, w4: c_int, max_rows: c_int, step: c_int, have_newmv_match: [*c]c_int, have_refmv_match: [*c]c_int) callconv(.c) c_int {
    _ = &mvstack;
    _ = &cnt;
    _ = &ref;
    var gmv = arg_gmv;
    _ = &gmv;
    var b = arg_b;
    _ = &b;
    _ = &bw4;
    _ = &w4;
    _ = &max_rows;
    _ = &step;
    _ = &have_newmv_match;
    _ = &have_refmv_match;
    var cand_b: [*c]const refmvs_block = b;
    _ = &cand_b;
    const first_cand_bs: enum_BlockSize = cand_b.*.bs;
    _ = &first_cand_bs;
    const first_cand_b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[first_cand_bs]));
    _ = &first_cand_b_dim;
    var cand_bw4: c_int = first_cand_b_dim[@as(c_int, 0)];
    _ = &cand_bw4;
    var len: c_int = imax(step, imin(bw4, cand_bw4));
    _ = &len;
    if (bw4 <= cand_bw4) {
        const weight: c_int = if (bw4 == @as(c_int, 1)) @as(c_int, 2) else imax(2, imin(@as(c_int, 2) * max_rows, first_cand_b_dim[@as(c_int, 1)]));
        _ = &weight;
        add_spatial_candidate(mvstack, cnt, len * weight, cand_b, ref, gmv, have_newmv_match, have_refmv_match);
        return weight >> @intCast(@as(c_int, 1));
    }
    var x: c_int = 0;
    while (true) {
        add_spatial_candidate(mvstack, cnt, len * @as(c_int, 2), cand_b, ref, gmv, have_newmv_match, have_refmv_match);
        x += len;
        if (x >= w4) return 1;
        cand_b = &b[@bitCast(@as(isize, @intCast(x)))];
        cand_bw4 = antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 0)];
        while (true) {
            if (!(cand_bw4 < bw4)) {
                unreachable;
            }
            if (!false) break;
        }
        len = imax(step, cand_bw4);
    }
    return undefined;
}
pub fn scan_col(mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, ref: union_refmvs_refpair, arg_gmv: [*c]const mv, arg_b: [*c]const [*c]refmvs_block, bh4: c_int, h4: c_int, bx4: c_int, max_cols: c_int, step: c_int, have_newmv_match: [*c]c_int, have_refmv_match: [*c]c_int) callconv(.c) c_int {
    _ = &mvstack;
    _ = &cnt;
    _ = &ref;
    var gmv = arg_gmv;
    _ = &gmv;
    var b = arg_b;
    _ = &b;
    _ = &bh4;
    _ = &h4;
    _ = &bx4;
    _ = &max_cols;
    _ = &step;
    _ = &have_newmv_match;
    _ = &have_refmv_match;
    var cand_b: [*c]const refmvs_block = &b[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))];
    _ = &cand_b;
    const first_cand_bs: enum_BlockSize = cand_b.*.bs;
    _ = &first_cand_bs;
    const first_cand_b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[first_cand_bs]));
    _ = &first_cand_b_dim;
    var cand_bh4: c_int = first_cand_b_dim[@as(c_int, 1)];
    _ = &cand_bh4;
    var len: c_int = imax(step, imin(bh4, cand_bh4));
    _ = &len;
    if (bh4 <= cand_bh4) {
        const weight: c_int = if (bh4 == @as(c_int, 1)) @as(c_int, 2) else imax(2, imin(@as(c_int, 2) * max_cols, first_cand_b_dim[@as(c_int, 0)]));
        _ = &weight;
        add_spatial_candidate(mvstack, cnt, len * weight, cand_b, ref, gmv, have_newmv_match, have_refmv_match);
        return weight >> @intCast(@as(c_int, 1));
    }
    var y: c_int = 0;
    while (true) {
        add_spatial_candidate(mvstack, cnt, len * @as(c_int, 2), cand_b, ref, gmv, have_newmv_match, have_refmv_match);
        y += len;
        if (y >= h4) return 1;
        cand_b = &b[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(bx4)))];
        cand_bh4 = antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 1)];
        while (true) {
            if (!(cand_bh4 < bh4)) {
                unreachable;
            }
            if (!false) break;
        }
        len = imax(step, cand_bh4);
    }
    return undefined;
}
pub fn mv_projection(mv_1: union_mv, num: c_int, den: c_int) callconv(.c) union_mv {
    _ = &mv_1;
    _ = &num;
    _ = &den;
    const static_local_div_mult = struct {
        const div_mult: [32]u16 = [32]u16{
            0,
            16384,
            8192,
            5461,
            4096,
            3276,
            2730,
            2340,
            2048,
            1820,
            1638,
            1489,
            1365,
            1260,
            1170,
            1092,
            1024,
            963,
            910,
            862,
            819,
            780,
            744,
            712,
            682,
            655,
            630,
            606,
            585,
            564,
            546,
            528,
        };
    };
    _ = &static_local_div_mult;
    while (true) {
        if (!((den > @as(c_int, 0)) and (den < @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!((num > -@as(c_int, 32)) and (num < @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    const frac: c_int = num * @as(c_int, static_local_div_mult.div_mult[@bitCast(@as(isize, @intCast(den)))]);
    _ = &frac;
    const y: c_int = @as(c_int, mv_1.unnamed_0.y) * frac;
    _ = &y;
    const x: c_int = @as(c_int, mv_1.unnamed_0.x) * frac;
    _ = &x;
    return union_mv{
        .unnamed_0 = struct_unnamed_19{
            .y = @truncate(iclip(((y + @as(c_int, 8192)) + (y >> @intCast(@as(c_int, 31)))) >> @intCast(@as(c_int, 14)), -@as(c_int, 16383), 16383)),
            .x = @truncate(iclip(((x + @as(c_int, 8192)) + (x >> @intCast(@as(c_int, 31)))) >> @intCast(@as(c_int, 14)), -@as(c_int, 16383), 16383)),
        },
    };
}
pub fn add_temporal_candidate(rf: [*c]const refmvs_frame, mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, rb: [*c]const refmvs_temporal_block, ref: union_refmvs_refpair, globalmv_ctx: [*c]c_int, arg_gmv: [*c]const union_mv) callconv(.c) void {
    _ = &rf;
    _ = &mvstack;
    _ = &cnt;
    _ = &rb;
    _ = &ref;
    _ = &globalmv_ctx;
    var gmv = arg_gmv;
    _ = &gmv;
    if (rb.*.mv.n == @as(c_uint, 2147516416)) return;
    var mv_1: union_mv = mv_projection(rb.*.mv, rf.*.pocdiff[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], rb.*.ref);
    _ = &mv_1;
    fix_mv_precision(rf.*.frm_hdr, &mv_1);
    const last: c_int = cnt.*;
    _ = &last;
    if (@as(c_int, ref.ref[@as(c_int, 1)]) == -@as(c_int, 1)) {
        if (globalmv_ctx != null) {
            globalmv_ctx.* = @as(c_int, @intFromBool((antfly_codec_abs(@as(c_int, mv_1.unnamed_0.x) - @as(c_int, gmv[@as(c_int, 0)].unnamed_0.x)) | antfly_codec_abs(@as(c_int, mv_1.unnamed_0.y) - @as(c_int, gmv[@as(c_int, 0)].unnamed_0.y))) >= @as(c_int, 16)));
        }
        {
            var n: c_int = 0;
            _ = &n;
            while (n < last) : (n += 1) if (mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].n == mv_1.n) {
                mvstack[@bitCast(@as(isize, @intCast(n)))].weight += 2;
                return;
            };
        }
        if (last < @as(c_int, 8)) {
            mvstack[@bitCast(@as(isize, @intCast(last)))].mv.mv[@as(c_int, 0)] = mv_1;
            mvstack[@bitCast(@as(isize, @intCast(last)))].weight = 2;
            cnt.* = last + @as(c_int, 1);
        }
    } else {
        var mvp: refmvs_mvpair = refmvs_mvpair{
            .mv = [2]mv{
                mv_1,
                mv_projection(rb.*.mv, rf.*.pocdiff[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 1)]) - @as(c_int, 1))))], rb.*.ref),
            },
        };
        _ = &mvp;
        fix_mv_precision(rf.*.frm_hdr, &mvp.mv[@as(c_int, 1)]);
        {
            var n: c_int = 0;
            _ = &n;
            while (n < last) : (n += 1) if (mvstack[@bitCast(@as(isize, @intCast(n)))].mv.n == mvp.n) {
                mvstack[@bitCast(@as(isize, @intCast(n)))].weight += 2;
                return;
            };
        }
        if (last < @as(c_int, 8)) {
            mvstack[@bitCast(@as(isize, @intCast(last)))].mv = mvp;
            mvstack[@bitCast(@as(isize, @intCast(last)))].weight = 2;
            cnt.* = last + @as(c_int, 1);
        }
    }
}
pub fn add_compound_extended_candidate(same: [*c]refmvs_candidate, same_count: [*c]c_int, cand_b: [*c]const refmvs_block, sign0: c_int, sign1: c_int, ref: union_refmvs_refpair, sign_bias: [*c]const u8) callconv(.c) void {
    _ = &same;
    _ = &same_count;
    _ = &cand_b;
    _ = &sign0;
    _ = &sign1;
    _ = &ref;
    _ = &sign_bias;
    const diff: [*c]refmvs_candidate = &same[@as(c_int, 2)];
    _ = &diff;
    const diff_count: [*c]c_int = &same_count[@as(c_int, 2)];
    _ = &diff_count;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 2)) : (n += 1) {
            const cand_ref: c_int = cand_b.*.ref.ref[@bitCast(@as(isize, @intCast(n)))];
            _ = &cand_ref;
            if (cand_ref <= @as(c_int, 0)) break;
            var cand_mv: mv = cand_b.*.mv.mv[@bitCast(@as(isize, @intCast(n)))];
            _ = &cand_mv;
            if (cand_ref == @as(c_int, ref.ref[@as(c_int, 0)])) {
                if (same_count[@as(c_int, 0)] < @as(c_int, 2)) {
                    same[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &same_count[@as(c_int, 0)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 0)] = cand_mv;
                }
                if (diff_count[@as(c_int, 1)] < @as(c_int, 2)) {
                    if ((sign1 ^ @as(c_int, sign_bias[@bitCast(@as(isize, @intCast(cand_ref - @as(c_int, 1))))])) != 0) {
                        cand_mv.unnamed_0.y = @truncate(-@as(c_int, cand_mv.unnamed_0.y));
                        cand_mv.unnamed_0.x = @truncate(-@as(c_int, cand_mv.unnamed_0.x));
                    }
                    diff[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &diff_count[@as(c_int, 1)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 1)] = cand_mv;
                }
            } else if (cand_ref == @as(c_int, ref.ref[@as(c_int, 1)])) {
                if (same_count[@as(c_int, 1)] < @as(c_int, 2)) {
                    same[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &same_count[@as(c_int, 1)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 1)] = cand_mv;
                }
                if (diff_count[@as(c_int, 0)] < @as(c_int, 2)) {
                    if ((sign0 ^ @as(c_int, sign_bias[@bitCast(@as(isize, @intCast(cand_ref - @as(c_int, 1))))])) != 0) {
                        cand_mv.unnamed_0.y = @truncate(-@as(c_int, cand_mv.unnamed_0.y));
                        cand_mv.unnamed_0.x = @truncate(-@as(c_int, cand_mv.unnamed_0.x));
                    }
                    diff[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &diff_count[@as(c_int, 0)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 0)] = cand_mv;
                }
            } else {
                var i_cand_mv: mv = union_mv{
                    .unnamed_0 = struct_unnamed_19{
                        .y = @truncate(-@as(c_int, cand_mv.unnamed_0.y)),
                        .x = @truncate(-@as(c_int, cand_mv.unnamed_0.x)),
                    },
                };
                _ = &i_cand_mv;
                if (diff_count[@as(c_int, 0)] < @as(c_int, 2)) {
                    diff[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &diff_count[@as(c_int, 0)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 0)] = if ((sign0 ^ @as(c_int, sign_bias[@bitCast(@as(isize, @intCast(cand_ref - @as(c_int, 1))))])) != 0) i_cand_mv else cand_mv;
                }
                if (diff_count[@as(c_int, 1)] < @as(c_int, 2)) {
                    diff[
                        @bitCast(@as(isize, @intCast(blk: {
                            const ref_1 = &diff_count[@as(c_int, 1)];
                            const tmp = ref_1.*;
                            ref_1.* += 1;
                            break :blk tmp;
                        })))
                    ].mv.mv[@as(c_int, 1)] = if ((sign1 ^ @as(c_int, sign_bias[@bitCast(@as(isize, @intCast(cand_ref - @as(c_int, 1))))])) != 0) i_cand_mv else cand_mv;
                }
            }
        }
    }
}
pub fn add_single_extended_candidate(arg_mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, cand_b: [*c]const refmvs_block, sign: c_int, sign_bias: [*c]const u8) callconv(.c) void {
    var mvstack = arg_mvstack;
    _ = &mvstack;
    _ = &cnt;
    _ = &cand_b;
    _ = &sign;
    _ = &sign_bias;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < @as(c_int, 2)) : (n += 1) {
            const cand_ref: c_int = cand_b.*.ref.ref[@bitCast(@as(isize, @intCast(n)))];
            _ = &cand_ref;
            if (cand_ref <= @as(c_int, 0)) break;
            var cand_mv: mv = cand_b.*.mv.mv[@bitCast(@as(isize, @intCast(n)))];
            _ = &cand_mv;
            if ((sign ^ @as(c_int, sign_bias[@bitCast(@as(isize, @intCast(cand_ref - @as(c_int, 1))))])) != 0) {
                cand_mv.unnamed_0.y = @truncate(-@as(c_int, cand_mv.unnamed_0.y));
                cand_mv.unnamed_0.x = @truncate(-@as(c_int, cand_mv.unnamed_0.x));
            }
            var m: c_int = undefined;
            _ = &m;
            const last: c_int = cnt.*;
            _ = &last;
            {
                m = 0;
                while (m < last) : (m += 1) if (cand_mv.n == mvstack[@bitCast(@as(isize, @intCast(m)))].mv.mv[@as(c_int, 0)].n) break;
            }
            if (m == last) {
                mvstack[@bitCast(@as(isize, @intCast(m)))].mv.mv[@as(c_int, 0)] = cand_mv;
                mvstack[@bitCast(@as(isize, @intCast(m)))].weight = 2;
                cnt.* = last + @as(c_int, 1);
            }
        }
    }
}
pub export fn antfly_av1_dav1d_refmvs_find(rt: [*c]const refmvs_tile, arg_mvstack: [*c]refmvs_candidate, cnt: [*c]c_int, ctx: [*c]c_int, ref: union_refmvs_refpair, bs: enum_BlockSize, edge_flags: enum_EdgeFlags, by4: c_int, bx4: c_int) void {
    _ = &rt;
    var mvstack = arg_mvstack;
    _ = &mvstack;
    _ = &cnt;
    _ = &ctx;
    _ = &ref;
    _ = &bs;
    _ = &edge_flags;
    _ = &by4;
    _ = &bx4;
    const rf: [*c]const refmvs_frame = rt.*.rf;
    _ = &rf;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bw4: c_int = b_dim[@as(c_int, 0)];
    _ = &bw4;
    const w4: c_int = imin(imin(bw4, 16), rt.*.tile_col.end - bx4);
    _ = &w4;
    const bh4: c_int = b_dim[@as(c_int, 1)];
    _ = &bh4;
    const h4: c_int = imin(imin(bh4, 16), rt.*.tile_row.end - by4);
    _ = &h4;
    var gmv: [2]mv = undefined;
    _ = &gmv;
    var tgmv: [2]mv = undefined;
    _ = &tgmv;
    cnt.* = 0;
    while (true) {
        if (!((((@as(c_int, ref.ref[@as(c_int, 0)]) >= @as(c_int, 0)) and (@as(c_int, ref.ref[@as(c_int, 0)]) <= @as(c_int, 8))) and (@as(c_int, ref.ref[@as(c_int, 1)]) >= -@as(c_int, 1))) and (@as(c_int, ref.ref[@as(c_int, 1)]) <= @as(c_int, 8)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (@as(c_int, ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) {
        tgmv[@as(c_int, 0)] = get_gmv_2d(&rf.*.frm_hdr.*.gmv[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))], bx4, by4, bw4, bh4, rf.*.frm_hdr);
        gmv[@as(c_int, 0)] = if (rf.*.frm_hdr.*.gmv[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))].type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)) tgmv[@as(c_int, 0)] else @as(mv, mv{
            .n = 2147516416,
        });
    } else {
        tgmv[@as(c_int, 0)] = mv{
            .n = 0,
        };
        gmv[@as(c_int, 0)] = mv{
            .n = 2147516416,
        };
    }
    if (@as(c_int, ref.ref[@as(c_int, 1)]) > @as(c_int, 0)) {
        tgmv[@as(c_int, 1)] = get_gmv_2d(&rf.*.frm_hdr.*.gmv[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 1)]) - @as(c_int, 1))))], bx4, by4, bw4, bh4, rf.*.frm_hdr);
        gmv[@as(c_int, 1)] = if (rf.*.frm_hdr.*.gmv[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 1)]) - @as(c_int, 1))))].type > @as(enum_Dav1dWarpedMotionType, DAV1D_WM_TYPE_TRANSLATION)) tgmv[@as(c_int, 1)] else @as(mv, mv{
            .n = 2147516416,
        });
    }
    var have_newmv: c_int = 0;
    _ = &have_newmv;
    var have_col_mvs: c_int = 0;
    _ = &have_col_mvs;
    var have_row_mvs: c_int = 0;
    _ = &have_row_mvs;
    var max_rows: c_uint = 0;
    _ = &max_rows;
    var n_rows: c_uint = @bitCast(@as(c_int, ~@as(c_int, 0)));
    _ = &n_rows;
    var b_top: [*c]const refmvs_block = undefined;
    _ = &b_top;
    if (by4 > rt.*.tile_row.start) {
        max_rows = @bitCast(@as(c_int, imin(((by4 - rt.*.tile_row.start) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)), @as(c_int, 2) + @as(c_int, @intFromBool(bh4 > @as(c_int, 1))))));
        b_top = &rt.*.r[@bitCast(@as(isize, @intCast(((by4 & @as(c_int, 31)) - @as(c_int, 1)) + @as(c_int, 5))))][@bitCast(@as(isize, @intCast(bx4)))];
        n_rows = @bitCast(@as(c_int, scan_row(mvstack, cnt, ref, @ptrCast(@alignCast(&gmv)), b_top, bw4, w4, @bitCast(@as(c_uint, @truncate(max_rows))), if (bw4 >= @as(c_int, 16)) @as(c_int, 4) else @as(c_int, 1), &have_newmv, &have_row_mvs)));
    }
    var max_cols: c_uint = 0;
    _ = &max_cols;
    var n_cols: c_uint = ~@as(c_uint, 0);
    _ = &n_cols;
    var b_left: [*c]const [*c]refmvs_block = undefined;
    _ = &b_left;
    if (bx4 > rt.*.tile_col.start) {
        max_cols = @bitCast(@as(c_int, imin(((bx4 - rt.*.tile_col.start) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)), @as(c_int, 2) + @as(c_int, @intFromBool(bw4 > @as(c_int, 1))))));
        b_left = &rt.*.r[@bitCast(@as(isize, @intCast((by4 & @as(c_int, 31)) + @as(c_int, 5))))];
        n_cols = @bitCast(@as(c_int, scan_col(mvstack, cnt, ref, @ptrCast(@alignCast(&gmv)), b_left, bh4, h4, bx4 - @as(c_int, 1), @bitCast(@as(c_uint, @truncate(max_cols))), if (bh4 >= @as(c_int, 16)) @as(c_int, 4) else @as(c_int, 1), &have_newmv, &have_col_mvs)));
    }
    if ((((n_rows != ~@as(c_uint, 0)) and (@as(c_int, @bitCast(edge_flags & @as(enum_EdgeFlags, EDGE_I444_TOP_HAS_RIGHT))) != 0)) and (imax(bw4, bh4) <= @as(c_int, 16))) and ((bw4 + bx4) < rt.*.tile_col.end)) {
        add_spatial_candidate(mvstack, cnt, 4, &b_top[@bitCast(@as(isize, @intCast(bw4)))], ref, @ptrCast(@alignCast(&gmv)), &have_newmv, &have_row_mvs);
    }
    const nearest_match: c_int = have_col_mvs + have_row_mvs;
    _ = &nearest_match;
    const nearest_cnt: c_int = cnt.*;
    _ = &nearest_cnt;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < nearest_cnt) : (n += 1) {
            mvstack[@bitCast(@as(isize, @intCast(n)))].weight += 640;
        }
    }
    var globalmv_ctx: c_int = rf.*.frm_hdr.*.use_ref_frame_mvs;
    _ = &globalmv_ctx;
    if (rf.*.use_ref_frame_mvs != 0) {
        const stride: ptrdiff_t = rf.*.rp_stride;
        _ = &stride;
        const by8: c_int = by4 >> @intCast(@as(c_int, 1));
        _ = &by8;
        const bx8: c_int = bx4 >> @intCast(@as(c_int, 1));
        _ = &bx8;
        const rbi: [*c]const refmvs_temporal_block = &rt.*.rp_proj[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, by8 & @as(c_int, 15)) * stride) + @as(ptrdiff_t, bx8))))];
        _ = &rbi;
        var rb: [*c]const refmvs_temporal_block = rbi;
        _ = &rb;
        const step_h: c_int = if (bw4 >= @as(c_int, 16)) @as(c_int, 2) else @as(c_int, 1);
        _ = &step_h;
        const step_v: c_int = if (bh4 >= @as(c_int, 16)) @as(c_int, 2) else @as(c_int, 1);
        _ = &step_v;
        const w8: c_int = imin((w4 + @as(c_int, 1)) >> @intCast(@as(c_int, 1)), 8);
        _ = &w8;
        const h8: c_int = imin((h4 + @as(c_int, 1)) >> @intCast(@as(c_int, 1)), 8);
        _ = &h8;
        {
            var y: c_int = 0;
            _ = &y;
            while (y < h8) : (y += step_v) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < w8) : (x += step_h) {
                        add_temporal_candidate(rf, mvstack, cnt, &rb[@bitCast(@as(isize, @intCast(x)))], ref, @ptrCast(@alignCast(if (!((x | y) != 0)) @as(?*anyopaque, @ptrCast(@alignCast(&globalmv_ctx))) else @as(?*anyopaque, null))), @ptrCast(@alignCast(&tgmv)));
                    }
                }
                rb += @as(usize, @bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, step_v)))));
            }
        }
        if ((imin(bw4, bh4) >= @as(c_int, 2)) and (imax(bw4, bh4) < @as(c_int, 16))) {
            const bh8: c_int = bh4 >> @intCast(@as(c_int, 1));
            _ = &bh8;
            const bw8: c_int = bw4 >> @intCast(@as(c_int, 1));
            _ = &bw8;
            rb = &rbi[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bh8) * stride)))];
            const has_bottom: c_int = @as(c_int, @intFromBool((by8 + bh8) < imin(rt.*.tile_row.end >> @intCast(@as(c_int, 1)), (by8 & ~@as(c_int, 7)) + @as(c_int, 8))));
            _ = &has_bottom;
            if ((has_bottom != 0) and ((bx8 - @as(c_int, 1)) >= imax(rt.*.tile_col.start >> @intCast(@as(c_int, 1)), bx8 & ~@as(c_int, 7)))) {
                add_temporal_candidate(rf, mvstack, cnt, &rb[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))], ref, null, null);
            }
            if ((bx8 + bw8) < imin(rt.*.tile_col.end >> @intCast(@as(c_int, 1)), (bx8 & ~@as(c_int, 7)) + @as(c_int, 8))) {
                if (has_bottom != 0) {
                    add_temporal_candidate(rf, mvstack, cnt, &rb[@bitCast(@as(isize, @intCast(bw8)))], ref, null, null);
                }
                if (((by8 + bh8) - @as(c_int, 1)) < imin(rt.*.tile_row.end >> @intCast(@as(c_int, 1)), (by8 & ~@as(c_int, 7)) + @as(c_int, 8))) {
                    add_temporal_candidate(rf, mvstack, cnt, &rb[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bw8) - stride)))], ref, null, null);
                }
            }
        }
    }
    while (true) {
        if (!(cnt.* <= @as(c_int, 8))) {
            unreachable;
        }
        if (!false) break;
    }
    var have_dummy_newmv_match: c_int = undefined;
    _ = &have_dummy_newmv_match;
    if ((n_rows | n_cols) != ~@as(c_uint, 0)) {
        add_spatial_candidate(mvstack, cnt, 4, &b_top[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))], ref, @ptrCast(@alignCast(&gmv)), &have_dummy_newmv_match, &have_row_mvs);
    }
    {
        var n: c_int = 2;
        _ = &n;
        while (n <= @as(c_int, 3)) : (n += 1) {
            if ((@as(c_uint, @bitCast(@as(c_int, n))) > n_rows) and (@as(c_uint, @bitCast(@as(c_int, n))) <= max_rows)) {
                n_rows +%= @bitCast(@as(c_int, scan_row(mvstack, cnt, ref, @ptrCast(@alignCast(&gmv)), &rt.*.r[@bitCast(@as(isize, @intCast(((((by4 & @as(c_int, 31)) - (@as(c_int, 2) * n)) + @as(c_int, 1)) | @as(c_int, 1)) + @as(c_int, 5))))][@bitCast(@as(isize, @intCast(bx4 | @as(c_int, 1))))], bw4, w4, @bitCast(@as(c_uint, @truncate((@as(c_uint, 1) +% max_rows) -% @as(c_uint, @bitCast(@as(c_int, n)))))), if (bw4 >= @as(c_int, 16)) @as(c_int, 4) else @as(c_int, 2), &have_dummy_newmv_match, &have_row_mvs)));
            }
            if ((@as(c_uint, @bitCast(@as(c_int, n))) > n_cols) and (@as(c_uint, @bitCast(@as(c_int, n))) <= max_cols)) {
                n_cols +%= @bitCast(@as(c_int, scan_col(mvstack, cnt, ref, @ptrCast(@alignCast(&gmv)), &rt.*.r[@bitCast(@as(isize, @intCast(((by4 & @as(c_int, 31)) | @as(c_int, 1)) + @as(c_int, 5))))], bh4, h4, ((bx4 - (n * @as(c_int, 2))) + @as(c_int, 1)) | @as(c_int, 1), @bitCast(@as(c_uint, @truncate((@as(c_uint, 1) +% max_cols) -% @as(c_uint, @bitCast(@as(c_int, n)))))), if (bh4 >= @as(c_int, 16)) @as(c_int, 4) else @as(c_int, 2), &have_dummy_newmv_match, &have_col_mvs)));
            }
        }
    }
    while (true) {
        if (!(cnt.* <= @as(c_int, 8))) {
            unreachable;
        }
        if (!false) break;
    }
    const ref_match_count: c_int = have_col_mvs + have_row_mvs;
    _ = &ref_match_count;
    var refmv_ctx: c_int = undefined;
    _ = &refmv_ctx;
    var newmv_ctx: c_int = undefined;
    _ = &newmv_ctx;
    blk: {
        switch (nearest_match) {
            @as(c_int, 0) => {
                refmv_ctx = imin(2, ref_match_count);
                newmv_ctx = @as(c_int, @intFromBool(ref_match_count > @as(c_int, 0)));
                break :blk;
            },
            @as(c_int, 1) => {
                refmv_ctx = imin(ref_match_count * @as(c_int, 3), 4);
                newmv_ctx = @as(c_int, 3) - have_newmv;
                break :blk;
            },
            @as(c_int, 2) => {
                refmv_ctx = 5;
                newmv_ctx = @as(c_int, 5) - have_newmv;
                break :blk;
            },
            else => {},
        }
    }
    var len: c_int = nearest_cnt;
    _ = &len;
    while (len != 0) {
        var last: c_int = 0;
        _ = &last;
        {
            var n: c_int = 1;
            _ = &n;
            while (n < len) : (n += 1) {
                if (mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))].weight < mvstack[@bitCast(@as(isize, @intCast(n)))].weight) {
                    while (true) {
                        var tmp: refmvs_candidate = mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))];
                        _ = &tmp;
                        mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))] = mvstack[@bitCast(@as(isize, @intCast(n)))];
                        mvstack[@bitCast(@as(isize, @intCast(n)))] = tmp;
                        if (!false) break;
                    }
                    last = n;
                }
            }
        }
        len = last;
    }
    len = cnt.*;
    while (len > nearest_cnt) {
        var last: c_int = nearest_cnt;
        _ = &last;
        {
            var n: c_int = nearest_cnt + @as(c_int, 1);
            _ = &n;
            while (n < len) : (n += 1) {
                if (mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))].weight < mvstack[@bitCast(@as(isize, @intCast(n)))].weight) {
                    while (true) {
                        var tmp: refmvs_candidate = mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))];
                        _ = &tmp;
                        mvstack[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))] = mvstack[@bitCast(@as(isize, @intCast(n)))];
                        mvstack[@bitCast(@as(isize, @intCast(n)))] = tmp;
                        if (!false) break;
                    }
                    last = n;
                }
            }
        }
        len = last;
    }
    if (@as(c_int, ref.ref[@as(c_int, 1)]) > @as(c_int, 0)) {
        if (cnt.* < @as(c_int, 2)) {
            const sign0: c_int = rf.*.sign_bias[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))];
            _ = &sign0;
            const sign1: c_int = rf.*.sign_bias[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 1)]) - @as(c_int, 1))))];
            _ = &sign1;
            const sz4: c_int = imin(w4, h4);
            _ = &sz4;
            const same: [*c]refmvs_candidate = &mvstack[@bitCast(@as(isize, @intCast(cnt.*)))];
            _ = &same;
            var same_count: [4]c_int = [1]c_int{
                0,
            } ++ @as([3]c_int, @splat(0));
            _ = &same_count;
            if (n_rows != ~@as(c_uint, 0)) {
                var x: c_int = 0;
                _ = &x;
                while (x < sz4) {
                    const cand_b: [*c]const refmvs_block = &b_top[@bitCast(@as(isize, @intCast(x)))];
                    _ = &cand_b;
                    add_compound_extended_candidate(same, @ptrCast(@alignCast(&same_count)), cand_b, sign0, sign1, ref, @ptrCast(@alignCast(&rf.*.sign_bias)));
                    x += antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 0)];
                }
            }
            if (n_cols != ~@as(c_uint, 0)) {
                var y: c_int = 0;
                _ = &y;
                while (y < sz4) {
                    const cand_b: [*c]const refmvs_block = &b_left[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(bx4 - @as(c_int, 1))))];
                    _ = &cand_b;
                    add_compound_extended_candidate(same, @ptrCast(@alignCast(&same_count)), cand_b, sign0, sign1, ref, @ptrCast(@alignCast(&rf.*.sign_bias)));
                    y += antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 1)];
                }
            }
            const diff: [*c]refmvs_candidate = &same[@as(c_int, 2)];
            _ = &diff;
            const diff_count: [*c]const c_int = &same_count[@as(c_int, 2)];
            _ = &diff_count;
            {
                var n: c_int = 0;
                _ = &n;
                while (n < @as(c_int, 2)) : (n += 1) {
                    var m: c_int = same_count[@bitCast(@as(isize, @intCast(n)))];
                    _ = &m;
                    if (m >= @as(c_int, 2)) continue;
                    const l: c_int = diff_count[@bitCast(@as(isize, @intCast(n)))];
                    _ = &l;
                    if (l != 0) {
                        same[@bitCast(@as(isize, @intCast(m)))].mv.mv[@bitCast(@as(isize, @intCast(n)))] = diff[@as(c_int, 0)].mv.mv[@bitCast(@as(isize, @intCast(n)))];
                        if ((blk: {
                            const ref_1 = &m;
                            ref_1.* += 1;
                            break :blk ref_1.*;
                        }) == @as(c_int, 2)) continue;
                        if (l == @as(c_int, 2)) {
                            same[@as(c_int, 1)].mv.mv[@bitCast(@as(isize, @intCast(n)))] = diff[@as(c_int, 1)].mv.mv[@bitCast(@as(isize, @intCast(n)))];
                            continue;
                        }
                    }
                    while (true) {
                        same[@bitCast(@as(isize, @intCast(m)))].mv.mv[@bitCast(@as(isize, @intCast(n)))] = tgmv[@bitCast(@as(isize, @intCast(n)))];
                        if (!((blk: {
                            const ref_1 = &m;
                            ref_1.* += 1;
                            break :blk ref_1.*;
                        }) < @as(c_int, 2))) break;
                    }
                }
            }
            var n: c_int = cnt.*;
            _ = &n;
            if ((n == @as(c_int, 1)) and (mvstack[@as(c_int, 0)].mv.n == same[@as(c_int, 0)].mv.n)) {
                mvstack[@as(c_int, 1)].mv = mvstack[@as(c_int, 2)].mv;
            }
            while (true) {
                mvstack[@bitCast(@as(isize, @intCast(n)))].weight = 2;
                if (!((blk: {
                    const ref_1 = &n;
                    ref_1.* += 1;
                    break :blk ref_1.*;
                }) < @as(c_int, 2))) break;
            }
            cnt.* = 2;
        }
        const left: c_int = (-((bx4 + bw4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &left;
        const right: c_int = (((rf.*.iw4 - bx4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &right;
        const top: c_int = (-((by4 + bh4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &top;
        const bottom: c_int = (((rf.*.ih4 - by4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &bottom;
        const n_refmvs: c_int = cnt.*;
        _ = &n_refmvs;
        var n: c_int = 0;
        _ = &n;
        while (true) {
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.x = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.x, left, right));
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.y = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.y, top, bottom));
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 1)].unnamed_0.x = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 1)].unnamed_0.x, left, right));
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 1)].unnamed_0.y = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 1)].unnamed_0.y, top, bottom));
            if (!((blk: {
                const ref_1 = &n;
                ref_1.* += 1;
                break :blk ref_1.*;
            }) < n_refmvs)) break;
        }
        blk: {
            switch (refmv_ctx >> @intCast(@as(c_int, 1))) {
                @as(c_int, 0) => {
                    ctx.* = imin(newmv_ctx, 1);
                    break :blk;
                },
                @as(c_int, 1) => {
                    ctx.* = @as(c_int, 1) + imin(newmv_ctx, 3);
                    break :blk;
                },
                @as(c_int, 2) => {
                    ctx.* = iclip(@as(c_int, 3) + newmv_ctx, 4, 7);
                    break :blk;
                },
                else => {},
            }
        }
        return;
    } else if ((cnt.* < @as(c_int, 2)) and (@as(c_int, ref.ref[@as(c_int, 0)]) > @as(c_int, 0))) {
        const sign: c_int = rf.*.sign_bias[@bitCast(@as(isize, @intCast(@as(c_int, ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))];
        _ = &sign;
        const sz4: c_int = imin(w4, h4);
        _ = &sz4;
        if (n_rows != ~@as(c_uint, 0)) {
            var x: c_int = 0;
            _ = &x;
            while ((x < sz4) and (cnt.* < @as(c_int, 2))) {
                const cand_b: [*c]const refmvs_block = &b_top[@bitCast(@as(isize, @intCast(x)))];
                _ = &cand_b;
                add_single_extended_candidate(mvstack, cnt, cand_b, sign, @ptrCast(@alignCast(&rf.*.sign_bias)));
                x += antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 0)];
            }
        }
        if (n_cols != ~@as(c_uint, 0)) {
            var y: c_int = 0;
            _ = &y;
            while ((y < sz4) and (cnt.* < @as(c_int, 2))) {
                const cand_b: [*c]const refmvs_block = &b_left[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(bx4 - @as(c_int, 1))))];
                _ = &cand_b;
                add_single_extended_candidate(mvstack, cnt, cand_b, sign, @ptrCast(@alignCast(&rf.*.sign_bias)));
                y += antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 1)];
            }
        }
    }
    while (true) {
        if (!(cnt.* <= @as(c_int, 8))) {
            unreachable;
        }
        if (!false) break;
    }
    var n_refmvs: c_int = cnt.*;
    _ = &n_refmvs;
    if (n_refmvs != 0) {
        const left: c_int = (-((bx4 + bw4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &left;
        const right: c_int = (((rf.*.iw4 - bx4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &right;
        const top: c_int = (-((by4 + bh4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &top;
        const bottom: c_int = (((rf.*.ih4 - by4) + @as(c_int, 4)) * @as(c_int, 4)) * @as(c_int, 8);
        _ = &bottom;
        var n: c_int = 0;
        _ = &n;
        while (true) {
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.x = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.x, left, right));
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.y = @truncate(iclip(mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)].unnamed_0.y, top, bottom));
            if (!((blk: {
                const ref_1 = &n;
                ref_1.* += 1;
                break :blk ref_1.*;
            }) < n_refmvs)) break;
        }
    }
    {
        var n: c_int = cnt.*;
        _ = &n;
        while (n < @as(c_int, 2)) : (n += 1) {
            mvstack[@bitCast(@as(isize, @intCast(n)))].mv.mv[@as(c_int, 0)] = tgmv[@as(c_int, 0)];
        }
    }
    ctx.* = ((refmv_ctx << @intCast(@as(c_int, 4))) | (globalmv_ctx << @intCast(@as(c_int, 3)))) | newmv_ctx;
}
pub export fn antfly_av1_dav1d_refmvs_tile_sbrow_init(rt: [*c]refmvs_tile, rf: [*c]const refmvs_frame, tile_col_start4: c_int, tile_col_end4: c_int, tile_row_start4: c_int, tile_row_end4: c_int, sby: c_int, arg_tile_row_idx: c_int, pass: c_int) void {
    _ = &rt;
    _ = &rf;
    _ = &tile_col_start4;
    _ = &tile_col_end4;
    _ = &tile_row_start4;
    _ = &tile_row_end4;
    _ = &sby;
    var tile_row_idx = arg_tile_row_idx;
    _ = &tile_row_idx;
    _ = &pass;
    if (rf.*.n_tile_threads == @as(c_int, 1)) {
        tile_row_idx = 0;
    }
    rt.*.rp_proj = &rf.*.rp_proj[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 16) * rf.*.rp_stride) * @as(ptrdiff_t, tile_row_idx))))];
    const r_stride: ptrdiff_t = rf.*.rp_stride * @as(ptrdiff_t, 2);
    _ = &r_stride;
    const pass_off: ptrdiff_t = if ((rf.*.n_frame_threads > @as(c_int, 1)) and (pass == @as(c_int, 2))) (@as(c_int, 35) * @as(c_int, 2)) * rf.*.n_blocks else @as(c_int, 0);
    _ = &pass_off;
    var r: [*c]refmvs_block = &rf.*.r[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, 35) * r_stride) * @as(ptrdiff_t, tile_row_idx)) + pass_off)))];
    _ = &r;
    const sbsz: c_int = rf.*.sbsz;
    _ = &sbsz;
    const off: c_int = (sbsz * sby) & @as(c_int, 16);
    _ = &off;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < sbsz) : ({
            i += 1;
            r += @as(usize, @bitCast(@as(isize, @intCast(r_stride))));
        }) {
            rt.*.r[@bitCast(@as(isize, @intCast((off + @as(c_int, 5)) + i)))] = r;
        }
    }
    rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 0))))] = r;
    r += @as(usize, @bitCast(@as(isize, @intCast(r_stride))));
    rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 1))))] = null;
    rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 2))))] = r;
    r += @as(usize, @bitCast(@as(isize, @intCast(r_stride))));
    rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 3))))] = null;
    rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 4))))] = r;
    if ((sby & @as(c_int, 1)) != 0) {
        while (true) {
            const tmp: ?*anyopaque = @ptrCast(@alignCast(rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 0))))]));
            _ = &tmp;
            rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 0))))] = rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 0))))];
            rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 0))))] = @ptrCast(@alignCast(tmp));
            if (!false) break;
        }
        while (true) {
            const tmp: ?*anyopaque = @ptrCast(@alignCast(rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 2))))]));
            _ = &tmp;
            rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 2))))] = rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 2))))];
            rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 2))))] = @ptrCast(@alignCast(tmp));
            if (!false) break;
        }
        while (true) {
            const tmp: ?*anyopaque = @ptrCast(@alignCast(rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 4))))]));
            _ = &tmp;
            rt.*.r[@bitCast(@as(isize, @intCast(off + @as(c_int, 4))))] = rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 4))))];
            rt.*.r[@bitCast(@as(isize, @intCast((off + sbsz) + @as(c_int, 4))))] = @ptrCast(@alignCast(tmp));
            if (!false) break;
        }
    }
    rt.*.rf = rf;
    rt.*.tile_row.start = tile_row_start4;
    rt.*.tile_row.end = imin(tile_row_end4, rf.*.ih4);
    rt.*.tile_col.start = tile_col_start4;
    rt.*.tile_col.end = imin(tile_col_end4, rf.*.iw4);
}
pub fn load_tmvs_c(rf: [*c]const refmvs_frame, arg_tile_row_idx: c_int, col_start8: c_int, col_end8: c_int, row_start8: c_int, arg_row_end8: c_int) callconv(.c) void {
    _ = &rf;
    var tile_row_idx = arg_tile_row_idx;
    _ = &tile_row_idx;
    _ = &col_start8;
    _ = &col_end8;
    _ = &row_start8;
    var row_end8 = arg_row_end8;
    _ = &row_end8;
    if (rf.*.n_tile_threads == @as(c_int, 1)) {
        tile_row_idx = 0;
    }
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
    const col_start8i: c_int = imax(col_start8 - @as(c_int, 8), 0);
    _ = &col_start8i;
    const col_end8i: c_int = imin(col_end8 + @as(c_int, 8), rf.*.iw8);
    _ = &col_end8i;
    const stride: ptrdiff_t = rf.*.rp_stride;
    _ = &stride;
    var rp_proj: [*c]refmvs_temporal_block = &rf.*.rp_proj[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, 16) * stride) * @as(ptrdiff_t, tile_row_idx)) + (@as(ptrdiff_t, row_start8 & @as(c_int, 15)) * stride))))];
    _ = &rp_proj;
    {
        var y: c_int = row_start8;
        _ = &y;
        while (y < row_end8) : (y += 1) {
            {
                var x: c_int = col_start8;
                _ = &x;
                while (x < col_end8) : (x += 1) {
                    rp_proj[@bitCast(@as(isize, @intCast(x)))].mv.n = 2147516416;
                }
            }
            rp_proj += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        }
    }
    rp_proj = &rf.*.rp_proj[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, 16) * stride) * @as(ptrdiff_t, tile_row_idx))))];
    {
        var n: c_int = 0;
        _ = &n;
        while (n < rf.*.n_mfmvs) : (n += 1) {
            const ref2cur: c_int = rf.*.mfmv_ref2cur[@bitCast(@as(isize, @intCast(n)))];
            _ = &ref2cur;
            if (ref2cur == (-@as(c_int, 2147483647) - @as(c_int, 1))) continue;
            const ref: c_int = rf.*.mfmv_ref[@bitCast(@as(isize, @intCast(n)))];
            _ = &ref;
            const ref_sign: c_int = ref - @as(c_int, 4);
            _ = &ref_sign;
            var r: [*c]const refmvs_temporal_block = &rf.*.rp_ref[@bitCast(@as(isize, @intCast(ref)))][@bitCast(@as(isize, @intCast(@as(ptrdiff_t, row_start8) * stride)))];
            _ = &r;
            {
                var y: c_int = row_start8;
                _ = &y;
                while (y < row_end8) : (y += 1) {
                    const y_sb_align: c_int = y & ~@as(c_int, 7);
                    _ = &y_sb_align;
                    const y_proj_start: c_int = imax(y_sb_align, row_start8);
                    _ = &y_proj_start;
                    const y_proj_end: c_int = imin(y_sb_align + @as(c_int, 8), row_end8);
                    _ = &y_proj_end;
                    {
                        var x: c_int = col_start8i;
                        _ = &x;
                        while (x < col_end8i) : (x += 1) {
                            var rb: [*c]const refmvs_temporal_block = &r[@bitCast(@as(isize, @intCast(x)))];
                            _ = &rb;
                            const b_ref: c_int = rb.*.ref;
                            _ = &b_ref;
                            if (!(b_ref != 0)) continue;
                            const ref2ref: c_int = rf.*.mfmv_ref2ref[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(b_ref - @as(c_int, 1))))];
                            _ = &ref2ref;
                            if (!(ref2ref != 0)) continue;
                            const b_mv: mv = rb.*.mv;
                            _ = &b_mv;
                            const offset: mv = mv_projection(b_mv, ref2cur, ref2ref);
                            _ = &offset;
                            var pos_x: c_int = x + apply_sign(antfly_codec_abs(offset.unnamed_0.x) >> @intCast(@as(c_int, 6)), @as(c_int, offset.unnamed_0.x) ^ ref_sign);
                            _ = &pos_x;
                            const pos_y: c_int = y + apply_sign(antfly_codec_abs(offset.unnamed_0.y) >> @intCast(@as(c_int, 6)), @as(c_int, offset.unnamed_0.y) ^ ref_sign);
                            _ = &pos_y;
                            if ((pos_y >= y_proj_start) and (pos_y < y_proj_end)) {
                                const pos: ptrdiff_t = @as(ptrdiff_t, pos_y & @as(c_int, 15)) * stride;
                                _ = &pos;
                                while (true) {
                                    const x_sb_align: c_int = x & ~@as(c_int, 7);
                                    _ = &x_sb_align;
                                    if ((pos_x >= imax(x_sb_align - @as(c_int, 8), col_start8)) and (pos_x < imin(x_sb_align + @as(c_int, 16), col_end8))) {
                                        rp_proj[@bitCast(@as(isize, @intCast(pos + @as(ptrdiff_t, pos_x))))].mv = rb.*.mv;
                                        rp_proj[@bitCast(@as(isize, @intCast(pos + @as(ptrdiff_t, pos_x))))].ref = @truncate(ref2ref);
                                    }
                                    if ((blk: {
                                        const ref_1 = &x;
                                        ref_1.* += 1;
                                        break :blk ref_1.*;
                                    }) >= col_end8i) break;
                                    rb += 1;
                                    if ((@as(c_int, rb.*.ref) != b_ref) or (rb.*.mv.n != b_mv.n)) break;
                                    pos_x += 1;
                                }
                            } else {
                                while (true) {
                                    if ((blk: {
                                        const ref_1 = &x;
                                        ref_1.* += 1;
                                        break :blk ref_1.*;
                                    }) >= col_end8i) break;
                                    rb += 1;
                                    if ((@as(c_int, rb.*.ref) != b_ref) or (rb.*.mv.n != b_mv.n)) break;
                                }
                            }
                            x -= 1;
                        }
                    }
                    r += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                }
            }
        }
    }
}
pub fn save_tmvs_c(arg_rp: [*c]refmvs_temporal_block, stride: ptrdiff_t, rr: [*c]const [*c]refmvs_block, ref_sign: [*c]const u8, col_end8: c_int, row_end8: c_int, col_start8: c_int, row_start8: c_int) callconv(.c) void {
    var rp = arg_rp;
    _ = &rp;
    _ = &stride;
    _ = &rr;
    _ = &ref_sign;
    _ = &col_end8;
    _ = &row_end8;
    _ = &col_start8;
    _ = &row_start8;
    {
        var y: c_int = row_start8;
        _ = &y;
        while (y < row_end8) : (y += 1) {
            const b: [*c]const refmvs_block = rr[@bitCast(@as(isize, @intCast((y & @as(c_int, 15)) * @as(c_int, 2))))];
            _ = &b;
            {
                var x: c_int = col_start8;
                _ = &x;
                while (x < col_end8) {
                    const cand_b: [*c]const refmvs_block = &b[@bitCast(@as(isize, @intCast((x * @as(c_int, 2)) + @as(c_int, 1))))];
                    _ = &cand_b;
                    const bw8: c_int = (@as(c_int, antfly_av1_dav1d_block_dimensions[cand_b.*.bs][@as(c_int, 0)]) + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
                    _ = &bw8;
                    if (((@as(c_int, cand_b.*.ref.ref[@as(c_int, 1)]) > @as(c_int, 0)) and (@as(c_int, ref_sign[@bitCast(@as(isize, @intCast(@as(c_int, cand_b.*.ref.ref[@as(c_int, 1)]) - @as(c_int, 1))))]) != 0)) and ((antfly_codec_abs(cand_b.*.mv.mv[@as(c_int, 1)].unnamed_0.y) | antfly_codec_abs(cand_b.*.mv.mv[@as(c_int, 1)].unnamed_0.x)) < @as(c_int, 4096))) {
                        {
                            var n: c_int = 0;
                            _ = &n;
                            while (n < bw8) : ({
                                n += 1;
                                x += 1;
                            }) {
                                rp[@bitCast(@as(isize, @intCast(x)))] = refmvs_temporal_block{
                                    .mv = cand_b.*.mv.mv[@as(c_int, 1)],
                                    .ref = cand_b.*.ref.ref[@as(c_int, 1)],
                                };
                            }
                        }
                    } else if (((@as(c_int, cand_b.*.ref.ref[@as(c_int, 0)]) > @as(c_int, 0)) and (@as(c_int, ref_sign[@bitCast(@as(isize, @intCast(@as(c_int, cand_b.*.ref.ref[@as(c_int, 0)]) - @as(c_int, 1))))]) != 0)) and ((antfly_codec_abs(cand_b.*.mv.mv[@as(c_int, 0)].unnamed_0.y) | antfly_codec_abs(cand_b.*.mv.mv[@as(c_int, 0)].unnamed_0.x)) < @as(c_int, 4096))) {
                        {
                            var n: c_int = 0;
                            _ = &n;
                            while (n < bw8) : ({
                                n += 1;
                                x += 1;
                            }) {
                                rp[@bitCast(@as(isize, @intCast(x)))] = refmvs_temporal_block{
                                    .mv = cand_b.*.mv.mv[@as(c_int, 0)],
                                    .ref = cand_b.*.ref.ref[@as(c_int, 0)],
                                };
                            }
                        }
                    } else {
                        {
                            var n: c_int = 0;
                            _ = &n;
                            while (n < bw8) : ({
                                n += 1;
                                x += 1;
                            }) {
                                rp[@bitCast(@as(isize, @intCast(x)))].mv.n = 0;
                                rp[@bitCast(@as(isize, @intCast(x)))].ref = 0;
                            }
                        }
                    }
                }
            }
            rp += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        }
    }
}
pub export fn antfly_av1_dav1d_refmvs_init_frame(rf: [*c]refmvs_frame, seq_hdr: [*c]const Dav1dSequenceHeader, frm_hdr: [*c]const Dav1dFrameHeader, arg_ref_poc: [*c]const c_uint, rp: [*c]refmvs_temporal_block, arg_ref_ref_poc: [*c][7]c_uint, arg_rp_ref: [*c]const [*c]refmvs_temporal_block, n_tile_threads: c_int, n_frame_threads: c_int) c_int {
    _ = &rf;
    _ = &seq_hdr;
    _ = &frm_hdr;
    var ref_poc = arg_ref_poc;
    _ = &ref_poc;
    _ = &rp;
    var ref_ref_poc = arg_ref_ref_poc;
    _ = &ref_ref_poc;
    var rp_ref = arg_rp_ref;
    _ = &rp_ref;
    _ = &n_tile_threads;
    _ = &n_frame_threads;
    const rp_stride: c_int = ((frm_hdr.*.width[@as(c_int, 0)] + @as(c_int, 127)) & ~@as(c_int, 127)) >> @intCast(@as(c_int, 3));
    _ = &rp_stride;
    const n_tile_rows: c_int = if (n_tile_threads > @as(c_int, 1)) @as(c_int, frm_hdr.*.tiling.rows) else @as(c_int, 1);
    _ = &n_tile_rows;
    const n_blocks: c_int = rp_stride * n_tile_rows;
    _ = &n_blocks;
    rf.*.sbsz = @as(c_int, 16) << @intCast(@as(c_int, seq_hdr.*.sb128));
    rf.*.frm_hdr = frm_hdr;
    rf.*.iw8 = (frm_hdr.*.width[@as(c_int, 0)] + @as(c_int, 7)) >> @intCast(@as(c_int, 3));
    rf.*.ih8 = (frm_hdr.*.height + @as(c_int, 7)) >> @intCast(@as(c_int, 3));
    rf.*.iw4 = rf.*.iw8 << @intCast(@as(c_int, 1));
    rf.*.ih4 = rf.*.ih8 << @intCast(@as(c_int, 1));
    rf.*.rp = rp;
    rf.*.rp_stride = rp_stride;
    rf.*.n_tile_threads = n_tile_threads;
    rf.*.n_frame_threads = n_frame_threads;
    if (n_blocks != rf.*.n_blocks) {
        const r_sz: usize = (((@sizeOf(@TypeOf(rf.*.r.*)) *% @as(c_ulong, 35)) *% @as(c_ulong, 2)) *% @as(c_ulong, @bitCast(@as(c_long, n_blocks)))) *% @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 1) + @as(c_int, @intFromBool(n_frame_threads > @as(c_int, 1))))));
        _ = &r_sz;
        const rp_proj_sz: usize = (@sizeOf(@TypeOf(rf.*.rp_proj.*)) *% @as(c_ulong, 16)) *% @as(c_ulong, @bitCast(@as(c_long, n_blocks)));
        _ = &rp_proj_sz;
        dav1d_free_aligned_internal(@ptrCast(@alignCast(rf.*.r)));
        rf.*.r = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(r_sz +% rp_proj_sz, 64)));
        if (!(rf.*.r != null)) {
            rf.*.n_blocks = 0;
            return -@as(c_int, 12);
        }
        rf.*.rp_proj = @ptrFromInt(@as(usize, @intCast(@intFromPtr(rf.*.r))) +% r_sz);
        rf.*.n_blocks = n_blocks;
    }
    const poc: c_uint = frm_hdr.*.frame_offset;
    _ = &poc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 7)) : (i += 1) {
            const poc_diff: c_int = get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(ref_poc[@bitCast(@as(isize, @intCast(i)))]))), @bitCast(@as(c_uint, @truncate(poc))));
            _ = &poc_diff;
            rf.*.sign_bias[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(poc_diff > @as(c_int, 0))))));
            rf.*.mfmv_sign[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @intCast(@as(c_int, @intFromBool(poc_diff < @as(c_int, 0))))));
            rf.*.pocdiff[@bitCast(@as(isize, @intCast(i)))] = @truncate(iclip(get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(poc))), @bitCast(@as(c_uint, @truncate(ref_poc[@bitCast(@as(isize, @intCast(i)))])))), -@as(c_int, 31), 31));
        }
    }
    rf.*.n_mfmvs = 0;
    rf.*.rp_ref = rp_ref;
    if ((@as(c_int, frm_hdr.*.use_ref_frame_mvs) != 0) and (@as(c_int, seq_hdr.*.order_hint_n_bits) != 0)) {
        var total: c_int = 2;
        _ = &total;
        if ((rp_ref[@as(c_int, 0)] != null) and (ref_ref_poc[@as(c_int, 0)][@as(c_int, 6)] != ref_poc[@as(c_int, 3)])) {
            rf.*.mfmv_ref[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &rf.*.n_mfmvs;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = 0;
            total = 3;
        }
        if ((rp_ref[@as(c_int, 4)] != null) and (get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(ref_poc[@as(c_int, 4)]))), frm_hdr.*.frame_offset) > @as(c_int, 0))) {
            rf.*.mfmv_ref[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &rf.*.n_mfmvs;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = 4;
        }
        if ((rp_ref[@as(c_int, 5)] != null) and (get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(ref_poc[@as(c_int, 5)]))), frm_hdr.*.frame_offset) > @as(c_int, 0))) {
            rf.*.mfmv_ref[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &rf.*.n_mfmvs;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = 5;
        }
        if (((rf.*.n_mfmvs < total) and (rp_ref[@as(c_int, 6)] != null)) and (get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(ref_poc[@as(c_int, 6)]))), frm_hdr.*.frame_offset) > @as(c_int, 0))) {
            rf.*.mfmv_ref[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &rf.*.n_mfmvs;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = 6;
        }
        if ((rf.*.n_mfmvs < total) and (rp_ref[@as(c_int, 1)] != null)) {
            rf.*.mfmv_ref[
                @bitCast(@as(isize, @intCast(blk: {
                    const ref = &rf.*.n_mfmvs;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                })))
            ] = 1;
        }
        {
            var n: c_int = 0;
            _ = &n;
            while (n < rf.*.n_mfmvs) : (n += 1) {
                const rpoc: c_uint = ref_poc[rf.*.mfmv_ref[@bitCast(@as(isize, @intCast(n)))]];
                _ = &rpoc;
                const diff1: c_int = get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(rpoc))), frm_hdr.*.frame_offset);
                _ = &diff1;
                if (antfly_codec_abs(diff1) > @as(c_int, 31)) {
                    rf.*.mfmv_ref2cur[@bitCast(@as(isize, @intCast(n)))] = -@as(c_int, 2147483647) - @as(c_int, 1);
                } else {
                    rf.*.mfmv_ref2cur[@bitCast(@as(isize, @intCast(n)))] = if (@as(c_int, rf.*.mfmv_ref[@bitCast(@as(isize, @intCast(n)))]) < @as(c_int, 4)) -diff1 else diff1;
                    {
                        var m: c_int = 0;
                        _ = &m;
                        while (m < @as(c_int, 7)) : (m += 1) {
                            const rrpoc: c_uint = ref_ref_poc[rf.*.mfmv_ref[@bitCast(@as(isize, @intCast(n)))]][@bitCast(@as(isize, @intCast(m)))];
                            _ = &rrpoc;
                            const diff2: c_int = get_poc_diff(seq_hdr.*.order_hint_n_bits, @bitCast(@as(c_uint, @truncate(rpoc))), @bitCast(@as(c_uint, @truncate(rrpoc))));
                            _ = &diff2;
                            rf.*.mfmv_ref2ref[@bitCast(@as(isize, @intCast(n)))][@bitCast(@as(isize, @intCast(m)))] = if (@as(c_uint, @bitCast(@as(c_int, diff2))) > @as(c_uint, 31)) @as(c_int, 0) else diff2;
                        }
                    }
                }
            }
        }
    }
    rf.*.use_ref_frame_mvs = @as(c_int, @intFromBool(rf.*.n_mfmvs > @as(c_int, 0)));
    return 0;
}
pub fn splat_mv_c(arg_rr: [*c][*c]refmvs_block, rmv: [*c]const refmvs_block, bx4: c_int, bw4: c_int, arg_bh4: c_int) callconv(.c) void {
    var rr = arg_rr;
    _ = &rr;
    _ = &rmv;
    _ = &bx4;
    _ = &bw4;
    var bh4 = arg_bh4;
    _ = &bh4;
    while (true) {
        const r: [*c]refmvs_block = (blk: {
            const ref = &rr;
            const tmp = ref.*;
            ref.* += 1;
            break :blk tmp;
        }).* + @as(usize, @bitCast(@as(isize, @intCast(bx4))));
        _ = &r;
        {
            var x: c_int = 0;
            _ = &x;
            while (x < bw4) : (x += 1) {
                r[@bitCast(@as(isize, @intCast(x)))] = rmv.*;
            }
        }
        if (!((blk: {
            const ref = &bh4;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
}
pub export fn antfly_av1_dav1d_refmvs_dsp_init(c: [*c]Dav1dRefmvsDSPContext) void {
    _ = &c;
    c.*.load_tmvs = load_tmvs_c;
    c.*.save_tmvs = save_tmvs_c;
    c.*.splat_mv = splat_mv_c;
}
