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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const ptrdiff_t = common.ptrdiff_t;
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
pub const enum_Dav1dTxfmMode = common.enum_Dav1dTxfmMode;
pub const enum_Dav1dFilterMode = common.enum_Dav1dFilterMode;
pub const enum_Dav1dRestorationType = common.enum_Dav1dRestorationType;
pub const enum_Dav1dWarpedMotionType = common.enum_Dav1dWarpedMotionType;
const struct_unnamed_2 = extern struct {
    alpha: i16 = 0,
    beta: i16 = 0,
    gamma: i16 = 0,
    delta: i16 = 0,
};
const union_unnamed_1 = extern union {
    p: struct_unnamed_2,
    abcd: [4]i16,
};
pub const struct_Dav1dWarpedMotionParams = extern struct {
    type: enum_Dav1dWarpedMotionType = @import("std").mem.zeroes(enum_Dav1dWarpedMotionType),
    matrix: [6]i32 = @import("std").mem.zeroes([6]i32),
    u: union_unnamed_1 = @import("std").mem.zeroes(union_unnamed_1),
};
pub const Dav1dWarpedMotionParams = struct_Dav1dWarpedMotionParams;
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const enum_Dav1dPixelLayout = common.enum_Dav1dPixelLayout;
pub const enum_Dav1dFrameType = common.enum_Dav1dFrameType;
pub const Dav1dSegmentationData = common.Dav1dSegmentationData;
pub const Dav1dSegmentationDataSet = common.Dav1dSegmentationDataSet;
pub const Dav1dLoopfilterModeRefDeltas = common.Dav1dLoopfilterModeRefDeltas;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
const struct_unnamed_5 = extern struct {
    data: Dav1dFilmGrainData = @import("std").mem.zeroes(Dav1dFilmGrainData),
    present: u8 = 0,
    update: u8 = 0,
};
pub const struct_Dav1dFrameHeaderOperatingPoint_6 = common.struct_Dav1dFrameHeaderOperatingPoint_6;
const struct_unnamed_7 = extern struct {
    width_scale_denominator: u8 = 0,
    enabled: u8 = 0,
};
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
const struct_unnamed_16 = extern struct {
    type: [3]enum_Dav1dRestorationType = @import("std").mem.zeroes([3]enum_Dav1dRestorationType),
    unit_size: [2]u8 = @import("std").mem.zeroes([2]u8),
};
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
pub const TX_4X4 = common.TX_4X4;
pub const enum_RectTxfmSize = common.enum_RectTxfmSize;
pub const enum_BlockSize = common.enum_BlockSize;
pub const Av1FilterLUT = common.Av1FilterLUT;
pub const Av1Filter = common.Av1Filter;
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
pub fn decomp_tx(txa: [*c][2][32][32]u8, from: enum_RectTxfmSize, depth: c_int, y_off: c_int, x_off: c_int, tx_masks: [*c]const u16) callconv(.c) void {
    _ = &txa;
    _ = &from;
    _ = &depth;
    _ = &y_off;
    _ = &x_off;
    _ = &tx_masks;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[from];
    _ = &t_dim;
    const is_split: c_int = if ((from == @as(enum_RectTxfmSize, @bitCast(TX_4X4))) or (depth > @as(c_int, 1))) @as(c_int, 0) else (@as(c_int, tx_masks[@bitCast(@as(isize, @intCast(depth)))]) >> @intCast((y_off * @as(c_int, 4)) + x_off)) & @as(c_int, 1);
    _ = &is_split;
    if (is_split != 0) {
        const sub: enum_RectTxfmSize = t_dim.*.sub;
        _ = &sub;
        const htw4: c_int = @as(c_int, t_dim.*.w) >> @intCast(@as(c_int, 1));
        _ = &htw4;
        const hth4: c_int = @as(c_int, t_dim.*.h) >> @intCast(@as(c_int, 1));
        _ = &hth4;
        decomp_tx(txa, sub, depth + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 0), (x_off * @as(c_int, 2)) + @as(c_int, 0), tx_masks);
        if (@as(c_int, t_dim.*.w) >= @as(c_int, t_dim.*.h)) {
            decomp_tx(@ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(htw4)))])), sub, depth + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 0), (x_off * @as(c_int, 2)) + @as(c_int, 1), tx_masks);
        }
        if (@as(c_int, t_dim.*.h) >= @as(c_int, t_dim.*.w)) {
            decomp_tx(@ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(hth4)))][@as(c_int, 0)])), sub, depth + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 1), (x_off * @as(c_int, 2)) + @as(c_int, 0), tx_masks);
            if (@as(c_int, t_dim.*.w) >= @as(c_int, t_dim.*.h)) {
                decomp_tx(@ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(hth4)))][@bitCast(@as(isize, @intCast(htw4)))])), sub, depth + @as(c_int, 1), (y_off * @as(c_int, 2)) + @as(c_int, 1), (x_off * @as(c_int, 2)) + @as(c_int, 1), tx_masks);
            }
        }
    } else {
        const lw: c_int = imin(2, t_dim.*.lw);
        _ = &lw;
        const lh: c_int = imin(2, t_dim.*.lh);
        _ = &lh;
        blk: {
            switch (@as(c_int, t_dim.*.lw)) {
                @as(c_int, 0) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < @as(c_int, t_dim.*.h)) : (y += 1) {
                            @as([*c]union_alias8, @ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u8 = @bitCast(@as(i8, @truncate(lw * @as(c_int, 1))));
                            @as([*c]union_alias8, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u8 = @bitCast(@as(i8, @truncate(lh * @as(c_int, 1))));
                            txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)] = t_dim.*.w;
                        }
                    }
                    break :blk;
                },
                @as(c_int, 1) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < @as(c_int, t_dim.*.h)) : (y += 1) {
                            @as([*c]union_alias16, @ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u16 = @bitCast(@as(c_short, @truncate(lw * @as(c_int, 257))));
                            @as([*c]union_alias16, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u16 = @bitCast(@as(c_short, @truncate(lh * @as(c_int, 257))));
                            txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)] = t_dim.*.w;
                        }
                    }
                    break :blk;
                },
                @as(c_int, 2) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < @as(c_int, t_dim.*.h)) : (y += 1) {
                            @as([*c]union_alias32, @ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, lw))) *% @as(c_uint, 16843009);
                            @as([*c]union_alias32, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, lh))) *% @as(c_uint, 16843009);
                            txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)] = t_dim.*.w;
                        }
                    }
                    break :blk;
                },
                @as(c_int, 3) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < @as(c_int, t_dim.*.h)) : (y += 1) {
                            @as([*c]union_alias64, @ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, lw))) *% @as(c_ulonglong, 72340172838076673);
                            @as([*c]union_alias64, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, lh))) *% @as(c_ulonglong, 72340172838076673);
                            txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)] = t_dim.*.w;
                        }
                    }
                    break :blk;
                },
                @as(c_int, 4) => {
                    {
                        var y: c_int = 0;
                        _ = &y;
                        while (y < @as(c_int, t_dim.*.h)) : (y += 1) {
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)])), lw, 16);
                                if (!false) break;
                            }
                            while (true) {
                                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)])), lh, 16);
                                if (!false) break;
                            }
                            txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)] = t_dim.*.w;
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
        antfly_av1_dav1d_memset_pow2[t_dim.*.lw].?(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 0)]))))), t_dim.*.h);
    }
}
pub fn mask_edges_inter(masks: [*c][32][3][2]u16, by4: c_int, bx4: c_int, w4: c_int, h4: c_int, skip: c_int, max_tx: enum_RectTxfmSize, tx_masks: [*c]const u16, a: [*c]u8, l: [*c]u8) callconv(.c) void {
    _ = &masks;
    _ = &by4;
    _ = &bx4;
    _ = &w4;
    _ = &h4;
    _ = &skip;
    _ = &max_tx;
    _ = &tx_masks;
    _ = &a;
    _ = &l;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[max_tx];
    _ = &t_dim;
    var y: c_int = undefined;
    _ = &y;
    var x: c_int = undefined;
    _ = &x;
    var txa: [2][2][32][32]u8 align(8) = undefined;
    _ = &txa;
    {
        var y_off: c_int = 0;
        _ = &y_off;
        var y_1: c_int = 0;
        _ = &y_1;
        while (y_1 < h4) : ({
            y_1 += t_dim.*.h;
            y_off += 1;
        }) {
            var x_off: c_int = 0;
            _ = &x_off;
            var x_1: c_int = 0;
            _ = &x_1;
            while (x_1 < w4) : ({
                x_1 += t_dim.*.w;
                x_off += 1;
            }) {
                decomp_tx(@ptrCast(@alignCast(&txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y_1)))][@bitCast(@as(isize, @intCast(x_1)))])), max_tx, 0, y_off, x_off, tx_masks);
            }
        }
    }
    var mask: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, by4))));
    _ = &mask;
    {
        y = 0;
        while (y < h4) : ({
            y += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))][@bitCast(@as(isize, @intCast(imin(txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)], l[@bitCast(@as(isize, @intCast(y)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    {
        x = 0;
        mask = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, bx4))));
        while (x < w4) : ({
            x += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4)))][@bitCast(@as(isize, @intCast(imin(txa[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))], a[@bitCast(@as(isize, @intCast(x)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    if (!(skip != 0)) {
        {
            y = 0;
            mask = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, by4))));
            while (y < h4) : ({
                y += 1;
                mask <<= @intCast(@as(c_uint, 1));
            }) {
                const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
                _ = &sidx;
                const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
                _ = &smask;
                var ltx: c_int = txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)];
                _ = &ltx;
                var step: c_int = txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)];
                _ = &step;
                {
                    x = step;
                    while (x < w4) : (x += step) {
                        const rtx: c_int = txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))];
                        _ = &rtx;
                        {
                            const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4 + x)))][@bitCast(@as(isize, @intCast(imin(rtx, ltx))))][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                        }
                        ltx = rtx;
                        step = txa[@as(c_int, 0)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))];
                    }
                }
            }
        }
        {
            x = 0;
            mask = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, bx4))));
            while (x < w4) : ({
                x += 1;
                mask <<= @intCast(@as(c_uint, 1));
            }) {
                const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
                _ = &sidx;
                const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
                _ = &smask;
                var ttx: c_int = txa[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
                _ = &ttx;
                var step: c_int = txa[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
                _ = &step;
                {
                    y = step;
                    while (y < h4) : (y += step) {
                        const btx: c_int = txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))];
                        _ = &btx;
                        {
                            const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4 + y)))][@bitCast(@as(isize, @intCast(imin(ttx, btx))))][@bitCast(@as(isize, @intCast(sidx)))];
                            ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
                        }
                        ttx = btx;
                        step = txa[@as(c_int, 1)][@as(c_int, 1)][@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))];
                    }
                }
            }
        }
    }
    {
        y = 0;
        while (y < h4) : (y += 1) {
            l[@bitCast(@as(isize, @intCast(y)))] = txa[@as(c_int, 0)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(w4 - @as(c_int, 1))))];
        }
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(a)), @ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&txa[@as(c_int, 1)][@as(c_int, 0)][@bitCast(@as(isize, @intCast(h4 - @as(c_int, 1))))]))))), @bitCast(@as(c_long, w4)));
}
pub fn mask_edges_intra(masks: [*c][32][3][2]u16, by4: c_int, bx4: c_int, w4: c_int, h4: c_int, tx: enum_RectTxfmSize, a: [*c]u8, l: [*c]u8) callconv(.c) void {
    _ = &masks;
    _ = &by4;
    _ = &bx4;
    _ = &w4;
    _ = &h4;
    _ = &tx;
    _ = &a;
    _ = &l;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[tx];
    _ = &t_dim;
    const twl4: c_int = t_dim.*.lw;
    _ = &twl4;
    const thl4: c_int = t_dim.*.lh;
    _ = &thl4;
    const twl4c: c_int = imin(2, twl4);
    _ = &twl4c;
    const thl4c: c_int = imin(2, thl4);
    _ = &thl4c;
    var y: c_int = undefined;
    _ = &y;
    var x: c_int = undefined;
    _ = &x;
    var mask: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, by4))));
    _ = &mask;
    {
        y = 0;
        while (y < h4) : ({
            y += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4)))][@bitCast(@as(isize, @intCast(imin(twl4c, l[@bitCast(@as(isize, @intCast(y)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    {
        x = 0;
        mask = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, bx4))));
        while (x < w4) : ({
            x += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= @as(c_uint, 65536)));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(@as(c_int, 4))))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4)))][@bitCast(@as(isize, @intCast(imin(thl4c, a[@bitCast(@as(isize, @intCast(x)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    const hstep: c_int = t_dim.*.w;
    _ = &hstep;
    var t: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, by4))));
    _ = &t;
    var inner: c_uint = @truncate((@as(u64, t) << @intCast(@as(u64, @bitCast(@as(c_longlong, h4))))) -% @as(u64, t));
    _ = &inner;
    var inner1: c_uint = inner & @as(c_uint, 65535);
    _ = &inner1;
    var inner2: c_uint = inner >> @intCast(@as(c_uint, 16));
    _ = &inner2;
    {
        x = hstep;
        while (x < w4) : (x += hstep) {
            if (inner1 != 0) {
                {
                    const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4 + x)))][@bitCast(@as(isize, @intCast(twl4c)))][@as(c_int, 0)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner1);
                }
            }
            if (inner2 != 0) {
                {
                    const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(bx4 + x)))][@bitCast(@as(isize, @intCast(twl4c)))][@as(c_int, 1)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner2);
                }
            }
        }
    }
    const vstep: c_int = t_dim.*.h;
    _ = &vstep;
    t = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, bx4))));
    inner = @truncate((@as(u64, t) << @intCast(@as(u64, @bitCast(@as(c_longlong, w4))))) -% @as(u64, t));
    inner1 = inner & @as(c_uint, 65535);
    inner2 = inner >> @intCast(@as(c_uint, 16));
    {
        y = vstep;
        while (y < h4) : (y += vstep) {
            if (inner1 != 0) {
                {
                    const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4 + y)))][@bitCast(@as(isize, @intCast(thl4c)))][@as(c_int, 0)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner1);
                }
            }
            if (inner2 != 0) {
                {
                    const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(by4 + y)))][@bitCast(@as(isize, @intCast(thl4c)))][@as(c_int, 1)];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner2);
                }
            }
        }
    }
    dav1d_memset_likely_pow2(@ptrCast(@alignCast(a)), thl4c, w4);
    dav1d_memset_likely_pow2(@ptrCast(@alignCast(l)), twl4c, h4);
}
pub fn mask_edges_chroma(masks: [*c][32][2][2]u16, cby4: c_int, cbx4: c_int, cw4: c_int, ch4: c_int, skip_inter: c_int, tx: enum_RectTxfmSize, a: [*c]u8, l: [*c]u8, ss_hor: c_int, ss_ver: c_int) callconv(.c) void {
    _ = &masks;
    _ = &cby4;
    _ = &cbx4;
    _ = &cw4;
    _ = &ch4;
    _ = &skip_inter;
    _ = &tx;
    _ = &a;
    _ = &l;
    _ = &ss_hor;
    _ = &ss_ver;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[tx];
    _ = &t_dim;
    const twl4: c_int = t_dim.*.lw;
    _ = &twl4;
    const thl4: c_int = t_dim.*.lh;
    _ = &thl4;
    const twl4c: c_int = @as(c_int, @intFromBool(!!(twl4 != 0)));
    _ = &twl4c;
    const thl4c: c_int = @as(c_int, @intFromBool(!!(thl4 != 0)));
    _ = &thl4c;
    var y: c_int = undefined;
    _ = &y;
    var x: c_int = undefined;
    _ = &x;
    const vbits: c_int = @as(c_int, 4) - ss_ver;
    _ = &vbits;
    const hbits: c_int = @as(c_int, 4) - ss_hor;
    _ = &hbits;
    const vmask: c_int = @as(c_int, 16) >> @intCast(ss_ver);
    _ = &vmask;
    const hmask: c_int = @as(c_int, 16) >> @intCast(ss_hor);
    _ = &hmask;
    const vmax: c_uint = @bitCast(@as(c_int, @as(c_int, 1) << @intCast(vmask)));
    _ = &vmax;
    const hmax: c_uint = @bitCast(@as(c_int, @as(c_int, 1) << @intCast(hmask)));
    _ = &hmax;
    var mask: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, cby4))));
    _ = &mask;
    {
        y = 0;
        while (y < ch4) : ({
            y += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= vmax));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(vbits)))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cbx4)))][@bitCast(@as(isize, @intCast(imin(twl4c, l[@bitCast(@as(isize, @intCast(y)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    {
        x = 0;
        mask = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, cbx4))));
        while (x < cw4) : ({
            x += 1;
            mask <<= @intCast(@as(c_uint, 1));
        }) {
            const sidx: c_int = @as(c_int, @intFromBool(mask >= hmax));
            _ = &sidx;
            const smask: c_uint = mask >> @intCast(@as(c_uint, @bitCast(@as(c_int, sidx << @intCast(hbits)))));
            _ = &smask;
            {
                const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(cby4)))][@bitCast(@as(isize, @intCast(imin(thl4c, a[@bitCast(@as(isize, @intCast(x)))]))))][@bitCast(@as(isize, @intCast(sidx)))];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | smask);
            }
        }
    }
    if (!(skip_inter != 0)) {
        const hstep: c_int = t_dim.*.w;
        _ = &hstep;
        var t: c_uint = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, cby4))));
        _ = &t;
        var inner: c_uint = @truncate((@as(u64, t) << @intCast(@as(u64, @bitCast(@as(c_longlong, ch4))))) -% @as(u64, t));
        _ = &inner;
        var inner1: c_uint = inner & @as(c_uint, @bitCast(@as(c_int, (@as(c_int, 1) << @intCast(vmask)) - @as(c_int, 1))));
        _ = &inner1;
        var inner2: c_uint = inner >> @intCast(@as(c_uint, @bitCast(@as(c_int, vmask))));
        _ = &inner2;
        {
            x = hstep;
            while (x < cw4) : (x += hstep) {
                if (inner1 != 0) {
                    {
                        const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cbx4 + x)))][@bitCast(@as(isize, @intCast(twl4c)))][@as(c_int, 0)];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner1);
                    }
                }
                if (inner2 != 0) {
                    {
                        const ref = &masks[@as(c_int, 0)][@bitCast(@as(isize, @intCast(cbx4 + x)))][@bitCast(@as(isize, @intCast(twl4c)))][@as(c_int, 1)];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner2);
                    }
                }
            }
        }
        const vstep: c_int = t_dim.*.h;
        _ = &vstep;
        t = @as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, cbx4))));
        inner = @truncate((@as(u64, t) << @intCast(@as(u64, @bitCast(@as(c_longlong, cw4))))) -% @as(u64, t));
        inner1 = inner & @as(c_uint, @bitCast(@as(c_int, (@as(c_int, 1) << @intCast(hmask)) - @as(c_int, 1))));
        inner2 = inner >> @intCast(@as(c_uint, @bitCast(@as(c_int, hmask))));
        {
            y = vstep;
            while (y < ch4) : (y += vstep) {
                if (inner1 != 0) {
                    {
                        const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(cby4 + y)))][@bitCast(@as(isize, @intCast(thl4c)))][@as(c_int, 0)];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner1);
                    }
                }
                if (inner2 != 0) {
                    {
                        const ref = &masks[@as(c_int, 1)][@bitCast(@as(isize, @intCast(cby4 + y)))][@bitCast(@as(isize, @intCast(thl4c)))][@as(c_int, 1)];
                        ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) | inner2);
                    }
                }
            }
        }
    }
    dav1d_memset_likely_pow2(@ptrCast(@alignCast(a)), thl4c, cw4);
    dav1d_memset_likely_pow2(@ptrCast(@alignCast(l)), twl4c, ch4);
}
pub export fn antfly_av1_dav1d_create_lf_mask_intra(lflvl: [*c]Av1Filter, level_cache: [*c][4]u8, b4_stride: ptrdiff_t, arg_filter_level: [*c][8][2]u8, bx: c_int, by: c_int, iw: c_int, ih: c_int, bs: enum_BlockSize, ytx: enum_RectTxfmSize, uvtx: enum_RectTxfmSize, layout: enum_Dav1dPixelLayout, ay: [*c]u8, ly: [*c]u8, auv: [*c]u8, luv: [*c]u8) void {
    _ = &lflvl;
    _ = &level_cache;
    _ = &b4_stride;
    var filter_level = arg_filter_level;
    _ = &filter_level;
    _ = &bx;
    _ = &by;
    _ = &iw;
    _ = &ih;
    _ = &bs;
    _ = &ytx;
    _ = &uvtx;
    _ = &layout;
    _ = &ay;
    _ = &ly;
    _ = &auv;
    _ = &luv;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bw4: c_int = imin(iw - bx, b_dim[@as(c_int, 0)]);
    _ = &bw4;
    const bh4: c_int = imin(ih - by, b_dim[@as(c_int, 1)]);
    _ = &bh4;
    const bx4: c_int = bx & @as(c_int, 31);
    _ = &bx4;
    const by4: c_int = by & @as(c_int, 31);
    _ = &by4;
    while (true) {
        if (!((bw4 >= @as(c_int, 0)) and (bh4 >= @as(c_int, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    if ((bw4 != 0) and (bh4 != 0)) {
        var level_cache_ptr: [*c][4]u8 = (level_cache + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, by) * b4_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(bx))));
        _ = &level_cache_ptr;
        {
            var y: c_int = 0;
            _ = &y;
            while (y < bh4) : (y += 1) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < bw4) : (x += 1) {
                        level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)] = filter_level[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)];
                        level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)] = filter_level[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)];
                    }
                }
                level_cache_ptr += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
            }
        }
        mask_edges_intra(@ptrCast(@alignCast(&lflvl.*.filter_y)), by4, bx4, bw4, bh4, ytx, ay, ly);
    }
    if (!(auv != null)) return;
    const ss_ver: c_int = @as(c_int, @intFromBool(layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const cbw4: c_int = imin(((iw + ss_hor) >> @intCast(ss_hor)) - (bx >> @intCast(ss_hor)), (@as(c_int, b_dim[@as(c_int, 0)]) + ss_hor) >> @intCast(ss_hor));
    _ = &cbw4;
    const cbh4: c_int = imin(((ih + ss_ver) >> @intCast(ss_ver)) - (by >> @intCast(ss_ver)), (@as(c_int, b_dim[@as(c_int, 1)]) + ss_ver) >> @intCast(ss_ver));
    _ = &cbh4;
    while (true) {
        if (!((cbw4 >= @as(c_int, 0)) and (cbh4 >= @as(c_int, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (!(cbw4 != 0) or !(cbh4 != 0)) return;
    const cbx4: c_int = bx4 >> @intCast(ss_hor);
    _ = &cbx4;
    const cby4: c_int = by4 >> @intCast(ss_ver);
    _ = &cby4;
    var level_cache_ptr: [*c][4]u8 = (level_cache + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, by >> @intCast(ss_ver)) * b4_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(bx >> @intCast(ss_hor)))));
    _ = &level_cache_ptr;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < cbh4) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < cbw4) : (x += 1) {
                    level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)] = filter_level[@as(c_int, 2)][@as(c_int, 0)][@as(c_int, 0)];
                    level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 3)] = filter_level[@as(c_int, 3)][@as(c_int, 0)][@as(c_int, 0)];
                }
            }
            level_cache_ptr += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }
    }
    mask_edges_chroma(@ptrCast(@alignCast(&lflvl.*.filter_uv)), cby4, cbx4, cbw4, cbh4, 0, uvtx, auv, luv, ss_hor, ss_ver);
}
pub export fn antfly_av1_dav1d_create_lf_mask_inter(lflvl: [*c]Av1Filter, level_cache: [*c][4]u8, b4_stride: ptrdiff_t, arg_filter_level: [*c][8][2]u8, bx: c_int, by: c_int, iw: c_int, ih: c_int, skip: c_int, bs: enum_BlockSize, max_ytx: enum_RectTxfmSize, tx_masks: [*c]const u16, uvtx: enum_RectTxfmSize, layout: enum_Dav1dPixelLayout, ay: [*c]u8, ly: [*c]u8, auv: [*c]u8, luv: [*c]u8) void {
    _ = &lflvl;
    _ = &level_cache;
    _ = &b4_stride;
    var filter_level = arg_filter_level;
    _ = &filter_level;
    _ = &bx;
    _ = &by;
    _ = &iw;
    _ = &ih;
    _ = &skip;
    _ = &bs;
    _ = &max_ytx;
    _ = &tx_masks;
    _ = &uvtx;
    _ = &layout;
    _ = &ay;
    _ = &ly;
    _ = &auv;
    _ = &luv;
    const b_dim: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_block_dimensions[bs]));
    _ = &b_dim;
    const bw4: c_int = imin(iw - bx, b_dim[@as(c_int, 0)]);
    _ = &bw4;
    const bh4: c_int = imin(ih - by, b_dim[@as(c_int, 1)]);
    _ = &bh4;
    const bx4: c_int = bx & @as(c_int, 31);
    _ = &bx4;
    const by4: c_int = by & @as(c_int, 31);
    _ = &by4;
    while (true) {
        if (!((bw4 >= @as(c_int, 0)) and (bh4 >= @as(c_int, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    if ((bw4 != 0) and (bh4 != 0)) {
        var level_cache_ptr: [*c][4]u8 = (level_cache + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, by) * b4_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(bx))));
        _ = &level_cache_ptr;
        {
            var y: c_int = 0;
            _ = &y;
            while (y < bh4) : (y += 1) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < bw4) : (x += 1) {
                        level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)] = filter_level[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)];
                        level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)] = filter_level[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)];
                    }
                }
                level_cache_ptr += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
            }
        }
        mask_edges_inter(@ptrCast(@alignCast(&lflvl.*.filter_y)), by4, bx4, bw4, bh4, skip, max_ytx, tx_masks, ay, ly);
    }
    if (!(auv != null)) return;
    const ss_ver: c_int = @as(c_int, @intFromBool(layout == @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I420)));
    _ = &ss_ver;
    const ss_hor: c_int = @as(c_int, @intFromBool(layout != @as(enum_Dav1dPixelLayout, DAV1D_PIXEL_LAYOUT_I444)));
    _ = &ss_hor;
    const cbw4: c_int = imin(((iw + ss_hor) >> @intCast(ss_hor)) - (bx >> @intCast(ss_hor)), (@as(c_int, b_dim[@as(c_int, 0)]) + ss_hor) >> @intCast(ss_hor));
    _ = &cbw4;
    const cbh4: c_int = imin(((ih + ss_ver) >> @intCast(ss_ver)) - (by >> @intCast(ss_ver)), (@as(c_int, b_dim[@as(c_int, 1)]) + ss_ver) >> @intCast(ss_ver));
    _ = &cbh4;
    while (true) {
        if (!((cbw4 >= @as(c_int, 0)) and (cbh4 >= @as(c_int, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (!(cbw4 != 0) or !(cbh4 != 0)) return;
    const cbx4: c_int = bx4 >> @intCast(ss_hor);
    _ = &cbx4;
    const cby4: c_int = by4 >> @intCast(ss_ver);
    _ = &cby4;
    var level_cache_ptr: [*c][4]u8 = (level_cache + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, by >> @intCast(ss_ver)) * b4_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(bx >> @intCast(ss_hor)))));
    _ = &level_cache_ptr;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < cbh4) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < cbw4) : (x += 1) {
                    level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 2)] = filter_level[@as(c_int, 2)][@as(c_int, 0)][@as(c_int, 0)];
                    level_cache_ptr[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 3)] = filter_level[@as(c_int, 3)][@as(c_int, 0)][@as(c_int, 0)];
                }
            }
            level_cache_ptr += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }
    }
    mask_edges_chroma(@ptrCast(@alignCast(&lflvl.*.filter_uv)), cby4, cbx4, cbw4, cbh4, skip, uvtx, auv, luv, ss_hor, ss_ver);
}
pub export fn antfly_av1_dav1d_calc_eih(lim_lut: [*c]Av1FilterLUT, filter_sharpness: c_int) void {
    _ = &lim_lut;
    _ = &filter_sharpness;
    const sharp: c_int = filter_sharpness;
    _ = &sharp;
    {
        var level: c_int = 0;
        _ = &level;
        while (level < @as(c_int, 64)) : (level += 1) {
            var limit: c_int = level;
            _ = &limit;
            if (sharp > @as(c_int, 0)) {
                limit >>= @intCast((sharp + @as(c_int, 3)) >> @intCast(@as(c_int, 2)));
                limit = imin(limit, @as(c_int, 9) - sharp);
            }
            limit = imax(limit, 1);
            lim_lut.*.i[@bitCast(@as(isize, @intCast(level)))] = @bitCast(@as(i8, @truncate(limit)));
            lim_lut.*.e[@bitCast(@as(isize, @intCast(level)))] = @bitCast(@as(i8, @truncate((@as(c_int, 2) * (level + @as(c_int, 2))) + limit)));
        }
    }
    lim_lut.*.sharp[@as(c_int, 0)] = @bitCast(@as(c_longlong, (sharp + @as(c_int, 3)) >> @intCast(@as(c_int, 2))));
    lim_lut.*.sharp[@as(c_int, 1)] = @bitCast(@as(c_longlong, if (sharp != 0) @as(c_int, 9) - sharp else @as(c_int, 255)));
}
pub fn calc_lf_value(lflvl_values: [*c][2]u8, base_lvl: c_int, lf_delta: c_int, seg_delta: c_int, mr_delta: [*c]const Dav1dLoopfilterModeRefDeltas) callconv(.c) void {
    _ = &lflvl_values;
    _ = &base_lvl;
    _ = &lf_delta;
    _ = &seg_delta;
    _ = &mr_delta;
    const base: c_int = iclip(iclip(base_lvl + lf_delta, 0, 63) + seg_delta, 0, 63);
    _ = &base;
    if (!(mr_delta != null)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lflvl_values)), base, @sizeOf(@TypeOf(lflvl_values.*)) *% @as(c_ulong, 8));
    } else {
        const sh: c_int = @as(c_int, @intFromBool(base >= @as(c_int, 32)));
        _ = &sh;
        lflvl_values[@as(c_int, 0)][@as(c_int, 0)] = blk: {
            const tmp = @as(u8, @bitCast(@as(i8, @truncate(iclip(base + (@as(c_int, mr_delta.*.ref_delta[@as(c_int, 0)]) * (@as(c_int, 1) << @intCast(sh))), 0, 63)))));
            lflvl_values[@as(c_int, 0)][@as(c_int, 1)] = tmp;
            break :blk tmp;
        };
        {
            var r: c_int = 1;
            _ = &r;
            while (r < @as(c_int, 8)) : (r += 1) {
                {
                    var m: c_int = 0;
                    _ = &m;
                    while (m < @as(c_int, 2)) : (m += 1) {
                        const delta: c_int = @as(c_int, mr_delta.*.mode_delta[@bitCast(@as(isize, @intCast(m)))]) + @as(c_int, mr_delta.*.ref_delta[@bitCast(@as(isize, @intCast(r)))]);
                        _ = &delta;
                        lflvl_values[@bitCast(@as(isize, @intCast(r)))][@bitCast(@as(isize, @intCast(m)))] = @bitCast(@as(i8, @truncate(iclip(base + (delta * (@as(c_int, 1) << @intCast(sh))), 0, 63))));
                    }
                }
            }
        }
    }
}
pub fn calc_lf_value_chroma(lflvl_values: [*c][2]u8, base_lvl: c_int, lf_delta: c_int, seg_delta: c_int, mr_delta: [*c]const Dav1dLoopfilterModeRefDeltas) callconv(.c) void {
    _ = &lflvl_values;
    _ = &base_lvl;
    _ = &lf_delta;
    _ = &seg_delta;
    _ = &mr_delta;
    if (!(base_lvl != 0)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lflvl_values)), 0, @sizeOf(@TypeOf(lflvl_values.*)) *% @as(c_ulong, 8));
    } else {
        calc_lf_value(lflvl_values, base_lvl, lf_delta, seg_delta, mr_delta);
    }
}
pub export fn antfly_av1_dav1d_calc_lf_values(lflvl_values: [*c][4][8][2]u8, hdr: [*c]const Dav1dFrameHeader, arg_lf_delta: [*c]const i8) void {
    _ = &lflvl_values;
    _ = &hdr;
    var lf_delta = arg_lf_delta;
    _ = &lf_delta;
    const n_seg: c_int = if (hdr.*.segmentation.enabled != 0) @as(c_int, 8) else @as(c_int, 1);
    _ = &n_seg;
    if (!(@as(c_int, hdr.*.loopfilter.level_y[@as(c_int, 0)]) != 0) and !(@as(c_int, hdr.*.loopfilter.level_y[@as(c_int, 1)]) != 0)) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(lflvl_values)), 0, @sizeOf(@TypeOf(lflvl_values.*)) *% @as(c_ulong, @bitCast(@as(c_long, n_seg))));
        return;
    }
    const mr_deltas: [*c]const Dav1dLoopfilterModeRefDeltas = @ptrCast(@alignCast(if (hdr.*.loopfilter.mode_ref_delta_enabled != 0) @as(?*anyopaque, @ptrCast(@alignCast(@constCast(&hdr.*.loopfilter.mode_ref_deltas)))) else @as(?*anyopaque, null)));
    _ = &mr_deltas;
    {
        var s: c_int = 0;
        _ = &s;
        while (s < n_seg) : (s += 1) {
            const segd: [*c]const Dav1dSegmentationData = @ptrCast(@alignCast(if (hdr.*.segmentation.enabled != 0) @as(?*anyopaque, @ptrCast(@alignCast(@constCast(&hdr.*.segmentation.seg_data.d[@bitCast(@as(isize, @intCast(s)))])))) else @as(?*anyopaque, null)));
            _ = &segd;
            calc_lf_value(@ptrCast(@alignCast(&lflvl_values[@bitCast(@as(isize, @intCast(s)))][@as(c_int, 0)])), hdr.*.loopfilter.level_y[@as(c_int, 0)], lf_delta[@as(c_int, 0)], if (segd != null) @as(c_int, segd.*.delta_lf_y_v) else @as(c_int, 0), mr_deltas);
            calc_lf_value(@ptrCast(@alignCast(&lflvl_values[@bitCast(@as(isize, @intCast(s)))][@as(c_int, 1)])), hdr.*.loopfilter.level_y[@as(c_int, 1)], lf_delta[@bitCast(@as(isize, @intCast(if (hdr.*.delta.lf.multi != 0) @as(c_int, 1) else @as(c_int, 0))))], if (segd != null) @as(c_int, segd.*.delta_lf_y_h) else @as(c_int, 0), mr_deltas);
            calc_lf_value_chroma(@ptrCast(@alignCast(&lflvl_values[@bitCast(@as(isize, @intCast(s)))][@as(c_int, 2)])), hdr.*.loopfilter.level_u, lf_delta[@bitCast(@as(isize, @intCast(if (hdr.*.delta.lf.multi != 0) @as(c_int, 2) else @as(c_int, 0))))], if (segd != null) @as(c_int, segd.*.delta_lf_u) else @as(c_int, 0), mr_deltas);
            calc_lf_value_chroma(@ptrCast(@alignCast(&lflvl_values[@bitCast(@as(isize, @intCast(s)))][@as(c_int, 3)])), hdr.*.loopfilter.level_v, lf_delta[@bitCast(@as(isize, @intCast(if (hdr.*.delta.lf.multi != 0) @as(c_int, 3) else @as(c_int, 0))))], if (segd != null) @as(c_int, segd.*.delta_lf_v) else @as(c_int, 0), mr_deltas);
        }
    }
}
