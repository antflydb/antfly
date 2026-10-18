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
// /*
//  *  Copyright (c) 2010 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const struct_yv12_buffer_config = extern struct {
    y_width: c_int = 0,
    y_height: c_int = 0,
    y_crop_width: c_int = 0,
    y_crop_height: c_int = 0,
    y_stride: c_int = 0,
    uv_width: c_int = 0,
    uv_height: c_int = 0,
    uv_crop_width: c_int = 0,
    uv_crop_height: c_int = 0,
    uv_stride: c_int = 0,
    alpha_width: c_int = 0,
    alpha_height: c_int = 0,
    alpha_stride: c_int = 0,
    y_buffer: [*c]u8 = null,
    u_buffer: [*c]u8 = null,
    v_buffer: [*c]u8 = null,
    alpha_buffer: [*c]u8 = null,
    buffer_alloc: [*c]u8 = null,
    buffer_alloc_sz: usize = 0,
    border: c_int = 0,
    frame_size: usize = 0,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    bit_depth: c_uint = 0,
    color_space: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    color_range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    render_width: c_int = 0,
    render_height: c_int = 0,
    corrupted: c_int = 0,
    flags: c_int = 0,
};
pub const tran_low_t = common.tran_low_t;
pub fn clamp(arg_value: c_int, arg_low: c_int, arg_high: c_int) callconv(.c) c_int {
    var value = arg_value;
    _ = &value;
    var low = arg_low;
    _ = &low;
    var high = arg_high;
    _ = &high;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (value < low) low else if (value > high) high else value;
}
pub const YV12_BUFFER_CONFIG = struct_yv12_buffer_config;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const vpx_prob = common.vpx_prob;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const struct_scale_factors = common.struct_scale_factors;
pub const struct_RefBuffer = extern struct {
    idx: c_int = 0,
    buf: [*c]YV12_BUFFER_CONFIG = null,
    sf: struct_scale_factors = @import("std").mem.zeroes(struct_scale_factors),
};
pub const RefBuffer = struct_RefBuffer;
pub const INTERP_FILTER = common.INTERP_FILTER;
pub const MV_REFERENCE_FRAME = common.MV_REFERENCE_FRAME;
pub const struct_mv = extern struct {
    row: i16 = 0,
    col: i16 = 0,
};
pub const MV = struct_mv;
pub const union_int_mv = extern union {
    as_int: u32,
    as_mv: MV,
};
pub const int_mv = union_int_mv;
pub const struct_MODE_INFO = extern struct {
    sb_type: BLOCK_SIZE = 0,
    mode: PREDICTION_MODE = 0,
    tx_size: TX_SIZE = 0,
    skip: i8 = 0,
    segment_id: i8 = 0,
    seg_id_predicted: i8 = 0,
    uv_mode: PREDICTION_MODE = 0,
    interp_filter: INTERP_FILTER = 0,
    ref_frame: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
    mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
    bmi: [4]b_mode_info = @import("std").mem.zeroes([4]b_mode_info),
};
pub const MODE_INFO = struct_MODE_INFO;
pub const vp9_coeff_probs_model = common.vp9_coeff_probs_model;
pub const struct_tx_probs = extern struct {
    p32x32: [2][3]vpx_prob = @import("std").mem.zeroes([2][3]vpx_prob),
    p16x16: [2][2]vpx_prob = @import("std").mem.zeroes([2][2]vpx_prob),
    p8x8: [2][1]vpx_prob = @import("std").mem.zeroes([2][1]vpx_prob),
};
pub const struct_frame_contexts = extern struct {
    y_mode_prob: [4][9]vpx_prob = @import("std").mem.zeroes([4][9]vpx_prob),
    uv_mode_prob: [10][9]vpx_prob = @import("std").mem.zeroes([10][9]vpx_prob),
    partition_prob: [16][3]vpx_prob = @import("std").mem.zeroes([16][3]vpx_prob),
    coef_probs: [4][2]vp9_coeff_probs_model = @import("std").mem.zeroes([4][2]vp9_coeff_probs_model),
    switchable_interp_prob: [4][2]vpx_prob = @import("std").mem.zeroes([4][2]vpx_prob),
    inter_mode_probs: [7][3]vpx_prob = @import("std").mem.zeroes([7][3]vpx_prob),
    intra_inter_prob: [4]vpx_prob = @import("std").mem.zeroes([4]vpx_prob),
    comp_inter_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    single_ref_prob: [5][2]vpx_prob = @import("std").mem.zeroes([5][2]vpx_prob),
    comp_ref_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    tx_probs: struct_tx_probs = @import("std").mem.zeroes(struct_tx_probs),
    skip_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    nmvc: nmv_context = @import("std").mem.zeroes(nmv_context),
    initialized: c_int = 0,
};
pub const FRAME_CONTEXT = struct_frame_contexts;
pub const vp9_coeff_count_model = common.vp9_coeff_count_model;
pub const struct_tx_counts = common.struct_tx_counts;
pub const struct_FRAME_COUNTS = extern struct {
    y_mode: [4][10]c_uint = @import("std").mem.zeroes([4][10]c_uint),
    uv_mode: [10][10]c_uint = @import("std").mem.zeroes([10][10]c_uint),
    partition: [16][4]c_uint = @import("std").mem.zeroes([16][4]c_uint),
    coef: [4][2]vp9_coeff_count_model = @import("std").mem.zeroes([4][2]vp9_coeff_count_model),
    eob_branch: [4][2][2][6][6]c_uint = @import("std").mem.zeroes([4][2][2][6][6]c_uint),
    switchable_interp: [4][3]c_uint = @import("std").mem.zeroes([4][3]c_uint),
    inter_mode: [7][4]c_uint = @import("std").mem.zeroes([7][4]c_uint),
    intra_inter: [4][2]c_uint = @import("std").mem.zeroes([4][2]c_uint),
    comp_inter: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    single_ref: [5][2][2]c_uint = @import("std").mem.zeroes([5][2][2]c_uint),
    comp_ref: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    tx: struct_tx_counts = @import("std").mem.zeroes(struct_tx_counts),
    skip: [3][2]c_uint = @import("std").mem.zeroes([3][2]c_uint),
    mv: nmv_context_counts = @import("std").mem.zeroes(nmv_context_counts),
};
pub const FRAME_COUNTS = struct_FRAME_COUNTS;
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
pub fn clamp_mv(arg_mv_1: [*c]MV, arg_min_col: c_int, arg_max_col: c_int, arg_min_row: c_int, arg_max_row: c_int) callconv(.c) void {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var min_col = arg_min_col;
    _ = &min_col;
    var max_col = arg_max_col;
    _ = &max_col;
    var min_row = arg_min_row;
    _ = &min_row;
    var max_row = arg_max_row;
    _ = &max_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.col = @truncate(clamp(mv_1.*.col, min_col, max_col));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mv_1.*.row = @truncate(clamp(mv_1.*.row, min_row, max_row));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
pub const struct_buf_2d = extern struct {
    buf: [*c]u8 = null,
    stride: c_int = 0,
};
pub const struct_macroblockd_plane = extern struct {
    dqcoeff: [*c]tran_low_t = null,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    dst: struct_buf_2d = @import("std").mem.zeroes(struct_buf_2d),
    pre: [2]struct_buf_2d = @import("std").mem.zeroes([2]struct_buf_2d),
    above_context: [*c]ENTROPY_CONTEXT = null,
    left_context: [*c]ENTROPY_CONTEXT = null,
    seg_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    n4_w: u16 = 0,
    n4_h: u16 = 0,
    n4_wl: u8 = 0,
    n4_hl: u8 = 0,
    dequant: [*c]const i16 = null,
    eob: [*c]c_int = null,
};
pub const struct_macroblockd = extern struct {
    plane: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    bmode_blocks_wl: u8 = 0,
    bmode_blocks_hl: u8 = 0,
    counts: [*c]FRAME_COUNTS = null,
    tile: TileInfo = @import("std").mem.zeroes(TileInfo),
    mi_stride: c_int = 0,
    mi: [*c][*c]MODE_INFO = null,
    left_mi: [*c]MODE_INFO = null,
    above_mi: [*c]MODE_INFO = null,
    max_blocks_wide: c_uint = 0,
    max_blocks_high: c_uint = 0,
    partition_probs: [*c][3]vpx_prob = null,
    mb_to_left_edge: c_int = 0,
    mb_to_right_edge: c_int = 0,
    mb_to_top_edge: c_int = 0,
    mb_to_bottom_edge: c_int = 0,
    fc: [*c]FRAME_CONTEXT = null,
    block_refs: [2][*c]const RefBuffer = @import("std").mem.zeroes([2][*c]const RefBuffer),
    cur_buf: [*c]const YV12_BUFFER_CONFIG = null,
    above_context: [3][*c]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][*c]ENTROPY_CONTEXT),
    left_context: [3][16]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][16]ENTROPY_CONTEXT),
    above_seg_context: [*c]PARTITION_CONTEXT = null,
    left_seg_context: [8]PARTITION_CONTEXT = @import("std").mem.zeroes([8]PARTITION_CONTEXT),
    bd: c_int = 0,
    lossless: c_int = 0,
    corrupted: c_int = 0,
    error_info: [*c]struct_vpx_internal_error_info = null,
    partition: [*c]PARTITION_TYPE = null,
};
pub const MACROBLOCKD = struct_macroblockd;
pub fn scaled_buffer_offset(arg_x_offset: c_int, arg_y_offset: c_int, arg_stride: c_int, arg_sf: [*c]const struct_scale_factors) callconv(.c) i64 {
    var x_offset = arg_x_offset;
    _ = &x_offset;
    var y_offset = arg_y_offset;
    _ = &y_offset;
    var stride = arg_stride;
    _ = &stride;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const x: c_int = if (sf != null) sf.*.scale_value_x.?(x_offset, sf) else x_offset;
    _ = &x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const y: c_int = if (sf != null) sf.*.scale_value_y.?(y_offset, sf) else y_offset;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return (@as(i64, y) * @as(i64, stride)) + @as(i64, x);
}
pub fn setup_pred_plane(arg_dst: [*c]struct_buf_2d, arg_src: [*c]u8, arg_stride: c_int, arg_mi_row: c_int, arg_mi_col: c_int, arg_scale: [*c]const struct_scale_factors, arg_subsampling_x: c_int, arg_subsampling_y: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var src = arg_src;
    _ = &src;
    var stride = arg_stride;
    _ = &stride;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var scale = arg_scale;
    _ = &scale;
    var subsampling_x = arg_subsampling_x;
    _ = &subsampling_x;
    var subsampling_y = arg_subsampling_y;
    _ = &subsampling_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const x: c_int = ((@as(c_int, 1) << @intCast(@as(c_int, 3))) * mi_col) >> @intCast(subsampling_x);
    _ = &x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const y: c_int = ((@as(c_int, 1) << @intCast(@as(c_int, 3))) * mi_row) >> @intCast(subsampling_y);
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst.*.buf = src + @as(usize, @bitCast(@as(isize, @intCast(scaled_buffer_offset(x, y, stride, scale)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst.*.stride = stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn round_mv_comp_q4(arg_value: c_int) callconv(.c) c_int {
    var value = arg_value;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @divTrunc(if (value < @as(c_int, 0)) value - @as(c_int, 2) else value + @as(c_int, 2), @as(c_int, 4));
}
pub fn mi_mv_pred_q4(arg_mi: [*c]const MODE_INFO, arg_idx: c_int) callconv(.c) MV {
    var mi = arg_mi;
    _ = &mi;
    var idx = arg_idx;
    _ = &idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    var res: MV = MV{
        .row = @truncate(round_mv_comp_q4(((@as(c_int, mi.*.bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row) + @as(c_int, mi.*.bmi[@as(c_int, 1)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row)) + @as(c_int, mi.*.bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row)) + @as(c_int, mi.*.bmi[@as(c_int, 3)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row))),
        .col = @truncate(round_mv_comp_q4(((@as(c_int, mi.*.bmi[@as(c_int, 0)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col) + @as(c_int, mi.*.bmi[@as(c_int, 1)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col)) + @as(c_int, mi.*.bmi[@as(c_int, 2)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col)) + @as(c_int, mi.*.bmi[@as(c_int, 3)].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col))),
    };
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    return res;
}
pub fn round_mv_comp_q2(arg_value: c_int) callconv(.c) c_int {
    var value = arg_value;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @divTrunc(if (value < @as(c_int, 0)) value - @as(c_int, 1) else value + @as(c_int, 1), @as(c_int, 2));
}
pub fn mi_mv_pred_q2(arg_mi: [*c]const MODE_INFO, arg_idx: c_int, arg_block0: c_int, arg_block1: c_int) callconv(.c) MV {
    var mi = arg_mi;
    _ = &mi;
    var idx = arg_idx;
    _ = &idx;
    var block0 = arg_block0;
    _ = &block0;
    var block1 = arg_block1;
    _ = &block1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    var res: MV = MV{
        .row = @truncate(round_mv_comp_q2(@as(c_int, mi.*.bmi[@bitCast(@as(isize, @intCast(block0)))].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row) + @as(c_int, mi.*.bmi[@bitCast(@as(isize, @intCast(block1)))].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.row))),
        .col = @truncate(round_mv_comp_q2(@as(c_int, mi.*.bmi[@bitCast(@as(isize, @intCast(block0)))].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col) + @as(c_int, mi.*.bmi[@bitCast(@as(isize, @intCast(block1)))].as_mv[@bitCast(@as(isize, @intCast(idx)))].as_mv.col))),
    };
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    return res;
}
pub export fn antfly_vp9_clamp_mv_to_umv_border_sb(arg_xd: [*c]const MACROBLOCKD, arg_src_mv: [*c]const MV, arg_bw: c_int, arg_bh: c_int, arg_ss_x: c_int, arg_ss_y: c_int) MV {
    var xd = arg_xd;
    _ = &xd;
    var src_mv = arg_src_mv;
    _ = &src_mv;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    var ss_x = arg_ss_x;
    _ = &ss_x;
    var ss_y = arg_ss_y;
    _ = &ss_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    const spel_left: c_int = (@as(c_int, 4) + bw) << @intCast(@as(c_int, 4));
    _ = &spel_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    const spel_right: c_int = spel_left - (@as(c_int, 1) << @intCast(@as(c_int, 4)));
    _ = &spel_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    const spel_top: c_int = (@as(c_int, 4) + bh) << @intCast(@as(c_int, 4));
    _ = &spel_top;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    const spel_bottom: c_int = spel_top - (@as(c_int, 1) << @intCast(@as(c_int, 4)));
    _ = &spel_bottom;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    var clamped_mv: MV = MV{
        .row = @truncate(@as(c_int, src_mv.*.row) * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_y))),
        .col = @truncate(@as(c_int, src_mv.*.col) * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_x))),
    };
    _ = &clamped_mv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    clamp_mv(&clamped_mv, (xd.*.mb_to_left_edge * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_x))) - spel_left, (xd.*.mb_to_right_edge * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_x))) + spel_right, (xd.*.mb_to_top_edge * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_y))) - spel_top, (xd.*.mb_to_bottom_edge * (@as(c_int, 1) << @intCast(@as(c_int, 1) - ss_y))) + spel_bottom);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    return clamped_mv;
}
pub export fn antfly_vp9_average_split_mvs(arg_pd: [*c]const struct_macroblockd_plane, arg_mi: [*c]const MODE_INFO, arg_ref: c_int, arg_block: c_int) MV {
    var pd = arg_pd;
    _ = &pd;
    var mi = arg_mi;
    _ = &mi;
    var ref = arg_ref;
    _ = &ref;
    var block = arg_block;
    _ = &block;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    const ss_idx: c_int = (@as(c_int, @intFromBool((pd.*.subsampling_x > @as(c_int, 0)))) << @intCast(@as(c_int, 1))) | @as(c_int, @intFromBool(pd.*.subsampling_y > @as(c_int, 0)));
    _ = &ss_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    var res: MV = MV{
        .row = 0,
        .col = 0,
    };
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    blk: {
        switch (ss_idx) {
            @as(c_int, 0) => {
                res = mi.*.bmi[@bitCast(@as(isize, @intCast(block)))].as_mv[@bitCast(@as(isize, @intCast(ref)))].as_mv;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return MV{
                        .row = 0,
                        .col = 0,
                    };
                }
                break :blk;
            },
            @as(c_int, 1) => {
                res = mi_mv_pred_q2(mi, ref, block, block + @as(c_int, 2));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return MV{
                        .row = 0,
                        .col = 0,
                    };
                }
                break :blk;
            },
            @as(c_int, 2) => {
                res = mi_mv_pred_q2(mi, ref, block, block + @as(c_int, 1));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return MV{
                        .row = 0,
                        .col = 0,
                    };
                }
                break :blk;
            },
            @as(c_int, 3) => {
                res = mi_mv_pred_q4(mi, ref);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return MV{
                        .row = 0,
                        .col = 0,
                    };
                }
                break :blk;
            },
            else => {
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return MV{
                        .row = 0,
                        .col = 0,
                    };
                }
            },
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV{
            .row = 0,
            .col = 0,
        };
    }
    return res;
}
pub export fn antfly_vp9_vp9_setup_dst_planes(arg_planes: [*c]struct_macroblockd_plane, arg_src: [*c]const YV12_BUFFER_CONFIG, arg_mi_row: c_int, arg_mi_col: c_int) void {
    var planes = arg_planes;
    _ = &planes;
    var src = arg_src;
    _ = &src;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const buffers: [3][*c]u8 = [3][*c]u8{
        src.*.y_buffer,
        src.*.u_buffer,
        src.*.v_buffer,
    };
    _ = &buffers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const strides: [3]c_int = [3]c_int{
        src.*.y_stride,
        src.*.uv_stride,
        src.*.uv_stride,
    };
    _ = &strides;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const pd: [*c]struct_macroblockd_plane = &planes[@bitCast(@as(isize, @intCast(i)))];
            _ = &pd;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            setup_pred_plane(&pd.*.dst, buffers[@bitCast(@as(isize, @intCast(i)))], strides[@bitCast(@as(isize, @intCast(i)))], mi_row, mi_col, null, pd.*.subsampling_x, pd.*.subsampling_y);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_setup_pre_planes(arg_xd: [*c]MACROBLOCKD, arg_idx: c_int, arg_src: [*c]const YV12_BUFFER_CONFIG, arg_mi_row: c_int, arg_mi_col: c_int, arg_sf: [*c]const struct_scale_factors) void {
    var xd = arg_xd;
    _ = &xd;
    var idx = arg_idx;
    _ = &idx;
    var src = arg_src;
    _ = &src;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(src)))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var i: c_int = undefined;
        _ = &i;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const buffers: [3][*c]u8 = [3][*c]u8{
            src.*.y_buffer,
            src.*.u_buffer,
            src.*.v_buffer,
        };
        _ = &buffers;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const strides: [3]c_int = [3]c_int{
            src.*.y_stride,
            src.*.uv_stride,
            src.*.uv_stride,
        };
        _ = &strides;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 3)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(i)))];
                _ = &pd;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                setup_pred_plane(&pd.*.pre[@bitCast(@as(isize, @intCast(idx)))], buffers[@bitCast(@as(isize, @intCast(i)))], strides[@bitCast(@as(isize, @intCast(i)))], mi_row, mi_col, sf, pd.*.subsampling_x, pd.*.subsampling_y);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
