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
pub const __builtin = common.__builtin;
pub const __helpers = common.__helpers;
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_afz_vp9_fail(?*anyopaque, c_int) void;
pub extern fn antfly_codec_sort(__base: ?*anyopaque, __nel: usize, __width: usize, __compar: ?*const fn (?*const anyopaque, ?*const anyopaque) callconv(.c) c_int) void;
pub const tran_low_t = common.tran_low_t;
pub extern fn antfly_vp9_vpx_memalign(@"align": usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_malloc(size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_calloc(num: usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub fn vpx_memset16(arg_dest: ?*anyopaque, arg_val: c_int, arg_length: usize) callconv(.c) ?*anyopaque {
    var dest = arg_dest;
    _ = &dest;
    var val = arg_val;
    _ = &val;
    var length = arg_length;
    _ = &length;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var i: usize = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var dest16: [*c]u16 = @ptrCast(@alignCast(dest));
    _ = &dest16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        i = 0;
        while (i < length) : (i +%= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            (blk: {
                const ref = &dest16;
                const tmp = ref.*;
                ref.* += 1;
                break :blk tmp;
            }).* = @bitCast(@as(c_short, @truncate(val)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    return dest;
}
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memmove(__dst: ?*anyopaque, __src: ?*const anyopaque, __len: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub fn get_msb(arg_n: c_uint) callconv(.c) c_int {
    var n = arg_n;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, 31) ^ __builtin.clz(n);
}
pub fn get_unsigned_bits(arg_num_values: c_uint) callconv(.c) c_int {
    var num_values = arg_num_values;
    _ = &num_values;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (num_values > @as(c_uint, 0)) get_msb(num_values) + @as(c_int, 1) else @as(c_int, 0);
}
pub const PROFILE_0 = common.PROFILE_0;
pub const PROFILE_1 = common.PROFILE_1;
pub const PROFILE_2 = common.PROFILE_2;
pub const PROFILE_3 = common.PROFILE_3;
pub const MAX_PROFILES = common.MAX_PROFILES;
pub const BITSTREAM_PROFILE = common.BITSTREAM_PROFILE;
pub const PARSE = common.PARSE;
pub const RECON = common.RECON;
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_NONE = common.PARTITION_NONE;
pub const PARTITION_HORZ = common.PARTITION_HORZ;
pub const PARTITION_VERT = common.PARTITION_VERT;
pub const PARTITION_SPLIT = common.PARTITION_SPLIT;
pub const PARTITION_TYPES = common.PARTITION_TYPES;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const ONLY_4X4 = common.ONLY_4X4;
pub const ALLOW_32X32 = common.ALLOW_32X32;
pub const TX_MODE_SELECT = common.TX_MODE_SELECT;
pub const TX_MODE = common.TX_MODE;
pub const DCT_DCT = common.DCT_DCT;
pub const TX_TYPE = common.TX_TYPE;
pub const PLANE_TYPE_Y = common.PLANE_TYPE_Y;
pub const PLANE_TYPES = common.PLANE_TYPES;
pub const PLANE_TYPE = common.PLANE_TYPE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const InterpKernel = common.InterpKernel;
pub const INTERP_FILTER = common.INTERP_FILTER;
pub extern var antfly_vp9_vp9_filter_kernels: [5][*c]const InterpKernel;
pub const struct_buf_2d = extern struct {
    buf: [*c]u8 = null,
    stride: c_int = 0,
};
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
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
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
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
pub const vpx_prob = common.vpx_prob;
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
pub const VPX_CS_BT_601 = common.VPX_CS_BT_601;
pub const VPX_CS_SRGB = common.VPX_CS_SRGB;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const VPX_CR_STUDIO_RANGE = common.VPX_CR_STUDIO_RANGE;
pub const VPX_CR_FULL_RANGE = common.VPX_CR_FULL_RANGE;
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
pub const YV12_BUFFER_CONFIG = struct_yv12_buffer_config;
pub const struct_scale_factors = common.struct_scale_factors;
pub const struct_RefBuffer = extern struct {
    idx: c_int = 0,
    buf: [*c]YV12_BUFFER_CONFIG = null,
    sf: struct_scale_factors = @import("std").mem.zeroes(struct_scale_factors),
};
pub const RefBuffer = struct_RefBuffer;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
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
pub const struct_ScanOrder = extern struct {
    scan: [*c]const i16 = null,
    iscan: [*c]const i16 = null,
    neighbors: [*c]const i16 = null,
};
pub const vpx_rb_error_handler = common.vpx_rb_error_handler;
pub const struct_vpx_read_bit_buffer = extern struct {
    bit_buffer: [*c]const u8 = null,
    bit_buffer_end: [*c]const u8 = null,
    bit_offset: usize = 0,
    error_handler_data: ?*anyopaque = null,
    error_handler: vpx_rb_error_handler = null,
};
pub extern fn antfly_vp9_vpx_rb_bytes_read(rb: [*c]struct_vpx_read_bit_buffer) usize;
pub extern fn antfly_vp9_vpx_rb_read_bit(rb: [*c]struct_vpx_read_bit_buffer) c_int;
pub extern fn antfly_vp9_vpx_rb_read_literal(rb: [*c]struct_vpx_read_bit_buffer, bits: c_int) c_int;
pub extern fn antfly_vp9_vpx_rb_read_signed_literal(rb: [*c]struct_vpx_read_bit_buffer, bits: c_int) c_int;
pub const VPX_CODEC_ERROR = common.VPX_CODEC_ERROR;
pub const VPX_CODEC_MEM_ERROR = common.VPX_CODEC_MEM_ERROR;
pub const VPX_CODEC_UNSUP_BITSTREAM = common.VPX_CODEC_UNSUP_BITSTREAM;
pub const VPX_CODEC_CORRUPT_FRAME = common.VPX_CODEC_CORRUPT_FRAME;
pub const VPX_BITS_8 = common.VPX_BITS_8;
pub const VPX_BITS_10 = common.VPX_BITS_10;
pub const VPX_BITS_12 = common.VPX_BITS_12;
pub const vpx_bit_depth_t = common.vpx_bit_depth_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
pub const vpx_release_frame_buffer_cb_fn_t = common.vpx_release_frame_buffer_cb_fn_t;
pub const vpx_decrypt_cb = common.vpx_decrypt_cb;
pub const vpx_tree_index = common.vpx_tree_index;
pub extern const antfly_vp9_vpx_norm: [256]u8 align(16);
pub const BD_VALUE = common.BD_VALUE;
pub const vpx_reader = extern struct {
    value: BD_VALUE = 0,
    range: c_uint = 0,
    count: c_int = 0,
    buffer_end: [*c]const u8 = null,
    buffer: [*c]const u8 = null,
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    clear_buffer: [9]u8 = @import("std").mem.zeroes([9]u8),
};
pub extern fn antfly_vp9_vpx_reader_init(r: [*c]vpx_reader, buffer: [*c]const u8, size: usize, decrypt_cb: vpx_decrypt_cb, decrypt_state: ?*anyopaque) c_int;
pub extern fn antfly_vp9_vpx_reader_fill(r: [*c]vpx_reader) void;
pub extern fn antfly_vp9_vpx_reader_find_end(r: [*c]vpx_reader) [*c]const u8;
pub fn vpx_reader_has_error(arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((r.*.count > (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8))) and (r.*.count < @as(c_int, 1073741824))));
}
pub fn vpx_read(arg_r: [*c]vpx_reader, arg_prob: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var prob = arg_prob;
    _ = &prob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bit: c_uint = 0;
    _ = &bit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var value: BD_VALUE = undefined;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bigsplit: BD_VALUE = undefined;
    _ = &bigsplit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var count: c_int = undefined;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var range: c_uint = undefined;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var split: c_uint = ((r.*.range *% @as(c_uint, @bitCast(@as(c_int, prob)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 256) - prob)))) >> @intCast(@as(c_uint, 8));
    _ = &split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (r.*.count < @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_reader_fill(r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    value = r.*.value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    count = r.*.count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    bigsplit = @as(BD_VALUE, split) << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - @as(c_int, 8)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    range = split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (value >= bigsplit) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range = r.*.range -% split;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value = value -% bigsplit;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        bit = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const shift: u8 = antfly_vp9_vpx_norm[@as(u8, @truncate(range))];
        _ = &shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range <<= @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value <<= @intCast(@as(BD_VALUE, @bitCast(@as(c_long, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        count -= shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.value = value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.count = count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.range = range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(c_uint, @truncate(bit)));
}
pub fn vpx_read_bit(arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return vpx_read(r, 128);
}
pub fn vpx_read_literal(arg_r: [*c]vpx_reader, arg_bits: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var bits = arg_bits;
    _ = &bits;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var literal: c_int = 0;
    _ = &literal;
    var bit: c_int = undefined;
    _ = &bit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        bit = bits - @as(c_int, 1);
        while (bit >= @as(c_int, 0)) : (bit -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            literal |= vpx_read_bit(r) << @intCast(bit);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return literal;
}
pub fn vpx_read_tree(arg_r: [*c]vpx_reader, arg_tree: [*c]const vpx_tree_index, arg_probs: [*c]const vpx_prob) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var tree = arg_tree;
    _ = &tree;
    var probs = arg_probs;
    _ = &probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: vpx_tree_index = 0;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (@as(c_int, blk: {
        const tmp = tree[@bitCast(@as(isize, @intCast(@as(c_int, i) + vpx_read(r, probs[@bitCast(@as(isize, @intCast(@as(c_int, i) >> @intCast(@as(c_int, 1)))))]))))];
        i = tmp;
        break :blk tmp;
    }) > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        continue;
    }
    return -@as(c_int, i);
}
pub fn mem_get_be32_as_int(arg_vmem: ?*const anyopaque) callconv(.c) c_uint {
    var vmem = arg_vmem;
    _ = &vmem;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var val: c_uint = undefined;
    _ = &val;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mem: [*c]const u8 = @ptrCast(@alignCast(vmem));
    _ = &mem;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    val = @as(c_uint, mem[@as(c_int, 0)]) << @intCast(@as(c_uint, 24));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    val |= @bitCast(@as(c_int, @as(c_int, mem[@as(c_int, 1)]) << @intCast(@as(c_int, 16))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    val |= @bitCast(@as(c_int, @as(c_int, mem[@as(c_int, 2)]) << @intCast(@as(c_int, 8))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    val |= @bitCast(@as(c_int, @as(c_int, mem[@as(c_int, 3)])));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return val;
}
pub extern fn antfly_vp9_vpx_realloc_frame_buffer(ybf: [*c]YV12_BUFFER_CONFIG, width: c_int, height: c_int, ss_x: c_int, ss_y: c_int, use_highbitdepth: c_int, border: c_int, byte_alignment: c_int, fb: [*c]vpx_codec_frame_buffer_t, cb: vpx_get_frame_buffer_cb_fn_t, cb_priv: ?*anyopaque) c_int;
pub const VPxWorkerStatus = common.VPxWorkerStatus;
pub const VPxWorkerHook = common.VPxWorkerHook;
pub const VPxWorkerImpl = common.VPxWorkerImpl;
pub const VPxWorker = extern struct {
    impl_: ?*VPxWorkerImpl = null,
    status_: VPxWorkerStatus = @import("std").mem.zeroes(VPxWorkerStatus),
    thread_name: [*c]const u8 = null,
    hook: VPxWorkerHook = null,
    data1: ?*anyopaque = null,
    data2: ?*anyopaque = null,
    had_error: c_int = 0,
};
pub const VPxWorkerInterface = extern struct {
    init: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    reset: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    sync: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    launch: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    execute: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    end: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
};
pub extern fn antfly_vp9_vpx_get_worker_interface() [*c]const VPxWorkerInterface;
pub const struct_loopfilter = extern struct {
    filter_level: c_int = 0,
    last_filt_level: c_int = 0,
    sharpness_level: c_int = 0,
    last_sharpness_level: c_int = 0,
    mode_ref_delta_enabled: u8 = 0,
    mode_ref_delta_update: u8 = 0,
    ref_deltas: [4]i8 = @import("std").mem.zeroes([4]i8),
    last_ref_deltas: [4]i8 = @import("std").mem.zeroes([4]i8),
    mode_deltas: [2]i8 = @import("std").mem.zeroes([2]i8),
    last_mode_deltas: [2]i8 = @import("std").mem.zeroes([2]i8),
    lfm: [*c]LOOP_FILTER_MASK = null,
    lfm_stride: c_int = 0,
};
pub const struct_segmentation = extern struct {
    enabled: u8 = 0,
    update_map: u8 = 0,
    update_data: u8 = 0,
    abs_delta: u8 = 0,
    temporal_update: u8 = 0,
    tree_probs: [7]vpx_prob = @import("std").mem.zeroes([7]vpx_prob),
    pred_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    feature_data: [8][4]i16 = @import("std").mem.zeroes([8][4]i16),
    feature_mask: [8]u32 = @import("std").mem.zeroes([8]u32),
    aq_av_offset: c_int = 0,
};
pub const InternalFrameBufferList = common.InternalFrameBufferList;
pub const struct_BufferPool = extern struct {
    cb_priv: ?*anyopaque = null,
    get_fb_cb: vpx_get_frame_buffer_cb_fn_t = null,
    release_fb_cb: vpx_release_frame_buffer_cb_fn_t = null,
    frame_bufs: [12]RefCntBuffer = @import("std").mem.zeroes([12]RefCntBuffer),
    int_frame_buffers: InternalFrameBufferList = @import("std").mem.zeroes(InternalFrameBufferList),
};
pub const BufferPool = struct_BufferPool;
pub const struct_VP9Common = extern struct {
    @"error": struct_vpx_internal_error_info = @import("std").mem.zeroes(struct_vpx_internal_error_info),
    color_space: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    color_range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    width: c_int = 0,
    height: c_int = 0,
    render_width: c_int = 0,
    render_height: c_int = 0,
    last_width: c_int = 0,
    last_height: c_int = 0,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    use_highbitdepth: c_int = 0,
    frame_to_show: [*c]YV12_BUFFER_CONFIG = null,
    prev_frame: [*c]RefCntBuffer = null,
    cur_frame: [*c]RefCntBuffer = null,
    ref_frame_map: [8]c_int = @import("std").mem.zeroes([8]c_int),
    next_ref_frame_map: [8]c_int = @import("std").mem.zeroes([8]c_int),
    frame_refs: [3]RefBuffer = @import("std").mem.zeroes([3]RefBuffer),
    new_fb_idx: c_int = 0,
    cur_show_frame_fb_idx: c_int = 0,
    last_frame_type: FRAME_TYPE = @import("std").mem.zeroes(FRAME_TYPE),
    frame_type: FRAME_TYPE = @import("std").mem.zeroes(FRAME_TYPE),
    show_frame: c_int = 0,
    last_show_frame: c_int = 0,
    show_existing_frame: c_int = 0,
    intra_only: u8 = 0,
    last_intra_only: u8 = 0,
    allow_high_precision_mv: c_int = 0,
    reset_frame_context: c_int = 0,
    MBs: c_int = 0,
    mb_rows: c_int = 0,
    mi_rows: c_int = 0,
    mb_cols: c_int = 0,
    mi_cols: c_int = 0,
    mi_stride: c_int = 0,
    tx_mode: TX_MODE = @import("std").mem.zeroes(TX_MODE),
    base_qindex: c_int = 0,
    y_dc_delta_q: c_int = 0,
    uv_dc_delta_q: c_int = 0,
    uv_ac_delta_q: c_int = 0,
    y_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    uv_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    mi_alloc_size: c_int = 0,
    mip: [*c]MODE_INFO = null,
    mi: [*c]MODE_INFO = null,
    prev_mip: [*c]MODE_INFO = null,
    prev_mi: [*c]MODE_INFO = null,
    alloc_mi: ?*const fn (cm: [*c]struct_VP9Common, mi_size: c_int) callconv(.c) c_int = null,
    free_mi: ?*const fn (cm: [*c]struct_VP9Common) callconv(.c) void = null,
    setup_mi: ?*const fn (cm: [*c]struct_VP9Common) callconv(.c) void = null,
    mi_grid_base: [*c][*c]MODE_INFO = null,
    mi_grid_visible: [*c][*c]MODE_INFO = null,
    prev_mi_grid_base: [*c][*c]MODE_INFO = null,
    prev_mi_grid_visible: [*c][*c]MODE_INFO = null,
    use_prev_frame_mvs: c_int = 0,
    seg_map_idx: c_int = 0,
    prev_seg_map_idx: c_int = 0,
    seg_map_array: [2][*c]u8 = @import("std").mem.zeroes([2][*c]u8),
    last_frame_seg_map: [*c]u8 = null,
    current_frame_seg_map: [*c]u8 = null,
    seg_map_alloc_size: c_int = 0,
    interp_filter: INTERP_FILTER = 0,
    lf_info: loop_filter_info_n = @import("std").mem.zeroes(loop_filter_info_n),
    refresh_frame_context: c_int = 0,
    ref_frame_sign_bias: [4]c_int = @import("std").mem.zeroes([4]c_int),
    lf: struct_loopfilter = @import("std").mem.zeroes(struct_loopfilter),
    seg: struct_segmentation = @import("std").mem.zeroes(struct_segmentation),
    comp_fixed_ref: MV_REFERENCE_FRAME = 0,
    comp_var_ref: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
    reference_mode: REFERENCE_MODE = @import("std").mem.zeroes(REFERENCE_MODE),
    fc: [*c]FRAME_CONTEXT = null,
    frame_contexts: [*c]FRAME_CONTEXT = null,
    frame_context_idx: c_uint = 0,
    counts: FRAME_COUNTS = @import("std").mem.zeroes(FRAME_COUNTS),
    current_video_frame: c_uint = 0,
    current_frame_coding_index: c_int = 0,
    profile: BITSTREAM_PROFILE = @import("std").mem.zeroes(BITSTREAM_PROFILE),
    bit_depth: vpx_bit_depth_t = @import("std").mem.zeroes(vpx_bit_depth_t),
    dequant_bit_depth: vpx_bit_depth_t = @import("std").mem.zeroes(vpx_bit_depth_t),
    error_resilient_mode: c_int = 0,
    frame_parallel_decoding_mode: c_int = 0,
    log2_tile_cols: c_int = 0,
    log2_tile_rows: c_int = 0,
    byte_alignment: c_int = 0,
    skip_loop_filter: c_int = 0,
    buffer_pool: [*c]BufferPool = null,
    above_seg_context: [*c]PARTITION_CONTEXT = null,
    above_context: [*c]ENTROPY_CONTEXT = null,
    above_context_alloc_cols: c_int = 0,
    lf_row: c_int = 0,
};
pub extern fn antfly_vp9_vp9_alloc_context_buffers(cm: [*c]struct_VP9Common, width: c_int, height: c_int) c_int;
pub extern fn antfly_vp9_vp9_init_context_buffers(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_set_mb_mi(cm: [*c]struct_VP9Common, width: c_int, height: c_int) void;
pub extern fn antfly_vp9_vp9_adapt_coef_probs(cm: [*c]struct_VP9Common) void;
pub const MV32 = common.MV32;
pub extern fn antfly_vp9_vp9_adapt_mv_probs(cm: [*c]struct_VP9Common, allow_hp: c_int) void;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub extern var antfly_vp9_vp9_kf_partition_probs: [16][3]vpx_prob;
pub extern const antfly_vp9_vp9_partition_tree: [6]vpx_tree_index;
pub extern fn antfly_vp9_vp9_setup_past_independence(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_adapt_mode_probs(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_iwht4x4_add(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_idct4x4_add(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_idct8x8_add(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_idct16x16_add(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_idct32x32_add(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_iht4x4_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_iht8x8_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_iht16x16_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int, eob: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_iwht4x4_add(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_idct4x4_add(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_idct8x8_add(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_idct16x16_add(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_idct32x32_add(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_iht4x4_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_iht8x8_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vp9_highbd_iht16x16_add(tx_type: TX_TYPE, input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, eob: c_int, bd: c_int) void;
pub extern var antfly_vp9_subsize_lookup: [4][13]BLOCK_SIZE;
pub extern const antfly_vp9_tx_mode_to_biggest_tx_size: [5]TX_SIZE;
pub extern var antfly_vp9_ss_size_lookup: [13][2][2]BLOCK_SIZE;
pub extern var antfly_vp9_uv_txsize_lookup: [13][4][2][2]TX_SIZE;
pub extern fn antfly_vp9_vp9_scale_mv(mv: [*c]const MV, x: c_int, y: c_int, sf: [*c]const struct_scale_factors) MV32;
pub extern fn antfly_vp9_vp9_setup_scale_factors_for_frame(sf: [*c]struct_scale_factors, other_w: c_int, other_h: c_int, this_w: c_int, this_h: c_int, use_highbd: c_int) void;
pub fn vp9_is_valid_scale(arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((sf.*.x_scale_fp != -@as(c_int, 1)) and (sf.*.y_scale_fp != -@as(c_int, 1))));
}
pub fn vp9_is_scaled(arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((vp9_is_valid_scale(sf) != 0) and ((sf.*.x_scale_fp != (@as(c_int, 1) << @intCast(@as(c_int, 14)))) or (sf.*.y_scale_fp != (@as(c_int, 1) << @intCast(@as(c_int, 14)))))));
}
pub fn valid_ref_frame_size(arg_ref_width: c_int, arg_ref_height: c_int, arg_this_width: c_int, arg_this_height: c_int) callconv(.c) c_int {
    var ref_width = arg_ref_width;
    _ = &ref_width;
    var ref_height = arg_ref_height;
    _ = &ref_height;
    var this_width = arg_this_width;
    _ = &this_width;
    var this_height = arg_this_height;
    _ = &this_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(((((@as(c_int, 2) * this_width) >= ref_width) and ((@as(c_int, 2) * this_height) >= ref_height)) and (this_width <= (@as(c_int, 16) * ref_width))) and (this_height <= (@as(c_int, 16) * ref_height))));
}
pub const SEG_LVL_MAX = common.SEG_LVL_MAX;
pub const SEG_LVL_FEATURES = common.SEG_LVL_FEATURES;
pub extern fn antfly_vp9_vp9_clearall_segfeatures(seg: [*c]struct_segmentation) void;
pub extern fn antfly_vp9_vp9_enable_segfeature(seg: [*c]struct_segmentation, segment_id: c_int, feature_id: SEG_LVL_FEATURES) void;
pub extern fn antfly_vp9_vp9_seg_feature_data_max(feature_id: SEG_LVL_FEATURES) c_int;
pub extern fn antfly_vp9_vp9_is_segfeature_signed(feature_id: SEG_LVL_FEATURES) c_int;
pub extern fn antfly_vp9_vp9_set_segdata(seg: [*c]struct_segmentation, segment_id: c_int, feature_id: SEG_LVL_FEATURES, seg_data: c_int) void;
pub extern fn antfly_vp9_vp9_tile_init(tile: [*c]TileInfo, cm: [*c]const struct_VP9Common, row: c_int, col: c_int) void;
pub extern fn antfly_vp9_vp9_tile_set_row(tile: [*c]TileInfo, cm: [*c]const struct_VP9Common, row: c_int) void;
pub extern fn antfly_vp9_vp9_tile_set_col(tile: [*c]TileInfo, cm: [*c]const struct_VP9Common, col: c_int) void;
pub extern fn antfly_vp9_vp9_get_tile_n_bits(mi_cols: c_int, min_log2_tile_cols: [*c]c_int, max_log2_tile_cols: [*c]c_int) void;
pub const KEY_FRAME = common.KEY_FRAME;
pub const FRAME_TYPE = common.FRAME_TYPE;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
pub fn is_inter_block(arg_mi: [*c]const MODE_INFO) callconv(.c) c_int {
    var mi = arg_mi;
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(@as(c_int, mi.*.ref_frame[@as(c_int, 0)]) > @as(c_int, 0)));
}
pub fn has_second_ref(arg_mi: [*c]const MODE_INFO) callconv(.c) c_int {
    var mi = arg_mi;
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(@as(c_int, mi.*.ref_frame[@as(c_int, 1)]) > @as(c_int, 0)));
}
pub const MACROBLOCKD = struct_macroblockd;
pub fn get_plane_type(arg_plane: c_int) callconv(.c) PLANE_TYPE {
    var plane = arg_plane;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @intCast(@as(c_int, @intFromBool(plane > @as(c_int, 0))));
}
pub fn get_subsize(arg_bsize: BLOCK_SIZE, arg_partition: PARTITION_TYPE) callconv(.c) BLOCK_SIZE {
    var bsize = arg_bsize;
    _ = &bsize;
    var partition = arg_partition;
    _ = &partition;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return antfly_vp9_subsize_lookup[partition][bsize];
}
pub extern const antfly_vp9_intra_mode_to_tx_type_lookup: [10]TX_TYPE;
pub extern fn antfly_vp9_vp9_setup_block_planes(xd: [*c]MACROBLOCKD, ss_x: c_int, ss_y: c_int) void;
pub fn get_uv_tx_size(arg_mi: [*c]const MODE_INFO, arg_pd: [*c]const struct_macroblockd_plane) callconv(.c) TX_SIZE {
    var mi = arg_mi;
    _ = &mi;
    var pd = arg_pd;
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return antfly_vp9_uv_txsize_lookup[mi.*.sb_type][mi.*.tx_size][@bitCast(@as(isize, @intCast(pd.*.subsampling_x)))][@bitCast(@as(isize, @intCast(pd.*.subsampling_y)))];
}
pub const loop_filter_thresh = common.loop_filter_thresh;
pub const loop_filter_info_n = extern struct {
    lfthr: [64]loop_filter_thresh = @import("std").mem.zeroes([64]loop_filter_thresh),
    lvl: [8][4][2]u8 = @import("std").mem.zeroes([8][4][2]u8),
};
pub const LOOP_FILTER_MASK = common.LOOP_FILTER_MASK;
pub const struct_LoopFilterWorkerData = extern struct {
    frame_buffer: [*c]YV12_BUFFER_CONFIG = null,
    cm: [*c]struct_VP9Common = null,
    planes: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    start: c_int = 0,
    stop: c_int = 0,
    y_only: c_int = 0,
};
pub const LFWorkerData = struct_LoopFilterWorkerData;
pub const struct_VP9LfSyncData = extern struct {
    cur_sb_col: [*c]c_int = null,
    sync_range: c_int = 0,
    rows: c_int = 0,
    lfdata: [*c]LFWorkerData = null,
    num_workers: c_int = 0,
    num_active_workers: c_int = 0,
    num_tiles_done: [*c]c_int = null,
    corrupted: c_int = 0,
};
pub extern fn antfly_vp9_vp9_loop_filter_frame_init(cm: [*c]struct_VP9Common, default_filt_lvl: c_int) void;
pub extern fn antfly_vp9_vp9_build_mask(cm: [*c]struct_VP9Common, mi: [*c]const MODE_INFO, mi_row: c_int, mi_col: c_int, bw: c_int, bh: c_int) void;
pub extern fn antfly_vp9_vp9_reset_lfm(cm: [*c]struct_VP9Common) void;
pub extern fn antfly_vp9_vp9_loop_filter_data_reset(lf_data: [*c]LFWorkerData, frame_buffer: [*c]YV12_BUFFER_CONFIG, cm: [*c]struct_VP9Common, planes: [*c]const struct_macroblockd_plane) void;
pub extern fn antfly_vp9_vp9_loop_filter_worker(arg1: ?*anyopaque, unused: ?*anyopaque) c_int;
pub const VP9LfSync = struct_VP9LfSyncData;
pub extern fn antfly_vp9_vp9_loop_filter_frame_mt(frame: [*c]YV12_BUFFER_CONFIG, cm: [*c]struct_VP9Common, planes: [*c]struct_macroblockd_plane, frame_filter_level: c_int, y_only: c_int, partial_frame: c_int, workers: [*c]VPxWorker, num_workers: c_int, lf_sync: [*c]VP9LfSync) void;
pub extern fn antfly_vp9_vp9_lpf_mt_init(lf_sync: [*c]VP9LfSync, cm: [*c]struct_VP9Common, frame_filter_level: c_int, num_workers: c_int) void;
pub extern fn antfly_vp9_vp9_loopfilter_rows(lf_data: [*c]LFWorkerData, lf_sync: [*c]VP9LfSync) void;
pub extern fn antfly_vp9_vp9_set_row(lf_sync: [*c]VP9LfSync, num_tiles: c_int, row: c_int, is_last_row: c_int, corrupted: c_int) void;
pub extern fn antfly_vp9_vp9_loopfilter_job(lf_data: [*c]LFWorkerData, lf_sync: [*c]VP9LfSync) void;
pub extern fn antfly_vp9_vp9_accumulate_frame_counts(accum: [*c]struct_FRAME_COUNTS, counts: [*c]const struct_FRAME_COUNTS, is_dec: c_int) void;
pub extern fn antfly_vp9_vp9_dc_quant(qindex: c_int, delta: c_int, bit_depth: vpx_bit_depth_t) i16;
pub extern fn antfly_vp9_vp9_ac_quant(qindex: c_int, delta: c_int, bit_depth: vpx_bit_depth_t) i16;
pub extern fn antfly_vp9_vp9_get_qindex(seg: [*c]const struct_segmentation, segment_id: c_int, base_qindex: c_int) c_int;
const struct_unnamed_12 = extern struct {
    above: PARTITION_CONTEXT = 0,
    left: PARTITION_CONTEXT = 0,
};
pub extern const antfly_vp9_partition_context_lookup: [13]struct_unnamed_12;
pub const SINGLE_REFERENCE = common.SINGLE_REFERENCE;
pub const COMPOUND_REFERENCE = common.COMPOUND_REFERENCE;
pub const REFERENCE_MODE_SELECT = common.REFERENCE_MODE_SELECT;
pub const REFERENCE_MODE = common.REFERENCE_MODE;
pub const MV_REF = extern struct {
    mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
    ref_frame: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
};
pub const RefCntBuffer = extern struct {
    ref_count: c_int = 0,
    mvs: [*c]MV_REF = null,
    mi_rows: c_int = 0,
    mi_cols: c_int = 0,
    released: u8 = 0,
    frame_index: c_int = 0,
    frame_coding_index: c_int = 0,
    raw_frame_buffer: vpx_codec_frame_buffer_t = @import("std").mem.zeroes(vpx_codec_frame_buffer_t),
    buf: YV12_BUFFER_CONFIG = @import("std").mem.zeroes(YV12_BUFFER_CONFIG),
};
pub const VP9_COMMON = struct_VP9Common;
pub fn get_frame_new_buffer(arg_cm: [*c]VP9_COMMON) callconv(.c) [*c]YV12_BUFFER_CONFIG {
    var cm = arg_cm;
    _ = &cm;
    // Pure frame view: polling here can cancel between the caller guard and
    // its dereference. The surrounding decoder checkpoints own cancellation.
    return &cm.*.buffer_pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf;
}
pub fn ref_cnt_fb(arg_bufs: [*c]RefCntBuffer, arg_idx: [*c]c_int, arg_new_idx: c_int) callconv(.c) void {
    var bufs = arg_bufs;
    _ = &bufs;
    var idx = arg_idx;
    _ = &idx;
    var new_idx = arg_new_idx;
    _ = &new_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ref_index: c_int = idx.*;
    _ = &ref_index;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((ref_index >= @as(c_int, 0)) and (bufs[@bitCast(@as(isize, @intCast(ref_index)))].ref_count > @as(c_int, 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        bufs[@bitCast(@as(isize, @intCast(ref_index)))].ref_count -= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    idx.* = new_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    bufs[@bitCast(@as(isize, @intCast(new_idx)))].ref_count += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn mi_cols_aligned_to_sb(arg_n_mis: c_int) callconv(.c) c_int {
    var n_mis = arg_n_mis;
    _ = &n_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return (n_mis + ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1));
}
pub fn frame_is_intra_only(cm: [*c]const VP9_COMMON) callconv(.c) c_int {
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((cm.*.frame_type == @as(FRAME_TYPE, KEY_FRAME)) or (@as(c_int, cm.*.intra_only) != 0)));
}
pub fn set_partition_probs(cm: [*c]const VP9_COMMON, xd: [*c]MACROBLOCKD) callconv(.c) void {
    _ = &cm;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.partition_probs = if (frame_is_intra_only(cm) != 0) &antfly_vp9_vp9_kf_partition_probs[@as(c_int, 0)] else @as([*c][3]vpx_prob, @ptrCast(@alignCast(&cm.*.fc.*.partition_prob)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn vp9_init_macroblockd(arg_cm: [*c]VP9_COMMON, arg_xd: [*c]MACROBLOCKD, arg_dqcoeff: [*c]tran_low_t) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var xd = arg_xd;
    _ = &xd;
    var dqcoeff = arg_dqcoeff;
    _ = &dqcoeff;
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
            xd.*.plane[@bitCast(@as(isize, @intCast(i)))].dqcoeff = dqcoeff;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.above_context[@bitCast(@as(isize, @intCast(i)))] = cm.*.above_context + (((@as(c_ulong, @bitCast(@as(c_long, i))) *% @sizeOf(@TypeOf(cm.*.above_context.*))) *% @as(c_ulong, 2)) *% @as(c_ulong, @bitCast(@as(c_long, mi_cols_aligned_to_sb(cm.*.mi_cols)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (get_plane_type(i) == @as(PLANE_TYPE, PLANE_TYPE_Y)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][2]i16, @ptrCast(@alignCast(&xd.*.plane[@bitCast(@as(isize, @intCast(i)))].seg_dequant))))), @ptrCast(@alignCast(@as([*c][2]i16, @ptrCast(@alignCast(&cm.*.y_dequant))))), @sizeOf(@TypeOf(cm.*.y_dequant)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(@as([*c][2]i16, @ptrCast(@alignCast(&xd.*.plane[@bitCast(@as(isize, @intCast(i)))].seg_dequant))))), @ptrCast(@alignCast(@as([*c][2]i16, @ptrCast(@alignCast(&cm.*.uv_dequant))))), @sizeOf(@TypeOf(cm.*.uv_dequant)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.fc = cm.*.fc;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.above_seg_context = cm.*.above_seg_context;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.mi_stride = cm.*.mi_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.error_info = &cm.*.@"error";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    set_partition_probs(cm, xd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn set_skip_context(arg_xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_idx: c_int = mi_col * @as(c_int, 2);
    _ = &above_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const left_idx: c_int = (mi_row * @as(c_int, 2)) & @as(c_int, 15);
    _ = &left_idx;
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
            const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(i)))];
            _ = &pd;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            pd.*.above_context = &xd.*.above_context[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(above_idx >> @intCast(pd.*.subsampling_x))))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            pd.*.left_context = &xd.*.left_context[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(left_idx >> @intCast(pd.*.subsampling_y))))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn set_mi_row_col(arg_xd: [*c]MACROBLOCKD, tile: [*c]const TileInfo, arg_mi_row: c_int, arg_bh: c_int, arg_mi_col: c_int, arg_bw: c_int, arg_mi_rows: c_int, arg_mi_cols: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    _ = &tile;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var bh = arg_bh;
    _ = &bh;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bw = arg_bw;
    _ = &bw;
    var mi_rows = arg_mi_rows;
    _ = &mi_rows;
    var mi_cols = arg_mi_cols;
    _ = &mi_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.mb_to_top_edge = -((mi_row * (@as(c_int, 1) << @intCast(@as(c_int, 3)))) * @as(c_int, 8));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.mb_to_bottom_edge = (((mi_rows - bh) - mi_row) * (@as(c_int, 1) << @intCast(@as(c_int, 3)))) * @as(c_int, 8);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.mb_to_left_edge = -((mi_col * (@as(c_int, 1) << @intCast(@as(c_int, 3)))) * @as(c_int, 8));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.mb_to_right_edge = (((mi_cols - bw) - mi_col) * (@as(c_int, 1) << @intCast(@as(c_int, 3)))) * @as(c_int, 8);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.above_mi = @ptrCast(@alignCast(if (mi_row != @as(c_int, 0)) @as(?*anyopaque, @ptrCast(@alignCast(xd.*.mi[@bitCast(@as(isize, @intCast(-xd.*.mi_stride)))]))) else @as(?*anyopaque, null)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.left_mi = @ptrCast(@alignCast(if (mi_col > tile.*.mi_col_start) @as(?*anyopaque, @ptrCast(@alignCast(xd.*.mi[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) else @as(?*anyopaque, null)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub extern fn antfly_vp9_vp9_compound_reference_allowed(cm: [*c]const VP9_COMMON) c_int;
pub extern fn antfly_vp9_vp9_setup_compound_reference_mode(cm: [*c]VP9_COMMON) void;
pub extern fn antfly_vp9_vp9_predict_intra_block(xd: [*c]const MACROBLOCKD, bwl_in: c_int, tx_size: TX_SIZE, mode: PREDICTION_MODE, ref: [*c]const u8, ref_stride: c_int, dst: [*c]u8, dst_stride: c_int, aoff: c_int, loff: c_int, plane: c_int) void;
pub fn inter_predictor(arg_src: [*c]const u8, arg_src_stride: c_int, arg_dst: [*c]u8, arg_dst_stride: c_int, subpel_x: c_int, subpel_y: c_int, arg_sf: [*c]const struct_scale_factors, arg_w: c_int, arg_h: c_int, arg_ref: c_int, arg_kernel: [*c]const InterpKernel, arg_xs: c_int, arg_ys: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    _ = &subpel_x;
    _ = &subpel_y;
    var sf = arg_sf;
    _ = &sf;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var ref = arg_ref;
    _ = &ref;
    var kernel = arg_kernel;
    _ = &kernel;
    var xs = arg_xs;
    _ = &xs;
    var ys = arg_ys;
    _ = &ys;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.predict[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(subpel_x != @as(c_int, 0))))))][@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(subpel_y != @as(c_int, 0))))))][@bitCast(@as(isize, @intCast(ref)))].?(src, src_stride, dst, dst_stride, kernel, subpel_x, xs, subpel_y, ys, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_inter_predictor(arg_src: [*c]const u16, arg_src_stride: c_int, arg_dst: [*c]u16, arg_dst_stride: c_int, subpel_x: c_int, subpel_y: c_int, arg_sf: [*c]const struct_scale_factors, arg_w: c_int, arg_h: c_int, arg_ref: c_int, arg_kernel: [*c]const InterpKernel, arg_xs: c_int, arg_ys: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    _ = &subpel_x;
    _ = &subpel_y;
    var sf = arg_sf;
    _ = &sf;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var ref = arg_ref;
    _ = &ref;
    var kernel = arg_kernel;
    _ = &kernel;
    var xs = arg_xs;
    _ = &xs;
    var ys = arg_ys;
    _ = &ys;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.highbd_predict[@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(subpel_x != @as(c_int, 0))))))][@bitCast(@as(isize, @intCast(@as(c_int, @intFromBool(subpel_y != @as(c_int, 0))))))][@bitCast(@as(isize, @intCast(ref)))].?(src, src_stride, dst, dst_stride, kernel, subpel_x, xs, subpel_y, ys, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub extern fn antfly_vp9_average_split_mvs(pd: [*c]const struct_macroblockd_plane, mi: [*c]const MODE_INFO, ref: c_int, block: c_int) MV;
pub extern fn antfly_vp9_clamp_mv_to_umv_border_sb(xd: [*c]const MACROBLOCKD, src_mv: [*c]const MV, bw: c_int, bh: c_int, ss_x: c_int, ss_y: c_int) MV;
pub extern fn antfly_vp9_vp9_setup_dst_planes(planes: [*c]struct_macroblockd_plane, src: [*c]const YV12_BUFFER_CONFIG, mi_row: c_int, mi_col: c_int) void;
pub extern fn antfly_vp9_vp9_setup_pre_planes(xd: [*c]MACROBLOCKD, idx: c_int, src: [*c]const YV12_BUFFER_CONFIG, mi_row: c_int, mi_col: c_int, sf: [*c]const struct_scale_factors) void;
pub const struct_TileWorkerData = extern struct {
    data_end: [*c]const u8 align(32) = null,
    buf_start: c_int = 0,
    buf_end: c_int = 0,
    bit_reader: vpx_reader = @import("std").mem.zeroes(vpx_reader),
    counts: FRAME_COUNTS = @import("std").mem.zeroes(FRAME_COUNTS),
    lf_data: [*c]LFWorkerData = null,
    lf_sync: [*c]VP9LfSync = null,
    xd: MACROBLOCKD align(32) = @import("std").mem.zeroes(MACROBLOCKD),
    dqcoeff: [1024]tran_low_t = @import("std").mem.zeroes([1024]tran_low_t),
    extend_and_predict_buf: [25600]u16 = @import("std").mem.zeroes([25600]u16),
    error_info: struct_vpx_internal_error_info = @import("std").mem.zeroes(struct_vpx_internal_error_info),
};
pub const TileWorkerData = struct_TileWorkerData;
pub const TileBuffer = common.TileBuffer;
pub const struct_ThreadData = extern struct {
    pbi: [*c]struct_VP9Decoder = null,
    lf_data: [*c]LFWorkerData = null,
    lf_sync: [*c]VP9LfSync = null,
};
pub const ThreadData = struct_ThreadData;
pub const struct_RowMTWorkerData = extern struct {
    num_sbs: c_int = 0,
    eob: [3][*c]c_int = @import("std").mem.zeroes([3][*c]c_int),
    partition: [*c]PARTITION_TYPE = null,
    dqcoeff: [3][*c]tran_low_t = @import("std").mem.zeroes([3][*c]tran_low_t),
    recon_map: [*c]i8 = null,
    data_end: [*c]const u8 = null,
    jobq_buf: [*c]u8 = null,
    jobq: JobQueueRowMt = @import("std").mem.zeroes(JobQueueRowMt),
    jobq_size: usize = 0,
    num_tiles_done: c_int = 0,
    num_jobs: c_int = 0,
    thread_data: [*c]ThreadData = null,
};
pub const RowMTWorkerData = struct_RowMTWorkerData;
pub const struct_VP9Decoder = extern struct {
    mb: MACROBLOCKD = @import("std").mem.zeroes(MACROBLOCKD),
    common: VP9_COMMON = @import("std").mem.zeroes(VP9_COMMON),
    ready_for_new_data: c_int = 0,
    refresh_frame_flags: c_int = 0,
    cur_buf: [*c]RefCntBuffer = null,
    lf_worker: VPxWorker = @import("std").mem.zeroes(VPxWorker),
    tile_workers: [*c]VPxWorker = null,
    tile_worker_data: [*c]TileWorkerData = null,
    tile_buffers: [64]TileBuffer = @import("std").mem.zeroes([64]TileBuffer),
    num_tile_workers: c_int = 0,
    total_tiles: c_int = 0,
    lf_row_sync: VP9LfSync = @import("std").mem.zeroes(VP9LfSync),
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    max_threads: c_int = 0,
    inv_tile_order: c_int = 0,
    need_resync: c_int = 0,
    hold_ref_buf: c_int = 0,
    row_mt: c_int = 0,
    lpf_mt_opt: c_int = 0,
    row_mt_worker_data: [*c]RowMTWorkerData = null,
};
pub const JobQueueRowMt = common.JobQueueRowMt;
pub extern fn antfly_vp9_vp9_jobq_init(jobq: [*c]JobQueueRowMt, buf: [*c]u8, buf_size: usize) void;
pub extern fn antfly_vp9_vp9_jobq_reset(jobq: [*c]JobQueueRowMt) void;
pub extern fn antfly_vp9_vp9_jobq_queue(jobq: [*c]JobQueueRowMt, job: ?*anyopaque, job_size: usize) c_int;
pub extern fn antfly_vp9_vp9_jobq_dequeue(jobq: [*c]JobQueueRowMt, job: ?*anyopaque, job_size: usize, blocking: c_int) c_int;
pub const PARSE_JOB = common.PARSE_JOB;
pub const RECON_JOB = common.RECON_JOB;
pub const LPF_JOB = common.LPF_JOB;
pub const JobType = common.JobType;
pub const process_block_fn_t = ?*const fn (twd: [*c]TileWorkerData, pbi: [*c]struct_VP9Decoder, mi_row: c_int, mi_col: c_int, bsize: BLOCK_SIZE, bwl: c_int, bhl: c_int) callconv(.c) void;
pub const Job = common.Job;
pub const VP9Decoder = struct_VP9Decoder;
pub extern fn antfly_vp9_vp9_dec_alloc_row_mt_mem(row_mt_worker_data: [*c]RowMTWorkerData, cm: [*c]VP9_COMMON, num_sbs: c_int, max_threads: c_int, num_jobs: c_int) void;
pub extern fn antfly_vp9_vp9_dec_free_row_mt_mem(row_mt_worker_data: [*c]RowMTWorkerData) void;
pub const ScanOrder = struct_ScanOrder;
pub extern const antfly_vp9_vp9_default_scan_orders: [4]ScanOrder;
pub extern var antfly_vp9_vp9_scan_orders: [4][4]ScanOrder;
pub extern fn antfly_vp9_vp9_decode_block_tokens(twd: [*c]TileWorkerData, plane: c_int, sc: [*c]const ScanOrder, x: c_int, y: c_int, tx_size: TX_SIZE, seg_id: c_int) c_int;
pub extern fn antfly_vp9_vp9_read_mode_info(twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, mi_row: c_int, mi_col: c_int, x_mis: c_int, y_mis: c_int) void;
pub extern fn antfly_vp9_vp9_diff_update_prob(r: [*c]vpx_reader, p: [*c]vpx_prob) void;
pub const predict_recon_func = ?*const fn (twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, plane: c_int, row: c_int, col: c_int, tx_size: TX_SIZE) callconv(.c) c_int;
pub const intra_recon_func = ?*const fn (twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, plane: c_int, row: c_int, col: c_int, tx_size: TX_SIZE) callconv(.c) void;
pub fn read_is_valid(arg_start: [*c]const u8, arg_len: usize, arg_end: [*c]const u8) callconv(.c) c_int {
    var start = arg_start;
    _ = &start;
    var len = arg_len;
    _ = &len;
    var end = arg_end;
    _ = &end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((len != @as(usize, 0)) and (len <= @as(usize, @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(end) -% @intFromPtr(start))), @sizeOf(u8))))))));
}
pub fn decode_unsigned_max(arg_rb: [*c]struct_vpx_read_bit_buffer, arg_max: c_int) callconv(.c) c_int {
    var rb = arg_rb;
    _ = &rb;
    var max = arg_max;
    _ = &max;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const data: c_int = antfly_vp9_vpx_rb_read_literal(rb, get_unsigned_bits(@bitCast(@as(c_int, max))));
    _ = &data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (data > max) max else data;
}
pub fn read_tx_mode(arg_r: [*c]vpx_reader) callconv(.c) TX_MODE {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tx_mode: TX_MODE = @bitCast(vpx_read_literal(r, 2));
    _ = &tx_mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (tx_mode == @as(TX_MODE, ALLOW_32X32)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tx_mode +%= @bitCast(vpx_read_bit(r));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return tx_mode;
}
pub fn read_tx_mode_probs(arg_tx_probs_1: [*c]struct_tx_probs, arg_r: [*c]vpx_reader) callconv(.c) void {
    var tx_probs_1 = arg_tx_probs_1;
    _ = &tx_probs_1;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 3))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_diff_update_prob(r, &tx_probs_1.*.p8x8[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))]);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 2))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_diff_update_prob(r, &tx_probs_1.*.p16x16[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))]);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 4)))))) - @as(c_int, 1))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_diff_update_prob(r, &tx_probs_1.*.p32x32[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))]);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn read_switchable_interp_probs(arg_fc: [*c]FRAME_CONTEXT, arg_r: [*c]vpx_reader) callconv(.c) void {
    var fc = arg_fc;
    _ = &fc;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < (@as(c_int, 3) + @as(c_int, 1))) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < (@as(c_int, 3) - @as(c_int, 1))) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_diff_update_prob(r, &fc.*.switchable_interp_prob[@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(i)))]);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn read_inter_mode_probs(arg_fc: [*c]FRAME_CONTEXT, arg_r: [*c]vpx_reader) callconv(.c) void {
    var fc = arg_fc;
    _ = &fc;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 7)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (((@as(c_int, 1) + @as(c_int, 13)) - @as(c_int, 10)) - @as(c_int, 1))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vp9_diff_update_prob(r, &fc.*.inter_mode_probs[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))]);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn read_frame_reference_mode(arg_cm: [*c]const VP9_COMMON, arg_r: [*c]vpx_reader) callconv(.c) REFERENCE_MODE {
    var cm = arg_cm;
    _ = &cm;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_vp9_vp9_compound_reference_allowed(cm) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @bitCast(if (vpx_read_bit(r) != 0) if (vpx_read_bit(r) != 0) REFERENCE_MODE_SELECT else COMPOUND_REFERENCE else SINGLE_REFERENCE);
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return SINGLE_REFERENCE;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub fn read_frame_reference_mode_probs(arg_cm: [*c]VP9_COMMON, arg_r: [*c]vpx_reader) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const fc: [*c]FRAME_CONTEXT = cm.*.fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.reference_mode == @as(REFERENCE_MODE, REFERENCE_MODE_SELECT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 5)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_diff_update_prob(r, &fc.*.comp_inter_prob[@bitCast(@as(isize, @intCast(i)))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (cm.*.reference_mode != @as(REFERENCE_MODE, COMPOUND_REFERENCE)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 5)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_diff_update_prob(r, &fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_diff_update_prob(r, &fc.*.single_ref_prob[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (cm.*.reference_mode != @as(REFERENCE_MODE, SINGLE_REFERENCE)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 5)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_diff_update_prob(r, &fc.*.comp_ref_prob[@bitCast(@as(isize, @intCast(i)))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn update_mv_probs(arg_p: [*c]vpx_prob, arg_n: c_int, arg_r: [*c]vpx_reader) callconv(.c) void {
    var p = arg_p;
    _ = &p;
    var n = arg_n;
    _ = &n;
    var r = arg_r;
    _ = &r;
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
        while (i < n) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (vpx_read(r, 252) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                p[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate((vpx_read_literal(r, 7) << @intCast(@as(c_int, 1))) | @as(c_int, 1))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn read_mv_probs(arg_ctx: [*c]nmv_context, arg_allow_hp: c_int, arg_r: [*c]vpx_reader) callconv(.c) void {
    var ctx = arg_ctx;
    _ = &ctx;
    var allow_hp = arg_allow_hp;
    _ = &allow_hp;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    update_mv_probs(@ptrCast(@alignCast(&ctx.*.joints)), @as(c_int, 4) - @as(c_int, 1), r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const comp_ctx: [*c]nmv_component = &ctx.*.comps[@bitCast(@as(isize, @intCast(i)))];
            _ = &comp_ctx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            update_mv_probs(&comp_ctx.*.sign, 1, r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            update_mv_probs(@ptrCast(@alignCast(&comp_ctx.*.classes)), @as(c_int, 11) - @as(c_int, 1), r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            update_mv_probs(@ptrCast(@alignCast(&comp_ctx.*.class0)), (@as(c_int, 1) << @intCast(@as(c_int, 1))) - @as(c_int, 1), r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            update_mv_probs(@ptrCast(@alignCast(&comp_ctx.*.bits)), (@as(c_int, 11) + @as(c_int, 1)) - @as(c_int, 2), r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 2)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const comp_ctx: [*c]nmv_component = &ctx.*.comps[@bitCast(@as(isize, @intCast(i)))];
            _ = &comp_ctx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < (@as(c_int, 1) << @intCast(@as(c_int, 1)))) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    update_mv_probs(@ptrCast(@alignCast(&comp_ctx.*.class0_fp[@bitCast(@as(isize, @intCast(j)))])), @as(c_int, 4) - @as(c_int, 1), r);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            update_mv_probs(@ptrCast(@alignCast(&comp_ctx.*.fp)), 3, r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (allow_hp != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 2)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const comp_ctx: [*c]nmv_component = &ctx.*.comps[@bitCast(@as(isize, @intCast(i)))];
                _ = &comp_ctx;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                update_mv_probs(&comp_ctx.*.class0_hp, 1, r);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                update_mv_probs(&comp_ctx.*.hp, 1, r);
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
pub fn inverse_transform_block_inter(arg_xd: [*c]MACROBLOCKD, arg_plane: c_int, tx_size: TX_SIZE, arg_dst: [*c]u8, arg_stride: c_int, arg_eob: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var plane = arg_plane;
    _ = &plane;
    _ = &tx_size;
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dqcoeff: [*c]tran_low_t = pd.*.dqcoeff;
    _ = &dqcoeff;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((xd.*.cur_buf.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const dst16: [*c]u16 = @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst))) << @intCast(@as(usize, 1))));
        _ = &dst16;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (xd.*.lossless != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_highbd_iwht4x4_add(dqcoeff, dst16, stride, eob, xd.*.bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            blk: {
                switch (@as(c_int, tx_size)) {
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))) => {
                        antfly_vp9_vp9_highbd_idct4x4_add(dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                        antfly_vp9_vp9_highbd_idct8x8_add(dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                        antfly_vp9_vp9_highbd_idct16x16_add(dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                        antfly_vp9_vp9_highbd_idct32x32_add(dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    else => {
                        _ = @as(c_int, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    },
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (xd.*.lossless != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_iwht4x4_add(dqcoeff, dst, stride, eob);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            blk: {
                switch (@as(c_int, tx_size)) {
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))) => {
                        antfly_vp9_vp9_idct4x4_add(dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                        antfly_vp9_vp9_idct8x8_add(dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                        antfly_vp9_vp9_idct16x16_add(dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                        antfly_vp9_vp9_idct32x32_add(dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    else => {
                        _ = @as(c_int, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        return;
                    },
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dqcoeff[@as(c_int, 0)] = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((@as(c_int, tx_size) <= @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))))) and (eob <= @as(c_int, 10))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 4) * (@as(c_int, 4) << @intCast(@as(c_int, tx_size)))))) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) and (eob <= @as(c_int, 34))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, 256) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn inverse_transform_block_intra(arg_xd: [*c]MACROBLOCKD, arg_plane: c_int, tx_type: TX_TYPE, tx_size: TX_SIZE, arg_dst: [*c]u8, arg_stride: c_int, arg_eob: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var plane = arg_plane;
    _ = &plane;
    _ = &tx_type;
    _ = &tx_size;
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dqcoeff: [*c]tran_low_t = pd.*.dqcoeff;
    _ = &dqcoeff;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((xd.*.cur_buf.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const dst16: [*c]u16 = @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst))) << @intCast(@as(usize, 1))));
        _ = &dst16;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (xd.*.lossless != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_highbd_iwht4x4_add(dqcoeff, dst16, stride, eob, xd.*.bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            blk: {
                switch (@as(c_int, tx_size)) {
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))) => {
                        antfly_vp9_vp9_highbd_iht4x4_add(tx_type, dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                        antfly_vp9_vp9_highbd_iht8x8_add(tx_type, dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                        antfly_vp9_vp9_highbd_iht16x16_add(tx_type, dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                        antfly_vp9_vp9_highbd_idct32x32_add(dqcoeff, dst16, stride, eob, xd.*.bd);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    else => {
                        _ = @as(c_int, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    },
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (xd.*.lossless != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_iwht4x4_add(dqcoeff, dst, stride, eob);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            blk: {
                switch (@as(c_int, tx_size)) {
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))) => {
                        antfly_vp9_vp9_iht4x4_add(tx_type, dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))) => {
                        antfly_vp9_vp9_iht8x8_add(tx_type, dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))) => {
                        antfly_vp9_vp9_iht16x16_add(tx_type, dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))) => {
                        antfly_vp9_vp9_idct32x32_add(dqcoeff, dst, stride, eob);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        break :blk;
                    },
                    else => {
                        _ = @as(c_int, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        return;
                    },
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dqcoeff[@as(c_int, 0)] = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (((tx_type == @as(TX_TYPE, DCT_DCT)) and (@as(c_int, tx_size) <= @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))))) and (eob <= @as(c_int, 10))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 4) * (@as(c_int, 4) << @intCast(@as(c_int, tx_size)))))) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((@as(c_int, tx_size) == @as(c_int, @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))))) and (eob <= @as(c_int, 34))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, 256) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dqcoeff)), 0, @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))) *% @sizeOf(@TypeOf(dqcoeff[@as(c_int, 0)])));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn predict_and_reconstruct_intra_block(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mode: PREDICTION_MODE = @bitCast(@as(i8, @truncate(if (plane == @as(c_int, 0)) @as(c_int, mi.*.mode) else @as(c_int, mi.*.uv_mode))));
    _ = &mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var dst: [*c]u8 = undefined;
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst = &pd.*.dst.buf[@bitCast(@as(isize, @intCast(((@as(c_int, 4) * row) * pd.*.dst.stride) + (@as(c_int, 4) * col))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, mi.*.sb_type) < @as(c_int, 3)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (plane == @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mode = xd.*.mi[@as(c_int, 0)].*.bmi[@bitCast(@as(isize, @intCast((row << @intCast(@as(c_int, 1))) + col)))].as_mode;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    antfly_vp9_vp9_predict_intra_block(xd, pd.*.n4_wl, tx_size, mode, dst, pd.*.dst.stride, dst, pd.*.dst.stride, col, row, plane);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(@as(c_int, mi.*.skip) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const tx_type: TX_TYPE = @bitCast(if ((plane != 0) or (xd.*.lossless != 0)) DCT_DCT else @as(c_int, @bitCast(antfly_vp9_intra_mode_to_tx_type_lookup[mode])));
        _ = &tx_type;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var sc: [*c]const ScanOrder = if ((plane != 0) or (xd.*.lossless != 0)) &antfly_vp9_vp9_default_scan_orders[tx_size] else &antfly_vp9_vp9_scan_orders[tx_size][tx_type];
        _ = &sc;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const eob: c_int = antfly_vp9_vp9_decode_block_tokens(twd, plane, sc, col, row, tx_size, mi.*.segment_id);
        _ = &eob;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob > @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            inverse_transform_block_intra(xd, plane, tx_type, tx_size, dst, pd.*.dst.stride, eob);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
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
pub fn parse_intra_block_row_mt(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mode: PREDICTION_MODE = @bitCast(@as(i8, @truncate(if (plane == @as(c_int, 0)) @as(c_int, mi.*.mode) else @as(c_int, mi.*.uv_mode))));
    _ = &mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, mi.*.sb_type) < @as(c_int, 3)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (plane == @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mode = xd.*.mi[@as(c_int, 0)].*.bmi[@bitCast(@as(isize, @intCast((row << @intCast(@as(c_int, 1))) + col)))].as_mode;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (!(@as(c_int, mi.*.skip) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
        _ = &pd;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const tx_type: TX_TYPE = @bitCast(if ((plane != 0) or (xd.*.lossless != 0)) DCT_DCT else @as(c_int, @bitCast(antfly_vp9_intra_mode_to_tx_type_lookup[mode])));
        _ = &tx_type;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var sc: [*c]const ScanOrder = if ((plane != 0) or (xd.*.lossless != 0)) &antfly_vp9_vp9_default_scan_orders[tx_size] else &antfly_vp9_vp9_scan_orders[tx_size][tx_type];
        _ = &sc;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pd.*.eob.* = antfly_vp9_vp9_decode_block_tokens(twd, plane, sc, col, row, tx_size, mi.*.segment_id);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pd.*.dqcoeff += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pd.*.eob += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn predict_and_reconstruct_intra_block_row_mt(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mode: PREDICTION_MODE = @bitCast(@as(i8, @truncate(if (plane == @as(c_int, 0)) @as(c_int, mi.*.mode) else @as(c_int, mi.*.uv_mode))));
    _ = &mode;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var dst: [*c]u8 = &pd.*.dst.buf[@bitCast(@as(isize, @intCast(((@as(c_int, 4) * row) * pd.*.dst.stride) + (@as(c_int, 4) * col))))];
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, mi.*.sb_type) < @as(c_int, 3)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (plane == @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            mode = xd.*.mi[@as(c_int, 0)].*.bmi[@bitCast(@as(isize, @intCast((row << @intCast(@as(c_int, 1))) + col)))].as_mode;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    antfly_vp9_vp9_predict_intra_block(xd, pd.*.n4_wl, tx_size, mode, dst, pd.*.dst.stride, dst, pd.*.dst.stride, col, row, plane);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(@as(c_int, mi.*.skip) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const tx_type: TX_TYPE = @bitCast(if ((plane != 0) or (xd.*.lossless != 0)) DCT_DCT else @as(c_int, @bitCast(antfly_vp9_intra_mode_to_tx_type_lookup[mode])));
        _ = &tx_type;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (pd.*.eob.* > @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            inverse_transform_block_intra(xd, plane, tx_type, tx_size, dst, pd.*.dst.stride, pd.*.eob.*);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pd.*.dqcoeff += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pd.*.eob += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn reconstruct_inter_block(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE, arg_mi_row: c_int, arg_mi_col: c_int) callconv(.c) c_int {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var sc: [*c]const ScanOrder = &antfly_vp9_vp9_default_scan_orders[tx_size];
    _ = &sc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const eob: c_int = antfly_vp9_vp9_decode_block_tokens(twd, plane, sc, col, row, tx_size, mi.*.segment_id);
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var dst: [*c]u8 = &pd.*.dst.buf[@bitCast(@as(isize, @intCast(((@as(c_int, 4) * row) * pd.*.dst.stride) + (@as(c_int, 4) * col))))];
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (eob > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        inverse_transform_block_inter(xd, plane, tx_size, dst, pd.*.dst.stride, eob);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &mi_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return eob;
}
pub fn parse_inter_block_row_mt(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE) callconv(.c) c_int {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var sc: [*c]const ScanOrder = &antfly_vp9_vp9_default_scan_orders[tx_size];
    _ = &sc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const eob: c_int = antfly_vp9_vp9_decode_block_tokens(twd, plane, sc, col, row, tx_size, mi.*.segment_id);
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pd.*.eob.* = eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pd.*.dqcoeff += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pd.*.eob += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return eob;
}
pub fn reconstruct_inter_block_row_mt(arg_twd: [*c]TileWorkerData, mi: [*c]MODE_INFO, arg_plane: c_int, arg_row: c_int, arg_col: c_int, arg_tx_size: TX_SIZE) callconv(.c) c_int {
    var twd = arg_twd;
    _ = &twd;
    _ = &mi;
    var plane = arg_plane;
    _ = &plane;
    var row = arg_row;
    _ = &row;
    var col = arg_col;
    _ = &col;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const eob: c_int = pd.*.eob.*;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (eob > @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        inverse_transform_block_inter(xd, plane, tx_size, &pd.*.dst.buf[@bitCast(@as(isize, @intCast(((@as(c_int, 4) * row) * pd.*.dst.stride) + (@as(c_int, 4) * col))))], pd.*.dst.stride, eob);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pd.*.dqcoeff += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16) << @intCast(@as(c_int, tx_size) << @intCast(@as(c_int, 1)))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pd.*.eob += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return eob;
}
pub fn build_mc_border(arg_src: [*c]const u8, arg_src_stride: c_int, arg_dst: [*c]u8, arg_dst_stride: c_int, arg_x: c_int, arg_y: c_int, arg_b_w: c_int, arg_b_h: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var b_w = arg_b_w;
    _ = &b_w;
    var b_h = arg_b_h;
    _ = &b_h;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref_row: [*c]const u8 = (src - @as(usize, @bitCast(@as(isize, @intCast(x))))) - @as(usize, @bitCast(@as(isize, @intCast(y * src_stride))));
    _ = &ref_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (y >= h) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_row += @as(usize, @bitCast(@as(isize, @intCast((h - @as(c_int, 1)) * src_stride))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (y > @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_row += @as(usize, @bitCast(@as(isize, @intCast(y * src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var right: c_int = 0;
        _ = &right;
        var copy: c_int = undefined;
        _ = &copy;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var left: c_int = if (x < @as(c_int, 0)) -x else @as(c_int, 0);
        _ = &left;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left > b_w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            left = b_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if ((x + b_w) > w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            right = (x + b_w) - w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (right > b_w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            right = b_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        copy = (b_w - left) - right;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), ref_row[@as(c_int, 0)], @bitCast(@as(c_long, left)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (copy != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(left)))))), @ptrCast(@alignCast((ref_row + @as(usize, @bitCast(@as(isize, @intCast(x))))) + @as(usize, @bitCast(@as(isize, @intCast(left)))))), @bitCast(@as(c_long, copy)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (right != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(left))))) + @as(usize, @bitCast(@as(isize, @intCast(copy)))))), ref_row[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))], @bitCast(@as(c_long, right)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((y > @as(c_int, 0)) and (y < h)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_row += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (!((blk: {
            const ref = &b_h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn high_build_mc_border(arg_src8: [*c]const u8, arg_src_stride: c_int, arg_dst: [*c]u16, arg_dst_stride: c_int, arg_x: c_int, arg_y: c_int, arg_b_w: c_int, arg_b_h: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src8 = arg_src8;
    _ = &src8;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var b_w = arg_b_w;
    _ = &b_w;
    var b_h = arg_b_h;
    _ = &b_h;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var src: [*c]const u16 = @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(src8))) << @intCast(@as(usize, 1))));
    _ = &src;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref_row: [*c]const u16 = (src - @as(usize, @bitCast(@as(isize, @intCast(x))))) - @as(usize, @bitCast(@as(isize, @intCast(y * src_stride))));
    _ = &ref_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (y >= h) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_row += @as(usize, @bitCast(@as(isize, @intCast((h - @as(c_int, 1)) * src_stride))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (y > @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_row += @as(usize, @bitCast(@as(isize, @intCast(y * src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var right: c_int = 0;
        _ = &right;
        var copy: c_int = undefined;
        _ = &copy;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var left: c_int = if (x < @as(c_int, 0)) -x else @as(c_int, 0);
        _ = &left;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left > b_w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            left = b_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if ((x + b_w) > w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            right = (x + b_w) - w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (right > b_w) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            right = b_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        copy = (b_w - left) - right;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), ref_row[@as(c_int, 0)], @bitCast(@as(c_long, left)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (copy != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(left)))))), @ptrCast(@alignCast((ref_row + @as(usize, @bitCast(@as(isize, @intCast(x))))) + @as(usize, @bitCast(@as(isize, @intCast(left)))))), @as(c_ulong, @bitCast(@as(c_long, copy))) *% @sizeOf(u16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (right != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(left))))) + @as(usize, @bitCast(@as(isize, @intCast(copy)))))), ref_row[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))], @bitCast(@as(c_long, right)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((y > @as(c_int, 0)) and (y < h)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ref_row += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (!((blk: {
            const ref = &b_h;
            ref.* -= 1;
            break :blk ref.*;
        }) != 0)) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn extend_and_predict(arg_twd: [*c]TileWorkerData, arg_buf_ptr1: [*c]const u8, arg_pre_buf_stride: c_int, arg_x0: c_int, arg_y0: c_int, arg_b_w: c_int, arg_b_h: c_int, arg_frame_width: c_int, arg_frame_height: c_int, arg_border_offset: c_int, dst: [*c]u8, arg_dst_buf_stride: c_int, arg_subpel_x: c_int, arg_subpel_y: c_int, arg_kernel: [*c]const InterpKernel, arg_sf: [*c]const struct_scale_factors, arg_xd: [*c]MACROBLOCKD, arg_w: c_int, arg_h: c_int, arg_ref: c_int, arg_xs: c_int, arg_ys: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    var buf_ptr1 = arg_buf_ptr1;
    _ = &buf_ptr1;
    var pre_buf_stride = arg_pre_buf_stride;
    _ = &pre_buf_stride;
    var x0 = arg_x0;
    _ = &x0;
    var y0 = arg_y0;
    _ = &y0;
    var b_w = arg_b_w;
    _ = &b_w;
    var b_h = arg_b_h;
    _ = &b_h;
    var frame_width = arg_frame_width;
    _ = &frame_width;
    var frame_height = arg_frame_height;
    _ = &frame_height;
    var border_offset = arg_border_offset;
    _ = &border_offset;
    _ = &dst;
    var dst_buf_stride = arg_dst_buf_stride;
    _ = &dst_buf_stride;
    var subpel_x = arg_subpel_x;
    _ = &subpel_x;
    var subpel_y = arg_subpel_y;
    _ = &subpel_y;
    var kernel = arg_kernel;
    _ = &kernel;
    var sf = arg_sf;
    _ = &sf;
    var xd = arg_xd;
    _ = &xd;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var ref = arg_ref;
    _ = &ref;
    var xs = arg_xs;
    _ = &xs;
    var ys = arg_ys;
    _ = &ys;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mc_buf_high: [*c]u16 = @ptrCast(@alignCast(&twd.*.extend_and_predict_buf));
    _ = &mc_buf_high;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((xd.*.cur_buf.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        high_build_mc_border(buf_ptr1, pre_buf_stride, mc_buf_high, b_w, x0, y0, b_w, b_h, frame_width, frame_height);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        highbd_inter_predictor(mc_buf_high + @as(usize, @bitCast(@as(isize, @intCast(border_offset)))), b_w, @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst))) << @intCast(@as(usize, 1)))), dst_buf_stride, subpel_x, subpel_y, sf, w, h, ref, kernel, xs, ys, xd.*.bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        build_mc_border(buf_ptr1, pre_buf_stride, @ptrCast(@alignCast(mc_buf_high)), b_w, x0, y0, b_w, b_h, frame_width, frame_height);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        inter_predictor(@as([*c]u8, @ptrCast(@alignCast(mc_buf_high))) + @as(usize, @bitCast(@as(isize, @intCast(border_offset)))), b_w, dst, dst_buf_stride, subpel_x, subpel_y, sf, w, h, ref, kernel, xs, ys);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dec_build_inter_predictors(arg_twd: [*c]TileWorkerData, arg_xd: [*c]MACROBLOCKD, arg_plane: c_int, arg_bw: c_int, arg_bh: c_int, arg_x: c_int, arg_y: c_int, arg_w: c_int, arg_h: c_int, arg_mi_x: c_int, arg_mi_y: c_int, arg_kernel: [*c]const InterpKernel, arg_sf: [*c]const struct_scale_factors, arg_pre_buf: [*c]struct_buf_2d, arg_dst_buf: [*c]struct_buf_2d, arg_mv_1: [*c]const MV, arg_ref_frame_buf: [*c]RefCntBuffer, arg_is_scaled: c_int, arg_ref: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    var xd = arg_xd;
    _ = &xd;
    var plane = arg_plane;
    _ = &plane;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var mi_x = arg_mi_x;
    _ = &mi_x;
    var mi_y = arg_mi_y;
    _ = &mi_y;
    var kernel = arg_kernel;
    _ = &kernel;
    var sf = arg_sf;
    _ = &sf;
    var pre_buf = arg_pre_buf;
    _ = &pre_buf;
    var dst_buf = arg_dst_buf;
    _ = &dst_buf;
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var ref_frame_buf = arg_ref_frame_buf;
    _ = &ref_frame_buf;
    var is_scaled = arg_is_scaled;
    _ = &is_scaled;
    var ref = arg_ref;
    _ = &ref;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst: [*c]u8 = (dst_buf.*.buf + @as(usize, @bitCast(@as(isize, @intCast(dst_buf.*.stride * y))))) + @as(usize, @bitCast(@as(isize, @intCast(x))));
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var scaled_mv: MV32 = undefined;
    _ = &scaled_mv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var xs: c_int = undefined;
    _ = &xs;
    var ys: c_int = undefined;
    _ = &ys;
    var x0: c_int = undefined;
    _ = &x0;
    var y0: c_int = undefined;
    _ = &y0;
    var x0_16: c_int = undefined;
    _ = &x0_16;
    var y0_16: c_int = undefined;
    _ = &y0_16;
    var frame_width: c_int = undefined;
    _ = &frame_width;
    var frame_height: c_int = undefined;
    _ = &frame_height;
    var buf_stride: c_int = undefined;
    _ = &buf_stride;
    var subpel_x: c_int = undefined;
    _ = &subpel_x;
    var subpel_y: c_int = undefined;
    _ = &subpel_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref_frame: [*c]u8 = undefined;
    _ = &ref_frame;
    var buf_ptr: [*c]u8 = undefined;
    _ = &buf_ptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (plane == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = ref_frame_buf.*.buf.y_crop_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = ref_frame_buf.*.buf.y_crop_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_frame = ref_frame_buf.*.buf.y_buffer;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = ref_frame_buf.*.buf.uv_crop_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = ref_frame_buf.*.buf.uv_crop_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ref_frame = if (plane == @as(c_int, 1)) ref_frame_buf.*.buf.u_buffer else ref_frame_buf.*.buf.v_buffer;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (is_scaled != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const mv_q4: MV = antfly_vp9_clamp_mv_to_umv_border_sb(xd, mv_1, bw, bh, pd.*.subsampling_x, pd.*.subsampling_y);
        _ = &mv_q4;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var x_start: c_int = -xd.*.mb_to_left_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_x);
        _ = &x_start;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var y_start: c_int = -xd.*.mb_to_top_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_y);
        _ = &y_start;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        x0_16 = (x_start + x) << @intCast(@as(c_int, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y0_16 = (y_start + y) << @intCast(@as(c_int, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        x0_16 = sf.*.scale_value_x.?(x0_16, sf);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y0_16 = sf.*.scale_value_y.?(y0_16, sf);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        x0 = sf.*.scale_value_x.?(x_start + x, sf);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y0 = sf.*.scale_value_y.?(y_start + y, sf);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        scaled_mv = antfly_vp9_vp9_scale_mv(&mv_q4, mi_x + x, mi_y + y, sf);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xs = sf.*.x_step_q4;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        ys = sf.*.y_step_q4;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        x0 = (-xd.*.mb_to_left_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_x)) + x;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y0 = (-xd.*.mb_to_top_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_y)) + y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        x0_16 = x0 << @intCast(@as(c_int, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        y0_16 = y0 << @intCast(@as(c_int, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        scaled_mv.row = @as(c_int, mv_1.*.row) * (@as(c_int, 1) << @intCast(@as(c_int, 1) - pd.*.subsampling_y));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        scaled_mv.col = @as(c_int, mv_1.*.col) * (@as(c_int, 1) << @intCast(@as(c_int, 1) - pd.*.subsampling_x));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xs = blk: {
            const tmp = @as(c_int, 16);
            ys = tmp;
            break :blk tmp;
        };
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    subpel_x = scaled_mv.col & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    subpel_y = scaled_mv.row & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 += scaled_mv.col >> @intCast(@as(c_int, 4));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    y0 += scaled_mv.row >> @intCast(@as(c_int, 4));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0_16 += scaled_mv.col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    y0_16 += scaled_mv.row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    buf_ptr = (ref_frame + @as(usize, @bitCast(@as(isize, @intCast(y0 * pre_buf.*.stride))))) + @as(usize, @bitCast(@as(isize, @intCast(x0))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    buf_stride = pre_buf.*.stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((((is_scaled != 0) or (scaled_mv.col != 0)) or (scaled_mv.row != 0)) or ((frame_width & @as(c_int, 7)) != 0)) or ((frame_height & @as(c_int, 7)) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var y1: c_int = ((y0_16 + ((h - @as(c_int, 1)) * ys)) >> @intCast(@as(c_int, 4))) + @as(c_int, 1);
        _ = &y1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var x1: c_int = ((x0_16 + ((w - @as(c_int, 1)) * xs)) >> @intCast(@as(c_int, 4))) + @as(c_int, 1);
        _ = &x1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var x_pad: c_int = 0;
        _ = &x_pad;
        var y_pad: c_int = 0;
        _ = &y_pad;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((subpel_x != 0) or (sf.*.x_step_q4 != (@as(c_int, 1) << @intCast(@as(c_int, 4))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            x0 -= @as(c_int, 4) - @as(c_int, 1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            x1 += 4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            x_pad = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((subpel_y != 0) or (sf.*.y_step_q4 != (@as(c_int, 1) << @intCast(@as(c_int, 4))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            y0 -= @as(c_int, 4) - @as(c_int, 1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            y1 += 4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            y_pad = 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((((((((x0 < @as(c_int, 0)) or (x0 > (frame_width - @as(c_int, 1)))) or (x1 < @as(c_int, 0))) or (x1 > (frame_width - @as(c_int, 1)))) or (y0 < @as(c_int, 0))) or (y0 > (frame_height - @as(c_int, 1)))) or (y1 < @as(c_int, 0))) or (y1 > (frame_height - @as(c_int, 1)))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const buf_ptr1: [*c]const u8 = (ref_frame + @as(usize, @bitCast(@as(isize, @intCast(y0 * buf_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(x0))));
            _ = &buf_ptr1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const b_w: c_int = (x1 - x0) + @as(c_int, 1);
            _ = &b_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const b_h: c_int = (y1 - y0) + @as(c_int, 1);
            _ = &b_h;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const border_offset: c_int = ((y_pad * @as(c_int, 3)) * b_w) + (x_pad * @as(c_int, 3));
            _ = &border_offset;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            extend_and_predict(twd, buf_ptr1, buf_stride, x0, y0, b_w, b_h, frame_width, frame_height, border_offset, dst, dst_buf.*.stride, subpel_x, subpel_y, kernel, sf, xd, w, h, ref, xs, ys);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((xd.*.cur_buf.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        highbd_inter_predictor(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(buf_ptr))) << @intCast(@as(usize, 1)))), buf_stride, @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst))) << @intCast(@as(usize, 1)))), dst_buf.*.stride, subpel_x, subpel_y, sf, w, h, ref, kernel, xs, ys, xd.*.bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        inter_predictor(buf_ptr, buf_stride, dst, dst_buf.*.stride, subpel_x, subpel_y, sf, w, h, ref, kernel, xs, ys);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dec_build_inter_predictors_sb(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var xd = arg_xd;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var plane: c_int = undefined;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi_x: c_int = mi_col * (@as(c_int, 1) << @intCast(@as(c_int, 3)));
    _ = &mi_x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const mi_y: c_int = mi_row * (@as(c_int, 1) << @intCast(@as(c_int, 3)));
    _ = &mi_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi: [*c]const MODE_INFO = xd.*.mi[@as(c_int, 0)];
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var kernel: [*c]const InterpKernel = antfly_vp9_vp9_filter_kernels[mi.*.interp_filter];
    _ = &kernel;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_type: BLOCK_SIZE = mi.*.sb_type;
    _ = &sb_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const is_compound: c_int = has_second_ref(mi);
    _ = &is_compound;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref: c_int = undefined;
    _ = &ref;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var is_scaled: c_int = undefined;
    _ = &is_scaled;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        ref = 0;
        while (ref < (@as(c_int, 1) + is_compound)) : (ref += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const frame: MV_REFERENCE_FRAME = mi.*.ref_frame[@bitCast(@as(isize, @intCast(ref)))];
            _ = &frame;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var ref_buf: [*c]RefBuffer = &pbi.*.common.frame_refs[@bitCast(@as(isize, @intCast(@as(c_int, frame) - @as(c_int, 1))))];
            _ = &ref_buf;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const sf: [*c]const struct_scale_factors = &ref_buf.*.sf;
            _ = &sf;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const idx: c_int = ref_buf.*.idx;
            _ = &idx;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const pool: [*c]BufferPool = pbi.*.common.buffer_pool;
            _ = &pool;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const ref_frame_buf: [*c]RefCntBuffer = &pool.*.frame_bufs[@bitCast(@as(isize, @intCast(idx)))];
            _ = &ref_frame_buf;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(vp9_is_valid_scale(sf) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(xd.*.error_info)), VPX_CODEC_UNSUP_BITSTREAM);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            is_scaled = vp9_is_scaled(sf);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_setup_pre_planes(xd, ref, ref_buf.*.buf, mi_row, mi_col, @ptrCast(@alignCast(if (is_scaled != 0) @as(?*const anyopaque, @ptrCast(@alignCast(sf))) else @as(?*const anyopaque, @ptrCast(@alignCast(@as(?*anyopaque, null)))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.block_refs[@bitCast(@as(isize, @intCast(ref)))] = ref_buf;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (@as(c_int, sb_type) < @as(c_int, 3)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    plane = 0;
                    while (plane < @as(c_int, 3)) : (plane += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                        _ = &pd;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const dst_buf: [*c]struct_buf_2d = &pd.*.dst;
                        _ = &dst_buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const num_4x4_w: c_int = pd.*.n4_w;
                        _ = &num_4x4_w;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const num_4x4_h: c_int = pd.*.n4_h;
                        _ = &num_4x4_h;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const n4w_x4: c_int = @as(c_int, 4) * num_4x4_w;
                        _ = &n4w_x4;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const n4h_x4: c_int = @as(c_int, 4) * num_4x4_h;
                        _ = &n4h_x4;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const pre_buf: [*c]struct_buf_2d = &pd.*.pre[@bitCast(@as(isize, @intCast(ref)))];
                        _ = &pre_buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var i: c_int = 0;
                        _ = &i;
                        var x: c_int = undefined;
                        _ = &x;
                        var y: c_int = undefined;
                        _ = &y;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            y = 0;
                            while (y < num_4x4_h) : (y += 1) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                {
                                    x = 0;
                                    while (x < num_4x4_w) : (x += 1) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        const mv_1: MV = antfly_vp9_average_split_mvs(pd, mi, ref, blk: {
                                            const ref_1 = &i;
                                            const tmp = ref_1.*;
                                            ref_1.* += 1;
                                            break :blk tmp;
                                        });
                                        _ = &mv_1;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        dec_build_inter_predictors(twd, xd, plane, n4w_x4, n4h_x4, @as(c_int, 4) * x, @as(c_int, 4) * y, 4, 4, mi_x, mi_y, kernel, sf, pre_buf, dst_buf, &mv_1, ref_frame_buf, is_scaled, ref);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                    }
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const mv_1: MV = mi.*.mv[@bitCast(@as(isize, @intCast(ref)))].as_mv;
                _ = &mv_1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    plane = 0;
                    while (plane < @as(c_int, 3)) : (plane += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                        _ = &pd;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const dst_buf: [*c]struct_buf_2d = &pd.*.dst;
                        _ = &dst_buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const num_4x4_w: c_int = pd.*.n4_w;
                        _ = &num_4x4_w;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const num_4x4_h: c_int = pd.*.n4_h;
                        _ = &num_4x4_h;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const n4w_x4: c_int = @as(c_int, 4) * num_4x4_w;
                        _ = &n4w_x4;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const n4h_x4: c_int = @as(c_int, 4) * num_4x4_h;
                        _ = &n4h_x4;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const pre_buf: [*c]struct_buf_2d = &pd.*.pre[@bitCast(@as(isize, @intCast(ref)))];
                        _ = &pre_buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        dec_build_inter_predictors(twd, xd, plane, n4w_x4, n4h_x4, 0, 0, n4w_x4, n4h_x4, mi_x, mi_y, kernel, sf, pre_buf, dst_buf, &mv_1, ref_frame_buf, is_scaled, ref);
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
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dec_reset_skip_context(arg_xd: [*c]MACROBLOCKD) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
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
            const pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(i)))];
            _ = &pd;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(pd.*.above_context)), 0, @sizeOf(ENTROPY_CONTEXT) *% @as(c_ulong, @bitCast(@as(c_long, @as(c_int, pd.*.n4_w)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(pd.*.left_context)), 0, @sizeOf(ENTROPY_CONTEXT) *% @as(c_ulong, @bitCast(@as(c_long, @as(c_int, pd.*.n4_h)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn set_plane_n4(xd: [*c]MACROBLOCKD, arg_bw: c_int, arg_bh: c_int, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) void {
    _ = &xd;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
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
            xd.*.plane[@bitCast(@as(isize, @intCast(i)))].n4_w = @bitCast(@as(c_short, @truncate((bw << @intCast(@as(c_int, 1))) >> @intCast(xd.*.plane[@bitCast(@as(isize, @intCast(i)))].subsampling_x))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.plane[@bitCast(@as(isize, @intCast(i)))].n4_h = @bitCast(@as(c_short, @truncate((bh << @intCast(@as(c_int, 1))) >> @intCast(xd.*.plane[@bitCast(@as(isize, @intCast(i)))].subsampling_y))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.plane[@bitCast(@as(isize, @intCast(i)))].n4_wl = @bitCast(@as(i8, @truncate(bwl - xd.*.plane[@bitCast(@as(isize, @intCast(i)))].subsampling_x)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.plane[@bitCast(@as(isize, @intCast(i)))].n4_hl = @bitCast(@as(i8, @truncate(bhl - xd.*.plane[@bitCast(@as(isize, @intCast(i)))].subsampling_y)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn set_offsets_recon(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_mi_row: c_int, arg_mi_col: c_int, arg_bw: c_int, arg_bh: c_int, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) [*c]MODE_INFO {
    _ = &cm;
    _ = &xd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const offset: c_int = (mi_row * cm.*.mi_stride) + mi_col;
    _ = &offset;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile: [*c]const TileInfo = &xd.*.tile;
    _ = &tile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    xd.*.mi = cm.*.mi_grid_visible + @as(usize, @bitCast(@as(isize, @intCast(offset))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    set_plane_n4(xd, bw, bh, bwl, bhl);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    set_skip_context(xd, mi_row, mi_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    set_mi_row_col(xd, tile, mi_row, bh, mi_col, bw, cm.*.mi_rows, cm.*.mi_cols);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vp9_setup_dst_planes(@ptrCast(@alignCast(&xd.*.plane)), get_frame_new_buffer(cm), mi_row, mi_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return xd.*.mi[@as(c_int, 0)];
}
pub fn set_offsets(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_bsize: BLOCK_SIZE, arg_mi_row: c_int, arg_mi_col: c_int, arg_bw: c_int, arg_bh: c_int, arg_x_mis: c_int, arg_y_mis: c_int, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) [*c]MODE_INFO {
    _ = &cm;
    _ = &xd;
    var bsize = arg_bsize;
    _ = &bsize;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bw = arg_bw;
    _ = &bw;
    var bh = arg_bh;
    _ = &bh;
    var x_mis = arg_x_mis;
    _ = &x_mis;
    var y_mis = arg_y_mis;
    _ = &y_mis;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const offset: c_int = (mi_row * cm.*.mi_stride) + mi_col;
    _ = &offset;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile: [*c]const TileInfo = &xd.*.tile;
    _ = &tile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    xd.*.mi = cm.*.mi_grid_visible + @as(usize, @bitCast(@as(isize, @intCast(offset))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    xd.*.mi[@as(c_int, 0)] = &cm.*.mi[@bitCast(@as(isize, @intCast(offset)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    xd.*.mi[@as(c_int, 0)].*.sb_type = bsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        y = 0;
        while (y < y_mis) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            {
                x = @as(c_int, @intFromBool(!(y != 0)));
                while (x < x_mis) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    xd.*.mi[@bitCast(@as(isize, @intCast((y * cm.*.mi_stride) + x)))] = xd.*.mi[@as(c_int, 0)];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    set_plane_n4(xd, bw, bh, bwl, bhl);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    set_skip_context(xd, mi_row, mi_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    set_mi_row_col(xd, tile, mi_row, bh, mi_col, bw, cm.*.mi_rows, cm.*.mi_cols);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vp9_setup_dst_planes(@ptrCast(@alignCast(&xd.*.plane)), get_frame_new_buffer(cm), mi_row, mi_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return xd.*.mi[@as(c_int, 0)];
}
pub fn predict_recon_inter(arg_xd: [*c]MACROBLOCKD, arg_mi: [*c]MODE_INFO, arg_twd: [*c]TileWorkerData, arg_func: predict_recon_func) callconv(.c) c_int {
    var xd = arg_xd;
    _ = &xd;
    var mi = arg_mi;
    _ = &mi;
    var twd = arg_twd;
    _ = &twd;
    var func = arg_func;
    _ = &func;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var eobtotal: c_int = 0;
    _ = &eobtotal;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var plane: c_int = undefined;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        plane = 0;
        while (plane < @as(c_int, 3)) : (plane += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
            _ = &pd;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const tx_size: TX_SIZE = @bitCast(@as(i8, @truncate(if (plane != 0) @as(c_int, get_uv_tx_size(mi, pd)) else @as(c_int, mi.*.tx_size))));
            _ = &tx_size;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const num_4x4_w: c_int = pd.*.n4_w;
            _ = &num_4x4_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const num_4x4_h: c_int = pd.*.n4_h;
            _ = &num_4x4_h;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const step: c_int = @as(c_int, 1) << @intCast(@as(c_int, tx_size));
            _ = &step;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            var row: c_int = undefined;
            _ = &row;
            var col: c_int = undefined;
            _ = &col;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const max_blocks_wide: c_int = num_4x4_w + (if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_right_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_x));
            _ = &max_blocks_wide;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const max_blocks_high: c_int = num_4x4_h + (if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_bottom_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_y));
            _ = &max_blocks_high;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            xd.*.max_blocks_wide = @bitCast(@as(c_int, if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_wide));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            xd.*.max_blocks_high = @bitCast(@as(c_int, if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_high));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            {
                row = 0;
                while (row < max_blocks_high) : (row += step) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    {
                        col = 0;
                        while (col < max_blocks_wide) : (col += step) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            eobtotal += func.?(twd, mi, plane, row, col, tx_size);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return eobtotal;
}
pub fn predict_recon_intra(arg_xd: [*c]MACROBLOCKD, arg_mi: [*c]MODE_INFO, arg_twd: [*c]TileWorkerData, arg_func: intra_recon_func) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var mi = arg_mi;
    _ = &mi;
    var twd = arg_twd;
    _ = &twd;
    var func = arg_func;
    _ = &func;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var plane: c_int = undefined;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        plane = 0;
        while (plane < @as(c_int, 3)) : (plane += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
            _ = &pd;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const tx_size: TX_SIZE = @bitCast(@as(i8, @truncate(if (plane != 0) @as(c_int, get_uv_tx_size(mi, pd)) else @as(c_int, mi.*.tx_size))));
            _ = &tx_size;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const num_4x4_w: c_int = pd.*.n4_w;
            _ = &num_4x4_w;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const num_4x4_h: c_int = pd.*.n4_h;
            _ = &num_4x4_h;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const step: c_int = @as(c_int, 1) << @intCast(@as(c_int, tx_size));
            _ = &step;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var row: c_int = undefined;
            _ = &row;
            var col: c_int = undefined;
            _ = &col;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const max_blocks_wide: c_int = num_4x4_w + (if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_right_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_x));
            _ = &max_blocks_wide;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const max_blocks_high: c_int = num_4x4_h + (if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_bottom_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_y));
            _ = &max_blocks_high;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.max_blocks_wide = @bitCast(@as(c_int, if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_wide));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            xd.*.max_blocks_high = @bitCast(@as(c_int, if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_high));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                row = 0;
                while (row < max_blocks_high) : (row += step) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        col = 0;
                        while (col < max_blocks_wide) : (col += step) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            func.?(twd, mi, plane, row, col, tx_size);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn decode_block(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsize: BLOCK_SIZE, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsize = arg_bsize;
    _ = &bsize;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const less8x8: c_int = @as(c_int, @intFromBool(@as(c_int, bsize) < @as(c_int, 3)));
    _ = &less8x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bw: c_int = @as(c_int, 1) << @intCast(bwl - @as(c_int, 1));
    _ = &bw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bh: c_int = @as(c_int, 1) << @intCast(bhl - @as(c_int, 1));
    _ = &bh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const x_mis: c_int = if (bw < (cm.*.mi_cols - mi_col)) bw else cm.*.mi_cols - mi_col;
    _ = &x_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const y_mis: c_int = if (bh < (cm.*.mi_rows - mi_row)) bh else cm.*.mi_rows - mi_row;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: [*c]vpx_reader = &twd.*.bit_reader;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi: [*c]MODE_INFO = set_offsets(cm, xd, bsize, mi_row, mi_col, bw, bh, x_mis, y_mis, bwl, bhl);
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, bsize) >= @as(c_int, 3)) and ((cm.*.subsampling_x != 0) or (cm.*.subsampling_y != 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const uv_subsize: BLOCK_SIZE = antfly_vp9_ss_size_lookup[bsize][@bitCast(@as(isize, @intCast(cm.*.subsampling_x)))][@bitCast(@as(isize, @intCast(cm.*.subsampling_y)))];
        _ = &uv_subsize;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, uv_subsize) == @as(c_int, 13)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(xd.*.error_info)), VPX_CODEC_CORRUPT_FRAME);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_read_mode_info(twd, pbi, mi_row, mi_col, x_mis, y_mis);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, mi.*.skip) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dec_reset_skip_context(xd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(is_inter_block(mi) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var plane: c_int = undefined;
        _ = &plane;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            plane = 0;
            while (plane < @as(c_int, 3)) : (plane += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                _ = &pd;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const tx_size: TX_SIZE = @bitCast(@as(i8, @truncate(if (plane != 0) @as(c_int, get_uv_tx_size(mi, pd)) else @as(c_int, mi.*.tx_size))));
                _ = &tx_size;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const num_4x4_w: c_int = pd.*.n4_w;
                _ = &num_4x4_w;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const num_4x4_h: c_int = pd.*.n4_h;
                _ = &num_4x4_h;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const step: c_int = @as(c_int, 1) << @intCast(@as(c_int, tx_size));
                _ = &step;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var row: c_int = undefined;
                _ = &row;
                var col: c_int = undefined;
                _ = &col;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const max_blocks_wide: c_int = num_4x4_w + (if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_right_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_x));
                _ = &max_blocks_wide;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const max_blocks_high: c_int = num_4x4_h + (if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_bottom_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_y));
                _ = &max_blocks_high;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                xd.*.max_blocks_wide = @bitCast(@as(c_int, if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_wide));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                xd.*.max_blocks_high = @bitCast(@as(c_int, if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_high));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    row = 0;
                    while (row < max_blocks_high) : (row += step) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            col = 0;
                            while (col < max_blocks_wide) : (col += step) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                predict_and_reconstruct_intra_block(twd, mi, plane, row, col, tx_size);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dec_build_inter_predictors_sb(twd, pbi, xd, mi_row, mi_col);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(@as(c_int, mi.*.skip) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var eobtotal: c_int = 0;
            _ = &eobtotal;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var plane: c_int = undefined;
            _ = &plane;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                plane = 0;
                while (plane < @as(c_int, 3)) : (plane += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                    _ = &pd;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const tx_size: TX_SIZE = @bitCast(@as(i8, @truncate(if (plane != 0) @as(c_int, get_uv_tx_size(mi, pd)) else @as(c_int, mi.*.tx_size))));
                    _ = &tx_size;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const num_4x4_w: c_int = pd.*.n4_w;
                    _ = &num_4x4_w;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const num_4x4_h: c_int = pd.*.n4_h;
                    _ = &num_4x4_h;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const step: c_int = @as(c_int, 1) << @intCast(@as(c_int, tx_size));
                    _ = &step;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var row: c_int = undefined;
                    _ = &row;
                    var col: c_int = undefined;
                    _ = &col;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const max_blocks_wide: c_int = num_4x4_w + (if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_right_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_x));
                    _ = &max_blocks_wide;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const max_blocks_high: c_int = num_4x4_h + (if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else xd.*.mb_to_bottom_edge >> @intCast(@as(c_int, 5) + pd.*.subsampling_y));
                    _ = &max_blocks_high;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    xd.*.max_blocks_wide = @bitCast(@as(c_int, if (xd.*.mb_to_right_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_wide));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    xd.*.max_blocks_high = @bitCast(@as(c_int, if (xd.*.mb_to_bottom_edge >= @as(c_int, 0)) @as(c_int, 0) else max_blocks_high));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        row = 0;
                        while (row < max_blocks_high) : (row += step) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            {
                                col = 0;
                                while (col < max_blocks_wide) : (col += step) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    eobtotal += reconstruct_inter_block(twd, mi, plane, row, col, tx_size, mi_row, mi_col);
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return;
                                    }
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(less8x8 != 0) and (eobtotal == @as(c_int, 0))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.skip = 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
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
    xd.*.corrupted |= vpx_reader_has_error(r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.lf.filter_level != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_build_mask(cm, mi, mi_row, mi_col, bw, bh);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn recon_block(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsize: BLOCK_SIZE, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsize = arg_bsize;
    _ = &bsize;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bw: c_int = @as(c_int, 1) << @intCast(bwl - @as(c_int, 1));
    _ = &bw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bh: c_int = @as(c_int, 1) << @intCast(bhl - @as(c_int, 1));
    _ = &bh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi: [*c]MODE_INFO = set_offsets_recon(cm, xd, mi_row, mi_col, bw, bh, bwl, bhl);
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, bsize) >= @as(c_int, 3)) and ((cm.*.subsampling_x != 0) or (cm.*.subsampling_y != 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const uv_subsize: BLOCK_SIZE = antfly_vp9_ss_size_lookup[bsize][@bitCast(@as(isize, @intCast(cm.*.subsampling_x)))][@bitCast(@as(isize, @intCast(cm.*.subsampling_y)))];
        _ = &uv_subsize;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, uv_subsize) == @as(c_int, 13)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(xd.*.error_info)), VPX_CODEC_CORRUPT_FRAME);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(is_inter_block(mi) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        predict_recon_intra(xd, mi, twd, predict_and_reconstruct_intra_block_row_mt);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dec_build_inter_predictors_sb(twd, pbi, xd, mi_row, mi_col);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(@as(c_int, mi.*.skip) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = predict_recon_inter(xd, mi, twd, reconstruct_inter_block_row_mt);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_build_mask(cm, mi, mi_row, mi_col, bw, bh);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn parse_block(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsize: BLOCK_SIZE, arg_bwl: c_int, arg_bhl: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsize = arg_bsize;
    _ = &bsize;
    var bwl = arg_bwl;
    _ = &bwl;
    var bhl = arg_bhl;
    _ = &bhl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bw: c_int = @as(c_int, 1) << @intCast(bwl - @as(c_int, 1));
    _ = &bw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bh: c_int = @as(c_int, 1) << @intCast(bhl - @as(c_int, 1));
    _ = &bh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const x_mis: c_int = if (bw < (cm.*.mi_cols - mi_col)) bw else cm.*.mi_cols - mi_col;
    _ = &x_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const y_mis: c_int = if (bh < (cm.*.mi_rows - mi_row)) bh else cm.*.mi_rows - mi_row;
    _ = &y_mis;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: [*c]vpx_reader = &twd.*.bit_reader;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi: [*c]MODE_INFO = set_offsets(cm, xd, bsize, mi_row, mi_col, bw, bh, x_mis, y_mis, bwl, bhl);
    _ = &mi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, bsize) >= @as(c_int, 3)) and ((cm.*.subsampling_x != 0) or (cm.*.subsampling_y != 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const uv_subsize: BLOCK_SIZE = antfly_vp9_ss_size_lookup[bsize][@bitCast(@as(isize, @intCast(cm.*.subsampling_x)))][@bitCast(@as(isize, @intCast(cm.*.subsampling_y)))];
        _ = &uv_subsize;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, uv_subsize) == @as(c_int, 13)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(xd.*.error_info)), VPX_CODEC_CORRUPT_FRAME);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_read_mode_info(twd, pbi, mi_row, mi_col, x_mis, y_mis);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, mi.*.skip) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dec_reset_skip_context(xd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(is_inter_block(mi) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        predict_recon_intra(xd, mi, twd, parse_intra_block_row_mt);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(@as(c_int, mi.*.skip) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var dqcoeff: [3][*c]tran_low_t = undefined;
            _ = &dqcoeff;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var eob: [3][*c]c_int = undefined;
            _ = &eob;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var plane: c_int = undefined;
            _ = &plane;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var eobtotal: c_int = undefined;
            _ = &eobtotal;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                plane = 0;
                while (plane < @as(c_int, 3)) : (plane += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                    _ = &pd;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dqcoeff[@bitCast(@as(isize, @intCast(plane)))] = pd.*.dqcoeff;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    eob[@bitCast(@as(isize, @intCast(plane)))] = pd.*.eob;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            eobtotal = predict_recon_inter(xd, mi, twd, parse_inter_block_row_mt);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((@as(c_int, bsize) >= @as(c_int, 3)) and (eobtotal == @as(c_int, 0))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                mi.*.skip = 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    plane = 0;
                    while (plane < @as(c_int, 3)) : (plane += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var pd: [*c]struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
                        _ = &pd;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        pd.*.dqcoeff = dqcoeff[@bitCast(@as(isize, @intCast(plane)))];
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        pd.*.eob = eob[@bitCast(@as(isize, @intCast(plane)))];
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
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.corrupted |= vpx_reader_has_error(r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dec_partition_plane_context(arg_twd: [*c]TileWorkerData, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsl: c_int) callconv(.c) c_int {
    var twd = arg_twd;
    _ = &twd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsl = arg_bsl;
    _ = &bsl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var above_ctx: [*c]const PARTITION_CONTEXT = twd.*.xd.above_seg_context + @as(usize, @bitCast(@as(isize, @intCast(mi_col))));
    _ = &above_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var left_ctx: [*c]const PARTITION_CONTEXT = @as([*c]PARTITION_CONTEXT, @ptrCast(@alignCast(&twd.*.xd.left_seg_context))) + @as(usize, @bitCast(@as(isize, @intCast(mi_row & ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))))));
    _ = &left_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var above: c_int = (@as(c_int, above_ctx.*) >> @intCast(bsl)) & @as(c_int, 1);
    _ = &above;
    var left: c_int = (@as(c_int, left_ctx.*) >> @intCast(bsl)) & @as(c_int, 1);
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ((left * @as(c_int, 2)) + above) + (bsl * @as(c_int, 4));
}
pub fn dec_update_partition_context(arg_twd: [*c]TileWorkerData, arg_mi_row: c_int, arg_mi_col: c_int, arg_subsize: BLOCK_SIZE, arg_bw: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var subsize = arg_subsize;
    _ = &subsize;
    var bw = arg_bw;
    _ = &bw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_ctx: [*c]PARTITION_CONTEXT = twd.*.xd.above_seg_context + @as(usize, @bitCast(@as(isize, @intCast(mi_col))));
    _ = &above_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const left_ctx: [*c]PARTITION_CONTEXT = @as([*c]PARTITION_CONTEXT, @ptrCast(@alignCast(&twd.*.xd.left_seg_context))) + @as(usize, @bitCast(@as(isize, @intCast(mi_row & ((@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) - @as(c_int, 1))))));
    _ = &left_ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_ctx)), antfly_vp9_partition_context_lookup[subsize].above, @bitCast(@as(c_long, bw)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(left_ctx)), antfly_vp9_partition_context_lookup[subsize].left, @bitCast(@as(c_long, bw)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_partition(arg_twd: [*c]TileWorkerData, arg_mi_row: c_int, arg_mi_col: c_int, arg_has_rows: c_int, arg_has_cols: c_int, arg_bsl: c_int) callconv(.c) PARTITION_TYPE {
    var twd = arg_twd;
    _ = &twd;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var has_rows = arg_has_rows;
    _ = &has_rows;
    var has_cols = arg_has_cols;
    _ = &has_cols;
    var bsl = arg_bsl;
    _ = &bsl;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const ctx: c_int = dec_partition_plane_context(twd, mi_row, mi_col, bsl);
    _ = &ctx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const probs: [*c]const vpx_prob = @ptrCast(@alignCast(&twd.*.xd.partition_probs[@bitCast(@as(isize, @intCast(ctx)))]));
    _ = &probs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var counts: [*c]FRAME_COUNTS = twd.*.xd.counts;
    _ = &counts;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var p: PARTITION_TYPE = undefined;
    _ = &p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var r: [*c]vpx_reader = &twd.*.bit_reader;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((has_rows != 0) and (has_cols != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        p = @bitCast(vpx_read_tree(r, @ptrCast(@alignCast(&antfly_vp9_vp9_partition_tree)), probs));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(has_rows != 0) and (has_cols != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            p = @bitCast(if (vpx_read(r, probs[@as(c_int, 1)]) != 0) PARTITION_SPLIT else PARTITION_HORZ);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((has_rows != 0) and !(has_cols != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                p = @bitCast(if (vpx_read(r, probs[@as(c_int, 2)]) != 0) PARTITION_SPLIT else PARTITION_VERT);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                p = PARTITION_SPLIT;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (counts != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        counts.*.partition[@bitCast(@as(isize, @intCast(ctx)))][p] +%= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return p;
}
pub fn decode_partition(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsize: BLOCK_SIZE, arg_n4x4_l2: c_int) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsize = arg_bsize;
    _ = &bsize;
    var n4x4_l2 = arg_n4x4_l2;
    _ = &n4x4_l2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const n8x8_l2: c_int = n4x4_l2 - @as(c_int, 1);
    _ = &n8x8_l2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_8x8_wh: c_int = @as(c_int, 1) << @intCast(n8x8_l2);
    _ = &num_8x8_wh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const hbs: c_int = num_8x8_wh >> @intCast(@as(c_int, 1));
    _ = &hbs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var partition: PARTITION_TYPE = undefined;
    _ = &partition;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var subsize: BLOCK_SIZE = undefined;
    _ = &subsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const has_rows: c_int = @as(c_int, @intFromBool((mi_row + hbs) < cm.*.mi_rows));
    _ = &has_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const has_cols: c_int = @as(c_int, @intFromBool((mi_col + hbs) < cm.*.mi_cols));
    _ = &has_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((mi_row >= cm.*.mi_rows) or (mi_col >= cm.*.mi_cols)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    partition = read_partition(twd, mi_row, mi_col, has_rows, has_cols, n8x8_l2);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    subsize = antfly_vp9_subsize_lookup[partition][bsize];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(hbs != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xd.*.bmode_blocks_wl = @bitCast(@as(i8, @truncate(@as(c_int, 1) >> @intCast(@as(c_int, @intFromBool(!!((partition & @as(PARTITION_TYPE, PARTITION_VERT)) != 0)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xd.*.bmode_blocks_hl = @bitCast(@as(i8, @truncate(@as(c_int, 1) >> @intCast(@as(c_int, @intFromBool(!!((partition & @as(PARTITION_TYPE, PARTITION_HORZ)) != 0)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        decode_block(twd, pbi, mi_row, mi_col, subsize, 1, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        blk: {
            switch (partition) {
                @as(PARTITION_TYPE, PARTITION_NONE) => {
                    decode_block(twd, pbi, mi_row, mi_col, subsize, n4x4_l2, n4x4_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_HORZ) => {
                    decode_block(twd, pbi, mi_row, mi_col, subsize, n4x4_l2, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (has_rows != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        decode_block(twd, pbi, mi_row + hbs, mi_col, subsize, n4x4_l2, n8x8_l2);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_VERT) => {
                    decode_block(twd, pbi, mi_row, mi_col, subsize, n8x8_l2, n4x4_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (has_cols != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        decode_block(twd, pbi, mi_row, mi_col + hbs, subsize, n8x8_l2, n4x4_l2);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_SPLIT) => {
                    decode_partition(twd, pbi, mi_row, mi_col, subsize, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    decode_partition(twd, pbi, mi_row, mi_col + hbs, subsize, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    decode_partition(twd, pbi, mi_row + hbs, mi_col, subsize, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    decode_partition(twd, pbi, mi_row + hbs, mi_col + hbs, subsize, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    break :blk;
                },
                else => {
                    _ = @as(c_int, 0);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                },
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, bsize) >= @as(c_int, 3)) and ((@as(c_int, bsize) == @as(c_int, 3)) or (partition != @as(PARTITION_TYPE, PARTITION_SPLIT)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dec_update_partition_context(twd, mi_row, mi_col, subsize, num_8x8_wh);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn process_partition(arg_twd: [*c]TileWorkerData, pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_mi_col: c_int, arg_bsize: BLOCK_SIZE, arg_n4x4_l2: c_int, arg_parse_recon_flag: c_int, arg_process_block: process_block_fn_t) callconv(.c) void {
    var twd = arg_twd;
    _ = &twd;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var mi_col = arg_mi_col;
    _ = &mi_col;
    var bsize = arg_bsize;
    _ = &bsize;
    var n4x4_l2 = arg_n4x4_l2;
    _ = &n4x4_l2;
    var parse_recon_flag = arg_parse_recon_flag;
    _ = &parse_recon_flag;
    var process_block = arg_process_block;
    _ = &process_block;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const n8x8_l2: c_int = n4x4_l2 - @as(c_int, 1);
    _ = &n8x8_l2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const num_8x8_wh: c_int = @as(c_int, 1) << @intCast(n8x8_l2);
    _ = &num_8x8_wh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const hbs: c_int = num_8x8_wh >> @intCast(@as(c_int, 1));
    _ = &hbs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var partition: PARTITION_TYPE = undefined;
    _ = &partition;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var subsize: BLOCK_SIZE = undefined;
    _ = &subsize;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const has_rows: c_int = @as(c_int, @intFromBool((mi_row + hbs) < cm.*.mi_rows));
    _ = &has_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const has_cols: c_int = @as(c_int, @intFromBool((mi_col + hbs) < cm.*.mi_cols));
    _ = &has_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &twd.*.xd;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((mi_row >= cm.*.mi_rows) or (mi_col >= cm.*.mi_cols)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if ((parse_recon_flag & PARSE) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xd.*.partition.* = read_partition(twd, mi_row, mi_col, has_rows, has_cols, n8x8_l2);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    partition = xd.*.partition.*;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.partition += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    subsize = get_subsize(bsize, partition);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(hbs != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xd.*.bmode_blocks_wl = @bitCast(@as(i8, @truncate(@as(c_int, 1) >> @intCast(@as(c_int, @intFromBool(!!((partition & @as(PARTITION_TYPE, PARTITION_VERT)) != 0)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        xd.*.bmode_blocks_hl = @bitCast(@as(i8, @truncate(@as(c_int, 1) >> @intCast(@as(c_int, @intFromBool(!!((partition & @as(PARTITION_TYPE, PARTITION_HORZ)) != 0)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        process_block.?(twd, pbi, mi_row, mi_col, subsize, 1, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        blk: {
            switch (partition) {
                @as(PARTITION_TYPE, PARTITION_NONE) => {
                    process_block.?(twd, pbi, mi_row, mi_col, subsize, n4x4_l2, n4x4_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_HORZ) => {
                    process_block.?(twd, pbi, mi_row, mi_col, subsize, n4x4_l2, n8x8_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (has_rows != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        process_block.?(twd, pbi, mi_row + hbs, mi_col, subsize, n4x4_l2, n8x8_l2);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_VERT) => {
                    process_block.?(twd, pbi, mi_row, mi_col, subsize, n8x8_l2, n4x4_l2);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (has_cols != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        process_block.?(twd, pbi, mi_row, mi_col + hbs, subsize, n8x8_l2, n4x4_l2);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    break :blk;
                },
                @as(PARTITION_TYPE, PARTITION_SPLIT) => {
                    process_partition(twd, pbi, mi_row, mi_col, subsize, n8x8_l2, parse_recon_flag, process_block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    process_partition(twd, pbi, mi_row, mi_col + hbs, subsize, n8x8_l2, parse_recon_flag, process_block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    process_partition(twd, pbi, mi_row + hbs, mi_col, subsize, n8x8_l2, parse_recon_flag, process_block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    process_partition(twd, pbi, mi_row + hbs, mi_col + hbs, subsize, n8x8_l2, parse_recon_flag, process_block);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    break :blk;
                },
                else => {
                    _ = @as(c_int, 0);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                },
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((parse_recon_flag & PARSE) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (((@as(c_int, bsize) == @as(c_int, 3)) or (partition != @as(PARTITION_TYPE, PARTITION_SPLIT))) and (@as(c_int, bsize) >= @as(c_int, 3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dec_update_partition_context(twd, mi_row, mi_col, subsize, num_8x8_wh);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn setup_token_decoder(arg_data: [*c]const u8, arg_data_end: [*c]const u8, arg_read_size: usize, arg_error_info: [*c]struct_vpx_internal_error_info, arg_r: [*c]vpx_reader, arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque) callconv(.c) void {
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    var read_size = arg_read_size;
    _ = &read_size;
    var error_info = arg_error_info;
    _ = &error_info;
    var r = arg_r;
    _ = &r;
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(read_is_valid(data, read_size, data_end) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(error_info)), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_vp9_vpx_reader_init(r, data, read_size, decrypt_cb, decrypt_state) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(error_info)), VPX_CODEC_MEM_ERROR);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn read_coef_probs_common(arg_coef_probs: [*c]vp9_coeff_probs_model, arg_r: [*c]vpx_reader) callconv(.c) void {
    var coef_probs = arg_coef_probs;
    _ = &coef_probs;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    var k: c_int = undefined;
    _ = &k;
    var l: c_int = undefined;
    _ = &l;
    var m: c_int = undefined;
    _ = &m;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (vpx_read_bit(r) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < PLANE_TYPES) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < @as(c_int, 2)) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        {
                            k = 0;
                            while (k < @as(c_int, 6)) : (k += 1) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                {
                                    l = 0;
                                    while (l < (if (k == @as(c_int, 0)) @as(c_int, 3) else @as(c_int, 6))) : (l += 1) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                        {
                                            m = 0;
                                            while (m < @as(c_int, 3)) : (m += 1) {
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                                antfly_vp9_vp9_diff_update_prob(r, &coef_probs[@bitCast(@as(isize, @intCast(i)))][@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(l)))][@bitCast(@as(isize, @intCast(m)))]);
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return;
                                                }
                                            }
                                        }
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return;
                                        }
                                    }
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn read_coef_probs(arg_fc: [*c]FRAME_CONTEXT, arg_tx_mode: TX_MODE, arg_r: [*c]vpx_reader) callconv(.c) void {
    var fc = arg_fc;
    _ = &fc;
    var tx_mode = arg_tx_mode;
    _ = &tx_mode;
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const max_tx_size: TX_SIZE = antfly_vp9_tx_mode_to_biggest_tx_size[tx_mode];
    _ = &max_tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var tx_size: TX_SIZE = undefined;
    _ = &tx_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        tx_size = @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))));
        while (@as(c_int, tx_size) <= @as(c_int, max_tx_size)) : (tx_size +%= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            read_coef_probs_common(@ptrCast(@alignCast(&fc.*.coef_probs[tx_size])), r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
}
pub fn setup_segmentation(arg_seg: [*c]struct_segmentation, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var seg = arg_seg;
    _ = &seg;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var j: c_int = undefined;
    _ = &j;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.update_map = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.update_data = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    seg.*.enabled = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(@as(c_int, seg.*.enabled) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    seg.*.update_map = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, seg.*.update_map) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < (@as(c_int, 8) - @as(c_int, 1))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                seg.*.tree_probs[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(if (antfly_vp9_vpx_rb_read_bit(rb) != 0) antfly_vp9_vpx_rb_read_literal(rb, 8) else @as(c_int, 255))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        seg.*.temporal_update = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, seg.*.temporal_update) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 3)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    seg.*.pred_probs[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(if (antfly_vp9_vpx_rb_read_bit(rb) != 0) antfly_vp9_vpx_rb_read_literal(rb, 8) else @as(c_int, 255))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 3)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    seg.*.pred_probs[@bitCast(@as(isize, @intCast(i)))] = 255;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
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
    seg.*.update_data = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, seg.*.update_data) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        seg.*.abs_delta = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_clearall_segfeatures(seg);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            i = 0;
            while (i < @as(c_int, 8)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    j = 0;
                    while (j < SEG_LVL_MAX) : (j += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var data: c_int = 0;
                        _ = &data;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const feature_enabled: c_int = antfly_vp9_vpx_rb_read_bit(rb);
                        _ = &feature_enabled;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (feature_enabled != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            antfly_vp9_vp9_enable_segfeature(seg, i, @bitCast(j));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            data = decode_unsigned_max(rb, antfly_vp9_vp9_seg_feature_data_max(@bitCast(j)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_vp9_vp9_is_segfeature_signed(@bitCast(j)) != 0) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                data = if (antfly_vp9_vpx_rb_read_bit(rb) != 0) -data else data;
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return;
                                }
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vp9_set_segdata(seg, i, @bitCast(j), data);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
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
pub fn setup_loopfilter(arg_lf: [*c]struct_loopfilter, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var lf = arg_lf;
    _ = &lf;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.filter_level = antfly_vp9_vpx_rb_read_literal(rb, 6);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.sharpness_level = antfly_vp9_vpx_rb_read_literal(rb, 3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_ref_delta_update = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    lf.*.mode_ref_delta_enabled = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, lf.*.mode_ref_delta_enabled) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        lf.*.mode_ref_delta_update = @bitCast(@as(i8, @truncate(antfly_vp9_vpx_rb_read_bit(rb))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (@as(c_int, lf.*.mode_ref_delta_update) != 0) {
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
                while (i < @as(c_int, 4)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        lf.*.ref_deltas[@bitCast(@as(isize, @intCast(i)))] = @truncate(antfly_vp9_vpx_rb_read_signed_literal(rb, 6));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            {
                i = 0;
                while (i < @as(c_int, 2)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        lf.*.mode_deltas[@bitCast(@as(isize, @intCast(i)))] = @truncate(antfly_vp9_vpx_rb_read_signed_literal(rb, 6));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
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
pub fn read_delta_q(arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) c_int {
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (antfly_vp9_vpx_rb_read_bit(rb) != 0) antfly_vp9_vpx_rb_read_signed_literal(rb, 4) else @as(c_int, 0);
}
pub fn setup_quantization(cm: [*c]VP9_COMMON, xd: [*c]MACROBLOCKD, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    _ = &cm;
    _ = &xd;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.base_qindex = antfly_vp9_vpx_rb_read_literal(rb, 8);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.y_dc_delta_q = read_delta_q(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.uv_dc_delta_q = read_delta_q(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.uv_ac_delta_q = read_delta_q(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.dequant_bit_depth = cm.*.bit_depth;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.lossless = @as(c_int, @intFromBool((((cm.*.base_qindex == @as(c_int, 0)) and (cm.*.y_dc_delta_q == @as(c_int, 0))) and (cm.*.uv_dc_delta_q == @as(c_int, 0))) and (cm.*.uv_ac_delta_q == @as(c_int, 0))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.bd = @bitCast(cm.*.bit_depth);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn setup_segmentation_dequant(cm: [*c]VP9_COMMON) callconv(.c) void {
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(c_int, cm.*.seg.enabled) != 0) {
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
            while (i < @as(c_int, 8)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const qindex: c_int = antfly_vp9_vp9_get_qindex(&cm.*.seg, i, cm.*.base_qindex);
                _ = &qindex;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.y_dequant[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)] = antfly_vp9_vp9_dc_quant(qindex, cm.*.y_dc_delta_q, cm.*.bit_depth);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.y_dequant[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)] = antfly_vp9_vp9_ac_quant(qindex, 0, cm.*.bit_depth);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.uv_dequant[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)] = antfly_vp9_vp9_dc_quant(qindex, cm.*.uv_dc_delta_q, cm.*.bit_depth);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.uv_dequant[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)] = antfly_vp9_vp9_ac_quant(qindex, cm.*.uv_ac_delta_q, cm.*.bit_depth);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const qindex: c_int = cm.*.base_qindex;
        _ = &qindex;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.y_dequant[@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vp9_dc_quant(qindex, cm.*.y_dc_delta_q, cm.*.bit_depth);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.y_dequant[@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vp9_ac_quant(qindex, 0, cm.*.bit_depth);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.uv_dequant[@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vp9_dc_quant(qindex, cm.*.uv_dc_delta_q, cm.*.bit_depth);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.uv_dequant[@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vp9_ac_quant(qindex, cm.*.uv_ac_delta_q, cm.*.bit_depth);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_interp_filter(arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) INTERP_FILTER {
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const literal_to_filter: [4]INTERP_FILTER = [4]INTERP_FILTER{
        1,
        0,
        2,
        3,
    };
    _ = &literal_to_filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(i8, @truncate(if (antfly_vp9_vpx_rb_read_bit(rb) != 0) @as(c_int, 4) else @as(c_int, literal_to_filter[@bitCast(@as(isize, @intCast(antfly_vp9_vpx_rb_read_literal(rb, 2))))]))));
}
pub fn setup_render_size(arg_cm: [*c]VP9_COMMON, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.render_width = cm.*.width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.render_height = cm.*.height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_read_frame_size(rb, &cm.*.render_width, &cm.*.render_height);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn resize_mv_buffer(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(cm.*.cur_frame.*.mvs)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.cur_frame.*.mi_rows = cm.*.mi_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.cur_frame.*.mi_cols = cm.*.mi_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.cur_frame.*.mvs = @as([*c]MV_REF, @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, cm.*.mi_rows * cm.*.mi_cols)), @sizeOf(@TypeOf(cm.*.cur_frame.*.mvs.*))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(cm.*.cur_frame.*.mvs != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (!false) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn resize_context_buffers(arg_cm: [*c]VP9_COMMON, arg_width: c_int, arg_height: c_int) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((cm.*.width != width) or (cm.*.height != height)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const new_mi_rows: c_int = ((height + ((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) >> @intCast(@as(c_int, 3));
        _ = &new_mi_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const new_mi_cols: c_int = ((width + ((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) >> @intCast(@as(c_int, 3));
        _ = &new_mi_cols;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((new_mi_cols > cm.*.mi_cols) or (new_mi_rows > cm.*.mi_rows)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_vp9_vp9_alloc_context_buffers(cm, width, height) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.width = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                cm.*.height = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_set_mb_mi(cm, width, height);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_init_context_buffers(cm);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.width = width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.height = height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((@as(?*anyopaque, @ptrCast(@alignCast(cm.*.cur_frame.*.mvs))) == @as(?*anyopaque, null)) or (cm.*.mi_rows > cm.*.cur_frame.*.mi_rows)) or (cm.*.mi_cols > cm.*.cur_frame.*.mi_cols)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        resize_mv_buffer(cm);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn setup_frame_size(arg_cm: [*c]VP9_COMMON, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var width: c_int = undefined;
    _ = &width;
    var height: c_int = undefined;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_read_frame_size(rb, &width, &height);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    resize_context_buffers(cm, width, height);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    setup_render_size(cm, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (antfly_vp9_vpx_realloc_frame_buffer(get_frame_new_buffer(cm), cm.*.width, cm.*.height, cm.*.subsampling_x, cm.*.subsampling_y, cm.*.use_highbitdepth, 32, cm.*.byte_alignment, &pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].raw_frame_buffer, pool.*.get_fb_cb, pool.*.cb_priv) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].released = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.subsampling_x = cm.*.subsampling_x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.subsampling_y = cm.*.subsampling_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.bit_depth = cm.*.bit_depth;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.color_space = cm.*.color_space;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.color_range = cm.*.color_range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.render_width = cm.*.render_width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.render_height = cm.*.render_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn valid_ref_frame_img_fmt(arg_ref_bit_depth: vpx_bit_depth_t, arg_ref_xss: c_int, arg_ref_yss: c_int, arg_this_bit_depth: vpx_bit_depth_t, arg_this_xss: c_int, arg_this_yss: c_int) callconv(.c) c_int {
    var ref_bit_depth = arg_ref_bit_depth;
    _ = &ref_bit_depth;
    var ref_xss = arg_ref_xss;
    _ = &ref_xss;
    var ref_yss = arg_ref_yss;
    _ = &ref_yss;
    var this_bit_depth = arg_this_bit_depth;
    _ = &this_bit_depth;
    var this_xss = arg_this_xss;
    _ = &this_xss;
    var this_yss = arg_this_yss;
    _ = &this_yss;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(((ref_bit_depth == this_bit_depth) and (ref_xss == this_xss)) and (ref_yss == this_yss)));
}
pub fn setup_frame_size_with_refs(arg_cm: [*c]VP9_COMMON, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var width: c_int = undefined;
    _ = &width;
    var height: c_int = undefined;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var found: c_int = 0;
    _ = &found;
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var has_valid_ref_frame: c_int = 0;
    _ = &has_valid_ref_frame;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))].idx != -@as(c_int, 1)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const buf: [*c]YV12_BUFFER_CONFIG = cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))].buf;
                    _ = &buf;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    width = buf.*.y_crop_width;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    height = buf.*.y_crop_height;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    found = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    break;
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
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
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(found != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_read_frame_size(rb, &width, &height);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if ((width <= @as(c_int, 0)) or (height <= @as(c_int, 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const ref_frame: [*c]RefBuffer = &cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))];
            _ = &ref_frame;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            has_valid_ref_frame |= @as(c_int, @intFromBool((ref_frame.*.idx != -@as(c_int, 1)) and (valid_ref_frame_size(ref_frame.*.buf.*.y_crop_width, ref_frame.*.buf.*.y_crop_height, width, height) != 0)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(has_valid_ref_frame != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    {
        i = 0;
        while (i < @as(c_int, 3)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const ref_frame: [*c]RefBuffer = &cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))];
            _ = &ref_frame;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((ref_frame.*.idx == -@as(c_int, 1)) or !(valid_ref_frame_img_fmt(ref_frame.*.buf.*.bit_depth, ref_frame.*.buf.*.subsampling_x, ref_frame.*.buf.*.subsampling_y, cm.*.bit_depth, cm.*.subsampling_x, cm.*.subsampling_y) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    resize_context_buffers(cm, width, height);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    setup_render_size(cm, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (antfly_vp9_vpx_realloc_frame_buffer(get_frame_new_buffer(cm), cm.*.width, cm.*.height, cm.*.subsampling_x, cm.*.subsampling_y, cm.*.use_highbitdepth, 32, cm.*.byte_alignment, &pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].raw_frame_buffer, pool.*.get_fb_cb, pool.*.cb_priv) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].released = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.subsampling_x = cm.*.subsampling_x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.subsampling_y = cm.*.subsampling_y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.bit_depth = cm.*.bit_depth;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.color_space = cm.*.color_space;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.color_range = cm.*.color_range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.render_width = cm.*.render_width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pool.*.frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].buf.render_height = cm.*.render_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn setup_tile_info(arg_cm: [*c]VP9_COMMON, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var min_log2_tile_cols: c_int = undefined;
    _ = &min_log2_tile_cols;
    var max_log2_tile_cols: c_int = undefined;
    _ = &max_log2_tile_cols;
    var max_ones: c_int = undefined;
    _ = &max_ones;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_get_tile_n_bits(cm.*.mi_cols, &min_log2_tile_cols, &max_log2_tile_cols);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    max_ones = max_log2_tile_cols - min_log2_tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.log2_tile_cols = min_log2_tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (((blk: {
        const ref = &max_ones;
        const tmp = ref.*;
        ref.* -= 1;
        break :blk tmp;
    }) != 0) and (antfly_vp9_vpx_rb_read_bit(rb) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.log2_tile_cols += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (cm.*.log2_tile_cols > @as(c_int, 6)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    cm.*.log2_tile_rows = antfly_vp9_vpx_rb_read_bit(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.log2_tile_rows != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.log2_tile_rows += antfly_vp9_vpx_rb_read_bit(rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub fn get_tile_buffer(data_end: [*c]const u8, arg_is_last: c_int, arg_error_info: [*c]struct_vpx_internal_error_info, arg_data: [*c][*c]const u8, arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque, arg_buf: [*c]TileBuffer) callconv(.c) void {
    _ = &data_end;
    var is_last = arg_is_last;
    _ = &is_last;
    var error_info = arg_error_info;
    _ = &error_info;
    var data = arg_data;
    _ = &data;
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    var buf = arg_buf;
    _ = &buf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var size: usize = undefined;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(is_last != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(read_is_valid(data.*, 4, data_end) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(error_info)), VPX_CODEC_CORRUPT_FRAME);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (decrypt_cb != null) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var be_data: [4]u8 = undefined;
            _ = &be_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            decrypt_cb.?(decrypt_state, data.*, @ptrCast(@alignCast(&be_data)), 4);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            size = mem_get_be32_as_int(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&be_data))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            size = mem_get_be32_as_int(@ptrCast(@alignCast(data.*)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        data.* += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (size > @as(usize, @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data.*))), @sizeOf(u8)))))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(error_info)), VPX_CODEC_CORRUPT_FRAME);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        size = @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data.*))), @sizeOf(u8))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    buf.*.data = data.*;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    buf.*.size = size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    data.* += size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_tile_buffers(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_data_end: [*c]const u8, arg_tile_cols: c_int, arg_tile_rows: c_int, arg_tile_buffers: [*c][64]TileBuffer) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    var tile_cols = arg_tile_cols;
    _ = &tile_cols;
    var tile_rows = arg_tile_rows;
    _ = &tile_rows;
    var tile_buffers = arg_tile_buffers;
    _ = &tile_buffers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    var c: c_int = undefined;
    _ = &c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < tile_rows) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < tile_cols) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const is_last: c_int = @as(c_int, @intFromBool((r == (tile_rows - @as(c_int, 1))) and (c == (tile_cols - @as(c_int, 1)))));
                    _ = &is_last;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const buf: [*c]TileBuffer = &tile_buffers[@bitCast(@as(isize, @intCast(r)))][@bitCast(@as(isize, @intCast(c)))];
                    _ = &buf;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    buf.*.col = c;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    get_tile_buffer(data_end, is_last, &pbi.*.common.@"error", &data, pbi.*.decrypt_cb, pbi.*.decrypt_state, buf);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn map_write(row_mt_worker_data: [*c]RowMTWorkerData, arg_map_idx: c_int, arg_sync_idx: c_int) callconv(.c) void {
    _ = &row_mt_worker_data;
    var map_idx = arg_map_idx;
    _ = &map_idx;
    var sync_idx = arg_sync_idx;
    _ = &sync_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &map_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &sync_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn map_read(row_mt_worker_data: [*c]RowMTWorkerData, arg_map_idx: c_int, arg_sync_idx: c_int) callconv(.c) void {
    _ = &row_mt_worker_data;
    var map_idx = arg_map_idx;
    _ = &map_idx;
    var sync_idx = arg_sync_idx;
    _ = &sync_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &map_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &sync_idx;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn lpf_map_write_check(arg_lf_sync: [*c]VP9LfSync, arg_row: c_int, arg_num_tile_cols: c_int) callconv(.c) c_int {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var row = arg_row;
    _ = &row;
    var num_tile_cols = arg_num_tile_cols;
    _ = &num_tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var return_val: c_int = 0;
    _ = &return_val;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &num_tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return return_val;
}
pub fn vp9_tile_done(arg_pbi: [*c]VP9Decoder) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn vp9_jobq_alloc(arg_pbi: [*c]VP9Decoder) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows);
    _ = &aligned_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_rows: c_int = aligned_rows >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const jobq_size: usize = @as(c_ulong, @bitCast(@as(c_long, ((tile_cols * sb_rows) * @as(c_int, 2)) + sb_rows))) *% @sizeOf(Job);
    _ = &jobq_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (jobq_size > row_mt_worker_data.*.jobq_size) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(row_mt_worker_data.*.jobq_buf)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        while (true) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            row_mt_worker_data.*.jobq_buf = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, jobq_size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(row_mt_worker_data.*.jobq_buf != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (!false) break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_jobq_init(&row_mt_worker_data.*.jobq, row_mt_worker_data.*.jobq_buf, jobq_size);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        row_mt_worker_data.*.jobq_size = jobq_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn recon_tile_row(arg_tile_data: [*c]TileWorkerData, arg_pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_is_last_row: c_int, arg_lf_sync: [*c]VP9LfSync, arg_cur_tile_col: c_int) callconv(.c) void {
    var tile_data = arg_tile_data;
    _ = &tile_data;
    var pbi = arg_pbi;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var is_last_row = arg_is_last_row;
    _ = &is_last_row;
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var cur_tile_col = arg_cur_tile_col;
    _ = &cur_tile_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
    _ = &aligned_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const sb_cols: c_int = aligned_cols >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cur_sb_row: c_int = mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &cur_sb_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_col_start: c_int = tile_data.*.xd.tile.mi_col_start;
    _ = &mi_col_start;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_col_end: c_int = tile_data.*.xd.tile.mi_col_end;
    _ = &mi_col_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_col: c_int = undefined;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_seg_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_seg_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mi_col = mi_col_start;
        while (mi_col < mi_col_end) : (mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const c: c_int = mi_col >> @intCast(@as(c_int, 6) - @as(c_int, 3));
            _ = &c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var plane: c_int = undefined;
            _ = &plane;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const sb_num: c_int = (cur_sb_row * (aligned_cols >> @intCast(@as(c_int, 6) - @as(c_int, 3)))) + c;
            _ = &sb_num;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (cur_sb_row != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                map_read(row_mt_worker_data, ((cur_sb_row - @as(c_int, 1)) * sb_cols) + c, ((cur_sb_row - @as(c_int, 1)) * tile_cols) + cur_tile_col);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                plane = 0;
                while (plane < @as(c_int, 3)) : (plane += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].eob = row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))] + @as(usize, @bitCast(@as(isize, @intCast(sb_num << @intCast(@as(c_int, 8))))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].dqcoeff = row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))] + @as(usize, @bitCast(@as(isize, @intCast(sb_num << @intCast(@as(c_int, 12))))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            tile_data.*.xd.partition = row_mt_worker_data.*.partition + @as(usize, @bitCast(@as(isize, @intCast(sb_num * @as(c_int, 85)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            process_partition(tile_data, pbi, mi_row, mi_col, 12, 4, RECON, recon_block);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                var is_lpf_job_ready: c_int = 0;
                _ = &is_lpf_job_ready;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((mi_col + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) >= mi_col_end) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    is_lpf_job_ready = lpf_map_write_check(lf_sync, cur_sb_row, tile_cols);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (is_lpf_job_ready != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        var lpf_job: Job = undefined;
                        _ = &lpf_job;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        lpf_job.job_type = LPF_JOB;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (cur_sb_row > @as(c_int, 0)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            lpf_job.row_num = mi_row - (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_vp9_vp9_jobq_queue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&lpf_job)), @sizeOf(@TypeOf(lpf_job)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (is_last_row != 0) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            lpf_job.row_num = mi_row;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_vp9_vp9_jobq_queue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&lpf_job)), @sizeOf(@TypeOf(lpf_job)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
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
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            map_write(row_mt_worker_data, (cur_sb_row * sb_cols) + c, (cur_sb_row * tile_cols) + cur_tile_col);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn parse_tile_row(arg_tile_data: [*c]TileWorkerData, arg_pbi: [*c]VP9Decoder, arg_mi_row: c_int, arg_cur_tile_col: c_int, arg_data_end: [*c][*c]u8) callconv(.c) void {
    var tile_data = arg_tile_data;
    _ = &tile_data;
    var pbi = arg_pbi;
    _ = &pbi;
    var mi_row = arg_mi_row;
    _ = &mi_row;
    var cur_tile_col = arg_cur_tile_col;
    _ = &cur_tile_col;
    var data_end = arg_data_end;
    _ = &data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var mi_col: c_int = undefined;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var tile: [*c]TileInfo = &tile_data.*.xd.tile;
    _ = &tile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const buf: [*c]TileBuffer = &pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(cur_tile_col)))];
    _ = &buf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
    _ = &aligned_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.dqcoeff)), 0, @sizeOf(@TypeOf(tile_data.*.dqcoeff)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_tile_init(tile, cm, 0, cur_tile_col);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (mi_row == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        setup_token_decoder(buf.*.data, data_end.*, buf.*.size, &tile_data.*.error_info, &tile_data.*.bit_reader, pbi.*.decrypt_cb, pbi.*.decrypt_state);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    vp9_init_macroblockd(cm, &tile_data.*.xd, @ptrCast(@alignCast(&tile_data.*.dqcoeff)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    tile_data.*.xd.error_info = &tile_data.*.error_info;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_seg_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_seg_context)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        mi_col = tile.*.mi_col_start;
        while (mi_col < tile.*.mi_col_end) : (mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const r: c_int = mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3));
            _ = &r;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const c: c_int = mi_col >> @intCast(@as(c_int, 6) - @as(c_int, 3));
            _ = &c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var plane: c_int = undefined;
            _ = &plane;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const sb_num: c_int = (r * (aligned_cols >> @intCast(@as(c_int, 6) - @as(c_int, 3)))) + c;
            _ = &sb_num;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                plane = 0;
                while (plane < @as(c_int, 3)) : (plane += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].eob = row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))] + @as(usize, @bitCast(@as(isize, @intCast(sb_num << @intCast(@as(c_int, 8))))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].dqcoeff = row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))] + @as(usize, @bitCast(@as(isize, @intCast(sb_num << @intCast(@as(c_int, 12))))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            tile_data.*.xd.partition = row_mt_worker_data.*.partition + @as(usize, @bitCast(@as(isize, @intCast(sb_num * @as(c_int, 85)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            process_partition(tile_data, pbi, mi_row, mi_col, 12, 4, PARSE, parse_block);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn row_decode_worker_hook(arg_arg1: ?*anyopaque, arg_arg2: ?*anyopaque) callconv(.c) c_int {
    var arg1 = arg_arg1;
    _ = &arg1;
    var arg2 = arg_arg2;
    _ = &arg2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const thread_data: [*c]ThreadData = @ptrCast(@alignCast(arg1));
    _ = &thread_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var data_end: [*c][*c]u8 = @ptrCast(@alignCast(arg2));
    _ = &data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pbi: [*c]VP9Decoder = thread_data.*.pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const aligned_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
    _ = &aligned_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const aligned_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows);
    _ = &aligned_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const sb_rows: c_int = aligned_rows >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var job: Job = undefined;
    _ = &job;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var lf_data: [*c]LFWorkerData = thread_data.*.lf_data;
    _ = &lf_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var lf_sync: [*c]VP9LfSync = thread_data.*.lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var corrupted: c_int = 0;
    _ = &corrupted;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tile_data_recon: [*c]TileWorkerData = null;
    _ = &tile_data_recon;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (!(antfly_vp9_vp9_jobq_dequeue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&job)), @sizeOf(@TypeOf(job)), 1) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var mi_col: c_int = undefined;
        _ = &mi_col;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const mi_row: c_int = job.row_num;
        _ = &mi_row;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (job.job_type == @as(JobType, LPF_JOB)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            lf_data.*.start = mi_row;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            lf_data.*.stop = lf_data.*.start + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) and (mi_row < cm.*.mi_rows)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_vp9_vp9_loopfilter_job(lf_data, lf_sync);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (job.job_type == @as(JobType, RECON_JOB)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const cur_sb_row: c_int = mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                _ = &cur_sb_row;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                const is_last_row: c_int = @as(c_int, @intFromBool((sb_rows - @as(c_int, 1)) == cur_sb_row));
                _ = &is_last_row;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                var mi_col_start: c_int = undefined;
                _ = &mi_col_start;
                var mi_col_end: c_int = undefined;
                _ = &mi_col_end;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(tile_data_recon != null)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    while (true) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        _ = @as(c_int, 0);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        tile_data_recon = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, @sizeOf(TileWorkerData))));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (!(tile_data_recon != null)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                        if (!false) break;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                tile_data_recon.*.xd = pbi.*.mb;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_vp9_vp9_tile_init(&tile_data_recon.*.xd.tile, cm, 0, job.tile_col);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                vp9_init_macroblockd(cm, &tile_data_recon.*.xd, @ptrCast(@alignCast(&tile_data_recon.*.dqcoeff)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                mi_col_start = tile_data_recon.*.xd.tile.mi_col_start;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                mi_col_end = tile_data_recon.*.xd.tile.mi_col_end;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (false) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const sb_cols: c_int = aligned_cols >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                    _ = &sb_cols;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    tile_data_recon.*.error_info.setjmp = 0;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    corrupted = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    {
                        mi_col = mi_col_start;
                        while (mi_col < mi_col_end) : (mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            const c: c_int = mi_col >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                            _ = &c;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                            map_write(row_mt_worker_data, (cur_sb_row * sb_cols) + c, (cur_sb_row * tile_cols) + job.tile_col);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return 0;
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (is_last_row != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        vp9_tile_done(pbi);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    continue;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                tile_data_recon.*.error_info.setjmp = 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                tile_data_recon.*.xd.error_info = &tile_data_recon.*.error_info;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                recon_tile_row(tile_data_recon, pbi, mi_row, is_last_row, lf_sync, job.tile_col);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (corrupted != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&tile_data_recon.*.error_info)), VPX_CODEC_CORRUPT_FRAME);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (is_last_row != 0) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    vp9_tile_done(pbi);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (job.job_type == @as(JobType, PARSE_JOB)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(job.tile_col)))];
                    _ = &tile_data;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (false) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        tile_data.*.error_info.setjmp = 0;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        corrupted = 1;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        vp9_tile_done(pbi);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        continue;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    tile_data.*.xd = pbi.*.mb;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    tile_data.*.xd.counts = if (cm.*.frame_parallel_decoding_mode != 0) @as([*c]FRAME_COUNTS, null) else &tile_data.*.counts;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    tile_data.*.error_info.setjmp = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    parse_tile_row(tile_data, pbi, mi_row, job.tile_col, data_end);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    corrupted |= tile_data.*.xd.corrupted;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (corrupted != 0) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&tile_data.*.error_info)), VPX_CODEC_CORRUPT_FRAME);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        var recon_job: Job = undefined;
                        _ = &recon_job;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        recon_job.row_num = mi_row;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        recon_job.tile_col = job.tile_col;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        recon_job.job_type = RECON_JOB;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        _ = antfly_vp9_vp9_jobq_queue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&recon_job)), @sizeOf(@TypeOf(recon_job)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if ((mi_row + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) < cm.*.mi_rows) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        var parse_job: Job = undefined;
                        _ = &parse_job;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        parse_job.row_num = mi_row + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        parse_job.tile_col = job.tile_col;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        parse_job.job_type = PARSE_JOB;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        _ = antfly_vp9_vp9_jobq_queue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&parse_job)), @sizeOf(@TypeOf(parse_job)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(tile_data_recon)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(!(corrupted != 0)));
}
pub fn decode_tiles(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_data_end: [*c]const u8) callconv(.c) [*c]const u8 {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const aligned_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
    _ = &aligned_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_rows: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_rows);
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var tile_buffers: [4][64]TileBuffer = undefined;
    _ = &tile_buffers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var tile_row: c_int = undefined;
    _ = &tile_row;
    var tile_col: c_int = undefined;
    _ = &tile_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var mi_row: c_int = undefined;
    _ = &mi_row;
    var mi_col: c_int = undefined;
    _ = &mi_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var tile_data: [*c]TileWorkerData = null;
    _ = &tile_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) and (pbi.*.lf_worker.data1 == @as(?*anyopaque, null))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        while (true) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            pbi.*.lf_worker.data1 = antfly_vp9_vpx_memalign(32, @sizeOf(LFWorkerData));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (!(pbi.*.lf_worker.data1 != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
            if (!false) break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        pbi.*.lf_worker.hook = antfly_vp9_vp9_loop_filter_worker;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if ((pbi.*.max_threads > @as(c_int, 1)) and !(winterface.*.reset.?(&pbi.*.lf_worker) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_ERROR);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const lf_data: [*c]LFWorkerData = @ptrCast(@alignCast(pbi.*.lf_worker.data1));
        _ = &lf_data;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = winterface.*.sync.?(&pbi.*.lf_worker);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        antfly_vp9_vp9_loop_filter_data_reset(lf_data, get_frame_new_buffer(cm), cm, @ptrCast(@alignCast(&pbi.*.mb.plane)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.above_context)), 0, ((@sizeOf(@TypeOf(cm.*.above_context.*)) *% @as(c_ulong, 3)) *% @as(c_ulong, 2)) *% @as(c_ulong, @bitCast(@as(c_long, aligned_cols))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.above_seg_context)), 0, @sizeOf(@TypeOf(cm.*.above_seg_context.*)) *% @as(c_ulong, @bitCast(@as(c_long, aligned_cols))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vp9_reset_lfm(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    get_tile_buffers(pbi, data, data_end, tile_cols, tile_rows, @ptrCast(@alignCast(&tile_buffers)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        tile_row = 0;
        while (tile_row < tile_rows) : (tile_row += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            {
                tile_col = 0;
                while (tile_col < tile_cols) : (tile_col += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    const buf: [*c]const TileBuffer = &tile_buffers[@bitCast(@as(isize, @intCast(tile_row)))][@bitCast(@as(isize, @intCast(tile_col)))];
                    _ = &buf;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    tile_data = (pbi.*.tile_worker_data + @as(usize, @bitCast(@as(isize, @intCast(tile_cols * tile_row))))) + @as(usize, @bitCast(@as(isize, @intCast(tile_col))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    tile_data.*.xd = pbi.*.mb;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    tile_data.*.xd.corrupted = 0;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    tile_data.*.xd.counts = @ptrCast(@alignCast(if (cm.*.frame_parallel_decoding_mode != 0) @as(?*anyopaque, null) else @as(?*anyopaque, @ptrCast(@alignCast(&cm.*.counts)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.dqcoeff)), 0, @sizeOf(@TypeOf(tile_data.*.dqcoeff)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    antfly_vp9_vp9_tile_init(&tile_data.*.xd.tile, cm, tile_row, tile_col);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    setup_token_decoder(buf.*.data, data_end, buf.*.size, &cm.*.@"error", &tile_data.*.bit_reader, pbi.*.decrypt_cb, pbi.*.decrypt_state);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    vp9_init_macroblockd(cm, &tile_data.*.xd, @ptrCast(@alignCast(&tile_data.*.dqcoeff)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        tile_row = 0;
        while (tile_row < tile_rows) : (tile_row += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            var tile: TileInfo = undefined;
            _ = &tile;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            antfly_vp9_vp9_tile_set_row(&tile, cm, tile_row);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            {
                mi_row = tile.mi_row_start;
                while (mi_row < tile.mi_row_end) : (mi_row += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    {
                        tile_col = 0;
                        while (tile_col < tile_cols) : (tile_col += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            const col: c_int = if (pbi.*.inv_tile_order != 0) (tile_cols - tile_col) - @as(c_int, 1) else tile_col;
                            _ = &col;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            tile_data = (pbi.*.tile_worker_data + @as(usize, @bitCast(@as(isize, @intCast(tile_cols * tile_row))))) + @as(usize, @bitCast(@as(isize, @intCast(col))));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            antfly_vp9_vp9_tile_set_col(&tile, cm, col);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_context)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_seg_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_seg_context)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            {
                                mi_col = tile.mi_col_start;
                                while (mi_col < tile.mi_col_end) : (mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return null;
                                    }
                                    if (pbi.*.row_mt == @as(c_int, 1)) {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        var plane: c_int = undefined;
                                        _ = &plane;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
                                        _ = &row_mt_worker_data;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        {
                                            plane = 0;
                                            while (plane < @as(c_int, 3)) : (plane += 1) {
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                                tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].eob = row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))];
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                                tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].dqcoeff = row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))];
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                            }
                                        }
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        tile_data.*.xd.partition = row_mt_worker_data.*.partition;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        process_partition(tile_data, pbi, mi_row, mi_col, 12, 4, PARSE, parse_block);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        {
                                            plane = 0;
                                            while (plane < @as(c_int, 3)) : (plane += 1) {
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                                tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].eob = row_mt_worker_data.*.eob[@bitCast(@as(isize, @intCast(plane)))];
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                                tile_data.*.xd.plane[@bitCast(@as(isize, @intCast(plane)))].dqcoeff = row_mt_worker_data.*.dqcoeff[@bitCast(@as(isize, @intCast(plane)))];
                                                if (antfly_codec_afz_vp9_failed() != 0) {
                                                    return null;
                                                }
                                            }
                                        }
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        tile_data.*.xd.partition = row_mt_worker_data.*.partition;
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        process_partition(tile_data, pbi, mi_row, mi_col, 12, 4, RECON, recon_block);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                    } else {
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                        decode_partition(tile_data, pbi, mi_row, mi_col, 12, 4);
                                        if (antfly_codec_afz_vp9_failed() != 0) {
                                            return null;
                                        }
                                    }
                                    if (antfly_codec_afz_vp9_failed() != 0) {
                                        return null;
                                    }
                                }
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            pbi.*.mb.corrupted |= tile_data.*.xd.corrupted;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            if (pbi.*.mb.corrupted != 0) {
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return null;
                                }
                                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return null;
                                }
                                if (antfly_codec_afz_vp9_failed() != 0) {
                                    return null;
                                }
                            }
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        const lf_start: c_int = mi_row - (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)));
                        _ = &lf_start;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        const lf_data: [*c]LFWorkerData = @ptrCast(@alignCast(pbi.*.lf_worker.data1));
                        _ = &lf_data;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        if (lf_start < @as(c_int, 0)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            continue;
                        }
                        if ((mi_row + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3)))) >= cm.*.mi_rows) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            continue;
                        }
                        _ = winterface.*.sync.?(&pbi.*.lf_worker);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        lf_data.*.start = lf_start;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        lf_data.*.stop = mi_row;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                        if (pbi.*.max_threads > @as(c_int, 1)) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            winterface.*.launch.?(&pbi.*.lf_worker);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                            winterface.*.execute.?(&pbi.*.lf_worker);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return null;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return null;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const lf_data: [*c]LFWorkerData = @ptrCast(@alignCast(pbi.*.lf_worker.data1));
        _ = &lf_data;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = winterface.*.sync.?(&pbi.*.lf_worker);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        lf_data.*.start = lf_data.*.stop;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        lf_data.*.stop = cm.*.mi_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        winterface.*.execute.?(&pbi.*.lf_worker);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    tile_data = (pbi.*.tile_worker_data + @as(usize, @bitCast(@as(isize, @intCast(tile_cols * tile_rows))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return antfly_vp9_vpx_reader_find_end(&tile_data.*.bit_reader);
}
pub fn set_rows_after_error(arg_lf_sync: [*c]VP9LfSync, arg_start_row: c_int, arg_mi_rows: c_int, arg_num_tiles_left: c_int, arg_total_num_tiles: c_int) callconv(.c) void {
    var lf_sync = arg_lf_sync;
    _ = &lf_sync;
    var start_row = arg_start_row;
    _ = &start_row;
    var mi_rows = arg_mi_rows;
    _ = &mi_rows;
    var num_tiles_left = arg_num_tiles_left;
    _ = &num_tiles_left;
    var total_num_tiles = arg_total_num_tiles;
    _ = &total_num_tiles;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var mi_row: c_int = undefined;
        _ = &mi_row;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const aligned_rows: c_int = mi_cols_aligned_to_sb(mi_rows);
        _ = &aligned_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const sb_rows: c_int = aligned_rows >> @intCast(@as(c_int, 6) - @as(c_int, 3));
        _ = &sb_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const corrupted: c_int = 1;
        _ = &corrupted;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            mi_row = start_row;
            while (mi_row < mi_rows) : (mi_row += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const is_last_row: c_int = @as(c_int, @intFromBool((sb_rows - @as(c_int, 1)) == (mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3)))));
                _ = &is_last_row;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_set_row(lf_sync, total_num_tiles, mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3)), is_last_row, corrupted);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        start_row = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!((blk: {
            const ref = &num_tiles_left;
            const tmp = ref.*;
            ref.* -= 1;
            break :blk tmp;
        }) != 0)) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn tile_worker_hook(arg_arg1: ?*anyopaque, arg_arg2: ?*anyopaque) callconv(.c) c_int {
    var arg1 = arg_arg1;
    _ = &arg1;
    var arg2 = arg_arg2;
    _ = &arg2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const tile_data: [*c]TileWorkerData = @ptrCast(@alignCast(arg1));
    _ = &tile_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pbi: [*c]VP9Decoder = @ptrCast(@alignCast(arg2));
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var tile: [*c]TileInfo = &tile_data.*.xd.tile;
    _ = &tile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const final_col: c_int = (@as(c_int, 1) << @intCast(pbi.*.common.log2_tile_cols)) - @as(c_int, 1);
    _ = &final_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bit_reader_end: [*c]const u8 = null;
    _ = &bit_reader_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var lf_data: [*c]LFWorkerData = tile_data.*.lf_data;
    _ = &lf_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var lf_sync: [*c]VP9LfSync = tile_data.*.lf_sync;
    _ = &lf_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mi_row: c_int = 0;
    _ = &mi_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var n: c_int = tile_data.*.buf_start;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (false) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tile_data.*.error_info.setjmp = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tile_data.*.xd.corrupted = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tile_data.*.data_end = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (((pbi.*.lpf_mt_opt != 0) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const num_tiles_left: c_int = tile_data.*.buf_end - n;
            _ = &num_tiles_left;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const mi_row_start: c_int = mi_row;
            _ = &mi_row_start;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            set_rows_after_error(lf_sync, mi_row_start, cm.*.mi_rows, num_tiles_left, @as(c_int, 1) << @intCast(cm.*.log2_tile_cols));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    tile_data.*.error_info.setjmp = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    tile_data.*.xd.corrupted = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (true) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var mi_col: c_int = undefined;
        _ = &mi_col;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const buf: [*c]const TileBuffer = @as([*c]TileBuffer, @ptrCast(@alignCast(&pbi.*.tile_buffers))) + @as(usize, @bitCast(@as(isize, @intCast(n))));
        _ = &buf;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        mi_row = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.dqcoeff)), 0, @sizeOf(@TypeOf(tile_data.*.dqcoeff)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_tile_init(tile, &pbi.*.common, 0, buf.*.col);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        setup_token_decoder(buf.*.data, tile_data.*.data_end, buf.*.size, &tile_data.*.error_info, &tile_data.*.bit_reader, pbi.*.decrypt_cb, pbi.*.decrypt_state);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        vp9_init_macroblockd(&pbi.*.common, &tile_data.*.xd, @ptrCast(@alignCast(&tile_data.*.dqcoeff)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        tile_data.*.xd.error_info = &tile_data.*.error_info;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            mi_row = tile.*.mi_row_start;
            while (mi_row < tile.*.mi_row_end) : (mi_row += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_context)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.xd.left_seg_context)), 0, @sizeOf(@TypeOf(tile_data.*.xd.left_seg_context)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                {
                    mi_col = tile.*.mi_col_start;
                    while (mi_col < tile.*.mi_col_end) : (mi_col += @as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 3))) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        decode_partition(tile_data, pbi, mi_row, mi_col, 12, 4);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (((pbi.*.lpf_mt_opt != 0) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const aligned_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows);
                    _ = &aligned_rows;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const sb_rows: c_int = aligned_rows >> @intCast(@as(c_int, 6) - @as(c_int, 3));
                    _ = &sb_rows;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    const is_last_row: c_int = @as(c_int, @intFromBool((sb_rows - @as(c_int, 1)) == (mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3)))));
                    _ = &is_last_row;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    antfly_vp9_vp9_set_row(lf_sync, @as(c_int, 1) << @intCast(cm.*.log2_tile_cols), mi_row >> @intCast(@as(c_int, 6) - @as(c_int, 3)), is_last_row, tile_data.*.xd.corrupted);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (buf.*.col == final_col) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            bit_reader_end = antfly_vp9_vpx_reader_find_end(&tile_data.*.bit_reader);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(!(tile_data.*.xd.corrupted != 0) and ((blk: {
            const ref = &n;
            ref.* += 1;
            break :blk ref.*;
        }) <= tile_data.*.buf_end))) break;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((((pbi.*.lpf_mt_opt != 0) and (n < tile_data.*.buf_end)) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        n += 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        set_rows_after_error(lf_sync, 0, cm.*.mi_rows, tile_data.*.buf_end - n, @as(c_int, 1) << @intCast(cm.*.log2_tile_cols));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((((pbi.*.lpf_mt_opt != 0) and !(tile_data.*.xd.corrupted != 0)) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_loopfilter_rows(lf_data, lf_sync);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    tile_data.*.data_end = bit_reader_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(!(tile_data.*.xd.corrupted != 0)));
}
pub fn compare_tile_buffers(arg_a: ?*const anyopaque, arg_b: ?*const anyopaque) callconv(.c) c_int {
    var a = arg_a;
    _ = &a;
    var b = arg_b;
    _ = &b;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const buf_a: [*c]const TileBuffer = @ptrCast(@alignCast(a));
    _ = &buf_a;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const buf_b: [*c]const TileBuffer = @ptrCast(@alignCast(b));
    _ = &buf_b;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(buf_a.*.size < buf_b.*.size)) - @as(c_int, @intFromBool(buf_a.*.size > buf_b.*.size));
}
pub fn init_mt(arg_pbi: [*c]VP9Decoder) callconv(.c) void {
    var pbi = arg_pbi;
    _ = &pbi;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var n: c_int = undefined;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var lf_row_sync: [*c]VP9LfSync = &pbi.*.lf_row_sync;
    _ = &lf_row_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const aligned_mi_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
    _ = &aligned_mi_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (pbi.*.num_tile_workers == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const num_threads: c_int = pbi.*.max_threads;
        _ = &num_threads;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        while (true) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            pbi.*.tile_workers = @ptrCast(@alignCast(antfly_vp9_vpx_malloc(@as(c_ulong, @bitCast(@as(c_long, num_threads))) *% @sizeOf(@TypeOf(pbi.*.tile_workers.*)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(pbi.*.tile_workers != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (!false) break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        {
            n = 0;
            while (n < num_threads) : (n += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))];
                _ = &worker;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                pbi.*.num_tile_workers += 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                winterface.*.init.?(worker);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                worker.*.thread_name = "vpx tile worker";
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((n < (num_threads - @as(c_int, 1))) and !(winterface.*.reset.?(worker) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    while (true) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        winterface.*.end.?(&pbi.*.tile_workers[@bitCast(@as(isize, @intCast(pbi.*.num_tile_workers - @as(c_int, 1))))]);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (!((blk: {
                            const ref = &pbi.*.num_tile_workers;
                            ref.* -= 1;
                            break :blk ref.*;
                        }) != @as(c_int, 0))) break;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.tile_workers)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    pbi.*.tile_workers = null;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_ERROR);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
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
    if ((((pbi.*.lpf_mt_opt != 0) or (pbi.*.row_mt != 0)) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_lpf_mt_init(lf_row_sync, cm, cm.*.lf.filter_level, pbi.*.num_tile_workers);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.above_context)), 0, ((@sizeOf(@TypeOf(cm.*.above_context.*)) *% @as(c_ulong, 3)) *% @as(c_ulong, 2)) *% @as(c_ulong, @bitCast(@as(c_long, aligned_mi_cols))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(cm.*.above_seg_context)), 0, @sizeOf(@TypeOf(cm.*.above_seg_context.*)) *% @as(c_ulong, @bitCast(@as(c_long, aligned_mi_cols))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_reset_lfm(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn decode_tiles_row_wise_mt(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_data_end: [*c]const u8) callconv(.c) [*c]const u8 {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const row_mt_worker_data: [*c]RowMTWorkerData = pbi.*.row_mt_worker_data;
    _ = &row_mt_worker_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_rows: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_rows);
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const num_workers: c_int = pbi.*.max_threads;
    _ = &num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var i: c_int = undefined;
    _ = &i;
    var n: c_int = undefined;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var col: c_int = undefined;
    _ = &col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var corrupted: c_int = 0;
    _ = &corrupted;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const sb_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows) >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const sb_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols) >> @intCast(@as(c_int, 6) - @as(c_int, 3));
    _ = &sb_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var lf_row_sync: [*c]VP9LfSync = &pbi.*.lf_row_sync;
    _ = &lf_row_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const new_fb: [*c]YV12_BUFFER_CONFIG = get_frame_new_buffer(cm);
    _ = &new_fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(row_mt_worker_data.*.recon_map)), 0, @as(c_ulong, @bitCast(@as(c_long, sb_rows * sb_cols))) *% @sizeOf(@TypeOf(row_mt_worker_data.*.recon_map.*)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    init_mt(pbi);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        n = 0;
        while (n < num_workers) : (n += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const thread_data: [*c]ThreadData = &pbi.*.row_mt_worker_data.*.thread_data[@bitCast(@as(isize, @intCast(n)))];
            _ = &thread_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            _ = winterface.*.sync.?(worker);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                thread_data.*.lf_sync = lf_row_sync;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                thread_data.*.lf_data = &thread_data.*.lf_sync.*.lfdata[@bitCast(@as(isize, @intCast(n)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                antfly_vp9_vp9_loop_filter_data_reset(thread_data.*.lf_data, new_fb, cm, @ptrCast(@alignCast(&pbi.*.mb.plane)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            thread_data.*.pbi = pbi;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.hook = row_decode_worker_hook;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.data1 = @ptrCast(@alignCast(thread_data));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.data2 = @ptrCast(@alignCast(&row_mt_worker_data.*.data_end));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        col = 0;
        while (col < tile_cols) : (col += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(col)))];
            _ = &tile_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            tile_data.*.xd = pbi.*.mb;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            tile_data.*.xd.counts = @ptrCast(@alignCast(if (cm.*.frame_parallel_decoding_mode != 0) @as(?*anyopaque, null) else @as(?*anyopaque, @ptrCast(@alignCast(&tile_data.*.counts)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_vp9_vp9_jobq_reset(&row_mt_worker_data.*.jobq);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    row_mt_worker_data.*.num_tiles_done = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    row_mt_worker_data.*.data_end = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    get_tile_buffers(pbi, data, data_end, tile_cols, tile_rows, &pbi.*.tile_buffers);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(cm.*.frame_parallel_decoding_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        {
            col = 0;
            while (col < tile_cols) : (col += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(col)))];
                _ = &tile_data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.counts)), 0, @sizeOf(@TypeOf(tile_data.*.counts)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        col = 0;
        while (col < tile_cols) : (col += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            var parse_job: Job = undefined;
            _ = &parse_job;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            parse_job.row_num = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            parse_job.tile_col = col;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            parse_job.job_type = PARSE_JOB;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            _ = antfly_vp9_vp9_jobq_queue(&row_mt_worker_data.*.jobq, @ptrCast(@alignCast(&parse_job)), @sizeOf(@TypeOf(parse_job)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        i = 0;
        while (i < num_workers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(i)))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.had_error = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (i == (num_workers - @as(c_int, 1))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                winterface.*.execute.?(worker);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                winterface.*.launch.?(worker);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    while (n > @as(c_int, 0)) : (n -= 1) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))];
        _ = &worker;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        corrupted |= @as(c_int, @intFromBool(!(winterface.*.sync.?(worker) != 0)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    pbi.*.mb.corrupted = corrupted;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(tile_cols - @as(c_int, 1))))];
        _ = &tile_data;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        row_mt_worker_data.*.data_end = antfly_vp9_vpx_reader_find_end(&tile_data.*.bit_reader);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(cm.*.frame_parallel_decoding_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        {
            i = 0;
            while (i < tile_cols) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(i)))];
                _ = &tile_data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                antfly_vp9_vp9_accumulate_frame_counts(&cm.*.counts, &tile_data.*.counts, 1);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return row_mt_worker_data.*.data_end;
}
pub fn decode_tiles_mt(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_data_end: [*c]const u8) callconv(.c) [*c]const u8 {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const winterface: [*c]const VPxWorkerInterface = antfly_vp9_vpx_get_worker_interface();
    _ = &winterface;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var bit_reader_end: [*c]const u8 = null;
    _ = &bit_reader_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var lf_row_sync: [*c]VP9LfSync = &pbi.*.lf_row_sync;
    _ = &lf_row_sync;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const new_fb: [*c]YV12_BUFFER_CONFIG = get_frame_new_buffer(cm);
    _ = &new_fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const tile_rows: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_rows);
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const num_workers: c_int = if (pbi.*.max_threads < tile_cols) pbi.*.max_threads else tile_cols;
    _ = &num_workers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var n: c_int = undefined;
    _ = &n;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    init_mt(pbi);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        n = 0;
        while (n < num_workers) : (n += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const tile_data: [*c]TileWorkerData = &pbi.*.tile_worker_data[@bitCast(@as(isize, @intCast(n + pbi.*.total_tiles)))];
            _ = &tile_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            _ = winterface.*.sync.?(worker);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (((pbi.*.lpf_mt_opt != 0) and (cm.*.lf.filter_level != 0)) and !(cm.*.skip_loop_filter != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.lf_sync = lf_row_sync;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.lf_data = &tile_data.*.lf_sync.*.lfdata[@bitCast(@as(isize, @intCast(n)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                antfly_vp9_vp9_loop_filter_data_reset(tile_data.*.lf_data, new_fb, cm, @ptrCast(@alignCast(&pbi.*.mb.plane)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.lf_data.*.y_only = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            tile_data.*.xd = pbi.*.mb;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            tile_data.*.xd.counts = @ptrCast(@alignCast(if (cm.*.frame_parallel_decoding_mode != 0) @as(?*anyopaque, null) else @as(?*anyopaque, @ptrCast(@alignCast(&tile_data.*.counts)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.hook = tile_worker_hook;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.data1 = @ptrCast(@alignCast(tile_data));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            worker.*.data2 = @ptrCast(@alignCast(pbi));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    get_tile_buffers(pbi, data, data_end, tile_cols, tile_rows, &pbi.*.tile_buffers);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    antfly_codec_sort(@ptrCast(@alignCast(@as([*c]TileBuffer, @ptrCast(@alignCast(&pbi.*.tile_buffers))))), @bitCast(@as(c_long, tile_cols)), @sizeOf(@TypeOf(pbi.*.tile_buffers[@as(c_int, 0)])), compare_tile_buffers);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (num_workers == tile_cols) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const largest: TileBuffer = pbi.*.tile_buffers[@as(c_int, 0)];
        _ = &largest;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = antfly_codec_afz_memmove(@ptrCast(@alignCast(@as([*c]TileBuffer, @ptrCast(@alignCast(&pbi.*.tile_buffers))))), @ptrCast(@alignCast(@as([*c]TileBuffer, @ptrCast(@alignCast(&pbi.*.tile_buffers))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))))), @as(c_ulong, @bitCast(@as(c_long, tile_cols - @as(c_int, 1)))) *% @sizeOf(@TypeOf(pbi.*.tile_buffers[@as(c_int, 0)])));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(tile_cols - @as(c_int, 1))))] = largest;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        var start: c_int = 0;
        _ = &start;
        var end: c_int = tile_cols - @as(c_int, 2);
        _ = &end;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        var tmp: TileBuffer = undefined;
        _ = &tmp;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        while (start < end) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            tmp = pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(start)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(start)))] = pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(end)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            pbi.*.tile_buffers[@bitCast(@as(isize, @intCast(end)))] = tmp;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            start += 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            end -= 2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(cm.*.frame_parallel_decoding_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        {
            n = 0;
            while (n < num_workers) : (n += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const tile_data: [*c]TileWorkerData = @ptrCast(@alignCast(pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))].data1));
                _ = &tile_data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&tile_data.*.counts)), 0, @sizeOf(@TypeOf(tile_data.*.counts)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const base: c_int = @divTrunc(tile_cols, num_workers);
        _ = &base;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const remain: c_int = __helpers.signedRemainder(tile_cols, num_workers);
        _ = &remain;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        var buf_start: c_int = 0;
        _ = &buf_start;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        {
            n = 0;
            while (n < num_workers) : (n += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const count: c_int = base + @divTrunc(remain + n, num_workers);
                _ = &count;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))];
                _ = &worker;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const tile_data: [*c]TileWorkerData = @ptrCast(@alignCast(worker.*.data1));
                _ = &tile_data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.buf_start = buf_start;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.buf_end = (buf_start + count) - @as(c_int, 1);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                tile_data.*.data_end = data_end;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                buf_start += count;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                worker.*.had_error = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                if (n == (num_workers - @as(c_int, 1))) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    _ = @as(c_int, 0);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    winterface.*.execute.?(worker);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                    winterface.*.launch.?(worker);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return null;
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        while (n > @as(c_int, 0)) : (n -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const worker: [*c]VPxWorker = &pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))];
            _ = &worker;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            const tile_data: [*c]TileWorkerData = @ptrCast(@alignCast(worker.*.data1));
            _ = &tile_data;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            pbi.*.mb.corrupted |= @as(c_int, @intFromBool(!(winterface.*.sync.?(worker) != 0)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (!(bit_reader_end != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                bit_reader_end = tile_data.*.data_end;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(cm.*.frame_parallel_decoding_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        {
            n = 0;
            while (n < num_workers) : (n += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                const tile_data: [*c]TileWorkerData = @ptrCast(@alignCast(pbi.*.tile_workers[@bitCast(@as(isize, @intCast(n)))].data1));
                _ = &tile_data;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
                antfly_vp9_vp9_accumulate_frame_counts(&cm.*.counts, &tile_data.*.counts, 1);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return null;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return bit_reader_end;
}
pub fn error_handler(arg_data: ?*anyopaque) callconv(.c) void {
    var data = arg_data;
    _ = &data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = @ptrCast(@alignCast(data));
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn read_bitdepth_colorspace_sampling(arg_cm: [*c]VP9_COMMON, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.profile >= @as(BITSTREAM_PROFILE, PROFILE_2)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.bit_depth = @bitCast(if (antfly_vp9_vpx_rb_read_bit(rb) != 0) VPX_BITS_12 else VPX_BITS_10);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.use_highbitdepth = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.bit_depth = VPX_BITS_8;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.use_highbitdepth = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.color_space = @bitCast(antfly_vp9_vpx_rb_read_literal(rb, 3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (cm.*.color_space != @as(vpx_color_space_t, VPX_CS_SRGB)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.color_range = @bitCast(antfly_vp9_vpx_rb_read_bit(rb));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((cm.*.profile == @as(BITSTREAM_PROFILE, PROFILE_1)) or (cm.*.profile == @as(BITSTREAM_PROFILE, PROFILE_3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.subsampling_x = antfly_vp9_vpx_rb_read_bit(rb);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.subsampling_y = antfly_vp9_vpx_rb_read_bit(rb);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if ((cm.*.subsampling_x == @as(c_int, 1)) and (cm.*.subsampling_y == @as(c_int, 1))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.subsampling_y = blk: {
                const tmp = @as(c_int, 1);
                cm.*.subsampling_x = tmp;
                break :blk tmp;
            };
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.color_range = VPX_CR_FULL_RANGE;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((cm.*.profile == @as(BITSTREAM_PROFILE, PROFILE_1)) or (cm.*.profile == @as(BITSTREAM_PROFILE, PROFILE_3))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            cm.*.subsampling_y = blk: {
                const tmp = @as(c_int, 0);
                cm.*.subsampling_x = tmp;
                break :blk tmp;
            };
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_vp9_vpx_rb_read_bit(rb) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
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
pub fn flush_all_fb_on_key(arg_cm: [*c]VP9_COMMON) callconv(.c) void {
    var cm = arg_cm;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((cm.*.frame_type == @as(FRAME_TYPE, KEY_FRAME)) and (cm.*.current_video_frame > @as(c_uint, 0))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&cm.*.buffer_pool.*.frame_bufs));
        _ = &frame_bufs;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const pool: [*c]BufferPool = cm.*.buffer_pool;
        _ = &pool;
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
            while (i < (((@as(c_int, 1) << @intCast(@as(c_int, 3))) + @as(c_int, 1)) + @as(c_int, 3))) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (i == cm.*.new_fb_idx) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    continue;
                }
                frame_bufs[@bitCast(@as(isize, @intCast(i)))].ref_count = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (!(@as(c_int, frame_bufs[@bitCast(@as(isize, @intCast(i)))].released) != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = pool.*.release_fb_cb.?(pool.*.cb_priv, &frame_bufs[@bitCast(@as(isize, @intCast(i)))].raw_frame_buffer);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    frame_bufs[@bitCast(@as(isize, @intCast(i)))].released = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
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
pub fn read_uncompressed_header(arg_pbi: [*c]VP9Decoder, arg_rb: [*c]struct_vpx_read_bit_buffer) callconv(.c) usize {
    var pbi = arg_pbi;
    _ = &pbi;
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const pool: [*c]BufferPool = cm.*.buffer_pool;
    _ = &pool;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const frame_bufs: [*c]RefCntBuffer = @ptrCast(@alignCast(&pool.*.frame_bufs));
    _ = &frame_bufs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    var mask: c_int = undefined;
    _ = &mask;
    var ref_index: c_int = 0;
    _ = &ref_index;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var sz: usize = undefined;
    _ = &sz;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.last_frame_type = cm.*.frame_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.last_intra_only = cm.*.intra_only;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_vp9_vpx_rb_read_literal(rb, 2) != @as(c_int, 2)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    cm.*.profile = antfly_vp9_vp9_read_profile(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.profile >= @as(BITSTREAM_PROFILE, MAX_PROFILES)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    cm.*.show_existing_frame = antfly_vp9_vpx_rb_read_bit(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.show_existing_frame != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const frame_to_show: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(antfly_vp9_vpx_rb_read_literal(rb, 3))))];
        _ = &frame_to_show;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((frame_to_show < @as(c_int, 0)) or (frame_bufs[@bitCast(@as(isize, @intCast(frame_to_show)))].ref_count < @as(c_int, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ref_cnt_fb(frame_bufs, &cm.*.new_fb_idx, frame_to_show);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pbi.*.refresh_frame_flags = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.lf.filter_level = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.show_frame = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.frame_type = @bitCast(antfly_vp9_vpx_rb_read_bit(rb));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.show_frame = antfly_vp9_vpx_rb_read_bit(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.error_resilient_mode = antfly_vp9_vpx_rb_read_bit(rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.frame_type == @as(FRAME_TYPE, KEY_FRAME)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(antfly_vp9_vp9_read_sync_code(rb) != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        read_bitdepth_colorspace_sampling(cm, rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        pbi.*.refresh_frame_flags = (@as(c_int, 1) << @intCast(@as(c_int, 1) << @intCast(@as(c_int, 3)))) - @as(c_int, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            i = 0;
            while (i < @as(c_int, 3)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))].idx = -@as(c_int, 1);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))].buf = null;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        setup_frame_size(cm, rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (pbi.*.need_resync != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.ref_frame_map)), -@as(c_int, 1), @sizeOf(@TypeOf(cm.*.ref_frame_map)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            flush_all_fb_on_key(cm);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pbi.*.need_resync = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.intra_only = @bitCast(@as(i8, @truncate(if (cm.*.show_frame != 0) @as(c_int, 0) else antfly_vp9_vpx_rb_read_bit(rb))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.reset_frame_context = if (cm.*.error_resilient_mode != 0) @as(c_int, 0) else antfly_vp9_vpx_rb_read_literal(rb, 2);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(c_int, cm.*.intra_only) != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(antfly_vp9_vp9_read_sync_code(rb) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_UNSUP_BITSTREAM);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (cm.*.profile > @as(BITSTREAM_PROFILE, PROFILE_0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                read_bitdepth_colorspace_sampling(cm, rb);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.color_space = VPX_CS_BT_601;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.color_range = VPX_CR_STUDIO_RANGE;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.subsampling_y = blk: {
                    const tmp = @as(c_int, 1);
                    cm.*.subsampling_x = tmp;
                    break :blk tmp;
                };
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.bit_depth = VPX_BITS_8;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.use_highbitdepth = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            pbi.*.refresh_frame_flags = antfly_vp9_vpx_rb_read_literal(rb, @as(c_int, 1) << @intCast(@as(c_int, 3)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            setup_frame_size(cm, rb);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (pbi.*.need_resync != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.ref_frame_map)), -@as(c_int, 1), @sizeOf(@TypeOf(cm.*.ref_frame_map)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pbi.*.need_resync = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (pbi.*.need_resync != @as(c_int, 1)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pbi.*.refresh_frame_flags = antfly_vp9_vpx_rb_read_literal(rb, @as(c_int, 1) << @intCast(@as(c_int, 3)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                {
                    i = 0;
                    while (i < @as(c_int, 3)) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const ref: c_int = antfly_vp9_vpx_rb_read_literal(rb, 3);
                        _ = &ref;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const idx: c_int = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref)))];
                        _ = &idx;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const ref_frame: [*c]RefBuffer = &cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))];
                        _ = &ref_frame;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        ref_frame.*.idx = idx;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        ref_frame.*.buf = &frame_bufs[@bitCast(@as(isize, @intCast(idx)))].buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        cm.*.ref_frame_sign_bias[@bitCast(@as(isize, @intCast(@as(c_int, 1) + i)))] = antfly_vp9_vpx_rb_read_bit(rb);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                setup_frame_size_with_refs(cm, rb);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.allow_high_precision_mv = antfly_vp9_vpx_rb_read_bit(rb);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.interp_filter = read_interp_filter(rb);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                {
                    i = 0;
                    while (i < @as(c_int, 3)) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        const ref_buf: [*c]RefBuffer = &cm.*.frame_refs[@bitCast(@as(isize, @intCast(i)))];
                        _ = &ref_buf;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        antfly_vp9_vp9_setup_scale_factors_for_frame(&ref_buf.*.sf, ref_buf.*.buf.*.y_crop_width, ref_buf.*.buf.*.y_crop_height, cm.*.width, cm.*.height, cm.*.use_highbitdepth);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    get_frame_new_buffer(cm).*.bit_depth = cm.*.bit_depth;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    get_frame_new_buffer(cm).*.color_space = cm.*.color_space;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    get_frame_new_buffer(cm).*.color_range = cm.*.color_range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    get_frame_new_buffer(cm).*.render_width = cm.*.render_width;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    get_frame_new_buffer(cm).*.render_height = cm.*.render_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (pbi.*.need_resync != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(cm.*.error_resilient_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.refresh_frame_context = antfly_vp9_vpx_rb_read_bit(rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.frame_parallel_decoding_mode = antfly_vp9_vpx_rb_read_bit(rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(cm.*.frame_parallel_decoding_mode != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&cm.*.counts)), 0, @sizeOf(@TypeOf(cm.*.counts)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.refresh_frame_context = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.frame_parallel_decoding_mode = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    cm.*.frame_context_idx = @bitCast(@as(c_int, antfly_vp9_vpx_rb_read_literal(rb, 2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        mask = pbi.*.refresh_frame_flags;
        while (mask != 0) : (mask >>= @intCast(@as(c_int, 1))) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if ((mask & @as(c_int, 1)) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.next_ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] = cm.*.new_fb_idx;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                frame_bufs[@bitCast(@as(isize, @intCast(cm.*.new_fb_idx)))].ref_count += 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                cm.*.next_ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] >= @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                frame_bufs[@bitCast(@as(isize, @intCast(cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))])))].ref_count += 1;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            ref_index += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    while (ref_index < (@as(c_int, 1) << @intCast(@as(c_int, 3)))) : (ref_index += 1) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        cm.*.next_ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] = cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))];
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))] >= @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            frame_bufs[@bitCast(@as(isize, @intCast(cm.*.ref_frame_map[@bitCast(@as(isize, @intCast(ref_index)))])))].ref_count += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    pbi.*.hold_ref_buf = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((frame_is_intra_only(cm) != 0) or (cm.*.error_resilient_mode != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vp9_setup_past_independence(cm);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    setup_loopfilter(&cm.*.lf, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    setup_quantization(cm, &pbi.*.mb, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    setup_segmentation(&cm.*.seg, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    setup_segmentation_dequant(cm);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    setup_tile_info(cm, rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (pbi.*.row_mt == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var num_sbs: c_int = 1;
        _ = &num_sbs;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const aligned_rows: c_int = mi_cols_aligned_to_sb(cm.*.mi_rows);
        _ = &aligned_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const sb_rows: c_int = aligned_rows >> @intCast(@as(c_int, 6) - @as(c_int, 3));
        _ = &sb_rows;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const num_jobs: c_int = sb_rows << @intCast(cm.*.log2_tile_cols);
        _ = &num_jobs;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(pbi.*.row_mt_worker_data))) == @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            while (true) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = @as(c_int, 0);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                pbi.*.row_mt_worker_data = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, @sizeOf(@TypeOf(pbi.*.row_mt_worker_data.*)))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(pbi.*.row_mt_worker_data != null)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                }
                if (!false) break;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (pbi.*.max_threads > @as(c_int, 1)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const aligned_cols: c_int = mi_cols_aligned_to_sb(cm.*.mi_cols);
            _ = &aligned_cols;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const sb_cols: c_int = aligned_cols >> @intCast(@as(c_int, 6) - @as(c_int, 3));
            _ = &sb_cols;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            num_sbs = sb_cols * sb_rows;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if ((num_sbs > pbi.*.row_mt_worker_data.*.num_sbs) or (num_jobs > pbi.*.row_mt_worker_data.*.num_jobs)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vp9_dec_free_row_mt_mem(pbi.*.row_mt_worker_data);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vp9_dec_alloc_row_mt_mem(pbi.*.row_mt_worker_data, cm, num_sbs, pbi.*.max_threads, num_jobs);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        vp9_jobq_alloc(pbi);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    sz = @bitCast(@as(c_long, antfly_vp9_vpx_rb_read_literal(rb, 16)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (sz == @as(usize, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return sz;
}
pub fn read_compressed_header(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_partition_size: usize) callconv(.c) c_int {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var partition_size = arg_partition_size;
    _ = &partition_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const xd: [*c]MACROBLOCKD = &pbi.*.mb;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const fc: [*c]FRAME_CONTEXT = cm.*.fc;
    _ = &fc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var r: vpx_reader = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var k: c_int = undefined;
    _ = &k;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (antfly_vp9_vpx_reader_init(&r, data, partition_size, pbi.*.decrypt_cb, pbi.*.decrypt_state) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    cm.*.tx_mode = @bitCast(if (xd.*.lossless != 0) ONLY_4X4 else @as(c_int, @bitCast(read_tx_mode(&r))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (cm.*.tx_mode == @as(TX_MODE, TX_MODE_SELECT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        read_tx_mode_probs(&fc.*.tx_probs, &r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    read_coef_probs(fc, cm.*.tx_mode, &r);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        k = 0;
        while (k < @as(c_int, 3)) : (k += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vp9_diff_update_prob(&r, &fc.*.skip_probs[@bitCast(@as(isize, @intCast(k)))]);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    if (!(frame_is_intra_only(cm) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const nmvc: [*c]nmv_context = &fc.*.nmvc;
        _ = &nmvc;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var i: c_int = undefined;
        _ = &i;
        var j: c_int = undefined;
        _ = &j;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        read_inter_mode_probs(fc, &r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(c_int, cm.*.interp_filter) == @as(c_int, 4)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            read_switchable_interp_probs(fc, &r);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        {
            i = 0;
            while (i < @as(c_int, 4)) : (i += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_vp9_vp9_diff_update_prob(&r, &fc.*.intra_inter_prob[@bitCast(@as(isize, @intCast(i)))]);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        cm.*.reference_mode = read_frame_reference_mode(cm, &r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (cm.*.reference_mode != @as(REFERENCE_MODE, SINGLE_REFERENCE)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vp9_setup_compound_reference_mode(cm);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        read_frame_reference_mode_probs(cm, &r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        {
            j = 0;
            while (j < @as(c_int, 4)) : (j += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                {
                    i = 0;
                    while (i < ((@as(c_int, 9) + @as(c_int, 1)) - @as(c_int, 1))) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        antfly_vp9_vp9_diff_update_prob(&r, &fc.*.y_mode_prob[@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(i)))]);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        {
            j = 0;
            while (j < (@as(c_int, 4) * @as(c_int, 4))) : (j += 1) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                {
                    i = 0;
                    while (i < (PARTITION_TYPES - @as(c_int, 1))) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        antfly_vp9_vp9_diff_update_prob(&r, &fc.*.partition_prob[@bitCast(@as(isize, @intCast(j)))][@bitCast(@as(isize, @intCast(i)))]);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return 0;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
        }
        read_mv_probs(nmvc, cm.*.allow_high_precision_mv, &r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return vpx_reader_has_error(&r);
}
pub fn init_read_bit_buffer(arg_pbi: [*c]VP9Decoder, arg_rb: [*c]struct_vpx_read_bit_buffer, arg_data: [*c]const u8, arg_data_end: [*c]const u8, arg_clear_data: [*c]u8) callconv(.c) [*c]struct_vpx_read_bit_buffer {
    var pbi = arg_pbi;
    _ = &pbi;
    var rb = arg_rb;
    _ = &rb;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    var clear_data = arg_clear_data;
    _ = &clear_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    rb.*.bit_offset = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    rb.*.error_handler = error_handler;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    rb.*.error_handler_data = @ptrCast(@alignCast(&pbi.*.common));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (pbi.*.decrypt_cb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        const n: c_int = @truncate(if (@as(c_long, @as(c_int, 80)) < @divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data))), @sizeOf(u8))) @as(c_long, @as(c_int, 80)) else @divExact(@as(c_long, @bitCast(@intFromPtr(data_end) -% @intFromPtr(data))), @sizeOf(u8)));
        _ = &n;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        pbi.*.decrypt_cb.?(pbi.*.decrypt_state, data, clear_data, n);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        rb.*.bit_buffer = clear_data;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        rb.*.bit_buffer_end = clear_data + @as(usize, @bitCast(@as(isize, @intCast(n))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        rb.*.bit_buffer = data;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        rb.*.bit_buffer_end = data_end;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return rb;
}
pub export fn antfly_vp9_vp9_read_sync_code(rb: [*c]struct_vpx_read_bit_buffer) c_int {
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(((antfly_vp9_vpx_rb_read_literal(rb, 8) == @as(c_int, 73)) and (antfly_vp9_vpx_rb_read_literal(rb, 8) == @as(c_int, 131))) and (antfly_vp9_vpx_rb_read_literal(rb, 8) == @as(c_int, 66))));
}
pub export fn antfly_vp9_vp9_read_frame_size(arg_rb: [*c]struct_vpx_read_bit_buffer, arg_width: [*c]c_int, arg_height: [*c]c_int) void {
    var rb = arg_rb;
    _ = &rb;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    width.* = antfly_vp9_vpx_rb_read_literal(rb, 16) + @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    height.* = antfly_vp9_vpx_rb_read_literal(rb, 16) + @as(c_int, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_read_profile(arg_rb: [*c]struct_vpx_read_bit_buffer) BITSTREAM_PROFILE {
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var profile: c_int = antfly_vp9_vpx_rb_read_bit(rb);
    _ = &profile;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    profile |= antfly_vp9_vpx_rb_read_bit(rb) << @intCast(@as(c_int, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (profile > @as(c_int, 2)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        profile += antfly_vp9_vpx_rb_read_bit(rb);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return @bitCast(profile);
}
pub export fn antfly_vp9_vp9_decode_frame(arg_pbi: [*c]VP9Decoder, arg_data: [*c]const u8, arg_data_end: [*c]const u8, arg_p_data_end: [*c][*c]const u8) void {
    var pbi = arg_pbi;
    _ = &pbi;
    var data = arg_data;
    _ = &data;
    var data_end = arg_data_end;
    _ = &data_end;
    var p_data_end = arg_p_data_end;
    _ = &p_data_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const cm: [*c]VP9_COMMON = &pbi.*.common;
    _ = &cm;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const xd: [*c]MACROBLOCKD = &pbi.*.mb;
    _ = &xd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var rb: struct_vpx_read_bit_buffer = undefined;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var context_updated: c_int = 0;
    _ = &context_updated;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var clear_data: [80]u8 = undefined;
    _ = &clear_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const first_partition_size: usize = read_uncompressed_header(pbi, init_read_bit_buffer(pbi, &rb, data, data_end, @ptrCast(@alignCast(&clear_data))));
    _ = &first_partition_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tile_rows: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_rows);
    _ = &tile_rows;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const tile_cols: c_int = @as(c_int, 1) << @intCast(cm.*.log2_tile_cols);
    _ = &tile_cols;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const new_fb: [*c]YV12_BUFFER_CONFIG = get_frame_new_buffer(cm);
    _ = &new_fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    xd.*.cur_buf = new_fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(first_partition_size != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        p_data_end.* = data + @as(usize, @bitCast(@as(isize, @intCast(if (cm.*.profile <= @as(BITSTREAM_PROFILE, PROFILE_2)) @as(c_int, 1) else @as(c_int, 2)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    data += antfly_vp9_vpx_rb_bytes_read(&rb);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(read_is_valid(data, first_partition_size, data_end) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    cm.*.use_prev_frame_mvs = @as(c_int, @intFromBool(((((!(cm.*.error_resilient_mode != 0) and (cm.*.width == cm.*.last_width)) and (cm.*.height == cm.*.last_height)) and !(@as(c_int, cm.*.last_intra_only) != 0)) and (cm.*.last_show_frame != 0)) and (cm.*.last_frame_type != @as(FRAME_TYPE, KEY_FRAME))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_setup_block_planes(xd, cm.*.subsampling_x, cm.*.subsampling_y);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    cm.*.fc.* = cm.*.frame_contexts[cm.*.frame_context_idx];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(cm.*.fc.*.initialized != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    xd.*.corrupted = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    new_fb.*.corrupted = read_compressed_header(pbi, data, first_partition_size);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (new_fb.*.corrupted != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if ((cm.*.lf.filter_level != 0) and !(cm.*.skip_loop_filter != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_loop_filter_frame_init(cm, cm.*.lf.filter_level);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(?*anyopaque, @ptrCast(@alignCast(pbi.*.tile_worker_data))) == @as(?*anyopaque, null)) or ((tile_cols * tile_rows) != pbi.*.total_tiles)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const num_tile_workers: c_int = (tile_cols * tile_rows) + (if (pbi.*.max_threads > @as(c_int, 1)) pbi.*.max_threads else @as(c_int, 0));
        _ = &num_tile_workers;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const twd_size: usize = @as(c_ulong, @bitCast(@as(c_long, num_tile_workers))) *% @sizeOf(@TypeOf(pbi.*.tile_worker_data.*));
        _ = &twd_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(pbi.*.tile_worker_data)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        while (true) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            pbi.*.tile_worker_data = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, twd_size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(pbi.*.tile_worker_data != null)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_MEM_ERROR);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (!false) break;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pbi.*.total_tiles = tile_rows * tile_cols;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((pbi.*.max_threads > @as(c_int, 1)) and (tile_rows == @as(c_int, 1))) and ((tile_cols > @as(c_int, 1)) or (pbi.*.row_mt == @as(c_int, 1)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (pbi.*.row_mt == @as(c_int, 1)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            p_data_end.* = decode_tiles_row_wise_mt(pbi, data + first_partition_size, data_end);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            p_data_end.* = decode_tiles_mt(pbi, data + first_partition_size, data_end);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(pbi.*.lpf_mt_opt != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (!(xd.*.corrupted != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (!(cm.*.skip_loop_filter != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        antfly_vp9_vp9_loop_filter_frame_mt(new_fb, cm, @ptrCast(@alignCast(&pbi.*.mb.plane)), cm.*.lf.filter_level, 0, 0, pbi.*.tile_workers, pbi.*.num_tile_workers, &pbi.*.lf_row_sync);
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
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
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        p_data_end.* = decode_tiles(pbi, data + first_partition_size, data_end);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(xd.*.corrupted != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(cm.*.error_resilient_mode != 0) and !(cm.*.frame_parallel_decoding_mode != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vp9_adapt_coef_probs(cm);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (!(frame_is_intra_only(cm) != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_adapt_mode_probs(cm);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vp9_adapt_mv_probs(cm, cm.*.allow_high_precision_mv);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_vp9_fail(@ptrCast(@alignCast(&cm.*.@"error")), VPX_CODEC_CORRUPT_FRAME);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((cm.*.refresh_frame_context != 0) and !(context_updated != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        cm.*.frame_contexts[cm.*.frame_context_idx] = cm.*.fc.*;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
pub const frame_contexts = struct_frame_contexts;
