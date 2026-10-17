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
//  * Copyright © 2018-2019, VideoLAN and dav1d authors
//  * Copyright © 2018-2019, Two Orioles, LLC
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
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
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
pub const pixel = u8;
pub const coef = i16;
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
pub const DCT_DCT = common.DCT_DCT;
pub const ADST_DCT = common.ADST_DCT;
pub const DCT_ADST = common.DCT_ADST;
pub const ADST_ADST = common.ADST_ADST;
pub const FLIPADST_DCT = common.FLIPADST_DCT;
pub const DCT_FLIPADST = common.DCT_FLIPADST;
pub const FLIPADST_FLIPADST = common.FLIPADST_FLIPADST;
pub const ADST_FLIPADST = common.ADST_FLIPADST;
pub const FLIPADST_ADST = common.FLIPADST_ADST;
pub const IDTX = common.IDTX;
pub const V_DCT = common.V_DCT;
pub const H_DCT = common.H_DCT;
pub const V_ADST = common.V_ADST;
pub const H_ADST = common.H_ADST;
pub const V_FLIPADST = common.V_FLIPADST;
pub const H_FLIPADST = common.H_FLIPADST;
pub const WHT_WHT = common.WHT_WHT;
pub const enum_TxfmType = common.enum_TxfmType;
pub const itxfm_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void;
pub const struct_Dav1dInvTxfmDSPContext = extern struct {
    itxfm_add: [19][17]itxfm_fn = @import("std").mem.zeroes([19][17]itxfm_fn),
};
pub const Dav1dInvTxfmDSPContext = struct_Dav1dInvTxfmDSPContext;
pub const IDENTITY = common.IDENTITY;
pub const itx_1d_fn = ?*const fn (c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void;
pub extern var antfly_av1_dav1d_tx1d_fns: [5][4]itx_1d_fn;
pub extern var antfly_av1_dav1d_tx1d_types: [16][2]u8;
pub extern fn antfly_av1_dav1d_inv_wht4_1d_c(c: [*c]i32, stride: ptrdiff_t) void;
pub extern const antfly_av1_dav1d_last_nonzero_col_from_eob: [19][*c]const u8;
pub extern fn antfly_av1_dav1d_init_last_nonzero_col_from_eob_tables() void;
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
pub fn inv_txfm_add_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int, tx: c_int, shift: c_int, txtp: enum_TxfmType) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    _ = &tx;
    _ = &shift;
    _ = &txtp;
    const t_dim: [*c]const TxfmInfo = &antfly_av1_dav1d_txfm_dimensions[@bitCast(@as(isize, @intCast(tx)))];
    _ = &t_dim;
    const w: c_int = @as(c_int, 4) * @as(c_int, t_dim.*.w);
    _ = &w;
    const h: c_int = @as(c_int, 4) * @as(c_int, t_dim.*.h);
    _ = &h;
    const has_dconly: c_int = @as(c_int, @intFromBool(txtp == @as(enum_TxfmType, DCT_DCT)));
    _ = &has_dconly;
    while (true) {
        if (!((w >= @as(c_int, 4)) and (w <= @as(c_int, 64)))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!((h >= @as(c_int, 4)) and (h <= @as(c_int, 64)))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(eob >= @as(c_int, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const is_rect2: c_int = @as(c_int, @intFromBool(((w * @as(c_int, 2)) == h) or ((h * @as(c_int, 2)) == w)));
    _ = &is_rect2;
    const rnd: c_int = (@as(c_int, 1) << @intCast(shift)) >> @intCast(@as(c_int, 1));
    _ = &rnd;
    if (eob < has_dconly) {
        var dc: c_int = coeff[@as(c_int, 0)];
        _ = &dc;
        coeff[@as(c_int, 0)] = 0;
        if (is_rect2 != 0) {
            dc = ((dc * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
        }
        dc = ((dc * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
        dc = (dc + rnd) >> @intCast(shift);
        dc = (((dc * @as(c_int, 181)) + @as(c_int, 128)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        {
            var y: c_int = 0;
            _ = &y;
            while (y < h) : ({
                y += 1;
                dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            }) {
                var x: c_int = 0;
                _ = &x;
                while (x < w) : (x += 1) {
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(iclip_u8(@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + dc))));
                }
            }
        }
        return;
    }
    const txtps: [*c]const u8 = @ptrCast(@alignCast(&antfly_av1_dav1d_tx1d_types[txtp]));
    _ = &txtps;
    const first_1d_fn: itx_1d_fn = antfly_av1_dav1d_tx1d_fns[t_dim.*.lw][txtps[@as(c_int, 0)]];
    _ = &first_1d_fn;
    const second_1d_fn: itx_1d_fn = antfly_av1_dav1d_tx1d_fns[t_dim.*.lh][txtps[@as(c_int, 1)]];
    _ = &second_1d_fn;
    const sh: c_int = imin(h, 32);
    _ = &sh;
    const sw: c_int = imin(w, 32);
    _ = &sw;
    const row_clip_min: c_int = -@as(c_int, 32768);
    _ = &row_clip_min;
    const col_clip_min: c_int = -@as(c_int, 32768);
    _ = &col_clip_min;
    const row_clip_max: c_int = ~row_clip_min;
    _ = &row_clip_max;
    const col_clip_max: c_int = ~col_clip_min;
    _ = &col_clip_max;
    var tmp: [4096]i32 = undefined;
    _ = &tmp;
    var c: [*c]i32 = @ptrCast(@alignCast(&tmp));
    _ = &c;
    var last_nonzero_col: c_int = undefined;
    _ = &last_nonzero_col;
    if ((@as(c_int, txtps[@as(c_int, 1)]) == IDENTITY) and (@as(c_int, txtps[@as(c_int, 0)]) != IDENTITY)) {
        last_nonzero_col = imin(sh - @as(c_int, 1), eob);
    } else if ((@as(c_int, txtps[@as(c_int, 0)]) == IDENTITY) and (@as(c_int, txtps[@as(c_int, 1)]) != IDENTITY)) {
        last_nonzero_col = eob >> @intCast(@as(c_int, t_dim.*.lw) + @as(c_int, 2));
    } else {
        last_nonzero_col = antfly_av1_dav1d_last_nonzero_col_from_eob[@bitCast(@as(isize, @intCast(tx)))][@bitCast(@as(isize, @intCast(eob)))];
    }
    while (true) {
        if (!(last_nonzero_col < sh)) {
            unreachable;
        }
        if (!false) break;
    }
    {
        var y: c_int = 0;
        _ = &y;
        while (y <= last_nonzero_col) : ({
            y += 1;
            c += @as(usize, @bitCast(@as(isize, @intCast(w))));
        }) {
            if (is_rect2 != 0) {
                {
                    var x: c_int = 0;
                    _ = &x;
                    while (x < sw) : (x += 1) {
                        c[@bitCast(@as(isize, @intCast(x)))] = ((@as(c_int, coeff[@bitCast(@as(isize, @intCast(y + (x * sh))))]) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
                    }
                }
            } else {
                var x: c_int = 0;
                _ = &x;
                while (x < sw) : (x += 1) {
                    c[@bitCast(@as(isize, @intCast(x)))] = coeff[@bitCast(@as(isize, @intCast(y + (x * sh))))];
                }
            }
            first_1d_fn.?(c, 1, row_clip_min, row_clip_max);
        }
    }
    if ((last_nonzero_col + @as(c_int, 1)) < sh) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(c)), 0, (@sizeOf(@TypeOf(c.*)) *% @as(c_ulong, @bitCast(@as(c_long, (sh - last_nonzero_col) - @as(c_int, 1))))) *% @as(c_ulong, @bitCast(@as(c_long, w))));
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(coeff)), 0, (@sizeOf(@TypeOf(coeff.*)) *% @as(c_ulong, @bitCast(@as(c_long, sw)))) *% @as(c_ulong, @bitCast(@as(c_long, sh))));
    {
        var i: c_int = 0;
        _ = &i;
        while (i < (w * sh)) : (i += 1) {
            tmp[@bitCast(@as(isize, @intCast(i)))] = iclip((tmp[@bitCast(@as(isize, @intCast(i)))] + rnd) >> @intCast(shift), col_clip_min, col_clip_max);
        }
    }
    {
        var x: c_int = 0;
        _ = &x;
        while (x < w) : (x += 1) {
            second_1d_fn.?(&tmp[@bitCast(@as(isize, @intCast(x)))], w, col_clip_min, col_clip_max);
        }
    }
    c = @ptrCast(@alignCast(&tmp));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : ({
            y += 1;
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        }) {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(iclip_u8(@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + (((blk: {
                    const ref = &c;
                    const tmp_1 = ref.*;
                    ref.* += 1;
                    break :blk tmp_1;
                }).* + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))))));
            }
        }
    }
}
pub fn inv_txfm_add_dct_dct_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, IDTX);
}
pub fn inv_txfm_add_adst_dct_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, H_DCT);
}
pub fn inv_txfm_add_dct_identity_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, H_ADST);
}
pub fn inv_txfm_add_adst_identity_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_4X4, 0, V_ADST);
}
pub fn inv_txfm_add_dct_dct_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, IDTX);
}
pub fn inv_txfm_add_adst_dct_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, H_DCT);
}
pub fn inv_txfm_add_dct_identity_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, H_ADST);
}
pub fn inv_txfm_add_adst_identity_4x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X8, 0, V_ADST);
}
pub fn inv_txfm_add_dct_dct_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, IDTX);
}
pub fn inv_txfm_add_adst_dct_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, H_DCT);
}
pub fn inv_txfm_add_dct_identity_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, H_ADST);
}
pub fn inv_txfm_add_adst_identity_4x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_4X16, 1, V_ADST);
}
pub fn inv_txfm_add_dct_dct_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, IDTX);
}
pub fn inv_txfm_add_adst_dct_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, H_DCT);
}
pub fn inv_txfm_add_dct_identity_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, H_ADST);
}
pub fn inv_txfm_add_adst_identity_8x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X4, 0, V_ADST);
}
pub fn inv_txfm_add_dct_dct_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, IDTX);
}
pub fn inv_txfm_add_adst_dct_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, H_DCT);
}
pub fn inv_txfm_add_dct_identity_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, H_ADST);
}
pub fn inv_txfm_add_adst_identity_8x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_8X8, 1, V_ADST);
}
pub fn inv_txfm_add_dct_dct_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, IDTX);
}
pub fn inv_txfm_add_adst_dct_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, H_DCT);
}
pub fn inv_txfm_add_dct_identity_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, H_ADST);
}
pub fn inv_txfm_add_adst_identity_8x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X16, 1, V_ADST);
}
pub fn inv_txfm_add_dct_dct_8x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X32, 2, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_8x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_8X32, 2, IDTX);
}
pub fn inv_txfm_add_dct_dct_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, IDTX);
}
pub fn inv_txfm_add_adst_dct_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, H_DCT);
}
pub fn inv_txfm_add_dct_identity_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, H_ADST);
}
pub fn inv_txfm_add_adst_identity_16x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X4, 1, V_ADST);
}
pub fn inv_txfm_add_dct_dct_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, IDTX);
}
pub fn inv_txfm_add_adst_dct_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, H_DCT);
}
pub fn inv_txfm_add_dct_identity_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, V_DCT);
}
pub fn inv_txfm_add_identity_flipadst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, H_FLIPADST);
}
pub fn inv_txfm_add_flipadst_identity_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, V_FLIPADST);
}
pub fn inv_txfm_add_identity_adst_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, H_ADST);
}
pub fn inv_txfm_add_adst_identity_16x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X8, 1, V_ADST);
}
pub fn inv_txfm_add_dct_dct_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, IDTX);
}
pub fn inv_txfm_add_adst_dct_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, ADST_DCT);
}
pub fn inv_txfm_add_dct_adst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, DCT_ADST);
}
pub fn inv_txfm_add_adst_adst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, ADST_ADST);
}
pub fn inv_txfm_add_dct_flipadst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, DCT_FLIPADST);
}
pub fn inv_txfm_add_flipadst_dct_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, FLIPADST_DCT);
}
pub fn inv_txfm_add_adst_flipadst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, ADST_FLIPADST);
}
pub fn inv_txfm_add_flipadst_adst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, FLIPADST_ADST);
}
pub fn inv_txfm_add_flipadst_flipadst_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, FLIPADST_FLIPADST);
}
pub fn inv_txfm_add_identity_dct_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, H_DCT);
}
pub fn inv_txfm_add_dct_identity_16x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_16X16, 2, V_DCT);
}
pub fn inv_txfm_add_dct_dct_16x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X32, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_16x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X32, 1, IDTX);
}
pub fn inv_txfm_add_dct_dct_16x64_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_16X64, 2, DCT_DCT);
}
pub fn inv_txfm_add_dct_dct_32x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_32X8, 2, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_32x8_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_32X8, 2, IDTX);
}
pub fn inv_txfm_add_dct_dct_32x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_32X16, 1, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_32x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_32X16, 1, IDTX);
}
pub fn inv_txfm_add_dct_dct_32x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_32X32, 2, DCT_DCT);
}
pub fn inv_txfm_add_identity_identity_32x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_32X32, 2, IDTX);
}
pub fn inv_txfm_add_dct_dct_32x64_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_32X64, 1, DCT_DCT);
}
pub fn inv_txfm_add_dct_dct_64x16_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_64X16, 2, DCT_DCT);
}
pub fn inv_txfm_add_dct_dct_64x32_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, RTX_64X32, 1, DCT_DCT);
}
pub fn inv_txfm_add_dct_dct_64x64_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    inv_txfm_add_c(dst, stride, coeff, eob, TX_64X64, 2, DCT_DCT);
}
pub fn inv_txfm_add_wht_wht_4x4_c(arg_dst: [*c]pixel, stride: ptrdiff_t, coeff: [*c]coef, eob: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &coeff;
    _ = &eob;
    var tmp: [16]i32 = undefined;
    _ = &tmp;
    var c: [*c]i32 = @ptrCast(@alignCast(&tmp));
    _ = &c;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 4)) : ({
            y += 1;
            c += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < @as(c_int, 4)) : (x += 1) {
                    c[@bitCast(@as(isize, @intCast(x)))] = @as(c_int, coeff[@bitCast(@as(isize, @intCast(y + (x * @as(c_int, 4)))))]) >> @intCast(@as(c_int, 2));
                }
            }
            antfly_av1_dav1d_inv_wht4_1d_c(c, 1);
        }
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(coeff)), 0, (@sizeOf(@TypeOf(coeff.*)) *% @as(c_ulong, 4)) *% @as(c_ulong, 4));
    {
        var x: c_int = 0;
        _ = &x;
        while (x < @as(c_int, 4)) : (x += 1) {
            antfly_av1_dav1d_inv_wht4_1d_c(&tmp[@bitCast(@as(isize, @intCast(x)))], 4);
        }
    }
    c = @ptrCast(@alignCast(&tmp));
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 4)) : ({
            y += 1;
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
        }) {
            var x: c_int = 0;
            _ = &x;
            while (x < @as(c_int, 4)) : (x += 1) {
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(iclip_u8(@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + (blk: {
                    const ref = &c;
                    const tmp_1 = ref.*;
                    ref.* += 1;
                    break :blk tmp_1;
                }).*))));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_itx_dsp_init_8bpc(c: [*c]Dav1dInvTxfmDSPContext, arg_bpc: c_int) void {
    _ = &c;
    var bpc = arg_bpc;
    _ = &bpc;
    c.*.itxfm_add[TX_4X4][WHT_WHT] = inv_txfm_add_wht_wht_4x4_c;
    c.*.itxfm_add[TX_4X4][DCT_DCT] = inv_txfm_add_dct_dct_4x4_c;
    c.*.itxfm_add[TX_4X4][IDTX] = inv_txfm_add_identity_identity_4x4_c;
    c.*.itxfm_add[TX_4X4][DCT_ADST] = inv_txfm_add_adst_dct_4x4_c;
    c.*.itxfm_add[TX_4X4][ADST_DCT] = inv_txfm_add_dct_adst_4x4_c;
    c.*.itxfm_add[TX_4X4][ADST_ADST] = inv_txfm_add_adst_adst_4x4_c;
    c.*.itxfm_add[TX_4X4][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_4x4_c;
    c.*.itxfm_add[TX_4X4][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_4x4_c;
    c.*.itxfm_add[TX_4X4][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_4x4_c;
    c.*.itxfm_add[TX_4X4][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_4x4_c;
    c.*.itxfm_add[TX_4X4][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_4x4_c;
    c.*.itxfm_add[TX_4X4][H_DCT] = inv_txfm_add_dct_identity_4x4_c;
    c.*.itxfm_add[TX_4X4][V_DCT] = inv_txfm_add_identity_dct_4x4_c;
    c.*.itxfm_add[TX_4X4][H_FLIPADST] = inv_txfm_add_flipadst_identity_4x4_c;
    c.*.itxfm_add[TX_4X4][V_FLIPADST] = inv_txfm_add_identity_flipadst_4x4_c;
    c.*.itxfm_add[TX_4X4][H_ADST] = inv_txfm_add_adst_identity_4x4_c;
    c.*.itxfm_add[TX_4X4][V_ADST] = inv_txfm_add_identity_adst_4x4_c;
    c.*.itxfm_add[RTX_4X8][DCT_DCT] = inv_txfm_add_dct_dct_4x8_c;
    c.*.itxfm_add[RTX_4X8][IDTX] = inv_txfm_add_identity_identity_4x8_c;
    c.*.itxfm_add[RTX_4X8][DCT_ADST] = inv_txfm_add_adst_dct_4x8_c;
    c.*.itxfm_add[RTX_4X8][ADST_DCT] = inv_txfm_add_dct_adst_4x8_c;
    c.*.itxfm_add[RTX_4X8][ADST_ADST] = inv_txfm_add_adst_adst_4x8_c;
    c.*.itxfm_add[RTX_4X8][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_4x8_c;
    c.*.itxfm_add[RTX_4X8][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_4x8_c;
    c.*.itxfm_add[RTX_4X8][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_4x8_c;
    c.*.itxfm_add[RTX_4X8][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_4x8_c;
    c.*.itxfm_add[RTX_4X8][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_4x8_c;
    c.*.itxfm_add[RTX_4X8][H_DCT] = inv_txfm_add_dct_identity_4x8_c;
    c.*.itxfm_add[RTX_4X8][V_DCT] = inv_txfm_add_identity_dct_4x8_c;
    c.*.itxfm_add[RTX_4X8][H_FLIPADST] = inv_txfm_add_flipadst_identity_4x8_c;
    c.*.itxfm_add[RTX_4X8][V_FLIPADST] = inv_txfm_add_identity_flipadst_4x8_c;
    c.*.itxfm_add[RTX_4X8][H_ADST] = inv_txfm_add_adst_identity_4x8_c;
    c.*.itxfm_add[RTX_4X8][V_ADST] = inv_txfm_add_identity_adst_4x8_c;
    c.*.itxfm_add[RTX_4X16][DCT_DCT] = inv_txfm_add_dct_dct_4x16_c;
    c.*.itxfm_add[RTX_4X16][IDTX] = inv_txfm_add_identity_identity_4x16_c;
    c.*.itxfm_add[RTX_4X16][DCT_ADST] = inv_txfm_add_adst_dct_4x16_c;
    c.*.itxfm_add[RTX_4X16][ADST_DCT] = inv_txfm_add_dct_adst_4x16_c;
    c.*.itxfm_add[RTX_4X16][ADST_ADST] = inv_txfm_add_adst_adst_4x16_c;
    c.*.itxfm_add[RTX_4X16][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_4x16_c;
    c.*.itxfm_add[RTX_4X16][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_4x16_c;
    c.*.itxfm_add[RTX_4X16][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_4x16_c;
    c.*.itxfm_add[RTX_4X16][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_4x16_c;
    c.*.itxfm_add[RTX_4X16][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_4x16_c;
    c.*.itxfm_add[RTX_4X16][H_DCT] = inv_txfm_add_dct_identity_4x16_c;
    c.*.itxfm_add[RTX_4X16][V_DCT] = inv_txfm_add_identity_dct_4x16_c;
    c.*.itxfm_add[RTX_4X16][H_FLIPADST] = inv_txfm_add_flipadst_identity_4x16_c;
    c.*.itxfm_add[RTX_4X16][V_FLIPADST] = inv_txfm_add_identity_flipadst_4x16_c;
    c.*.itxfm_add[RTX_4X16][H_ADST] = inv_txfm_add_adst_identity_4x16_c;
    c.*.itxfm_add[RTX_4X16][V_ADST] = inv_txfm_add_identity_adst_4x16_c;
    c.*.itxfm_add[RTX_8X4][DCT_DCT] = inv_txfm_add_dct_dct_8x4_c;
    c.*.itxfm_add[RTX_8X4][IDTX] = inv_txfm_add_identity_identity_8x4_c;
    c.*.itxfm_add[RTX_8X4][DCT_ADST] = inv_txfm_add_adst_dct_8x4_c;
    c.*.itxfm_add[RTX_8X4][ADST_DCT] = inv_txfm_add_dct_adst_8x4_c;
    c.*.itxfm_add[RTX_8X4][ADST_ADST] = inv_txfm_add_adst_adst_8x4_c;
    c.*.itxfm_add[RTX_8X4][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_8x4_c;
    c.*.itxfm_add[RTX_8X4][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_8x4_c;
    c.*.itxfm_add[RTX_8X4][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_8x4_c;
    c.*.itxfm_add[RTX_8X4][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_8x4_c;
    c.*.itxfm_add[RTX_8X4][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_8x4_c;
    c.*.itxfm_add[RTX_8X4][H_DCT] = inv_txfm_add_dct_identity_8x4_c;
    c.*.itxfm_add[RTX_8X4][V_DCT] = inv_txfm_add_identity_dct_8x4_c;
    c.*.itxfm_add[RTX_8X4][H_FLIPADST] = inv_txfm_add_flipadst_identity_8x4_c;
    c.*.itxfm_add[RTX_8X4][V_FLIPADST] = inv_txfm_add_identity_flipadst_8x4_c;
    c.*.itxfm_add[RTX_8X4][H_ADST] = inv_txfm_add_adst_identity_8x4_c;
    c.*.itxfm_add[RTX_8X4][V_ADST] = inv_txfm_add_identity_adst_8x4_c;
    c.*.itxfm_add[TX_8X8][DCT_DCT] = inv_txfm_add_dct_dct_8x8_c;
    c.*.itxfm_add[TX_8X8][IDTX] = inv_txfm_add_identity_identity_8x8_c;
    c.*.itxfm_add[TX_8X8][DCT_ADST] = inv_txfm_add_adst_dct_8x8_c;
    c.*.itxfm_add[TX_8X8][ADST_DCT] = inv_txfm_add_dct_adst_8x8_c;
    c.*.itxfm_add[TX_8X8][ADST_ADST] = inv_txfm_add_adst_adst_8x8_c;
    c.*.itxfm_add[TX_8X8][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_8x8_c;
    c.*.itxfm_add[TX_8X8][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_8x8_c;
    c.*.itxfm_add[TX_8X8][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_8x8_c;
    c.*.itxfm_add[TX_8X8][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_8x8_c;
    c.*.itxfm_add[TX_8X8][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_8x8_c;
    c.*.itxfm_add[TX_8X8][H_DCT] = inv_txfm_add_dct_identity_8x8_c;
    c.*.itxfm_add[TX_8X8][V_DCT] = inv_txfm_add_identity_dct_8x8_c;
    c.*.itxfm_add[TX_8X8][H_FLIPADST] = inv_txfm_add_flipadst_identity_8x8_c;
    c.*.itxfm_add[TX_8X8][V_FLIPADST] = inv_txfm_add_identity_flipadst_8x8_c;
    c.*.itxfm_add[TX_8X8][H_ADST] = inv_txfm_add_adst_identity_8x8_c;
    c.*.itxfm_add[TX_8X8][V_ADST] = inv_txfm_add_identity_adst_8x8_c;
    c.*.itxfm_add[RTX_8X16][DCT_DCT] = inv_txfm_add_dct_dct_8x16_c;
    c.*.itxfm_add[RTX_8X16][IDTX] = inv_txfm_add_identity_identity_8x16_c;
    c.*.itxfm_add[RTX_8X16][DCT_ADST] = inv_txfm_add_adst_dct_8x16_c;
    c.*.itxfm_add[RTX_8X16][ADST_DCT] = inv_txfm_add_dct_adst_8x16_c;
    c.*.itxfm_add[RTX_8X16][ADST_ADST] = inv_txfm_add_adst_adst_8x16_c;
    c.*.itxfm_add[RTX_8X16][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_8x16_c;
    c.*.itxfm_add[RTX_8X16][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_8x16_c;
    c.*.itxfm_add[RTX_8X16][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_8x16_c;
    c.*.itxfm_add[RTX_8X16][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_8x16_c;
    c.*.itxfm_add[RTX_8X16][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_8x16_c;
    c.*.itxfm_add[RTX_8X16][H_DCT] = inv_txfm_add_dct_identity_8x16_c;
    c.*.itxfm_add[RTX_8X16][V_DCT] = inv_txfm_add_identity_dct_8x16_c;
    c.*.itxfm_add[RTX_8X16][H_FLIPADST] = inv_txfm_add_flipadst_identity_8x16_c;
    c.*.itxfm_add[RTX_8X16][V_FLIPADST] = inv_txfm_add_identity_flipadst_8x16_c;
    c.*.itxfm_add[RTX_8X16][H_ADST] = inv_txfm_add_adst_identity_8x16_c;
    c.*.itxfm_add[RTX_8X16][V_ADST] = inv_txfm_add_identity_adst_8x16_c;
    c.*.itxfm_add[RTX_8X32][DCT_DCT] = inv_txfm_add_dct_dct_8x32_c;
    c.*.itxfm_add[RTX_8X32][IDTX] = inv_txfm_add_identity_identity_8x32_c;
    c.*.itxfm_add[RTX_16X4][DCT_DCT] = inv_txfm_add_dct_dct_16x4_c;
    c.*.itxfm_add[RTX_16X4][IDTX] = inv_txfm_add_identity_identity_16x4_c;
    c.*.itxfm_add[RTX_16X4][DCT_ADST] = inv_txfm_add_adst_dct_16x4_c;
    c.*.itxfm_add[RTX_16X4][ADST_DCT] = inv_txfm_add_dct_adst_16x4_c;
    c.*.itxfm_add[RTX_16X4][ADST_ADST] = inv_txfm_add_adst_adst_16x4_c;
    c.*.itxfm_add[RTX_16X4][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_16x4_c;
    c.*.itxfm_add[RTX_16X4][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_16x4_c;
    c.*.itxfm_add[RTX_16X4][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_16x4_c;
    c.*.itxfm_add[RTX_16X4][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_16x4_c;
    c.*.itxfm_add[RTX_16X4][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_16x4_c;
    c.*.itxfm_add[RTX_16X4][H_DCT] = inv_txfm_add_dct_identity_16x4_c;
    c.*.itxfm_add[RTX_16X4][V_DCT] = inv_txfm_add_identity_dct_16x4_c;
    c.*.itxfm_add[RTX_16X4][H_FLIPADST] = inv_txfm_add_flipadst_identity_16x4_c;
    c.*.itxfm_add[RTX_16X4][V_FLIPADST] = inv_txfm_add_identity_flipadst_16x4_c;
    c.*.itxfm_add[RTX_16X4][H_ADST] = inv_txfm_add_adst_identity_16x4_c;
    c.*.itxfm_add[RTX_16X4][V_ADST] = inv_txfm_add_identity_adst_16x4_c;
    c.*.itxfm_add[RTX_16X8][DCT_DCT] = inv_txfm_add_dct_dct_16x8_c;
    c.*.itxfm_add[RTX_16X8][IDTX] = inv_txfm_add_identity_identity_16x8_c;
    c.*.itxfm_add[RTX_16X8][DCT_ADST] = inv_txfm_add_adst_dct_16x8_c;
    c.*.itxfm_add[RTX_16X8][ADST_DCT] = inv_txfm_add_dct_adst_16x8_c;
    c.*.itxfm_add[RTX_16X8][ADST_ADST] = inv_txfm_add_adst_adst_16x8_c;
    c.*.itxfm_add[RTX_16X8][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_16x8_c;
    c.*.itxfm_add[RTX_16X8][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_16x8_c;
    c.*.itxfm_add[RTX_16X8][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_16x8_c;
    c.*.itxfm_add[RTX_16X8][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_16x8_c;
    c.*.itxfm_add[RTX_16X8][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_16x8_c;
    c.*.itxfm_add[RTX_16X8][H_DCT] = inv_txfm_add_dct_identity_16x8_c;
    c.*.itxfm_add[RTX_16X8][V_DCT] = inv_txfm_add_identity_dct_16x8_c;
    c.*.itxfm_add[RTX_16X8][H_FLIPADST] = inv_txfm_add_flipadst_identity_16x8_c;
    c.*.itxfm_add[RTX_16X8][V_FLIPADST] = inv_txfm_add_identity_flipadst_16x8_c;
    c.*.itxfm_add[RTX_16X8][H_ADST] = inv_txfm_add_adst_identity_16x8_c;
    c.*.itxfm_add[RTX_16X8][V_ADST] = inv_txfm_add_identity_adst_16x8_c;
    c.*.itxfm_add[TX_16X16][DCT_DCT] = inv_txfm_add_dct_dct_16x16_c;
    c.*.itxfm_add[TX_16X16][IDTX] = inv_txfm_add_identity_identity_16x16_c;
    c.*.itxfm_add[TX_16X16][DCT_ADST] = inv_txfm_add_adst_dct_16x16_c;
    c.*.itxfm_add[TX_16X16][ADST_DCT] = inv_txfm_add_dct_adst_16x16_c;
    c.*.itxfm_add[TX_16X16][ADST_ADST] = inv_txfm_add_adst_adst_16x16_c;
    c.*.itxfm_add[TX_16X16][ADST_FLIPADST] = inv_txfm_add_flipadst_adst_16x16_c;
    c.*.itxfm_add[TX_16X16][FLIPADST_ADST] = inv_txfm_add_adst_flipadst_16x16_c;
    c.*.itxfm_add[TX_16X16][DCT_FLIPADST] = inv_txfm_add_flipadst_dct_16x16_c;
    c.*.itxfm_add[TX_16X16][FLIPADST_DCT] = inv_txfm_add_dct_flipadst_16x16_c;
    c.*.itxfm_add[TX_16X16][FLIPADST_FLIPADST] = inv_txfm_add_flipadst_flipadst_16x16_c;
    c.*.itxfm_add[TX_16X16][H_DCT] = inv_txfm_add_dct_identity_16x16_c;
    c.*.itxfm_add[TX_16X16][V_DCT] = inv_txfm_add_identity_dct_16x16_c;
    c.*.itxfm_add[RTX_16X32][DCT_DCT] = inv_txfm_add_dct_dct_16x32_c;
    c.*.itxfm_add[RTX_16X32][IDTX] = inv_txfm_add_identity_identity_16x32_c;
    c.*.itxfm_add[RTX_16X64][DCT_DCT] = inv_txfm_add_dct_dct_16x64_c;
    c.*.itxfm_add[RTX_32X8][DCT_DCT] = inv_txfm_add_dct_dct_32x8_c;
    c.*.itxfm_add[RTX_32X8][IDTX] = inv_txfm_add_identity_identity_32x8_c;
    c.*.itxfm_add[RTX_32X16][DCT_DCT] = inv_txfm_add_dct_dct_32x16_c;
    c.*.itxfm_add[RTX_32X16][IDTX] = inv_txfm_add_identity_identity_32x16_c;
    c.*.itxfm_add[TX_32X32][DCT_DCT] = inv_txfm_add_dct_dct_32x32_c;
    c.*.itxfm_add[TX_32X32][IDTX] = inv_txfm_add_identity_identity_32x32_c;
    c.*.itxfm_add[RTX_32X64][DCT_DCT] = inv_txfm_add_dct_dct_32x64_c;
    c.*.itxfm_add[RTX_64X16][DCT_DCT] = inv_txfm_add_dct_dct_64x16_c;
    c.*.itxfm_add[RTX_64X32][DCT_DCT] = inv_txfm_add_dct_dct_64x32_c;
    c.*.itxfm_add[TX_64X64][DCT_DCT] = inv_txfm_add_dct_dct_64x64_c;
    var all_simd: c_int = 0;
    _ = &all_simd;
    if (!(all_simd != 0)) {
        antfly_av1_dav1d_init_last_nonzero_col_from_eob_tables();
    }
}
