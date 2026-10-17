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
pub fn iclip(v: c_int, min: c_int, max: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &min;
    _ = &max;
    return if (v < min) min else if (v > max) max else v;
}
pub const DCT = common.DCT;
pub const ADST = common.ADST;
pub const IDENTITY = common.IDENTITY;
pub const FLIPADST = common.FLIPADST;
pub const itx_1d_fn = ?*const fn (c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void;
pub fn inv_dct4_1d_internal_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int, tx64: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    _ = &tx64;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const in0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &in0;
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    var t0: c_int = undefined;
    _ = &t0;
    var t1: c_int = undefined;
    _ = &t1;
    var t2: c_int = undefined;
    _ = &t2;
    var t3: c_int = undefined;
    _ = &t3;
    if (tx64 != 0) {
        t0 = blk: {
            const tmp = ((in0 * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
            t1 = tmp;
            break :blk tmp;
        };
        t2 = ((in1 * @as(c_int, 1567)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t3 = ((in1 * @as(c_int, 3784)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    } else {
        const in2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
        _ = &in2;
        const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
        _ = &in3;
        t0 = (((in0 + in2) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
        t1 = (((in0 - in2) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
        t2 = ((((in1 * @as(c_int, 1567)) - (in3 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in3;
        t3 = ((((in1 * (@as(c_int, 3784) - @as(c_int, 4096))) + (in3 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    }
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = iclip(t0 + t3, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = iclip(t1 + t2, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = iclip(t1 - t2, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = iclip(t0 - t3, min, max);
}
pub fn inv_dct4_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_dct4_1d_internal_c(c, stride, min, max, 0);
}
pub fn inv_dct8_1d_internal_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int, tx64: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    _ = &tx64;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    inv_dct4_1d_internal_c(c, stride << @intCast(@as(ptrdiff_t, 1)), min, max, tx64);
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
    _ = &in3;
    var t4a: c_int = undefined;
    _ = &t4a;
    var t5a: c_int = undefined;
    _ = &t5a;
    var t6a: c_int = undefined;
    _ = &t6a;
    var t7a: c_int = undefined;
    _ = &t7a;
    if (tx64 != 0) {
        t4a = ((in1 * @as(c_int, 799)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t5a = ((in3 * -@as(c_int, 2276)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t6a = ((in3 * @as(c_int, 3406)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t7a = ((in1 * @as(c_int, 4017)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    } else {
        const in5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))];
        _ = &in5;
        const in7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))];
        _ = &in7;
        t4a = ((((in1 * @as(c_int, 799)) - (in7 * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in7;
        t5a = (((in5 * @as(c_int, 1703)) - (in3 * @as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t6a = (((in5 * @as(c_int, 1138)) + (in3 * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t7a = ((((in1 * (@as(c_int, 4017) - @as(c_int, 4096))) + (in7 * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    }
    const t4: c_int = iclip(t4a + t5a, min, max);
    _ = &t4;
    t5a = iclip(t4a - t5a, min, max);
    const t7: c_int = iclip(t7a + t6a, min, max);
    _ = &t7;
    t6a = iclip(t7a - t6a, min, max);
    const t5: c_int = (((t6a - t5a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    _ = &t5;
    const t6: c_int = (((t6a + t5a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    _ = &t6;
    const t0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &t0;
    const t1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
    _ = &t1;
    const t2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))];
    _ = &t2;
    const t3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))];
    _ = &t3;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = iclip(t0 + t7, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = iclip(t1 + t6, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = iclip(t2 + t5, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = iclip(t3 + t4, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))] = iclip(t3 - t4, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))] = iclip(t2 - t5, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))] = iclip(t1 - t6, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))] = iclip(t0 - t7, min, max);
}
pub fn inv_dct8_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_dct8_1d_internal_c(c, stride, min, max, 0);
}
pub fn inv_dct16_1d_internal_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int, arg_tx64: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    var tx64 = arg_tx64;
    _ = &tx64;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    inv_dct8_1d_internal_c(c, stride << @intCast(@as(ptrdiff_t, 1)), min, max, tx64);
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
    _ = &in3;
    const in5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))];
    _ = &in5;
    const in7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))];
    _ = &in7;
    var t8a: c_int = undefined;
    _ = &t8a;
    var t9a: c_int = undefined;
    _ = &t9a;
    var t10a: c_int = undefined;
    _ = &t10a;
    var t11a: c_int = undefined;
    _ = &t11a;
    var t12a: c_int = undefined;
    _ = &t12a;
    var t13a: c_int = undefined;
    _ = &t13a;
    var t14a: c_int = undefined;
    _ = &t14a;
    var t15a: c_int = undefined;
    _ = &t15a;
    if (tx64 != 0) {
        t8a = ((in1 * @as(c_int, 401)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t9a = ((in7 * -@as(c_int, 2598)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t10a = ((in5 * @as(c_int, 1931)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t11a = ((in3 * -@as(c_int, 1189)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t12a = ((in3 * @as(c_int, 3920)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t13a = ((in5 * @as(c_int, 3612)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t14a = ((in7 * @as(c_int, 3166)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t15a = ((in1 * @as(c_int, 4076)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    } else {
        const in9: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))];
        _ = &in9;
        const in11: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))];
        _ = &in11;
        const in13: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))];
        _ = &in13;
        const in15: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))];
        _ = &in15;
        t8a = ((((in1 * @as(c_int, 401)) - (in15 * (@as(c_int, 4076) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in15;
        t9a = (((in9 * @as(c_int, 1583)) - (in7 * @as(c_int, 1299))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t10a = ((((in5 * @as(c_int, 1931)) - (in11 * (@as(c_int, 3612) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in11;
        t11a = ((((in13 * (@as(c_int, 3920) - @as(c_int, 4096))) - (in3 * @as(c_int, 1189))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in13;
        t12a = ((((in13 * @as(c_int, 1189)) + (in3 * (@as(c_int, 3920) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in3;
        t13a = ((((in5 * (@as(c_int, 3612) - @as(c_int, 4096))) + (in11 * @as(c_int, 1931))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in5;
        t14a = (((in9 * @as(c_int, 1299)) + (in7 * @as(c_int, 1583))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t15a = ((((in1 * (@as(c_int, 4076) - @as(c_int, 4096))) + (in15 * @as(c_int, 401))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    }
    var t8: c_int = iclip(t8a + t9a, min, max);
    _ = &t8;
    var t9: c_int = iclip(t8a - t9a, min, max);
    _ = &t9;
    var t10: c_int = iclip(t11a - t10a, min, max);
    _ = &t10;
    var t11: c_int = iclip(t11a + t10a, min, max);
    _ = &t11;
    var t12: c_int = iclip(t12a + t13a, min, max);
    _ = &t12;
    var t13: c_int = iclip(t12a - t13a, min, max);
    _ = &t13;
    var t14: c_int = iclip(t15a - t14a, min, max);
    _ = &t14;
    var t15: c_int = iclip(t15a + t14a, min, max);
    _ = &t15;
    t9a = ((((t14 * @as(c_int, 1567)) - (t9 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t9;
    t14a = ((((t14 * (@as(c_int, 3784) - @as(c_int, 4096))) + (t9 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t14;
    t10a = ((-((t13 * (@as(c_int, 3784) - @as(c_int, 4096))) + (t10 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t13;
    t13a = ((((t13 * @as(c_int, 1567)) - (t10 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t10;
    t8a = iclip(t8 + t11, min, max);
    t9 = iclip(t9a + t10a, min, max);
    t10 = iclip(t9a - t10a, min, max);
    t11a = iclip(t8 - t11, min, max);
    t12a = iclip(t15 - t12, min, max);
    t13 = iclip(t14a - t13a, min, max);
    t14 = iclip(t14a + t13a, min, max);
    t15a = iclip(t15 + t12, min, max);
    t10a = (((t13 - t10) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t13a = (((t13 + t10) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t11 = (((t12a - t11a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t12 = (((t12a + t11a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    const t0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &t0;
    const t1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
    _ = &t1;
    const t2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))];
    _ = &t2;
    const t3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))];
    _ = &t3;
    const t4: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))];
    _ = &t4;
    const t5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))];
    _ = &t5;
    const t6: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))];
    _ = &t6;
    const t7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))];
    _ = &t7;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = iclip(t0 + t15a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = iclip(t1 + t14, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = iclip(t2 + t13a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = iclip(t3 + t12, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))] = iclip(t4 + t11, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))] = iclip(t5 + t10a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))] = iclip(t6 + t9, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))] = iclip(t7 + t8a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))] = iclip(t7 - t8a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))] = iclip(t6 - t9, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))] = iclip(t5 - t10a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))] = iclip(t4 - t11, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))] = iclip(t3 - t12, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))] = iclip(t2 - t13a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))] = iclip(t1 - t14, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))] = iclip(t0 - t15a, min, max);
}
pub fn inv_dct16_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_dct16_1d_internal_c(c, stride, min, max, 0);
}
pub fn inv_dct32_1d_internal_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int, tx64: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    _ = &tx64;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    inv_dct16_1d_internal_c(c, stride << @intCast(@as(ptrdiff_t, 1)), min, max, tx64);
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
    _ = &in3;
    const in5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))];
    _ = &in5;
    const in7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))];
    _ = &in7;
    const in9: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))];
    _ = &in9;
    const in11: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))];
    _ = &in11;
    const in13: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))];
    _ = &in13;
    const in15: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))];
    _ = &in15;
    var t16a: c_int = undefined;
    _ = &t16a;
    var t17a: c_int = undefined;
    _ = &t17a;
    var t18a: c_int = undefined;
    _ = &t18a;
    var t19a: c_int = undefined;
    _ = &t19a;
    var t20a: c_int = undefined;
    _ = &t20a;
    var t21a: c_int = undefined;
    _ = &t21a;
    var t22a: c_int = undefined;
    _ = &t22a;
    var t23a: c_int = undefined;
    _ = &t23a;
    var t24a: c_int = undefined;
    _ = &t24a;
    var t25a: c_int = undefined;
    _ = &t25a;
    var t26a: c_int = undefined;
    _ = &t26a;
    var t27a: c_int = undefined;
    _ = &t27a;
    var t28a: c_int = undefined;
    _ = &t28a;
    var t29a: c_int = undefined;
    _ = &t29a;
    var t30a: c_int = undefined;
    _ = &t30a;
    var t31a: c_int = undefined;
    _ = &t31a;
    if (tx64 != 0) {
        t16a = ((in1 * @as(c_int, 201)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t17a = ((in15 * -@as(c_int, 2751)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t18a = ((in9 * @as(c_int, 1751)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t19a = ((in7 * -@as(c_int, 1380)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t20a = ((in5 * @as(c_int, 995)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t21a = ((in11 * -@as(c_int, 2106)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t22a = ((in13 * @as(c_int, 2440)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t23a = ((in3 * -@as(c_int, 601)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t24a = ((in3 * @as(c_int, 4052)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t25a = ((in13 * @as(c_int, 3290)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t26a = ((in11 * @as(c_int, 3513)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t27a = ((in5 * @as(c_int, 3973)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t28a = ((in7 * @as(c_int, 3857)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t29a = ((in9 * @as(c_int, 3703)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t30a = ((in15 * @as(c_int, 3035)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
        t31a = ((in1 * @as(c_int, 4091)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    } else {
        const in17: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 17) * stride)))];
        _ = &in17;
        const in19: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 19) * stride)))];
        _ = &in19;
        const in21: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 21) * stride)))];
        _ = &in21;
        const in23: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 23) * stride)))];
        _ = &in23;
        const in25: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 25) * stride)))];
        _ = &in25;
        const in27: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 27) * stride)))];
        _ = &in27;
        const in29: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 29) * stride)))];
        _ = &in29;
        const in31: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 31) * stride)))];
        _ = &in31;
        t16a = ((((in1 * @as(c_int, 201)) - (in31 * (@as(c_int, 4091) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in31;
        t17a = ((((in17 * (@as(c_int, 3035) - @as(c_int, 4096))) - (in15 * @as(c_int, 2751))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in17;
        t18a = ((((in9 * @as(c_int, 1751)) - (in23 * (@as(c_int, 3703) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in23;
        t19a = ((((in25 * (@as(c_int, 3857) - @as(c_int, 4096))) - (in7 * @as(c_int, 1380))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in25;
        t20a = ((((in5 * @as(c_int, 995)) - (in27 * (@as(c_int, 3973) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in27;
        t21a = ((((in21 * (@as(c_int, 3513) - @as(c_int, 4096))) - (in11 * @as(c_int, 2106))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in21;
        t22a = (((in13 * @as(c_int, 1220)) - (in19 * @as(c_int, 1645))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t23a = ((((in29 * (@as(c_int, 4052) - @as(c_int, 4096))) - (in3 * @as(c_int, 601))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in29;
        t24a = ((((in29 * @as(c_int, 601)) + (in3 * (@as(c_int, 4052) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in3;
        t25a = (((in13 * @as(c_int, 1645)) + (in19 * @as(c_int, 1220))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
        t26a = ((((in21 * @as(c_int, 2106)) + (in11 * (@as(c_int, 3513) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in11;
        t27a = ((((in5 * (@as(c_int, 3973) - @as(c_int, 4096))) + (in27 * @as(c_int, 995))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in5;
        t28a = ((((in25 * @as(c_int, 1380)) + (in7 * (@as(c_int, 3857) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in7;
        t29a = ((((in9 * (@as(c_int, 3703) - @as(c_int, 4096))) + (in23 * @as(c_int, 1751))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in9;
        t30a = ((((in17 * @as(c_int, 2751)) + (in15 * (@as(c_int, 3035) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in15;
        t31a = ((((in1 * (@as(c_int, 4091) - @as(c_int, 4096))) + (in31 * @as(c_int, 201))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    }
    var t16: c_int = iclip(t16a + t17a, min, max);
    _ = &t16;
    var t17: c_int = iclip(t16a - t17a, min, max);
    _ = &t17;
    var t18: c_int = iclip(t19a - t18a, min, max);
    _ = &t18;
    var t19: c_int = iclip(t19a + t18a, min, max);
    _ = &t19;
    var t20: c_int = iclip(t20a + t21a, min, max);
    _ = &t20;
    var t21: c_int = iclip(t20a - t21a, min, max);
    _ = &t21;
    var t22: c_int = iclip(t23a - t22a, min, max);
    _ = &t22;
    var t23: c_int = iclip(t23a + t22a, min, max);
    _ = &t23;
    var t24: c_int = iclip(t24a + t25a, min, max);
    _ = &t24;
    var t25: c_int = iclip(t24a - t25a, min, max);
    _ = &t25;
    var t26: c_int = iclip(t27a - t26a, min, max);
    _ = &t26;
    var t27: c_int = iclip(t27a + t26a, min, max);
    _ = &t27;
    var t28: c_int = iclip(t28a + t29a, min, max);
    _ = &t28;
    var t29: c_int = iclip(t28a - t29a, min, max);
    _ = &t29;
    var t30: c_int = iclip(t31a - t30a, min, max);
    _ = &t30;
    var t31: c_int = iclip(t31a + t30a, min, max);
    _ = &t31;
    t17a = ((((t30 * @as(c_int, 799)) - (t17 * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t17;
    t30a = ((((t30 * (@as(c_int, 4017) - @as(c_int, 4096))) + (t17 * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t30;
    t18a = ((-((t29 * (@as(c_int, 4017) - @as(c_int, 4096))) + (t18 * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t29;
    t29a = ((((t29 * @as(c_int, 799)) - (t18 * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t18;
    t21a = (((t26 * @as(c_int, 1703)) - (t21 * @as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t26a = (((t26 * @as(c_int, 1138)) + (t21 * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t22a = (-((t25 * @as(c_int, 1138)) + (t22 * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t25a = (((t25 * @as(c_int, 1703)) - (t22 * @as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t16a = iclip(t16 + t19, min, max);
    t17 = iclip(t17a + t18a, min, max);
    t18 = iclip(t17a - t18a, min, max);
    t19a = iclip(t16 - t19, min, max);
    t20a = iclip(t23 - t20, min, max);
    t21 = iclip(t22a - t21a, min, max);
    t22 = iclip(t22a + t21a, min, max);
    t23a = iclip(t23 + t20, min, max);
    t24a = iclip(t24 + t27, min, max);
    t25 = iclip(t25a + t26a, min, max);
    t26 = iclip(t25a - t26a, min, max);
    t27a = iclip(t24 - t27, min, max);
    t28a = iclip(t31 - t28, min, max);
    t29 = iclip(t30a - t29a, min, max);
    t30 = iclip(t30a + t29a, min, max);
    t31a = iclip(t31 + t28, min, max);
    t18a = ((((t29 * @as(c_int, 1567)) - (t18 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t18;
    t29a = ((((t29 * (@as(c_int, 3784) - @as(c_int, 4096))) + (t18 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t29;
    t19 = ((((t28a * @as(c_int, 1567)) - (t19a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t19a;
    t28 = ((((t28a * (@as(c_int, 3784) - @as(c_int, 4096))) + (t19a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t28a;
    t20 = ((-((t27a * (@as(c_int, 3784) - @as(c_int, 4096))) + (t20a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t27a;
    t27 = ((((t27a * @as(c_int, 1567)) - (t20a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t20a;
    t21a = ((-((t26 * (@as(c_int, 3784) - @as(c_int, 4096))) + (t21 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t26;
    t26a = ((((t26 * @as(c_int, 1567)) - (t21 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t21;
    t16 = iclip(t16a + t23a, min, max);
    t17a = iclip(t17 + t22, min, max);
    t18 = iclip(t18a + t21a, min, max);
    t19a = iclip(t19 + t20, min, max);
    t20a = iclip(t19 - t20, min, max);
    t21 = iclip(t18a - t21a, min, max);
    t22a = iclip(t17 - t22, min, max);
    t23 = iclip(t16a - t23a, min, max);
    t24 = iclip(t31a - t24a, min, max);
    t25a = iclip(t30 - t25, min, max);
    t26 = iclip(t29a - t26a, min, max);
    t27a = iclip(t28 - t27, min, max);
    t28a = iclip(t28 + t27, min, max);
    t29 = iclip(t29a + t26a, min, max);
    t30a = iclip(t30 + t25, min, max);
    t31 = iclip(t31a + t24a, min, max);
    t20 = (((t27a - t20a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t27 = (((t27a + t20a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t21a = (((t26 - t21) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t26a = (((t26 + t21) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t22 = (((t25a - t22a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t25 = (((t25a + t22a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t23a = (((t24 - t23) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t24a = (((t24 + t23) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    const t0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &t0;
    const t1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
    _ = &t1;
    const t2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))];
    _ = &t2;
    const t3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))];
    _ = &t3;
    const t4: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))];
    _ = &t4;
    const t5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))];
    _ = &t5;
    const t6: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))];
    _ = &t6;
    const t7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))];
    _ = &t7;
    const t8: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 16) * stride)))];
    _ = &t8;
    const t9: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 18) * stride)))];
    _ = &t9;
    const t10: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 20) * stride)))];
    _ = &t10;
    const t11: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 22) * stride)))];
    _ = &t11;
    const t12: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 24) * stride)))];
    _ = &t12;
    const t13: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 26) * stride)))];
    _ = &t13;
    const t14: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 28) * stride)))];
    _ = &t14;
    const t15: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 30) * stride)))];
    _ = &t15;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = iclip(t0 + t31, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = iclip(t1 + t30a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = iclip(t2 + t29, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = iclip(t3 + t28a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))] = iclip(t4 + t27, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))] = iclip(t5 + t26a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))] = iclip(t6 + t25, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))] = iclip(t7 + t24a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))] = iclip(t8 + t23a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))] = iclip(t9 + t22, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))] = iclip(t10 + t21a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))] = iclip(t11 + t20, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))] = iclip(t12 + t19a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))] = iclip(t13 + t18, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))] = iclip(t14 + t17a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))] = iclip(t15 + t16, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 16) * stride)))] = iclip(t15 - t16, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 17) * stride)))] = iclip(t14 - t17a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 18) * stride)))] = iclip(t13 - t18, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 19) * stride)))] = iclip(t12 - t19a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 20) * stride)))] = iclip(t11 - t20, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 21) * stride)))] = iclip(t10 - t21a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 22) * stride)))] = iclip(t9 - t22, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 23) * stride)))] = iclip(t8 - t23a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 24) * stride)))] = iclip(t7 - t24a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 25) * stride)))] = iclip(t6 - t25, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 26) * stride)))] = iclip(t5 - t26a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 27) * stride)))] = iclip(t4 - t27, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 28) * stride)))] = iclip(t3 - t28a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 29) * stride)))] = iclip(t2 - t29, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 30) * stride)))] = iclip(t1 - t30a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 31) * stride)))] = iclip(t0 - t31, min, max);
}
pub fn inv_dct32_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_dct32_1d_internal_c(c, stride, min, max, 0);
}
pub fn inv_dct64_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    inv_dct32_1d_internal_c(c, stride << @intCast(@as(ptrdiff_t, 1)), min, max, 1);
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
    _ = &in3;
    const in5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))];
    _ = &in5;
    const in7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))];
    _ = &in7;
    const in9: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))];
    _ = &in9;
    const in11: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))];
    _ = &in11;
    const in13: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))];
    _ = &in13;
    const in15: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))];
    _ = &in15;
    const in17: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 17) * stride)))];
    _ = &in17;
    const in19: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 19) * stride)))];
    _ = &in19;
    const in21: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 21) * stride)))];
    _ = &in21;
    const in23: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 23) * stride)))];
    _ = &in23;
    const in25: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 25) * stride)))];
    _ = &in25;
    const in27: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 27) * stride)))];
    _ = &in27;
    const in29: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 29) * stride)))];
    _ = &in29;
    const in31: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 31) * stride)))];
    _ = &in31;
    var t32a: c_int = ((in1 * @as(c_int, 101)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t32a;
    var t33a: c_int = ((in31 * -@as(c_int, 2824)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t33a;
    var t34a: c_int = ((in17 * @as(c_int, 1660)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t34a;
    var t35a: c_int = ((in15 * -@as(c_int, 1474)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t35a;
    var t36a: c_int = ((in9 * @as(c_int, 897)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t36a;
    var t37a: c_int = ((in23 * -@as(c_int, 2191)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t37a;
    var t38a: c_int = ((in25 * @as(c_int, 2359)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t38a;
    var t39a: c_int = ((in7 * -@as(c_int, 700)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t39a;
    var t40a: c_int = ((in5 * @as(c_int, 501)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t40a;
    var t41a: c_int = ((in27 * -@as(c_int, 2520)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t41a;
    var t42a: c_int = ((in21 * @as(c_int, 2019)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t42a;
    var t43a: c_int = ((in11 * -@as(c_int, 1092)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t43a;
    var t44a: c_int = ((in13 * @as(c_int, 1285)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t44a;
    var t45a: c_int = ((in19 * -@as(c_int, 1842)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t45a;
    var t46a: c_int = ((in29 * @as(c_int, 2675)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t46a;
    var t47a: c_int = ((in3 * -@as(c_int, 301)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t47a;
    var t48a: c_int = ((in3 * @as(c_int, 4085)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t48a;
    var t49a: c_int = ((in29 * @as(c_int, 3102)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t49a;
    var t50a: c_int = ((in19 * @as(c_int, 3659)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t50a;
    var t51a: c_int = ((in13 * @as(c_int, 3889)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t51a;
    var t52a: c_int = ((in11 * @as(c_int, 3948)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t52a;
    var t53a: c_int = ((in21 * @as(c_int, 3564)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t53a;
    var t54a: c_int = ((in27 * @as(c_int, 3229)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t54a;
    var t55a: c_int = ((in5 * @as(c_int, 4065)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t55a;
    var t56a: c_int = ((in7 * @as(c_int, 4036)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t56a;
    var t57a: c_int = ((in25 * @as(c_int, 3349)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t57a;
    var t58a: c_int = ((in23 * @as(c_int, 3461)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t58a;
    var t59a: c_int = ((in9 * @as(c_int, 3996)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t59a;
    var t60a: c_int = ((in15 * @as(c_int, 3822)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t60a;
    var t61a: c_int = ((in17 * @as(c_int, 3745)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t61a;
    var t62a: c_int = ((in31 * @as(c_int, 2967)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t62a;
    var t63a: c_int = ((in1 * @as(c_int, 4095)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12));
    _ = &t63a;
    var t32: c_int = iclip(t32a + t33a, min, max);
    _ = &t32;
    var t33: c_int = iclip(t32a - t33a, min, max);
    _ = &t33;
    var t34: c_int = iclip(t35a - t34a, min, max);
    _ = &t34;
    var t35: c_int = iclip(t35a + t34a, min, max);
    _ = &t35;
    var t36: c_int = iclip(t36a + t37a, min, max);
    _ = &t36;
    var t37: c_int = iclip(t36a - t37a, min, max);
    _ = &t37;
    var t38: c_int = iclip(t39a - t38a, min, max);
    _ = &t38;
    var t39: c_int = iclip(t39a + t38a, min, max);
    _ = &t39;
    var t40: c_int = iclip(t40a + t41a, min, max);
    _ = &t40;
    var t41: c_int = iclip(t40a - t41a, min, max);
    _ = &t41;
    var t42: c_int = iclip(t43a - t42a, min, max);
    _ = &t42;
    var t43: c_int = iclip(t43a + t42a, min, max);
    _ = &t43;
    var t44: c_int = iclip(t44a + t45a, min, max);
    _ = &t44;
    var t45: c_int = iclip(t44a - t45a, min, max);
    _ = &t45;
    var t46: c_int = iclip(t47a - t46a, min, max);
    _ = &t46;
    var t47: c_int = iclip(t47a + t46a, min, max);
    _ = &t47;
    var t48: c_int = iclip(t48a + t49a, min, max);
    _ = &t48;
    var t49: c_int = iclip(t48a - t49a, min, max);
    _ = &t49;
    var t50: c_int = iclip(t51a - t50a, min, max);
    _ = &t50;
    var t51: c_int = iclip(t51a + t50a, min, max);
    _ = &t51;
    var t52: c_int = iclip(t52a + t53a, min, max);
    _ = &t52;
    var t53: c_int = iclip(t52a - t53a, min, max);
    _ = &t53;
    var t54: c_int = iclip(t55a - t54a, min, max);
    _ = &t54;
    var t55: c_int = iclip(t55a + t54a, min, max);
    _ = &t55;
    var t56: c_int = iclip(t56a + t57a, min, max);
    _ = &t56;
    var t57: c_int = iclip(t56a - t57a, min, max);
    _ = &t57;
    var t58: c_int = iclip(t59a - t58a, min, max);
    _ = &t58;
    var t59: c_int = iclip(t59a + t58a, min, max);
    _ = &t59;
    var t60: c_int = iclip(t60a + t61a, min, max);
    _ = &t60;
    var t61: c_int = iclip(t60a - t61a, min, max);
    _ = &t61;
    var t62: c_int = iclip(t63a - t62a, min, max);
    _ = &t62;
    var t63: c_int = iclip(t63a + t62a, min, max);
    _ = &t63;
    t33a = ((((t33 * (@as(c_int, 4096) - @as(c_int, 4076))) + (t62 * @as(c_int, 401))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t33;
    t34a = ((((t34 * -@as(c_int, 401)) + (t61 * (@as(c_int, 4096) - @as(c_int, 4076)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t61;
    t37a = (((t37 * -@as(c_int, 1299)) + (t58 * @as(c_int, 1583))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t38a = (((t38 * -@as(c_int, 1583)) + (t57 * -@as(c_int, 1299))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t41a = ((((t41 * (@as(c_int, 4096) - @as(c_int, 3612))) + (t54 * @as(c_int, 1931))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t41;
    t42a = ((((t42 * -@as(c_int, 1931)) + (t53 * (@as(c_int, 4096) - @as(c_int, 3612)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t53;
    t45a = ((((t45 * -@as(c_int, 1189)) + (t50 * (@as(c_int, 3920) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t50;
    t46a = ((((t46 * (@as(c_int, 4096) - @as(c_int, 3920))) + (t49 * -@as(c_int, 1189))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t46;
    t49a = ((((t46 * -@as(c_int, 1189)) + (t49 * (@as(c_int, 3920) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t49;
    t50a = ((((t45 * (@as(c_int, 3920) - @as(c_int, 4096))) + (t50 * @as(c_int, 1189))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t45;
    t53a = ((((t42 * (@as(c_int, 4096) - @as(c_int, 3612))) + (t53 * @as(c_int, 1931))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t42;
    t54a = ((((t41 * @as(c_int, 1931)) + (t54 * (@as(c_int, 3612) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t54;
    t57a = (((t38 * -@as(c_int, 1299)) + (t57 * @as(c_int, 1583))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t58a = (((t37 * @as(c_int, 1583)) + (t58 * @as(c_int, 1299))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t61a = ((((t34 * (@as(c_int, 4096) - @as(c_int, 4076))) + (t61 * @as(c_int, 401))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t34;
    t62a = ((((t33 * @as(c_int, 401)) + (t62 * (@as(c_int, 4076) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t62;
    t32a = iclip(t32 + t35, min, max);
    t33 = iclip(t33a + t34a, min, max);
    t34 = iclip(t33a - t34a, min, max);
    t35a = iclip(t32 - t35, min, max);
    t36a = iclip(t39 - t36, min, max);
    t37 = iclip(t38a - t37a, min, max);
    t38 = iclip(t38a + t37a, min, max);
    t39a = iclip(t39 + t36, min, max);
    t40a = iclip(t40 + t43, min, max);
    t41 = iclip(t41a + t42a, min, max);
    t42 = iclip(t41a - t42a, min, max);
    t43a = iclip(t40 - t43, min, max);
    t44a = iclip(t47 - t44, min, max);
    t45 = iclip(t46a - t45a, min, max);
    t46 = iclip(t46a + t45a, min, max);
    t47a = iclip(t47 + t44, min, max);
    t48a = iclip(t48 + t51, min, max);
    t49 = iclip(t49a + t50a, min, max);
    t50 = iclip(t49a - t50a, min, max);
    t51a = iclip(t48 - t51, min, max);
    t52a = iclip(t55 - t52, min, max);
    t53 = iclip(t54a - t53a, min, max);
    t54 = iclip(t54a + t53a, min, max);
    t55a = iclip(t55 + t52, min, max);
    t56a = iclip(t56 + t59, min, max);
    t57 = iclip(t57a + t58a, min, max);
    t58 = iclip(t57a - t58a, min, max);
    t59a = iclip(t56 - t59, min, max);
    t60a = iclip(t63 - t60, min, max);
    t61 = iclip(t62a - t61a, min, max);
    t62 = iclip(t62a + t61a, min, max);
    t63a = iclip(t63 + t60, min, max);
    t34a = ((((t34 * (@as(c_int, 4096) - @as(c_int, 4017))) + (t61 * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t34;
    t35 = ((((t35a * (@as(c_int, 4096) - @as(c_int, 4017))) + (t60a * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t35a;
    t36 = ((((t36a * -@as(c_int, 799)) + (t59a * (@as(c_int, 4096) - @as(c_int, 4017)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t59a;
    t37a = ((((t37 * -@as(c_int, 799)) + (t58 * (@as(c_int, 4096) - @as(c_int, 4017)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t58;
    t42a = (((t42 * -@as(c_int, 1138)) + (t53 * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t43 = (((t43a * -@as(c_int, 1138)) + (t52a * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t44 = (((t44a * -@as(c_int, 1703)) + (t51a * -@as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t45a = (((t45 * -@as(c_int, 1703)) + (t50 * -@as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t50a = (((t45 * -@as(c_int, 1138)) + (t50 * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t51 = (((t44a * -@as(c_int, 1138)) + (t51a * @as(c_int, 1703))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t52 = (((t43a * @as(c_int, 1703)) + (t52a * @as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t53a = (((t42 * @as(c_int, 1703)) + (t53 * @as(c_int, 1138))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    t58a = ((((t37 * (@as(c_int, 4096) - @as(c_int, 4017))) + (t58 * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t37;
    t59 = ((((t36a * (@as(c_int, 4096) - @as(c_int, 4017))) + (t59a * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t36a;
    t60 = ((((t35a * @as(c_int, 799)) + (t60a * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t60a;
    t61a = ((((t34 * @as(c_int, 799)) + (t61 * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t61;
    t32 = iclip(t32a + t39a, min, max);
    t33a = iclip(t33 + t38, min, max);
    t34 = iclip(t34a + t37a, min, max);
    t35a = iclip(t35 + t36, min, max);
    t36a = iclip(t35 - t36, min, max);
    t37 = iclip(t34a - t37a, min, max);
    t38a = iclip(t33 - t38, min, max);
    t39 = iclip(t32a - t39a, min, max);
    t40 = iclip(t47a - t40a, min, max);
    t41a = iclip(t46 - t41, min, max);
    t42 = iclip(t45a - t42a, min, max);
    t43a = iclip(t44 - t43, min, max);
    t44a = iclip(t44 + t43, min, max);
    t45 = iclip(t45a + t42a, min, max);
    t46a = iclip(t46 + t41, min, max);
    t47 = iclip(t47a + t40a, min, max);
    t48 = iclip(t48a + t55a, min, max);
    t49a = iclip(t49 + t54, min, max);
    t50 = iclip(t50a + t53a, min, max);
    t51a = iclip(t51 + t52, min, max);
    t52a = iclip(t51 - t52, min, max);
    t53 = iclip(t50a - t53a, min, max);
    t54a = iclip(t49 - t54, min, max);
    t55 = iclip(t48a - t55a, min, max);
    t56 = iclip(t63a - t56a, min, max);
    t57a = iclip(t62 - t57, min, max);
    t58 = iclip(t61a - t58a, min, max);
    t59a = iclip(t60 - t59, min, max);
    t60a = iclip(t60 + t59, min, max);
    t61 = iclip(t61a + t58a, min, max);
    t62a = iclip(t62 + t57, min, max);
    t63 = iclip(t63a + t56a, min, max);
    t36 = ((((t36a * (@as(c_int, 4096) - @as(c_int, 3784))) + (t59a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t36a;
    t37a = ((((t37 * (@as(c_int, 4096) - @as(c_int, 3784))) + (t58 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t37;
    t38 = ((((t38a * (@as(c_int, 4096) - @as(c_int, 3784))) + (t57a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t38a;
    t39a = ((((t39 * (@as(c_int, 4096) - @as(c_int, 3784))) + (t56 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t39;
    t40a = ((((t40 * -@as(c_int, 1567)) + (t55 * (@as(c_int, 4096) - @as(c_int, 3784)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t55;
    t41 = ((((t41a * -@as(c_int, 1567)) + (t54a * (@as(c_int, 4096) - @as(c_int, 3784)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t54a;
    t42a = ((((t42 * -@as(c_int, 1567)) + (t53 * (@as(c_int, 4096) - @as(c_int, 3784)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t53;
    t43 = ((((t43a * -@as(c_int, 1567)) + (t52a * (@as(c_int, 4096) - @as(c_int, 3784)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t52a;
    t52 = ((((t43a * (@as(c_int, 4096) - @as(c_int, 3784))) + (t52a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t43a;
    t53a = ((((t42 * (@as(c_int, 4096) - @as(c_int, 3784))) + (t53 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t42;
    t54 = ((((t41a * (@as(c_int, 4096) - @as(c_int, 3784))) + (t54a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t41a;
    t55a = ((((t40 * (@as(c_int, 4096) - @as(c_int, 3784))) + (t55 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t40;
    t56a = ((((t39 * @as(c_int, 1567)) + (t56 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t56;
    t57 = ((((t38a * @as(c_int, 1567)) + (t57a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t57a;
    t58a = ((((t37 * @as(c_int, 1567)) + (t58 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t58;
    t59 = ((((t36a * @as(c_int, 1567)) + (t59a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t59a;
    t32a = iclip(t32 + t47, min, max);
    t33 = iclip(t33a + t46a, min, max);
    t34a = iclip(t34 + t45, min, max);
    t35 = iclip(t35a + t44a, min, max);
    t36a = iclip(t36 + t43, min, max);
    t37 = iclip(t37a + t42a, min, max);
    t38a = iclip(t38 + t41, min, max);
    t39 = iclip(t39a + t40a, min, max);
    t40 = iclip(t39a - t40a, min, max);
    t41a = iclip(t38 - t41, min, max);
    t42 = iclip(t37a - t42a, min, max);
    t43a = iclip(t36 - t43, min, max);
    t44 = iclip(t35a - t44a, min, max);
    t45a = iclip(t34 - t45, min, max);
    t46 = iclip(t33a - t46a, min, max);
    t47a = iclip(t32 - t47, min, max);
    t48a = iclip(t63 - t48, min, max);
    t49 = iclip(t62a - t49a, min, max);
    t50a = iclip(t61 - t50, min, max);
    t51 = iclip(t60a - t51a, min, max);
    t52a = iclip(t59 - t52, min, max);
    t53 = iclip(t58a - t53a, min, max);
    t54a = iclip(t57 - t54, min, max);
    t55 = iclip(t56a - t55a, min, max);
    t56 = iclip(t56a + t55a, min, max);
    t57a = iclip(t57 + t54, min, max);
    t58 = iclip(t58a + t53a, min, max);
    t59a = iclip(t59 + t52, min, max);
    t60 = iclip(t60a + t51a, min, max);
    t61a = iclip(t61 + t50, min, max);
    t62 = iclip(t62a + t49a, min, max);
    t63a = iclip(t63 + t48, min, max);
    t40a = (((t55 - t40) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t41 = (((t54a - t41a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t42a = (((t53 - t42) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t43 = (((t52a - t43a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t44a = (((t51 - t44) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t45 = (((t50a - t45a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t46a = (((t49 - t46) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t47 = (((t48a - t47a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t48 = (((t47a + t48a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t49a = (((t46 + t49) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t50 = (((t45a + t50a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t51a = (((t44 + t51) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t52 = (((t43a + t52a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t53a = (((t42 + t53) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t54 = (((t41a + t54a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    t55a = (((t40 + t55) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    const t0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &t0;
    const t1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
    _ = &t1;
    const t2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))];
    _ = &t2;
    const t3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))];
    _ = &t3;
    const t4: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))];
    _ = &t4;
    const t5: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))];
    _ = &t5;
    const t6: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))];
    _ = &t6;
    const t7: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))];
    _ = &t7;
    const t8: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 16) * stride)))];
    _ = &t8;
    const t9: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 18) * stride)))];
    _ = &t9;
    const t10: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 20) * stride)))];
    _ = &t10;
    const t11: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 22) * stride)))];
    _ = &t11;
    const t12: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 24) * stride)))];
    _ = &t12;
    const t13: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 26) * stride)))];
    _ = &t13;
    const t14: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 28) * stride)))];
    _ = &t14;
    const t15: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 30) * stride)))];
    _ = &t15;
    const t16: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 32) * stride)))];
    _ = &t16;
    const t17: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 34) * stride)))];
    _ = &t17;
    const t18: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 36) * stride)))];
    _ = &t18;
    const t19: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 38) * stride)))];
    _ = &t19;
    const t20: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 40) * stride)))];
    _ = &t20;
    const t21: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 42) * stride)))];
    _ = &t21;
    const t22: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 44) * stride)))];
    _ = &t22;
    const t23: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 46) * stride)))];
    _ = &t23;
    const t24: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 48) * stride)))];
    _ = &t24;
    const t25: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 50) * stride)))];
    _ = &t25;
    const t26: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 52) * stride)))];
    _ = &t26;
    const t27: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 54) * stride)))];
    _ = &t27;
    const t28: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 56) * stride)))];
    _ = &t28;
    const t29: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 58) * stride)))];
    _ = &t29;
    const t30: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 60) * stride)))];
    _ = &t30;
    const t31: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 62) * stride)))];
    _ = &t31;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = iclip(t0 + t63a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = iclip(t1 + t62, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = iclip(t2 + t61a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = iclip(t3 + t60, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride)))] = iclip(t4 + t59a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * stride)))] = iclip(t5 + t58, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))] = iclip(t6 + t57a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * stride)))] = iclip(t7 + t56, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * stride)))] = iclip(t8 + t55a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * stride)))] = iclip(t9 + t54, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * stride)))] = iclip(t10 + t53a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * stride)))] = iclip(t11 + t52, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * stride)))] = iclip(t12 + t51a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * stride)))] = iclip(t13 + t50, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * stride)))] = iclip(t14 + t49a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * stride)))] = iclip(t15 + t48, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 16) * stride)))] = iclip(t16 + t47, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 17) * stride)))] = iclip(t17 + t46a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 18) * stride)))] = iclip(t18 + t45, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 19) * stride)))] = iclip(t19 + t44a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 20) * stride)))] = iclip(t20 + t43, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 21) * stride)))] = iclip(t21 + t42a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 22) * stride)))] = iclip(t22 + t41, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 23) * stride)))] = iclip(t23 + t40a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 24) * stride)))] = iclip(t24 + t39, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 25) * stride)))] = iclip(t25 + t38a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 26) * stride)))] = iclip(t26 + t37, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 27) * stride)))] = iclip(t27 + t36a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 28) * stride)))] = iclip(t28 + t35, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 29) * stride)))] = iclip(t29 + t34a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 30) * stride)))] = iclip(t30 + t33, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 31) * stride)))] = iclip(t31 + t32a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 32) * stride)))] = iclip(t31 - t32a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 33) * stride)))] = iclip(t30 - t33, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 34) * stride)))] = iclip(t29 - t34a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 35) * stride)))] = iclip(t28 - t35, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 36) * stride)))] = iclip(t27 - t36a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 37) * stride)))] = iclip(t26 - t37, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 38) * stride)))] = iclip(t25 - t38a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 39) * stride)))] = iclip(t24 - t39, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 40) * stride)))] = iclip(t23 - t40a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 41) * stride)))] = iclip(t22 - t41, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 42) * stride)))] = iclip(t21 - t42a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 43) * stride)))] = iclip(t20 - t43, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 44) * stride)))] = iclip(t19 - t44a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 45) * stride)))] = iclip(t18 - t45, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 46) * stride)))] = iclip(t17 - t46a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 47) * stride)))] = iclip(t16 - t47, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 48) * stride)))] = iclip(t15 - t48, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 49) * stride)))] = iclip(t14 - t49a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 50) * stride)))] = iclip(t13 - t50, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 51) * stride)))] = iclip(t12 - t51a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 52) * stride)))] = iclip(t11 - t52, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 53) * stride)))] = iclip(t10 - t53a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 54) * stride)))] = iclip(t9 - t54, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 55) * stride)))] = iclip(t8 - t55a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 56) * stride)))] = iclip(t7 - t56, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 57) * stride)))] = iclip(t6 - t57a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 58) * stride)))] = iclip(t5 - t58, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 59) * stride)))] = iclip(t4 - t59a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 60) * stride)))] = iclip(t3 - t60, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 61) * stride)))] = iclip(t2 - t61a, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 62) * stride)))] = iclip(t1 - t62, min, max);
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 63) * stride)))] = iclip(t0 - t63a, min, max);
}
pub fn inv_adst4_1d_internal_c(in: [*c]const i32, in_s: ptrdiff_t, min: c_int, max: c_int, out: [*c]i32, out_s: ptrdiff_t) callconv(.c) void {
    _ = &in;
    _ = &in_s;
    _ = &min;
    _ = &max;
    _ = &out;
    _ = &out_s;
    while (true) {
        if (!((in_s > @as(ptrdiff_t, 0)) and (out_s != @as(ptrdiff_t, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    const in0: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * in_s)))];
    _ = &in0;
    const in1: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * in_s)))];
    _ = &in1;
    const in2: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * in_s)))];
    _ = &in2;
    const in3: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * in_s)))];
    _ = &in3;
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * out_s)))] = ((((((((@as(c_int, 1321) * in0) + ((@as(c_int, 3803) - @as(c_int, 4096)) * in2)) + ((@as(c_int, 2482) - @as(c_int, 4096)) * in3)) + ((@as(c_int, 3344) - @as(c_int, 4096)) * in1)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in2) + in3) + in1;
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * out_s)))] = (((((((((@as(c_int, 2482) - @as(c_int, 4096)) * in0) - (@as(c_int, 1321) * in2)) - ((@as(c_int, 3803) - @as(c_int, 4096)) * in3)) + ((@as(c_int, 3344) - @as(c_int, 4096)) * in1)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in0) - in3) + in1;
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * out_s)))] = ((@as(c_int, 209) * ((in0 - in2) + in3)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * out_s)))] = (((((((((@as(c_int, 3803) - @as(c_int, 4096)) * in0) + ((@as(c_int, 2482) - @as(c_int, 4096)) * in2)) - (@as(c_int, 1321) * in3)) - ((@as(c_int, 3344) - @as(c_int, 4096)) * in1)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in0) + in2) - in1;
}
pub fn inv_adst8_1d_internal_c(in: [*c]const i32, in_s: ptrdiff_t, min: c_int, max: c_int, out: [*c]i32, out_s: ptrdiff_t) callconv(.c) void {
    _ = &in;
    _ = &in_s;
    _ = &min;
    _ = &max;
    _ = &out;
    _ = &out_s;
    while (true) {
        if (!((in_s > @as(ptrdiff_t, 0)) and (out_s != @as(ptrdiff_t, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    const in0: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * in_s)))];
    _ = &in0;
    const in1: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * in_s)))];
    _ = &in1;
    const in2: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * in_s)))];
    _ = &in2;
    const in3: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * in_s)))];
    _ = &in3;
    const in4: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * in_s)))];
    _ = &in4;
    const in5: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * in_s)))];
    _ = &in5;
    const in6: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * in_s)))];
    _ = &in6;
    const in7: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * in_s)))];
    _ = &in7;
    const t0a: c_int = (((((@as(c_int, 4076) - @as(c_int, 4096)) * in7) + (@as(c_int, 401) * in0)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in7;
    _ = &t0a;
    const t1a: c_int = ((((@as(c_int, 401) * in7) - ((@as(c_int, 4076) - @as(c_int, 4096)) * in0)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in0;
    _ = &t1a;
    const t2a: c_int = (((((@as(c_int, 3612) - @as(c_int, 4096)) * in5) + (@as(c_int, 1931) * in2)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in5;
    _ = &t2a;
    const t3a: c_int = ((((@as(c_int, 1931) * in5) - ((@as(c_int, 3612) - @as(c_int, 4096)) * in2)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in2;
    _ = &t3a;
    var t4a: c_int = (((@as(c_int, 1299) * in3) + (@as(c_int, 1583) * in4)) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    _ = &t4a;
    var t5a: c_int = (((@as(c_int, 1583) * in3) - (@as(c_int, 1299) * in4)) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    _ = &t5a;
    var t6a: c_int = ((((@as(c_int, 1189) * in1) + ((@as(c_int, 3920) - @as(c_int, 4096)) * in6)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in6;
    _ = &t6a;
    var t7a: c_int = (((((@as(c_int, 3920) - @as(c_int, 4096)) * in1) - (@as(c_int, 1189) * in6)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    _ = &t7a;
    const t0: c_int = iclip(t0a + t4a, min, max);
    _ = &t0;
    const t1: c_int = iclip(t1a + t5a, min, max);
    _ = &t1;
    var t2: c_int = iclip(t2a + t6a, min, max);
    _ = &t2;
    var t3: c_int = iclip(t3a + t7a, min, max);
    _ = &t3;
    const t4: c_int = iclip(t0a - t4a, min, max);
    _ = &t4;
    const t5: c_int = iclip(t1a - t5a, min, max);
    _ = &t5;
    var t6: c_int = iclip(t2a - t6a, min, max);
    _ = &t6;
    var t7: c_int = iclip(t3a - t7a, min, max);
    _ = &t7;
    t4a = (((((@as(c_int, 3784) - @as(c_int, 4096)) * t4) + (@as(c_int, 1567) * t5)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t4;
    t5a = ((((@as(c_int, 1567) * t4) - ((@as(c_int, 3784) - @as(c_int, 4096)) * t5)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t5;
    t6a = (((((@as(c_int, 3784) - @as(c_int, 4096)) * t7) - (@as(c_int, 1567) * t6)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t7;
    t7a = ((((@as(c_int, 1567) * t7) + ((@as(c_int, 3784) - @as(c_int, 4096)) * t6)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t6;
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * out_s)))] = iclip(t0 + t2, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * out_s)))] = -iclip(t1 + t3, min, max);
    t2 = iclip(t0 - t2, min, max);
    t3 = iclip(t1 - t3, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * out_s)))] = -iclip(t4a + t6a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * out_s)))] = iclip(t5a + t7a, min, max);
    t6 = iclip(t4a - t6a, min, max);
    t7 = iclip(t5a - t7a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * out_s)))] = -((((t2 + t3) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * out_s)))] = (((t2 - t3) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * out_s)))] = (((t6 + t7) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * out_s)))] = -((((t6 - t7) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
}
pub fn inv_adst16_1d_internal_c(in: [*c]const i32, in_s: ptrdiff_t, min: c_int, max: c_int, out: [*c]i32, out_s: ptrdiff_t) callconv(.c) void {
    _ = &in;
    _ = &in_s;
    _ = &min;
    _ = &max;
    _ = &out;
    _ = &out_s;
    while (true) {
        if (!((in_s > @as(ptrdiff_t, 0)) and (out_s != @as(ptrdiff_t, 0)))) {
            unreachable;
        }
        if (!false) break;
    }
    const in0: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * in_s)))];
    _ = &in0;
    const in1: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * in_s)))];
    _ = &in1;
    const in2: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * in_s)))];
    _ = &in2;
    const in3: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * in_s)))];
    _ = &in3;
    const in4: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * in_s)))];
    _ = &in4;
    const in5: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * in_s)))];
    _ = &in5;
    const in6: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * in_s)))];
    _ = &in6;
    const in7: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * in_s)))];
    _ = &in7;
    const in8: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * in_s)))];
    _ = &in8;
    const in9: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * in_s)))];
    _ = &in9;
    const in10: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * in_s)))];
    _ = &in10;
    const in11: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * in_s)))];
    _ = &in11;
    const in12: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * in_s)))];
    _ = &in12;
    const in13: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * in_s)))];
    _ = &in13;
    const in14: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * in_s)))];
    _ = &in14;
    const in15: c_int = in[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * in_s)))];
    _ = &in15;
    var t0: c_int = ((((in15 * (@as(c_int, 4091) - @as(c_int, 4096))) + (in0 * @as(c_int, 201))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in15;
    _ = &t0;
    var t1: c_int = ((((in15 * @as(c_int, 201)) - (in0 * (@as(c_int, 4091) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in0;
    _ = &t1;
    var t2: c_int = ((((in13 * (@as(c_int, 3973) - @as(c_int, 4096))) + (in2 * @as(c_int, 995))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in13;
    _ = &t2;
    var t3: c_int = ((((in13 * @as(c_int, 995)) - (in2 * (@as(c_int, 3973) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in2;
    _ = &t3;
    var t4: c_int = ((((in11 * (@as(c_int, 3703) - @as(c_int, 4096))) + (in4 * @as(c_int, 1751))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in11;
    _ = &t4;
    var t5: c_int = ((((in11 * @as(c_int, 1751)) - (in4 * (@as(c_int, 3703) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - in4;
    _ = &t5;
    var t6: c_int = (((in9 * @as(c_int, 1645)) + (in6 * @as(c_int, 1220))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    _ = &t6;
    var t7: c_int = (((in9 * @as(c_int, 1220)) - (in6 * @as(c_int, 1645))) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11));
    _ = &t7;
    var t8: c_int = ((((in7 * @as(c_int, 2751)) + (in8 * (@as(c_int, 3035) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in8;
    _ = &t8;
    var t9: c_int = ((((in7 * (@as(c_int, 3035) - @as(c_int, 4096))) - (in8 * @as(c_int, 2751))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in7;
    _ = &t9;
    var t10: c_int = ((((in5 * @as(c_int, 2106)) + (in10 * (@as(c_int, 3513) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in10;
    _ = &t10;
    var t11: c_int = ((((in5 * (@as(c_int, 3513) - @as(c_int, 4096))) - (in10 * @as(c_int, 2106))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in5;
    _ = &t11;
    var t12: c_int = ((((in3 * @as(c_int, 1380)) + (in12 * (@as(c_int, 3857) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in12;
    _ = &t12;
    var t13: c_int = ((((in3 * (@as(c_int, 3857) - @as(c_int, 4096))) - (in12 * @as(c_int, 1380))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in3;
    _ = &t13;
    var t14: c_int = ((((in1 * @as(c_int, 601)) + (in14 * (@as(c_int, 4052) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in14;
    _ = &t14;
    var t15: c_int = ((((in1 * (@as(c_int, 4052) - @as(c_int, 4096))) - (in14 * @as(c_int, 601))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + in1;
    _ = &t15;
    var t0a: c_int = iclip(t0 + t8, min, max);
    _ = &t0a;
    var t1a: c_int = iclip(t1 + t9, min, max);
    _ = &t1a;
    var t2a: c_int = iclip(t2 + t10, min, max);
    _ = &t2a;
    var t3a: c_int = iclip(t3 + t11, min, max);
    _ = &t3a;
    var t4a: c_int = iclip(t4 + t12, min, max);
    _ = &t4a;
    var t5a: c_int = iclip(t5 + t13, min, max);
    _ = &t5a;
    var t6a: c_int = iclip(t6 + t14, min, max);
    _ = &t6a;
    var t7a: c_int = iclip(t7 + t15, min, max);
    _ = &t7a;
    var t8a: c_int = iclip(t0 - t8, min, max);
    _ = &t8a;
    var t9a: c_int = iclip(t1 - t9, min, max);
    _ = &t9a;
    var t10a: c_int = iclip(t2 - t10, min, max);
    _ = &t10a;
    var t11a: c_int = iclip(t3 - t11, min, max);
    _ = &t11a;
    var t12a: c_int = iclip(t4 - t12, min, max);
    _ = &t12a;
    var t13a: c_int = iclip(t5 - t13, min, max);
    _ = &t13a;
    var t14a: c_int = iclip(t6 - t14, min, max);
    _ = &t14a;
    var t15a: c_int = iclip(t7 - t15, min, max);
    _ = &t15a;
    t8 = ((((t8a * (@as(c_int, 4017) - @as(c_int, 4096))) + (t9a * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t8a;
    t9 = ((((t8a * @as(c_int, 799)) - (t9a * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t9a;
    t10 = ((((t10a * @as(c_int, 2276)) + (t11a * (@as(c_int, 3406) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t11a;
    t11 = ((((t10a * (@as(c_int, 3406) - @as(c_int, 4096))) - (t11a * @as(c_int, 2276))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t10a;
    t12 = ((((t13a * (@as(c_int, 4017) - @as(c_int, 4096))) - (t12a * @as(c_int, 799))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t13a;
    t13 = ((((t13a * @as(c_int, 799)) + (t12a * (@as(c_int, 4017) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t12a;
    t14 = ((((t15a * @as(c_int, 2276)) - (t14a * (@as(c_int, 3406) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t14a;
    t15 = ((((t15a * (@as(c_int, 3406) - @as(c_int, 4096))) + (t14a * @as(c_int, 2276))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t15a;
    t0 = iclip(t0a + t4a, min, max);
    t1 = iclip(t1a + t5a, min, max);
    t2 = iclip(t2a + t6a, min, max);
    t3 = iclip(t3a + t7a, min, max);
    t4 = iclip(t0a - t4a, min, max);
    t5 = iclip(t1a - t5a, min, max);
    t6 = iclip(t2a - t6a, min, max);
    t7 = iclip(t3a - t7a, min, max);
    t8a = iclip(t8 + t12, min, max);
    t9a = iclip(t9 + t13, min, max);
    t10a = iclip(t10 + t14, min, max);
    t11a = iclip(t11 + t15, min, max);
    t12a = iclip(t8 - t12, min, max);
    t13a = iclip(t9 - t13, min, max);
    t14a = iclip(t10 - t14, min, max);
    t15a = iclip(t11 - t15, min, max);
    t4a = ((((t4 * (@as(c_int, 3784) - @as(c_int, 4096))) + (t5 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t4;
    t5a = ((((t4 * @as(c_int, 1567)) - (t5 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t5;
    t6a = ((((t7 * (@as(c_int, 3784) - @as(c_int, 4096))) - (t6 * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t7;
    t7a = ((((t7 * @as(c_int, 1567)) + (t6 * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t6;
    t12 = ((((t12a * (@as(c_int, 3784) - @as(c_int, 4096))) + (t13a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t12a;
    t13 = ((((t12a * @as(c_int, 1567)) - (t13a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) - t13a;
    t14 = ((((t15a * (@as(c_int, 3784) - @as(c_int, 4096))) - (t14a * @as(c_int, 1567))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t15a;
    t15 = ((((t15a * @as(c_int, 1567)) + (t14a * (@as(c_int, 3784) - @as(c_int, 4096)))) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12))) + t14a;
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * out_s)))] = iclip(t0 + t2, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 15) * out_s)))] = -iclip(t1 + t3, min, max);
    t2a = iclip(t0 - t2, min, max);
    t3a = iclip(t1 - t3, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * out_s)))] = -iclip(t4a + t6a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 12) * out_s)))] = iclip(t5a + t7a, min, max);
    t6 = iclip(t4a - t6a, min, max);
    t7 = iclip(t5a - t7a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * out_s)))] = -iclip(t8a + t10a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 14) * out_s)))] = iclip(t9a + t11a, min, max);
    t10 = iclip(t8a - t10a, min, max);
    t11 = iclip(t9a - t11a, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * out_s)))] = iclip(t12 + t14, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 13) * out_s)))] = -iclip(t13 + t15, min, max);
    t14a = iclip(t12 - t14, min, max);
    t15a = iclip(t13 - t15, min, max);
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 7) * out_s)))] = -((((t2a + t3a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 8) * out_s)))] = (((t2a - t3a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * out_s)))] = (((t6 + t7) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 11) * out_s)))] = -((((t6 - t7) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * out_s)))] = (((t10 + t11) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 9) * out_s)))] = -((((t10 - t11) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 5) * out_s)))] = -((((t14a + t15a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8)));
    out[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 10) * out_s)))] = (((t14a - t15a) * @as(c_int, 181)) + @as(c_int, 128)) >> @intCast(@as(c_int, 8));
}
pub fn inv_adst4_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst4_1d_internal_c(c, stride, min, max, c, stride);
}
pub fn inv_flipadst4_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst4_1d_internal_c(c, stride, min, max, &c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 4) - @as(c_int, 1)) * stride)))], -stride);
}
pub fn inv_adst8_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst8_1d_internal_c(c, stride, min, max, c, stride);
}
pub fn inv_flipadst8_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst8_1d_internal_c(c, stride, min, max, &c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 8) - @as(c_int, 1)) * stride)))], -stride);
}
pub fn inv_adst16_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst16_1d_internal_c(c, stride, min, max, c, stride);
}
pub fn inv_flipadst16_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    inv_adst16_1d_internal_c(c, stride, min, max, &c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 16) - @as(c_int, 1)) * stride)))], -stride);
}
pub fn inv_identity4_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 4)) : (i += 1) {
            const in: c_int = c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))];
            _ = &in;
            c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))] = in + (((in * @as(c_int, 1697)) + @as(c_int, 2048)) >> @intCast(@as(c_int, 12)));
        }
    }
}
pub fn inv_identity8_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 8)) : (i += 1) {
            c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))] *= 2;
        }
    }
}
pub fn inv_identity16_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 16)) : (i += 1) {
            const in: c_int = c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))];
            _ = &in;
            c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))] = (@as(c_int, 2) * in) + (((in * @as(c_int, 1697)) + @as(c_int, 1024)) >> @intCast(@as(c_int, 11)));
        }
    }
}
pub fn inv_identity32_1d_c(c: [*c]i32, stride: ptrdiff_t, min: c_int, max: c_int) callconv(.c) void {
    _ = &c;
    _ = &stride;
    _ = &min;
    _ = &max;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 32)) : (i += 1) {
            c[@bitCast(@as(isize, @intCast(stride * @as(ptrdiff_t, i))))] *= 4;
        }
    }
}
pub export var antfly_av1_dav1d_tx1d_fns: [5][4]itx_1d_fn = [5][4]itx_1d_fn{
    [4]itx_1d_fn{
        inv_dct4_1d_c,
        inv_adst4_1d_c,
        inv_identity4_1d_c,
        inv_flipadst4_1d_c,
    },
    [4]itx_1d_fn{
        inv_dct8_1d_c,
        inv_adst8_1d_c,
        inv_identity8_1d_c,
        inv_flipadst8_1d_c,
    },
    [4]itx_1d_fn{
        inv_dct16_1d_c,
        inv_adst16_1d_c,
        inv_identity16_1d_c,
        inv_flipadst16_1d_c,
    },
    [1]itx_1d_fn{
        inv_dct32_1d_c,
    } ++ @as([1]itx_1d_fn, @splat(null)) ++ [1]itx_1d_fn{
        inv_identity32_1d_c,
    } ++ @as([1]itx_1d_fn, @splat(null)),
    [1]itx_1d_fn{
        inv_dct64_1d_c,
    } ++ @as([3]itx_1d_fn, @splat(null)),
};
pub export var antfly_av1_dav1d_tx1d_types: [16][2]u8 = [16][2]u8{
    [2]u8{
        DCT,
        DCT,
    },
    [2]u8{
        ADST,
        DCT,
    },
    [2]u8{
        DCT,
        ADST,
    },
    [2]u8{
        ADST,
        ADST,
    },
    [2]u8{
        FLIPADST,
        DCT,
    },
    [2]u8{
        DCT,
        FLIPADST,
    },
    [2]u8{
        FLIPADST,
        FLIPADST,
    },
    [2]u8{
        ADST,
        FLIPADST,
    },
    [2]u8{
        FLIPADST,
        ADST,
    },
    [2]u8{
        IDENTITY,
        IDENTITY,
    },
    [2]u8{
        DCT,
        IDENTITY,
    },
    [2]u8{
        IDENTITY,
        DCT,
    },
    [2]u8{
        ADST,
        IDENTITY,
    },
    [2]u8{
        IDENTITY,
        ADST,
    },
    [2]u8{
        FLIPADST,
        IDENTITY,
    },
    [2]u8{
        IDENTITY,
        FLIPADST,
    },
};
pub export fn antfly_av1_dav1d_inv_wht4_1d_c(c: [*c]i32, stride: ptrdiff_t) void {
    _ = &c;
    _ = &stride;
    while (true) {
        if (!(stride > @as(ptrdiff_t, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const in0: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))];
    _ = &in0;
    const in1: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))];
    _ = &in1;
    const in2: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))];
    _ = &in2;
    const in3: c_int = c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))];
    _ = &in3;
    const t0: c_int = in0 + in1;
    _ = &t0;
    const t2: c_int = in2 - in3;
    _ = &t2;
    const t4: c_int = (t0 - t2) >> @intCast(@as(c_int, 1));
    _ = &t4;
    const t3: c_int = t4 - in3;
    _ = &t3;
    const t1: c_int = t4 - in1;
    _ = &t1;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 0) * stride)))] = t0 - t3;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 1) * stride)))] = t3;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 2) * stride)))] = t1;
    c[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, 3) * stride)))] = t2 + t1;
}
