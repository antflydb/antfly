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
pub extern fn antfly_codec_abs(c_int) c_int;
pub extern fn antfly_codec_llabs(c_longlong) c_longlong;
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub extern fn antfly_codec_clzll(mask: c_ulonglong) callconv(.c) c_int;
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
pub fn apply_sign64(v: c_int, s: i64) callconv(.c) c_int {
    _ = &v;
    _ = &s;
    return if (s < @as(i64, 0)) -v else v;
}
pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
}
pub fn u64log2(v: u64) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 63) ^ antfly_codec_clzll(v);
}
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
const struct_unnamed_18 = extern struct {
    y: i16 = 0,
    x: i16 = 0,
};
pub const union_mv = extern union {
    unnamed_0: struct_unnamed_18,
    n: u32,
};
pub const mv = union_mv;
pub const div_lut: [257]u16 = [257]u16{
    16384,
    16320,
    16257,
    16194,
    16132,
    16070,
    16009,
    15948,
    15888,
    15828,
    15768,
    15709,
    15650,
    15592,
    15534,
    15477,
    15420,
    15364,
    15308,
    15252,
    15197,
    15142,
    15087,
    15033,
    14980,
    14926,
    14873,
    14821,
    14769,
    14717,
    14665,
    14614,
    14564,
    14513,
    14463,
    14413,
    14364,
    14315,
    14266,
    14218,
    14170,
    14122,
    14075,
    14028,
    13981,
    13935,
    13888,
    13843,
    13797,
    13752,
    13707,
    13662,
    13618,
    13574,
    13530,
    13487,
    13443,
    13400,
    13358,
    13315,
    13273,
    13231,
    13190,
    13148,
    13107,
    13066,
    13026,
    12985,
    12945,
    12906,
    12866,
    12827,
    12788,
    12749,
    12710,
    12672,
    12633,
    12596,
    12558,
    12520,
    12483,
    12446,
    12409,
    12373,
    12336,
    12300,
    12264,
    12228,
    12193,
    12157,
    12122,
    12087,
    12053,
    12018,
    11984,
    11950,
    11916,
    11882,
    11848,
    11815,
    11782,
    11749,
    11716,
    11683,
    11651,
    11619,
    11586,
    11555,
    11523,
    11491,
    11460,
    11429,
    11398,
    11367,
    11336,
    11305,
    11275,
    11245,
    11215,
    11185,
    11155,
    11125,
    11096,
    11067,
    11038,
    11009,
    10980,
    10951,
    10923,
    10894,
    10866,
    10838,
    10810,
    10782,
    10755,
    10727,
    10700,
    10673,
    10645,
    10618,
    10592,
    10565,
    10538,
    10512,
    10486,
    10460,
    10434,
    10408,
    10382,
    10356,
    10331,
    10305,
    10280,
    10255,
    10230,
    10205,
    10180,
    10156,
    10131,
    10107,
    10082,
    10058,
    10034,
    10010,
    9986,
    9963,
    9939,
    9916,
    9892,
    9869,
    9846,
    9823,
    9800,
    9777,
    9754,
    9732,
    9709,
    9687,
    9664,
    9642,
    9620,
    9598,
    9576,
    9554,
    9533,
    9511,
    9489,
    9468,
    9447,
    9425,
    9404,
    9383,
    9362,
    9341,
    9321,
    9300,
    9279,
    9259,
    9239,
    9218,
    9198,
    9178,
    9158,
    9138,
    9118,
    9098,
    9079,
    9059,
    9039,
    9020,
    9001,
    8981,
    8962,
    8943,
    8924,
    8905,
    8886,
    8867,
    8849,
    8830,
    8812,
    8793,
    8775,
    8756,
    8738,
    8720,
    8702,
    8684,
    8666,
    8648,
    8630,
    8613,
    8595,
    8577,
    8560,
    8542,
    8525,
    8508,
    8490,
    8473,
    8456,
    8439,
    8422,
    8405,
    8389,
    8372,
    8355,
    8339,
    8322,
    8306,
    8289,
    8273,
    8257,
    8240,
    8224,
    8208,
    8192,
};
pub fn iclip_wmp(v: c_int) callconv(.c) c_int {
    _ = &v;
    const cv: c_int = iclip(v, -@as(c_int, 32768), 32767);
    _ = &cv;
    return apply_sign((antfly_codec_abs(cv) + @as(c_int, 32)) >> @intCast(@as(c_int, 6)), cv) * (@as(c_int, 1) << @intCast(@as(c_int, 6)));
}
pub fn resolve_divisor_32(d: c_uint, shift: [*c]c_int) callconv(.c) c_int {
    _ = &d;
    _ = &shift;
    shift.* = ulog2(d);
    const e: c_int = @bitCast(@as(c_uint, @truncate(d -% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(shift.*)))))));
    _ = &e;
    const f: c_int = if (shift.* > @as(c_int, 8)) (e + (@as(c_int, 1) << @intCast(shift.* - @as(c_int, 9)))) >> @intCast(shift.* - @as(c_int, 8)) else e << @intCast(@as(c_int, 8) - shift.*);
    _ = &f;
    while (true) {
        if (!(f <= @as(c_int, 256))) {
            unreachable;
        }
        if (!false) break;
    }
    shift.* += 14;
    return div_lut[@bitCast(@as(isize, @intCast(f)))];
}
pub export fn antfly_av1_dav1d_get_shear_params(wm: [*c]Dav1dWarpedMotionParams) c_int {
    _ = &wm;
    const mat: [*c]const i32 = @ptrCast(@alignCast(&wm.*.matrix));
    _ = &mat;
    if (mat[@as(c_int, 2)] <= @as(c_int, 0)) return 1;
    wm.*.u.p.alpha = @truncate(iclip_wmp(mat[@as(c_int, 2)] - @as(c_int, 65536)));
    wm.*.u.p.beta = @truncate(iclip_wmp(mat[@as(c_int, 3)]));
    var shift: c_int = undefined;
    _ = &shift;
    const y: c_int = apply_sign(resolve_divisor_32(@bitCast(@as(c_int, antfly_codec_abs(mat[@as(c_int, 2)]))), &shift), mat[@as(c_int, 2)]);
    _ = &y;
    const v1: i64 = (@as(i64, mat[@as(c_int, 4)]) * @as(i64, 65536)) * @as(i64, y);
    _ = &v1;
    const rnd: c_int = (@as(c_int, 1) << @intCast(shift)) >> @intCast(@as(c_int, 1));
    _ = &rnd;
    wm.*.u.p.gamma = @truncate(iclip_wmp(apply_sign64(@truncate((antfly_codec_llabs(v1) + @as(c_longlong, rnd)) >> @intCast(@as(c_longlong, shift))), v1)));
    const v2: i64 = (@as(i64, mat[@as(c_int, 3)]) * @as(i64, mat[@as(c_int, 4)])) * @as(i64, y);
    _ = &v2;
    wm.*.u.p.delta = @truncate(iclip_wmp((mat[@as(c_int, 5)] - apply_sign64(@truncate((antfly_codec_llabs(v2) + @as(c_longlong, rnd)) >> @intCast(@as(c_longlong, shift))), v2)) - @as(c_int, 65536)));
    return @as(c_int, @intFromBool((((@as(c_int, 4) * antfly_codec_abs(wm.*.u.p.alpha)) + (@as(c_int, 7) * antfly_codec_abs(wm.*.u.p.beta))) >= @as(c_int, 65536)) or (((@as(c_int, 4) * antfly_codec_abs(wm.*.u.p.gamma)) + (@as(c_int, 4) * antfly_codec_abs(wm.*.u.p.delta))) >= @as(c_int, 65536))));
}
pub fn resolve_divisor_64(d: u64, shift: [*c]c_int) callconv(.c) c_int {
    _ = &d;
    _ = &shift;
    shift.* = u64log2(d);
    const e: i64 = @bitCast(@as(c_ulonglong, @truncate(d -% @as(u64, @bitCast(@as(c_longlong, @as(c_longlong, 1) << @intCast(@as(c_longlong, shift.*))))))));
    _ = &e;
    const f: i64 = if (shift.* > @as(c_int, 8)) (e + (@as(c_longlong, 1) << @intCast(@as(c_longlong, shift.* - @as(c_int, 9))))) >> @intCast(@as(i64, shift.* - @as(c_int, 8))) else e << @intCast(@as(i64, @as(c_int, 8) - shift.*));
    _ = &f;
    while (true) {
        if (!(f <= @as(i64, 256))) {
            unreachable;
        }
        if (!false) break;
    }
    shift.* += 14;
    return div_lut[@bitCast(@as(isize, @intCast(f)))];
}
pub fn get_mult_shift_ndiag(px: i64, idet: c_int, shift: c_int) callconv(.c) c_int {
    _ = &px;
    _ = &idet;
    _ = &shift;
    const v1: i64 = px * @as(i64, idet);
    _ = &v1;
    const v2: c_int = apply_sign64(@truncate((antfly_codec_llabs(v1) + ((@as(c_longlong, 1) << @intCast(@as(c_longlong, shift))) >> @intCast(@as(c_longlong, 1)))) >> @intCast(@as(c_longlong, shift))), v1);
    _ = &v2;
    return iclip(v2, -@as(c_int, 8191), 8191);
}
pub fn get_mult_shift_diag(px: i64, idet: c_int, shift: c_int) callconv(.c) c_int {
    _ = &px;
    _ = &idet;
    _ = &shift;
    const v1: i64 = px * @as(i64, idet);
    _ = &v1;
    const v2: c_int = apply_sign64(@truncate((antfly_codec_llabs(v1) + ((@as(c_longlong, 1) << @intCast(@as(c_longlong, shift))) >> @intCast(@as(c_longlong, 1)))) >> @intCast(@as(c_longlong, shift))), v1);
    _ = &v2;
    return iclip(v2, 57345, 73727);
}
pub export fn antfly_av1_dav1d_set_affine_mv2d(bw4: c_int, bh4: c_int, mv_1: mv, wm: [*c]Dav1dWarpedMotionParams, bx4: c_int, by4: c_int) void {
    _ = &bw4;
    _ = &bh4;
    _ = &mv_1;
    _ = &wm;
    _ = &bx4;
    _ = &by4;
    const mat: [*c]i32 = @ptrCast(@alignCast(&wm.*.matrix));
    _ = &mat;
    const rsuy: c_int = (@as(c_int, 2) * bh4) - @as(c_int, 1);
    _ = &rsuy;
    const rsux: c_int = (@as(c_int, 2) * bw4) - @as(c_int, 1);
    _ = &rsux;
    const isuy: c_int = (by4 * @as(c_int, 4)) + rsuy;
    _ = &isuy;
    const isux: c_int = (bx4 * @as(c_int, 4)) + rsux;
    _ = &isux;
    mat[@as(c_int, 0)] = iclip((@as(c_int, mv_1.unnamed_0.x) * @as(c_int, 8192)) - ((isux * (mat[@as(c_int, 2)] - @as(c_int, 65536))) + (isuy * mat[@as(c_int, 3)])), -@as(c_int, 8388608), 8388607);
    mat[@as(c_int, 1)] = iclip((@as(c_int, mv_1.unnamed_0.y) * @as(c_int, 8192)) - ((isux * mat[@as(c_int, 4)]) + (isuy * (mat[@as(c_int, 5)] - @as(c_int, 65536)))), -@as(c_int, 8388608), 8388607);
}
pub export fn antfly_av1_dav1d_find_affine_int(arg_pts: [*c][2][2]c_int, np: c_int, bw4: c_int, bh4: c_int, mv_1: mv, wm: [*c]Dav1dWarpedMotionParams, bx4: c_int, by4: c_int) c_int {
    var pts = arg_pts;
    _ = &pts;
    _ = &np;
    _ = &bw4;
    _ = &bh4;
    _ = &mv_1;
    _ = &wm;
    _ = &bx4;
    _ = &by4;
    const mat: [*c]i32 = @ptrCast(@alignCast(&wm.*.matrix));
    _ = &mat;
    var a: [2][2]c_int = [2][2]c_int{
        [2]c_int{
            0,
            0,
        },
        [2]c_int{
            0,
            0,
        },
    };
    _ = &a;
    var bx: [2]c_int = [2]c_int{
        0,
        0,
    };
    _ = &bx;
    var by: [2]c_int = [2]c_int{
        0,
        0,
    };
    _ = &by;
    const rsuy: c_int = (@as(c_int, 2) * bh4) - @as(c_int, 1);
    _ = &rsuy;
    const rsux: c_int = (@as(c_int, 2) * bw4) - @as(c_int, 1);
    _ = &rsux;
    const suy: c_int = rsuy * @as(c_int, 8);
    _ = &suy;
    const sux: c_int = rsux * @as(c_int, 8);
    _ = &sux;
    const duy: c_int = suy + @as(c_int, mv_1.unnamed_0.y);
    _ = &duy;
    const dux: c_int = sux + @as(c_int, mv_1.unnamed_0.x);
    _ = &dux;
    const isuy: c_int = (by4 * @as(c_int, 4)) + rsuy;
    _ = &isuy;
    const isux: c_int = (bx4 * @as(c_int, 4)) + rsux;
    _ = &isux;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < np) : (i += 1) {
            const dx: c_int = pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 0)] - dux;
            _ = &dx;
            const dy: c_int = pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 1)][@as(c_int, 1)] - duy;
            _ = &dy;
            const sx: c_int = pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 0)] - sux;
            _ = &sx;
            const sy: c_int = pts[@bitCast(@as(isize, @intCast(i)))][@as(c_int, 0)][@as(c_int, 1)] - suy;
            _ = &sy;
            if ((antfly_codec_abs(sx - dx) < @as(c_int, 256)) and (antfly_codec_abs(sy - dy) < @as(c_int, 256))) {
                a[@as(c_int, 0)][@as(c_int, 0)] += (((sx * sx) >> @intCast(@as(c_int, 2))) + (sx * @as(c_int, 2))) + @as(c_int, 8);
                a[@as(c_int, 0)][@as(c_int, 1)] += ((((sx * sy) >> @intCast(@as(c_int, 2))) + sx) + sy) + @as(c_int, 4);
                a[@as(c_int, 1)][@as(c_int, 1)] += (((sy * sy) >> @intCast(@as(c_int, 2))) + (sy * @as(c_int, 2))) + @as(c_int, 8);
                bx[@as(c_int, 0)] += ((((sx * dx) >> @intCast(@as(c_int, 2))) + sx) + dx) + @as(c_int, 8);
                bx[@as(c_int, 1)] += ((((sy * dx) >> @intCast(@as(c_int, 2))) + sy) + dx) + @as(c_int, 4);
                by[@as(c_int, 0)] += ((((sx * dy) >> @intCast(@as(c_int, 2))) + sx) + dy) + @as(c_int, 4);
                by[@as(c_int, 1)] += ((((sy * dy) >> @intCast(@as(c_int, 2))) + sy) + dy) + @as(c_int, 8);
            }
        }
    }
    const det: i64 = (@as(i64, a[@as(c_int, 0)][@as(c_int, 0)]) * @as(i64, a[@as(c_int, 1)][@as(c_int, 1)])) - (@as(i64, a[@as(c_int, 0)][@as(c_int, 1)]) * @as(i64, a[@as(c_int, 0)][@as(c_int, 1)]));
    _ = &det;
    if (det == @as(i64, 0)) return 1;
    var shift: c_int = undefined;
    _ = &shift;
    var idet: c_int = apply_sign64(resolve_divisor_64(@bitCast(@as(c_longlong, antfly_codec_llabs(det))), &shift), det);
    _ = &idet;
    shift -= 16;
    if (shift < @as(c_int, 0)) {
        idet <<= @intCast(-shift);
        shift = 0;
    }
    mat[@as(c_int, 2)] = get_mult_shift_diag((@as(i64, a[@as(c_int, 1)][@as(c_int, 1)]) * @as(i64, bx[@as(c_int, 0)])) - (@as(i64, a[@as(c_int, 0)][@as(c_int, 1)]) * @as(i64, bx[@as(c_int, 1)])), idet, shift);
    mat[@as(c_int, 3)] = get_mult_shift_ndiag((@as(i64, a[@as(c_int, 0)][@as(c_int, 0)]) * @as(i64, bx[@as(c_int, 1)])) - (@as(i64, a[@as(c_int, 0)][@as(c_int, 1)]) * @as(i64, bx[@as(c_int, 0)])), idet, shift);
    mat[@as(c_int, 4)] = get_mult_shift_ndiag((@as(i64, a[@as(c_int, 1)][@as(c_int, 1)]) * @as(i64, by[@as(c_int, 0)])) - (@as(i64, a[@as(c_int, 0)][@as(c_int, 1)]) * @as(i64, by[@as(c_int, 1)])), idet, shift);
    mat[@as(c_int, 5)] = get_mult_shift_diag((@as(i64, a[@as(c_int, 0)][@as(c_int, 0)]) * @as(i64, by[@as(c_int, 1)])) - (@as(i64, a[@as(c_int, 0)][@as(c_int, 1)]) * @as(i64, by[@as(c_int, 0)])), idet, shift);
    mat[@as(c_int, 0)] = iclip((@as(c_int, mv_1.unnamed_0.x) * @as(c_int, 8192)) - ((isux * (mat[@as(c_int, 2)] - @as(c_int, 65536))) + (isuy * mat[@as(c_int, 3)])), -@as(c_int, 8388608), 8388607);
    mat[@as(c_int, 1)] = iclip((@as(c_int, mv_1.unnamed_0.y) * @as(c_int, 8192)) - ((isux * mat[@as(c_int, 4)]) + (isuy * (mat[@as(c_int, 5)] - @as(c_int, 65536)))), -@as(c_int, 8388608), 8388607);
    return 0;
}
