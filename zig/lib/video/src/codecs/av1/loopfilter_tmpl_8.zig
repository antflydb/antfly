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
pub extern fn antfly_codec_abs(c_int) c_int;
pub const ptrdiff_t = common.ptrdiff_t;
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
pub const Av1FilterLUT = common.Av1FilterLUT;
pub const loopfilter_sb_fn = ?*const fn (dst: [*c]pixel, stride: ptrdiff_t, mask: [*c]const u32, lvl: [*c][4]u8, lvl_stride: ptrdiff_t, lut: [*c]const Av1FilterLUT, w: c_int) callconv(.c) void;
pub const struct_Dav1dLoopFilterDSPContext = extern struct {
    loop_filter_sb: [2][2]loopfilter_sb_fn = @import("std").mem.zeroes([2][2]loopfilter_sb_fn),
};
pub const Dav1dLoopFilterDSPContext = struct_Dav1dLoopFilterDSPContext;
pub fn loop_filter(arg_dst: [*c]pixel, arg_E: c_int, arg_I: c_int, arg_H: c_int, stridea: ptrdiff_t, strideb: ptrdiff_t, wd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var E = arg_E;
    _ = &E;
    var I = arg_I;
    _ = &I;
    var H = arg_H;
    _ = &H;
    _ = &stridea;
    _ = &strideb;
    _ = &wd;
    const bitdepth_min_8: c_int = @as(c_int, 8) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    const F: c_int = @as(c_int, 1) << @intCast(bitdepth_min_8);
    _ = &F;
    E <<= @intCast(bitdepth_min_8);
    I <<= @intCast(bitdepth_min_8);
    H <<= @intCast(bitdepth_min_8);
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 4)) : ({
            i += 1;
            dst += @as(usize, @bitCast(@as(isize, @intCast(stridea))));
        }) {
            var p6: c_int = undefined;
            _ = &p6;
            var p5: c_int = undefined;
            _ = &p5;
            var p4: c_int = undefined;
            _ = &p4;
            var p3: c_int = undefined;
            _ = &p3;
            var p2: c_int = undefined;
            _ = &p2;
            var p1: c_int = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 2)))))];
            _ = &p1;
            var p0: c_int = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))];
            _ = &p0;
            var q0: c_int = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))];
            _ = &q0;
            var q1: c_int = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 1))))];
            _ = &q1;
            var q2: c_int = undefined;
            _ = &q2;
            var q3: c_int = undefined;
            _ = &q3;
            var q4: c_int = undefined;
            _ = &q4;
            var q5: c_int = undefined;
            _ = &q5;
            var q6: c_int = undefined;
            _ = &q6;
            var fm: c_int = undefined;
            _ = &fm;
            var flat8out: c_int = undefined;
            _ = &flat8out;
            var flat8in: c_int = undefined;
            _ = &flat8in;
            fm = @as(c_int, @intFromBool(((antfly_codec_abs(p1 - p0) <= I) and (antfly_codec_abs(q1 - q0) <= I)) and (((antfly_codec_abs(p0 - q0) * @as(c_int, 2)) + (antfly_codec_abs(p1 - q1) >> @intCast(@as(c_int, 1)))) <= E)));
            if (wd > @as(c_int, 4)) {
                p2 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 3)))))];
                q2 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 2))))];
                fm &= @as(c_int, @intFromBool((antfly_codec_abs(p2 - p1) <= I) and (antfly_codec_abs(q2 - q1) <= I)));
                if (wd > @as(c_int, 6)) {
                    p3 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 4)))))];
                    q3 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 3))))];
                    fm &= @as(c_int, @intFromBool((antfly_codec_abs(p3 - p2) <= I) and (antfly_codec_abs(q3 - q2) <= I)));
                }
            }
            if (!(fm != 0)) continue;
            if (wd >= @as(c_int, 16)) {
                p6 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 7)))))];
                p5 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 6)))))];
                p4 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 5)))))];
                q4 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 4))))];
                q5 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 5))))];
                q6 = dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 6))))];
                flat8out = @as(c_int, @intFromBool((((((antfly_codec_abs(p6 - p0) <= F) and (antfly_codec_abs(p5 - p0) <= F)) and (antfly_codec_abs(p4 - p0) <= F)) and (antfly_codec_abs(q4 - q0) <= F)) and (antfly_codec_abs(q5 - q0) <= F)) and (antfly_codec_abs(q6 - q0) <= F)));
            }
            if (wd >= @as(c_int, 6)) {
                flat8in = @as(c_int, @intFromBool((((antfly_codec_abs(p2 - p0) <= F) and (antfly_codec_abs(p1 - p0) <= F)) and (antfly_codec_abs(q1 - q0) <= F)) and (antfly_codec_abs(q2 - q0) <= F)));
            }
            if (wd >= @as(c_int, 8)) {
                flat8in &= @as(c_int, @intFromBool((antfly_codec_abs(p3 - p0) <= F) and (antfly_codec_abs(q3 - q0) <= F)));
            }
            if ((wd >= @as(c_int, 16)) and ((flat8out & flat8in) != 0)) {
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 6)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p6) + p6) + p6) + p6) + (p6 * @as(c_int, 2))) + (p5 * @as(c_int, 2))) + (p4 * @as(c_int, 2))) + p3) + p2) + p1) + p0) + q0) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 5)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p6) + p6) + p6) + p6) + (p5 * @as(c_int, 2))) + (p4 * @as(c_int, 2))) + (p3 * @as(c_int, 2))) + p2) + p1) + p0) + q0) + q1) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 4)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p6) + p6) + p6) + p5) + (p4 * @as(c_int, 2))) + (p3 * @as(c_int, 2))) + (p2 * @as(c_int, 2))) + p1) + p0) + q0) + q1) + q2) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 3)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p6) + p6) + p5) + p4) + (p3 * @as(c_int, 2))) + (p2 * @as(c_int, 2))) + (p1 * @as(c_int, 2))) + p0) + q0) + q1) + q2) + q3) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 2)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p6) + p5) + p4) + p3) + (p2 * @as(c_int, 2))) + (p1 * @as(c_int, 2))) + (p0 * @as(c_int, 2))) + q0) + q1) + q2) + q3) + q4) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate((((((((((((((p6 + p5) + p4) + p3) + p2) + (p1 * @as(c_int, 2))) + (p0 * @as(c_int, 2))) + (q0 * @as(c_int, 2))) + q1) + q2) + q3) + q4) + q5) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))] = @bitCast(@as(i8, @truncate((((((((((((((p5 + p4) + p3) + p2) + p1) + (p0 * @as(c_int, 2))) + (q0 * @as(c_int, 2))) + (q1 * @as(c_int, 2))) + q2) + q3) + q4) + q5) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 1))))] = @bitCast(@as(i8, @truncate((((((((((((((p4 + p3) + p2) + p1) + p0) + (q0 * @as(c_int, 2))) + (q1 * @as(c_int, 2))) + (q2 * @as(c_int, 2))) + q3) + q4) + q5) + q6) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 2))))] = @bitCast(@as(i8, @truncate((((((((((((((p3 + p2) + p1) + p0) + q0) + (q1 * @as(c_int, 2))) + (q2 * @as(c_int, 2))) + (q3 * @as(c_int, 2))) + q4) + q5) + q6) + q6) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 3))))] = @bitCast(@as(i8, @truncate((((((((((((((p2 + p1) + p0) + q0) + q1) + (q2 * @as(c_int, 2))) + (q3 * @as(c_int, 2))) + (q4 * @as(c_int, 2))) + q5) + q6) + q6) + q6) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 4))))] = @bitCast(@as(i8, @truncate((((((((((((((p1 + p0) + q0) + q1) + q2) + (q3 * @as(c_int, 2))) + (q4 * @as(c_int, 2))) + (q5 * @as(c_int, 2))) + q6) + q6) + q6) + q6) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 5))))] = @bitCast(@as(i8, @truncate((((((((((((((p0 + q0) + q1) + q2) + q3) + (q4 * @as(c_int, 2))) + (q5 * @as(c_int, 2))) + (q6 * @as(c_int, 2))) + q6) + q6) + q6) + q6) + q6) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
            } else if ((wd >= @as(c_int, 8)) and (flat8in != 0)) {
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 3)))))] = @bitCast(@as(i8, @truncate((((((((p3 + p3) + p3) + (@as(c_int, 2) * p2)) + p1) + p0) + q0) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 2)))))] = @bitCast(@as(i8, @truncate((((((((p3 + p3) + p2) + (@as(c_int, 2) * p1)) + p0) + q0) + q1) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate((((((((p3 + p2) + p1) + (@as(c_int, 2) * p0)) + q0) + q1) + q2) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))] = @bitCast(@as(i8, @truncate((((((((p2 + p1) + p0) + (@as(c_int, 2) * q0)) + q1) + q2) + q3) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 1))))] = @bitCast(@as(i8, @truncate((((((((p1 + p0) + q0) + (@as(c_int, 2) * q1)) + q2) + q3) + q3) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 2))))] = @bitCast(@as(i8, @truncate((((((((p0 + q0) + q1) + (@as(c_int, 2) * q2)) + q3) + q3) + q3) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
            } else if ((wd == @as(c_int, 6)) and (flat8in != 0)) {
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 2)))))] = @bitCast(@as(i8, @truncate((((((p2 + (@as(c_int, 2) * p2)) + (@as(c_int, 2) * p1)) + (@as(c_int, 2) * p0)) + q0) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate((((((p2 + (@as(c_int, 2) * p1)) + (@as(c_int, 2) * p0)) + (@as(c_int, 2) * q0)) + q1) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))] = @bitCast(@as(i8, @truncate((((((p1 + (@as(c_int, 2) * p0)) + (@as(c_int, 2) * q0)) + (@as(c_int, 2) * q1)) + q2) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
                dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 1))))] = @bitCast(@as(i8, @truncate((((((p0 + (@as(c_int, 2) * q0)) + (@as(c_int, 2) * q1)) + (@as(c_int, 2) * q2)) + q2) + @as(c_int, 4)) >> @intCast(@as(c_int, 3)))));
            } else {
                const hev: c_int = @as(c_int, @intFromBool((antfly_codec_abs(p1 - p0) > H) or (antfly_codec_abs(q1 - q0) > H)));
                _ = &hev;
                if (hev != 0) {
                    var f: c_int = iclip(p1 - q1, -@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8)), (@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8))) - @as(c_int, 1));
                    _ = &f;
                    var f1: c_int = undefined;
                    _ = &f1;
                    var f2: c_int = undefined;
                    _ = &f2;
                    f = iclip((@as(c_int, 3) * (q0 - p0)) + f, -@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8)), (@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8))) - @as(c_int, 1));
                    f1 = imin(f + @as(c_int, 4), (@as(c_int, 128) << @intCast(bitdepth_min_8)) - @as(c_int, 1)) >> @intCast(@as(c_int, 3));
                    f2 = imin(f + @as(c_int, 3), (@as(c_int, 128) << @intCast(bitdepth_min_8)) - @as(c_int, 1)) >> @intCast(@as(c_int, 3));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate(iclip_u8(p0 + f2))));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))] = @bitCast(@as(i8, @truncate(iclip_u8(q0 - f1))));
                } else {
                    var f: c_int = iclip(@as(c_int, 3) * (q0 - p0), -@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8)), (@as(c_int, 128) * (@as(c_int, 1) << @intCast(bitdepth_min_8))) - @as(c_int, 1));
                    _ = &f;
                    var f1: c_int = undefined;
                    _ = &f1;
                    var f2: c_int = undefined;
                    _ = &f2;
                    f1 = imin(f + @as(c_int, 4), (@as(c_int, 128) << @intCast(bitdepth_min_8)) - @as(c_int, 1)) >> @intCast(@as(c_int, 3));
                    f2 = imin(f + @as(c_int, 3), (@as(c_int, 128) << @intCast(bitdepth_min_8)) - @as(c_int, 1)) >> @intCast(@as(c_int, 3));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 1)))))] = @bitCast(@as(i8, @truncate(iclip_u8(p0 + f2))));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 0))))] = @bitCast(@as(i8, @truncate(iclip_u8(q0 - f1))));
                    f = (f1 + @as(c_int, 1)) >> @intCast(@as(c_int, 1));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, -@as(c_int, 2)))))] = @bitCast(@as(i8, @truncate(iclip_u8(p1 + f))));
                    dst[@bitCast(@as(isize, @intCast(strideb * @as(ptrdiff_t, 1))))] = @bitCast(@as(i8, @truncate(iclip_u8(q1 - f))));
                }
            }
        }
    }
}
pub fn loop_filter_h_sb128y_c(arg_dst: [*c]pixel, stride: ptrdiff_t, vmask: [*c]const u32, arg_l: [*c][4]u8, arg_b4_stride: ptrdiff_t, arg_lut: [*c]const Av1FilterLUT, h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &vmask;
    var l = arg_l;
    _ = &l;
    var b4_stride = arg_b4_stride;
    _ = &b4_stride;
    var lut = arg_lut;
    _ = &lut;
    _ = &h;
    const vm: c_uint = (vmask[@as(c_int, 0)] | vmask[@as(c_int, 1)]) | vmask[@as(c_int, 2)];
    _ = &vm;
    {
        var y: c_uint = 1;
        _ = &y;
        while ((vm & ~(y -% @as(c_uint, 1))) != 0) : ({
            y <<= @intCast(@as(c_uint, 1));
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride))));
            l += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }) {
            if ((vm & y) != 0) {
                const L: c_int = if (l[@as(c_int, 0)][@as(c_int, 0)] != 0) @as(c_int, l[@as(c_int, 0)][@as(c_int, 0)]) else @as(c_int, l[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_int, 0)]);
                _ = &L;
                if (!(L != 0)) continue;
                const H: c_int = L >> @intCast(@as(c_int, 4));
                _ = &H;
                const E: c_int = lut.*.e[@bitCast(@as(isize, @intCast(L)))];
                _ = &E;
                const I: c_int = lut.*.i[@bitCast(@as(isize, @intCast(L)))];
                _ = &I;
                const idx: c_int = if ((vmask[@as(c_int, 2)] & y) != 0) @as(c_int, 2) else @as(c_int, @intFromBool(!!((vmask[@as(c_int, 1)] & y) != 0)));
                _ = &idx;
                loop_filter(dst, E, I, H, stride, 1, @as(c_int, 4) << @intCast(idx));
            }
        }
    }
}
pub fn loop_filter_v_sb128y_c(arg_dst: [*c]pixel, stride: ptrdiff_t, vmask: [*c]const u32, arg_l: [*c][4]u8, arg_b4_stride: ptrdiff_t, arg_lut: [*c]const Av1FilterLUT, w: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &vmask;
    var l = arg_l;
    _ = &l;
    var b4_stride = arg_b4_stride;
    _ = &b4_stride;
    var lut = arg_lut;
    _ = &lut;
    _ = &w;
    const vm: c_uint = (vmask[@as(c_int, 0)] | vmask[@as(c_int, 1)]) | vmask[@as(c_int, 2)];
    _ = &vm;
    {
        var x: c_uint = 1;
        _ = &x;
        while ((vm & ~(x -% @as(c_uint, 1))) != 0) : ({
            x <<= @intCast(@as(c_uint, 1));
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            l += 1;
        }) {
            if ((vm & x) != 0) {
                const L: c_int = if (l[@as(c_int, 0)][@as(c_int, 0)] != 0) @as(c_int, l[@as(c_int, 0)][@as(c_int, 0)]) else @as(c_int, l[@bitCast(@as(isize, @intCast(-b4_stride)))][@as(c_int, 0)]);
                _ = &L;
                if (!(L != 0)) continue;
                const H: c_int = L >> @intCast(@as(c_int, 4));
                _ = &H;
                const E: c_int = lut.*.e[@bitCast(@as(isize, @intCast(L)))];
                _ = &E;
                const I: c_int = lut.*.i[@bitCast(@as(isize, @intCast(L)))];
                _ = &I;
                const idx: c_int = if ((vmask[@as(c_int, 2)] & x) != 0) @as(c_int, 2) else @as(c_int, @intFromBool(!!((vmask[@as(c_int, 1)] & x) != 0)));
                _ = &idx;
                loop_filter(dst, E, I, H, 1, stride, @as(c_int, 4) << @intCast(idx));
            }
        }
    }
}
pub fn loop_filter_h_sb128uv_c(arg_dst: [*c]pixel, stride: ptrdiff_t, vmask: [*c]const u32, arg_l: [*c][4]u8, arg_b4_stride: ptrdiff_t, arg_lut: [*c]const Av1FilterLUT, h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &vmask;
    var l = arg_l;
    _ = &l;
    var b4_stride = arg_b4_stride;
    _ = &b4_stride;
    var lut = arg_lut;
    _ = &lut;
    _ = &h;
    const vm: c_uint = vmask[@as(c_int, 0)] | vmask[@as(c_int, 1)];
    _ = &vm;
    {
        var y: c_uint = 1;
        _ = &y;
        while ((vm & ~(y -% @as(c_uint, 1))) != 0) : ({
            y <<= @intCast(@as(c_uint, 1));
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 4) * stride))));
            l += @as(usize, @bitCast(@as(isize, @intCast(b4_stride))));
        }) {
            if ((vm & y) != 0) {
                const L: c_int = if (l[@as(c_int, 0)][@as(c_int, 0)] != 0) @as(c_int, l[@as(c_int, 0)][@as(c_int, 0)]) else @as(c_int, l[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))][@as(c_int, 0)]);
                _ = &L;
                if (!(L != 0)) continue;
                const H: c_int = L >> @intCast(@as(c_int, 4));
                _ = &H;
                const E: c_int = lut.*.e[@bitCast(@as(isize, @intCast(L)))];
                _ = &E;
                const I: c_int = lut.*.i[@bitCast(@as(isize, @intCast(L)))];
                _ = &I;
                const idx: c_int = @as(c_int, @intFromBool(!!((vmask[@as(c_int, 1)] & y) != 0)));
                _ = &idx;
                loop_filter(dst, E, I, H, stride, 1, @as(c_int, 4) + (@as(c_int, 2) * idx));
            }
        }
    }
}
pub fn loop_filter_v_sb128uv_c(arg_dst: [*c]pixel, stride: ptrdiff_t, vmask: [*c]const u32, arg_l: [*c][4]u8, arg_b4_stride: ptrdiff_t, arg_lut: [*c]const Av1FilterLUT, w: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    _ = &vmask;
    var l = arg_l;
    _ = &l;
    var b4_stride = arg_b4_stride;
    _ = &b4_stride;
    var lut = arg_lut;
    _ = &lut;
    _ = &w;
    const vm: c_uint = vmask[@as(c_int, 0)] | vmask[@as(c_int, 1)];
    _ = &vm;
    {
        var x: c_uint = 1;
        _ = &x;
        while ((vm & ~(x -% @as(c_uint, 1))) != 0) : ({
            x <<= @intCast(@as(c_uint, 1));
            dst += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            l += 1;
        }) {
            if ((vm & x) != 0) {
                const L: c_int = if (l[@as(c_int, 0)][@as(c_int, 0)] != 0) @as(c_int, l[@as(c_int, 0)][@as(c_int, 0)]) else @as(c_int, l[@bitCast(@as(isize, @intCast(-b4_stride)))][@as(c_int, 0)]);
                _ = &L;
                if (!(L != 0)) continue;
                const H: c_int = L >> @intCast(@as(c_int, 4));
                _ = &H;
                const E: c_int = lut.*.e[@bitCast(@as(isize, @intCast(L)))];
                _ = &E;
                const I: c_int = lut.*.i[@bitCast(@as(isize, @intCast(L)))];
                _ = &I;
                const idx: c_int = @as(c_int, @intFromBool(!!((vmask[@as(c_int, 1)] & x) != 0)));
                _ = &idx;
                loop_filter(dst, E, I, H, 1, stride, @as(c_int, 4) + (@as(c_int, 2) * idx));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_loop_filter_dsp_init_8bpc(c: [*c]Dav1dLoopFilterDSPContext) void {
    _ = &c;
    c.*.loop_filter_sb[@as(c_int, 0)][@as(c_int, 0)] = loop_filter_h_sb128y_c;
    c.*.loop_filter_sb[@as(c_int, 0)][@as(c_int, 1)] = loop_filter_v_sb128y_c;
    c.*.loop_filter_sb[@as(c_int, 1)][@as(c_int, 0)] = loop_filter_h_sb128uv_c;
    c.*.loop_filter_sb[@as(c_int, 1)][@as(c_int, 1)] = loop_filter_v_sb128uv_c;
}
