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
//  *  Copyright (c) 2015 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_abs(c_int) c_int;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const tran_high_t = common.tran_high_t;
pub const tran_low_t = common.tran_low_t;
pub fn clip_pixel(arg_val: c_int) callconv(.c) u8 {
    var val = arg_val;
    _ = &val;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(i8, @truncate(if (val > @as(c_int, 255)) @as(c_int, 255) else if (val < @as(c_int, 0)) @as(c_int, 0) else val)));
}
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
pub fn clip_pixel_highbd(arg_val: c_int, arg_bd: c_int) callconv(.c) u16 {
    var val = arg_val;
    _ = &val;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        switch (bd) {
            else => {
                return @bitCast(@as(c_short, @truncate(clamp(val, 0, 255))));
            },
            @as(c_int, 10) => {
                return @bitCast(@as(c_short, @truncate(clamp(val, 0, 1023))));
            },
            @as(c_int, 12) => {
                return @bitCast(@as(c_short, @truncate(clamp(val, 0, 4095))));
            },
        }
    }
}
pub const cospi_1_64 = common.cospi_1_64;
pub const cospi_2_64 = common.cospi_2_64;
pub const cospi_3_64 = common.cospi_3_64;
pub const cospi_4_64 = common.cospi_4_64;
pub const cospi_5_64 = common.cospi_5_64;
pub const cospi_6_64 = common.cospi_6_64;
pub const cospi_7_64 = common.cospi_7_64;
pub const cospi_8_64 = common.cospi_8_64;
pub const cospi_9_64 = common.cospi_9_64;
pub const cospi_10_64 = common.cospi_10_64;
pub const cospi_11_64 = common.cospi_11_64;
pub const cospi_12_64 = common.cospi_12_64;
pub const cospi_13_64 = common.cospi_13_64;
pub const cospi_14_64 = common.cospi_14_64;
pub const cospi_15_64 = common.cospi_15_64;
pub const cospi_16_64 = common.cospi_16_64;
pub const cospi_17_64 = common.cospi_17_64;
pub const cospi_18_64 = common.cospi_18_64;
pub const cospi_19_64 = common.cospi_19_64;
pub const cospi_20_64 = common.cospi_20_64;
pub const cospi_21_64 = common.cospi_21_64;
pub const cospi_22_64 = common.cospi_22_64;
pub const cospi_23_64 = common.cospi_23_64;
pub const cospi_24_64 = common.cospi_24_64;
pub const cospi_25_64 = common.cospi_25_64;
pub const cospi_26_64 = common.cospi_26_64;
pub const cospi_27_64 = common.cospi_27_64;
pub const cospi_28_64 = common.cospi_28_64;
pub const cospi_29_64 = common.cospi_29_64;
pub const cospi_30_64 = common.cospi_30_64;
pub const cospi_31_64 = common.cospi_31_64;
pub const sinpi_1_9 = common.sinpi_1_9;
pub const sinpi_2_9 = common.sinpi_2_9;
pub const sinpi_3_9 = common.sinpi_3_9;
pub const sinpi_4_9 = common.sinpi_4_9;
pub fn check_range(input: tran_high_t) callconv(.c) tran_high_t {
    return @import("../runtime.zig").vp9TransformRange(input, 8);
}
pub fn dct_const_round_shift(arg_input: tran_high_t) callconv(.c) tran_high_t {
    var input = arg_input;
    _ = &input;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var rv: tran_high_t = (input + @as(tran_high_t, @as(c_int, 1) << @intCast(@as(c_int, 14) - @as(c_int, 1)))) >> @intCast(@as(tran_high_t, @as(c_int, 14)));
    _ = &rv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return rv;
}
pub fn highbd_check_range(input: tran_high_t, bd: c_int) callconv(.c) tran_high_t {
    return @import("../runtime.zig").vp9TransformRange(input, bd);
}
pub fn highbd_clip_pixel_add(arg_dest: u16, arg_trans: tran_high_t, arg_bd: c_int) callconv(.c) u16 {
    var dest = arg_dest;
    _ = &dest;
    var trans = arg_trans;
    _ = &trans;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    trans = @as(i32, @truncate(highbd_check_range(trans, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return clip_pixel_highbd(@as(c_int, dest) + @as(c_int, @truncate(trans)), bd);
}
pub fn clip_pixel_add(arg_dest: u8, arg_trans: tran_high_t) callconv(.c) u8 {
    var dest = arg_dest;
    _ = &dest;
    var trans = arg_trans;
    _ = &trans;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    trans = @as(i32, @truncate(check_range(trans)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return clip_pixel(@as(c_int, dest) + @as(c_int, @truncate(trans)));
}
pub export fn antfly_vp9_vpx_iwht4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var output: [16]tran_low_t = undefined;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    var b1: tran_high_t = undefined;
    _ = &b1;
    var c1: tran_high_t = undefined;
    _ = &c1;
    var d1: tran_high_t = undefined;
    _ = &d1;
    var e1: tran_high_t = undefined;
    _ = &e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ip: [*c]const tran_low_t = input;
    _ = &ip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var op: [*c]tran_low_t = @ptrCast(@alignCast(&output));
    _ = &op;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = ip[@as(c_int, 1)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 = ip[@as(c_int, 2)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = ip[@as(c_int, 3)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = (a1 - d1) >> @intCast(@as(tran_high_t, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = e1 - b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = e1 - c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 0)] = @as(i32, @truncate(check_range(a1)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 1)] = @as(i32, @truncate(check_range(b1)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 2)] = @as(i32, @truncate(check_range(c1)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 3)] = @as(i32, @truncate(check_range(d1)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ip = @ptrCast(@alignCast(&output));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = ip[@as(c_int, 4) * @as(c_int, 0)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = ip[@as(c_int, 4) * @as(c_int, 1)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 = ip[@as(c_int, 4) * @as(c_int, 2)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = ip[@as(c_int, 4) * @as(c_int, 3)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = (a1 - d1) >> @intCast(@as(tran_high_t, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = e1 - b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = e1 - c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))], @as(i32, @truncate(check_range(a1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))], @as(i32, @truncate(check_range(b1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))], @as(i32, @truncate(check_range(c1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))], @as(i32, @truncate(check_range(d1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_iwht4x4_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    var e1: tran_high_t = undefined;
    _ = &e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var tmp: [4]tran_low_t = undefined;
    _ = &tmp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ip: [*c]const tran_low_t = input;
    _ = &ip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var op: [*c]tran_low_t = @ptrCast(@alignCast(&tmp));
    _ = &op;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 2));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    e1 = a1 >> @intCast(@as(tran_high_t, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 -= e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op[@as(c_int, 0)] = @as(i32, @truncate(check_range(a1)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op[@as(c_int, 1)] = blk: {
        const tmp_1 = blk_1: {
            const tmp_2 = @as(i32, @truncate(check_range(e1)));
            op[@as(c_int, 3)] = tmp_2;
            break :blk_1 tmp_2;
        };
        op[@as(c_int, 2)] = tmp_1;
        break :blk tmp_1;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ip = @ptrCast(@alignCast(&tmp));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = @as(tran_high_t, ip[@as(c_int, 0)]) - e1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))], a1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))], e1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))], e1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))], e1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_iadst4_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_low_t = input[@as(c_int, 0)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_low_t = input[@as(c_int, 1)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_low_t = input[@as(c_int, 2)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_low_t = input[@as(c_int, 3)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((x0 | x1) | x2) | x3) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 4) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = @as(c_int, sinpi_1_9) * x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = @as(c_int, sinpi_2_9) * x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(c_int, sinpi_3_9) * x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @as(c_int, sinpi_4_9) * x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = @as(c_int, sinpi_1_9) * x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = @as(c_int, sinpi_2_9) * x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @as(c_int, sinpi_4_9) * x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @as(i32, @truncate(check_range((x0 - x2) + x3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = (s0 + s3) + s5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = (s1 - s4) - s6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = s2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, @as(c_int, sinpi_3_9)) * s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(dct_const_round_shift(s0 + s3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(dct_const_round_shift(s1 + s3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(dct_const_round_shift(s2))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(dct_const_round_shift((s0 + s1) - s3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_idct4_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step: [4]i16 = undefined;
    _ = &step;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) + @as(c_int, @as(i16, @truncate(input[@as(c_int, 2)])))) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) - @as(c_int, @as(i16, @truncate(input[@as(c_int, 2)])))) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_24_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_8_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(@as(c_int, step[@as(c_int, 0)]) + @as(c_int, step[@as(c_int, 3)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(@as(c_int, step[@as(c_int, 1)]) + @as(c_int, step[@as(c_int, 2)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(@as(c_int, step[@as(c_int, 1)]) - @as(c_int, step[@as(c_int, 2)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(@as(c_int, step[@as(c_int, 0)]) - @as(c_int, step[@as(c_int, 3)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_idct4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [16]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [4]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [4]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct4_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
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
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 4)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 4)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct4_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 4)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)));
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
pub export fn antfly_vp9_vpx_idct4x4_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(check_range(dct_const_round_shift(@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) * @as(c_int, cospi_16_64)))));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(check_range(dct_const_round_shift(out * @as(c_int, cospi_16_64)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 0)] = clip_pixel_add(dest[@as(c_int, 0)], a1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 1)] = clip_pixel_add(dest[@as(c_int, 1)], a1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 2)] = clip_pixel_add(dest[@as(c_int, 2)], a1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 3)] = clip_pixel_add(dest[@as(c_int, 3)], a1);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_iadst8_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_high_t = input[@as(c_int, 7)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_high_t = input[@as(c_int, 0)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_high_t = input[@as(c_int, 5)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_high_t = input[@as(c_int, 2)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x4: tran_high_t = input[@as(c_int, 3)];
    _ = &x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x5: tran_high_t = input[@as(c_int, 4)];
    _ = &x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x6: tran_high_t = input[@as(c_int, 1)];
    _ = &x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x7: tran_high_t = input[@as(c_int, 6)];
    _ = &x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((((((x0 | x1) | x2) | x3) | x4) | x5) | x6) | x7) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 8) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = @truncate((@as(tran_high_t, @as(c_int, cospi_2_64)) * x0) + (@as(tran_high_t, @as(c_int, cospi_30_64)) * x1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = @truncate((@as(tran_high_t, @as(c_int, cospi_30_64)) * x0) - (@as(tran_high_t, @as(c_int, cospi_2_64)) * x1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @truncate((@as(tran_high_t, @as(c_int, cospi_10_64)) * x2) + (@as(tran_high_t, @as(c_int, cospi_22_64)) * x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @truncate((@as(tran_high_t, @as(c_int, cospi_22_64)) * x2) - (@as(tran_high_t, @as(c_int, cospi_10_64)) * x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = @truncate((@as(tran_high_t, @as(c_int, cospi_18_64)) * x4) + (@as(tran_high_t, @as(c_int, cospi_14_64)) * x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = @truncate((@as(tran_high_t, @as(c_int, cospi_14_64)) * x4) - (@as(tran_high_t, @as(c_int, cospi_18_64)) * x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @truncate((@as(tran_high_t, @as(c_int, cospi_26_64)) * x6) + (@as(tran_high_t, @as(c_int, cospi_6_64)) * x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @truncate((@as(tran_high_t, @as(c_int, cospi_6_64)) * x6) - (@as(tran_high_t, @as(c_int, cospi_26_64)) * x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(check_range(dct_const_round_shift(s0 + s4))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(check_range(dct_const_round_shift(s1 + s5))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(dct_const_round_shift(s2 + s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(dct_const_round_shift(s3 + s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(check_range(dct_const_round_shift(s0 - s4))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(check_range(dct_const_round_shift(s1 - s5))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s2 - s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s3 - s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = @truncate(x0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = @truncate(x1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @truncate(x2);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @truncate(x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = @truncate((@as(tran_high_t, @as(c_int, cospi_8_64)) * x4) + (@as(tran_high_t, @as(c_int, cospi_24_64)) * x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = @truncate((@as(tran_high_t, @as(c_int, cospi_24_64)) * x4) - (@as(tran_high_t, @as(c_int, cospi_8_64)) * x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @truncate((@as(tran_high_t, -@as(c_int, cospi_24_64)) * x6) + (@as(tran_high_t, @as(c_int, cospi_8_64)) * x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @truncate((@as(tran_high_t, @as(c_int, cospi_8_64)) * x6) + (@as(tran_high_t, @as(c_int, cospi_24_64)) * x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(check_range(s0 + s2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(check_range(s1 + s3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(s0 - s2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(s1 - s3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 + s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 + s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 - s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 - s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @truncate(@as(tran_high_t, @as(c_int, cospi_16_64)) * (x2 + x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @truncate(@as(tran_high_t, @as(c_int, cospi_16_64)) * (x2 - x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @truncate(@as(tran_high_t, @as(c_int, cospi_16_64)) * (x6 + x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @truncate(@as(tran_high_t, @as(c_int, cospi_16_64)) * (x6 - x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(dct_const_round_shift(s2))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(dct_const_round_shift(s3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(x0)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(-x4)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(x6)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(-x2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(check_range(x3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(check_range(-x7)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(check_range(x5)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(check_range(-x1)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_idct8_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [8]i16 = undefined;
    _ = &step1;
    var step2: [8]i16 = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(input[@as(c_int, 0)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(input[@as(c_int, 4)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(input[@as(c_int, 2)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(input[@as(c_int, 6)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_28_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 7)]))) * @as(c_int, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_4_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 7)]))) * @as(c_int, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 5)]))) * @as(c_int, cospi_12_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 5)]))) * @as(c_int, cospi_20_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 2)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 2)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 1)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 1)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) + @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) - @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) + @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) + @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) - @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) - @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 6)]) - @as(c_int, step2[@as(c_int, 5)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 5)]) + @as(c_int, step2[@as(c_int, 6)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 7)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) + @as(c_int, step1[@as(c_int, 6)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) + @as(c_int, step1[@as(c_int, 5)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) + @as(c_int, step1[@as(c_int, 4)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) - @as(c_int, step1[@as(c_int, 4)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) - @as(c_int, step1[@as(c_int, 5)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) - @as(c_int, step1[@as(c_int, 6)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 7)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_idct8x8_64_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [64]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [8]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [8]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct8_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
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
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 8)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct8_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5)));
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
pub export fn antfly_vp9_vpx_idct8x8_12_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [64]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([63]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [8]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [8]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct8_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
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
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 8)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct8_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5)));
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
pub export fn antfly_vp9_vpx_idct8x8_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(check_range(dct_const_round_shift(@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) * @as(c_int, cospi_16_64)))));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(check_range(dct_const_round_shift(out * @as(c_int, cospi_16_64)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 8)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 8)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_iadst16_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    var s8: tran_high_t = undefined;
    _ = &s8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s9: tran_high_t = undefined;
    _ = &s9;
    var s10: tran_high_t = undefined;
    _ = &s10;
    var s11: tran_high_t = undefined;
    _ = &s11;
    var s12: tran_high_t = undefined;
    _ = &s12;
    var s13: tran_high_t = undefined;
    _ = &s13;
    var s14: tran_high_t = undefined;
    _ = &s14;
    var s15: tran_high_t = undefined;
    _ = &s15;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_high_t = input[@as(c_int, 15)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_high_t = input[@as(c_int, 0)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_high_t = input[@as(c_int, 13)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_high_t = input[@as(c_int, 2)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x4: tran_high_t = input[@as(c_int, 11)];
    _ = &x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x5: tran_high_t = input[@as(c_int, 4)];
    _ = &x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x6: tran_high_t = input[@as(c_int, 9)];
    _ = &x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x7: tran_high_t = input[@as(c_int, 6)];
    _ = &x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x8: tran_high_t = input[@as(c_int, 7)];
    _ = &x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x9: tran_high_t = input[@as(c_int, 8)];
    _ = &x9;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x10: tran_high_t = input[@as(c_int, 5)];
    _ = &x10;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x11: tran_high_t = input[@as(c_int, 10)];
    _ = &x11;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x12: tran_high_t = input[@as(c_int, 3)];
    _ = &x12;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x13: tran_high_t = input[@as(c_int, 12)];
    _ = &x13;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x14: tran_high_t = input[@as(c_int, 1)];
    _ = &x14;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x15: tran_high_t = input[@as(c_int, 14)];
    _ = &x15;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((((((((((((((x0 | x1) | x2) | x3) | x4) | x5) | x6) | x7) | x8) | x9) | x10) | x11) | x12) | x13) | x14) | x15) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 16) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = (x0 * @as(tran_high_t, @as(c_int, cospi_1_64))) + (x1 * @as(tran_high_t, @as(c_int, cospi_31_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = (x0 * @as(tran_high_t, @as(c_int, cospi_31_64))) - (x1 * @as(tran_high_t, @as(c_int, cospi_1_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = (x2 * @as(tran_high_t, @as(c_int, cospi_5_64))) + (x3 * @as(tran_high_t, @as(c_int, cospi_27_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = (x2 * @as(tran_high_t, @as(c_int, cospi_27_64))) - (x3 * @as(tran_high_t, @as(c_int, cospi_5_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (x4 * @as(tran_high_t, @as(c_int, cospi_9_64))) + (x5 * @as(tran_high_t, @as(c_int, cospi_23_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (x4 * @as(tran_high_t, @as(c_int, cospi_23_64))) - (x5 * @as(tran_high_t, @as(c_int, cospi_9_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (x6 * @as(tran_high_t, @as(c_int, cospi_13_64))) + (x7 * @as(tran_high_t, @as(c_int, cospi_19_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (x6 * @as(tran_high_t, @as(c_int, cospi_19_64))) - (x7 * @as(tran_high_t, @as(c_int, cospi_13_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = (x8 * @as(tran_high_t, @as(c_int, cospi_17_64))) + (x9 * @as(tran_high_t, @as(c_int, cospi_15_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = (x8 * @as(tran_high_t, @as(c_int, cospi_15_64))) - (x9 * @as(tran_high_t, @as(c_int, cospi_17_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = (x10 * @as(tran_high_t, @as(c_int, cospi_21_64))) + (x11 * @as(tran_high_t, @as(c_int, cospi_11_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = (x10 * @as(tran_high_t, @as(c_int, cospi_11_64))) - (x11 * @as(tran_high_t, @as(c_int, cospi_21_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (x12 * @as(tran_high_t, @as(c_int, cospi_25_64))) + (x13 * @as(tran_high_t, @as(c_int, cospi_7_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (x12 * @as(tran_high_t, @as(c_int, cospi_7_64))) - (x13 * @as(tran_high_t, @as(c_int, cospi_25_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (x14 * @as(tran_high_t, @as(c_int, cospi_29_64))) + (x15 * @as(tran_high_t, @as(c_int, cospi_3_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (x14 * @as(tran_high_t, @as(c_int, cospi_3_64))) - (x15 * @as(tran_high_t, @as(c_int, cospi_29_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(check_range(dct_const_round_shift(s0 + s8))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(check_range(dct_const_round_shift(s1 + s9))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(dct_const_round_shift(s2 + s10))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(dct_const_round_shift(s3 + s11))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 + s12))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 + s13))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s6 + s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s7 + s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(check_range(dct_const_round_shift(s0 - s8))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(check_range(dct_const_round_shift(s1 - s9))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(check_range(dct_const_round_shift(s2 - s10))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(check_range(dct_const_round_shift(s3 - s11))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 - s12))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 - s13))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(check_range(dct_const_round_shift(s6 - s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(check_range(dct_const_round_shift(s7 - s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = (x8 * @as(tran_high_t, @as(c_int, cospi_4_64))) + (x9 * @as(tran_high_t, @as(c_int, cospi_28_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = (x8 * @as(tran_high_t, @as(c_int, cospi_28_64))) - (x9 * @as(tran_high_t, @as(c_int, cospi_4_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = (x10 * @as(tran_high_t, @as(c_int, cospi_20_64))) + (x11 * @as(tran_high_t, @as(c_int, cospi_12_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = (x10 * @as(tran_high_t, @as(c_int, cospi_12_64))) - (x11 * @as(tran_high_t, @as(c_int, cospi_20_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (-x12 * @as(tran_high_t, @as(c_int, cospi_28_64))) + (x13 * @as(tran_high_t, @as(c_int, cospi_4_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (x12 * @as(tran_high_t, @as(c_int, cospi_4_64))) + (x13 * @as(tran_high_t, @as(c_int, cospi_28_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (-x14 * @as(tran_high_t, @as(c_int, cospi_12_64))) + (x15 * @as(tran_high_t, @as(c_int, cospi_20_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (x14 * @as(tran_high_t, @as(c_int, cospi_20_64))) + (x15 * @as(tran_high_t, @as(c_int, cospi_12_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(check_range(s0 + s4)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(check_range(s1 + s5)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(s2 + s6)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(s3 + s7)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(check_range(s0 - s4)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(check_range(s1 - s5)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(s2 - s6)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(s3 - s7)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(check_range(dct_const_round_shift(s8 + s12))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(check_range(dct_const_round_shift(s9 + s13))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(check_range(dct_const_round_shift(s10 + s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(check_range(dct_const_round_shift(s11 + s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(check_range(dct_const_round_shift(s8 - s12))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(check_range(dct_const_round_shift(s9 - s13))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(check_range(dct_const_round_shift(s10 - s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(check_range(dct_const_round_shift(s11 - s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (x4 * @as(tran_high_t, @as(c_int, cospi_8_64))) + (x5 * @as(tran_high_t, @as(c_int, cospi_24_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (x4 * @as(tran_high_t, @as(c_int, cospi_24_64))) - (x5 * @as(tran_high_t, @as(c_int, cospi_8_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (-x6 * @as(tran_high_t, @as(c_int, cospi_24_64))) + (x7 * @as(tran_high_t, @as(c_int, cospi_8_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (x6 * @as(tran_high_t, @as(c_int, cospi_8_64))) + (x7 * @as(tran_high_t, @as(c_int, cospi_24_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = x9;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = x10;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = x11;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (x12 * @as(tran_high_t, @as(c_int, cospi_8_64))) + (x13 * @as(tran_high_t, @as(c_int, cospi_24_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (x12 * @as(tran_high_t, @as(c_int, cospi_24_64))) - (x13 * @as(tran_high_t, @as(c_int, cospi_8_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (-x14 * @as(tran_high_t, @as(c_int, cospi_24_64))) + (x15 * @as(tran_high_t, @as(c_int, cospi_8_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (x14 * @as(tran_high_t, @as(c_int, cospi_8_64))) + (x15 * @as(tran_high_t, @as(c_int, cospi_24_64)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(check_range(s0 + s2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(check_range(s1 + s3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(s0 - s2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(s1 - s3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 + s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 + s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s4 - s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s5 - s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(check_range(s8 + s10)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(check_range(s9 + s11)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(check_range(s8 - s10)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(check_range(s9 - s11)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(check_range(dct_const_round_shift(s12 + s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(check_range(dct_const_round_shift(s13 + s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(check_range(dct_const_round_shift(s12 - s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(check_range(dct_const_round_shift(s13 - s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, -@as(c_int, cospi_16_64)) * (x2 + x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (x2 - x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (x6 + x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (-x6 + x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (x10 + x11);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (-x10 + x11);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = @as(tran_high_t, -@as(c_int, cospi_16_64)) * (x14 + x15);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = @as(tran_high_t, @as(c_int, cospi_16_64)) * (x14 - x15);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(check_range(dct_const_round_shift(s2))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(check_range(dct_const_round_shift(s3))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(check_range(dct_const_round_shift(s6))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(check_range(dct_const_round_shift(s7))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(check_range(dct_const_round_shift(s10))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(check_range(dct_const_round_shift(s11))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(check_range(dct_const_round_shift(s14))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(check_range(dct_const_round_shift(s15))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(x0)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(-x8)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(x12)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(-x4)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(check_range(x6)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(check_range(x14)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(check_range(x10)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(check_range(x2)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(check_range(x3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(check_range(x11)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(check_range(x15)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(check_range(x7)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(check_range(x5)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(check_range(-x13)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(check_range(x9)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(check_range(-x1)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_idct16_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [16]i16 = undefined;
    _ = &step1;
    var step2: [16]i16 = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(input[@divTrunc(@as(c_int, 0), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(input[@divTrunc(@as(c_int, 16), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(input[@divTrunc(@as(c_int, 8), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(input[@divTrunc(@as(c_int, 24), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(input[@divTrunc(@as(c_int, 4), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(input[@divTrunc(@as(c_int, 20), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(input[@divTrunc(@as(c_int, 12), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(input[@divTrunc(@as(c_int, 28), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(input[@divTrunc(@as(c_int, 2), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(input[@divTrunc(@as(c_int, 18), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(input[@divTrunc(@as(c_int, 10), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(input[@divTrunc(@as(c_int, 26), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(input[@divTrunc(@as(c_int, 6), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(input[@divTrunc(@as(c_int, 22), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(input[@divTrunc(@as(c_int, 14), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(input[@divTrunc(@as(c_int, 30), @as(c_int, 2))]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = step1[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = step1[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = step1[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = step1[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = step1[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = step1[@as(c_int, 5)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = step1[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = step1[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 8)]) * @as(c_int, cospi_30_64)) - (@as(c_int, step1[@as(c_int, 15)]) * @as(c_int, cospi_2_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 8)]) * @as(c_int, cospi_2_64)) + (@as(c_int, step1[@as(c_int, 15)]) * @as(c_int, cospi_30_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_14_64)) - (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_18_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_18_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_14_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_22_64)) - (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_10_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_10_64)) + (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_22_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 11)]) * @as(c_int, cospi_6_64)) - (@as(c_int, step1[@as(c_int, 12)]) * @as(c_int, cospi_26_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 11)]) * @as(c_int, cospi_26_64)) + (@as(c_int, step1[@as(c_int, 12)]) * @as(c_int, cospi_6_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = step2[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = step2[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = step2[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = step2[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 4)]) * @as(c_int, cospi_28_64)) - (@as(c_int, step2[@as(c_int, 7)]) * @as(c_int, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 4)]) * @as(c_int, cospi_4_64)) + (@as(c_int, step2[@as(c_int, 7)]) * @as(c_int, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 5)]) * @as(c_int, cospi_12_64)) - (@as(c_int, step2[@as(c_int, 6)]) * @as(c_int, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 5)]) * @as(c_int, cospi_20_64)) + (@as(c_int, step2[@as(c_int, 6)]) * @as(c_int, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) + @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) - @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 10)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 10)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) - @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 14)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 14)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 1)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 1)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 2)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 2)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) + @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) - @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_24_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = step1[@as(c_int, 11)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = step1[@as(c_int, 12)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) + @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) + @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) - @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) - @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 6)]) - @as(c_int, step2[@as(c_int, 5)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 5)]) + @as(c_int, step2[@as(c_int, 6)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 9)]) + @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 9)]) - @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) - @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 13)]) + @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 13)]) + @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) + @as(c_int, step1[@as(c_int, 6)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) + @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) + @as(c_int, step1[@as(c_int, 4)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) - @as(c_int, step1[@as(c_int, 4)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) - @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) - @as(c_int, step1[@as(c_int, 6)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = step1[@as(c_int, 9)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 10)]) + @as(c_int, step1[@as(c_int, 13)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 10)]) + @as(c_int, step1[@as(c_int, 13)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 11)]) + @as(c_int, step1[@as(c_int, 12)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 11)]) + @as(c_int, step1[@as(c_int, 12)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = step1[@as(c_int, 14)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) + @as(c_int, step2[@as(c_int, 15)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) + @as(c_int, step2[@as(c_int, 14)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 2)]) + @as(c_int, step2[@as(c_int, 13)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 3)]) + @as(c_int, step2[@as(c_int, 12)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 4)]) + @as(c_int, step2[@as(c_int, 11)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 5)]) + @as(c_int, step2[@as(c_int, 10)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 6)]) + @as(c_int, step2[@as(c_int, 9)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 7)]) + @as(c_int, step2[@as(c_int, 8)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 7)]) - @as(c_int, step2[@as(c_int, 8)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 6)]) - @as(c_int, step2[@as(c_int, 9)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 5)]) - @as(c_int, step2[@as(c_int, 10)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 4)]) - @as(c_int, step2[@as(c_int, 11)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 3)]) - @as(c_int, step2[@as(c_int, 12)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 2)]) - @as(c_int, step2[@as(c_int, 13)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) - @as(c_int, step2[@as(c_int, 14)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) - @as(c_int, step2[@as(c_int, 15)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_idct16x16_256_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [256]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct16_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct16x16_38_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [256]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([255]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct16_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct16x16_10_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [256]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([255]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct16_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct16x16_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(check_range(dct_const_round_shift(@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) * @as(c_int, cospi_16_64)))));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(check_range(dct_const_round_shift(out * @as(c_int, cospi_16_64)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 16)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 16)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_idct32_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [32]i16 = undefined;
    _ = &step1;
    var step2: [32]i16 = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(input[@as(c_int, 0)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(input[@as(c_int, 16)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(input[@as(c_int, 8)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(input[@as(c_int, 24)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(input[@as(c_int, 4)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(input[@as(c_int, 20)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(input[@as(c_int, 12)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(input[@as(c_int, 28)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(input[@as(c_int, 2)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(input[@as(c_int, 18)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(input[@as(c_int, 10)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(input[@as(c_int, 26)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(input[@as(c_int, 6)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(input[@as(c_int, 22)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(input[@as(c_int, 14)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(input[@as(c_int, 30)]);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_31_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 31)]))) * @as(c_int, cospi_1_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 1)]))) * @as(c_int, cospi_1_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 31)]))) * @as(c_int, cospi_31_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 17)]))) * @as(c_int, cospi_15_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 15)]))) * @as(c_int, cospi_17_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 17)]))) * @as(c_int, cospi_17_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 15)]))) * @as(c_int, cospi_15_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 9)]))) * @as(c_int, cospi_23_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 23)]))) * @as(c_int, cospi_9_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 9)]))) * @as(c_int, cospi_9_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 23)]))) * @as(c_int, cospi_23_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 25)]))) * @as(c_int, cospi_7_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 7)]))) * @as(c_int, cospi_25_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 25)]))) * @as(c_int, cospi_25_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 7)]))) * @as(c_int, cospi_7_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 5)]))) * @as(c_int, cospi_27_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 27)]))) * @as(c_int, cospi_5_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 5)]))) * @as(c_int, cospi_5_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 27)]))) * @as(c_int, cospi_27_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 21)]))) * @as(c_int, cospi_11_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 11)]))) * @as(c_int, cospi_21_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 21)]))) * @as(c_int, cospi_21_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 11)]))) * @as(c_int, cospi_11_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 13)]))) * @as(c_int, cospi_19_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 19)]))) * @as(c_int, cospi_13_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 13)]))) * @as(c_int, cospi_13_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 19)]))) * @as(c_int, cospi_19_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 29)]))) * @as(c_int, cospi_3_64)) - (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_29_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, @as(i16, @truncate(input[@as(c_int, 29)]))) * @as(c_int, cospi_29_64)) + (@as(c_int, @as(i16, @truncate(input[@as(c_int, 3)]))) * @as(c_int, cospi_3_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = step1[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = step1[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = step1[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = step1[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = step1[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = step1[@as(c_int, 5)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = step1[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = step1[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 8)]) * @as(c_int, cospi_30_64)) - (@as(c_int, step1[@as(c_int, 15)]) * @as(c_int, cospi_2_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 8)]) * @as(c_int, cospi_2_64)) + (@as(c_int, step1[@as(c_int, 15)]) * @as(c_int, cospi_30_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_14_64)) - (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_18_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_18_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_14_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_22_64)) - (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_10_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_10_64)) + (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_22_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 11)]) * @as(c_int, cospi_6_64)) - (@as(c_int, step1[@as(c_int, 12)]) * @as(c_int, cospi_26_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 11)]) * @as(c_int, cospi_26_64)) + (@as(c_int, step1[@as(c_int, 12)]) * @as(c_int, cospi_6_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) + @as(c_int, step1[@as(c_int, 17)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) - @as(c_int, step1[@as(c_int, 17)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 18)]) + @as(c_int, step1[@as(c_int, 19)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 18)]) + @as(c_int, step1[@as(c_int, 19)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 20)]) + @as(c_int, step1[@as(c_int, 21)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 20)]) - @as(c_int, step1[@as(c_int, 21)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 22)]) + @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 22)]) + @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 24)]) + @as(c_int, step1[@as(c_int, 25)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 24)]) - @as(c_int, step1[@as(c_int, 25)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 26)]) + @as(c_int, step1[@as(c_int, 27)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 26)]) + @as(c_int, step1[@as(c_int, 27)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 28)]) + @as(c_int, step1[@as(c_int, 29)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 28)]) - @as(c_int, step1[@as(c_int, 29)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 30)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 30)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = step2[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = step2[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = step2[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = step2[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 4)]) * @as(c_int, cospi_28_64)) - (@as(c_int, step2[@as(c_int, 7)]) * @as(c_int, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 4)]) * @as(c_int, cospi_4_64)) + (@as(c_int, step2[@as(c_int, 7)]) * @as(c_int, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 5)]) * @as(c_int, cospi_12_64)) - (@as(c_int, step2[@as(c_int, 6)]) * @as(c_int, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 5)]) * @as(c_int, cospi_20_64)) + (@as(c_int, step2[@as(c_int, 6)]) * @as(c_int, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) + @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) - @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 10)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 10)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) - @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 14)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 14)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 17)]) * @as(c_int, cospi_4_64)) + (@as(c_int, step2[@as(c_int, 30)]) * @as(c_int, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 17)]) * @as(c_int, cospi_28_64)) + (@as(c_int, step2[@as(c_int, 30)]) * @as(c_int, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 18)]) * @as(c_int, cospi_28_64)) - (@as(c_int, step2[@as(c_int, 29)]) * @as(c_int, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step2[@as(c_int, 18)]) * @as(c_int, cospi_4_64)) + (@as(c_int, step2[@as(c_int, 29)]) * @as(c_int, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = step2[@as(c_int, 19)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = step2[@as(c_int, 20)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 21)]) * @as(c_int, cospi_20_64)) + (@as(c_int, step2[@as(c_int, 26)]) * @as(c_int, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 21)]) * @as(c_int, cospi_12_64)) + (@as(c_int, step2[@as(c_int, 26)]) * @as(c_int, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 22)]) * @as(c_int, cospi_12_64)) - (@as(c_int, step2[@as(c_int, 25)]) * @as(c_int, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step2[@as(c_int, 22)]) * @as(c_int, cospi_20_64)) + (@as(c_int, step2[@as(c_int, 25)]) * @as(c_int, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = step2[@as(c_int, 23)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = step2[@as(c_int, 24)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = step2[@as(c_int, 27)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = step2[@as(c_int, 28)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 1)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 1)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step1[@as(c_int, 2)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 2)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 3)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) + @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) - @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 9)]) * @as(c_int, cospi_24_64)) + (@as(c_int, step1[@as(c_int, 14)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step1[@as(c_int, 10)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step1[@as(c_int, 13)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = step1[@as(c_int, 11)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = step1[@as(c_int, 12)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) + @as(c_int, step1[@as(c_int, 19)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 17)]) + @as(c_int, step1[@as(c_int, 18)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 17)]) - @as(c_int, step1[@as(c_int, 18)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) - @as(c_int, step1[@as(c_int, 19)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 20)]) + @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 21)]) + @as(c_int, step1[@as(c_int, 22)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 21)]) + @as(c_int, step1[@as(c_int, 22)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 20)]) + @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 24)]) + @as(c_int, step1[@as(c_int, 27)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 25)]) + @as(c_int, step1[@as(c_int, 26)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 25)]) - @as(c_int, step1[@as(c_int, 26)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 24)]) - @as(c_int, step1[@as(c_int, 27)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 28)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 29)]) + @as(c_int, step1[@as(c_int, 30)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 29)]) + @as(c_int, step1[@as(c_int, 30)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 28)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) + @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) + @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) - @as(c_int, step2[@as(c_int, 2)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) - @as(c_int, step2[@as(c_int, 3)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(c_int, step2[@as(c_int, 6)]) - @as(c_int, step2[@as(c_int, 5)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 5)]) + @as(c_int, step2[@as(c_int, 6)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 9)]) + @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 9)]) - @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 8)]) - @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step2[@as(c_int, 13)]) + @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 13)]) + @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 12)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = step2[@as(c_int, 17)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 18)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step2[@as(c_int, 29)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 18)]) * @as(c_int, cospi_24_64)) + (@as(c_int, step2[@as(c_int, 29)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 19)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step2[@as(c_int, 28)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 19)]) * @as(c_int, cospi_24_64)) + (@as(c_int, step2[@as(c_int, 28)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 20)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step2[@as(c_int, 27)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step2[@as(c_int, 20)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step2[@as(c_int, 27)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 21)]) * @as(c_int, cospi_24_64)) - (@as(c_int, step2[@as(c_int, 26)]) * @as(c_int, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (-@as(c_int, step2[@as(c_int, 21)]) * @as(c_int, cospi_8_64)) + (@as(c_int, step2[@as(c_int, 26)]) * @as(c_int, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = step2[@as(c_int, 22)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = step2[@as(c_int, 23)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = step2[@as(c_int, 24)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = step2[@as(c_int, 25)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = step2[@as(c_int, 30)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) + @as(c_int, step1[@as(c_int, 6)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) + @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) + @as(c_int, step1[@as(c_int, 4)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) - @as(c_int, step1[@as(c_int, 4)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) - @as(c_int, step1[@as(c_int, 5)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) - @as(c_int, step1[@as(c_int, 6)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 7)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = step1[@as(c_int, 9)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 10)]) + @as(c_int, step1[@as(c_int, 13)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 10)]) + @as(c_int, step1[@as(c_int, 13)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step1[@as(c_int, 11)]) + @as(c_int, step1[@as(c_int, 12)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step1[@as(c_int, 11)]) + @as(c_int, step1[@as(c_int, 12)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = step1[@as(c_int, 14)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) + @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 17)]) + @as(c_int, step1[@as(c_int, 22)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 18)]) + @as(c_int, step1[@as(c_int, 21)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 19)]) + @as(c_int, step1[@as(c_int, 20)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 19)]) - @as(c_int, step1[@as(c_int, 20)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 18)]) - @as(c_int, step1[@as(c_int, 21)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 17)]) - @as(c_int, step1[@as(c_int, 22)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 16)]) - @as(c_int, step1[@as(c_int, 23)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 24)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 25)]) + @as(c_int, step1[@as(c_int, 30)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 26)]) + @as(c_int, step1[@as(c_int, 29)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(-@as(c_int, step1[@as(c_int, 27)]) + @as(c_int, step1[@as(c_int, 28)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 27)]) + @as(c_int, step1[@as(c_int, 28)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 26)]) + @as(c_int, step1[@as(c_int, 29)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 25)]) + @as(c_int, step1[@as(c_int, 30)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 24)]) + @as(c_int, step1[@as(c_int, 31)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) + @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) + @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 2)]) + @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 3)]) + @as(c_int, step2[@as(c_int, 12)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 4)]) + @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 5)]) + @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 6)]) + @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 7)]) + @as(c_int, step2[@as(c_int, 8)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 7)]) - @as(c_int, step2[@as(c_int, 8)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 6)]) - @as(c_int, step2[@as(c_int, 9)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 5)]) - @as(c_int, step2[@as(c_int, 10)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 4)]) - @as(c_int, step2[@as(c_int, 11)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 3)]) - @as(c_int, step2[@as(c_int, 12)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 2)]) - @as(c_int, step2[@as(c_int, 13)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 1)]) - @as(c_int, step2[@as(c_int, 14)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @truncate(@as(i32, @truncate(check_range(@as(c_int, step2[@as(c_int, 0)]) - @as(c_int, step2[@as(c_int, 15)])))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = step2[@as(c_int, 17)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = step2[@as(c_int, 18)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = step2[@as(c_int, 19)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 20)]) + @as(c_int, step2[@as(c_int, 27)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 20)]) + @as(c_int, step2[@as(c_int, 27)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 21)]) + @as(c_int, step2[@as(c_int, 26)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 21)]) + @as(c_int, step2[@as(c_int, 26)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 22)]) + @as(c_int, step2[@as(c_int, 25)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 22)]) + @as(c_int, step2[@as(c_int, 25)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (-@as(c_int, step2[@as(c_int, 23)]) + @as(c_int, step2[@as(c_int, 24)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(c_int, step2[@as(c_int, 23)]) + @as(c_int, step2[@as(c_int, 24)])) * @as(c_int, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = @truncate(@as(i32, @truncate(check_range(dct_const_round_shift(temp2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = step2[@as(c_int, 28)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = step2[@as(c_int, 29)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = step2[@as(c_int, 30)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) + @as(c_int, step1[@as(c_int, 31)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) + @as(c_int, step1[@as(c_int, 30)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) + @as(c_int, step1[@as(c_int, 29)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) + @as(c_int, step1[@as(c_int, 28)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) + @as(c_int, step1[@as(c_int, 27)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 5)]) + @as(c_int, step1[@as(c_int, 26)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 6)]) + @as(c_int, step1[@as(c_int, 25)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 7)]) + @as(c_int, step1[@as(c_int, 24)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 8)]) + @as(c_int, step1[@as(c_int, 23)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 9)]) + @as(c_int, step1[@as(c_int, 22)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 10)]) + @as(c_int, step1[@as(c_int, 21)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 11)]) + @as(c_int, step1[@as(c_int, 20)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 12)]) + @as(c_int, step1[@as(c_int, 19)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 13)]) + @as(c_int, step1[@as(c_int, 18)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 14)]) + @as(c_int, step1[@as(c_int, 17)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 15)]) + @as(c_int, step1[@as(c_int, 16)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 16)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 15)]) - @as(c_int, step1[@as(c_int, 16)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 17)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 14)]) - @as(c_int, step1[@as(c_int, 17)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 18)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 13)]) - @as(c_int, step1[@as(c_int, 18)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 19)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 12)]) - @as(c_int, step1[@as(c_int, 19)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 20)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 11)]) - @as(c_int, step1[@as(c_int, 20)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 21)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 10)]) - @as(c_int, step1[@as(c_int, 21)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 22)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 9)]) - @as(c_int, step1[@as(c_int, 22)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 23)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 8)]) - @as(c_int, step1[@as(c_int, 23)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 24)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 7)]) - @as(c_int, step1[@as(c_int, 24)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 25)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 6)]) - @as(c_int, step1[@as(c_int, 25)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 26)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 5)]) - @as(c_int, step1[@as(c_int, 26)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 27)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 4)]) - @as(c_int, step1[@as(c_int, 27)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 28)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 3)]) - @as(c_int, step1[@as(c_int, 28)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 29)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 2)]) - @as(c_int, step1[@as(c_int, 29)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 30)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 1)]) - @as(c_int, step1[@as(c_int, 30)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 31)] = @as(i32, @truncate(check_range(@as(c_int, step1[@as(c_int, 0)]) - @as(c_int, step1[@as(c_int, 31)]))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_idct32x32_1024_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [1024]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var zero_coeff: i16 = 0;
            _ = &zero_coeff;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        const ref = &zero_coeff;
                        ref.* = @truncate(@as(c_int, ref.*) | input[@bitCast(@as(isize, @intCast(j)))]);
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (@as(c_int, zero_coeff) != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_idct32_c(input, outptr);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(outptr)), 0, @sizeOf(tran_low_t) *% @as(c_ulong, 32));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct32x32_135_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [1024]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([1023]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct32_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct32x32_34_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var out: [1024]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([1023]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_idct32_c(input, outptr);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)));
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
pub export fn antfly_vp9_vpx_idct32x32_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
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
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(check_range(dct_const_round_shift(@as(c_int, @as(i16, @truncate(input[@as(c_int, 0)]))) * @as(c_int, cospi_16_64)))));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(check_range(dct_const_round_shift(out * @as(c_int, cospi_16_64)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 32)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 32)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn detect_invalid_highbd_input(arg_input: [*c]const tran_low_t, arg_size: c_int) callconv(.c) c_int {
    var input = arg_input;
    _ = &input;
    var size = arg_size;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        i = 0;
        while (i < size) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_abs(input[@bitCast(@as(isize, @intCast(i)))]) >= (@as(c_int, 1) << @intCast(@as(c_int, 25)))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return 1;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return 0;
}
pub export fn antfly_vp9_vpx_highbd_iwht4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var output: [16]tran_low_t = undefined;
    _ = &output;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    var b1: tran_high_t = undefined;
    _ = &b1;
    var c1: tran_high_t = undefined;
    _ = &c1;
    var d1: tran_high_t = undefined;
    _ = &d1;
    var e1: tran_high_t = undefined;
    _ = &e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ip: [*c]const tran_low_t = input;
    _ = &ip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var op: [*c]tran_low_t = @ptrCast(@alignCast(&output));
    _ = &op;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = ip[@as(c_int, 1)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 = ip[@as(c_int, 2)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = ip[@as(c_int, 3)] >> @intCast(@as(c_int, 2));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = (a1 - d1) >> @intCast(@as(tran_high_t, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = e1 - b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = e1 - c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(a1, bd)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(b1, bd)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(c1, bd)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(d1, bd)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            op += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ip = @ptrCast(@alignCast(&output));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = ip[@as(c_int, 4) * @as(c_int, 0)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = ip[@as(c_int, 4) * @as(c_int, 1)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 = ip[@as(c_int, 4) * @as(c_int, 2)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = ip[@as(c_int, 4) * @as(c_int, 3)];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = (a1 - d1) >> @intCast(@as(tran_high_t, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            b1 = e1 - b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            c1 = e1 - c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 -= b1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            d1 += c1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))], @as(i32, @truncate(highbd_check_range(a1, bd))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))], @as(i32, @truncate(highbd_check_range(b1, bd))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))], @as(i32, @truncate(highbd_check_range(c1, bd))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))], @as(i32, @truncate(highbd_check_range(d1, bd))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_iwht4x4_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    var e1: tran_high_t = undefined;
    _ = &e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var tmp: [4]tran_low_t = undefined;
    _ = &tmp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ip: [*c]const tran_low_t = input;
    _ = &ip;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var op: [*c]tran_low_t = @ptrCast(@alignCast(&tmp));
    _ = &op;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 2));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    e1 = a1 >> @intCast(@as(tran_high_t, 1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 -= e1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(a1, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op[@as(c_int, 1)] = blk: {
        const tmp_1 = blk_1: {
            const tmp_2 = @as(i32, @truncate(highbd_check_range(e1, bd)));
            op[@as(c_int, 3)] = tmp_2;
            break :blk_1 tmp_2;
        };
        op[@as(c_int, 2)] = tmp_1;
        break :blk tmp_1;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    ip = @ptrCast(@alignCast(&tmp));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            e1 = ip[@as(c_int, 0)] >> @intCast(@as(c_int, 1));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            a1 = @as(tran_high_t, ip[@as(c_int, 0)]) - e1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 0))))], a1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 1))))], e1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 2))))], e1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(stride * @as(c_int, 3))))], e1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ip += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_iadst4_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_low_t = input[@as(c_int, 0)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_low_t = input[@as(c_int, 1)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_low_t = input[@as(c_int, 2)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_low_t = input[@as(c_int, 3)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 4) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((x0 | x1) | x2) | x3) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 4) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = @as(tran_high_t, sinpi_1_9) * @as(tran_high_t, x0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = @as(tran_high_t, sinpi_2_9) * @as(tran_high_t, x0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, sinpi_3_9) * @as(tran_high_t, x1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @as(tran_high_t, sinpi_4_9) * @as(tran_high_t, x2);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = @as(tran_high_t, sinpi_1_9) * @as(tran_high_t, x2);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = @as(tran_high_t, sinpi_2_9) * @as(tran_high_t, x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @as(tran_high_t, sinpi_4_9) * @as(tran_high_t, x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @as(i32, @truncate(highbd_check_range((x0 - x2) + x3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = (s0 + s3) + s5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = (s1 - s4) - s6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = s2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, @as(c_int, sinpi_3_9)) * s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s0 + s3), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s1 + s3), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift((s0 + s1) - s3), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct4_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step: [4]tran_low_t = undefined;
    _ = &step;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 4) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 4));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, input[@as(c_int, 0)] + input[@as(c_int, 2)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, input[@as(c_int, 0)] - input[@as(c_int, 2)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step[@as(c_int, 0)] + step[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step[@as(c_int, 1)] + step[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step[@as(c_int, 1)] - step[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step[@as(c_int, 0)] - step[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [16]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [4]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [4]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct4_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4)))));
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
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 4)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 4)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct4_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 4)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct4x4_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, input[@as(c_int, 0)]) * @as(tran_high_t, cospi_16_64)), bd)));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, out) * @as(tran_high_t, cospi_16_64)), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 0)] = highbd_clip_pixel_add(dest[@as(c_int, 0)], a1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 1)] = highbd_clip_pixel_add(dest[@as(c_int, 1)], a1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 2)] = highbd_clip_pixel_add(dest[@as(c_int, 2)], a1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest[@as(c_int, 3)] = highbd_clip_pixel_add(dest[@as(c_int, 3)], a1, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_iadst8_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_low_t = input[@as(c_int, 7)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_low_t = input[@as(c_int, 0)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_low_t = input[@as(c_int, 5)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_low_t = input[@as(c_int, 2)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x4: tran_low_t = input[@as(c_int, 3)];
    _ = &x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x5: tran_low_t = input[@as(c_int, 4)];
    _ = &x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x6: tran_low_t = input[@as(c_int, 1)];
    _ = &x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x7: tran_low_t = input[@as(c_int, 6)];
    _ = &x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 8) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 8));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((((((x0 | x1) | x2) | x3) | x4) | x5) | x6) | x7) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 8) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = (@as(tran_high_t, cospi_2_64) * @as(tran_high_t, x0)) + (@as(tran_high_t, cospi_30_64) * @as(tran_high_t, x1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = (@as(tran_high_t, cospi_30_64) * @as(tran_high_t, x0)) - (@as(tran_high_t, cospi_2_64) * @as(tran_high_t, x1));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = (@as(tran_high_t, cospi_10_64) * @as(tran_high_t, x2)) + (@as(tran_high_t, cospi_22_64) * @as(tran_high_t, x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = (@as(tran_high_t, cospi_22_64) * @as(tran_high_t, x2)) - (@as(tran_high_t, cospi_10_64) * @as(tran_high_t, x3));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (@as(tran_high_t, cospi_18_64) * @as(tran_high_t, x4)) + (@as(tran_high_t, cospi_14_64) * @as(tran_high_t, x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (@as(tran_high_t, cospi_14_64) * @as(tran_high_t, x4)) - (@as(tran_high_t, cospi_18_64) * @as(tran_high_t, x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (@as(tran_high_t, cospi_26_64) * @as(tran_high_t, x6)) + (@as(tran_high_t, cospi_6_64) * @as(tran_high_t, x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (@as(tran_high_t, cospi_6_64) * @as(tran_high_t, x6)) - (@as(tran_high_t, cospi_26_64) * @as(tran_high_t, x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s0 + s4), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s1 + s5), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2 + s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3 + s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s0 - s4), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s1 - s5), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2 - s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3 - s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (@as(tran_high_t, cospi_8_64) * @as(tran_high_t, x4)) + (@as(tran_high_t, cospi_24_64) * @as(tran_high_t, x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (@as(tran_high_t, cospi_24_64) * @as(tran_high_t, x4)) - (@as(tran_high_t, cospi_8_64) * @as(tran_high_t, x5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (@as(tran_high_t, -@as(c_int, cospi_24_64)) * @as(tran_high_t, x6)) + (@as(tran_high_t, cospi_8_64) * @as(tran_high_t, x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (@as(tran_high_t, cospi_8_64) * @as(tran_high_t, x6)) + (@as(tran_high_t, cospi_24_64) * @as(tran_high_t, x7));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(highbd_check_range(s0 + s2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(highbd_check_range(s1 + s3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(s0 - s2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(s1 - s3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 + s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 + s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 - s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 - s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x2 + x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x2 - x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x6 + x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x6 - x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(x0, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(-x4, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(x6, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(-x2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(x3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(-x7, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(x5, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(-x1, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct8_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [8]tran_low_t = undefined;
    _ = &step1;
    var step2: [8]tran_low_t = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 8) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 8));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = input[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = input[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = input[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = input[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_28_64)) - (@as(tran_high_t, input[@as(c_int, 7)]) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, input[@as(c_int, 7)]) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 5)]) * @as(tran_high_t, cospi_12_64)) - (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 5)]) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_idct4_c(@ptrCast(@alignCast(&step1)), @ptrCast(@alignCast(&step1)), bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, step2[@as(c_int, 6)] - step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 5)] + step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] + step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] + step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] - step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] - step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] - step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct8x8_64_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [64]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [8]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [8]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct8_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
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
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 8)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct8_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct8x8_12_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [64]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([63]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [8]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [8]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct8_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8)))));
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
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 8)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct8_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 8)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct8x8_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, input[@as(c_int, 0)]) * @as(tran_high_t, cospi_16_64)), bd)));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, out) * @as(tran_high_t, cospi_16_64)), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 5) - @as(c_int, 1)))) >> @intCast(@as(c_int, 5));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 8)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 8)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1, bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_iadst16_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s0: tran_high_t = undefined;
    _ = &s0;
    var s1: tran_high_t = undefined;
    _ = &s1;
    var s2: tran_high_t = undefined;
    _ = &s2;
    var s3: tran_high_t = undefined;
    _ = &s3;
    var s4: tran_high_t = undefined;
    _ = &s4;
    var s5: tran_high_t = undefined;
    _ = &s5;
    var s6: tran_high_t = undefined;
    _ = &s6;
    var s7: tran_high_t = undefined;
    _ = &s7;
    var s8: tran_high_t = undefined;
    _ = &s8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var s9: tran_high_t = undefined;
    _ = &s9;
    var s10: tran_high_t = undefined;
    _ = &s10;
    var s11: tran_high_t = undefined;
    _ = &s11;
    var s12: tran_high_t = undefined;
    _ = &s12;
    var s13: tran_high_t = undefined;
    _ = &s13;
    var s14: tran_high_t = undefined;
    _ = &s14;
    var s15: tran_high_t = undefined;
    _ = &s15;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: tran_low_t = input[@as(c_int, 15)];
    _ = &x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x1: tran_low_t = input[@as(c_int, 0)];
    _ = &x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x2: tran_low_t = input[@as(c_int, 13)];
    _ = &x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x3: tran_low_t = input[@as(c_int, 2)];
    _ = &x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x4: tran_low_t = input[@as(c_int, 11)];
    _ = &x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x5: tran_low_t = input[@as(c_int, 4)];
    _ = &x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x6: tran_low_t = input[@as(c_int, 9)];
    _ = &x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x7: tran_low_t = input[@as(c_int, 6)];
    _ = &x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x8: tran_low_t = input[@as(c_int, 7)];
    _ = &x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x9: tran_low_t = input[@as(c_int, 8)];
    _ = &x9;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x10: tran_low_t = input[@as(c_int, 5)];
    _ = &x10;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x11: tran_low_t = input[@as(c_int, 10)];
    _ = &x11;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x12: tran_low_t = input[@as(c_int, 3)];
    _ = &x12;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x13: tran_low_t = input[@as(c_int, 12)];
    _ = &x13;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x14: tran_low_t = input[@as(c_int, 1)];
    _ = &x14;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x15: tran_low_t = input[@as(c_int, 14)];
    _ = &x15;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 16) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 16));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!((((((((((((((((x0 | x1) | x2) | x3) | x4) | x5) | x6) | x7) | x8) | x9) | x10) | x11) | x12) | x13) | x14) | x15) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @as(c_ulong, 16) *% @sizeOf(@TypeOf(output.*)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = (@as(tran_high_t, x0) * @as(tran_high_t, cospi_1_64)) + (@as(tran_high_t, x1) * @as(tran_high_t, cospi_31_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = (@as(tran_high_t, x0) * @as(tran_high_t, cospi_31_64)) - (@as(tran_high_t, x1) * @as(tran_high_t, cospi_1_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = (@as(tran_high_t, x2) * @as(tran_high_t, cospi_5_64)) + (@as(tran_high_t, x3) * @as(tran_high_t, cospi_27_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = (@as(tran_high_t, x2) * @as(tran_high_t, cospi_27_64)) - (@as(tran_high_t, x3) * @as(tran_high_t, cospi_5_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (@as(tran_high_t, x4) * @as(tran_high_t, cospi_9_64)) + (@as(tran_high_t, x5) * @as(tran_high_t, cospi_23_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (@as(tran_high_t, x4) * @as(tran_high_t, cospi_23_64)) - (@as(tran_high_t, x5) * @as(tran_high_t, cospi_9_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (@as(tran_high_t, x6) * @as(tran_high_t, cospi_13_64)) + (@as(tran_high_t, x7) * @as(tran_high_t, cospi_19_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (@as(tran_high_t, x6) * @as(tran_high_t, cospi_19_64)) - (@as(tran_high_t, x7) * @as(tran_high_t, cospi_13_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = (@as(tran_high_t, x8) * @as(tran_high_t, cospi_17_64)) + (@as(tran_high_t, x9) * @as(tran_high_t, cospi_15_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = (@as(tran_high_t, x8) * @as(tran_high_t, cospi_15_64)) - (@as(tran_high_t, x9) * @as(tran_high_t, cospi_17_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = (@as(tran_high_t, x10) * @as(tran_high_t, cospi_21_64)) + (@as(tran_high_t, x11) * @as(tran_high_t, cospi_11_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = (@as(tran_high_t, x10) * @as(tran_high_t, cospi_11_64)) - (@as(tran_high_t, x11) * @as(tran_high_t, cospi_21_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (@as(tran_high_t, x12) * @as(tran_high_t, cospi_25_64)) + (@as(tran_high_t, x13) * @as(tran_high_t, cospi_7_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (@as(tran_high_t, x12) * @as(tran_high_t, cospi_7_64)) - (@as(tran_high_t, x13) * @as(tran_high_t, cospi_25_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (@as(tran_high_t, x14) * @as(tran_high_t, cospi_29_64)) + (@as(tran_high_t, x15) * @as(tran_high_t, cospi_3_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (@as(tran_high_t, x14) * @as(tran_high_t, cospi_3_64)) - (@as(tran_high_t, x15) * @as(tran_high_t, cospi_29_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s0 + s8), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s1 + s9), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2 + s10), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3 + s11), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 + s12), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 + s13), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s6 + s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s7 + s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s0 - s8), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s1 - s9), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2 - s10), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3 - s11), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 - s12), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 - s13), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s6 - s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s7 - s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = x4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = x5;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = x6;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = x7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = (@as(tran_high_t, x8) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, x9) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = (@as(tran_high_t, x8) * @as(tran_high_t, cospi_28_64)) - (@as(tran_high_t, x9) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = (@as(tran_high_t, x10) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, x11) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = (@as(tran_high_t, x10) * @as(tran_high_t, cospi_12_64)) - (@as(tran_high_t, x11) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (@as(tran_high_t, -x12) * @as(tran_high_t, cospi_28_64)) + (@as(tran_high_t, x13) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (@as(tran_high_t, x12) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, x13) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (@as(tran_high_t, -x14) * @as(tran_high_t, cospi_12_64)) + (@as(tran_high_t, x15) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (@as(tran_high_t, x14) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, x15) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(highbd_check_range(s0 + s4, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(highbd_check_range(s1 + s5, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(s2 + s6, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(s3 + s7, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(highbd_check_range(s0 - s4, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(highbd_check_range(s1 - s5, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(s2 - s6, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(s3 - s7, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s8 + s12), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s9 + s13), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s10 + s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s11 + s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s8 - s12), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s9 - s13), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s10 - s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s11 - s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s0 = x0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s1 = x1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = x2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = x3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s4 = (@as(tran_high_t, x4) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, x5) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s5 = (@as(tran_high_t, x4) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, x5) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = (@as(tran_high_t, -x6) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, x7) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = (@as(tran_high_t, x6) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, x7) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s8 = x8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s9 = x9;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = x10;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = x11;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s12 = (@as(tran_high_t, x12) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, x13) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s13 = (@as(tran_high_t, x12) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, x13) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = (@as(tran_high_t, -x14) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, x15) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = (@as(tran_high_t, x14) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, x15) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = @as(i32, @truncate(highbd_check_range(s0 + s2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x1 = @as(i32, @truncate(highbd_check_range(s1 + s3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(s0 - s2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(s1 - s3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x4 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 + s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x5 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 + s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s4 - s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s5 - s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x8 = @as(i32, @truncate(highbd_check_range(s8 + s10, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x9 = @as(i32, @truncate(highbd_check_range(s9 + s11, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(highbd_check_range(s8 - s10, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(highbd_check_range(s9 - s11, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x12 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s12 + s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x13 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s13 + s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s12 - s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s13 - s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s2 = @as(tran_high_t, -@as(c_int, cospi_16_64)) * @as(tran_high_t, x2 + x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s3 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x2 - x3);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s6 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x6 + x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s7 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, -x6 + x7);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s10 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x10 + x11);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s11 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, -x10 + x11);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s14 = @as(tran_high_t, -@as(c_int, cospi_16_64)) * @as(tran_high_t, x14 + x15);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    s15 = @as(tran_high_t, cospi_16_64) * @as(tran_high_t, x14 - x15);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x2 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x3 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s3), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x6 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s6), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x7 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s7), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x10 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s10), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x11 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s11), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x14 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s14), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x15 = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(s15), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(x0, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(-x8, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(x12, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(-x4, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(x6, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(x14, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(x10, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(x2, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(x3, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(x11, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(x15, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(x7, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(x5, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(-x13, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(x9, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(-x1, bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct16_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [16]tran_low_t = undefined;
    _ = &step1;
    var step2: [16]tran_low_t = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 16) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 16));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = input[@divTrunc(@as(c_int, 0), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = input[@divTrunc(@as(c_int, 16), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = input[@divTrunc(@as(c_int, 8), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = input[@divTrunc(@as(c_int, 24), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = input[@divTrunc(@as(c_int, 4), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = input[@divTrunc(@as(c_int, 20), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = input[@divTrunc(@as(c_int, 12), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = input[@divTrunc(@as(c_int, 28), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = input[@divTrunc(@as(c_int, 2), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = input[@divTrunc(@as(c_int, 18), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = input[@divTrunc(@as(c_int, 10), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = input[@divTrunc(@as(c_int, 26), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = input[@divTrunc(@as(c_int, 6), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = input[@divTrunc(@as(c_int, 22), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = input[@divTrunc(@as(c_int, 14), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = input[@divTrunc(@as(c_int, 30), @as(c_int, 2))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = step1[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = step1[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = step1[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = step1[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = step1[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = step1[@as(c_int, 5)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = step1[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = step1[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 8)]) * @as(tran_high_t, cospi_30_64)) - (@as(tran_high_t, step1[@as(c_int, 15)]) * @as(tran_high_t, cospi_2_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 8)]) * @as(tran_high_t, cospi_2_64)) + (@as(tran_high_t, step1[@as(c_int, 15)]) * @as(tran_high_t, cospi_30_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_14_64)) - (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_18_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_18_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_14_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_22_64)) - (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_10_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_10_64)) + (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_22_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 11)]) * @as(tran_high_t, cospi_6_64)) - (@as(tran_high_t, step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_26_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 11)]) * @as(tran_high_t, cospi_26_64)) + (@as(tran_high_t, step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_6_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = step2[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = step2[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = step2[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = step2[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step2[@as(c_int, 4)]) * @as(tran_high_t, cospi_28_64)) - (@as(tran_high_t, step2[@as(c_int, 7)]) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 4)]) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, step2[@as(c_int, 7)]) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_12_64)) - (@as(tran_high_t, step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] + step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] - step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 10)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 10)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] + step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] - step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 14)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 14)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, step1[@as(c_int, 0)] + step1[@as(c_int, 1)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 0)] - step1[@as(c_int, 1)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 2)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step1[@as(c_int, 3)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 2)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 3)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = step1[@as(c_int, 11)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = step1[@as(c_int, 12)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] + step2[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] + step2[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] - step2[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] - step2[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, step2[@as(c_int, 6)] - step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 5)] + step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 9)] + step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 9)] - step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] - step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 12)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 13)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 13)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] + step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] + step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] - step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] - step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] - step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = step1[@as(c_int, 9)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step1[@as(c_int, 10)] + step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 10)] + step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step1[@as(c_int, 11)] + step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 11)] + step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = step1[@as(c_int, 14)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 2)] + step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 3)] + step2[@as(c_int, 12)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 4)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 5)] + step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 6)] + step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 7)] + step2[@as(c_int, 8)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 7)] - step2[@as(c_int, 8)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 6)] - step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 5)] - step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 4)] - step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 3)] - step2[@as(c_int, 12)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 2)] - step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] - step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] - step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct16x16_256_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [256]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct16_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct16x16_38_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [256]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([255]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct16_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var destT: [*c]u16 = dest;
            _ = &destT;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    destT[@bitCast(@as(isize, @intCast(i)))] = highbd_clip_pixel_add(destT[@bitCast(@as(isize, @intCast(i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    destT += @as(usize, @bitCast(@as(isize, @intCast(stride))));
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
pub export fn antfly_vp9_vpx_highbd_idct16x16_10_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [256]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([255]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [16]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [16]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 4)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct16_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
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
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 16)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            antfly_vp9_vpx_highbd_idct16_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 16)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct16x16_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var a1: tran_high_t = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, input[@as(c_int, 0)]) * @as(tran_high_t, cospi_16_64)), bd)));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, out) * @as(tran_high_t, cospi_16_64)), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 16)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 16)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1, bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_idct32_c(arg_input: [*c]const tran_low_t, arg_output: [*c]tran_low_t, arg_bd: c_int) callconv(.c) void {
    var input = arg_input;
    _ = &input;
    var output = arg_output;
    _ = &output;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var step1: [32]tran_low_t = undefined;
    _ = &step1;
    var step2: [32]tran_low_t = undefined;
    _ = &step2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp1: tran_high_t = undefined;
    _ = &temp1;
    var temp2: tran_high_t = undefined;
    _ = &temp2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (detect_invalid_highbd_input(input, 32) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(output)), 0, @sizeOf(@TypeOf(output.*)) *% @as(c_ulong, 32));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = input[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = input[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = input[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = input[@as(c_int, 24)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = input[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = input[@as(c_int, 20)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = input[@as(c_int, 12)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = input[@as(c_int, 28)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = input[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = input[@as(c_int, 18)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = input[@as(c_int, 10)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = input[@as(c_int, 26)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = input[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = input[@as(c_int, 22)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = input[@as(c_int, 14)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = input[@as(c_int, 30)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_31_64)) - (@as(tran_high_t, input[@as(c_int, 31)]) * @as(tran_high_t, cospi_1_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 1)]) * @as(tran_high_t, cospi_1_64)) + (@as(tran_high_t, input[@as(c_int, 31)]) * @as(tran_high_t, cospi_31_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 17)]) * @as(tran_high_t, cospi_15_64)) - (@as(tran_high_t, input[@as(c_int, 15)]) * @as(tran_high_t, cospi_17_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 17)]) * @as(tran_high_t, cospi_17_64)) + (@as(tran_high_t, input[@as(c_int, 15)]) * @as(tran_high_t, cospi_15_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 9)]) * @as(tran_high_t, cospi_23_64)) - (@as(tran_high_t, input[@as(c_int, 23)]) * @as(tran_high_t, cospi_9_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 9)]) * @as(tran_high_t, cospi_9_64)) + (@as(tran_high_t, input[@as(c_int, 23)]) * @as(tran_high_t, cospi_23_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 25)]) * @as(tran_high_t, cospi_7_64)) - (@as(tran_high_t, input[@as(c_int, 7)]) * @as(tran_high_t, cospi_25_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 25)]) * @as(tran_high_t, cospi_25_64)) + (@as(tran_high_t, input[@as(c_int, 7)]) * @as(tran_high_t, cospi_7_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 5)]) * @as(tran_high_t, cospi_27_64)) - (@as(tran_high_t, input[@as(c_int, 27)]) * @as(tran_high_t, cospi_5_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 5)]) * @as(tran_high_t, cospi_5_64)) + (@as(tran_high_t, input[@as(c_int, 27)]) * @as(tran_high_t, cospi_27_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 21)]) * @as(tran_high_t, cospi_11_64)) - (@as(tran_high_t, input[@as(c_int, 11)]) * @as(tran_high_t, cospi_21_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 21)]) * @as(tran_high_t, cospi_21_64)) + (@as(tran_high_t, input[@as(c_int, 11)]) * @as(tran_high_t, cospi_11_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 13)]) * @as(tran_high_t, cospi_19_64)) - (@as(tran_high_t, input[@as(c_int, 19)]) * @as(tran_high_t, cospi_13_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 13)]) * @as(tran_high_t, cospi_13_64)) + (@as(tran_high_t, input[@as(c_int, 19)]) * @as(tran_high_t, cospi_19_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, input[@as(c_int, 29)]) * @as(tran_high_t, cospi_3_64)) - (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_29_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, input[@as(c_int, 29)]) * @as(tran_high_t, cospi_29_64)) + (@as(tran_high_t, input[@as(c_int, 3)]) * @as(tran_high_t, cospi_3_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = step1[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = step1[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = step1[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = step1[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = step1[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = step1[@as(c_int, 5)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = step1[@as(c_int, 6)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = step1[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 8)]) * @as(tran_high_t, cospi_30_64)) - (@as(tran_high_t, step1[@as(c_int, 15)]) * @as(tran_high_t, cospi_2_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 8)]) * @as(tran_high_t, cospi_2_64)) + (@as(tran_high_t, step1[@as(c_int, 15)]) * @as(tran_high_t, cospi_30_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_14_64)) - (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_18_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_18_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_14_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_22_64)) - (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_10_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_10_64)) + (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_22_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 11)]) * @as(tran_high_t, cospi_6_64)) - (@as(tran_high_t, step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_26_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 11)]) * @as(tran_high_t, cospi_26_64)) + (@as(tran_high_t, step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_6_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] + step1[@as(c_int, 17)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] - step1[@as(c_int, 17)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 18)] + step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 18)] + step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 20)] + step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 20)] - step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 22)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 22)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 24)] + step1[@as(c_int, 25)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 24)] - step1[@as(c_int, 25)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 26)] + step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 26)] + step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 28)] + step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 28)] - step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 30)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 30)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = step2[@as(c_int, 0)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = step2[@as(c_int, 1)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = step2[@as(c_int, 2)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = step2[@as(c_int, 3)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step2[@as(c_int, 4)]) * @as(tran_high_t, cospi_28_64)) - (@as(tran_high_t, step2[@as(c_int, 7)]) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 4)]) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, step2[@as(c_int, 7)]) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_12_64)) - (@as(tran_high_t, step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] + step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] - step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 10)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 10)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] + step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] - step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 14)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 14)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 17)]) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, step2[@as(c_int, 30)]) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 17)]) * @as(tran_high_t, cospi_28_64)) + (@as(tran_high_t, step2[@as(c_int, 30)]) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 18)]) * @as(tran_high_t, cospi_28_64)) - (@as(tran_high_t, step2[@as(c_int, 29)]) * @as(tran_high_t, cospi_4_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step2[@as(c_int, 18)]) * @as(tran_high_t, cospi_4_64)) + (@as(tran_high_t, step2[@as(c_int, 29)]) * @as(tran_high_t, cospi_28_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = step2[@as(c_int, 19)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = step2[@as(c_int, 20)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 21)]) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 21)]) * @as(tran_high_t, cospi_12_64)) + (@as(tran_high_t, step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 22)]) * @as(tran_high_t, cospi_12_64)) - (@as(tran_high_t, step2[@as(c_int, 25)]) * @as(tran_high_t, cospi_20_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step2[@as(c_int, 22)]) * @as(tran_high_t, cospi_20_64)) + (@as(tran_high_t, step2[@as(c_int, 25)]) * @as(tran_high_t, cospi_12_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = step2[@as(c_int, 23)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = step2[@as(c_int, 24)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = step2[@as(c_int, 27)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = step2[@as(c_int, 28)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, step1[@as(c_int, 0)] + step1[@as(c_int, 1)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 0)] - step1[@as(c_int, 1)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, step1[@as(c_int, 2)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step1[@as(c_int, 3)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 2)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 3)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 6)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step1[@as(c_int, 9)]) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, step1[@as(c_int, 14)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step1[@as(c_int, 10)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = step1[@as(c_int, 11)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = step1[@as(c_int, 12)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] + step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 17)] + step1[@as(c_int, 18)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 17)] - step1[@as(c_int, 18)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] - step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 20)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 21)] + step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 21)] + step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 20)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 24)] + step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 25)] + step1[@as(c_int, 26)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 25)] - step1[@as(c_int, 26)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 24)] - step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 28)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 29)] + step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 29)] + step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 28)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] + step2[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] + step2[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] - step2[@as(c_int, 2)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] - step2[@as(c_int, 3)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = step2[@as(c_int, 4)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, step2[@as(c_int, 6)] - step2[@as(c_int, 5)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 5)] + step2[@as(c_int, 6)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = step2[@as(c_int, 7)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 9)] + step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 9)] - step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 8)] - step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 12)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(-step2[@as(c_int, 13)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 13)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 12)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = step2[@as(c_int, 17)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 18)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step2[@as(c_int, 29)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 18)]) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, step2[@as(c_int, 29)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 19)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step2[@as(c_int, 28)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, step2[@as(c_int, 19)]) * @as(tran_high_t, cospi_24_64)) + (@as(tran_high_t, step2[@as(c_int, 28)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 20)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step2[@as(c_int, 27)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step2[@as(c_int, 20)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step2[@as(c_int, 27)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = (@as(tran_high_t, -step2[@as(c_int, 21)]) * @as(tran_high_t, cospi_24_64)) - (@as(tran_high_t, step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_8_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = (@as(tran_high_t, -step2[@as(c_int, 21)]) * @as(tran_high_t, cospi_8_64)) + (@as(tran_high_t, step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_24_64));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = step2[@as(c_int, 22)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = step2[@as(c_int, 23)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = step2[@as(c_int, 24)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = step2[@as(c_int, 25)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = step2[@as(c_int, 30)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] + step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] + step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] + step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] + step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] - step1[@as(c_int, 4)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] - step1[@as(c_int, 5)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] - step1[@as(c_int, 6)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] - step1[@as(c_int, 7)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 8)] = step1[@as(c_int, 8)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 9)] = step1[@as(c_int, 9)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step1[@as(c_int, 10)] + step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 10)] + step1[@as(c_int, 13)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step1[@as(c_int, 11)] + step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step1[@as(c_int, 11)] + step1[@as(c_int, 12)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 14)] = step1[@as(c_int, 14)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 15)] = step1[@as(c_int, 15)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 16)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 17)] + step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 18)] + step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 19)] + step1[@as(c_int, 20)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 19)] - step1[@as(c_int, 20)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 18)] - step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 17)] - step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 16)] - step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 24)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 25)] + step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 26)] + step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(-step1[@as(c_int, 27)] + step1[@as(c_int, 28)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 27)] + step1[@as(c_int, 28)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 26)] + step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 25)] + step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step2[@as(c_int, 31)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 24)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] + step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] + step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 2)] + step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 3)] + step2[@as(c_int, 12)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 4)] + step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 5)] + step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 6)] + step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 7)] + step2[@as(c_int, 8)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 7)] - step2[@as(c_int, 8)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 6)] - step2[@as(c_int, 9)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 5)] - step2[@as(c_int, 10)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 4)] - step2[@as(c_int, 11)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 3)] - step2[@as(c_int, 12)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 2)] - step2[@as(c_int, 13)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 1)] - step2[@as(c_int, 14)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step2[@as(c_int, 0)] - step2[@as(c_int, 15)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 16)] = step2[@as(c_int, 16)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 17)] = step2[@as(c_int, 17)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 18)] = step2[@as(c_int, 18)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 19)] = step2[@as(c_int, 19)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step2[@as(c_int, 20)] + step2[@as(c_int, 27)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 20)] + step2[@as(c_int, 27)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step2[@as(c_int, 21)] + step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 21)] + step2[@as(c_int, 26)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step2[@as(c_int, 22)] + step2[@as(c_int, 25)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 22)] + step2[@as(c_int, 25)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp1 = @as(tran_high_t, -step2[@as(c_int, 23)] + step2[@as(c_int, 24)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    temp2 = @as(tran_high_t, step2[@as(c_int, 23)] + step2[@as(c_int, 24)]) * @as(tran_high_t, cospi_16_64);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp1), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(temp2), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 28)] = step2[@as(c_int, 28)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 29)] = step2[@as(c_int, 29)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 30)] = step2[@as(c_int, 30)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    step1[@as(c_int, 31)] = step2[@as(c_int, 31)];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 0)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] + step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 1)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] + step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 2)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] + step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 3)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] + step1[@as(c_int, 28)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 4)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] + step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 5)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 5)] + step1[@as(c_int, 26)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 6)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 6)] + step1[@as(c_int, 25)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 7)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 7)] + step1[@as(c_int, 24)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 8)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 8)] + step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 9)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 9)] + step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 10)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 10)] + step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 11)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 11)] + step1[@as(c_int, 20)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 12)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 12)] + step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 13)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 13)] + step1[@as(c_int, 18)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 14)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 14)] + step1[@as(c_int, 17)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 15)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 15)] + step1[@as(c_int, 16)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 16)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 15)] - step1[@as(c_int, 16)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 17)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 14)] - step1[@as(c_int, 17)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 18)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 13)] - step1[@as(c_int, 18)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 19)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 12)] - step1[@as(c_int, 19)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 20)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 11)] - step1[@as(c_int, 20)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 21)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 10)] - step1[@as(c_int, 21)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 22)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 9)] - step1[@as(c_int, 22)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 23)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 8)] - step1[@as(c_int, 23)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 24)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 7)] - step1[@as(c_int, 24)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 25)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 6)] - step1[@as(c_int, 25)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 26)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 5)] - step1[@as(c_int, 26)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 27)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 4)] - step1[@as(c_int, 27)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 28)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 3)] - step1[@as(c_int, 28)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 29)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 2)] - step1[@as(c_int, 29)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 30)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 1)] - step1[@as(c_int, 30)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    output[@as(c_int, 31)] = @as(i32, @truncate(highbd_check_range(step1[@as(c_int, 0)] - step1[@as(c_int, 31)], bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_idct32x32_1024_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [1024]tran_low_t = undefined;
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var zero_coeff: tran_low_t = 0;
            _ = &zero_coeff;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    zero_coeff |= input[@bitCast(@as(isize, @intCast(j)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (zero_coeff != 0) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                highbd_idct32_c(input, outptr, bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(outptr)), 0, @sizeOf(tran_low_t) *% @as(c_ulong, 32));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            highbd_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct32x32_135_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [1024]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([1023]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_idct32_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var destT: [*c]u16 = dest;
            _ = &destT;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            highbd_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    destT[@bitCast(@as(isize, @intCast(i)))] = highbd_clip_pixel_add(destT[@bitCast(@as(isize, @intCast(i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    destT += @as(usize, @bitCast(@as(isize, @intCast(stride))));
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
pub export fn antfly_vp9_vpx_highbd_idct32x32_34_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var out: [1024]tran_low_t = [1]tran_low_t{
        0,
    } ++ @as([1023]tran_low_t, @splat(0));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var outptr: [*c]tran_low_t = @ptrCast(@alignCast(&out));
    _ = &outptr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp_in: [32]tran_low_t = undefined;
    _ = &temp_in;
    var temp_out: [32]tran_low_t = undefined;
    _ = &temp_out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_idct32_c(input, outptr, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            input += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            outptr += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 32)))));
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
        while (i < @as(c_int, 32)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    temp_in[@bitCast(@as(isize, @intCast(j)))] = out[@bitCast(@as(isize, @intCast((j * @as(c_int, 32)) + i)))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            highbd_idct32_c(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                j = 0;
                while (j < @as(c_int, 32)) : (j += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast((j * stride) + i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast((j * stride) + i)))], (temp_out[@bitCast(@as(isize, @intCast(j)))] + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6)), bd);
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
pub export fn antfly_vp9_vpx_highbd_idct32x32_1_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var bd = arg_bd;
    _ = &bd;
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
    var a1: c_int = undefined;
    _ = &a1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var out: tran_low_t = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, input[@as(c_int, 0)]) * @as(tran_high_t, cospi_16_64)), bd)));
    _ = &out;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    out = @as(i32, @truncate(highbd_check_range(dct_const_round_shift(@as(tran_high_t, out) * @as(tran_high_t, cospi_16_64)), bd)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    a1 = (out + (@as(c_int, 1) << @intCast(@as(c_int, 6) - @as(c_int, 1)))) >> @intCast(@as(c_int, 6));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        j = 0;
        while (j < @as(c_int, 32)) : (j += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                i = 0;
                while (i < @as(c_int, 32)) : (i += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dest[@bitCast(@as(isize, @intCast(i)))] = highbd_clip_pixel_add(dest[@bitCast(@as(isize, @intCast(i)))], a1, bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dest += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
