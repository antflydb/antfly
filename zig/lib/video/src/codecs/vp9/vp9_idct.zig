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
pub const DCT_DCT = common.DCT_DCT;
pub const TX_TYPE = common.TX_TYPE;
pub extern fn antfly_vp9_vpx_highbd_idct16x16_10_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct16x16_1_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct16x16_256_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct16x16_38_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct32x32_1024_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct32x32_135_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct32x32_1_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct32x32_34_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct4x4_16_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct4x4_1_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct8x8_12_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct8x8_1_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct8x8_64_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_iwht4x4_16_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_iwht4x4_1_add_c(input: [*c]const tran_low_t, dest: [*c]u16, stride: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_idct16x16_10_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct16x16_1_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct16x16_256_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct16x16_38_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct32x32_1024_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct32x32_135_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct32x32_1_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct32x32_34_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct4x4_16_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct4x4_1_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct8x8_12_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct8x8_1_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_idct8x8_64_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_iwht4x4_16_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub extern fn antfly_vp9_vpx_iwht4x4_1_add_c(input: [*c]const tran_low_t, dest: [*c]u8, stride: c_int) void;
pub fn check_range(input: tran_high_t) callconv(.c) tran_high_t {
    return @import("../runtime.zig").vp9TransformRange(input, 8);
}
pub fn highbd_check_range(input: tran_high_t, bd: c_int) callconv(.c) tran_high_t {
    return @import("../runtime.zig").vp9TransformRange(input, bd);
}
pub extern fn antfly_vp9_idct4_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_idct8_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_idct16_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_iadst4_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_iadst8_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_iadst16_c(input: [*c]const tran_low_t, output: [*c]tran_low_t) void;
pub extern fn antfly_vp9_vpx_highbd_idct4_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct8_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_idct16_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_iadst4_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_iadst8_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_iadst16_c(input: [*c]const tran_low_t, output: [*c]tran_low_t, bd: c_int) void;
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
pub const transform_1d = ?*const fn ([*c]const tran_low_t, [*c]tran_low_t) callconv(.c) void;
pub const transform_2d = extern struct {
    cols: transform_1d = null,
    rows: transform_1d = null,
};
pub const highbd_transform_1d = ?*const fn ([*c]const tran_low_t, [*c]tran_low_t, bd: c_int) callconv(.c) void;
pub const highbd_transform_2d = extern struct {
    cols: highbd_transform_1d = null,
    rows: highbd_transform_1d = null,
};
pub export fn antfly_vp9_vp9_iht4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_tx_type: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const IHT_4: [4]transform_2d = [4]transform_2d{
        transform_2d{
            .cols = antfly_vp9_idct4_c,
            .rows = antfly_vp9_idct4_c,
        },
        transform_2d{
            .cols = antfly_vp9_iadst4_c,
            .rows = antfly_vp9_idct4_c,
        },
        transform_2d{
            .cols = antfly_vp9_idct4_c,
            .rows = antfly_vp9_iadst4_c,
        },
        transform_2d{
            .cols = antfly_vp9_iadst4_c,
            .rows = antfly_vp9_iadst4_c,
        },
    };
    _ = &IHT_4;
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
            IHT_4[@bitCast(@as(isize, @intCast(tx_type)))].rows.?(input, outptr);
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
            IHT_4[@bitCast(@as(isize, @intCast(tx_type)))].cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
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
pub const IHT_8: [4]transform_2d = [4]transform_2d{
    transform_2d{
        .cols = antfly_vp9_idct8_c,
        .rows = antfly_vp9_idct8_c,
    },
    transform_2d{
        .cols = antfly_vp9_iadst8_c,
        .rows = antfly_vp9_idct8_c,
    },
    transform_2d{
        .cols = antfly_vp9_idct8_c,
        .rows = antfly_vp9_iadst8_c,
    },
    transform_2d{
        .cols = antfly_vp9_iadst8_c,
        .rows = antfly_vp9_iadst8_c,
    },
};
pub export fn antfly_vp9_vp9_iht8x8_64_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_tx_type: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
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
    const ht: transform_2d = IHT_8[@bitCast(@as(isize, @intCast(tx_type)))];
    _ = &ht;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ht.rows.?(input, outptr);
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
            ht.cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
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
pub const IHT_16: [4]transform_2d = [4]transform_2d{
    transform_2d{
        .cols = antfly_vp9_idct16_c,
        .rows = antfly_vp9_idct16_c,
    },
    transform_2d{
        .cols = antfly_vp9_iadst16_c,
        .rows = antfly_vp9_idct16_c,
    },
    transform_2d{
        .cols = antfly_vp9_idct16_c,
        .rows = antfly_vp9_iadst16_c,
    },
    transform_2d{
        .cols = antfly_vp9_iadst16_c,
        .rows = antfly_vp9_iadst16_c,
    },
};
pub export fn antfly_vp9_vp9_iht16x16_256_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_tx_type: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
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
    const ht: transform_2d = IHT_16[@bitCast(@as(isize, @intCast(tx_type)))];
    _ = &ht;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ht.rows.?(input, outptr);
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
            ht.cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)));
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
pub export fn antfly_vp9_vp9_idct4x4_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob > @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_idct4x4_16_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_idct4x4_1_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_iwht4x4_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob > @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_iwht4x4_16_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_iwht4x4_1_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_idct8x8_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_idct8x8_1_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 12)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_idct8x8_12_add_c(input, dest, stride);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_idct8x8_64_add_c(input, dest, stride);
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
pub export fn antfly_vp9_vp9_idct16x16_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_idct16x16_1_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 10)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_idct16x16_10_add_c(input, dest, stride);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (eob <= @as(c_int, 38)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_idct16x16_38_add_c(input, dest, stride);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_idct16x16_256_add_c(input, dest, stride);
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
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_idct32x32_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_idct32x32_1_add_c(input, dest, stride);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 34)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_idct32x32_34_add_c(input, dest, stride);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (eob <= @as(c_int, 135)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_idct32x32_135_add_c(input, dest, stride);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_idct32x32_1024_add_c(input, dest, stride);
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
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_iht4x4_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_idct4x4_add(input, dest, stride, eob);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_iht4x4_16_add_c(input, dest, stride, @bitCast(tx_type));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_iht8x8_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_idct8x8_add(input, dest, stride, eob);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_iht8x8_64_add_c(input, dest, stride, @bitCast(tx_type));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_iht16x16_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u8, arg_stride: c_int, arg_eob: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_idct16x16_add(input, dest, stride, eob);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_iht16x16_256_add_c(input, dest, stride, @bitCast(tx_type));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_highbd_iht4x4_16_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_tx_type: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const IHT_4: [4]highbd_transform_2d = [4]highbd_transform_2d{
        highbd_transform_2d{
            .cols = antfly_vp9_vpx_highbd_idct4_c,
            .rows = antfly_vp9_vpx_highbd_idct4_c,
        },
        highbd_transform_2d{
            .cols = antfly_vp9_vpx_highbd_iadst4_c,
            .rows = antfly_vp9_vpx_highbd_idct4_c,
        },
        highbd_transform_2d{
            .cols = antfly_vp9_vpx_highbd_idct4_c,
            .rows = antfly_vp9_vpx_highbd_iadst4_c,
        },
        highbd_transform_2d{
            .cols = antfly_vp9_vpx_highbd_iadst4_c,
            .rows = antfly_vp9_vpx_highbd_iadst4_c,
        },
    };
    _ = &IHT_4;
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
            IHT_4[@bitCast(@as(isize, @intCast(tx_type)))].rows.?(input, outptr, bd);
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
            IHT_4[@bitCast(@as(isize, @intCast(tx_type)))].cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
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
pub const HIGH_IHT_8: [4]highbd_transform_2d = [4]highbd_transform_2d{
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_idct8_c,
        .rows = antfly_vp9_vpx_highbd_idct8_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_iadst8_c,
        .rows = antfly_vp9_vpx_highbd_idct8_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_idct8_c,
        .rows = antfly_vp9_vpx_highbd_iadst8_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_iadst8_c,
        .rows = antfly_vp9_vpx_highbd_iadst8_c,
    },
};
pub export fn antfly_vp9_vp9_highbd_iht8x8_64_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_tx_type: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
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
    const ht: highbd_transform_2d = HIGH_IHT_8[@bitCast(@as(isize, @intCast(tx_type)))];
    _ = &ht;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ht.rows.?(input, outptr, bd);
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
            ht.cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
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
pub const HIGH_IHT_16: [4]highbd_transform_2d = [4]highbd_transform_2d{
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_idct16_c,
        .rows = antfly_vp9_vpx_highbd_idct16_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_iadst16_c,
        .rows = antfly_vp9_vpx_highbd_idct16_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_idct16_c,
        .rows = antfly_vp9_vpx_highbd_iadst16_c,
    },
    highbd_transform_2d{
        .cols = antfly_vp9_vpx_highbd_iadst16_c,
        .rows = antfly_vp9_vpx_highbd_iadst16_c,
    },
};
pub export fn antfly_vp9_vp9_highbd_iht16x16_256_add_c(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_tx_type: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var tx_type = arg_tx_type;
    _ = &tx_type;
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
    const ht: highbd_transform_2d = HIGH_IHT_16[@bitCast(@as(isize, @intCast(tx_type)))];
    _ = &ht;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 16)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            ht.rows.?(input, outptr, bd);
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
            ht.cols.?(@ptrCast(@alignCast(&temp_in)), @ptrCast(@alignCast(&temp_out)), bd);
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
pub export fn antfly_vp9_vp9_highbd_idct4x4_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob > @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_idct4x4_16_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_idct4x4_1_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_highbd_iwht4x4_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob > @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_iwht4x4_16_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_iwht4x4_1_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_highbd_idct8x8_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_idct8x8_1_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 12)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct8x8_12_add_c(input, dest, stride, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct8x8_64_add_c(input, dest, stride, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_highbd_idct16x16_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_idct16x16_1_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 10)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct16x16_10_add_c(input, dest, stride, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (eob <= @as(c_int, 38)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_highbd_idct16x16_38_add_c(input, dest, stride, bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_highbd_idct16x16_256_add_c(input, dest, stride, bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
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
pub export fn antfly_vp9_vp9_highbd_idct32x32_add(arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (eob == @as(c_int, 1)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vpx_highbd_idct32x32_1_add_c(input, dest, stride, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (eob <= @as(c_int, 34)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_highbd_idct32x32_34_add_c(input, dest, stride, bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (eob <= @as(c_int, 135)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_highbd_idct32x32_135_add_c(input, dest, stride, bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                antfly_vp9_vpx_highbd_idct32x32_1024_add_c(input, dest, stride, bd);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
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
pub export fn antfly_vp9_vp9_highbd_iht4x4_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_idct4x4_add(input, dest, stride, eob, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_iht4x4_16_add_c(input, dest, stride, @bitCast(tx_type), bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
}
pub export fn antfly_vp9_vp9_highbd_iht8x8_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_idct8x8_add(input, dest, stride, eob, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_iht8x8_64_add_c(input, dest, stride, @bitCast(tx_type), bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_highbd_iht16x16_add(arg_tx_type: TX_TYPE, arg_input: [*c]const tran_low_t, arg_dest: [*c]u16, arg_stride: c_int, arg_eob: c_int, arg_bd: c_int) void {
    var tx_type = arg_tx_type;
    _ = &tx_type;
    var input = arg_input;
    _ = &input;
    var dest = arg_dest;
    _ = &dest;
    var stride = arg_stride;
    _ = &stride;
    var eob = arg_eob;
    _ = &eob;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (tx_type == @as(TX_TYPE, DCT_DCT)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_idct16x16_add(input, dest, stride, eob, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_vp9_vp9_highbd_iht16x16_256_add_c(input, dest, stride, @bitCast(tx_type), bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
