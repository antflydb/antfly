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
pub const ptrdiff_t = common.ptrdiff_t;
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
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub fn d207_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < (bs - @as(c_int, 1))) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(i8, @truncate(((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 1))))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 1)) * stride)))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < (bs - @as(c_int, 2))) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 2)) * stride)))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 1)) * stride)))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < (bs - @as(c_int, 2))) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, bs - @as(c_int, 1)) * stride) + @as(ptrdiff_t, c))))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        r = bs - @as(c_int, 2);
        while (r >= @as(c_int, 0)) : (r -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < (bs - @as(c_int, 2))) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, r) * stride) + @as(ptrdiff_t, c))))] = dst[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, r + @as(c_int, 1)) * stride) + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 2))))];
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
pub fn d63_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
    var size: c_int = undefined;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(i8, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(c)))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(stride + @as(ptrdiff_t, c))))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 2;
        size = bs - @as(c_int, 2);
        while (r < bs) : ({
            r += 2;
            size -= 1;
        }) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 0)) * stride)))))), @ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(r >> @intCast(@as(c_int, 1)))))))), @bitCast(@as(c_long, size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 0)) * stride))))) + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs - size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 1)) * stride)))))), @ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(stride))))) + @as(usize, @bitCast(@as(isize, @intCast(r >> @intCast(@as(c_int, 1)))))))), @bitCast(@as(c_long, size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 1)) * stride))))) + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs - size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn d45_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_right: u8 = above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    _ = &above_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst_row0: [*c]const u8 = dst;
    _ = &dst_row0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var size: c_int = undefined;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < (bs - @as(c_int, 1))) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(x)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(x + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))] = above_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 1;
        size = bs - @as(c_int, 2);
        while (x < bs) : ({
            x += 1;
            size -= 1;
        }) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(dst_row0 + @as(usize, @bitCast(@as(isize, @intCast(x)))))), @bitCast(@as(c_long, size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above_right, @bitCast(@as(c_long, x + @as(c_int, 1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn d117_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
        c = 0;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(i8, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 1;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 3;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r - @as(c_int, 2)) * stride)))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 3))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 2))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        r = 2;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 1;
                while (c < bs) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = dst[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, -@as(c_int, 2)) * stride) + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 1))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn d135_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var border: [63]u8 = undefined;
    _ = &border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (bs - @as(c_int, 2))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            border[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 3)) - i)))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 2)) - i)))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 1)) - i)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 2))))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 0))))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, above[@as(c_int, 0)]))) + @as(c_int, above[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (bs - @as(c_int, 2))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            border[@bitCast(@as(isize, @intCast((bs + @as(c_int, 1)) + i)))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(i)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(i + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
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
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, i) * stride)))))), @ptrCast(@alignCast(((@as([*c]u8, @ptrCast(@alignCast(&border))) + @as(usize, @bitCast(@as(isize, @intCast(bs))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))) - @as(usize, @bitCast(@as(isize, @intCast(i)))))), @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn d153_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
    dst[@as(c_int, 0)] = @bitCast(@as(i8, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + @as(c_int, left[@as(c_int, 0)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 1;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(i8, @truncate(((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))]) + @as(c_int, left[@bitCast(@as(isize, @intCast(r)))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(stride)))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 2;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(i8, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < (bs - @as(c_int, 2))) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(i8, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c)))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 1;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < (bs - @as(c_int, 2))) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = dst[@bitCast(@as(isize, @intCast((-stride + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 2))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn v_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(above)), @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn h_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), left[@bitCast(@as(isize, @intCast(r)))], @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn tm_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
    var ytop_left: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &ytop_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < bs) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = clip_pixel((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) - ytop_left);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dc_128_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), 128, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dc_left_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += left[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    expected_dc = @divTrunc(sum + (bs >> @intCast(@as(c_int, 1))), bs);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dc_top_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += above[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    expected_dc = @divTrunc(sum + (bs >> @intCast(@as(c_int, 1))), bs);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn dc_predictor(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u8, arg_left: [*c]const u8) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const count: c_int = @as(c_int, 2) * bs;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += above[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += left[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    expected_dc = @divTrunc(sum + (count >> @intCast(@as(c_int, 1))), count);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d207_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate(((I + J) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((J + K) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((K + L) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate((((I + (@as(c_int, 2) * J)) + K) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((J + (@as(c_int, 2) * K)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((K + (@as(c_int, 2) * L)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = blk_3: {
                    const tmp_4 = blk_4: {
                        const tmp_5 = @as(u8, @bitCast(@as(i8, @truncate(L))));
                        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_5;
                        break :blk_4 tmp_5;
                    };
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_4;
                    break :blk_3 tmp_4;
                };
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d63_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const E: c_int = above[@as(c_int, 4)];
    _ = &E;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const F: c_int = above[@as(c_int, 5)];
    _ = &F;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const G: c_int = above[@as(c_int, 6)];
    _ = &G;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate(((A + B) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((B + C) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((C + D) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((D + E) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = @bitCast(@as(i8, @truncate(((E + F) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = @bitCast(@as(i8, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((C + (@as(c_int, 2) * D)) + E) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((D + (@as(c_int, 2) * E)) + F) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate((((E + (@as(c_int, 2) * F)) + G) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d45_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const E: c_int = above[@as(c_int, 4)];
    _ = &E;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const F: c_int = above[@as(c_int, 5)];
    _ = &F;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const G: c_int = above[@as(c_int, 6)];
    _ = &G;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const H: c_int = above[@as(c_int, 7)];
    _ = &H;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u8, @bitCast(@as(i8, @truncate((((C + (@as(c_int, 2) * D)) + E) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = @as(u8, @bitCast(@as(i8, @truncate((((D + (@as(c_int, 2) * E)) + F) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u8, @bitCast(@as(i8, @truncate((((E + (@as(c_int, 2) * F)) + G) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((F + (@as(c_int, 2) * G)) + H) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate(H)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d117_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((X + A) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((A + B) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((B + C) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate(((C + D) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate((((K + (@as(c_int, 2) * J)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = @bitCast(@as(i8, @truncate((((J + (@as(c_int, 2) * I)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((I + (@as(c_int, 2) * X)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((X + (@as(c_int, 2) * A)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = @bitCast(@as(i8, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d135_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate((((J + (@as(c_int, 2) * K)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((I + (@as(c_int, 2) * J)) + K) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u8, @bitCast(@as(i8, @truncate((((X + (@as(c_int, 2) * I)) + J) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = @as(u8, @bitCast(@as(i8, @truncate((((A + (@as(c_int, 2) * X)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u8, @bitCast(@as(i8, @truncate((((B + (@as(c_int, 2) * A)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((C + (@as(c_int, 2) * B)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate((((D + (@as(c_int, 2) * C)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d153_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((I + X) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((J + I) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate(((K + J) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate(((L + K) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(i8, @truncate((((X + (@as(c_int, 2) * A)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((I + (@as(c_int, 2) * X)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((J + (@as(c_int, 2) * I)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u8, @bitCast(@as(i8, @truncate((((K + (@as(c_int, 2) * J)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(i8, @truncate((((L + (@as(c_int, 2) * K)) + J) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_d207_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
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
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < (bs - @as(c_int, 1))) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 1))))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 1)) * stride)))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < (bs - @as(c_int, 2))) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 2)) * stride)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, bs - @as(c_int, 1)) * stride)))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < (bs - @as(c_int, 2))) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, bs - @as(c_int, 1)) * stride) + @as(ptrdiff_t, c))))] = left[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        r = bs - @as(c_int, 2);
        while (r >= @as(c_int, 0)) : (r -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < (bs - @as(c_int, 2))) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, r) * stride) + @as(ptrdiff_t, c))))] = dst[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, r + @as(c_int, 1)) * stride) + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 2))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
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
pub fn highbd_d63_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
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
    var size: c_int = undefined;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(c)))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(stride + @as(ptrdiff_t, c))))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 2;
        size = bs - @as(c_int, 2);
        while (r < bs) : ({
            r += 2;
            size -= 1;
        }) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 0)) * stride)))))), @ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(r >> @intCast(@as(c_int, 1)))))))), @as(c_ulong, @bitCast(@as(c_long, size))) *% @sizeOf(@TypeOf(dst.*)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 0)) * stride))))) + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs - size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 1)) * stride)))))), @ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(stride))))) + @as(usize, @bitCast(@as(isize, @intCast(r >> @intCast(@as(c_int, 1)))))))), @as(c_ulong, @bitCast(@as(c_long, size))) *% @sizeOf(@TypeOf(dst.*)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast((dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, r + @as(c_int, 1)) * stride))))) + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs - size)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_d45_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const above_right: u16 = above[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))];
    _ = &above_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const dst_row0: [*c]const u16 = dst;
    _ = &dst_row0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var size: c_int = undefined;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < (bs - @as(c_int, 1))) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(x)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(x + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))] = above_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 1;
        size = bs - @as(c_int, 2);
        while (x < bs) : ({
            x += 1;
            size -= 1;
        }) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(dst_row0 + @as(usize, @bitCast(@as(isize, @intCast(x)))))), @as(c_ulong, @bitCast(@as(c_long, size))) *% @sizeOf(@TypeOf(dst.*)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(size)))))), above_right, @bitCast(@as(c_long, x + @as(c_int, 1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_d117_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
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
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 1;
        while (c < bs) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 3;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r - @as(c_int, 2)) * stride)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 3))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 2))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    {
        r = 2;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 1;
                while (c < bs) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = dst[@bitCast(@as(isize, @intCast(((@as(ptrdiff_t, -@as(c_int, 2)) * stride) + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 1))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_d135_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
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
    var border: [63]u16 = undefined;
    _ = &border;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (bs - @as(c_int, 2))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            border[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 3)) - i)))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 2)) - i)))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast((bs - @as(c_int, 1)) - i)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 2))))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    border[@bitCast(@as(isize, @intCast(bs - @as(c_int, 0))))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, above[@as(c_int, 0)]))) + @as(c_int, above[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (bs - @as(c_int, 2))) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            border[@bitCast(@as(isize, @intCast((bs + @as(c_int, 1)) + i)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(i)))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(i + @as(c_int, 2))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
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
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, i) * stride)))))), @ptrCast(@alignCast(((@as([*c]u16, @ptrCast(@alignCast(&border))) + @as(usize, @bitCast(@as(isize, @intCast(bs))))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))))) - @as(usize, @bitCast(@as(isize, @intCast(i)))))), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(dst[@as(c_int, 0)])));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_d153_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
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
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(c_short, @truncate(((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + @as(c_int, left[@as(c_int, 0)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 1;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))]) + @as(c_int, left[@bitCast(@as(isize, @intCast(r)))])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@as(c_int, 0)] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@as(c_int, 0)]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))) + @as(c_int, above[@as(c_int, 0)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(stride)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, left[@as(c_int, 0)]))) + @as(c_int, left[@as(c_int, 1)])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 2;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, r) * stride)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 2))))]) + (@as(c_int, 2) * @as(c_int, left[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))]))) + @as(c_int, left[@bitCast(@as(isize, @intCast(r)))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        c = 0;
        while (c < (bs - @as(c_int, 2))) : (c += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst[@bitCast(@as(isize, @intCast(c)))] = @bitCast(@as(c_short, @truncate((((@as(c_int, above[@bitCast(@as(isize, @intCast(c - @as(c_int, 1))))]) + (@as(c_int, 2) * @as(c_int, above[@bitCast(@as(isize, @intCast(c)))]))) + @as(c_int, above[@bitCast(@as(isize, @intCast(c + @as(c_int, 1))))])) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 1;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < (bs - @as(c_int, 2))) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = dst[@bitCast(@as(isize, @intCast((-stride + @as(ptrdiff_t, c)) - @as(ptrdiff_t, 2))))];
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_v_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(above)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(u16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_h_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), left[@bitCast(@as(isize, @intCast(r)))], @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_tm_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
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
    var ytop_left: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &ytop_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                c = 0;
                while (c < bs) : (c += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(c)))] = clip_pixel_highbd((@as(c_int, left[@bitCast(@as(isize, @intCast(r)))]) + @as(c_int, above[@bitCast(@as(isize, @intCast(c)))])) - ytop_left, bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_dc_128_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), @as(c_int, 128) << @intCast(bd - @as(c_int, 8)), @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_dc_left_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += left[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    expected_dc = @divTrunc(sum + (bs >> @intCast(@as(c_int, 1))), bs);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_dc_top_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += above[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    expected_dc = @divTrunc(sum + (bs >> @intCast(@as(c_int, 1))), bs);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_dc_predictor(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_bs: c_int, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var bs = arg_bs;
    _ = &bs;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    var r: c_int = undefined;
    _ = &r;
    var expected_dc: c_int = undefined;
    _ = &expected_dc;
    var sum: c_int = 0;
    _ = &sum;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const count: c_int = @as(c_int, 2) * bs;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < bs) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += above[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sum += left[@bitCast(@as(isize, @intCast(i)))];
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    expected_dc = @divTrunc(sum + (count >> @intCast(@as(c_int, 1))), count);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = 0;
        while (r < bs) : (r += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(dst)), expected_dc, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d207_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate(((I + J) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((J + K) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((K + L) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate((((I + (@as(c_int, 2) * J)) + K) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((J + (@as(c_int, 2) * K)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((K + (@as(c_int, 2) * L)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = blk_3: {
                    const tmp_4 = blk_4: {
                        const tmp_5 = @as(u16, @bitCast(@as(c_short, @truncate(L))));
                        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_5;
                        break :blk_4 tmp_5;
                    };
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_4;
                    break :blk_3 tmp_4;
                };
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d63_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const E: c_int = above[@as(c_int, 4)];
    _ = &E;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const F: c_int = above[@as(c_int, 5)];
    _ = &F;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const G: c_int = above[@as(c_int, 6)];
    _ = &G;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate(((A + B) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((B + C) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((C + D) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((D + E) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = @bitCast(@as(c_short, @truncate(((E + F) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = @bitCast(@as(c_short, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((C + (@as(c_int, 2) * D)) + E) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((D + (@as(c_int, 2) * E)) + F) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate((((E + (@as(c_int, 2) * F)) + G) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d45_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const E: c_int = above[@as(c_int, 4)];
    _ = &E;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const F: c_int = above[@as(c_int, 5)];
    _ = &F;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const G: c_int = above[@as(c_int, 6)];
    _ = &G;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const H: c_int = above[@as(c_int, 7)];
    _ = &H;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u16, @bitCast(@as(c_short, @truncate((((C + (@as(c_int, 2) * D)) + E) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = @as(u16, @bitCast(@as(c_short, @truncate((((D + (@as(c_int, 2) * E)) + F) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u16, @bitCast(@as(c_short, @truncate((((E + (@as(c_int, 2) * F)) + G) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((F + (@as(c_int, 2) * G)) + H) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate(H)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d117_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((X + A) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((A + B) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((B + C) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate(((C + D) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate((((K + (@as(c_int, 2) * J)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = @bitCast(@as(c_short, @truncate((((J + (@as(c_int, 2) * I)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((I + (@as(c_int, 2) * X)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((X + (@as(c_int, 2) * A)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = @bitCast(@as(c_short, @truncate((((B + (@as(c_int, 2) * C)) + D) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d135_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const D: c_int = above[@as(c_int, 3)];
    _ = &D;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate((((J + (@as(c_int, 2) * K)) + L) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((I + (@as(c_int, 2) * J)) + K) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u16, @bitCast(@as(c_short, @truncate((((X + (@as(c_int, 2) * I)) + J) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = blk_2: {
                const tmp_3 = @as(u16, @bitCast(@as(c_short, @truncate((((A + (@as(c_int, 2) * X)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
                dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp_3;
                break :blk_2 tmp_3;
            };
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(u16, @bitCast(@as(c_short, @truncate((((B + (@as(c_int, 2) * A)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
            dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp_2;
            break :blk_1 tmp_2;
        };
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((C + (@as(c_int, 2) * B)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate((((D + (@as(c_int, 2) * C)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d153_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const I: c_int = left[@as(c_int, 0)];
    _ = &I;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const J: c_int = left[@as(c_int, 1)];
    _ = &J;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const K: c_int = left[@as(c_int, 2)];
    _ = &K;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const L: c_int = left[@as(c_int, 3)];
    _ = &L;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const X: c_int = above[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &X;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const A: c_int = above[@as(c_int, 0)];
    _ = &A;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const B: c_int = above[@as(c_int, 1)];
    _ = &B;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const C: c_int = above[@as(c_int, 2)];
    _ = &C;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((I + X) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((J + I) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate(((K + J) + @as(c_int, 1)) >> @intCast(@as(c_int, 1))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 0)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate(((L + K) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate((((A + (@as(c_int, 2) * B)) + C) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 2)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = @bitCast(@as(c_short, @truncate((((X + (@as(c_int, 2) * A)) + B) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 0)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((I + (@as(c_int, 2) * X)) + A) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 1)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((J + (@as(c_int, 2) * I)) + X) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 2)) * stride))))] = blk: {
        const tmp = @as(u16, @bitCast(@as(c_short, @truncate((((K + (@as(c_int, 2) * J)) + I) + @as(c_int, 2)) >> @intCast(@as(c_int, 2))))));
        dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 3)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = tmp;
        break :blk tmp;
    };
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, @as(c_int, 1)) + (@as(ptrdiff_t, @as(c_int, 3)) * stride))))] = @bitCast(@as(c_short, @truncate((((L + (@as(c_int, 2) * K)) + J) + @as(c_int, 2)) >> @intCast(@as(c_int, 2)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d207_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d207_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d207_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d207_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d207_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d207_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d207_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d207_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d207_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d207_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d207_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d207_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d63_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d63_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d63_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d63_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d63_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d63_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d63_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d63_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d63_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d63_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d63_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d63_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d45_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d45_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d45_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d45_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d45_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d45_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d45_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d45_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d45_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d45_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d45_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d45_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d117_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d117_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d117_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d117_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d117_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d117_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d117_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d117_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d117_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d117_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d117_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d117_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d135_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d135_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d135_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d135_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d135_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d135_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d135_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d135_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d135_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d135_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d135_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d135_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d153_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d153_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d153_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d153_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_d153_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    d153_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d153_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d153_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d153_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d153_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_d153_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_d153_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_v_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    v_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_v_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    v_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_v_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    v_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_v_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    v_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_v_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_v_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_v_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_v_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_v_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_v_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_v_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_v_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_h_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    h_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_h_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    h_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_h_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    h_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_h_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    h_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_h_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_h_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_h_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_h_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_h_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_h_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_h_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_h_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_tm_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    tm_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_tm_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    tm_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_tm_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    tm_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_tm_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    tm_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_tm_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_tm_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_tm_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_tm_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_tm_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_tm_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_tm_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_tm_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_128_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_128_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_128_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_128_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_128_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_128_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_128_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_128_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_128_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_128_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_128_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_128_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_128_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_128_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_128_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_128_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_left_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_left_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_left_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_left_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_left_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_left_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_left_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_left_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_left_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_left_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_left_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_left_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_left_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_left_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_left_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_left_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_top_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_top_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_top_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_top_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_top_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_top_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_top_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_top_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_top_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_top_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_top_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_top_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_top_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_top_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_top_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_top_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_predictor_4x4_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_predictor(dst, stride, 4, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_predictor_8x8_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_predictor(dst, stride, 8, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_predictor_16x16_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_predictor(dst, stride, 16, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_dc_predictor_32x32_c(arg_dst: [*c]u8, arg_stride: ptrdiff_t, arg_above: [*c]const u8, arg_left: [*c]const u8) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_predictor(dst, stride, 32, above, left);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_predictor_4x4_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_predictor(dst, stride, 4, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_predictor_8x8_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_predictor(dst, stride, 8, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_predictor_16x16_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_predictor(dst, stride, 16, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_dc_predictor_32x32_c(arg_dst: [*c]u16, arg_stride: ptrdiff_t, arg_above: [*c]const u16, arg_left: [*c]const u16, arg_bd: c_int) void {
    var dst = arg_dst;
    _ = &dst;
    var stride = arg_stride;
    _ = &stride;
    var above = arg_above;
    _ = &above;
    var left = arg_left;
    _ = &left;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_dc_predictor(dst, stride, 32, above, left, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
