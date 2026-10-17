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
//  *  Copyright (c) 2013 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
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
pub const InterpKernel = common.InterpKernel;
pub fn convolve_horiz(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_x_filters: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x_filters = arg_x_filters;
    _ = &x_filters;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var x_q4: c_int = x0_q4;
            _ = &x_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const src_x: [*c]const u8 = &src[@bitCast(@as(isize, @intCast(x_q4 >> @intCast(@as(c_int, 4)))))];
                    _ = &src_x;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const x_filter: [*c]const i16 = @ptrCast(@alignCast(&x_filters[@bitCast(@as(isize, @intCast(x_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &x_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_x[@bitCast(@as(isize, @intCast(k)))]) * @as(c_int, x_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = clip_pixel((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    x_q4 += x_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn convolve_avg_horiz(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_x_filters: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x_filters = arg_x_filters;
    _ = &x_filters;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var x_q4: c_int = x0_q4;
            _ = &x_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const src_x: [*c]const u8 = &src[@bitCast(@as(isize, @intCast(x_q4 >> @intCast(@as(c_int, 4)))))];
                    _ = &src_x;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const x_filter: [*c]const i16 = @ptrCast(@alignCast(&x_filters[@bitCast(@as(isize, @intCast(x_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &x_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_x[@bitCast(@as(isize, @intCast(k)))]) * @as(c_int, x_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, clip_pixel((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7))))) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    x_q4 += x_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn convolve_vert(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_y_filters: [*c]const InterpKernel, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var y_filters = arg_y_filters;
    _ = &y_filters;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < w) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var y_q4: c_int = y0_q4;
            _ = &y_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                y = 0;
                while (y < h) : (y += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var src_y: [*c]const u8 = &src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_q4 >> @intCast(@as(c_int, 4))) * src_stride)))];
                    _ = &src_y;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const y_filter: [*c]const i16 = @ptrCast(@alignCast(&y_filters[@bitCast(@as(isize, @intCast(y_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &y_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_y[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, k) * src_stride)))]) * @as(c_int, y_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))] = clip_pixel((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    y_q4 += y_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn convolve_avg_vert(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_y_filters: [*c]const InterpKernel, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var y_filters = arg_y_filters;
    _ = &y_filters;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < w) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var y_q4: c_int = y0_q4;
            _ = &y_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                y = 0;
                while (y < h) : (y += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var src_y: [*c]const u8 = &src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_q4 >> @intCast(@as(c_int, 4))) * src_stride)))];
                    _ = &src_y;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const y_filter: [*c]const i16 = @ptrCast(@alignCast(&y_filters[@bitCast(@as(isize, @intCast(y_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &y_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_y[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, k) * src_stride)))]) * @as(c_int, y_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))] = @bitCast(@as(i8, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))]) + @as(c_int, clip_pixel((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7))))) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    y_q4 += y_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_horiz_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_horiz(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_avg_horiz_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_avg_horiz(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_vert_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_vert(src, src_stride, dst, dst_stride, filter, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_avg_vert_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_avg_vert(src, src_stride, dst, dst_stride, filter, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp: [8640]u8 = undefined;
    _ = &temp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const intermediate_height: c_int = ((((h - @as(c_int, 1)) * y_step_q4) + y0_q4) >> @intCast(@as(c_int, 4))) + @as(c_int, 8);
    _ = &intermediate_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_horiz(src - @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))))), src_stride, @ptrCast(@alignCast(&temp)), 64, filter, x0_q4, x_step_q4, w, intermediate_height);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    convolve_vert(@as([*c]u8, @ptrCast(@alignCast(&temp))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 64) * (@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))))), 64, dst, dst_stride, filter, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve8_avg_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp: [4096]u8 align(16) = undefined;
    _ = &temp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_c(src, src_stride, @ptrCast(@alignCast(&temp)), 64, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve_avg_c(@ptrCast(@alignCast(&temp)), 64, dst, dst_stride, null, 0, 0, 0, 0, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve_copy_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var r: c_int = undefined;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = h;
        while (r > @as(c_int, 0)) : (r -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @bitCast(@as(c_long, w)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_convolve_avg_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_horiz_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_horiz_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_vert_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_vert_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_2d_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_avg_horiz_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_avg_horiz_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_avg_vert_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_avg_vert_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_scaled_avg_2d_c(arg_src: [*c]const u8, arg_src_stride: ptrdiff_t, arg_dst: [*c]u8, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_convolve8_avg_c(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_convolve_horiz(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_x_filters: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x_filters = arg_x_filters;
    _ = &x_filters;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var x_q4: c_int = x0_q4;
            _ = &x_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const src_x: [*c]const u16 = &src[@bitCast(@as(isize, @intCast(x_q4 >> @intCast(@as(c_int, 4)))))];
                    _ = &src_x;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const x_filter: [*c]const i16 = @ptrCast(@alignCast(&x_filters[@bitCast(@as(isize, @intCast(x_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &x_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_x[@bitCast(@as(isize, @intCast(k)))]) * @as(c_int, x_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = clip_pixel_highbd((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)), bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    x_q4 += x_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_convolve_avg_horiz(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_x_filters: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var x_filters = arg_x_filters;
    _ = &x_filters;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var x_q4: c_int = x0_q4;
            _ = &x_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const src_x: [*c]const u16 = &src[@bitCast(@as(isize, @intCast(x_q4 >> @intCast(@as(c_int, 4)))))];
                    _ = &src_x;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const x_filter: [*c]const i16 = @ptrCast(@alignCast(&x_filters[@bitCast(@as(isize, @intCast(x_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &x_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_x[@bitCast(@as(isize, @intCast(k)))]) * @as(c_int, x_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, clip_pixel_highbd((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)), bd))) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    x_q4 += x_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_convolve_vert(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_y_filters: [*c]const InterpKernel, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var y_filters = arg_y_filters;
    _ = &y_filters;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < w) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var y_q4: c_int = y0_q4;
            _ = &y_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                y = 0;
                while (y < h) : (y += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var src_y: [*c]const u16 = &src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_q4 >> @intCast(@as(c_int, 4))) * src_stride)))];
                    _ = &src_y;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const y_filter: [*c]const i16 = @ptrCast(@alignCast(&y_filters[@bitCast(@as(isize, @intCast(y_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &y_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_y[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, k) * src_stride)))]) * @as(c_int, y_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))] = clip_pixel_highbd((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)), bd);
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    y_q4 += y_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_convolve_avg_vert(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_y_filters: [*c]const InterpKernel, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var y_filters = arg_y_filters;
    _ = &y_filters;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    src -= @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        x = 0;
        while (x < w) : (x += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var y_q4: c_int = y0_q4;
            _ = &y_q4;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                y = 0;
                while (y < h) : (y += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var src_y: [*c]const u16 = &src[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y_q4 >> @intCast(@as(c_int, 4))) * src_stride)))];
                    _ = &src_y;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const y_filter: [*c]const i16 = @ptrCast(@alignCast(&y_filters[@bitCast(@as(isize, @intCast(y_q4 & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1)))))]));
                    _ = &y_filter;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    var k: c_int = undefined;
                    _ = &k;
                    var sum: c_int = 0;
                    _ = &sum;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        k = 0;
                        while (k < @as(c_int, 8)) : (k += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            sum += @as(c_int, src_y[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, k) * src_stride)))]) * @as(c_int, y_filter[@bitCast(@as(isize, @intCast(k)))]);
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * dst_stride)))]) + @as(c_int, clip_pixel_highbd((sum + (@as(c_int, 1) << @intCast(@as(c_int, 7) - @as(c_int, 1)))) >> @intCast(@as(c_int, 7)), bd))) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    y_q4 += y_step_q4;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_convolve(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) callconv(.c) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp: [8640]u16 = undefined;
    _ = &temp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const intermediate_height: c_int = ((((h - @as(c_int, 1)) * y_step_q4) + y0_q4) >> @intCast(@as(c_int, 4))) + @as(c_int, 8);
    _ = &intermediate_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_horiz(src - @as(usize, @bitCast(@as(isize, @intCast(src_stride * @as(ptrdiff_t, @divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))))), src_stride, @ptrCast(@alignCast(&temp)), 64, filter, x0_q4, x_step_q4, w, intermediate_height, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_vert(@as([*c]u16, @ptrCast(@alignCast(&temp))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 64) * (@divTrunc(@as(c_int, 8), @as(c_int, 2)) - @as(c_int, 1)))))), 64, dst, dst_stride, filter, y0_q4, y_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_horiz_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_horiz(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_avg_horiz_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_avg_horiz(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_vert_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_vert(src, src_stride, dst, dst_stride, filter, y0_q4, y_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_avg_vert_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve_avg_vert(src, src_stride, dst, dst_stride, filter, y0_q4, y_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_convolve(src, src_stride, dst, dst_stride, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve8_avg_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var temp: [4096]u16 align(16) = undefined;
    _ = &temp;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_convolve8_c(src, src_stride, @ptrCast(@alignCast(&temp)), 64, filter, x0_q4, x_step_q4, y0_q4, y_step_q4, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_convolve_avg_c(@ptrCast(@alignCast(&temp)), 64, dst, dst_stride, null, 0, 0, 0, 0, w, h, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve_copy_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
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
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        r = h;
        while (r > @as(c_int, 0)) : (r -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(src)), @as(c_ulong, @bitCast(@as(c_long, w))) *% @sizeOf(u16));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_convolve_avg_c(arg_src: [*c]const u16, arg_src_stride: ptrdiff_t, arg_dst: [*c]u16, arg_dst_stride: ptrdiff_t, arg_filter: [*c]const InterpKernel, arg_x0_q4: c_int, arg_x_step_q4: c_int, arg_y0_q4: c_int, arg_y_step_q4: c_int, arg_w: c_int, arg_h: c_int, arg_bd: c_int) void {
    var src = arg_src;
    _ = &src;
    var src_stride = arg_src_stride;
    _ = &src_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var filter = arg_filter;
    _ = &filter;
    var x0_q4 = arg_x0_q4;
    _ = &x0_q4;
    var x_step_q4 = arg_x_step_q4;
    _ = &x_step_q4;
    var y0_q4 = arg_y0_q4;
    _ = &y0_q4;
    var y_step_q4 = arg_y_step_q4;
    _ = &y_step_q4;
    var w = arg_w;
    _ = &w;
    var h = arg_h;
    _ = &h;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x: c_int = undefined;
    _ = &x;
    var y: c_int = undefined;
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &x_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y0_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &y_step_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        y = 0;
        while (y < h) : (y += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            {
                x = 0;
                while (x < w) : (x += 1) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(((@as(c_int, dst[@bitCast(@as(isize, @intCast(x)))]) + @as(c_int, src[@bitCast(@as(isize, @intCast(x)))])) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1)))));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            }
            src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
