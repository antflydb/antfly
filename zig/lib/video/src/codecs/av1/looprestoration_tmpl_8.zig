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
pub extern fn antfly_codec_afz_trap() void;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const ptrdiff_t = common.ptrdiff_t;
pub const pixel = u8;
pub const coef = i16;
pub fn imax(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a > b) a else b;
}
pub fn umin(a: c_uint, b: c_uint) callconv(.c) c_uint {
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
pub const LR_HAVE_LEFT = common.LR_HAVE_LEFT;
pub const LR_HAVE_RIGHT = common.LR_HAVE_RIGHT;
pub const LR_HAVE_TOP = common.LR_HAVE_TOP;
pub const LR_HAVE_BOTTOM = common.LR_HAVE_BOTTOM;
pub const enum_LrEdgeFlags = common.enum_LrEdgeFlags;
pub const const_left_pixel_row = [*c][4]pixel;
const struct_unnamed_3 = extern struct {
    s0: u32 = 0,
    s1: u32 = 0,
    w0: i16 = 0,
    w1: i16 = 0,
};
pub const union_LooprestorationParams = extern union {
    filter: [2][8]i16 align(16),
    sgr: struct_unnamed_3 align(16),
};
pub const LooprestorationParams = union_LooprestorationParams;
pub const looprestorationfilter_fn = ?*const fn (dst: [*c]pixel, dst_stride: ptrdiff_t, left: const_left_pixel_row, lpf: [*c]const pixel, w: c_int, h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void;
pub const struct_Dav1dLoopRestorationDSPContext = extern struct {
    wiener: [2]looprestorationfilter_fn = @import("std").mem.zeroes([2]looprestorationfilter_fn),
    sgr: [3]looprestorationfilter_fn = @import("std").mem.zeroes([3]looprestorationfilter_fn),
};
pub const Dav1dLoopRestorationDSPContext = struct_Dav1dLoopRestorationDSPContext;
pub extern const antfly_av1_dav1d_sgr_x_by_x: [256]u8;
pub fn wiener_filter_h(arg_dst: [*c]u16, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, arg_fh: [*c]const i16, w: c_int, edges: enum_LrEdgeFlags) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    var fh = arg_fh;
    _ = &fh;
    _ = &w;
    _ = &edges;
    const bitdepth: c_int = 8;
    _ = &bitdepth;
    const round_bits_h: c_int = @as(c_int, 3) + (@as(c_int, @intFromBool(bitdepth == @as(c_int, 12))) * @as(c_int, 2));
    _ = &round_bits_h;
    const rounding_off_h: c_int = @as(c_int, 1) << @intCast(round_bits_h - @as(c_int, 1));
    _ = &rounding_off_h;
    const clip_limit: c_int = @as(c_int, 1) << @intCast(((bitdepth + @as(c_int, 1)) + @as(c_int, 7)) - round_bits_h);
    _ = &clip_limit;
    if (w < @as(c_int, 6)) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < w) : (x += 1) {
                var sum: c_int = @as(c_int, 1) << @intCast(bitdepth + @as(c_int, 6));
                _ = &sum;
                sum += @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, 128);
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < @as(c_int, 7)) : (i += 1) {
                        var idx: c_int = (x + i) - @as(c_int, 3);
                        _ = &idx;
                        if (idx < @as(c_int, 0)) {
                            if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0)) {
                                sum += @as(c_int, src[@as(c_int, 0)]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                            } else if (left != null) {
                                sum += @as(c_int, left[@as(c_int, 0)][@bitCast(@as(isize, @intCast(@as(c_int, 4) + idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                            } else {
                                sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                            }
                        } else if ((idx >= w) and !((edges & @as(enum_LrEdgeFlags, LR_HAVE_RIGHT)) != 0)) {
                            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        } else {
                            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        }
                    }
                }
                sum = iclip((sum + rounding_off_h) >> @intCast(round_bits_h), 0, clip_limit - @as(c_int, 1));
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(sum)));
            }
        }
        return;
    }
    var start: c_int = 3;
    _ = &start;
    if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0)) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < @as(c_int, 3)) : (x += 1) {
                var sum: c_int = @as(c_int, 1) << @intCast(bitdepth + @as(c_int, 6));
                _ = &sum;
                sum += @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, 128);
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < @as(c_int, 7)) : (i += 1) {
                        var idx: c_int = (x + i) - @as(c_int, 3);
                        _ = &idx;
                        if (idx < @as(c_int, 0)) {
                            sum += @as(c_int, src[@as(c_int, 0)]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        } else {
                            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        }
                    }
                }
                sum = iclip((sum + rounding_off_h) >> @intCast(round_bits_h), 0, clip_limit - @as(c_int, 1));
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(sum)));
            }
        }
    } else if (left != null) {
        {
            var x: c_int = 0;
            _ = &x;
            while (x < @as(c_int, 3)) : (x += 1) {
                var sum: c_int = @as(c_int, 1) << @intCast(bitdepth + @as(c_int, 6));
                _ = &sum;
                sum += @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, 128);
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < @as(c_int, 7)) : (i += 1) {
                        var idx: c_int = (x + i) - @as(c_int, 3);
                        _ = &idx;
                        if (idx < @as(c_int, 0)) {
                            sum += @as(c_int, left[@as(c_int, 0)][@bitCast(@as(isize, @intCast(@as(c_int, 4) + idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        } else {
                            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                        }
                    }
                }
                sum = iclip((sum + rounding_off_h) >> @intCast(round_bits_h), 0, clip_limit - @as(c_int, 1));
                dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(sum)));
            }
        }
    } else {
        start = 0;
    }
    var end: c_int = w - @as(c_int, 3);
    _ = &end;
    if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_RIGHT)) != 0) {
        end = w;
    }
    {
        var x: c_int = start;
        _ = &x;
        while (x < end) : (x += 1) {
            var sum: c_int = @as(c_int, 1) << @intCast(bitdepth + @as(c_int, 6));
            _ = &sum;
            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, 128);
            {
                var i: c_int = 0;
                _ = &i;
                while (i < @as(c_int, 7)) : (i += 1) {
                    var idx: c_int = (x + i) - @as(c_int, 3);
                    _ = &idx;
                    sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                }
            }
            sum = iclip((sum + rounding_off_h) >> @intCast(round_bits_h), 0, clip_limit - @as(c_int, 1));
            dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(sum)));
        }
    }
    {
        var x: c_int = end;
        _ = &x;
        while (x < w) : (x += 1) {
            var sum: c_int = @as(c_int, 1) << @intCast(bitdepth + @as(c_int, 6));
            _ = &sum;
            sum += @as(c_int, src[@bitCast(@as(isize, @intCast(x)))]) * @as(c_int, 128);
            {
                var i: c_int = 0;
                _ = &i;
                while (i < @as(c_int, 7)) : (i += 1) {
                    var idx: c_int = (x + i) - @as(c_int, 3);
                    _ = &idx;
                    if (idx >= w) {
                        sum += @as(c_int, src[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                    } else {
                        sum += @as(c_int, src[@bitCast(@as(isize, @intCast(idx)))]) * @as(c_int, fh[@bitCast(@as(isize, @intCast(i)))]);
                    }
                }
            }
            sum = iclip((sum + rounding_off_h) >> @intCast(round_bits_h), 0, clip_limit - @as(c_int, 1));
            dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(c_short, @truncate(sum)));
        }
    }
}
pub fn wiener_filter_v(arg_p: [*c]pixel, arg_ptrs: [*c][*c]u16, arg_fv: [*c]const i16, w: c_int) callconv(.c) void {
    var p = arg_p;
    _ = &p;
    var ptrs = arg_ptrs;
    _ = &ptrs;
    var fv = arg_fv;
    _ = &fv;
    _ = &w;
    const bitdepth: c_int = 8;
    _ = &bitdepth;
    const round_bits_v: c_int = @as(c_int, 11) - (@as(c_int, @intFromBool(bitdepth == @as(c_int, 12))) * @as(c_int, 2));
    _ = &round_bits_v;
    const rounding_off_v: c_int = @as(c_int, 1) << @intCast(round_bits_v - @as(c_int, 1));
    _ = &rounding_off_v;
    const round_offset: c_int = @as(c_int, 1) << @intCast(bitdepth + (round_bits_v - @as(c_int, 1)));
    _ = &round_offset;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            var sum: c_int = -round_offset;
            _ = &sum;
            {
                var k: c_int = 0;
                _ = &k;
                while (k < @as(c_int, 6)) : (k += 1) {
                    sum += @as(c_int, ptrs[@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(i)))]) * @as(c_int, fv[@bitCast(@as(isize, @intCast(k)))]);
                }
            }
            sum += @as(c_int, ptrs[@as(c_int, 5)][@bitCast(@as(isize, @intCast(i)))]) * @as(c_int, fv[@as(c_int, 6)]);
            p[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(iclip_u8((sum + rounding_off_v) >> @intCast(round_bits_v)))));
        }
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 5)) : (i += 1) {
            ptrs[@bitCast(@as(isize, @intCast(i)))] = ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
        }
    }
}
pub fn wiener_filter_hv(arg_p: [*c]pixel, arg_ptrs: [*c][*c]u16, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, arg_filter: [*c][8]i16, w: c_int, edges: enum_LrEdgeFlags) callconv(.c) void {
    var p = arg_p;
    _ = &p;
    var ptrs = arg_ptrs;
    _ = &ptrs;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    var filter = arg_filter;
    _ = &filter;
    _ = &w;
    _ = &edges;
    const bitdepth: c_int = 8;
    _ = &bitdepth;
    const round_bits_v: c_int = @as(c_int, 11) - (@as(c_int, @intFromBool(bitdepth == @as(c_int, 12))) * @as(c_int, 2));
    _ = &round_bits_v;
    const rounding_off_v: c_int = @as(c_int, 1) << @intCast(round_bits_v - @as(c_int, 1));
    _ = &rounding_off_v;
    const round_offset: c_int = @as(c_int, 1) << @intCast(bitdepth + (round_bits_v - @as(c_int, 1)));
    _ = &round_offset;
    var fh: [*c]const i16 = @ptrCast(@alignCast(&filter[@as(c_int, 0)]));
    _ = &fh;
    var fv: [*c]const i16 = @ptrCast(@alignCast(&filter[@as(c_int, 1)]));
    _ = &fv;
    var tmp: [390]u16 = undefined;
    _ = &tmp;
    wiener_filter_h(@ptrCast(@alignCast(&tmp)), left, src, fh, w, edges);
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            var sum: c_int = -round_offset;
            _ = &sum;
            {
                var k: c_int = 0;
                _ = &k;
                while (k < @as(c_int, 6)) : (k += 1) {
                    sum += @as(c_int, ptrs[@bitCast(@as(isize, @intCast(k)))][@bitCast(@as(isize, @intCast(i)))]) * @as(c_int, fv[@bitCast(@as(isize, @intCast(k)))]);
                }
            }
            sum += @as(c_int, tmp[@bitCast(@as(isize, @intCast(i)))]) * @as(c_int, fv[@as(c_int, 6)]);
            p[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(iclip_u8((sum + rounding_off_v) >> @intCast(round_bits_v)))));
        }
    }
    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(ptrs[@as(c_int, 6)])), @ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&tmp))))), @sizeOf(u16) *% @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 390)))));
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 6)) : (i += 1) {
            ptrs[@bitCast(@as(isize, @intCast(i)))] = ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
        }
    }
    ptrs[@as(c_int, 6)] = ptrs[@as(c_int, 0)];
}
pub fn wiener_c(arg_p: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][4]pixel, arg_lpf: [*c]const pixel, w: c_int, arg_h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void {
    var p = arg_p;
    _ = &p;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    var lpf = arg_lpf;
    _ = &lpf;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &params;
    _ = &edges;
    var afz_src_8: [*c]pixel = undefined;
    _ = &afz_src_8;
    var afz_lpf_bottom_7: [*c]pixel = undefined;
    _ = &afz_lpf_bottom_7;
    var afz_fv_6: [*c]i16 = undefined;
    _ = &afz_fv_6;
    var afz_fh_5: [*c]i16 = undefined;
    _ = &afz_fh_5;
    var afz_filter_4: [*c][8]i16 = undefined;
    _ = &afz_filter_4;
    var afz_i_3: c_int = undefined;
    _ = &afz_i_3;
    var afz_ptrs_1: [7][*c]u16 = undefined;
    _ = &afz_ptrs_1;
    var afz_rows_2: [6][*c]u16 = undefined;
    _ = &afz_rows_2;
    var afz_hor_0: [2340]u16 = undefined;
    _ = &afz_hor_0;
    var afz_pc: c_uint = 95;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return;
            },
            1 => {
                afz_pc = 7;
                continue;
            },
            2 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 1;
                continue;
            },
            3 => {
                wiener_filter_v(p, @ptrCast(@alignCast(&afz_ptrs_1)), afz_fv_6, w);
                afz_pc = 2;
                continue;
            },
            4 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 3;
                continue;
            },
            5 => {
                wiener_filter_v(p, @ptrCast(@alignCast(&afz_ptrs_1)), afz_fv_6, w);
                afz_pc = 4;
                continue;
            },
            6 => {
                return;
            },
            7 => {
                wiener_filter_v(p, @ptrCast(@alignCast(&afz_ptrs_1)), afz_fv_6, w);
                afz_pc = 6;
                continue;
            },
            8 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 7;
                continue;
            },
            9 => {
                wiener_filter_hv(p, @ptrCast(@alignCast(&afz_ptrs_1)), null, afz_lpf_bottom_7, afz_filter_4, w, edges);
                afz_pc = 8;
                continue;
            },
            10 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_lpf_bottom_7 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 10;
                continue;
            },
            12 => {
                wiener_filter_hv(p, @ptrCast(@alignCast(&afz_ptrs_1)), null, afz_lpf_bottom_7, afz_filter_4, w, edges);
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_pc = 5;
                continue;
            },
            14 => {
                afz_pc = @bitCast(@as(c_int, if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_BOTTOM)) != 0)) @as(c_int, 13) else @as(c_int, 12)));
                continue;
            },
            15 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) > @as(c_int, 0)) @as(c_int, 19) else @as(c_int, 14)));
                continue;
            },
            16 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 16;
                continue;
            },
            18 => {
                left += 1;
                afz_pc = 17;
                continue;
            },
            19 => {
                wiener_filter_hv(p, @ptrCast(@alignCast(&afz_ptrs_1)), left, afz_src_8, afz_filter_4, w, edges);
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_ptrs_1[@as(c_int, 6)] = afz_ptrs_1[@as(c_int, 5)] + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 390)))));
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_pc = 5;
                continue;
            },
            22 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 21) else @as(c_int, 20)));
                continue;
            },
            23 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 22;
                continue;
            },
            24 => {
                left += 1;
                afz_pc = 23;
                continue;
            },
            25 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 4)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_ptrs_1[@as(c_int, 5)] = afz_rows_2[@as(c_int, 4)];
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_pc = 3;
                continue;
            },
            28 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 27) else @as(c_int, 26)));
                continue;
            },
            29 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 28;
                continue;
            },
            30 => {
                left += 1;
                afz_pc = 29;
                continue;
            },
            31 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 3)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 30;
                continue;
            },
            32 => {
                afz_ptrs_1[@as(c_int, 4)] = blk: {
                    const tmp = afz_rows_2[@as(c_int, 3)];
                    afz_ptrs_1[@as(c_int, 5)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_pc = 7;
                continue;
            },
            34 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 33) else @as(c_int, 32)));
                continue;
            },
            35 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 34;
                continue;
            },
            36 => {
                left += 1;
                afz_pc = 35;
                continue;
            },
            37 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 2)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 36;
                continue;
            },
            38 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 1)], null, lpf, afz_fh_5, w, edges);
                afz_pc = 37;
                continue;
            },
            39 => {
                lpf += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 38;
                continue;
            },
            40 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 0)], null, lpf, afz_fh_5, w, edges);
                afz_pc = 39;
                continue;
            },
            41 => {
                afz_ptrs_1[@as(c_int, 5)] = afz_rows_2[@as(c_int, 2)];
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_ptrs_1[@as(c_int, 4)] = afz_rows_2[@as(c_int, 2)];
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_ptrs_1[@as(c_int, 3)] = afz_rows_2[@as(c_int, 2)];
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_ptrs_1[@as(c_int, 2)] = afz_rows_2[@as(c_int, 1)];
                afz_pc = 43;
                continue;
            },
            45 => {
                afz_ptrs_1[@as(c_int, 1)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 44;
                continue;
            },
            46 => {
                afz_ptrs_1[@as(c_int, 0)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_pc = 5;
                continue;
            },
            48 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 47) else @as(c_int, 20)));
                continue;
            },
            49 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 49;
                continue;
            },
            51 => {
                left += 1;
                afz_pc = 50;
                continue;
            },
            52 => {
                wiener_filter_hv(p, @ptrCast(@alignCast(&afz_ptrs_1)), left, afz_src_8, afz_filter_4, w, edges);
                afz_pc = 51;
                continue;
            },
            53 => {
                afz_ptrs_1[@as(c_int, 6)] = afz_rows_2[@as(c_int, 4)];
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_pc = 5;
                continue;
            },
            55 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 54) else @as(c_int, 53)));
                continue;
            },
            56 => {
                p += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 56;
                continue;
            },
            58 => {
                left += 1;
                afz_pc = 57;
                continue;
            },
            59 => {
                wiener_filter_hv(p, @ptrCast(@alignCast(&afz_ptrs_1)), left, afz_src_8, afz_filter_4, w, edges);
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_ptrs_1[@as(c_int, 6)] = afz_rows_2[@as(c_int, 3)];
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_pc = 5;
                continue;
            },
            62 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 61) else @as(c_int, 60)));
                continue;
            },
            63 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 62;
                continue;
            },
            64 => {
                left += 1;
                afz_pc = 63;
                continue;
            },
            65 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 2)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_ptrs_1[@as(c_int, 5)] = afz_rows_2[@as(c_int, 2)];
                afz_pc = 65;
                continue;
            },
            67 => {
                afz_pc = 3;
                continue;
            },
            68 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 67) else @as(c_int, 66)));
                continue;
            },
            69 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 68;
                continue;
            },
            70 => {
                left += 1;
                afz_pc = 69;
                continue;
            },
            71 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 1)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 70;
                continue;
            },
            72 => {
                afz_ptrs_1[@as(c_int, 4)] = blk: {
                    const tmp = afz_rows_2[@as(c_int, 1)];
                    afz_ptrs_1[@as(c_int, 5)] = tmp;
                    break :blk tmp;
                };
                afz_pc = 71;
                continue;
            },
            73 => {
                afz_pc = 7;
                continue;
            },
            74 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 73) else @as(c_int, 72)));
                continue;
            },
            75 => {
                afz_src_8 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 74;
                continue;
            },
            76 => {
                left += 1;
                afz_pc = 75;
                continue;
            },
            77 => {
                wiener_filter_h(afz_rows_2[@as(c_int, 0)], left, afz_src_8, afz_fh_5, w, edges);
                afz_pc = 76;
                continue;
            },
            78 => {
                afz_ptrs_1[@as(c_int, 5)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_ptrs_1[@as(c_int, 4)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 78;
                continue;
            },
            80 => {
                afz_ptrs_1[@as(c_int, 3)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_ptrs_1[@as(c_int, 2)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_ptrs_1[@as(c_int, 1)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_ptrs_1[@as(c_int, 0)] = afz_rows_2[@as(c_int, 0)];
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_TOP)) != 0) @as(c_int, 46) else @as(c_int, 83)));
                continue;
            },
            85 => {
                afz_src_8 = p;
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_lpf_bottom_7 = @ptrCast(@alignCast(@constCast(lpf + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))))));
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_fv_6 = @ptrCast(@alignCast(@constCast(&params.*.filter[@as(c_int, 1)])));
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_fh_5 = @ptrCast(@alignCast(@constCast(&params.*.filter[@as(c_int, 0)])));
                afz_pc = 87;
                continue;
            },
            89 => {
                afz_filter_4 = @ptrCast(@alignCast(@constCast(&params.*.filter)));
                afz_pc = 88;
                continue;
            },
            90 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_3 < @as(c_int, 6)) @as(c_int, 92) else @as(c_int, 89)));
                continue;
            },
            91 => {
                afz_i_3 += 1;
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_rows_2[@bitCast(@as(isize, @intCast(afz_i_3)))] = &afz_hor_0[@bitCast(@as(isize, @intCast(afz_i_3 * @as(c_int, 390))))];
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_i_3 = 0;
                afz_pc = 90;
                continue;
            },
            94 => {
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_pc = 94;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
}
pub fn rotate(arg_sumsq_ptrs: [*c][*c]i32, arg_sum_ptrs: [*c][*c]coef, arg_n: c_int) callconv(.c) void {
    var sumsq_ptrs = arg_sumsq_ptrs;
    _ = &sumsq_ptrs;
    var sum_ptrs = arg_sum_ptrs;
    _ = &sum_ptrs;
    var n = arg_n;
    _ = &n;
    var tmp32: [*c]i32 = sumsq_ptrs[@as(c_int, 0)];
    _ = &tmp32;
    var tmpc: [*c]coef = sum_ptrs[@as(c_int, 0)];
    _ = &tmpc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < (n - @as(c_int, 1))) : (i += 1) {
            sumsq_ptrs[@bitCast(@as(isize, @intCast(i)))] = sumsq_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
            sum_ptrs[@bitCast(@as(isize, @intCast(i)))] = sum_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))];
        }
    }
    sumsq_ptrs[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))] = tmp32;
    sum_ptrs[@bitCast(@as(isize, @intCast(n - @as(c_int, 1))))] = tmpc;
}
pub fn rotate5_x2(arg_sumsq_ptrs: [*c][*c]i32, arg_sum_ptrs: [*c][*c]coef) callconv(.c) void {
    var sumsq_ptrs = arg_sumsq_ptrs;
    _ = &sumsq_ptrs;
    var sum_ptrs = arg_sum_ptrs;
    _ = &sum_ptrs;
    var tmp32: [2][*c]i32 = undefined;
    _ = &tmp32;
    var tmpc: [2][*c]coef = undefined;
    _ = &tmpc;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 2)) : (i += 1) {
            tmp32[@bitCast(@as(isize, @intCast(i)))] = sumsq_ptrs[@bitCast(@as(isize, @intCast(i)))];
            tmpc[@bitCast(@as(isize, @intCast(i)))] = sum_ptrs[@bitCast(@as(isize, @intCast(i)))];
        }
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 3)) : (i += 1) {
            sumsq_ptrs[@bitCast(@as(isize, @intCast(i)))] = sumsq_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 2))))];
            sum_ptrs[@bitCast(@as(isize, @intCast(i)))] = sum_ptrs[@bitCast(@as(isize, @intCast(i + @as(c_int, 2))))];
        }
    }
    {
        var i: c_int = 0;
        _ = &i;
        while (i < @as(c_int, 2)) : (i += 1) {
            sumsq_ptrs[@bitCast(@as(isize, @intCast(@as(c_int, 3) + i)))] = tmp32[@bitCast(@as(isize, @intCast(i)))];
            sum_ptrs[@bitCast(@as(isize, @intCast(@as(c_int, 3) + i)))] = tmpc[@bitCast(@as(isize, @intCast(i)))];
        }
    }
}
pub fn sgr_box3_row_h(arg_sumsq: [*c]i32, arg_sum: [*c]coef, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, w: c_int, edges: enum_LrEdgeFlags) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    _ = &w;
    _ = &edges;
    sumsq += 1;
    sum += 1;
    var a: c_int = if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0) if (left != null) @as(c_int, left[@as(c_int, 0)][@as(c_int, 2)]) else @as(c_int, src[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))]) else @as(c_int, src[@as(c_int, 0)]);
    _ = &a;
    var b: c_int = if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0) if (left != null) @as(c_int, left[@as(c_int, 0)][@as(c_int, 3)]) else @as(c_int, src[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else @as(c_int, src[@as(c_int, 0)]);
    _ = &b;
    {
        var x: c_int = -@as(c_int, 1);
        _ = &x;
        while (x < (w + @as(c_int, 1))) : (x += 1) {
            var c: c_int = if (((x + @as(c_int, 1)) < w) or (@as(c_int, @bitCast(edges & @as(enum_LrEdgeFlags, LR_HAVE_RIGHT))) != 0)) @as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 1))))]) else @as(c_int, src[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))]);
            _ = &c;
            sum[@bitCast(@as(isize, @intCast(x)))] = @truncate((a + b) + c);
            sumsq[@bitCast(@as(isize, @intCast(x)))] = ((a * a) + (b * b)) + (c * c);
            a = b;
            b = c;
        }
    }
}
pub fn sgr_box5_row_h(arg_sumsq: [*c]i32, arg_sum: [*c]coef, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, w: c_int, edges: enum_LrEdgeFlags) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    _ = &w;
    _ = &edges;
    sumsq += 1;
    sum += 1;
    var a: c_int = if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0) if (left != null) @as(c_int, left[@as(c_int, 0)][@as(c_int, 1)]) else @as(c_int, src[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))]) else @as(c_int, src[@as(c_int, 0)]);
    _ = &a;
    var b: c_int = if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0) if (left != null) @as(c_int, left[@as(c_int, 0)][@as(c_int, 2)]) else @as(c_int, src[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))]) else @as(c_int, src[@as(c_int, 0)]);
    _ = &b;
    var c: c_int = if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_LEFT)) != 0) if (left != null) @as(c_int, left[@as(c_int, 0)][@as(c_int, 3)]) else @as(c_int, src[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else @as(c_int, src[@as(c_int, 0)]);
    _ = &c;
    var d: c_int = src[@as(c_int, 0)];
    _ = &d;
    {
        var x: c_int = -@as(c_int, 1);
        _ = &x;
        while (x < (w + @as(c_int, 1))) : (x += 1) {
            var e: c_int = if (((x + @as(c_int, 2)) < w) or (@as(c_int, @bitCast(edges & @as(enum_LrEdgeFlags, LR_HAVE_RIGHT))) != 0)) @as(c_int, src[@bitCast(@as(isize, @intCast(x + @as(c_int, 2))))]) else @as(c_int, src[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))]);
            _ = &e;
            sum[@bitCast(@as(isize, @intCast(x)))] = @truncate((((a + b) + c) + d) + e);
            sumsq[@bitCast(@as(isize, @intCast(x)))] = ((((a * a) + (b * b)) + (c * c)) + (d * d)) + (e * e);
            a = b;
            b = c;
            c = d;
            d = e;
        }
    }
}
pub fn sgr_box35_row_h(arg_sumsq3: [*c]i32, arg_sum3: [*c]coef, arg_sumsq5: [*c]i32, arg_sum5: [*c]coef, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, w: c_int, edges: enum_LrEdgeFlags) callconv(.c) void {
    var sumsq3 = arg_sumsq3;
    _ = &sumsq3;
    var sum3 = arg_sum3;
    _ = &sum3;
    var sumsq5 = arg_sumsq5;
    _ = &sumsq5;
    var sum5 = arg_sum5;
    _ = &sum5;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    _ = &w;
    _ = &edges;
    sgr_box3_row_h(sumsq3, sum3, left, src, w, edges);
    sgr_box5_row_h(sumsq5, sum5, left, src, w, edges);
}
pub fn sgr_box3_row_v(arg_sumsq: [*c][*c]i32, arg_sum: [*c][*c]coef, arg_sumsq_out: [*c]i32, arg_sum_out: [*c]coef, w: c_int) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var sumsq_out = arg_sumsq_out;
    _ = &sumsq_out;
    var sum_out = arg_sum_out;
    _ = &sum_out;
    _ = &w;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < (w + @as(c_int, 2))) : (x += 1) {
            var sq_a: c_int = sumsq[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_a;
            var sq_b: c_int = sumsq[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_b;
            var sq_c: c_int = sumsq[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_c;
            var s_a: c_int = sum[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_a;
            var s_b: c_int = sum[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_b;
            var s_c: c_int = sum[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_c;
            sumsq_out[@bitCast(@as(isize, @intCast(x)))] = (sq_a + sq_b) + sq_c;
            sum_out[@bitCast(@as(isize, @intCast(x)))] = @truncate((s_a + s_b) + s_c);
        }
    }
}
pub fn sgr_box5_row_v(arg_sumsq: [*c][*c]i32, arg_sum: [*c][*c]coef, arg_sumsq_out: [*c]i32, arg_sum_out: [*c]coef, w: c_int) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var sumsq_out = arg_sumsq_out;
    _ = &sumsq_out;
    var sum_out = arg_sum_out;
    _ = &sum_out;
    _ = &w;
    {
        var x: c_int = 0;
        _ = &x;
        while (x < (w + @as(c_int, 2))) : (x += 1) {
            var sq_a: c_int = sumsq[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_a;
            var sq_b: c_int = sumsq[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_b;
            var sq_c: c_int = sumsq[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_c;
            var sq_d: c_int = sumsq[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_d;
            var sq_e: c_int = sumsq[@as(c_int, 4)][@bitCast(@as(isize, @intCast(x)))];
            _ = &sq_e;
            var s_a: c_int = sum[@as(c_int, 0)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_a;
            var s_b: c_int = sum[@as(c_int, 1)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_b;
            var s_c: c_int = sum[@as(c_int, 2)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_c;
            var s_d: c_int = sum[@as(c_int, 3)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_d;
            var s_e: c_int = sum[@as(c_int, 4)][@bitCast(@as(isize, @intCast(x)))];
            _ = &s_e;
            sumsq_out[@bitCast(@as(isize, @intCast(x)))] = (((sq_a + sq_b) + sq_c) + sq_d) + sq_e;
            sum_out[@bitCast(@as(isize, @intCast(x)))] = @truncate((((s_a + s_b) + s_c) + s_d) + s_e);
        }
    }
}
pub fn sgr_calc_row_ab(arg_AA: [*c]i32, arg_BB: [*c]coef, arg_w: c_int, arg_s: c_int, arg_bitdepth_max: c_int, arg_n: c_int, arg_sgr_one_by_x: c_int) callconv(.c) void {
    var AA = arg_AA;
    _ = &AA;
    var BB = arg_BB;
    _ = &BB;
    var w = arg_w;
    _ = &w;
    var s = arg_s;
    _ = &s;
    var bitdepth_max = arg_bitdepth_max;
    _ = &bitdepth_max;
    var n = arg_n;
    _ = &n;
    var sgr_one_by_x = arg_sgr_one_by_x;
    _ = &sgr_one_by_x;
    const bitdepth_min_8: c_int = @as(c_int, 8) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < (w + @as(c_int, 2))) : (i += 1) {
            const a: c_int = (AA[@bitCast(@as(isize, @intCast(i)))] + ((@as(c_int, 1) << @intCast(@as(c_int, 2) * bitdepth_min_8)) >> @intCast(@as(c_int, 1)))) >> @intCast(@as(c_int, 2) * bitdepth_min_8);
            _ = &a;
            const b: c_int = (@as(c_int, BB[@bitCast(@as(isize, @intCast(i)))]) + ((@as(c_int, 1) << @intCast(bitdepth_min_8)) >> @intCast(@as(c_int, 1)))) >> @intCast(bitdepth_min_8);
            _ = &b;
            const p: c_uint = @bitCast(@as(c_int, imax((a * n) - (b * b), 0)));
            _ = &p;
            const z: c_uint = ((p *% @as(c_uint, @bitCast(@as(c_int, s)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, 19)))))) >> @intCast(@as(c_uint, 20));
            _ = &z;
            const x: c_uint = antfly_av1_dav1d_sgr_x_by_x[umin(z, 255)];
            _ = &x;
            AA[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(c_uint, @truncate((((x *% @as(c_uint, @bitCast(@as(c_int, @as(c_int, BB[@bitCast(@as(isize, @intCast(i)))]))))) *% @as(c_uint, @bitCast(@as(c_int, sgr_one_by_x)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 1) << @intCast(@as(c_int, 11)))))) >> @intCast(@as(c_uint, 12)))));
            BB[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(c_ushort, @truncate(x)));
        }
    }
}
pub fn sgr_box3_vert(arg_sumsq: [*c][*c]i32, arg_sum: [*c][*c]coef, arg_sumsq_out: [*c]i32, arg_sum_out: [*c]coef, w: c_int, s: c_int, bitdepth_max: c_int) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var sumsq_out = arg_sumsq_out;
    _ = &sumsq_out;
    var sum_out = arg_sum_out;
    _ = &sum_out;
    _ = &w;
    _ = &s;
    _ = &bitdepth_max;
    sgr_box3_row_v(sumsq, sum, sumsq_out, sum_out, w);
    sgr_calc_row_ab(sumsq_out, sum_out, w, s, bitdepth_max, 9, 455);
    rotate(sumsq, sum, 3);
}
pub fn sgr_box5_vert(arg_sumsq: [*c][*c]i32, arg_sum: [*c][*c]coef, arg_sumsq_out: [*c]i32, arg_sum_out: [*c]coef, w: c_int, s: c_int, bitdepth_max: c_int) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var sumsq_out = arg_sumsq_out;
    _ = &sumsq_out;
    var sum_out = arg_sum_out;
    _ = &sum_out;
    _ = &w;
    _ = &s;
    _ = &bitdepth_max;
    sgr_box5_row_v(sumsq, sum, sumsq_out, sum_out, w);
    sgr_calc_row_ab(sumsq_out, sum_out, w, s, bitdepth_max, 25, 164);
    rotate5_x2(sumsq, sum);
}
pub fn sgr_box3_hv(arg_sumsq: [*c][*c]i32, arg_sum: [*c][*c]coef, arg_AA: [*c]i32, arg_BB: [*c]coef, arg_left: [*c][4]pixel, arg_src: [*c]const pixel, w: c_int, s: c_int, edges: enum_LrEdgeFlags, bitdepth_max: c_int) callconv(.c) void {
    var sumsq = arg_sumsq;
    _ = &sumsq;
    var sum = arg_sum;
    _ = &sum;
    var AA = arg_AA;
    _ = &AA;
    var BB = arg_BB;
    _ = &BB;
    var left = arg_left;
    _ = &left;
    var src = arg_src;
    _ = &src;
    _ = &w;
    _ = &s;
    _ = &edges;
    _ = &bitdepth_max;
    sgr_box3_row_h(sumsq[@as(c_int, 2)], sum[@as(c_int, 2)], left, src, w, edges);
    sgr_box3_vert(sumsq, sum, AA, BB, w, s, bitdepth_max);
}
pub fn sgr_finish_filter_row1(arg_tmp: [*c]coef, arg_src: [*c]const pixel, arg_A_ptrs: [*c][*c]i32, arg_B_ptrs: [*c][*c]coef, w: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    var A_ptrs = arg_A_ptrs;
    _ = &A_ptrs;
    var B_ptrs = arg_B_ptrs;
    _ = &B_ptrs;
    _ = &w;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            const a: c_int = (((((@as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) + @as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))])) * @as(c_int, 4)) + ((((@as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))]) + @as(c_int, B_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))])) * @as(c_int, 3));
            _ = &a;
            const b: c_int = (((((A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))] + A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))]) + A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))]) + A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) + A_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) * @as(c_int, 4)) + ((((A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))] + A_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))]) + A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))]) + A_ptrs[@as(c_int, 2)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))]) * @as(c_int, 3));
            _ = &b;
            tmp[@bitCast(@as(isize, @intCast(i)))] = @truncate(((b - (a * @as(c_int, src[@bitCast(@as(isize, @intCast(i)))]))) + (@as(c_int, 1) << @intCast(@as(c_int, 8)))) >> @intCast(@as(c_int, 9)));
        }
    }
}
pub fn sgr_finish_filter2(arg_tmp: [*c]coef, arg_src: [*c]const pixel, src_stride: ptrdiff_t, arg_A_ptrs: [*c][*c]i32, arg_B_ptrs: [*c][*c]coef, w: c_int, h: c_int) callconv(.c) void {
    var tmp = arg_tmp;
    _ = &tmp;
    var src = arg_src;
    _ = &src;
    _ = &src_stride;
    var A_ptrs = arg_A_ptrs;
    _ = &A_ptrs;
    var B_ptrs = arg_B_ptrs;
    _ = &B_ptrs;
    _ = &w;
    _ = &h;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            const a: c_int = ((@as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) + @as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))])) * @as(c_int, 6)) + ((((@as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))]) + @as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))])) + @as(c_int, B_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))])) * @as(c_int, 5));
            _ = &a;
            const b: c_int = ((A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))] + A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) * @as(c_int, 6)) + ((((A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))] + A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) - @as(c_int, 1))))]) + A_ptrs[@as(c_int, 0)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))]) + A_ptrs[@as(c_int, 1)][@bitCast(@as(isize, @intCast((i + @as(c_int, 1)) + @as(c_int, 1))))]) * @as(c_int, 5));
            _ = &b;
            tmp[@bitCast(@as(isize, @intCast(i)))] = @truncate(((b - (a * @as(c_int, src[@bitCast(@as(isize, @intCast(i)))]))) + (@as(c_int, 1) << @intCast(@as(c_int, 8)))) >> @intCast(@as(c_int, 9)));
        }
    }
    if (h <= @as(c_int, 1)) return;
    tmp += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 384)))));
    src += @as(usize, @bitCast(@as(isize, @intCast(src_stride))));
    var A: [*c]const i32 = &A_ptrs[@as(c_int, 1)][@as(c_int, 1)];
    _ = &A;
    var B: [*c]const coef = &B_ptrs[@as(c_int, 1)][@as(c_int, 1)];
    _ = &B;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            const a: c_int = (@as(c_int, B[@bitCast(@as(isize, @intCast(i)))]) * @as(c_int, 6)) + ((@as(c_int, B[@bitCast(@as(isize, @intCast(i - @as(c_int, 1))))]) + @as(c_int, B[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))])) * @as(c_int, 5));
            _ = &a;
            const b: c_int = (A[@bitCast(@as(isize, @intCast(i)))] * @as(c_int, 6)) + ((A[@bitCast(@as(isize, @intCast(i - @as(c_int, 1))))] + A[@bitCast(@as(isize, @intCast(i + @as(c_int, 1))))]) * @as(c_int, 5));
            _ = &b;
            tmp[@bitCast(@as(isize, @intCast(i)))] = @truncate(((b - (a * @as(c_int, src[@bitCast(@as(isize, @intCast(i)))]))) + (@as(c_int, 1) << @intCast(@as(c_int, 7)))) >> @intCast(@as(c_int, 8)));
        }
    }
}
pub fn sgr_weighted_row1(arg_dst: [*c]pixel, arg_t1: [*c]const coef, w: c_int, w1: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var t1 = arg_t1;
    _ = &t1;
    _ = &w;
    _ = &w1;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < w) : (i += 1) {
            const v: c_int = w1 * @as(c_int, t1[@bitCast(@as(isize, @intCast(i)))]);
            _ = &v;
            dst[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(iclip_u8(@as(c_int, dst[@bitCast(@as(isize, @intCast(i)))]) + ((v + (@as(c_int, 1) << @intCast(@as(c_int, 10)))) >> @intCast(@as(c_int, 11)))))));
        }
    }
}
pub fn sgr_weighted2(arg_dst: [*c]pixel, dst_stride: ptrdiff_t, arg_t1: [*c]const coef, arg_t2: [*c]const coef, w: c_int, h: c_int, w0: c_int, w1: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &dst_stride;
    var t1 = arg_t1;
    _ = &t1;
    var t2 = arg_t2;
    _ = &t2;
    _ = &w;
    _ = &h;
    _ = &w0;
    _ = &w1;
    {
        var j: c_int = 0;
        _ = &j;
        while (j < h) : (j += 1) {
            {
                var i: c_int = 0;
                _ = &i;
                while (i < w) : (i += 1) {
                    const v: c_int = (w0 * @as(c_int, t1[@bitCast(@as(isize, @intCast(i)))])) + (w1 * @as(c_int, t2[@bitCast(@as(isize, @intCast(i)))]));
                    _ = &v;
                    dst[@bitCast(@as(isize, @intCast(i)))] = @bitCast(@as(i8, @truncate(iclip_u8(@as(c_int, dst[@bitCast(@as(isize, @intCast(i)))]) + ((v + (@as(c_int, 1) << @intCast(@as(c_int, 10)))) >> @intCast(@as(c_int, 11)))))));
                }
            }
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_stride))));
            t1 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 384)))));
            t2 += @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 384)))));
        }
    }
}
pub fn sgr_finish1(arg_dst: [*c][*c]pixel, stride: ptrdiff_t, arg_A_ptrs: [*c][*c]i32, arg_B_ptrs: [*c][*c]coef, w: c_int, w1: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var A_ptrs = arg_A_ptrs;
    _ = &A_ptrs;
    var B_ptrs = arg_B_ptrs;
    _ = &B_ptrs;
    _ = &w;
    _ = &w1;
    var tmp: [384]coef align(8) = undefined;
    _ = &tmp;
    sgr_finish_filter_row1(@ptrCast(@alignCast(&tmp)), dst.*, A_ptrs, B_ptrs, w);
    sgr_weighted_row1(dst.*, @ptrCast(@alignCast(&tmp)), w, w1);
    dst.* += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    rotate(A_ptrs, B_ptrs, 3);
}
pub fn sgr_finish2(arg_dst: [*c][*c]pixel, stride: ptrdiff_t, arg_A_ptrs: [*c][*c]i32, arg_B_ptrs: [*c][*c]coef, w: c_int, h: c_int, w1: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var A_ptrs = arg_A_ptrs;
    _ = &A_ptrs;
    var B_ptrs = arg_B_ptrs;
    _ = &B_ptrs;
    _ = &w;
    _ = &h;
    _ = &w1;
    var tmp: [768]coef align(8) = undefined;
    _ = &tmp;
    sgr_finish_filter2(@ptrCast(@alignCast(&tmp)), dst.*, stride, A_ptrs, B_ptrs, w, h);
    sgr_weighted_row1(dst.*, @ptrCast(@alignCast(&tmp)), w, w1);
    dst.* += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    if (h > @as(c_int, 1)) {
        sgr_weighted_row1(dst.*, @as([*c]coef, @ptrCast(@alignCast(&tmp))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 384))))), w, w1);
        dst.* += @as(usize, @bitCast(@as(isize, @intCast(stride))));
    }
    rotate(A_ptrs, B_ptrs, 2);
}
pub fn sgr_finish_mix(arg_dst: [*c][*c]pixel, stride: ptrdiff_t, arg_A5_ptrs: [*c][*c]i32, arg_B5_ptrs: [*c][*c]coef, arg_A3_ptrs: [*c][*c]i32, arg_B3_ptrs: [*c][*c]coef, w: c_int, h: c_int, w0: c_int, w1: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var A5_ptrs = arg_A5_ptrs;
    _ = &A5_ptrs;
    var B5_ptrs = arg_B5_ptrs;
    _ = &B5_ptrs;
    var A3_ptrs = arg_A3_ptrs;
    _ = &A3_ptrs;
    var B3_ptrs = arg_B3_ptrs;
    _ = &B3_ptrs;
    _ = &w;
    _ = &h;
    _ = &w0;
    _ = &w1;
    var tmp5: [768]coef align(8) = undefined;
    _ = &tmp5;
    var tmp3: [768]coef align(8) = undefined;
    _ = &tmp3;
    sgr_finish_filter2(@ptrCast(@alignCast(&tmp5)), dst.*, stride, A5_ptrs, B5_ptrs, w, h);
    sgr_finish_filter_row1(@ptrCast(@alignCast(&tmp3)), dst.*, A3_ptrs, B3_ptrs, w);
    if (h > @as(c_int, 1)) {
        sgr_finish_filter_row1(@as([*c]coef, @ptrCast(@alignCast(&tmp3))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 384))))), dst.* + @as(usize, @bitCast(@as(isize, @intCast(stride)))), &A3_ptrs[@as(c_int, 1)], &B3_ptrs[@as(c_int, 1)], w);
    }
    sgr_weighted2(dst.*, stride, @ptrCast(@alignCast(&tmp5)), @ptrCast(@alignCast(&tmp3)), w, h, w0, w1);
    dst.* += @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, h) * stride))));
    rotate(A5_ptrs, B5_ptrs, 2);
    rotate(A3_ptrs, B3_ptrs, 4);
}
pub fn sgr_3x3_c(arg_dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][4]pixel, arg_lpf: [*c]const pixel, w: c_int, arg_h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    var lpf = arg_lpf;
    _ = &lpf;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &params;
    _ = &edges;
    var afz_lpf_bottom_13: [*c]pixel = undefined;
    _ = &afz_lpf_bottom_13;
    var afz_src_12: [*c]pixel = undefined;
    _ = &afz_src_12;
    var afz_i_11: c_int = undefined;
    _ = &afz_i_11;
    var afz_B_ptrs_10: [3][*c]coef = undefined;
    _ = &afz_B_ptrs_10;
    var afz_A_ptrs_9: [3][*c]i32 = undefined;
    _ = &afz_A_ptrs_9;
    var afz_B_buf_8: [1216]coef = undefined;
    _ = &afz_B_buf_8;
    var afz_A_buf_7: [1216]i32 = undefined;
    _ = &afz_A_buf_7;
    var afz_i_6: c_int = undefined;
    _ = &afz_i_6;
    var afz_sum_ptrs_4: [3][*c]coef = undefined;
    _ = &afz_sum_ptrs_4;
    var afz_sum_rows_5: [3][*c]coef = undefined;
    _ = &afz_sum_rows_5;
    var afz_sumsq_ptrs_2: [3][*c]i32 = undefined;
    _ = &afz_sumsq_ptrs_2;
    var afz_sumsq_rows_3: [3][*c]i32 = undefined;
    _ = &afz_sumsq_rows_3;
    var afz_sum_buf_1: [1216]coef = undefined;
    _ = &afz_sum_buf_1;
    var afz_sumsq_buf_0: [1216]i32 = undefined;
    _ = &afz_sumsq_buf_0;
    var afz_pc: c_uint = 92;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return;
            },
            1 => {
                afz_pc = 10;
                continue;
            },
            2 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 3);
                afz_pc = 1;
                continue;
            },
            3 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 2;
                continue;
            },
            4 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_ptrs_4[@as(c_int, 1)];
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_ptrs_2[@as(c_int, 1)];
                afz_pc = 4;
                continue;
            },
            6 => {
                return;
            },
            7 => {
                sgr_finish1(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, params.*.sgr.w1);
                afz_pc = 6;
                continue;
            },
            8 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_ptrs_4[@as(c_int, 1)];
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_ptrs_2[@as(c_int, 1)];
                afz_pc = 9;
                continue;
            },
            11 => {
                sgr_finish1(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, params.*.sgr.w1);
                afz_pc = 10;
                continue;
            },
            12 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 11;
                continue;
            },
            13 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_ptrs_4[@as(c_int, 1)];
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_ptrs_2[@as(c_int, 1)];
                afz_pc = 13;
                continue;
            },
            15 => {
                return;
            },
            16 => {
                sgr_finish1(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, params.*.sgr.w1);
                afz_pc = 15;
                continue;
            },
            17 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], null, afz_lpf_bottom_13, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 16;
                continue;
            },
            18 => {
                sgr_finish1(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, params.*.sgr.w1);
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_lpf_bottom_13 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 18;
                continue;
            },
            20 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], null, afz_lpf_bottom_13, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_pc = 14;
                continue;
            },
            22 => {
                afz_pc = @bitCast(@as(c_int, if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_BOTTOM)) != 0)) @as(c_int, 21) else @as(c_int, 20)));
                continue;
            },
            23 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) > @as(c_int, 0)) @as(c_int, 27) else @as(c_int, 22)));
                continue;
            },
            24 => {
                sgr_finish1(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, params.*.sgr.w1);
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 24;
                continue;
            },
            26 => {
                left += 1;
                afz_pc = 25;
                continue;
            },
            27 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], left, afz_src_12, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_pc = 14;
                continue;
            },
            29 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 28) else @as(c_int, 27)));
                continue;
            },
            30 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 3);
                afz_pc = 29;
                continue;
            },
            31 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 30;
                continue;
            },
            32 => {
                left += 1;
                afz_pc = 31;
                continue;
            },
            33 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], left, afz_src_12, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 32;
                continue;
            },
            34 => {
                afz_pc = 5;
                continue;
            },
            35 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 34) else @as(c_int, 33)));
                continue;
            },
            36 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 3);
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 36;
                continue;
            },
            38 => {
                left += 1;
                afz_pc = 37;
                continue;
            },
            39 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], left, afz_src_12, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 38;
                continue;
            },
            40 => {
                sgr_box3_row_h(afz_sumsq_rows_3[@as(c_int, 1)], afz_sum_rows_5[@as(c_int, 1)], null, lpf, w, edges);
                afz_pc = 39;
                continue;
            },
            41 => {
                lpf += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 40;
                continue;
            },
            42 => {
                sgr_box3_row_h(afz_sumsq_rows_3[@as(c_int, 0)], afz_sum_rows_5[@as(c_int, 0)], null, lpf, w, edges);
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 2)];
                afz_pc = 42;
                continue;
            },
            44 => {
                afz_sum_ptrs_4[@as(c_int, 1)] = afz_sum_rows_5[@as(c_int, 1)];
                afz_pc = 43;
                continue;
            },
            45 => {
                afz_sum_ptrs_4[@as(c_int, 0)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 44;
                continue;
            },
            46 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 2)];
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_sumsq_ptrs_2[@as(c_int, 1)] = afz_sumsq_rows_3[@as(c_int, 1)];
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_sumsq_ptrs_2[@as(c_int, 0)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 47;
                continue;
            },
            49 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 2)];
                afz_pc = 27;
                continue;
            },
            50 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 2)];
                afz_pc = 49;
                continue;
            },
            51 => {
                afz_pc = 14;
                continue;
            },
            52 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 51) else @as(c_int, 50)));
                continue;
            },
            53 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 3);
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 53;
                continue;
            },
            55 => {
                left += 1;
                afz_pc = 54;
                continue;
            },
            56 => {
                sgr_box3_hv(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], left, afz_src_12, w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), edges, 255);
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 1)];
                afz_pc = 56;
                continue;
            },
            58 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 1)];
                afz_pc = 57;
                continue;
            },
            59 => {
                afz_pc = 5;
                continue;
            },
            60 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 59) else @as(c_int, 58)));
                continue;
            },
            61 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 3);
                afz_pc = 60;
                continue;
            },
            62 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 2)], afz_B_ptrs_10[@as(c_int, 2)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 62;
                continue;
            },
            64 => {
                left += 1;
                afz_pc = 63;
                continue;
            },
            65 => {
                sgr_box3_row_h(afz_sumsq_rows_3[@as(c_int, 0)], afz_sum_rows_5[@as(c_int, 0)], left, afz_src_12, w, edges);
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 65;
                continue;
            },
            67 => {
                afz_sum_ptrs_4[@as(c_int, 1)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 66;
                continue;
            },
            68 => {
                afz_sum_ptrs_4[@as(c_int, 0)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 67;
                continue;
            },
            69 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_sumsq_ptrs_2[@as(c_int, 1)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 69;
                continue;
            },
            71 => {
                afz_sumsq_ptrs_2[@as(c_int, 0)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 70;
                continue;
            },
            72 => {
                afz_pc = @bitCast(@as(c_int, if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_TOP)) != 0) @as(c_int, 48) else @as(c_int, 71)));
                continue;
            },
            73 => {
                afz_lpf_bottom_13 = @ptrCast(@alignCast(@constCast(lpf + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))))));
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_src_12 = dst;
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_11 < @as(c_int, 3)) @as(c_int, 78) else @as(c_int, 74)));
                continue;
            },
            76 => {
                afz_i_11 += 1;
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_B_ptrs_10[@bitCast(@as(isize, @intCast(afz_i_11)))] = &afz_B_buf_8[@bitCast(@as(isize, @intCast(afz_i_11 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 76;
                continue;
            },
            78 => {
                afz_A_ptrs_9[@bitCast(@as(isize, @intCast(afz_i_11)))] = &afz_A_buf_7[@bitCast(@as(isize, @intCast(afz_i_11 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_i_11 = 0;
                afz_pc = 75;
                continue;
            },
            80 => {
                afz_pc = 79;
                continue;
            },
            81 => {
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_6 < @as(c_int, 3)) @as(c_int, 87) else @as(c_int, 83)));
                continue;
            },
            85 => {
                afz_i_6 += 1;
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_sum_rows_5[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sum_buf_1[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_sumsq_rows_3[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sumsq_buf_0[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_i_6 = 0;
                afz_pc = 84;
                continue;
            },
            89 => {
                afz_pc = 88;
                continue;
            },
            90 => {
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_pc = 91;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
}
pub fn sgr_5x5_c(arg_dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][4]pixel, arg_lpf: [*c]const pixel, w: c_int, arg_h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    var lpf = arg_lpf;
    _ = &lpf;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &params;
    _ = &edges;
    var afz_lpf_bottom_13: [*c]pixel = undefined;
    _ = &afz_lpf_bottom_13;
    var afz_src_12: [*c]pixel = undefined;
    _ = &afz_src_12;
    var afz_i_11: c_int = undefined;
    _ = &afz_i_11;
    var afz_B_ptrs_10: [2][*c]coef = undefined;
    _ = &afz_B_ptrs_10;
    var afz_A_ptrs_9: [2][*c]i32 = undefined;
    _ = &afz_A_ptrs_9;
    var afz_B_buf_8: [816]coef = undefined;
    _ = &afz_B_buf_8;
    var afz_A_buf_7: [816]i32 = undefined;
    _ = &afz_A_buf_7;
    var afz_i_6: c_int = undefined;
    _ = &afz_i_6;
    var afz_sum_ptrs_4: [5][*c]coef = undefined;
    _ = &afz_sum_ptrs_4;
    var afz_sum_rows_5: [5][*c]coef = undefined;
    _ = &afz_sum_rows_5;
    var afz_sumsq_ptrs_2: [5][*c]i32 = undefined;
    _ = &afz_sumsq_ptrs_2;
    var afz_sumsq_rows_3: [5][*c]i32 = undefined;
    _ = &afz_sumsq_rows_3;
    var afz_sum_buf_1: [2016]coef = undefined;
    _ = &afz_sum_buf_1;
    var afz_sumsq_buf_0: [2016]i32 = undefined;
    _ = &afz_sumsq_buf_0;
    var afz_pc: c_uint = 130;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return;
            },
            1 => {
                afz_pc = 12;
                continue;
            },
            2 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 2);
                afz_pc = 1;
                continue;
            },
            3 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 2;
                continue;
            },
            4 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_ptrs_4[@as(c_int, 3)];
                afz_pc = 3;
                continue;
            },
            5 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_ptrs_2[@as(c_int, 3)];
                afz_pc = 4;
                continue;
            },
            6 => {
                return;
            },
            7 => {
                sgr_finish2(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, 1, params.*.sgr.w0);
                afz_pc = 6;
                continue;
            },
            8 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_ptrs_4[@as(c_int, 2)];
                afz_pc = 8;
                continue;
            },
            10 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_ptrs_4[@as(c_int, 2)];
                afz_pc = 9;
                continue;
            },
            11 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_ptrs_2[@as(c_int, 2)];
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_ptrs_2[@as(c_int, 2)];
                afz_pc = 11;
                continue;
            },
            13 => {
                sgr_finish2(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, 2, params.*.sgr.w0);
                afz_pc = 12;
                continue;
            },
            14 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_ptrs_4[@as(c_int, 3)];
                afz_pc = 14;
                continue;
            },
            16 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_ptrs_2[@as(c_int, 3)];
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_pc = 24;
                continue;
            },
            18 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_ptrs_4[@as(c_int, 2)];
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_ptrs_4[@as(c_int, 2)];
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_ptrs_2[@as(c_int, 2)];
                afz_pc = 19;
                continue;
            },
            21 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_ptrs_2[@as(c_int, 2)];
                afz_pc = 20;
                continue;
            },
            22 => {
                return;
            },
            23 => {
                sgr_finish2(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, 2, params.*.sgr.w0);
                afz_pc = 22;
                continue;
            },
            24 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 23;
                continue;
            },
            25 => {
                sgr_box5_row_h(afz_sumsq_ptrs_2[@as(c_int, 4)], afz_sum_ptrs_4[@as(c_int, 4)], null, afz_lpf_bottom_13, w, edges);
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_lpf_bottom_13 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 25;
                continue;
            },
            27 => {
                sgr_box5_row_h(afz_sumsq_ptrs_2[@as(c_int, 3)], afz_sum_ptrs_4[@as(c_int, 3)], null, afz_lpf_bottom_13, w, edges);
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_pc = 21;
                continue;
            },
            29 => {
                afz_pc = @bitCast(@as(c_int, if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_BOTTOM)) != 0)) @as(c_int, 28) else @as(c_int, 27)));
                continue;
            },
            30 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) > @as(c_int, 0)) @as(c_int, 40) else @as(c_int, 29)));
                continue;
            },
            31 => {
                sgr_finish2(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, 2, params.*.sgr.w0);
                afz_pc = 30;
                continue;
            },
            32 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 32;
                continue;
            },
            34 => {
                left += 1;
                afz_pc = 33;
                continue;
            },
            35 => {
                sgr_box5_row_h(afz_sumsq_ptrs_2[@as(c_int, 4)], afz_sum_ptrs_4[@as(c_int, 4)], left, afz_src_12, w, edges);
                afz_pc = 34;
                continue;
            },
            36 => {
                afz_pc = 16;
                continue;
            },
            37 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 36) else @as(c_int, 35)));
                continue;
            },
            38 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 37;
                continue;
            },
            39 => {
                left += 1;
                afz_pc = 38;
                continue;
            },
            40 => {
                sgr_box5_row_h(afz_sumsq_ptrs_2[@as(c_int, 3)], afz_sum_ptrs_4[@as(c_int, 3)], left, afz_src_12, w, edges);
                afz_pc = 39;
                continue;
            },
            41 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_rows_5[@as(c_int, 4)];
                afz_pc = 40;
                continue;
            },
            42 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_rows_3[@as(c_int, 4)];
                afz_pc = 41;
                continue;
            },
            43 => {
                afz_pc = 21;
                continue;
            },
            44 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 43) else @as(c_int, 42)));
                continue;
            },
            45 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 2);
                afz_pc = 44;
                continue;
            },
            46 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 45;
                continue;
            },
            47 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 46;
                continue;
            },
            48 => {
                left += 1;
                afz_pc = 47;
                continue;
            },
            49 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 3)], afz_sum_rows_5[@as(c_int, 3)], left, afz_src_12, w, edges);
                afz_pc = 48;
                continue;
            },
            50 => {
                afz_pc = 5;
                continue;
            },
            51 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 50) else @as(c_int, 49)));
                continue;
            },
            52 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 51;
                continue;
            },
            53 => {
                left += 1;
                afz_pc = 52;
                continue;
            },
            54 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 2)], afz_sum_rows_5[@as(c_int, 2)], left, afz_src_12, w, edges);
                afz_pc = 53;
                continue;
            },
            55 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 1)], afz_sum_rows_5[@as(c_int, 1)], null, lpf, w, edges);
                afz_pc = 54;
                continue;
            },
            56 => {
                lpf += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 55;
                continue;
            },
            57 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 0)], afz_sum_rows_5[@as(c_int, 0)], null, lpf, w, edges);
                afz_pc = 56;
                continue;
            },
            58 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_rows_5[@as(c_int, 3)];
                afz_pc = 57;
                continue;
            },
            59 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_rows_5[@as(c_int, 2)];
                afz_pc = 58;
                continue;
            },
            60 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 1)];
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_sum_ptrs_4[@as(c_int, 1)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 60;
                continue;
            },
            62 => {
                afz_sum_ptrs_4[@as(c_int, 0)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 61;
                continue;
            },
            63 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_rows_3[@as(c_int, 3)];
                afz_pc = 62;
                continue;
            },
            64 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_rows_3[@as(c_int, 2)];
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 1)];
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_sumsq_ptrs_2[@as(c_int, 1)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 65;
                continue;
            },
            67 => {
                afz_sumsq_ptrs_2[@as(c_int, 0)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 66;
                continue;
            },
            68 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_rows_5[@as(c_int, 4)];
                afz_pc = 40;
                continue;
            },
            69 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_rows_3[@as(c_int, 4)];
                afz_pc = 68;
                continue;
            },
            70 => {
                afz_pc = 21;
                continue;
            },
            71 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 70) else @as(c_int, 69)));
                continue;
            },
            72 => {
                sgr_finish2(&dst, stride, @ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), w, 2, params.*.sgr.w0);
                afz_pc = 71;
                continue;
            },
            73 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 72;
                continue;
            },
            74 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 73;
                continue;
            },
            75 => {
                left += 1;
                afz_pc = 74;
                continue;
            },
            76 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 3)], afz_sum_rows_5[@as(c_int, 3)], left, afz_src_12, w, edges);
                afz_pc = 75;
                continue;
            },
            77 => {
                afz_pc = 16;
                continue;
            },
            78 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 77) else @as(c_int, 76)));
                continue;
            },
            79 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 78;
                continue;
            },
            80 => {
                left += 1;
                afz_pc = 79;
                continue;
            },
            81 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 2)], afz_sum_rows_5[@as(c_int, 2)], left, afz_src_12, w, edges);
                afz_pc = 80;
                continue;
            },
            82 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_rows_5[@as(c_int, 3)];
                afz_pc = 81;
                continue;
            },
            83 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_rows_5[@as(c_int, 2)];
                afz_pc = 82;
                continue;
            },
            84 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_rows_3[@as(c_int, 3)];
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_rows_3[@as(c_int, 2)];
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_pc = 21;
                continue;
            },
            87 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 86) else @as(c_int, 85)));
                continue;
            },
            88 => {
                rotate(@ptrCast(@alignCast(&afz_A_ptrs_9)), @ptrCast(@alignCast(&afz_B_ptrs_10)), 2);
                afz_pc = 87;
                continue;
            },
            89 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq_ptrs_2)), @ptrCast(@alignCast(&afz_sum_ptrs_4)), afz_A_ptrs_9[@as(c_int, 1)], afz_B_ptrs_10[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 88;
                continue;
            },
            90 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 89;
                continue;
            },
            91 => {
                left += 1;
                afz_pc = 90;
                continue;
            },
            92 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 1)], afz_sum_rows_5[@as(c_int, 1)], left, afz_src_12, w, edges);
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_rows_5[@as(c_int, 1)];
                afz_pc = 92;
                continue;
            },
            94 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_rows_3[@as(c_int, 1)];
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_pc = 5;
                continue;
            },
            96 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 95) else @as(c_int, 94)));
                continue;
            },
            97 => {
                afz_src_12 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 96;
                continue;
            },
            98 => {
                left += 1;
                afz_pc = 97;
                continue;
            },
            99 => {
                sgr_box5_row_h(afz_sumsq_rows_3[@as(c_int, 0)], afz_sum_rows_5[@as(c_int, 0)], left, afz_src_12, w, edges);
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_sum_ptrs_4[@as(c_int, 4)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 99;
                continue;
            },
            101 => {
                afz_sum_ptrs_4[@as(c_int, 3)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 100;
                continue;
            },
            102 => {
                afz_sum_ptrs_4[@as(c_int, 2)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 101;
                continue;
            },
            103 => {
                afz_sum_ptrs_4[@as(c_int, 1)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 102;
                continue;
            },
            104 => {
                afz_sum_ptrs_4[@as(c_int, 0)] = afz_sum_rows_5[@as(c_int, 0)];
                afz_pc = 103;
                continue;
            },
            105 => {
                afz_sumsq_ptrs_2[@as(c_int, 4)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 104;
                continue;
            },
            106 => {
                afz_sumsq_ptrs_2[@as(c_int, 3)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 105;
                continue;
            },
            107 => {
                afz_sumsq_ptrs_2[@as(c_int, 2)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_sumsq_ptrs_2[@as(c_int, 1)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 107;
                continue;
            },
            109 => {
                afz_sumsq_ptrs_2[@as(c_int, 0)] = afz_sumsq_rows_3[@as(c_int, 0)];
                afz_pc = 108;
                continue;
            },
            110 => {
                afz_pc = @bitCast(@as(c_int, if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_TOP)) != 0) @as(c_int, 67) else @as(c_int, 109)));
                continue;
            },
            111 => {
                afz_lpf_bottom_13 = @ptrCast(@alignCast(@constCast(lpf + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))))));
                afz_pc = 110;
                continue;
            },
            112 => {
                afz_src_12 = dst;
                afz_pc = 111;
                continue;
            },
            113 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_11 < @as(c_int, 2)) @as(c_int, 116) else @as(c_int, 112)));
                continue;
            },
            114 => {
                afz_i_11 += 1;
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_B_ptrs_10[@bitCast(@as(isize, @intCast(afz_i_11)))] = &afz_B_buf_8[@bitCast(@as(isize, @intCast(afz_i_11 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 114;
                continue;
            },
            116 => {
                afz_A_ptrs_9[@bitCast(@as(isize, @intCast(afz_i_11)))] = &afz_A_buf_7[@bitCast(@as(isize, @intCast(afz_i_11 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 115;
                continue;
            },
            117 => {
                afz_i_11 = 0;
                afz_pc = 113;
                continue;
            },
            118 => {
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_pc = 120;
                continue;
            },
            122 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_6 < @as(c_int, 5)) @as(c_int, 125) else @as(c_int, 121)));
                continue;
            },
            123 => {
                afz_i_6 += 1;
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_sum_rows_5[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sum_buf_1[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 123;
                continue;
            },
            125 => {
                afz_sumsq_rows_3[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sumsq_buf_0[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 124;
                continue;
            },
            126 => {
                afz_i_6 = 0;
                afz_pc = 122;
                continue;
            },
            127 => {
                afz_pc = 126;
                continue;
            },
            128 => {
                afz_pc = 127;
                continue;
            },
            129 => {
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_pc = 129;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
}
pub fn sgr_mix_c(arg_dst: [*c]pixel, stride: ptrdiff_t, arg_left: [*c][4]pixel, arg_lpf: [*c]const pixel, w: c_int, arg_h: c_int, params: [*c]const LooprestorationParams, edges: enum_LrEdgeFlags) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    _ = &stride;
    var left = arg_left;
    _ = &left;
    var lpf = arg_lpf;
    _ = &lpf;
    _ = &w;
    var h = arg_h;
    _ = &h;
    _ = &params;
    _ = &edges;
    var afz_lpf_bottom_25: [*c]pixel = undefined;
    _ = &afz_lpf_bottom_25;
    var afz_src_24: [*c]pixel = undefined;
    _ = &afz_src_24;
    var afz_i_23: c_int = undefined;
    _ = &afz_i_23;
    var afz_B3_ptrs_22: [4][*c]coef = undefined;
    _ = &afz_B3_ptrs_22;
    var afz_A3_ptrs_21: [4][*c]i32 = undefined;
    _ = &afz_A3_ptrs_21;
    var afz_B3_buf_20: [1616]coef = undefined;
    _ = &afz_B3_buf_20;
    var afz_A3_buf_19: [1616]i32 = undefined;
    _ = &afz_A3_buf_19;
    var afz_i_18: c_int = undefined;
    _ = &afz_i_18;
    var afz_B5_ptrs_17: [2][*c]coef = undefined;
    _ = &afz_B5_ptrs_17;
    var afz_A5_ptrs_16: [2][*c]i32 = undefined;
    _ = &afz_A5_ptrs_16;
    var afz_B5_buf_15: [816]coef = undefined;
    _ = &afz_B5_buf_15;
    var afz_A5_buf_14: [816]i32 = undefined;
    _ = &afz_A5_buf_14;
    var afz_i_13: c_int = undefined;
    _ = &afz_i_13;
    var afz_sum3_ptrs_11: [3][*c]coef = undefined;
    _ = &afz_sum3_ptrs_11;
    var afz_sum3_rows_12: [3][*c]coef = undefined;
    _ = &afz_sum3_rows_12;
    var afz_sumsq3_ptrs_9: [3][*c]i32 = undefined;
    _ = &afz_sumsq3_ptrs_9;
    var afz_sumsq3_rows_10: [3][*c]i32 = undefined;
    _ = &afz_sumsq3_rows_10;
    var afz_sum3_buf_8: [1216]coef = undefined;
    _ = &afz_sum3_buf_8;
    var afz_sumsq3_buf_7: [1216]i32 = undefined;
    _ = &afz_sumsq3_buf_7;
    var afz_i_6: c_int = undefined;
    _ = &afz_i_6;
    var afz_sum5_ptrs_4: [5][*c]coef = undefined;
    _ = &afz_sum5_ptrs_4;
    var afz_sum5_rows_5: [5][*c]coef = undefined;
    _ = &afz_sum5_rows_5;
    var afz_sumsq5_ptrs_2: [5][*c]i32 = undefined;
    _ = &afz_sumsq5_ptrs_2;
    var afz_sumsq5_rows_3: [5][*c]i32 = undefined;
    _ = &afz_sumsq5_rows_3;
    var afz_sum5_buf_1: [2016]coef = undefined;
    _ = &afz_sum5_buf_1;
    var afz_sumsq5_buf_0: [2016]i32 = undefined;
    _ = &afz_sumsq5_buf_0;
    var afz_pc: c_uint = 198;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return;
            },
            1 => {
                afz_pc = 20;
                continue;
            },
            2 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 1;
                continue;
            },
            3 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 2;
                continue;
            },
            4 => {
                rotate(@ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), 2);
                afz_pc = 3;
                continue;
            },
            5 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 4;
                continue;
            },
            6 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_ptrs_11[@as(c_int, 1)];
                afz_pc = 5;
                continue;
            },
            7 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_ptrs_9[@as(c_int, 1)];
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_ptrs_4[@as(c_int, 3)];
                afz_pc = 7;
                continue;
            },
            9 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_ptrs_2[@as(c_int, 3)];
                afz_pc = 8;
                continue;
            },
            10 => {
                return;
            },
            11 => {
                sgr_finish_mix(&dst, stride, @ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), @ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), w, 1, params.*.sgr.w0, params.*.sgr.w1);
                afz_pc = 10;
                continue;
            },
            12 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 11;
                continue;
            },
            13 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 12;
                continue;
            },
            14 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 13;
                continue;
            },
            15 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_ptrs_11[@as(c_int, 1)];
                afz_pc = 14;
                continue;
            },
            16 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_ptrs_9[@as(c_int, 1)];
                afz_pc = 15;
                continue;
            },
            17 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_ptrs_4[@as(c_int, 2)];
                afz_pc = 16;
                continue;
            },
            18 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_ptrs_4[@as(c_int, 2)];
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_ptrs_2[@as(c_int, 2)];
                afz_pc = 18;
                continue;
            },
            20 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_ptrs_2[@as(c_int, 2)];
                afz_pc = 19;
                continue;
            },
            21 => {
                sgr_finish_mix(&dst, stride, @ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), @ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), w, 2, params.*.sgr.w0, params.*.sgr.w1);
                afz_pc = 20;
                continue;
            },
            22 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 21;
                continue;
            },
            23 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_ptrs_11[@as(c_int, 1)];
                afz_pc = 23;
                continue;
            },
            25 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_ptrs_9[@as(c_int, 1)];
                afz_pc = 24;
                continue;
            },
            26 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_ptrs_4[@as(c_int, 3)];
                afz_pc = 25;
                continue;
            },
            27 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_ptrs_2[@as(c_int, 3)];
                afz_pc = 26;
                continue;
            },
            28 => {
                afz_pc = 42;
                continue;
            },
            29 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_ptrs_11[@as(c_int, 1)];
                afz_pc = 28;
                continue;
            },
            30 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_ptrs_9[@as(c_int, 1)];
                afz_pc = 29;
                continue;
            },
            31 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 30;
                continue;
            },
            32 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 31;
                continue;
            },
            33 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_ptrs_11[@as(c_int, 1)];
                afz_pc = 32;
                continue;
            },
            34 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_ptrs_9[@as(c_int, 1)];
                afz_pc = 33;
                continue;
            },
            35 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_ptrs_4[@as(c_int, 2)];
                afz_pc = 34;
                continue;
            },
            36 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_ptrs_4[@as(c_int, 2)];
                afz_pc = 35;
                continue;
            },
            37 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_ptrs_2[@as(c_int, 2)];
                afz_pc = 36;
                continue;
            },
            38 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_ptrs_2[@as(c_int, 2)];
                afz_pc = 37;
                continue;
            },
            39 => {
                return;
            },
            40 => {
                sgr_finish_mix(&dst, stride, @ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), @ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), w, 2, params.*.sgr.w0, params.*.sgr.w1);
                afz_pc = 39;
                continue;
            },
            41 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 40;
                continue;
            },
            42 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 41;
                continue;
            },
            43 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_ptrs_2[@as(c_int, 4)], afz_sum5_ptrs_4[@as(c_int, 4)], null, afz_lpf_bottom_25, w, edges);
                afz_pc = 42;
                continue;
            },
            44 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 43;
                continue;
            },
            45 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 44;
                continue;
            },
            46 => {
                afz_lpf_bottom_25 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 45;
                continue;
            },
            47 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_ptrs_2[@as(c_int, 3)], afz_sum5_ptrs_4[@as(c_int, 3)], null, afz_lpf_bottom_25, w, edges);
                afz_pc = 46;
                continue;
            },
            48 => {
                afz_pc = 38;
                continue;
            },
            49 => {
                afz_pc = @bitCast(@as(c_int, if (!((edges & @as(enum_LrEdgeFlags, LR_HAVE_BOTTOM)) != 0)) @as(c_int, 48) else @as(c_int, 47)));
                continue;
            },
            50 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) > @as(c_int, 0)) @as(c_int, 63) else @as(c_int, 49)));
                continue;
            },
            51 => {
                sgr_finish_mix(&dst, stride, @ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), @ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), w, 2, params.*.sgr.w0, params.*.sgr.w1);
                afz_pc = 50;
                continue;
            },
            52 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 51;
                continue;
            },
            53 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 52;
                continue;
            },
            54 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 53;
                continue;
            },
            55 => {
                left += 1;
                afz_pc = 54;
                continue;
            },
            56 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_ptrs_2[@as(c_int, 4)], afz_sum5_ptrs_4[@as(c_int, 4)], left, afz_src_24, w, edges);
                afz_pc = 55;
                continue;
            },
            57 => {
                afz_pc = 27;
                continue;
            },
            58 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 57) else @as(c_int, 56)));
                continue;
            },
            59 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 58;
                continue;
            },
            60 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 59;
                continue;
            },
            61 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 60;
                continue;
            },
            62 => {
                left += 1;
                afz_pc = 61;
                continue;
            },
            63 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_ptrs_2[@as(c_int, 3)], afz_sum5_ptrs_4[@as(c_int, 3)], left, afz_src_24, w, edges);
                afz_pc = 62;
                continue;
            },
            64 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_rows_5[@as(c_int, 4)];
                afz_pc = 63;
                continue;
            },
            65 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_rows_3[@as(c_int, 4)];
                afz_pc = 64;
                continue;
            },
            66 => {
                afz_pc = 38;
                continue;
            },
            67 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 66) else @as(c_int, 65)));
                continue;
            },
            68 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 67;
                continue;
            },
            69 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 68;
                continue;
            },
            70 => {
                rotate(@ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), 2);
                afz_pc = 69;
                continue;
            },
            71 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 70;
                continue;
            },
            72 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 71;
                continue;
            },
            73 => {
                left += 1;
                afz_pc = 72;
                continue;
            },
            74 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_rows_3[@as(c_int, 3)], afz_sum5_rows_5[@as(c_int, 3)], left, afz_src_24, w, edges);
                afz_pc = 73;
                continue;
            },
            75 => {
                afz_pc = 9;
                continue;
            },
            76 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 75) else @as(c_int, 74)));
                continue;
            },
            77 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 76;
                continue;
            },
            78 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 77;
                continue;
            },
            79 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 78;
                continue;
            },
            80 => {
                left += 1;
                afz_pc = 79;
                continue;
            },
            81 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 2)], afz_sum3_rows_12[@as(c_int, 2)], afz_sumsq5_rows_3[@as(c_int, 2)], afz_sum5_rows_5[@as(c_int, 2)], left, afz_src_24, w, edges);
                afz_pc = 80;
                continue;
            },
            82 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 1)], afz_sum3_rows_12[@as(c_int, 1)], afz_sumsq5_rows_3[@as(c_int, 1)], afz_sum5_rows_5[@as(c_int, 1)], null, lpf, w, edges);
                afz_pc = 81;
                continue;
            },
            83 => {
                lpf += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 82;
                continue;
            },
            84 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 0)], afz_sum3_rows_12[@as(c_int, 0)], afz_sumsq5_rows_3[@as(c_int, 0)], afz_sum5_rows_5[@as(c_int, 0)], null, lpf, w, edges);
                afz_pc = 83;
                continue;
            },
            85 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_rows_12[@as(c_int, 2)];
                afz_pc = 84;
                continue;
            },
            86 => {
                afz_sum3_ptrs_11[@as(c_int, 1)] = afz_sum3_rows_12[@as(c_int, 1)];
                afz_pc = 85;
                continue;
            },
            87 => {
                afz_sum3_ptrs_11[@as(c_int, 0)] = afz_sum3_rows_12[@as(c_int, 0)];
                afz_pc = 86;
                continue;
            },
            88 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_rows_10[@as(c_int, 2)];
                afz_pc = 87;
                continue;
            },
            89 => {
                afz_sumsq3_ptrs_9[@as(c_int, 1)] = afz_sumsq3_rows_10[@as(c_int, 1)];
                afz_pc = 88;
                continue;
            },
            90 => {
                afz_sumsq3_ptrs_9[@as(c_int, 0)] = afz_sumsq3_rows_10[@as(c_int, 0)];
                afz_pc = 89;
                continue;
            },
            91 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_rows_5[@as(c_int, 3)];
                afz_pc = 90;
                continue;
            },
            92 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_rows_5[@as(c_int, 2)];
                afz_pc = 91;
                continue;
            },
            93 => {
                afz_sum5_ptrs_4[@as(c_int, 2)] = afz_sum5_rows_5[@as(c_int, 1)];
                afz_pc = 92;
                continue;
            },
            94 => {
                afz_sum5_ptrs_4[@as(c_int, 1)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 93;
                continue;
            },
            95 => {
                afz_sum5_ptrs_4[@as(c_int, 0)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 94;
                continue;
            },
            96 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_rows_3[@as(c_int, 3)];
                afz_pc = 95;
                continue;
            },
            97 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_rows_3[@as(c_int, 2)];
                afz_pc = 96;
                continue;
            },
            98 => {
                afz_sumsq5_ptrs_2[@as(c_int, 2)] = afz_sumsq5_rows_3[@as(c_int, 1)];
                afz_pc = 97;
                continue;
            },
            99 => {
                afz_sumsq5_ptrs_2[@as(c_int, 1)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 98;
                continue;
            },
            100 => {
                afz_sumsq5_ptrs_2[@as(c_int, 0)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 99;
                continue;
            },
            101 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_rows_5[@as(c_int, 4)];
                afz_pc = 63;
                continue;
            },
            102 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_rows_3[@as(c_int, 4)];
                afz_pc = 101;
                continue;
            },
            103 => {
                afz_pc = 38;
                continue;
            },
            104 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 103) else @as(c_int, 102)));
                continue;
            },
            105 => {
                sgr_finish_mix(&dst, stride, @ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), @ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), w, 2, params.*.sgr.w0, params.*.sgr.w1);
                afz_pc = 104;
                continue;
            },
            106 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 105;
                continue;
            },
            107 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 106;
                continue;
            },
            108 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 107;
                continue;
            },
            109 => {
                left += 1;
                afz_pc = 108;
                continue;
            },
            110 => {
                sgr_box35_row_h(afz_sumsq3_ptrs_9[@as(c_int, 2)], afz_sum3_ptrs_11[@as(c_int, 2)], afz_sumsq5_rows_3[@as(c_int, 3)], afz_sum5_rows_5[@as(c_int, 3)], left, afz_src_24, w, edges);
                afz_pc = 109;
                continue;
            },
            111 => {
                afz_pc = 27;
                continue;
            },
            112 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 111) else @as(c_int, 110)));
                continue;
            },
            113 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 112;
                continue;
            },
            114 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 113;
                continue;
            },
            115 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 114;
                continue;
            },
            116 => {
                left += 1;
                afz_pc = 115;
                continue;
            },
            117 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 2)], afz_sum3_rows_12[@as(c_int, 2)], afz_sumsq5_rows_3[@as(c_int, 2)], afz_sum5_rows_5[@as(c_int, 2)], left, afz_src_24, w, edges);
                afz_pc = 116;
                continue;
            },
            118 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_rows_12[@as(c_int, 2)];
                afz_pc = 117;
                continue;
            },
            119 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_rows_10[@as(c_int, 2)];
                afz_pc = 118;
                continue;
            },
            120 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_rows_5[@as(c_int, 3)];
                afz_pc = 119;
                continue;
            },
            121 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_rows_5[@as(c_int, 2)];
                afz_pc = 120;
                continue;
            },
            122 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_rows_3[@as(c_int, 3)];
                afz_pc = 121;
                continue;
            },
            123 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_rows_3[@as(c_int, 2)];
                afz_pc = 122;
                continue;
            },
            124 => {
                afz_pc = 38;
                continue;
            },
            125 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 124) else @as(c_int, 123)));
                continue;
            },
            126 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 125;
                continue;
            },
            127 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 126;
                continue;
            },
            128 => {
                rotate(@ptrCast(@alignCast(&afz_A5_ptrs_16)), @ptrCast(@alignCast(&afz_B5_ptrs_17)), 2);
                afz_pc = 127;
                continue;
            },
            129 => {
                sgr_box5_vert(@ptrCast(@alignCast(&afz_sumsq5_ptrs_2)), @ptrCast(@alignCast(&afz_sum5_ptrs_4)), afz_A5_ptrs_16[@as(c_int, 1)], afz_B5_ptrs_17[@as(c_int, 1)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s0))), 255);
                afz_pc = 128;
                continue;
            },
            130 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 129;
                continue;
            },
            131 => {
                left += 1;
                afz_pc = 130;
                continue;
            },
            132 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 1)], afz_sum3_rows_12[@as(c_int, 1)], afz_sumsq5_rows_3[@as(c_int, 1)], afz_sum5_rows_5[@as(c_int, 1)], left, afz_src_24, w, edges);
                afz_pc = 131;
                continue;
            },
            133 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_rows_12[@as(c_int, 1)];
                afz_pc = 132;
                continue;
            },
            134 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_rows_10[@as(c_int, 1)];
                afz_pc = 133;
                continue;
            },
            135 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_rows_5[@as(c_int, 1)];
                afz_pc = 134;
                continue;
            },
            136 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_rows_3[@as(c_int, 1)];
                afz_pc = 135;
                continue;
            },
            137 => {
                afz_pc = 9;
                continue;
            },
            138 => {
                afz_pc = @bitCast(@as(c_int, if ((blk: {
                    const ref = &h;
                    ref.* -= 1;
                    break :blk ref.*;
                }) <= @as(c_int, 0)) @as(c_int, 137) else @as(c_int, 136)));
                continue;
            },
            139 => {
                rotate(@ptrCast(@alignCast(&afz_A3_ptrs_21)), @ptrCast(@alignCast(&afz_B3_ptrs_22)), 4);
                afz_pc = 138;
                continue;
            },
            140 => {
                sgr_box3_vert(@ptrCast(@alignCast(&afz_sumsq3_ptrs_9)), @ptrCast(@alignCast(&afz_sum3_ptrs_11)), afz_A3_ptrs_21[@as(c_int, 3)], afz_B3_ptrs_22[@as(c_int, 3)], w, @bitCast(@as(c_uint, @truncate(params.*.sgr.s1))), 255);
                afz_pc = 139;
                continue;
            },
            141 => {
                afz_src_24 += @as(usize, @bitCast(@as(isize, @intCast(stride))));
                afz_pc = 140;
                continue;
            },
            142 => {
                left += 1;
                afz_pc = 141;
                continue;
            },
            143 => {
                sgr_box35_row_h(afz_sumsq3_rows_10[@as(c_int, 0)], afz_sum3_rows_12[@as(c_int, 0)], afz_sumsq5_rows_3[@as(c_int, 0)], afz_sum5_rows_5[@as(c_int, 0)], left, afz_src_24, w, edges);
                afz_pc = 142;
                continue;
            },
            144 => {
                afz_sum3_ptrs_11[@as(c_int, 2)] = afz_sum3_rows_12[@as(c_int, 0)];
                afz_pc = 143;
                continue;
            },
            145 => {
                afz_sum3_ptrs_11[@as(c_int, 1)] = afz_sum3_rows_12[@as(c_int, 0)];
                afz_pc = 144;
                continue;
            },
            146 => {
                afz_sum3_ptrs_11[@as(c_int, 0)] = afz_sum3_rows_12[@as(c_int, 0)];
                afz_pc = 145;
                continue;
            },
            147 => {
                afz_sumsq3_ptrs_9[@as(c_int, 2)] = afz_sumsq3_rows_10[@as(c_int, 0)];
                afz_pc = 146;
                continue;
            },
            148 => {
                afz_sumsq3_ptrs_9[@as(c_int, 1)] = afz_sumsq3_rows_10[@as(c_int, 0)];
                afz_pc = 147;
                continue;
            },
            149 => {
                afz_sumsq3_ptrs_9[@as(c_int, 0)] = afz_sumsq3_rows_10[@as(c_int, 0)];
                afz_pc = 148;
                continue;
            },
            150 => {
                afz_sum5_ptrs_4[@as(c_int, 4)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 149;
                continue;
            },
            151 => {
                afz_sum5_ptrs_4[@as(c_int, 3)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 150;
                continue;
            },
            152 => {
                afz_sum5_ptrs_4[@as(c_int, 2)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 151;
                continue;
            },
            153 => {
                afz_sum5_ptrs_4[@as(c_int, 1)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 152;
                continue;
            },
            154 => {
                afz_sum5_ptrs_4[@as(c_int, 0)] = afz_sum5_rows_5[@as(c_int, 0)];
                afz_pc = 153;
                continue;
            },
            155 => {
                afz_sumsq5_ptrs_2[@as(c_int, 4)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 154;
                continue;
            },
            156 => {
                afz_sumsq5_ptrs_2[@as(c_int, 3)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 155;
                continue;
            },
            157 => {
                afz_sumsq5_ptrs_2[@as(c_int, 2)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 156;
                continue;
            },
            158 => {
                afz_sumsq5_ptrs_2[@as(c_int, 1)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 157;
                continue;
            },
            159 => {
                afz_sumsq5_ptrs_2[@as(c_int, 0)] = afz_sumsq5_rows_3[@as(c_int, 0)];
                afz_pc = 158;
                continue;
            },
            160 => {
                afz_pc = @bitCast(@as(c_int, if ((edges & @as(enum_LrEdgeFlags, LR_HAVE_TOP)) != 0) @as(c_int, 100) else @as(c_int, 159)));
                continue;
            },
            161 => {
                afz_lpf_bottom_25 = @ptrCast(@alignCast(@constCast(lpf + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, 6) * stride)))))));
                afz_pc = 160;
                continue;
            },
            162 => {
                afz_src_24 = dst;
                afz_pc = 161;
                continue;
            },
            163 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_23 < @as(c_int, 4)) @as(c_int, 166) else @as(c_int, 162)));
                continue;
            },
            164 => {
                afz_i_23 += 1;
                afz_pc = 163;
                continue;
            },
            165 => {
                afz_B3_ptrs_22[@bitCast(@as(isize, @intCast(afz_i_23)))] = &afz_B3_buf_20[@bitCast(@as(isize, @intCast(afz_i_23 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 164;
                continue;
            },
            166 => {
                afz_A3_ptrs_21[@bitCast(@as(isize, @intCast(afz_i_23)))] = &afz_A3_buf_19[@bitCast(@as(isize, @intCast(afz_i_23 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 165;
                continue;
            },
            167 => {
                afz_i_23 = 0;
                afz_pc = 163;
                continue;
            },
            168 => {
                afz_pc = 167;
                continue;
            },
            169 => {
                afz_pc = 168;
                continue;
            },
            170 => {
                afz_pc = 169;
                continue;
            },
            171 => {
                afz_pc = 170;
                continue;
            },
            172 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_18 < @as(c_int, 2)) @as(c_int, 175) else @as(c_int, 171)));
                continue;
            },
            173 => {
                afz_i_18 += 1;
                afz_pc = 172;
                continue;
            },
            174 => {
                afz_B5_ptrs_17[@bitCast(@as(isize, @intCast(afz_i_18)))] = &afz_B5_buf_15[@bitCast(@as(isize, @intCast(afz_i_18 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 173;
                continue;
            },
            175 => {
                afz_A5_ptrs_16[@bitCast(@as(isize, @intCast(afz_i_18)))] = &afz_A5_buf_14[@bitCast(@as(isize, @intCast(afz_i_18 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 174;
                continue;
            },
            176 => {
                afz_i_18 = 0;
                afz_pc = 172;
                continue;
            },
            177 => {
                afz_pc = 176;
                continue;
            },
            178 => {
                afz_pc = 177;
                continue;
            },
            179 => {
                afz_pc = 178;
                continue;
            },
            180 => {
                afz_pc = 179;
                continue;
            },
            181 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_13 < @as(c_int, 3)) @as(c_int, 184) else @as(c_int, 180)));
                continue;
            },
            182 => {
                afz_i_13 += 1;
                afz_pc = 181;
                continue;
            },
            183 => {
                afz_sum3_rows_12[@bitCast(@as(isize, @intCast(afz_i_13)))] = &afz_sum3_buf_8[@bitCast(@as(isize, @intCast(afz_i_13 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 182;
                continue;
            },
            184 => {
                afz_sumsq3_rows_10[@bitCast(@as(isize, @intCast(afz_i_13)))] = &afz_sumsq3_buf_7[@bitCast(@as(isize, @intCast(afz_i_13 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 183;
                continue;
            },
            185 => {
                afz_i_13 = 0;
                afz_pc = 181;
                continue;
            },
            186 => {
                afz_pc = 185;
                continue;
            },
            187 => {
                afz_pc = 186;
                continue;
            },
            188 => {
                afz_pc = 187;
                continue;
            },
            189 => {
                afz_pc = 188;
                continue;
            },
            190 => {
                afz_pc = @bitCast(@as(c_int, if (afz_i_6 < @as(c_int, 5)) @as(c_int, 193) else @as(c_int, 189)));
                continue;
            },
            191 => {
                afz_i_6 += 1;
                afz_pc = 190;
                continue;
            },
            192 => {
                afz_sum5_rows_5[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sum5_buf_1[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 191;
                continue;
            },
            193 => {
                afz_sumsq5_rows_3[@bitCast(@as(isize, @intCast(afz_i_6)))] = &afz_sumsq5_buf_0[@bitCast(@as(isize, @intCast(afz_i_6 * (@as(c_int, 384) + @as(c_int, 16)))))];
                afz_pc = 192;
                continue;
            },
            194 => {
                afz_i_6 = 0;
                afz_pc = 190;
                continue;
            },
            195 => {
                afz_pc = 194;
                continue;
            },
            196 => {
                afz_pc = 195;
                continue;
            },
            197 => {
                afz_pc = 196;
                continue;
            },
            198 => {
                afz_pc = 197;
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
}
pub export fn antfly_av1_dav1d_loop_restoration_dsp_init_8bpc(c: [*c]Dav1dLoopRestorationDSPContext, bpc: c_int) void {
    _ = &c;
    _ = &bpc;
    c.*.wiener[@as(c_int, 0)] = blk: {
        const tmp = wiener_c;
        c.*.wiener[@as(c_int, 1)] = tmp;
        break :blk tmp;
    };
    c.*.sgr[@as(c_int, 0)] = sgr_5x5_c;
    c.*.sgr[@as(c_int, 1)] = sgr_3x3_c;
    c.*.sgr[@as(c_int, 2)] = sgr_mix_c;
}
