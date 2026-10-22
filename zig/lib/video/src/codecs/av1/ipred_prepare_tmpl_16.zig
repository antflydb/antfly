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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const ptrdiff_t = common.ptrdiff_t;
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub const pixel = u16;
pub fn pixel_set(dst: [*c]pixel, val: c_int, num: c_int) callconv(.c) void {
    _ = &dst;
    _ = &val;
    _ = &num;
    {
        var n: c_int = 0;
        _ = &n;
        while (n < num) : (n += 1) {
            dst[@bitCast(@as(isize, @intCast(n)))] = @bitCast(@as(c_short, @truncate(val)));
        }
    }
}
pub fn PXSTRIDE(x: ptrdiff_t) callconv(.c) ptrdiff_t {
    _ = &x;
    while (true) {
        if (!!((x & @as(ptrdiff_t, 1)) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    return x >> @intCast(@as(ptrdiff_t, 1));
}
pub const DC_PRED = common.DC_PRED;
pub const VERT_PRED = common.VERT_PRED;
pub const HOR_PRED = common.HOR_PRED;
pub const DIAG_DOWN_LEFT_PRED = common.DIAG_DOWN_LEFT_PRED;
pub const DIAG_DOWN_RIGHT_PRED = common.DIAG_DOWN_RIGHT_PRED;
pub const VERT_RIGHT_PRED = common.VERT_RIGHT_PRED;
pub const HOR_DOWN_PRED = common.HOR_DOWN_PRED;
pub const HOR_UP_PRED = common.HOR_UP_PRED;
pub const VERT_LEFT_PRED = common.VERT_LEFT_PRED;
pub const PAETH_PRED = common.PAETH_PRED;
pub const LEFT_DC_PRED = common.LEFT_DC_PRED;
pub const TOP_DC_PRED = common.TOP_DC_PRED;
pub const DC_128_PRED = common.DC_128_PRED;
pub const Z1_PRED = common.Z1_PRED;
pub const Z2_PRED = common.Z2_PRED;
pub const Z3_PRED = common.Z3_PRED;
pub const enum_IntraPredMode = common.enum_IntraPredMode;
pub const EDGE_I444_TOP_HAS_RIGHT = common.EDGE_I444_TOP_HAS_RIGHT;
pub const EDGE_I444_LEFT_HAS_BOTTOM = common.EDGE_I444_LEFT_HAS_BOTTOM;
pub const enum_EdgeFlags = common.enum_EdgeFlags;
pub var av1_mode_conv: [13][2][2]u8 = [1][2][2]u8{
    [2][2]u8{
        [2]u8{
            DC_128_PRED,
            TOP_DC_PRED,
        },
        [2]u8{
            LEFT_DC_PRED,
            DC_PRED,
        },
    },
} ++ @as([11][2][2]u8, @splat(@import("std").mem.zeroes([2][2]u8))) ++ [1][2][2]u8{
    [2][2]u8{
        [2]u8{
            DC_128_PRED,
            VERT_PRED,
        },
        [2]u8{
            HOR_PRED,
            PAETH_PRED,
        },
    },
};
pub const av1_mode_to_angle_map: [8]u8 = [8]u8{
    90,
    180,
    45,
    135,
    113,
    157,
    203,
    67,
};
const struct_unnamed_27 = extern struct {
    needs_left: u8 = 0,
    needs_top: u8 = 0,
    needs_topleft: u8 = 0,
    needs_topright: u8 = 0,
    needs_bottomleft: u8 = 0,
};
pub const av1_intra_prediction_edges: [14]struct_unnamed_27 = [14]struct_unnamed_27{
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 0,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 0,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 0,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 0,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 0,
        .needs_top = 0,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 0,
        .needs_top = 1,
        .needs_topleft = 1,
        .needs_topright = 1,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 1,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 0,
        .needs_topleft = 1,
        .needs_topright = 0,
        .needs_bottomleft = 1,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 0,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 1,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
    struct_unnamed_27{
        .needs_left = 1,
        .needs_top = 1,
        .needs_topleft = 1,
        .needs_topright = 0,
        .needs_bottomleft = 0,
    },
};
pub export fn antfly_av1_dav1d_prepare_intra_edges_16bpc(x: c_int, have_left: c_int, y: c_int, have_top: c_int, w: c_int, h: c_int, edge_flags: enum_EdgeFlags, dst: [*c]const pixel, stride: ptrdiff_t, arg_prefilter_toplevel_sb_edge: [*c]const pixel, arg_mode: enum_IntraPredMode, angle: [*c]c_int, tw: c_int, th: c_int, filter_edge: c_int, topleft_out: [*c]pixel, bitdepth_max: c_int) enum_IntraPredMode {
    _ = &x;
    _ = &have_left;
    _ = &y;
    _ = &have_top;
    _ = &w;
    _ = &h;
    _ = &edge_flags;
    _ = &dst;
    _ = &stride;
    var prefilter_toplevel_sb_edge = arg_prefilter_toplevel_sb_edge;
    _ = &prefilter_toplevel_sb_edge;
    var mode = arg_mode;
    _ = &mode;
    _ = &angle;
    _ = &tw;
    _ = &th;
    _ = &filter_edge;
    _ = &topleft_out;
    _ = &bitdepth_max;
    const bitdepth: c_int = @as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)));
    _ = &bitdepth;
    while (true) {
        if (!((y < h) and (x < w))) {
            unreachable;
        }
        if (!false) break;
    }
    blk: {
        switch (mode) {
            @as(enum_IntraPredMode, VERT_PRED), @as(enum_IntraPredMode, HOR_PRED), @as(enum_IntraPredMode, DIAG_DOWN_LEFT_PRED), @as(enum_IntraPredMode, DIAG_DOWN_RIGHT_PRED), @as(enum_IntraPredMode, VERT_RIGHT_PRED), @as(enum_IntraPredMode, HOR_DOWN_PRED), @as(enum_IntraPredMode, HOR_UP_PRED), @as(enum_IntraPredMode, VERT_LEFT_PRED) => {
                {
                    angle.* = @as(c_int, av1_mode_to_angle_map[mode -% @as(enum_IntraPredMode, VERT_PRED)]) + (@as(c_int, 3) * angle.*);
                    if (angle.* <= @as(c_int, 90)) {
                        mode = @bitCast(if ((angle.* < @as(c_int, 90)) and (have_top != 0)) Z1_PRED else VERT_PRED);
                    } else if (angle.* < @as(c_int, 180)) {
                        mode = Z2_PRED;
                    } else {
                        mode = @bitCast(if ((angle.* > @as(c_int, 180)) and (have_left != 0)) Z3_PRED else HOR_PRED);
                    }
                    break :blk;
                }
            },
            @as(enum_IntraPredMode, DC_PRED), @as(enum_IntraPredMode, PAETH_PRED) => {
                mode = av1_mode_conv[mode][@bitCast(@as(isize, @intCast(have_left)))][@bitCast(@as(isize, @intCast(have_top)))];
                break :blk;
            },
            else => {
                break :blk;
            },
        }
    }
    var dst_top: [*c]const pixel = undefined;
    _ = &dst_top;
    if ((have_top != 0) and (((@as(c_int, av1_intra_prediction_edges[mode].needs_top) != 0) or (@as(c_int, av1_intra_prediction_edges[mode].needs_topleft) != 0)) or ((@as(c_int, av1_intra_prediction_edges[mode].needs_left) != 0) and !(have_left != 0)))) {
        if (prefilter_toplevel_sb_edge != null) {
            dst_top = &prefilter_toplevel_sb_edge[@bitCast(@as(isize, @intCast(x * @as(c_int, 4))))];
        } else {
            dst_top = &dst[@bitCast(@as(isize, @intCast(-PXSTRIDE(stride))))];
        }
    }
    if (@as(c_int, av1_intra_prediction_edges[mode].needs_left) != 0) {
        const sz: c_int = th << @intCast(@as(c_int, 2));
        _ = &sz;
        const left: [*c]pixel = &topleft_out[@bitCast(@as(isize, @intCast(-sz)))];
        _ = &left;
        if (have_left != 0) {
            const px_have: c_int = imin(sz, (h - y) << @intCast(@as(c_int, 2)));
            _ = &px_have;
            {
                var i: c_int = 0;
                _ = &i;
                while (i < px_have) : (i += 1) {
                    left[@bitCast(@as(isize, @intCast((sz - @as(c_int, 1)) - i)))] = dst[@bitCast(@as(isize, @intCast((PXSTRIDE(stride) * @as(ptrdiff_t, i)) - @as(ptrdiff_t, 1))))];
                }
            }
            if (px_have < sz) {
                pixel_set(left, left[@bitCast(@as(isize, @intCast(sz - px_have)))], sz - px_have);
            }
        } else {
            pixel_set(left, if (have_top != 0) @as(c_int, dst_top.*) else ((@as(c_int, 1) << @intCast(bitdepth)) >> @intCast(@as(c_int, 1))) + @as(c_int, 1), sz);
        }
        if (@as(c_int, av1_intra_prediction_edges[mode].needs_bottomleft) != 0) {
            const have_bottomleft: c_int = if (!(have_left != 0) or ((y + th) >= h)) @as(c_int, 0) else @as(c_int, @bitCast(edge_flags & @as(enum_EdgeFlags, EDGE_I444_LEFT_HAS_BOTTOM)));
            _ = &have_bottomleft;
            if (have_bottomleft != 0) {
                const px_have: c_int = imin(sz, ((h - y) - th) << @intCast(@as(c_int, 2)));
                _ = &px_have;
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < px_have) : (i += 1) {
                        left[@bitCast(@as(isize, @intCast(-(i + @as(c_int, 1)))))] = dst[@bitCast(@as(isize, @intCast((@as(ptrdiff_t, sz + i) * PXSTRIDE(stride)) - @as(ptrdiff_t, 1))))];
                    }
                }
                if (px_have < sz) {
                    pixel_set(left - @as(usize, @bitCast(@as(isize, @intCast(sz)))), left[@bitCast(@as(isize, @intCast(-px_have)))], sz - px_have);
                }
            } else {
                pixel_set(left - @as(usize, @bitCast(@as(isize, @intCast(sz)))), left[@as(c_int, 0)], sz);
            }
        }
    }
    if (@as(c_int, av1_intra_prediction_edges[mode].needs_top) != 0) {
        const sz: c_int = tw << @intCast(@as(c_int, 2));
        _ = &sz;
        const top: [*c]pixel = &topleft_out[@as(c_int, 1)];
        _ = &top;
        if (have_top != 0) {
            const px_have: c_int = imin(sz, (w - x) << @intCast(@as(c_int, 2)));
            _ = &px_have;
            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(top)), @ptrCast(@alignCast(dst_top)), @bitCast(@as(c_long, px_have << @intCast(@as(c_int, 1)))));
            if (px_have < sz) {
                pixel_set(top + @as(usize, @bitCast(@as(isize, @intCast(px_have)))), top[@bitCast(@as(isize, @intCast(px_have - @as(c_int, 1))))], sz - px_have);
            }
        } else {
            pixel_set(top, if (have_left != 0) @as(c_int, dst[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else ((@as(c_int, 1) << @intCast(bitdepth)) >> @intCast(@as(c_int, 1))) - @as(c_int, 1), sz);
        }
        if (@as(c_int, av1_intra_prediction_edges[mode].needs_topright) != 0) {
            const have_topright: c_int = if (!(have_top != 0) or ((x + tw) >= w)) @as(c_int, 0) else @as(c_int, @bitCast(edge_flags & @as(enum_EdgeFlags, EDGE_I444_TOP_HAS_RIGHT)));
            _ = &have_topright;
            if (have_topright != 0) {
                const px_have: c_int = imin(sz, ((w - x) - tw) << @intCast(@as(c_int, 2)));
                _ = &px_have;
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(top + @as(usize, @bitCast(@as(isize, @intCast(sz)))))), @ptrCast(@alignCast(&dst_top[@bitCast(@as(isize, @intCast(sz)))])), @bitCast(@as(c_long, px_have << @intCast(@as(c_int, 1)))));
                if (px_have < sz) {
                    pixel_set((top + @as(usize, @bitCast(@as(isize, @intCast(sz))))) + @as(usize, @bitCast(@as(isize, @intCast(px_have)))), top[@bitCast(@as(isize, @intCast((sz + px_have) - @as(c_int, 1))))], sz - px_have);
                }
            } else {
                pixel_set(top + @as(usize, @bitCast(@as(isize, @intCast(sz)))), top[@bitCast(@as(isize, @intCast(sz - @as(c_int, 1))))], sz);
            }
        }
    }
    if (@as(c_int, av1_intra_prediction_edges[mode].needs_topleft) != 0) {
        if (have_left != 0) {
            topleft_out.* = @bitCast(@as(c_short, @truncate(if (have_top != 0) @as(c_int, dst_top[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else @as(c_int, dst[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]))));
        } else {
            topleft_out.* = @bitCast(@as(c_short, @truncate(if (have_top != 0) @as(c_int, dst_top.*) else (@as(c_int, 1) << @intCast(bitdepth)) >> @intCast(@as(c_int, 1)))));
        }
        if (((mode == @as(enum_IntraPredMode, Z2_PRED)) and ((tw + th) >= @as(c_int, 6))) and (filter_edge != 0)) {
            topleft_out.* = @bitCast(@as(c_short, @truncate(((((@as(c_int, topleft_out[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) + @as(c_int, topleft_out[@as(c_int, 1)])) * @as(c_int, 5)) + (@as(c_int, topleft_out[@as(c_int, 0)]) * @as(c_int, 6))) + @as(c_int, 8)) >> @intCast(@as(c_int, 4)))));
        }
    }
    return mode;
}
