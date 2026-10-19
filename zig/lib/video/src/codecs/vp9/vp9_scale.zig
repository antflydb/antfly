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
pub const ptrdiff_t = common.ptrdiff_t;
pub const InterpKernel = common.InterpKernel;
pub extern fn antfly_vp9_vpx_convolve8_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve8_avg_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve8_avg_horiz_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve8_avg_vert_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve8_horiz_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve8_vert_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve_avg_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_convolve_copy_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_avg_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_avg_horiz_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_avg_vert_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_horiz_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve8_vert_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve_avg_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_convolve_copy_c(src: [*c]const u16, src_stride: ptrdiff_t, dst: [*c]u16, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int, bd: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_2d_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_avg_2d_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_avg_horiz_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_avg_vert_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_horiz_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub extern fn antfly_vp9_vpx_scaled_vert_c(src: [*c]const u8, src_stride: ptrdiff_t, dst: [*c]u8, dst_stride: ptrdiff_t, filter: [*c]const InterpKernel, x0_q4: c_int, x_step_q4: c_int, y0_q4: c_int, y_step_q4: c_int, w: c_int, h: c_int) void;
pub const struct_mv = extern struct {
    row: i16 = 0,
    col: i16 = 0,
};
pub const MV = struct_mv;
pub const MV32 = common.MV32;
pub const struct_scale_factors = common.struct_scale_factors;
pub fn vp9_is_valid_scale(arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((sf.*.x_scale_fp != -@as(c_int, 1)) and (sf.*.y_scale_fp != -@as(c_int, 1))));
}
pub fn vp9_is_scaled(arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool((vp9_is_valid_scale(sf) != 0) and ((sf.*.x_scale_fp != (@as(c_int, 1) << @intCast(@as(c_int, 14)))) or (sf.*.y_scale_fp != (@as(c_int, 1) << @intCast(@as(c_int, 14)))))));
}
pub fn valid_ref_frame_size(arg_ref_width: c_int, arg_ref_height: c_int, arg_this_width: c_int, arg_this_height: c_int) callconv(.c) c_int {
    var ref_width = arg_ref_width;
    _ = &ref_width;
    var ref_height = arg_ref_height;
    _ = &ref_height;
    var this_width = arg_this_width;
    _ = &this_width;
    var this_height = arg_this_height;
    _ = &this_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(((((@as(c_int, 2) * this_width) >= ref_width) and ((@as(c_int, 2) * this_height) >= ref_height)) and (this_width <= (@as(c_int, 16) * ref_width))) and (this_height <= (@as(c_int, 16) * ref_height))));
}
pub fn scaled_x(arg_val: c_int, arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var val = arg_val;
    _ = &val;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate((@as(i64, val) * @as(i64, sf.*.x_scale_fp)) >> @intCast(@as(i64, 14)));
}
pub fn scaled_y(arg_val: c_int, arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var val = arg_val;
    _ = &val;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate((@as(i64, val) * @as(i64, sf.*.y_scale_fp)) >> @intCast(@as(i64, 14)));
}
pub fn unscaled_value(arg_val: c_int, arg_sf: [*c]const struct_scale_factors) callconv(.c) c_int {
    var val = arg_val;
    _ = &val;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return val;
}
pub fn get_fixed_point_scale_factor(arg_other_size: c_int, arg_this_size: c_int) callconv(.c) c_int {
    var other_size = arg_other_size;
    _ = &other_size;
    var this_size = arg_this_size;
    _ = &this_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @divTrunc(other_size << @intCast(@as(c_int, 14)), this_size);
}
pub export fn antfly_vp9_vp9_scale_mv(arg_mv_1: [*c]const MV, arg_x: c_int, arg_y: c_int, arg_sf: [*c]const struct_scale_factors) MV32 {
    var mv_1 = arg_mv_1;
    _ = &mv_1;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var sf = arg_sf;
    _ = &sf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV32{
            .row = 0,
            .col = 0,
        };
    }
    const x_off_q4: c_int = scaled_x(x << @intCast(@as(c_int, 4)), sf) & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1));
    _ = &x_off_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV32{
            .row = 0,
            .col = 0,
        };
    }
    const y_off_q4: c_int = scaled_y(y << @intCast(@as(c_int, 4)), sf) & ((@as(c_int, 1) << @intCast(@as(c_int, 4))) - @as(c_int, 1));
    _ = &y_off_q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV32{
            .row = 0,
            .col = 0,
        };
    }
    const res: MV32 = MV32{
        .row = scaled_y(mv_1.*.row, sf) + y_off_q4,
        .col = scaled_x(mv_1.*.col, sf) + x_off_q4,
    };
    _ = &res;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return MV32{
            .row = 0,
            .col = 0,
        };
    }
    return res;
}
pub export fn antfly_vp9_vp9_setup_scale_factors_for_frame(arg_sf: [*c]struct_scale_factors, arg_other_w: c_int, arg_other_h: c_int, arg_this_w: c_int, arg_this_h: c_int, arg_use_highbd: c_int) void {
    var sf = arg_sf;
    _ = &sf;
    var other_w = arg_other_w;
    _ = &other_w;
    var other_h = arg_other_h;
    _ = &other_h;
    var this_w = arg_this_w;
    _ = &this_w;
    var this_h = arg_this_h;
    _ = &this_h;
    var use_highbd = arg_use_highbd;
    _ = &use_highbd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(valid_ref_frame_size(other_w, other_h, this_w, this_h) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.x_scale_fp = -@as(c_int, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.y_scale_fp = -@as(c_int, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.x_scale_fp = get_fixed_point_scale_factor(other_w, this_w);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.y_scale_fp = get_fixed_point_scale_factor(other_h, this_h);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.x_step_q4 = scaled_x(16, sf);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    sf.*.y_step_q4 = scaled_y(16, sf);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (vp9_is_scaled(sf) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.scale_value_x = scaled_x;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.scale_value_y = scaled_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.scale_value_x = unscaled_value;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.scale_value_y = unscaled_value;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (sf.*.x_step_q4 == @as(c_int, 16)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (sf.*.y_step_q4 == @as(c_int, 16)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_convolve_copy_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_convolve_avg_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_convolve8_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_convolve8_avg_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_convolve8_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_convolve8_avg_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_vert_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (sf.*.y_step_q4 == @as(c_int, 16)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_horiz_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            sf.*.predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
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
    if ((sf.*.x_step_q4 != @as(c_int, 16)) or (sf.*.y_step_q4 != @as(c_int, 16))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_scaled_2d_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_scaled_avg_2d_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_convolve8_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_convolve8_avg_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (use_highbd != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (sf.*.x_step_q4 == @as(c_int, 16)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (sf.*.y_step_q4 == @as(c_int, 16)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve_copy_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve_avg_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_vert_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (sf.*.y_step_q4 == @as(c_int, 16)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_horiz_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 0)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 0)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
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
        sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 0)] = antfly_vp9_vpx_highbd_convolve8_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        sf.*.highbd_predict[@as(c_int, 1)][@as(c_int, 1)][@as(c_int, 1)] = antfly_vp9_vpx_highbd_convolve8_avg_c;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
