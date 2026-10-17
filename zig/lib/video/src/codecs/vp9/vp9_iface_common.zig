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
//  *  Copyright (c) 2019 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed  by a BSD-style license that can be
//  *  found in the LICENSE file in the root of the source tree. An additional
//  *  intellectual property  rights grant can  be found in the  file PATENTS.
//  *  All contributing  project authors may be  found in the AUTHORS  file in
//  *  the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const VPX_IMG_FMT_I420 = common.VPX_IMG_FMT_I420;
pub const VPX_IMG_FMT_I422 = common.VPX_IMG_FMT_I422;
pub const VPX_IMG_FMT_I444 = common.VPX_IMG_FMT_I444;
pub const VPX_IMG_FMT_I440 = common.VPX_IMG_FMT_I440;
pub const vpx_img_fmt_t = common.vpx_img_fmt_t;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const struct_vpx_image = extern struct {
    fmt: vpx_img_fmt_t = @import("std").mem.zeroes(vpx_img_fmt_t),
    cs: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    w: c_uint = 0,
    h: c_uint = 0,
    bit_depth: c_uint = 0,
    d_w: c_uint = 0,
    d_h: c_uint = 0,
    r_w: c_uint = 0,
    r_h: c_uint = 0,
    x_chroma_shift: c_uint = 0,
    y_chroma_shift: c_uint = 0,
    planes: [4][*c]u8 = @import("std").mem.zeroes([4][*c]u8),
    stride: [4]c_int = @import("std").mem.zeroes([4]c_int),
    bps: c_int = 0,
    user_priv: ?*anyopaque = null,
    img_data: [*c]u8 = null,
    img_data_owner: c_int = 0,
    self_allocd: c_int = 0,
    fb_priv: ?*anyopaque = null,
};
pub const vpx_image_t = struct_vpx_image;
pub const struct_yv12_buffer_config = extern struct {
    y_width: c_int = 0,
    y_height: c_int = 0,
    y_crop_width: c_int = 0,
    y_crop_height: c_int = 0,
    y_stride: c_int = 0,
    uv_width: c_int = 0,
    uv_height: c_int = 0,
    uv_crop_width: c_int = 0,
    uv_crop_height: c_int = 0,
    uv_stride: c_int = 0,
    alpha_width: c_int = 0,
    alpha_height: c_int = 0,
    alpha_stride: c_int = 0,
    y_buffer: [*c]u8 = null,
    u_buffer: [*c]u8 = null,
    v_buffer: [*c]u8 = null,
    alpha_buffer: [*c]u8 = null,
    buffer_alloc: [*c]u8 = null,
    buffer_alloc_sz: usize = 0,
    border: c_int = 0,
    frame_size: usize = 0,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    bit_depth: c_uint = 0,
    color_space: vpx_color_space_t = @import("std").mem.zeroes(vpx_color_space_t),
    color_range: vpx_color_range_t = @import("std").mem.zeroes(vpx_color_range_t),
    render_width: c_int = 0,
    render_height: c_int = 0,
    corrupted: c_int = 0,
    flags: c_int = 0,
};
pub const YV12_BUFFER_CONFIG = struct_yv12_buffer_config;
pub export fn antfly_vp9_yuvconfig2image(arg_img: [*c]vpx_image_t, arg_yv12: [*c]const YV12_BUFFER_CONFIG, arg_user_priv: ?*anyopaque) void {
    var img = arg_img;
    _ = &img;
    var yv12 = arg_yv12;
    _ = &yv12;
    var user_priv = arg_user_priv;
    _ = &user_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var bps: c_int = undefined;
    _ = &bps;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (!(yv12.*.subsampling_y != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (!(yv12.*.subsampling_x != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            img.*.fmt = VPX_IMG_FMT_I444;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            bps = 24;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            img.*.fmt = VPX_IMG_FMT_I422;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            bps = 16;
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
        if (!(yv12.*.subsampling_x != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            img.*.fmt = VPX_IMG_FMT_I440;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            bps = 16;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            img.*.fmt = VPX_IMG_FMT_I420;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            bps = 12;
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
    img.*.cs = yv12.*.color_space;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.range = yv12.*.color_range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.bit_depth = 8;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.w = @bitCast(@as(c_int, yv12.*.y_stride));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.h = @bitCast(@as(c_int, ((yv12.*.y_height + (@as(c_int, 2) * @as(c_int, 160))) + ((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))) & ~((@as(c_int, 1) << @intCast(@as(c_int, 3))) - @as(c_int, 1))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.d_w = @bitCast(@as(c_int, yv12.*.y_crop_width));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.d_h = @bitCast(@as(c_int, yv12.*.y_crop_height));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.r_w = @bitCast(@as(c_int, yv12.*.render_width));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.r_h = @bitCast(@as(c_int, yv12.*.render_height));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.x_chroma_shift = @bitCast(@as(c_int, yv12.*.subsampling_x));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.y_chroma_shift = @bitCast(@as(c_int, yv12.*.subsampling_y));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.planes[@as(c_int, 0)] = yv12.*.y_buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.planes[@as(c_int, 1)] = yv12.*.u_buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.planes[@as(c_int, 2)] = yv12.*.v_buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.planes[@as(c_int, 3)] = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.stride[@as(c_int, 0)] = yv12.*.y_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.stride[@as(c_int, 1)] = yv12.*.uv_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.stride[@as(c_int, 2)] = yv12.*.uv_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.stride[@as(c_int, 3)] = yv12.*.y_stride;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((yv12.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.fmt = img.*.fmt | @as(vpx_img_fmt_t, 2048);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.bit_depth = yv12.*.bit_depth;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.planes[@as(c_int, 0)] = @ptrCast(@alignCast(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(yv12.*.y_buffer))) << @intCast(@as(usize, 1))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.planes[@as(c_int, 1)] = @ptrCast(@alignCast(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(yv12.*.u_buffer))) << @intCast(@as(usize, 1))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.planes[@as(c_int, 2)] = @ptrCast(@alignCast(@as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(yv12.*.v_buffer))) << @intCast(@as(usize, 1))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.planes[@as(c_int, 3)] = null;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.stride[@as(c_int, 0)] = @as(c_int, 2) * yv12.*.y_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.stride[@as(c_int, 1)] = @as(c_int, 2) * yv12.*.uv_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.stride[@as(c_int, 2)] = @as(c_int, 2) * yv12.*.uv_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        img.*.stride[@as(c_int, 3)] = @as(c_int, 2) * yv12.*.y_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.bps = bps;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.user_priv = user_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.img_data = yv12.*.buffer_alloc;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.img_data_owner = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    img.*.self_allocd = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
