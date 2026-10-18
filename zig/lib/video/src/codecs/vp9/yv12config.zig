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
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const vpx_get_frame_buffer_cb_fn_t = common.vpx_get_frame_buffer_cb_fn_t;
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
pub extern fn antfly_vp9_vpx_memalign(@"align": usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub export fn antfly_vp9_vpx_free_frame_buffer(arg_ybf: [*c]YV12_BUFFER_CONFIG) c_int {
    var ybf = arg_ybf;
    _ = &ybf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (ybf != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (ybf.*.buffer_alloc_sz > @as(usize, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            antfly_vp9_vpx_free(@ptrCast(@alignCast(ybf.*.buffer_alloc)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(ybf)), 0, @sizeOf(YV12_BUFFER_CONFIG));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return -@as(c_int, 1);
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 0;
}
pub export fn antfly_vp9_vpx_realloc_frame_buffer(arg_ybf: [*c]YV12_BUFFER_CONFIG, arg_width: c_int, arg_height: c_int, arg_ss_x: c_int, arg_ss_y: c_int, arg_use_highbitdepth: c_int, arg_border: c_int, arg_byte_alignment: c_int, arg_fb: [*c]vpx_codec_frame_buffer_t, arg_cb: vpx_get_frame_buffer_cb_fn_t, arg_cb_priv: ?*anyopaque) c_int {
    var ybf = arg_ybf;
    _ = &ybf;
    var width = arg_width;
    _ = &width;
    var height = arg_height;
    _ = &height;
    var ss_x = arg_ss_x;
    _ = &ss_x;
    var ss_y = arg_ss_y;
    _ = &ss_y;
    var use_highbitdepth = arg_use_highbitdepth;
    _ = &use_highbitdepth;
    var border = arg_border;
    _ = &border;
    var byte_alignment = arg_byte_alignment;
    _ = &byte_alignment;
    var fb = arg_fb;
    _ = &fb;
    var cb = arg_cb;
    _ = &cb;
    var cb_priv = arg_cb_priv;
    _ = &cb_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((border & @as(c_int, 31)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return -@as(c_int, 3);
    }
    if (ybf != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const vp9_byte_align: c_int = if (byte_alignment == @as(c_int, 0)) @as(c_int, 1) else byte_alignment;
        _ = &vp9_byte_align;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const aligned_width: c_int = (width + @as(c_int, 7)) & ~@as(c_int, 7);
        _ = &aligned_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const aligned_height: c_int = (height + @as(c_int, 7)) & ~@as(c_int, 7);
        _ = &aligned_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const y_stride: c_int = ((aligned_width + (@as(c_int, 2) * border)) + @as(c_int, 31)) & ~@as(c_int, 31);
        _ = &y_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const yplane_size: u64 = (@as(u64, @bitCast(@as(c_longlong, aligned_height + (@as(c_int, 2) * border)))) *% @as(u64, @bitCast(@as(c_longlong, y_stride)))) +% @as(u64, @bitCast(@as(c_longlong, byte_alignment)));
        _ = &yplane_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uv_width: c_int = aligned_width >> @intCast(ss_x);
        _ = &uv_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uv_height: c_int = aligned_height >> @intCast(ss_y);
        _ = &uv_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uv_stride: c_int = y_stride >> @intCast(ss_x);
        _ = &uv_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uv_border_w: c_int = border >> @intCast(ss_x);
        _ = &uv_border_w;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uv_border_h: c_int = border >> @intCast(ss_y);
        _ = &uv_border_h;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const uvplane_size: u64 = (@as(u64, @bitCast(@as(c_longlong, uv_height + (@as(c_int, 2) * uv_border_h)))) *% @as(u64, @bitCast(@as(c_longlong, uv_stride)))) +% @as(u64, @bitCast(@as(c_longlong, byte_alignment)));
        _ = &uvplane_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const frame_size: u64 = @as(u64, @bitCast(@as(c_longlong, @as(c_int, 1) + use_highbitdepth))) *% (yplane_size +% (@as(u64, 2) *% uvplane_size));
        _ = &frame_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        var buf: [*c]u8 = null;
        _ = &buf;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (frame_size > @as(u64, 18446744073709551615)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return -@as(c_int, 1);
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(cb)))) != @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const align_addr_extra_size: c_int = 31;
            _ = &align_addr_extra_size;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            const external_frame_size: u64 = frame_size +% @as(u64, @bitCast(@as(c_longlong, align_addr_extra_size)));
            _ = &external_frame_size;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            _ = @as(c_int, 0);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (external_frame_size != @as(u64, @as(usize, @truncate(external_frame_size)))) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return -@as(c_int, 1);
            }
            if (cb.?(cb_priv, @truncate(external_frame_size), fb) < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return -@as(c_int, 1);
            }
            if ((@as(?*anyopaque, @ptrCast(@alignCast(fb.*.data))) == @as(?*anyopaque, null)) or (@as(u64, fb.*.size) < external_frame_size)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                return -@as(c_int, 1);
            }
            ybf.*.buffer_alloc = @ptrCast(@alignCast(@as(?*anyopaque, @ptrFromInt((@as(usize, @intCast(@intFromPtr(fb.*.data))) +% @as(usize, @bitCast(@as(c_long, @as(c_int, 32) - @as(c_int, 1))))) & @as(usize, @bitCast(@as(c_long, -@as(c_int, 32))))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (frame_size > @as(u64, ybf.*.buffer_alloc_sz)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                antfly_vp9_vpx_free(@ptrCast(@alignCast(ybf.*.buffer_alloc)));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ybf.*.buffer_alloc = null;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ybf.*.buffer_alloc_sz = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ybf.*.buffer_alloc = @ptrCast(@alignCast(antfly_vp9_vpx_memalign(32, @truncate(frame_size))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (!(ybf.*.buffer_alloc != null)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    return -@as(c_int, 1);
                }
                ybf.*.buffer_alloc_sz = @truncate(frame_size);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(ybf.*.buffer_alloc)), 0, ybf.*.buffer_alloc_sz);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        ybf.*.y_crop_width = width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.y_crop_height = height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.y_width = aligned_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.y_height = aligned_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.y_stride = y_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.uv_crop_width = (width + ss_x) >> @intCast(ss_x);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.uv_crop_height = (height + ss_y) >> @intCast(ss_y);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.uv_width = uv_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.uv_height = uv_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.uv_stride = uv_stride;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.border = border;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.frame_size = @truncate(frame_size);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.subsampling_x = ss_x;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.subsampling_y = ss_y;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        buf = ybf.*.buffer_alloc;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (use_highbitdepth != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            buf = @as([*c]u8, @ptrFromInt(@as(usize, @intCast(@intFromPtr(ybf.*.buffer_alloc))) >> @intCast(@as(usize, 1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ybf.*.flags = 8;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ybf.*.flags = 0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.y_buffer = @ptrCast(@alignCast(@as(?*anyopaque, @ptrFromInt((@as(usize, @intCast(@intFromPtr((buf + @as(usize, @bitCast(@as(isize, @intCast(border * y_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(border))))))) +% @as(usize, @bitCast(@as(c_long, vp9_byte_align - @as(c_int, 1))))) & @as(usize, @bitCast(@as(c_long, -vp9_byte_align)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.u_buffer = @ptrCast(@alignCast(@as(?*anyopaque, @ptrFromInt((@as(usize, @intCast(@intFromPtr(((buf + yplane_size) + @as(usize, @bitCast(@as(isize, @intCast(uv_border_h * uv_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(uv_border_w))))))) +% @as(usize, @bitCast(@as(c_long, vp9_byte_align - @as(c_int, 1))))) & @as(usize, @bitCast(@as(c_long, -vp9_byte_align)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.v_buffer = @ptrCast(@alignCast(@as(?*anyopaque, @ptrFromInt((@as(usize, @intCast(@intFromPtr((((buf + yplane_size) + uvplane_size) + @as(usize, @bitCast(@as(isize, @intCast(uv_border_h * uv_stride))))) + @as(usize, @bitCast(@as(isize, @intCast(uv_border_w))))))) +% @as(usize, @bitCast(@as(c_long, vp9_byte_align - @as(c_int, 1))))) & @as(usize, @bitCast(@as(c_long, -vp9_byte_align)))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ybf.*.corrupted = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return -@as(c_int, 2);
}
