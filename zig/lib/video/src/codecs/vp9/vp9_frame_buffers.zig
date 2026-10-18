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
//  *  Copyright (c) 2014 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const vpx_codec_frame_buffer_t = common.vpx_codec_frame_buffer_t;
pub const InternalFrameBuffer = common.InternalFrameBuffer;
pub const InternalFrameBufferList = common.InternalFrameBufferList;
pub extern fn antfly_vp9_vpx_calloc(num: usize, size: usize) ?*anyopaque;
pub extern fn antfly_vp9_vpx_free(memblk: ?*anyopaque) void;
pub export fn antfly_vp9_vp9_alloc_internal_frame_buffers(arg_list: [*c]InternalFrameBufferList) c_int {
    var list = arg_list;
    _ = &list;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const num_buffers: c_int = @as(c_int, 8) + @as(c_int, 8);
    _ = &num_buffers;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    antfly_vp9_vp9_free_internal_frame_buffers(list);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    list.*.int_fb = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(@bitCast(@as(c_long, num_buffers)), @sizeOf(@TypeOf(list.*.int_fb.*)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (list.*.int_fb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        list.*.num_internal_frame_buffers = num_buffers;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return -@as(c_int, 1);
}
pub export fn antfly_vp9_vp9_free_internal_frame_buffers(arg_list: [*c]InternalFrameBufferList) void {
    var list = arg_list;
    _ = &list;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < list.*.num_internal_frame_buffers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            antfly_vp9_vpx_free(@ptrCast(@alignCast(list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data = null;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_free(@ptrCast(@alignCast(list.*.int_fb)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    list.*.int_fb = null;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    list.*.num_internal_frame_buffers = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_get_frame_buffer(arg_cb_priv: ?*anyopaque, arg_min_size: usize, arg_fb: [*c]vpx_codec_frame_buffer_t) c_int {
    var cb_priv = arg_cb_priv;
    _ = &cb_priv;
    var min_size = arg_min_size;
    _ = &min_size;
    var fb = arg_fb;
    _ = &fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const int_fb_list: [*c]InternalFrameBufferList = @ptrCast(@alignCast(cb_priv));
    _ = &int_fb_list;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(int_fb_list))) == @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return -@as(c_int, 1);
    }
    {
        i = 0;
        while (i < int_fb_list.*.num_internal_frame_buffers) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (!(int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].in_use != 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (i == int_fb_list.*.num_internal_frame_buffers) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return -@as(c_int, 1);
    }
    if (int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].size < min_size) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_free(@ptrCast(@alignCast(int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data = @ptrCast(@alignCast(antfly_vp9_vpx_calloc(1, min_size)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (!(int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data != null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            return -@as(c_int, 1);
        }
        int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].size = min_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    fb.*.data = int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    fb.*.size = int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))].in_use = 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    fb.*.priv = @ptrCast(@alignCast(&int_fb_list.*.int_fb[@bitCast(@as(isize, @intCast(i)))]));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return 0;
}
pub export fn antfly_vp9_vp9_release_frame_buffer(arg_cb_priv: ?*anyopaque, arg_fb: [*c]vpx_codec_frame_buffer_t) c_int {
    var cb_priv = arg_cb_priv;
    _ = &cb_priv;
    var fb = arg_fb;
    _ = &fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const int_fb: [*c]InternalFrameBuffer = @ptrCast(@alignCast(fb.*.priv));
    _ = &int_fb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = &cb_priv;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (int_fb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        int_fb.*.in_use = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    return 0;
}
