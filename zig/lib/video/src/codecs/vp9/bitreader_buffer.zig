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
pub const vpx_rb_error_handler = common.vpx_rb_error_handler;
pub const struct_vpx_read_bit_buffer = extern struct {
    bit_buffer: [*c]const u8 = null,
    bit_buffer_end: [*c]const u8 = null,
    bit_offset: usize = 0,
    error_handler_data: ?*anyopaque = null,
    error_handler: vpx_rb_error_handler = null,
};
pub export fn antfly_vp9_vpx_rb_bytes_read(arg_rb: [*c]struct_vpx_read_bit_buffer) usize {
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return (rb.*.bit_offset +% @as(usize, 7)) >> @intCast(@as(usize, 3));
}
pub export fn antfly_vp9_vpx_rb_read_bit(arg_rb: [*c]struct_vpx_read_bit_buffer) c_int {
    var rb = arg_rb;
    _ = &rb;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const off: usize = rb.*.bit_offset;
    _ = &off;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const p: usize = off >> @intCast(@as(usize, 3));
    _ = &p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const q: c_int = @as(c_int, 7) - @as(c_int, @bitCast(@as(c_uint, @truncate(off & @as(usize, 7)))));
    _ = &q;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((rb.*.bit_buffer + p) < rb.*.bit_buffer_end) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const bit: c_int = (@as(c_int, rb.*.bit_buffer[p]) >> @intCast(q)) & @as(c_int, 1);
        _ = &bit;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        rb.*.bit_offset = off +% @as(usize, 1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return bit;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(rb.*.error_handler)))) != @as(?*anyopaque, null)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            rb.*.error_handler.?(rb.*.error_handler_data);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        return 0;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub export fn antfly_vp9_vpx_rb_read_literal(arg_rb: [*c]struct_vpx_read_bit_buffer, arg_bits: c_int) c_int {
    var rb = arg_rb;
    _ = &rb;
    var bits = arg_bits;
    _ = &bits;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var value: c_int = 0;
    _ = &value;
    var bit: c_int = undefined;
    _ = &bit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        bit = bits - @as(c_int, 1);
        while (bit >= @as(c_int, 0)) : (bit -= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            value |= antfly_vp9_vpx_rb_read_bit(rb) << @intCast(bit);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return value;
}
pub export fn antfly_vp9_vpx_rb_read_signed_literal(arg_rb: [*c]struct_vpx_read_bit_buffer, arg_bits: c_int) c_int {
    var rb = arg_rb;
    _ = &rb;
    var bits = arg_bits;
    _ = &bits;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const value: c_int = antfly_vp9_vpx_rb_read_literal(rb, bits);
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (antfly_vp9_vpx_rb_read_bit(rb) != 0) -value else value;
}
