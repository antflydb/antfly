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
pub const vpx_decrypt_cb = common.vpx_decrypt_cb;
pub extern const antfly_vp9_vpx_norm: [256]u8 align(16);
pub const BD_VALUE = common.BD_VALUE;
pub const vpx_reader = extern struct {
    value: BD_VALUE = 0,
    range: c_uint = 0,
    count: c_int = 0,
    buffer_end: [*c]const u8 = null,
    buffer: [*c]const u8 = null,
    decrypt_cb: vpx_decrypt_cb = null,
    decrypt_state: ?*anyopaque = null,
    clear_buffer: [9]u8 = @import("std").mem.zeroes([9]u8),
};
pub fn vpx_read(arg_r: [*c]vpx_reader, arg_prob: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var prob = arg_prob;
    _ = &prob;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bit: c_uint = 0;
    _ = &bit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var value: BD_VALUE = undefined;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var bigsplit: BD_VALUE = undefined;
    _ = &bigsplit;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var count: c_int = undefined;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var range: c_uint = undefined;
    _ = &range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var split: c_uint = ((r.*.range *% @as(c_uint, @bitCast(@as(c_int, prob)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 256) - prob)))) >> @intCast(@as(c_uint, 8));
    _ = &split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (r.*.count < @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_reader_fill(r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    value = r.*.value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    count = r.*.count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    bigsplit = @as(BD_VALUE, split) << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - @as(c_int, 8)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    range = split;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (value >= bigsplit) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range = r.*.range -% split;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value = value -% bigsplit;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        bit = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        const shift: u8 = antfly_vp9_vpx_norm[@as(u8, @truncate(range))];
        _ = &shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        range <<= @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        value <<= @intCast(@as(BD_VALUE, @bitCast(@as(c_long, @as(c_int, shift)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        count -= shift;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.value = value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.count = count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    r.*.range = range;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @bitCast(@as(c_uint, @truncate(bit)));
}
pub fn vpx_read_bit(arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return vpx_read(r, 128);
}
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub fn BSwap64(arg_x: u64) callconv(.c) u64 {
    var x = arg_x;
    _ = &x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @byteSwap(@as(c_ulong, @truncate(x)));
}
pub export fn antfly_vp9_vpx_reader_init(arg_r: [*c]vpx_reader, arg_buffer: [*c]const u8, arg_size: usize, arg_decrypt_cb: vpx_decrypt_cb, arg_decrypt_state: ?*anyopaque) c_int {
    var r = arg_r;
    _ = &r;
    var buffer = arg_buffer;
    _ = &buffer;
    var size = arg_size;
    _ = &size;
    var decrypt_cb = arg_decrypt_cb;
    _ = &decrypt_cb;
    var decrypt_state = arg_decrypt_state;
    _ = &decrypt_state;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((size != 0) and (@as(usize, @bitCast(@as(c_long, @as(c_int, @intFromBool(!(buffer != null)))))) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.buffer_end = buffer + size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.buffer = buffer;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.value = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.count = -@as(c_int, 8);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.range = 255;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.decrypt_cb = decrypt_cb;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        r.*.decrypt_state = decrypt_state;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        antfly_vp9_vpx_reader_fill(r);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @as(c_int, @intFromBool(vpx_read_bit(r) != @as(c_int, 0)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub export fn antfly_vp9_vpx_reader_fill(arg_r: [*c]vpx_reader) void {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const buffer_end: [*c]const u8 = r.*.buffer_end;
    _ = &buffer_end;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var buffer: [*c]const u8 = r.*.buffer;
    _ = &buffer;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var buffer_start: [*c]const u8 = buffer;
    _ = &buffer_start;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var value: BD_VALUE = r.*.value;
    _ = &value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var count: c_int = r.*.count;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bytes_left: usize = @bitCast(@as(c_long, @divExact(@as(c_long, @bitCast(@intFromPtr(buffer_end) -% @intFromPtr(buffer))), @sizeOf(u8))));
    _ = &bytes_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bits_left: usize = bytes_left *% @as(usize, 8);
    _ = &bits_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var shift: c_int = ((@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - @as(c_int, 8)) - (count + @as(c_int, 8));
    _ = &shift;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (r.*.decrypt_cb != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var n: usize = if (@sizeOf(@TypeOf(r.*.clear_buffer)) < bytes_left) @sizeOf(@TypeOf(r.*.clear_buffer)) else bytes_left;
        _ = &n;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        r.*.decrypt_cb.?(r.*.decrypt_state, buffer, @ptrCast(@alignCast(&r.*.clear_buffer)), @bitCast(@as(c_uint, @truncate(n))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        buffer = @ptrCast(@alignCast(&r.*.clear_buffer));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        buffer_start = @ptrCast(@alignCast(&r.*.clear_buffer));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (bits_left > @as(usize, @bitCast(@as(c_long, @as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8))))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const bits: c_int = @bitCast(@as(c_uint, @truncate((@as(c_uint, @bitCast(@as(c_int, shift))) & @as(c_uint, 4294967288)) +% @as(c_uint, 8))));
        _ = &bits;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var nv: BD_VALUE = undefined;
        _ = &nv;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var big_endian_values: BD_VALUE = undefined;
        _ = &big_endian_values;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(&big_endian_values)), @ptrCast(@alignCast(buffer)), @sizeOf(BD_VALUE));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        big_endian_values = @truncate(BSwap64(big_endian_values));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        nv = big_endian_values >> @intCast(@as(BD_VALUE, @bitCast(@as(c_long, (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)) - bits))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        count += bits;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        buffer += @as(usize, @bitCast(@as(isize, @intCast(bits >> @intCast(@as(c_int, 3))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        value = r.*.value | (nv << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, shift & @as(c_int, 7))))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const bits_over: c_int = (shift + @as(c_int, 8)) - @as(c_int, @bitCast(@as(c_uint, @truncate(bits_left))));
        _ = &bits_over;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var loop_end: c_int = 0;
        _ = &loop_end;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (bits_over >= @as(c_int, 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            count += 1073741824;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            loop_end = bits_over;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if ((@as(usize, @bitCast(@as(c_long, @as(c_int, @intFromBool(bits_over < @as(c_int, 0)))))) != 0) or (bits_left != 0)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            while (shift >= loop_end) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                count += 8;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                value |= @as(BD_VALUE, (blk: {
                    const ref = &buffer;
                    const tmp = ref.*;
                    ref.* += 1;
                    break :blk tmp;
                }).*) << @intCast(@as(BD_VALUE, @bitCast(@as(c_long, shift))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                shift -= 8;
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
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    r.*.buffer += @as(usize, @bitCast(@as(isize, @intCast(@divExact(@as(c_long, @bitCast(@intFromPtr(buffer) -% @intFromPtr(buffer_start))), @sizeOf(u8))))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    r.*.value = value;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    r.*.count = count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_reader_find_end(arg_r: [*c]vpx_reader) [*c]const u8 {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    while ((r.*.count > @as(c_int, 8)) and (r.*.count < (@as(c_int, @bitCast(@as(c_uint, @truncate(@sizeOf(BD_VALUE))))) * @as(c_int, 8)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        r.*.count -= 8;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        r.*.buffer -= 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return r.*.buffer;
}
