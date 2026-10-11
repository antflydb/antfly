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
//  * Copyright © 2023, VideoLAN and dav1d authors
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
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const Dav1dPalDSPContext = common.Dav1dPalDSPContext;
pub fn pal_idx_finish_c(arg_dst: [*c]u8, arg_src: [*c]const u8, bw: c_int, bh: c_int, w: c_int, h: c_int) callconv(.c) void {
    var dst = arg_dst;
    _ = &dst;
    var src = arg_src;
    _ = &src;
    _ = &bw;
    _ = &bh;
    _ = &w;
    _ = &h;
    while (true) {
        if (!(((bw >= @as(c_int, 4)) and (bw <= @as(c_int, 64))) and !((bw & (bw - @as(c_int, 1))) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(((bh >= @as(c_int, 4)) and (bh <= @as(c_int, 64))) and !((bh & (bh - @as(c_int, 1))) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(((w >= @as(c_int, 4)) and (w <= bw)) and !((w & @as(c_int, 3)) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(((h >= @as(c_int, 4)) and (h <= bh)) and !((h & @as(c_int, 3)) != 0))) {
            unreachable;
        }
        if (!false) break;
    }
    const dst_w: c_int = @divTrunc(w, @as(c_int, 2));
    _ = &dst_w;
    const dst_bw: c_int = @divTrunc(bw, @as(c_int, 2));
    _ = &dst_bw;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < h) : ({
            y += 1;
            src += @as(usize, @bitCast(@as(isize, @intCast(bw))));
            dst += @as(usize, @bitCast(@as(isize, @intCast(dst_bw))));
        }) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < dst_w) : (x += 1) {
                    dst[@bitCast(@as(isize, @intCast(x)))] = @bitCast(@as(i8, @truncate(@as(c_int, src[@bitCast(@as(isize, @intCast((x * @as(c_int, 2)) + @as(c_int, 0))))]) | (@as(c_int, src[@bitCast(@as(isize, @intCast((x * @as(c_int, 2)) + @as(c_int, 1))))]) << @intCast(@as(c_int, 4))))));
                }
            }
            if (dst_w < dst_bw) {
                _ = antfly_codec_afz_memset(@ptrCast(@alignCast(dst + @as(usize, @bitCast(@as(isize, @intCast(dst_w)))))), @as(c_int, src[@bitCast(@as(isize, @intCast(w - @as(c_int, 1))))]) * @as(c_int, 17), @bitCast(@as(c_long, dst_bw - dst_w)));
            }
        }
    }
    if (h < bh) {
        const last_row: [*c]const u8 = &dst[@bitCast(@as(isize, @intCast(-dst_bw)))];
        _ = &last_row;
        {
            var y: c_int = h;
            _ = &y;
            while (y < bh) : ({
                y += 1;
                dst += @as(usize, @bitCast(@as(isize, @intCast(dst_bw))));
            }) {
                _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(dst)), @ptrCast(@alignCast(last_row)), @bitCast(@as(c_long, dst_bw)));
            }
        }
    }
}
pub export fn antfly_av1_dav1d_pal_dsp_init(c: [*c]Dav1dPalDSPContext) void {
    _ = &c;
    c.*.pal_idx_finish = pal_idx_finish_c;
}
