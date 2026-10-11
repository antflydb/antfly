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
//  *  Copyright (c) 2015 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_abs(c_int) c_int;
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
pub fn signed_char_clamp(arg_t: c_int) callconv(.c) i8 {
    var t = arg_t;
    _ = &t;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(clamp(t, -@as(c_int, 128), 127));
}
pub fn signed_char_clamp_high(arg_t: c_int, arg_bd: c_int) callconv(.c) i16 {
    var t = arg_t;
    _ = &t;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        switch (bd) {
            @as(c_int, 10) => {
                return @truncate(clamp(t, -@as(c_int, 128) * @as(c_int, 4), (@as(c_int, 128) * @as(c_int, 4)) - @as(c_int, 1)));
            },
            @as(c_int, 12) => {
                return @truncate(clamp(t, -@as(c_int, 128) * @as(c_int, 16), (@as(c_int, 128) * @as(c_int, 16)) - @as(c_int, 1)));
            },
            else => {
                return @truncate(clamp(t, -@as(c_int, 128), @as(c_int, 128) - @as(c_int, 1)));
            },
        }
    }
}
pub fn filter_mask(arg_limit: u8, arg_blimit: u8, arg_p3: u8, arg_p2: u8, arg_p1: u8, arg_p0: u8, arg_q0: u8, arg_q1: u8, arg_q2: u8, arg_q3: u8) callconv(.c) i8 {
    var limit = arg_limit;
    _ = &limit;
    var blimit = arg_blimit;
    _ = &blimit;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = 0;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p3) - @as(c_int, p2)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p2) - @as(c_int, p1)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q2) - @as(c_int, q1)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q3) - @as(c_int, q2)) > @as(c_int, limit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(((antfly_codec_abs(@as(c_int, p0) - @as(c_int, q0)) * @as(c_int, 2)) + @divTrunc(antfly_codec_abs(@as(c_int, p1) - @as(c_int, q1)), @as(c_int, 2))) > @as(c_int, blimit))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn flat_mask4(arg_thresh: u8, arg_p3: u8, arg_p2: u8, arg_p1: u8, arg_p0: u8, arg_q0: u8, arg_q1: u8, arg_q2: u8, arg_q3: u8) callconv(.c) i8 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = 0;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p2) - @as(c_int, p0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q2) - @as(c_int, q0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p3) - @as(c_int, p0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q3) - @as(c_int, q0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn flat_mask5(arg_thresh: u8, arg_p4: u8, arg_p3: u8, arg_p2: u8, arg_p1: u8, arg_p0: u8, arg_q0: u8, arg_q1: u8, arg_q2: u8, arg_q3: u8, arg_q4: u8) callconv(.c) i8 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p4 = arg_p4;
    _ = &p4;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    var q4 = arg_q4;
    _ = &q4;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = @truncate(~@as(c_int, flat_mask4(thresh, p3, p2, p1, p0, q0, q1, q2, q3)));
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p4) - @as(c_int, p0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q4) - @as(c_int, q0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn hev_mask(arg_thresh: u8, arg_p1: u8, arg_p0: u8, arg_q0: u8, arg_q1: u8) callconv(.c) i8 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var hev: i8 = 0;
    _ = &hev;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &hev;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &hev;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, thresh))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return hev;
}
pub fn filter4(arg_mask: i8, arg_thresh: u8, arg_op1: [*c]u8, arg_op0: [*c]u8, arg_oq0: [*c]u8, arg_oq1: [*c]u8) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var filter1: i8 = undefined;
    _ = &filter1;
    var filter2: i8 = undefined;
    _ = &filter2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ps1: i8 = @truncate(@as(c_int, op1.*) ^ @as(c_int, 128));
    _ = &ps1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ps0: i8 = @truncate(@as(c_int, op0.*) ^ @as(c_int, 128));
    _ = &ps0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const qs0: i8 = @truncate(@as(c_int, oq0.*) ^ @as(c_int, 128));
    _ = &qs0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const qs1: i8 = @truncate(@as(c_int, oq1.*) ^ @as(c_int, 128));
    _ = &qs1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const hev: i8 = hev_mask(thresh, op1.*, op0.*, oq0.*, oq1.*);
    _ = &hev;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var filter: i8 = @truncate(@as(c_int, signed_char_clamp(@as(c_int, ps1) - @as(c_int, qs1))) & @as(c_int, hev));
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter = @truncate(@as(c_int, signed_char_clamp(@as(c_int, filter) + (@as(c_int, 3) * (@as(c_int, qs0) - @as(c_int, ps0))))) & @as(c_int, mask));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter1 = @truncate(@as(c_int, signed_char_clamp(@as(c_int, filter) + @as(c_int, 4))) >> @intCast(@as(c_int, 3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter2 = @truncate(@as(c_int, signed_char_clamp(@as(c_int, filter) + @as(c_int, 3))) >> @intCast(@as(c_int, 3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    oq0.* = @bitCast(@as(i8, @truncate(@as(c_int, signed_char_clamp(@as(c_int, qs0) - @as(c_int, filter1))) ^ @as(c_int, 128))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op0.* = @bitCast(@as(i8, @truncate(@as(c_int, signed_char_clamp(@as(c_int, ps0) + @as(c_int, filter2))) ^ @as(c_int, 128))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter = @truncate(((@as(c_int, filter1) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1))) & ~@as(c_int, hev));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    oq1.* = @bitCast(@as(i8, @truncate(@as(c_int, signed_char_clamp(@as(c_int, qs1) - @as(c_int, filter))) ^ @as(c_int, 128))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op1.* = @bitCast(@as(i8, @truncate(@as(c_int, signed_char_clamp(@as(c_int, ps1) + @as(c_int, filter))) ^ @as(c_int, 128))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_4_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            const q1: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            const q2: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            const q3: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter4(mask, thresh.*, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_4_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_horizontal_4_c(s, pitch, blimit0, limit0, thresh0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), pitch, blimit1, limit1, thresh1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_4_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@as(c_int, 0)];
            _ = &q0;
            const q1: u8 = s[@as(c_int, 1)];
            _ = &q1;
            const q2: u8 = s[@as(c_int, 2)];
            _ = &q2;
            const q3: u8 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter4(mask, thresh.*, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_4_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_vertical_4_c(s, pitch, blimit0, limit0, thresh0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_vertical_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), pitch, blimit1, limit1, thresh1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn filter8(arg_mask: i8, arg_thresh: u8, arg_flat: i8, arg_op3: [*c]u8, arg_op2: [*c]u8, arg_op1: [*c]u8, arg_op0: [*c]u8, arg_oq0: [*c]u8, arg_oq1: [*c]u8, arg_oq2: [*c]u8, arg_oq3: [*c]u8) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var flat = arg_flat;
    _ = &flat;
    var op3 = arg_op3;
    _ = &op3;
    var op2 = arg_op2;
    _ = &op2;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    var oq2 = arg_oq2;
    _ = &oq2;
    var oq3 = arg_oq3;
    _ = &oq3;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, flat) != 0) and (@as(c_int, mask) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p3: u8 = op3.*;
        _ = &p3;
        const p2: u8 = op2.*;
        _ = &p2;
        const p1: u8 = op1.*;
        _ = &p1;
        const p0: u8 = op0.*;
        _ = &p0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q0: u8 = oq0.*;
        _ = &q0;
        const q1: u8 = oq1.*;
        _ = &q1;
        const q2: u8 = oq2.*;
        _ = &q2;
        const q3: u8 = oq3.*;
        _ = &q3;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op2.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p3) + @as(c_int, p3)) + @as(c_int, p3)) + (@as(c_int, 2) * @as(c_int, p2))) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op1.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p3) + @as(c_int, p3)) + @as(c_int, p2)) + (@as(c_int, 2) * @as(c_int, p1))) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op0.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p3) + @as(c_int, p2)) + @as(c_int, p1)) + (@as(c_int, 2) * @as(c_int, p0))) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq0.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p2) + @as(c_int, p1)) + @as(c_int, p0)) + (@as(c_int, 2) * @as(c_int, q0))) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq1.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p1) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 2) * @as(c_int, q1))) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq2.* = @bitCast(@as(i8, @truncate((((((((@as(c_int, p0) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 2) * @as(c_int, q2))) + @as(c_int, q3)) + @as(c_int, q3)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        filter4(mask, thresh, op1, op0, oq0, oq1);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_8_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            const q1: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            const q2: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            const q3: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter8(mask, thresh.*, flat, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_8_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_horizontal_8_c(s, pitch, blimit0, limit0, thresh0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_horizontal_8_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), pitch, blimit1, limit1, thresh1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_8_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@as(c_int, 0)];
            _ = &q0;
            const q1: u8 = s[@as(c_int, 1)];
            _ = &q1;
            const q2: u8 = s[@as(c_int, 2)];
            _ = &q2;
            const q3: u8 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter8(mask, thresh.*, flat, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_8_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_vertical_8_c(s, pitch, blimit0, limit0, thresh0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_lpf_vertical_8_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), pitch, blimit1, limit1, thresh1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn filter16(arg_mask: i8, arg_thresh: u8, arg_flat: i8, arg_flat2: i8, arg_op7: [*c]u8, arg_op6: [*c]u8, arg_op5: [*c]u8, arg_op4: [*c]u8, arg_op3: [*c]u8, arg_op2: [*c]u8, arg_op1: [*c]u8, arg_op0: [*c]u8, arg_oq0: [*c]u8, arg_oq1: [*c]u8, arg_oq2: [*c]u8, arg_oq3: [*c]u8, arg_oq4: [*c]u8, arg_oq5: [*c]u8, arg_oq6: [*c]u8, arg_oq7: [*c]u8) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var flat = arg_flat;
    _ = &flat;
    var flat2 = arg_flat2;
    _ = &flat2;
    var op7 = arg_op7;
    _ = &op7;
    var op6 = arg_op6;
    _ = &op6;
    var op5 = arg_op5;
    _ = &op5;
    var op4 = arg_op4;
    _ = &op4;
    var op3 = arg_op3;
    _ = &op3;
    var op2 = arg_op2;
    _ = &op2;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    var oq2 = arg_oq2;
    _ = &oq2;
    var oq3 = arg_oq3;
    _ = &oq3;
    var oq4 = arg_oq4;
    _ = &oq4;
    var oq5 = arg_oq5;
    _ = &oq5;
    var oq6 = arg_oq6;
    _ = &oq6;
    var oq7 = arg_oq7;
    _ = &oq7;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((@as(c_int, flat2) != 0) and (@as(c_int, flat) != 0)) and (@as(c_int, mask) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p7: u8 = op7.*;
        _ = &p7;
        const p6: u8 = op6.*;
        _ = &p6;
        const p5: u8 = op5.*;
        _ = &p5;
        const p4: u8 = op4.*;
        _ = &p4;
        const p3: u8 = op3.*;
        _ = &p3;
        const p2: u8 = op2.*;
        _ = &p2;
        const p1: u8 = op1.*;
        _ = &p1;
        const p0: u8 = op0.*;
        _ = &p0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q0: u8 = oq0.*;
        _ = &q0;
        const q1: u8 = oq1.*;
        _ = &q1;
        const q2: u8 = oq2.*;
        _ = &q2;
        const q3: u8 = oq3.*;
        _ = &q3;
        const q4: u8 = oq4.*;
        _ = &q4;
        const q5: u8 = oq5.*;
        _ = &q5;
        const q6: u8 = oq6.*;
        _ = &q6;
        const q7: u8 = oq7.*;
        _ = &q7;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op6.* = @bitCast(@as(i8, @truncate(((((((((((@as(c_int, p7) * @as(c_int, 7)) + (@as(c_int, p6) * @as(c_int, 2))) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op5.* = @bitCast(@as(i8, @truncate((((((((((((@as(c_int, p7) * @as(c_int, 6)) + @as(c_int, p6)) + (@as(c_int, p5) * @as(c_int, 2))) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op4.* = @bitCast(@as(i8, @truncate(((((((((((((@as(c_int, p7) * @as(c_int, 5)) + @as(c_int, p6)) + @as(c_int, p5)) + (@as(c_int, p4) * @as(c_int, 2))) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op3.* = @bitCast(@as(i8, @truncate((((((((((((((@as(c_int, p7) * @as(c_int, 4)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + (@as(c_int, p3) * @as(c_int, 2))) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op2.* = @bitCast(@as(i8, @truncate(((((((((((((((@as(c_int, p7) * @as(c_int, 3)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + (@as(c_int, p2) * @as(c_int, 2))) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op1.* = @bitCast(@as(i8, @truncate((((((((((((((((@as(c_int, p7) * @as(c_int, 2)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + (@as(c_int, p1) * @as(c_int, 2))) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op0.* = @bitCast(@as(i8, @truncate((((((((((((((((@as(c_int, p7) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + (@as(c_int, p0) * @as(c_int, 2))) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq0.* = @bitCast(@as(i8, @truncate((((((((((((((((@as(c_int, p6) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + (@as(c_int, q0) * @as(c_int, 2))) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + @as(c_int, q7)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq1.* = @bitCast(@as(i8, @truncate(((((((((((((((@as(c_int, p5) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, q1) * @as(c_int, 2))) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 2))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq2.* = @bitCast(@as(i8, @truncate((((((((((((((@as(c_int, p4) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, q2) * @as(c_int, 2))) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 3))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq3.* = @bitCast(@as(i8, @truncate(((((((((((((@as(c_int, p3) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, q3) * @as(c_int, 2))) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 4))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq4.* = @bitCast(@as(i8, @truncate((((((((((((@as(c_int, p2) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, q4) * @as(c_int, 2))) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 5))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq5.* = @bitCast(@as(i8, @truncate(((((((((((@as(c_int, p1) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + (@as(c_int, q5) * @as(c_int, 2))) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 6))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq6.* = @bitCast(@as(i8, @truncate((((((((((@as(c_int, p0) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + (@as(c_int, q6) * @as(c_int, 2))) + (@as(c_int, q7) * @as(c_int, 7))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        filter8(mask, thresh, flat, op3, op2, op1, op0, oq0, oq1, oq2, oq3);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn mb_lpf_horizontal_edge_w(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_count: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var count = arg_count;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (@as(c_int, 8) * count)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            const q1: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            const q2: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            const q3: u8 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat2: i8 = flat_mask5(1, s[@bitCast(@as(isize, @intCast(-@as(c_int, 8) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 7) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 6) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 5) * pitch)))], p0, q0, s[@bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))]);
            _ = &flat2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter16(mask, thresh.*, flat, flat2, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_16_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_lpf_horizontal_edge_w(s, pitch, blimit, limit, thresh, 1);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_horizontal_16_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_lpf_horizontal_edge_w(s, pitch, blimit, limit, thresh, 2);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn mb_lpf_vertical_edge_w(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_count: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var count = arg_count;
    _ = &count;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < count) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            const p2: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            const p1: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            const p0: u8 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u8 = s[@as(c_int, 0)];
            _ = &q0;
            const q1: u8 = s[@as(c_int, 1)];
            _ = &q1;
            const q2: u8 = s[@as(c_int, 2)];
            _ = &q2;
            const q3: u8 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat2: i8 = flat_mask5(1, s[@bitCast(@as(isize, @intCast(-@as(c_int, 8))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 7))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 6))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 5))))], p0, q0, s[@as(c_int, 4)], s[@as(c_int, 5)], s[@as(c_int, 6)], s[@as(c_int, 7)]);
            _ = &flat2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            filter16(mask, thresh.*, flat, flat2, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7))))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_16_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_lpf_vertical_edge_w(s, pitch, blimit, limit, thresh, 8);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_lpf_vertical_16_dual_c(arg_s: [*c]u8, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    mb_lpf_vertical_edge_w(s, pitch, blimit, limit, thresh, 16);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter_mask(arg_limit: u8, arg_blimit: u8, arg_p3: u16, arg_p2: u16, arg_p1: u16, arg_p0: u16, arg_q0: u16, arg_q1: u16, arg_q2: u16, arg_q3: u16, arg_bd: c_int) callconv(.c) i8 {
    var limit = arg_limit;
    _ = &limit;
    var blimit = arg_blimit;
    _ = &blimit;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = 0;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var limit16: i16 = @truncate(@as(c_int, @as(u16, limit)) << @intCast(bd - @as(c_int, 8)));
    _ = &limit16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var blimit16: i16 = @truncate(@as(c_int, @as(u16, blimit)) << @intCast(bd - @as(c_int, 8)));
    _ = &blimit16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p3) - @as(c_int, p2)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p2) - @as(c_int, p1)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q2) - @as(c_int, q1)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q3) - @as(c_int, q2)) > @as(c_int, limit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(((antfly_codec_abs(@as(c_int, p0) - @as(c_int, q0)) * @as(c_int, 2)) + @divTrunc(antfly_codec_abs(@as(c_int, p1) - @as(c_int, q1)), @as(c_int, 2))) > @as(c_int, blimit16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn highbd_flat_mask4(arg_thresh: u8, arg_p3: u16, arg_p2: u16, arg_p1: u16, arg_p0: u16, arg_q0: u16, arg_q1: u16, arg_q2: u16, arg_q3: u16, arg_bd: c_int) callconv(.c) i8 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = 0;
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var thresh16: i16 = @truncate(@as(c_int, @as(u16, thresh)) << @intCast(bd - @as(c_int, 8)));
    _ = &thresh16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p2) - @as(c_int, p0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q2) - @as(c_int, q0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p3) - @as(c_int, p0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q3) - @as(c_int, q0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn highbd_flat_mask5(arg_thresh: u8, arg_p4: u16, arg_p3: u16, arg_p2: u16, arg_p1: u16, arg_p0: u16, arg_q0: u16, arg_q1: u16, arg_q2: u16, arg_q3: u16, arg_q4: u16, arg_bd: c_int) callconv(.c) i8 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p4 = arg_p4;
    _ = &p4;
    var p3 = arg_p3;
    _ = &p3;
    var p2 = arg_p2;
    _ = &p2;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var q2 = arg_q2;
    _ = &q2;
    var q3 = arg_q3;
    _ = &q3;
    var q4 = arg_q4;
    _ = &q4;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var mask: i8 = @truncate(~@as(c_int, highbd_flat_mask4(thresh, p3, p2, p1, p0, q0, q1, q2, q3, bd)));
    _ = &mask;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var thresh16: i16 = @truncate(@as(c_int, @as(u16, thresh)) << @intCast(bd - @as(c_int, 8)));
    _ = &thresh16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p4) - @as(c_int, p0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &mask;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q4) - @as(c_int, q0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @truncate(~@as(c_int, mask));
}
pub fn highbd_hev_mask(arg_thresh: u8, arg_p1: u16, arg_p0: u16, arg_q0: u16, arg_q1: u16, arg_bd: c_int) callconv(.c) i16 {
    var thresh = arg_thresh;
    _ = &thresh;
    var p1 = arg_p1;
    _ = &p1;
    var p0 = arg_p0;
    _ = &p0;
    var q0 = arg_q0;
    _ = &q0;
    var q1 = arg_q1;
    _ = &q1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var hev: i16 = 0;
    _ = &hev;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var thresh16: i16 = @truncate(@as(c_int, @as(u16, thresh)) << @intCast(bd - @as(c_int, 8)));
    _ = &thresh16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &hev;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, p1) - @as(c_int, p0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    {
        const ref = &hev;
        ref.* = @truncate(@as(c_int, ref.*) | (@as(c_int, @intFromBool(antfly_codec_abs(@as(c_int, q1) - @as(c_int, q0)) > @as(c_int, thresh16))) * -@as(c_int, 1)));
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return hev;
}
pub fn highbd_filter4(arg_mask: i8, arg_thresh: u8, arg_op1: [*c]u16, arg_op0: [*c]u16, arg_oq0: [*c]u16, arg_oq1: [*c]u16, arg_bd: c_int) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var filter1: i16 = undefined;
    _ = &filter1;
    var filter2: i16 = undefined;
    _ = &filter2;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var shift: c_int = bd - @as(c_int, 8);
    _ = &shift;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ps1: i16 = @truncate(@as(c_int, @as(i16, @bitCast(@as(c_ushort, @truncate(op1.*))))) - (@as(c_int, 128) << @intCast(shift)));
    _ = &ps1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const ps0: i16 = @truncate(@as(c_int, @as(i16, @bitCast(@as(c_ushort, @truncate(op0.*))))) - (@as(c_int, 128) << @intCast(shift)));
    _ = &ps0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const qs0: i16 = @truncate(@as(c_int, @as(i16, @bitCast(@as(c_ushort, @truncate(oq0.*))))) - (@as(c_int, 128) << @intCast(shift)));
    _ = &qs0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const qs1: i16 = @truncate(@as(c_int, @as(i16, @bitCast(@as(c_ushort, @truncate(oq1.*))))) - (@as(c_int, 128) << @intCast(shift)));
    _ = &qs1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const hev: i16 = highbd_hev_mask(thresh, op1.*, op0.*, oq0.*, oq1.*, bd);
    _ = &hev;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var filter: i16 = @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, ps1) - @as(c_int, qs1), bd)) & @as(c_int, hev));
    _ = &filter;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter = @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, filter) + (@as(c_int, 3) * (@as(c_int, qs0) - @as(c_int, ps0))), bd)) & @as(c_int, mask));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter1 = @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, filter) + @as(c_int, 4), bd)) >> @intCast(@as(c_int, 3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter2 = @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, filter) + @as(c_int, 3), bd)) >> @intCast(@as(c_int, 3)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    oq0.* = @bitCast(@as(c_short, @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, qs0) - @as(c_int, filter1), bd)) + (@as(c_int, 128) << @intCast(shift)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op0.* = @bitCast(@as(c_short, @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, ps0) + @as(c_int, filter2), bd)) + (@as(c_int, 128) << @intCast(shift)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    filter = @truncate(((@as(c_int, filter1) + (@as(c_int, 1) << @intCast(@as(c_int, 1) - @as(c_int, 1)))) >> @intCast(@as(c_int, 1))) & ~@as(c_int, hev));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    oq1.* = @bitCast(@as(c_short, @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, qs1) - @as(c_int, filter), bd)) + (@as(c_int, 128) << @intCast(shift)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    op1.* = @bitCast(@as(c_short, @truncate(@as(c_int, signed_char_clamp_high(@as(c_int, ps1) + @as(c_int, filter), bd)) + (@as(c_int, 128) << @intCast(shift)))));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_4_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q1: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q2: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q3: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter4(mask, thresh.*, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_4_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s, pitch, blimit0, limit0, thresh0, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_horizontal_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), pitch, blimit1, limit1, thresh1, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_4_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@as(c_int, 0)];
            _ = &q0;
            const q1: u16 = s[@as(c_int, 1)];
            _ = &q1;
            const q2: u16 = s[@as(c_int, 2)];
            _ = &q2;
            const q3: u16 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter4(mask, thresh.*, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_4_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_vertical_4_c(s, pitch, blimit0, limit0, thresh0, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_vertical_4_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), pitch, blimit1, limit1, thresh1, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter8(arg_mask: i8, arg_thresh: u8, arg_flat: i8, arg_op3: [*c]u16, arg_op2: [*c]u16, arg_op1: [*c]u16, arg_op0: [*c]u16, arg_oq0: [*c]u16, arg_oq1: [*c]u16, arg_oq2: [*c]u16, arg_oq3: [*c]u16, arg_bd: c_int) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var flat = arg_flat;
    _ = &flat;
    var op3 = arg_op3;
    _ = &op3;
    var op2 = arg_op2;
    _ = &op2;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    var oq2 = arg_oq2;
    _ = &oq2;
    var oq3 = arg_oq3;
    _ = &oq3;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, flat) != 0) and (@as(c_int, mask) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p3: u16 = op3.*;
        _ = &p3;
        const p2: u16 = op2.*;
        _ = &p2;
        const p1: u16 = op1.*;
        _ = &p1;
        const p0: u16 = op0.*;
        _ = &p0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q0: u16 = oq0.*;
        _ = &q0;
        const q1: u16 = oq1.*;
        _ = &q1;
        const q2: u16 = oq2.*;
        _ = &q2;
        const q3: u16 = oq3.*;
        _ = &q3;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op2.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p3) + @as(c_int, p3)) + @as(c_int, p3)) + (@as(c_int, 2) * @as(c_int, p2))) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op1.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p3) + @as(c_int, p3)) + @as(c_int, p2)) + (@as(c_int, 2) * @as(c_int, p1))) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op0.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p3) + @as(c_int, p2)) + @as(c_int, p1)) + (@as(c_int, 2) * @as(c_int, p0))) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq0.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p2) + @as(c_int, p1)) + @as(c_int, p0)) + (@as(c_int, 2) * @as(c_int, q0))) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq1.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p1) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 2) * @as(c_int, q1))) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq2.* = @bitCast(@as(c_short, @truncate((((((((@as(c_int, p0) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 2) * @as(c_int, q2))) + @as(c_int, q3)) + @as(c_int, q3)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 3) - @as(c_int, 1)))) >> @intCast(@as(c_int, 3)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        highbd_filter4(mask, thresh, op1, op0, oq0, oq1, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_8_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            const q1: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            const q2: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            const q3: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = highbd_flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter8(mask, thresh.*, flat, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_8_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_horizontal_8_c(s, pitch, blimit0, limit0, thresh0, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_horizontal_8_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), pitch, blimit1, limit1, thresh1, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_8_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < @as(c_int, 8)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@as(c_int, 0)];
            _ = &q0;
            const q1: u16 = s[@as(c_int, 1)];
            _ = &q1;
            const q2: u16 = s[@as(c_int, 2)];
            _ = &q2;
            const q3: u16 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = highbd_flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter8(mask, thresh.*, flat, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_8_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit0: [*c]const u8, arg_limit0: [*c]const u8, arg_thresh0: [*c]const u8, arg_blimit1: [*c]const u8, arg_limit1: [*c]const u8, arg_thresh1: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit0 = arg_blimit0;
    _ = &blimit0;
    var limit0 = arg_limit0;
    _ = &limit0;
    var thresh0 = arg_thresh0;
    _ = &thresh0;
    var blimit1 = arg_blimit1;
    _ = &blimit1;
    var limit1 = arg_limit1;
    _ = &limit1;
    var thresh1 = arg_thresh1;
    _ = &thresh1;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_vertical_8_c(s, pitch, blimit0, limit0, thresh0, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vpx_highbd_lpf_vertical_8_c(s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), pitch, blimit1, limit1, thresh1, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_filter16(arg_mask: i8, arg_thresh: u8, arg_flat: i8, arg_flat2: i8, arg_op7: [*c]u16, arg_op6: [*c]u16, arg_op5: [*c]u16, arg_op4: [*c]u16, arg_op3: [*c]u16, arg_op2: [*c]u16, arg_op1: [*c]u16, arg_op0: [*c]u16, arg_oq0: [*c]u16, arg_oq1: [*c]u16, arg_oq2: [*c]u16, arg_oq3: [*c]u16, arg_oq4: [*c]u16, arg_oq5: [*c]u16, arg_oq6: [*c]u16, arg_oq7: [*c]u16, arg_bd: c_int) callconv(.c) void {
    var mask = arg_mask;
    _ = &mask;
    var thresh = arg_thresh;
    _ = &thresh;
    var flat = arg_flat;
    _ = &flat;
    var flat2 = arg_flat2;
    _ = &flat2;
    var op7 = arg_op7;
    _ = &op7;
    var op6 = arg_op6;
    _ = &op6;
    var op5 = arg_op5;
    _ = &op5;
    var op4 = arg_op4;
    _ = &op4;
    var op3 = arg_op3;
    _ = &op3;
    var op2 = arg_op2;
    _ = &op2;
    var op1 = arg_op1;
    _ = &op1;
    var op0 = arg_op0;
    _ = &op0;
    var oq0 = arg_oq0;
    _ = &oq0;
    var oq1 = arg_oq1;
    _ = &oq1;
    var oq2 = arg_oq2;
    _ = &oq2;
    var oq3 = arg_oq3;
    _ = &oq3;
    var oq4 = arg_oq4;
    _ = &oq4;
    var oq5 = arg_oq5;
    _ = &oq5;
    var oq6 = arg_oq6;
    _ = &oq6;
    var oq7 = arg_oq7;
    _ = &oq7;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (((@as(c_int, flat2) != 0) and (@as(c_int, flat) != 0)) and (@as(c_int, mask) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p7: u16 = op7.*;
        _ = &p7;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p6: u16 = op6.*;
        _ = &p6;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p5: u16 = op5.*;
        _ = &p5;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p4: u16 = op4.*;
        _ = &p4;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p3: u16 = op3.*;
        _ = &p3;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p2: u16 = op2.*;
        _ = &p2;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p1: u16 = op1.*;
        _ = &p1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const p0: u16 = op0.*;
        _ = &p0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q0: u16 = oq0.*;
        _ = &q0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q1: u16 = oq1.*;
        _ = &q1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q2: u16 = oq2.*;
        _ = &q2;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q3: u16 = oq3.*;
        _ = &q3;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q4: u16 = oq4.*;
        _ = &q4;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q5: u16 = oq5.*;
        _ = &q5;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q6: u16 = oq6.*;
        _ = &q6;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const q7: u16 = oq7.*;
        _ = &q7;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op6.* = @bitCast(@as(c_short, @truncate(((((((((((@as(c_int, p7) * @as(c_int, 7)) + (@as(c_int, p6) * @as(c_int, 2))) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op5.* = @bitCast(@as(c_short, @truncate((((((((((((@as(c_int, p7) * @as(c_int, 6)) + @as(c_int, p6)) + (@as(c_int, p5) * @as(c_int, 2))) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op4.* = @bitCast(@as(c_short, @truncate(((((((((((((@as(c_int, p7) * @as(c_int, 5)) + @as(c_int, p6)) + @as(c_int, p5)) + (@as(c_int, p4) * @as(c_int, 2))) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op3.* = @bitCast(@as(c_short, @truncate((((((((((((((@as(c_int, p7) * @as(c_int, 4)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + (@as(c_int, p3) * @as(c_int, 2))) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op2.* = @bitCast(@as(c_short, @truncate(((((((((((((((@as(c_int, p7) * @as(c_int, 3)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + (@as(c_int, p2) * @as(c_int, 2))) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op1.* = @bitCast(@as(c_short, @truncate((((((((((((((((@as(c_int, p7) * @as(c_int, 2)) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + (@as(c_int, p1) * @as(c_int, 2))) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        op0.* = @bitCast(@as(c_short, @truncate((((((((((((((((@as(c_int, p7) + @as(c_int, p6)) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + (@as(c_int, p0) * @as(c_int, 2))) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq0.* = @bitCast(@as(c_short, @truncate((((((((((((((((@as(c_int, p6) + @as(c_int, p5)) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + (@as(c_int, q0) * @as(c_int, 2))) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + @as(c_int, q7)) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq1.* = @bitCast(@as(c_short, @truncate(((((((((((((((@as(c_int, p5) + @as(c_int, p4)) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + (@as(c_int, q1) * @as(c_int, 2))) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 2))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq2.* = @bitCast(@as(c_short, @truncate((((((((((((((@as(c_int, p4) + @as(c_int, p3)) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + (@as(c_int, q2) * @as(c_int, 2))) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 3))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq3.* = @bitCast(@as(c_short, @truncate(((((((((((((@as(c_int, p3) + @as(c_int, p2)) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + (@as(c_int, q3) * @as(c_int, 2))) + @as(c_int, q4)) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 4))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq4.* = @bitCast(@as(c_short, @truncate((((((((((((@as(c_int, p2) + @as(c_int, p1)) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + (@as(c_int, q4) * @as(c_int, 2))) + @as(c_int, q5)) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 5))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq5.* = @bitCast(@as(c_short, @truncate(((((((((((@as(c_int, p1) + @as(c_int, p0)) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + (@as(c_int, q5) * @as(c_int, 2))) + @as(c_int, q6)) + (@as(c_int, q7) * @as(c_int, 6))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        oq6.* = @bitCast(@as(c_short, @truncate((((((((((@as(c_int, p0) + @as(c_int, q0)) + @as(c_int, q1)) + @as(c_int, q2)) + @as(c_int, q3)) + @as(c_int, q4)) + @as(c_int, q5)) + (@as(c_int, q6) * @as(c_int, 2))) + (@as(c_int, q7) * @as(c_int, 7))) + (@as(c_int, 1) << @intCast(@as(c_int, 4) - @as(c_int, 1)))) >> @intCast(@as(c_int, 4)))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        highbd_filter8(mask, thresh, flat, op3, op2, op1, op0, oq0, oq1, oq2, oq3, bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_mb_lpf_horizontal_edge_w(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_count: c_int, arg_bd: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var count = arg_count;
    _ = &count;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < (@as(c_int, 8) * count)) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4) * pitch)))];
            _ = &p3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3) * pitch)))];
            _ = &p2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2) * pitch)))];
            _ = &p1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-pitch)))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 0) * pitch)))];
            _ = &q0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q1: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))];
            _ = &q1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q2: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))];
            _ = &q2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q3: u16 = s[@bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = highbd_flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat2: i8 = highbd_flat_mask5(1, s[@bitCast(@as(isize, @intCast(-@as(c_int, 8) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 7) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 6) * pitch)))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 5) * pitch)))], p0, q0, s[@bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))], s[@bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))], bd);
            _ = &flat2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter16(mask, thresh.*, flat, flat2, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6) * pitch)))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7) * pitch)))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += 1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_16_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_mb_lpf_horizontal_edge_w(s, pitch, blimit, limit, thresh, 1, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_horizontal_16_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_mb_lpf_horizontal_edge_w(s, pitch, blimit, limit, thresh, 2, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn highbd_mb_lpf_vertical_edge_w(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_count: c_int, arg_bd: c_int) callconv(.c) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var count = arg_count;
    _ = &count;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    {
        i = 0;
        while (i < count) : (i += 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p3: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 4))))];
            _ = &p3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p2: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 3))))];
            _ = &p2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p1: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 2))))];
            _ = &p1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const p0: u16 = s[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
            _ = &p0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q0: u16 = s[@as(c_int, 0)];
            _ = &q0;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q1: u16 = s[@as(c_int, 1)];
            _ = &q1;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q2: u16 = s[@as(c_int, 2)];
            _ = &q2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const q3: u16 = s[@as(c_int, 3)];
            _ = &q3;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const mask: i8 = highbd_filter_mask(limit.*, blimit.*, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &mask;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat: i8 = highbd_flat_mask4(1, p3, p2, p1, p0, q0, q1, q2, q3, bd);
            _ = &flat;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            const flat2: i8 = highbd_flat_mask5(1, s[@bitCast(@as(isize, @intCast(-@as(c_int, 8))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 7))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 6))))], s[@bitCast(@as(isize, @intCast(-@as(c_int, 5))))], p0, q0, s[@as(c_int, 4)], s[@as(c_int, 5)], s[@as(c_int, 6)], s[@as(c_int, 7)], bd);
            _ = &flat2;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            highbd_filter16(mask, thresh.*, flat, flat2, s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 8))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s, s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 2))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 3))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 4))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 5))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 6))))), s + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 7))))), bd);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            s += @as(usize, @bitCast(@as(isize, @intCast(pitch))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_16_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_mb_lpf_vertical_edge_w(s, pitch, blimit, limit, thresh, 8, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vpx_highbd_lpf_vertical_16_dual_c(arg_s: [*c]u16, arg_pitch: c_int, arg_blimit: [*c]const u8, arg_limit: [*c]const u8, arg_thresh: [*c]const u8, arg_bd: c_int) void {
    var s = arg_s;
    _ = &s;
    var pitch = arg_pitch;
    _ = &pitch;
    var blimit = arg_blimit;
    _ = &blimit;
    var limit = arg_limit;
    _ = &limit;
    var thresh = arg_thresh;
    _ = &thresh;
    var bd = arg_bd;
    _ = &bd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    highbd_mb_lpf_vertical_edge_w(s, pitch, blimit, limit, thresh, 16, bd);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
