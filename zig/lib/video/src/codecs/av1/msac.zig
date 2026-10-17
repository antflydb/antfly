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
//  * Copyright © 2019, VideoLAN and dav1d authors
//  * Copyright © 2019, Two Orioles, LLC
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
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub fn inv_recenter(r: c_uint, v: c_uint) callconv(.c) c_uint {
    _ = &r;
    _ = &v;
    if (v > (r << @intCast(@as(c_uint, 1)))) return v else if ((v & @as(c_uint, 1)) == @as(c_uint, 0)) return (v >> @intCast(@as(c_uint, 1))) +% r else return r -% ((v +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1)));
}
pub const ec_win = common.ec_win;
pub const struct_MsacContext = extern struct {
    buf_pos: [*c]const u8 = null,
    buf_end: [*c]const u8 = null,
    dif: ec_win = 0,
    rng: c_uint = 0,
    cnt: c_int = 0,
    allow_update_cdf: c_int = 0,
};
pub const MsacContext = struct_MsacContext;
pub fn dav1d_msac_decode_bools(s: [*c]MsacContext, arg_n: c_uint) callconv(.c) c_uint {
    _ = &s;
    var n = arg_n;
    _ = &n;
    var v: c_uint = 0;
    _ = &v;
    while ((blk: {
        const ref = &n;
        const tmp = ref.*;
        ref.* -%= 1;
        break :blk tmp;
    }) != 0) {
        v = (v << @intCast(@as(c_uint, 1))) | antfly_av1_dav1d_msac_decode_bool_equi_c(s);
    }
    return v;
}
pub fn ctx_refill(s: [*c]MsacContext) callconv(.c) void {
    _ = &s;
    var buf_pos: [*c]const u8 = s.*.buf_pos;
    _ = &buf_pos;
    var buf_end: [*c]const u8 = s.*.buf_end;
    _ = &buf_end;
    var c: c_int = @bitCast(@as(c_uint, @truncate(((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, @bitCast(@as(c_long, s.*.cnt)))) -% @as(c_ulong, 24))));
    _ = &c;
    var dif: ec_win = s.*.dif;
    _ = &dif;
    while (true) {
        if (buf_pos >= buf_end) {
            dif |= ~(~@as(ec_win, @bitCast(@as(c_long, @as(c_int, 255)))) << @intCast(@as(ec_win, @bitCast(@as(c_long, c)))));
            break;
        }
        dif |= @as(ec_win, @bitCast(@as(c_long, @as(c_int, (blk: {
            const ref = &buf_pos;
            const tmp = ref.*;
            ref.* += 1;
            break :blk tmp;
        }).*) ^ @as(c_int, 255)))) << @intCast(@as(ec_win, @bitCast(@as(c_long, c))));
        c -= 8;
        if (!(c >= @as(c_int, 0))) break;
    }
    s.*.dif = dif;
    s.*.cnt = @bitCast(@as(c_uint, @truncate(((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, @bitCast(@as(c_long, c)))) -% @as(c_ulong, 24))));
    s.*.buf_pos = buf_pos;
}
pub export fn antfly_av1_dav1d_msac_decode_subexp(s: [*c]MsacContext, ref: c_int, n: c_int, arg_k: c_uint) c_int {
    _ = &s;
    _ = &ref;
    _ = &n;
    var k = arg_k;
    _ = &k;
    while (true) {
        if (!((@as(c_uint, @bitCast(@as(c_int, n))) >> @intCast(k)) == @as(c_uint, 8))) {
            unreachable;
        }
        if (!false) break;
    }
    var a: c_uint = 0;
    _ = &a;
    if (antfly_av1_dav1d_msac_decode_bool_equi_c(s) != 0) {
        if (antfly_av1_dav1d_msac_decode_bool_equi_c(s) != 0) {
            k +%= antfly_av1_dav1d_msac_decode_bool_equi_c(s) +% @as(c_uint, 1);
        }
        a = @as(c_uint, 1) << @intCast(k);
    }
    const v: c_uint = dav1d_msac_decode_bools(s, k) +% a;
    _ = &v;
    return @bitCast(@as(c_uint, @truncate(if ((ref * @as(c_int, 2)) <= n) inv_recenter(@bitCast(@as(c_int, ref)), v) else @as(c_uint, @bitCast(@as(c_int, n - @as(c_int, 1)))) -% inv_recenter(@bitCast(@as(c_int, (n - @as(c_int, 1)) - ref)), v))));
}
pub fn ctx_norm(s: [*c]MsacContext, dif: ec_win, rng: c_uint) callconv(.c) void {
    _ = &s;
    _ = &dif;
    _ = &rng;
    const d: c_int = @as(c_int, 15) ^ (@as(c_int, 31) ^ clz(rng));
    _ = &d;
    const cnt: c_int = s.*.cnt;
    _ = &cnt;
    while (true) {
        if (!(rng <= @as(c_uint, 65535))) {
            unreachable;
        }
        if (!false) break;
    }
    s.*.dif = dif << @intCast(@as(ec_win, @bitCast(@as(c_long, d))));
    s.*.rng = rng << @intCast(@as(c_uint, @bitCast(@as(c_int, d))));
    s.*.cnt = cnt - d;
    if (@as(c_uint, @bitCast(@as(c_int, cnt))) < @as(c_uint, @bitCast(@as(c_int, d)))) {
        ctx_refill(s);
    }
}
pub export fn antfly_av1_dav1d_msac_decode_bool_equi_c(s: [*c]MsacContext) c_uint {
    _ = &s;
    const r: c_uint = s.*.rng;
    _ = &r;
    var dif: ec_win = s.*.dif;
    _ = &dif;
    while (true) {
        if (!((dif >> @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16))) < @as(ec_win, r))) {
            unreachable;
        }
        if (!false) break;
    }
    var v: c_uint = ((r >> @intCast(@as(c_uint, 8))) << @intCast(@as(c_uint, 7))) +% @as(c_uint, 4);
    _ = &v;
    const vw: ec_win = @as(ec_win, v) << @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16));
    _ = &vw;
    const ret: c_uint = @bitCast(@as(c_int, @as(c_int, @intFromBool(dif >= vw))));
    _ = &ret;
    dif -%= @as(ec_win, ret) *% vw;
    v +%= ret *% (r -% (@as(c_uint, 2) *% v));
    ctx_norm(s, dif, v);
    return @bitCast(@as(c_int, @as(c_int, @intFromBool(!(ret != 0)))));
}
pub export fn antfly_av1_dav1d_msac_decode_bool_c(s: [*c]MsacContext, f: c_uint) c_uint {
    _ = &s;
    _ = &f;
    const r: c_uint = s.*.rng;
    _ = &r;
    var dif: ec_win = s.*.dif;
    _ = &dif;
    while (true) {
        if (!((dif >> @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16))) < @as(ec_win, r))) {
            unreachable;
        }
        if (!false) break;
    }
    var v: c_uint = (((r >> @intCast(@as(c_uint, 8))) *% (f >> @intCast(@as(c_uint, 6)))) >> @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 7) - @as(c_int, 6)))))) +% @as(c_uint, 4);
    _ = &v;
    const vw: ec_win = @as(ec_win, v) << @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16));
    _ = &vw;
    const ret: c_uint = @bitCast(@as(c_int, @as(c_int, @intFromBool(dif >= vw))));
    _ = &ret;
    dif -%= @as(ec_win, ret) *% vw;
    v +%= ret *% (r -% (@as(c_uint, 2) *% v));
    ctx_norm(s, dif, v);
    return @bitCast(@as(c_int, @as(c_int, @intFromBool(!(ret != 0)))));
}
pub export fn antfly_av1_dav1d_msac_decode_symbol_adapt_c(s: [*c]MsacContext, cdf: [*c]u16, n_symbols: usize) c_uint {
    _ = &s;
    _ = &cdf;
    _ = &n_symbols;
    const c: c_uint = @truncate(s.*.dif >> @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16)));
    _ = &c;
    const r: c_uint = s.*.rng >> @intCast(@as(c_uint, 8));
    _ = &r;
    var u: c_uint = undefined;
    _ = &u;
    var v: c_uint = s.*.rng;
    _ = &v;
    var val: c_uint = @bitCast(@as(c_int, -@as(c_int, 1)));
    _ = &val;
    while (true) {
        if (!(n_symbols <= @as(usize, 15))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(@as(c_int, cdf[n_symbols]) <= @as(c_int, 32))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        val +%= 1;
        u = v;
        v = r *% @as(c_uint, @bitCast(@as(c_int, @as(c_int, cdf[val]) >> @intCast(@as(c_int, 6)))));
        v >>= @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 7) - @as(c_int, 6)))));
        v +%= @as(c_uint, 4) *% (@as(c_uint, @truncate(n_symbols)) -% val);
        if (!(c < v)) break;
    }
    while (true) {
        if (!(u <= s.*.rng)) {
            unreachable;
        }
        if (!false) break;
    }
    ctx_norm(s, s.*.dif -% (@as(ec_win, v) << @intCast((@sizeOf(ec_win) << @intCast(@as(c_ulong, 3))) -% @as(c_ulong, 16))), u -% v);
    if (s.*.allow_update_cdf != 0) {
        const count: c_uint = cdf[n_symbols];
        _ = &count;
        const rate: c_uint = (@as(c_uint, 4) +% (count >> @intCast(@as(c_uint, 4)))) +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(n_symbols > @as(usize, 2))))));
        _ = &rate;
        var i: c_uint = undefined;
        _ = &i;
        {
            i = 0;
            while (i < val) : (i +%= 1) {
                {
                    const ref = &cdf[i];
                    ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) +% (@as(c_uint, @bitCast(@as(c_int, @as(c_int, 32768) - @as(c_int, cdf[i])))) >> @intCast(rate)));
                }
            }
        }
        while (@as(usize, i) < n_symbols) : (i +%= 1) {
            {
                const ref = &cdf[i];
                ref.* = @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, ref.*)))) -% (@as(c_uint, @bitCast(@as(c_int, @as(c_int, cdf[i])))) >> @intCast(rate)));
            }
        }
        cdf[n_symbols] = @truncate(count +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(count < @as(c_uint, 32)))))));
    }
    return val;
}
pub export fn antfly_av1_dav1d_msac_decode_bool_adapt_c(s: [*c]MsacContext, cdf: [*c]u16) c_uint {
    _ = &s;
    _ = &cdf;
    const bit: c_uint = antfly_av1_dav1d_msac_decode_bool_c(s, cdf.*);
    _ = &bit;
    if (s.*.allow_update_cdf != 0) {
        const count: c_uint = cdf[@as(c_int, 1)];
        _ = &count;
        const rate: c_int = @bitCast(@as(c_uint, @truncate(@as(c_uint, 4) +% (count >> @intCast(@as(c_uint, 4))))));
        _ = &rate;
        if (bit != 0) {
            {
                const ref = &cdf[@as(c_int, 0)];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) + ((@as(c_int, 32768) - @as(c_int, cdf[@as(c_int, 0)])) >> @intCast(rate)))));
            }
        } else {
            {
                const ref = &cdf[@as(c_int, 0)];
                ref.* = @bitCast(@as(c_short, @truncate(@as(c_int, ref.*) - (@as(c_int, cdf[@as(c_int, 0)]) >> @intCast(rate)))));
            }
        }
        cdf[@as(c_int, 1)] = @truncate(count +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(count < @as(c_uint, 32)))))));
    }
    return bit;
}
pub export fn antfly_av1_dav1d_msac_decode_hi_tok_c(s: [*c]MsacContext, cdf: [*c]u16) c_uint {
    _ = &s;
    _ = &cdf;
    var tok_br: c_uint = antfly_av1_dav1d_msac_decode_symbol_adapt_c(s, cdf, 3);
    _ = &tok_br;
    var tok: c_uint = @as(c_uint, 3) +% tok_br;
    _ = &tok;
    if (tok_br == @as(c_uint, 3)) {
        tok_br = antfly_av1_dav1d_msac_decode_symbol_adapt_c(s, cdf, 3);
        tok = @as(c_uint, 6) +% tok_br;
        if (tok_br == @as(c_uint, 3)) {
            tok_br = antfly_av1_dav1d_msac_decode_symbol_adapt_c(s, cdf, 3);
            tok = @as(c_uint, 9) +% tok_br;
            if (tok_br == @as(c_uint, 3)) {
                tok = @as(c_uint, 12) +% antfly_av1_dav1d_msac_decode_symbol_adapt_c(s, cdf, 3);
            }
        }
    }
    return tok;
}
pub export fn antfly_av1_dav1d_msac_init(s: [*c]MsacContext, data: [*c]const u8, sz: usize, disable_cdf_update_flag: c_int) void {
    _ = &s;
    _ = &data;
    _ = &sz;
    _ = &disable_cdf_update_flag;
    s.*.buf_pos = data;
    s.*.buf_end = data + sz;
    s.*.dif = 0;
    s.*.rng = 32768;
    s.*.cnt = -@as(c_int, 15);
    s.*.allow_update_cdf = @as(c_int, @intFromBool(!(disable_cdf_update_flag != 0)));
    ctx_refill(s);
}
