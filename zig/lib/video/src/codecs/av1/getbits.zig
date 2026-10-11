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
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub fn ulog2(v: c_uint) callconv(.c) c_int {
    _ = &v;
    return @as(c_int, 31) ^ clz(v);
}
pub fn inv_recenter(r: c_uint, v: c_uint) callconv(.c) c_uint {
    _ = &r;
    _ = &v;
    if (v > (r << @intCast(@as(c_uint, 1)))) return v else if ((v & @as(c_uint, 1)) == @as(c_uint, 0)) return (v >> @intCast(@as(c_uint, 1))) +% r else return r -% ((v +% @as(c_uint, 1)) >> @intCast(@as(c_uint, 1)));
}
pub const struct_GetBits = extern struct {
    state: u64 = 0,
    bits_left: c_int = 0,
    @"error": c_int = 0,
    ptr: [*c]const u8 = null,
    ptr_start: [*c]const u8 = null,
    ptr_end: [*c]const u8 = null,
};
pub const GetBits = struct_GetBits;
pub export fn antfly_av1_dav1d_init_get_bits(c: [*c]GetBits, data: [*c]const u8, sz: usize) void {
    _ = &c;
    _ = &data;
    _ = &sz;
    while (true) {
        if (!(sz != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    c.*.ptr = blk: {
        const tmp = data;
        c.*.ptr_start = tmp;
        break :blk tmp;
    };
    c.*.ptr_end = &c.*.ptr_start[sz];
    c.*.state = 0;
    c.*.bits_left = 0;
    c.*.@"error" = 0;
}
pub export fn antfly_av1_dav1d_get_bit(c: [*c]GetBits) c_uint {
    _ = &c;
    if (!(c.*.bits_left != 0)) {
        if (c.*.ptr >= c.*.ptr_end) {
            c.*.@"error" = 1;
        } else {
            const state: c_uint = (blk: {
                const ref = &c.*.ptr;
                const tmp = ref.*;
                ref.* += 1;
                break :blk tmp;
            }).*;
            _ = &state;
            c.*.bits_left = 7;
            c.*.state = @as(u64, state) << @intCast(@as(u64, 57));
            return state >> @intCast(@as(c_uint, 7));
        }
    }
    const state: u64 = c.*.state;
    _ = &state;
    c.*.bits_left -= 1;
    c.*.state = state << @intCast(@as(u64, 1));
    return @truncate(state >> @intCast(@as(u64, 63)));
}
pub fn refill(c: [*c]GetBits, n: c_int) callconv(.c) void {
    _ = &c;
    _ = &n;
    while (true) {
        if (!((c.*.bits_left >= @as(c_int, 0)) and (c.*.bits_left < @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    var state: c_uint = 0;
    _ = &state;
    while (true) {
        if (c.*.ptr >= c.*.ptr_end) {
            c.*.@"error" = 1;
            if (state != 0) break;
            return;
        }
        state = (state << @intCast(@as(c_uint, 8))) | @as(c_uint, @bitCast(@as(c_int, @as(c_int, (blk: {
            const ref = &c.*.ptr;
            const tmp = ref.*;
            ref.* += 1;
            break :blk tmp;
        }).*))));
        c.*.bits_left += 8;
        if (!(n > c.*.bits_left)) break;
    }
    c.*.state |= @as(u64, state) << @intCast(@as(u64, @bitCast(@as(c_longlong, @as(c_int, 64) - c.*.bits_left))));
}
pub export fn antfly_av1_dav1d_get_bits(c: [*c]GetBits, n: c_int) c_uint {
    _ = &c;
    _ = &n;
    while (true) {
        if (!((n > @as(c_int, 0)) and (n <= @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (@as(c_uint, @bitCast(@as(c_int, n))) > @as(c_uint, @bitCast(@as(c_int, c.*.bits_left)))) {
        refill(c, n);
    }
    const state: u64 = c.*.state;
    _ = &state;
    c.*.bits_left -= n;
    c.*.state = state << @intCast(@as(u64, @bitCast(@as(c_longlong, n))));
    return @truncate(state >> @intCast(@as(u64, @bitCast(@as(c_longlong, @as(c_int, 64) - n)))));
}
pub export fn antfly_av1_dav1d_get_sbits(c: [*c]GetBits, n: c_int) c_int {
    _ = &c;
    _ = &n;
    while (true) {
        if (!((n > @as(c_int, 0)) and (n <= @as(c_int, 32)))) {
            unreachable;
        }
        if (!false) break;
    }
    if (@as(c_uint, @bitCast(@as(c_int, n))) > @as(c_uint, @bitCast(@as(c_int, c.*.bits_left)))) {
        refill(c, n);
    }
    const state: u64 = c.*.state;
    _ = &state;
    c.*.bits_left -= n;
    c.*.state = state << @intCast(@as(u64, @bitCast(@as(c_longlong, n))));
    return @truncate(@as(i64, @bitCast(@as(c_ulonglong, @truncate(state)))) >> @intCast(@as(i64, @as(c_int, 64) - n)));
}
pub export fn antfly_av1_dav1d_get_uleb128(c: [*c]GetBits) c_uint {
    _ = &c;
    var val: u64 = 0;
    _ = &val;
    var i: c_uint = 0;
    _ = &i;
    var more: c_uint = undefined;
    _ = &more;
    while (true) {
        const v: c_int = @bitCast(@as(c_uint, @truncate(antfly_av1_dav1d_get_bits(c, 8))));
        _ = &v;
        more = @bitCast(@as(c_int, v & @as(c_int, 128)));
        val |= @as(u64, @bitCast(@as(c_longlong, v & @as(c_int, 127)))) << @intCast(@as(u64, i));
        i +%= 7;
        if (!((more != 0) and (@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(i < @as(c_uint, 56)))))) != 0))) break;
    }
    if ((@as(c_uint, @bitCast(@as(c_int, @as(c_int, @intFromBool(val > @as(u64, (@as(c_uint, 2147483647) *% @as(c_uint, 2)) +% @as(c_uint, 1))))))) != 0) or (more != 0)) {
        c.*.@"error" = 1;
        return 0;
    }
    return @truncate(val);
}
pub export fn antfly_av1_dav1d_get_uniform(c: [*c]GetBits, max: c_uint) c_uint {
    _ = &c;
    _ = &max;
    while (true) {
        if (!(max > @as(c_uint, 1))) {
            unreachable;
        }
        if (!false) break;
    }
    const l: c_int = ulog2(max) + @as(c_int, 1);
    _ = &l;
    while (true) {
        if (!(l > @as(c_int, 1))) {
            unreachable;
        }
        if (!false) break;
    }
    const m: c_uint = (@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, l))))) -% max;
    _ = &m;
    const v: c_uint = antfly_av1_dav1d_get_bits(c, l - @as(c_int, 1));
    _ = &v;
    return if (v < m) v else ((v << @intCast(@as(c_uint, 1))) -% m) +% antfly_av1_dav1d_get_bit(c);
}
pub export fn antfly_av1_dav1d_get_vlc(c: [*c]GetBits) c_uint {
    _ = &c;
    if (antfly_av1_dav1d_get_bit(c) != 0) return 0;
    var n_bits: c_int = 0;
    _ = &n_bits;
    while (true) {
        if ((blk: {
            const ref = &n_bits;
            ref.* += 1;
            break :blk ref.*;
        }) == @as(c_int, 32)) return 4294967295;
        if (!!(antfly_av1_dav1d_get_bit(c) != 0)) break;
    }
    return ((@as(c_uint, 1) << @intCast(@as(c_uint, @bitCast(@as(c_int, n_bits))))) -% @as(c_uint, 1)) +% antfly_av1_dav1d_get_bits(c, n_bits);
}
pub fn get_bits_subexp_u(c: [*c]GetBits, ref: c_uint, n: c_uint) callconv(.c) c_uint {
    _ = &c;
    _ = &ref;
    _ = &n;
    var v: c_uint = 0;
    _ = &v;
    {
        var i: c_int = 0;
        _ = &i;
        while (true) : (i += 1) {
            const b: c_int = if (i != 0) (@as(c_int, 3) + i) - @as(c_int, 1) else @as(c_int, 3);
            _ = &b;
            if (n < (v +% @as(c_uint, @bitCast(@as(c_int, @as(c_int, 3) * (@as(c_int, 1) << @intCast(b))))))) {
                v +%= antfly_av1_dav1d_get_uniform(c, (n -% v) +% @as(c_uint, 1));
                break;
            }
            if (!(antfly_av1_dav1d_get_bit(c) != 0)) {
                v +%= antfly_av1_dav1d_get_bits(c, b);
                break;
            }
            v +%= @bitCast(@as(c_int, @as(c_int, 1) << @intCast(b)));
        }
    }
    return if ((ref *% @as(c_uint, 2)) <= n) inv_recenter(ref, v) else n -% inv_recenter(n -% ref, v);
}
pub export fn antfly_av1_dav1d_get_bits_subexp(c: [*c]GetBits, ref: c_int, n: c_uint) c_int {
    _ = &c;
    _ = &ref;
    _ = &n;
    return @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, @bitCast(@as(c_uint, @truncate(get_bits_subexp_u(c, @as(c_uint, @bitCast(@as(c_int, ref))) +% (@as(c_uint, 1) << @intCast(n)), @as(c_uint, 2) << @intCast(n))))))))) -% (@as(c_uint, 1) << @intCast(n)))));
}
