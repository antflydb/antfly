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
//   Copyright (c) 2010 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub const vpx_prob = common.vpx_prob;
pub extern const antfly_vp9_vpx_norm: [256]u8 align(16);
pub const vpx_decrypt_cb = common.vpx_decrypt_cb;
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
pub extern fn antfly_vp9_vpx_reader_fill(r: [*c]vpx_reader) void;
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
pub fn vpx_read_literal(arg_r: [*c]vpx_reader, arg_bits: c_int) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    var bits = arg_bits;
    _ = &bits;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var literal: c_int = 0;
    _ = &literal;
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
            literal |= vpx_read_bit(r) << @intCast(bit);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
    }
    return literal;
}
pub fn inv_recenter_nonneg(arg_v: c_int, arg_m: c_int) callconv(.c) c_int {
    var v = arg_v;
    _ = &v;
    var m = arg_m;
    _ = &m;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (v > (@as(c_int, 2) * m)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return v;
    }
    return if ((v & @as(c_int, 1)) != 0) m - ((v + @as(c_int, 1)) >> @intCast(@as(c_int, 1))) else m + (v >> @intCast(@as(c_int, 1)));
}
pub fn decode_uniform(arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const l: c_int = 8;
    _ = &l;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const m: c_int = (@as(c_int, 1) << @intCast(l)) - @as(c_int, 191);
    _ = &m;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const v: c_int = vpx_read_literal(r, l - @as(c_int, 1));
    _ = &v;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return if (v < m) v else ((v << @intCast(@as(c_int, 1))) - m) + vpx_read_bit(r);
}
pub fn inv_remap_prob(arg_v: c_int, arg_m: c_int) callconv(.c) c_int {
    var v = arg_v;
    _ = &v;
    var m = arg_m;
    _ = &m;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const static_local_inv_map_table = struct {
        var inv_map_table: [255]u8 = [255]u8{
            7,
            20,
            33,
            46,
            59,
            72,
            85,
            98,
            111,
            124,
            137,
            150,
            163,
            176,
            189,
            202,
            215,
            228,
            241,
            254,
            1,
            2,
            3,
            4,
            5,
            6,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            32,
            34,
            35,
            36,
            37,
            38,
            39,
            40,
            41,
            42,
            43,
            44,
            45,
            47,
            48,
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
            70,
            71,
            73,
            74,
            75,
            76,
            77,
            78,
            79,
            80,
            81,
            82,
            83,
            84,
            86,
            87,
            88,
            89,
            90,
            91,
            92,
            93,
            94,
            95,
            96,
            97,
            99,
            100,
            101,
            102,
            103,
            104,
            105,
            106,
            107,
            108,
            109,
            110,
            112,
            113,
            114,
            115,
            116,
            117,
            118,
            119,
            120,
            121,
            122,
            123,
            125,
            126,
            127,
            128,
            129,
            130,
            131,
            132,
            133,
            134,
            135,
            136,
            138,
            139,
            140,
            141,
            142,
            143,
            144,
            145,
            146,
            147,
            148,
            149,
            151,
            152,
            153,
            154,
            155,
            156,
            157,
            158,
            159,
            160,
            161,
            162,
            164,
            165,
            166,
            167,
            168,
            169,
            170,
            171,
            172,
            173,
            174,
            175,
            177,
            178,
            179,
            180,
            181,
            182,
            183,
            184,
            185,
            186,
            187,
            188,
            190,
            191,
            192,
            193,
            194,
            195,
            196,
            197,
            198,
            199,
            200,
            201,
            203,
            204,
            205,
            206,
            207,
            208,
            209,
            210,
            211,
            212,
            213,
            214,
            216,
            217,
            218,
            219,
            220,
            221,
            222,
            223,
            224,
            225,
            226,
            227,
            229,
            230,
            231,
            232,
            233,
            234,
            235,
            236,
            237,
            238,
            239,
            240,
            242,
            243,
            244,
            245,
            246,
            247,
            248,
            249,
            250,
            251,
            252,
            253,
            253,
        };
    };
    _ = &static_local_inv_map_table;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    v = static_local_inv_map_table.inv_map_table[@bitCast(@as(isize, @intCast(v)))];
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    m -= 1;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if ((m << @intCast(@as(c_int, 1))) <= @as(c_int, 255)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @as(c_int, 1) + inv_recenter_nonneg(v, m);
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return @as(c_int, 255) - inv_recenter_nonneg(v, (@as(c_int, 255) - @as(c_int, 1)) - m);
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return undefined;
}
pub fn decode_term_subexp(arg_r: [*c]vpx_reader) callconv(.c) c_int {
    var r = arg_r;
    _ = &r;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (!(vpx_read_bit(r) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return vpx_read_literal(r, 4);
    }
    if (!(vpx_read_bit(r) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return vpx_read_literal(r, 4) + @as(c_int, 16);
    }
    if (!(vpx_read_bit(r) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return vpx_read_literal(r, 5) + @as(c_int, 32);
    }
    return decode_uniform(r) + @as(c_int, 64);
}
pub export fn antfly_vp9_vp9_diff_update_prob(arg_r: [*c]vpx_reader, arg_p: [*c]vpx_prob) void {
    var r = arg_r;
    _ = &r;
    var p = arg_p;
    _ = &p;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (vpx_read(r, 252) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        const delp: c_int = decode_term_subexp(r);
        _ = &delp;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        p.* = @bitCast(@as(i8, @truncate(inv_remap_prob(delp, p.*))));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
