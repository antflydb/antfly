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
//  * Copyright © 2018, Niklas Haas
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
pub const ptrdiff_t = common.ptrdiff_t;
pub fn clz(mask: c_uint) callconv(.c) c_int {
    _ = &mask;
    return __builtin.clz(mask);
}

pub fn imin(a: c_int, b: c_int) callconv(.c) c_int {
    _ = &a;
    _ = &b;
    return if (a < b) a else b;
}
pub fn iclip(v: c_int, min: c_int, max: c_int) callconv(.c) c_int {
    _ = &v;
    _ = &min;
    _ = &max;
    return if (v < min) min else if (v > max) max else v;
}
pub const pixel = u16;
pub fn PXSTRIDE(x: ptrdiff_t) callconv(.c) ptrdiff_t {
    _ = &x;
    while (true) {
        if (!!((x & @as(ptrdiff_t, 1)) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    return x >> @intCast(@as(ptrdiff_t, 1));
}
pub const DAV1D_PIXEL_LAYOUT_I420 = common.DAV1D_PIXEL_LAYOUT_I420;
pub const DAV1D_PIXEL_LAYOUT_I422 = common.DAV1D_PIXEL_LAYOUT_I422;
pub const DAV1D_PIXEL_LAYOUT_I444 = common.DAV1D_PIXEL_LAYOUT_I444;
pub const Dav1dFilmGrainData = common.Dav1dFilmGrainData;
pub const entry = i16;
pub const generate_grain_y_fn = ?*const fn (buf: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, bitdepth_max: c_int) callconv(.c) void;
pub const generate_grain_uv_fn = ?*const fn (buf: [*c][82]entry, buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, bitdepth_max: c_int) callconv(.c) void;
pub const fgy_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const fguv_32x32xn_fn = ?*const fn (dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, scaling: [*c]const u8, grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, luma_row: [*c]const pixel, luma_stride: ptrdiff_t, uv_pl: c_int, is_id: c_int, bitdepth_max: c_int) callconv(.c) void;
pub const struct_Dav1dFilmGrainDSPContext = extern struct {
    generate_grain_y: generate_grain_y_fn = null,
    generate_grain_uv: [3]generate_grain_uv_fn = @import("std").mem.zeroes([3]generate_grain_uv_fn),
    fgy_32x32xn: fgy_32x32xn_fn = null,
    fguv_32x32xn: [3]fguv_32x32xn_fn = @import("std").mem.zeroes([3]fguv_32x32xn_fn),
};
pub const Dav1dFilmGrainDSPContext = struct_Dav1dFilmGrainDSPContext;
pub extern const antfly_av1_dav1d_gaussian_sequence: [2048]i16;
pub fn get_random_number(bits: c_int, state: [*c]c_uint) callconv(.c) c_int {
    _ = &bits;
    _ = &state;
    const r: c_int = @bitCast(@as(c_uint, @truncate(state.*)));
    _ = &r;
    var bit: c_uint = @bitCast(@as(c_int, ((((r >> @intCast(@as(c_int, 0))) ^ (r >> @intCast(@as(c_int, 1)))) ^ (r >> @intCast(@as(c_int, 3)))) ^ (r >> @intCast(@as(c_int, 12)))) & @as(c_int, 1)));
    _ = &bit;
    state.* = @as(c_uint, @bitCast(@as(c_int, r >> @intCast(@as(c_int, 1))))) | (bit << @intCast(@as(c_uint, 15)));
    return @bitCast(@as(c_uint, @truncate((state.* >> @intCast(@as(c_uint, @bitCast(@as(c_int, @as(c_int, 16) - bits))))) & @as(c_uint, @bitCast(@as(c_int, (@as(c_int, 1) << @intCast(bits)) - @as(c_int, 1)))))));
}
pub fn round2(x: c_int, shift: u64) callconv(.c) c_int {
    _ = &x;
    _ = &shift;
    return @bitCast(@as(c_uint, @truncate((@as(u64, @bitCast(@as(c_longlong, x))) +% ((@as(u64, 1) << @intCast(shift)) >> @intCast(@as(u64, 1)))) >> @intCast(shift))));
}
pub fn generate_grain_y_c(arg_buf: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, bitdepth_max: c_int) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    _ = &data;
    _ = &bitdepth_max;
    const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    var seed: c_uint = data.*.seed;
    _ = &seed;
    const shift: c_int = (@as(c_int, 4) - bitdepth_min_8) + data.*.grain_scale_shift;
    _ = &shift;
    const grain_ctr: c_int = @as(c_int, 128) << @intCast(bitdepth_min_8);
    _ = &grain_ctr;
    const grain_min: c_int = -grain_ctr;
    _ = &grain_min;
    const grain_max: c_int = grain_ctr - @as(c_int, 1);
    _ = &grain_max;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < @as(c_int, 73)) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < @as(c_int, 82)) : (x += 1) {
                    const value: c_int = get_random_number(11, &seed);
                    _ = &value;
                    buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))] = @truncate(round2(antfly_av1_dav1d_gaussian_sequence[@bitCast(@as(isize, @intCast(value)))], @bitCast(@as(c_longlong, shift))));
                }
            }
        }
    }
    const ar_pad: c_int = 3;
    _ = &ar_pad;
    const ar_lag: c_int = data.*.ar_coeff_lag;
    _ = &ar_lag;
    {
        var y: c_int = ar_pad;
        _ = &y;
        while (y < @as(c_int, 73)) : (y += 1) {
            {
                var x: c_int = ar_pad;
                _ = &x;
                while (x < (@as(c_int, 82) - ar_pad)) : (x += 1) {
                    var coeff: [*c]const i8 = @ptrCast(@alignCast(&data.*.ar_coeffs_y));
                    _ = &coeff;
                    var sum: c_int = 0;
                    _ = &sum;
                    {
                        var dy: c_int = -ar_lag;
                        _ = &dy;
                        while (dy <= @as(c_int, 0)) : (dy += 1) {
                            {
                                var dx: c_int = -ar_lag;
                                _ = &dx;
                                while (dx <= ar_lag) : (dx += 1) {
                                    if (!(dx != 0) and !(dy != 0)) break;
                                    sum += @as(c_int, (blk: {
                                        const ref = &coeff;
                                        const tmp = ref.*;
                                        ref.* += 1;
                                        break :blk tmp;
                                    }).*) * @as(c_int, buf[@bitCast(@as(isize, @intCast(y + dy)))][@bitCast(@as(isize, @intCast(x + dx)))]);
                                }
                            }
                        }
                    }
                    const grain: c_int = @as(c_int, buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))]) + round2(sum, data.*.ar_coeff_shift);
                    _ = &grain;
                    buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))] = @truncate(iclip(grain, grain_min, grain_max));
                }
            }
        }
    }
}
pub fn generate_grain_uv_c(arg_buf: [*c][82]entry, arg_buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, subx: c_int, suby: c_int, bitdepth_max: c_int) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    var buf_y = arg_buf_y;
    _ = &buf_y;
    _ = &data;
    _ = &uv;
    _ = &subx;
    _ = &suby;
    _ = &bitdepth_max;
    const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    var seed: c_uint = data.*.seed ^ @as(c_uint, @bitCast(@as(c_int, if (uv != 0) @as(c_int, 18904) else @as(c_int, 46372))));
    _ = &seed;
    const shift: c_int = (@as(c_int, 4) - bitdepth_min_8) + data.*.grain_scale_shift;
    _ = &shift;
    const grain_ctr: c_int = @as(c_int, 128) << @intCast(bitdepth_min_8);
    _ = &grain_ctr;
    const grain_min: c_int = -grain_ctr;
    _ = &grain_min;
    const grain_max: c_int = grain_ctr - @as(c_int, 1);
    _ = &grain_max;
    const chromaW: c_int = if (subx != 0) @as(c_int, 44) else @as(c_int, 82);
    _ = &chromaW;
    const chromaH: c_int = if (suby != 0) @as(c_int, 38) else @as(c_int, 73);
    _ = &chromaH;
    {
        var y: c_int = 0;
        _ = &y;
        while (y < chromaH) : (y += 1) {
            {
                var x: c_int = 0;
                _ = &x;
                while (x < chromaW) : (x += 1) {
                    const value: c_int = get_random_number(11, &seed);
                    _ = &value;
                    buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))] = @truncate(round2(antfly_av1_dav1d_gaussian_sequence[@bitCast(@as(isize, @intCast(value)))], @bitCast(@as(c_longlong, shift))));
                }
            }
        }
    }
    const ar_pad: c_int = 3;
    _ = &ar_pad;
    const ar_lag: c_int = data.*.ar_coeff_lag;
    _ = &ar_lag;
    {
        var y: c_int = ar_pad;
        _ = &y;
        while (y < chromaH) : (y += 1) {
            {
                var x: c_int = ar_pad;
                _ = &x;
                while (x < (chromaW - ar_pad)) : (x += 1) {
                    var coeff: [*c]const i8 = @ptrCast(@alignCast(&data.*.ar_coeffs_uv[@bitCast(@as(isize, @intCast(uv)))]));
                    _ = &coeff;
                    var sum: c_int = 0;
                    _ = &sum;
                    {
                        var dy: c_int = -ar_lag;
                        _ = &dy;
                        while (dy <= @as(c_int, 0)) : (dy += 1) {
                            {
                                var dx: c_int = -ar_lag;
                                _ = &dx;
                                while (dx <= ar_lag) : (dx += 1) {
                                    if (!(dx != 0) and !(dy != 0)) {
                                        if (!(data.*.num_y_points != 0)) break;
                                        var luma: c_int = 0;
                                        _ = &luma;
                                        const lumaX: c_int = ((x - ar_pad) << @intCast(subx)) + ar_pad;
                                        _ = &lumaX;
                                        const lumaY: c_int = ((y - ar_pad) << @intCast(suby)) + ar_pad;
                                        _ = &lumaY;
                                        {
                                            var i: c_int = 0;
                                            _ = &i;
                                            while (i <= suby) : (i += 1) {
                                                {
                                                    var j: c_int = 0;
                                                    _ = &j;
                                                    while (j <= subx) : (j += 1) {
                                                        luma += buf_y[@bitCast(@as(isize, @intCast(lumaY + i)))][@bitCast(@as(isize, @intCast(lumaX + j)))];
                                                    }
                                                }
                                            }
                                        }
                                        luma = round2(luma, @bitCast(@as(c_longlong, subx + suby)));
                                        sum += luma * @as(c_int, coeff.*);
                                        break;
                                    }
                                    sum += @as(c_int, (blk: {
                                        const ref = &coeff;
                                        const tmp = ref.*;
                                        ref.* += 1;
                                        break :blk tmp;
                                    }).*) * @as(c_int, buf[@bitCast(@as(isize, @intCast(y + dy)))][@bitCast(@as(isize, @intCast(x + dx)))]);
                                }
                            }
                        }
                    }
                    const grain: c_int = @as(c_int, buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))]) + round2(sum, data.*.ar_coeff_shift);
                    _ = &grain;
                    buf[@bitCast(@as(isize, @intCast(y)))][@bitCast(@as(isize, @intCast(x)))] = @truncate(iclip(grain, grain_min, grain_max));
                }
            }
        }
    }
}
pub fn generate_grain_uv_420_c(arg_buf: [*c][82]entry, arg_buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, bitdepth_max: c_int) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    var buf_y = arg_buf_y;
    _ = &buf_y;
    _ = &data;
    _ = &uv;
    _ = &bitdepth_max;
    generate_grain_uv_c(buf, buf_y, data, uv, 1, 1, bitdepth_max);
}
pub fn generate_grain_uv_422_c(arg_buf: [*c][82]entry, arg_buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, bitdepth_max: c_int) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    var buf_y = arg_buf_y;
    _ = &buf_y;
    _ = &data;
    _ = &uv;
    _ = &bitdepth_max;
    generate_grain_uv_c(buf, buf_y, data, uv, 1, 0, bitdepth_max);
}
pub fn generate_grain_uv_444_c(arg_buf: [*c][82]entry, arg_buf_y: [*c][82]entry, data: [*c]const Dav1dFilmGrainData, uv: isize, bitdepth_max: c_int) callconv(.c) void {
    var buf = arg_buf;
    _ = &buf;
    var buf_y = arg_buf_y;
    _ = &buf_y;
    _ = &data;
    _ = &uv;
    _ = &bitdepth_max;
    generate_grain_uv_c(buf, buf_y, data, uv, 0, 0, bitdepth_max);
}
pub fn sample_lut(arg_grain_lut: [*c][82]entry, arg_offsets: [*c][2]c_int, subx: c_int, suby: c_int, bx: c_int, by: c_int, x: c_int, y: c_int) callconv(.c) entry {
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    var offsets = arg_offsets;
    _ = &offsets;
    _ = &subx;
    _ = &suby;
    _ = &bx;
    _ = &by;
    _ = &x;
    _ = &y;
    const randval: c_int = offsets[@bitCast(@as(isize, @intCast(bx)))][@bitCast(@as(isize, @intCast(by)))];
    _ = &randval;
    const offx: c_int = @as(c_int, 3) + ((@as(c_int, 2) >> @intCast(subx)) * (@as(c_int, 3) + (randval >> @intCast(@as(c_int, 4)))));
    _ = &offx;
    const offy: c_int = @as(c_int, 3) + ((@as(c_int, 2) >> @intCast(suby)) * (@as(c_int, 3) + (randval & @as(c_int, 15))));
    _ = &offy;
    return grain_lut[@bitCast(@as(isize, @intCast((offy + y) + ((@as(c_int, 32) >> @intCast(suby)) * by))))][@bitCast(@as(isize, @intCast((offx + x) + ((@as(c_int, 32) >> @intCast(subx)) * bx))))];
}
pub fn fgy_32x32xn_c(dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, arg_scaling: [*c]const u8, arg_grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst_row;
    _ = &src_row;
    _ = &stride;
    _ = &data;
    _ = &pw;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    _ = &bh;
    _ = &row_num;
    _ = &bitdepth_max;
    const rows: c_int = @as(c_int, 1) + @as(c_int, @intFromBool((data.*.overlap_flag != 0) and (row_num > @as(c_int, 0))));
    _ = &rows;
    const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    const grain_ctr: c_int = @as(c_int, 128) << @intCast(bitdepth_min_8);
    _ = &grain_ctr;
    const grain_min: c_int = -grain_ctr;
    _ = &grain_min;
    const grain_max: c_int = grain_ctr - @as(c_int, 1);
    _ = &grain_max;
    var min_value: c_int = undefined;
    _ = &min_value;
    var max_value: c_int = undefined;
    _ = &max_value;
    if (data.*.clip_to_restricted_range != 0) {
        min_value = @as(c_int, 16) << @intCast(bitdepth_min_8);
        max_value = @as(c_int, 235) << @intCast(bitdepth_min_8);
    } else {
        min_value = 0;
        max_value = bitdepth_max;
    }
    var seed: [2]c_uint = undefined;
    _ = &seed;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < rows) : (i += 1) {
            seed[@bitCast(@as(isize, @intCast(i)))] = data.*.seed;
            seed[@bitCast(@as(isize, @intCast(i)))] ^= @bitCast(@as(c_int, ((((row_num - i) * @as(c_int, 37)) + @as(c_int, 178)) & @as(c_int, 255)) << @intCast(@as(c_int, 8))));
            seed[@bitCast(@as(isize, @intCast(i)))] ^= @bitCast(@as(c_int, (((row_num - i) * @as(c_int, 173)) + @as(c_int, 105)) & @as(c_int, 255)));
        }
    }
    while (true) {
        if (!((@as(c_ulong, @bitCast(@as(c_long, stride))) % (@as(c_ulong, 32) *% @sizeOf(pixel))) == @as(c_ulong, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    var offsets: [2][2]c_int = undefined;
    _ = &offsets;
    {
        var bx: c_uint = 0;
        _ = &bx;
        while (@as(usize, bx) < pw) : (bx +%= 32) {
            const bw: c_int = imin(32, @bitCast(@as(c_uint, @truncate(@as(c_uint, @bitCast(@as(c_int, @as(c_int, @bitCast(@as(c_uint, @truncate(pw))))))) -% bx))));
            _ = &bw;
            if ((@as(c_uint, @bitCast(@as(c_int, data.*.overlap_flag))) != 0) and (bx != 0)) {
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < rows) : (i += 1) {
                        offsets[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i)))] = offsets[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i)))];
                    }
                }
            }
            {
                var i: c_int = 0;
                _ = &i;
                while (i < rows) : (i += 1) {
                    offsets[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i)))] = get_random_number(8, &seed[@bitCast(@as(isize, @intCast(i)))]);
                }
            }
            const ystart: c_int = if ((data.*.overlap_flag != 0) and (row_num != 0)) imin(2, bh) else @as(c_int, 0);
            _ = &ystart;
            const xstart: c_int = if ((@as(c_uint, @bitCast(@as(c_int, data.*.overlap_flag))) != 0) and (bx != 0)) imin(2, bw) else @as(c_int, 0);
            _ = &xstart;
            const static_local_w = struct {
                var w: [2][2]c_int = [2][2]c_int{
                    [2]c_int{
                        27,
                        17,
                    },
                    [2]c_int{
                        17,
                        27,
                    },
                };
            };
            _ = &static_local_w;
            {
                var y: c_int = ystart;
                _ = &y;
                while (y < bh) : (y += 1) {
                    {
                        var x: c_int = xstart;
                        _ = &x;
                        while (x < bw) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 0, x, y);
                            _ = &grain;
                            const src: [*c]const pixel = ((src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &src;
                            const dst: [*c]pixel = ((dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &dst;
                            const noise: c_int = round2(@as(c_int, scaling[src.*]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                    {
                        var x: c_int = 0;
                        _ = &x;
                        while (x < xstart) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 0, x, y);
                            _ = &grain;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 1, 0, x, y);
                            _ = &old;
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const src: [*c]const pixel = ((src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &src;
                            const dst: [*c]pixel = ((dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &dst;
                            const noise: c_int = round2(@as(c_int, scaling[src.*]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                }
            }
            {
                var y: c_int = 0;
                _ = &y;
                while (y < ystart) : (y += 1) {
                    {
                        var x: c_int = xstart;
                        _ = &x;
                        while (x < bw) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 0, x, y);
                            _ = &grain;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 1, x, y);
                            _ = &old;
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const src: [*c]const pixel = ((src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &src;
                            const dst: [*c]pixel = ((dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &dst;
                            const noise: c_int = round2(@as(c_int, scaling[src.*]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                    {
                        var x: c_int = 0;
                        _ = &x;
                        while (x < xstart) : (x += 1) {
                            var top: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 1, x, y);
                            _ = &top;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 1, 1, x, y);
                            _ = &old;
                            top = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (top * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            top = iclip(top, grain_min, grain_max);
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 0, 0, x, y);
                            _ = &grain;
                            old = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), 0, 0, 1, 0, x, y);
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            grain = round2((top * static_local_w.w[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const src: [*c]const pixel = ((src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &src;
                            const dst: [*c]pixel = ((dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(x))))) + bx;
                            _ = &dst;
                            const noise: c_int = round2(@as(c_int, scaling[src.*]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                }
            }
        }
    }
}
pub fn fguv_32x32xn_c(dst_row: [*c]pixel, src_row: [*c]const pixel, stride: ptrdiff_t, data: [*c]const Dav1dFilmGrainData, pw: usize, arg_scaling: [*c]const u8, arg_grain_lut: [*c][82]entry, bh: c_int, row_num: c_int, luma_row: [*c]const pixel, luma_stride: ptrdiff_t, uv: c_int, is_id: c_int, sx: c_int, sy: c_int, bitdepth_max: c_int) callconv(.c) void {
    _ = &dst_row;
    _ = &src_row;
    _ = &stride;
    _ = &data;
    _ = &pw;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    _ = &bh;
    _ = &row_num;
    _ = &luma_row;
    _ = &luma_stride;
    _ = &uv;
    _ = &is_id;
    _ = &sx;
    _ = &sy;
    _ = &bitdepth_max;
    const rows: c_int = @as(c_int, 1) + @as(c_int, @intFromBool((data.*.overlap_flag != 0) and (row_num > @as(c_int, 0))));
    _ = &rows;
    const bitdepth_min_8: c_int = (@as(c_int, 32) - clz(@bitCast(@as(c_int, bitdepth_max)))) - @as(c_int, 8);
    _ = &bitdepth_min_8;
    const grain_ctr: c_int = @as(c_int, 128) << @intCast(bitdepth_min_8);
    _ = &grain_ctr;
    const grain_min: c_int = -grain_ctr;
    _ = &grain_min;
    const grain_max: c_int = grain_ctr - @as(c_int, 1);
    _ = &grain_max;
    var min_value: c_int = undefined;
    _ = &min_value;
    var max_value: c_int = undefined;
    _ = &max_value;
    if (data.*.clip_to_restricted_range != 0) {
        min_value = @as(c_int, 16) << @intCast(bitdepth_min_8);
        max_value = (if (is_id != 0) @as(c_int, 235) else @as(c_int, 240)) << @intCast(bitdepth_min_8);
    } else {
        min_value = 0;
        max_value = bitdepth_max;
    }
    var seed: [2]c_uint = undefined;
    _ = &seed;
    {
        var i: c_int = 0;
        _ = &i;
        while (i < rows) : (i += 1) {
            seed[@bitCast(@as(isize, @intCast(i)))] = data.*.seed;
            seed[@bitCast(@as(isize, @intCast(i)))] ^= @bitCast(@as(c_int, ((((row_num - i) * @as(c_int, 37)) + @as(c_int, 178)) & @as(c_int, 255)) << @intCast(@as(c_int, 8))));
            seed[@bitCast(@as(isize, @intCast(i)))] ^= @bitCast(@as(c_int, (((row_num - i) * @as(c_int, 173)) + @as(c_int, 105)) & @as(c_int, 255)));
        }
    }
    while (true) {
        if (!((@as(c_ulong, @bitCast(@as(c_long, stride))) % (@as(c_ulong, 32) *% @sizeOf(pixel))) == @as(c_ulong, 0))) {
            unreachable;
        }
        if (!false) break;
    }
    var offsets: [2][2]c_int = undefined;
    _ = &offsets;
    {
        var bx: c_uint = 0;
        _ = &bx;
        while (@as(usize, bx) < pw) : (bx +%= @bitCast(@as(c_int, @as(c_int, 32) >> @intCast(sx)))) {
            const bw: c_int = imin(@as(c_int, 32) >> @intCast(sx), @bitCast(@as(c_uint, @truncate(pw -% @as(usize, bx)))));
            _ = &bw;
            if ((@as(c_uint, @bitCast(@as(c_int, data.*.overlap_flag))) != 0) and (bx != 0)) {
                {
                    var i: c_int = 0;
                    _ = &i;
                    while (i < rows) : (i += 1) {
                        offsets[@as(c_int, 1)][@bitCast(@as(isize, @intCast(i)))] = offsets[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i)))];
                    }
                }
            }
            {
                var i: c_int = 0;
                _ = &i;
                while (i < rows) : (i += 1) {
                    offsets[@as(c_int, 0)][@bitCast(@as(isize, @intCast(i)))] = get_random_number(8, &seed[@bitCast(@as(isize, @intCast(i)))]);
                }
            }
            const ystart: c_int = if ((data.*.overlap_flag != 0) and (row_num != 0)) imin(@as(c_int, 2) >> @intCast(sy), bh) else @as(c_int, 0);
            _ = &ystart;
            const xstart: c_int = if ((@as(c_uint, @bitCast(@as(c_int, data.*.overlap_flag))) != 0) and (bx != 0)) imin(@as(c_int, 2) >> @intCast(sx), bw) else @as(c_int, 0);
            _ = &xstart;
            const static_local_w = struct {
                var w: [2][2][2]c_int = [2][2][2]c_int{
                    [2][2]c_int{
                        [2]c_int{
                            27,
                            17,
                        },
                        [2]c_int{
                            17,
                            27,
                        },
                    },
                    [1][2]c_int{
                        [2]c_int{
                            23,
                            22,
                        },
                    } ++ @as([1][2]c_int, @splat(@import("std").mem.zeroes([2]c_int))),
                };
            };
            _ = &static_local_w;
            {
                var y: c_int = ystart;
                _ = &y;
                while (y < bh) : (y += 1) {
                    {
                        var x: c_int = xstart;
                        _ = &x;
                        while (x < bw) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 0, x, y);
                            _ = &grain;
                            const lx: c_int = @bitCast(@as(c_uint, @truncate((bx +% @as(c_uint, @bitCast(@as(c_int, x)))) << @intCast(@as(c_uint, @bitCast(@as(c_int, sx)))))));
                            _ = &lx;
                            const ly: c_int = y << @intCast(sy);
                            _ = &ly;
                            const luma: [*c]const pixel = (luma_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, ly) * PXSTRIDE(luma_stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(lx))));
                            _ = &luma;
                            var avg: pixel = luma[@as(c_int, 0)];
                            _ = &avg;
                            if (sx != 0) {
                                avg = @bitCast(@as(c_short, @truncate(((@as(c_int, avg) + @as(c_int, luma[@as(c_int, 1)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
                            }
                            const src: [*c]const pixel = (src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &src;
                            const dst: [*c]pixel = (dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &dst;
                            var val: c_int = avg;
                            _ = &val;
                            if (!(data.*.chroma_scaling_from_luma != 0)) {
                                const combined: c_int = (@as(c_int, avg) * data.*.uv_luma_mult[@bitCast(@as(isize, @intCast(uv)))]) + (@as(c_int, src.*) * data.*.uv_mult[@bitCast(@as(isize, @intCast(uv)))]);
                                _ = &combined;
                                val = iclip((combined >> @intCast(@as(c_int, 6))) + (data.*.uv_offset[@bitCast(@as(isize, @intCast(uv)))] * (@as(c_int, 1) << @intCast(bitdepth_min_8))), 0, bitdepth_max);
                            }
                            const noise: c_int = round2(@as(c_int, scaling[@bitCast(@as(isize, @intCast(val)))]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                    {
                        var x: c_int = 0;
                        _ = &x;
                        while (x < xstart) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 0, x, y);
                            _ = &grain;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 1, 0, x, y);
                            _ = &old;
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const lx: c_int = @bitCast(@as(c_uint, @truncate((bx +% @as(c_uint, @bitCast(@as(c_int, x)))) << @intCast(@as(c_uint, @bitCast(@as(c_int, sx)))))));
                            _ = &lx;
                            const ly: c_int = y << @intCast(sy);
                            _ = &ly;
                            const luma: [*c]const pixel = (luma_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, ly) * PXSTRIDE(luma_stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(lx))));
                            _ = &luma;
                            var avg: pixel = luma[@as(c_int, 0)];
                            _ = &avg;
                            if (sx != 0) {
                                avg = @bitCast(@as(c_short, @truncate(((@as(c_int, avg) + @as(c_int, luma[@as(c_int, 1)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
                            }
                            const src: [*c]const pixel = (src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &src;
                            const dst: [*c]pixel = (dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &dst;
                            var val: c_int = avg;
                            _ = &val;
                            if (!(data.*.chroma_scaling_from_luma != 0)) {
                                const combined: c_int = (@as(c_int, avg) * data.*.uv_luma_mult[@bitCast(@as(isize, @intCast(uv)))]) + (@as(c_int, src.*) * data.*.uv_mult[@bitCast(@as(isize, @intCast(uv)))]);
                                _ = &combined;
                                val = iclip((combined >> @intCast(@as(c_int, 6))) + (data.*.uv_offset[@bitCast(@as(isize, @intCast(uv)))] * (@as(c_int, 1) << @intCast(bitdepth_min_8))), 0, bitdepth_max);
                            }
                            const noise: c_int = round2(@as(c_int, scaling[@bitCast(@as(isize, @intCast(val)))]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                }
            }
            {
                var y: c_int = 0;
                _ = &y;
                while (y < ystart) : (y += 1) {
                    {
                        var x: c_int = xstart;
                        _ = &x;
                        while (x < bw) : (x += 1) {
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 0, x, y);
                            _ = &grain;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 1, x, y);
                            _ = &old;
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(sy)))][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(sy)))][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const lx: c_int = @bitCast(@as(c_uint, @truncate((bx +% @as(c_uint, @bitCast(@as(c_int, x)))) << @intCast(@as(c_uint, @bitCast(@as(c_int, sx)))))));
                            _ = &lx;
                            const ly: c_int = y << @intCast(sy);
                            _ = &ly;
                            const luma: [*c]const pixel = (luma_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, ly) * PXSTRIDE(luma_stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(lx))));
                            _ = &luma;
                            var avg: pixel = luma[@as(c_int, 0)];
                            _ = &avg;
                            if (sx != 0) {
                                avg = @bitCast(@as(c_short, @truncate(((@as(c_int, avg) + @as(c_int, luma[@as(c_int, 1)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
                            }
                            const src: [*c]const pixel = (src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &src;
                            const dst: [*c]pixel = (dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &dst;
                            var val: c_int = avg;
                            _ = &val;
                            if (!(data.*.chroma_scaling_from_luma != 0)) {
                                const combined: c_int = (@as(c_int, avg) * data.*.uv_luma_mult[@bitCast(@as(isize, @intCast(uv)))]) + (@as(c_int, src.*) * data.*.uv_mult[@bitCast(@as(isize, @intCast(uv)))]);
                                _ = &combined;
                                val = iclip((combined >> @intCast(@as(c_int, 6))) + (data.*.uv_offset[@bitCast(@as(isize, @intCast(uv)))] * (@as(c_int, 1) << @intCast(bitdepth_min_8))), 0, bitdepth_max);
                            }
                            const noise: c_int = round2(@as(c_int, scaling[@bitCast(@as(isize, @intCast(val)))]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                    {
                        var x: c_int = 0;
                        _ = &x;
                        while (x < xstart) : (x += 1) {
                            var top: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 1, x, y);
                            _ = &top;
                            var old: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 1, 1, x, y);
                            _ = &old;
                            top = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (top * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            top = iclip(top, grain_min, grain_max);
                            var grain: c_int = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 0, 0, x, y);
                            _ = &grain;
                            old = sample_lut(grain_lut, @ptrCast(@alignCast(&offsets)), sx, sy, 1, 0, x, y);
                            grain = round2((old * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(sx)))][@bitCast(@as(isize, @intCast(x)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            grain = round2((top * static_local_w.w[@bitCast(@as(isize, @intCast(sy)))][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 0)]) + (grain * static_local_w.w[@bitCast(@as(isize, @intCast(sy)))][@bitCast(@as(isize, @intCast(y)))][@as(c_int, 1)]), 5);
                            grain = iclip(grain, grain_min, grain_max);
                            const lx: c_int = @bitCast(@as(c_uint, @truncate((bx +% @as(c_uint, @bitCast(@as(c_int, x)))) << @intCast(@as(c_uint, @bitCast(@as(c_int, sx)))))));
                            _ = &lx;
                            const ly: c_int = y << @intCast(sy);
                            _ = &ly;
                            const luma: [*c]const pixel = (luma_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, ly) * PXSTRIDE(luma_stride)))))) + @as(usize, @bitCast(@as(isize, @intCast(lx))));
                            _ = &luma;
                            var avg: pixel = luma[@as(c_int, 0)];
                            _ = &avg;
                            if (sx != 0) {
                                avg = @bitCast(@as(c_short, @truncate(((@as(c_int, avg) + @as(c_int, luma[@as(c_int, 1)])) + @as(c_int, 1)) >> @intCast(@as(c_int, 1)))));
                            }
                            const src: [*c]const pixel = (src_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &src;
                            const dst: [*c]pixel = (dst_row + @as(usize, @bitCast(@as(isize, @intCast(@as(ptrdiff_t, y) * PXSTRIDE(stride)))))) + (bx +% @as(c_uint, @bitCast(@as(c_int, x))));
                            _ = &dst;
                            var val: c_int = avg;
                            _ = &val;
                            if (!(data.*.chroma_scaling_from_luma != 0)) {
                                const combined: c_int = (@as(c_int, avg) * data.*.uv_luma_mult[@bitCast(@as(isize, @intCast(uv)))]) + (@as(c_int, src.*) * data.*.uv_mult[@bitCast(@as(isize, @intCast(uv)))]);
                                _ = &combined;
                                val = iclip((combined >> @intCast(@as(c_int, 6))) + (data.*.uv_offset[@bitCast(@as(isize, @intCast(uv)))] * (@as(c_int, 1) << @intCast(bitdepth_min_8))), 0, bitdepth_max);
                            }
                            const noise: c_int = round2(@as(c_int, scaling[@bitCast(@as(isize, @intCast(val)))]) * grain, @bitCast(@as(c_longlong, data.*.scaling_shift)));
                            _ = &noise;
                            dst.* = @bitCast(@as(c_short, @truncate(iclip(@as(c_int, src.*) + noise, min_value, max_value))));
                        }
                    }
                }
            }
        }
    }
}
pub fn fguv_32x32xn_420_c(arg_dst_row: [*c]pixel, arg_src_row: [*c]const pixel, arg_stride: ptrdiff_t, arg_data: [*c]const Dav1dFilmGrainData, arg_pw: usize, arg_scaling: [*c]const u8, arg_grain_lut: [*c][82]entry, arg_bh: c_int, arg_row_num: c_int, arg_luma_row: [*c]const pixel, arg_luma_stride: ptrdiff_t, arg_uv_pl: c_int, arg_is_id: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst_row = arg_dst_row;
    _ = &dst_row;
    var src_row = arg_src_row;
    _ = &src_row;
    var stride = arg_stride;
    _ = &stride;
    var data = arg_data;
    _ = &data;
    var pw = arg_pw;
    _ = &pw;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    var bh = arg_bh;
    _ = &bh;
    var row_num = arg_row_num;
    _ = &row_num;
    var luma_row = arg_luma_row;
    _ = &luma_row;
    var luma_stride = arg_luma_stride;
    _ = &luma_stride;
    var uv_pl = arg_uv_pl;
    _ = &uv_pl;
    var is_id = arg_is_id;
    _ = &is_id;
    _ = &bitdepth_max;
    fguv_32x32xn_c(dst_row, src_row, stride, data, pw, scaling, grain_lut, bh, row_num, luma_row, luma_stride, uv_pl, is_id, 1, 1, bitdepth_max);
}
pub fn fguv_32x32xn_422_c(arg_dst_row: [*c]pixel, arg_src_row: [*c]const pixel, arg_stride: ptrdiff_t, arg_data: [*c]const Dav1dFilmGrainData, arg_pw: usize, arg_scaling: [*c]const u8, arg_grain_lut: [*c][82]entry, arg_bh: c_int, arg_row_num: c_int, arg_luma_row: [*c]const pixel, arg_luma_stride: ptrdiff_t, arg_uv_pl: c_int, arg_is_id: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst_row = arg_dst_row;
    _ = &dst_row;
    var src_row = arg_src_row;
    _ = &src_row;
    var stride = arg_stride;
    _ = &stride;
    var data = arg_data;
    _ = &data;
    var pw = arg_pw;
    _ = &pw;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    var bh = arg_bh;
    _ = &bh;
    var row_num = arg_row_num;
    _ = &row_num;
    var luma_row = arg_luma_row;
    _ = &luma_row;
    var luma_stride = arg_luma_stride;
    _ = &luma_stride;
    var uv_pl = arg_uv_pl;
    _ = &uv_pl;
    var is_id = arg_is_id;
    _ = &is_id;
    _ = &bitdepth_max;
    fguv_32x32xn_c(dst_row, src_row, stride, data, pw, scaling, grain_lut, bh, row_num, luma_row, luma_stride, uv_pl, is_id, 1, 0, bitdepth_max);
}
pub fn fguv_32x32xn_444_c(arg_dst_row: [*c]pixel, arg_src_row: [*c]const pixel, arg_stride: ptrdiff_t, arg_data: [*c]const Dav1dFilmGrainData, arg_pw: usize, arg_scaling: [*c]const u8, arg_grain_lut: [*c][82]entry, arg_bh: c_int, arg_row_num: c_int, arg_luma_row: [*c]const pixel, arg_luma_stride: ptrdiff_t, arg_uv_pl: c_int, arg_is_id: c_int, bitdepth_max: c_int) callconv(.c) void {
    var dst_row = arg_dst_row;
    _ = &dst_row;
    var src_row = arg_src_row;
    _ = &src_row;
    var stride = arg_stride;
    _ = &stride;
    var data = arg_data;
    _ = &data;
    var pw = arg_pw;
    _ = &pw;
    var scaling = arg_scaling;
    _ = &scaling;
    var grain_lut = arg_grain_lut;
    _ = &grain_lut;
    var bh = arg_bh;
    _ = &bh;
    var row_num = arg_row_num;
    _ = &row_num;
    var luma_row = arg_luma_row;
    _ = &luma_row;
    var luma_stride = arg_luma_stride;
    _ = &luma_stride;
    var uv_pl = arg_uv_pl;
    _ = &uv_pl;
    var is_id = arg_is_id;
    _ = &is_id;
    _ = &bitdepth_max;
    fguv_32x32xn_c(dst_row, src_row, stride, data, pw, scaling, grain_lut, bh, row_num, luma_row, luma_stride, uv_pl, is_id, 0, 0, bitdepth_max);
}
pub export fn antfly_av1_dav1d_film_grain_dsp_init_16bpc(c: [*c]Dav1dFilmGrainDSPContext) void {
    _ = &c;
    c.*.generate_grain_y = generate_grain_y_c;
    c.*.generate_grain_uv[DAV1D_PIXEL_LAYOUT_I420 - @as(c_int, 1)] = generate_grain_uv_420_c;
    c.*.generate_grain_uv[DAV1D_PIXEL_LAYOUT_I422 - @as(c_int, 1)] = generate_grain_uv_422_c;
    c.*.generate_grain_uv[DAV1D_PIXEL_LAYOUT_I444 - @as(c_int, 1)] = generate_grain_uv_444_c;
    c.*.fgy_32x32xn = fgy_32x32xn_c;
    c.*.fguv_32x32xn[DAV1D_PIXEL_LAYOUT_I420 - @as(c_int, 1)] = fguv_32x32xn_420_c;
    c.*.fguv_32x32xn[DAV1D_PIXEL_LAYOUT_I422 - @as(c_int, 1)] = fguv_32x32xn_422_c;
    c.*.fguv_32x32xn[DAV1D_PIXEL_LAYOUT_I444 - @as(c_int, 1)] = fguv_32x32xn_444_c;
}
