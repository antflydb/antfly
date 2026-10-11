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
pub const ptrdiff_t = common.ptrdiff_t;
pub const tran_low_t = common.tran_low_t;
pub extern fn antfly_vp9_vpx_d117_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d117_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d117_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d117_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d135_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d135_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d135_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d135_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d153_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d153_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d153_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d153_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d207_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d207_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d207_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d207_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d45_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d45_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d45_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d45_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d63_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d63_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d63_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_d63_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_128_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_128_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_128_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_128_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_left_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_left_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_left_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_left_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_top_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_top_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_top_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_dc_top_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_h_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_h_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_h_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_h_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_highbd_d117_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d117_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d117_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d117_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d135_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d135_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d135_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d135_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d153_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d153_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d153_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d153_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d207_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d207_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d207_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d207_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d45_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d45_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d45_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d45_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d63_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d63_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d63_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_d63_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_128_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_128_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_128_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_128_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_left_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_left_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_left_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_left_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_top_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_top_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_top_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_dc_top_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_h_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_h_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_h_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_h_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_tm_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_tm_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_tm_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_tm_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_v_predictor_16x16_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_v_predictor_32x32_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_v_predictor_4x4_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_highbd_v_predictor_8x8_c(dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) void;
pub extern fn antfly_vp9_vpx_tm_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_tm_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_tm_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_tm_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_v_predictor_16x16_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_v_predictor_32x32_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_v_predictor_4x4_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub extern fn antfly_vp9_vpx_v_predictor_8x8_c(dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) void;
pub fn vpx_memset16(arg_dest: ?*anyopaque, arg_val: c_int, arg_length: usize) callconv(.c) ?*anyopaque {
    var dest = arg_dest;
    _ = &dest;
    var val = arg_val;
    _ = &val;
    var length = arg_length;
    _ = &length;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var i: usize = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var dest16: [*c]u16 = @ptrCast(@alignCast(dest));
    _ = &dest16;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    {
        i = 0;
        while (i < length) : (i +%= 1) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            (blk: {
                const ref = &dest16;
                const tmp = ref.*;
                ref.* += 1;
                break :blk tmp;
            }).* = @bitCast(@as(c_short, @truncate(val)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return null;
            }
        }
    }
    return dest;
}
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_vp9_once(callback: ?*const fn () callconv(.c) void) void;
pub const vpx_color_space_t = common.vpx_color_space_t;
pub const vpx_color_range_t = common.vpx_color_range_t;
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
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_TYPE = common.PARTITION_TYPE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub const DCT_DCT = common.DCT_DCT;
pub const ADST_DCT = common.ADST_DCT;
pub const DCT_ADST = common.DCT_ADST;
pub const ADST_ADST = common.ADST_ADST;
pub const TX_TYPE = common.TX_TYPE;
pub const PREDICTION_MODE = common.PREDICTION_MODE;
pub const vpx_prob = common.vpx_prob;
pub const struct_vpx_internal_error_info = common.struct_vpx_internal_error_info;
pub const struct_scale_factors = common.struct_scale_factors;
pub const struct_RefBuffer = extern struct {
    idx: c_int = 0,
    buf: [*c]YV12_BUFFER_CONFIG = null,
    sf: struct_scale_factors = @import("std").mem.zeroes(struct_scale_factors),
};
pub const RefBuffer = struct_RefBuffer;
pub const INTERP_FILTER = common.INTERP_FILTER;
pub const MV_REFERENCE_FRAME = common.MV_REFERENCE_FRAME;
pub const struct_mv = extern struct {
    row: i16 = 0,
    col: i16 = 0,
};
pub const MV = struct_mv;
pub const union_int_mv = extern union {
    as_int: u32,
    as_mv: MV,
};
pub const int_mv = union_int_mv;
pub const struct_MODE_INFO = extern struct {
    sb_type: BLOCK_SIZE = 0,
    mode: PREDICTION_MODE = 0,
    tx_size: TX_SIZE = 0,
    skip: i8 = 0,
    segment_id: i8 = 0,
    seg_id_predicted: i8 = 0,
    uv_mode: PREDICTION_MODE = 0,
    interp_filter: INTERP_FILTER = 0,
    ref_frame: [2]MV_REFERENCE_FRAME = @import("std").mem.zeroes([2]MV_REFERENCE_FRAME),
    mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
    bmi: [4]b_mode_info = @import("std").mem.zeroes([4]b_mode_info),
};
pub const MODE_INFO = struct_MODE_INFO;
pub const vp9_coeff_probs_model = common.vp9_coeff_probs_model;
pub const struct_tx_probs = extern struct {
    p32x32: [2][3]vpx_prob = @import("std").mem.zeroes([2][3]vpx_prob),
    p16x16: [2][2]vpx_prob = @import("std").mem.zeroes([2][2]vpx_prob),
    p8x8: [2][1]vpx_prob = @import("std").mem.zeroes([2][1]vpx_prob),
};
pub const struct_frame_contexts = extern struct {
    y_mode_prob: [4][9]vpx_prob = @import("std").mem.zeroes([4][9]vpx_prob),
    uv_mode_prob: [10][9]vpx_prob = @import("std").mem.zeroes([10][9]vpx_prob),
    partition_prob: [16][3]vpx_prob = @import("std").mem.zeroes([16][3]vpx_prob),
    coef_probs: [4][2]vp9_coeff_probs_model = @import("std").mem.zeroes([4][2]vp9_coeff_probs_model),
    switchable_interp_prob: [4][2]vpx_prob = @import("std").mem.zeroes([4][2]vpx_prob),
    inter_mode_probs: [7][3]vpx_prob = @import("std").mem.zeroes([7][3]vpx_prob),
    intra_inter_prob: [4]vpx_prob = @import("std").mem.zeroes([4]vpx_prob),
    comp_inter_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    single_ref_prob: [5][2]vpx_prob = @import("std").mem.zeroes([5][2]vpx_prob),
    comp_ref_prob: [5]vpx_prob = @import("std").mem.zeroes([5]vpx_prob),
    tx_probs: struct_tx_probs = @import("std").mem.zeroes(struct_tx_probs),
    skip_probs: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    nmvc: nmv_context = @import("std").mem.zeroes(nmv_context),
    initialized: c_int = 0,
};
pub const FRAME_CONTEXT = struct_frame_contexts;
pub const vp9_coeff_count_model = common.vp9_coeff_count_model;
pub const struct_tx_counts = common.struct_tx_counts;
pub const struct_FRAME_COUNTS = extern struct {
    y_mode: [4][10]c_uint = @import("std").mem.zeroes([4][10]c_uint),
    uv_mode: [10][10]c_uint = @import("std").mem.zeroes([10][10]c_uint),
    partition: [16][4]c_uint = @import("std").mem.zeroes([16][4]c_uint),
    coef: [4][2]vp9_coeff_count_model = @import("std").mem.zeroes([4][2]vp9_coeff_count_model),
    eob_branch: [4][2][2][6][6]c_uint = @import("std").mem.zeroes([4][2][2][6][6]c_uint),
    switchable_interp: [4][3]c_uint = @import("std").mem.zeroes([4][3]c_uint),
    inter_mode: [7][4]c_uint = @import("std").mem.zeroes([7][4]c_uint),
    intra_inter: [4][2]c_uint = @import("std").mem.zeroes([4][2]c_uint),
    comp_inter: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    single_ref: [5][2][2]c_uint = @import("std").mem.zeroes([5][2][2]c_uint),
    comp_ref: [5][2]c_uint = @import("std").mem.zeroes([5][2]c_uint),
    tx: struct_tx_counts = @import("std").mem.zeroes(struct_tx_counts),
    skip: [3][2]c_uint = @import("std").mem.zeroes([3][2]c_uint),
    mv: nmv_context_counts = @import("std").mem.zeroes(nmv_context_counts),
};
pub const FRAME_COUNTS = struct_FRAME_COUNTS;
pub const ENTROPY_CONTEXT = common.ENTROPY_CONTEXT;
pub const nmv_component = common.nmv_component;
pub const nmv_context = extern struct {
    joints: [3]vpx_prob = @import("std").mem.zeroes([3]vpx_prob),
    comps: [2]nmv_component = @import("std").mem.zeroes([2]nmv_component),
};
pub const nmv_context_counts = common.nmv_context_counts;
pub const struct_TileInfo = extern struct {
    mi_row_start: c_int = 0,
    mi_row_end: c_int = 0,
    mi_col_start: c_int = 0,
    mi_col_end: c_int = 0,
};
pub const TileInfo = struct_TileInfo;
pub const b_mode_info = extern struct {
    as_mode: PREDICTION_MODE = 0,
    as_mv: [2]int_mv = @import("std").mem.zeroes([2]int_mv),
};
pub const struct_buf_2d = extern struct {
    buf: [*c]u8 = null,
    stride: c_int = 0,
};
pub const struct_macroblockd_plane = extern struct {
    dqcoeff: [*c]tran_low_t = null,
    subsampling_x: c_int = 0,
    subsampling_y: c_int = 0,
    dst: struct_buf_2d = @import("std").mem.zeroes(struct_buf_2d),
    pre: [2]struct_buf_2d = @import("std").mem.zeroes([2]struct_buf_2d),
    above_context: [*c]ENTROPY_CONTEXT = null,
    left_context: [*c]ENTROPY_CONTEXT = null,
    seg_dequant: [8][2]i16 = @import("std").mem.zeroes([8][2]i16),
    n4_w: u16 = 0,
    n4_h: u16 = 0,
    n4_wl: u8 = 0,
    n4_hl: u8 = 0,
    dequant: [*c]const i16 = null,
    eob: [*c]c_int = null,
};
pub const struct_macroblockd = extern struct {
    plane: [3]struct_macroblockd_plane = @import("std").mem.zeroes([3]struct_macroblockd_plane),
    bmode_blocks_wl: u8 = 0,
    bmode_blocks_hl: u8 = 0,
    counts: [*c]FRAME_COUNTS = null,
    tile: TileInfo = @import("std").mem.zeroes(TileInfo),
    mi_stride: c_int = 0,
    mi: [*c][*c]MODE_INFO = null,
    left_mi: [*c]MODE_INFO = null,
    above_mi: [*c]MODE_INFO = null,
    max_blocks_wide: c_uint = 0,
    max_blocks_high: c_uint = 0,
    partition_probs: [*c][3]vpx_prob = null,
    mb_to_left_edge: c_int = 0,
    mb_to_right_edge: c_int = 0,
    mb_to_top_edge: c_int = 0,
    mb_to_bottom_edge: c_int = 0,
    fc: [*c]FRAME_CONTEXT = null,
    block_refs: [2][*c]const RefBuffer = @import("std").mem.zeroes([2][*c]const RefBuffer),
    cur_buf: [*c]const YV12_BUFFER_CONFIG = null,
    above_context: [3][*c]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][*c]ENTROPY_CONTEXT),
    left_context: [3][16]ENTROPY_CONTEXT = @import("std").mem.zeroes([3][16]ENTROPY_CONTEXT),
    above_seg_context: [*c]PARTITION_CONTEXT = null,
    left_seg_context: [8]PARTITION_CONTEXT = @import("std").mem.zeroes([8]PARTITION_CONTEXT),
    bd: c_int = 0,
    lossless: c_int = 0,
    corrupted: c_int = 0,
    error_info: [*c]struct_vpx_internal_error_info = null,
    partition: [*c]PARTITION_TYPE = null,
};
pub const MACROBLOCKD = struct_macroblockd;
pub export const antfly_vp9_intra_mode_to_tx_type_lookup: [10]TX_TYPE = [10]TX_TYPE{
    DCT_DCT,
    ADST_DCT,
    DCT_ADST,
    DCT_DCT,
    ADST_ADST,
    ADST_DCT,
    DCT_ADST,
    DCT_ADST,
    ADST_DCT,
    ADST_ADST,
};
pub const NEED_LEFT: c_int = 2;
pub const NEED_ABOVE: c_int = 4;
pub const NEED_ABOVERIGHT: c_int = 8;
pub const extend_modes: [10]u8 = [10]u8{
    @bitCast(@as(i8, @truncate(NEED_ABOVE | NEED_LEFT))),
    NEED_ABOVE,
    NEED_LEFT,
    NEED_ABOVERIGHT,
    @bitCast(@as(i8, @truncate(NEED_LEFT | NEED_ABOVE))),
    @bitCast(@as(i8, @truncate(NEED_LEFT | NEED_ABOVE))),
    @bitCast(@as(i8, @truncate(NEED_LEFT | NEED_ABOVE))),
    NEED_LEFT,
    NEED_ABOVERIGHT,
    @bitCast(@as(i8, @truncate(NEED_LEFT | NEED_ABOVE))),
};
pub const intra_pred_fn = ?*const fn (dst: [*c]u8, stride: ptrdiff_t, above: [*c]const u8, left: [*c]const u8) callconv(.c) void;
pub var pred: [10][4]intra_pred_fn = @import("std").mem.zeroes([10][4]intra_pred_fn);
pub var dc_pred: [2][2][4]intra_pred_fn = @import("std").mem.zeroes([2][2][4]intra_pred_fn);
pub const intra_high_pred_fn = ?*const fn (dst: [*c]u16, stride: ptrdiff_t, above: [*c]const u16, left: [*c]const u16, bd: c_int) callconv(.c) void;
pub var pred_high: [10][4]intra_high_pred_fn = @import("std").mem.zeroes([10][4]intra_high_pred_fn);
pub var dc_pred_high: [2][2][4]intra_high_pred_fn = @import("std").mem.zeroes([2][2][4]intra_high_pred_fn);
pub fn vp9_init_intra_predictors_internal() callconv(.c) void {
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_v_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_v_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_v_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_v_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_h_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_h_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_h_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_h_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d207_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d207_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d207_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d207_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d45_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d45_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d45_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d45_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d63_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d63_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d63_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d63_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d117_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d117_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d117_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d117_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d135_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d135_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d135_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d135_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_d153_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_d153_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_d153_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_d153_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_tm_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_tm_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_tm_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_tm_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_dc_128_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_dc_128_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_dc_128_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_dc_128_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_dc_top_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_dc_top_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_dc_top_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_dc_top_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_dc_left_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_dc_left_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_dc_left_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_dc_left_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_dc_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_dc_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_dc_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_dc_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_v_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_v_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_v_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_v_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_h_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_h_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_h_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 2)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_h_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d207_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d207_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d207_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 7)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d207_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d45_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d45_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d45_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 3)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d45_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d63_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d63_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d63_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 8)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d63_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d117_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d117_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d117_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 5)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d117_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d135_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d135_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d135_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 4)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d135_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_d153_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_d153_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_d153_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 6)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_d153_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_tm_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_tm_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_tm_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    pred_high[@as(c_int, 9)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_tm_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_dc_128_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_dc_128_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_dc_128_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_dc_128_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_dc_top_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_dc_top_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_dc_top_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 0)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_dc_top_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_dc_left_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_dc_left_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_dc_left_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 0)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_dc_left_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0)))))] = antfly_vp9_vpx_highbd_dc_predictor_4x4_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1)))))] = antfly_vp9_vpx_highbd_dc_predictor_8x8_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2)))))] = antfly_vp9_vpx_highbd_dc_predictor_16x16_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    dc_pred_high[@as(c_int, 1)][@as(c_int, 1)][@as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3)))))] = antfly_vp9_vpx_highbd_dc_predictor_32x32_c;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn build_intra_predictors_high(arg_xd: [*c]const MACROBLOCKD, arg_ref8: [*c]const u8, arg_ref_stride: c_int, arg_dst8: [*c]u8, arg_dst_stride: c_int, arg_mode: PREDICTION_MODE, arg_tx_size: TX_SIZE, arg_up_available: c_int, arg_left_available: c_int, arg_right_available: c_int, arg_x: c_int, arg_y: c_int, arg_plane: c_int, arg_bd: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var ref8 = arg_ref8;
    _ = &ref8;
    var ref_stride = arg_ref_stride;
    _ = &ref_stride;
    var dst8 = arg_dst8;
    _ = &dst8;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var mode = arg_mode;
    _ = &mode;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var up_available = arg_up_available;
    _ = &up_available;
    var left_available = arg_left_available;
    _ = &left_available;
    var right_available = arg_right_available;
    _ = &right_available;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var plane = arg_plane;
    _ = &plane;
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
    var dst: [*c]u16 = @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(dst8))) << @intCast(@as(usize, 1))));
    _ = &dst;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var ref: [*c]u16 = @as([*c]u16, @ptrFromInt(@as(usize, @intCast(@intFromPtr(ref8))) << @intCast(@as(usize, 1))));
    _ = &ref;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var left_col: [32]u16 align(16) = undefined;
    _ = &left_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var above_data: [80]u16 align(16) = undefined;
    _ = &above_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var above_row: [*c]u16 = @as([*c]u16, @ptrCast(@alignCast(&above_data))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
    _ = &above_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var const_above_row: [*c]const u16 = above_row;
    _ = &const_above_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bs: c_int = @as(c_int, 4) << @intCast(@as(c_int, tx_size));
    _ = &bs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var frame_width: c_int = undefined;
    _ = &frame_width;
    var frame_height: c_int = undefined;
    _ = &frame_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: c_int = undefined;
    _ = &x0;
    var y0: c_int = undefined;
    _ = &y0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const need_left: c_int = @as(c_int, extend_modes[mode]) & NEED_LEFT;
    _ = &need_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const need_above: c_int = @as(c_int, extend_modes[mode]) & NEED_ABOVE;
    _ = &need_above;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const need_aboveright: c_int = @as(c_int, extend_modes[mode]) & NEED_ABOVERIGHT;
    _ = &need_aboveright;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var base: c_int = @as(c_int, 128) << @intCast(bd - @as(c_int, 8));
    _ = &base;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (plane == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = xd.*.cur_buf.*.y_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = xd.*.cur_buf.*.y_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = xd.*.cur_buf.*.uv_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = xd.*.cur_buf.*.uv_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = (-xd.*.mb_to_left_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_x)) + x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    y0 = (-xd.*.mb_to_top_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_y)) + y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (need_left != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_bottom_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((y0 + bs) <= frame_height) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        i = 0;
                        while (i < bs) : (i += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const extend_bottom: c_int = frame_height - y0;
                    _ = &extend_bottom;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        i = 0;
                        while (i < extend_bottom) : (i += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    while (i < bs) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast(((extend_bottom - @as(c_int, 1)) * ref_stride) - @as(c_int, 1))))];
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    i = 0;
                    while (i < bs) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(@as([*c]u16, @ptrCast(@alignCast(&left_col))))), base + @as(c_int, 1), @bitCast(@as(c_long, bs)));
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
    if (need_above != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (up_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var above_ref: [*c]const u16 = ref - @as(usize, @bitCast(@as(isize, @intCast(ref_stride))));
            _ = &above_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_right_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((x0 + bs) <= frame_width) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (x0 <= frame_width) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const r: c_int = frame_width - x0;
                        _ = &r;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, r))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + bs) - frame_width)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (((bs == @as(c_int, 4)) and (right_available != 0)) and (left_available != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const_above_row = above_ref;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
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
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(if (left_available != 0) @as(c_int, above_ref[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else base + @as(c_int, 1))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(above_row)), base - @as(c_int, 1), @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(base - @as(c_int, 1))));
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
    if (need_aboveright != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (up_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var above_ref: [*c]const u16 = ref - @as(usize, @bitCast(@as(isize, @intCast(ref_stride))));
            _ = &above_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_right_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((x0 + (@as(c_int, 2) * bs)) <= frame_width) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((right_available != 0) and (bs == @as(c_int, 4))) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, @as(c_int, 2) * bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((x0 + bs) <= frame_width) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const r: c_int = frame_width - x0;
                        _ = &r;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((right_available != 0) and (bs == @as(c_int, 4))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, r))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + (@as(c_int, 2) * bs)) - frame_width)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (x0 <= frame_width) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            const r: c_int = frame_width - x0;
                            _ = &r;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, r))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + (@as(c_int, 2) * bs)) - frame_width)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
                above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(if (left_available != 0) @as(c_int, above_ref[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else base + @as(c_int, 1))));
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (((bs == @as(c_int, 4)) and (right_available != 0)) and (left_available != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const_above_row = above_ref;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((bs == @as(c_int, 4)) and (right_available != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), @ptrCast(@alignCast(above_ref + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), @as(c_ulong, @bitCast(@as(c_long, bs))) *% @sizeOf(@TypeOf(above_row[@as(c_int, 0)])));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = vpx_memset16(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(if (left_available != 0) @as(c_int, above_ref[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else base + @as(c_int, 1))));
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
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = vpx_memset16(@ptrCast(@alignCast(above_row)), base - @as(c_int, 1), @bitCast(@as(c_long, bs * @as(c_int, 2))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(c_short, @truncate(base - @as(c_int, 1))));
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
    if (@as(c_int, mode) == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dc_pred_high[@bitCast(@as(isize, @intCast(left_available)))][@bitCast(@as(isize, @intCast(up_available)))][tx_size].?(dst, dst_stride, const_above_row, @ptrCast(@alignCast(&left_col)), xd.*.bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pred_high[mode][tx_size].?(dst, dst_stride, const_above_row, @ptrCast(@alignCast(&left_col)), xd.*.bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn build_intra_predictors(arg_xd: [*c]const MACROBLOCKD, arg_ref: [*c]const u8, arg_ref_stride: c_int, arg_dst: [*c]u8, arg_dst_stride: c_int, arg_mode: PREDICTION_MODE, arg_tx_size: TX_SIZE, arg_up_available: c_int, arg_left_available: c_int, arg_right_available: c_int, arg_x: c_int, arg_y: c_int, arg_plane: c_int) callconv(.c) void {
    var xd = arg_xd;
    _ = &xd;
    var ref = arg_ref;
    _ = &ref;
    var ref_stride = arg_ref_stride;
    _ = &ref_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var mode = arg_mode;
    _ = &mode;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var up_available = arg_up_available;
    _ = &up_available;
    var left_available = arg_left_available;
    _ = &left_available;
    var right_available = arg_right_available;
    _ = &right_available;
    var x = arg_x;
    _ = &x;
    var y = arg_y;
    _ = &y;
    var plane = arg_plane;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var i: c_int = undefined;
    _ = &i;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var left_col: [32]u8 align(16) = undefined;
    _ = &left_col;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var above_data: [80]u8 align(16) = undefined;
    _ = &above_data;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var above_row: [*c]u8 = @as([*c]u8, @ptrCast(@alignCast(&above_data))) + @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 16)))));
    _ = &above_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var const_above_row: [*c]const u8 = above_row;
    _ = &const_above_row;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bs: c_int = @as(c_int, 4) << @intCast(@as(c_int, tx_size));
    _ = &bs;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var frame_width: c_int = undefined;
    _ = &frame_width;
    var frame_height: c_int = undefined;
    _ = &frame_height;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    var x0: c_int = undefined;
    _ = &x0;
    var y0: c_int = undefined;
    _ = &y0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const pd: [*c]const struct_macroblockd_plane = &xd.*.plane[@bitCast(@as(isize, @intCast(plane)))];
    _ = &pd;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (plane == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = xd.*.cur_buf.*.y_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = xd.*.cur_buf.*.y_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_width = xd.*.cur_buf.*.uv_width;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        frame_height = xd.*.cur_buf.*.uv_height;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    x0 = (-xd.*.mb_to_left_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_x)) + x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    y0 = (-xd.*.mb_to_top_edge >> @intCast(@as(c_int, 3) + pd.*.subsampling_y)) + y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((@as(c_int, extend_modes[mode]) & NEED_LEFT) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (left_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_bottom_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((y0 + bs) <= frame_height) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        i = 0;
                        while (i < bs) : (i += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const extend_bottom: c_int = frame_height - y0;
                    _ = &extend_bottom;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    {
                        i = 0;
                        while (i < extend_bottom) : (i += 1) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                    }
                    while (i < bs) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast(((extend_bottom - @as(c_int, 1)) * ref_stride) - @as(c_int, 1))))];
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                {
                    i = 0;
                    while (i < bs) : (i += 1) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        left_col[@bitCast(@as(isize, @intCast(i)))] = ref[@bitCast(@as(isize, @intCast((i * ref_stride) - @as(c_int, 1))))];
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(@as([*c]u8, @ptrCast(@alignCast(&left_col))))), 129, @bitCast(@as(c_long, bs)));
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
    if ((@as(c_int, extend_modes[mode]) & NEED_ABOVE) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (up_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var above_ref: [*c]const u8 = ref - @as(usize, @bitCast(@as(isize, @intCast(ref_stride))));
            _ = &above_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_right_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((x0 + bs) <= frame_width) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, bs)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (x0 <= frame_width) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const r: c_int = frame_width - x0;
                        _ = &r;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, r)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + bs) - frame_width)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (((bs == @as(c_int, 4)) and (right_available != 0)) and (left_available != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const_above_row = above_ref;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, bs)));
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
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(i8, @truncate(if (left_available != 0) @as(c_int, above_ref[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else @as(c_int, 129))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row)), 127, @bitCast(@as(c_long, bs)));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = 127;
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
    if ((@as(c_int, extend_modes[mode]) & NEED_ABOVERIGHT) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        if (up_available != 0) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            var above_ref: [*c]const u8 = ref - @as(usize, @bitCast(@as(isize, @intCast(ref_stride))));
            _ = &above_ref;
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            if (xd.*.mb_to_right_edge < @as(c_int, 0)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if ((x0 + (@as(c_int, 2) * bs)) <= frame_width) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((right_available != 0) and (bs == @as(c_int, 4))) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, @as(c_int, 2) * bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((x0 + bs) <= frame_width) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        const r: c_int = frame_width - x0;
                        _ = &r;
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if ((right_available != 0) and (bs == @as(c_int, 4))) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, r)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + (@as(c_int, 2) * bs)) - frame_width)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        } else {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, bs)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (x0 <= frame_width) {
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            const r: c_int = frame_width - x0;
                            _ = &r;
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, r)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(r)))))), above_row[@bitCast(@as(isize, @intCast(r - @as(c_int, 1))))], @bitCast(@as(c_long, (x0 + (@as(c_int, 2) * bs)) - frame_width)));
                            if (antfly_codec_afz_vp9_failed() != 0) {
                                return;
                            }
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                }
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
                if (((bs == @as(c_int, 4)) and (right_available != 0)) and (left_available != 0)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    const_above_row = above_ref;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row)), @ptrCast(@alignCast(above_ref)), @bitCast(@as(c_long, bs)));
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return;
                    }
                    if ((bs == @as(c_int, 4)) and (right_available != 0)) {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), @ptrCast(@alignCast(above_ref + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    } else {
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row + @as(usize, @bitCast(@as(isize, @intCast(bs)))))), above_row[@bitCast(@as(isize, @intCast(bs - @as(c_int, 1))))], @bitCast(@as(c_long, bs)));
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                        if (antfly_codec_afz_vp9_failed() != 0) {
                            return;
                        }
                    }
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = @bitCast(@as(i8, @truncate(if (left_available != 0) @as(c_int, above_ref[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))]) else @as(c_int, 129))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
        } else {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            _ = antfly_codec_afz_memset(@ptrCast(@alignCast(above_row)), 127, @bitCast(@as(c_long, bs * @as(c_int, 2))));
            if (antfly_codec_afz_vp9_failed() != 0) {
                return;
            }
            above_row[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))] = 127;
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
    if (@as(c_int, mode) == @as(c_int, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        dc_pred[@bitCast(@as(isize, @intCast(left_available)))][@bitCast(@as(isize, @intCast(up_available)))][tx_size].?(dst, dst_stride, const_above_row, @ptrCast(@alignCast(&left_col)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        pred[mode][tx_size].?(dst, dst_stride, const_above_row, @ptrCast(@alignCast(&left_col)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_predict_intra_block(arg_xd: [*c]const MACROBLOCKD, arg_bwl_in: c_int, arg_tx_size: TX_SIZE, arg_mode: PREDICTION_MODE, arg_ref: [*c]const u8, arg_ref_stride: c_int, arg_dst: [*c]u8, arg_dst_stride: c_int, arg_aoff: c_int, arg_loff: c_int, arg_plane: c_int) void {
    var xd = arg_xd;
    _ = &xd;
    var bwl_in = arg_bwl_in;
    _ = &bwl_in;
    var tx_size = arg_tx_size;
    _ = &tx_size;
    var mode = arg_mode;
    _ = &mode;
    var ref = arg_ref;
    _ = &ref;
    var ref_stride = arg_ref_stride;
    _ = &ref_stride;
    var dst = arg_dst;
    _ = &dst;
    var dst_stride = arg_dst_stride;
    _ = &dst_stride;
    var aoff = arg_aoff;
    _ = &aoff;
    var loff = arg_loff;
    _ = &loff;
    var plane = arg_plane;
    _ = &plane;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const bw: c_int = @as(c_int, 1) << @intCast(bwl_in);
    _ = &bw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const txw: c_int = @as(c_int, 1) << @intCast(@as(c_int, tx_size));
    _ = &txw;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const have_top: c_int = @as(c_int, @intFromBool((loff != 0) or (@as(?*anyopaque, @ptrCast(@alignCast(xd.*.above_mi))) != @as(?*anyopaque, null))));
    _ = &have_top;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const have_left: c_int = @as(c_int, @intFromBool((aoff != 0) or (@as(?*anyopaque, @ptrCast(@alignCast(xd.*.left_mi))) != @as(?*anyopaque, null))));
    _ = &have_left;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const have_right: c_int = @as(c_int, @intFromBool((aoff + txw) < bw));
    _ = &have_right;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const x: c_int = aoff * @as(c_int, 4);
    _ = &x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const y: c_int = loff * @as(c_int, 4);
    _ = &y;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if ((xd.*.cur_buf.*.flags & @as(c_int, 8)) != 0) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        build_intra_predictors_high(xd, ref, ref_stride, dst, dst_stride, mode, tx_size, have_top, have_left, have_right, x, y, plane, xd.*.bd);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        return;
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    build_intra_predictors(xd, ref, ref_stride, dst, dst_stride, mode, tx_size, have_top, have_left, have_right, x, y, plane);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_init_intra_predictors() void {
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_codec_afz_vp9_once(vp9_init_intra_predictors_internal);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}

pub const mv = struct_mv;
pub const tx_probs = struct_tx_probs;
