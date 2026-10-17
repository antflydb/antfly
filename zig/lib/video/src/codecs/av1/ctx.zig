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
//  * Copyright © 2024, VideoLAN and dav1d authors
//  * Copyright © 2024, Two Orioles, LLC
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
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const union_alias64 = common.union_alias64;
pub const union_alias32 = common.union_alias32;
pub const union_alias16 = common.union_alias16;
pub const union_alias8 = common.union_alias8;
pub const dav1d_memset_pow2_fn = common.dav1d_memset_pow2_fn;
pub fn memset_w1(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    @as([*c]union_alias8, @ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)]))).*.u8 = @bitCast(@as(i8, @truncate(value * @as(c_int, 1))));
}
pub fn memset_w2(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    @as([*c]union_alias16, @ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)]))).*.u16 = @bitCast(@as(c_short, @truncate(value * @as(c_int, 257))));
}
pub fn memset_w4(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    @as([*c]union_alias32, @ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)]))).*.u32 = @as(c_uint, @bitCast(@as(c_int, value))) *% @as(c_uint, 16843009);
}
pub fn memset_w8(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    @as([*c]union_alias64, @ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)]))).*.u64 = @as(c_ulonglong, @bitCast(@as(c_longlong, value))) *% @as(c_ulonglong, 72340172838076673);
}
pub fn memset_w16(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    while (true) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)])), value, 16);
        if (!false) break;
    }
}
pub fn memset_w32(ptr: ?*anyopaque, value: c_int) callconv(.c) void {
    _ = &ptr;
    _ = &value;
    while (true) {
        _ = antfly_codec_afz_memset(@ptrCast(@alignCast(&@as([*c]u8, @ptrCast(@alignCast(ptr)))[@as(c_int, 0)])), value, 32);
        if (!false) break;
    }
}
pub export const antfly_av1_dav1d_memset_pow2: [6]dav1d_memset_pow2_fn = [6]dav1d_memset_pow2_fn{
    memset_w1,
    memset_w2,
    memset_w4,
    memset_w8,
    memset_w16,
    memset_w32,
};
