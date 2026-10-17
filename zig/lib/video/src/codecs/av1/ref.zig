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
pub const struct_Dav1dRef = common.struct_Dav1dRef;
pub const Dav1dRef = struct_Dav1dRef;
pub extern fn antfly_codec_afz_free(?*anyopaque) void;
pub extern fn antfly_codec_afz_posix_memalign(__memptr: [*c]?*anyopaque, __alignment: usize, __size: usize) c_int;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const Dav1dMemPoolBuffer = common.Dav1dMemPoolBuffer;
pub const struct_Dav1dMemPool = extern struct {
    lock: afz_pthread_mutex_t = 0,
    buf: [*c]Dav1dMemPoolBuffer = null,
    ref_cnt: c_int = 0,
    end: c_int = 0,
};
pub const Dav1dMemPool = struct_Dav1dMemPool;
pub fn dav1d_alloc_aligned_internal(sz: usize, @"align": usize) callconv(.c) ?*anyopaque {
    _ = &sz;
    _ = &@"align";
    while (true) {
        if (!!((@"align" & (@"align" -% @as(usize, 1))) != 0)) {
            unreachable;
        }
        if (!false) break;
    }
    var ptr: ?*anyopaque = undefined;
    _ = &ptr;
    if (antfly_codec_afz_posix_memalign(&ptr, @"align", sz) != 0) return null;
    return ptr;
}
pub fn dav1d_free_aligned_internal(arg_ptr: ?*anyopaque) callconv(.c) void {
    var ptr = arg_ptr;
    _ = &ptr;
    antfly_codec_afz_free(ptr);
}
pub extern fn antfly_av1_dav1d_mem_pool_push(pool: [*c]Dav1dMemPool, buf: [*c]Dav1dMemPoolBuffer) void;
pub extern fn antfly_av1_dav1d_mem_pool_pop(pool: [*c]Dav1dMemPool, size: usize) [*c]Dav1dMemPoolBuffer;
pub extern fn antfly_codec_afz_atomic_store(p: ?*anyopaque, v: c_int) void;
pub extern fn antfly_codec_afz_atomic_add(p: ?*anyopaque, v: c_int) c_int;
pub fn default_free_callback(data: [*c]const u8, user_data: ?*anyopaque) callconv(.c) void {
    _ = &data;
    _ = &user_data;
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(data)))) == user_data)) {
            unreachable;
        }
        if (!false) break;
    }
    dav1d_free_aligned_internal(user_data);
}
pub export fn antfly_av1_dav1d_ref_create(arg_size: usize) [*c]Dav1dRef {
    var size = arg_size;
    _ = &size;
    size = ((size +% @sizeOf(?*anyopaque)) -% @as(usize, 1)) & ~(@sizeOf(?*anyopaque) -% @as(c_ulong, 1));
    const data: [*c]u8 = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(size +% @sizeOf(Dav1dRef), 64)));
    _ = &data;
    if (!(data != null)) return null;
    const res: [*c]Dav1dRef = @ptrCast(@alignCast(data + size));
    _ = &res;
    res.*.const_data = blk: {
        const tmp = blk_1: {
            const tmp_2 = @as(?*anyopaque, @ptrCast(@alignCast(data)));
            res.*.data = tmp_2;
            break :blk_1 tmp_2;
        };
        res.*.user_data = tmp;
        break :blk tmp;
    };
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&res.*.ref_cnt)), 1);
    res.*.free_ref = 0;
    res.*.free_callback = default_free_callback;
    return res;
}
pub fn pool_free_callback(data: [*c]const u8, user_data: ?*anyopaque) callconv(.c) void {
    _ = &data;
    _ = &user_data;
    antfly_av1_dav1d_mem_pool_push(@ptrCast(@alignCast(@constCast(data))), @ptrCast(@alignCast(user_data)));
}
pub export fn antfly_av1_dav1d_ref_create_using_pool(pool: [*c]Dav1dMemPool, arg_size: usize) [*c]Dav1dRef {
    _ = &pool;
    var size = arg_size;
    _ = &size;
    size = ((size +% @sizeOf(?*anyopaque)) -% @as(usize, 1)) & ~(@sizeOf(?*anyopaque) -% @as(c_ulong, 1));
    const buf: [*c]Dav1dMemPoolBuffer = antfly_av1_dav1d_mem_pool_pop(pool, size +% @sizeOf(Dav1dRef));
    _ = &buf;
    if (!(buf != null)) return null;
    const res: [*c]Dav1dRef = &@as([*c]Dav1dRef, @ptrCast(@alignCast(buf)))[@bitCast(@as(isize, @intCast(-@as(c_int, 1))))];
    _ = &res;
    res.*.data = buf.*.data;
    res.*.const_data = @ptrCast(@alignCast(pool));
    antfly_codec_afz_atomic_store(@ptrCast(@alignCast(&res.*.ref_cnt)), 1);
    res.*.free_ref = 0;
    res.*.free_callback = pool_free_callback;
    res.*.user_data = @ptrCast(@alignCast(buf));
    return res;
}
pub export fn antfly_av1_dav1d_ref_dec(pref: [*c][*c]Dav1dRef) void {
    _ = &pref;
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(pref))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    const ref: [*c]Dav1dRef = pref.*;
    _ = &ref;
    if (!(ref != null)) return;
    pref.* = null;
    if (antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ref.*.ref_cnt)), -@as(c_int, 1)) == @as(c_int, 1)) {
        const free_ref: c_int = ref.*.free_ref;
        _ = &free_ref;
        ref.*.free_callback.?(@ptrCast(@alignCast(ref.*.const_data)), ref.*.user_data);
        if (free_ref != 0) {
            antfly_codec_afz_free(@ptrCast(@alignCast(ref)));
        }
    }
}
