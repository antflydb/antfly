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
//  * Copyright © 2020, VideoLAN and dav1d authors
//  * Copyright © 2020, Two Orioles, LLC
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
pub extern fn antfly_codec_afz_trap() void;
pub const afz_pthread_mutex_t = common.afz_pthread_mutex_t;
pub const Dav1dMemPoolBuffer = common.Dav1dMemPoolBuffer;
pub const struct_Dav1dMemPool = extern struct {
    lock: afz_pthread_mutex_t = 0,
    buf: [*c]Dav1dMemPoolBuffer = null,
    ref_cnt: c_int = 0,
    end: c_int = 0,
};
pub const Dav1dMemPool = struct_Dav1dMemPool;
pub extern fn antfly_codec_afz_malloc(__size: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_free(?*anyopaque) void;
pub extern fn antfly_codec_afz_posix_memalign(__memptr: [*c]?*anyopaque, __alignment: usize, __size: usize) c_int;
pub extern fn antfly_codec_afz_mutex_init([*c]afz_pthread_mutex_t, ?*const anyopaque) c_int;
pub extern fn antfly_codec_afz_mutex_destroy([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_lock([*c]afz_pthread_mutex_t) c_int;
pub extern fn antfly_codec_afz_mutex_unlock([*c]afz_pthread_mutex_t) c_int;
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
pub fn mem_pool_destroy(pool: [*c]Dav1dMemPool) callconv(.c) void {
    _ = &pool;
    _ = antfly_codec_afz_mutex_destroy(&pool.*.lock);
    antfly_codec_afz_free(@ptrCast(@alignCast(pool)));
}
pub export fn antfly_av1_dav1d_mem_pool_push(pool: [*c]Dav1dMemPool, buf: [*c]Dav1dMemPoolBuffer) void {
    _ = &pool;
    _ = &buf;
    _ = antfly_codec_afz_mutex_lock(&pool.*.lock);
    const ref_cnt: c_int = blk: {
        const ref = &pool.*.ref_cnt;
        ref.* -= 1;
        break :blk ref.*;
    };
    _ = &ref_cnt;
    if (!(pool.*.end != 0)) {
        buf.*.next = pool.*.buf;
        pool.*.buf = buf;
        _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
        while (true) {
            if (!(ref_cnt > @as(c_int, 0))) {
                unreachable;
            }
            if (!false) break;
        }
    } else {
        _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
        dav1d_free_aligned_internal(buf.*.data);
        if (!(ref_cnt != 0)) {
            mem_pool_destroy(pool);
        }
    }
}
pub export fn antfly_av1_dav1d_mem_pool_pop(pool: [*c]Dav1dMemPool, size: usize) [*c]Dav1dMemPoolBuffer {
    _ = &pool;
    _ = &size;
    var afz_ref_cnt_2: c_int = undefined;
    _ = &afz_ref_cnt_2;
    var afz_data_1: [*c]u8 = undefined;
    _ = &afz_data_1;
    var afz_buf_0: [*c]Dav1dMemPoolBuffer = undefined;
    _ = &afz_buf_0;
    var afz_pc: c_uint = 26;
    _ = &afz_pc;
    while (true) {
        switch (afz_pc) {
            0 => {
                return null;
            },
            1 => {
                return afz_buf_0;
            },
            2 => {
                afz_pc = 17;
                continue;
            },
            3 => {
                dav1d_free_aligned_internal(@ptrCast(@alignCast(afz_data_1)));
                afz_pc = 2;
                continue;
            },
            4 => {
                afz_pc = @bitCast(@as(c_int, if ((@as(usize, @intCast(@intFromPtr(afz_buf_0))) -% @as(usize, @intCast(@intFromPtr(afz_data_1)))) != size) @as(c_int, 3) else @as(c_int, 1)));
                continue;
            },
            5 => {
                afz_data_1 = @ptrCast(@alignCast(afz_buf_0.*.data));
                afz_pc = 4;
                continue;
            },
            6 => {
                _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
                afz_pc = 5;
                continue;
            },
            7 => {
                pool.*.buf = afz_buf_0.*.next;
                afz_pc = 6;
                continue;
            },
            8 => {
                afz_buf_0.*.data = @ptrCast(@alignCast(afz_data_1));
                afz_pc = 1;
                continue;
            },
            9 => {
                afz_buf_0 = @ptrCast(@alignCast(afz_data_1 + size));
                afz_pc = 8;
                continue;
            },
            10 => {
                return null;
            },
            11 => {
                mem_pool_destroy(pool);
                afz_pc = 10;
                continue;
            },
            12 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_ref_cnt_2 != 0)) @as(c_int, 11) else @as(c_int, 10)));
                continue;
            },
            13 => {
                _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
                afz_pc = 12;
                continue;
            },
            14 => {
                afz_ref_cnt_2 = blk: {
                    const ref = &pool.*.ref_cnt;
                    ref.* -= 1;
                    break :blk ref.*;
                };
                afz_pc = 13;
                continue;
            },
            15 => {
                _ = antfly_codec_afz_mutex_lock(&pool.*.lock);
                afz_pc = 14;
                continue;
            },
            16 => {
                afz_pc = @bitCast(@as(c_int, if (!(afz_data_1 != null)) @as(c_int, 15) else @as(c_int, 9)));
                continue;
            },
            17 => {
                afz_data_1 = @ptrCast(@alignCast(dav1d_alloc_aligned_internal(size +% @sizeOf(Dav1dMemPoolBuffer), 64)));
                afz_pc = 16;
                continue;
            },
            18 => {
                _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
                afz_pc = 17;
                continue;
            },
            19 => {
                afz_pc = @bitCast(@as(c_int, if (afz_buf_0 != null) @as(c_int, 7) else @as(c_int, 18)));
                continue;
            },
            20 => {
                afz_pc = 19;
                continue;
            },
            21 => {
                pool.*.ref_cnt += 1;
                afz_pc = 20;
                continue;
            },
            22 => {
                afz_buf_0 = pool.*.buf;
                afz_pc = 21;
                continue;
            },
            23 => {
                _ = antfly_codec_afz_mutex_lock(&pool.*.lock);
                afz_pc = 22;
                continue;
            },
            24 => {
                afz_pc = if (@as(c_int, 0) != 0) @as(c_int, 26) else @as(c_int, 23);
                continue;
            },
            25 => {
                unreachable;
            },
            26 => {
                afz_pc = @bitCast(@as(c_int, if (!!((size & (@sizeOf(?*anyopaque) -% @as(c_ulong, 1))) != 0)) @as(c_int, 25) else @as(c_int, 24)));
                continue;
            },
            else => antfly_codec_afz_trap(),
        }
    }
    return undefined;
}
pub export fn antfly_av1_dav1d_mem_pool_init(ppool: [*c][*c]Dav1dMemPool) c_int {
    _ = &ppool;
    const pool: [*c]Dav1dMemPool = @ptrCast(@alignCast(antfly_codec_afz_malloc(@sizeOf(Dav1dMemPool))));
    _ = &pool;
    if (pool != null) {
        if (!(antfly_codec_afz_mutex_init(&pool.*.lock, null) != 0)) {
            pool.*.buf = null;
            pool.*.ref_cnt = 1;
            pool.*.end = 0;
            ppool.* = pool;
            return 0;
        }
        antfly_codec_afz_free(@ptrCast(@alignCast(pool)));
    }
    ppool.* = null;
    return -@as(c_int, 12);
}
pub export fn antfly_av1_dav1d_mem_pool_end(pool: [*c]Dav1dMemPool) void {
    _ = &pool;
    if (pool != null) {
        _ = antfly_codec_afz_mutex_lock(&pool.*.lock);
        var buf: [*c]Dav1dMemPoolBuffer = pool.*.buf;
        _ = &buf;
        const ref_cnt: c_int = blk: {
            const ref = &pool.*.ref_cnt;
            ref.* -= 1;
            break :blk ref.*;
        };
        _ = &ref_cnt;
        pool.*.buf = null;
        pool.*.end = 1;
        _ = antfly_codec_afz_mutex_unlock(&pool.*.lock);
        while (buf != null) {
            const data: ?*anyopaque = buf.*.data;
            _ = &data;
            buf = buf.*.next;
            dav1d_free_aligned_internal(data);
        }
        if (!(ref_cnt != 0)) {
            mem_pool_destroy(pool);
        }
    }
}
