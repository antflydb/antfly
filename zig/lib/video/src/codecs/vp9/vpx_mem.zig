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
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_afz_malloc(__size: usize) ?*anyopaque;
pub extern fn antfly_codec_afz_free(?*anyopaque) void;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub fn check_size_argument_overflow(arg_nmemb: u64, arg_size: u64) callconv(.c) c_int {
    var nmemb = arg_nmemb;
    _ = &nmemb;
    var size = arg_size;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    const total_size: u64 = nmemb *% size;
    _ = &total_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (nmemb == @as(u64, 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 1;
    }
    if (size > ((@as(c_ulonglong, 1) << @intCast(@as(c_ulonglong, 40))) / nmemb)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    if (total_size != @as(u64, @as(usize, @truncate(total_size)))) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        return 0;
    }
    return 1;
}
pub fn get_malloc_address_location(mem: ?*anyopaque) callconv(.c) [*c]usize {
    _ = &mem;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return @as([*c]usize, @ptrCast(@alignCast(mem))) - @as(usize, @bitCast(@as(isize, @intCast(@as(c_int, 1)))));
}
pub fn get_aligned_malloc_size(arg_size: usize, arg_align: usize) callconv(.c) u64 {
    var size = arg_size;
    _ = &size;
    var @"align" = arg_align;
    _ = &@"align";
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ((@as(u64, size) +% @as(u64, @"align")) -% @as(u64, 1)) +% @as(u64, @sizeOf(usize));
}
pub fn set_actual_malloc_address(mem: ?*anyopaque, malloc_addr: ?*const anyopaque) callconv(.c) void {
    _ = &mem;
    _ = &malloc_addr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    const malloc_addr_location: [*c]usize = get_malloc_address_location(mem);
    _ = &malloc_addr_location;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    malloc_addr_location.* = @intCast(@intFromPtr(malloc_addr));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn get_actual_malloc_address(mem: ?*anyopaque) callconv(.c) ?*anyopaque {
    _ = &mem;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const malloc_addr_location: [*c]usize = get_malloc_address_location(mem);
    _ = &malloc_addr_location;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return @ptrFromInt(malloc_addr_location.*);
}
pub export fn antfly_vp9_vpx_memalign(arg_align: usize, arg_size: usize) ?*anyopaque {
    var @"align" = arg_align;
    _ = &@"align";
    var size = arg_size;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var x: ?*anyopaque = null;
    _ = &x;
    var addr: ?*anyopaque = undefined;
    _ = &addr;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    const aligned_size: u64 = get_aligned_malloc_size(size, @"align");
    _ = &aligned_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(check_size_argument_overflow(1, aligned_size) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        return null;
    }
    addr = antfly_codec_afz_malloc(@truncate(aligned_size));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (addr != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        x = @ptrFromInt((@as(usize, @intCast(@intFromPtr(@as([*c]u8, @ptrCast(@alignCast(addr))) + @sizeOf(usize)))) +% (@"align" -% @as(usize, 1))) & ~(@"align" -% @as(usize, 1)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        set_actual_malloc_address(x, addr);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return x;
}
pub export fn antfly_vp9_vpx_malloc(arg_size: usize) ?*anyopaque {
    var size = arg_size;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return antfly_vp9_vpx_memalign(@as(c_ulong, 2) *% @sizeOf(?*anyopaque), size);
}
pub export fn antfly_vp9_vpx_calloc(arg_num: usize, arg_size: usize) ?*anyopaque {
    var num = arg_num;
    _ = &num;
    var size = arg_size;
    _ = &size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    var x: ?*anyopaque = undefined;
    _ = &x;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (!(check_size_argument_overflow(num, size) != 0)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        return null;
    }
    x = antfly_vp9_vpx_malloc(num *% size);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    if (x != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        _ = antfly_codec_afz_memset(x, 0, num *% size);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return null;
        }
    }
    return x;
}
pub export fn antfly_vp9_vpx_free(arg_memblk: ?*anyopaque) void {
    var memblk = arg_memblk;
    _ = &memblk;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (memblk != null) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        var addr: ?*anyopaque = get_actual_malloc_address(memblk);
        _ = &addr;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        antfly_codec_afz_free(addr);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
