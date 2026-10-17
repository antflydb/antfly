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
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const struct_Dav1dRef = common.struct_Dav1dRef;
pub const struct_Dav1dUserData = extern struct {
    data: [*c]const u8 = null,
    ref: [*c]struct_Dav1dRef = null,
};
pub const struct_Dav1dDataProps = extern struct {
    timestamp: i64 = 0,
    duration: i64 = 0,
    offset: i64 = 0,
    size: usize = 0,
    user_data: struct_Dav1dUserData = @import("std").mem.zeroes(struct_Dav1dUserData),
};
pub const Dav1dDataProps = struct_Dav1dDataProps;
pub const struct_Dav1dData = extern struct {
    data: [*c]const u8 = null,
    sz: usize = 0,
    ref: [*c]struct_Dav1dRef = null,
    m: Dav1dDataProps = @import("std").mem.zeroes(Dav1dDataProps),
};
pub const Dav1dData = struct_Dav1dData;
pub const Dav1dRef = struct_Dav1dRef;
pub extern fn antfly_codec_afz_atomic_add(p: ?*anyopaque, v: c_int) c_int;
pub extern fn antfly_av1_dav1d_ref_create(size: usize) [*c]Dav1dRef;
pub extern fn antfly_av1_dav1d_ref_dec(ref: [*c][*c]Dav1dRef) void;
pub fn dav1d_ref_inc(ref: [*c]Dav1dRef) callconv(.c) void {
    _ = &ref;
    _ = antfly_codec_afz_atomic_add(@ptrCast(@alignCast(&ref.*.ref_cnt)), 1);
}
pub export fn antfly_av1_dav1d_data_create_internal(buf: [*c]Dav1dData, sz: usize) [*c]u8 {
    _ = &buf;
    _ = &sz;
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(buf))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        return null;
    }
    if (sz > (@as(c_ulong, 18446744073709551615) / @as(c_ulong, 2))) return null;
    buf.*.ref = antfly_av1_dav1d_ref_create(sz);
    if (!(buf.*.ref != null)) return null;
    buf.*.data = @ptrCast(@alignCast(buf.*.ref.*.const_data));
    buf.*.sz = sz;
    antfly_av1_dav1d_data_props_set_defaults(&buf.*.m);
    buf.*.m.size = sz;
    return @ptrCast(@alignCast(buf.*.ref.*.data));
}
pub export fn antfly_av1_dav1d_data_ref(dst: [*c]Dav1dData, src: [*c]const Dav1dData) void {
    _ = &dst;
    _ = &src;
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(dst))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(dst.*.data)))) == @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(src)))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    if (src.*.ref != null) {
        while (true) {
            if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(src.*.data)))) != @as(?*anyopaque, null))) {
                unreachable;
            }
            if (!false) break;
        }
        dav1d_ref_inc(src.*.ref);
    }
    if (src.*.m.user_data.ref != null) {
        dav1d_ref_inc(src.*.m.user_data.ref);
    }
    dst.* = src.*;
}
pub export fn antfly_av1_dav1d_data_props_copy(dst: [*c]Dav1dDataProps, src: [*c]const Dav1dDataProps) void {
    _ = &dst;
    _ = &src;
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(dst))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(src)))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    antfly_av1_dav1d_ref_dec(&dst.*.user_data.ref);
    dst.* = src.*;
    if (dst.*.user_data.ref != null) {
        dav1d_ref_inc(dst.*.user_data.ref);
    }
}
pub export fn antfly_av1_dav1d_data_props_set_defaults(props: [*c]Dav1dDataProps) void {
    _ = &props;
    while (true) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(props))) != @as(?*anyopaque, null))) {
            unreachable;
        }
        if (!false) break;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(props)), 0, @sizeOf(@TypeOf(props.*)));
    props.*.timestamp = -@as(c_longlong, 9223372036854775807) - @as(c_longlong, 1);
    props.*.offset = -@as(c_int, 1);
}
pub export fn antfly_av1_dav1d_data_props_unref_internal(props: [*c]Dav1dDataProps) void {
    _ = &props;
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(props))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        return;
    }
    var user_data_ref: [*c]struct_Dav1dRef = props.*.user_data.ref;
    _ = &user_data_ref;
    antfly_av1_dav1d_data_props_set_defaults(props);
    antfly_av1_dav1d_ref_dec(&user_data_ref);
}
pub export fn antfly_av1_dav1d_data_unref_internal(buf: [*c]Dav1dData) void {
    _ = &buf;
    if (!(@as(?*anyopaque, @ptrCast(@alignCast(buf))) != @as(?*anyopaque, null))) {
        while (true) {
            if (!false) break;
        }
        while (true) {
            if (!false) break;
        }
        return;
    }
    var user_data_ref: [*c]struct_Dav1dRef = buf.*.m.user_data.ref;
    _ = &user_data_ref;
    if (buf.*.ref != null) {
        if (!(@as(?*anyopaque, @ptrCast(@alignCast(@constCast(buf.*.data)))) != @as(?*anyopaque, null))) {
            while (true) {
                if (!false) break;
            }
            while (true) {
                if (!false) break;
            }
            return;
        }
        antfly_av1_dav1d_ref_dec(&buf.*.ref);
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(buf)), 0, @sizeOf(@TypeOf(buf.*)));
    antfly_av1_dav1d_data_props_set_defaults(&buf.*.m);
    antfly_av1_dav1d_ref_dec(&user_data_ref);
}
