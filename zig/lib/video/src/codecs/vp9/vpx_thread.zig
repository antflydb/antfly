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
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_afz_memset(__b: ?*anyopaque, __c: c_int, __len: usize) ?*anyopaque;
pub const VPX_WORKER_STATUS_NOT_OK = common.VPX_WORKER_STATUS_NOT_OK;
pub const VPX_WORKER_STATUS_OK = common.VPX_WORKER_STATUS_OK;
pub const VPxWorkerStatus = common.VPxWorkerStatus;
pub const VPxWorkerHook = common.VPxWorkerHook;
pub const VPxWorkerImpl = common.VPxWorkerImpl;
pub const VPxWorker = extern struct {
    impl_: ?*VPxWorkerImpl = null,
    status_: VPxWorkerStatus = @import("std").mem.zeroes(VPxWorkerStatus),
    thread_name: [*c]const u8 = null,
    hook: VPxWorkerHook = null,
    data1: ?*anyopaque = null,
    data2: ?*anyopaque = null,
    had_error: c_int = 0,
};
pub const VPxWorkerInterface = extern struct {
    init: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    reset: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    sync: ?*const fn (worker: [*c]VPxWorker) callconv(.c) c_int = null,
    launch: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    execute: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
    end: ?*const fn (worker: [*c]VPxWorker) callconv(.c) void = null,
};
pub fn init(worker: [*c]VPxWorker) callconv(.c) void {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = antfly_codec_afz_memset(@ptrCast(@alignCast(worker)), 0, @sizeOf(@TypeOf(worker.*)));
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    worker.*.status_ = VPX_WORKER_STATUS_NOT_OK;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn sync(worker: [*c]VPxWorker) callconv(.c) c_int {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return @as(c_int, @intFromBool(!(worker.*.had_error != 0)));
}
pub fn reset(worker: [*c]VPxWorker) callconv(.c) c_int {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ok: c_int = 1;
    _ = &ok;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    worker.*.had_error = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (worker.*.status_ < @as(VPxWorkerStatus, VPX_WORKER_STATUS_OK)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        worker.*.status_ = VPX_WORKER_STATUS_OK;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (worker.*.status_ > @as(VPxWorkerStatus, VPX_WORKER_STATUS_OK)) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            ok = sync(worker);
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ok;
}
pub fn execute(worker: [*c]VPxWorker) callconv(.c) void {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    if (@as(?*anyopaque, @ptrCast(@alignCast(@constCast(worker.*.hook)))) != @as(?*anyopaque, null)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
        worker.*.had_error |= @as(c_int, @intFromBool(!(worker.*.hook.?(worker.*.data1, worker.*.data2) != 0)));
        if (antfly_codec_afz_vp9_failed() != 0) {
            return;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn launch(worker: [*c]VPxWorker) callconv(.c) void {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    execute(worker);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub fn end(worker: [*c]VPxWorker) callconv(.c) void {
    _ = &worker;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    worker.*.status_ = VPX_WORKER_STATUS_NOT_OK;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    _ = @as(c_int, 0);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub var g_worker_interface: VPxWorkerInterface = VPxWorkerInterface{
    .init = init,
    .reset = reset,
    .sync = sync,
    .launch = launch,
    .execute = execute,
    .end = end,
};
pub export fn antfly_vp9_vpx_get_worker_interface() [*c]const VPxWorkerInterface {
    if (antfly_codec_afz_vp9_failed() != 0) {
        return null;
    }
    return &g_worker_interface;
}
