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
//  *  Copyright (c) 2018 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub extern fn antfly_codec_afz_vp9_failed() c_int;
pub extern fn antfly_codec_afz_memcpy(__dst: ?*anyopaque, __src: ?*const anyopaque, __n: usize) ?*anyopaque;
pub const JobQueueRowMt = common.JobQueueRowMt;
pub export fn antfly_vp9_vp9_jobq_init(arg_jobq: [*c]JobQueueRowMt, arg_buf: [*c]u8, arg_buf_size: usize) void {
    var jobq = arg_jobq;
    _ = &jobq;
    var buf = arg_buf;
    _ = &buf;
    var buf_size = arg_buf_size;
    _ = &buf_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_base = buf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_wr = buf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_rd = buf;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_end = buf + buf_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.terminate = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_jobq_reset(arg_jobq: [*c]JobQueueRowMt) void {
    var jobq = arg_jobq;
    _ = &jobq;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_wr = jobq.*.buf_base;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.buf_rd = jobq.*.buf_base;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    jobq.*.terminate = 0;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_jobq_deinit(arg_jobq: [*c]JobQueueRowMt) void {
    var jobq = arg_jobq;
    _ = &jobq;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
    antfly_vp9_vp9_jobq_reset(jobq);
    if (antfly_codec_afz_vp9_failed() != 0) {
        return;
    }
}
pub export fn antfly_vp9_vp9_jobq_queue(arg_jobq: [*c]JobQueueRowMt, arg_job: ?*anyopaque, arg_job_size: usize) c_int {
    var jobq = arg_jobq;
    _ = &jobq;
    var job = arg_job;
    _ = &job;
    var job_size = arg_job_size;
    _ = &job_size;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ret: c_int = 0;
    _ = &ret;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (jobq.*.buf_end >= (jobq.*.buf_wr + job_size)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = antfly_codec_afz_memcpy(@ptrCast(@alignCast(jobq.*.buf_wr)), job, job_size);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        jobq.*.buf_wr = jobq.*.buf_wr + job_size;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ret = 0;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        _ = @as(c_int, 0);
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ret = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ret;
}
pub export fn antfly_vp9_vp9_jobq_dequeue(arg_jobq: [*c]JobQueueRowMt, arg_job: ?*anyopaque, arg_job_size: usize, arg_blocking: c_int) c_int {
    var jobq = arg_jobq;
    _ = &jobq;
    var job = arg_job;
    _ = &job;
    var job_size = arg_job_size;
    _ = &job_size;
    var blocking = arg_blocking;
    _ = &blocking;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    var ret: c_int = 0;
    _ = &ret;
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    if (jobq.*.buf_end >= (jobq.*.buf_rd + job_size)) {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        while (true) {
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
            if (jobq.*.buf_wr >= (jobq.*.buf_rd + job_size)) {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                _ = antfly_codec_afz_memcpy(job, @ptrCast(@alignCast(jobq.*.buf_rd)), job_size);
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                jobq.*.buf_rd = jobq.*.buf_rd + job_size;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                ret = 0;
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                break;
            } else {
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (jobq.*.terminate == @as(c_int, 1)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    ret = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
                if (blocking == @as(c_int, 1)) {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                } else {
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    ret = 1;
                    if (antfly_codec_afz_vp9_failed() != 0) {
                        return 0;
                    }
                    break;
                }
                if (antfly_codec_afz_vp9_failed() != 0) {
                    return 0;
                }
            }
            if (antfly_codec_afz_vp9_failed() != 0) {
                return 0;
            }
        }
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    } else {
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
        ret = 1;
        if (antfly_codec_afz_vp9_failed() != 0) {
            return 0;
        }
    }
    if (antfly_codec_afz_vp9_failed() != 0) {
        return 0;
    }
    return ret;
}
