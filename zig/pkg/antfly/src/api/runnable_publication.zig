// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CPU partition transfer for one original guarded publication. Callers own
//! the service scope; this adapter cannot accept writes or mint completion.
const runtime = @import("antfly_runtime_abi");
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
fn execution(raw: *anyopaque) *native.Execution {
    return @ptrCast(@alignCast(raw));
}
pub fn park(raw: *anyopaque, out: *u8) callconv(.c) runtime.error_abi.Status {
    out.* = @intFromBool(execution(raw).parkPublication() catch |cause| return runtime.error_abi.statusFromError(cause));
    return .ok;
}
pub fn reenter(raw: *anyopaque, restriction: ?*const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
    const Check = struct {
        fn call(context: *anyopaque) !void {
            const value: *const runtime.runnable_abi.Reentry = @ptrCast(@alignCast(context));
            try value.checkpoint();
        }
    };
    if (restriction) |fence| fence.validate() catch |cause| return runtime.error_abi.statusFromError(cause);
    execution(raw).resumePublication(if (restriction) |fence| .{ .context = @ptrCast(@constCast(fence)), .check = Check.call } else null) catch |cause| return runtime.error_abi.statusFromError(cause);
    return .ok;
}
pub fn finish(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    execution(raw).finishPublication() catch |cause| return runtime.error_abi.statusFromError(cause);
    return .ok;
}
