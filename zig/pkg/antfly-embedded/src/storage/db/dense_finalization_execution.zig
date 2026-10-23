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

const runtime = @import("antfly_runtime_abi");
const admission = @import("../../common/read_admission.zig");

/// Borrowed only through preparation. After consumption, physical finish uses
/// retained execution without reviving the original caller's expired authority.
pub const Control = struct {
    grant: runtime.runnable_abi.ReservedCleanup.Grant,
    original: ?admission.Port,
    payload: admission.Payload = undefined,
    pub fn port(self: *Control) admission.Port {
        self.payload = .{ .ptr = self, .vtable = &.{ .checkpoint = checkpoint, .check_lifetime = lifetime, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
        return .init(&self.payload);
    }
    fn checked(status: runtime.error_abi.Status) !void {
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
    fn lifetime(raw: *anyopaque) !void {
        const self: *Control = @ptrCast(@alignCast(raw));
        if (self.original) |original| try original.checkLifetime();
        try self.grant.borrow.checkLifetime();
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Control = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        try checked(self.grant.borrow.checkpoint(self.grant.borrow.context));
        try lifetime(raw);
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *Control = @ptrCast(@alignCast(raw));
        try lifetime(raw);
        // The two authorities may use different clocks. Each lifetime check
        // enforces its own deadline; never compare their raw timestamps.
        return self.grant.deadline_ns;
    }
    fn begin(raw: *anyopaque) !void {
        const self: *Control = @ptrCast(@alignCast(raw));
        try checkpoint(raw);
        try checked(self.grant.borrow.park(self.grant.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    }
    fn reentry(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        lifetime(raw) catch |cause| return runtime.error_abi.statusFromError(cause);
        return .ok;
    }
    fn end(raw: *anyopaque) !void {
        const self: *Control = @ptrCast(@alignCast(raw));
        try self.grant.borrow.enterChecked(try self.grant.borrow.currentClass(), &.{ .context = self, .check = reentry });
        try checkpoint(raw);
    }
};
