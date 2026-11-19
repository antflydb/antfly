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

//! Raw graph reads retain a provider-owned policy lease. Only checked complete
//! lease operations cross the boundary; gate state stays in the physical owner.
const callback = @import("antfly_runtime_abi").callback_abi;
const Outcome = @import("native_outcome.zig").Outcome;
pub const Operations = struct {
    check: *const fn (Handle) Outcome(void),
    close: *const fn (Handle) Outcome(void),
};
pub const Boundary = callback.Boundary(Operations);
pub const Handle = extern struct {
    version: u32 = 1,
    context: ?*anyopaque = null,
    operations: ?*const Operations = null,
    dispatch: ?Boundary.Dispatch = null,
    pub fn validate(self: Handle) !void {
        if (self.version != 1) return error.UnsupportedVersion;
        if (self.context == null or self.operations == null or self.dispatch == null) return error.InvalidArgument;
    }
    pub fn resolve(self: Handle, comptime T: type) !*T {
        try self.validate();
        if (@intFromPtr(self.context.?) % @alignOf(T) != 0) return error.InvalidArgument;
        return @ptrCast(@alignCast(self.context.?));
    }
};
pub const Lease = struct {
    handle: Handle,
    pub fn check(self: *const Lease) !void {
        try call("check", self.handle);
    }
    pub fn release(self: *Lease) void {
        call("close", self.handle) catch @panic("incompatible storage owner");
        self.* = undefined;
    }
};
fn call(comptime name: []const u8, handle: Handle) !void {
    try handle.validate();
    const result = try Boundary.callRemote(name, handle.dispatch.?, @field(handle.operations.?.*, name), .{handle});
    if (result.failure.ptr) |text| return @import("storage_engine_errors").fromName(text[0..result.failure.len]);
}
test "raw graph lease refuses stale and missing capabilities before borrowing state" {
    const std = @import("std");
    const stale: Lease = .{ .handle = .{ .version = 0 } };
    try std.testing.expectError(error.UnsupportedVersion, stale.check());
    const absent: Lease = .{ .handle = .{} };
    try std.testing.expectError(error.InvalidArgument, absent.check());
}
