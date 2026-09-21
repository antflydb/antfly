// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Creator-owned HA publication for the compiled metadata store. No Primary,
//! allocator, file, or Io implementation is dereferenced across this boundary.
const callback = @import("../runtime_callback_abi.zig");

pub const Identity = struct { next_lsn: u64, timeline_id: u64, epoch: u64 };
pub const VTable = struct {
    lock: *const fn (*anyopaque) anyerror!void,
    unlock: *const fn (*anyopaque) void,
    check: *const fn (*anyopaque) anyerror!void,
    identity: *const fn (*anyopaque) anyerror!Identity,
    /// On success retain the promotion lock until the native outbox deletion
    /// is durable. On error release every acquired lock and retain the outbox.
    publish_and_lock: *const fn (*anyopaque, []const u8) anyerror!void,
};
pub const Boundary = callback.Boundary(VTable);
pub const Port = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    has_mirror: bool,

    pub fn lock(self: Port) !void {
        try Boundary.call("lock", self.dispatch, self.vtable.lock, .{self.ptr});
    }
    pub fn unlock(self: Port) void {
        Boundary.call("unlock", self.dispatch, self.vtable.unlock, .{self.ptr}) catch @panic("metadata HA callback ABI mismatch");
    }
    pub fn check(self: Port) !void {
        try Boundary.call("check", self.dispatch, self.vtable.check, .{self.ptr});
    }
    pub fn identity(self: Port) !Identity {
        return Boundary.call("identity", self.dispatch, self.vtable.identity, .{self.ptr});
    }
    pub fn publishAndLock(self: Port, bytes: []const u8) !void {
        try Boundary.call("publish_and_lock", self.dispatch, self.vtable.publish_and_lock, .{ self.ptr, bytes });
    }
};
