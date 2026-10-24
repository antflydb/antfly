// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Frontend-local transport callbacks borrow the checked shared API owner.
//! The listener joins all writers before releasing this consumer lease.
const std = @import("std");
const httpx = @import("httpx");
const runnable = @import("runtime_runnable_abi.zig");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");

pub const Owner = struct {
    lease: runnable.Lease,

    pub fn init(port: runnable.Owner) !Owner {
        return .{ .lease = try runnable.Lease.init(port) };
    }
    pub fn deinit(self: *Owner) void {
        self.lease.deinit();
    }
    pub fn view(self: *Owner) httpx.server_mod.CleanupExecution {
        return .{ .context = self, .start = start, .park = park, .enter = enter, .finish = finish };
    }
    fn start(raw: *anyopaque, io: std.Io, deadline_ns: u64, bytes: u64) !*anyopaque {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const port = self.lease.owner;
        const borrow = native.IoBorrow.init(&io);
        var handle: ?*anyopaque = null;
        try checked(port.start(port.context, &.{
            .io = &borrow,
            .deadline_ns = deadline_ns,
            .has_deadline = 1,
            .class = @backingInt(runnable.Class.control),
            .slots = 1,
            .allow_wait = 0,
            .cleanup = 1,
            .retained_bytes = bytes,
            .out_handle = &handle,
        }));
        return handle orelse return error.InvalidRunnableTransition;
    }
    fn park(raw: *anyopaque, handle: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try checked(self.lease.owner.park(handle, @backingInt(runnable.Wait.output)));
    }
    fn enter(raw: *anyopaque, handle: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try checked(self.lease.owner.enter(handle, @backingInt(runnable.Class.control)));
    }
    fn finish(raw: *anyopaque, handle: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.lease.owner.finish(handle);
    }
    fn checked(status: errors.Status) !void {
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
};
