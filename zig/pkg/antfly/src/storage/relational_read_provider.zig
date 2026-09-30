// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Checked native retained-read capability across the hidden storage archive.
//! Every archive is linked by one compiler invocation; calls still validate
//! signature/layout contracts and translate error identities at the boundary.
const std = @import("std");
const native = @import("../runtime_native_abi.zig");
const callbacks = @import("../runtime_callback_abi.zig");
const errors = @import("../runtime_error_abi.zig");
const types = @import("db/types.zig");
pub const View = @import("relational_read_view.zig").View;
pub const Fence = @import("statement_read_fence.zig").Fence;

pub const Provider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Abi.Dispatch = Abi.local_dispatch,

    pub const VTable = struct {
        open: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8, types.ScanOptions) anyerror!View,
        try_fence: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, types.ScanOptions) anyerror!?Fence = null,
    };
    const Abi = callbacks.Boundary(VTable);

    pub fn open(self: Provider, alloc: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: types.ScanOptions) !View {
        return Abi.call("open", self.dispatch, self.vtable.open, .{ self.ptr, alloc, table, from, to, opts });
    }

    pub fn tryFence(self: Provider, alloc: std.mem.Allocator, table: []const u8, opts: types.ScanOptions) !?Fence {
        const capture = self.vtable.try_fence orelse return error.SqlStatementSnapshotRequired;
        return Abi.call("try_fence", self.dispatch, capture, .{ self.ptr, alloc, table, opts });
    }
};

extern fn antfly_storage_owner_relational_read_provider(owner: ?*anyopaque, contract: *const native.TypeContract, output: *anyopaque) callconv(.c) errors.Status;

pub fn acquire(owner: ?*anyopaque) !Provider {
    var provider: Provider = undefined;
    const status = antfly_storage_owner_relational_read_provider(owner, &native.TypeContract.of(Provider), &provider);
    if (!status.isOk()) return errors.errorFromStatus(status);
    return provider;
}
