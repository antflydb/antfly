// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Checked native retained-read capability across the hidden storage archive.
//! Every archive is linked by one compiler invocation; calls still validate
//! signature/layout contracts and translate error identities at the boundary.
const std = @import("std");
const native = @import("antfly_runtime_abi").native_abi;
const callbacks = @import("antfly_local_sources").runtime_callback_abi;
const errors = @import("antfly_runtime_abi").error_abi;
const types = @import("antfly_local_sources").storage_db_types;
pub const View = @import("antfly_local_sources").storage_relational_read_view.View;
pub const Fence = @import("antfly_local_sources").storage_statement_read_fence.Fence;

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
