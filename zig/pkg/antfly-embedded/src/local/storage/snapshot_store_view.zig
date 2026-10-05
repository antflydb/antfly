// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Read-only adapter for algorithms expressed against Store. Every nested
//! scan forks the caller's single immutable snapshot, never the current root.
const std = @import("std");
const erased = @import("backend_erased.zig");
const types = @import("backend_types.zig");

/// The transaction address and snapshot must outlive this borrowed facade and
/// every nested reader. No write or ownership-transfer operation is supported.
pub fn borrow(alloc: std.mem.Allocator, txn: *erased.ReadTxn) erased.Store {
    return .{ .allocator = alloc, .ptr = txn, .vtable = &.{
        .deinit = deinit,
        .capabilities = capabilities,
        .begin_read = beginRead,
        .begin_write = beginWrite,
        .begin_batch = beginBatch,
    } };
}

pub fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}
fn capabilities(_: *anyopaque) types.Capabilities {
    return .{ .read_snapshots = .snapshot };
}
fn beginRead(_: std.mem.Allocator, ptr: *anyopaque) !erased.ReadTxn {
    const txn: *erased.ReadTxn = @ptrCast(@alignCast(ptr));
    return txn.forkRead();
}
fn beginWrite(_: std.mem.Allocator, _: *anyopaque) !erased.WriteTxn {
    return error.ReadOnlyTransaction;
}
fn beginBatch(_: std.mem.Allocator, _: *anyopaque) !erased.Batch {
    return error.ReadOnlyTransaction;
}
