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
