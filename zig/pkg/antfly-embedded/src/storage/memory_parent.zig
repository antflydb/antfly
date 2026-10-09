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

//! Borrowed, allocation-free host accounting across independently compiled owners.
//! The parent outlives the child. Accounting callbacks never call into the child,
//! reclaim, or allocate. The separate reclamation callback runs outside child
//! accounting locks and uses bounded cache owners with non-blocking owner locks.
pub const Snapshot = extern struct {
    used_bytes: u64,
    soft_limit_bytes: u64,
    hard_limit_bytes: u64,
};

pub const Port = extern struct {
    pub const abi_version: u32 = 3;
    pub const ReserveResult = enum(c_int) { reserved, busy, denied, unsupported };
    pub const SnapshotResult = enum(c_int) { ready, busy, unsupported };
    pub const ReleaseResult = enum(c_int) { released, busy, mismatch, unsupported };
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    ptr: *anyopaque,
    reserve_fn: *const fn (*anyopaque, u64) callconv(.c) bool,
    reclaim_fn: *const fn (*anyopaque, u64) callconv(.c) u64,
    observe_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    release_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    snapshot_fn: *const fn (*anyopaque, *Snapshot) callconv(.c) void,
    epoch_fn: *const fn (*anyopaque) callconv(.c) u64,
    wait_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    notify_fn: *const fn (*anyopaque) callconv(.c) void,

    /// Allocation-free attempt: busy/denied/unsupported leave every ledger
    /// unchanged. Never wait, allocate, reclaim, or call back into the child.
    try_reserve_fn: ?*const fn (*anyopaque, u64) callconv(.c) ReserveResult = null,

    /// Busy/unsupported must not publish a snapshot or wait for any owner.
    try_snapshot_fn: ?*const fn (*anyopaque, *Snapshot) callconv(.c) SnapshotResult = null,

    /// Optional checked payload supplied by the parent owner. Consumers
    /// validate its native contract before forwarding original admission.
    /// Kept opaque so this language-neutral ABI does not import an executor.
    reclaim_admission: ?*const anyopaque = null,

    /// Allocation-free release attempt after physical ownership is retired.
    /// Busy/mismatch/unsupported leave the entire ancestor chain unchanged.
    /// A released result consumes this exact charge once; never wait or reclaim.
    try_release_fn: ?*const fn (*anyopaque, u64) callconv(.c) ReleaseResult = null,

    pub fn valid(self: Port) bool {
        return self.version == abi_version and self.struct_size == @sizeOf(Port);
    }

    pub fn available(self: Port) u64 {
        const value = self.snapshot();
        return if (value.hard_limit_bytes == 0) ~@as(u64, 0) else value.hard_limit_bytes -| value.used_bytes;
    }

    pub fn snapshot(self: Port) Snapshot {
        var value: Snapshot = undefined;
        self.snapshot_fn(self.ptr, &value);
        return value;
    }
};
