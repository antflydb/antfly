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
    ptr: *anyopaque,
    reserve_fn: *const fn (*anyopaque, u64) callconv(.c) bool,
    reclaim_fn: *const fn (*anyopaque, u64) callconv(.c) u64,
    observe_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    release_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    snapshot_fn: *const fn (*anyopaque, *Snapshot) callconv(.c) void,
    epoch_fn: *const fn (*anyopaque) callconv(.c) u64,
    wait_fn: *const fn (*anyopaque, u64) callconv(.c) void,
    notify_fn: *const fn (*anyopaque) callconv(.c) void,

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
