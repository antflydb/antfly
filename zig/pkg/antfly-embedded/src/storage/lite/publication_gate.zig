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

const std = @import("std");
const native = @import("native.zig");

/// Admission for owned native commands. Retains only file-lifetime gate/IO
/// resources: it must never retain a Connection, Owner, Store or index runtime.
/// Commands acquire this before opening a writer or borrowing any mutable state.
pub const Gate = struct {
    ptr: *anyopaque,
    retain_fn: *const fn (*anyopaque) void,
    release_fn: *const fn (*anyopaque) void,
    acquire_fn: *const fn (*anyopaque, std.Io, i96) anyerror!Lease,

    pub fn retained(self: Gate) Gate {
        self.retain_fn(self.ptr);
        return self;
    }
    pub fn deinit(self: *Gate) void {
        self.release_fn(self.ptr);
        self.* = undefined;
    }
    pub fn acquire(self: Gate, io: std.Io, deadline_ns: i96) !Lease {
        return self.acquire_fn(self.ptr, io, deadline_ns);
    }
};

/// Both the process gate and kernel lease remain held until deinit. The caller
/// retains its Gate for this lifetime; no heap command or runtime is borrowed.
pub const Lease = struct {
    ptr: *anyopaque,
    /// File/allocator-cache resources live as long as the gate capability.
    io: std.Io,
    allocator: std.mem.Allocator,
    lock: native.PathWriterLock,
    allocator_cache: *native.WriterAllocatorCache,
    release_fn: *const fn (*anyopaque, *native.PathWriterLock) void,

    pub fn deinit(self: *Lease) void {
        self.release_fn(self.ptr, &self.lock);
        self.* = undefined;
    }
};
