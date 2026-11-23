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

//! Bounded control-lane bridge to the existing replica checkpoint owner.
//! Retention is local durability, not a metadata grant or quorum proof.
const std = @import("std");
const operation = @import("antfly_local_sources").api_operation;
pub const RetainedSnapshot = @import("snapshot_payload_store.zig").RetainedSnapshot;
pub const RetainedReader = @import("snapshot_payload_store.zig").RetainedReader;
pub const CapturedPayload = @import("snapshot_payload_store.zig").CapturedPayload;

/// Synchronous durable capture barrier, before source quorum certification or
/// signing. Implementations must release their file guard before returning.
pub const CaptureObserver = struct {
    ptr: *anyopaque,
    record: *const fn (*anyopaque, RetainedSnapshot) anyerror!void,
};

pub const Request = struct {
    group_id: u64,
    index: u64,
    term: u64,
    reference: [32]u8,
    /// Present for a signed producer intent. The actual provider checks it
    /// before pinning; recovery uses the metadata saved with that exact intent.
    metadata_digest: ?[32]u8 = null,

    pub fn validate(self: @This()) !void {
        if (self.group_id == 0 or self.index == 0 or self.term == 0 or std.mem.allEqual(u8, &self.reference, 0)) return error.InvalidSnapshotRetention;
    }
};

pub const Source = struct {
    ptr: *anyopaque,
    retain: *const fn (*anyopaque, std.mem.Allocator, Request, operation.RequestContext) anyerror!RetainedSnapshot,
    /// Opens only. The callback releases replica serialization before returning;
    /// its IO runtime must outlive the returned descriptor. No replica-state
    /// pointer may escape that serialization.
    reopen: ?*const fn (*anyopaque, std.mem.Allocator, Request, operation.RequestContext) anyerror!RetainedReader = null,
    /// Only the producer's durably authorized retirement lane may call this.
    /// It remains available after leadership loss and replica removal.
    release: ?*const fn (*anyopaque, std.mem.Allocator, Request, ?CapturedPayload, operation.RequestContext) anyerror!void = null,

    pub fn releaseRetained(self: @This(), alloc: std.mem.Allocator, request: Request, expected: ?CapturedPayload, context: operation.RequestContext) !void {
        try context.ensureActive();
        try request.validate();
        const release = self.release orelse return error.NativeSnapshotRequired;
        try release(self.ptr, alloc, request, expected, context);
    }

    pub fn retainPublished(self: @This(), alloc: std.mem.Allocator, request: Request, context: operation.RequestContext) !RetainedSnapshot {
        try context.ensureActive();
        try request.validate();
        return self.retain(self.ptr, alloc, request, context);
    }

    pub fn recoverCaptured(self: @This(), alloc: std.mem.Allocator, request: Request, expected: CapturedPayload, context: operation.RequestContext) !RetainedSnapshot {
        try context.ensureActive();
        try request.validate();
        const reopen = self.reopen orelse return error.NativeSnapshotRequired;
        var reader = try reopen(self.ptr, alloc, request, context);
        defer reader.deinit();
        return reader.readCaptured(alloc, expected, context);
    }

    pub fn recoverRetained(self: @This(), alloc: std.mem.Allocator, request: Request, context: operation.RequestContext) !RetainedSnapshot {
        try context.ensureActive();
        try request.validate();
        const reopen = self.reopen orelse return error.NativeSnapshotRequired;
        var reader = try reopen(self.ptr, alloc, request, context);
        defer reader.deinit();
        return reader.readWithContext(alloc, context);
    }
};
