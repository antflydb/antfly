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

//! Registered ownership of an actual completed file-builder output. A bound
//! producer and directory preparers retain fixed leases; physical bytes and
//! their stable observer survive producer stack and storage-runtime teardown.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const writer = @import("owned_run_writer.zig");
const Run = @import("repository.zig").Run;
const generation = @import("read_generation.zig");

pub const Metadata = struct { id: u64, level: u32, visibility_id: u64 = 0, gc_requested: bool = false, tombstone_count: ?u32 = null, oldest_tombstone_unix_ns: u64 = 0 };
pub const Lease = struct {
    owner: ?*Owner,
    run: Run,
    pub fn releaseWithAdmission(self: *@This(), original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
        const owner = self.owner orelse return;
        self.owner = null;
        owner.anchor.release(); // Final access; the registered final anchor remains.
    }
};
pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    output: writer.Output,
    anchor: writer.OutputAnchor = undefined,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    identity: generation.Identity = .{},
    complete: bool = false,

    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Owner), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .output = .{ .allocator = a, .manager = manager } };
        self.anchor = .{ .output = &self.output, .registration = &self.registration };
        self.output.registered = &self.anchor;
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        return self;
    }
    pub fn acquireSourceWithAdmission(self: *Owner, metadata: Metadata, original: admission.Port) !Lease {
        try original.checkpoint();
        if (!self.output.taken or self.output.result == null or self.output.bounds == null or self.output.retiring or self.output.retired or self.anchor.closing) return error.InvalidArgument;
        if (!self.identity.valid()) self.identity = try generation.allocate();
        const result = self.output.result.?;
        const count = std.math.cast(u32, result.entry_count) orelse return error.ResourceBudgetExceeded;
        if (metadata.tombstone_count) |tombstones| if (tombstones > count) return error.InvalidArgument;
        const bounds = &self.output.bounds.?;
        const run: Run = .{ .id = metadata.id, .read_identity = self.identity, .level = metadata.level, .visibility_id = metadata.visibility_id, .gc_requested = metadata.gc_requested, .size_bytes = result.size_bytes, .compression_stats = result.compression_stats, .path = self.output.path[0..self.output.path_len], .smallest_namespace_name = bounds.fields[0], .smallest_key = bounds.fields[1].?, .largest_namespace_name = bounds.fields[2], .largest_key = bounds.fields[3].?, .entry_count = count, .tombstone_count = metadata.tombstone_count, .oldest_tombstone_unix_ns = if (metadata.tombstone_count == 0) 0 else metadata.oldest_tombstone_unix_ns, .bloom_filter = result.filter, .owns_metadata = false, .owns_path = false, .owns_bloom_filter = false, .state = null };
        try self.anchor.acquire(original);
        return .{ .owner = self, .run = run };
    }
    pub fn requestRetirement(self: *Owner) void {
        if (self.anchor.closing) return;
        self.anchor.closing = true;
        self.registration.publish();
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (!self.anchor.claimed) {
            if (self.anchor.refs.load(.acquire) != 1) return false;
            self.anchor.claimed = true;
        }
        if (!try self.output.retireStep(original)) return false;
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.output.retired and self.output.tracked_bytes == 0 and self.anchor.refs.load(.acquire) == 1);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
