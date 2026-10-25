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

//! Registered writer records borrow physical bytes from an acquired immutable
//! directory source. Roots and plans keep only fixed references; registered
//! retirement keeps the final anchor, exact host and ancestry until they leave.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const repository = @import("repository.zig");
const Run = repository.Run;

pub const Record = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    source: ?Directory.RegisteredSource = null,
    header: repository.RunOwner = undefined,
    run: Run = undefined,
    ready: bool = false,
    closing: bool = false,
    exclusive: bool = false,
    complete: bool = false,

    pub const Revision = struct { level: ?u32 = null, gc_requested: ?bool = null };

    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory.EntryLease, revision: Revision, scope: admission.Port) !*Record {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        _ = try source.registeredRouting(original);
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Record), original);
        defer construction.retire();
        const self = try a.create(Record);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.? };
        self.header = .{ .raw = &self.run, .destroy = forbiddenDestroy, .registered = &self.registration };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        self.source = try source.retainRegisteredSourceWithAdmission(original);
        var run = self.source.?.run().*;
        // All physical state remains with the source's existing funder. This
        // record is immutable; legacy cache allocators refuse registered owners.
        run.level = revision.level orelse run.level;
        run.gc_requested = revision.gc_requested orelse run.gc_requested;
        run.owner = &self.header;
        run.output_ticket = null;
        run.owns_metadata = false;
        run.owns_path = false;
        run.bloom_filter = null;
        run.owns_bloom_filter = false;
        run.table_index = null;
        run.shared_memory = null;
        run.version_ref_pinned = false;
        run.shared_read_version = false;
        run.cached_state_index = null;
        run.cached_index_index = null;
        run.cached_table_index = null;
        if (run.path != null) run.state = null;
        self.run = run;
        self.ready = true;
        return self;
    }
    pub fn takeRunWithAdmission(self: *Record, original: admission.Port) !Run {
        try original.checkpoint();
        if (!self.ready or self.closing) return error.InvalidArgument;
        return self.run.retainOwned();
    }
    /// Validate the canonical record in constant work. A generic owned view
    /// cannot forge metadata revisions or replace its original allocator host.
    pub fn validate(run: *const Run, original: admission.Port) !*Record {
        try original.checkpoint();
        const header = run.owner orelse return error.ExecutionWaitUnsupported;
        const registration = header.registered orelse return error.ExecutionWaitUnsupported;
        if (header.destroy != forbiddenDestroy or registration.context == null or registration.credit == null) return error.InvalidArgument;
        const self: *Record = @ptrCast(@alignCast(registration.context.?));
        if (&self.registration != registration or &self.header != header or header.raw != &self.run or registration.credit.?.manager != self.manager or !self.ready or self.exclusive or self.source == null or !std.meta.eql(run.*, self.run)) return error.InvalidArgument;
        return self;
    }
    pub fn requestRetirement(self: *Record) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Record = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (!self.exclusive) {
            if (self.header.refs.load(.acquire) != 1) return false;
            // Acquisition requires an existing lease, or the serialized open
            // constructor. Closing excludes the latter; no external lease is left.
            self.header.refs.store(0, .release);
            self.exclusive = true;
        }
        if (self.source) |*source| {
            try source.releaseWithAdmission(original);
            self.source = null;
        }
        self.complete = true;
        return true;
    }
    fn forbiddenDestroy(_: *repository.RunOwner, _: std.mem.Allocator) void {
        @panic("registered writer final anchor released outside retirement");
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Record) <= 16 * 1024);
        const self: *Record = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.source == null and self.exclusive);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
