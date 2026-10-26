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

//! Registered exclusive handoff from a completed flush to coupled live roots.
//! This owner protects file existence at metadata installation. It does not
//! acknowledge manifest/WAL durability or permit an early immutable retirement.
//! Like Run.commitOutput, the live-root handoff ends unpublished-file deletion
//! authority. Registered roots retain physical metadata; manifest/IO owners must
//! keep their own accepted completion obligations. Cancelling this owner never
//! turns a live file back into an unpublished deletion candidate.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Builder = @import("registered_flush_builder.zig");
const Output = @import("registered_run_output.zig");
const Cleanup = @import("output_cleanup.zig");
const Publication = @import("registered_publication.zig");
const Directory = @import("run_directory.zig").Directory;
const Run = @import("repository.zig").Run;
const slice = .lsm_table_builder_working_set;

const Node = struct {
    next: ?*Node = null,
    credit: resources.Reservation,
    output: ?*Output.Owner = null,
    ticket: ?*Cleanup.Ticket = null,
    metadata: Output.Metadata = undefined,
    rows: usize = 0,
    tombstones: u32 = 0,
    run: ?Run = null,
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    queue: *Cleanup.Queue,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    pending_credit: ?resources.Reservation = null,
    head: ?*Node = null,
    tail: ?*Node = null,
    pending: ?*Node = null,
    // Borrowed only by synchronous collection. Retirement clears every borrow
    // before publishing its independently owned cleanup continuation.
    builder: ?*Builder.Owner = null,
    range: Builder.Owner.CompletedRange = undefined,
    count: u64 = 0,
    collected: bool = false,
    frame: ?*Publication.Candidate = null,
    receipt: ?Publication.Candidate.PreparedReceipt = null,
    next_validation: ?*Node = null,
    search: ?Directory.IdSearch = null,
    validated: bool = false,
    live: bool = false,
    closing: bool = false,
    complete: bool = false,

    /// The builder must remain stable until collection completes or is aborted.
    /// No completed output may already have been taken by another receiver.
    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, builder: *Builder.Owner, original: admission.Port) !*Owner {
        try original.checkpoint();
        if (builder.manager != manager or builder.allocator.ptr != a.ptr or builder.allocator.vtable != a.vtable) return error.ResourceAccountingMismatch;
        const queue = builder.queue;
        try queue.retainProducerWithAdmission(original);
        var queue_retained = true;
        defer if (queue_retained) queue.releaseProducer();
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(slice, @sizeOf(Owner), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .queue = queue, .builder = builder };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = retire }, destroy, original);
        registered = true;
        host = null;
        queue_retained = false;
        errdefer self.requestRetirement();
        self.range = try builder.claimCompletedWithAdmission(self, original);
        return self;
    }

    pub fn collectWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.live) return error.InvalidArgument;
        if (self.collected) return;
        const builder = self.builder orelse return error.InvalidArgument;
        while (self.count < self.range.end_id - self.range.first_id) {
            try original.checkpoint();
            if (self.pending == null) {
                if (self.pending_credit == null) self.pending_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, @sizeOf(Node), original);
                const node = try self.allocator.create(Node);
                node.* = .{ .credit = self.pending_credit.? };
                self.pending_credit = null;
                if (self.tail) |tail| tail.next = node else self.head = node;
                self.tail = node;
                self.pending = node;
            }
            const node = self.pending.?;
            if (node.output == null) {
                const completed = (try builder.takeCompletedForOwnerWithAdmission(self, original)) orelse return error.InvalidArgument;
                // Fixed transfer receipt precedes every subsequent rejection.
                node.output = completed.output;
                node.ticket = completed.ticket;
                node.metadata = .{ .id = completed.id, .level = 0, .visibility_id = completed.visibility_id };
                node.rows = completed.rows;
                node.tombstones = completed.tombstones;
            }
            if (node.metadata.id != self.range.first_id + self.count or node.metadata.visibility_id != self.range.end_id - 1 or node.tombstones > node.rows) return error.InvalidArgument;
            if (node.run == null) {
                var held = try node.output.?.acquireSourceWithAdmission(node.metadata, original);
                defer held.releaseWithAdmission(null) catch unreachable;
                node.run = held.run;
            }
            const run = &node.run.?;
            if (run.entry_count != node.rows or node.ticket.?.queue != self.queue or !std.mem.eql(u8, run.path.?, node.ticket.?.path)) return error.InvalidArgument;
            self.count += 1;
            self.pending = null;
        }
        if (!try builder.completedExhaustedForOwnerWithAdmission(self, original)) return error.InvalidArgument;
        self.collected = true;
        self.builder = null;
    }

    pub const View = struct { output: *Output.Owner, metadata: Output.Metadata, rows: usize, tombstones: u32 };
    pub const Cursor = struct {
        owner: *Owner,
        next: ?*Node,
        /// Synchronous borrow only. The batch creator must outlive the cursor
        /// and every returned view; callers never receive deletion tickets.
        pub fn nextWithAdmission(self: *Cursor, original: admission.Port) !?View {
            try original.checkpoint();
            if (self.owner.closing or !self.owner.collected) return error.InvalidArgument;
            const node = self.next orelse return null;
            self.next = node.next;
            return .{ .output = node.output.?, .metadata = node.metadata, .rows = node.rows, .tombstones = node.tombstones };
        }
    };
    pub fn cursorWithAdmission(self: *Owner, original: admission.Port) !Cursor {
        try original.checkpoint();
        if (self.closing or !self.collected) return error.InvalidArgument;
        return .{ .owner = self, .next = self.head };
    }

    /// Rebinding discards only borrowed search state. The old frame may retire
    /// afterward; the new frame must remain alive until unbinding/installation.
    pub fn bindPublicationWithAdmission(self: *Owner, frame: *Publication.Candidate, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing or self.live or !self.collected or self.count == 0) return error.InvalidArgument;
        if (frame.manager != self.manager or frame.allocator.ptr != self.allocator.ptr or frame.allocator.vtable != self.allocator.vtable) return error.ResourceAccountingMismatch;
        const receipt = try frame.preparedReceiptWithAdmission(original);
        self.frame = frame;
        self.receipt = receipt;
        self.next_validation = self.head;
        self.search = null;
        self.validated = false;
    }

    pub fn validatePublicationWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.live) return error.InvalidArgument;
        const frame = self.frame orelse return error.InvalidArgument;
        try frame.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
        const roots = try frame.preparedRootsWithAdmission(original);
        const directory = roots.directory orelse return error.InvalidArgument;
        while (self.next_validation) |node| {
            try original.checkpoint();
            if (self.search == null) self.search = directory.locateId(&node.run.?);
            try self.search.?.resumeWithAdmission(original);
            const found = (self.search.?.matched orelse return error.InvalidArgument).entry.run;
            const run = &node.run.?;
            if (!std.meta.eql(found.read_identity, run.read_identity) or found.level != run.level or found.visibility_id != run.visibility_id or found.size_bytes != run.size_bytes or found.entry_count != run.entry_count) return error.CompactionPlanningStale;
            self.search = null;
            self.next_validation = node.next;
        }
        self.validated = true;
    }

    /// Caller owns backend.mu/publication authority. Coverage was checked in
    /// admitted quanta beforehand. The fixed tail changes file ownership only
    /// after the Backend's coupled root swap. It makes no durability receipt.
    pub fn installWithAdmission(self: *Owner, backend: anytype, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing or self.live or !self.validated) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable or backend.options.unpublished_outputs != self.queue) return error.ResourceAccountingMismatch;
        const frame = self.frame orelse return error.InvalidArgument;
        try frame.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
        try backend.installRegisteredPublicationWithAdmission(frame, original);
        // No checkpoint, allocation, IO or per-file work can interrupt this
        // receipt after the swap. Cancellation can no longer queue live SSTs.
        self.live = true;
        self.frame = null;
        self.receipt = null;
        self.next_validation = null;
        self.search = null;
    }

    /// Final caller access; no builder/frame/backend/execution borrow escapes.
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing) return;
        self.closing = true;
        self.builder = null;
        self.frame = null;
        self.receipt = null;
        self.next_validation = null;
        self.search = null;
        self.registration.publish();
    }
    fn retire(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.head) |node| {
            if (node.ticket) |ticket| {
                if (self.live) ticket.destroy() else if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
                node.ticket = null;
            }
            if (node.output) |output| output.requestRetirement();
            node.output = null;
            self.head = node.next;
            var credit = node.credit;
            self.allocator.destroy(node);
            credit.release();
            return false;
        }
        if (self.pending_credit) |*credit| credit.release();
        self.pending_credit = null;
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024 and @sizeOf(Node) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.head == null);
        const a = self.allocator;
        const host = self.host;
        const queue = self.queue;
        a.destroy(self);
        queue.releaseProducer();
        host.release();
    }
};
