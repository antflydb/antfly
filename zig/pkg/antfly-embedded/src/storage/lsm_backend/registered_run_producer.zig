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

//! Address-stable registered ownership of an unfinished SST writer and its
//! precreated final-file cleanup ticket. Driving methods and close serialize.
//! The ticket's queue joins all live tickets before destroying its header; the
//! independent allocator host keeps the actual allocator/manager alive. No
//! Backend receiver or application admission port escapes into this owner.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const io = @import("storage_io.zig");
const writer_mod = @import("owned_run_writer.zig");
const table = @import("../lsm/table_file.zig");
const cleanup = @import("output_cleanup.zig");

pub const Owner = struct {
    allocator: std.mem.Allocator,
    host: resources.AllocatorHost,
    writer: ?*writer_mod.Writer = null,
    writer_live: bool = false,
    writer_retire_offset: usize = 0,
    ticket: ?*cleanup.Ticket = null,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    closing: bool = false,
    complete: bool = false,

    /// grant_bytes is the actual storage's appendResourceBytes(0) bound.
    /// The caller funds/retains the offered grant until successful transfer.
    /// Both in/out owners remain with the caller on constructor refusal.
    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, grant: *?io.DurabilityResources, grant_bytes: u64, ticket: *?*cleanup.Ticket, expected_entries: usize, max_file_bytes: usize, options: table.StreamingEncoderOptions, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        const offered = ticket.* orelse return error.InvalidArgument;
        if (grant.* == null or offered.queue.manager != manager or offered.retirement.credit == null) return error.ResourceAccountingMismatch;
        if (offered.queue.allocator.ptr != a.ptr or offered.queue.allocator.vtable != a.vtable) return error.ResourceAccountingMismatch;
        const fixed = std.math.add(u64, @sizeOf(Owner), @sizeOf(writer_mod.Writer)) catch return error.ResourceBudgetExceeded;
        const bound = std.math.add(u64, fixed, grant_bytes) catch return error.ResourceBudgetExceeded;
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .host = host.? };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        // Initialization copies paths, allocates nothing and consumes only on
        // success. There is no fallible gap after either ownership transfer.
        try original.checkpoint();
        self.writer = try a.create(writer_mod.Writer);
        try self.writer.?.initInPlace(a, manager, grant.*.?, offered.path, expected_entries, max_file_bytes, options);
        self.writer_live = true;
        grant.* = null;
        self.ticket = offered;
        ticket.* = null;
        return self;
    }

    pub fn drivingWriter(self: *Owner) !*writer_mod.Writer {
        if (self.closing or !self.writer_live or self.writer.?.aborting or self.writer.?.retired) return error.InvalidArgument;
        return self.writer.?;
    }

    /// Transfer the path obligation to a durable completion/controller owner
    /// before installing live metadata. Caller publishes its receipt before any
    /// further fallible work. This does not acknowledge manifest durability.
    pub fn takeTicketWithAdmission(self: *Owner, original: admission.Port) !*cleanup.Ticket {
        try original.checkLifetime();
        if (self.closing or !self.writer_live or !self.writer.?.published or !self.writer.?.result_taken) return error.InvalidArgument;
        const ticket = self.ticket orelse return error.InvalidArgument;
        self.ticket = null;
        return ticket;
    }

    /// Final caller access: after publish, a cleanup worker may destroy self.
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing) return;
        self.closing = true;
        // No input borrow is read after this transition. Caller may now drop
        // pending input, including when original append admission refused.
        if (self.writer_live) self.writer.?.beginAbort();
        self.registration.publish();
    }

    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.writer_live) {
            if (original) |port| {
                try self.writer.?.abortWithAdmission(port);
            }
            // Null is the manager's explicit quiesced join path. The normal
            // admitted path above has already joined IO and retired buffers.
            self.writer.?.deinit();
            self.writer_live = false;
        }
        if (self.writer) |writer| {
            // The aggregate is larger than one cleanup quantum. Use rawFree
            // after bounded wiping; allocator.destroy would poison it again.
            if (original) |port| try port.checkpoint();
            const bytes = std.mem.asBytes(writer);
            const n = @min(bytes.len - self.writer_retire_offset, 16 * 1024);
            @memset(bytes[self.writer_retire_offset..][0..n], undefined);
            self.writer_retire_offset += n;
            if (self.writer_retire_offset != bytes.len) return false;
            self.allocator.rawFree(bytes, .of(writer_mod.Writer), @returnAddress());
            self.writer = null;
        }
        if (self.ticket) |ticket| {
            // Conservatively keep even uncertain final publication as debt.
            // The queue's durable consumer decides physical deletion; this
            // destructor never removes a possibly live final SST.
            if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
            self.ticket = null;
        }
        self.complete = true;
        return true;
    }

    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.writer == null and !self.writer_live and self.ticket == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
