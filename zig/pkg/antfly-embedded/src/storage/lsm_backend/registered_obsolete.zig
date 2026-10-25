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

//! Prepaid immutable obsolete-file edits. New paths come from registered writer
//! records; deadlines never shorten. Node and path final anchors survive every
//! descendant root and generic entry drop, without a borrowed backend or port.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const obsolete = @import("obsolete_ledger.zig");
const Ledger = obsolete.Ledger;
const Account = @import("memory_account.zig").Account;
const Run = @import("repository.zig").Run;
const Record = @import("registered_writer.zig").Record;

pub const Candidate = struct {
    const Edit = union(enum) { put: Ledger.Tree.RankInsertion, remove: Ledger.Tree.RankRemoval };
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    account: ?*Account = null,
    ledger: ?Ledger = null,
    held: ?Run = null,
    payload: obsolete.Payload = .{ .path = &.{} },
    allocated: bool = false,
    offset: usize = 0,
    retirement_offset: usize = 0,
    deadline: ?u64,
    prepared: bool = false,
    applied: bool = false,
    low: usize = 0,
    high: usize = 0,
    middle: usize = 0,
    comparing: bool = false,
    compare_offset: usize = 0,
    rank: ?usize = null,
    replacing: bool = false,
    edit: ?Edit = null,
    detached: ?Ledger.Tree.Reclaimer = null,
    reclaimer: ?Ledger.Reclaimer = null,
    closing: bool = false,
    ready: bool = false,
    complete: bool = false,

    fn validateSource(source: *const Ledger, a: std.mem.Allocator, original: admission.Port) !void {
        try original.checkpoint();
        const registration = source.registered_owner orelse {
            if (source.count() != 0 or source.tree.account != null or source.tree.spare.capacity != 0 or source.spare.capacity != 0) return error.ExecutionWaitUnsupported;
            return;
        };
        if (registration.step == null or registration.step.?.advance != advance or registration.context == null or registration.credit == null) return error.InvalidArgument;
        const owner: *Candidate = @ptrCast(@alignCast(registration.context.?));
        const account = owner.account orelse return error.InvalidArgument;
        if (&owner.registration != registration or owner.manager != registration.credit.?.manager or source.tree.account != account or !account.independently_funded or account.backing.ptr != a.ptr or account.backing.vtable != a.vtable or source.spare.capacity != 0) return error.ResourceAccountingMismatch;
    }
    /// Source is a pinned immutable ledger. No arbitrary path or path-cost
    /// estimate is accepted. The registered writer holds the actual input bytes.
    pub fn startPut(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Ledger, run: *const Run, deadline: u64, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try validateSource(source, a, original);
        _ = try Record.validate(run, original);
        const path = run.path orelse return error.InvalidArgument;
        const self = try allocate(a, manager, source, path.len, deadline, original);
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.held = run.retainOwned();
        return self;
    }
    /// Rank is relative to the pinned source root, not a later live generation.
    /// The eventual coupled publication must validate that source generation.
    pub fn startRemove(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Ledger, rank: usize, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try validateSource(source, a, original);
        if (rank >= source.count()) return error.InvalidArgument;
        const self = try allocate(a, manager, source, 0, null, original);
        self.rank = rank;
        return self;
    }
    fn allocate(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Ledger, path_bytes: usize, deadline: ?u64, original: admission.Port) !*Candidate {
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        const fixed = try std.math.add(u64, @sizeOf(Candidate) + @sizeOf(Account), path_bytes);
        const bound = try std.math.add(u64, fixed, try source.tree.prepareMemoryBound(1));
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        defer construction.retire();
        const self = try a.create(Candidate);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .deadline = deadline, .high = source.count() };
        self.payload.registered = &self.registration;
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.account = try Account.createIndependentlyFunded(a);
        self.ledger = source.fork();
        if (self.ledger.?.tree.account) |account| account.release();
        self.ledger.?.tree.account = self.account.?.retain();
        self.ledger.?.registered_owner = &self.registration;
        self.ledger.?.allocator = a;
        return self;
    }
    pub fn resumeWithAdmission(self: *Candidate, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.ledger == null) return error.InvalidArgument;
        if (self.ready) return;
        const ledger = &self.ledger.?;
        if (self.deadline != null) {
            if (!self.allocated) {
                self.payload.path = try self.allocator.alloc(u8, self.held.?.path.?.len);
                self.allocated = true;
            }
            while (self.offset < self.payload.path.len) {
                try original.checkpoint();
                const end = @min(self.payload.path.len, self.offset + 16 * 1024);
                @memcpy(self.payload.path[self.offset..end], self.held.?.path.?[self.offset..end]);
                self.offset = end;
            }
            self.releaseHeld();
        }
        if (!self.prepared) {
            try ledger.tree.prepareWithAdmission(self.allocator, original);
            self.prepared = true;
        }
        while (self.rank == null) {
            try original.checkpoint();
            if (self.low == self.high) {
                self.rank = self.low;
                break;
            }
            if (!self.comparing) {
                self.middle = self.low + (self.high - self.low) / 2;
                self.compare_offset = 0;
                self.comparing = true;
            }
            const left = ledger.at(self.middle);
            const right = self.payload.path;
            const end = @min(@min(left.path.len, right.len), self.compare_offset + 16 * 1024);
            var order = std.mem.order(u8, left.path[self.compare_offset..end], right[self.compare_offset..end]);
            self.compare_offset = end;
            if (order == .eq and end < @min(left.path.len, right.len)) continue;
            if (order == .eq) order = std.math.order(left.path.len, right.len);
            self.comparing = false;
            switch (order) {
                .lt => self.low = self.middle + 1,
                .gt => self.high = self.middle,
                .eq => {
                    self.rank = self.middle;
                    self.replacing = true;
                    self.deadline = @max(self.deadline.?, left.delete_after_ns);
                },
            }
        }
        if (!self.applied) {
            if (self.edit == null) self.edit = if (self.deadline) |deadline|
                .{ .put = try Ledger.Tree.RankInsertion.init(&ledger.tree, .{ .path = self.payload.path, .deadline = deadline, .payload = &self.payload }, self.rank.?, self.replacing) }
            else
                .{ .remove = try Ledger.Tree.RankRemoval.init(&ledger.tree, self.rank.?) };
            switch (self.edit.?) {
                .put => |*edit| {
                    try edit.resumeWithAdmission(self.allocator, original);
                    if (try edit.takeRetired()) |entry| entry.deinit(self.allocator);
                },
                .remove => |*edit| {
                    try edit.resumeWithAdmission(self.allocator, original);
                    if (try edit.takeRetired()) |node| self.detached = .init(.{ .root = node });
                },
            }
            self.edit = null;
            self.applied = true;
        }
        if (self.detached) |*retired| {
            var steps: usize = 1;
            try original.checkpoint();
            std.debug.assert(retired.step(self.allocator, &steps));
            self.detached = null;
        }
        self.ready = true;
    }
    /// Returns a value root; the receiver owns its header and must pin it before
    /// using a cursor. All shared nodes/path allocations retain their funders.
    pub fn takeWithAdmission(self: *Candidate, original: admission.Port) !Ledger {
        try original.checkpoint();
        if (self.closing or !self.ready or self.ledger == null) return error.InvalidArgument;
        const ledger = self.ledger.?;
        self.ledger = null;
        return ledger;
    }
    pub fn requestRetirement(self: *Candidate) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    fn releaseHeld(self: *Candidate) void {
        if (self.held) |*run| run.deinit(self.allocator);
        self.held = null;
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Candidate = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.ledger) |ledger| {
            if (self.reclaimer == null) {
                self.reclaimer = .init(ledger);
                self.ledger.?.tree.root = null;
            }
        }
        if (self.reclaimer) |*cursor| {
            var steps: usize = 1;
            if (!cursor.step(self.allocator, &steps)) return false;
            self.reclaimer = null;
        }
        if (self.edit) |*edit| {
            switch (edit.*) {
                .put => |*put| if (try put.takeRetired()) |entry| {
                    entry.deinit(self.allocator);
                },
                .remove => |*remove| if (try remove.takeRetired()) |node| {
                    self.detached = .init(.{ .root = node });
                },
            }
            self.edit = null;
        }
        self.ledger = null;
        if (self.detached) |*cursor| {
            var steps: usize = 1;
            if (!cursor.step(self.allocator, &steps)) return false;
            self.detached = null;
        }
        self.releaseHeld();
        if (self.account) |account| if (account.refs.load(.acquire) != 1) return false;
        if (self.payload.refs.load(.acquire) != 1) return false;
        if (self.allocated) {
            if (original) |port| try port.checkpoint();
            const bytes = self.payload.path;
            const end = @min(bytes.len, self.retirement_offset + 16 * 1024);
            @memset(bytes[self.retirement_offset..end], undefined);
            self.retirement_offset = end;
            if (end != bytes.len) return false;
            if (bytes.len != 0) self.allocator.rawFree(bytes, .of(u8), @returnAddress());
            self.payload.path = &.{};
            self.allocated = false;
        }
        if (self.account) |account| {
            if (original) |port| try port.checkpoint();
            std.debug.assert(account.bytes.load(.acquire) == @sizeOf(Account));
            self.account = null;
            account.release();
        }
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Candidate) <= 16 * 1024);
        const self: *Candidate = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.ledger == null and self.reclaimer == null and self.edit == null and self.detached == null and self.account == null and self.held == null and !self.allocated and self.payload.refs.load(.acquire) == 1);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
