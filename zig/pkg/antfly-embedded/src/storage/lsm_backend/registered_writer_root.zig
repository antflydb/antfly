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

//! One prepaid writer-root edit. An independent account/final anchor funds all
//! copied nodes, spare pools and root headers through descendants and readers.
//! Input/output stores contain only registered immutable writer records.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Store = @import("run_store.zig").Store;
const Account = @import("memory_account.zig").Account;
const Record = @import("registered_writer.zig").Record;
const Run = @import("repository.zig").Run;
const Compare = @import("run_comparison.zig").Cursor;

pub const Candidate = struct {
    const Edit = union(enum) { put: Store.Tree.RankInsertion, remove: Store.Tree.RankRemoval };
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    account: ?*Account = null,
    store: ?*Store = null,
    held: ?Run = null,
    prepared: bool = false,
    low: usize = 0,
    high: usize = 0,
    middle: usize = 0,
    comparison: ?Compare = null,
    edit: ?Edit = null,
    detached: ?Store.Tree.Reclaimer = null,
    applied: bool = false,
    removing: bool,
    rank: ?usize = null,
    replacing: bool = false,
    allow_replace: bool,
    reclaimer: ?Store.Reclaimer = null,
    closing: bool = false,
    ready: bool = false,
    complete: bool = false,

    pub fn validateSource(source: *const Store, a: std.mem.Allocator, original: admission.Port) !void {
        try original.checkpoint();
        const registration = source.registered_owner orelse {
            if (source.count() != 0 or source.tree.account != null or source.tree.spare.capacity != 0) return error.ExecutionWaitUnsupported;
            return;
        };
        if (registration.step == null or registration.step.?.advance != advance or registration.context == null or registration.credit == null) return error.InvalidArgument;
        const owner: *Candidate = @ptrCast(@alignCast(registration.context.?));
        const account = owner.account orelse return error.InvalidArgument;
        if (&owner.registration != registration or owner.manager != registration.credit.?.manager or source.tree.account != account or source.registered_account != account or !account.independently_funded or account.backing.ptr != a.ptr or account.backing.vtable != a.vtable) return error.ResourceAccountingMismatch;
    }
    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Store, run: *const Run, replace: bool, scope: admission.Port) !*Candidate {
        return startEdit(a, manager, source, run, replace, false, scope);
    }
    pub fn startRemove(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Store, run: *const Run, scope: admission.Port) !*Candidate {
        return startEdit(a, manager, source, run, false, true, scope);
    }
    fn startEdit(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Store, run: *const Run, replace: bool, removing: bool, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try validateSource(source, a, original);
        _ = try Record.validate(run, original);
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        const fixed = @sizeOf(Candidate) + @sizeOf(Account) + @sizeOf(Store);
        const bound = try std.math.add(u64, fixed, try source.tree.prepareMemoryBound(1));
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        defer construction.retire();
        const self = try a.create(Candidate);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .allow_replace = replace, .removing = removing };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.held = run.retainOwned();
        self.account = try Account.createIndependentlyFunded(a);
        try original.checkpoint();
        const store = try a.create(Store);
        store.* = source.fork();
        // The fork retains old nodes and their own accounts. All new pools and
        // path copies use this frame's prepaid, independently funded account.
        if (store.tree.account) |account| account.release();
        if (store.registered_account) |account| account.release();
        store.tree.account = self.account.?.retain();
        store.registered_account = self.account.?.retain();
        store.registered_owner = &self.registration;
        self.store = store;
        self.high = store.count();
        return self;
    }
    pub fn resumeWithAdmission(self: *Candidate, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.store == null) return error.InvalidArgument;
        if (self.ready) return;
        const store = self.store.?;
        if (!self.prepared) {
            try store.tree.prepareWithAdmission(self.allocator, original);
            self.prepared = true;
        }
        while (self.rank == null) {
            try original.checkpoint();
            if (self.low == self.high) {
                if (self.removing) return error.CompactionPlanningStale;
                self.rank = self.low;
                break;
            }
            if (self.comparison == null) {
                self.middle = self.low + (self.high - self.low) / 2;
                self.comparison = .init(.read, .{ .run = store.at(self.middle) }, .{ .run = &self.held.? });
            }
            const order = try self.comparison.?.resumeWithAdmission(original);
            self.comparison = null;
            switch (order) {
                .lt => self.low = self.middle + 1,
                .gt => self.high = self.middle,
                .eq => {
                    if (self.removing) {
                        // Read order does not include every metadata revision.
                        // A captured GC/level record must still be this exact owner.
                        if (store.at(self.middle).owner != self.held.?.owner) return error.CompactionPlanningStale;
                    } else if (!self.allow_replace) return error.DuplicateRun;
                    self.rank = self.middle;
                    self.replacing = true;
                },
            }
        }
        if (!self.applied) {
            if (self.edit == null) {
                // Publish a tag only after the fallible constructor succeeds.
                // Refusal must not leave a tagged, uninitialized cursor.
                if (self.removing) {
                    const edit = try Store.Tree.RankRemoval.init(&store.tree, self.rank.?);
                    self.edit = .{ .remove = edit };
                } else {
                    const edit = try store.beginRegisteredInsertion(self.held.?, self.rank.?, self.replacing, original);
                    self.edit = .{ .put = edit };
                }
            }
            switch (self.edit.?) {
                .put => |*edit| {
                    try edit.resumeWithAdmission(self.allocator, original);
                    if (try edit.takeRetired()) |retired| retired.deinit(self.allocator);
                },
                .remove => |*edit| {
                    try edit.resumeWithAdmission(self.allocator, original);
                    if (try edit.takeRetired()) |node| self.detached = .init(.{ .root = node });
                },
            }
            self.edit = null;
            self.applied = true;
        }
        if (self.detached) |*cursor| {
            try original.checkpoint();
            var steps: usize = 1;
            std.debug.assert(cursor.step(self.allocator, &steps));
            self.detached = null;
        }
        self.ready = true;
    }
    pub fn takeWithAdmission(self: *Candidate, original: admission.Port) !*Store {
        try original.checkpoint();
        if (self.closing or !self.ready or self.store == null) return error.InvalidArgument;
        const store = self.store.?;
        self.store = null;
        self.releaseHeld();
        return store;
    }
    pub fn requestRetirement(self: *Candidate) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    fn releaseHeld(self: *Candidate) void {
        if (self.held) |*held| held.deinit(self.allocator);
        self.held = null;
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Candidate = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.store) |store| {
            if (self.reclaimer == null) {
                self.reclaimer = .init(store);
                // Transfer the root and pool to its cursor. A partial insertion
                // can still own a detached old entry until the tree is gone.
                store.tree.root = null;
            }
            var steps: usize = 1;
            if (!self.reclaimer.?.step(self.allocator, &steps)) return false;
            if (self.edit) |*edit| {
                switch (edit.*) {
                    .put => |*put| if (try put.takeRetired()) |retired| {
                        retired.deinit(self.allocator);
                    },
                    .remove => |*remove| if (try remove.takeRetired()) |node| {
                        self.detached = .init(.{ .root = node });
                    },
                }
                self.edit = null;
            }
            if (self.detached) |*cursor| {
                if (original) |port| try port.checkpoint();
                var detached_steps: usize = 1;
                if (!cursor.step(self.allocator, &detached_steps)) return false;
                self.detached = null;
            }
            if (original) |port| try port.checkpoint();
            self.reclaimer.?.finish(self.allocator);
            self.reclaimer = null;
            self.store = null;
        }
        self.releaseHeld();
        if (self.account) |account| {
            if (account.refs.load(.acquire) != 1) return false;
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
        std.debug.assert(self.complete and self.store == null and self.account == null and self.held == null and self.reclaimer == null and self.edit == null and self.detached == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
