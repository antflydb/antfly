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

//! Registered ownership of a private edit across all six directory indexes.
//! The source is an independently pinned value header, never a backend borrow.
//! One account/final anchor funds every new node, pool and result header.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const Account = @import("memory_account.zig").Account;
const fields = .{ "tree", "ids", "bounds", "ends", "levels", "generations" };

pub const Candidate = struct {
    const Edit = union(enum) { put: Directory.Put, remove: Directory.Removal };
    const State = struct {
        source: ?Directory = null,
        directory: ?*Directory = null,
        held: ?Directory.RegisteredSource = null,
        validation: ?Directory.ReadSearch = null,
        pools: ?Directory.PutPools = null,
        edit: ?Edit = null,
        cleanup: ?Directory.Reclaimer = null,
        source_cleanup: bool = false,
    };
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    account: ?*Account = null,
    routing: ?Directory.Routing = null,
    state: ?*State = null,
    state_retiring: bool = false,
    state_offset: usize = 0,
    remove: bool,
    ready: bool = false,
    closing: bool = false,
    complete: bool = false,

    fn validateSource(source: *const Directory, a: std.mem.Allocator, original: admission.Port) !?Directory.Routing {
        try original.checkpoint();
        const registration = source.registered_owner orelse {
            if (source.registered_account != null or source.total_run_bytes != 0 or source.memory_run_count != 0) return error.ExecutionWaitUnsupported;
            inline for (fields) |field| {
                const tree = @field(source, field);
                if (tree.root != null or tree.spare.capacity != 0) return error.ExecutionWaitUnsupported;
                if (tree.account) |account| if (account.backing.ptr != a.ptr or account.backing.vtable != a.vtable or account.bytes.load(.acquire) != @sizeOf(Account)) return error.ResourceAccountingMismatch;
            }
            return null;
        };
        if (registration.step == null or registration.step.?.advance != advance or registration.context == null or registration.credit == null) return error.InvalidArgument;
        const owner: *Candidate = @ptrCast(@alignCast(registration.context.?));
        const account = owner.account orelse return error.InvalidArgument;
        if (&owner.registration != registration or registration.credit.?.manager != owner.manager or source.registered_account != account or !account.independently_funded or account.backing.ptr != a.ptr or account.backing.vtable != a.vtable) return error.ResourceAccountingMismatch;
        inline for (fields) |field| if (@field(source, field).account != account) return error.ResourceAccountingMismatch;
        return owner.routing;
    }
    pub fn startPut(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, lease: *const Directory.EntryLease, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        const routing = try validateSource(source, a, original);
        const incoming = try lease.registeredRouting(original);
        if (routing) |previous| if (!previous.matches(incoming)) return error.CompactionPlanningStale;
        const self = try allocate(a, manager, source, false, routing orelse incoming, original);
        errdefer self.requestRetirement();
        self.state.?.held = try lease.retainRegisteredSourceWithAdmission(original);
        return self;
    }
    pub fn startRemove(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, handle: Directory.Handle, scope: admission.Port) !*Candidate {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        const routing = try validateSource(source, a, original);
        const self = try allocate(a, manager, source, true, routing, original);
        errdefer self.requestRetirement();
        self.state.?.held = try handle.retainRegisteredSourceWithAdmission(original);
        if (routing) |previous| if (!previous.matches(self.state.?.held.?.routing)) return error.CompactionPlanningStale;
        self.state.?.validation = self.state.?.source.?.locateRead(self.state.?.held.?.run());
        return self;
    }
    fn allocate(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, remove: bool, routing: ?Directory.Routing, original: admission.Port) !*Candidate {
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        const plan: Directory.PutPools = .{ .directory = @constCast(source) };
        const bound = try std.math.add(u64, @sizeOf(Candidate) + @sizeOf(State) + @sizeOf(Directory) + @sizeOf(Account), try plan.memoryBound());
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        defer construction.retire();
        const self = try a.create(Candidate);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .remove = remove, .routing = routing };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.state = try a.create(State);
        self.state.?.* = .{};
        self.state.?.source = source.forkContents();
        try original.checkpoint();
        self.account = try Account.createIndependentlyFunded(a);
        try original.checkpoint();
        const directory = try a.create(Directory);
        directory.* = source.forkContents();
        inline for (fields) |field| {
            if (@field(directory, field).account) |old| old.release();
            @field(directory, field).account = self.account.?.retain();
        }
        if (directory.registered_account) |old| old.release();
        directory.registered_account = self.account.?.retain();
        directory.registered_owner = &self.registration;
        self.state.?.directory = directory;
        self.state.?.pools = .{ .directory = directory };
        return self;
    }
    pub fn resumeWithAdmission(self: *Candidate, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.state == null or self.state.?.directory == null) return error.InvalidArgument;
        if (self.ready) return;
        const state = self.state.?;
        if (state.validation) |*search| {
            try search.resumeWithAdmission(original);
            const node = search.matched orelse return error.CompactionPlanningStale;
            if (node.entry.payload != state.held.?.lease.?.entry.payload) return error.CompactionPlanningStale;
            state.validation = null;
        }
        try state.pools.?.resumeWithAdmission(self.allocator, original);
        if (state.edit == null) {
            if (self.remove) {
                state.edit = .{ .remove = try Directory.Removal.init(&state.source.?, state.directory.?, state.held.?.run()) };
            } else {
                state.edit = .{ .put = try Directory.Put.init(&state.source.?, state.directory.?, state.held.?.lease.?) };
                state.held.?.lease = null;
                state.held = null;
            }
        }
        switch (state.edit.?) {
            .put => |*edit| try edit.resumeWithAdmission(self.allocator, original),
            .remove => |*edit| try edit.resumeWithAdmission(self.allocator, original),
        }
        self.ready = true;
    }
    pub fn takeWithAdmission(self: *Candidate, original: admission.Port) !*Directory {
        try original.checkpoint();
        if (self.closing or !self.ready or self.state == null or self.state.?.directory == null) return error.InvalidArgument;
        const state = self.state.?;
        const out = switch (state.edit.?) {
            .put => |*edit| blk: {
                const taken = try edit.takePreparedWithAdmission(original);
                var lease = taken.lease;
                lease.deinit(self.allocator); // Registered final anchor makes this fixed.
                break :blk taken.directory;
            },
            .remove => |*edit| try edit.takePreparedWithAdmission(original),
        };
        state.edit = null;
        state.directory = null;
        if (state.held) |*held| held.releaseWithAdmission(null) catch unreachable;
        state.held = null;
        return out;
    }
    pub fn requestRetirement(self: *Candidate) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Candidate = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.state) |state| {
            if (!self.state_retiring) {
                if (state.edit) |*edit| {
                    const done = switch (edit.*) {
                        .put => |*put| try put.retireStep(self.allocator, original),
                        .remove => |*remove| try remove.retireStep(self.allocator, original),
                    };
                    if (!done) return false;
                    switch (edit.*) {
                        .put => |*put| {
                            if (put.lease) |*lease| lease.deinit(self.allocator);
                            put.lease = null;
                        },
                        .remove => {},
                    }
                    state.edit = null;
                    state.directory = null; // Edit retired the candidate header.
                }
                if (state.directory) |directory| {
                    if (state.cleanup == null) state.cleanup = .init(directory);
                    var steps: usize = 1;
                    if (!state.cleanup.?.step(self.allocator, &steps)) return false;
                    if (original) |port| try port.checkpoint();
                    state.cleanup.?.finish(self.allocator);
                    state.cleanup = null;
                    state.directory = null;
                    return false;
                }
                if (state.held) |*held| try held.releaseWithAdmission(original);
                state.held = null;
                if (state.source != null) {
                    if (!state.source_cleanup) {
                        state.cleanup = .init(&state.source.?);
                        state.source_cleanup = true;
                    }
                    var steps: usize = 1;
                    if (!state.cleanup.?.step(self.allocator, &steps)) return false;
                    if (original) |port| try port.checkpoint();
                    state.cleanup.?.finishContents();
                    state.cleanup = null;
                    state.source = null;
                    return false;
                }
                self.state_retiring = true;
            }
            const bytes = std.mem.asBytes(state);
            const end = @min(bytes.len, self.state_offset + 16 * 1024);
            @memset(bytes[self.state_offset..end], undefined);
            self.state_offset = end;
            if (end != bytes.len) return false;
            self.state = null;
            self.allocator.rawFree(bytes, .of(State), @returnAddress());
            return false;
        }
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
        std.debug.assert(self.complete and self.state == null and self.account == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
