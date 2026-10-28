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

//! Independently funded metadata and state for an existing SST or memory run.
//! The controller retains the immutable source and file-existence authority until copying completes or
//! requestRetirement stops input reads. This owner never deletes an SST, claims
//! a durability receipt, or retains Backend, Storage or an execution port.
const std = @import("std");
const resource = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Run = @import("repository.zig").Run;
const Metadata = @import("run_metadata_clone.zig").Clone;
const State = @import("state.zig").State;
const Retirement = @import("run_snapshot_clone.zig").Retirement;
const Lifetime = @import("directory_entry_owner.zig").Lifetime;
const Filter = @typeInfo(@FieldType(Run, "bloom_filter")).optional.child;
const slice = .lsm_table_builder_working_set;

pub const Lease = struct {
    owner: ?*Owner,
    run: Run,
    pub fn releaseWithAdmission(self: *@This(), original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
        const owner = self.owner orelse return;
        self.owner = null;
        owner.release();
    }
    pub fn takeLifetimeWithAdmission(self: *@This(), original: admission.Port) !Lifetime {
        try original.checkpoint();
        const owner = self.owner orelse return error.InvalidArgument;
        self.owner = null;
        return .{ .context = owner, .release = Owner.releaseLifetime };
    }
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    host: resource.AllocatorHost,
    registration: resource.PayloadRetirement = .{},
    credit: ?resource.Reservation = null,
    variable_credit: ?resource.Reservation = null,
    refs: std.atomic.Value(usize) = .init(1),
    metadata: Metadata,
    state: ?State.Clone = null,
    input_filter: ?Filter,
    filter: ?Filter = null,
    filter_offset: usize = 0,
    retire_filter_offset: usize = 0,
    run: ?Run = null,
    retirement: ?Retirement = null,
    ready: bool = false,
    closing: bool = false,
    complete: bool = false,

    /// The source is borrowed through resume or requestRetirement. A captured
    /// live-root controller supplies file-existence authority separately.
    pub fn start(a: std.mem.Allocator, manager: *resource.ResourceManager, source: Run, original: admission.Port) !*Owner {
        try original.checkpoint();
        if (source.path == null) return error.InvalidArgument;
        return startSource(a, manager, source, false, original);
    }
    /// Copies every state byte, including shared entry storage. The registered
    /// lifetime owns its complete charge independently of the old memory root.
    pub fn startMemory(a: std.mem.Allocator, manager: *resource.ResourceManager, source: Run, original: admission.Port) !*Owner {
        try original.checkpoint();
        if (source.path != null or source.state == null) return error.InvalidArgument;
        return startSource(a, manager, source, true, original);
    }
    fn startSource(a: std.mem.Allocator, manager: *resource.ResourceManager, source: Run, memory: bool, original: admission.Port) !*Owner {
        if (source.output_ticket != null) return error.InvalidArgument;
        var host: ?resource.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(slice, @sizeOf(Owner), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .metadata = .init(source), .input_filter = source.bloom_filter, .state = if (memory) .initIndependent(&source.state.?) else null };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        return self;
    }
    pub fn resumeWithAdmission(self: *Owner, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        if (self.ready) return;
        if (self.variable_credit == null) {
            var bound = std.math.add(u64, try self.metadata.memoryBound(), if (self.input_filter) |filter| filter.bytes.len else 0) catch return error.ResourceBudgetExceeded;
            if (self.state) |*state| bound = std.math.add(u64, bound, try state.memoryBoundWithAdmission(original)) catch return error.ResourceBudgetExceeded;
            self.variable_credit = try self.manager.reserveWithoutReclaimWithAdmission(slice, bound, original);
        }
        try self.metadata.resumeWithAdmission(self.allocator, original);
        if (self.state) |*state| try state.resumeWithAdmission(self.allocator, original);
        if (self.input_filter) |input| {
            if (self.filter == null) {
                try original.checkpoint();
                const bytes = try self.allocator.alloc(u8, input.bytes.len);
                self.filter = .{ .bytes = bytes, .bit_count = input.bit_count, .hash_count = input.hash_count };
            }
            while (self.filter_offset < input.bytes.len) {
                try original.checkpoint();
                const end = @min(input.bytes.len, self.filter_offset + Metadata.quantum);
                @memcpy(self.filter.?.bytes[self.filter_offset..end], input.bytes[self.filter_offset..end]);
                self.filter_offset = end;
            }
        }
        try original.checkpoint();
        var run = self.metadata.take() catch unreachable;
        if (self.state) |*state| {
            run.state = state.takeWithAdmission(null) catch unreachable;
            state.source = undefined;
        }
        run.bloom_filter = self.filter;
        run.owns_bloom_filter = self.filter != null;
        self.run = run;
        self.filter = null;
        self.input_filter = null;
        self.metadata.source = undefined;
        self.ready = true;
    }
    /// Synchronous lease acquisition; creator retirement and driving are
    /// serialized by the controller. Drops and independent cleanup are atomic.
    pub fn acquireSourceWithAdmission(self: *Owner, original: admission.Port) !Lease {
        try original.checkpoint();
        if (self.closing or !self.ready) return error.InvalidArgument;
        var run = self.run.?;
        run.owns_metadata = false;
        run.owns_path = false;
        run.owns_bloom_filter = false;
        _ = self.refs.fetchAdd(1, .monotonic);
        return .{ .owner = self, .run = run };
    }
    fn release(self: *Owner) void {
        const previous = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 1);
    }
    fn releaseLifetime(raw: *anyopaque, original: ?admission.Port) !bool {
        if (original) |port| try port.checkpoint();
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.release();
        return true;
    }
    /// Final caller access. Cleanup never reads old metadata/filter storage,
    /// even when copying was interrupted and its source exits immediately.
    pub fn requestRetirement(self: *Owner) void {
        std.debug.assert(!self.closing);
        self.closing = true;
        self.input_filter = null;
        self.metadata.source = undefined;
        if (self.state) |*state| state.source = undefined;
        self.registration.publish();
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.refs.load(.acquire) != 1) return false;
        if (self.run) |run| {
            self.retirement = try Retirement.init(run);
            self.run = null;
        }
        if (self.retirement) |*retirement| {
            if (!try retirement.step(self.allocator, original)) return false;
            self.retirement = null;
        }
        if (self.state) |*state| {
            try state.retireWithAdmission(self.allocator, original);
            self.state = null;
        }
        if (!try self.metadata.retireStep(self.allocator, original)) return false;
        if (self.filter) |filter| {
            const end = @min(filter.bytes.len, self.retire_filter_offset + Metadata.quantum);
            @memset(filter.bytes[self.retire_filter_offset..end], undefined);
            self.retire_filter_offset = end;
            if (end != filter.bytes.len) return false;
            if (filter.bytes.len != 0) self.allocator.rawFree(filter.bytes, .of(u8), @returnAddress());
            self.filter = null;
            return false;
        }
        if (self.variable_credit) |*credit| credit.release();
        self.variable_credit = null;
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= Metadata.quantum);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.refs.load(.acquire) == 1 and self.variable_credit == null and self.run == null and self.filter == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
