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

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_types = @import("backend_types.zig");
const change_journal_mod = @import("db/derived/change_journal.zig");
const internal_keys = @import("internal_keys.zig");
const read_admission = @import("../common/read_admission.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");

pub const types = backend_types;

pub const Entry = backend_adapter.Entry;
pub const ReplayEntry = backend_types.ReplayEntry;

fn Box(comptime T: type) type {
    return struct {
        allocator: Allocator,
        handle: T,
    };
}

fn ParentBox(comptime T: type) type {
    return struct {
        allocator: Allocator,
        handle: T,
        mutex: std.atomic.Mutex = .unlocked,
        child_count: usize = 0,
        owner_closed: bool = false,
        finalizing: bool = false,
        parent_release: ?ParentRelease = null,

        fn retainChild(self: *@This()) !void {
            platform.sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.owner_closed or self.finalizing) return error.TransactionClosed;
            self.child_count += 1;
        }

        // A live fork can extend the immutable snapshot's lifetime after its
        // original owner closes. Its own reference proves the anchor is live.
        fn retainSnapshot(self: *@This()) !void {
            platform.sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (self.finalizing or (self.owner_closed and self.child_count == 0)) return error.TransactionClosed;
            self.child_count = try std.math.add(usize, self.child_count, 1);
        }

        fn requestAbort(self: *@This()) void {
            platform.sync.lockYielding(&self.mutex);
            std.debug.assert(!self.owner_closed);
            self.owner_closed = true;
            const finalize = self.child_count == 0;
            if (finalize) self.finalizing = true;
            self.mutex.unlock();
            if (finalize) self.finalizeAbort();
        }

        fn releaseChild(self: *@This()) void {
            platform.sync.lockYielding(&self.mutex);
            std.debug.assert(self.child_count > 0);
            self.child_count -= 1;
            const finalize = self.owner_closed and self.child_count == 0 and !self.finalizing;
            if (finalize) self.finalizing = true;
            self.mutex.unlock();
            if (finalize) self.finalizeAbort();
        }

        fn commit(self: *@This()) !void {
            return self.commitControlled(null);
        }
        fn commitWithAdmission(self: *@This(), original: read_admission.Port) !void {
            return self.commitControlled(original);
        }
        fn commitHandle(self: *@This(), control: ?read_admission.Port) !void {
            if (control) |original| {
                if (comptime @hasDecl(T, "commitWithAdmission")) return self.handle.commitWithAdmission(original);
                try original.checkpoint();
            }
            return self.handle.commit();
        }
        fn commitControlled(self: *@This(), control: ?read_admission.Port) !void {
            platform.sync.lockYielding(&self.mutex);
            if (self.owner_closed or self.finalizing) {
                self.mutex.unlock();
                return error.TransactionClosed;
            }
            if (self.child_count != 0) {
                self.mutex.unlock();
                return error.TransactionCursorActive;
            }
            self.finalizing = true;
            self.mutex.unlock();

            const committed = self.commitHandle(control);
            committed catch |err| {
                platform.sync.lockYielding(&self.mutex);
                self.finalizing = false;
                self.mutex.unlock();
                return err;
            };
            const allocator = self.allocator;
            allocator.destroy(self);
        }

        fn finalizeAbort(self: *@This()) void {
            const allocator = self.allocator;
            const parent_release = self.parent_release;
            self.handle.abort();
            allocator.destroy(self);
            if (parent_release) |parent| parent.release(parent.ptr);
        }
    };
}

const ParentRelease = struct {
    ptr: *anyopaque,
    release: *const fn (*anyopaque) void,
    retain_snapshot: *const fn (*anyopaque) anyerror!void,
};

fn parentReleaseFor(parent: anytype) ParentRelease {
    const Parent = @TypeOf(parent.*);
    const release = struct {
        fn run(ptr: *anyopaque) void {
            const typed: *Parent = @ptrCast(@alignCast(ptr));
            typed.releaseChild();
        }
    }.run;
    const retain_snapshot = struct {
        fn run(ptr: *anyopaque) !void {
            const typed: *Parent = @ptrCast(@alignCast(ptr));
            try typed.retainSnapshot();
        }
    }.run;
    return .{ .ptr = parent, .release = release, .retain_snapshot = retain_snapshot };
}

fn allocBox(allocator: Allocator, value: anytype) !*Box(@TypeOf(value)) {
    const T = @TypeOf(value);
    const box = try allocator.create(Box(T));
    box.* = .{ .allocator = allocator, .handle = value };
    return box;
}

fn allocParentBox(allocator: Allocator, value: anytype) !*ParentBox(@TypeOf(value)) {
    const T = @TypeOf(value);
    const a = if (@hasDecl(T, "metadataAllocator")) value.metadataAllocator(allocator) else allocator;
    const box = try a.create(ParentBox(T));
    box.* = .{ .allocator = a, .handle = value };
    return box;
}

fn wrapperBoxAllocator(fallback: Allocator) Allocator {
    const effective_fallback = if (!builtin.single_threaded) std.heap.smp_allocator else fallback;
    return platform.allocator.processAllocator(effective_fallback);
}

fn replayHintOrdinalFromSingleMask(mask: u8) ?u8 {
    if (mask == 0 or (mask & (mask - 1)) != 0) return null;
    inline for (@typeInfo(change_journal_mod.TargetHint).@"enum".field_values) |field_value| {
        if (mask == (@as(u8, 1) << @intCast(field_value))) return @intCast(field_value);
    }
    return null;
}

pub const Cursor = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        close: *const fn (Allocator, *anyopaque) void,
        first: *const fn (*anyopaque) anyerror!?Entry,
        last: *const fn (*anyopaque) anyerror!?Entry,
        next: *const fn (*anyopaque) anyerror!?Entry,
        prev: *const fn (*anyopaque) anyerror!?Entry,
        seek_at_or_after: *const fn (*anyopaque, []const u8) anyerror!?Entry,
        seek_at_or_before: *const fn (*anyopaque, []const u8) anyerror!?Entry,
        set_upper_bound: ?*const fn (*anyopaque, ?[]const u8) void = null,
        next_with_admission: ?*const fn (*anyopaque, read_admission.Port) anyerror!?Entry = null,
        seek_at_or_after_with_admission: ?*const fn (*anyopaque, []const u8, read_admission.Port) anyerror!?Entry = null,
        set_keys_only: ?*const fn (*anyopaque) void = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn close(self: *Cursor) void {
        self.vtable.close(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn first(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("first", self.boundary_dispatch, self.vtable.first, .{self.ptr});
    }

    pub fn last(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("last", self.boundary_dispatch, self.vtable.last, .{self.ptr});
    }

    pub fn next(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("next", self.boundary_dispatch, self.vtable.next, .{self.ptr});
    }

    /// Admission is borrowed only during the call, and is never retained.
    pub fn nextWithAdmission(self: *Cursor, control: read_admission.Port) !?Entry {
        try control.checkpoint();
        const next_controlled = self.vtable.next_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("next_with_admission", self.boundary_dispatch, next_controlled, .{ self.ptr, control });
    }
    pub fn seekAtOrAfterWithAdmission(self: *Cursor, key: []const u8, control: read_admission.Port) !?Entry {
        try control.checkpoint();
        const seek = self.vtable.seek_at_or_after_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("seek_at_or_after_with_admission", self.boundary_dispatch, seek, .{ self.ptr, key, control });
    }

    pub fn prev(self: *Cursor) !?Entry {
        return try BoundaryAbi.call("prev", self.boundary_dispatch, self.vtable.prev, .{self.ptr});
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !?Entry {
        return try BoundaryAbi.call("seek_at_or_after", self.boundary_dispatch, self.vtable.seek_at_or_after, .{ self.ptr, key });
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !?Entry {
        return try BoundaryAbi.call("seek_at_or_before", self.boundary_dispatch, self.vtable.seek_at_or_before, .{ self.ptr, key });
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        if (self.vtable.set_upper_bound) |set_upper_bound| set_upper_bound(self.ptr, upper);
    }

    /// Call before the first move. Unsupported adapters retain normal reads.
    pub fn setKeysOnly(self: *Cursor) void {
        if (self.vtable.set_keys_only) |set| set(self.ptr);
    }

    pub fn start(self: *Cursor, options: backend_types.CursorOptions) !?Entry {
        return switch (options.start) {
            .first => switch (options.order) {
                .forward => try self.first(),
                .reverse => try self.last(),
            },
            .at_or_after => |key| try self.seekAtOrAfter(key),
            .at_or_before => |key| try self.seekAtOrBefore(key),
        };
    }
};

/// Values remain valid until reset or close, independently of other scopes.
/// Read transactions share one immutable snapshot. Native Lite write scopes
/// copy the current pending overlay; cursor fallbacks use their cursor view.
pub const RangeRequest = @import("value_ranges.zig").Request;
pub const ValueRange = @import("value_ranges.zig").Range;
pub const ReadScope = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct {
        get_ranges_many: ?*const fn (*anyopaque, []const RangeRequest) anyerror!void = null,
        get_ranges: ?*const fn (*anyopaque, []const u8, []const ValueRange, [][]const u8) anyerror!void = null,
        get_range: ?*const fn (*anyopaque, []const u8, u64, usize) anyerror![]const u8 = null,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        get_many_sorted_with_admission: ?*const fn (*anyopaque, []const []const u8, []?[]const u8, read_admission.Port) anyerror!void = null,
        close: *const fn (Allocator, *anyopaque) void,
        reset: ?*const fn (*anyopaque) anyerror!void = null,
        get_many_sorted_into_with_admission: ?*const fn (*anyopaque, []const []const u8, []const []u8, []?[]const u8, read_admission.Port) anyerror!void = null,
        begin_score_batch: ?*const fn (*anyopaque) anyerror!void = null,
        reset_score_batch: ?*const fn (*anyopaque) anyerror!void = null,
        get_many_sorted_into: ?*const fn (*anyopaque, []const []const u8, []const []u8, []?[]const u8) anyerror!void = null,
        visit_many_sorted_scores: ?*const fn (*anyopaque, []const []const u8, backend_types.ScoreSink) anyerror!void = null,
        read_sorted_score_column: ?*const fn (*anyopaque, []const u8, []const []const u8, []f64) anyerror!usize = null,
        working_allocator: ?*const fn (*anyopaque) Allocator = null,
        prepared_score_plan: ?*const fn (*anyopaque) ?*@import("prepared_score_plan.zig").PreparedScorePlan = null,
        prepared_score_allocator: ?*const fn (*anyopaque) Allocator = null,
        visit_prepared_request: ?*const fn (*anyopaque, *const @import("prepared_score_request.zig").PreparedScoreRequest, backend_types.CompleteScoreSink) anyerror!void = null,
        visit_prepared_scores: ?*const fn (*anyopaque, *@import("prepared_score_plan.zig").PreparedScorePlan, backend_types.CompleteScoreSink) anyerror!void = null,
        get_many_sorted_scores: ?*const fn (*anyopaque, []const []const u8, []?f64) anyerror!void = null,
        get_into: ?*const fn (*anyopaque, []const u8, []u8) anyerror![]const u8 = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);
    /// Sorted document keys share one index traversal on capable backends.
    pub fn getRangesMany(self: *@This(), requests: []const RangeRequest) !void {
        if (requests.len > 64) return error.InvalidBatch;
        if (requests.len == 0) return;
        for (requests, 0..) |request, i| {
            if (request.ranges.len != request.output.len or request.ranges.len > @import("value_ranges.zig").maximum_ranges) return error.InvalidBatch;
            if (i != 0 and std.mem.order(u8, requests[i - 1].key, request.key) == .gt) return error.InvalidBatch;
        }
        if (self.vtable.get_ranges_many) |read| return BoundaryAbi.call("get_ranges_many", self.boundary_dispatch, read, .{ self.ptr, requests });
        for (requests) |request| try self.getRanges(request.key, request.ranges, request.output);
    }
    pub fn getRanges(self: *@This(), key: []const u8, ranges: []const ValueRange, output: [][]const u8) !void {
        if (ranges.len != output.len or ranges.len > @import("value_ranges.zig").maximum_ranges) return error.InvalidBatch;
        if (ranges.len == 0) return;
        if (self.vtable.get_ranges) |read| return BoundaryAbi.call("get_ranges", self.boundary_dispatch, read, .{ self.ptr, key, ranges, output });
        const bytes = try self.get(key);
        for (ranges, output) |range, *out| {
            if (range.offset > bytes.len or range.length > bytes.len - range.offset) return error.EndOfStream;
            out.* = bytes[@intCast(range.offset)..][0..range.length];
        }
    }
    pub fn supportsRange(self: *const @This()) bool {
        return self.vtable.get_range != null;
    }
    pub fn getRange(self: *@This(), key: []const u8, offset: u64, length: usize) ![]const u8 {
        const read_range = self.vtable.get_range orelse return error.Unsupported;
        return BoundaryAbi.call("get_range", self.boundary_dispatch, read_range, .{ self.ptr, key, offset, length });
    }
    pub fn supportsSortedScoreColumn(self: *const @This()) bool {
        return self.vtable.read_sorted_score_column != null;
    }
    /// A bounded, strictly increasing logical node span and one physical metric
    /// prefix. The backend encodes keys once and fills caller-owned finite/NaN
    /// scores directly; no per-score erased callback or prepared owner.
    pub fn readSortedScoreColumn(self: *@This(), prefix: []const u8, nodes: []const []const u8, values: []f64) !usize {
        const read = self.vtable.read_sorted_score_column orelse return error.Unsupported;
        return BoundaryAbi.call("read_sorted_score_column", self.boundary_dispatch, read, .{ self.ptr, prefix, nodes, values });
    }
    pub fn preparedScorePlan(self: *@This()) ?*@import("prepared_score_plan.zig").PreparedScorePlan {
        return if (self.vtable.prepared_score_plan) |get_plan| get_plan(self.ptr) else null;
    }
    pub fn preparedScoreAllocator(self: *@This()) Allocator {
        return self.vtable.prepared_score_allocator.?(self.ptr);
    }
    pub fn supportsPreparedRequest(self: *const @This()) bool {
        return self.vtable.visit_prepared_request != null;
    }
    pub fn visitPreparedRequest(self: *@This(), request: *const @import("prepared_score_request.zig").PreparedScoreRequest, sink: backend_types.CompleteScoreSink) !void {
        return BoundaryAbi.call("visit_prepared_request", self.boundary_dispatch, self.vtable.visit_prepared_request orelse return error.Unsupported, .{ self.ptr, request, sink });
    }
    pub fn supportsPreparedScores(self: *const @This()) bool {
        return self.vtable.visit_prepared_scores != null;
    }
    pub fn visitPreparedScores(self: *@This(), plan: *@import("prepared_score_plan.zig").PreparedScorePlan, sink: backend_types.CompleteScoreSink) !void {
        return BoundaryAbi.call("visit_prepared_scores", self.boundary_dispatch, self.vtable.visit_prepared_scores orelse return error.Unsupported, .{ self.ptr, plan, sink });
    }
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }
    /// The returned slice belongs to out, independently of scope reset.
    pub fn getInto(self: *@This(), key: []const u8, out: []u8) ![]const u8 {
        if (self.vtable.get_into) |get_into| return BoundaryAbi.call("get_into", self.boundary_dispatch, get_into, .{ self.ptr, key, out });
        const value = try self.get(key);
        if (value.len > out.len) return error.BufferTooSmall;
        @memcpy(out[0..value.len], value);
        return out[0..value.len];
    }

    /// Results share the scope lifetime, not the parent snapshot's lifetime.
    pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        for (keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        if (self.vtable.get_many_sorted) |get_many| return BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many, .{ self.ptr, keys, values });
        for (keys, values) |key, *value| value.* = self.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }
    /// True only when the backend implements direct batch delivery.
    pub fn supportsDirectBatchInto(self: *const @This()) bool {
        return self.vtable.get_many_sorted_into != null;
    }

    pub fn workingAllocator(self: *@This()) Allocator {
        return if (self.vtable.working_allocator) |working| working(self.ptr) else self.allocator;
    }
    pub fn supportsScoreVisitor(self: *const @This()) bool {
        return self.vtable.visit_many_sorted_scores != null;
    }
    pub fn visitManySortedScores(self: *@This(), keys: []const []const u8, sink: backend_types.ScoreSink) !void {
        const visit = self.vtable.visit_many_sorted_scores orelse return error.Unsupported;
        return BoundaryAbi.call("visit_many_sorted_scores", self.boundary_dispatch, visit, .{ self.ptr, keys, sink });
    }
    pub fn supportsDirectScores(self: *const @This()) bool {
        return self.vtable.get_many_sorted_scores != null;
    }
    pub fn getManySortedScores(self: *@This(), keys: []const []const u8, scores: []?f64) !void {
        if (keys.len != scores.len) return error.InvalidBatch;
        @memset(scores, null);
        errdefer @memset(scores, null);
        const read = self.vtable.get_many_sorted_scores orelse return error.Unsupported;
        return BoundaryAbi.call("get_many_sorted_scores", self.boundary_dispatch, read, .{ self.ptr, keys, scores });
    }

    /// Slices belong to caller buffers, independently of reset or close.
    /// Output buffers must not overlap each other or the input keys.
    /// Contents may be partially written on errors; result slots are cleared.
    pub fn getManySortedInto(self: *@This(), keys: []const []const u8, buffers: []const []u8, values: []?[]const u8) !void {
        if (keys.len != values.len or keys.len != buffers.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        for (keys, 0..) |key, i| {
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        if (self.vtable.get_many_sorted_into) |read| return BoundaryAbi.call("get_many_sorted_into", self.boundary_dispatch, read, .{ self.ptr, keys, buffers, values });
        // Preserve a backend's batched lookup when it lacks direct delivery.
        try self.getManySorted(keys, values);
        for (buffers, values) |buffer, *value| if (value.*) |bytes| {
            if (bytes.len > buffer.len) return error.BufferTooSmall;
            @memcpy(buffer[0..bytes.len], bytes);
            value.* = buffer[0..bytes.len];
        };
    }

    pub fn getManySortedIntoWithAdmission(self: *@This(), keys: []const []const u8, buffers: []const []u8, values: []?[]const u8, control: read_admission.Port) !void {
        if (keys.len != values.len or keys.len != buffers.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        try control.checkpoint();
        if (self.vtable.get_many_sorted_into_with_admission) |read| {
            var relay: read_admission.ErrorRelay = .{};
            const borrowed = try relay.bind(control);
            BoundaryAbi.call("get_many_sorted_into_with_admission", self.boundary_dispatch, read, .{ self.ptr, keys, buffers, values, borrowed }) catch |err| return relay.rejection orelse err;
            if (relay.rejection) |err| return err;
        } else {
            try self.getManySortedWithAdmission(keys, values, control);
            for (buffers, values) |buffer, *value| if (value.*) |bytes| {
                if (bytes.len > buffer.len) return error.BufferTooSmall;
                var offset: usize = 0;
                while (offset < bytes.len) {
                    try control.checkpoint();
                    const end = @min(bytes.len, offset + 64 * 1024);
                    @memcpy(buffer[offset..end], bytes[offset..end]);
                    offset = end;
                }
                value.* = buffer[0..bytes.len];
            };
        }
        try control.checkpoint();
    }

    /// Invalidates scope values and releases pins; keeps bounded reusable scratch.
    /// Borrows authority only for this call. Results remain owned by the
    /// existing snapshot/scope; failed calls publish no partial output.
    pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        try control.checkpoint();
        for (keys, 0..) |key, i| {
            try control.checkpoint();
            if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        }
        const get_many = self.vtable.get_many_sorted_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        BoundaryAbi.call("get_many_sorted_with_admission", self.boundary_dispatch, get_many, .{ self.ptr, keys, values, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
    }

    pub fn reset(self: *@This()) !void {
        const reset_scope = self.vtable.reset orelse return error.ReadScopeResetUnsupported;
        try BoundaryAbi.call("reset", self.boundary_dispatch, reset_scope, .{self.ptr});
    }

    /// Opt into working-arena reuse only for bounded, transient score batches.
    pub fn beginScoreBatch(self: *@This()) !void {
        const begin_batch = self.vtable.begin_score_batch orelse return error.Unsupported;
        try BoundaryAbi.call("begin_score_batch", self.boundary_dispatch, begin_batch, .{self.ptr});
    }

    /// Invalidates result and prepared-plan borrows, retaining bounded scratch.
    /// Unsupported scopes must be closed between batches instead.
    pub fn resetScoreBatch(self: *@This()) !void {
        const reset_batch = self.vtable.reset_score_batch orelse return error.Unsupported;
        try BoundaryAbi.call("reset_score_batch", self.boundary_dispatch, reset_batch, .{self.ptr});
    }

    pub fn close(self: *@This()) void {
        self.vtable.close(self.allocator, self.ptr);
        self.* = undefined;
    }
};

pub fn readScopeFrom(alloc: Allocator, handle: anytype) !ReadScope {
    return readScopeFromWithParent(alloc, handle, null);
}

fn readScopeFromWithParent(alloc: Allocator, handle: anytype, parent: ?ParentRelease) !ReadScope {
    const pooled_metadata = @hasDecl(@TypeOf(handle), "metadataAllocator");
    const Receipt = @import("resource_manager.zig").BudgetedAllocator.BorrowedScratch;
    const State = struct {
        handle: @TypeOf(handle),
        parent: ?ParentRelease,
        admission: if (pooled_metadata) ?Receipt else void,
        fn getRangesMany(ptr: *anyopaque, requests: []const RangeRequest) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getRangesMany(requests);
        }
        fn getRanges(ptr: *anyopaque, key: []const u8, ranges: []const ValueRange, output: [][]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getRanges(key, ranges, output);
        }
        fn getRange(ptr: *anyopaque, key: []const u8, offset: u64, length: usize) anyerror![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getRange(key, offset, length);
        }
        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.get(key);
        }
        pub fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getManySorted(keys, values);
        }
        pub fn getManySortedWithAdmission(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getManySortedWithAdmission(keys, values, control);
        }
        fn getManySortedIntoWithAdmission(ptr: *anyopaque, keys: []const []const u8, buffers: []const []u8, values: []?[]const u8, control: read_admission.Port) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getManySortedIntoWithAdmission(keys, buffers, values, control);
        }
        fn getManySortedInto(ptr: *anyopaque, keys: []const []const u8, buffers: []const []u8, values: []?[]const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getManySortedInto(keys, buffers, values);
        }
        fn visitManySortedScores(ptr: *anyopaque, keys: []const []const u8, sink: backend_types.ScoreSink) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.visitManySortedScores(keys, sink);
        }
        fn visitPreparedRequest(ptr: *anyopaque, request: *const @import("prepared_score_request.zig").PreparedScoreRequest, sink: backend_types.CompleteScoreSink) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.visitPreparedRequest(request, sink);
        }
        fn visitPreparedScores(ptr: *anyopaque, plan: *@import("prepared_score_plan.zig").PreparedScorePlan, sink: backend_types.CompleteScoreSink) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.visitPreparedScores(plan, sink);
        }
        fn preparedScorePlan(ptr: *anyopaque) ?*@import("prepared_score_plan.zig").PreparedScorePlan {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.preparedScorePlan();
        }
        fn preparedScoreAllocator(ptr: *anyopaque) Allocator {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.preparedScoreAllocator();
        }
        fn workingAllocator(ptr: *anyopaque) Allocator {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.workingAllocator();
        }
        fn getManySortedScores(ptr: *anyopaque, keys: []const []const u8, scores: []?f64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getManySortedScores(keys, scores);
        }
        fn getInto(ptr: *anyopaque, key: []const u8, out: []u8) anyerror![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.getInto(key, out);
        }

        fn reset(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.reset();
        }

        fn readSortedScoreColumn(ptr: *anyopaque, prefix: []const u8, nodes: []const []const u8, values: []f64) anyerror!usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.readSortedScoreColumn(prefix, nodes, values);
        }
        fn beginScoreBatch(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.beginScoreBatch();
        }
        fn resetScoreBatch(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.handle.resetScoreBatch();
        }

        fn close(a: Allocator, ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const parent_owner = self.parent;
            var admission = self.admission;
            self.handle.close();
            // Destroy store-owned wrapper before dropping its last snapshot pin.
            a.destroy(self);
            if (pooled_metadata) {
                if (admission) |*receipt| receipt.release();
            }
            if (parent_owner) |owner| owner.release(owner.ptr);
        }
    };
    const a = if (@hasDecl(@TypeOf(handle), "metadataAllocator")) handle.metadataAllocator(alloc) else alloc;
    var admission: @FieldType(State, "admission") = if (pooled_metadata) null else {};
    if (pooled_metadata and (a.ptr != alloc.ptr or a.vtable != alloc.vtable)) {
        if (@import("resource_manager.zig").BudgetedAllocator.fromAllocator(alloc)) |budget| {
            const resident = if (@hasDecl(@TypeOf(handle), "metadataResidentManager")) handle.metadataResidentManager() else null;
            admission = try budget.borrowResidentScratch(@sizeOf(State), resident);
        }
    }
    errdefer if (pooled_metadata) {
        if (admission) |*receipt| receipt.release();
    };
    const state = try a.create(State);
    state.* = .{ .handle = handle, .parent = parent, .admission = admission };
    return .{ .allocator = a, .ptr = state, .vtable = &.{ .get_ranges_many = if (@hasDecl(@TypeOf(handle), "getRangesMany")) State.getRangesMany else null, .get_ranges = if (@hasDecl(@TypeOf(handle), "getRanges")) State.getRanges else null, .get_range = if (@hasDecl(@TypeOf(handle), "getRange")) State.getRange else null, .get = State.get, .get_many_sorted_with_admission = if (@hasDecl(@TypeOf(handle), "getManySortedWithAdmission")) State.getManySortedWithAdmission else null, .get_many_sorted_into_with_admission = if (@hasDecl(@TypeOf(handle), "getManySortedIntoWithAdmission")) State.getManySortedIntoWithAdmission else null, .get_many_sorted = if (@hasDecl(@TypeOf(handle), "getManySorted")) State.getManySorted else null, .close = State.close, .reset = if (@hasDecl(@TypeOf(handle), "reset")) State.reset else null, .begin_score_batch = if (@hasDecl(@TypeOf(handle), "beginScoreBatch")) State.beginScoreBatch else null, .reset_score_batch = if (@hasDecl(@TypeOf(handle), "resetScoreBatch")) State.resetScoreBatch else null, .get_many_sorted_into = if (@hasDecl(@TypeOf(handle), "getManySortedInto")) State.getManySortedInto else null, .visit_many_sorted_scores = if (@hasDecl(@TypeOf(handle), "visitManySortedScores")) State.visitManySortedScores else null, .read_sorted_score_column = if (@hasDecl(@TypeOf(handle), "readSortedScoreColumn")) State.readSortedScoreColumn else null, .working_allocator = if (@hasDecl(@TypeOf(handle), "workingAllocator")) State.workingAllocator else null, .prepared_score_plan = if (@hasDecl(@TypeOf(handle), "preparedScorePlan")) State.preparedScorePlan else null, .prepared_score_allocator = if (@hasDecl(@TypeOf(handle), "preparedScoreAllocator")) State.preparedScoreAllocator else null, .visit_prepared_request = if (@hasDecl(@TypeOf(handle), "visitPreparedRequest")) State.visitPreparedRequest else null, .visit_prepared_scores = if (@hasDecl(@TypeOf(handle), "visitPreparedScores")) State.visitPreparedScores else null, .get_many_sorted_scores = if (@hasDecl(@TypeOf(handle), "getManySortedScores")) State.getManySortedScores else null, .get_into = if (@hasDecl(@TypeOf(handle), "getInto")) State.getInto else null } };
}

/// Portable fallback: cursor-owned storage pins are bounded; returned copies
/// belong to this scope, never to the long-lived transaction.
pub fn cursorReadScope(alloc: Allocator, cursor: Cursor) !ReadScope {
    const Scope = struct {
        cursor: Cursor,
        scratch: @import("../segment_source.zig").Scratch,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            const entry = (try self.cursor.seekAtOrAfter(key)) orelse return error.NotFound;
            if (!std.mem.eql(u8, entry.key, key)) return error.NotFound;
            return self.scratch.allocator().dupe(u8, entry.value);
        }
        pub fn reset(self: *@This()) void {
            self.scratch.reset();
        }

        pub fn close(self: *@This()) void {
            self.cursor.close();
            self.scratch.deinit();
        }
    };
    return readScopeFrom(alloc, Scope{ .cursor = cursor, .scratch = @import("../segment_source.zig").Scratch.init(alloc, 256 * 1024) });
}

var next_read_transaction_identity = platform.atomic.Value(u64).init(1);
fn newReadTransactionIdentity() u64 {
    const id = next_read_transaction_identity.fetchAdd(1, .monotonic);
    if (id == 0 or id == std.math.maxInt(u64)) @panic("read transaction identity exhausted");
    return id;
}

pub const ReadTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    identity: u64 = 0,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        decoded_results_namespace: ?*const fn (*anyopaque) ?[]const u8 = null,
        can_reuse_decoded_results: ?*const fn (*anyopaque) bool = null,
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        get_many_sorted_with_admission: ?*const fn (*anyopaque, []const []const u8, []?[]const u8, read_admission.Port) anyerror!void = null,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
        fork_read: ?*const fn (Allocator, *anyopaque) anyerror!ReadTxn = null,
        open_read_scope: ?*const fn (Allocator, *anyopaque) anyerror!ReadScope = null,
        open_cursor_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!Cursor = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    /// True only for immutable pinned views that permit cached delivery now.
    /// Mutable overlays, validation bypass and unsupported backends return false.
    pub fn canReuseDecodedResults(self: *const ReadTxn) bool {
        return if (self.vtable.can_reuse_decoded_results) |check| check(self.ptr) else false;
    }

    pub fn decodedResultsNamespace(self: *const ReadTxn) ?[]const u8 {
        if (!self.canReuseDecodedResults()) return null;
        return if (self.vtable.decoded_results_namespace) |get_namespace| get_namespace(self.ptr) else null;
    }

    pub fn abort(self: *ReadTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *ReadTxn, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }

    pub fn getManySorted(self: *ReadTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    /// Borrows authority only for this call. Results remain owned by the
    /// existing snapshot/scope; failed calls publish no partial output.
    pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        try control.checkpoint();

        const get_many = self.vtable.get_many_sorted_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        BoundaryAbi.call("get_many_sorted_with_admission", self.boundary_dispatch, get_many, .{ self.ptr, keys, values, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
    }

    pub fn openCursor(self: *ReadTxn) !Cursor {
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, self.vtable.open_cursor, .{ self.allocator, self.ptr });
    }

    pub fn openCursorWithAdmission(self: *ReadTxn, control: read_admission.Port) !Cursor {
        try control.checkpoint();
        const open = self.vtable.open_cursor_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var cursor = BoundaryAbi.call("open_cursor_with_admission", self.boundary_dispatch, open, .{ self.allocator, self.ptr, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer cursor.close();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return cursor;
    }

    /// Owns the same immutable snapshot, with independent read/cursor scratch.
    /// The original handle may be aborted before this handle or its cursors.
    pub fn forkRead(self: *ReadTxn) !ReadTxn {
        const fork = self.vtable.fork_read orelse return error.ReadSnapshotForkUnsupported;
        return BoundaryAbi.call("fork_read", self.boundary_dispatch, fork, .{ self.allocator, self.ptr });
    }

    pub fn forkBorrowedRead(self: *ReadTxn) !ReadTxn {
        return self.forkRead();
    }

    pub fn openReadScope(self: *ReadTxn, alloc: Allocator) !ReadScope {
        if (self.vtable.open_read_scope) |open| return BoundaryAbi.call("open_read_scope", self.boundary_dispatch, open, .{ alloc, self.ptr });
        var cursor = try self.openCursor();
        errdefer cursor.close();
        return cursorReadScope(alloc, cursor);
    }
};

pub const ProbeTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        /// One get_many_sorted call observes a single committed view. Separate
        /// calls need not share a snapshot; generic per-key fallbacks cannot
        /// claim this capability.
        get_many_sorted_is_atomic: bool = false,
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        contains: ?*const fn (*anyopaque, []const u8) anyerror!bool = null,
        get_leased: ?*const fn (*anyopaque, []const u8) anyerror![]const u8 = null,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        get_many_sorted_with_admission: ?*const fn (*anyopaque, []const []const u8, []?[]const u8, read_admission.Port) anyerror!void = null,
        get_many_sorted_with_block_cache_admission: ?*const fn (*anyopaque, []const []const u8, []?[]const u8, backend_types.Namespace.BlockCacheAdmission) anyerror!void = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *ProbeTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *ProbeTxn, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }

    /// Exact presence in the probe's committed view, without retaining a value
    /// on backends that implement native presence reads.
    pub fn contains(self: *ProbeTxn, key: []const u8) !bool {
        if (self.vtable.contains) |check| return try BoundaryAbi.call("contains", self.boundary_dispatch, check, .{ self.ptr, key });
        _ = self.get(key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        return true;
    }

    /// May pin an immutable generation until abort. Prefer get for long-lived
    /// probes; use this for a short-lived point projection.
    pub fn getLeased(self: *ProbeTxn, key: []const u8) ![]const u8 {
        if (self.vtable.get_leased) |get_leased| return try BoundaryAbi.call("get_leased", self.boundary_dispatch, get_leased, .{ self.ptr, key });
        return try self.get(key);
    }

    pub fn getManySorted(self: *ProbeTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn getManySortedWithAdmission(self: *ProbeTxn, keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        errdefer @memset(values, null);
        try control.checkpoint();
        const get_many = self.vtable.get_many_sorted_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        BoundaryAbi.call("get_many_sorted_with_admission", self.boundary_dispatch, get_many, .{ self.ptr, keys, values, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
    }

    pub fn getManySortedWithBlockCacheAdmission(
        self: *ProbeTxn,
        keys: []const []const u8,
        values: []?[]const u8,
        admission: backend_types.Namespace.BlockCacheAdmission,
    ) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        if (self.vtable.get_many_sorted_with_block_cache_admission) |get_many_sorted| {
            @memset(values, null);
            return try BoundaryAbi.call("get_many_sorted_with_block_cache_admission", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values, admission });
        }
        return try self.getManySorted(keys, values);
    }
};

pub const CurrentScanTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
        open_cursor_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!Cursor = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *CurrentScanTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn openCursor(self: *CurrentScanTxn) !Cursor {
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, self.vtable.open_cursor, .{ self.allocator, self.ptr });
    }
    pub fn openCursorWithAdmission(self: *CurrentScanTxn, control: read_admission.Port) !Cursor {
        try control.checkpoint();
        const open = self.vtable.open_cursor_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("open_cursor_with_admission", self.boundary_dispatch, open, .{ self.allocator, self.ptr, control });
    }
};

pub const NamespaceReadTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_with_admission: ?*const fn (*anyopaque, backend_types.Namespace, []const u8, read_admission.Port) anyerror![]const u8 = null,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        open_cursor: ?*const fn (Allocator, *anyopaque, backend_types.Namespace) anyerror!Cursor = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *NamespaceReadTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn get(self: *NamespaceReadTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, namespace, key });
    }

    pub fn getWithAdmission(self: *NamespaceReadTxn, namespace: backend_types.Namespace, key: []const u8, control: read_admission.Port) ![]const u8 {
        try control.checkpoint();
        const admitted_get = self.vtable.get_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        const value = BoundaryAbi.call("get_with_admission", self.boundary_dispatch, admitted_get, .{ self.ptr, namespace, key, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return value;
    }

    pub fn getManySorted(self: *NamespaceReadTxn, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, namespace, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(namespace, key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn openCursor(self: *NamespaceReadTxn, namespace: backend_types.Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, open_cursor, .{ self.allocator, self.ptr, namespace });
    }
};

pub const WriteTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    write_gate: ?*std.atomic.Mutex = null,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        has_prefix: ?*const fn (*anyopaque, []const u8) anyerror!bool = null,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        open_cursor: *const fn (Allocator, *anyopaque) anyerror!Cursor,
        open_read_scope: ?*const fn (Allocator, *anyopaque) anyerror!ReadScope = null,
        contains: ?*const fn (*anyopaque, []const u8) anyerror!bool = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *WriteTxn) void {
        const gate = self.write_gate;
        self.vtable.abort(self.allocator, self.ptr);
        if (gate) |mutex| mutex.unlock();
        self.* = undefined;
    }

    pub fn commit(self: *WriteTxn) !void {
        try BoundaryAbi.call("commit", self.boundary_dispatch, self.vtable.commit, .{ self.allocator, self.ptr });
        if (self.write_gate) |mutex| mutex.unlock();
        self.* = undefined;
    }

    /// Temporary reads own a separate lifetime. Close before commit; the
    /// adapter prevents publication while native scopes retain the parent.
    pub fn openReadScope(self: *WriteTxn, allocator: Allocator) !ReadScope {
        if (self.vtable.open_read_scope) |open_scope| {
            return try BoundaryAbi.call("open_read_scope", self.boundary_dispatch, open_scope, .{ allocator, self.ptr });
        }
        var cursor = try self.openCursor();
        errdefer cursor.close();
        return cursorReadScope(allocator, cursor);
    }

    pub fn get(self: *WriteTxn, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }

    pub fn contains(self: *WriteTxn, key: []const u8) !bool {
        if (self.vtable.contains) |check| return BoundaryAbi.call("contains", self.boundary_dispatch, check, .{ self.ptr, key });
        _ = self.get(key) catch |err| return if (err == error.NotFound) false else err;
        return true;
    }

    pub fn getManySorted(self: *WriteTxn, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn put(self: *WriteTxn, key: []const u8, value: []const u8) !void {
        try BoundaryAbi.call("put", self.boundary_dispatch, self.vtable.put, .{ self.ptr, key, value });
    }

    pub fn delete(self: *WriteTxn, key: []const u8) !void {
        try BoundaryAbi.call("delete", self.boundary_dispatch, self.vtable.delete, .{ self.ptr, key });
    }

    /// Test the current transactional key view without hydrating values.
    /// Backends without native support retain ordinary cursor semantics.
    pub fn hasPrefix(self: *WriteTxn, prefix: []const u8) !bool {
        if (self.vtable.has_prefix) |has_prefix| {
            return try BoundaryAbi.call("has_prefix", self.boundary_dispatch, has_prefix, .{ self.ptr, prefix });
        }
        var cursor = try self.openCursor();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }

    pub fn openCursor(self: *WriteTxn) !Cursor {
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, self.vtable.open_cursor, .{ self.allocator, self.ptr });
    }
};

pub const NamespaceWriteTxn = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror!void,
        open_cursor: ?*const fn (Allocator, *anyopaque, backend_types.Namespace) anyerror!Cursor = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *NamespaceWriteTxn) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commit(self: *NamespaceWriteTxn) !void {
        try BoundaryAbi.call("commit", self.boundary_dispatch, self.vtable.commit, .{ self.allocator, self.ptr });
        self.* = undefined;
    }

    pub fn get(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, namespace, key });
    }

    pub fn getManySorted(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        if (self.vtable.get_many_sorted) |get_many_sorted| return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, namespace, keys, values });
        @memset(values, null);
        for (keys, values) |key, *value| {
            value.* = BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, namespace, key }) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
    }

    pub fn put(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        try BoundaryAbi.call("put", self.boundary_dispatch, self.vtable.put, .{ self.ptr, namespace, key, value });
    }

    pub fn appendPut(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try BoundaryAbi.call("append_put", self.boundary_dispatch, append_put, .{ self.ptr, namespace, key, value });
    }

    pub fn delete(self: *NamespaceWriteTxn, namespace: backend_types.Namespace, key: []const u8) !void {
        try BoundaryAbi.call("delete", self.boundary_dispatch, self.vtable.delete, .{ self.ptr, namespace, key });
    }

    pub fn openCursor(self: *NamespaceWriteTxn, namespace: backend_types.Namespace) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, open_cursor, .{ self.allocator, self.ptr, namespace });
    }
};

/// Borrowed scalar/key plan derived by the trusted preparation owner. The
/// backend validates its frame binding and funds it in the open writer batch.
pub const PreparedReplayReservation = struct {
    txn_id: [16]u8,
    frame: [32]u8,
    base: @import("replay_record_shape.zig").Record,
    roots: []const []const u8,
};

pub const Batch = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    write_gate: ?*std.atomic.Mutex = null,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        commit_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!void = null,
        get: *const fn (*anyopaque, []const u8) anyerror![]const u8,
        has_prefix: ?*const fn (*anyopaque, []const u8) anyerror!bool = null,
        get_many_sorted: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        open_cursor: ?*const fn (Allocator, *anyopaque) anyerror!Cursor = null,
        set_replay_opaque: ?*const fn (*anyopaque, u64, []const u8) anyerror!void = null,
        set_commit_participant: ?*const fn (*anyopaque, @import("commit_participant.zig").Participant) anyerror!void = null,
        contains_many_sorted: ?*const fn (*anyopaque, []const []const u8, []bool) anyerror!void = null,
        reserve_prepared_replay: ?*const fn (*anyopaque, PreparedReplayReservation, ?read_admission.Port) anyerror!void = null,
        cancel_prepared_replay: ?*const fn (*anyopaque, [16]u8) anyerror!void = null,
        retire_prepared_read_guards: ?*const fn (*anyopaque, [16]u8) anyerror!void = null,
        set_read_guard_scope: ?*const fn (*anyopaque, @import("transaction_read_guards.zig").Scope) anyerror!void = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn retirePreparedReadGuards(self: *Batch, id: [16]u8) !void {
        const retire = self.vtable.retire_prepared_read_guards orelse return error.Unsupported;
        try BoundaryAbi.call("retire_prepared_read_guards", self.boundary_dispatch, retire, .{ self.ptr, id });
    }

    pub fn setReadGuardScope(self: *Batch, scope: @import("transaction_read_guards.zig").Scope) !void {
        const set = self.vtable.set_read_guard_scope orelse return error.Unsupported;
        try BoundaryAbi.call("set_read_guard_scope", self.boundary_dispatch, set, .{ self.ptr, scope });
    }

    pub fn cancelPreparedReplay(self: *Batch, id: [16]u8) !void {
        const cancel = self.vtable.cancel_prepared_replay orelse return error.Unsupported;
        try BoundaryAbi.call("cancel_prepared_replay", self.boundary_dispatch, cancel, .{ self.ptr, id });
    }

    pub fn reservePreparedReplay(self: *Batch, plan: PreparedReplayReservation, original: ?read_admission.Port) !void {
        const reserve = self.vtable.reserve_prepared_replay orelse return error.Unsupported;
        if (original) |port| try port.checkpoint();
        try BoundaryAbi.call("reserve_prepared_replay", self.boundary_dispatch, reserve, .{ self.ptr, plan, original });
    }

    pub fn abort(self: *Batch) void {
        const gate = self.write_gate;
        self.vtable.abort(self.allocator, self.ptr);
        if (gate) |mutex| mutex.unlock();
        self.* = undefined;
    }

    pub fn commitWithAdmission(self: *Batch, original: read_admission.Port) !void {
        try original.checkpoint();
        if (self.vtable.commit_with_admission) |commit_callback| {
            try BoundaryAbi.call("commit_with_admission", self.boundary_dispatch, commit_callback, .{ self.allocator, self.ptr, original });
            if (self.write_gate) |mutex| mutex.unlock();
            self.* = undefined;
            return;
        }
        return self.commit();
    }

    pub fn commit(self: *Batch) !void {
        try BoundaryAbi.call("commit", self.boundary_dispatch, self.vtable.commit, .{ self.allocator, self.ptr });
        if (self.write_gate) |mutex| mutex.unlock();
        self.* = undefined;
    }

    pub fn get(self: *Batch, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, key });
    }

    /// Return presence only. Native backends avoid retained value payloads and
    /// share sorted-run/block probes; portable fallbacks preserve get semantics.
    pub fn containsManySorted(self: *Batch, keys: []const []const u8, present: []bool) !void {
        if (keys.len != present.len) return error.InvalidBatch;
        for (keys, 0..) |key, i| if (i != 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.InvalidBatch;
        @memset(present, false);
        if (self.vtable.contains_many_sorted) |contains| return BoundaryAbi.call("contains_many_sorted", self.boundary_dispatch, contains, .{ self.ptr, keys, present });
        for (keys, present) |key, *exists| exists.* = if (self.get(key)) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
    }

    pub fn getManySorted(self: *Batch, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        @memset(values, null);
        if (self.vtable.get_many_sorted) |get_many_sorted| {
            return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, keys, values });
        }
        for (keys, 0..) |key, i| {
            values[i] = self.get(key) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
        }
    }

    pub fn put(self: *Batch, key: []const u8, value: []const u8) !void {
        try BoundaryAbi.call("put", self.boundary_dispatch, self.vtable.put, .{ self.ptr, key, value });
    }

    pub fn appendPut(self: *Batch, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try BoundaryAbi.call("append_put", self.boundary_dispatch, append_put, .{ self.ptr, key, value });
    }

    pub fn delete(self: *Batch, key: []const u8) !void {
        try BoundaryAbi.call("delete", self.boundary_dispatch, self.vtable.delete, .{ self.ptr, key });
    }

    /// Test the current transactional key view without hydrating values.
    /// Backends without native support retain ordinary cursor semantics.
    pub fn hasPrefix(self: *Batch, prefix: []const u8) !bool {
        if (self.vtable.has_prefix) |has_prefix| {
            return try BoundaryAbi.call("has_prefix", self.boundary_dispatch, has_prefix, .{ self.ptr, prefix });
        }
        var cursor = try self.openCursor();
        defer cursor.close();
        const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
        return std.mem.startsWith(u8, row.key, prefix);
    }

    pub fn openCursor(self: *Batch) !Cursor {
        const open_cursor = self.vtable.open_cursor orelse return error.Unsupported;
        return try BoundaryAbi.call("open_cursor", self.boundary_dispatch, open_cursor, .{ self.allocator, self.ptr });
    }

    pub fn setReplayOpaque(self: *Batch, sequence: u64, payload: []const u8) !void {
        const set_replay_opaque = self.vtable.set_replay_opaque orelse return error.Unsupported;
        try BoundaryAbi.call("set_replay_opaque", self.boundary_dispatch, set_replay_opaque, .{ self.ptr, sequence, payload });
    }

    /// Optional facade capability, not a raw-engine callback. Unsupported
    /// backends must fail rather than silently omit atomic acceptance work.
    pub fn setCommitParticipant(self: *Batch, participant: @import("commit_participant.zig").Participant) !void {
        const attach = self.vtable.set_commit_participant orelse return error.Unsupported;
        try BoundaryAbi.call("set_commit_participant", self.boundary_dispatch, attach, .{ self.ptr, participant });
    }
};

test "ordered artifact inventory erased batches reject unsupported commit participants" {
    const Raw = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
    };
    const Observer = struct {
        fn touch(ptr: *anyopaque) void {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
        }
        fn observe(ptr: *anyopaque, _: []const u8, _: ?[]const u8) void {
            touch(ptr);
        }
        fn stage(ptr: *anyopaque, _: @import("commit_participant.zig").View, _: []const u8) !void {
            touch(ptr);
        }
    };
    var calls: usize = 0;
    var batch = try batchFrom(std.testing.allocator, Raw{});
    defer batch.abort();
    try std.testing.expectError(error.Unsupported, batch.setCommitParticipant(.{
        .ptr = &calls,
        .reset = Observer.touch,
        .observe = Observer.observe,
        .stage = Observer.stage,
    }));
    try std.testing.expectEqual(@as(usize, 0), calls);
}

pub const NamespaceBatch = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        abort: *const fn (Allocator, *anyopaque) void,
        commit: *const fn (Allocator, *anyopaque) anyerror!void,
        commit_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!void = null,
        get: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror![]const u8,
        get_many_sorted: ?*const fn (*anyopaque, backend_types.Namespace, []const []const u8, []?[]const u8) anyerror!void = null,
        put: *const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void,
        append_put: ?*const fn (*anyopaque, backend_types.Namespace, []const u8, []const u8) anyerror!void = null,
        delete: *const fn (*anyopaque, backend_types.Namespace, []const u8) anyerror!void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn abort(self: *NamespaceBatch) void {
        self.vtable.abort(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn commitWithAdmission(self: *NamespaceBatch, original: read_admission.Port) !void {
        try original.checkpoint();
        if (self.vtable.commit_with_admission) |commit_callback| {
            try BoundaryAbi.call("commit_with_admission", self.boundary_dispatch, commit_callback, .{ self.allocator, self.ptr, original });
            self.* = undefined;
            return;
        }
        return self.commit();
    }

    pub fn commit(self: *NamespaceBatch) !void {
        try BoundaryAbi.call("commit", self.boundary_dispatch, self.vtable.commit, .{ self.allocator, self.ptr });
        self.* = undefined;
    }

    pub fn get(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        return try BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, namespace, key });
    }

    pub fn getManySorted(self: *NamespaceBatch, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
        if (keys.len != values.len) return error.InvalidBatch;
        if (self.vtable.get_many_sorted) |get_many_sorted| return try BoundaryAbi.call("get_many_sorted", self.boundary_dispatch, get_many_sorted, .{ self.ptr, namespace, keys, values });
        @memset(values, null);
        for (keys, values) |key, *value| {
            value.* = BoundaryAbi.call("get", self.boundary_dispatch, self.vtable.get, .{ self.ptr, namespace, key }) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
    }

    pub fn put(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        try BoundaryAbi.call("put", self.boundary_dispatch, self.vtable.put, .{ self.ptr, namespace, key, value });
    }

    pub fn appendPut(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        const append_put = self.vtable.append_put orelse return error.Unsupported;
        try BoundaryAbi.call("append_put", self.boundary_dispatch, append_put, .{ self.ptr, namespace, key, value });
    }

    pub fn delete(self: *NamespaceBatch, namespace: backend_types.Namespace, key: []const u8) !void {
        try BoundaryAbi.call("delete", self.boundary_dispatch, self.vtable.delete, .{ self.ptr, namespace, key });
    }
};

pub const Store = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    /// Opt-in transaction-wide serialization for read/modify/write users.
    /// The backend owns this gate and must outlive all stores/transactions.
    /// Reads and computation outside a write transaction remain concurrent.
    /// Contenders return WriterLocked before state construction; retries must
    /// not occupy a CPU slot waiting for another writer's parked durability IO.
    write_gate: ?*std.atomic.Mutex = null,
    /// Shared across all namespace views; borrowed for publication debt RMW.
    publication_gate: ?*std.atomic.Mutex = null,

    pub fn publicationGate(self: *Store) ?*std.atomic.Mutex {
        return self.publication_gate;
    }

    pub const ReplayCallback = *const fn (*anyopaque, u64, []const u8) anyerror!void;

    /// Synchronous borrowed replay delivery across archive boundaries. Zig
    /// error integers belong to the consumer's compilation and stay there.
    pub const ReplayVisitor = extern struct {
        context: *anyopaque,
        visit: *const fn (*anyopaque, u64, [*]const u8, usize) callconv(.c) bool,

        fn consume(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            const self: *const ReplayVisitor = @ptrCast(@alignCast(ptr));
            // Only this owner-local adapter returns a Zig error. The caller
            // restores its original failure after the scan unwinds.
            if (!self.visit(self.context, sequence, payload.ptr, payload.len)) return error.Cancelled;
        }
    };

    fn ReplayConsumer(comptime Context: type, comptime callback: fn (Context, u64, []const u8) anyerror!void) type {
        return struct {
            context: Context,
            failure: ?anyerror = null,

            fn visitor(self: *@This()) ReplayVisitor {
                return .{ .context = self, .visit = visit };
            }

            fn visit(ptr: *anyopaque, sequence: u64, data: [*]const u8, len: usize) callconv(.c) bool {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.failure != null) return false;
                callback(self.context, sequence, data[0..len]) catch |err| {
                    self.failure = err;
                    return false;
                };
                return true;
            }
        };
    }

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        capabilities: *const fn (*anyopaque) backend_types.Capabilities,
        begin_read: *const fn (Allocator, *anyopaque) anyerror!ReadTxn,
        begin_probe_with_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission, read_admission.Port) anyerror!ProbeTxn = null,
        begin_current_scan_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!CurrentScanTxn = null,
        begin_probe: ?*const fn (Allocator, *anyopaque) anyerror!ProbeTxn = null,
        begin_current_scan: ?*const fn (Allocator, *anyopaque) anyerror!CurrentScanTxn = null,
        begin_write: *const fn (Allocator, *anyopaque) anyerror!WriteTxn,
        begin_batch: *const fn (Allocator, *anyopaque) anyerror!Batch,
        begin_batch_with_options: ?*const fn (Allocator, *anyopaque, backend_types.BatchOptions) anyerror!Batch = null,
        sync: ?*const fn (*anyopaque, bool) anyerror!void = null,
        sync_replay_state: ?*const fn (*anyopaque) anyerror!void = null,
        begin_bulk_ingest_session: ?*const fn (*anyopaque) anyerror!void = null,
        finish_bulk_ingest_session: ?*const fn (*anyopaque, backend_types.BulkIngestFinishOptions) anyerror!void = null,
        flush_buffered_writes: ?*const fn (*anyopaque, backend_types.BulkIngestFinishOptions) anyerror!void = null,
        abort_bulk_ingest_session: ?*const fn (*anyopaque) void = null,
        last_replay_sequence: ?*const fn (*anyopaque, u64) u64 = null,
        next_replay_sequence: ?*const fn (*anyopaque, u64) u64 = null,
        append_replay_opaque: ?*const fn (Allocator, *anyopaque, u64, []const u8) anyerror!void = null,
        iterate_replay_from: ?*const fn (Allocator, *anyopaque, u64) anyerror![]ReplayEntry = null,
        for_each_replay_from: ?*const fn (*anyopaque, u64, ReplayVisitor) anyerror!void = null,
        for_each_replay_lane_from: ?*const fn (*anyopaque, u8, u64, usize, ReplayVisitor) anyerror!backend_types.ReplayLaneIterationStats = null,
        for_each_replay_from_matching_hint_mask: ?*const fn (*anyopaque, u64, u8, ReplayVisitor) anyerror!void = null,
        truncate_replay_up_to: ?*const fn (Allocator, *anyopaque, u64) anyerror!void = null,
        // Append-only optional ABI extensions: older stores retain their
        // original transaction layout and fall back to ordinary admission.
        begin_read_with_block_cache_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission) anyerror!ReadTxn = null,
        begin_probe_with_block_cache_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission) anyerror!ProbeTxn = null,
        /// Open one stable, range-scoped replay-lane view. The returned
        /// transaction may outlive multiple bounded derived-index windows.
        begin_replay_lane_scan: ?*const fn (Allocator, *anyopaque, u8, u64) anyerror!CurrentScanTxn = null,
        /// Required control never falls back to an unbounded legacy read.
        begin_read_with_admission: ?*const fn (Allocator, *anyopaque, backend_types.Namespace.BlockCacheAdmission, read_admission.Port) anyerror!ReadTxn = null,
        begin_owned_value_read: ?*const fn (Allocator, *anyopaque, []const u8, read_admission.Port) anyerror!OwnedValueRead = null,
        begin_owned_key_cursor: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!OwnedKeyCursor = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn deinit(self: *Store) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn capabilities(self: *Store) backend_types.Capabilities {
        return self.vtable.capabilities(self.ptr);
    }

    pub fn beginRead(self: *Store) !ReadTxn {
        return try BoundaryAbi.call("begin_read", self.boundary_dispatch, self.vtable.begin_read, .{ self.allocator, self.ptr });
    }

    /// The caller retains the control payload only until this call returns.
    pub fn beginReadWithAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) !ReadTxn {
        try control.checkpoint();
        const begin = self.vtable.begin_read_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("begin_read_with_admission", self.boundary_dispatch, begin, .{ self.allocator, self.ptr, admission, control });
    }

    /// Detached values and operation metadata belong to the independent caller
    /// root. Only synchronous original authority crosses the owning boundary.
    pub fn beginOwnedValueRead(self: *Store, allocator: Allocator, key: []const u8, control: read_admission.Port) !OwnedValueRead {
        try control.checkpoint();
        const begin = self.vtable.begin_owned_value_read orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var owned = BoundaryAbi.call("begin_owned_value_read", self.boundary_dispatch, begin, .{ allocator, self.ptr, key, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer owned.deinit();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return owned;
    }

    pub fn beginOwnedKeyCursor(self: *Store, allocator: Allocator, control: read_admission.Port) !OwnedKeyCursor {
        try control.checkpoint();
        const begin = self.vtable.begin_owned_key_cursor orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var owner = BoundaryAbi.call("begin_owned_key_cursor", self.boundary_dispatch, begin, .{ allocator, self.ptr, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer owner.deinit();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return owner;
    }

    pub fn beginReadWithBlockCacheAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission) !ReadTxn {
        if (self.vtable.begin_read_with_block_cache_admission) |begin_read| {
            return try BoundaryAbi.call("begin_read_with_block_cache_admission", self.boundary_dispatch, begin_read, .{ self.allocator, self.ptr, admission });
        }
        return try self.beginRead();
    }

    pub fn beginProbeWithAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) !ProbeTxn {
        try control.checkpoint();
        const open = self.vtable.begin_probe_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("begin_probe_with_admission", self.boundary_dispatch, open, .{ self.allocator, self.ptr, admission, control });
    }

    pub fn beginCurrentScanWithAdmission(self: *Store, control: read_admission.Port) !CurrentScanTxn {
        try control.checkpoint();
        const open = self.vtable.begin_current_scan_with_admission orelse return error.ExecutionWaitUnsupported;
        return try BoundaryAbi.call("begin_current_scan_with_admission", self.boundary_dispatch, open, .{ self.allocator, self.ptr, control });
    }

    pub fn beginProbe(self: *Store) !ProbeTxn {
        if (self.vtable.begin_probe) |begin_probe| {
            return try BoundaryAbi.call("begin_probe", self.boundary_dispatch, begin_probe, .{ self.allocator, self.ptr });
        }
        return try probeTxnFrom(self.allocator, try self.beginRead());
    }

    pub fn beginProbeWithBlockCacheAdmission(self: *Store, admission: backend_types.Namespace.BlockCacheAdmission) !ProbeTxn {
        if (self.vtable.begin_probe_with_block_cache_admission) |begin_probe| {
            return try BoundaryAbi.call("begin_probe_with_block_cache_admission", self.boundary_dispatch, begin_probe, .{ self.allocator, self.ptr, admission });
        }
        return try self.beginProbe();
    }

    pub fn beginCurrentScan(self: *Store) !CurrentScanTxn {
        if (self.vtable.begin_current_scan) |begin_current_scan| {
            return try BoundaryAbi.call("begin_current_scan", self.boundary_dispatch, begin_current_scan, .{ self.allocator, self.ptr });
        }
        return try currentScanTxnFrom(self.allocator, try self.beginRead());
    }

    pub fn beginReplayLaneScan(self: *Store, lane_ordinal: u8, from_sequence: u64) !CurrentScanTxn {
        if (self.vtable.begin_replay_lane_scan) |begin_replay_lane_scan| {
            return try BoundaryAbi.call("begin_replay_lane_scan", self.boundary_dispatch, begin_replay_lane_scan, .{
                self.allocator,
                self.ptr,
                lane_ordinal,
                from_sequence,
            });
        }
        return try self.beginCurrentScan();
    }

    pub fn beginWrite(self: *Store) !WriteTxn {
        if (self.write_gate) |mutex| if (!mutex.tryLock()) return error.WriterLocked;
        errdefer if (self.write_gate) |mutex| mutex.unlock();
        var txn = try BoundaryAbi.call("begin_write", self.boundary_dispatch, self.vtable.begin_write, .{ self.allocator, self.ptr });
        txn.write_gate = self.write_gate;
        return txn;
    }

    pub fn beginBatch(self: *Store) !Batch {
        if (self.write_gate) |mutex| if (!mutex.tryLock()) return error.WriterLocked;
        errdefer if (self.write_gate) |mutex| mutex.unlock();
        var batch = try BoundaryAbi.call("begin_batch", self.boundary_dispatch, self.vtable.begin_batch, .{ self.allocator, self.ptr });
        batch.write_gate = self.write_gate;
        return batch;
    }

    /// Controlled mutations retain their CPU and must not wait behind the
    /// store's serialization gate. The caller owns any retry of this batch.
    pub fn beginBatchWithAdmission(self: *Store, control: read_admission.Port) !Batch {
        try control.checkLifetime();
        if (self.write_gate) |mutex| if (!mutex.tryLock()) {
            try control.checkLifetime();
            return error.Backpressured;
        };
        errdefer if (self.write_gate) |mutex| mutex.unlock();
        var batch = try BoundaryAbi.call("begin_batch", self.boundary_dispatch, self.vtable.begin_batch, .{ self.allocator, self.ptr });
        // Abort owns gate release after the batch is returned to its caller.
        batch.write_gate = self.write_gate;
        return batch;
    }

    /// Controlled callers never occupy CPU waiting for the transaction gate.
    /// Preserve batch options and return backpressure before constructing state.
    pub fn beginBatchWithOptionsAndAdmission(self: *Store, options: backend_types.BatchOptions, original: read_admission.Port) !Batch {
        try original.checkpoint();
        if (self.write_gate) |mutex| if (!mutex.tryLock()) {
            try original.checkpoint();
            return error.Backpressured;
        };
        errdefer if (self.write_gate) |mutex| mutex.unlock();
        var batch = if (self.vtable.begin_batch_with_options) |begin_options|
            try BoundaryAbi.call("begin_batch_with_options", self.boundary_dispatch, begin_options, .{ self.allocator, self.ptr, options })
        else
            try BoundaryAbi.call("begin_batch", self.boundary_dispatch, self.vtable.begin_batch, .{ self.allocator, self.ptr });
        batch.write_gate = self.write_gate;
        return batch;
    }

    pub fn beginBatchWithOptions(self: *Store, options: backend_types.BatchOptions) !Batch {
        if (self.vtable.begin_batch_with_options) |begin_batch_with_options| {
            if (self.write_gate) |mutex| if (!mutex.tryLock()) return error.WriterLocked;
            errdefer if (self.write_gate) |mutex| mutex.unlock();
            var batch = try BoundaryAbi.call("begin_batch_with_options", self.boundary_dispatch, begin_batch_with_options, .{ self.allocator, self.ptr, options });
            batch.write_gate = self.write_gate;
            return batch;
        }
        return try self.beginBatch();
    }

    pub fn sync(self: *Store, force: bool) !void {
        if (self.vtable.sync) |sync_fn| {
            try BoundaryAbi.call("sync", self.boundary_dispatch, sync_fn, .{ self.ptr, force });
        }
    }

    pub fn syncReplayState(self: *Store) !void {
        if (self.vtable.sync_replay_state) |sync_replay_state_fn| {
            try BoundaryAbi.call("sync_replay_state", self.boundary_dispatch, sync_replay_state_fn, .{self.ptr});
            return;
        }
        try self.sync(false);
    }

    pub fn beginBulkIngestSession(self: *Store) !void {
        if (self.vtable.begin_bulk_ingest_session) |begin_bulk_ingest_session| {
            try BoundaryAbi.call("begin_bulk_ingest_session", self.boundary_dispatch, begin_bulk_ingest_session, .{self.ptr});
        }
    }

    pub fn finishBulkIngestSessionWithOptions(self: *Store, options: backend_types.BulkIngestFinishOptions) !void {
        if (self.vtable.finish_bulk_ingest_session) |finish_bulk_ingest_session| {
            try BoundaryAbi.call("finish_bulk_ingest_session", self.boundary_dispatch, finish_bulk_ingest_session, .{ self.ptr, options });
        }
    }

    pub fn flushBufferedWritesWithOptions(self: *Store, options: backend_types.BulkIngestFinishOptions) !void {
        if (self.vtable.flush_buffered_writes) |flush_buffered_writes| {
            try BoundaryAbi.call("flush_buffered_writes", self.boundary_dispatch, flush_buffered_writes, .{ self.ptr, options });
        }
    }

    pub fn abortBulkIngestSession(self: *Store) void {
        if (self.vtable.abort_bulk_ingest_session) |abort_bulk_ingest_session| {
            abort_bulk_ingest_session(self.ptr);
        }
    }

    pub fn lastReplaySequence(self: *Store, fallback_last: u64) u64 {
        if (self.vtable.last_replay_sequence) |f| return f(self.ptr, fallback_last);
        return fallback_last;
    }

    pub fn nextReplaySequence(self: *Store, fallback_next: u64) u64 {
        if (self.vtable.next_replay_sequence) |f| return f(self.ptr, fallback_next);
        return fallback_next;
    }

    pub fn appendReplayOpaque(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        const f = self.vtable.append_replay_opaque orelse return error.Unsupported;
        return try BoundaryAbi.call("append_replay_opaque", self.boundary_dispatch, f, .{ alloc, self.ptr, sequence, payload });
    }

    pub fn iterateReplayFrom(self: *Store, alloc: Allocator, from_sequence: u64) ![]ReplayEntry {
        const f = self.vtable.iterate_replay_from orelse return error.Unsupported;
        return try BoundaryAbi.call("iterate_replay_from", self.boundary_dispatch, f, .{ alloc, self.ptr, from_sequence });
    }

    pub fn forEachReplayFrom(
        self: *Store,
        from_sequence: u64,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (self.vtable.for_each_replay_from) |f| {
            var consumer = ReplayConsumer(@TypeOf(ctx), callback){ .context = ctx };
            const result = BoundaryAbi.call("for_each_replay_from", self.boundary_dispatch, f, .{ self.ptr, from_sequence, consumer.visitor() });
            if (consumer.failure) |err| return err;
            return result;
        }

        const entries = try self.iterateReplayFrom(self.allocator, from_sequence);
        defer {
            for (entries) |*entry| entry.deinit(self.allocator);
            self.allocator.free(entries);
        }
        for (entries) |entry| try callback(ctx, entry.sequence, entry.payload);
    }

    pub fn forEachReplayFromMatchingHintMask(
        self: *Store,
        from_sequence: u64,
        required_hint_mask: u8,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (self.vtable.for_each_replay_from_matching_hint_mask) |f| {
            var consumer = ReplayConsumer(@TypeOf(ctx), callback){ .context = ctx };
            const result = BoundaryAbi.call("for_each_replay_from_matching_hint_mask", self.boundary_dispatch, f, .{ self.ptr, from_sequence, required_hint_mask, consumer.visitor() });
            if (consumer.failure) |err| return err;
            return result;
        }
        if (self.vtable.for_each_replay_lane_from != null) {
            const lane_ordinal = if (required_hint_mask == 0)
                internal_keys.replay_all_kind
            else if (replayHintOrdinalFromSingleMask(required_hint_mask)) |ordinal|
                ordinal
            else
                return error.Unsupported;
            _ = try self.forEachReplayLaneFrom(lane_ordinal, from_sequence, 0, ctx, callback);
            return;
        }

        if (required_hint_mask != 0) return error.Unsupported;
        const entries = try self.iterateReplayFrom(self.allocator, from_sequence);
        defer {
            for (entries) |*entry| entry.deinit(self.allocator);
            self.allocator.free(entries);
        }
        for (entries) |entry| try callback(ctx, entry.sequence, entry.payload);
    }

    pub fn forEachReplayLaneFrom(
        self: *Store,
        lane_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !backend_types.ReplayLaneIterationStats {
        const f = self.vtable.for_each_replay_lane_from orelse return error.Unsupported;
        var consumer = ReplayConsumer(@TypeOf(ctx), callback){ .context = ctx };
        const result = BoundaryAbi.call("for_each_replay_lane_from", self.boundary_dispatch, f, .{ self.ptr, lane_ordinal, from_sequence, max_entries, consumer.visitor() });
        if (consumer.failure) |err| return err;
        return result;
    }

    pub fn truncateReplayUpTo(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        const f = self.vtable.truncate_replay_up_to orelse return error.Unsupported;
        return try BoundaryAbi.call("truncate_replay_up_to", self.boundary_dispatch, f, .{ alloc, self.ptr, up_to_sequence });
    }
};

/// Caller-allocator-owned bytes and their logical value span. The allocator
/// context, not a store/transaction wrapper, owns eventual destruction.
pub const OwnedValue = struct {
    allocator: Allocator,
    allocation: []u8,
    offset: usize,
    len: usize,
    pub fn bytes(self: @This()) []const u8 {
        return self.allocation[self.offset..][0..self.len];
    }
    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.allocation);
        self.* = undefined;
    }
};

pub const OwnedValueRead = struct {
    pub const Status = enum { pending, ready };
    allocator: Allocator,
    ptr: ?*anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    failure: ?anyerror = null,
    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        step: *const fn (*anyopaque, usize, read_admission.Port) anyerror!Status,
        take_output: *const fn (*anyopaque) anyerror!OwnedValue,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn step(self: *@This(), quantum: usize, control: read_admission.Port) !Status {
        if (quantum == 0 or quantum > 64 * 1024) return error.InvalidArgument;
        if (self.failure) |err| return err;
        const ptr = self.ptr orelse return error.TransactionClosed;
        return self.stepControlled(ptr, quantum, control) catch |err| {
            self.deinit();
            self.failure = err;
            return err;
        };
    }
    fn stepControlled(self: *@This(), ptr: *anyopaque, quantum: usize, control: read_admission.Port) !Status {
        try control.checkpoint();
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        const result = BoundaryAbi.call("step", self.boundary_dispatch, self.vtable.step, .{ ptr, quantum, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return result;
    }
    pub fn takeOutput(self: *@This()) !OwnedValue {
        const ptr = self.ptr orelse return error.TransactionClosed;
        const result = try BoundaryAbi.call("take_output", self.boundary_dispatch, self.vtable.take_output, .{ptr});
        self.deinit();
        return result;
    }
    pub fn deinit(self: *@This()) void {
        const ptr = self.ptr orelse return;
        self.ptr = null;
        self.vtable.deinit(self.allocator, ptr);
    }
};

fn ownedValueReadFrom(allocator: Allocator, handle: anytype) !OwnedValueRead {
    const Handle = @TypeOf(handle);
    const box = try allocBox(allocator, handle);
    const vt = struct {
        fn state(raw: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(raw));
        }
        fn deinit(_: Allocator, raw: *anyopaque) void {
            const owner = state(raw);
            const allocation = owner.allocator;
            owner.handle.deinit();
            allocation.destroy(owner);
        }
        fn step(raw: *anyopaque, quantum: usize, control: read_admission.Port) !OwnedValueRead.Status {
            return switch (try state(raw).handle.step(quantum, control)) {
                .pending => .pending,
                .ready => .ready,
            };
        }
        fn take(raw: *anyopaque) !OwnedValue {
            const value = try state(raw).handle.takeOutput();
            return .{ .allocator = value.allocator, .allocation = value.allocation, .offset = value.offset, .len = value.len };
        }
    };
    return .{ .allocator = allocator, .ptr = box, .vtable = &.{ .deinit = vt.deinit, .step = vt.step, .take_output = vt.take } };
}

/// Caller-funded cursor metadata and snapshot. Key spans are synchronous
/// borrows. intoValueRead transfers the same snapshot to a funded point reader.
pub const OwnedKeyCursor = struct {
    pub const Status = OwnedValueRead.Status;
    allocator: Allocator,
    ptr: ?*anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    failure: ?anyerror = null,
    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        seek: *const fn (*anyopaque, []const u8, ?[]const u8, bool, read_admission.Port) anyerror!void,
        step: *const fn (*anyopaque, usize, read_admission.Port) anyerror!Status,
        key: *const fn (*anyopaque) anyerror!?[]const u8,
        into_value: *const fn (*anyopaque, read_admission.Port) anyerror!OwnedValueRead,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);
    fn live(self: *@This()) !*anyopaque {
        if (self.failure) |err| return err;
        return self.ptr orelse error.TransactionClosed;
    }
    fn fail(self: *@This(), err: anyerror) anyerror {
        self.deinit();
        self.failure = err;
        return err;
    }
    pub fn seek(self: *@This(), lower: []const u8, upper: ?[]const u8, exclusive: bool, control: read_admission.Port) !void {
        const ptr = try self.live();
        self.seekControlled(ptr, lower, upper, exclusive, control) catch |err| return self.fail(err);
    }
    fn seekControlled(self: *@This(), ptr: *anyopaque, lower: []const u8, upper: ?[]const u8, exclusive: bool, control: read_admission.Port) !void {
        try control.checkpoint();
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        BoundaryAbi.call("seek", self.boundary_dispatch, self.vtable.seek, .{ ptr, lower, upper, exclusive, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
    }
    pub fn step(self: *@This(), quantum: usize, control: read_admission.Port) !Status {
        if (quantum == 0 or quantum > 65536) return error.InvalidArgument;
        const ptr = try self.live();
        return self.stepControlled(ptr, quantum, control) catch |err| return self.fail(err);
    }
    fn stepControlled(self: *@This(), ptr: *anyopaque, quantum: usize, control: read_admission.Port) !Status {
        try control.checkpoint();
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        const result = BoundaryAbi.call("step", self.boundary_dispatch, self.vtable.step, .{ ptr, quantum, borrowed }) catch |err| return relay.rejection orelse err;
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return result;
    }
    pub fn key(self: *@This(), control: read_admission.Port) !?[]const u8 {
        const ptr = try self.live();
        return self.keyControlled(ptr, control) catch |err| return self.fail(err);
    }
    fn keyControlled(self: *@This(), ptr: *anyopaque, control: read_admission.Port) !?[]const u8 {
        try control.checkpoint();
        const result = try BoundaryAbi.call("key", self.boundary_dispatch, self.vtable.key, .{ptr});
        try control.checkpoint();
        return result;
    }
    pub fn intoValueRead(self: *@This(), control: read_admission.Port) !OwnedValueRead {
        const ptr = try self.live();
        return self.intoControlled(ptr, control) catch |err| return self.fail(err);
    }
    fn intoControlled(self: *@This(), ptr: *anyopaque, control: read_admission.Port) !OwnedValueRead {
        try control.checkpoint();
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var result = BoundaryAbi.call("into_value", self.boundary_dispatch, self.vtable.into_value, .{ ptr, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer result.deinit();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        self.deinit();
        return result;
    }
    pub fn deinit(self: *@This()) void {
        const ptr = self.ptr orelse return;
        self.ptr = null;
        self.vtable.deinit(self.allocator, ptr);
    }
};

fn ownedKeyCursorFrom(allocator: Allocator, handle: anytype) !OwnedKeyCursor {
    const Handle = @TypeOf(handle);
    const box = try allocBox(allocator, handle);
    const vt = struct {
        fn state(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }
        fn deinit(_: Allocator, ptr: *anyopaque) void {
            const owner = state(ptr);
            const allocation = owner.allocator;
            owner.handle.deinit();
            allocation.destroy(owner);
        }
        fn seek(ptr: *anyopaque, lower: []const u8, upper: ?[]const u8, exclusive: bool, control: read_admission.Port) !void {
            return state(ptr).handle.seek(lower, upper, exclusive, control);
        }
        fn step(ptr: *anyopaque, quantum: usize, control: read_admission.Port) !OwnedKeyCursor.Status {
            return switch (try state(ptr).handle.step(quantum, control)) {
                .pending => .pending,
                .ready => .ready,
            };
        }
        fn key(ptr: *anyopaque) !?[]const u8 {
            return state(ptr).handle.key();
        }
        fn into(ptr: *anyopaque, control: read_admission.Port) !OwnedValueRead {
            const owner = state(ptr);
            var value = try owner.handle.intoValueRead(control);
            errdefer value.deinit();
            try control.checkpoint();
            return ownedValueReadFrom(owner.allocator, value);
        }
    };
    return .{ .allocator = allocator, .ptr = box, .vtable = &.{ .deinit = vt.deinit, .seek = vt.seek, .step = vt.step, .key = vt.key, .into_value = vt.into } };
}

pub const NamespaceStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        capabilities: *const fn (*anyopaque) backend_types.Capabilities,
        begin_read: *const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn,
        begin_read_with_admission: ?*const fn (Allocator, *anyopaque, read_admission.Port) anyerror!NamespaceReadTxn = null,
        begin_probe: ?*const fn (Allocator, *anyopaque) anyerror!NamespaceReadTxn = null,
        begin_owned_value_read: ?*const fn (Allocator, *anyopaque, backend_types.Namespace, []const u8, read_admission.Port) anyerror!OwnedValueRead = null,
        begin_write: *const fn (Allocator, *anyopaque) anyerror!NamespaceWriteTxn,
        begin_batch: *const fn (Allocator, *anyopaque) anyerror!NamespaceBatch,
        begin_batch_with_options: ?*const fn (Allocator, *anyopaque, backend_types.BatchOptions) anyerror!NamespaceBatch = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn deinit(self: *NamespaceStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn capabilities(self: *NamespaceStore) backend_types.Capabilities {
        return self.vtable.capabilities(self.ptr);
    }

    pub fn beginRead(self: *NamespaceStore) !NamespaceReadTxn {
        return try BoundaryAbi.call("begin_read", self.boundary_dispatch, self.vtable.begin_read, .{ self.allocator, self.ptr });
    }

    pub fn beginReadWithAdmission(self: *NamespaceStore, control: read_admission.Port) !NamespaceReadTxn {
        try control.checkpoint();
        const begin = self.vtable.begin_read_with_admission orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var txn = BoundaryAbi.call("begin_read_with_admission", self.boundary_dispatch, begin, .{ self.allocator, self.ptr, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer txn.abort();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return txn;
    }

    pub fn beginOwnedValueRead(self: *@This(), allocator: Allocator, namespace: backend_types.Namespace, key: []const u8, control: read_admission.Port) !OwnedValueRead {
        try control.checkpoint();
        const begin = self.vtable.begin_owned_value_read orelse return error.ExecutionWaitUnsupported;
        var relay: read_admission.ErrorRelay = .{};
        const borrowed = try relay.bind(control);
        var owned = BoundaryAbi.call("begin_owned_value_read", self.boundary_dispatch, begin, .{ allocator, self.ptr, namespace, key, borrowed }) catch |err| return relay.rejection orelse err;
        errdefer owned.deinit();
        if (relay.rejection) |err| return err;
        try control.checkpoint();
        return owned;
    }

    pub fn beginProbe(self: *NamespaceStore) !NamespaceReadTxn {
        if (self.vtable.begin_probe) |begin_probe| {
            return try BoundaryAbi.call("begin_probe", self.boundary_dispatch, begin_probe, .{ self.allocator, self.ptr });
        }
        return try self.beginRead();
    }

    pub fn beginWrite(self: *NamespaceStore) !NamespaceWriteTxn {
        return try BoundaryAbi.call("begin_write", self.boundary_dispatch, self.vtable.begin_write, .{ self.allocator, self.ptr });
    }

    pub fn beginBatch(self: *NamespaceStore) !NamespaceBatch {
        return try BoundaryAbi.call("begin_batch", self.boundary_dispatch, self.vtable.begin_batch, .{ self.allocator, self.ptr });
    }

    pub fn beginBatchWithOptions(self: *NamespaceStore, options: backend_types.BatchOptions) !NamespaceBatch {
        if (self.vtable.begin_batch_with_options) |begin_batch_with_options| {
            return try BoundaryAbi.call("begin_batch_with_options", self.boundary_dispatch, begin_batch_with_options, .{ self.allocator, self.ptr, options });
        }
        return try self.beginBatch();
    }
};

pub fn cursorFrom(allocator: Allocator, handle: anytype) !Cursor {
    return try cursorFromWithParent(allocator, handle, null);
}

fn cursorFromWithParent(allocator: Allocator, handle: anytype, parent: ?ParentRelease) !Cursor {
    const Handle = @TypeOf(handle);
    const cursor_box_allocator = wrapperBoxAllocator(allocator);
    const CursorBox = struct {
        allocator: Allocator,
        handle: Handle,
        parent: ?ParentRelease,
    };
    const box_ptr = try cursor_box_allocator.create(CursorBox);
    box_ptr.* = .{
        .allocator = cursor_box_allocator,
        .handle = handle,
        .parent = parent,
    };

    const vt = struct {
        fn unbox(ptr: *anyopaque) *CursorBox {
            return @ptrCast(@alignCast(ptr));
        }

        fn close(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.close();
            if (state.parent) |retained_parent| retained_parent.release(retained_parent.ptr);
            box_allocator.destroy(state);
        }

        fn first(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.first() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn last(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.last() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn next(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.next() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn prev(ptr: *anyopaque) anyerror!?Entry {
            return unbox(ptr).handle.prev() catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn seekAtOrAfter(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return unbox(ptr).handle.seekAtOrAfter(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        fn seekAtOrBefore(ptr: *anyopaque, key: []const u8) anyerror!?Entry {
            return unbox(ptr).handle.seekAtOrBefore(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn nextWithAdmission(ptr: *anyopaque, control: read_admission.Port) anyerror!?Entry {
            var trace: read_admission.RejectionTrace = .{};
            const result = unbox(ptr).handle.nextWithAdmission(trace.bind(control).?);
            if (trace.failure) |cause| return cause;
            return result catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }
        pub fn seekAtOrAfterWithAdmission(ptr: *anyopaque, key: []const u8, control: read_admission.Port) anyerror!?Entry {
            var trace: read_admission.RejectionTrace = .{};
            const result = unbox(ptr).handle.seekAtOrAfterWithAdmission(key, trace.bind(control).?);
            if (trace.failure) |cause| return cause;
            return result catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
        }

        pub fn setKeysOnly(ptr: *anyopaque) void {
            unbox(ptr).handle.setKeysOnly();
        }

        pub fn setUpperBound(ptr: *anyopaque, upper: ?[]const u8) void {
            unbox(ptr).handle.setUpperBound(upper);
        }
    };

    return .{
        .allocator = cursor_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .close = vt.close,
            .next_with_admission = if (@hasDecl(Handle, "nextWithAdmission")) vt.nextWithAdmission else null,
            .seek_at_or_after_with_admission = if (@hasDecl(Handle, "seekAtOrAfterWithAdmission")) vt.seekAtOrAfterWithAdmission else null,
            .first = vt.first,
            .last = vt.last,
            .next = vt.next,
            .prev = vt.prev,
            .seek_at_or_after = vt.seekAtOrAfter,
            .seek_at_or_before = vt.seekAtOrBefore,
            .set_upper_bound = if (@hasDecl(Handle, "setUpperBound")) vt.setUpperBound else null,
            .set_keys_only = if (@hasDecl(Handle, "setKeysOnly")) vt.setKeysOnly else null,
        },
    };
}

pub fn readTxnFrom(allocator: Allocator, handle: anytype) !ReadTxn {
    return readTxnFromWithParent(allocator, handle, null);
}

fn readTxnFromWithParent(allocator: Allocator, handle: anytype, parent_release: ?ParentRelease) anyerror!ReadTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);
    box_ptr.parent_release = parent_release;

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn decodedResultsNamespace(ptr: *anyopaque) ?[]const u8 {
            return unbox(ptr).handle.decodedResultsNamespace();
        }

        fn canReuseDecodedResults(ptr: *anyopaque) bool {
            return unbox(ptr).handle.canReuseDecodedResults();
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        pub fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        pub fn getManySortedWithAdmission(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) anyerror!void {
            const parent = unbox(ptr);
            try parent.retainChild();
            defer parent.releaseChild();
            return parent.handle.getManySortedWithAdmission(keys, values, control);
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }

        pub fn openCursorWithAdmission(alloc: Allocator, ptr: *anyopaque, control: read_admission.Port) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursorWithAdmission(control);
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }

        pub fn forkRead(alloc: Allocator, ptr: *anyopaque) anyerror!ReadTxn {
            const parent = unbox(ptr);
            try parent.retainChild();
            defer parent.releaseChild();
            // Forks pin the original immutable owner, never their immediate
            // scratch handle. Repeated fork/close therefore retains constant
            // ownership depth and cannot recurse through an unbounded chain.
            const anchor = parent.parent_release orelse parentReleaseFor(parent);
            try anchor.retain_snapshot(anchor.ptr);
            errdefer anchor.release(anchor.ptr);
            var forked = try parent.handle.forkBorrowedRead();
            errdefer forked.abort();
            return readTxnFromWithParent(alloc, forked, anchor);
        }

        pub fn openReadScope(alloc: Allocator, ptr: *anyopaque) anyerror!ReadScope {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var scope = try parent.handle.openReadScope(alloc);
            errdefer scope.close();
            return readScopeFromWithParent(alloc, scope, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .identity = newReadTransactionIdentity(),
        .vtable = &.{
            .decoded_results_namespace = if (@hasDecl(Handle, "decodedResultsNamespace")) vt.decodedResultsNamespace else null,
            .can_reuse_decoded_results = if (@hasDecl(Handle, "canReuseDecodedResults")) vt.canReuseDecodedResults else null,
            .abort = vt.abort,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .get_many_sorted_with_admission = if (@hasDecl(Handle, "getManySortedWithAdmission")) vt.getManySortedWithAdmission else null,
            .open_cursor_with_admission = if (@hasDecl(Handle, "openCursorWithAdmission")) vt.openCursorWithAdmission else null,
            .open_cursor = vt.openCursor,
            .fork_read = if (@hasDecl(Handle, "forkBorrowedRead")) vt.forkRead else null,
            .open_read_scope = if (@hasDecl(Handle, "openReadScope")) vt.openReadScope else null,
        },
    };
}

pub fn probeTxnFrom(allocator: Allocator, handle: anytype) !ProbeTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.abort();
            box_allocator.destroy(state);
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn contains(ptr: *anyopaque, key: []const u8) anyerror!bool {
            return try unbox(ptr).handle.contains(key);
        }

        pub fn getLeased(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            if (@hasDecl(Handle, "getLeased")) return try unbox(ptr).handle.getLeased(key);
            return try unbox(ptr).handle.get(key);
        }

        pub fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        pub fn getManySortedWithAdmission(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) anyerror!void {
            return try unbox(ptr).handle.getManySortedWithAdmission(keys, values, control);
        }

        pub fn getManySortedWithBlockCacheAdmission(
            ptr: *anyopaque,
            keys: []const []const u8,
            values: []?[]const u8,
            admission: backend_types.Namespace.BlockCacheAdmission,
        ) anyerror!void {
            if (@hasDecl(Handle, "getManySortedWithBlockCacheAdmission")) {
                return try unbox(ptr).handle.getManySortedWithBlockCacheAdmission(keys, values, admission);
            }
            return try getManySorted(ptr, keys, values);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .get_many_sorted_is_atomic = @hasDecl(Handle, "get_many_sorted_is_atomic") and Handle.get_many_sorted_is_atomic,
            .abort = vt.abort,
            .get = vt.get,
            .contains = if (@hasDecl(Handle, "contains")) vt.contains else null,
            .get_leased = vt.getLeased,
            .get_many_sorted = vt.getManySorted,
            .get_many_sorted_with_admission = if (@hasDecl(Handle, "getManySortedWithAdmission")) vt.getManySortedWithAdmission else null,
            .get_many_sorted_with_block_cache_admission = vt.getManySortedWithBlockCacheAdmission,
        },
    };
}

pub fn currentScanTxnFrom(allocator: Allocator, handle: anytype) !CurrentScanTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
        pub fn openCursorWithAdmission(alloc: Allocator, ptr: *anyopaque, control: read_admission.Port) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursorWithAdmission(control);
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .open_cursor = vt.openCursor,
            .open_cursor_with_admission = if (@hasDecl(Handle, "openCursorWithAdmission")) vt.openCursorWithAdmission else null,
        },
    };
}

pub fn namespaceReadTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceReadTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        pub fn getWithAdmission(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, control: read_admission.Port) anyerror![]const u8 {
            return try unbox(ptr).handle.getWithAdmission(try mapNamespace(namespace), key, control);
        }

        pub fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(try mapNamespace(namespace), key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: backend_types.Namespace) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor(try mapNamespace(namespace));
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .get = vt.get,
            .get_with_admission = if (@hasDecl(Handle, "getWithAdmission")) vt.getWithAdmission else null,
            .get_many_sorted = vt.getManySorted,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn writeTxnFrom(allocator: Allocator, handle: anytype) !WriteTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        fn contains(ptr: *anyopaque, key: []const u8) anyerror!bool {
            return unbox(ptr).handle.contains(key);
        }

        pub fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (keys.len != values.len) return error.InvalidBatch;
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn hasPrefix(ptr: *anyopaque, prefix: []const u8) anyerror!bool {
            return try unbox(ptr).handle.hasPrefix(prefix);
        }

        fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(key, value);
        }

        fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(key);
        }

        pub fn openReadScope(alloc: Allocator, ptr: *anyopaque) anyerror!ReadScope {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var scope = try parent.handle.openReadScope(alloc);
            errdefer scope.close();
            return readScopeFromWithParent(alloc, scope, parentReleaseFor(parent));
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .contains = if (@hasDecl(Handle, "contains")) vt.contains else null,
            .get_many_sorted = vt.getManySorted,
            .has_prefix = if (@hasDecl(Handle, "hasPrefix")) vt.hasPrefix else null,
            .put = vt.put,
            .delete = vt.delete,
            .open_cursor = vt.openCursor,
            .open_read_scope = if (@hasDecl(Handle, "openReadScope")) vt.openReadScope else null,
        },
    };
}

pub fn namespaceWriteTxnFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceWriteTxn {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        pub fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
        }

        fn put(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        pub fn appendPut(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque, namespace: backend_types.Namespace) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor(try mapNamespace(namespace));
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .get = vt.get,
            .get_many_sorted = if (@hasDecl(Handle, "getManySorted")) vt.getManySorted else null,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
        },
    };
}

pub fn batchFrom(allocator: Allocator, handle: anytype) !Batch {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocParentBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *ParentBox(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            unbox(ptr).requestAbort();
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            try unbox(ptr).commit();
        }

        fn commitWithAdmission(_: Allocator, ptr: *anyopaque, original: read_admission.Port) anyerror!void {
            try unbox(ptr).commitWithAdmission(original);
        }

        fn get(ptr: *anyopaque, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(key);
        }

        pub fn containsManySorted(ptr: *anyopaque, keys: []const []const u8, present: []bool) anyerror!void {
            return unbox(ptr).handle.containsManySorted(keys, present);
        }

        pub fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            if (@hasDecl(Handle, "getManySorted")) {
                return try unbox(ptr).handle.getManySorted(keys, values);
            }
            for (keys, 0..) |key, i| {
                values[i] = unbox(ptr).handle.get(key) catch |err| blk: {
                    if (err == error.NotFound) break :blk null;
                    return err;
                };
            }
        }

        fn hasPrefix(ptr: *anyopaque, prefix: []const u8) anyerror!bool {
            return try unbox(ptr).handle.hasPrefix(prefix);
        }

        fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(key, value);
        }

        pub fn appendPut(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(key, value);
        }

        fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(key);
        }

        pub fn openCursor(alloc: Allocator, ptr: *anyopaque) anyerror!Cursor {
            const parent = unbox(ptr);
            try parent.retainChild();
            errdefer parent.releaseChild();
            var cursor = try parent.handle.openCursor();
            errdefer cursor.close();
            return try cursorFromWithParent(alloc, cursor, parentReleaseFor(parent));
        }

        pub fn setReplayOpaque(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            if (@hasDecl(Handle, "setReplayOpaque")) {
                return try unbox(ptr).handle.setReplayOpaque(sequence, payload);
            }
            return error.Unsupported;
        }

        pub fn retirePreparedReadGuards(ptr: *anyopaque, id: [16]u8) anyerror!void {
            try unbox(ptr).handle.retirePreparedReadGuards(id);
        }

        pub fn setReadGuardScope(ptr: *anyopaque, scope: @import("transaction_read_guards.zig").Scope) anyerror!void {
            try unbox(ptr).handle.setReadGuardScope(scope);
        }

        pub fn cancelPreparedReplay(ptr: *anyopaque, id: [16]u8) anyerror!void {
            try unbox(ptr).handle.cancelPreparedReplayCredit(id);
        }

        pub fn reservePreparedReplay(ptr: *anyopaque, plan: PreparedReplayReservation, original: ?read_admission.Port) anyerror!void {
            try unbox(ptr).handle.reservePreparedReplayCredit(plan.txn_id, plan.frame, plan.base, plan.roots, original);
        }

        pub fn setCommitParticipant(ptr: *anyopaque, participant: @import("commit_participant.zig").Participant) anyerror!void {
            return unbox(ptr).handle.setCommitParticipant(participant);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .commit_with_admission = vt.commitWithAdmission,
            .get = vt.get,
            .get_many_sorted = vt.getManySorted,
            .has_prefix = if (@hasDecl(Handle, "hasPrefix")) vt.hasPrefix else null,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
            .open_cursor = if (@hasDecl(Handle, "openCursor")) vt.openCursor else null,
            .set_replay_opaque = if (@hasDecl(Handle, "setReplayOpaque")) vt.setReplayOpaque else null,
            .set_commit_participant = if (@hasDecl(Handle, "setCommitParticipant")) vt.setCommitParticipant else null,
            .contains_many_sorted = if (@hasDecl(Handle, "containsManySorted")) vt.containsManySorted else null,
            .reserve_prepared_replay = if (@hasDecl(Handle, "reservePreparedReplayCredit")) vt.reservePreparedReplay else null,
            .set_read_guard_scope = if (@hasDecl(Handle, "setReadGuardScope")) vt.setReadGuardScope else null,
            .retire_prepared_read_guards = if (@hasDecl(Handle, "retirePreparedReadGuards")) vt.retirePreparedReadGuards else null,
            .cancel_prepared_replay = if (@hasDecl(Handle, "cancelPreparedReplayCredit")) vt.cancelPreparedReplay else null,
        },
    };
}

pub fn namespaceBatchFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceBatch {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        fn abort(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            state.handle.abort();
            box_allocator.destroy(state);
        }

        fn commit(_: Allocator, ptr: *anyopaque) anyerror!void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            try state.handle.commit();
            box_allocator.destroy(state);
        }

        fn commitWithAdmission(_: Allocator, ptr: *anyopaque, original: read_admission.Port) anyerror!void {
            const state = unbox(ptr);
            if (comptime @hasDecl(Handle, "commitWithAdmission")) try state.handle.commitWithAdmission(original) else {
                try original.checkpoint();
                try state.handle.commit();
            }
            state.allocator.destroy(state);
        }

        fn get(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror![]const u8 {
            return try unbox(ptr).handle.get(try mapNamespace(namespace), key);
        }

        pub fn getManySorted(ptr: *anyopaque, namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            try unbox(ptr).handle.getManySorted(try mapNamespace(namespace), keys, values);
        }

        fn put(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.put(try mapNamespace(namespace), key, value);
        }

        pub fn appendPut(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, value: []const u8) anyerror!void {
            try unbox(ptr).handle.appendPut(try mapNamespace(namespace), key, value);
        }

        fn delete(ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8) anyerror!void {
            try unbox(ptr).handle.delete(try mapNamespace(namespace), key);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .abort = vt.abort,
            .commit = vt.commit,
            .commit_with_admission = vt.commitWithAdmission,
            .get = vt.get,
            .get_many_sorted = if (@hasDecl(Handle, "getManySorted")) vt.getManySorted else null,
            .put = vt.put,
            .append_put = if (@hasDecl(Handle, "appendPut")) vt.appendPut else null,
            .delete = vt.delete,
        },
    };
}

pub fn storeFrom(allocator: Allocator, handle: anytype) !Store {
    const Handle = @TypeOf(handle);
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        pub fn deinit(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            box_allocator.destroy(state);
        }

        fn capabilities(ptr: *anyopaque) backend_types.Capabilities {
            return unbox(ptr).handle.capabilities();
        }

        fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!ReadTxn {
            return try readTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        pub fn beginOwnedKeyCursor(alloc: Allocator, ptr: *anyopaque, control: read_admission.Port) !OwnedKeyCursor {
            if (Handle == Store) return unbox(ptr).handle.beginOwnedKeyCursor(alloc, control);
            var owner = try unbox(ptr).handle.beginOwnedKeyCursor(alloc, control);
            errdefer owner.deinit();
            try control.checkpoint();
            return ownedKeyCursorFrom(alloc, owner);
        }

        pub fn beginOwnedValueRead(alloc: Allocator, ptr: *anyopaque, key: []const u8, control: read_admission.Port) !OwnedValueRead {
            if (Handle == Store) return unbox(ptr).handle.beginOwnedValueRead(alloc, key, control);
            var owner = try unbox(ptr).handle.beginOwnedValueRead(alloc, key, control);
            errdefer owner.deinit();
            try control.checkpoint();
            return ownedValueReadFrom(alloc, owner);
        }

        pub fn beginReadWithAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) anyerror!ReadTxn {
            if (Handle == Store) return try unbox(ptr).handle.beginReadWithAdmission(admission, control);
            var txn = try unbox(ptr).handle.beginReadWithAdmission(admission, control);
            errdefer txn.abort();
            return try readTxnFrom(alloc, txn);
        }

        pub fn beginReadWithBlockCacheAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission) anyerror!ReadTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginReadWithBlockCacheAdmission(admission);
            }
            return try readTxnFrom(alloc, try unbox(ptr).handle.beginReadWithBlockCacheAdmission(admission));
        }

        pub fn beginProbeWithAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) anyerror!ProbeTxn {
            if (Handle == Store) return unbox(ptr).handle.beginProbeWithAdmission(admission, control);
            var txn = try unbox(ptr).handle.beginProbeWithAdmission(admission, control);
            errdefer txn.abort();
            return probeTxnFrom(alloc, txn);
        }

        pub fn beginCurrentScanWithAdmission(alloc: Allocator, ptr: *anyopaque, control: read_admission.Port) anyerror!CurrentScanTxn {
            if (Handle == Store) return unbox(ptr).handle.beginCurrentScanWithAdmission(control);
            var txn = try unbox(ptr).handle.beginCurrentScanWithAdmission(control);
            errdefer txn.abort();
            return currentScanTxnFrom(alloc, txn);
        }

        pub fn beginProbe(alloc: Allocator, ptr: *anyopaque) anyerror!ProbeTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginProbe();
            }
            if (@hasDecl(Handle, "beginProbe")) {
                return try probeTxnFrom(alloc, try unbox(ptr).handle.beginProbe());
            }
            return try probeTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        pub fn beginProbeWithBlockCacheAdmission(alloc: Allocator, ptr: *anyopaque, admission: backend_types.Namespace.BlockCacheAdmission) anyerror!ProbeTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginProbeWithBlockCacheAdmission(admission);
            }
            return try probeTxnFrom(alloc, try unbox(ptr).handle.beginProbeWithBlockCacheAdmission(admission));
        }

        pub fn beginCurrentScan(alloc: Allocator, ptr: *anyopaque) anyerror!CurrentScanTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginCurrentScan();
            }
            if (@hasDecl(Handle, "beginCurrentScan")) {
                return try currentScanTxnFrom(alloc, try unbox(ptr).handle.beginCurrentScan());
            }
            return try currentScanTxnFrom(alloc, try unbox(ptr).handle.beginRead());
        }

        pub fn beginReplayLaneScan(alloc: Allocator, ptr: *anyopaque, lane_ordinal: u8, from_sequence: u64) anyerror!CurrentScanTxn {
            if (Handle == Store) {
                return try unbox(ptr).handle.beginReplayLaneScan(lane_ordinal, from_sequence);
            }
            if (@hasDecl(Handle, "beginReplayLaneScan")) {
                return try currentScanTxnFrom(alloc, try unbox(ptr).handle.beginReplayLaneScan(lane_ordinal, from_sequence));
            }
            return try beginCurrentScan(alloc, ptr);
        }

        fn beginWrite(alloc: Allocator, ptr: *anyopaque) anyerror!WriteTxn {
            return try writeTxnFrom(alloc, try unbox(ptr).handle.beginWrite());
        }

        fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!Batch {
            return try batchFrom(alloc, try unbox(ptr).handle.beginBatch());
        }

        pub fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, options: backend_types.BatchOptions) anyerror!Batch {
            if (@hasDecl(Handle, "beginBatchWithOptions")) {
                return try batchFrom(alloc, try unbox(ptr).handle.beginBatchWithOptions(options));
            }
            return try batchFrom(alloc, try unbox(ptr).handle.beginBatch());
        }

        pub fn sync(ptr: *anyopaque, force: bool) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.sync) |sync_fn| {
                    return try sync_fn(state.handle.ptr, force);
                }
                return;
            }
            if (@hasDecl(Handle, "sync")) {
                try unbox(ptr).handle.sync(force);
            }
        }

        pub fn syncReplayState(ptr: *anyopaque) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.sync_replay_state) |sync_replay_state_fn| {
                    return try sync_replay_state_fn(state.handle.ptr);
                }
                if (state.handle.vtable.sync) |sync_fn| {
                    return try sync_fn(state.handle.ptr, false);
                }
                return;
            }
            if (@hasDecl(Handle, "syncReplayState")) {
                try unbox(ptr).handle.syncReplayState();
                return;
            }
            if (@hasDecl(Handle, "sync")) {
                try unbox(ptr).handle.sync(false);
            }
        }

        pub fn beginBulkIngestSession(ptr: *anyopaque) anyerror!void {
            if (@hasDecl(Handle, "beginBulkIngestSession")) {
                try unbox(ptr).handle.beginBulkIngestSession();
            }
        }

        pub fn finishBulkIngestSession(ptr: *anyopaque, options: backend_types.BulkIngestFinishOptions) anyerror!void {
            if (@hasDecl(Handle, "finishBulkIngestSessionWithOptions")) {
                try unbox(ptr).handle.finishBulkIngestSessionWithOptions(options);
            } else if (@hasDecl(Handle, "finishBulkIngestSession")) {
                try unbox(ptr).handle.finishBulkIngestSession();
            }
        }

        fn flushBufferedWrites(ptr: *anyopaque, options: backend_types.BulkIngestFinishOptions) anyerror!void {
            if (Handle == Store) {
                const state = unbox(ptr);
                if (state.handle.vtable.flush_buffered_writes) |flush_buffered_writes| {
                    return try flush_buffered_writes(state.handle.ptr, options);
                }
                return;
            }
            if (@hasDecl(Handle, "flushBufferedWritesWithOptions")) {
                try unbox(ptr).handle.flushBufferedWritesWithOptions(options);
            }
        }

        pub fn abortBulkIngestSession(ptr: *anyopaque) void {
            if (@hasDecl(Handle, "abortBulkIngestSession")) {
                unbox(ptr).handle.abortBulkIngestSession();
            }
        }

        pub fn lastReplaySequence(ptr: *anyopaque, fallback_last: u64) u64 {
            if (@hasDecl(Handle, "lastReplaySequence")) {
                return unbox(ptr).handle.lastReplaySequence(fallback_last);
            }
            return fallback_last;
        }

        pub fn nextReplaySequence(ptr: *anyopaque, fallback_next: u64) u64 {
            if (@hasDecl(Handle, "nextReplaySequence")) {
                return unbox(ptr).handle.nextReplaySequence(fallback_next);
            }
            return fallback_next;
        }

        pub fn appendReplayOpaque(alloc: Allocator, ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            if (@hasDecl(Handle, "appendReplayOpaque")) {
                return try unbox(ptr).handle.appendReplayOpaque(alloc, sequence, payload);
            }
            return error.Unsupported;
        }

        pub fn iterateReplayFrom(alloc: Allocator, ptr: *anyopaque, from_sequence: u64) anyerror![]ReplayEntry {
            if (@hasDecl(Handle, "iterateReplayFrom")) {
                return try unbox(ptr).handle.iterateReplayFrom(alloc, from_sequence);
            }
            return error.Unsupported;
        }

        pub fn forEachReplayFrom(
            ptr: *anyopaque,
            from_sequence: u64,
            replay_visitor: Store.ReplayVisitor,
        ) anyerror!void {
            var visitor = replay_visitor;
            if (Handle == Store) {
                return try unbox(ptr).handle.forEachReplayFrom(from_sequence, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            } else if (@hasDecl(Handle, "forEachReplayFrom")) {
                return try unbox(ptr).handle.forEachReplayFrom(from_sequence, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            }
            if (@hasDecl(Handle, "iterateReplayFrom")) {
                const state = unbox(ptr);
                const entries = try state.handle.iterateReplayFrom(state.allocator, from_sequence);
                defer {
                    for (entries) |*entry| entry.deinit(state.allocator);
                    state.allocator.free(entries);
                }
                for (entries) |entry| try Store.ReplayVisitor.consume(&visitor, entry.sequence, entry.payload);
                return;
            }
            return error.Unsupported;
        }

        pub fn forEachReplayFromMatchingHintMask(
            ptr: *anyopaque,
            from_sequence: u64,
            required_hint_mask: u8,
            replay_visitor: Store.ReplayVisitor,
        ) anyerror!void {
            var visitor = replay_visitor;
            if (Handle == Store) {
                return try unbox(ptr).handle.forEachReplayFromMatchingHintMask(from_sequence, required_hint_mask, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            } else if (@hasDecl(Handle, "forEachReplayFromMatchingHintMask")) {
                return try unbox(ptr).handle.forEachReplayFromMatchingHintMask(from_sequence, required_hint_mask, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            } else if (@hasDecl(Handle, "forEachReplayLaneFrom")) {
                const lane_ordinal = if (required_hint_mask == 0)
                    internal_keys.replay_all_kind
                else if (replayHintOrdinalFromSingleMask(required_hint_mask)) |ordinal|
                    ordinal
                else
                    return error.Unsupported;
                _ = try unbox(ptr).handle.forEachReplayLaneFrom(lane_ordinal, from_sequence, 0, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
                return;
            }
            return error.Unsupported;
        }

        pub fn forEachReplayLaneFrom(
            ptr: *anyopaque,
            lane_ordinal: u8,
            from_sequence: u64,
            max_entries: usize,
            replay_visitor: Store.ReplayVisitor,
        ) anyerror!backend_types.ReplayLaneIterationStats {
            var visitor = replay_visitor;
            if (Handle == Store) {
                return try unbox(ptr).handle.forEachReplayLaneFrom(lane_ordinal, from_sequence, max_entries, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            } else if (@hasDecl(Handle, "forEachReplayLaneFrom")) {
                return try unbox(ptr).handle.forEachReplayLaneFrom(lane_ordinal, from_sequence, max_entries, @as(*anyopaque, &visitor), Store.ReplayVisitor.consume);
            }
            return error.Unsupported;
        }

        pub fn truncateReplayUpTo(alloc: Allocator, ptr: *anyopaque, up_to_sequence: u64) anyerror!void {
            if (@hasDecl(Handle, "truncateReplayUpTo")) {
                return try unbox(ptr).handle.truncateReplayUpTo(alloc, up_to_sequence);
            }
            return error.Unsupported;
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .publication_gate = if (@hasDecl(Handle, "publicationGate")) box_ptr.handle.publicationGate() else null,
        .vtable = &.{
            .deinit = vt.deinit,
            .capabilities = vt.capabilities,
            .begin_read = vt.beginRead,
            .begin_owned_value_read = if (Handle == Store or @hasDecl(Handle, "beginOwnedValueRead")) vt.beginOwnedValueRead else null,
            .begin_owned_key_cursor = if (Handle == Store or @hasDecl(Handle, "beginOwnedKeyCursor")) vt.beginOwnedKeyCursor else null,
            .begin_read_with_admission = if (Handle == Store or @hasDecl(Handle, "beginReadWithAdmission")) vt.beginReadWithAdmission else null,
            .begin_read_with_block_cache_admission = if (Handle == Store or @hasDecl(Handle, "beginReadWithBlockCacheAdmission")) vt.beginReadWithBlockCacheAdmission else null,
            .begin_probe = if (Handle == Store or @hasDecl(Handle, "beginProbe")) vt.beginProbe else null,
            .begin_probe_with_admission = if (Handle == Store or @hasDecl(Handle, "beginProbeWithAdmission")) vt.beginProbeWithAdmission else null,
            .begin_current_scan_with_admission = if (Handle == Store or @hasDecl(Handle, "beginCurrentScanWithAdmission")) vt.beginCurrentScanWithAdmission else null,
            .begin_probe_with_block_cache_admission = if (Handle == Store or @hasDecl(Handle, "beginProbeWithBlockCacheAdmission")) vt.beginProbeWithBlockCacheAdmission else null,
            .begin_current_scan = if (Handle == Store or @hasDecl(Handle, "beginCurrentScan")) vt.beginCurrentScan else null,
            .begin_replay_lane_scan = if (Handle == Store or @hasDecl(Handle, "beginReplayLaneScan")) vt.beginReplayLaneScan else null,
            .begin_write = vt.beginWrite,
            .begin_batch = vt.beginBatch,
            .begin_batch_with_options = vt.beginBatchWithOptions,
            .sync = vt.sync,
            .sync_replay_state = vt.syncReplayState,
            .begin_bulk_ingest_session = vt.beginBulkIngestSession,
            .finish_bulk_ingest_session = vt.finishBulkIngestSession,
            .flush_buffered_writes = vt.flushBufferedWrites,
            .abort_bulk_ingest_session = vt.abortBulkIngestSession,
            .last_replay_sequence = vt.lastReplaySequence,
            .next_replay_sequence = vt.nextReplaySequence,
            .append_replay_opaque = vt.appendReplayOpaque,
            .iterate_replay_from = vt.iterateReplayFrom,
            .for_each_replay_from = vt.forEachReplayFrom,
            .for_each_replay_lane_from = vt.forEachReplayLaneFrom,
            .for_each_replay_from_matching_hint_mask = vt.forEachReplayFromMatchingHintMask,
            .truncate_replay_up_to = vt.truncateReplayUpTo,
        },
    };
}

pub fn namespaceStoreFrom(
    allocator: Allocator,
    handle: anytype,
    comptime LocalNamespace: type,
    comptime mapNamespace: fn (backend_types.Namespace) anyerror!LocalNamespace,
) !NamespaceStore {
    const Handle = @TypeOf(handle);
    const HandleDecl = switch (@typeInfo(Handle)) {
        .pointer => |pointer| pointer.child,
        else => Handle,
    };
    const wrapper_box_allocator = wrapperBoxAllocator(allocator);
    const box_ptr = try allocBox(wrapper_box_allocator, handle);

    const vt = struct {
        fn unbox(ptr: *anyopaque) *Box(Handle) {
            return @ptrCast(@alignCast(ptr));
        }

        pub fn deinit(_: Allocator, ptr: *anyopaque) void {
            const state = unbox(ptr);
            const box_allocator = state.allocator;
            box_allocator.destroy(state);
        }

        fn capabilities(ptr: *anyopaque) backend_types.Capabilities {
            return unbox(ptr).handle.capabilities();
        }

        fn beginRead(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginRead(), LocalNamespace, mapNamespace);
        }

        pub fn beginReadWithAdmission(alloc: Allocator, ptr: *anyopaque, control: read_admission.Port) anyerror!NamespaceReadTxn {
            var txn = try unbox(ptr).handle.beginReadWithAdmission(control);
            errdefer txn.abort();
            try control.checkpoint();
            return try namespaceReadTxnFrom(alloc, txn, LocalNamespace, mapNamespace);
        }

        pub fn beginOwnedValueRead(alloc: Allocator, ptr: *anyopaque, namespace: backend_types.Namespace, key: []const u8, control: read_admission.Port) !OwnedValueRead {
            var owned = try unbox(ptr).handle.beginOwnedValueRead(alloc, try mapNamespace(namespace), key, control);
            errdefer owned.deinit();
            try control.checkpoint();
            return ownedValueReadFrom(alloc, owned);
        }

        pub fn beginProbe(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceReadTxn {
            if (@hasDecl(HandleDecl, "beginProbe")) {
                return try namespaceReadTxnFrom(alloc, try unbox(ptr).handle.beginProbe(), LocalNamespace, mapNamespace);
            }
            return try beginRead(alloc, ptr);
        }

        fn beginWrite(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceWriteTxn {
            return try namespaceWriteTxnFrom(alloc, try unbox(ptr).handle.beginWrite(), LocalNamespace, mapNamespace);
        }

        fn beginBatch(alloc: Allocator, ptr: *anyopaque) anyerror!NamespaceBatch {
            return try namespaceBatchFrom(alloc, try unbox(ptr).handle.beginBatch(), LocalNamespace, mapNamespace);
        }

        pub fn beginBatchWithOptions(alloc: Allocator, ptr: *anyopaque, options: backend_types.BatchOptions) anyerror!NamespaceBatch {
            if (@hasDecl(HandleDecl, "beginBatchWithOptions")) {
                return try namespaceBatchFrom(alloc, try unbox(ptr).handle.beginBatchWithOptions(options), LocalNamespace, mapNamespace);
            }
            return try beginBatch(alloc, ptr);
        }
    };

    return .{
        .allocator = wrapper_box_allocator,
        .ptr = box_ptr,
        .vtable = &.{
            .deinit = vt.deinit,
            .capabilities = vt.capabilities,
            .begin_read = vt.beginRead,
            .begin_read_with_admission = if (@hasDecl(HandleDecl, "beginReadWithAdmission")) vt.beginReadWithAdmission else null,
            .begin_owned_value_read = if (@hasDecl(HandleDecl, "beginOwnedValueRead")) vt.beginOwnedValueRead else null,
            .begin_probe = if (Handle == NamespaceStore or @hasDecl(HandleDecl, "beginProbe")) vt.beginProbe else null,
            .begin_write = vt.beginWrite,
            .begin_batch = vt.beginBatch,
            .begin_batch_with_options = vt.beginBatchWithOptions,
        },
    };
}

test "runtime store erases concrete single-namespace store handles" {
    const MockCursor = struct {
        closed: bool = false,

        pub fn close(self: *@This()) void {
            self.closed = true;
        }

        pub fn first(_: *@This()) !?Entry {
            return .{ .key = "a", .value = "1" };
        }

        pub fn last(_: *@This()) !?Entry {
            return .{ .key = "z", .value = "9" };
        }

        pub fn next(_: *@This()) !?Entry {
            return null;
        }

        pub fn prev(_: *@This()) !?Entry {
            return null;
        }

        pub fn seekAtOrAfter(_: *@This(), key: []const u8) !?Entry {
            return .{ .key = key, .value = "x" };
        }

        pub fn seekAtOrBefore(_: *@This(), key: []const u8) !?Entry {
            return .{ .key = key, .value = "y" };
        }
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return "r";
        }
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    const MockWrite = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    const MockBatch = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: []const u8) !void {
            self.value = null;
        }
    };

    const MockStore = struct {
        fail_open: *bool,
        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{ .cursors = true };
        }

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(self: *@This()) !MockWrite {
            if (self.fail_open.*) return error.OpenFailed;
            return .{};
        }

        pub fn beginBatch(self: *@This()) !MockBatch {
            if (self.fail_open.*) return error.OpenFailed;
            return .{};
        }
    };

    var fail_open = false;
    const mock = MockStore{ .fail_open = &fail_open };
    var store = try storeFrom(std.testing.allocator, mock);
    defer store.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    store.write_gate = &gate;
    try std.testing.expect(store.capabilities().cursors);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("r", try read.get("k"));
    var cur = try read.openCursor();
    defer cur.close();
    try std.testing.expectEqualStrings("a", (try cur.first()).?.key);

    var probe = try store.beginProbe();
    defer probe.abort();
    try std.testing.expectEqualStrings("r", try probe.get("k"));

    var current_scan = try store.beginCurrentScan();
    defer current_scan.abort();
    var current_scan_cur = try current_scan.openCursor();
    defer current_scan_cur.close();
    try std.testing.expectEqualStrings("a", (try current_scan_cur.first()).?.key);

    var write = try store.beginWrite();
    try std.testing.expect(!gate.tryLock());
    try write.put("k", "w");
    try std.testing.expectEqualStrings("w", try write.get("k"));
    try write.commit();
    try std.testing.expect(gate.tryLock());
    gate.unlock();

    var batch = try store.beginBatch();
    try std.testing.expect(!gate.tryLock());
    try batch.put("k", "b");
    try std.testing.expectEqualStrings("b", try batch.get("k"));
    try batch.commit();
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    batch = try store.beginBatchWithOptions(.{});
    try std.testing.expect(!gate.tryLock());
    batch.abort();
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    fail_open = true;
    try std.testing.expectError(error.OpenFailed, store.beginWrite());
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expectError(error.OpenFailed, store.beginBatch());
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expectError(error.OpenFailed, store.beginBatchWithOptions(.{}));
    try std.testing.expect(gate.tryLock());
    gate.unlock();
}

test "failed commit keeps erased write handle abortable" {
    const MockCursor = struct {
        pub fn close(_: *@This()) void {}
        pub fn first(_: *@This()) !?Entry {
            return null;
        }
        pub fn last(_: *@This()) !?Entry {
            return null;
        }
        pub fn next(_: *@This()) !?Entry {
            return null;
        }
        pub fn prev(_: *@This()) !?Entry {
            return null;
        }
        pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
            return null;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?Entry {
            return null;
        }
    };

    const Shared = struct {
        commits: usize = 0,
        aborted: bool = false,
    };

    const MockWrite = struct {
        shared: *Shared,

        pub fn abort(self: *@This()) void {
            self.shared.aborted = true;
        }
        pub fn commit(self: *@This()) !void {
            self.shared.commits += 1;
            return error.CommitFailed;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    var shared = Shared{};
    var txn = try writeTxnFrom(std.testing.allocator, MockWrite{ .shared = &shared });
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    txn.write_gate = &gate;
    try std.testing.expectError(error.CommitFailed, txn.commit());
    try std.testing.expect(!gate.tryLock());
    txn.abort();
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expectEqual(@as(usize, 1), shared.commits);
    try std.testing.expect(shared.aborted);
    shared = .{};
    var batch = try batchFrom(std.testing.allocator, MockWrite{ .shared = &shared });
    try std.testing.expect(gate.tryLock());
    batch.write_gate = &gate;
    try std.testing.expectError(error.CommitFailed, batch.commit());
    try std.testing.expect(!gate.tryLock());
    batch.abort();
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(shared.aborted);
}

test "runtime namespace store maps logical namespaces into concrete partitions" {
    const LocalNamespace = enum { meta, docs };

    const Shared = struct {
        read_calls: usize = 0,
        probe_calls: usize = 0,
    };

    const MockRead = struct {
        probe: bool = false,

        pub fn abort(_: *@This()) void {}
        pub fn get(self: *@This(), ns: LocalNamespace, _: []const u8) ![]const u8 {
            if (self.probe) return "p";
            return switch (ns) {
                .meta => "m",
                .docs => "d",
            };
        }
    };

    const MockWrite = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: LocalNamespace, _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: LocalNamespace, _: []const u8) !void {
            self.value = null;
        }
    };

    const MockBatch = struct {
        value: ?[]const u8 = null,

        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(self: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return self.value orelse error.NotFound;
        }
        pub fn put(self: *@This(), _: LocalNamespace, _: []const u8, value: []const u8) !void {
            self.value = value;
        }
        pub fn delete(self: *@This(), _: LocalNamespace, _: []const u8) !void {
            self.value = null;
        }
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{ .native_namespaces = true };
        }

        pub fn beginRead(self: *@This()) !MockRead {
            self.shared.read_calls += 1;
            return .{};
        }

        pub fn beginProbe(self: *@This()) !MockRead {
            self.shared.probe_calls += 1;
            return .{ .probe = true };
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }
    };

    const mapNamespace = struct {
        fn map(ns: backend_types.Namespace) !LocalNamespace {
            if (ns.name == null) return .meta;
            if (std.mem.eql(u8, ns.name.?, "docs")) return .docs;
            return error.InvalidNamespace;
        }
    }.map;

    var shared = Shared{};
    const mock = MockStore{ .shared = &shared };
    var store = try namespaceStoreFrom(std.testing.allocator, mock, LocalNamespace, mapNamespace);
    defer store.deinit();
    try std.testing.expect(store.capabilities().native_namespaces);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("m", try read.get(.{}, "k"));
    try std.testing.expectEqualStrings("d", try read.get(.{ .name = "docs" }, "k"));

    var probe = try store.beginProbe();
    defer probe.abort();
    try std.testing.expectEqualStrings("p", try probe.get(.{}, "k"));
    try std.testing.expectEqual(@as(usize, 1), shared.read_calls);
    try std.testing.expectEqual(@as(usize, 1), shared.probe_calls);

    var write = try store.beginWrite();
    try write.put(.{ .name = "docs" }, "k", "wv");
    try std.testing.expectEqualStrings("wv", try write.get(.{ .name = "docs" }, "k"));
    try write.commit();

    var batch = try store.beginBatch();
    try batch.put(.{}, "k", "bv");
    try std.testing.expectEqualStrings("bv", try batch.get(.{}, "k"));
    try batch.commit();
}

test "runtime namespace store forwards batch options" {
    const LocalNamespace = enum { meta, docs };

    const Shared = struct {
        saw_batch_options: bool = false,
        last_mode: backend_types.BatchMode = .default,
    };

    const MockRead = struct {
        pub fn abort(_: *@This()) void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
    };

    const MockWrite = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: LocalNamespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: LocalNamespace, _: []const u8) !void {}
    };

    const MockBatch = struct {
        pub fn abort(_: *@This()) void {}
        pub fn commit(_: *@This()) !void {}
        pub fn get(_: *@This(), _: LocalNamespace, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: LocalNamespace, _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: LocalNamespace, _: []const u8) !void {}
    };

    const MockStore = struct {
        shared: *Shared,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{};
        }

        pub fn beginRead(_: *@This()) !MockRead {
            return .{};
        }

        pub fn beginWrite(_: *@This()) !MockWrite {
            return .{};
        }

        pub fn beginBatch(_: *@This()) !MockBatch {
            return .{};
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !MockBatch {
            self.shared.saw_batch_options = true;
            self.shared.last_mode = options.mode;
            return .{};
        }
    };

    const mapNamespace = struct {
        fn map(ns: backend_types.Namespace) !LocalNamespace {
            if (ns.name == null) return .meta;
            if (std.mem.eql(u8, ns.name.?, "docs")) return .docs;
            return error.InvalidNamespace;
        }
    }.map;

    var shared = Shared{};
    const mock = MockStore{ .shared = &shared };
    var store = try namespaceStoreFrom(std.testing.allocator, mock, LocalNamespace, mapNamespace);
    defer store.deinit();

    var batch = try store.beginBatchWithOptions(.{ .mode = .bulk_ingest });
    try batch.commit();
    try std.testing.expect(shared.saw_batch_options);
    try std.testing.expectEqual(backend_types.BatchMode.bulk_ingest, shared.last_mode);
}

test "graph maintenance erased read forks flatten snapshot ownership and retain cursors" {
    const Shared = struct { live: usize = 1, owner_aborts: usize = 0 };
    const MockCursor = struct {
        shared: *Shared,
        pub fn close(_: *@This()) void {}
        pub fn first(self: *@This()) !?Entry {
            if (self.shared.owner_aborts != 0) return error.TransactionClosed;
            return .{ .key = "key", .value = "snapshot" };
        }
        pub fn last(self: *@This()) !?Entry {
            return self.first();
        }
        pub fn next(_: *@This()) !?Entry {
            return null;
        }
        pub fn prev(_: *@This()) !?Entry {
            return null;
        }
        pub fn seekAtOrAfter(self: *@This(), _: []const u8) !?Entry {
            return self.first();
        }
        pub fn seekAtOrBefore(self: *@This(), _: []const u8) !?Entry {
            return self.first();
        }
    };
    const MockRead = struct {
        shared: *Shared,
        owner: bool = true,
        pub fn forkBorrowedRead(self: *@This()) !@This() {
            if (self.shared.owner_aborts != 0) return error.TransactionClosed;
            self.shared.live += 1;
            return .{ .shared = self.shared, .owner = false };
        }
        pub fn abort(self: *@This()) void {
            self.shared.live -= 1;
            if (self.owner) self.shared.owner_aborts += 1;
        }
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            if (self.shared.owner_aborts != 0) return error.TransactionClosed;
            return "snapshot";
        }
        pub fn openCursor(self: *@This()) !MockCursor {
            return .{ .shared = self.shared };
        }
    };
    const Run = struct {
        fn run(a: Allocator, iterations: usize) !void {
            var shared: Shared = .{};
            var read = try readTxnFrom(a, MockRead{ .shared = &shared });
            var read_open = true;
            defer if (read_open) read.abort();
            var retained: ?Cursor = null;
            defer if (retained) |*cursor| cursor.close();
            for (0..iterations) |i| {
                const next = try read.forkRead();
                read.abort();
                read = next;
                if (i == 1) retained = try read.openCursor();
                try std.testing.expect(shared.live <= 3);
                try std.testing.expectEqualStrings("snapshot", try read.get("key"));
            }
            read.abort();
            read_open = false;
            try std.testing.expectEqualStrings("snapshot", (try retained.?.first()).?.value);
            retained.?.close();
            retained = null;
            try std.testing.expectEqual(@as(usize, 0), shared.live);
            try std.testing.expectEqual(@as(usize, 1), shared.owner_aborts);
        }
    };
    try Run.run(std.testing.allocator, 100_000);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{@as(usize, 8)});
}

test "erased cursor retains read transaction until cursor close" {
    const Shared = struct {
        aborted: bool = false,
        cursor_closed: bool = false,
    };
    const MockCursor = struct {
        shared: *Shared,

        pub fn close(self: *@This()) void {
            self.shared.cursor_closed = true;
        }
        pub fn first(self: *@This()) !?Entry {
            if (self.shared.aborted) return error.TransactionClosed;
            return .{ .key = "key", .value = "value" };
        }
        pub fn last(_: *@This()) !?Entry {
            return null;
        }
        pub fn next(_: *@This()) !?Entry {
            return null;
        }
        pub fn prev(_: *@This()) !?Entry {
            return null;
        }
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
            return if (std.mem.eql(u8, key, "key")) self.first() else null;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?Entry {
            return null;
        }
    };
    const MockRead = struct {
        shared: *Shared,

        pub fn abort(self: *@This()) void {
            self.shared.aborted = true;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn openCursor(self: *@This()) !MockCursor {
            return .{ .shared = self.shared };
        }
    };

    var shared = Shared{};
    var txn = try readTxnFrom(std.testing.allocator, MockRead{ .shared = &shared });
    var cursor = try txn.openCursor();
    txn.abort();

    try std.testing.expect(!shared.aborted);
    try std.testing.expectEqualStrings("key", (try cursor.first()).?.key);
    cursor.close();
    try std.testing.expect(shared.cursor_closed);
    try std.testing.expect(shared.aborted);

    // Backends without a native scope use bounded cursor-owned copies while
    // retaining the same parent snapshot, including after owner abort.
    shared = .{};
    var scoped_txn = try readTxnFrom(std.testing.allocator, MockRead{ .shared = &shared });
    var scope = try scoped_txn.openReadScope(std.testing.allocator);
    scoped_txn.abort();
    try std.testing.expect(!shared.aborted);
    const value = try scope.get("key");
    try std.testing.expectError(error.NotFound, scope.get("missing"));
    try std.testing.expectEqualStrings("value", value);
    scope.close();
    try std.testing.expect(shared.cursor_closed);
    try std.testing.expect(shared.aborted);
}

test "erased write transaction rejects commit while cursor retains snapshot" {
    const Shared = struct {
        committed: bool = false,
    };
    const MockCursor = struct {
        pub fn close(_: *@This()) void {}
        pub fn first(_: *@This()) !?Entry {
            return null;
        }
        pub fn last(_: *@This()) !?Entry {
            return null;
        }
        pub fn next(_: *@This()) !?Entry {
            return null;
        }
        pub fn prev(_: *@This()) !?Entry {
            return null;
        }
        pub fn seekAtOrAfter(_: *@This(), _: []const u8) !?Entry {
            return null;
        }
        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?Entry {
            return null;
        }
    };
    const MockWrite = struct {
        shared: *Shared,

        pub fn abort(_: *@This()) void {}
        pub fn commit(self: *@This()) !void {
            self.shared.committed = true;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {}
        pub fn delete(_: *@This(), _: []const u8) !void {}
        pub fn openCursor(_: *@This()) !MockCursor {
            return .{};
        }
    };

    var shared = Shared{};
    var txn = try writeTxnFrom(std.testing.allocator, MockWrite{ .shared = &shared });
    var cursor = try txn.openCursor();
    try std.testing.expectError(error.TransactionCursorActive, txn.commit());
    try std.testing.expect(!shared.committed);
    cursor.close();
    try txn.commit();
    try std.testing.expect(shared.committed);
}

fn TestForeignDispatch(comptime Interface: type) type {
    return struct {
        var calls: usize = 0;
        fn dispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            calls += 1;
            const VTable = @typeInfo(@FieldType(Interface, "vtable")).pointer.child;
            return runtime_callback_abi.Boundary(VTable).local_dispatch(contract, callback, args, output);
        }
    };
}

test "erased read handles preserve missing-key errors through the owning dispatcher" {
    const Mock = struct {
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) anyerror![]const u8 {
            return error.NotFound;
        }
        fn cursor(_: Allocator, _: *anyopaque) anyerror!Cursor {
            return error.NotFound;
        }
        fn fork(_: Allocator, _: *anyopaque) anyerror!ReadTxn {
            return error.NotFound;
        }
        fn scope(_: Allocator, _: *anyopaque) anyerror!ReadScope {
            return error.NotFound;
        }
    };
    const Foreign = TestForeignDispatch(ReadTxn);
    Foreign.calls = 0;
    var state: u8 = 0;
    var read: ReadTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Mock.abort, .get = Mock.get, .open_cursor = Mock.cursor, .fork_read = Mock.fork, .open_read_scope = Mock.scope }, .boundary_dispatch = Foreign.dispatch };
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("missing"));
    var values: [1]?[]const u8 = undefined;
    try read.getManySorted(&.{"missing"}, &values);
    try std.testing.expect(values[0] == null);
    try std.testing.expectError(error.NotFound, read.openCursor());
    try std.testing.expectError(error.NotFound, read.forkRead());
    try std.testing.expectError(error.NotFound, read.openReadScope(std.testing.allocator));
    try std.testing.expectEqual(@as(usize, 5), Foreign.calls);

    const ProbeForeign = TestForeignDispatch(ProbeTxn);
    ProbeForeign.calls = 0;
    var probe: ProbeTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Mock.abort, .get = Mock.get, .get_leased = Mock.get }, .boundary_dispatch = ProbeForeign.dispatch };
    defer probe.abort();
    try std.testing.expectError(error.NotFound, probe.get("missing"));
    try std.testing.expectError(error.NotFound, probe.getLeased("missing"));
    try std.testing.expect(!try probe.contains("missing"));
    try std.testing.expectEqual(@as(usize, 3), ProbeForeign.calls);
}

test "erased read scopes dispatch bounded reads and score operations by their exact method" {
    const Mock = struct {
        fn close(_: Allocator, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) anyerror![]const u8 {
            return error.NotFound;
        }
        fn reset(_: *anyopaque) anyerror!void {
            return error.NotFound;
        }
        fn column(_: *anyopaque, _: []const u8, _: []const []const u8, _: []f64) anyerror!usize {
            return error.NotFound;
        }
        fn into(_: *anyopaque, _: []const []const u8, _: []const []u8, _: []?[]const u8) anyerror!void {
            return error.NotFound;
        }
    };
    const Foreign = TestForeignDispatch(ReadScope);
    Foreign.calls = 0;
    var state: u8 = 0;
    var scope: ReadScope = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .close = Mock.close, .get = Mock.get, .reset = Mock.reset, .begin_score_batch = Mock.reset, .reset_score_batch = Mock.reset, .read_sorted_score_column = Mock.column, .get_many_sorted_into = Mock.into }, .boundary_dispatch = Foreign.dispatch };
    defer scope.close();
    try std.testing.expectError(error.NotFound, scope.get("missing"));
    var bytes: [1]u8 = undefined;
    try std.testing.expectError(error.NotFound, scope.getInto("missing", &bytes));
    try std.testing.expectError(error.NotFound, scope.reset());
    try std.testing.expectError(error.NotFound, scope.beginScoreBatch());
    try std.testing.expectError(error.NotFound, scope.resetScoreBatch());
    var scores: [1]f64 = undefined;
    try std.testing.expectError(error.NotFound, scope.readSortedScoreColumn("score/", &.{"missing"}, &scores));
    var values: [1]?[]const u8 = undefined;
    try std.testing.expectError(error.NotFound, scope.getManySortedInto(&.{"missing"}, &.{&bytes}, &values));
    try std.testing.expectEqual(@as(usize, 7), Foreign.calls);
}

test "erased probes dispatch both batched read signatures and admission fallback" {
    const Mock = struct {
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) anyerror![]const u8 {
            return error.TestUnexpectedResult;
        }
        fn many(_: *anyopaque, keys: []const []const u8, values: []?[]const u8) anyerror!void {
            try std.testing.expectEqual(@as(usize, 2), keys.len);
            try std.testing.expectEqualStrings("a", keys[0]);
            try std.testing.expectEqualStrings("missing", keys[1]);
            try std.testing.expect(values[0] == null and values[1] == null);
            values[0] = "value";
        }
        fn admitted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8, admission: backend_types.Namespace.BlockCacheAdmission) anyerror!void {
            try std.testing.expectEqual(backend_types.Namespace.BlockCacheAdmission.transient, admission);
            try many(ptr, keys, values);
        }
    };
    const Foreign = TestForeignDispatch(ProbeTxn);
    Foreign.calls = 0;
    var state: u8 = 0;
    var vtable: ProbeTxn.VTable = .{ .abort = Mock.abort, .get = Mock.get, .get_many_sorted = Mock.many, .get_many_sorted_with_block_cache_admission = Mock.admitted };
    var probe: ProbeTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &vtable, .boundary_dispatch = Foreign.dispatch };
    defer probe.abort();
    var values: [2]?[]const u8 = .{ "stale", "stale" };
    try probe.getManySorted(&.{ "a", "missing" }, &values);
    try std.testing.expectEqualStrings("value", values[0].?);
    try std.testing.expect(values[1] == null);
    try probe.getManySortedWithBlockCacheAdmission(&.{ "a", "missing" }, &values, .transient);
    try std.testing.expectEqualStrings("value", values[0].?);
    try std.testing.expect(values[1] == null);
    vtable.get_many_sorted_with_block_cache_admission = null;
    try probe.getManySortedWithBlockCacheAdmission(&.{ "a", "missing" }, &values, .transient);
    try std.testing.expectEqualStrings("value", values[0].?);
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(usize, 3), Foreign.calls);
}

test "retained chunks fall back to the owning read when foreign snapshot forks are unsupported" {
    const Mock = struct {
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn get(_: *anyopaque, key: []const u8) anyerror![]const u8 {
            try std.testing.expectEqualStrings(&@import("retained_effects.zig").chunkKey(7, 2), key);
            return "chunk";
        }
        fn cursor(_: Allocator, _: *anyopaque) anyerror!Cursor {
            return error.TestUnexpectedResult;
        }
        fn fork(_: Allocator, _: *anyopaque) anyerror!ReadTxn {
            return error.ReadSnapshotForkUnsupported;
        }
    };
    const Foreign = TestForeignDispatch(ReadTxn);
    Foreign.calls = 0;
    var state: u8 = 0;
    var read: ReadTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Mock.abort, .get = Mock.get, .open_cursor = Mock.cursor, .fork_read = Mock.fork }, .boundary_dispatch = Foreign.dispatch };
    defer read.abort();
    const source = @import("retained_effects.zig").chunkSource(&read);
    var bytes: [5]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try source.read_chunk(source.context, 7, 2, &bytes));
    try std.testing.expectEqualStrings("chunk", &bytes);
    try std.testing.expectEqual(@as(usize, 2), Foreign.calls);
}

test "score reads tolerate unsupported resets through the owning scope dispatcher" {
    const Mock = struct {
        resets: usize = 0,
        closes: usize = 0,
        raw: [8]u8 = .{ 0, 0, 0, 0, 0, 0, 8, 64 }, // 3.0, little endian
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn get(ptr: *anyopaque, _: []const u8) anyerror![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return &self.raw;
        }
        fn cursor(_: Allocator, _: *anyopaque) anyerror!Cursor {
            return error.TestUnexpectedResult;
        }
        fn scope(a: Allocator, ptr: *anyopaque) anyerror!ReadScope {
            return .{ .allocator = a, .ptr = ptr, .vtable = &.{ .close = close, .get = get, .reset = reset }, .boundary_dispatch = TestForeignDispatch(ReadScope).dispatch };
        }
        fn close(_: Allocator, ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
        fn reset(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resets += 1;
            return error.ReadScopeResetUnsupported;
        }
    };
    const Foreign = TestForeignDispatch(ReadScope);
    Foreign.calls = 0;
    var state = Mock{};
    var read: ReadTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Mock.abort, .get = Mock.get, .open_cursor = Mock.cursor, .open_read_scope = Mock.scope }, .boundary_dispatch = TestForeignDispatch(ReadTxn).dispatch };
    defer read.abort();
    var scores: [3]?f64 = undefined;
    _ = try @import("../graph/score_read.zig").populate(std.testing.allocator, &read, &.{"p/"}, &.{ "b", "a", "b" }, &.{&scores});
    try std.testing.expectEqualSlices(?f64, &.{ 3, 3, 3 }, &scores);
    try std.testing.expectEqual(@as(usize, 1), state.resets);
    try std.testing.expectEqual(@as(usize, 1), state.closes);
    try std.testing.expect(Foreign.calls > 1);
}

test "foreign replay visitors preserve consumer failures and resume rejected entries" {
    const State = struct { fail_read: bool = false };
    const Mock = struct {
        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn capabilities(_: *anyopaque) backend_types.Capabilities {
            return .{};
        }
        fn read(_: Allocator, _: *anyopaque) anyerror!ReadTxn {
            return error.Unsupported;
        }
        fn write(_: Allocator, _: *anyopaque) anyerror!WriteTxn {
            return error.Unsupported;
        }
        fn batch(_: Allocator, _: *anyopaque) anyerror!Batch {
            return error.Unsupported;
        }
        fn lane(ptr: *anyopaque, _: u8, from: u64, limit: usize, replay_visitor: Store.ReplayVisitor) anyerror!backend_types.ReplayLaneIterationStats {
            const state: *State = @ptrCast(@alignCast(ptr));
            if (state.fail_read) return error.OutOfMemory;
            var visitor = replay_visitor;
            var stats: backend_types.ReplayLaneIterationStats = .{ .scan_batches = 1 };
            var sequence = from;
            while (sequence <= 3) : (sequence += 1) {
                try Store.ReplayVisitor.consume(&visitor, sequence, "borrowed payload");
                stats.scanned_entries += 1;
                stats.matched_entries += 1;
                stats.last_sequence = sequence;
                if (limit != 0 and stats.matched_entries >= limit) break;
            }
            return stats;
        }
        fn all(ptr: *anyopaque, from: u64, visitor: Store.ReplayVisitor) anyerror!void {
            _ = try lane(ptr, internal_keys.replay_all_kind, from, 0, visitor);
        }
        fn matching(ptr: *anyopaque, from: u64, _: u8, visitor: Store.ReplayVisitor) anyerror!void {
            try all(ptr, from, visitor);
        }
    };
    const Consumer = struct {
        failure: ?anyerror,
        accepted: usize = 0,
        calls: usize = 0,
        next: u64 = 1,
        fn consume(self: *@This(), sequence: u64, payload: []const u8) anyerror!void {
            self.calls += 1;
            try std.testing.expectEqualStrings("borrowed payload", payload);
            if (sequence == 2) if (self.failure) |err| return err;
            self.accepted += 1;
            self.next = sequence + 1;
        }
    };
    const Foreign = TestForeignDispatch(Store);
    Foreign.calls = 0;
    var state = State{};
    var vtable: Store.VTable = .{
        .deinit = Mock.deinit,
        .capabilities = Mock.capabilities,
        .begin_read = Mock.read,
        .begin_write = Mock.write,
        .begin_batch = Mock.batch,
        .for_each_replay_from = Mock.all,
        .for_each_replay_from_matching_hint_mask = Mock.matching,
        .for_each_replay_lane_from = Mock.lane,
    };
    var store: Store = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &vtable, .boundary_dispatch = Foreign.dispatch };
    // Nested erased wrappers must call the imported dispatcher rather than
    // invoking a foreign raw vtable and leaking local Zig errors into it.
    var wrapped = try storeFrom(std.testing.allocator, store);
    defer wrapped.deinit();
    wrapped.boundary_dispatch = Foreign.dispatch;
    for ([_]*Store{ &store, &wrapped }) |handle| {
        for ([_]anyerror{ error.StopReplayChunk, error.ConsumerPrivateFailure, error.OutOfMemory, error.Cancelled }) |failure| {
            for (0..4) |method| {
                var consumer = Consumer{ .failure = failure };
                const result: anyerror!void = switch (method) {
                    0 => handle.forEachReplayFrom(1, &consumer, Consumer.consume),
                    1 => handle.forEachReplayFromMatchingHintMask(1, 0, &consumer, Consumer.consume),
                    2 => blk: {
                        _ = handle.forEachReplayLaneFrom(internal_keys.replay_all_kind, 1, 0, &consumer, Consumer.consume) catch |err| break :blk err;
                        break :blk {};
                    },
                    3 => blk: {
                        const saved = vtable.for_each_replay_from_matching_hint_mask;
                        vtable.for_each_replay_from_matching_hint_mask = null;
                        defer vtable.for_each_replay_from_matching_hint_mask = saved;
                        break :blk handle.forEachReplayFromMatchingHintMask(1, 0, &consumer, Consumer.consume);
                    },
                    else => unreachable,
                };
                try std.testing.expectError(failure, result);
                try std.testing.expectEqual(@as(usize, 2), consumer.calls);
                try std.testing.expectEqual(@as(usize, 1), consumer.accepted);
                try std.testing.expectEqual(@as(u64, 2), consumer.next);
                consumer.failure = null;
                const stats = try handle.forEachReplayLaneFrom(internal_keys.replay_all_kind, consumer.next, 0, &consumer, Consumer.consume);
                try std.testing.expectEqual(@as(usize, 2), stats.matched_entries);
                try std.testing.expectEqual(@as(usize, 3), consumer.accepted);
            }
        }
        state.fail_read = true;
        var consumer = Consumer{ .failure = null };
        try std.testing.expectError(error.OutOfMemory, handle.forEachReplayLaneFrom(0, 1, 0, &consumer, Consumer.consume));
        try std.testing.expectEqual(@as(usize, 0), consumer.calls);
        state.fail_read = false;
    }
    try std.testing.expect(Foreign.calls > 0);
}

test "namespace read admission dispatches original controls and retires rejected captures" {
    const State = struct {
        active: usize = 0,
        opens: usize = 0,
        aborts: usize = 0,
        legacy: usize = 0,
        checks: usize = 0,
        reject_at: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return error.NamespaceOwnerRetired;
        }
    };
    const Txn = struct {
        state: *State,
        pub fn abort(self: *@This()) void {
            std.debug.assert(self.state.active == 1);
            self.state.active -= 1;
            self.state.aborts += 1;
        }
        pub fn get(self: *@This(), _: backend_types.Namespace, _: []const u8) ![]const u8 {
            self.state.legacy += 1;
            return error.TestUnexpectedResult;
        }
        pub fn getWithAdmission(_: *@This(), _: backend_types.Namespace, _: []const u8, control: read_admission.Port) ![]const u8 {
            try control.checkpoint();
            return "value";
        }
    };
    const NamespaceBackend = struct {
        state: *State,
        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{};
        }
        pub fn beginRead(self: *@This()) !Txn {
            self.state.legacy += 1;
            return error.TestUnexpectedResult;
        }
        pub fn beginReadWithAdmission(self: *@This(), control: read_admission.Port) !Txn {
            try control.checkpoint();
            self.state.active += 1;
            self.state.opens += 1;
            return .{ .state = self.state };
        }
        pub fn beginWrite(_: *@This()) !NamespaceWriteTxn {
            return error.Unsupported;
        }
        pub fn beginBatch(_: *@This()) !NamespaceBatch {
            return error.Unsupported;
        }
    };
    const Map = struct {
        fn namespace(ns: backend_types.Namespace) !backend_types.Namespace {
            return ns;
        }
    };
    var state: State = .{};
    var backend: NamespaceBackend = .{ .state = &state };
    var store = try namespaceStoreFrom(std.testing.allocator, &backend, backend_types.Namespace, Map.namespace);
    defer store.deinit();
    const ForeignStore = TestForeignDispatch(NamespaceStore);
    const ForeignRead = TestForeignDispatch(NamespaceReadTxn);
    store.boundary_dispatch = ForeignStore.dispatch;
    ForeignStore.calls = 0;
    ForeignRead.calls = 0;
    const payload: read_admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const control = read_admission.Port.init(&payload);
    {
        var txn = try store.beginReadWithAdmission(control);
        defer txn.abort();
        txn.boundary_dispatch = ForeignRead.dispatch;
        try std.testing.expectEqualStrings("value", try txn.getWithAdmission(.{}, "key", control));
    }
    const checks = state.checks;
    try std.testing.expect(ForeignStore.calls > 0 and ForeignRead.calls > 0);
    for (1..checks + 1) |reject_at| {
        state.checks = 0;
        state.reject_at = reject_at;
        const result = blk: {
            var txn = store.beginReadWithAdmission(control) catch |err| break :blk err;
            defer txn.abort();
            txn.boundary_dispatch = ForeignRead.dispatch;
            _ = txn.getWithAdmission(.{}, "key", control) catch |err| break :blk err;
            break :blk error.TestUnexpectedResult;
        };
        try std.testing.expectEqual(error.NamespaceOwnerRetired, result);
        try std.testing.expectEqual(@as(usize, 0), state.active);
        try std.testing.expectEqual(state.opens, state.aborts);
    }
    state.reject_at = 0;
    var legacy_store = store.vtable.*;
    legacy_store.begin_read_with_admission = null;
    const original = store.vtable;
    store.vtable = &legacy_store;
    try std.testing.expectError(error.ExecutionWaitUnsupported, store.beginReadWithAdmission(control));
    store.vtable = original;
    {
        var txn = try store.beginReadWithAdmission(control);
        defer txn.abort();
        var legacy_read = txn.vtable.*;
        legacy_read.get_with_admission = null;
        txn.vtable = &legacy_read;
        try std.testing.expectError(error.ExecutionWaitUnsupported, txn.getWithAdmission(.{}, "key", control));
    }
    try std.testing.expectEqual(@as(usize, 0), state.legacy);
    try std.testing.expectEqual(state.opens, state.aborts);
}

test "owned namespace boundary preserves ignored original rejection and retires returned owners through checked calls" {
    const a = std.testing.allocator;
    const Fixture = struct {
        var rejected: bool = false;
        var closes: usize = 0;
        const Outer = @This();
        const Fake = struct {
            pub fn deinit(_: *@This()) void {
                Outer.closes += 1;
            }
            pub fn step(_: *@This(), _: usize, control: read_admission.Port) !OwnedValueRead.Status {
                Outer.rejected = true;
                control.checkpoint() catch {};
                return .ready;
            }
            pub fn takeOutput(_: *@This()) !OwnedValue {
                return error.TestUnexpectedOwnedTake;
            }
        };
        fn begin(allocator: Allocator, _: *anyopaque, _: backend_types.Namespace, _: []const u8, control: read_admission.Port) !OwnedValueRead {
            rejected = true;
            control.checkpoint() catch {};
            return ownedValueReadFrom(allocator, Fake{});
        }
        fn storeDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return NamespaceStore.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
        fn readDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return OwnedValueRead.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
    };
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            if (Fixture.rejected) return error.TestOwnedBoundaryOriginalRejection;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedOwnedWait;
        }
    };
    var memory = @import("lsm_backend/storage_io.zig").MemoryStorage.init(a);
    defer memory.deinit();
    var backend = @import("lsm_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeNamespaceStore(a);
    defer store.deinit();
    var table = store.vtable.*;
    table.begin_owned_value_read = Fixture.begin;
    store.vtable = &table;
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = read_admission.Port.init(&payload);
    for ([_]bool{ false, true }) |checked| {
        store.boundary_dispatch = if (checked) Fixture.storeDispatch else NamespaceStore.BoundaryAbi.local_dispatch;
        Fixture.rejected = false;
        Fixture.closes = 0;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, store.beginOwnedValueRead(a, .{}, "key", control));
        try std.testing.expectEqual(@as(usize, 1), Fixture.closes);
        Fixture.rejected = false;
        var read = try ownedValueReadFrom(a, Fixture.Fake{});
        defer read.deinit();
        if (checked) read.boundary_dispatch = Fixture.readDispatch;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, read.step(1, control));
        try std.testing.expect(read.ptr == null);
        Fixture.rejected = false;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, read.step(1, control));
        try std.testing.expectEqual(@as(usize, 2), Fixture.closes);
        table.begin_owned_value_read = null;
        try std.testing.expectError(error.ExecutionWaitUnsupported, store.beginOwnedValueRead(a, .{}, "key", control));
        table.begin_owned_value_read = Fixture.begin;
    }
}

test "owned namespace boundary single Store preserves ignored original rejection and retires returned owners through checked calls" {
    const a = std.testing.allocator;
    const Fixture = struct {
        var rejected: bool = false;
        var closes: usize = 0;
        const Outer = @This();
        const Fake = struct {
            pub fn deinit(_: *@This()) void {
                Outer.closes += 1;
            }
            pub fn step(_: *@This(), _: usize, control: read_admission.Port) !OwnedValueRead.Status {
                Outer.rejected = true;
                control.checkpoint() catch {};
                return .ready;
            }
            pub fn takeOutput(_: *@This()) !OwnedValue {
                return error.TestUnexpectedOwnedTake;
            }
        };
        fn begin(allocator: Allocator, _: *anyopaque, _: []const u8, control: read_admission.Port) !OwnedValueRead {
            rejected = true;
            control.checkpoint() catch {};
            return ownedValueReadFrom(allocator, Fake{});
        }
        fn storeDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return Store.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
        fn readDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return OwnedValueRead.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
    };
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            if (Fixture.rejected) return error.TestOwnedBoundaryOriginalRejection;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedOwnedWait;
        }
    };
    var memory = @import("lsm_backend/storage_io.zig").MemoryStorage.init(a);
    defer memory.deinit();
    var backend = @import("lsm_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{});
    defer store.deinit();
    var table = store.vtable.*;
    table.begin_owned_value_read = Fixture.begin;
    store.vtable = &table;
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = read_admission.Port.init(&payload);
    for ([_]bool{ false, true }) |checked| {
        store.boundary_dispatch = if (checked) Fixture.storeDispatch else Store.BoundaryAbi.local_dispatch;
        Fixture.rejected = false;
        Fixture.closes = 0;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, store.beginOwnedValueRead(a, "key", control));
        try std.testing.expectEqual(@as(usize, 1), Fixture.closes);
        Fixture.rejected = false;
        var read = try ownedValueReadFrom(a, Fixture.Fake{});
        defer read.deinit();
        if (checked) read.boundary_dispatch = Fixture.readDispatch;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, read.step(1, control));
        try std.testing.expect(read.ptr == null);
        Fixture.rejected = false;
        try std.testing.expectError(error.TestOwnedBoundaryOriginalRejection, read.step(1, control));
        try std.testing.expectEqual(@as(usize, 2), Fixture.closes);
        table.begin_owned_value_read = null;
        try std.testing.expectError(error.ExecutionWaitUnsupported, store.beginOwnedValueRead(a, "key", control));
        table.begin_owned_value_read = Fixture.begin;
    }
}

test "owned namespace boundary key cursor preserves ignored original errors and retires snapshot and value handoff" {
    const a = std.testing.allocator;
    const Fixture = struct {
        const Stage = enum { begin, seek, step, into };
        var stage: Stage = .begin;
        var rejected: bool = false;
        var keys_closed: usize = 0;
        var values_closed: usize = 0;
        const Outer = @This();
        fn ignore(control: read_admission.Port) void {
            rejected = true;
            control.checkpoint() catch {};
        }
        const Value = struct {
            pub fn deinit(_: *@This()) void {
                Outer.values_closed += 1;
            }
            pub fn step(_: *@This(), _: usize, _: read_admission.Port) !OwnedValueRead.Status {
                return .ready;
            }
            pub fn takeOutput(_: *@This()) !OwnedValue {
                return error.TestUnexpectedKeyValueTake;
            }
        };
        const Key = struct {
            pub fn deinit(_: *@This()) void {
                Outer.keys_closed += 1;
            }
            pub fn seek(_: *@This(), _: []const u8, _: ?[]const u8, _: bool, control: read_admission.Port) !void {
                if (Outer.stage == .seek) Outer.ignore(control);
            }
            pub fn step(_: *@This(), _: usize, control: read_admission.Port) !OwnedKeyCursor.Status {
                if (Outer.stage == .step) Outer.ignore(control);
                return .ready;
            }
            pub fn key(_: *@This()) !?[]const u8 {
                return "key";
            }
            pub fn intoValueRead(_: *@This(), control: read_admission.Port) !Value {
                if (Outer.stage == .into) Outer.ignore(control);
                return .{};
            }
        };
        fn begin(allocator: Allocator, _: *anyopaque, control: read_admission.Port) !OwnedKeyCursor {
            if (stage == .begin) ignore(control);
            return ownedKeyCursorFrom(allocator, Key{});
        }
        fn storeDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return Store.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
        fn cursorDispatch(contract: *const @import("antfly_runtime_abi").native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, result: ?*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
            return OwnedKeyCursor.BoundaryAbi.local_dispatch(contract, callback, args, result);
        }
    };
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            if (Fixture.rejected) return error.TestOwnedKeyBoundaryOriginal;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedKeyBoundaryWait;
        }
    };
    var backend = @import("lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{});
    defer store.deinit();
    var table = store.vtable.*;
    table.begin_owned_key_cursor = Fixture.begin;
    store.vtable = &table;
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = read_admission.Port.init(&payload);
    for ([_]bool{ false, true }) |checked| {
        store.boundary_dispatch = if (checked) Fixture.storeDispatch else Store.BoundaryAbi.local_dispatch;
        for ([_]Fixture.Stage{ .begin, .seek, .step, .into }) |stage| {
            Fixture.stage = stage;
            Fixture.rejected = false;
            Fixture.keys_closed = 0;
            Fixture.values_closed = 0;
            if (stage == .begin) {
                try std.testing.expectError(error.TestOwnedKeyBoundaryOriginal, store.beginOwnedKeyCursor(a, control));
            } else {
                var cursor = try store.beginOwnedKeyCursor(a, control);
                defer cursor.deinit();
                if (checked) cursor.boundary_dispatch = Fixture.cursorDispatch;
                if (stage == .seek) {
                    try std.testing.expectError(error.TestOwnedKeyBoundaryOriginal, cursor.seek("", null, false, control));
                } else {
                    try cursor.seek("", null, false, control);
                    if (stage == .step) {
                        try std.testing.expectError(error.TestOwnedKeyBoundaryOriginal, cursor.step(1, control));
                    } else {
                        try std.testing.expectEqual(OwnedKeyCursor.Status.ready, try cursor.step(1, control));
                        try std.testing.expectError(error.TestOwnedKeyBoundaryOriginal, cursor.intoValueRead(control));
                    }
                }
                Fixture.rejected = false;
                try std.testing.expectError(error.TestOwnedKeyBoundaryOriginal, cursor.step(1, control));
                try std.testing.expect(cursor.ptr == null);
            }
            try std.testing.expectEqual(@as(usize, 1), Fixture.keys_closed);
            try std.testing.expectEqual(@as(usize, @intFromBool(stage == .into)), Fixture.values_closed);
        }
    }
    Fixture.rejected = false;
    table.begin_owned_key_cursor = null;
    try std.testing.expectError(error.ExecutionWaitUnsupported, store.beginOwnedKeyCursor(a, control));
}

test "retained metadata read admission dispatches errors and rejects partial foreign results" {
    const Fixture = struct {
        cause: anyerror = error.Backpressured,
        reject_original: bool = false,
        rejected: bool = false,
        ignore_rejection: bool = false,
        closed: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejected) return error.TestOriginalReadRejection;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_original) {
                self.rejected = true;
                return error.TestOriginalReadRejection;
            }
            return self.cause;
        }
        fn fail(raw: *anyopaque, control: read_admission.Port) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            control.waitForProgress() catch |err| {
                if (!self.ignore_rejection) return err;
            };
        }
        fn many(raw: *anyopaque, _: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
            values[0] = "partial";
            try fail(raw, control);
        }
        fn into(raw: *anyopaque, keys: []const []const u8, _: []const []u8, values: []?[]const u8, control: read_admission.Port) !void {
            return many(raw, keys, values, control);
        }
        fn abort(_: Allocator, _: *anyopaque) void {}
        fn get(_: *anyopaque, _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        fn cursor(a: Allocator, raw: *anyopaque) !Cursor {
            return .{ .allocator = a, .ptr = raw, .vtable = &.{ .close = close, .first = entry, .last = entry, .next = entry, .prev = entry, .seek_at_or_after = seek, .seek_at_or_before = seek } };
        }
        fn open(a: Allocator, raw: *anyopaque, control: read_admission.Port) !Cursor {
            try fail(raw, control);
            return cursor(a, raw);
        }
        fn close(_: Allocator, raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closed += 1;
        }
        fn entry(_: *anyopaque) !?Entry {
            return null;
        }
        fn seek(raw: *anyopaque, _: []const u8) !?Entry {
            return entry(raw);
        }
    };
    const ReadDispatch = TestForeignDispatch(ReadTxn);
    const ScopeDispatch = TestForeignDispatch(ReadScope);
    const ProbeDispatch = TestForeignDispatch(ProbeTxn);
    ReadDispatch.calls = 0;
    ScopeDispatch.calls = 0;
    ProbeDispatch.calls = 0;
    var state: Fixture = .{};
    const payload: read_admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Fixture.check, .wait_for_progress = Fixture.wait } };
    const control = read_admission.Port.init(&payload);
    var read: ReadTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Fixture.abort, .get = Fixture.get, .get_many_sorted_with_admission = Fixture.many, .open_cursor = Fixture.cursor, .open_cursor_with_admission = Fixture.open }, .boundary_dispatch = ReadDispatch.dispatch };
    var scope: ReadScope = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .close = Fixture.abort, .get = Fixture.get, .get_many_sorted_with_admission = Fixture.many, .get_many_sorted_into_with_admission = Fixture.into }, .boundary_dispatch = ScopeDispatch.dispatch };
    var probe: ProbeTxn = .{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{ .abort = Fixture.abort, .get = Fixture.get, .get_many_sorted_with_admission = Fixture.many }, .boundary_dispatch = ProbeDispatch.dispatch };
    var values: [1]?[]const u8 = undefined;
    var buffer: [16]u8 = undefined;
    for ([_]anyerror{ error.Backpressured, error.Canceled, error.DeadlineExceeded }) |cause| {
        state.cause = cause;
        try std.testing.expectError(cause, read.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        try std.testing.expectError(cause, scope.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        try std.testing.expectError(cause, scope.getManySortedIntoWithAdmission(&.{"key"}, &.{&buffer}, &values, control));
        try std.testing.expect(values[0] == null);
        try std.testing.expectError(cause, probe.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        try std.testing.expectError(cause, read.openCursorWithAdmission(control));
    }
    state.reject_original = true;
    for ([_]bool{ false, true }) |ignore| {
        state.ignore_rejection = ignore;
        state.rejected = false;
        try std.testing.expectError(error.TestOriginalReadRejection, read.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        state.rejected = false;
        try std.testing.expectError(error.TestOriginalReadRejection, scope.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        state.rejected = false;
        try std.testing.expectError(error.TestOriginalReadRejection, scope.getManySortedIntoWithAdmission(&.{"key"}, &.{&buffer}, &values, control));
        try std.testing.expect(values[0] == null);
        state.rejected = false;
        try std.testing.expectError(error.TestOriginalReadRejection, probe.getManySortedWithAdmission(&.{"key"}, &values, control));
        try std.testing.expect(values[0] == null);
        state.rejected = false;
        try std.testing.expectError(error.TestOriginalReadRejection, read.openCursorWithAdmission(control));
    }
    try std.testing.expectEqual(@as(usize, 10), ReadDispatch.calls);
    try std.testing.expectEqual(@as(usize, 10), ScopeDispatch.calls);
    try std.testing.expectEqual(@as(usize, 5), ProbeDispatch.calls);
    // A provider that ignores cancellation still transfers a cursor; reject
    // and close it before any failed call can publish that resource.
    try std.testing.expectEqual(@as(usize, 1), state.closed);
}
