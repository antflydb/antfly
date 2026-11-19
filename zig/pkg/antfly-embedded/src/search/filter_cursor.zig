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

//! Paged exact postings selection over one frozen inventory/deletion cut.
//! The writer captures visibility under its mutation fence. Pulls retain only
//! one segment bitmap, never a corpus-sized ID array or a ranked top-K prefix.
const std = @import("std");
const index = @import("../index.zig");
const query = @import("query.zig");
const roaring = @import("../encoding/roaring.zig");
const MemoryBudget = @import("../sql/memory_budget.zig");
const A = std.mem.Allocator;
pub const Limits = struct {
    memory_bytes: usize = 32 * 1024 * 1024,
    segments: usize = 65_536,
};
pub const Page = struct { len: usize, exhausted: bool, segments_opened: usize };
pub const Cursor = struct {
    backing: A,
    budget: MemoryBudget,
    snapshot: *index.IndexSnapshot,
    /// Borrowed immutable query nodes must outlive this cursor.
    filter: query.Filter,
    control: query.ExecutionControl,
    deleted: []?roaring.RoaringBitmap,
    segment: usize = 0,
    offset: u32 = 0,
    bitmap: ?roaring.RoaringBitmap = null,
    iterator: ?roaring.Iterator = null,
    failed: bool = false,

    /// Internal constructor: the caller holds the writer's mutation fence.
    /// Capture never binds cold navigation or reads stored documents.
    pub fn captureLocked(a: A, snapshot: *index.IndexSnapshot, filter: query.Filter, control: query.ExecutionControl, limits: Limits) !*Cursor {
        try control.check();
        if (snapshot.segments.len > limits.segments) return error.QueryCandidateBudgetExceeded;
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = limits.memory_bytes }, .snapshot = snapshot.retain(), .filter = filter, .control = control, .deleted = &.{} };
        errdefer self.snapshot.release();
        self.deleted = self.budget.allocator().alloc(?roaring.RoaringBitmap, snapshot.segments.len) catch |err| return self.allocationError(err);
        @memset(self.deleted, null);
        errdefer {
            for (self.deleted) |*mask| if (mask.*) |*value| value.deinit();
            self.budget.allocator().free(self.deleted);
        }
        for (snapshot.segments, self.deleted) |*segment, *mask| {
            try control.check();
            mask.* = segment.cloneDeleted(self.budget.allocator()) catch |err| return self.allocationError(err);
        }
        return self;
    }
    fn allocationError(self: *Cursor, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.budget.isExhausted()) error.QueryCandidateBudgetExceeded else err;
    }
    fn retireSegment(self: *Cursor) void {
        self.iterator = null;
        if (self.bitmap) |*bitmap| bitmap.deinit();
        self.bitmap = null;
    }
    pub fn deinit(self: *Cursor) void {
        self.retireSegment();
        for (self.deleted) |*mask| if (mask.*) |*value| value.deinit();
        self.budget.allocator().free(self.deleted);
        self.snapshot.release();
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
    pub fn peakMemoryBytes(self: *const Cursor) usize {
        return self.budget.peak;
    }
    /// A page may be empty and nonterminal when sparse segments consume this
    /// pull's work budget. Consumers must continue until exhausted is true.
    pub fn nextPage(self: *Cursor, output: []u32, segment_budget: usize) !Page {
        if (self.failed) return error.InvalidTextFilterCursorState;
        if (output.len == 0 or segment_budget == 0) return error.InvalidTextFilterCursorBudget;
        errdefer self.failed = true;
        try self.control.check();
        var written: usize = 0;
        var opened: usize = 0;
        while (self.segment < self.snapshot.segments.len) {
            if (self.bitmap == null) {
                if (opened == segment_budget) break;
                try self.control.check();
                var lease = try self.snapshot.leaseSegment(self.segment);
                defer lease.deinit();
                self.bitmap = self.filter.withControl(self.control).executeWithOffset(self.budget.allocator(), lease.entry, self.offset) catch |err| return self.allocationError(err);
                // Never consult the live shared mask after capture.
                if (self.deleted[self.segment]) |*mask| self.bitmap.?.andNotWith(mask);
                self.iterator = self.bitmap.?.iterator();
                opened += 1;
            }
            while (written < output.len) {
                if (written % 64 == 0) try self.control.check();
                const local = self.iterator.?.next() orelse break;
                output[written] = std.math.add(u32, self.offset, local) catch return error.InvalidSegment;
                written += 1;
            }
            if (written == output.len) break;
            self.retireSegment();
            self.offset = std.math.add(u32, self.offset, self.snapshot.segments[self.segment].reader.doc_count) catch return error.InvalidSegment;
            self.segment += 1;
        }
        return .{ .len = written, .exhausted = self.segment == self.snapshot.segments.len, .segments_opened = opened };
    }
    /// Result IDs resolve through the retained inventory, including after live
    /// compaction replaces the source segments. Identity expires with scratch.
    pub fn key(self: *Cursor, scratch: A, global_id: u32) ![]const u8 {
        if (self.failed) return error.InvalidTextFilterCursorState;
        errdefer self.failed = true;
        try self.control.check();
        return (try self.snapshot.storedIdScoped(scratch, global_id)) orelse error.MissingTextDocumentId;
    }
};
