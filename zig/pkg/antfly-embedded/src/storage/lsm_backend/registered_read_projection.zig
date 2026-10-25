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

//! Prepaid, registered reader projection construction. All physical buffers
//! and source guards survive refusal; no Backend or execution borrow escapes.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const ReadVersion = @import("runtime.zig").ReadVersion;
const Run = @import("repository.zig").Run;
const Group = @typeInfo(@FieldType(ReadVersion, "l0_groups")).pointer.child;
const Level = @typeInfo(@FieldType(ReadVersion, "levels")).pointer.child;
const Comparison = @import("run_comparison.zig").Cursor;
const Root = @import("registered_directory_root.zig").Candidate;

pub const Owner = struct {
    const State = struct {
        source: Directory = .{},
        runs: ?[]Run = null,
        groups: ?[]Group = null,
        levels: ?[]Level = null,
        indices: ?[]usize = null,
        assignments: ?[]usize = null,
        counts: ?[]usize = null,
        phase: enum { allocate, runs, groups, offsets, members, ready } = .allocate,
        allocation: usize = 0,
        index: usize = 0,
        l0_count: usize = 0,
        group_count: usize = 0,
        level_count: usize = 0,
        offset: usize = 0,
        read: ?Directory.Cursor = null,
        bounds: ?Directory.BoundsCursor = null,
        pending: ?Directory.Handle = null,
        group_phase: enum { lower, upper, grow_lower, grow_upper, rank } = .lower,
        comparison: ?Comparison = null,
        search: ?Directory.ReadSearch = null,
        cleanup_field: usize = 0,
        cleanup_offset: usize = 0,
        source_cleanup: ?Directory.Reclaimer = null,
        source_retired: bool = false,
    };
    pub const Views = struct { runs: []Run, groups: []Group, levels: []Level, bytes: u64 };
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    state: ?*State = null,
    count: usize,
    state_offset: usize = 0,
    state_retiring: bool = false,
    taken: bool = false,
    closing: bool = false,
    complete: bool = false,

    pub fn memoryBound(n: usize) !u64 {
        const per: u64 = @sizeOf(Run) + @sizeOf(Group) + @sizeOf(Level) + 3 * @sizeOf(usize);
        return std.math.add(u64, @sizeOf(Owner) + @sizeOf(State), std.math.mul(u64, n, per) catch return error.ResourceBudgetExceeded) catch return error.ResourceBudgetExceeded;
    }
    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        _ = try Root.validateSource(source, a, original);
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, try memoryBound(source.count()), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .count = source.count() };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.state = try a.create(State);
        self.state.?.* = .{ .source = source.forkContents() };
        return self;
    }
    fn newGroup(s: *State, run: *const Run) void {
        s.groups.?[s.group_count] = .{ .smallest_namespace_name = run.smallest_namespace_name, .smallest_key = run.smallest_key, .largest_namespace_name = run.largest_namespace_name, .largest_key = run.largest_key, .run_indices = &.{} };
        s.counts.?[s.group_count] = 0;
        s.group_count += 1;
        s.group_phase = .rank;
    }
    fn bound(s: *State, a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8, original: admission.Port) !std.math.Order {
        if (s.comparison == null) s.comparison = .bound(a_ns, a, b_ns, b);
        const result = try s.comparison.?.resumeWithAdmission(original);
        s.comparison = null;
        return result;
    }
    pub fn resumeWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.taken or self.state == null) return error.InvalidArgument;
        const s = self.state.?;
        while (true) {
            try original.checkpoint();
            switch (s.phase) {
                .allocate => {
                    switch (s.allocation) {
                        0 => s.runs = try self.allocator.alloc(Run, self.count),
                        1 => s.groups = try self.allocator.alloc(Group, self.count),
                        2 => s.levels = try self.allocator.alloc(Level, self.count),
                        3 => s.indices = try self.allocator.alloc(usize, self.count),
                        4 => s.assignments = try self.allocator.alloc(usize, self.count),
                        5 => s.counts = try self.allocator.alloc(usize, self.count),
                        else => {
                            s.read = s.source.readCursor();
                            s.bounds = s.source.boundsCursor();
                            s.phase = .runs;
                            continue;
                        },
                    }
                    s.allocation += 1;
                },
                .runs => {
                    if (s.index == self.count) {
                        s.index = 0;
                        s.phase = .groups;
                        continue;
                    }
                    var run = (s.read.?.next() orelse return error.CompactionPlanningStale).run.*;
                    run.owns_metadata = false;
                    run.owns_path = false;
                    run.owns_bloom_filter = false;
                    run.version_ref_pinned = false;
                    run.shared_read_version = true;
                    s.runs.?[s.index] = run;
                    if (run.level == 0) {
                        s.l0_count += 1;
                    } else if (s.level_count != 0 and s.levels.?[s.level_count - 1].level == run.level) {
                        s.levels.?[s.level_count - 1].len += 1;
                    } else {
                        s.levels.?[s.level_count] = .{ .level = run.level, .start_index = s.index, .len = 1 };
                        s.level_count += 1;
                    }
                    s.index += 1;
                },
                .groups => {
                    if (s.pending == null) {
                        const handle = try s.bounds.?.nextWithAdmission(original) orelse {
                            s.phase = .offsets;
                            continue;
                        };
                        if (handle.run.level != 0) continue;
                        s.pending = handle;
                        if (s.group_count == 0) newGroup(s, handle.run) else s.group_phase = .lower;
                    }
                    const run = s.pending.?.run;
                    const group = &s.groups.?[s.group_count - 1];
                    switch (s.group_phase) {
                        .lower => {
                            if (try bound(s, run.smallest_namespace_name, run.smallest_key, group.largest_namespace_name, group.largest_key, original) == .gt) newGroup(s, run) else s.group_phase = .upper;
                        },
                        .upper => {
                            if (try bound(s, run.largest_namespace_name, run.largest_key, group.smallest_namespace_name, group.smallest_key, original) == .lt) newGroup(s, run) else s.group_phase = .grow_lower;
                        },
                        .grow_lower => {
                            if (try bound(s, run.smallest_namespace_name, run.smallest_key, group.smallest_namespace_name, group.smallest_key, original) == .lt) {
                                group.smallest_namespace_name = run.smallest_namespace_name;
                                group.smallest_key = run.smallest_key;
                            }
                            s.group_phase = .grow_upper;
                        },
                        .grow_upper => {
                            if (try bound(s, run.largest_namespace_name, run.largest_key, group.largest_namespace_name, group.largest_key, original) == .gt) {
                                group.largest_namespace_name = run.largest_namespace_name;
                                group.largest_key = run.largest_key;
                            }
                            s.group_phase = .rank;
                        },
                        .rank => {
                            if (s.search == null) s.search = s.source.locateRead(run);
                            try s.search.?.resumeWithAdmission(original);
                            const matched = s.search.?.matched orelse return error.CompactionPlanningStale;
                            if (@as(*const anyopaque, matched.entry.payload.?) != s.pending.?.revision or s.search.?.rank >= s.l0_count) return error.CompactionPlanningStale;
                            s.assignments.?[s.search.?.rank] = s.group_count - 1;
                            s.counts.?[s.group_count - 1] += 1;
                            s.search = null;
                            s.pending = null;
                        },
                    }
                },
                .offsets => {
                    if (s.index == s.group_count) {
                        if (s.offset != s.l0_count) return error.ResourceAccountingMismatch;
                        s.index = 0;
                        s.phase = .members;
                        continue;
                    }
                    const n = s.counts.?[s.index];
                    s.groups.?[s.index].run_indices = s.indices.?[s.offset .. s.offset + n];
                    s.offset += n;
                    s.counts.?[s.index] = 0;
                    s.index += 1;
                },
                .members => {
                    if (s.index == s.l0_count) {
                        s.phase = .ready;
                        continue;
                    }
                    const group = s.assignments.?[s.index];
                    s.groups.?[group].run_indices[s.counts.?[group]] = s.index;
                    s.counts.?[group] += 1;
                    s.index += 1;
                },
                .ready => return,
            }
        }
    }
    /// Caller serializes publication and checks original lifetime before this
    /// fixed ownership tail. Physical buffers keep their complete allocations;
    /// group member views are disjoint slices of one owned indices allocation.
    pub fn takePrepared(self: *Owner) !Views {
        if (self.closing or self.taken or self.state == null or self.state.?.phase != .ready) return error.InvalidArgument;
        const s = self.state.?;
        self.taken = true;
        return .{ .runs = s.runs.?, .groups = s.groups.?[0..s.group_count], .levels = s.levels.?[0..s.level_count], .bytes = @as(u64, self.count) * (@sizeOf(Run) + @sizeOf(Group) + @sizeOf(Level) + 3 * @sizeOf(usize)) };
    }
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing) return;
        self.closing = true;
        self.registration.publish();
    }
    const Buffer = struct { bytes: []u8, alignment: std.mem.Alignment };
    fn view(comptime T: type, values: ?[]T) ?Buffer {
        return if (values) |v| .{ .bytes = std.mem.sliceAsBytes(v), .alignment = .of(T) } else null;
    }
    fn buffer(s: *State) ?Buffer {
        return switch (s.cleanup_field) {
            0 => view(Run, s.runs),
            1 => view(Group, s.groups),
            2 => view(Level, s.levels),
            3 => view(usize, s.indices),
            4 => view(usize, s.assignments),
            5 => view(usize, s.counts),
            else => unreachable,
        };
    }
    fn clear(s: *State) void {
        switch (s.cleanup_field) {
            0 => s.runs = null,
            1 => s.groups = null,
            2 => s.levels = null,
            3 => s.indices = null,
            4 => s.assignments = null,
            5 => s.counts = null,
            else => unreachable,
        }
        s.cleanup_field += 1;
        s.cleanup_offset = 0;
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.state) |s| {
            if (!self.state_retiring) {
                if (s.cleanup_field < 6) {
                    if (buffer(s)) |current| {
                        const end = s.cleanup_offset + @min(16 * 1024, current.bytes.len - s.cleanup_offset);
                        @memset(current.bytes[s.cleanup_offset..end], undefined);
                        s.cleanup_offset = end;
                        if (end != current.bytes.len) return false;
                        if (current.bytes.len != 0) self.allocator.rawFree(current.bytes, current.alignment, @returnAddress());
                    }
                    clear(s);
                    return false;
                }
                if (!s.source_retired) {
                    if (s.source_cleanup == null) s.source_cleanup = .init(&s.source);
                    var steps: usize = 1;
                    if (!s.source_cleanup.?.step(self.allocator, &steps)) return false;
                    if (original) |port| try port.checkpoint();
                    s.source_cleanup.?.finishContents();
                    s.source_cleanup = null;
                    s.source = .{};
                    s.source_retired = true;
                }
                self.state_retiring = true;
            }
            const bytes = std.mem.asBytes(s);
            const end = self.state_offset + @min(16 * 1024, bytes.len - self.state_offset);
            @memset(bytes[self.state_offset..end], undefined);
            self.state_offset = end;
            if (end != bytes.len) return false;
            self.state = null;
            self.allocator.rawFree(bytes, .of(State), @returnAddress());
            return false;
        }
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.state == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
