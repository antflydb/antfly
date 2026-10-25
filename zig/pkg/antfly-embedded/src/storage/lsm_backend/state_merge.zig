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

//! Retained newest-first immutable-state merge used by normal flush builds.
//! Inputs remain immutable/pinned through driving; abort stops input reads.
//! Array funding belongs to the caller. No admission port is retained.
const std = @import("std");
const State = @import("state.zig").State;
const table = @import("../lsm/table_file.zig");
const admission = @import("../../common/read_admission.zig");
const Comparison = @import("run_comparison.zig").Cursor;

pub const Heap = struct {
    allocator: std.mem.Allocator,
    states: []const *const State,
    positions: []usize = &.{},
    sources: []usize = &.{},
    advanced_sources: []usize = &.{},
    cursors: []State.EntryCursor = &.{},
    allocation_slot: usize = 0,
    len: usize = 0,
    initialized: bool = false,
    init_source: usize = 0,
    init_prepared: bool = false,
    init_pushed: bool = false,
    motion: enum { none, up, down_choose, down_compare } = .none,
    motion_index: usize = 0,
    child: usize = 0,
    comparison: ?Comparison = null,
    pending_key: ?Key = null,
    advance_stage: enum { check, pop, reinsert } = .check,
    popped_source: usize = 0,
    advanced_len: usize = 0,
    reinsert_index: usize = 0,
    reinserting: bool = false,
    aborting: bool = false,
    retire_slot: usize = 0,
    retire_offset: usize = 0,

    const Key = struct { namespace: ?[]const u8, bytes: []const u8 };
    pub const Winner = struct { source: usize, entry: table.Entry };

    pub fn memoryBound(count: usize) !u64 {
        const stride = 3 * @sizeOf(usize) + @sizeOf(State.EntryCursor);
        const arrays = std.math.mul(u64, count, stride) catch return error.ResourceBudgetExceeded;
        return std.math.add(u64, @sizeOf(Heap), arrays) catch error.ResourceBudgetExceeded;
    }
    /// Caller prepays memoryBound before allocating/driving this fixed header.
    /// Inputs and partial arrays remain owned until successful take or abort.
    pub fn prepare(allocator: std.mem.Allocator, states: []const *const State) !Heap {
        _ = try memoryBound(states.len);
        return .{ .allocator = allocator, .states = states };
    }
    pub fn allocate(allocator: std.mem.Allocator, states: []const *const State) !Heap {
        var out = try prepare(allocator, states);
        errdefer out.deinit();
        try out.allocateWithAdmission(null);
        return out;
    }
    pub fn allocateWithAdmission(self: *Heap, original: ?admission.Port) !void {
        if (self.aborting) return error.InvalidArgument;
        while (self.allocation_slot < 4) {
            try checkpoint(original);
            switch (self.allocation_slot) {
                0 => self.positions = try self.allocator.alloc(usize, self.states.len),
                1 => self.sources = try self.allocator.alloc(usize, self.states.len),
                2 => self.advanced_sources = try self.allocator.alloc(usize, self.states.len),
                3 => self.cursors = try self.allocator.alloc(State.EntryCursor, self.states.len),
                else => unreachable,
            }
            self.allocation_slot += 1;
        }
    }
    pub fn init(allocator: std.mem.Allocator, states: []const *const State) !Heap {
        var out = try allocate(allocator, states);
        errdefer out.deinit();
        try out.initializeWithAdmission(null);
        return out;
    }
    fn checkpoint(original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
    }
    pub fn initializeWithAdmission(self: *Heap, original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.aborting) return error.InvalidArgument;
        comptime std.debug.assert(@sizeOf(State.EntryCursor) <= Comparison.quantum);
        try self.allocateWithAdmission(original);
        while (self.init_source < self.states.len) {
            try checkpoint(original);
            const source = self.init_source;
            if (!self.init_prepared) {
                self.positions[source] = 0;
                self.cursors[source] = .{};
                self.init_prepared = true;
            }
            if (self.states[source].entryCount() != 0) {
                if (!self.init_pushed) {
                    self.beginPush(source);
                    self.init_pushed = true;
                }
                try self.finishMotion(original);
            }
            self.init_source += 1;
            self.init_prepared = false;
            self.init_pushed = false;
        }
        self.initialized = true;
    }
    pub fn peekSource(self: *const Heap) ?usize {
        std.debug.assert(self.initialized and !self.aborting and self.pending_key == null);
        return if (self.len == 0) null else self.sources[0];
    }
    pub fn currentEntry(self: *const Heap, source: usize) table.Entry {
        const row = self.cursors[source].at(self.states[source], self.positions[source]);
        return .{ .namespace_name = row.namespace_name, .key = row.key, .value = row.value, .tombstone = row.tombstone };
    }
    pub fn peekWithAdmission(self: *const Heap, original: admission.Port) !?Winner {
        try original.checkpoint();
        if (!self.initialized or self.aborting or self.pending_key != null) return error.InvalidArgument;
        const source = self.peekSource() orelse return null;
        return .{ .source = source, .entry = self.currentEntry(source) };
    }
    fn sameKey(a: Key, b: table.Entry) bool {
        if ((a.namespace == null) != (b.namespace_name == null)) return false;
        if (a.namespace) |ns| {
            const other = b.namespace_name.?;
            if (ns.ptr != other.ptr or ns.len != other.len) return false;
        }
        return a.bytes.ptr == b.key.ptr and a.bytes.len == b.key.len;
    }
    pub fn advanceTopSourcesAtKey(self: *Heap, entry: table.Entry) !usize {
        return self.advanceWithAdmission(entry, null);
    }
    /// Retry the same immutable key after refusal. A completed receipt precedes
    /// return; callers record its count before their next fallible operation.
    pub fn advanceWithAdmission(self: *Heap, entry: table.Entry, original: ?admission.Port) !usize {
        try checkpoint(original);
        if (!self.initialized or self.aborting) return error.InvalidArgument;
        if (self.pending_key) |key| {
            if (!sameKey(key, entry)) return error.InvalidArgument;
        } else {
            self.pending_key = .{ .namespace = entry.namespace_name, .bytes = entry.key };
            self.advance_stage = .check;
            self.advanced_len = 0;
            self.reinsert_index = 0;
            self.reinserting = false;
        }
        while (true) {
            try checkpoint(original);
            switch (self.advance_stage) {
                .check => {
                    if (self.len == 0) {
                        self.advance_stage = .reinsert;
                        continue;
                    }
                    const source = self.sources[0];
                    if (self.comparison == null) {
                        const row = self.currentEntry(source);
                        self.comparison = Comparison.bound(row.namespace_name, row.key, self.pending_key.?.namespace, self.pending_key.?.bytes);
                    }
                    const order = try self.comparison.?.resumeWithAdmission(original);
                    self.comparison = null;
                    if (order != .eq) {
                        self.advance_stage = .reinsert;
                        continue;
                    }
                    self.popped_source = source;
                    self.len -= 1;
                    if (self.len != 0) self.sources[0] = self.sources[self.len];
                    self.motion = if (self.len == 0) .none else .down_choose;
                    self.motion_index = 0;
                    self.advance_stage = .pop;
                },
                .pop => {
                    try self.finishMotion(original);
                    const source = self.popped_source;
                    self.positions[source] += 1;
                    self.advanced_sources[self.advanced_len] = source;
                    self.advanced_len += 1;
                    self.advance_stage = .check;
                },
                .reinsert => {
                    if (self.reinsert_index == self.advanced_len) {
                        const consumed = self.advanced_len;
                        self.pending_key = null;
                        return consumed;
                    }
                    const source = self.advanced_sources[self.reinsert_index];
                    if (self.positions[source] < self.states[source].entryCount()) {
                        if (!self.reinserting) {
                            self.beginPush(source);
                            self.reinserting = true;
                        }
                        try self.finishMotion(original);
                    }
                    self.reinserting = false;
                    self.reinsert_index += 1;
                },
            }
        }
    }
    fn beginPush(self: *Heap, source: usize) void {
        std.debug.assert(self.motion == .none and self.len < self.sources.len);
        self.sources[self.len] = source;
        self.motion_index = self.len;
        self.len += 1;
        self.motion = .up;
    }
    fn less(self: *Heap, a: usize, b: usize, original: ?admission.Port) !bool {
        if (self.comparison == null) {
            const lhs = self.currentEntry(a);
            const rhs = self.currentEntry(b);
            self.comparison = Comparison.bound(lhs.namespace_name, lhs.key, rhs.namespace_name, rhs.key);
        }
        const order = try self.comparison.?.resumeWithAdmission(original);
        self.comparison = null;
        return if (order == .eq) a < b else order == .lt;
    }
    fn finishMotion(self: *Heap, original: ?admission.Port) !void {
        while (self.motion != .none) {
            try checkpoint(original);
            const index = self.motion_index;
            switch (self.motion) {
                .up => {
                    if (index == 0) {
                        self.motion = .none;
                        continue;
                    }
                    const parent = (index - 1) / 2;
                    if (!try self.less(self.sources[index], self.sources[parent], original)) {
                        self.motion = .none;
                        continue;
                    }
                    std.mem.swap(usize, &self.sources[index], &self.sources[parent]);
                    self.motion_index = parent;
                },
                .down_choose => {
                    const left = index * 2 + 1;
                    if (left >= self.len) {
                        self.motion = .none;
                        continue;
                    }
                    const right = left + 1;
                    self.child = left;
                    if (right < self.len and try self.less(self.sources[right], self.sources[left], original)) self.child = right;
                    self.motion = .down_compare;
                },
                .down_compare => {
                    if (!try self.less(self.sources[self.child], self.sources[index], original)) {
                        self.motion = .none;
                        continue;
                    }
                    std.mem.swap(usize, &self.sources[self.child], &self.sources[index]);
                    self.motion_index = self.child;
                    self.motion = .down_choose;
                },
                .none => unreachable,
            }
        }
    }
    pub fn beginAbort(self: *Heap) void {
        self.aborting = true;
        self.comparison = null;
        self.pending_key = null;
        self.states = &.{};
    }
    fn retireArray(self: *Heap, comptime T: type, array: *[]T, original: ?admission.Port) !bool {
        try checkpoint(original);
        const bytes = std.mem.sliceAsBytes(array.*);
        const n = @min(bytes.len - self.retire_offset, 16 * 1024);
        @memset(bytes[self.retire_offset..][0..n], undefined);
        self.retire_offset += n;
        if (self.retire_offset != bytes.len) return false;
        if (bytes.len != 0) self.allocator.rawFree(bytes, .of(T), @returnAddress());
        array.* = &.{};
        self.retire_offset = 0;
        self.retire_slot += 1;
        return self.retire_slot == 4;
    }
    pub fn retireStep(self: *Heap, original: ?admission.Port) !bool {
        self.beginAbort();
        return switch (self.retire_slot) {
            0 => self.retireArray(usize, &self.positions, original),
            1 => self.retireArray(usize, &self.sources, original),
            2 => self.retireArray(usize, &self.advanced_sources, original),
            3 => self.retireArray(State.EntryCursor, &self.cursors, original),
            else => true,
        };
    }
    pub fn deinit(self: *Heap) void {
        while (!(self.retireStep(null) catch unreachable)) {}
    }
};

const Fixture = struct {
    states: [4]State = @splat(.{}),
    sources: [4]*const State = undefined,
    live: bool = false,
    fn init(self: *Fixture, ordered: bool) !void {
        self.* = .{ .live = true };
        errdefer self.deinit();
        const a = std.testing.allocator;
        const namespace: [32777]u8 = @splat('n');
        for (0..4) |source| {
            var active: @import("state.zig").ActiveMemTable = .{};
            defer active.deinit(a);
            if (source != 3) for ([_]?[]const u8{ null, "", &namespace }) |ns| {
                for (0..4) |ordinal| {
                    if ((ordinal + source) % 3 == 0) continue;
                    var key: [32777]u8 = @splat('k');
                    key[key.len - 1] = @intCast(ordinal);
                    const value = [_]u8{@intCast(source)};
                    const tombstone = source % 2 == 0 and ordinal % 2 == 0;
                    if (ordered and source % 2 == 0) try active.upsert(a, .{ .name = ns }, &key, &value, tombstone) else try self.states[source].upsert(a, .{ .name = ns }, &key, &value, tombstone);
                }
            };
            if (ordered and source % 2 == 0) self.states[source] = try active.toStateMove(a);
            self.sources[source] = &self.states[source];
        }
    }
    fn deinit(self: *Fixture) void {
        if (!self.live) return;
        for (&self.states) |*state| state.deinit(std.testing.allocator);
        self.live = false;
    }
    const Probe = struct {
        heap: *const Heap,
        calls: usize = 0,
        target: ?usize = null,
        failure: anyerror = error.Canceled,
        rejected: bool = false,
        progress_only: bool = false,
        saved: ?struct { part: usize, offset: usize, bytes: usize } = null,
        fn check(raw: *anyopaque) !void {
            const self: *Probe = @ptrCast(@alignCast(raw));
            if (self.saved) |saved| {
                const comparison = self.heap.comparison orelse return error.TestLostComparison;
                try std.testing.expect(comparison.part == saved.part and comparison.offset == saved.offset and comparison.compared_bytes == saved.bytes);
                self.saved = null;
            }
            const at = self.calls;
            self.calls += 1;
            const progressed = if (self.heap.comparison) |comparison| comparison.offset >= Comparison.quantum else false;
            if (!self.rejected and (self.target == at or (self.progress_only and progressed))) {
                self.rejected = true;
                if (self.heap.comparison) |comparison| self.saved = .{ .part = comparison.part, .offset = comparison.offset, .bytes = comparison.compared_bytes };
                return self.failure;
            }
        }
        fn payload(self: *Probe) admission.Payload {
            return .{ .ptr = self, .vtable = &.{ .checkpoint = check } };
        }
    };
    fn order(a: table.Entry, b: table.Entry) std.math.Order {
        if (a.namespace_name == null and b.namespace_name != null) return .lt;
        if (a.namespace_name != null and b.namespace_name == null) return .gt;
        if (a.namespace_name) |name| {
            const ns = std.mem.order(u8, name, b.namespace_name.?);
            if (ns != .eq) return ns;
        }
        return std.mem.order(u8, a.key, b.key);
    }
    fn run(self: *Fixture, target: ?usize) !usize {
        var heap = try Heap.prepare(std.testing.allocator, &self.sources);
        defer heap.deinit();
        var probe: Probe = .{ .heap = &heap, .target = target, .failure = switch ((target orelse 0) % 4) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            2 => error.ParentMemoryBusy,
            else => error.FileNotFound,
        } };
        const payload = probe.payload();
        const original = admission.Port.init(&payload);
        while (true) {
            heap.initializeWithAdmission(original) catch |cause| {
                try std.testing.expectError(probe.failure, @as(anyerror!void, cause));
                continue;
            };
            break;
        }
        var total: usize = 0;
        var expected_unique: usize = 0;
        for (&self.states, 0..) |*state, source| {
            total += state.entryCount();
            for (0..state.entryCount()) |rank| {
                const row = state.entryAt(rank);
                var earlier = false;
                for (self.states[0..source]) |*prior| if (prior.findIndex(.{ .name = row.namespace_name }, row.key) != null) {
                    earlier = true;
                    break;
                };
                if (!earlier) expected_unique += 1;
            }
        }
        var consumed: usize = 0;
        var emitted: usize = 0;
        var previous: ?table.Entry = null;
        while (true) {
            const winner = heap.peekWithAdmission(original) catch |cause| {
                try std.testing.expectError(probe.failure, @as(anyerror!void, cause));
                continue;
            } orelse break;
            if (previous) |prior| try std.testing.expect(order(prior, winner.entry) == .lt);
            var copies: usize = 0;
            var first: ?usize = null;
            for (&self.states, 0..) |*state, source| if (state.findIndex(.{ .name = winner.entry.namespace_name }, winner.entry.key)) |rank| {
                copies += 1;
                if (first == null) {
                    first = source;
                    const row = state.entryAt(rank);
                    try std.testing.expectEqualSlices(u8, row.value, winner.entry.value);
                    try std.testing.expectEqual(row.tombstone, winner.entry.tombstone);
                }
            };
            try std.testing.expectEqual(first.?, winner.source);
            while (true) {
                const count = heap.advanceWithAdmission(winner.entry, original) catch |cause| {
                    try std.testing.expectError(probe.failure, @as(anyerror!usize, cause));
                    if (heap.pending_key != null) try std.testing.expectError(error.InvalidArgument, heap.advanceWithAdmission(.{ .key = "different", .value = "" }, null));
                    continue;
                };
                try std.testing.expectEqual(copies, count);
                consumed += count;
                break;
            }
            previous = winner.entry;
            emitted += 1;
        }
        try std.testing.expectEqual(total, consumed);
        try std.testing.expectEqual(expected_unique, emitted);
        for (&self.states, 0..) |*state, source| try std.testing.expectEqual(state.entryCount(), heap.positions[source]);
        while (true) {
            const done = heap.retireStep(original) catch |cause| {
                try std.testing.expectError(probe.failure, @as(anyerror!bool, cause));
                continue;
            };
            if (done) break;
        }
        try std.testing.expectEqual(target != null, probe.rejected);
        return probe.calls;
    }
    fn allocationCase(a: std.mem.Allocator, self: *Fixture) !void {
        var heap = try Heap.init(a, &self.sources);
        defer heap.deinit();
        while (heap.peekSource()) |source| _ = try heap.advanceTopSourcesAtKey(heap.currentEntry(source));
    }
};

test "controlled physical sync flush state merge matches independent newest row oracle for flat and ordered sources" {
    for ([_]bool{ false, true }) |ordered| {
        var fixture: Fixture = undefined;
        try fixture.init(ordered);
        defer fixture.deinit();
        _ = try fixture.run(null);
    }
}

test "controlled physical sync flush state merge retains every original comparison heap and retirement refusal" {
    var fixture: Fixture = undefined;
    try fixture.init(true);
    defer fixture.deinit();
    const count = try fixture.run(null);
    for (0..count) |at| _ = try fixture.run(at);
}

test "controlled physical sync flush state merge initialization frees every allocation failure" {
    var fixture: Fixture = undefined;
    try fixture.init(true);
    defer fixture.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocationCase, .{&fixture});
}

test "controlled physical sync flush state merge abort drops input before bounded array retirement" {
    var fixture: Fixture = undefined;
    try fixture.init(true);
    defer fixture.deinit();
    var heap = try Heap.allocate(std.testing.allocator, &fixture.sources);
    defer heap.deinit();
    var probe: Fixture.Probe = .{ .heap = &heap, .progress_only = true };
    const payload = probe.payload();
    const original = admission.Port.init(&payload);
    try std.testing.expectError(error.Canceled, heap.initializeWithAdmission(original));
    try std.testing.expect(heap.comparison != null and heap.comparison.?.offset >= Comparison.quantum);
    heap.beginAbort();
    fixture.deinit();
    probe.saved = null;
    probe.progress_only = false;
    try std.testing.expectEqual(@as(usize, 0), heap.states.len);
    try std.testing.expectError(error.InvalidArgument, heap.initializeWithAdmission(original));
    while (!(try heap.retireStep(original))) {}
}

test "controlled physical sync flush state merge retains partial allocations for retry and exhausted abort" {
    var fixture: Fixture = undefined;
    try fixture.init(true);
    defer fixture.deinit();
    for ([_]bool{ false, true }) |retry| for (0..4) |at| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = at });
        var heap = try Heap.prepare(failing.allocator(), &fixture.sources);
        defer heap.deinit();
        try std.testing.expectError(error.OutOfMemory, heap.initializeWithAdmission(null));
        try std.testing.expect(failing.has_induced_failure);
        const positions = heap.positions.ptr;
        const sources = heap.sources.ptr;
        const advanced = heap.advanced_sources.ptr;
        if (retry) {
            failing.fail_index = std.math.maxInt(usize);
            try heap.initializeWithAdmission(null);
            if (at > 0) try std.testing.expect(positions == heap.positions.ptr);
            if (at > 1) try std.testing.expect(sources == heap.sources.ptr);
            if (at > 2) try std.testing.expect(advanced == heap.advanced_sources.ptr);
            while (heap.peekSource()) |source| _ = try heap.advanceTopSourcesAtKey(heap.currentEntry(source));
        }
        while (!(try heap.retireStep(null))) {}
    };
    var empty = try Heap.init(std.testing.allocator, &.{});
    defer empty.deinit();
    try std.testing.expect(empty.peekSource() == null);
}
