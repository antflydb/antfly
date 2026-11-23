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

//! Byte-bounded sorting of private variable-length records. Multiway carries
//! preserve logical run coordinates; payloads are copied in bounded windows.
const std = @import("std");
const Run = @import("postings_run.zig").Run;
const Scratch = @import("segment_source.zig").Scratch;
const Allocator = std.mem.Allocator;

pub const Options = struct {
    io: std.Io,
    cancellation: ?@import("antfly_cancellation").CancellationToken = null,
    directory: []const u8,
    resource_manager: ?*@import("storage/resource_manager.zig").ResourceManager = null,
    max_input_bytes: usize = std.math.maxInt(usize),
    chunk_bytes: usize = 256 * 1024,
    chunk_records: usize = 1024,
    fan_in: usize = 4,
    /// Order equal numeric keys by their complete payload, with bounded reads
    /// during merge. Hash prefixes are navigation hints, never full identity.
    lexical_payload_ties: bool = false,
    /// Existing postings callers normalize cancellation; owner validation
    /// must preserve fallible lease/deadline errors through long merge loops.
    fallible_cancellation: bool = false,
};
pub const Record = struct { key: u64, payload: []const u8 };
pub const Range = struct { start: usize, end: usize, count: usize };

/// A work unit performs one fixed-window comparison or payload copy. Numeric
/// head selection scans at most eight entries. No callback survives a turn.
pub const WorkBudget = struct {
    units: usize = 64,
    time_ns: u64 = 5 * std.time.ns_per_ms,
    io: ?std.Io = null,
    pub fn validate(self: @This()) !void {
        if (self.units == 0 or self.units > 1024 or self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidData;
    }
};

/// Read ranges stay live until the complete output is sealed. Logical views
/// remain valid after subsequent compaction, not during its exclusive moves.
pub const MergeJob = struct {
    run: *Run,
    ranges: [8]Range = undefined,
    cursors: [8]Cursor = undefined,
    heads: [8]?RecordView = @splat(null),
    len: usize = 0,
    lexical_payload_ties: bool,
    start: usize,
    count: usize = 0,
    scan: usize = 0,
    selected: ?usize = null,
    compare_offset: usize = 0,
    copying: ?usize = null,
    header_written: bool = false,
    copy_offset: usize = 0,
    output: ?Range = null,
    invalid: bool = false,
    last_units: usize = 0,
    last_copied_bytes: usize = 0,
    last_compared_bytes: usize = 0,

    pub fn init(a: Allocator, run: *Run, ranges: []const Range, lexical_payload_ties: bool) !MergeJob {
        if (ranges.len < 2 or ranges.len > 8) return error.InvalidData;
        var self: MergeJob = .{ .run = run, .lexical_payload_ties = lexical_payload_ties, .start = run.len() };
        errdefer self.deinit();
        for (ranges, 0..) |range, i| {
            if (range.count == 0 or range.end <= range.start or range.end > self.start) return error.InvalidData;
            for (ranges[0..i]) |other| if (range.start < other.end and other.start < range.end) return error.InvalidData;
            self.count = try std.math.add(usize, self.count, range.count);
            self.ranges[i] = range;
            self.cursors[i] = Cursor.init(a, run, range);
            self.len += 1;
            self.heads[i] = try self.cursors[i].nextView();
        }
        return self;
    }
    pub fn deinit(self: *MergeJob) void {
        for (self.cursors[0..self.len]) |*cursor| cursor.deinit();
        self.* = undefined;
    }
    pub fn result(self: *const MergeJob) !Range {
        if (self.invalid) return error.InvalidData;
        return self.output orelse error.InvalidData;
    }
    pub fn step(self: *MergeJob, budget: WorkBudget, control: anytype) !bool {
        try control.check();
        try budget.validate();
        if (self.invalid) return error.InvalidData;
        self.last_units = 0;
        self.last_copied_bytes = 0;
        self.last_compared_bytes = 0;
        if (self.output != null) return true;
        // Accepted work may already have appended output or advanced a cursor.
        // An error cannot be retried as a valid EOF using partial output.
        errdefer self.invalid = true;
        const started = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
        while (self.last_units < budget.units) {
            try control.check();
            const complete = try self.workOne();
            self.last_units += 1;
            if (complete) return true;
            if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - started >= budget.time_ns) break;
        }
        return false;
    }
    fn finishRecord(self: *MergeJob, index: usize) !void {
        self.heads[index] = try self.cursors[index].nextView();
        self.copying = null;
        self.copy_offset = 0;
        self.header_written = false;
        self.selected = null;
        self.scan = 0;
    }
    fn workOne(self: *MergeJob) !bool {
        if (self.copying) |index| {
            const record = self.heads[index].?;
            if (!self.header_written) {
                var header: [16]u8 = undefined;
                std.mem.writeInt(u64, header[0..8], record.key, .little);
                std.mem.writeInt(u64, header[8..16], record.payload.length, .little);
                try self.run.appendSlice(&header);
                self.header_written = true;
                self.last_copied_bytes += 16;
            }
            if (self.copy_offset < record.payload.length) {
                var copied: [4096]u8 = undefined;
                const take: usize = @intCast(@min(copied.len, record.payload.length - self.copy_offset));
                try record.payload.readInto(self.copy_offset, copied[0..take]);
                try self.run.appendSlice(copied[0..take]);
                self.copy_offset += take;
                self.last_copied_bytes += take;
            }
            if (self.copy_offset == record.payload.length) try self.finishRecord(index);
            return false;
        }
        while (self.scan < self.len) {
            const record = self.heads[self.scan] orelse {
                self.scan += 1;
                continue;
            };
            const selected = self.selected orelse {
                self.selected = self.scan;
                self.scan += 1;
                continue;
            };
            const best = self.heads[selected].?;
            if (record.key != best.key or !self.lexical_payload_ties) {
                if (record.key < best.key) self.selected = self.scan;
                self.scan += 1;
                continue;
            }
            const common = @min(record.payload.length, best.payload.length);
            var order: std.math.Order = .eq;
            if (self.compare_offset < common) {
                var left: [4096]u8 = undefined;
                var right: [4096]u8 = undefined;
                const take: usize = @intCast(@min(left.len, common - self.compare_offset));
                try record.payload.readInto(self.compare_offset, left[0..take]);
                try best.payload.readInto(self.compare_offset, right[0..take]);
                self.compare_offset += take;
                self.last_compared_bytes += take * 2;
                order = std.mem.order(u8, left[0..take], right[0..take]);
                if (order == .eq and self.compare_offset < common) return false;
            }
            if (order == .lt or (order == .eq and record.payload.length < best.payload.length)) self.selected = self.scan;
            self.scan += 1;
            self.compare_offset = 0;
            return false;
        }
        if (self.selected) |index| {
            self.copying = index;
            return false;
        }
        try self.run.seal(self.start);
        for (self.ranges[0..self.len]) |range| self.run.releaseRange(range.start);
        self.output = .{ .start = self.start, .end = self.run.len(), .count = self.count };
        return true;
    }
};

pub const Sorter = struct {
    allocator: Allocator,
    run: *Run,
    options: Options,
    chunk: std.ArrayListUnmanaged(Record) = .empty,
    payloads: Scratch,
    chunk_bytes: usize = 0,
    levels: [64][7]?Range = @splat(@splat(null)),
    input_bytes: usize = 0,
    merged_bytes: usize = 0,
    merge_passes: usize = 0,

    pub fn init(allocator: Allocator, options: Options) !Sorter {
        if (options.chunk_bytes == 0 or options.chunk_records == 0 or options.fan_in < 2 or options.fan_in > 8) return error.InvalidData;
        return .{ .allocator = allocator, .options = options, .run = try Run.createWithResources(allocator, options.io, options.directory, options.resource_manager), .payloads = .init(allocator, options.chunk_bytes) };
    }
    pub fn deinit(self: *Sorter) void {
        self.chunk.deinit(self.allocator);
        self.payloads.deinit();
        self.run.deinit();
        self.* = undefined;
    }
    fn check(self: *const Sorter) !void {
        if (self.options.cancellation) |token| {
            if (self.options.fallible_cancellation) try token.check() else if (token.isCancelled()) return error.Cancelled;
        }
    }
    pub fn add(self: *Sorter, key: u64, payload: []const u8) !void {
        try self.check();
        if (payload.len > self.options.max_input_bytes -| self.input_bytes or 16 > self.options.max_input_bytes -| (self.input_bytes +| payload.len)) return error.ResourceBudgetExceeded;
        if (self.chunk.items.len != 0 and (self.chunk.items.len >= self.options.chunk_records or self.chunk_bytes >= self.options.chunk_bytes)) try self.flush();
        const owned = try self.payloads.allocator().dupe(u8, payload);
        try self.chunk.append(self.allocator, .{ .key = key, .payload = owned });
        self.chunk_bytes = try std.math.add(usize, self.chunk_bytes, @sizeOf(Record) + payload.len);
        self.input_bytes = try std.math.add(usize, self.input_bytes, try std.math.add(usize, 16, payload.len));
    }
    fn append(self: *Sorter, record: Record) !void {
        var header: [16]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], record.key, .little);
        std.mem.writeInt(u64, header[8..16], record.payload.len, .little);
        try self.run.appendSlice(&header);
        try self.run.appendSlice(record.payload);
    }
    fn lessThan(payload_ties: bool, a: Record, b: Record) bool {
        return a.key < b.key or (payload_ties and a.key == b.key and std.mem.order(u8, a.payload, b.payload) == .lt);
    }
    fn flush(self: *Sorter) !void {
        if (self.chunk.items.len == 0) return;
        std.sort.pdq(Record, self.chunk.items, self.options.lexical_payload_ties, lessThan);
        const start = self.run.len();
        for (self.chunk.items) |record| try self.append(record);
        try self.run.seal(start);
        var range = Range{ .start = start, .end = self.run.len(), .count = self.chunk.items.len };
        self.chunk.clearRetainingCapacity();
        self.payloads.reset();
        self.chunk_bytes = 0;
        for (&self.levels) |*level| {
            for (level[0 .. self.options.fan_in - 1]) |*slot| {
                if (slot.* == null) {
                    slot.* = range;
                    return;
                }
            }
            var ranges: [8]Range = undefined;
            for (level[0 .. self.options.fan_in - 1], 0..) |*slot, i| {
                ranges[i] = slot.*.?;
                slot.* = null;
            }
            ranges[self.options.fan_in - 1] = range;
            range = try self.merge(ranges[0..self.options.fan_in]);
        }
        return error.Overflow;
    }
    fn merge(self: *Sorter, ranges: []const Range) !Range {
        var job = try MergeJob.init(self.allocator, self.run, ranges, self.options.lexical_payload_ties);
        defer job.deinit();
        const Control = struct {
            sorter: *Sorter,
            pub fn check(control: @This()) !void {
                try control.sorter.check();
            }
        };
        while (!try job.step(.{}, Control{ .sorter = self })) {}
        const result = try job.result();
        self.merged_bytes = try std.math.add(usize, self.merged_bytes, result.end - result.start);
        self.merge_passes += 1;
        try self.run.compact();
        return result;
    }
    pub fn finish(self: *Sorter) !?Range {
        try self.check();
        try self.flush();
        var range: ?Range = null;
        for (&self.levels) |*level| {
            var ranges: [8]Range = undefined;
            var count: usize = 0;
            for (level[0 .. self.options.fan_in - 1]) |*slot| if (slot.*) |previous| {
                ranges[count] = previous;
                count += 1;
                slot.* = null;
            };
            if (range) |current| {
                ranges[count] = current;
                count += 1;
            }
            range = if (count == 0) null else if (count == 1) ranges[0] else try self.merge(ranges[0..count]);
        }
        return range;
    }
};

/// Shape-bound fixed records make chunk sorting itself bounded: at most
/// 1024 records of at most 64 bytes. Carries, full-payload comparisons, output
/// copies and file/descriptor compaction are independently resumable.
/// No request cancellation token is retained by this long-lived job.
pub fn FixedSorter(comptime width: usize) type {
    if (width < 8 or width > 64) @compileError("fixed spill records must be 8..64 bytes");
    return struct {
        const Phase = enum { input, sort, write, carry, merge, compact, gather, done };
        base: Sorter,
        records: [1024][width]u8 = undefined,
        len: usize = 0,
        limit: usize,
        phase: Phase = .input,
        finishing: bool = false,
        invalid: bool = false,
        written: usize = 0,
        chunk_start: usize = 0,
        pending_range: ?Range = null,
        carry_level: usize = 0,
        gather_level: usize = 0,
        final_range: ?Range = null,
        merge_job: ?MergeJob = null,
        after_merge: Phase = .carry,
        last_units: usize = 0,
        last_sorted_records: usize = 0,
        last_compaction_bytes: usize = 0,

        pub fn init(a: Allocator, options: Options) !@This() {
            var owned_options = options;
            owned_options.cancellation = null;
            owned_options.lexical_payload_ties = true;
            const base = try Sorter.init(a, owned_options);
            return .{ .base = base, .limit = @min(1024, @min(options.chunk_records, @max(1, options.chunk_bytes / (width + @sizeOf(Record))))) };
        }
        pub fn deinit(self: *@This()) void {
            if (self.merge_job) |*job| job.deinit();
            self.base.deinit();
            self.* = undefined;
        }
        pub fn tryAdd(self: *@This(), record: [width]u8, control: anytype) !bool {
            try control.check();
            if (self.invalid or self.finishing) return error.InvalidData;
            if (self.phase != .input) return false;
            if (width + 16 > self.base.options.max_input_bytes -| self.base.input_bytes) return error.ResourceBudgetExceeded;
            if (self.len == self.limit) {
                self.phase = .sort;
                return false;
            }
            self.records[self.len] = record;
            self.len += 1;
            self.base.input_bytes += width + 16;
            return true;
        }
        pub fn add(self: *@This(), record: [width]u8, control: anytype) !void {
            while (!try self.tryAdd(record, control)) _ = try self.step(.{}, control);
        }
        fn less(_: void, a: [width]u8, b: [width]u8) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
        pub fn step(self: *@This(), budget: WorkBudget, control: anytype) !bool {
            try control.check();
            try budget.validate();
            if (self.invalid) return error.InvalidData;
            self.last_units = 0;
            self.last_sorted_records = 0;
            self.last_compaction_bytes = 0;
            if (self.phase == .done or (self.phase == .input and !self.finishing)) return true;
            errdefer self.invalid = true;
            const started = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
            while (self.last_units < budget.units) {
                try control.check();
                try self.workOne(control);
                self.last_units += 1;
                if (self.phase == .done or (self.phase == .input and !self.finishing)) return true;
                if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - started >= budget.time_ns) break;
            }
            return false;
        }
        pub fn finishStep(self: *@This(), budget: WorkBudget, control: anytype) !bool {
            self.finishing = true;
            _ = try self.step(budget, control);
            return self.phase == .done;
        }
        pub fn result(self: *const @This()) !?Range {
            if (self.invalid or self.phase != .done) return error.InvalidData;
            return self.final_range;
        }
        pub fn finish(self: *@This(), control: anytype) !?Range {
            while (!try self.finishStep(.{}, control)) {}
            return self.result();
        }
        fn beginMerge(self: *@This(), ranges: []const Range, after: Phase) !void {
            self.merge_job = try MergeJob.init(self.base.allocator, self.base.run, ranges, true);
            self.after_merge = after;
            self.phase = .merge;
        }
        fn workOne(self: *@This(), control: anytype) !void {
            switch (self.phase) {
                .input => self.phase = if (self.len == 0) .gather else .sort,
                .sort => {
                    std.sort.pdq([width]u8, self.records[0..self.len], {}, less);
                    self.last_sorted_records += self.len;
                    self.written = 0;
                    self.chunk_start = self.base.run.len();
                    self.phase = .write;
                },
                .write => {
                    const record = &self.records[self.written];
                    try self.base.append(.{ .key = std.mem.readInt(u64, record[0..8], .big), .payload = record });
                    self.written += 1;
                    if (self.written == self.len) {
                        try self.base.run.seal(self.chunk_start);
                        self.pending_range = .{ .start = self.chunk_start, .end = self.base.run.len(), .count = self.len };
                        self.len = 0;
                        self.carry_level = 0;
                        self.phase = .carry;
                    }
                },
                .carry => {
                    if (self.carry_level == self.base.levels.len) return error.Overflow;
                    const level = &self.base.levels[self.carry_level];
                    const slots = level[0 .. self.base.options.fan_in - 1];
                    for (slots) |*slot| if (slot.* == null) {
                        slot.* = self.pending_range.?;
                        self.pending_range = null;
                        self.phase = if (self.finishing) .gather else .input;
                        return;
                    };
                    var ranges: [8]Range = undefined;
                    for (slots, 0..) |slot, i| ranges[i] = slot.?;
                    ranges[slots.len] = self.pending_range.?;
                    try self.beginMerge(ranges[0..self.base.options.fan_in], .carry);
                    @memset(slots, null);
                    self.pending_range = null;
                    self.carry_level += 1;
                },
                .merge => {
                    const job = &self.merge_job.?;
                    if (!try job.step(.{ .units = 1 }, control)) return;
                    const range = try job.result();
                    self.base.merged_bytes = try std.math.add(usize, self.base.merged_bytes, range.end - range.start);
                    self.base.merge_passes += 1;
                    job.deinit();
                    self.merge_job = null;
                    if (self.after_merge == .carry) self.pending_range = range else self.final_range = range;
                    self.phase = .compact;
                },
                .compact => {
                    const complete = try self.base.run.compactStep(4096);
                    self.last_compaction_bytes += self.base.run.compactionWorkBytes();
                    if (complete) self.phase = self.after_merge;
                },
                .gather => {
                    if (self.gather_level == self.base.levels.len) {
                        self.phase = .done;
                        return;
                    }
                    const level = &self.base.levels[self.gather_level];
                    const slots = level[0 .. self.base.options.fan_in - 1];
                    var ranges: [8]Range = undefined;
                    var count: usize = 0;
                    for (slots) |slot| if (slot) |range| {
                        ranges[count] = range;
                        count += 1;
                    };
                    if (self.final_range) |range| {
                        ranges[count] = range;
                        count += 1;
                    }
                    if (count > 1) try self.beginMerge(ranges[0..count], .gather) else self.final_range = if (count == 0) null else ranges[0];
                    @memset(slots, null);
                    self.gather_level += 1;
                },
                .done => {},
            }
        }
    };
}

pub const RecordView = struct { key: u64, payload: @import("segment_source.zig").View };

pub const Cursor = struct {
    run: *Run,
    range: Range,
    position: usize,
    remaining: usize,
    payloads: Scratch,
    pub fn init(allocator: Allocator, run: *Run, range: Range) Cursor {
        return .{ .run = run, .range = range, .position = range.start, .remaining = range.count, .payloads = .init(allocator, 128 * 1024) };
    }
    pub fn deinit(self: *Cursor) void {
        self.payloads.deinit();
        self.* = undefined;
    }
    pub fn nextView(self: *Cursor) !?RecordView {
        if (self.remaining == 0) {
            if (self.position != self.range.end) return error.InvalidData;
            return null;
        }
        if (self.position > self.range.end or self.range.end - self.position < 16) return error.InvalidData;
        var header: [16]u8 = undefined;
        const view = try self.run.sealedView();
        try view.readInto(self.position, &header);
        self.position += 16;
        const length = std.math.cast(usize, std.mem.readInt(u64, header[8..16], .little)) orelse return error.InvalidData;
        if (length > self.range.end - self.position) return error.InvalidData;
        const payload = try @import("segment_source.zig").View.init(view.source, view.offset + self.position, length);
        self.position += length;
        self.remaining -= 1;
        return .{ .key = std.mem.readInt(u64, header[0..8], .little), .payload = payload };
    }
    pub fn next(self: *Cursor) !?Record {
        self.payloads.reset();
        const record = (try self.nextView()) orelse return null;
        const payload = try self.payloads.allocator().alloc(u8, @intCast(record.payload.length));
        try record.payload.readInto(0, payload);
        return .{ .key = record.key, .payload = payload };
    }
};

test "spill sort incremental merge bounds long full-payload tie comparisons and copies" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("spill-merge-steps");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    var sort = try Sorter.init(a, .{ .io = std.testing.io, .directory = directory.path(), .lexical_payload_ties = true });
    defer sort.deinit();
    const payload = try a.alloc(u8, 16 * 1024 + 7);
    defer a.free(payload);
    @memset(payload, 'k');
    var ranges: [2]Range = undefined;
    const suffixes = [_][2]u8{ .{ 'a', 'z' }, .{ 'b', 'y' } };
    for (suffixes, 0..) |suffix, i| {
        const start = sort.run.len();
        for (suffix) |last| {
            payload[payload.len - 1] = last;
            try sort.append(.{ .key = 7, .payload = payload });
        }
        try sort.run.seal(start);
        ranges[i] = .{ .start = start, .end = sort.run.len(), .count = 2 };
    }
    try std.testing.expectError(error.InvalidData, MergeJob.init(a, sort.run, &.{ ranges[0], ranges[0] }, true));
    var job = try MergeJob.init(a, sort.run, &ranges, true);
    defer job.deinit();
    const control = @import("antfly_cancellation").CancellationToken.none;
    var turns: usize = 0;
    while (true) {
        turns += 1;
        if (turns > 100) return error.TestUnexpectedResult;
        const complete = try job.step(.{ .units = 1 }, control);
        try std.testing.expect(job.last_units <= 1);
        try std.testing.expect(job.last_compared_bytes <= 8192);
        try std.testing.expect(job.last_copied_bytes <= 4096 + 16);
        if (complete) break;
        try std.testing.expectError(error.InvalidData, job.result());
        try std.testing.expectEqual(@as(usize, 0), sort.run.dead_extents);
    }
    try std.testing.expect(turns > 16);
    const output = try job.result();
    try std.testing.expectEqual(@as(usize, 4), output.count);
    try std.testing.expectEqual(@as(usize, 2), sort.run.dead_extents);
    try std.testing.expectEqual(output.end - output.start, sort.run.live_bytes);
    var cursor = Cursor.init(a, sort.run, output);
    defer cursor.deinit();
    for ("abyz") |expected| {
        const row = (try cursor.nextView()).?;
        try std.testing.expectEqual(@as(u64, 7), row.key);
        var suffix: [1]u8 = undefined;
        try row.payload.readInto(payload.len - 1, &suffix);
        try std.testing.expectEqual(expected, suffix[0]);
    }
    try std.testing.expect(try cursor.nextView() == null);
}

test "spill sort incremental merge cancellation never seals partial output or retires inputs" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("spill-merge-cancel");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    var sort = try Sorter.init(a, .{ .io = std.testing.io, .directory = directory.path() });
    defer sort.deinit();
    const payload: [16 * 1024]u8 = @splat(4);
    var ranges: [2]Range = undefined;
    for (&ranges, 0..) |*range, i| {
        const start = sort.run.len();
        try sort.append(.{ .key = @intCast(i), .payload = &payload });
        try sort.run.seal(start);
        range.* = .{ .start = start, .end = sort.run.len(), .count = 1 };
    }
    const original_length = sort.run.len();
    var job = try MergeJob.init(a, sort.run, &ranges, false);
    defer job.deinit();
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, job.step(.{}, @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled)));
    try std.testing.expect(!job.invalid);
    try std.testing.expectEqual(original_length, sort.run.len());
    const control = @import("antfly_cancellation").CancellationToken.none;
    try std.testing.expect(!try job.step(.{ .units = 1 }, control)); // selection
    const Fail = struct {
        calls: usize = 0,
        pub fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls >= 3) return error.InjectedLeaseLost;
        }
    };
    var failure: Fail = .{};
    try std.testing.expectError(error.InjectedLeaseLost, job.step(.{}, &failure));
    try std.testing.expect(job.invalid);
    try std.testing.expect(sort.run.len() > original_length);
    try std.testing.expectEqual(@as(usize, 0), sort.run.dead_extents);
    try std.testing.expectError(error.InvalidData, job.result());
    try std.testing.expectError(error.InvalidData, job.step(.{}, control));
}

test "spill sort incremental fixed cohorts yield through carries and physical compaction" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("spill-fixed-steps");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    var sort = try FixedSorter(33).init(a, .{ .io = std.testing.io, .directory = directory.path(), .chunk_records = 7, .chunk_bytes = 1024, .fan_in = 3 });
    defer sort.deinit();
    const control = @import("antfly_cancellation").CancellationToken.none;
    var turns: usize = 0;
    const budget: WorkBudget = .{ .units = 8 };
    for (0..5000) |i| {
        var record: [33]u8 = @splat(0);
        std.mem.writeInt(u64, record[0..8], 7, .big);
        std.mem.writeInt(u64, record[24..32], @intCast(4999 - i), .big);
        record[32] = 1;
        while (!try sort.tryAdd(record, control)) {
            _ = try sort.step(budget, control);
            turns += 1;
            try std.testing.expect(sort.last_units <= budget.units);
            try std.testing.expect(sort.last_sorted_records <= 1024);
            try std.testing.expect(sort.last_compaction_bytes <= budget.units * 4096);
        }
    }
    while (!try sort.finishStep(budget, control)) {
        turns += 1;
        if (turns > 100_000) return error.TestUnexpectedResult;
        try std.testing.expect(sort.last_units <= budget.units);
        try std.testing.expect(sort.last_sorted_records <= 1024);
        try std.testing.expect(sort.last_compaction_bytes <= budget.units * 4096);
        try std.testing.expectError(error.InvalidData, sort.result());
    }
    const range = (try sort.result()).?;
    try std.testing.expectEqual(@as(usize, 5000), range.count);
    try std.testing.expectEqual(@as(usize, 5000 * 49), sort.base.input_bytes);
    try std.testing.expect(turns > 1000);
    try std.testing.expect(sort.base.merge_passes > 3);
    try std.testing.expect(sort.base.run.reclaimed_bytes >= 1024 * 1024);
    try std.testing.expect(sort.base.run.physical_peak <= 2 * 1024 * 1024 + 4 * 5000 * 49);
    var cursor = Cursor.init(a, sort.base.run, range);
    defer cursor.deinit();
    for (0..5000) |i| {
        const row = (try cursor.nextView()).?;
        try std.testing.expectEqual(@as(u64, 7), row.key);
        var record: [33]u8 = undefined;
        try row.payload.readInto(0, &record);
        try std.testing.expectEqual(@as(u64, @intCast(i)), std.mem.readInt(u64, record[24..32], .big));
        try std.testing.expectEqual(@as(u8, 1), record[32]);
    }
    try std.testing.expect(try cursor.nextView() == null);
}

test "spill sort incremental fixed jobs retain no cancellation source and never qualify failed work" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("spill-fixed-cancel");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    var signal = std.atomic.Value(bool).init(true);
    const token = @import("antfly_cancellation").CancellationToken.fromAtomic(&signal);
    var sort = try FixedSorter(33).init(a, .{ .io = std.testing.io, .directory = directory.path(), .chunk_records = 1, .cancellation = token });
    defer sort.deinit();
    try std.testing.expect(sort.base.options.cancellation == null);
    const control = @import("antfly_cancellation").CancellationToken.none;
    try std.testing.expect(try sort.tryAdd(@splat(1), control));
    try std.testing.expect(!try sort.tryAdd(@splat(2), control));
    try std.testing.expectError(error.Canceled, sort.step(.{}, token));
    try std.testing.expect(!sort.invalid);
    try std.testing.expect(!try sort.step(.{ .units = 1 }, control)); // bounded chunk sort
    const Fail = struct {
        calls: usize = 0,
        pub fn check(self: *@This()) !void {
            self.calls += 1;
            if (self.calls >= 3) return error.InjectedLeaseLost;
        }
    };
    var failure: Fail = .{};
    try std.testing.expectError(error.InjectedLeaseLost, sort.step(.{}, &failure));
    try std.testing.expect(sort.invalid);
    try std.testing.expectError(error.InvalidData, sort.result());
    try std.testing.expectError(error.InvalidData, sort.finishStep(.{}, control));
}

test "spill sort incremental fixed jobs clean every failed allocation" {
    if (comptime @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var directory = try @import("common/test_directory.zig").TestDirectory.init("spill-fixed-alloc");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const Check = struct {
        fn run(a: Allocator, path: []const u8) !void {
            var sort = try FixedSorter(33).init(a, .{ .io = std.testing.io, .directory = path, .chunk_records = 2, .fan_in = 2 });
            defer sort.deinit();
            const control = @import("antfly_cancellation").CancellationToken.none;
            for (0..10) |i| {
                var record: [33]u8 = @splat(0);
                std.mem.writeInt(u64, record[24..32], @intCast(9 - i), .big);
                try sort.add(record, control);
            }
            _ = try sort.finish(control);
        }
    };
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{directory.path()});
}
