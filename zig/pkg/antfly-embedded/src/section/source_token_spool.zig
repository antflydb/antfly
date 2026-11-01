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

//! Compact replay storage for one analyzed field. Terms are interned once;
//! spans/positions append as fixed native records to the admitted private run.
const std = @import("std");
const analysis = @import("../search/analysis.zig");
const segment = @import("../segment.zig");
const Run = @import("../postings_run.zig").Run;
const View = @import("../segment_source.zig").View;
const A = std.mem.Allocator;
pub const Spool = struct {
    const Term = struct { text: []const u8, count: u32 = 0 };
    a: A,
    io: ?std.Io,
    directory: []const u8,
    resources: ?*@import("../storage/resource_manager.zig").ResourceManager,
    max_memory_bytes: usize = 256 * 1024,
    run: ?*Run = null,
    memory: segment.MemorySegmentSink,
    ids: std.StringHashMapUnmanaged(u32) = .empty,
    terms: std.ArrayList(Term) = .empty,
    ordered: bool = true,
    disjoint: bool = true,
    previous: ?struct { start: u32, end: u32 } = null,
    group_memory_records: usize = 16 * 1024,
    grouping_read_calls: usize = 0,
    group_buffer_allocations: usize = 0,
    count: usize = 0,
    view: ?View = null,
    pub fn init(a: A, io: ?std.Io, directory: []const u8, resources: ?*@import("../storage/resource_manager.zig").ResourceManager) Spool {
        const work = @import("../sql/memory_budget.zig").reclaimingAllocator(a);
        const allowance = if (@import("../sql/memory_budget.zig").headroom(work)) |bytes| @min(4 * 1024 * 1024, bytes / 8) else 256 * 1024;
        return .{ .a = work, .io = io, .directory = directory, .resources = resources, .max_memory_bytes = allowance, .memory = .init(work) };
    }
    pub fn deinit(self: *Spool) void {
        self.ids.deinit(self.a);
        for (self.terms.items) |term| self.a.free(term.text);
        self.terms.deinit(self.a);
        if (self.run) |run| run.deinit();
        self.memory.deinit();
    }
    pub fn append(self: *Spool, token: analysis.Token) !void {
        if (self.view != null or self.count == std.math.maxInt(u32) or token.start_byte > token.end_byte) return error.InvalidData;
        const id = self.ids.get(token.term) orelse intern: {
            const owned = try self.a.dupe(u8, token.term);
            errdefer self.a.free(owned);
            try self.terms.append(self.a, .{ .text = owned });
            errdefer _ = self.terms.pop();
            const value: u32 = @intCast(self.terms.items.len - 1);
            try self.ids.put(self.a, owned, value);
            break :intern value;
        };
        // Keep a bounded compact prefix in memory, then move it once into the
        // admitted run. Small fields avoid file creation and page-cache reads.
        if (self.run == null and self.memory.out.items.len >= self.max_memory_bytes) if (self.io) |io| {
            const run = try Run.createWithResources(self.a, io, self.directory, self.resources);
            errdefer run.deinit();
            var sink = run.sink();
            try sink.appendSlice(self.memory.out.items);
            self.run = run;
            self.memory.deinit();
            self.memory = .init(self.a);
        };
        var record: [16]u8 = undefined;
        std.mem.writeInt(u32, record[0..4], id, .little);
        std.mem.writeInt(u32, record[4..8], token.start_byte, .little);
        std.mem.writeInt(u32, record[8..12], token.end_byte, .little);
        std.mem.writeInt(u32, record[12..16], token.position, .little);
        var sink = if (self.run) |run| run.sink() else self.memory.sink();
        try sink.appendSlice(&record);
        self.count += 1;
        self.terms.items[id].count += 1;
        if (self.previous) |prior| {
            if (token.start_byte < prior.start or (token.start_byte == prior.start and token.end_byte > prior.end)) self.ordered = false;
            if (token.start_byte <= prior.end) self.disjoint = false;
        }
        self.previous = .{ .start = token.start_byte, .end = token.end_byte };
    }
    pub const Input = struct {
        spool: *Spool,
        count: usize,
        pub fn at(self: Input, index: usize) !analysis.Token {
            return self.spool.read(index);
        }
        pub fn cursor(self: Input, a: A) Cursor {
            return .{ .spool = self.spool, .raw = RawCursor.init(a, .{ .view = self.spool.view.?, .count = self.count }) };
        }
        pub fn prepareOffsets(self: Input, a: A) !Prepared {
            return Prepared.init(self.spool, a);
        }
    };
    pub fn input(self: *Spool) !Input {
        if (self.view == null) {
            if (self.run) |run| {
                try run.seal(0);
                self.view = try run.sealedView();
            } else self.view = try View.init(.{ .contiguous = self.memory.out.items }, 0, self.memory.out.items.len);
        }
        return .{ .spool = self, .count = self.count };
    }
    fn read(self: *Spool, index: usize) !analysis.Token {
        if (index >= self.count) return error.InvalidData;
        const offset = try std.math.mul(usize, index, 16);
        var buffer: [16]u8 = undefined;
        const record = if (self.run == null) self.memory.out.items[offset..][0..16] else read: {
            try self.view.?.readInto(offset, &buffer);
            break :read @as(*const [16]u8, &buffer);
        };
        return self.decodeRecord(record);
    }
    // Memory-only targets retain the same cursor contract without native run
    // instrumentation or file layout fields.
    fn readCalls(run: *Run) usize {
        if (comptime @hasField(Run, "read_calls")) return run.read_calls;
        return 0;
    }
    fn decodeRecord(self: *Spool, record: *const [16]u8) !analysis.Token {
        const id = std.mem.readInt(u32, record[0..4], .little);
        if (id >= self.terms.items.len) return error.InvalidData;
        return .{ .term = self.terms.items[id].text, .start_byte = std.mem.readInt(u32, record[4..8], .little), .end_byte = std.mem.readInt(u32, record[8..12], .little), .position = std.mem.readInt(u32, record[12..16], .little) };
    }
    const Range = struct { offset: usize, bytes: usize };
    const RecordSource = struct {
        view: View,
        count: usize,
        begin: usize = 0,
        ranges: []const Range = &.{},
    };
    /// Fixed records are read in bounded blocks. Memory sources are borrowed;
    /// file sources own only the current block, never one read per occurrence.
    const RawCursor = struct {
        a: A,
        source: RecordSource,
        range_index: usize = 0,
        range_position: usize = 0,
        block: []const u8 = &.{},
        at: usize = 0,
        buffer: []u8 = &.{},
        buffer_allocations: usize = 0,
        fn init(a: A, source: RecordSource) RawCursor {
            return .{ .a = @import("../sql/memory_budget.zig").reclaimingAllocator(a), .source = source };
        }
        fn deinit(self: *RawCursor) void {
            self.a.free(self.buffer);
        }
        fn reposition(self: *RawCursor, source: RecordSource) void {
            self.source = source;
            self.range_index = 0;
            self.range_position = 0;
            self.block = &.{};
            self.at = 0;
        }
        fn nextBlock(self: *RawCursor) !?[]const u8 {
            while (true) {
                const range = if (self.source.ranges.len != 0) blk: {
                    if (self.range_index == self.source.ranges.len) return null;
                    break :blk self.source.ranges[self.range_index];
                } else if (self.range_index == 0) Range{ .offset = self.source.begin, .bytes = self.source.count * 16 } else return null;
                if (self.range_position == range.bytes) {
                    self.range_index += 1;
                    self.range_position = 0;
                    continue;
                }
                const n = @min(64 * 1024, range.bytes - self.range_position);
                if (n % 16 != 0) return error.InvalidData;
                const offset = range.offset + self.range_position;
                self.range_position += n;
                if (self.source.view.source == .contiguous) {
                    const start: usize = @intCast(self.source.view.offset + offset);
                    return self.source.view.source.contiguous[start..][0..n];
                }
                if (self.buffer.len < n) {
                    self.a.free(self.buffer);
                    self.buffer = &.{};
                    self.buffer = try self.a.alloc(u8, n);
                    self.buffer_allocations += 1;
                }
                try self.source.view.readInto(offset, self.buffer[0..n]);
                return self.buffer[0..n];
            }
        }
        fn next(self: *RawCursor) !?*const [16]u8 {
            if (self.at == self.block.len) {
                self.block = (try self.nextBlock()) orelse return null;
                self.at = 0;
            }
            const record = self.block[self.at..][0..16];
            self.at += 16;
            return record;
        }
    };
    pub const Cursor = struct {
        spool: *Spool,
        raw: RawCursor,
        term_id: u32 = 0,
        pub fn deinit(self: *Cursor) void {
            self.raw.deinit();
        }
        pub fn next(self: *Cursor) !?analysis.Token {
            const record = (try self.raw.next()) orelse return null;
            self.term_id = std.mem.readInt(u32, record[0..4], .little);
            return try self.spool.decodeRecord(record);
        }
    };
    /// The encoder consumes the same term order and bytes as TokenSlice. Only
    /// the private occurrence access changes: linked memory or sorted runs.
    pub const Prepared = struct {
        pub const Group = struct { term: []const u8, id: u32, first: u32, last: u32 = 0, count: u32 };
        a: A,
        spool: *Spool,
        count: usize,
        groups: []Group,
        ranks: []u32,
        links: []u32 = &.{},
        ordered: bool,
        disjoint: bool,
        grouped: ?*Run = null,
        view: ?View = null,
        group_reader: ?RawCursor = null,
        group_position: usize = 0,
        group_reader_valid: bool = true,
        group_active: bool = false,
        fn init(spool: *Spool, allocator: A) !Prepared {
            const a = @import("../sql/memory_budget.zig").reclaimingAllocator(allocator);
            const groups = try a.alloc(Group, spool.terms.items.len);
            errdefer a.free(groups);
            const ranks = try a.alloc(u32, groups.len);
            errdefer a.free(ranks);
            for (groups, spool.terms.items, 0..) |*group, term, id| {
                if (term.count == 0) return error.InvalidData;
                group.* = .{ .term = term.text, .id = @intCast(id), .first = 0, .count = term.count };
            }
            std.mem.sort(Group, groups, {}, struct {
                fn less(_: void, l: Group, r: Group) bool {
                    return std.mem.lessThan(u8, l.term, r.term);
                }
            }.less);
            var count: u32 = 0;
            for (groups, 0..) |*group, rank| {
                ranks[group.id] = @intCast(rank);
                group.first = count;
                count = try std.math.add(u32, count, group.count);
            }
            if (count != spool.count) return error.InvalidData;
            var self: Prepared = .{ .a = a, .spool = spool, .count = spool.count, .groups = groups, .ranks = ranks, .ordered = spool.ordered, .disjoint = spool.disjoint };
            // groups/ranks are covered by the outer error defers until success.
            errdefer {
                a.free(self.links);
                if (self.grouped) |run| run.deinit();
            }
            if (spool.run == null) {
                self.links = try a.alloc(u32, spool.count);
                @memset(self.links, std.math.maxInt(u32));
                for (groups) |*group| group.first = std.math.maxInt(u32);
                var cursor = (try spool.input()).cursor(a);
                defer cursor.deinit();
                var at: u32 = 0;
                while (try cursor.next()) |_| : (at += 1) {
                    const group = &groups[ranks[cursor.term_id]];
                    if (group.first == std.math.maxInt(u32)) group.first = at else self.links[group.last] = at;
                    group.last = at;
                }
            } else {
                const run = try Run.createWithResources(a, spool.io.?, spool.directory, spool.resources);
                self.grouped = run;
                run.preferSequentialReads();
                const bits: u6 = if (groups.len < 2) 0 else @intCast(@bitSizeOf(u32) - @clz(@as(u32, @intCast(groups.len - 1))));
                const shift: u5 = if (bits == 0) 0 else @intCast((bits - 1) / 4 * 4);
                try self.partition(.{ .view = spool.view.?, .count = spool.count }, run, 0, groups.len, shift);
                try run.seal(0);
                self.view = try run.sealedView();
            }
            return self;
        }
        pub fn deinit(self: *Prepared) void {
            std.debug.assert(!self.group_active);
            if (self.group_reader) |*reader| {
                self.spool.group_buffer_allocations += reader.buffer_allocations;
                reader.deinit();
            }
            if (self.grouped) |run| {
                self.spool.grouping_read_calls += readCalls(run);
                run.deinit();
            }
            self.a.free(self.links);
            self.a.free(self.ranks);
            self.a.free(self.groups);
        }
        /// Disk groups borrow one owner-scoped cursor. Only one group lease
        /// can be active; returned tokens borrow interned terms, never buffer
        /// bytes. Closing a partial group permits a later explicit seek.
        pub const GroupCursor = struct {
            spool: *Spool,
            remaining: u32,
            at: u32,
            links: []const u32,
            owner: ?*Prepared = null,
            raw: ?*RawCursor = null,
            pub fn deinit(self: *GroupCursor) void {
                if (self.owner) |owner| {
                    std.debug.assert(owner.group_active);
                    owner.group_active = false;
                }
                self.owner = null;
                self.raw = null;
                self.remaining = 0;
            }
            pub fn next(self: *GroupCursor) !?analysis.Token {
                if (self.remaining == 0) return null;
                if (self.owner) |owner| {
                    if (!owner.group_reader_valid) return error.InvalidData;
                    const record = (self.raw.?.next() catch |err| {
                        owner.group_reader_valid = false;
                        return err;
                    }) orelse {
                        owner.group_reader_valid = false;
                        return error.InvalidData;
                    };
                    const token = self.spool.decodeRecord(record) catch |err| {
                        owner.group_reader_valid = false;
                        return err;
                    };
                    self.remaining -= 1;
                    owner.group_position += 1;
                    return token;
                }
                const token = try self.spool.read(self.at);
                self.at = self.links[self.at];
                self.remaining -= 1;
                return token;
            }
        };
        pub fn groupCursor(self: *Prepared, _: A, group: Group) !GroupCursor {
            if (self.view) |view| {
                if (self.group_active) return error.InvalidArgument;
                if (group.first > self.count or group.count == 0 or group.count > self.count - group.first) return error.InvalidData;
                const source: RecordSource = .{ .view = view, .count = self.count - group.first, .begin = @as(usize, group.first) * 16 };
                if (self.group_reader == null) {
                    // The owner, rather than an individual borrower's scratch
                    // region, owns the buffer through final preparation close.
                    self.group_reader = RawCursor.init(self.a, source);
                    self.group_position = group.first;
                } else if (!self.group_reader_valid or self.group_position != group.first) {
                    self.group_reader.?.reposition(source);
                    self.group_position = group.first;
                    self.group_reader_valid = true;
                }
                self.group_active = true;
                return .{ .spool = self.spool, .remaining = group.count, .at = group.first, .links = self.links, .owner = self, .raw = &self.group_reader.? };
            }
            return .{ .spool = self.spool, .remaining = group.count, .at = group.first, .links = self.links };
        }
        pub fn prepareSource(_: *Prepared) void {}
        pub const SourceCursor = struct {
            cursor: Cursor,
            ranks: []const u32,
            pub fn deinit(self: *SourceCursor) void {
                self.cursor.deinit();
            }
            pub fn next(self: *SourceCursor) !?struct { token: analysis.Token, ordinal: u32 } {
                const token = (try self.cursor.next()) orelse return null;
                return .{ .token = token, .ordinal = self.ranks[self.cursor.term_id] };
            }
        };
        pub fn sourceCursor(self: *Prepared, a: A) !SourceCursor {
            return .{ .cursor = (try self.spool.input()).cursor(a), .ranks = self.ranks };
        }
        /// Stable MSD partitioning on lexical term ranks. Each pass scans
        /// bounded blocks into sixteen small buffers in one private run. Leaves
        /// use counting scatter within a fixed workspace; a single huge term
        /// streams directly. No occurrence-count-sized payload allocation and
        /// no one-file-per-term fanout are needed.
        fn partition(self: *Prepared, source: RecordSource, output: *Run, first: usize, end: usize, shift: u5) anyerror!void {
            if (source.count == 0) return;
            if (end <= first or end > self.groups.len) return error.InvalidData;
            var cursor = RawCursor.init(self.a, source);
            defer cursor.deinit();
            if (end - first == 1) {
                while (try cursor.nextBlock()) |bytes| try output.appendSlice(bytes);
                return;
            }
            const headroom = @import("../sql/memory_budget.zig").headroom(self.a) orelse std.math.maxInt(usize);
            const leaf_records = @min(self.spool.group_memory_records, headroom / 64);
            if (source.count <= leaf_records) {
                const records = try self.a.alloc(u8, source.count * 16);
                defer self.a.free(records);
                const positions = try self.a.alloc(u32, end - first);
                defer self.a.free(positions);
                var total: u32 = 0;
                for (positions, self.groups[first..end]) |*position, group| {
                    position.* = total;
                    total = try std.math.add(u32, total, group.count);
                }
                if (total != source.count) return error.InvalidData;
                while (try cursor.next()) |record| {
                    const id = std.mem.readInt(u32, record[0..4], .little);
                    if (id >= self.ranks.len) return error.InvalidData;
                    const rank = self.ranks[id];
                    if (rank < first or rank >= end) return error.InvalidData;
                    const position = &positions[rank - first];
                    @memcpy(records[@as(usize, position.*) * 16 ..][0..16], record);
                    position.* += 1;
                }
                try output.appendSlice(records);
                return;
            }
            const temporary = try Run.createWithResources(self.a, self.spool.io.?, self.spool.directory, self.spool.resources);
            defer {
                self.spool.grouping_read_calls += readCalls(temporary);
                temporary.deinit();
            }
            temporary.preferSequentialReads();
            var ranges: [16]std.ArrayList(Range) = @splat(.empty);
            defer for (&ranges) |*list| list.deinit(self.a);
            var counts: [16]usize = @splat(0);
            {
                const buffer_bytes: usize = @min(16 * 1024, @max(16, (headroom / 64) / 16 * 16));
                const buffers = try self.a.alloc(u8, 16 * buffer_bytes);
                defer self.a.free(buffers);
                var used: [16]usize = @splat(0);
                while (try cursor.next()) |record| {
                    const id = std.mem.readInt(u32, record[0..4], .little);
                    if (id >= self.ranks.len) return error.InvalidData;
                    const rank = self.ranks[id];
                    if (rank < first or rank >= end) return error.InvalidData;
                    const bucket = (rank >> shift) & 15;
                    const buffer = buffers[bucket * buffer_bytes ..][0..buffer_bytes];
                    @memcpy(buffer[used[bucket]..][0..16], record);
                    used[bucket] += 16;
                    counts[bucket] += 1;
                    if (used[bucket] == buffer_bytes) {
                        try ranges[bucket].append(self.a, .{ .offset = temporary.len(), .bytes = buffer_bytes });
                        try temporary.appendSlice(buffer);
                        used[bucket] = 0;
                    }
                }
                for (used, 0..) |length, bucket| if (length != 0) {
                    try ranges[bucket].append(self.a, .{ .offset = temporary.len(), .bytes = length });
                    try temporary.appendSlice(buffers[bucket * buffer_bytes ..][0..length]);
                };
            }
            try temporary.seal(0);
            const view = try temporary.sealedView();
            const span = @as(usize, 1) << shift;
            const base = first & ~((span - 1) * 16 + 15);
            for (counts, 0..) |count, bucket| if (count != 0) {
                const lower = @max(first, base + bucket * span);
                const upper = @min(end, base + (bucket + 1) * span);
                try self.partition(.{ .view = view, .count = count, .ranges = ranges[bucket].items }, output, lower, upper, shift -| 4);
            };
        }
    };
};

test "highlight source token spool crosses its memory bound and refunds failures" {
    const Check = struct {
        fn run(a: A, disk: bool) !void {
            var spool = Spool.init(a, if (disk) std.testing.io else null, ".", null);
            defer spool.deinit();
            spool.max_memory_bytes = 128;
            var word: [32]u8 = undefined;
            for (0..20) |i| {
                const term = try std.fmt.bufPrint(&word, "term{d}", .{i % 3});
                try spool.append(.{ .term = term, .start_byte = @intCast(i * 10), .end_byte = @intCast(i * 10 + term.len), .position = @intCast(i) });
            }
            try std.testing.expectEqual(disk, spool.run != null);
            const input = try spool.input();
            // Random replay crosses the memory-to-file transition and borrows
            // interned terms after the caller's mutable word buffer changed.
            for (0..20) |n| {
                const i = (n * 7) % 20;
                const token = try input.at(i);
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&word, "term{d}", .{i % 3}), token.term);
                try std.testing.expectEqual(i * 10, token.start_byte);
                try std.testing.expectEqual(i, token.position);
            }
        }
    };
    for ([_]bool{ false, true }) |disk| {
        try Check.run(std.testing.allocator, disk);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{disk});
    }
}

test "highlight normalized grouping preserves exact offsets and stable source order across skew partitions and faults" {
    const offsets = @import("source_offsets.zig");
    const Check = struct {
        fn run(a: A, disk: bool, skew: bool) !void {
            var spool = Spool.init(a, if (disk) std.testing.io else null, ".", null);
            defer spool.deinit();
            spool.max_memory_bytes = 16;
            spool.group_memory_records = 4;
            var reference = std.heap.ArenaAllocator.init(a);
            defer reference.deinit();
            const ra = reference.allocator();
            const tokens = try ra.alloc(analysis.Token, 132);
            for (tokens, 0..) |*token, i| {
                // Lexical order differs from discovery. Positions deliberately
                // repeat; stable grouping must not sort equal terms by position.
                const id = if (skew and i > 64) 0 else i % 65;
                const word = try std.fmt.allocPrint(ra, "word{d}", .{id});
                const start = if (skew and i % 7 == 0) 0 else i * 8;
                token.* = .{ .term = word, .start_byte = @intCast(start), .end_byte = @intCast(start + 4), .position = @intCast(i % 7) };
                try spool.append(token.*);
            }
            var expected = segment.MemorySegmentSink.init(a);
            defer expected.deinit();
            var expected_sink = expected.sink();
            const encoded_expected = try offsets.write(a, &expected_sink, tokens);
            var actual = segment.MemorySegmentSink.init(a);
            defer actual.deinit();
            var actual_sink = actual.sink();
            const encoded_actual = try offsets.writeInput(a, &actual_sink, try spool.input());
            try std.testing.expectEqual(encoded_expected, encoded_actual);
            try std.testing.expectEqualSlices(u8, expected.out.items, actual.out.items);
            var cursor = (try spool.input()).cursor(a);
            defer cursor.deinit();
            for (tokens) |token| {
                const replayed = (try cursor.next()).?;
                try std.testing.expectEqualStrings(token.term, replayed.term);
                try std.testing.expectEqual(token.position, replayed.position);
                try std.testing.expectEqual(token.start_byte, replayed.start_byte);
                try std.testing.expectEqual(token.end_byte, replayed.end_byte);
            }
            try std.testing.expect((try cursor.next()) == null);
            if (disk) try std.testing.expect(spool.grouping_read_calls != 0);
        }
    };
    for ([_]bool{ false, true }) |disk| for ([_]bool{ false, true }) |skew| {
        try Check.run(std.testing.allocator, disk, skew);
        var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{ disk, skew });
    };
}

test "highlight normalized disk grouping bounds workspace and physical reads for diverse source order" {
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 8 << 20 };
    defer std.debug.assert(budget.live == 0);
    const a = budget.allocator();
    var spool = Spool.init(a, std.testing.io, ".", null);
    defer spool.deinit();
    spool.max_memory_bytes = 128;
    const total = 32769;
    for (0..total) |i| {
        var word: [32]u8 = undefined;
        const term = try std.fmt.bufPrint(&word, "word{d}", .{i % 4096});
        try spool.append(.{ .term = term, .start_byte = @intCast(i * 10), .end_byte = @intCast(i * 10 + 4), .position = @intCast(i) });
    }
    var prepared = try (try spool.input()).prepareOffsets(a);
    defer prepared.deinit();
    for (prepared.groups) |group| {
        var cursor = try prepared.groupCursor(a, group);
        defer cursor.deinit();
        var previous: ?u32 = null;
        while (try cursor.next()) |token| {
            try std.testing.expectEqualStrings(group.term, token.term);
            if (previous) |position| try std.testing.expect(token.position > position);
            previous = token.position;
        }
    }
    var source = (try spool.input()).cursor(a);
    defer source.deinit();
    var count: usize = 0;
    while (try source.next()) |token| : (count += 1) try std.testing.expectEqual(count, token.position);
    try std.testing.expectEqual(@as(usize, total), count);
    // Each source/group/partition lane reads blocks, rather than one page per
    // occurrence. Include the still-open final sorted run in this assertion.
    const reads = Spool.readCalls(spool.run.?) + spool.grouping_read_calls + Spool.readCalls(prepared.grouped.?);
    try std.testing.expect(reads < total / 8);
    try std.testing.expectEqual(@as(usize, 1), prepared.group_reader.?.buffer_allocations);
    try std.testing.expect(budget.peak < budget.limit);
    std.debug.print("PARTITIONED_REPLAY records={d} reads={d} peak={d} quota={d}\n", .{ total, reads, budget.peak, budget.limit });
}

test "highlight normalized group reader reuses one bounded buffer across partial leases and backward seeks under faults" {
    const Check = struct {
        fn run(a: A) !void {
            var spool = Spool.init(a, std.testing.io, ".", null);
            defer spool.deinit();
            spool.max_memory_bytes = 16;
            spool.group_memory_records = 4;
            for (0..132) |i| {
                var word: [32]u8 = undefined;
                const text = try std.fmt.bufPrint(&word, "word{d}", .{i % 65});
                try spool.append(.{ .term = text, .start_byte = @intCast(i * 8), .end_byte = @intCast(i * 8 + 4), .position = @intCast(i) });
            }
            var prepared = try (try spool.input()).prepareOffsets(a);
            var open = true;
            defer if (open) prepared.deinit();
            {
                var first = try prepared.groupCursor(a, prepared.groups[0]);
                defer first.deinit();
                try std.testing.expect((try first.next()) != null);
                try std.testing.expectError(error.InvalidArgument, prepared.groupCursor(a, prepared.groups[1]));
                // Leave a partial group. Its unread records are not a hidden
                // requirement on the next borrower or final owner teardown.
            }
            const original_buffer = prepared.group_reader.?.buffer.ptr;
            for (0..prepared.groups.len) |n| {
                const group = prepared.groups[(n * 17) % prepared.groups.len];
                var cursor = try prepared.groupCursor(a, group);
                defer cursor.deinit();
                var count: usize = 0;
                while (try cursor.next()) |token| : (count += 1) {
                    try std.testing.expectEqualStrings(group.term, token.term);
                    try std.testing.expectEqual(@as(u32, @intCast(token.position * 8)), token.start_byte);
                }
                try std.testing.expectEqual(@as(usize, group.count), count);
                try std.testing.expectEqual(original_buffer, prepared.group_reader.?.buffer.ptr);
            }
            try std.testing.expectEqual(@as(usize, 1), prepared.group_reader.?.buffer_allocations);
            prepared.deinit();
            open = false;
            try std.testing.expectEqual(@as(usize, 1), spool.group_buffer_allocations);
        }
    };
    try Check.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{});
}

test "highlight normalized group reader resets a failed read before a new lease without dropping records" {
    const a = std.testing.allocator;
    var spool = Spool.init(a, std.testing.io, ".", null);
    defer spool.deinit();
    spool.max_memory_bytes = 16;
    for (0..5001) |i| try spool.append(.{ .term = if (i == 5000) "z" else "alpha", .start_byte = @intCast(i * 8), .end_byte = @intCast(i * 8 + 4), .position = @intCast(i) });
    var prepared = try (try spool.input()).prepareOffsets(a);
    defer prepared.deinit();
    const Probe = struct {
        view: View,
        reads: usize = 0,
        fn close(_: *anyopaque) void {}
        fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            if (self.reads == 2) return error.InjectedReadFailure;
            try self.view.readInto(offset, output);
        }
    };
    var probe: Probe = .{ .view = prepared.view.? };
    prepared.view = try View.init(.{ .ranges = .{ .ptr = &probe, .length = probe.view.length, .read_into = Probe.read, .close = Probe.close } }, 0, probe.view.length);
    {
        var cursor = try prepared.groupCursor(a, prepared.groups[0]);
        defer cursor.deinit();
        for (0..4096) |i| try std.testing.expectEqual(@as(u32, @intCast(i)), (try cursor.next()).?.position);
        try std.testing.expectError(error.InjectedReadFailure, cursor.next());
        try std.testing.expectError(error.InvalidData, cursor.next());
    }
    const buffer = prepared.group_reader.?.buffer.ptr;
    var count: usize = 0;
    for (prepared.groups) |group| {
        var cursor = try prepared.groupCursor(a, group);
        defer cursor.deinit();
        while (try cursor.next()) |token| : (count += 1) try std.testing.expectEqual(@as(u32, @intCast(count)), token.position);
    }
    try std.testing.expectEqual(@as(usize, 5001), count);
    try std.testing.expectEqual(buffer, prepared.group_reader.?.buffer.ptr);
    try std.testing.expectEqual(@as(usize, 1), prepared.group_reader.?.buffer_allocations);
    try std.testing.expectEqual(@as(usize, 4), probe.reads);
}
