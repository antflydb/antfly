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
};
pub const Record = struct { key: u64, payload: []const u8 };
pub const Range = struct { start: usize, end: usize, count: usize };

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
        if (self.options.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
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
    fn lessThan(_: void, a: Record, b: Record) bool {
        return a.key < b.key;
    }
    fn flush(self: *Sorter) !void {
        if (self.chunk.items.len == 0) return;
        std.sort.pdq(Record, self.chunk.items, {}, lessThan);
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
        const start = self.run.len();
        var cursors: [8]Cursor = undefined;
        var heads: [8]?RecordView = @splat(null);
        for (ranges, 0..) |range, i| cursors[i] = Cursor.init(self.allocator, self.run, range);
        defer for (cursors[0..ranges.len]) |*cursor| cursor.deinit();
        for (cursors[0..ranges.len], 0..) |*cursor, i| heads[i] = try cursor.nextView();
        var copied: [16 * 1024]u8 = undefined;
        while (true) {
            try self.check();
            var selected: ?usize = null;
            for (heads[0..ranges.len], 0..) |head, i| if (head) |record| {
                if (selected == null or record.key < heads[selected.?].?.key) selected = i;
            };
            const i = selected orelse break;
            const record = heads[i].?;
            var header: [16]u8 = undefined;
            std.mem.writeInt(u64, header[0..8], record.key, .little);
            std.mem.writeInt(u64, header[8..16], record.payload.length, .little);
            try self.run.appendSlice(&header);
            var offset: usize = 0;
            while (offset < record.payload.length) {
                const take: usize = @intCast(@min(copied.len, record.payload.length - offset));
                try record.payload.readInto(offset, copied[0..take]);
                try self.run.appendSlice(copied[0..take]);
                offset += take;
            }
            heads[i] = try cursors[i].nextView();
        }
        try self.run.seal(start);
        var count: usize = 0;
        for (ranges) |range| {
            count = try std.math.add(usize, count, range.count);
            self.run.releaseRange(range.start);
        }
        const result = Range{ .start = start, .end = self.run.len(), .count = count };
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
