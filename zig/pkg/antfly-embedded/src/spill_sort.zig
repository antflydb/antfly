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

//! Byte-bounded sorting of private variable-length records. Binary carries
//! preserve logical run coordinates; only two records are decoded at a time.
const std = @import("std");
const Run = @import("postings_run.zig").Run;
const Scratch = @import("segment_source.zig").Scratch;
const Allocator = std.mem.Allocator;

pub const Options = struct {
    io: std.Io,
    directory: []const u8,
    resource_manager: ?*@import("storage/resource_manager.zig").ResourceManager = null,
    chunk_bytes: usize = 256 * 1024,
    chunk_records: usize = 1024,
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
    levels: [64]?Range = @splat(null),

    pub fn init(allocator: Allocator, options: Options) !Sorter {
        if (options.chunk_bytes == 0 or options.chunk_records == 0) return error.InvalidData;
        return .{ .allocator = allocator, .options = options, .run = try Run.createWithResources(allocator, options.io, options.directory, options.resource_manager), .payloads = .init(allocator, options.chunk_bytes) };
    }
    pub fn deinit(self: *Sorter) void {
        self.chunk.deinit(self.allocator);
        self.payloads.deinit();
        self.run.deinit();
        self.* = undefined;
    }
    pub fn add(self: *Sorter, key: u64, payload: []const u8) !void {
        if (self.chunk.items.len != 0 and (self.chunk.items.len >= self.options.chunk_records or self.chunk_bytes >= self.options.chunk_bytes)) try self.flush();
        const owned = try self.payloads.allocator().dupe(u8, payload);
        try self.chunk.append(self.allocator, .{ .key = key, .payload = owned });
        self.chunk_bytes = try std.math.add(usize, self.chunk_bytes, @sizeOf(Record) + payload.len);
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
        for (&self.levels) |*slot| {
            if (slot.*) |previous| {
                slot.* = null;
                range = try self.merge(previous, range);
            } else {
                slot.* = range;
                return;
            }
        }
        return error.Overflow;
    }
    fn merge(self: *Sorter, left: Range, right: Range) !Range {
        const start = self.run.len();
        var cursors = [_]Cursor{ Cursor.init(self.allocator, self.run, left), Cursor.init(self.allocator, self.run, right) };
        defer for (&cursors) |*cursor| cursor.deinit();
        var heads = [_]?Record{ try cursors[0].next(), try cursors[1].next() };
        while (heads[0] != null or heads[1] != null) {
            const selected: usize = if (heads[0] == null) 1 else if (heads[1] == null) 0 else if (heads[1].?.key < heads[0].?.key) 1 else 0;
            try self.append(heads[selected].?);
            heads[selected] = try cursors[selected].next();
        }
        try self.run.seal(start);
        const result = Range{ .start = start, .end = self.run.len(), .count = try std.math.add(usize, left.count, right.count) };
        self.run.releaseRange(left.start);
        self.run.releaseRange(right.start);
        try self.run.compact();
        return result;
    }
    pub fn finish(self: *Sorter) !?Range {
        try self.flush();
        var range: ?Range = null;
        for (&self.levels) |*slot| if (slot.*) |previous| {
            slot.* = null;
            range = if (range) |current| try self.merge(previous, current) else previous;
        };
        return range;
    }
};

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
    pub fn next(self: *Cursor) !?Record {
        self.payloads.reset();
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
        const payload = try self.payloads.allocator().alloc(u8, length);
        try view.readInto(self.position, payload);
        self.position += length;
        self.remaining -= 1;
        return .{ .key = std.mem.readInt(u64, header[0..8], .little), .payload = payload };
    }
};
