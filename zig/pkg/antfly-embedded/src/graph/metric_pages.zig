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

//! Stable, query-owned score pages for bounded native read windows.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Query-owned column pages. Native reads fill stable, contiguous column
/// windows; growth adds pages without copying any previously accepted score.
pub const ScorePages = struct {
    const chunk_alignment = @max(@alignOf(Chunk), @alignOf(f64));
    const header_bytes = std.mem.alignForward(usize, @sizeOf(Chunk), @alignOf(f64));
    const Chunk = struct {
        next: ?*Chunk = null,
        capacity: usize,
        used: usize = 0,
        fn data(self: *Chunk) []f64 {
            return @as([*]f64, @ptrCast(@alignCast(@as([*]u8, @ptrCast(self)) + header_bytes)))[0..self.capacity];
        }
    };
    alloc: Allocator,
    columns: usize,
    row_shift: std.math.Log2Int(usize),
    row_capacity: usize,
    count: usize = 0,
    pages: std.ArrayListUnmanaged([]f64) = .empty,
    head: ?*Chunk = null,
    tail: ?*Chunk = null,
    allocated: usize = @sizeOf(ScorePages),

    pub fn create(a: Allocator, columns: usize) !*ScorePages {
        std.debug.assert(columns != 0 and columns <= 16);
        return createForRows(a, columns, @as(usize, 1) << std.math.log2_int(usize, 4096 / columns));
    }
    pub fn createForRows(a: Allocator, columns: usize, rows: usize) !*ScorePages {
        std.debug.assert(columns != 0 and columns <= 16 and rows != 0 and std.math.isPowerOfTwo(rows) and rows <= 4096 / columns);
        const shift = std.math.log2_int(usize, rows);
        const owner = try a.create(ScorePages);
        owner.* = .{ .alloc = a, .columns = columns, .row_shift = shift, .row_capacity = @as(usize, 1) << shift };
        return owner;
    }
    pub fn destroy(self: *ScorePages) void {
        const a = self.alloc;
        self.pages.deinit(a);
        var chunk = self.head;
        while (chunk) |owned| {
            chunk = owned.next;
            const bytes: [*]align(chunk_alignment) u8 = @ptrCast(@alignCast(owned));
            a.free(bytes[0 .. header_bytes + owned.capacity * @sizeOf(f64)]);
        }
        a.destroy(self);
    }
    pub fn storageBytes(self: *const ScorePages) usize {
        return self.allocated + self.pages.capacity * @sizeOf([]f64);
    }
    pub fn ensureRows(self: *ScorePages, rows: usize) !void {
        const required = (rows + self.row_capacity - 1) >> self.row_shift;
        try self.pages.ensureTotalCapacity(self.alloc, required);
        while (self.pages.items.len < required) {
            const cells = self.row_capacity * self.columns;
            if (self.tail == null or self.tail.?.capacity - self.tail.?.used < cells) {
                // Speculative chunk geometry must not raise the page's minimum
                // admission budget. Retry with exactly one page on denial.
                const preferred = if (self.tail) |tail| @max(cells, @min(tail.capacity * 2, 256 * 1024 / @sizeOf(f64))) else cells;
                var capacity = preferred;
                const bytes = self.alloc.alignedAlloc(u8, .fromByteUnits(chunk_alignment), header_bytes + capacity * @sizeOf(f64)) catch blk: {
                    if (preferred == cells) return error.OutOfMemory;
                    capacity = cells;
                    break :blk try self.alloc.alignedAlloc(u8, .fromByteUnits(chunk_alignment), header_bytes + capacity * @sizeOf(f64));
                };
                const chunk: *Chunk = @ptrCast(bytes.ptr);
                chunk.* = .{ .capacity = capacity };
                if (self.tail) |tail| tail.next = chunk else self.head = chunk;
                self.tail = chunk;
                self.allocated += bytes.len;
            }
            const tail = self.tail.?;
            self.pages.appendAssumeCapacity(tail.data()[tail.used..][0..cells]);
            tail.used += cells;
        }
    }
    pub fn window(self: *ScorePages, column: usize, row: usize, count: usize) []f64 {
        const offset = row & (self.row_capacity - 1);
        std.debug.assert(column < self.columns and count <= self.row_capacity - offset);
        return self.pages.items[row >> self.row_shift][column * self.row_capacity + offset ..][0..count];
    }
    pub fn at(self: *const ScorePages, column: usize, row: usize) f64 {
        return self.pages.items[row >> self.row_shift][column * self.row_capacity + (row & (self.row_capacity - 1))];
    }
    pub fn set(self: *ScorePages, column: usize, row: usize, value: f64) void {
        self.pages.items[row >> self.row_shift][column * self.row_capacity + (row & (self.row_capacity - 1))] = value;
    }
};

test "metric score pages preserve borrowed windows across directory and chunk growth" {
    const owner = try ScorePages.create(std.testing.allocator, 3);
    defer owner.destroy();
    try owner.ensureRows(1);
    const first = owner.window(2, 0, 1);
    first[0] = 42;
    try owner.ensureRows(20000);
    try std.testing.expectEqual(first.ptr, owner.window(2, 0, 1).ptr);
    try std.testing.expectEqual(@as(f64, 42), first[0]);
    for (0..owner.columns) |column| for (0..20000) |row| owner.set(column, row, @floatFromInt(row * 10 + column));
    for (0..owner.columns) |column| for (0..20000) |row| try std.testing.expectEqual(@as(f64, @floatFromInt(row * 10 + column)), owner.at(column, row));
}

test "metric score pages retry exact admission when speculative chunk growth does not fit" {
    var buffer: [80 * 1024]u8 align(8) = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    const owner = try ScorePages.create(fixed.allocator(), 4);
    defer owner.destroy();
    try owner.ensureRows(owner.row_capacity * 2);
    owner.set(3, owner.row_capacity * 2 - 1, 99);
    try std.testing.expectEqual(@as(f64, 99), owner.at(3, owner.row_capacity * 2 - 1));
    try std.testing.expect(owner.storageBytes() < buffer.len);
}

test "metric score pages allocation failures release every owned chunk" {
    const Runner = struct {
        fn run(a: Allocator) !void {
            const owner = try ScorePages.create(a, 16);
            defer owner.destroy();
            try owner.ensureRows(10000);
            owner.set(15, 9999, 7);
            try std.testing.expectEqual(@as(f64, 7), owner.at(15, 9999));
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
