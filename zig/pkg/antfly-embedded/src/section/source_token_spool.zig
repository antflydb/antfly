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
    a: A,
    io: ?std.Io,
    directory: []const u8,
    resources: ?*@import("../storage/resource_manager.zig").ResourceManager,
    max_memory_bytes: usize = 256 * 1024,
    run: ?*Run = null,
    memory: segment.MemorySegmentSink,
    ids: std.StringHashMapUnmanaged(u32) = .empty,
    terms: std.ArrayList([]const u8) = .empty,
    count: usize = 0,
    view: ?View = null,
    pub fn init(a: A, io: ?std.Io, directory: []const u8, resources: ?*@import("../storage/resource_manager.zig").ResourceManager) Spool {
        const work = @import("../sql/memory_budget.zig").reclaimingAllocator(a);
        const allowance = if (@import("../sql/memory_budget.zig").headroom(work)) |bytes| @min(4 * 1024 * 1024, bytes / 8) else 256 * 1024;
        return .{ .a = work, .io = io, .directory = directory, .resources = resources, .max_memory_bytes = allowance, .memory = .init(work) };
    }
    pub fn deinit(self: *Spool) void {
        self.ids.deinit(self.a);
        for (self.terms.items) |term| self.a.free(term);
        self.terms.deinit(self.a);
        if (self.run) |run| run.deinit();
        self.memory.deinit();
    }
    pub fn append(self: *Spool, token: analysis.Token) !void {
        if (self.view != null or self.count == std.math.maxInt(u32) or token.start_byte > token.end_byte) return error.InvalidData;
        const id = self.ids.get(token.term) orelse intern: {
            const owned = try self.a.dupe(u8, token.term);
            errdefer self.a.free(owned);
            try self.terms.append(self.a, owned);
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
    }
    pub const Input = struct {
        spool: *Spool,
        count: usize,
        pub fn at(self: Input, index: usize) !analysis.Token {
            return self.spool.read(index);
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
        const id = std.mem.readInt(u32, record[0..4], .little);
        if (id >= self.terms.items.len) return error.InvalidData;
        return .{ .term = self.terms.items[id], .start_byte = std.mem.readInt(u32, record[4..8], .little), .end_byte = std.mem.readInt(u32, record[8..12], .little), .position = std.mem.readInt(u32, record[12..16], .little) };
    }
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
