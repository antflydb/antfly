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

//! Statement-owned dirty pages shared by join and aggregate state. Handles
//! survive eviction; pointers are valid only while their explicit lease is live.
const std = @import("std");
const spill = @import("spill.zig");
const A = std.mem.Allocator;
pub const page_bytes = 16 * 1024;
const alignment: std.mem.Alignment = .@"16";
pub const Pager = struct {
    const Page = struct {
        data: ?[]align(16) u8 = null,
        offset: ?u64 = null,
        pins: usize = 0,
        used: u64 = 0,
        dirty: bool = false,
        live: bool = true,
        fresh: bool = true,
    };
    manager: *spill.Manager,
    pages: std.ArrayList(Page) = .empty,
    file: ?spill.File = null,
    limit: usize,
    resident: usize = 0,
    peak: usize = 0,
    clock: u64 = 0,
    evictions: u64 = 0,
    restores: u64 = 0,
    mutex: std.Io.Mutex = .init,
    pub const Lease = struct {
        owner: *Pager,
        id: usize,
        data: []align(16) u8,
        pub fn release(self: Lease) void {
            // Cleanup must release pins even after request cancellation.
            const io = self.owner.manager.io;
            const protection = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(protection);
            self.owner.mutex.lock(io) catch unreachable;
            defer self.owner.mutex.unlock(io);
            const page = &self.owner.pages.items[self.id];
            std.debug.assert(page.live and page.pins != 0);
            page.pins -= 1;
        }
    };
    pub fn create(self: *Pager) !usize {
        try self.manager.check();
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        for (self.pages.items, 0..) |*page, id| if (!page.live) {
            // Reuse the physical slot as well as the handle directory entry.
            const offset = page.offset;
            page.* = .{ .offset = offset, .dirty = true };
            return id;
        };
        try self.pages.append(self.manager.allocator(), .{});
        return self.pages.items.len - 1;
    }
    fn evict(self: *Pager) !bool {
        var oldest: ?usize = null;
        for (self.pages.items, 0..) |page, id| if (page.live and page.data != null and page.pins == 0) {
            if (oldest == null or page.used < self.pages.items[oldest.?].used) oldest = id;
        };
        const page = &self.pages.items[oldest orelse return false];
        try self.manager.check();
        if (page.dirty) {
            if (self.file == null) {
                self.file = try self.manager.create();
                // Whole pages use positional I/O directly. Avoid allocating
                // ordinary row buffers while reclaiming a resident page.
                self.file.?.buffer_bytes = 0;
            }
            const offset = page.offset orelse self.file.?.size;
            try self.file.?.writeRaw(offset, page.data.?);
            page.offset = offset;
            page.dirty = false;
        }
        self.manager.allocator().free(page.data.?);
        page.data = null;
        self.resident -= page_bytes;
        self.evictions += 1;
        return true;
    }
    pub fn pin(self: *Pager, id: usize, write: bool) !Lease {
        try self.manager.check();
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        if (id >= self.pages.items.len or !self.pages.items[id].live) return error.InvalidSqlSpill;
        const page = &self.pages.items[id];
        if (page.data == null) {
            while (self.resident +| page_bytes > self.limit or
                (if (@import("memory_budget.zig").headroom(self.manager.allocator())) |available| available < page_bytes + 4096 else false))
                if (!try self.evict()) return error.SqlProgramLimitExceeded;
            const data = try self.manager.allocator().alignedAlloc(u8, alignment, page_bytes);
            errdefer self.manager.allocator().free(data);
            if (!page.fresh) {
                try self.file.?.readRaw(page.offset.?, data);
                self.restores += 1;
            } else {
                @memset(data, 0);
                page.dirty = true;
            }
            page.data = data;
            page.fresh = false;
            self.resident += page_bytes;
            self.peak = @max(self.peak, self.resident);
        }
        self.clock +|= 1;
        page.used = self.clock;
        page.pins += 1;
        page.dirty = page.dirty or write;
        return .{ .owner = self, .id = id, .data = page.data.? };
    }
    pub fn retire(self: *Pager, id: usize) void {
        const io = self.manager.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        self.mutex.lock(io) catch unreachable;
        defer self.mutex.unlock(io);
        const page = &self.pages.items[id];
        std.debug.assert(page.live and page.pins == 0);
        if (page.data) |data| {
            self.manager.allocator().free(data);
            self.resident -= page_bytes;
        }
        page.* = .{ .live = false, .offset = page.offset };
    }
    pub fn deinit(self: *Pager) void {
        for (self.pages.items) |page| {
            std.debug.assert(page.pins == 0);
            if (page.data) |data| self.manager.allocator().free(data);
        }
        self.pages.deinit(self.manager.allocator());
        if (self.file) |*file| file.close();
    }
};

/// Only pointer-free state belongs here. Variable-width keys and reducers use
/// their typed spill codecs; a raw pointer must never survive page eviction.
pub fn Array(comptime T: type) type {
    comptime {
        if (@alignOf(T) > 16 or @sizeOf(T) == 0 or @sizeOf(T) > page_bytes) @compileError("unsupported pager element layout");
        assertRelocatable(T);
    }
    return struct {
        const Self = @This();
        const per_page = page_bytes / @sizeOf(T);
        pager: *Pager,
        handles: std.ArrayList(usize) = .empty,
        len: usize = 0,
        pub const Pinned = struct {
            lease: Pager.Lease,
            value: *T,
            pub fn release(self: @This()) void {
                self.lease.release();
            }
        };
        pub fn deinit(self: *Self) void {
            for (self.handles.items) |id| self.pager.retire(id);
            self.handles.deinit(self.pager.manager.allocator());
            self.len = 0;
        }
        pub fn append(self: *Self, value: T) !void {
            const page = self.len / per_page;
            if (page == self.handles.items.len) {
                const id = try self.pager.create();
                errdefer self.pager.retire(id);
                try self.handles.append(self.pager.manager.allocator(), id);
            }
            const lease = try self.pager.pin(self.handles.items[page], true);
            defer lease.release();
            const values: []T = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
            values[self.len % per_page] = value;
            self.len += 1;
        }
        pub fn pin(self: Self, index: usize, write: bool) !Pinned {
            if (index >= self.len) return error.InvalidSqlSpill;
            const lease = try self.pager.pin(self.handles.items[index / per_page], write);
            const values: []T = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
            return .{ .lease = lease, .value = &values[index % per_page] };
        }
        pub fn get(self: Self, index: usize) !T {
            const pinned = try self.pin(index, false);
            defer pinned.release();
            return pinned.value.*;
        }
        pub fn set(self: Self, index: usize, value: T) !void {
            const pinned = try self.pin(index, true);
            defer pinned.release();
            pinned.value.* = value;
        }
    };
}
fn assertRelocatable(comptime T: type) void {
    switch (@typeInfo(T)) {
        .int, .float, .bool, .@"enum" => {},
        .array => |array| assertRelocatable(array.child),
        .@"struct" => |structure| inline for (structure.field_types) |field| assertRelocatable(field),
        else => @compileError("pager state must contain only relocatable fixed values"),
    }
}

test "SQL execution pager shares dirty state residency and restores pinned typed pages" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    var pager: Pager = .{ .manager = &manager, .limit = 2 * page_bytes };
    defer pager.deinit();
    var left: Array(u64) = .{ .pager = &pager };
    defer left.deinit();
    var right: Array(u64) = .{ .pager = &pager };
    defer right.deinit();
    for (0..8192) |i| {
        try left.append(i);
        try right.append(8192 - i);
    }
    const pinned = try left.pin(0, true);
    pinned.value.* = 987;
    for (0..8192) |i| try std.testing.expectEqual(@as(u64, 8192 - i), try right.get(i));
    try std.testing.expectEqual(@as(u64, 987), pinned.value.*);
    pinned.release();
    for (0..8192) |i| try std.testing.expectEqual(@as(u64, if (i == 0) 987 else i), try left.get(i));
    try std.testing.expect(pager.evictions > 0 and pager.restores > 0);
    try std.testing.expect(pager.peak <= pager.limit);
}

fn failureScenario(a: A) !void {
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    var pager: Pager = .{ .manager = &manager, .limit = 2 * page_bytes };
    defer pager.deinit();
    var values: Array([512]u64) = .{ .pager = &pager };
    defer values.deinit();
    for (0..16) |i| try values.append(@splat(i));
    for (0..16) |i| try std.testing.expectEqual(@as(u64, i), (try values.get(i))[0]);
}
test "SQL execution pager unwinds every allocation failure and cancellation releases pins" {
    const a = std.testing.allocator;
    try std.testing.checkAllAllocationFailures(a, failureScenario, .{});
    const Check = struct {
        fn check(raw: *anyopaque) !void {
            if (@as(*bool, @ptrCast(@alignCast(raw))).*) return error.Cancelled;
        }
    };
    var cancelled = false;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &cancelled, .checkpoint = Check.check };
    defer manager.deinit();
    var array: Array(u64) = .{ .pager = try manager.pager() };
    defer array.deinit();
    try array.append(3);
    const pinned = try array.pin(0, true);
    cancelled = true;
    try std.testing.expectError(error.Cancelled, array.get(0));
    pinned.release();
}

test "SQL execution pager refuses to evict a pinned page and reuses retired slots" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.check };
    defer manager.deinit();
    var pager: Pager = .{ .manager = &manager, .limit = page_bytes };
    defer pager.deinit();
    const first = try pager.create();
    const pinned = try pager.pin(first, true);
    pinned.data[0] = 77;
    const second = try pager.create();
    try std.testing.expectError(error.SqlProgramLimitExceeded, pager.pin(second, true));
    pinned.release();
    const other = try pager.pin(second, true);
    other.release();
    const restored = try pager.pin(first, false);
    try std.testing.expectEqual(@as(u8, 77), restored.data[0]);
    restored.release();
    pager.retire(first);
    const reused = try pager.create();
    try std.testing.expectEqual(first, reused);
    const fresh = try pager.pin(reused, false);
    try std.testing.expectEqual(@as(u8, 0), fresh.data[0]);
    fresh.release();
}
