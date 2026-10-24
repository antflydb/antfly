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
        dirty: bool = false,
        live: bool = true,
        fresh: bool = true,
        busy: bool = false,
        previous: ?usize = null,
        next: ?usize = null,
    };
    manager: *spill.Manager,
    pages: std.ArrayList(Page) = .empty,
    file: ?spill.File = null,
    free_head: ?usize = null,
    idle_head: ?usize = null,
    idle_tail: ?usize = null,
    inflight: usize = 0,
    transfers: usize = 0,
    limit: usize,
    resident: usize = 0,
    peak: usize = 0,
    evictions: u64 = 0,
    restores: u64 = 0,
    directory_steps: u64 = 0,
    pins: u64 = 0,
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Condition = .init,
    file_mutex: std.Io.Mutex = .init,
    pub const Lease = struct {
        owner: *Pager,
        id: usize,
        data: []align(16) u8,
        pub fn release(self: Lease) void {
            const owner = self.owner;
            const io = owner.manager.io;
            owner.mutex.lockUncancelable(io);
            defer owner.mutex.unlock(io);
            const page = &owner.pages.items[self.id];
            std.debug.assert(page.live and page.pins != 0 and !page.busy);
            page.pins -= 1;
            if (page.pins == 0) owner.addIdle(self.id);
            owner.changed.broadcast(io);
        }
    };
    // Only resident, unpinned frames enter the eviction queue. Logical handles
    // and retired slots never require directory-wide searches.
    fn addIdle(self: *Pager, id: usize) void {
        const page = &self.pages.items[id];
        page.previous = self.idle_tail;
        page.next = null;
        if (self.idle_tail) |tail| self.pages.items[tail].next = id else self.idle_head = id;
        self.idle_tail = id;
    }
    fn removeIdle(self: *Pager, id: usize) void {
        const page = &self.pages.items[id];
        if (page.previous) |previous| self.pages.items[previous].next = page.next else self.idle_head = page.next;
        if (page.next) |next| self.pages.items[next].previous = page.previous else self.idle_tail = page.previous;
        page.previous = null;
        page.next = null;
    }
    pub fn create(self: *Pager) !usize {
        try self.manager.check();
        try self.mutex.lock(self.manager.io);
        defer self.mutex.unlock(self.manager.io);
        self.directory_steps += 1;
        if (self.free_head) |id| {
            const page = &self.pages.items[id];
            self.free_head = page.next;
            const offset = page.offset;
            page.* = .{ .offset = offset, .dirty = true };
            return id;
        }
        try self.pages.append(self.manager.allocator(), .{});
        return self.pages.items.len - 1;
    }
    // The descriptor/buffer owner remains serialized, but a slow disk operation
    // holds no frame-directory lock. Other resident pages stay pinnable. IDs,
    // never pointers into the growable directory, survive this unlocked phase.
    fn transfer(self: *Pager, id: usize, data: []align(16) u8, write: bool) !u64 {
        const io = self.manager.io;
        const saved = self.pages.items[id].offset;
        self.transfers += 1;
        self.mutex.unlock(io);
        defer {
            self.mutex.lockUncancelable(io);
            self.transfers -= 1;
            self.changed.broadcast(io);
        }
        try self.file_mutex.lock(io);
        defer self.file_mutex.unlock(io);
        if (self.file == null) {
            std.debug.assert(write);
            self.file = try self.manager.create();
            self.file.?.buffer_bytes = 0;
        }
        const offset = saved orelse self.file.?.size;
        if (write) try self.file.?.writeRaw(offset, data) else try self.file.?.readRaw(offset, data);
        return offset;
    }
    fn evict(self: *Pager) !bool {
        const id = self.idle_head orelse return false;
        self.directory_steps += 1;
        self.removeIdle(id);
        self.pages.items[id].busy = true;
        self.inflight += 1;
        defer {
            self.pages.items[id].busy = false;
            self.inflight -= 1;
            self.changed.broadcast(self.manager.io);
        }
        errdefer self.addIdle(id);
        try self.manager.check();
        const data = self.pages.items[id].data.?;
        if (self.pages.items[id].dirty) {
            const offset = try self.transfer(id, data, true);
            self.pages.items[id].offset = offset;
            self.pages.items[id].dirty = false;
        }
        self.manager.allocator().free(data);
        self.pages.items[id].data = null;
        self.resident -= page_bytes;
        self.evictions += 1;
        return true;
    }
    pub fn pin(self: *Pager, id: usize, write: bool) !Lease {
        const io = self.manager.io;
        try self.manager.check();
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (id >= self.pages.items.len or !self.pages.items[id].live) return error.InvalidSqlSpill;
        while (self.pages.items[id].busy) try self.changed.wait(io, &self.mutex);
        if (self.pages.items[id].data == null) {
            self.pages.items[id].busy = true;
            self.inflight += 1;
            defer {
                self.pages.items[id].busy = false;
                self.inflight -= 1;
                self.changed.broadcast(io);
            }
            while (self.resident +| page_bytes > self.limit or
                (if (@import("memory_budget.zig").headroom(self.manager.allocator())) |available| available < page_bytes + 4096 else false))
            {
                if (!try self.evict()) {
                    if (self.transfers != 0) {
                        try self.changed.wait(io, &self.mutex);
                        continue;
                    }
                    return error.SqlProgramLimitExceeded;
                }
            }
            const data = try self.manager.allocator().alignedAlloc(u8, alignment, page_bytes);
            self.resident += page_bytes;
            self.peak = @max(self.peak, self.resident);
            errdefer {
                self.manager.allocator().free(data);
                self.resident -= page_bytes;
            }
            if (!self.pages.items[id].fresh) {
                _ = try self.transfer(id, data, false);
                self.restores += 1;
            } else {
                @memset(data, 0);
                self.pages.items[id].dirty = true;
            }
            self.pages.items[id].data = data;
            self.pages.items[id].fresh = false;
        } else if (self.pages.items[id].pins == 0) self.removeIdle(id);
        const page = &self.pages.items[id];
        page.pins += 1;
        self.pins += 1;
        page.dirty = page.dirty or write;
        return .{ .owner = self, .id = id, .data = page.data.? };
    }
    pub fn retire(self: *Pager, id: usize) void {
        const io = self.manager.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        while (self.pages.items[id].busy) self.changed.wait(io, &self.mutex) catch unreachable;
        const page = &self.pages.items[id];
        std.debug.assert(page.live and page.pins == 0);
        if (page.data) |data| {
            self.removeIdle(id);
            self.manager.allocator().free(data);
            self.resident -= page_bytes;
        }
        page.* = .{ .live = false, .offset = page.offset, .next = self.free_head };
        self.free_head = id;
    }
    pub fn deinit(self: *Pager) void {
        std.debug.assert(self.inflight == 0);
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
        const per_page: usize = page_bytes / @sizeOf(T);
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
        pub fn appendSlice(self: *Self, input: []const T) !void {
            var begin: usize = 0;
            while (begin < input.len) {
                const page = self.len / per_page;
                if (page == self.handles.items.len) {
                    const id = try self.pager.create();
                    errdefer self.pager.retire(id);
                    try self.handles.append(self.pager.manager.allocator(), id);
                }
                const lease = try self.pager.pin(self.handles.items[page], true);
                defer lease.release();
                const values = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
                const count = @min(per_page - self.len % per_page, input.len - begin);
                @memcpy(values[self.len % per_page ..][0..count], input[begin..][0..count]);
                self.len += count;
                begin += count;
            }
        }
        pub fn appendRepeated(self: *Self, value: T, count: usize) !void {
            var remaining = count;
            while (remaining != 0) {
                const page = self.len / per_page;
                if (page == self.handles.items.len) {
                    const id = try self.pager.create();
                    errdefer self.pager.retire(id);
                    try self.handles.append(self.pager.manager.allocator(), id);
                }
                const lease = try self.pager.pin(self.handles.items[page], true);
                defer lease.release();
                const values = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
                const length = @min(per_page - self.len % per_page, remaining);
                @memset(values[self.len % per_page ..][0..length], value);
                self.len += length;
                remaining -= length;
            }
        }
        pub fn getMany(self: Self, indices: []const usize, output: []T) !void {
            if (indices.len != output.len) return error.InvalidSqlSpill;
            for (indices) |index| if (index >= self.len) return error.InvalidSqlSpill;
            var begin: usize = 0;
            while (begin < indices.len) {
                const end = @min(indices.len, begin + 128);
                var done: [128]bool = @splat(false);
                for (indices[begin..end], 0..) |index, lane| {
                    if (done[lane]) continue;
                    const window = try self.pinPage(index, false);
                    defer window.lease.release();
                    for (indices[begin..end], 0..) |selected, position| {
                        if (done[position] or self.pageStart(selected) != window.begin) continue;
                        output[begin + position] = window.values[selected - window.begin];
                        done[position] = true;
                    }
                }
                begin = end;
            }
        }
        pub fn pin(self: Self, index: usize, write: bool) !Pinned {
            if (index >= self.len) return error.InvalidSqlSpill;
            const lease = try self.pager.pin(self.handles.items[index / per_page], write);
            const values: []T = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
            return .{ .lease = lease, .value = &values[index % per_page] };
        }
        pub const Window = struct { lease: Pager.Lease, values: []T, begin: usize };
        pub fn pageStart(_: Self, index: usize) usize {
            return index / per_page * per_page;
        }
        pub fn pinPage(self: Self, index: usize, write: bool) !Window {
            if (index >= self.len) return error.InvalidSqlSpill;
            const begin = self.pageStart(index);
            const lease = try self.pager.pin(self.handles.items[index / per_page], write);
            const length: usize = @min(per_page, self.len - begin);
            return .{ .lease = lease, .values = std.mem.bytesAsSlice(T, lease.data[0 .. length * @sizeOf(T)]), .begin = begin };
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

test "SQL execution pager directory work is linear and disk transfers permit resident pins" {
    const a = std.testing.allocator;
    const Check = struct {
        pager: ?*Pager = null,
        hot: ?usize = null,
        probing: bool = false,
        probes: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.probing) return;
            const pager = self.pager orelse return;
            const hot = self.hot orelse return;
            if (pager.file_mutex.tryLock()) {
                pager.file_mutex.unlock(pager.manager.io);
                return;
            }
            self.probing = true;
            defer self.probing = false;
            const held = try pager.pin(hot, false);
            defer held.release();
            try std.testing.expectEqual(@as(u8, 37), held.data[0]);
            self.probes += 1;
        }
    };
    var check: Check = .{};
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &check, .checkpoint = Check.check };
    defer manager.deinit();
    var pager: Pager = .{ .manager = &manager, .limit = 2 * page_bytes };
    defer pager.deinit();
    check.pager = &pager;
    var ids: [20000]usize = undefined;
    for (&ids) |*id| id.* = try pager.create();
    try std.testing.expectEqual(@as(u64, ids.len), pager.directory_steps);
    const hot = try pager.pin(ids[0], true);
    hot.data[0] = 37;
    check.hot = ids[0];
    for (ids[1..65]) |id| {
        const held = try pager.pin(id, true);
        held.release();
    }
    const restored = try pager.pin(ids[1], false);
    restored.release();
    try std.testing.expect(check.probes > 0);
    check.hot = null;
    hot.release();
    try std.testing.expectEqual(@as(u64, ids.len) + pager.evictions, pager.directory_steps);
    for (ids) |id| pager.retire(id);
    for (&ids) |*id| id.* = try pager.create();
    try std.testing.expectEqual(@as(usize, ids.len), pager.pages.items.len);
}
