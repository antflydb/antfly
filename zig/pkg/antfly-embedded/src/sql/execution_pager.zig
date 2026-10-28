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
const Runnable = @import("../common/runnable_scheduler.zig");
const Work = @import("operator_work.zig");
const waits = @import("execution_wait.zig");

/// Call-local authority, never retained by the shared pager or backing file.
/// A coordinator may supply its own restriction in addition to CPU ownership.
pub const Access = struct {
    execution: ?*Runnable.Execution = null,
    control: ?Work.Control = null,
    wait: ?waits.Boundary = null,
    pub fn check(self: Access, manager: *spill.Manager) !void {
        if (manager.cpu_scope != null and self.execution == null and self.wait == null) return error.ExecutionWaitUnsupported;
        if (self.control) |_| try Work.poll(self.control) else try manager.checkFor(self.execution);
    }
    pub fn boundary(self: Access, manager: *spill.Manager) ?waits.Boundary {
        return self.wait orelse if (self.execution) |owner| spill.childWait(owner) else manager.wait;
    }
    pub fn lock(self: Access, manager: *spill.Manager, mutex: anytype) !void {
        try self.check(manager);
        while (!mutex.tryLock()) {
            try waits.run(self.boundary(manager), std.Io.sleep, .{ manager.io, std.Io.Duration.fromMilliseconds(1), .awake });
            try self.check(manager);
        }
    }
};
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
        idle: bool = false,
        retire_requested: bool = false,
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
            if (page.pins == 0) {
                if (page.retire_requested) owner.retireLocked(self.id) else owner.addIdle(self.id);
            }
            owner.changed.broadcast(io);
        }
    };
    // Only resident, unpinned frames enter the eviction queue. Logical handles
    // and retired slots never require directory-wide searches.
    fn addIdle(self: *Pager, id: usize) void {
        const page = &self.pages.items[id];
        std.debug.assert(!page.idle);
        page.idle = true;
        page.previous = self.idle_tail;
        page.next = null;
        if (self.idle_tail) |tail| self.pages.items[tail].next = id else self.idle_head = id;
        self.idle_tail = id;
    }
    fn removeIdle(self: *Pager, id: usize) void {
        const page = &self.pages.items[id];
        std.debug.assert(page.idle);
        page.idle = false;
        if (page.previous) |previous| self.pages.items[previous].next = page.next else self.idle_head = page.next;
        if (page.next) |next| self.pages.items[next].previous = page.previous else self.idle_tail = page.previous;
        page.previous = null;
        page.next = null;
    }
    pub fn create(self: *Pager) !usize {
        return self.createFor(.{});
    }
    pub fn createFor(self: *Pager, access: Access) !usize {
        try access.lock(self.manager, &self.mutex);
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
    // Restore directory ownership on every exit, including denied reentry. A
    // failed authority check permits only bounded metadata rollback afterward.
    fn relock(self: *Pager, access: Access) !void {
        access.lock(self.manager, &self.mutex) catch |err| {
            self.mutex.lockUncancelable(self.manager.io);
            return err;
        };
    }
    fn pause(self: *Pager, access: Access, sleep: bool) !void {
        self.mutex.unlock(self.manager.io);
        const result = if (sleep) waits.run(access.boundary(self.manager), std.Io.sleep, .{ self.manager.io, std.Io.Duration.fromMilliseconds(1), .awake }) else @as(anyerror!void, {});
        if (result) |_| {
            try self.relock(access);
        } else |err| {
            self.mutex.lockUncancelable(self.manager.io);
            return err;
        }
    }
    fn transfer(self: *Pager, id: usize, data: []align(16) u8, write: bool, access: Access) !u64 {
        const saved = self.pages.items[id].offset;
        self.transfers += 1;
        self.mutex.unlock(self.manager.io);
        const result = self.transferUnlocked(id, saved, data, write, access);
        if (result) |_| {
            self.relock(access) catch |err| {
                self.transfers -= 1;
                self.changed.broadcast(self.manager.io);
                return err;
            };
        } else |_| self.mutex.lockUncancelable(self.manager.io);
        self.transfers -= 1;
        self.changed.broadcast(self.manager.io);
        return result;
    }
    fn transferUnlocked(self: *Pager, id: usize, saved: ?u64, data: []align(16) u8, write: bool, access: Access) !u64 {
        try access.lock(self.manager, &self.file_mutex);
        defer self.file_mutex.unlock(self.manager.io);
        if (self.file == null) {
            std.debug.assert(write);
            self.file = try self.manager.createFor(access.execution);
            // Creation borrows its caller; the shared file never retains it.
            self.file.?.execution = null;
            self.file.?.buffer_bytes = 0;
        }
        const offset = saved orelse self.file.?.size;
        const Receipt = struct {
            owner: *Pager,
            id: usize,
            fn publish(raw: *anyopaque, position: u64, completed: bool) void {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                const pager = self_.owner;
                pager.mutex.lockUncancelable(pager.manager.io);
                defer pager.mutex.unlock(pager.manager.io);
                const page = &pager.pages.items[self_.id];
                std.debug.assert(page.live and page.busy and page.pins == 0);
                page.offset = position;
                if (completed) page.dirty = false;
            }
        };
        var receipt: Receipt = .{ .owner = self, .id = id };
        if (write) try self.file.?.writePage(offset, data, access, &receipt, Receipt.publish) else try self.file.?.readPage(offset, data, access);
        return offset;
    }
    fn evict(self: *Pager, access: Access) !bool {
        const id = self.idle_head orelse return false;
        self.directory_steps += 1;
        self.removeIdle(id);
        self.pages.items[id].busy = true;
        self.inflight += 1;
        defer {
            self.pages.items[id].busy = false;
            self.inflight -= 1;
            if (self.pages.items[id].retire_requested) self.retireLocked(id);
            self.changed.broadcast(self.manager.io);
        }
        errdefer if (!self.pages.items[id].retire_requested) self.addIdle(id);
        const data = self.pages.items[id].data.?;
        if (self.pages.items[id].dirty) {
            const offset = try self.transfer(id, data, true, access);
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
        return self.pinFor(id, write, .{});
    }
    pub fn pinFor(self: *Pager, id: usize, write: bool, access: Access) !Lease {
        const io = self.manager.io;
        try access.lock(self.manager, &self.mutex);
        defer self.mutex.unlock(io);
        if (id >= self.pages.items.len or !self.pages.items[id].live or self.pages.items[id].retire_requested) return error.InvalidSqlSpill;
        while (self.pages.items[id].busy) try self.pause(access, true);
        if (!self.pages.items[id].live or self.pages.items[id].retire_requested) return error.InvalidSqlSpill;
        if (self.pages.items[id].data == null) {
            self.pages.items[id].busy = true;
            self.inflight += 1;
            defer {
                self.pages.items[id].busy = false;
                self.inflight -= 1;
                if (self.pages.items[id].retire_requested and self.pages.items[id].pins == 0) self.retireLocked(id);
                self.changed.broadcast(io);
            }
            while (self.resident +| page_bytes > self.limit or
                (if (@import("memory_budget.zig").headroom(self.manager.allocator())) |available| available < page_bytes + 4096 else false))
            {
                try self.pause(access, false);
                if (!try self.evict(access)) {
                    if (self.transfers != 0) {
                        try self.pause(access, true);
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
                _ = try self.transfer(id, data, false, access);
                self.restores += 1;
            } else {
                @memset(data, 0);
                self.pages.items[id].dirty = true;
            }
            if (self.pages.items[id].retire_requested) return error.InvalidSqlSpill;
            self.pages.items[id].data = data;
            self.pages.items[id].fresh = false;
        } else if (self.pages.items[id].pins == 0) self.removeIdle(id);
        const page = &self.pages.items[id];
        page.pins += 1;
        self.pins += 1;
        page.dirty = page.dirty or write;
        return .{ .owner = self, .id = id, .data = page.data.? };
    }
    /// Cancellation cannot wait on another worker's CPU reentry. A busy or
    /// leased payload remains pager-owned until its final transfer/lease ends.
    pub fn retire(self: *Pager, id: usize) void {
        const io = self.manager.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const page = &self.pages.items[id];
        std.debug.assert(page.live and !page.retire_requested);
        page.retire_requested = true;
        if (!page.busy and page.pins == 0) self.retireLocked(id);
    }
    fn retireLocked(self: *Pager, id: usize) void {
        const page = &self.pages.items[id];
        std.debug.assert(page.live and page.pins == 0 and !page.busy);
        if (page.data) |data| {
            if (page.idle) self.removeIdle(id);
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
            return self.appendFor(value, .{});
        }
        pub fn appendFor(self: *Self, value: T, access: Access) !void {
            const page = self.len / per_page;
            if (page == self.handles.items.len) {
                const id = try self.pager.createFor(access);
                errdefer self.pager.retire(id);
                try self.handles.append(self.pager.manager.allocator(), id);
            }
            const lease = try self.pager.pinFor(self.handles.items[page], true, access);
            defer lease.release();
            const values: []T = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
            values[self.len % per_page] = value;
            self.len += 1;
        }
        pub fn appendSlice(self: *Self, input: []const T) !void {
            return self.appendSliceFor(input, .{});
        }
        pub fn appendSliceFor(self: *Self, input: []const T, access: Access) !void {
            var begin: usize = 0;
            while (begin < input.len) {
                const page = self.len / per_page;
                if (page == self.handles.items.len) {
                    const id = try self.pager.createFor(access);
                    errdefer self.pager.retire(id);
                    try self.handles.append(self.pager.manager.allocator(), id);
                }
                const lease = try self.pager.pinFor(self.handles.items[page], true, access);
                defer lease.release();
                const values = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
                const count = @min(per_page - self.len % per_page, input.len - begin);
                @memcpy(values[self.len % per_page ..][0..count], input[begin..][0..count]);
                self.len += count;
                begin += count;
            }
        }
        pub fn appendRepeated(self: *Self, value: T, count: usize) !void {
            return self.appendRepeatedFor(value, count, .{});
        }
        pub fn appendRepeatedFor(self: *Self, value: T, count: usize, access: Access) !void {
            var remaining = count;
            while (remaining != 0) {
                const page = self.len / per_page;
                if (page == self.handles.items.len) {
                    const id = try self.pager.createFor(access);
                    errdefer self.pager.retire(id);
                    try self.handles.append(self.pager.manager.allocator(), id);
                }
                const lease = try self.pager.pinFor(self.handles.items[page], true, access);
                defer lease.release();
                const values = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
                const length = @min(per_page - self.len % per_page, remaining);
                @memset(values[self.len % per_page ..][0..length], value);
                self.len += length;
                remaining -= length;
            }
        }
        pub fn getMany(self: Self, indices: []const usize, output: []T) !void {
            return self.getManyFor(indices, output, .{});
        }
        pub fn getManyFor(self: Self, indices: []const usize, output: []T, access: Access) !void {
            if (indices.len != output.len) return error.InvalidSqlSpill;
            for (indices) |index| if (index >= self.len) return error.InvalidSqlSpill;
            var begin: usize = 0;
            while (begin < indices.len) {
                const end = @min(indices.len, begin + 128);
                var done: [128]bool = @splat(false);
                for (indices[begin..end], 0..) |index, lane| {
                    if (done[lane]) continue;
                    const window = try self.pinPageFor(index, false, access);
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
            return self.pinFor(index, write, .{});
        }
        pub fn pinFor(self: Self, index: usize, write: bool, access: Access) !Pinned {
            if (index >= self.len) return error.InvalidSqlSpill;
            const lease = try self.pager.pinFor(self.handles.items[index / per_page], write, access);
            const values: []T = std.mem.bytesAsSlice(T, lease.data[0 .. per_page * @sizeOf(T)]);
            return .{ .lease = lease, .value = &values[index % per_page] };
        }
        pub const Window = struct { lease: Pager.Lease, values: []T, begin: usize };
        pub fn pageStart(_: Self, index: usize) usize {
            return index / per_page * per_page;
        }
        pub fn pinPage(self: Self, index: usize, write: bool) !Window {
            return self.pinPageFor(index, write, .{});
        }
        pub fn pinPageFor(self: Self, index: usize, write: bool, access: Access) !Window {
            if (index >= self.len) return error.InvalidSqlSpill;
            const begin = self.pageStart(index);
            const lease = try self.pager.pinFor(self.handles.items[index / per_page], write, access);
            const length: usize = @min(per_page, self.len - begin);
            return .{ .lease = lease, .values = std.mem.bytesAsSlice(T, lease.data[0 .. length * @sizeOf(T)]), .begin = begin };
        }
        pub fn get(self: Self, index: usize) !T {
            return self.getFor(index, .{});
        }
        pub fn getFor(self: Self, index: usize, access: Access) !T {
            const pinned = try self.pinFor(index, false, access);
            defer pinned.release();
            return pinned.value.*;
        }
        pub fn set(self: Self, index: usize, value: T) !void {
            return self.setFor(index, value, .{});
        }
        pub fn setFor(self: Self, index: usize, value: T, access: Access) !void {
            const pinned = try self.pinFor(index, true, access);
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

test "SQL execution pager scoped transfers retain receipts and defer busy retirement without borrowing the parent" {
    const Case = enum { success, begin_denied, resume_denied, partial_write, contended, retire_busy, restore_denied, retire_restore };
    const Probe = struct {
        owner: *Runnable.Scheduler,
        child: *Runnable.Execution,
        pager: *Pager,
        case: Case,
        first: usize,
        second: usize,
        calls: usize = 0,
        retired: bool = false,
        locked: bool = false,
        fn original(_: *anyopaque) !void {
            return error.InvalidRunnableTransition;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.child.checkpoint();
            if (self.case == .begin_denied) return error.FileNotFound;
            try self.child.park(.resource_blocked);
            if (self.locked) {
                self.pager.file_mutex.unlock(std.testing.io);
                self.locked = false;
            }
            if (!self.retired and (self.case == .retire_busy or (self.case == .retire_restore and self.pager.pages.items[self.second].data == null))) {
                self.pager.retire(self.first);
                self.retired = true;
                try std.testing.expect(self.pager.resident != 0);
                try std.testing.expect(self.pager.pages.items[self.first].retire_requested);
            }
            var control: Runnable.Execution = .{};
            try control.start(self.owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try control.checkpoint();
            control.finish();
            self.calls += 1;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.case == .resume_denied or (self.case == .restore_denied and self.pager.manager.read_calls != 0)) return error.FileNotFound;
            try self.child.enter(.query);
        }
    };
    const Fault = struct {
        var calls: usize = 0;
        var active: bool = false;
        fn write(raw: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
            if (active) {
                calls += 1;
                if (calls == 1) return std.testing.io.vtable.fileWritePositional(raw, file, header, &.{data[0][0..7]}, splat, offset);
                active = false;
                return error.NoSpaceLeft;
            }
            return std.testing.io.vtable.fileWritePositional(raw, file, header, data, splat, offset);
        }
    };
    const a = std.testing.allocator;
    for (std.enums.values(Case)) |case| {
        Fault.calls = 0;
        Fault.active = case == .partial_write;
        var io_vtable = std.testing.io.vtable.*;
        io_vtable.fileWritePositional = Fault.write;
        const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &io_vtable };
        var owner = try Runnable.Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_operations = 16 });
        defer owner.close();
        var parent: Runnable.Execution = .{};
        try parent.start(&owner, .{ .io = std.testing.io }, .query);
        defer parent.finish();
        var child: Runnable.Execution = .{};
        try child.reserveChild(&parent, std.testing.io, 64 * 1024);
        defer child.finish();
        try parent.park(.resource_blocked);
        try child.activateChild();
        var marker: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = io, .context = &marker, .checkpoint = Probe.original, .cpu_scope = .{ .execution = &parent, .allocator = a, .io = std.testing.io, .wait = spill.childWait(&parent) } };
        defer manager.deinit();
        var pager: Pager = .{ .manager = &manager, .limit = page_bytes };
        defer pager.deinit();
        const access: Access = .{ .execution = &child };
        const first = try pager.createFor(access);
        const second = try pager.createFor(access);
        const initial = try pager.pinFor(first, true, access);
        initial.data[0] = 37;
        initial.release();
        try std.testing.expectError(error.ExecutionWaitUnsupported, pager.pin(first, false));
        if (case == .restore_denied or case == .retire_restore) {
            const staged = try pager.pinFor(second, true, access);
            staged.release();
        }
        var probe: Probe = .{ .owner = &owner, .child = &child, .pager = &pager, .case = case, .first = first, .second = second };
        if (case == .contended) {
            try std.testing.expect(pager.file_mutex.tryLock());
            probe.locked = true;
        }
        const operation = pager.pinFor(if (case == .restore_denied or case == .retire_restore) first else second, true, .{ .execution = &child, .wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end } });
        if (case == .restore_denied or case == .retire_restore) {
            try std.testing.expectError(if (case == .restore_denied) error.FileNotFound else error.InvalidSqlSpill, operation);
            try std.testing.expect(pager.pages.items[first].data == null);
            try std.testing.expectEqual(@as(usize, 0), pager.resident);
            try std.testing.expectEqual(@as(usize, 0), pager.inflight);
            try std.testing.expectEqual(@as(u64, 2 * page_bytes), manager.live_bytes);
        } else if (case == .begin_denied or case == .resume_denied or case == .partial_write) {
            try std.testing.expectError(if (case == .partial_write) error.NoSpaceLeft else error.FileNotFound, operation);
            try std.testing.expectEqual(@as(usize, 0), pager.inflight);
            try std.testing.expectEqual(@as(usize, 0), pager.transfers);
            try std.testing.expect(pager.pages.items[first].data != null);
            if (case == .begin_denied) {
                try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
                try std.testing.expect(pager.pages.items[first].offset == null);
            } else {
                try std.testing.expectEqual(@as(u64, page_bytes), manager.live_bytes);
                try std.testing.expectEqual(@as(?u64, 0), pager.pages.items[first].offset);
                try std.testing.expectEqual(case == .partial_write, pager.pages.items[first].dirty);
            }
        } else {
            const held = try operation;
            held.release();
            try std.testing.expect(probe.calls > 0);
            if (case == .retire_busy) {
                try std.testing.expect(!pager.pages.items[first].live);
                const recycled = try pager.createFor(access);
                try std.testing.expectEqual(first, recycled);
                const fresh = try pager.pinFor(recycled, false, access);
                try std.testing.expectEqual(@as(u8, 0), fresh.data[0]);
                fresh.release();
            }
        }
        try std.testing.expect(pager.file.?.execution == null);
        child.finish();
        try parent.enter(.query);
        var healthy: Runnable.Execution = .{};
        try healthy.reserveChild(&parent, std.testing.io, 64 * 1024);
        defer healthy.finish();
        try parent.park(.resource_blocked);
        try healthy.activateChild();
        if (case != .retire_busy and case != .retire_restore) {
            const current: Access = .{ .execution = &healthy };
            const other = try pager.pinFor(second, true, current);
            other.release();
            const restored = try pager.pinFor(first, false, current);
            try std.testing.expectEqual(@as(u8, 37), restored.data[0]);
            restored.release();
        }
        const borrowed = try pager.pinFor(second, false, .{ .execution = &healthy });
        pager.retire(second);
        try std.testing.expect(pager.pages.items[second].live);
        borrowed.release();
        try std.testing.expect(!pager.pages.items[second].live);
        try std.testing.expect(pager.peak <= pager.limit);
        healthy.finish();
    }
}
