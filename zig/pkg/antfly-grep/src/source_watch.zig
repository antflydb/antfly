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

//! Per-file kernel witnesses, used only to skip unchanged content reads.
//! Uncertified callers open/stat paths and check device/inode + stamps. The
//! optional discovery journal may use raw intact tokens only while namespace,
//! configuration and ancestor coverage remain complete.
//! Missing watches, failures, overflow and watch loss fail closed. No events
//! change the published immutable snapshot; refresh remains explicit.
const std = @import("std");
const builtin = @import("builtin");
const supported = builtin.link_libc and (builtin.os.tag == .macos or builtin.os.tag == .linux);
pub const Token = struct { slot: usize, generation: u64, sequence: u64 };
pub const Watch = struct {
    fd: c_int = -1,
    failed: bool = false,
    // Strict kqueue witnesses consume one FD per inode. Reserve descriptor
    // headroom and fall back explicitly above this bounded admission budget.
    limit: usize = 4096,
    generation: u64 = 0,
    epoch: u64 = 0,
    events: [4096]u16 = undefined,
    event_count: usize = 0,
    event_seen: [4096]bool = @splat(false),
    handles: std.AutoHashMapUnmanaged(c_int, usize) = .empty,
    identities: std.AutoHashMapUnmanaged(Identity, usize) = .empty,
    slots: [4096]Slot = @splat(.{}),
    occupied: [64]u64 = @splat(0),
    free_groups: u64 = std.math.maxInt(u64),
    registration_attempts: usize = 0,
    const Identity = struct { device: u64, inode: u64 };
    const Slot = struct { handle: c_int = -1, users: usize = 0, dirty: bool = true, generation: u64 = 0, sequence: u64 = 0, device: u64 = 0, inode: u64 = 0, mask: u32 = 0 };
    pub fn clearEvents(self: *Watch) void {
        self.event_count = 0;
        @memset(&self.event_seen, false);
    }
    fn note(self: *Watch, i: usize) void {
        if (!self.event_seen[i]) {
            self.event_seen[i] = true;
            self.events[self.event_count] = @intCast(i);
            self.event_count += 1;
        }
    }
    pub fn init() Watch {
        var self: Watch = .{};
        self.open();
        return self;
    }
    fn open(self: *Watch) void {
        if (comptime !supported) return;
        var quota: std.c.rlimit = undefined;
        if (std.c.getrlimit(.NOFILE, &quota) == 0) self.limit = @min(self.limit, @as(usize, @intCast(quota.cur / 4)));
        if (comptime builtin.os.tag == .macos) {
            self.fd = std.c.kqueue();
            if (self.fd >= 0 and std.c.fcntl(self.fd, std.c.F.SETFD, @as(c_int, std.c.FD_CLOEXEC)) < 0) {
                _ = std.c.close(self.fd);
                self.fd = -1;
            }
        } else self.fd = std.c.inotify_init1(std.os.linux.IN.NONBLOCK | std.os.linux.IN.CLOEXEC);
    }
    pub fn deinit(self: *Watch) void {
        if (comptime !supported) return;
        for (&self.slots) |*slot| {
            if (comptime builtin.os.tag == .macos) if (slot.handle >= 0) {
                _ = std.c.close(slot.handle);
            };
            slot.* = .{};
        }
        if (self.fd >= 0) _ = std.c.close(self.fd);
        self.fd = -1;
        self.handles.deinit(std.heap.page_allocator);
        self.handles = .empty;
        self.identities.deinit(std.heap.page_allocator);
        self.identities = .empty;
        self.clearEvents();
        @memset(&self.occupied, 0);
        self.free_groups = std.math.maxInt(u64);
    }
    pub fn reset(self: *Watch) void {
        self.deinit();
        self.epoch +%= 1;
        self.failed = false;
        self.open();
    }
    pub fn failClosed(self: *Watch) void {
        self.failed = true;
    }
    pub fn beginRefresh(self: *Watch) void {
        self.poll();
        if (self.failed or self.fd < 0) self.reset();
    }
    fn identity(fd: c_int) ?Identity {
        if (comptime !supported) return null;
        if (comptime builtin.os.tag == .linux) {
            const linux = std.os.linux;
            var stat: linux.Statx = undefined;
            if (linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .INO = true }, &stat) != 0 or !stat.mask.INO) return null;
            return .{ .device = (@as(u64, stat.dev_major) << 32) | stat.dev_minor, .inode = stat.ino };
        }
        var stat: std.c.Stat = undefined;
        if (std.c.fstat(fd, &stat) != 0) return null;
        return .{ .device = if (comptime builtin.os.tag == .macos) @as(u32, @bitCast(stat.dev)) else @intCast(stat.dev), .inode = @intCast(stat.ino) };
    }
    pub fn arm(self: *Watch, file: std.Io.File) ?Token {
        return self.armKind(file, false);
    }
    pub fn armDirectory(self: *Watch, dir: std.Io.Dir) ?Token {
        return self.armKind(.{ .handle = dir.handle, .flags = .{ .nonblocking = false } }, true);
    }
    fn freeSlot(self: *const Watch) ?usize {
        const limit = @min(self.limit, self.slots.len);
        if (limit == 0) return null;
        const groups = std.math.divCeil(usize, limit, 64) catch unreachable;
        const allowed = if (groups == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(groups)) - 1;
        const available = self.free_groups & allowed;
        if (available == 0) return null;
        const group: usize = @ctz(available);
        const count = @min(64, limit - group * 64);
        const mask = if (count == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(count)) - 1;
        const bits = ~self.occupied[group] & mask;
        if (bits == 0) return null;
        return group * 64 + @as(usize, @ctz(bits));
    }
    fn linuxMask(directory: bool) u32 {
        const IN = std.os.linux.IN;
        const changes: u32 = if (directory) IN.CREATE | IN.DELETE | IN.MOVED_FROM | IN.MOVED_TO else IN.MODIFY | IN.CLOSE_WRITE;
        return changes | IN.ATTRIB | IN.DELETE_SELF | IN.MOVE_SELF | IN.UNMOUNT;
    }
    fn linuxArm(self: *Watch, file: std.Io.File, mask: u32) c_int {
        var path: [64]u8 = undefined;
        const name = std.mem.printSentinel(&path, "/proc/self/fd/{d}", .{file.handle}, 0) catch return -1;
        self.registration_attempts += 1;
        return std.c.inotify_add_watch(self.fd, name, mask | std.os.linux.IN.MASK_ADD);
    }
    fn armKind(self: *Watch, file: std.Io.File, directory: bool) ?Token {
        if (comptime !supported) return null;
        if (self.fd < 0 or self.failed) return null;
        const id = identity(file.handle) orelse return null;
        // Full-table misses cannot acquire a witness. Do not drain the kernel
        // queue for each of the remaining files in a large cold capture. Keep
        // identity reuse (including hard links) and poll before slot recycling.
        // Capture/refresh still performs its final publication barrier.
        if (!self.identities.contains(id) and self.freeSlot() == null) return null;
        self.poll();
        if (self.fd < 0 or self.failed) return null;
        const mask = if (comptime builtin.os.tag == .linux) linuxMask(directory) else 0;
        if (self.identities.get(id)) |i| {
            const slot = &self.slots[i];
            if (slot.handle >= 0) {
                if (comptime builtin.os.tag == .linux) if (mask & ~slot.mask != 0) {
                    const handle = self.linuxArm(file, mask);
                    if (handle < 0) return null;
                    if (handle != slot.handle) {
                        _ = std.c.inotify_rm_watch(self.fd, handle);
                        self.failClosed();
                        return null;
                    }
                    slot.mask |= mask;
                };
                slot.users += 1;
                return .{ .slot = i, .generation = slot.generation, .sequence = slot.sequence };
            }
            _ = self.identities.remove(id);
        }
        // Admission precedes any kernel registration. A full table still
        // permits identity reuse and mask upgrades, with no linear slot walk.
        const free = self.freeSlot() orelse return null;
        var handle: c_int = -1;
        if (comptime builtin.os.tag == .linux) {
            handle = self.linuxArm(file, mask);
            if (handle < 0) return null;
            if (self.handles.contains(handle)) {
                // A recycled kernel descriptor cannot certify an older inode.
                self.failClosed();
                return null;
            }
        } else {
            self.registration_attempts += 1;
            handle = std.c.fcntl(file.handle, std.c.F.DUPFD_CLOEXEC, @as(c_int, 0));
            if (handle < 0) return null;
            var change: std.c.Kevent = .{ .ident = @intCast(handle), .filter = std.c.EVFILT.VNODE, .flags = std.c.EV.ADD | std.c.EV.CLEAR, .fflags = std.c.NOTE.DELETE | std.c.NOTE.WRITE | std.c.NOTE.EXTEND | std.c.NOTE.ATTRIB | std.c.NOTE.LINK | std.c.NOTE.RENAME | std.c.NOTE.REVOKE, .data = 0, .udata = free + 1 };
            var events: [1]std.c.Kevent = undefined;
            if (std.c.kevent(self.fd, @ptrCast(&change), 1, &events, 0, null) < 0) {
                _ = std.c.close(handle);
                return null;
            }
        }
        if (comptime builtin.os.tag == .linux) self.handles.put(std.heap.page_allocator, handle, free) catch {
            _ = std.c.inotify_rm_watch(self.fd, handle);
            return null;
        };
        self.identities.put(std.heap.page_allocator, id, free) catch {
            if (comptime builtin.os.tag == .linux) {
                _ = self.handles.remove(handle);
                _ = std.c.inotify_rm_watch(self.fd, handle);
            } else _ = std.c.close(handle);
            return null;
        };
        self.generation +%= 1;
        self.slots[free] = .{ .handle = handle, .users = 1, .generation = self.generation, .device = id.device, .inode = id.inode, .mask = mask };
        const group = free / 64;
        self.occupied[group] |= @as(u64, 1) << @intCast(free % 64);
        if (self.occupied[group] == std.math.maxInt(u64)) self.free_groups &= ~(@as(u64, 1) << @intCast(group));
        return .{ .slot = free, .generation = self.generation, .sequence = 0 };
    }
    fn get(self: *Watch, token: Token) ?*Slot {
        if (self.failed or self.fd < 0) return null;
        const slot = &self.slots[token.slot];
        return if (slot.users != 0 and slot.handle >= 0 and slot.generation == token.generation) slot else null;
    }
    pub fn release(self: *Watch, token: Token) void {
        if (comptime !supported) return;
        // Cleanup remains possible after a poll failure.
        const slot = &self.slots[token.slot];
        if (slot.users == 0 or slot.generation != token.generation) return;
        slot.users -= 1;
        if (slot.users != 0) return;
        const id: Identity = .{ .device = slot.device, .inode = slot.inode };
        if (self.identities.get(id) == token.slot) _ = self.identities.remove(id);
        if (comptime builtin.os.tag == .macos) {
            if (slot.handle >= 0) _ = std.c.close(slot.handle);
        } else if (self.fd >= 0 and slot.handle >= 0) {
            _ = self.handles.remove(slot.handle);
            _ = std.c.inotify_rm_watch(self.fd, slot.handle);
        }
        slot.handle = -1;
        self.occupied[token.slot / 64] &= ~(@as(u64, 1) << @intCast(token.slot % 64));
        self.free_groups |= @as(u64, 1) << @intCast(token.slot / 64);
    }
    pub fn certify(self: *Watch, token: Token) void {
        self.poll();
        self.certifyPending(token);
    }
    /// Mark only the recorded sequence. The capture coordinator must drain
    /// events at its publication barrier before using intact() or reusing any
    /// source. Delayed writes then invalidate this checkpoint normally. This
    /// avoids an extra kernel poll per freshly read file in a capture batch.
    pub fn certifyPending(self: *Watch, token: Token) void {
        if (self.get(token)) |slot| if (slot.sequence == token.sequence) {
            slot.dirty = false;
        };
    }
    pub fn observed(self: *Watch, token: Token) ?Token {
        const slot = self.get(token) orelse return null;
        return .{ .slot = token.slot, .generation = slot.generation, .sequence = slot.sequence };
    }
    pub fn intact(self: *Watch, token: Token) bool {
        const slot = self.get(token) orelse return false;
        return !slot.dirty and token.sequence == slot.sequence;
    }
    pub fn clean(self: *Watch, token: Token, file: std.Io.File) bool {
        if (comptime !supported) return false;
        self.poll();
        const slot = self.get(token) orelse return false;
        if (slot.dirty or token.sequence != slot.sequence) return false;
        const id = identity(file.handle) orelse return false;
        return id.device == slot.device and id.inode == slot.inode;
    }
    /// Namespace rediscovery can retain a source without opening it only when
    /// both its inode witness and the current pathname identity still agree.
    pub fn unchangedPath(self: *Watch, token: Token, path: []const u8) bool {
        if (comptime !supported) return false;
        self.poll();
        if (!self.intact(token) or !self.identityMatchesPath(token, path)) return false;
        self.poll();
        return self.intact(token);
    }
    pub fn identityMatchesPath(self: *Watch, token: Token, path: []const u8) bool {
        if (comptime !supported) return false;
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        const name = std.mem.printSentinel(&buffer, "{s}", .{path}, 0) catch return false;
        const slot = self.get(token) orelse return false;
        const id: Identity = if (comptime builtin.os.tag == .linux) blk: {
            const linux = std.os.linux;
            var stat: linux.Statx = undefined;
            if (linux.statx(linux.AT.FDCWD, name, 0, .{ .INO = true }, &stat) != 0 or !stat.mask.INO) return false;
            break :blk .{ .device = (@as(u64, stat.dev_major) << 32) | stat.dev_minor, .inode = stat.ino };
        } else blk: {
            var stat: std.c.Stat = undefined;
            if (std.c.stat(name, &stat) != 0) return false;
            break :blk .{ .device = @as(u32, @bitCast(stat.dev)), .inode = @intCast(stat.ino) };
        };
        return id.device == slot.device and id.inode == slot.inode;
    }
    fn dirty(slot: *Slot) void {
        slot.dirty = true;
        slot.sequence +%= 1;
    }
    pub fn poll(self: *Watch) void {
        if (comptime !supported) return;
        if (self.fd < 0 or self.failed) return;
        for (0..64) |_| {
            if (comptime builtin.os.tag == .macos) {
                var events: [64]std.c.Kevent = undefined;
                const timeout: std.c.timespec = .{ .sec = 0, .nsec = 0 };
                const count = std.c.kevent(self.fd, &.{}, 0, &events, events.len, &timeout);
                if (count < 0) {
                    if (std.posix.errno(count) == .INTR) continue;
                    self.failed = true;
                    return;
                }
                if (count == 0) return;
                for (events[0..@intCast(count)]) |event| {
                    if (event.flags & std.c.EV.ERROR != 0 or event.udata == 0 or event.udata > self.slots.len) {
                        self.failed = true;
                        return;
                    }
                    self.epoch +%= 1;
                    self.note(event.udata - 1);
                    dirty(&self.slots[event.udata - 1]);
                }
            } else {
                var buffer: [16384]u8 = undefined;
                const count = std.c.read(self.fd, &buffer, buffer.len);
                if (count < 0) {
                    switch (std.posix.errno(count)) {
                        .AGAIN => return,
                        .INTR => continue,
                        else => {
                            self.failed = true;
                            return;
                        },
                    }
                }
                if (count == 0) {
                    self.failed = true;
                    return;
                }
                const used: usize = @intCast(count);
                const Header = extern struct { wd: i32, mask: u32, cookie: u32, len: u32 };
                var offset: usize = 0;
                while (offset < used) {
                    if (@as(usize, @intCast(count)) - offset < @sizeOf(Header)) {
                        self.failed = true;
                        return;
                    }
                    const event = std.mem.bytesToValue(Header, buffer[offset..][0..@sizeOf(Header)]);
                    if (event.mask & std.os.linux.IN.Q_OVERFLOW != 0) {
                        self.failed = true;
                        return;
                    }
                    if (self.handles.get(event.wd)) |i| {
                        self.epoch +%= 1;
                        // Lost kernel subscriptions cannot be recertified by
                        // a later read. Unknown handles may be our own removals.
                        if (event.mask & std.os.linux.IN.IGNORED != 0) {
                            _ = self.handles.remove(event.wd);
                            self.slots[i].handle = -1;
                            const id: Identity = .{ .device = self.slots[i].device, .inode = self.slots[i].inode };
                            if (self.identities.get(id) == i) _ = self.identities.remove(id);
                        }
                        self.note(i);
                        dirty(&self.slots[i]);
                    }
                    const size = @sizeOf(Header) + @as(usize, event.len);
                    if (size > @as(usize, @intCast(count)) - offset) {
                        self.failed = true;
                        return;
                    }
                    offset += size;
                }
            }
        }
        // Event floods are treated like overflow rather than monopolizing a
        // refresh or assuming an unread suffix contains no relevant change.
        self.failed = true;
    }
};

test "kernel witnesses observe writes, bound admission and recover from watch loss" {
    if (comptime !supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "old" });
    var file = try tmp.dir.openFile(io, "a", .{});
    defer file.close(io);
    var watch = Watch.init();
    defer watch.deinit();
    const token = watch.arm(file) orelse return error.SkipZigTest;
    defer watch.release(token);
    watch.certify(token);
    try std.testing.expect(watch.clean(token, file));
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "new content" });
    try std.testing.expect(!watch.clean(token, file));
    const next = watch.arm(file).?;
    defer watch.release(next);
    watch.certify(next);
    try std.testing.expect(watch.clean(next, file));
    // Hard-linked paths share an inotify subscription. A newer reader may
    // certify its own checkpoint, never the older capture's checkpoint.
    try std.testing.expect(!watch.clean(token, file));
    if (comptime builtin.os.tag == .linux) {
        _ = std.c.inotify_rm_watch(watch.fd, watch.slots[next.slot].handle);
        try std.testing.expect(!watch.clean(next, file));
        watch.certify(next);
        try std.testing.expect(!watch.clean(next, file));
    }
    watch.failClosed();
    try std.testing.expect(!watch.clean(next, file));
    watch.beginRefresh();
    try std.testing.expect(!watch.clean(next, file));
    watch.limit = 0;
    try std.testing.expect(watch.arm(file) == null);
}

test "pathname identity rejects symlink retargeting independently of watch state" {
    if (comptime !supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "old" });
    try tmp.dir.writeFile(io, .{ .sub_path = "b", .data = "new" });
    try tmp.dir.symLink(io, "a", "link", .{});
    var file = try tmp.dir.openFile(io, "link", .{});
    defer file.close(io);
    var watch = Watch.init();
    defer watch.deinit();
    const token = watch.arm(file) orelse return error.SkipZigTest;
    defer watch.release(token);
    watch.certify(token);
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/link", .{tmp.sub_path});
    defer a.free(path);
    try std.testing.expect(watch.identityMatchesPath(token, path));
    try tmp.dir.deleteFile(io, "link");
    try tmp.dir.symLink(io, "b", "link", .{});
    try std.testing.expect(!watch.identityMatchesPath(token, path));
    try std.testing.expect(!watch.unchangedPath(token, path));
}

test "full witness admission avoids registrations but preserves reuse and recycled slots" {
    if (comptime !supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "a" });
    try tmp.dir.writeFile(io, .{ .sub_path = "b", .data = "b" });
    const a = try tmp.dir.openFile(io, "a", .{});
    defer a.close(io);
    const b = try tmp.dir.openFile(io, "b", .{});
    defer b.close(io);
    var watch = Watch.init();
    defer watch.deinit();
    watch.limit = 1;
    const first = watch.arm(a) orelse return error.SkipZigTest;
    const attempts = watch.registration_attempts;
    for (0..100) |_| try std.testing.expect(watch.arm(b) == null);
    const reused = watch.arm(a).?;
    try std.testing.expectEqual(first.slot, reused.slot);
    try std.testing.expectEqual(attempts, watch.registration_attempts);
    watch.release(first);
    try std.testing.expect(watch.arm(b) == null);
    watch.release(reused);
    const next = watch.arm(b).?;
    defer watch.release(next);
    try std.testing.expectEqual(first.slot, next.slot);
    try std.testing.expect(next.generation != first.generation);
    try std.testing.expectEqual(attempts + 1, watch.registration_attempts);
}

test "full witness table preserves hard links and Linux directory mask upgrades" {
    if (comptime !supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "a" });
    try tmp.dir.hardLink("a", tmp.dir, "alias", io, .{});
    const a = try tmp.dir.openFile(io, "a", .{});
    defer a.close(io);
    const alias = try tmp.dir.openFile(io, "alias", .{});
    defer alias.close(io);
    var watch = Watch.init();
    defer watch.deinit();
    watch.limit = 1;
    const first = watch.arm(a) orelse return error.SkipZigTest;
    const attempts = watch.registration_attempts;
    const linked = watch.arm(alias).?;
    try std.testing.expectEqual(first.slot, linked.slot);
    try std.testing.expectEqual(attempts, watch.registration_attempts);
    watch.release(first);
    watch.release(linked);
    if (comptime builtin.os.tag == .linux) {
        const file_token = watch.arm(.{ .handle = tmp.dir.handle, .flags = .{ .nonblocking = false } }).?;
        const before = watch.registration_attempts;
        const directory_token = watch.armDirectory(tmp.dir).?;
        defer watch.release(file_token);
        defer watch.release(directory_token);
        try std.testing.expectEqual(file_token.slot, directory_token.slot);
        try std.testing.expectEqual(before + 1, watch.registration_attempts);
        const mask = watch.slots[directory_token.slot].mask;
        try std.testing.expect(mask & std.os.linux.IN.MODIFY != 0);
        try std.testing.expect(mask & std.os.linux.IN.CREATE != 0);
        watch.certify(directory_token);
        try tmp.dir.writeFile(io, .{ .sub_path = "new", .data = "new" });
        watch.poll();
        try std.testing.expect(watch.slots[directory_token.slot].dirty);
    }
}

test "deferred certification preserves writes at the publication barrier" {
    if (comptime !supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "old" });
    const file = try tmp.dir.openFile(io, "a", .{});
    defer file.close(io);
    var watch = Watch.init();
    defer watch.deinit();
    const token = watch.arm(file) orelse return error.SkipZigTest;
    defer watch.release(token);
    watch.certifyPending(token);
    watch.poll();
    try std.testing.expect(watch.intact(token));
    try tmp.dir.writeFile(io, .{ .sub_path = "a", .data = "new" });
    // An unread write must not be erased by certification. Both writes queued
    // before this call and writes queued afterwards survive the final poll.
    watch.certifyPending(token);
    watch.poll();
    try std.testing.expect(!watch.intact(token));
    watch.certifyPending(token);
    try std.testing.expect(!watch.intact(token));
}
