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

//! Bounded named directory events. Kernel loss never certifies a clean tree.
//! Linux uses one inotify subscription per directory. macOS uses strict inode
//! and directory witnesses instead of asynchronous recursive event hints.
const std = @import("std");
const builtin = @import("builtin");
const linux = builtin.os.tag == .linux;
const supported = builtin.link_libc and linux;
const A = std.mem.Allocator;
pub const Events = struct {
    io: std.Io,
    budget: @import("antfly_regex").Budget,
    directories: std.StringHashMapUnmanaged(c_int) = .empty,
    handles: std.AutoHashMapUnmanaged(c_int, []const u8) = .empty,
    paths: std.StringHashMapUnmanaged(bool) = .empty,
    fd: c_int = -1,
    failed: bool = !supported,
    epoch: u64 = 0,
    trace: bool = false,
    mutex: std.Io.Mutex = .init,
    pub fn init(a: A, io: std.Io) Events {
        return .{ .io = io, .budget = .{ .backing = a, .limit = 2 * 1024 * 1024 } };
    }
    pub fn lock(self: *Events) void {
        self.mutex.lockUncancelable(self.io);
    }
    pub fn unlock(self: *Events) void {
        self.mutex.unlock(self.io);
    }
    fn record(self: *Events, path: []const u8, directory: bool) void {
        // Ignore paths outside tracked discovery directories before allocating.
        if (!self.directories.contains(std.fs.path.dirname(path) orelse "/") and !self.directories.contains(path)) return;
        self.epoch +%= 1;
        if (self.epoch == 0) self.failed = true;
        const a = self.budget.allocator();
        if (self.paths.getPtr(path)) |existing| {
            existing.* = existing.* or directory;
            return;
        }
        const key = a.dupe(u8, path) catch {
            self.failed = true;
            return;
        };
        self.paths.put(a, key, directory) catch {
            a.free(key);
            self.failed = true;
        };
    }
    pub fn fail(self: *Events) void {
        self.lock();
        defer self.unlock();
        self.failed = true;
    }
    pub fn isFailed(self: *Events) bool {
        self.lock();
        defer self.unlock();
        return self.failed;
    }
    pub fn checkpoint(self: *Events) u64 {
        self.lock();
        defer self.unlock();
        return self.epoch;
    }
    pub fn retire(self: *Events, epoch: u64) void {
        self.lock();
        defer self.unlock();
        // A callback racing publication must remain visible next refresh.
        if (self.epoch != epoch) return;
        self.clearLocked();
    }
    fn clearLocked(self: *Events) void {
        var it = self.paths.keyIterator();
        while (it.next()) |key| self.budget.allocator().free(key.*);
        self.paths.clearRetainingCapacity();
    }
    pub fn clear(self: *Events) void {
        self.lock();
        defer self.unlock();
        self.clearLocked();
    }
    pub fn deinit(self: *Events) void {
        if (comptime linux and supported) if (self.fd >= 0) {
            _ = std.c.close(self.fd);
        };
        self.clear();
        const a = self.budget.allocator();
        self.paths.deinit(a);
        self.handles.deinit(a);
        var it = self.directories.keyIterator();
        while (it.next()) |key| a.free(key.*);
        self.directories.deinit(a);
        self.paths = .empty;
        self.directories = .empty;
        self.handles = .empty;
        self.fd = -1;
    }
    pub fn track(self: *Events, directory: []const u8) void {
        if (comptime !supported) return;
        self.lock();
        defer self.unlock();
        if (self.failed) return;
        const a = self.budget.allocator();
        const path = std.Io.Dir.cwd().realPathFileAlloc(self.io, directory, a) catch {
            self.failed = true;
            return;
        };
        defer a.free(path);
        if (self.directories.contains(path)) return;
        const key = a.dupe(u8, path) catch {
            self.failed = true;
            return;
        };
        self.directories.put(a, key, -1) catch {
            a.free(key);
            self.failed = true;
            return;
        };
        {
            if (self.fd < 0) self.fd = std.c.inotify_init1(std.os.linux.IN.NONBLOCK | std.os.linux.IN.CLOEXEC);
            if (self.fd < 0) {
                self.failed = true;
                return;
            }
            const IN = std.os.linux.IN;
            const handle = std.c.inotify_add_watch(self.fd, path, IN.MODIFY | IN.CLOSE_WRITE | IN.ATTRIB | IN.CREATE | IN.DELETE | IN.MOVED_FROM | IN.MOVED_TO | IN.DELETE_SELF | IN.MOVE_SELF | IN.UNMOUNT | IN.ONLYDIR);
            if (handle < 0) {
                self.failed = true;
                return;
            }
            self.directories.getPtr(key).?.* = handle;
            self.handles.put(a, handle, key) catch {
                self.failed = true;
            };
        }
    }
    pub fn poll(self: *Events) void {
        if (comptime !supported) return;
        if (self.fd >= 0 and !self.failed) {
            const IN = std.os.linux.IN;
            var buffer: [16384]u8 = undefined;
            for (0..64) |_| {
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
                const Header = extern struct { wd: i32, mask: u32, cookie: u32, len: u32 };
                var offset: usize = 0;
                const used: usize = @intCast(count);
                while (offset < used) {
                    if (used - offset < @sizeOf(Header)) {
                        self.failed = true;
                        return;
                    }
                    const event = std.mem.bytesToValue(Header, buffer[offset..][0..@sizeOf(Header)]);
                    offset += @sizeOf(Header);
                    if (event.len > used - offset or event.mask & (IN.Q_OVERFLOW | IN.IGNORED | IN.UNMOUNT | IN.MOVE_SELF | IN.DELETE_SELF) != 0) {
                        self.failed = true;
                        return;
                    }
                    if (self.handles.get(event.wd)) |directory| {
                        const raw = buffer[offset..][0..event.len];
                        const end = std.mem.indexOfScalar(u8, raw, 0) orelse {
                            self.failed = true;
                            return;
                        };
                        const path = std.fs.path.join(self.budget.allocator(), &.{ directory, raw[0..end] }) catch {
                            self.failed = true;
                            return;
                        };
                        defer self.budget.allocator().free(path);
                        if (self.trace) std.debug.print("antfly-grep: directory event flags=0x{x} path={s}\n", .{ event.mask, path });
                        self.record(path, event.mask & IN.ISDIR != 0);
                    }
                    offset += event.len;
                }
            }
            self.failed = true;
        }
    }
};

test "event retirement preserves callbacks racing publication and stays bounded" {
    var events = Events.init(std.testing.allocator, std.testing.io);
    defer events.deinit();
    const directory = try events.budget.allocator().dupe(u8, "/source");
    try events.directories.put(events.budget.allocator(), directory, -1);
    events.record("/source/a", false);
    const checkpoint = events.checkpoint();
    events.record("/source/b", false);
    events.retire(checkpoint);
    try std.testing.expectEqual(@as(usize, 2), events.paths.count());
    events.retire(events.checkpoint());
    try std.testing.expectEqual(@as(usize, 0), events.paths.count());
    events.budget.limit = events.budget.live;
    events.record("/source/c", false);
    try std.testing.expect(events.isFailed());
}
