// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const builtin = @import("builtin");
const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const http_common = @import("http_common.zig");

const observation_interval_ms: u64 = 25;
// Linux exposes POLLRDHUP under its GNU poll ABI, but Zig 0.16's
// std.os.linux.POLL omits the name. The kernel ABI value is stable and shared
// with EPOLLRDHUP; keep it explicitly typed for pollfd.events/revents.
const linux_poll_rdhup: i16 = 0x2000;

fn sleepMs(ms: u64) void {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

/// A bounded, multiplexed H1 peer-lifetime observer.
///
/// One owner thread watches every admitted request. This deliberately avoids
/// `Io.concurrent`: the threaded Io backend assigns a blocking poll to one
/// worker and permanently grows its worker pool for each simultaneous request.
/// The registry therefore keeps cancellation O(1) threads per listener while
/// retaining O(n) descriptors and a small, fixed polling cadence.
pub const Observer = struct {
    const Entry = struct {
        id: u64,
        fd: std.posix.fd_t,
        cancellation: *http_common.RequestCancellation,
        peer_disconnects_total: ?*std.atomic.Value(u64),
        observer_failures_total: ?*std.atomic.Value(u64),
        unread_input: bool = false,
    };

    pub const Registration = struct {
        observer: ?*Observer = null,
        id: u64 = 0,

        pub fn deinit(self: *Registration) void {
            const observer = self.observer orelse return;
            observer.unregister(self.id);
            self.* = .{};
        }
    };

    alloc: std.mem.Allocator,
    capacity: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_id: u64 = 1,
    active_count: std.atomic.Value(usize) = .init(0),
    stopping: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    /// A single kqueue observes EOF behind unread pipelined bytes on Darwin.
    kernel_fd: ?std.posix.fd_t = null,

    pub fn init(alloc: std.mem.Allocator, capacity: usize) Observer {
        return .{ .alloc = alloc, .capacity = capacity };
    }

    pub fn start(self: *Observer) !void {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
        if (self.thread != null) return error.AlreadyStarted;
        try self.entries.ensureTotalCapacity(self.alloc, self.capacity);
        if (comptime builtin.os.tag == .macos) {
            const raw = std.posix.system.kqueue();
            if (std.posix.errno(raw) != .SUCCESS) return error.ObserverUnavailable;
            self.kernel_fd = @intCast(raw);
        }
        errdefer if (self.kernel_fd) |fd| {
            _ = std.posix.system.close(fd);
            self.kernel_fd = null;
        };
        self.stopping.store(false, .release);
        self.thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, run, .{self});
    }

    pub fn deinit(self: *Observer) void {
        self.stopping.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.thread = null;
        if (self.kernel_fd) |fd| _ = std.posix.system.close(fd);
        self.kernel_fd = null;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        // Owners unregister synchronously before their cancellation tokens leave
        // scope. Any residue here indicates a lifecycle bug and cannot be read.
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(self.active_count.load(.acquire) == 0);
        self.entries.deinit(self.alloc);
    }

    pub fn register(
        self: *Observer,
        fd: std.posix.fd_t,
        cancellation: *http_common.RequestCancellation,
        peer_disconnects_total: ?*std.atomic.Value(u64),
        observer_failures_total: ?*std.atomic.Value(u64),
    ) !Registration {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return .{};
        if (self.thread == null or self.stopping.load(.acquire)) return error.ObserverUnavailable;

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.entries.items.len >= self.capacity) return error.ObserverCapacityExceeded;

        const id = self.nextId();
        if (comptime builtin.os.tag == .macos) try self.updateKqueue(fd, id, true);
        self.entries.appendAssumeCapacity(.{
            .id = id,
            .fd = fd,
            .cancellation = cancellation,
            .peer_disconnects_total = peer_disconnects_total,
            .observer_failures_total = observer_failures_total,
        });
        _ = self.active_count.fetchAdd(1, .release);
        return .{ .observer = self, .id = id };
    }

    pub fn activeCount(self: *const Observer) usize {
        return self.active_count.load(.acquire);
    }

    fn nextId(self: *Observer) u64 {
        while (true) {
            const id = self.next_id;
            self.next_id +%= 1;
            if (id != 0) return id;
        }
    }

    fn unregister(self: *Observer, id: u64) void {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, index| {
            if (entry.id != id) continue;
            if (comptime builtin.os.tag == .macos) self.updateKqueue(entry.fd, id, false) catch {};
            _ = self.entries.swapRemove(index);
            _ = self.active_count.fetchSub(1, .release);
            return;
        }
    }

    fn run(self: *Observer) void {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
        if (comptime builtin.os.tag == .macos) return self.runKqueue();
        return self.runPoll();
    }

    fn runPoll(self: *Observer) void {
        var fds: std.ArrayListUnmanaged(std.posix.pollfd) = .empty;
        defer fds.deinit(self.alloc);
        var ids: std.ArrayListUnmanaged(u64) = .empty;
        defer ids.deinit(self.alloc);
        fds.ensureTotalCapacity(self.alloc, self.capacity) catch return self.stopAfterRuntimeFailure();
        ids.ensureTotalCapacity(self.alloc, self.capacity) catch return self.stopAfterRuntimeFailure();

        while (!self.stopping.load(.acquire)) {
            platform_sync.lockYielding(&self.mutex);
            fds.clearRetainingCapacity();
            ids.clearRetainingCapacity();
            if (self.entries.items.len == 0) {
                self.mutex.unlock();
                sleepMs(observation_interval_ms);
                continue;
            }
            for (self.entries.items) |entry| {
                // POLL constants are comptime integers. Give the mutable mask
                // the ABI type used by pollfd.events so Linux can add RDHUP at
                // runtime without leaving `events` inferred as comptime_int.
                var events: i16 = std.posix.POLL.ERR;
                if (!entry.unread_input) events |= std.posix.POLL.IN;
                if (comptime builtin.os.tag == .linux) events |= linux_poll_rdhup;
                fds.appendAssumeCapacity(.{ .fd = entry.fd, .events = events, .revents = 0 });
                ids.appendAssumeCapacity(entry.id);
            }
            self.mutex.unlock();

            const ready = std.posix.poll(fds.items, observation_interval_ms) catch {
                platform_sync.lockYielding(&self.mutex);
                self.failAllLocked();
                self.mutex.unlock();
                continue;
            };
            if (ready > 0) {
                platform_sync.lockYielding(&self.mutex);
                self.processPollEventsLocked(fds.items, ids.items);
                self.mutex.unlock();
            }
        }
    }

    fn processPollEventsLocked(self: *Observer, fds: []const std.posix.pollfd, ids: []const u64) void {
        for (fds, ids) |poll_fd, id| {
            const events = poll_fd.revents;
            if (events == 0) continue;
            const index = self.indexOfIdLocked(id) orelse continue;
            if (events & std.posix.POLL.NVAL != 0) {
                self.cancelEntryLocked(index, false);
                continue;
            }
            const peer_closed = if (comptime builtin.os.tag == .linux)
                events & linux_poll_rdhup != 0
            else
                false;
            if (events & std.posix.POLL.ERR != 0 or peer_closed) {
                self.cancelEntryLocked(index, true);
                continue;
            }
            if (events & (std.posix.POLL.IN | std.posix.POLL.HUP) == 0) continue;
            self.peekEntryLocked(index);
        }
    }

    fn runKqueue(self: *Observer) void {
        var events: std.ArrayListUnmanaged(std.posix.Kevent) = .empty;
        defer events.deinit(self.alloc);
        const kq = self.kernel_fd orelse return;
        const timeout = std.posix.timespec{ .sec = 0, .nsec = observation_interval_ms * std.time.ns_per_ms };
        events.resize(self.alloc, self.capacity) catch return self.stopAfterRuntimeFailure();

        while (!self.stopping.load(.acquire)) {
            if (self.active_count.load(.acquire) == 0) {
                sleepMs(observation_interval_ms);
                continue;
            }
            const ready_raw = std.posix.system.kevent(kq, events.items.ptr, 0, events.items.ptr, @intCast(events.items.len), &timeout);
            if (std.posix.errno(ready_raw) != .SUCCESS) {
                platform_sync.lockYielding(&self.mutex);
                self.failAllLocked();
                self.mutex.unlock();
                continue;
            }
            const ready: usize = @intCast(ready_raw);
            platform_sync.lockYielding(&self.mutex);
            for (events.items[0..ready]) |event| {
                const id: u64 = @intCast(event.udata);
                const index = self.indexOfIdLocked(id) orelse continue;
                if (event.flags & std.c.EV.ERROR != 0) {
                    self.cancelEntryLocked(index, false);
                } else if (event.flags & std.c.EV.EOF != 0) {
                    self.cancelEntryLocked(index, true);
                } else {
                    self.peekEntryLocked(index);
                }
            }
            self.mutex.unlock();
        }
    }

    fn stopAfterRuntimeFailure(self: *Observer) void {
        platform_sync.lockYielding(&self.mutex);
        self.failAllLocked();
        self.stopping.store(true, .release);
        self.mutex.unlock();
    }

    fn updateKqueue(self: *Observer, fd: std.posix.fd_t, id: u64, add: bool) !void {
        const kq = self.kernel_fd orelse return error.ObserverUnavailable;
        var changes = [_]std.posix.Kevent{.{
            .ident = @intCast(fd),
            .filter = std.c.EVFILT.READ,
            .flags = if (add) std.c.EV.ADD | std.c.EV.CLEAR else std.c.EV.DELETE,
            .fflags = 0,
            .data = 0,
            .udata = @intCast(id),
        }};
        var ignored: [1]std.posix.Kevent = undefined;
        const timeout = std.posix.timespec{ .sec = 0, .nsec = 0 };
        const rc = std.posix.system.kevent(kq, &changes, changes.len, &ignored, 0, &timeout);
        if (std.posix.errno(rc) != .SUCCESS) return error.ObserverUnavailable;
    }

    fn peekEntryLocked(self: *Observer, index: usize) void {
        const entry = &self.entries.items[index];
        var byte: [1]u8 = undefined;
        const n = std.c.recv(entry.fd, &byte, byte.len, @intCast(std.posix.MSG.PEEK | std.posix.MSG.DONTWAIT));
        if (n == 0) {
            self.cancelEntryLocked(index, true);
            return;
        }
        if (n > 0) {
            entry.unread_input = true;
            // Linux keeps observing RDHUP without repeatedly waking for the
            // unread byte. Kqueue EV_CLEAR similarly waits for the EOF change.
            if (comptime builtin.os.tag != .linux and builtin.os.tag != .macos) {
                // Other poll backends cannot reliably see EOF behind unread
                // data. Fail closed rather than orphaning expensive work.
                self.cancelEntryLocked(index, false);
            }
            return;
        }
        switch (std.posix.errno(n)) {
            .AGAIN, .INTR => {},
            .CONNRESET => self.cancelEntryLocked(index, true),
            else => self.cancelEntryLocked(index, false),
        }
    }

    fn indexOfIdLocked(self: *Observer, id: u64) ?usize {
        for (self.entries.items, 0..) |entry, index| if (entry.id == id) return index;
        return null;
    }

    fn cancelEntryLocked(self: *Observer, index: usize, peer_disconnect: bool) void {
        const entry = self.entries.items[index];
        if (peer_disconnect) {
            if (entry.peer_disconnects_total) |counter| _ = counter.fetchAdd(1, .monotonic);
        } else {
            if (entry.observer_failures_total) |counter| _ = counter.fetchAdd(1, .monotonic);
        }
        entry.cancellation.cancel();
        if (comptime builtin.os.tag == .macos) self.updateKqueue(entry.fd, entry.id, false) catch {};
        _ = self.entries.swapRemove(index);
        _ = self.active_count.fetchSub(1, .release);
    }

    fn failAllLocked(self: *Observer) void {
        while (self.entries.items.len > 0) self.cancelEntryLocked(self.entries.items.len - 1, false);
    }
};

test "std http listener multiplexed peer observer cancels many disconnected sockets with one owner thread" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    const io = std.Io.Threaded.global_single_threaded.io();
    var listener = try (std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }).listen(io, .{});
    defer listener.deinit(io);

    var observer = Observer.init(std.testing.allocator, 32);
    try observer.start();
    defer observer.deinit();

    var clients: [32]std.Io.net.Stream = undefined;
    var servers: [32]std.Io.net.Stream = undefined;
    var cancellations = [_]http_common.RequestCancellation{.{}} ** 32;
    var registrations: [32]Observer.Registration = undefined;
    var initialized: usize = 0;
    var clients_open = true;
    defer {
        for (registrations[0..initialized]) |*registration| registration.deinit();
        if (clients_open) for (clients[0..initialized]) |*stream| stream.close(io);
        for (servers[0..initialized]) |*stream| stream.close(io);
    }
    for (0..32) |index| {
        clients[index] = try listener.socket.address.connect(io, .{ .mode = .stream });
        servers[index] = try listener.accept(io);
        registrations[index] = try observer.register(servers[index].socket.handle, &cancellations[index], null, null);
        initialized += 1;
    }
    try std.testing.expectEqual(@as(usize, 32), observer.activeCount());
    for (&clients) |*stream| {
        stream.close(io);
    }
    clients_open = false;

    for (0..400) |_| {
        var cancelled: usize = 0;
        for (&cancellations) |*signal| {
            if (signal.isCancelled()) cancelled += 1;
        }
        if (cancelled == cancellations.len) break;
        sleepMs(5);
    }
    for (&cancellations) |*signal| try std.testing.expect(signal.isCancelled());
}

test "std http listener peer observer sees FIN behind unread bytes without consuming them" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    const io = std.Io.Threaded.global_single_threaded.io();
    var listener = try (std.Io.net.IpAddress{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }).listen(io, .{});
    defer listener.deinit(io);
    var client = try listener.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);
    var server = try listener.accept(io);
    defer server.close(io);

    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();
    var cancellation: http_common.RequestCancellation = .{};
    var registration = try observer.register(server.socket.handle, &cancellation, null, null);
    defer registration.deinit();

    var write_buffer: [1]u8 = undefined;
    var writer = client.writer(io, &write_buffer);
    try writer.interface.writeAll("B");
    try writer.interface.flush();
    try io.vtable.netShutdown(io.userdata, client.socket.handle, .send);
    for (0..400) |_| {
        if (cancellation.isCancelled()) break;
        sleepMs(5);
    }
    try std.testing.expect(cancellation.isCancelled());
    var queued: [1]u8 = undefined;
    const read_len = std.c.recv(server.socket.handle, &queued, queued.len, 0);
    try std.testing.expectEqual(@as(isize, 1), read_len);
    try std.testing.expectEqualStrings("B", &queued);
}
