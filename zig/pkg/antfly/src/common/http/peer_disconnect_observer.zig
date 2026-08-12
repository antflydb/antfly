// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const builtin = @import("builtin");
const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const http_common = @import("http_common.zig");
const thread_config = @import("../../runtime_thread_config.zig");

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

fn monotonicNowNs() ?u64 {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return null;
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS) return null;
    const seconds = std.math.cast(u64, ts.sec) orelse return null;
    const nanos = std.math.cast(u64, ts.nsec) orelse return null;
    return std.math.add(u64, std.math.mul(u64, seconds, std.time.ns_per_s) catch return null, nanos) catch null;
}

/// A bounded, multiplexed H1 socket-lifetime observer.
///
/// One owner thread watches every admitted request for peer disconnects and
/// enforces header/body deadlines. This deliberately avoids `Io.concurrent`:
/// the threaded Io backend permanently retains one worker for every concurrent
/// blocking timeout race. The registry therefore keeps observation O(1)
/// threads per listener while retaining O(n) descriptors and a small, fixed
/// polling cadence.
pub const Observer = struct {
    pub const DeadlineOutcome = enum {
        completed,
        expired,
        observer_failed,
    };

    pub const Deadline = struct {
        const State = enum(u8) {
            pending,
            expired,
            observer_failed,
        };

        state: std.atomic.Value(State) = .init(.pending),

        pub fn didExpire(self: *const Deadline) bool {
            return self.state.load(.acquire) == .expired;
        }

        pub fn observerFailed(self: *const Deadline) bool {
            return self.state.load(.acquire) == .observer_failed;
        }
    };

    const PeerAction = struct {
        cancellation: *http_common.RequestCancellation,
        peer_disconnects_total: ?*std.atomic.Value(u64),
        observer_failures_total: ?*std.atomic.Value(u64),
    };

    const DeadlineAction = struct {
        state: *Deadline,
        expires_at_ns: u64,
        expirations_total: ?*std.atomic.Value(u64),
    };

    const ProbeAction = struct {
        cancellation: *http_common.RequestCancellation,
        context: *const anyopaque,
        is_cancelled: *const fn (*const anyopaque) bool,
    };

    const Action = union(enum) {
        peer: PeerAction,
        deadline: DeadlineAction,
        probe: ProbeAction,
    };

    const Entry = struct {
        id: u64,
        fd: std.posix.fd_t,
        action: Action,
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

        /// Atomically retires a deadline registration before publishing its
        /// terminal outcome to the socket owner. A pending deadline becomes a
        /// successful completion only after unregister() has excluded the
        /// observer from any later shutdown of the descriptor.
        pub fn finishDeadline(self: *Registration, deadline: *const Deadline) DeadlineOutcome {
            self.deinit();
            return switch (deadline.state.load(.acquire)) {
                .pending => .completed,
                .expired => .expired,
                .observer_failed => .observer_failed,
            };
        }
    };

    alloc: std.mem.Allocator,
    capacity: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_id: u64 = 1,
    active_count: std.atomic.Value(usize) = .init(0),
    active_peer_count: std.atomic.Value(usize) = .init(0),
    active_deadline_count: std.atomic.Value(usize) = .init(0),
    deadline_expirations_total: std.atomic.Value(u64) = .init(0),
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
        self.thread = try std.Thread.spawn(
            .{ .stack_size = thread_config.minimum_partitioned_stack_size },
            run,
            .{self},
        );
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
        std.debug.assert(self.active_peer_count.load(.acquire) == 0);
        std.debug.assert(self.active_deadline_count.load(.acquire) == 0);
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
        // Close the check/register race with deinit() and fatal observer
        // failures. Once stopping is published, no stack-backed action may be
        // admitted because no owner thread is guaranteed to retire it.
        if (self.stopping.load(.acquire)) return error.ObserverUnavailable;
        if (self.entries.items.len >= self.capacity) return error.ObserverCapacityExceeded;

        const id = self.nextId();
        if (comptime builtin.os.tag == .macos) try self.updateKqueue(fd, id, true);
        self.entries.appendAssumeCapacity(.{
            .id = id,
            .fd = fd,
            .action = .{ .peer = .{
                .cancellation = cancellation,
                .peer_disconnects_total = peer_disconnects_total,
                .observer_failures_total = observer_failures_total,
            } },
        });
        _ = self.active_count.fetchAdd(1, .release);
        _ = self.active_peer_count.fetchAdd(1, .release);
        return .{ .observer = self, .id = id };
    }

    pub fn registerDeadline(
        self: *Observer,
        fd: std.posix.fd_t,
        timeout_ms: u32,
        state: *Deadline,
        expirations_total: ?*std.atomic.Value(u64),
    ) !Registration {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding)
            return error.ObserverUnavailable;
        if (self.thread == null or self.stopping.load(.acquire)) return error.ObserverUnavailable;
        const now_ns = monotonicNowNs() orelse return error.ObserverUnavailable;
        const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
        const expires_at_ns = std.math.add(u64, now_ns, timeout_ns) catch std.math.maxInt(u64);
        state.state.store(.pending, .release);

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire)) return error.ObserverUnavailable;
        if (self.entries.items.len >= self.capacity) return error.ObserverCapacityExceeded;

        const id = self.nextId();
        self.entries.appendAssumeCapacity(.{
            .id = id,
            .fd = fd,
            .action = .{ .deadline = .{
                .state = state,
                .expires_at_ns = expires_at_ns,
                .expirations_total = expirations_total,
            } },
        });
        _ = self.active_count.fetchAdd(1, .release);
        _ = self.active_deadline_count.fetchAdd(1, .release);
        return .{ .observer = self, .id = id };
    }

    /// Mirrors a transport-neutral cancellation callback into the atomic
    /// request signal still consumed by storage and search internals. Probe
    /// registrations share this observer's one bounded owner thread.
    pub fn registerProbe(
        self: *Observer,
        cancellation: *http_common.RequestCancellation,
        context: *const anyopaque,
        is_cancelled: *const fn (*const anyopaque) bool,
    ) !Registration {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding)
            return error.ObserverUnavailable;
        if (self.thread == null or self.stopping.load(.acquire)) return error.ObserverUnavailable;

        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire)) return error.ObserverUnavailable;
        if (self.entries.items.len >= self.capacity) return error.ObserverCapacityExceeded;

        const id = self.nextId();
        self.entries.appendAssumeCapacity(.{
            .id = id,
            .fd = 0,
            .action = .{ .probe = .{
                .cancellation = cancellation,
                .context = context,
                .is_cancelled = is_cancelled,
            } },
        });
        _ = self.active_count.fetchAdd(1, .release);
        return .{ .observer = self, .id = id };
    }

    pub fn activeCount(self: *const Observer) usize {
        return self.active_count.load(.acquire);
    }

    pub fn activePeerCount(self: *const Observer) usize {
        return self.active_peer_count.load(.acquire);
    }

    pub fn activeDeadlineCount(self: *const Observer) usize {
        return self.active_deadline_count.load(.acquire);
    }

    pub fn deadlineExpirationsTotal(self: *const Observer) u64 {
        return self.deadline_expirations_total.load(.acquire);
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
            self.removeEntryLocked(index);
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
                if (entry.action != .peer) continue;
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
                self.checkProbesLocked();
                self.expireDeadlinesLocked();
                self.mutex.unlock();
            } else {
                platform_sync.lockYielding(&self.mutex);
                self.checkProbesLocked();
                self.expireDeadlinesLocked();
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
            self.checkProbesLocked();
            self.expireDeadlinesLocked();
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
        std.debug.assert(entry.action == .peer);
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
        const peer = switch (entry.action) {
            .peer => |value| value,
            .deadline, .probe => unreachable,
        };
        if (peer_disconnect) {
            if (peer.peer_disconnects_total) |counter| _ = counter.fetchAdd(1, .monotonic);
        } else {
            if (peer.observer_failures_total) |counter| _ = counter.fetchAdd(1, .monotonic);
        }
        peer.cancellation.cancel();
        self.removeEntryLocked(index);
    }

    fn expireDeadlinesLocked(self: *Observer) void {
        const now_ns = monotonicNowNs() orelse {
            self.failAllLocked();
            self.stopping.store(true, .release);
            return;
        };
        var index: usize = 0;
        while (index < self.entries.items.len) {
            const entry = self.entries.items[index];
            const deadline = switch (entry.action) {
                .deadline => |value| value,
                .peer => {
                    index += 1;
                    continue;
                },
                .probe => {
                    index += 1;
                    continue;
                },
            };
            if (now_ns < deadline.expires_at_ns) {
                index += 1;
                continue;
            }
            deadline.state.state.store(.expired, .release);
            _ = self.deadline_expirations_total.fetchAdd(1, .monotonic);
            if (deadline.expirations_total) |counter| _ = counter.fetchAdd(1, .monotonic);
            _ = std.c.shutdown(entry.fd, std.c.SHUT.RDWR);
            self.removeEntryLocked(index);
        }
    }

    fn checkProbesLocked(self: *Observer) void {
        var index: usize = 0;
        while (index < self.entries.items.len) {
            const probe = switch (self.entries.items[index].action) {
                .probe => |value| value,
                else => {
                    index += 1;
                    continue;
                },
            };
            if (!probe.is_cancelled(probe.context)) {
                index += 1;
                continue;
            }
            probe.cancellation.cancel();
            self.removeEntryLocked(index);
        }
    }

    fn removeEntryLocked(self: *Observer, index: usize) void {
        const entry = self.entries.items[index];
        switch (entry.action) {
            .peer => {
                if (comptime builtin.os.tag == .macos) self.updateKqueue(entry.fd, entry.id, false) catch {};
                _ = self.active_peer_count.fetchSub(1, .release);
            },
            .deadline => {
                _ = self.active_deadline_count.fetchSub(1, .release);
            },
            .probe => {},
        }
        _ = self.entries.swapRemove(index);
        _ = self.active_count.fetchSub(1, .release);
    }

    fn failAllLocked(self: *Observer) void {
        while (self.entries.items.len > 0) {
            const index = self.entries.items.len - 1;
            const entry = self.entries.items[index];
            switch (entry.action) {
                .peer => self.cancelEntryLocked(index, false),
                .deadline => |deadline| {
                    deadline.state.state.store(.observer_failed, .release);
                    _ = std.c.shutdown(entry.fd, std.c.SHUT.RDWR);
                    self.removeEntryLocked(index);
                },
                .probe => |probe| {
                    probe.cancellation.cancel();
                    self.removeEntryLocked(index);
                },
            }
        }
    }
};

test "std http listener multiplexed peer observer cancels many disconnected sockets with one owner thread" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var observer = Observer.init(std.testing.allocator, 32);
    try observer.start();
    defer observer.deinit();

    var sockets: [32][2]std.posix.fd_t = undefined;
    var cancellations = [_]http_common.RequestCancellation{.{}} ** 32;
    var registrations: [32]Observer.Registration = undefined;
    var initialized: usize = 0;
    var clients_open: usize = 0;
    defer {
        for (registrations[0..initialized]) |*registration| registration.deinit();
        for (sockets[clients_open..initialized]) |pair| _ = std.posix.system.close(pair[0]);
        for (sockets[0..initialized]) |pair| _ = std.posix.system.close(pair[1]);
    }
    for (0..32) |index| {
        if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets[index]) != 0)
            return error.Unexpected;
        registrations[index] = try observer.register(sockets[index][1], &cancellations[index], null, null);
        initialized += 1;
    }
    try std.testing.expectEqual(@as(usize, 32), observer.activeCount());
    for (&sockets) |pair| {
        _ = std.posix.system.close(pair[0]);
        clients_open += 1;
    }

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

test "peer observer mirrors transport-neutral cancellation probes" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();

    var source = std.atomic.Value(bool).init(false);
    var cancellation: http_common.RequestCancellation = .{};
    var registration = try observer.registerProbe(
        &cancellation,
        &source,
        struct {
            fn call(raw: *const anyopaque) bool {
                const value: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
                return value.load(.acquire);
            }
        }.call,
    );
    defer registration.deinit();

    source.store(true, .release);
    for (0..400) |_| {
        if (cancellation.isCancelled()) break;
        sleepMs(5);
    }
    try std.testing.expect(cancellation.isCancelled());
}

test "std http listener peer observer sees FIN behind unread bytes without consuming them" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var sockets: [2]std.posix.fd_t = undefined;
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets) != 0)
        return error.Unexpected;
    defer _ = std.posix.system.close(sockets[0]);
    defer _ = std.posix.system.close(sockets[1]);

    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();
    var cancellation: http_common.RequestCancellation = .{};
    var registration = try observer.register(sockets[1], &cancellation, null, null);
    defer registration.deinit();

    try std.testing.expectEqual(@as(isize, 1), std.c.send(sockets[0], "B", 1, 0));
    try std.testing.expectEqual(@as(c_int, 0), std.c.shutdown(sockets[0], std.c.SHUT.WR));
    for (0..400) |_| {
        if (cancellation.isCancelled()) break;
        sleepMs(5);
    }
    try std.testing.expect(cancellation.isCancelled());
    var queued: [1]u8 = undefined;
    const read_len = std.c.recv(sockets[1], &queued, queued.len, 0);
    try std.testing.expectEqual(@as(isize, 1), read_len);
    try std.testing.expectEqualStrings("B", &queued);
}

fn runDeadlineStormRound(observer: *Observer) !void {
    const batch_size = 32;
    var sockets: [32][2]std.posix.fd_t = undefined;
    var deadlines = [_]Observer.Deadline{.{}} ** 32;
    var registrations: [32]Observer.Registration = undefined;
    var initialized: usize = 0;
    defer {
        for (registrations[0..initialized]) |*registration| registration.deinit();
        for (sockets[0..initialized]) |pair| {
            _ = std.posix.system.close(pair[0]);
            _ = std.posix.system.close(pair[1]);
        }
    }

    for (0..32) |index| {
        if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets[index]) != 0)
            return error.Unexpected;
        registrations[index] = try observer.registerDeadline(
            sockets[index][1],
            250,
            &deadlines[index],
            null,
        );
        initialized += 1;
    }
    try std.testing.expectEqual(@as(usize, batch_size), observer.activeDeadlineCount());

    for (0..400) |_| {
        if (observer.activeDeadlineCount() == 0) break;
        sleepMs(5);
    }
    for (&deadlines) |*deadline| try std.testing.expect(deadline.didExpire());
    try std.testing.expectEqual(@as(usize, 0), observer.activeCount());
    try std.testing.expectEqual(@as(usize, 0), observer.activePeerCount());
    try std.testing.expectEqual(@as(usize, 0), observer.activeDeadlineCount());
}

test "std http listener socket observer does not ratchet workers across deadline storms" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    var observer = Observer.init(std.testing.allocator, 32);
    try observer.start();
    defer observer.deinit();

    for (0..4) |round| {
        try runDeadlineStormRound(&observer);
        try std.testing.expectEqual(@as(u64, @intCast((round + 1) * 32)), observer.deadlineExpirationsTotal());
    }
}

test "std http listener socket observer unregisters completed deadline without firing" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var sockets: [2]std.posix.fd_t = undefined;
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets) != 0)
        return error.Unexpected;
    defer _ = std.posix.system.close(sockets[0]);
    defer _ = std.posix.system.close(sockets[1]);

    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();
    var deadline: Observer.Deadline = .{};
    var registration = try observer.registerDeadline(sockets[1], 50, &deadline, null);
    try std.testing.expectEqual(Observer.DeadlineOutcome.completed, registration.finishDeadline(&deadline));
    sleepMs(100);

    try std.testing.expect(!deadline.didExpire());
    try std.testing.expectEqual(@as(usize, 0), observer.activeCount());
    try std.testing.expectEqual(@as(u64, 0), observer.deadlineExpirationsTotal());
    // Completion owns the socket once unregister returns; a stale observer
    // must never shut it down after the stack-backed deadline leaves scope.
    try std.testing.expectEqual(@as(isize, 1), std.c.send(sockets[0], "C", 1, 0));
    var received: [1]u8 = undefined;
    try std.testing.expectEqual(@as(isize, 1), std.c.recv(sockets[1], &received, received.len, 0));
    try std.testing.expectEqualStrings("C", &received);
}

test "std http listener socket observer distinguishes observer failure from timeout" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var sockets: [2]std.posix.fd_t = undefined;
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets) != 0)
        return error.Unexpected;
    defer _ = std.posix.system.close(sockets[0]);
    defer _ = std.posix.system.close(sockets[1]);

    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();
    var deadline: Observer.Deadline = .{};
    var registration = try observer.registerDeadline(sockets[1], 60_000, &deadline, null);
    defer registration.deinit();

    platform_sync.lockYielding(&observer.mutex);
    observer.failAllLocked();
    observer.mutex.unlock();
    try std.testing.expect(deadline.observerFailed());
    try std.testing.expect(!deadline.didExpire());
    try std.testing.expectEqual(@as(u64, 0), observer.deadlineExpirationsTotal());
    try std.testing.expectEqual(@as(usize, 0), observer.activeCount());
}

test "std http listener socket observer rejects registration after fatal stop" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
    var sockets: [2]std.posix.fd_t = undefined;
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sockets) != 0)
        return error.Unexpected;
    defer _ = std.posix.system.close(sockets[0]);
    defer _ = std.posix.system.close(sockets[1]);

    var observer = Observer.init(std.testing.allocator, 1);
    try observer.start();
    defer observer.deinit();
    observer.stopAfterRuntimeFailure();

    var deadline: Observer.Deadline = .{};
    try std.testing.expectError(
        error.ObserverUnavailable,
        observer.registerDeadline(sockets[1], 60_000, &deadline, null),
    );
    try std.testing.expectEqual(@as(usize, 0), observer.activeCount());
}
