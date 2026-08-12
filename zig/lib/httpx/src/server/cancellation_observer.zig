//! Transport-owned HTTP/1 request cancellation observation.
//!
//! HTTP/2 has stream-local reset state. HTTP/1 has only a connection, so one
//! bounded listener-owned thread multiplexes peer-lifetime observation for
//! every active H1 request. It never consumes bytes from the parser's socket.

const builtin = @import("builtin");
const std = @import("std");

const observation_interval_ms: u64 = 25;
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

pub const Observer = struct {
    const Entry = struct {
        id: u64,
        fd: std.posix.fd_t,
        cancellation: *std.atomic.Value(bool),
        /// A FIN after pipelined input is not abandonment of the request
        /// currently executing. Once input is known to be buffered, passive
        /// connection observation is suppressed until this request completes.
        observe_orderly_eof: bool,
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
    thread_stack_size: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    next_id: u64 = 1,
    stopping: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    kernel_fd: ?std.posix.fd_t = null,
    active: std.atomic.Value(usize) = .init(0),
    cancellations_total: std.atomic.Value(u64) = .init(0),
    failures_total: std.atomic.Value(u64) = .init(0),

    pub fn init(alloc: std.mem.Allocator, capacity: usize, thread_stack_size: usize) Observer {
        return .{
            .alloc = alloc,
            .capacity = capacity,
            .thread_stack_size = thread_stack_size,
        };
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
        self.thread = try std.Thread.spawn(.{ .stack_size = self.thread_stack_size }, run, .{self});
    }

    pub fn stop(self: *Observer) void {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
        self.stopping.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.thread = null;
        if (self.kernel_fd) |fd| _ = std.posix.system.close(fd);
        self.kernel_fd = null;
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(self.active.load(.acquire) == 0);
    }

    pub fn deinit(self: *Observer) void {
        self.stop();
        self.entries.deinit(self.alloc);
    }

    pub fn register(
        self: *Observer,
        fd: std.posix.fd_t,
        cancellation: *std.atomic.Value(bool),
        observe_orderly_eof: bool,
    ) !Registration {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return .{};
        if (self.thread == null or self.stopping.load(.acquire)) return error.ObserverUnavailable;
        self.lock();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire)) return error.ObserverUnavailable;
        if (self.entries.items.len >= self.capacity) return error.ObserverCapacityExceeded;
        const id = self.nextId();
        if (comptime builtin.os.tag == .macos) try self.updateKqueue(fd, id, true);
        self.entries.appendAssumeCapacity(.{
            .id = id,
            .fd = fd,
            .cancellation = cancellation,
            .observe_orderly_eof = observe_orderly_eof,
        });
        _ = self.active.fetchAdd(1, .release);
        return .{ .observer = self, .id = id };
    }

    pub fn activeCount(self: *const Observer) usize {
        return self.active.load(.acquire);
    }

    pub fn cancellations(self: *const Observer) u64 {
        return self.cancellations_total.load(.acquire);
    }

    pub fn failures(self: *const Observer) u64 {
        return self.failures_total.load(.acquire);
    }

    fn lock(self: *Observer) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
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
        self.lock();
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, index| {
            if (entry.id != id) continue;
            self.removeLocked(index);
            return;
        }
    }

    fn run(self: *Observer) void {
        if (comptime builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;
        if (comptime builtin.os.tag == .macos) return self.runKqueue();
        self.runPoll();
    }

    fn runPoll(self: *Observer) void {
        var fds: std.ArrayListUnmanaged(std.posix.pollfd) = .empty;
        defer fds.deinit(self.alloc);
        var ids: std.ArrayListUnmanaged(u64) = .empty;
        defer ids.deinit(self.alloc);
        fds.ensureTotalCapacity(self.alloc, self.capacity) catch return self.stopAfterFailure();
        ids.ensureTotalCapacity(self.alloc, self.capacity) catch return self.stopAfterFailure();

        while (!self.stopping.load(.acquire)) {
            self.lock();
            fds.clearRetainingCapacity();
            ids.clearRetainingCapacity();
            for (self.entries.items) |entry| {
                // POLLHUP is reported even when it is not requested and would
                // make poll return continuously. Omit ambiguous pipelined
                // registrations entirely instead of creating a hot loop.
                if (!entry.observe_orderly_eof or entry.unread_input) continue;
                var events: i16 = std.posix.POLL.IN;
                if (comptime builtin.os.tag == .linux) events |= linux_poll_rdhup;
                fds.appendAssumeCapacity(.{ .fd = entry.fd, .events = events, .revents = 0 });
                ids.appendAssumeCapacity(entry.id);
            }
            self.mutex.unlock();
            if (fds.items.len == 0) {
                sleepMs(observation_interval_ms);
                continue;
            }
            const ready = std.posix.poll(fds.items, observation_interval_ms) catch {
                self.lock();
                self.failAllLocked();
                self.mutex.unlock();
                continue;
            };
            if (ready == 0) continue;
            self.lock();
            for (fds.items, ids.items) |poll_fd, id| {
                if (poll_fd.revents == 0) continue;
                const index = self.indexOfLocked(id) orelse continue;
                const entry = self.entries.items[index];
                if (poll_fd.revents & std.posix.POLL.NVAL != 0) {
                    self.cancelLocked(index, false);
                    continue;
                }
                if (poll_fd.revents & std.posix.POLL.ERR != 0) {
                    self.cancelLocked(index, true);
                    continue;
                }
                const orderly = if (comptime builtin.os.tag == .linux)
                    poll_fd.revents & linux_poll_rdhup != 0
                else
                    poll_fd.revents & std.posix.POLL.HUP != 0;
                if (orderly and entry.observe_orderly_eof) {
                    // A peer may pipeline another request and FIN in the same
                    // packet. Peek before interpreting EOF so unread protocol
                    // input wins over the ambiguous connection-level signal.
                    if (poll_fd.revents & std.posix.POLL.IN != 0)
                        self.peekLocked(index);
                    const current_index = self.indexOfLocked(id) orelse continue;
                    if (!self.entries.items[current_index].unread_input)
                        self.cancelLocked(current_index, true);
                    continue;
                }
                if (entry.observe_orderly_eof and poll_fd.revents & std.posix.POLL.IN != 0)
                    self.peekLocked(index);
            }
            self.mutex.unlock();
        }
    }

    fn runKqueue(self: *Observer) void {
        var events: std.ArrayListUnmanaged(std.posix.Kevent) = .empty;
        defer events.deinit(self.alloc);
        events.resize(self.alloc, self.capacity) catch return self.stopAfterFailure();
        const kq = self.kernel_fd orelse return;
        const timeout = std.posix.timespec{ .sec = 0, .nsec = observation_interval_ms * std.time.ns_per_ms };
        while (!self.stopping.load(.acquire)) {
            if (self.active.load(.acquire) == 0) {
                sleepMs(observation_interval_ms);
                continue;
            }
            const ready_raw = std.posix.system.kevent(kq, events.items.ptr, 0, events.items.ptr, @intCast(events.items.len), &timeout);
            if (std.posix.errno(ready_raw) != .SUCCESS) {
                self.lock();
                self.failAllLocked();
                self.mutex.unlock();
                continue;
            }
            self.lock();
            for (events.items[0..@intCast(ready_raw)]) |event| {
                const index = self.indexOfLocked(@intCast(event.udata)) orelse continue;
                const entry = self.entries.items[index];
                if (event.flags & std.c.EV.ERROR != 0) {
                    self.cancelLocked(index, false);
                } else if (event.flags & std.c.EV.EOF != 0 and entry.observe_orderly_eof) {
                    self.peekLocked(index);
                    const current_index = self.indexOfLocked(@intCast(event.udata)) orelse continue;
                    if (!self.entries.items[current_index].unread_input)
                        self.cancelLocked(current_index, true);
                } else if (entry.observe_orderly_eof) {
                    self.peekLocked(index);
                }
            }
            self.mutex.unlock();
        }
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

    fn peekLocked(self: *Observer, index: usize) void {
        const entry = &self.entries.items[index];
        var byte: [1]u8 = undefined;
        const n = std.c.recv(entry.fd, &byte, byte.len, @intCast(std.posix.MSG.PEEK | std.posix.MSG.DONTWAIT));
        if (n == 0) return self.cancelLocked(index, true);
        if (n > 0) {
            entry.unread_input = true;
            return;
        }
        switch (std.posix.errno(n)) {
            .AGAIN, .INTR => {},
            .CONNRESET => self.cancelLocked(index, true),
            else => self.cancelLocked(index, false),
        }
    }

    fn indexOfLocked(self: *Observer, id: u64) ?usize {
        for (self.entries.items, 0..) |entry, index| if (entry.id == id) return index;
        return null;
    }

    fn cancelLocked(self: *Observer, index: usize, peer_disconnect: bool) void {
        const entry = self.entries.items[index];
        entry.cancellation.store(true, .release);
        _ = if (peer_disconnect)
            self.cancellations_total.fetchAdd(1, .monotonic)
        else
            self.failures_total.fetchAdd(1, .monotonic);
        self.removeLocked(index);
    }

    fn removeLocked(self: *Observer, index: usize) void {
        const entry = self.entries.items[index];
        if (comptime builtin.os.tag == .macos) self.updateKqueue(entry.fd, entry.id, false) catch {};
        _ = self.entries.swapRemove(index);
        _ = self.active.fetchSub(1, .release);
    }

    fn failAllLocked(self: *Observer) void {
        while (self.entries.items.len > 0) self.cancelLocked(self.entries.items.len - 1, false);
    }

    fn stopAfterFailure(self: *Observer) void {
        self.lock();
        self.failAllLocked();
        self.stopping.store(true, .release);
        self.mutex.unlock();
    }
};
