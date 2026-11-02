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

const std = @import("std");
const protocol = @import("protocol.zig");
const backend = @import("backend.zig");
const Budget = @import("budget.zig").Budget;
const SharedBudget = @import("budget.zig").Shared;

/// Idle inbound sessions block for their entire lifetime. They cannot borrow
/// the bounded outbound API executor without exhausting its small task quota.
/// Keep this owner stable until Server joins all connection tasks.
pub fn initTransport(alloc: std.mem.Allocator, max_connections: u16) std.Io.Threaded {
    return .init(alloc, .{
        .async_limit = .nothing,
        // Sessions plus accept, shared deadline and cancel handshakes.
        .concurrent_limit = .limited(@as(usize, max_connections) + 6),
    });
}

pub const Config = struct {
    io: std.Io,
    backend: backend.Backend,
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 5432,
    max_connections: u16 = 32,
    /// Zero preserves the sum of per-connection ceilings.
    max_session_bytes: u64 = 0,
    max_retained_bytes: u64 = 0,
    limits: protocol.Limits = .{},
    protected_frame_allocator: ?std.mem.Allocator = null,
    control_allocator: ?std.mem.Allocator = null,
    is_protected_control: ?*const fn ([]const u8) bool = null,
    // There is no built-in TLS termination yet. A remote bind must explicitly
    // acknowledge that TLS/authenticated private-network protection is supplied
    // by the deployment; never silently send passwords over a public socket.
    allow_insecure_non_loopback: bool = false,
};

pub const Server = struct {
    state: ?*State,

    pub const Stats = struct {
        connections: u32,
        deadline_entries: usize,
        deadline_tasks: u32 = 1,
        expired_connections: u64,
        session_bytes: usize,
        retained_bytes: usize,
    };

    pub fn stats(self: Server) Stats {
        const state = self.state.?;
        state.deadline_mutex.lockUncancelable(state.config.io);
        defer state.deadline_mutex.unlock(state.config.io);
        var count: usize = 0;
        var entry = state.deadline_head;
        while (entry) |value| : (entry = value.next) count += 1;
        return .{
            .connections = state.active.load(.acquire),
            .deadline_entries = count,
            .expired_connections = state.expired.load(.acquire),
            .session_bytes = state.memory.used.load(.acquire),
            .retained_bytes = state.retained.used.load(.acquire),
        };
    }

    pub fn address(self: Server) std.Io.net.IpAddress {
        return self.state.?.listener.socket.address;
    }

    pub fn deinit(self: *Server) void {
        const state = self.state orelse return;
        self.state = null;
        // Structured cancellation joins accept, watchdogs and every connection
        // before freeing callbacks, sockets or registry entries. No detached
        // thread, timed leak, or shared-state use-after-free during shutdown.
        state.tasks.cancel(state.config.io);
        state.listener.deinit(state.config.io);
        std.debug.assert(state.active.load(.acquire) == 0);
        std.debug.assert(state.sessions.count() == 0);
        if (state.foreground_memory) |*foreground| std.debug.assert(foreground.used.load(.acquire) == 0);
        for (state.connection_slots) |*slot| std.debug.assert(!slot.in_use.load(.acquire));
        state.memory.allocator().free(state.connection_slots);
        std.debug.assert(state.memory.used.load(.acquire) == 0);
        std.debug.assert(state.retained.used.load(.acquire) == 0);
        std.debug.assert(state.deadline_head == null);
        state.sessions.deinit(state.alloc);
        state.alloc.destroy(state);
    }
};

const State = struct {
    alloc: std.mem.Allocator,
    config: Config,
    listener: std.Io.net.Server,
    tasks: std.Io.Group = .init,
    active: std.atomic.Value(u32) = .init(0),
    expired: std.atomic.Value(u64) = .init(0),
    mutex: std.Io.Mutex = .init,
    next_pid: u32 = 0,
    sessions: std.AutoHashMapUnmanaged(i32, *protocol.Session) = .empty,
    memory: SharedBudget,
    /// Ordinary data cannot consume the fixed pool or protected-frame reserve.
    /// Its backing still charges every actual byte to the aggregate envelope.
    foreground_memory: ?SharedBudget = null,
    retained: SharedBudget,
    connection_slots: []ConnectionSlot = &.{},
    deadline_mutex: std.Io.Mutex = .init,
    deadline_wakeup: std.Io.Event = .unset,
    deadline_head: ?*DeadlineEntry = null,

    const DeadlineEntry = struct {
        next: ?*DeadlineEntry = null,
        session: *protocol.Session,
        stream: std.Io.net.Stream,
        expired: bool = false,
    };

    /// One deadline task for the listener, including unauthenticated sockets.
    /// The list borrows stable connection owners until removed and joined.
    fn deadlines(self: *State) void {
        const io = self.config.io;
        while (true) {
            self.deadline_wakeup.reset();
            var next: i96 = std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s;
            self.deadline_mutex.lock(io) catch return;
            var entry = self.deadline_head;
            while (entry) |value| : (entry = value.next) {
                if (value.expired) continue;
                const deadline = value.session.deadline_ns.load(.acquire);
                if (deadline <= std.Io.Clock.awake.now(io).nanoseconds) {
                    value.expired = true;
                    _ = self.expired.fetchAdd(1, .monotonic);
                    value.session.cancel_requested.store(true, .release);
                    // Shutdown wakes socket readers/writers without closing or
                    // reusing the descriptor. Its connection owner still joins
                    // backend completion and alone performs close/destruction.
                    value.stream.shutdown(io, .both) catch {};
                } else next = @min(next, deadline);
            }
            self.deadline_mutex.unlock(io);
            self.deadline_wakeup.waitTimeout(io, .{ .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = next } } }) catch |err| switch (err) {
                error.Canceled => return,
                error.Timeout => {},
            };
        }
    }

    fn addDeadline(self: *State, entry: *DeadlineEntry) void {
        self.deadline_mutex.lockUncancelable(self.config.io);
        entry.next = self.deadline_head;
        self.deadline_head = entry;
        self.deadline_mutex.unlock(self.config.io);
        self.deadline_wakeup.set(self.config.io);
    }

    fn removeDeadline(self: *State, entry: *DeadlineEntry) void {
        self.deadline_mutex.lockUncancelable(self.config.io);
        defer self.deadline_mutex.unlock(self.config.io);
        var link = &self.deadline_head;
        while (link.*) |value| {
            if (value == entry) {
                link.* = value.next;
                return;
            }
            link = &value.next;
        }
        unreachable;
    }

    fn claimConnectionSlot(self: *State) ?*ConnectionSlot {
        for (self.connection_slots) |*slot| {
            if (slot.in_use.cmpxchgStrong(false, true, .acquire, .monotonic) == null) return slot;
        }
        return null;
    }

    fn acceptLoop(self: *State) void {
        const io = self.config.io;
        while (true) {
            const stream = self.listener.accept(io) catch |err| switch (err) {
                error.Canceled, error.SocketNotListening => return,
                // A peer disappearing during accept or temporary descriptor/
                // socket pressure must not permanently kill the listener.
                else => {
                    io.sleep(.fromMilliseconds(100), .awake) catch return;
                    continue;
                },
            };
            // A few short-lived handshake slots remain available for separate
            // CancelRequest connections when all authenticated slots are busy.
            if (self.active.fetchAdd(1, .acq_rel) >= @as(u32, self.config.max_connections) + 4) {
                _ = self.active.fetchSub(1, .release);
                stream.close(io);
                continue;
            }
            self.tasks.concurrent(io, serveConnection, .{ self, stream }) catch {
                _ = self.active.fetchSub(1, .release);
                stream.close(io);
            };
        }
    }

    fn register(raw: *anyopaque, session: *protocol.Session) !void {
        const self: *State = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.config.io);
        defer self.mutex.unlock(self.config.io);
        if (self.sessions.count() >= self.config.max_connections) return error.TooManyConnections;
        while (true) {
            self.next_pid = (self.next_pid % std.math.maxInt(i32)) + 1;
            const pid: i32 = @intCast(self.next_pid);
            if (self.sessions.contains(pid)) continue;
            var bytes: [4]u8 = undefined;
            try self.config.io.randomSecure(&bytes);
            const secret = std.mem.readInt(i32, &bytes, .big);
            try self.sessions.put(self.alloc, pid, session);
            session.backend_pid = pid;
            session.cancel_key = secret;
            return;
        }
    }

    fn unregister(raw: *anyopaque, session: *protocol.Session) void {
        if (session.backend_pid == 0) return;
        const self: *State = @ptrCast(@alignCast(raw));
        self.mutex.lockUncancelable(self.config.io);
        defer self.mutex.unlock(self.config.io);
        _ = self.sessions.remove(session.backend_pid);
        session.backend_pid = 0;
    }

    fn cancel(raw: *anyopaque, pid: i32, secret: i32) void {
        const self: *State = @ptrCast(@alignCast(raw));
        self.mutex.lock(self.config.io) catch return;
        defer self.mutex.unlock(self.config.io);
        const session = self.sessions.get(pid) orelse return;
        if (session.cancel_key != secret) return;
        if (session.executing.load(.acquire)) session.cancel_requested.store(true, .release);
    }
};

pub fn start(alloc: std.mem.Allocator, config: Config) !Server {
    if (config.max_connections == 0 or config.limits.frame_bytes < 8 or
        config.limits.frame_bytes > std.math.maxInt(i32) or
        config.limits.parameters > std.math.maxInt(i16) or config.limits.columns > std.math.maxInt(i16) or
        config.limits.connection_bytes < config.limits.frame_bytes or
        config.limits.startup_timeout_ms == 0 or config.limits.idle_timeout_ms == 0 or
        config.limits.statement_timeout_ms == 0 or config.limits.result_rows == 0 or
        config.limits.retained_bytes == 0 or config.limits.retained_bytes > config.limits.connection_bytes or
        config.limits.idle_transaction_timeout_ms == 0)
        return error.InvalidPgwireConfig;
    const address = try std.Io.net.IpAddress.resolve(config.io, config.bind_host, config.bind_port);
    if (!config.allow_insecure_non_loopback and !isLoopback(address)) return error.PgwireRequiresProtectedTransport;
    const self = try alloc.create(State);
    errdefer alloc.destroy(self);
    const session_bytes = if (config.max_session_bytes == 0)
        std.math.mul(usize, config.max_connections, config.limits.connection_bytes) catch return error.InvalidPgwireConfig
    else
        std.math.cast(usize, config.max_session_bytes) orelse return error.InvalidPgwireConfig;
    const slot_count = @as(usize, config.max_connections) + 4;
    const slot_bytes = std.math.mul(usize, slot_count, @sizeOf(ConnectionSlot)) catch return error.InvalidPgwireConfig;
    const protected_bytes: usize = if (config.protected_frame_allocator != null) 32 << 10 else 0;
    const control_reserve = std.math.mul(usize, slot_count, protected_bytes) catch return error.InvalidPgwireConfig;
    const fixed_reserve = std.math.add(usize, slot_bytes, control_reserve) catch return error.InvalidPgwireConfig;
    const minimum_session_bytes = std.math.add(usize, fixed_reserve, config.limits.frame_bytes) catch return error.InvalidPgwireConfig;
    if (session_bytes < minimum_session_bytes or
        config.limits.connection_bytes <= protected_bytes or
        config.limits.connection_bytes - protected_bytes < @sizeOf(ConnectionSlot)) return error.InvalidPgwireConfig;
    const retained_bytes = if (config.max_retained_bytes == 0)
        std.math.mul(usize, config.max_connections, config.limits.retained_bytes) catch return error.InvalidPgwireConfig
    else
        std.math.cast(usize, config.max_retained_bytes) orelse return error.InvalidPgwireConfig;
    self.* = .{ .alloc = alloc, .config = config, .retained = .{ .child = alloc, .limit = @min(retained_bytes, session_bytes) }, .listener = try address.listen(config.io, .{ .reuse_address = true }), .memory = .{ .child = alloc, .limit = session_bytes } };
    errdefer self.listener.deinit(config.io);
    // Stable allocator/request/cancellation contexts and socket buffers are
    // charged before accepts. Four preallocated handshake owners remain even
    // when ordinary allocations fill the listener's aggregate memory ceiling.
    self.connection_slots = try self.memory.allocator().alloc(ConnectionSlot, slot_count);
    errdefer self.memory.allocator().free(self.connection_slots);
    for (self.connection_slots) |*slot| slot.* = .{ .connection = undefined };
    self.foreground_memory = .{ .child = self.memory.allocator(), .limit = session_bytes - fixed_reserve };
    errdefer self.tasks.cancel(config.io);
    try self.tasks.concurrent(config.io, State.deadlines, .{self});
    try self.tasks.concurrent(config.io, State.acceptLoop, .{self});
    return .{ .state = self };
}

fn isLoopback(address: std.Io.net.IpAddress) bool {
    return switch (address) {
        .ip4 => |ip| ip.bytes[0] == 127,
        .ip6 => |ip| std.mem.eql(u8, &ip.bytes, &std.Io.net.Ip6Address.loopback(0).bytes),
    };
}

/// A slot's address never changes while live. Its startup charge includes
/// allocator/cancel contexts and both socket buffers; no task-stack receiver
/// escapes into a retained native reader. Teardown remains physically joined.
const ConnectionSlot = struct {
    in_use: std.atomic.Value(bool) = .init(false),
    connection: Connection,
};

const Connection = struct {
    slot: *ConnectionSlot,
    budget: Budget,
    frame_control: Budget,
    frame_memory: @import("../common/allocation_budget.zig").Partitioned,
    read_buffer: [8192]u8,
    write_buffer: [8192]u8,
    reader: std.Io.net.Stream.Reader,
    writer: std.Io.net.Stream.Writer,
    session: protocol.Session,
    deadline_entry: State.DeadlineEntry,

    fn init(self: *Connection, state: *State, slot: *ConnectionSlot, stream: std.Io.net.Stream) void {
        const io = state.config.io;
        const protected_bytes: usize = if (state.config.protected_frame_allocator != null) 32 << 10 else 0;
        self.slot = slot;
        // The slot is prepaid globally. Include its bytes in the connection
        // ceiling too, without charging the same physical body twice.
        const foreground = if (state.foreground_memory) |*memory| memory.allocator() else state.memory.allocator();
        self.budget = .{ .child = foreground, .limit = state.config.limits.connection_bytes - protected_bytes, .used = .init(@sizeOf(ConnectionSlot)) };
        self.frame_control = .{ .child = state.config.protected_frame_allocator orelse self.budget.allocator(), .limit = protected_bytes, .shared = &state.memory };
        self.frame_memory = .{
            .foreground = .{ .child = self.budget.allocator(), .limit = self.budget.limit },
            .control = .{ .child = self.frame_control.allocator(), .limit = protected_bytes },
        };
        self.reader = stream.reader(io, &self.read_buffer);
        self.writer = stream.writer(io, &self.write_buffer);
        self.session = .{
            .alloc = self.budget.allocator(),
            .io = io,
            .source = state.config.backend,
            .reader = &self.reader.interface,
            .writer = &self.writer.interface,
            .limits = state.config.limits,
            .frame_memory = if (protected_bytes != 0) &self.frame_memory else null,
            .control_allocator = state.config.control_allocator,
            .is_protected_control = state.config.is_protected_control,
            .retained_shared = &state.retained,
            .deadline_wakeup = &state.deadline_wakeup,
            .hooks = .{ .context = state, .register = State.register, .unregister = State.unregister, .cancel = State.cancel },
        };
        self.session.setDeadline(self.session.limits.startup_timeout_ms);
        self.deadline_entry = .{ .session = &self.session, .stream = stream };
    }

    fn deinit(self: *Connection) void {
        self.session.deinit();
        std.debug.assert(self.frame_memory.foreground.stats().used_bytes == 0);
        std.debug.assert(self.frame_memory.control.stats().used_bytes == 0);
        std.debug.assert(self.frame_control.used.load(.acquire) == 0);
        std.debug.assert(self.budget.used.load(.acquire) == @sizeOf(ConnectionSlot));
        // Physical slot memory remains charged until listener teardown. Only
        // after every reader/arena has retired may another task reuse its address.
        self.slot.in_use.store(false, .release);
    }
};

fn serveConnection(state: *State, stream: std.Io.net.Stream) void {
    const io = state.config.io;
    defer _ = state.active.fetchSub(1, .release);
    defer stream.close(io);
    const slot = state.claimConnectionSlot() orelse return;
    const connection = &slot.connection;
    connection.init(state, slot, stream);
    defer connection.deinit();
    state.addDeadline(&connection.deadline_entry);
    defer state.removeDeadline(&connection.deadline_entry);
    defer connection.session.cancel_requested.store(true, .release);
    connection.session.run() catch {};
}

fn watchDeadline(session: *protocol.Session) std.Io.Cancelable!void {
    while (true) {
        // Reset before loading: a concurrent deadline update is either included
        // in this load or leaves the event set. A newly shortened statement
        // deadline must wake a watchdog waiting on the former idle deadline.
        session.deadline_changed.reset();
        const raw = session.deadline_ns.load(.acquire);
        const deadline = std.Io.Clock.Timestamp{ .clock = .awake, .raw = .{ .nanoseconds = raw } };
        session.deadline_changed.waitTimeout(session.io, .{ .deadline = deadline }) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => {},
        };
        if (session.deadline_ns.load(.acquire) <= std.Io.Clock.awake.now(session.io).nanoseconds) return;
    }
}

test "pgwire remote plaintext binding fails closed" {
    try std.testing.expect(isLoopback(.{ .ip4 = .loopback(5432) }));
    try std.testing.expect(isLoopback(.{ .ip6 = .loopback(5432) }));
    try std.testing.expect(!isLoopback(.{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 5432 } }));
    try std.testing.expectError(error.PgwireRequiresProtectedTransport, start(std.testing.allocator, .{
        .io = std.testing.io,
        .backend = undefined,
        .bind_host = "0.0.0.0",
        .bind_port = 0,
    }));
}

test "pgwire listener owner joins idle accept and releases its bound port" {
    var listener = try start(std.testing.allocator, .{
        .io = std.testing.io,
        .backend = undefined,
        .bind_port = 0,
    });
    const address = listener.address();
    listener.deinit();
    listener.deinit();
    var rebound = try address.listen(std.testing.io, .{ .reuse_address = true });
    rebound.deinit(std.testing.io);
}

test "pgwire shortening an idle deadline wakes its owner IO watchdog" {
    const io = std.testing.io;
    var session = protocol.Session{
        .alloc = std.testing.allocator,
        .io = io,
        .source = undefined,
        .reader = undefined,
        .writer = undefined,
    };
    session.setDeadline(60_000);
    const Outcome = union(enum) { watchdog: std.Io.Cancelable!void, guard: std.Io.Cancelable!void };
    var outcomes: [2]Outcome = undefined;
    var select = std.Io.Select(Outcome).init(io, &outcomes);
    defer select.cancelDiscard();
    try select.concurrent(.watchdog, watchDeadline, .{&session});
    // Observe that the watcher consumed the old deadline notification before
    // shortening it; otherwise the test could pass by starting after the update.
    var retries: usize = 0;
    while (session.deadline_changed.isSet() and retries < 3000) : (retries += 1)
        try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(!session.deadline_changed.isSet());
    session.setDeadline(1);
    const guard = struct {
        fn wait(clock_io: std.Io) std.Io.Cancelable!void {
            try clock_io.sleep(.fromSeconds(10), .awake);
        }
    };
    try select.concurrent(.guard, guard.wait, .{io});
    const result = try select.await();
    try std.testing.expect(result == .watchdog);
    try result.watchdog;
}

test "pgwire shared session budget rejects impossible aggregate limits" {
    try std.testing.expectError(error.InvalidPgwireConfig, start(std.testing.allocator, .{
        .io = std.testing.io,
        .backend = undefined,
        .bind_port = 0,
        .max_session_bytes = 8,
    }));
}

test "pgwire idle sockets share one deadline owner and join under saturation" {
    const io = std.testing.io;
    var listener = try start(std.testing.allocator, .{
        .io = io,
        .backend = undefined,
        .bind_port = 0,
        .max_connections = 16,
        .limits = .{ .startup_timeout_ms = 30_000 },
    });
    defer listener.deinit();
    var streams: [16]std.Io.net.Stream = undefined;
    var opened: usize = 0;
    defer for (streams[0..opened]) |stream| stream.close(io);
    for (&streams) |*stream| {
        stream.* = try listener.address().connect(io, .{ .mode = .stream });
        opened += 1;
    }
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
    while (listener.stats().deadline_entries != streams.len) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ConnectionsDidNotStart;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const stats = listener.stats();
    try std.testing.expectEqual(@as(u32, 16), stats.connections);
    try std.testing.expectEqual(@as(u32, 1), stats.deadline_tasks);
    try std.testing.expectEqual(@sizeOf(ConnectionSlot) * (@as(usize, 16) + 4), stats.session_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.retained_bytes);
    // Shortening a live connection deadline must wake the one shared owner.
    const state = listener.state.?;
    state.deadline_mutex.lockUncancelable(io);
    state.deadline_head.?.session.setDeadline(1);
    state.deadline_mutex.unlock(io);
    while (listener.stats().connections == streams.len) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.DeadlineDidNotWake;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(u64, 1), listener.stats().expired_connections);
    listener.deinit();
}

test "pgwire retained sockets survive a saturated shared outbound executor" {
    const io = std.testing.io;
    var outbound = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .limited(8),
    });
    defer outbound.deinit();
    const outbound_io = outbound.io();
    var release: std.Io.Event = .unset;
    var jobs: std.Io.Group = .init;
    defer jobs.cancel(outbound_io);
    defer release.set(outbound_io);
    const Blocked = struct {
        fn run(event: *std.Io.Event, task_io: std.Io) std.Io.Cancelable!void {
            try event.wait(task_io);
        }
    };
    for (0..8) |_| try jobs.concurrent(outbound_io, Blocked.run, .{ &release, outbound_io });
    // The historical listener borrowed this executor. Saturation prevents even
    // submitting its accept task, independently of SQL admission and auth.
    try std.testing.expectError(error.ConcurrencyUnavailable, jobs.concurrent(outbound_io, Blocked.run, .{ &release, outbound_io }));

    var transport = initTransport(std.testing.allocator, 32);
    defer transport.deinit();
    var listener = try start(std.testing.allocator, .{
        .io = transport.io(),
        .backend = undefined,
        .bind_port = 0,
        .max_connections = 32,
        .limits = .{ .startup_timeout_ms = 30_000 },
    });
    defer listener.deinit();
    var peers: [32]std.Io.net.Stream = undefined;
    var opened: usize = 0;
    defer for (peers[0..opened]) |peer| peer.close(io);
    for (&peers) |*peer| {
        peer.* = try listener.address().connect(io, .{ .mode = .stream });
        opened += 1;
    }
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
    while (listener.stats().deadline_entries != peers.len) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.ConnectionsDidNotStart;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(u32, 32), listener.stats().connections);
    // All outbound slots remain occupied; the listener has not stolen one.
    try std.testing.expectError(error.ConcurrencyUnavailable, jobs.concurrent(outbound_io, Blocked.run, .{ &release, outbound_io }));
    const address = listener.address();
    listener.deinit();
    var rebound = try address.listen(io, .{ .reuse_address = true });
    rebound.deinit(io);
}

test "pgwire connection owners retain allocator cancellation and IO addresses under aggregate saturation" {
    const io = std.testing.io;
    const bind_address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var listener = try bind_address.listen(io, .{ .reuse_address = true });
    defer listener.deinit(io);
    const peer = try listener.socket.address.connect(io, .{ .mode = .stream });
    defer peer.close(io);
    const stream = try listener.accept(io);
    defer stream.close(io);
    var state: State = .{
        .alloc = std.testing.allocator,
        .config = .{ .io = io, .backend = undefined },
        .listener = undefined,
        .memory = .{ .child = std.testing.allocator, .limit = 2 * @sizeOf(ConnectionSlot) + 32 },
        .retained = .{ .child = std.testing.allocator, .limit = 128 },
    };
    state.connection_slots = try state.memory.allocator().alloc(ConnectionSlot, 2);
    defer state.memory.allocator().free(state.connection_slots);
    for (state.connection_slots) |*slot| slot.* = .{ .connection = undefined };
    const prepaid = state.memory.used.load(.acquire);
    try std.testing.expectEqual(2 * @sizeOf(ConnectionSlot), prepaid);
    const pressure = try state.memory.allocator().alloc(u8, 32);
    defer state.memory.allocator().free(pressure);
    try std.testing.expectEqual(state.memory.limit, state.memory.used.load(.acquire));
    const first = state.claimConnectionSlot() orelse return error.TestUnexpectedResult;
    first.connection.init(&state, first, stream);
    var first_live = true;
    defer if (first_live) first.connection.deinit();
    const second = state.claimConnectionSlot() orelse return error.TestUnexpectedResult;
    second.connection.init(&state, second, stream);
    defer second.connection.deinit();
    try std.testing.expect(first != second);
    try std.testing.expect(state.claimConnectionSlot() == null);
    try std.testing.expect(first.connection.session.alloc.ptr == @as(*anyopaque, @ptrCast(&first.connection.budget)));
    try std.testing.expect(first.connection.frame_memory.foreground.child.ptr == @as(*anyopaque, @ptrCast(&first.connection.budget)));
    try std.testing.expect(first.connection.frame_memory.control.child.ptr == @as(*anyopaque, @ptrCast(&first.connection.frame_control)));
    try std.testing.expect(first.connection.session.reader == &first.connection.reader.interface);
    try std.testing.expect(first.connection.session.writer == &first.connection.writer.interface);
    try std.testing.expect(first.connection.deadline_entry.session == &first.connection.session);
    first.connection.session.cancel_requested.store(true, .release);
    try std.testing.expect(first.connection.deadline_entry.session.cancel_requested.load(.acquire));
    try std.testing.expectError(error.OutOfMemory, first.connection.budget.allocator().alloc(u8, 1));
    // Slot reuse happens after physical session cleanup, without returning the
    // prepaid listener charge or borrowing an earlier task-stack allocator.
    first.connection.deinit();
    first_live = false;
    const reused = state.claimConnectionSlot() orelse return error.TestUnexpectedResult;
    try std.testing.expect(reused == first);
    reused.connection.init(&state, reused, stream);
    defer reused.connection.deinit();
    try std.testing.expect(!reused.connection.session.cancel_requested.load(.acquire));
    try std.testing.expectEqual(state.memory.limit, state.memory.used.load(.acquire));
}

test "pgwire startup rejects memory ceilings below its physical connection owner pool" {
    try std.testing.expectError(error.InvalidPgwireConfig, start(std.testing.allocator, .{
        .io = std.testing.io,
        .backend = undefined,
        .bind_port = 0,
        .max_connections = 1,
        .max_session_bytes = 5 * @sizeOf(ConnectionSlot),
    }));
}

test "pgwire connection owner pool allocation failure retires startup socket and metadata" {
    const io = std.testing.io;
    const bind_address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var initial = try bind_address.listen(io, .{ .reuse_address = true });
    const address = initial.socket.address;
    initial.deinit(io);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    try std.testing.expectError(error.OutOfMemory, start(failing.allocator(), .{
        .io = io,
        .backend = undefined,
        .bind_port = address.getPort(),
        .max_connections = 1,
    }));
    var rebound = try address.listen(io, .{ .reuse_address = true });
    rebound.deinit(io);
}

test "pgwire aggregate ordinary saturation preserves every protected frame and handshake allowance" {
    const io = std.testing.io;
    const bind_address: std.Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var transport = try bind_address.listen(io, .{ .reuse_address = true });
    defer transport.deinit(io);
    const peer = try transport.socket.address.connect(io, .{ .mode = .stream });
    defer peer.close(io);
    const stream = try transport.accept(io);
    defer stream.close(io);
    const owner_count = 5;
    const protected_bytes = 32 << 10;
    const ordinary_bytes = 4096;
    const pool_bytes = owner_count * @sizeOf(ConnectionSlot);
    var listener = try start(std.testing.allocator, .{
        .io = io,
        .backend = undefined,
        .bind_port = 0,
        .max_connections = 1,
        .max_session_bytes = pool_bytes + owner_count * protected_bytes + ordinary_bytes,
        .protected_frame_allocator = std.testing.allocator,
        .limits = .{ .frame_bytes = ordinary_bytes, .connection_bytes = 1024 * 1024, .retained_bytes = 16 * 1024 },
    });
    defer listener.deinit();
    const state = listener.state.?;
    try std.testing.expectEqual(@as(usize, ordinary_bytes), state.foreground_memory.?.limit);
    try std.testing.expectEqual(pool_bytes, state.memory.used.load(.acquire));
    var slots: [owner_count]*ConnectionSlot = undefined;
    var controls: [owner_count]?[]u8 = @splat(null);
    var claimed: usize = 0;
    defer {
        for (slots[0..claimed], controls[0..claimed]) |slot, control| {
            if (control) |bytes| slot.connection.frame_control.allocator().free(bytes);
            slot.connection.deinit();
        }
    }
    for (&slots) |*slot| {
        slot.* = state.claimConnectionSlot() orelse return error.TestUnexpectedResult;
        slot.*.connection.init(state, slot.*, stream);
        claimed += 1;
    }
    const ordinary = try slots[0].connection.budget.allocator().alloc(u8, ordinary_bytes);
    defer slots[0].connection.budget.allocator().free(ordinary);
    for (slots) |slot| try std.testing.expectError(error.OutOfMemory, slot.connection.budget.allocator().alloc(u8, 1));
    // All five configured owner allowances remain available simultaneously,
    // despite an explicit aggregate limit below the per-connection ceilings.
    for (slots, &controls) |slot, *bytes| bytes.* = try slot.connection.frame_control.allocator().alloc(u8, protected_bytes);
    try std.testing.expectEqual(state.memory.limit, state.memory.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, ordinary_bytes), state.foreground_memory.?.used.load(.acquire));
    slots[0].connection.frame_control.allocator().free(controls[0].?);
    controls[0] = null;
    // Returning protected credit does not authorize an ordinary allocation.
    try std.testing.expectError(error.OutOfMemory, slots[0].connection.budget.allocator().alloc(u8, 1));
    controls[0] = try slots[0].connection.frame_control.allocator().alloc(u8, protected_bytes);
    try std.testing.expectEqual(state.memory.limit, state.memory.used.load(.acquire));
}

test "pgwire startup rejects aggregate limits that cannot retain protected frame headroom" {
    try std.testing.expectError(error.InvalidPgwireConfig, start(std.testing.allocator, .{
        .io = std.testing.io,
        .backend = undefined,
        .bind_port = 0,
        .max_connections = 1,
        .max_session_bytes = 5 * @sizeOf(ConnectionSlot) + 4096,
        .protected_frame_allocator = std.testing.allocator,
        .limits = .{ .frame_bytes = 4096, .connection_bytes = 1024 * 1024, .retained_bytes = 16 * 1024 },
    }));
}
