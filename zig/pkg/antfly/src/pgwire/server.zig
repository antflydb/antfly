// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const protocol = @import("protocol.zig");
const backend = @import("backend.zig");
const Budget = @import("budget.zig").Budget;

pub const Config = struct {
    io: std.Io,
    backend: backend.Backend,
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 5432,
    max_connections: u16 = 32,
    limits: protocol.Limits = .{},
    // There is no built-in TLS termination yet. A remote bind must explicitly
    // acknowledge that TLS/authenticated private-network protection is supplied
    // by the deployment; never silently send passwords over a public socket.
    allow_insecure_non_loopback: bool = false,
};

pub const Server = struct {
    state: ?*State,

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
    mutex: std.Io.Mutex = .init,
    next_pid: u32 = 0,
    sessions: std.AutoHashMapUnmanaged(i32, *protocol.Session) = .empty,

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
        config.limits.statement_timeout_ms == 0 or config.limits.result_rows == 0)
        return error.InvalidPgwireConfig;
    const address = try std.Io.net.IpAddress.resolve(config.io, config.bind_host, config.bind_port);
    if (!config.allow_insecure_non_loopback and !isLoopback(address)) return error.PgwireRequiresProtectedTransport;
    const self = try alloc.create(State);
    errdefer alloc.destroy(self);
    self.* = .{ .alloc = alloc, .config = config, .listener = try address.listen(config.io, .{ .reuse_address = true }) };
    errdefer self.listener.deinit(config.io);
    try self.tasks.concurrent(config.io, State.acceptLoop, .{self});
    return .{ .state = self };
}

fn isLoopback(address: std.Io.net.IpAddress) bool {
    return switch (address) {
        .ip4 => |ip| ip.bytes[0] == 127,
        .ip6 => |ip| std.mem.eql(u8, &ip.bytes, &std.Io.net.Ip6Address.loopback(0).bytes),
    };
}

fn serveConnection(state: *State, stream: std.Io.net.Stream) void {
    const io = state.config.io;
    defer _ = state.active.fetchSub(1, .release);
    defer stream.close(io);
    var budget = Budget{ .child = state.alloc, .limit = state.config.limits.connection_bytes };
    var read_buffer: [8192]u8 = undefined;
    var write_buffer: [8192]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);
    var session = protocol.Session{
        .alloc = budget.allocator(),
        .io = io,
        .source = state.config.backend,
        .reader = &reader.interface,
        .writer = &writer.interface,
        .limits = state.config.limits,
        .hooks = .{ .context = state, .register = State.register, .unregister = State.unregister, .cancel = State.cancel },
    };
    defer session.deinit();
    session.setDeadline(session.limits.startup_timeout_ms);
    const Outcome = union(enum) { session: anyerror!void, deadline: std.Io.Cancelable!void };
    var outcomes: [2]Outcome = undefined;
    var select = std.Io.Select(Outcome).init(io, &outcomes);
    defer {
        // Also wake request-owned native checkpoints. A storage mutation job
        // may deliberately join uncancelably after durable admission instead
        // of abandoning its response/identity memory when a socket closes.
        session.cancel_requested.store(true, .release);
        select.cancelDiscard();
    }
    select.concurrent(.session, protocol.Session.run, .{&session}) catch return;
    select.concurrent(.deadline, watchDeadline, .{&session}) catch return;
    const outcome = select.await() catch return;
    switch (outcome) {
        .session => |result| result catch {},
        // Closing a timed-out connection communicates an uncertain outcome for
        // in-flight mutations; never fabricate a retry-safe backend error.
        .deadline => {},
    }
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
