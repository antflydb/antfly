// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
// Actual TCP clients driving the native adapter supplied by the storage fixture.
const std = @import("std");
const http = @import("http_server.zig");
const admission = @import("antfly_local_sources").common_request_admission.RequestAdmission;
const resources = @import("antfly_local_sources").storage_resource_manager;
const io = std.testing.io;
const alloc = std.testing.allocator;

const Peer = struct {
    stream: std.Io.net.Stream,
    buffer: [4096]u8 = undefined,
    reader: std.Io.net.Stream.Reader = undefined,
    payload: [65536]u8 = undefined,
    pid: i32 = 0,
    secret: i32 = 0,
    closed: bool = false,
    fn init(self: *Peer, address: std.Io.net.IpAddress) !void {
        self.* = .{ .stream = try address.connect(io, .{ .mode = .stream }) };
        self.reader = self.stream.reader(io, &self.buffer);
        var out = self.stream.writer(io, &.{});
        const body = "user\x00pool-reader\x00database\x00default\x00\x00";
        try out.interface.writeInt(u32, body.len + 8, .big);
        try out.interface.writeInt(u32, 196608, .big);
        try out.interface.writeAll(body);
        try frame(&out.interface, 'p', "secret\x00");
        try out.interface.flush();
        _ = try self.ready('I', null);
    }
    fn close(self: *Peer) void {
        if (!self.closed) self.stream.close(io);
        self.closed = true;
    }
    fn send(self: *Peer, tag: u8, bytes: []const u8) !void {
        var out = self.stream.writer(io, &.{});
        try frame(&out.interface, tag, bytes);
        try out.interface.flush();
    }
    fn query(self: *Peer, statement: [:0]const u8, status: u8, code: ?[]const u8) !Reply {
        try self.send('Q', statement[0 .. statement.len + 1]);
        return self.ready(status, code) catch |err| {
            std.debug.print("native pgwire failing statement: {s}\n", .{statement});
            return err;
        };
    }
    fn ready(self: *Peer, status: u8, code: ?[]const u8) !Reply {
        var reply: Reply = .{};
        var error_seen = false;
        while (true) {
            const tag = try self.reader.interface.takeByte();
            const size = try self.reader.interface.takeInt(u32, .big);
            try std.testing.expect(size >= 4 and size - 4 <= self.payload.len);
            const bytes = self.payload[0 .. size - 4];
            try self.reader.interface.readSliceAll(bytes);
            switch (tag) {
                'K' => {
                    self.pid = std.mem.readInt(i32, bytes[0..4], .big);
                    self.secret = std.mem.readInt(i32, bytes[4..8], .big);
                },
                'E' => {
                    error_seen = true;
                    if (code) |expected| {
                        const marker = try std.fmt.allocPrint(alloc, "C{s}\x00", .{expected});
                        defer alloc.free(marker);
                        try std.testing.expect(std.mem.indexOf(u8, bytes, marker) != null);
                    } else {
                        std.debug.print("native pgwire socket error: {s}\n", .{bytes});
                        return error.UnexpectedPgwireError;
                    }
                },
                'D' => {
                    reply.rows += 1;
                    if (bytes.len >= 7 and std.mem.readInt(u16, bytes[0..2], .big) > 0) {
                        const n = std.mem.readInt(u32, bytes[2..6], .big);
                        if (n <= bytes.len - 6) reply.exact_integer = reply.exact_integer or std.mem.eql(u8, bytes[6..][0..n], "9007199254740993");
                    }
                },
                's' => reply.suspended = true,
                'Z' => {
                    try std.testing.expectEqual(status, bytes[0]);
                    try std.testing.expectEqual(code != null, error_seen);
                    return reply;
                },
                else => {},
            }
        }
    }
};
const Reply = struct { rows: usize = 0, suspended: bool = false, exact_integer: bool = false };
fn frame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(payload.len + 4), .big);
    try out.writeAll(payload);
}
fn cancel(address: std.Io.net.IpAddress, pid: i32, secret: i32) !void {
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var out = stream.writer(io, &.{});
    try out.interface.writeInt(u32, 16, .big);
    try out.interface.writeInt(u32, 80877102, .big);
    try out.interface.writeInt(i32, pid, .big);
    try out.interface.writeInt(i32, secret, .big);
    try out.interface.flush();
}
fn awaitQueued(server: *http.ApiHttpServer) !void {
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
    while (server.queryAdmissionStats().queued == 0) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.WaiterDidNotQueue;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

fn holdQueryAdmission(server: *http.ApiHttpServer) !admission.Lease {
    // ReadyForQuery acknowledges protocol progress before the response owner's
    // physical retirement. Wait on the same bounded admission queue instead of
    // assuming its retained lease has already been released by another worker.
    return server.query_admission.acquire(.{
        .io = io,
        .deadline_ns = @intCast(std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3)).nanoseconds),
    });
}

pub fn qualify(server: *http.ApiHttpServer, storage: *resources.ResourceManager) !void {
    // A watchdog bounds the entire native socket qualification, including a
    // regression that loses ReadyForQuery or fails to join listener shutdown.
    var done: std.Io.Event = .unset;
    var future = try io.concurrent(run, .{ server, storage, &done });
    defer future.cancel(io) catch {};
    try done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(20), .clock = .awake } });
    try future.await(io);
}
fn run(server: *http.ApiHttpServer, storage: *resources.ResourceManager, done: *std.Io.Event) anyerror!void {
    defer done.set(io);
    var adapter = @import("sql_pgwire.zig").Adapter{ .server = server };
    const transport_alloc = if (server.transport_memory) |memory| memory.allocator() else server.state_alloc;
    var listener = try @import("../pgwire/server.zig").start(transport_alloc, .{ .io = io, .backend = adapter.backend(), .bind_port = 0, .max_connections = 2 });
    defer listener.deinit();
    var first: Peer = undefined;
    try first.init(listener.address());
    defer first.close();
    var second: Peer = undefined;
    try second.init(listener.address());
    defer second.close();
    _ = try first.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", 'T', null);
    _ = try second.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", 'T', null);
    _ = try second.query("SELECT", 'E', "42601");
    _ = try first.query("DECLARE held CURSOR WITH HOLD FOR SELECT id FROM usage_records ORDER BY id", 'T', null);
    _ = try first.query("FETCH NEXT FROM held", 'T', null);
    {
        var query = server.query_admission.tryAcquireLease();
        defer if (query) |*lease| lease.release();
        var write = server.write_admission.tryAcquireLease().?;
        defer write.release();
        var preparations: [2]admission.Lease = undefined;
        for (&preparations) |*lease| lease.* = server.sql_preparation_admission.tryAcquireLease().?;
        defer for (&preparations) |*lease| lease.release();
        var entered: std.atomic.Value(usize) = .init(0);
        var release: std.Io.Event = .unset;
        const foreground = try server.sqlExecutionIo();
        var workers: std.Io.Group = .init;
        defer workers.await(foreground) catch {};
        defer release.set(foreground);
        for (0..server.sql_workers.?.capacity) |_| try workers.concurrent(foreground, struct {
            fn block(n: *std.atomic.Value(usize), event: *std.Io.Event, worker_io: std.Io) void {
                _ = n.fetchAdd(1, .release);
                event.waitUncancelable(worker_io);
            }
        }.block, .{ &entered, &release, foreground });
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
        while (entered.load(.acquire) != server.sql_workers.?.capacity) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.WorkersDidNotStart;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        const memory = server.request_memory.?;
        const remaining = memory.foreground.limit - memory.foreground.used.load(.acquire);
        const pressure = try server.alloc.alloc(u8, remaining - @import("../common/allocation_budget.zig").Partitioned.overhead(.of(u8)));
        defer server.alloc.free(pressure);
        try std.testing.expectError(error.OutOfMemory, server.alloc.alloc(u8, 1));
        // Real storage owner: deny new managed memory while retiring existing
        // snapshots/cursors. No assertion of distributed WAL progress is made.
        const storage_stats = storage.snapshot().memory;
        var storage_pressure = try storage.reserveWithoutReclaim(.lsm_compaction_work, storage_stats.hard_limit_bytes - storage_stats.used_bytes);
        defer storage_pressure.release();
        try std.testing.expectError(error.ResourceBudgetExceeded, storage.reserveWithoutReclaim(.dense_search_working_set, 1));
        _ = try first.query("COMMIT", 'I', null);
        _ = try second.query("ROLLBACK", 'I', null);
        _ = try first.query("BEGIN", 'I', "53300");
    }
    const held_rows = try first.query("FETCH ALL FROM held", 'I', null);
    try std.testing.expectEqual(@as(usize, 8), held_rows.rows);
    try std.testing.expect(held_rows.exact_integer);
    _ = try first.query("CLOSE held", 'I', null);
    // Parse, text Bind and a suspended native portal, followed by continuation.
    _ = try first.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", 'T', null);
    var packets = std.Io.Writer.Allocating.init(alloc);
    defer packets.deinit();
    try frame(&packets.writer, 'P', "p\x00SELECT id FROM usage_records WHERE id >= $1 ORDER BY id\x00\x00\x01\x00\x00\x00\x14");
    try frame(&packets.writer, 'B', "portal\x00p\x00\x00\x00\x00\x01\x00\x00\x00\x01" ++ "2" ++ "\x00\x00");
    try frame(&packets.writer, 'E', "portal\x00\x00\x00\x00\x01");
    try frame(&packets.writer, 'S', "");
    var out = first.stream.writer(io, &.{});
    try out.interface.writeAll(packets.written());
    try out.interface.flush();
    const page = try first.ready('T', null);
    try std.testing.expectEqual(@as(usize, 1), page.rows);
    try std.testing.expect(page.suspended);
    try first.send('E', "portal\x00\x00\x00\x00\x00");
    try first.send('S', "");
    const rest = try first.ready('T', null);
    try std.testing.expectEqual(@as(usize, 8), rest.rows);
    try std.testing.expect(rest.exact_integer);
    try first.send('C', "Pportal\x00");
    try first.send('S', "");
    _ = try first.ready('T', null);
    _ = try first.query("ROLLBACK", 'I', null);
    // Extended-protocol error discards messages until Sync; then usable again.
    try first.send('P', "bad\x00SELECT\x00\x00\x00");
    try first.send('E', "missing\x00\x00\x00\x00\x00");
    try first.send('S', "");
    _ = try first.ready('I', "42601");
    const exact = try first.query("SELECT id FROM usage_records WHERE _id = 'a'", 'I', null);
    try std.testing.expect(exact.exact_integer);
    {
        var held = try holdQueryAdmission(server);
        defer held.release();
        try second.send('Q', "SELECT id FROM usage_records\x00");
        try awaitQueued(server);
        try cancel(listener.address(), second.pid, second.secret ^ 1);
        try io.sleep(.fromMilliseconds(20), .awake);
        try std.testing.expectEqual(@as(usize, 1), server.queryAdmissionStats().queued);
        try cancel(listener.address(), second.pid, second.secret);
        _ = try second.ready('I', "57014");
        try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().queued);
    }
    // Disconnect cleanup is joined on a reserved executor with fresh SQL full.
    const baseline = try server.txn_sessions.durable.?.sessionCount();
    _ = try first.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", 'T', null);
    {
        var preparations: [2]admission.Lease = undefined;
        for (&preparations) |*lease| lease.* = server.sql_preparation_admission.tryAcquireLease().?;
        defer for (&preparations) |*lease| lease.release();
        var write = server.write_admission.tryAcquireLease().?;
        defer write.release();
        first.close();
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
        while (try server.txn_sessions.durable.?.sessionCount() != baseline) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.DisconnectDidNotReclaimSession;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }
    // Shutdown must cancel and join an actual socket query in admission wait.
    var held = try holdQueryAdmission(server);
    defer held.release();
    try second.send('Q', "SELECT id FROM usage_records\x00");
    try awaitQueued(server);
    listener.deinit();
    try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().queued);
    try std.testing.expectEqual(@as(usize, 1), server.queryAdmissionStats().in_flight);
}
