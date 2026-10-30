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

//! Bounded real TCP client shared by focused pooling integration fixtures.
const std = @import("std");

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    stream: std.Io.net.Stream,
    deadline: std.Io.Clock.Timestamp,
    guard: std.Io.Group = .init,
    backend_pid: i32 = 0,
    backend_secret: i32 = 0,
    backend_key_received: bool = false,
    read_buffer: [2048]u8 = undefined,
    write_buffer: [512]u8 = undefined,
    body_buffer: [4096]u8 = undefined,
    reader: std.Io.net.Stream.Reader = undefined,
    writer: std.Io.net.Stream.Writer = undefined,
    pub const Frame = struct { tag: u8, body: []const u8 };

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, port: u16, password: []const u8) !*Client {
        return connectAs(allocator, io, port, "image-reader", password);
    }
    pub fn connectAs(allocator: std.mem.Allocator, io: std.Io, port: u16, username: []const u8, password: []const u8) !*Client {
        const deadline: std.Io.Clock.Timestamp = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(10)) };
        const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(port) };
        const stream = try address.connect(io, .{ .mode = .stream, .timeout = .{ .deadline = deadline } });
        errdefer stream.close(io);
        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .io = io, .stream = stream, .deadline = deadline };
        self.reader = stream.reader(io, &self.read_buffer);
        self.writer = stream.writer(io, &self.write_buffer);
        try self.guard.concurrent(io, expire, .{self});
        errdefer self.guard.cancel(io);
        try self.authenticate(username, password);
        return self;
    }
    pub fn expire(self: *Client) void {
        self.deadline.wait(self.io) catch return;
        self.stream.shutdown(self.io, .both) catch {};
    }
    pub fn close(self: *Client) void {
        self.guard.cancel(self.io);
        self.stream.shutdown(self.io, .both) catch {};
        self.stream.close(self.io);
        self.allocator.destroy(self);
    }
    pub fn send(self: *Client, tag: u8, body: []const u8) !void {
        try self.writer.interface.writeByte(tag);
        try self.writer.interface.writeInt(u32, @intCast(body.len + 4), .big);
        try self.writer.interface.writeAll(body);
        try self.writer.interface.flush();
    }
    pub fn next(self: *Client) !Frame {
        const tag = try self.reader.interface.takeByte();
        const length = try self.reader.interface.takeInt(u32, .big);
        if (length < 4 or length - 4 > self.body_buffer.len) return error.PgwireFixtureFrameTooLarge;
        const body = self.body_buffer[0 .. length - 4];
        try self.reader.interface.readSliceAll(body);
        return .{ .tag = tag, .body = body };
    }
    pub fn errorCode(body: []const u8) ![]const u8 {
        return errorField(body, 'C');
    }
    pub fn errorMessage(body: []const u8) ![]const u8 {
        return errorField(body, 'M');
    }
    pub fn expectQueryError(self: *Client, query: [:0]const u8, sqlstate: []const u8) !void {
        try self.send('Q', query[0 .. query.len + 1]);
        return self.expectError(sqlstate);
    }
    pub fn expectError(self: *Client, sqlstate: []const u8) !void {
        var errors: usize = 0;
        for (0..128) |_| {
            const frame = self.next() catch |err| {
                std.log.err("compiled pgwire refusal ended while expecting SQLSTATE={s}: {s}", .{ sqlstate, @errorName(err) });
                return err;
            };
            switch (frame.tag) {
                'E' => {
                    errors += 1;
                    try std.testing.expectEqualStrings(sqlstate, try errorCode(frame.body));
                },
                'T' => {}, // A schema description may precede execution refusal.
                'Z' => {
                    try std.testing.expectEqualStrings("I", frame.body);
                    try std.testing.expectEqual(@as(usize, 1), errors);
                    return;
                },
                else => return error.PgwireFixtureUnexpectedRefusalFrame,
            }
        }
        return error.PgwireFixtureFrameLimitExceeded;
    }
    pub fn cancelQuery(self: *Client, port: u16, secret: enum { valid, wrong }) !void {
        if (!self.backend_key_received or self.backend_pid == 0) return error.PgwireFixtureMissingBackendKey;
        const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(port) };
        const stream = try address.connect(self.io, .{ .mode = .stream, .timeout = .{ .deadline = self.deadline } });
        defer stream.close(self.io);
        var timed_out: std.atomic.Value(bool) = .init(false);
        var guard: std.Io.Group = .init;
        try guard.concurrent(self.io, expireCancel, .{ self.io, stream, self.deadline, &timed_out });
        var guard_live = true;
        defer if (guard_live) guard.cancel(self.io);
        var bytes: [16]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..4], 16, .big);
        std.mem.writeInt(u32, bytes[4..8], 80877102, .big);
        std.mem.writeInt(i32, bytes[8..12], self.backend_pid, .big);
        std.mem.writeInt(i32, bytes[12..16], if (secret == .valid) self.backend_secret else self.backend_secret ^ 1, .big);
        var write_buffer: [16]u8 = undefined;
        var writer = stream.writer(self.io, &write_buffer);
        try writer.interface.writeAll(&bytes);
        try writer.interface.flush();
        var read_buffer: [1]u8 = undefined;
        var reader = stream.reader(self.io, &read_buffer);
        try std.testing.expectError(error.EndOfStream, reader.interface.takeByte());
        guard.cancel(self.io);
        guard_live = false;
        if (timed_out.load(.acquire)) return error.PgwireFixtureCancelTimeout;
    }
    fn expireCancel(io: std.Io, stream: std.Io.net.Stream, deadline: std.Io.Clock.Timestamp, timed_out: *std.atomic.Value(bool)) void {
        deadline.wait(io) catch return;
        timed_out.store(true, .release);
        stream.shutdown(io, .both) catch {};
    }
    fn errorField(body: []const u8, field: u8) ![]const u8 {
        var offset: usize = 0;
        while (offset < body.len and body[offset] != 0) {
            const kind = body[offset];
            offset += 1;
            const end = std.mem.indexOfScalarPos(u8, body, offset, 0) orelse return error.PgwireFixtureMalformedError;
            if (kind == field) return body[offset..end];
            offset = end + 1;
        }
        return error.PgwireFixtureMissingSqlstate;
    }
    pub fn authenticate(self: *Client, username: []const u8, password: []const u8) !void {
        if (username.len == 0 or username.len > 128 or std.mem.indexOfScalar(u8, username, 0) != null) return error.PgwireFixtureBadUsername;
        var startup: [256]u8 = undefined;
        const body = try std.fmt.bufPrint(&startup, "user\x00{s}\x00database\x00default\x00\x00", .{username});
        try self.writer.interface.writeInt(u32, @intCast(body.len + 8), .big);
        try self.writer.interface.writeInt(u32, 196608, .big);
        try self.writer.interface.writeAll(body);
        try self.writer.interface.flush();
        var authenticated = false;
        var sent_password = false;
        for (0..128) |_| {
            const frame = try self.next();
            switch (frame.tag) {
                'R' => {
                    if (frame.body.len != 4) return error.PgwireFixtureBadAuthentication;
                    switch (std.mem.readInt(u32, frame.body[0..4], .big)) {
                        0 => authenticated = true,
                        3 => {
                            if (sent_password or password.len > 128) return error.PgwireFixtureBadAuthentication;
                            var payload: [129]u8 = undefined;
                            @memcpy(payload[0..password.len], password);
                            payload[password.len] = 0;
                            try self.send('p', payload[0 .. password.len + 1]);
                            sent_password = true;
                        },
                        else => return error.PgwireFixtureUnsupportedAuthentication,
                    }
                },
                'S' => {},
                'K' => {
                    if (frame.body.len != 8 or self.backend_key_received) return error.PgwireFixtureBadBackendKey;
                    self.backend_pid = std.mem.readInt(i32, frame.body[0..4], .big);
                    self.backend_secret = std.mem.readInt(i32, frame.body[4..8], .big);
                    self.backend_key_received = true;
                },
                'E' => {
                    if (!std.mem.eql(u8, try errorCode(frame.body), "28P01")) return error.PgwireFixtureUnexpectedAuthenticationError;
                    return error.PgwireAuthenticationRejected;
                },
                'Z' => {
                    if (!authenticated or frame.body.len != 1 or frame.body[0] != 'I') return error.PgwireFixtureNotReady;
                    return;
                },
                else => return error.PgwireFixtureUnexpectedAuthenticationFrame,
            }
        }
        return error.PgwireFixtureFrameLimit;
    }
    pub fn expectInteger(body: []const u8, digit: u8) !void {
        if (body.len != 7 or std.mem.readInt(u16, body[0..2], .big) != 1 or
            std.mem.readInt(i32, body[2..6], .big) != 1 or body[6] != digit)
            return error.PgwireFixtureUnexpectedPortalValue;
    }
    pub fn suspendPortal(self: *Client, digit: u8) !void {
        return self.suspendPortalQuery(digit, "SELECT $1 AS x UNION ALL SELECT $1 + 1");
    }
    pub fn suspendPortalQuery(self: *Client, digit: u8, query: []const u8) !void {
        return self.suspendPortalQueryExpected(digit, query, digit);
    }
    pub fn suspendPortalQueryExpected(self: *Client, digit: u8, query: []const u8, expected: u8) !void {
        return self.suspendPortalQueryExpectedWidth(digit, query, expected, 1);
    }
    pub fn suspendPortalQueryExpectedWidth(self: *Client, digit: u8, query: []const u8, expected: u8, width: usize) !void {
        if (width == 0 or width > 1024) return error.PgwireFixtureBadParameter;
        if (digit < '0' or digit > '8' or query.len > 1024 or std.mem.indexOfScalar(u8, query, 0) != null) return error.PgwireFixtureBadParameter;
        var parse_bytes: [1041]u8 = undefined;
        var parse = std.Io.Writer.fixed(&parse_bytes);
        try parse.writeAll("session-p\x00");
        try parse.writeAll(query);
        try parse.writeByte(0);
        try parse.writeInt(u16, 1, .big);
        try parse.writeInt(u32, 20, .big);
        try self.send('P', parse.buffered());
        return self.bindAndSuspend(digit, true, expected, width);
    }
    pub fn suspendPreparedPortal(self: *Client, digit: u8) !void {
        if (digit < '0' or digit > '8') return error.PgwireFixtureBadParameter;
        return self.bindAndSuspend(digit, false, digit, 1);
    }
    fn bindAndSuspend(self: *Client, digit: u8, expect_parse: bool, expected: u8, width: usize) !void {
        var bytes: [64]u8 = undefined;
        var bind = std.Io.Writer.fixed(&bytes);
        try bind.writeAll("session-held\x00session-p\x00");
        try bind.writeInt(u16, 0, .big);
        try bind.writeInt(u16, 1, .big);
        try bind.writeInt(i32, 1, .big);
        try bind.writeByte(digit);
        try bind.writeInt(u16, 0, .big);
        try self.send('B', bind.buffered());
        try self.send('D', "Psession-held\x00");
        try self.send('E', "session-held\x00\x00\x00\x00\x01");
        // Flush without Sync: the implicit transaction retains this portal.
        try self.send('H', "");
        var parsed = !expect_parse;
        var bound = false;
        var described = false;
        var rows: usize = 0;
        for (0..32) |_| {
            const frame = try self.next();
            switch (frame.tag) {
                '1' => {
                    if (parsed or frame.body.len != 0) return error.PgwireFixtureBadParseComplete;
                    parsed = true;
                },
                '2' => {
                    if (bound or frame.body.len != 0) return error.PgwireFixtureBadBindComplete;
                    bound = true;
                },
                'T' => {
                    if (described or frame.body.len < 2 or std.mem.readInt(u16, frame.body[0..2], .big) != 1) return error.PgwireFixtureBadPortalDescription;
                    described = true;
                },
                'D' => {
                    if (width == 1) {
                        try expectInteger(frame.body, expected);
                    } else {
                        if (frame.body.len != width + 6 or std.mem.readInt(u16, frame.body[0..2], .big) != 1 or
                            std.mem.readInt(i32, frame.body[2..6], .big) != width or frame.body[frame.body.len - 1] != expected)
                            return error.PgwireFixtureUnexpectedPortalValue;
                        for (frame.body[6 .. frame.body.len - 1]) |byte| if (byte != '0') return error.PgwireFixtureUnexpectedPortalValue;
                    }
                    rows += 1;
                },
                's' => {
                    if (!parsed or !bound or !described or rows != 1 or frame.body.len != 0) return error.PgwireFixturePortalNotSuspended;
                    return;
                },
                'E' => {
                    std.log.err("compiled pgwire portal failed SQLSTATE={s} message={s}", .{ try errorCode(frame.body), try errorMessage(frame.body) });
                    return error.PgwireFixturePortalFailed;
                },
                else => return error.PgwireFixtureUnexpectedPortalFrame,
            }
        }
        return error.PgwireFixtureFrameLimit;
    }
    pub fn closePortal(self: *Client) !void {
        try self.send('C', "Psession-held\x00");
        try self.send('H', "");
        const frame = try self.next();
        if (frame.tag == 'E') {
            std.log.err("compiled pgwire close failed SQLSTATE={s}", .{try errorCode(frame.body)});
            return error.PgwireFixturePortalFailed;
        }
        if (frame.tag != '3' or frame.body.len != 0) return error.PgwireFixtureBadCloseComplete;
        try self.send('S', "");
        const ready = try self.next();
        if (ready.tag != 'Z' or ready.body.len != 1 or ready.body[0] != 'I') return error.PgwireFixtureNotReady;
    }
    pub fn finishPortal(self: *Client, digit: u8) !void {
        try self.send('E', "session-held\x00\x00\x00\x00\x00");
        try self.send('S', "");
        var rows: usize = 0;
        var completed = false;
        for (0..32) |_| {
            const frame = try self.next();
            switch (frame.tag) {
                'D' => {
                    try expectInteger(frame.body, digit);
                    rows += 1;
                },
                'C' => {
                    if (completed or !std.mem.eql(u8, frame.body, "SELECT 2\x00")) return error.PgwireFixtureUnexpectedPortalCompletion;
                    completed = true;
                },
                'Z' => {
                    if (rows != 1 or !completed or frame.body.len != 1 or frame.body[0] != 'I') return error.PgwireFixtureNotReady;
                    return;
                },
                'E' => {
                    std.log.err("compiled pgwire continuation failed SQLSTATE={s}", .{try errorCode(frame.body)});
                    return error.PgwireFixturePortalFailed;
                },
                else => return error.PgwireFixtureUnexpectedPortalFrame,
            }
        }
        return error.PgwireFixtureFrameLimit;
    }

    /// Drain a suspended one-column stream, checking every continuation row.
    pub fn finishIntegerPortal(self: *Client, expected: []const i64) !void {
        try self.send('E', "session-held\x00\x00\x00\x00\x00");
        try self.send('S', "");
        var rows: usize = 0;
        var completed = false;
        var completion_bytes: [64]u8 = undefined;
        const completion = try std.fmt.bufPrint(&completion_bytes, "SELECT {d}\x00", .{expected.len + 1});
        for (0..1024) |_| {
            const frame = try self.next();
            switch (frame.tag) {
                'D' => {
                    if (completed or rows >= expected.len or frame.body.len < 7 or
                        std.mem.readInt(u16, frame.body[0..2], .big) != 1 or
                        std.mem.readInt(i32, frame.body[2..6], .big) != frame.body.len - 6)
                        return error.PgwireFixtureUnexpectedPortalValue;
                    const value = try std.fmt.parseInt(i64, frame.body[6..], 10);
                    try std.testing.expectEqual(expected[rows], value);
                    rows += 1;
                },
                'C' => {
                    if (completed or !std.mem.eql(u8, frame.body, completion)) return error.PgwireFixtureUnexpectedPortalCompletion;
                    completed = true;
                },
                'Z' => {
                    if (rows != expected.len or !completed or !std.mem.eql(u8, frame.body, "I")) return error.PgwireFixtureNotReady;
                    return;
                },
                'E' => {
                    std.log.err("compiled pgwire stream failed SQLSTATE={s}", .{try errorCode(frame.body)});
                    return error.PgwireFixturePortalFailed;
                },
                else => return error.PgwireFixtureUnexpectedPortalFrame,
            }
        }
        return error.PgwireFixtureFrameLimit;
    }

    pub fn command(self: *Client, query: [:0]const u8, tag: []const u8) !void {
        try self.send('Q', query[0 .. query.len + 1]);
        const complete = try self.next();
        if (complete.tag != 'C' or !std.mem.eql(u8, complete.body, tag)) return error.PgwireFixtureUnexpectedCommand;
        const ready = try self.next();
        if (ready.tag != 'Z' or !std.mem.eql(u8, ready.body, "I")) return error.PgwireFixtureNotReady;
    }

    pub fn scalar(self: *Client) !void {
        try self.send('Q', "SELECT 1\x00");
        try self.expectScalar();
    }
    pub fn expectScalar(self: *Client) !void {
        var rows: usize = 0;
        var completed = false;
        for (0..32) |_| {
            const frame = try self.next();
            switch (frame.tag) {
                'T' => {},
                'D' => {
                    if (frame.body.len != 7 or std.mem.readInt(u16, frame.body[0..2], .big) != 1 or
                        std.mem.readInt(i32, frame.body[2..6], .big) != 1 or frame.body[6] != '1') return error.PgwireFixtureUnexpectedScalar;
                    rows += 1;
                },
                'C' => completed = true,
                'E' => {
                    // Print only SQLSTATE; never request text or credentials.
                    std.log.err("compiled pgwire scalar failed SQLSTATE={s}", .{try errorCode(frame.body)});
                    return error.PgwireFixtureScalarFailed;
                },
                'Z' => {
                    if (!completed or rows != 1 or frame.body.len != 1 or frame.body[0] != 'I') return error.PgwireFixtureNotReady;
                    return;
                },
                else => return error.PgwireFixtureUnexpectedQueryFrame,
            }
        }
        return error.PgwireFixtureFrameLimit;
    }
};
