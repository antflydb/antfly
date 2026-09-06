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

//! Private, bounded, bidirectional RPC over inherited pipes. No listening
//! socket, native pointers, or Zig error integers are exposed by the transport.
//! Each process has its own request-ID parity. Resource callbacks may call
//! back into the parent while inference requests execute concurrently.
const std = @import("std");

pub const max_frame_bytes = 64 * 1024 * 1024;
const max_retained_bytes = 128 * 1024 * 1024;
const max_requests = 128;
const Kind = enum(u8) { request = 1, response, event, cancel, failure };
const header_len = 17;

pub const Control = struct {
    ptr: ?*anyopaque = null,
    check: ?*const fn (?*anyopaque) anyerror!void = null,
    event: ?*const fn (?*anyopaque, []const u8) anyerror!void = null,
};

const TestPair = struct {
    parent: Endpoint,
    child: Endpoint,
    files: [4]std.Io.File,
    cancelled: std.atomic.Value(bool) = .init(false),
    cancel_seen: std.Io.Event = .unset,

    fn init(self: *TestPair) !void {
        if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
        const up = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
        const down = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
        self.* = .{ .parent = undefined, .child = undefined, .files = undefined };
        for (up ++ down, &self.files) |fd, *file| file.* = .{ .handle = fd, .flags = .{ .nonblocking = false } };
        self.parent = .{
            .alloc = std.testing.allocator,
            .io = std.testing.io,
            .input = self.files[0],
            .output = self.files[3],
            .context = self,
            .handler = handle,
            .on_closed = closed,
            .next_id = 1,
        };
        self.child = .{
            .alloc = std.testing.allocator,
            .io = std.testing.io,
            .input = self.files[2],
            .output = self.files[1],
            .context = self,
            .handler = handle,
            .on_closed = closed,
            .next_id = 2,
        };
        try self.parent.start();
        try self.child.start();
    }

    fn deinit(self: *TestPair) void {
        self.parent.deinit();
        self.child.deinit();
        for (self.files) |file| file.close(std.testing.io);
    }

    fn closed(_: *anyopaque) void {}
    fn handle(raw: *anyopaque, request: *Request) ![]u8 {
        const self: *TestPair = @ptrCast(@alignCast(raw));
        if (std.mem.eql(u8, request.payload, "nested"))
            return request.endpoint.call("echo", .{});
        if (std.mem.eql(u8, request.payload, "cancel")) {
            try request.event("started");
            while (!request.cancelled.load(.acquire)) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            self.cancel_seen.set(std.testing.io);
        }
        return std.testing.allocator.dupe(u8, request.payload);
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *TestPair = @ptrCast(@alignCast(raw.?));
        if (self.cancelled.load(.acquire)) return error.Cancelled;
    }
    fn event(raw: ?*anyopaque, _: []const u8) !void {
        const self: *TestPair = @ptrCast(@alignCast(raw.?));
        self.cancelled.store(true, .release);
    }
};

test "inference worker RPC permits nested bidirectional callbacks" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const result = try pair.parent.call("nested", .{});
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("echo", result);
}

test "inference worker RPC cancellation reaches the active child request" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    try std.testing.expectError(error.Cancelled, pair.parent.call("cancel", .{
        .ptr = &pair,
        .check = TestPair.check,
        .event = TestPair.event,
    }));
    try pair.cancel_seen.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
}

test "inference worker RPC rejects malformed cancellation frames" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    try pair.parent.send(.cancel, 1, "invalid payload");
    var elapsed: usize = 0;
    while (!pair.child.closed.load(.acquire) and elapsed < 5000) : (elapsed += 1)
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(pair.child.closed.load(.acquire));
}

pub const Request = struct {
    endpoint: *Endpoint,
    id: u64,
    payload: []u8,
    cancelled: std.atomic.Value(bool) = .init(false),

    pub fn event(self: *Request, payload: []const u8) !void {
        if (self.cancelled.load(.acquire)) return error.Cancelled;
        try self.endpoint.send(.event, self.id, payload);
    }
};

const Message = struct { kind: Kind, payload: []u8 };
const Pending = struct {
    ready: std.Io.Event = .unset,
    messages: std.ArrayListUnmanaged(Message) = .empty,
};

pub const Endpoint = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    input: std.Io.File,
    output: std.Io.File,
    context: *anyopaque,
    handler: *const fn (*anyopaque, *Request) anyerror![]u8,
    on_closed: *const fn (*anyopaque) void,
    next_id: u64,
    closed: std.atomic.Value(bool) = .init(false),
    mutex: std.Io.Mutex = .init,
    writer_mutex: std.Io.Mutex = .init,
    pending: std.AutoHashMapUnmanaged(u64, *Pending) = .empty,
    active: std.AutoHashMapUnmanaged(u64, *Request) = .empty,
    retained_bytes: usize = 0,
    reader_group: std.Io.Group = .init,
    handlers: std.Io.Group = .init,

    pub fn start(self: *Endpoint) !void {
        try self.reader_group.concurrent(self.io, readLoop, .{self});
    }

    pub fn deinit(self: *Endpoint) void {
        self.reader_group.cancel(self.io);
        self.handlers.cancel(self.io);
        std.debug.assert(self.pending.count() == 0);
        std.debug.assert(self.active.count() == 0);
        self.pending.deinit(self.alloc);
        self.active.deinit(self.alloc);
    }

    pub fn call(self: *Endpoint, payload: []const u8, control: Control) ![]u8 {
        if (control.check) |check| try check(control.ptr);
        var pending = Pending{};
        self.mutex.lockUncancelable(self.io);
        if (self.closed.load(.acquire) or self.pending.count() >= max_requests) {
            self.mutex.unlock(self.io);
            return error.InferenceWorkerUnavailable;
        }
        const id = self.next_id;
        self.next_id = std.math.add(u64, id, 2) catch {
            self.mutex.unlock(self.io);
            return error.InferenceWorkerUnavailable;
        };
        self.pending.put(self.alloc, id, &pending) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.pending.remove(id);
            for (pending.messages.items) |message| {
                self.retained_bytes -= message.payload.len;
                self.alloc.free(message.payload);
            }
            pending.messages.deinit(self.alloc);
            self.mutex.unlock(self.io);
        }
        try self.send(.request, id, payload);
        errdefer self.send(.cancel, id, "") catch {};
        while (true) {
            if (control.check) |check| try check(control.ptr);
            self.mutex.lockUncancelable(self.io);
            const message: ?Message = if (pending.messages.items.len > 0) pending.messages.orderedRemove(0) else null;
            if (message) |item| self.retained_bytes -= item.payload.len;
            const closed = self.closed.load(.acquire);
            if (message == null) pending.ready.reset();
            self.mutex.unlock(self.io);
            if (message) |item| {
                switch (item.kind) {
                    .response => return item.payload,
                    .event => {
                        defer self.alloc.free(item.payload);
                        if (control.event) |event| try event(control.ptr, item.payload);
                    },
                    else => {
                        self.alloc.free(item.payload);
                        return error.InferenceWorkerRequestFailed;
                    },
                }
            } else {
                if (closed) return error.InferenceWorkerUnavailable;
                pending.ready.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
            }
        }
    }

    fn send(self: *Endpoint, kind: Kind, id: u64, payload: []const u8) !void {
        if (payload.len > max_frame_bytes) return error.InferenceWorkerFrameTooLarge;
        try self.writer_mutex.lock(self.io);
        defer self.writer_mutex.unlock(self.io);
        if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        var header: [header_len]u8 = undefined;
        @memcpy(header[0..4], "AFW1");
        header[4] = @intFromEnum(kind);
        std.mem.writeInt(u64, header[5..13], id, .little);
        std.mem.writeInt(u32, header[13..17], @intCast(payload.len), .little);
        try self.output.writeStreamingAll(self.io, &header);
        try self.output.writeStreamingAll(self.io, payload);
    }

    fn readExact(self: *Endpoint, bytes: []u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const count = try self.input.readStreaming(self.io, &.{bytes[offset..]});
            if (count == 0) return error.EndOfStream;
            offset += count;
        }
    }

    fn readLoop(self: *Endpoint) std.Io.Cancelable!void {
        self.readFrames() catch |err| {
            if (err != error.EndOfStream and err != error.Canceled)
                std.log.warn("inference worker transport closed err={s}", .{@errorName(err)});
        };
        self.mutex.lockUncancelable(self.io);
        self.closed.store(true, .release);
        var pending = self.pending.valueIterator();
        while (pending.next()) |entry| entry.*.ready.set(self.io);
        var active = self.active.valueIterator();
        while (active.next()) |entry| entry.*.cancelled.store(true, .release);
        self.mutex.unlock(self.io);
        self.on_closed(self.context);
    }

    fn readFrames(self: *Endpoint) !void {
        while (true) {
            var header: [header_len]u8 = undefined;
            try self.readExact(&header);
            if (!std.mem.eql(u8, header[0..4], "AFW1")) return error.InvalidWorkerFrame;
            const kind = std.enums.fromInt(Kind, header[4]) orelse return error.InvalidWorkerFrame;
            const id = std.mem.readInt(u64, header[5..13], .little);
            const length = std.mem.readInt(u32, header[13..17], .little);
            if (length > max_frame_bytes or id == 0 or (kind == .cancel and length != 0)) return error.InvalidWorkerFrame;
            self.mutex.lockUncancelable(self.io);
            if (self.retained_bytes + length > max_retained_bytes) {
                self.mutex.unlock(self.io);
                return error.InferenceWorkerFrameTooLarge;
            }
            self.retained_bytes += length;
            self.mutex.unlock(self.io);
            var owned = true;
            const payload = try self.alloc.alloc(u8, length);
            defer if (owned) {
                self.alloc.free(payload);
                self.mutex.lockUncancelable(self.io);
                self.retained_bytes -= length;
                self.mutex.unlock(self.io);
            };
            try self.readExact(payload);
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            switch (kind) {
                .request => {
                    if (id % 2 == self.next_id % 2 or self.active.contains(id) or self.active.count() >= max_requests)
                        return error.InvalidWorkerFrame;
                    const request = try self.alloc.create(Request);
                    errdefer self.alloc.destroy(request);
                    request.* = .{ .endpoint = self, .id = id, .payload = payload };
                    try self.active.put(self.alloc, id, request);
                    errdefer _ = self.active.remove(id);
                    try self.handlers.concurrent(self.io, handle, .{ self, request });
                    owned = false;
                },
                .cancel => if (self.active.get(id)) |request| request.cancelled.store(true, .release),
                .response, .event, .failure => if (self.pending.get(id)) |pending| {
                    try pending.messages.append(self.alloc, .{ .kind = kind, .payload = payload });
                    owned = false;
                    pending.ready.set(self.io);
                },
            }
        }
    }

    fn handle(self: *Endpoint, request: *Request) std.Io.Cancelable!void {
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.active.remove(request.id);
            self.retained_bytes -= request.payload.len;
            self.mutex.unlock(self.io);
            self.alloc.free(request.payload);
            self.alloc.destroy(request);
        }
        const response = self.handler(self.context, request) catch {
            self.send(.failure, request.id, "") catch {};
            return;
        };
        defer self.alloc.free(response);
        self.send(.response, request.id, response) catch {
            // A locally rejected oversized response must not leave its caller
            // waiting forever on a healthy transport.
            self.send(.failure, request.id, "") catch {};
        };
    }
};
