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

//! Versioned duplex document persistence transport. Storage authority remains
//! with the initiating job owner; no callback URL or storage credential crosses
//! the connection. Model execution is pinned to this single HTTP exchange.
const std = @import("std");
const abi = @import("antfly_runtime_abi").document_abi;
const errors = @import("antfly_runtime_abi").error_abi;
const httpx = @import("httpx");
const A = std.mem.Allocator;
pub const content_type = "application/octet-stream";
pub const header_bytes = 32;
pub const max_metadata = 1024 * 1024;
pub const max_body = 64 * 1024 * 1024;
pub const Kind = enum(u8) { start = 1, rpc = 2, reply = 3, result = 4 };
pub const Header = struct {
    kind: Kind,
    id: u64 = 0,
    code: i32 = 0,
    detail: i32 = 0,
    metadata_len: u32 = 0,
    body_len: u32 = 0,
    pub fn encode(self: Header) ![header_bytes]u8 {
        if (self.metadata_len > max_metadata or self.body_len > max_body) return error.InvalidDocumentCheckpoint;
        var result: [header_bytes]u8 = @splat(0);
        @memcpy(result[0..4], "AFDX");
        result[4] = 1;
        result[5] = @backingInt(self.kind);
        std.mem.writeInt(u64, result[8..16], self.id, .big);
        std.mem.writeInt(i32, result[16..20], self.code, .big);
        std.mem.writeInt(i32, result[20..24], self.detail, .big);
        std.mem.writeInt(u32, result[24..28], self.metadata_len, .big);
        std.mem.writeInt(u32, result[28..32], self.body_len, .big);
        return result;
    }
    pub fn decode(bytes: *const [header_bytes]u8) !Header {
        if (!std.mem.eql(u8, bytes[0..4], "AFDX") or bytes[4] != 1 or bytes[6] != 0 or bytes[7] != 0) return error.InvalidDocumentCheckpoint;
        const result = Header{ .kind = std.enums.fromInt(Kind, bytes[5]) orelse return error.InvalidDocumentCheckpoint, .id = std.mem.readInt(u64, bytes[8..16], .big), .code = std.mem.readInt(i32, bytes[16..20], .big), .detail = std.mem.readInt(i32, bytes[20..24], .big), .metadata_len = std.mem.readInt(u32, bytes[24..28], .big), .body_len = std.mem.readInt(u32, bytes[28..32], .big) };
        _ = try result.encode();
        return result;
    }
};
pub const Frame = struct {
    header: Header,
    bytes: []u8,
    pub fn metadata(self: Frame) []const u8 {
        return self.bytes[0..self.header.metadata_len];
    }
    pub fn body(self: Frame) []const u8 {
        return self.bytes[self.header.metadata_len..];
    }
    pub fn deinit(self: Frame, a: A) void {
        a.free(self.bytes);
    }
};
pub fn exact(reader: anytype, dest: []u8) !void {
    var offset: usize = 0;
    while (offset < dest.len) {
        const count = try reader.read(dest[offset..]);
        if (count == 0) return error.EndOfStream;
        offset += count;
    }
}
pub fn readFrame(a: A, reader: anytype) !Frame {
    var raw: [header_bytes]u8 = undefined;
    try exact(reader, &raw);
    const header = try Header.decode(&raw);
    const bytes = try a.alloc(u8, @as(usize, header.metadata_len) + header.body_len);
    errdefer a.free(bytes);
    try exact(reader, bytes);
    return .{ .header = header, .bytes = bytes };
}
pub fn encoded(a: A, header: Header, metadata: []const u8, body: []const u8) ![]u8 {
    var h = header;
    h.metadata_len = std.math.cast(u32, metadata.len) orelse return error.InvalidDocumentCheckpoint;
    h.body_len = std.math.cast(u32, body.len) orelse return error.InvalidDocumentCheckpoint;
    const prefix = try h.encode();
    const bytes = try a.alloc(u8, header_bytes + metadata.len + body.len);
    @memcpy(bytes[0..header_bytes], &prefix);
    @memcpy(bytes[header_bytes..][0..metadata.len], metadata);
    @memcpy(bytes[header_bytes + metadata.len ..], body);
    return bytes;
}
pub const Parser = struct {
    a: A,
    header: [header_bytes]u8 = undefined,
    header_used: usize = 0,
    payload: ?[]u8 = null,
    used: usize = 0,
    pub fn deinit(self: *Parser) void {
        if (self.payload) |bytes| self.a.free(bytes);
    }
    /// Dispatch takes ownership of a complete frame, including on error.
    pub fn feed(self: *Parser, input: []const u8, context: anytype, comptime dispatch: anytype) !void {
        var bytes = input;
        while (bytes.len > 0) {
            if (self.header_used < header_bytes) {
                const count = @min(header_bytes - self.header_used, bytes.len);
                @memcpy(self.header[self.header_used..][0..count], bytes[0..count]);
                self.header_used += count;
                bytes = bytes[count..];
                if (self.header_used < header_bytes) continue;
                const header = try Header.decode(&self.header);
                self.payload = try self.a.alloc(u8, @as(usize, header.metadata_len) + header.body_len);
            }
            const payload = self.payload.?;
            const count = @min(payload.len - self.used, bytes.len);
            @memcpy(payload[self.used..][0..count], bytes[0..count]);
            self.used += count;
            bytes = bytes[count..];
            if (self.used == payload.len) {
                const frame = Frame{ .header = try Header.decode(&self.header), .bytes = payload };
                self.payload = null;
                self.used = 0;
                self.header_used = 0;
                try dispatch(context, frame);
            }
        }
    }
    pub fn complete(self: *const Parser) bool {
        return self.header_used == 0 and self.payload == null;
    }
};

/// Bounded upload queue. Response processing and upload use independent I/O
/// tasks. Cancellation closes the queue and wakes both blocked directions.
pub const Queue = struct {
    a: A,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    changed: std.Io.Event = .unset,
    items: [4]?[]u8 = @splat(null),
    first: usize = 0,
    count: usize = 0,
    offset: usize = 0,
    closed: bool = false,
    retained: usize = 0,
    pub fn close(self: *Queue) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.closed = true;
        self.changed.set(self.io);
    }
    pub fn deinit(self: *Queue) void {
        for (self.items) |item| if (item) |bytes| self.a.free(bytes);
    }
    pub fn push(self: *Queue, bytes: []u8) !void {
        errdefer self.a.free(bytes);
        while (true) {
            try self.mutex.lock(self.io);
            if (self.closed) {
                self.mutex.unlock(self.io);
                return error.DocumentRunLeaseLost;
            }
            if (self.count < self.items.len and bytes.len <= 128 * 1024 * 1024 - self.retained) {
                self.items[(self.first + self.count) % self.items.len] = bytes;
                self.count += 1;
                self.retained += bytes.len;
                self.changed.set(self.io);
                self.mutex.unlock(self.io);
                return;
            }
            self.changed.reset();
            self.mutex.unlock(self.io);
            try self.changed.wait(self.io);
        }
    }
    pub fn read(raw: *anyopaque, dest: []u8) !usize {
        const self: *Queue = @ptrCast(@alignCast(raw));
        if (dest.len == 0) return 0;
        while (true) {
            try self.mutex.lock(self.io);
            if (self.count != 0) {
                const item = self.items[self.first].?;
                const count = @min(dest.len, item.len - self.offset);
                @memcpy(dest[0..count], item[self.offset..][0..count]);
                self.offset += count;
                if (self.offset == item.len) {
                    self.retained -= item.len;
                    self.a.free(item);
                    self.items[self.first] = null;
                    self.first = (self.first + 1) % self.items.len;
                    self.count -= 1;
                    self.offset = 0;
                    self.changed.set(self.io);
                }
                self.mutex.unlock(self.io);
                return count;
            }
            if (self.closed) {
                self.mutex.unlock(self.io);
                return 0;
            }
            self.changed.reset();
            self.mutex.unlock(self.io);
            try self.changed.wait(self.io);
        }
    }
};

/// Initiator's HTTP response consumer. The retained view is never serialized.
pub const Owner = struct {
    a: A,
    io: std.Io,
    view: abi.View,
    upload: Queue,
    parser: Parser,
    final: ?Frame = null,
    last_id: u64 = 0,
    status: u16 = 0,
    pub fn init(a: A, io: std.Io, view: abi.View, request: []const u8) !Owner {
        try view.retain();
        errdefer view.release();
        var result = Owner{ .a = a, .io = io, .view = view, .upload = .{ .a = a, .io = io }, .parser = .{ .a = a } };
        errdefer result.upload.deinit();
        try result.upload.push(try encoded(a, .{ .kind = .start }, "", request));
        return result;
    }
    pub fn deinit(self: *Owner) void {
        self.upload.close();
        self.upload.deinit();
        self.parser.deinit();
        if (self.final) |frame| frame.deinit(self.a);
        self.view.release();
    }
    pub fn startResponse(self: *Owner, response: httpx.Response) !void {
        self.status = response.status.code;
        if (self.status != 200) {
            if (self.status == 400 or self.status == 401 or self.status == 403 or self.status == 404 or self.status == 405 or self.status == 415) return error.DocumentStorageNotConfigured;
            return error.InferenceTransportUnavailable;
        }
        const mime = response.contentType() orelse return error.DocumentStorageNotConfigured;
        if (!std.mem.eql(u8, mime, content_type)) return error.DocumentStorageNotConfigured;
    }
    pub fn writeAll(self: *Owner, bytes: []const u8) !void {
        try self.parser.feed(bytes, self, dispatch);
    }
    fn dispatch(self: *Owner, frame: Frame) !void {
        var owned = true;
        defer if (owned) frame.deinit(self.a);
        if (self.final != null) return error.InvalidDocumentCheckpoint;
        switch (frame.header.kind) {
            .rpc => {
                if (frame.header.id <= self.last_id or frame.header.code != 0 or frame.header.detail != 0) return error.InvalidDocumentCheckpoint;
                self.last_id = frame.header.id;
                var response: abi.Response = .{};
                const status = self.view.invoke.?(self.view.context.?, .init(frame.metadata()), .init(frame.body()), &response);
                defer self.view.destroy(response);
                try self.upload.push(try encoded(self.a, .{ .kind = .reply, .id = frame.header.id, .code = status.code, .detail = status.detail }, response.metadata.slice(), response.body.slice()));
            },
            .result => {
                if (frame.header.id != 0 or frame.header.code < 200 or frame.header.code > 599 or frame.header.detail != 0 or frame.header.metadata_len > 4096) return error.InvalidDocumentCheckpoint;
                self.final = frame;
                owned = false;
                self.upload.close();
            },
            else => return error.InvalidDocumentCheckpoint,
        }
    }
};

/// Inference peer. Every outstanding call owns its reply until the C consumer
/// destroys it. The HTTP handler drains model work before destroying this peer.
pub const Remote = struct {
    a: A,
    io: std.Io,
    context: *anyopaque,
    send_fn: *const fn (*anyopaque, Header, []const u8, []const u8) anyerror!void,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.Io.Mutex = .init,
    sending: std.Io.Mutex = .init,
    pending: [64]?*Pending = @splat(null),
    next_id: u64 = 1,
    closed: std.atomic.Value(bool) = .init(false),
    const Pending = struct { id: u64, event: std.Io.Event = .unset, reply: ?Frame = null };
    pub fn view(self: *Remote) abi.View {
        return .{ .context = self, .retain_context = retain, .release_context = release, .invoke = invoke, .destroy_response = destroy };
    }
    fn retain(raw: *anyopaque) callconv(.c) u8 {
        const self: *Remote = @ptrCast(@alignCast(raw));
        _ = self.references.fetchAdd(1, .monotonic);
        return 1;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *Remote = @ptrCast(@alignCast(raw));
        _ = self.references.fetchSub(1, .acq_rel);
    }
    pub fn close(self: *Remote) void {
        self.closed.store(true, .release);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.pending) |entry| if (entry) |item| item.event.set(self.io);
    }
    pub fn deinit(self: *Remote) void {
        std.debug.assert(self.references.load(.acquire) == 1);
        for (self.pending) |item| std.debug.assert(item == null);
    }
    pub fn send(self: *Remote, header: Header, metadata: []const u8, body: []const u8) !void {
        try self.sending.lock(self.io);
        defer self.sending.unlock(self.io);
        try self.send_fn(self.context, header, metadata, body);
    }
    pub fn receive(self: *Remote, frame: Frame) !void {
        var owned = true;
        defer if (owned) frame.deinit(self.a);
        if (frame.header.kind != .reply or frame.header.id == 0) return error.InvalidDocumentCheckpoint;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.pending) |entry| if (entry) |item| if (item.id == frame.header.id) {
            if (item.reply != null) return error.InvalidDocumentCheckpoint;
            item.reply = frame;
            owned = false;
            item.event.set(self.io);
            return;
        };
        return error.InvalidDocumentCheckpoint;
    }
    fn invoke(raw: *anyopaque, metadata: @import("antfly_runtime_abi").http_abi.Bytes, body: @import("antfly_runtime_abi").http_abi.Bytes, out: *abi.Response) callconv(.c) errors.Status {
        const self: *Remote = @ptrCast(@alignCast(raw));
        out.* = .{};
        return self.call(metadata.slice(), body.slice(), out) catch |err| errors.statusFromErrorWithFallback(err, error.DocumentRunLeaseLost);
    }
    fn call(self: *Remote, metadata: []const u8, body: []const u8, out: *abi.Response) !errors.Status {
        // Allocate the response owner before dispatch; no ownership allocation
        // can fail after the storage mutation acknowledges its result.
        const response = try self.a.create(Frame);
        var transferred = false;
        defer if (!transferred) self.a.destroy(response);
        var request: Pending = .{ .id = 0 };
        try self.sending.lock(self.io);
        self.mutex.lockUncancelable(self.io);
        if (self.closed.load(.acquire)) {
            self.mutex.unlock(self.io);
            self.sending.unlock(self.io);
            return error.DocumentRunLeaseLost;
        }
        var slot: ?usize = null;
        for (self.pending, 0..) |entry, i| if (entry == null) {
            slot = i;
            break;
        };
        const index = slot orelse {
            self.mutex.unlock(self.io);
            self.sending.unlock(self.io);
            return error.DocumentRunBusy;
        };
        request.id = self.next_id;
        self.next_id += 1;
        self.pending[index] = &request;
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            self.pending[index] = null;
            self.mutex.unlock(self.io);
            if (request.reply) |reply| reply.deinit(self.a);
        }
        const sent = self.send_fn(self.context, .{ .kind = .rpc, .id = request.id }, metadata, body);
        self.sending.unlock(self.io);
        try sent;
        try request.event.wait(self.io);
        self.mutex.lockUncancelable(self.io);
        const reply = request.reply;
        request.reply = null;
        self.mutex.unlock(self.io);
        response.* = reply orelse return error.DocumentRunLeaseLost;
        transferred = true;
        _ = retain(self);
        out.* = .{ .handle = response, .metadata = .init(response.metadata()), .body = .init(response.body()) };
        return .{ .code = response.header.code, .detail = response.header.detail };
    }
    fn destroy(raw: *anyopaque, handle: *anyopaque) callconv(.c) void {
        const self: *Remote = @ptrCast(@alignCast(raw));
        const response: *Frame = @ptrCast(@alignCast(handle));
        response.deinit(self.a);
        self.a.destroy(response);
        release(self);
    }
};

test "document duplex framing preserves raw payloads across arbitrary fragmentation and rejects oversized frames" {
    const a = std.testing.allocator;
    const Capture = struct {
        a: A,
        count: usize = 0,
        fn frame(self: *@This(), value: Frame) !void {
            defer value.deinit(self.a);
            try std.testing.expectEqualStrings("{\"operation\":\"put\"}", value.metadata());
            try std.testing.expectEqualSlices(u8, &.{ 0, 1, 255, 0 }, value.body());
            self.count += 1;
        }
    };
    const bytes = try encoded(a, .{ .kind = .rpc, .id = 1 }, "{\"operation\":\"put\"}", &.{ 0, 1, 255, 0 });
    defer a.free(bytes);
    for (1..bytes.len) |width| {
        var parser = Parser{ .a = a };
        defer parser.deinit();
        var capture = Capture{ .a = a };
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = @min(width, bytes.len - offset);
            try parser.feed(bytes[offset..][0..n], &capture, Capture.frame);
            offset += n;
        }
        try std.testing.expect(parser.complete());
        try std.testing.expectEqual(@as(usize, 1), capture.count);
    }
    var corrupt = try (Header{ .kind = .rpc }).encode();
    std.mem.writeInt(u32, corrupt[28..32], max_body + 1, .big);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, Header.decode(&corrupt));
    corrupt[4] = 2;
    try std.testing.expectError(error.InvalidDocumentCheckpoint, Header.decode(&corrupt));
}

test "document duplex remote persistence calls retain independent response ownership and drain on close" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Echo = struct {
        fn retain(_: *anyopaque) callconv(.c) u8 {
            return 1;
        }
        fn release(_: *anyopaque) callconv(.c) void {}
        fn invoke(_: *anyopaque, metadata: @import("antfly_runtime_abi").http_abi.Bytes, body: @import("antfly_runtime_abi").http_abi.Bytes, out: *abi.Response) callconv(.c) errors.Status {
            out.* = .{ .metadata = metadata, .body = body };
            return .ok;
        }
        fn destroy(_: *anyopaque, _: *anyopaque) callconv(.c) void {}
    };
    var dummy: u8 = 0;
    const view: abi.View = .{ .context = &dummy, .retain_context = Echo.retain, .release_context = Echo.release, .invoke = Echo.invoke, .destroy_response = Echo.destroy };
    var owner = try Owner.init(a, io, view, "request");
    defer owner.deinit();
    const Link = struct {
        owner: *Owner,
        remote: *Remote,
        failure: ?anyerror = null,
        fn send(raw: *anyopaque, header: Header, metadata: []const u8, body: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = try encoded(self.owner.a, header, metadata, body);
            defer self.owner.a.free(bytes);
            try self.owner.writeAll(bytes);
        }
        fn read(self: *@This(), dest: []u8) !usize {
            return Queue.read(&self.owner.upload, dest);
        }
        fn receive(self: *@This()) std.Io.Cancelable!void {
            defer self.remote.close();
            while (true) {
                const frame = readFrame(self.owner.a, self) catch |err| {
                    if (err != error.EndOfStream) self.failure = err;
                    return;
                };
                if (frame.header.kind == .start) {
                    frame.deinit(self.owner.a);
                    continue;
                }
                self.remote.receive(frame) catch |err| {
                    self.failure = err;
                    return;
                };
            }
        }
    };
    var remote = Remote{ .a = a, .io = io, .context = undefined, .send_fn = Link.send };
    defer remote.deinit();
    var link = Link{ .owner = &owner, .remote = &remote };
    remote.context = &link;
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    try group.concurrent(io, Link.receive, .{&link});
    var response: abi.Response = .{};
    const status = remote.view().invoke.?(&remote, .init("metadata"), .init(&.{ 0, 1, 255 }), &response);
    try std.testing.expect(status.isOk());
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 255 }, response.body.slice());
    try std.testing.expectEqualStrings("metadata", response.metadata.slice());
    try remote.send(.{ .kind = .result, .code = 200 }, "", "done");
    try group.await(io);
    try std.testing.expect(link.failure == null);
    // The reply is still owned after the transport has closed.
    try std.testing.expectEqualStrings("metadata", response.metadata.slice());
    remote.view().destroy(response);
    try std.testing.expectEqualStrings("done", owner.final.?.body());
    try std.testing.expect(owner.parser.complete());
}
