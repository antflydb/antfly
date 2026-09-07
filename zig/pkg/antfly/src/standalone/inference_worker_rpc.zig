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

//! Versioned, bidirectional RPC over inherited pipes. Offers reserve the whole
//! logical message before any payload allocation or transmission. Raw metadata
//! and body bytes travel in bounded, interleavable frames. Capacity rejection
//! affects one transfer; malformed framing closes the connection.
const std = @import("std");

pub const max_frame_bytes = 64 * 1024;
pub const max_metadata_bytes = 1024 * 1024;
pub const max_body_bytes = 64 * 1024 * 1024;
const max_retained_bytes = 256 * 1024 * 1024;
const control_budget_bytes = 4 * 1024 * 1024;
const max_requests = 128;
const header_len = 17;
const Kind = enum(u8) { offer = 1, accept, reject, chunk, finish, abort, cancel, failure, capacity, resource_request, resource_reply };
const MessageKind = enum(u8) { request = 1, response, event };
const Failure = enum { failure, capacity };

// Resource mutations use dedicated frames and slots, never payload offers.
// The caller registers fixed reply storage before the peer may mutate a budget.
pub const max_resource_bytes = 1024;
const max_resource_requests = 128;
pub const ResourceReply = struct { code: i32 = 0, detail: i32 = 0, value: u64 = 0 };
const ResourcePending = struct { id: u64, ready: std.Io.Event = .unset, reply: ?ResourceReply = null };
const ResourceTask = struct { id: u64, len: usize, bytes: [max_resource_bytes]u8 };

pub const Payload = struct {
    metadata: []const u8 = "",
    body: []const u8 = "",

    fn size(self: Payload) !usize {
        if (self.metadata.len > max_metadata_bytes or self.body.len > max_body_bytes)
            return error.BodyTooLarge;
        return self.metadata.len + self.body.len;
    }
};

/// Includes the receive/output reservation until its final consumer releases it.
pub const OwnedPayload = struct {
    endpoint: ?*Endpoint,
    alloc: std.mem.Allocator,
    bytes: []u8,
    metadata_len: usize,

    pub fn view(self: OwnedPayload) Payload {
        return .{ .metadata = self.bytes[0..self.metadata_len], .body = self.bytes[self.metadata_len..] };
    }
    pub fn deinit(self: OwnedPayload) void {
        if (self.endpoint) |endpoint| {
            endpoint.mutex.lockUncancelable(endpoint.io);
            defer endpoint.mutex.unlock(endpoint.io);
            self.deinitLocked();
        } else self.alloc.free(self.bytes);
    }
    // Transfer response ownership to the host before releasing its worker
    // lifetime lease. No payload copy or pointer into a replaceable endpoint.
    pub fn detach(self: OwnedPayload) OwnedPayload {
        const endpoint = self.endpoint.?;
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        endpoint.releaseBytesLocked(self.bytes.len);
        var result = self;
        result.endpoint = null;
        return result;
    }
    fn deinitLocked(self: OwnedPayload) void {
        self.endpoint.?.releaseBytesLocked(self.bytes.len);
        self.alloc.free(self.bytes);
    }
};

pub const Control = struct {
    ptr: ?*anyopaque = null,
    check: ?*const fn (?*anyopaque) anyerror!void = null,
    event: ?*const fn (?*anyopaque, Payload) anyerror!void = null,
};

pub const Request = struct {
    endpoint: *Endpoint,
    id: u64,
    payload: OwnedPayload,
    cancelled: std.atomic.Value(bool) = .init(false),

    fn check(raw: ?*anyopaque) !void {
        const self: *Request = @ptrCast(@alignCast(raw.?));
        if (self.cancelled.load(.acquire)) return error.Cancelled;
    }
    pub fn event(self: *Request, payload: Payload) !void {
        try self.endpoint.sendMessage(.event, self.id, payload, .{ .ptr = self, .check = check });
    }
};

const Message = struct { kind: MessageKind, payload: OwnedPayload };
const Pending = struct {
    ready: std.Io.Event = .unset,
    terminal: ?Failure = null,
    messages: std.ArrayListUnmanaged(Message) = .empty,
};
const Offer = struct {
    ready: std.Io.Event = .unset,
    accepted: bool = false,
};
const Assembly = struct { kind: MessageKind, request_id: u64, payload: OwnedPayload, received: usize = 0 };

pub const Endpoint = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    input: std.Io.File,
    output: std.Io.File,
    context: *anyopaque,
    handler: *const fn (*anyopaque, *Request) anyerror!OwnedPayload,
    on_closed: *const fn (*anyopaque) void,
    resource_handler: ?*const fn (*anyopaque, []const u8) anyerror!ResourceReply = null,
    next_id: u64,
    next_transfer: u64 = 1,
    next_resource_id: u64 = 1,
    resource_pending: [max_resource_requests]?*ResourcePending = @splat(null),
    resource_active: [max_resource_requests]?u64 = @splat(null),
    failed: std.Io.Event = .unset,
    lifecycle_group: std.Io.Group = .init,
    closed: std.atomic.Value(bool) = .init(false),
    mutex: std.Io.Mutex = .init,
    writer_mutex: std.Io.Mutex = .init,
    pending: std.AutoHashMapUnmanaged(u64, *Pending) = .empty,
    active: std.AutoHashMapUnmanaged(u64, *Request) = .empty,
    offers: std.AutoHashMapUnmanaged(u64, *Offer) = .empty,
    incoming: std.AutoHashMapUnmanaged(u64, Assembly) = .empty,
    retained_bytes: usize = 0,
    control_bytes: usize = 0,
    // Configurable downward in tests to exercise capacity without huge buffers.
    retained_limit: usize = max_retained_bytes,
    reader_group: std.Io.Group = .init,
    handlers: std.Io.Group = .init,
    control_writes: std.Io.Group = .init,

    pub fn start(self: *Endpoint) !void {
        try self.reader_group.concurrent(self.io, readLoop, .{self});
        errdefer self.reader_group.cancel(self.io);
        try self.lifecycle_group.concurrent(self.io, lifecycle, .{self});
    }
    // Safe from a handler or under the resource mutex: the lifecycle task owns
    // reader cancellation and on_closed (including process reaping).
    pub fn fail(self: *Endpoint) void {
        self.closed.store(true, .release);
        self.failed.set(self.io);
    }
    fn lifecycle(self: *Endpoint) std.Io.Cancelable!void {
        self.failed.wait(self.io) catch {};
        self.reader_group.cancel(self.io);
    }
    pub fn deinit(self: *Endpoint) void {
        self.failed.set(self.io);
        self.lifecycle_group.await(self.io) catch self.lifecycle_group.cancel(self.io);
        self.control_writes.cancel(self.io);
        self.handlers.cancel(self.io);
        std.debug.assert(self.pending.count() == 0 and self.active.count() == 0 and self.offers.count() == 0);
        std.debug.assert(self.retained_bytes == 0 and self.control_bytes == 0);
        for (self.resource_pending) |entry| std.debug.assert(entry == null);
        for (self.resource_active) |entry| std.debug.assert(entry == null);
        self.pending.deinit(self.alloc);
        self.active.deinit(self.alloc);
        self.offers.deinit(self.alloc);
        self.incoming.deinit(self.alloc);
    }

    fn reserveBytesLocked(self: *Endpoint, size: usize) !void {
        // Keep small ordinary messages separate from bulk payloads. Resource
        // callbacks use neither allowance.
        const counter = if (size <= max_frame_bytes) &self.control_bytes else &self.retained_bytes;
        const limit = if (size <= max_frame_bytes) control_budget_bytes else self.retained_limit;
        if (size > limit - counter.*) return error.ResourceTemporarilyUnavailable;
        counter.* += size;
    }
    fn releaseBytesLocked(self: *Endpoint, size: usize) void {
        const counter = if (size <= max_frame_bytes) &self.control_bytes else &self.retained_bytes;
        counter.* -= size;
    }
    fn allocateLocked(self: *Endpoint, metadata_len: usize, body_len: usize) !OwnedPayload {
        if (metadata_len > max_metadata_bytes or body_len > max_body_bytes) return error.BodyTooLarge;
        const size = metadata_len + body_len;
        try self.reserveBytesLocked(size);
        errdefer self.releaseBytesLocked(size);
        return .{ .endpoint = self, .alloc = self.alloc, .bytes = try self.alloc.alloc(u8, size), .metadata_len = metadata_len };
    }
    pub fn copyPayload(self: *Endpoint, value: Payload) !OwnedPayload {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const owned = try self.allocateLocked(value.metadata.len, value.body.len);
        @memcpy(owned.bytes[0..value.metadata.len], value.metadata);
        @memcpy(owned.bytes[value.metadata.len..], value.body);
        return owned;
    }

    pub fn call(self: *Endpoint, payload: Payload, control: Control) !OwnedPayload {
        _ = try payload.size();
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
            for (pending.messages.items) |message| message.payload.deinitLocked();
            pending.messages.deinit(self.alloc);
            self.mutex.unlock(self.io);
        }
        errdefer self.sendFrame(.cancel, id, "") catch {};
        try self.sendMessage(.request, id, payload, control);
        while (true) {
            if (control.check) |check| try check(control.ptr);
            self.mutex.lockUncancelable(self.io);
            const message: ?Message = if (pending.messages.items.len > 0) pending.messages.orderedRemove(0) else null;
            const closed = self.closed.load(.acquire);
            const terminal = pending.terminal;
            if (message == null) pending.ready.reset();
            self.mutex.unlock(self.io);
            if (message) |item| {
                switch (item.kind) {
                    .response => return item.payload,
                    .event => {
                        defer item.payload.deinit();
                        if (control.event) |event| try event(control.ptr, item.payload.view());
                    },
                    .request => unreachable,
                }
            } else {
                if (terminal) |failure| return if (failure == .capacity) error.ResourceTemporarilyUnavailable else error.InferenceWorkerRequestFailed;
                if (closed) return error.InferenceWorkerUnavailable;
                try self.waitTick(&pending.ready);
            }
        }
    }

    pub fn callResource(self: *Endpoint, payload: []const u8) !ResourceReply {
        // Every failure here is a transport failure, not a budget denial.
        errdefer self.fail();
        if (payload.len == 0 or payload.len > max_resource_bytes) return error.InvalidResourceFrame;
        var pending: ResourcePending = undefined;
        const slot = blk: {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
            for (&self.resource_pending, 0..) |*entry, i| {
                if (entry.* != null) continue;
                const id = self.next_resource_id;
                self.next_resource_id = try std.math.add(u64, id, 1);
                pending = .{ .id = id };
                entry.* = &pending;
                break :blk i;
            }
            return error.ResourceCallbackCapacity;
        };
        defer {
            self.mutex.lockUncancelable(self.io);
            self.resource_pending[slot] = null;
            self.mutex.unlock(self.io);
        }
        try self.sendFrame(.resource_request, pending.id, payload);
        try pending.ready.wait(self.io);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return pending.reply orelse error.InferenceWorkerUnavailable;
    }

    fn handleResource(self: *Endpoint, task: *ResourceTask, slot: usize) std.Io.Cancelable!void {
        defer {
            self.mutex.lockUncancelable(self.io);
            self.resource_active[slot] = null;
            self.mutex.unlock(self.io);
            self.alloc.destroy(task);
        }
        // Reply storage is fixed and already available before the mutation.
        var bytes: [16]u8 = undefined;
        const reply = self.resource_handler.?(self.context, task.bytes[0..task.len]) catch {
            self.fail();
            return;
        };
        std.mem.writeInt(i32, bytes[0..4], reply.code, .little);
        std.mem.writeInt(i32, bytes[4..8], reply.detail, .little);
        std.mem.writeInt(u64, bytes[8..16], reply.value, .little);
        self.sendFrame(.resource_reply, task.id, &bytes) catch self.fail();
    }

    fn waitTick(self: *Endpoint, ready: *std.Io.Event) !void {
        ready.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => {},
            else => return err,
        };
    }

    fn sendMessage(self: *Endpoint, kind: MessageKind, request_id: u64, payload: Payload, control: Control) !void {
        _ = try payload.size();
        var offer = Offer{};
        self.mutex.lockUncancelable(self.io);
        if (self.closed.load(.acquire) or self.offers.count() >= max_requests) {
            self.mutex.unlock(self.io);
            return error.ResourceTemporarilyUnavailable;
        }
        const transfer = self.next_transfer;
        self.next_transfer = std.math.add(u64, transfer, 1) catch {
            self.mutex.unlock(self.io);
            return error.InferenceWorkerUnavailable;
        };
        self.offers.put(self.alloc, transfer, &offer) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.offers.remove(transfer);
            self.mutex.unlock(self.io);
        }
        var description: [21]u8 = undefined;
        description[0] = @intFromEnum(kind);
        std.mem.writeInt(u64, description[1..9], request_id, .little);
        std.mem.writeInt(u32, description[9..13], @intCast(payload.metadata.len), .little);
        std.mem.writeInt(u64, description[13..21], payload.body.len, .little);
        errdefer self.sendFrame(.abort, transfer, "") catch {};
        try self.sendFrame(.offer, transfer, &description);
        while (!offer.ready.isSet()) {
            if (control.check) |check| try check(control.ptr);
            if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
            try self.waitTick(&offer.ready);
        }
        if (!offer.accepted) return error.ResourceTemporarilyUnavailable;
        for ([_][]const u8{ payload.metadata, payload.body }) |part| {
            var offset: usize = 0;
            while (offset < part.len) {
                if (control.check) |check| try check(control.ptr);
                const end = @min(part.len, offset + max_frame_bytes);
                try self.sendFrame(.chunk, transfer, part[offset..end]);
                offset = end;
            }
        }
        if (control.check) |check| try check(control.ptr);
        try self.sendFrame(.finish, transfer, "");
    }

    fn sendFrame(self: *Endpoint, kind: Kind, id: u64, payload: []const u8) !void {
        if (payload.len > max_frame_bytes) return error.InferenceWorkerFrameTooLarge;
        // Cancellation is checked between frames. Never truncate a frame and
        // leave the next request's header inside this frame's payload.
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        self.writer_mutex.lockUncancelable(self.io);
        defer self.writer_mutex.unlock(self.io);
        if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        var header: [header_len]u8 = undefined;
        @memcpy(header[0..4], "AFW3");
        header[4] = @intFromEnum(kind);
        std.mem.writeInt(u64, header[5..13], id, .little);
        std.mem.writeInt(u32, header[13..17], @intCast(payload.len), .little);
        try self.output.writeStreamingAll(self.io, &header);
        try self.output.writeStreamingAll(self.io, payload);
    }

    // A reader must never block behind a pipe writer: both peers can be sending
    // chunks concurrently. Send the small credit replies on separate tasks.
    fn controlWrite(self: *Endpoint, kind: Kind, id: u64) std.Io.Cancelable!void {
        self.sendFrame(kind, id, "") catch {};
    }
    fn replyOffer(self: *Endpoint, kind: Kind, id: u64) !void {
        try self.control_writes.concurrent(self.io, controlWrite, .{ self, kind, id });
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
        var offers = self.offers.valueIterator();
        while (offers.next()) |entry| entry.*.ready.set(self.io);
        for (self.resource_pending) |entry| if (entry) |resource| resource.ready.set(self.io);
        var active = self.active.valueIterator();
        while (active.next()) |entry| entry.*.cancelled.store(true, .release);
        var incoming = self.incoming.valueIterator();
        while (incoming.next()) |entry| entry.payload.deinitLocked();
        self.incoming.clearRetainingCapacity();
        self.mutex.unlock(self.io);
        self.on_closed(self.context);
    }
    fn readFrames(self: *Endpoint) !void {
        var buffer: [max_frame_bytes]u8 = undefined;
        while (true) {
            var header: [header_len]u8 = undefined;
            try self.readExact(&header);
            if (!std.mem.eql(u8, header[0..4], "AFW3")) return error.InvalidWorkerFrame;
            const kind = std.enums.fromInt(Kind, header[4]) orelse return error.InvalidWorkerFrame;
            const id = std.mem.readInt(u64, header[5..13], .little);
            const length = std.mem.readInt(u32, header[13..17], .little);
            if (length > max_frame_bytes or id == 0) return error.InvalidWorkerFrame;
            const bytes = buffer[0..length];
            try self.readExact(bytes);
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            switch (kind) {
                .resource_request => {
                    if (self.resource_handler == null or length == 0 or length > max_resource_bytes)
                        return error.InvalidResourceFrame;
                    for (self.resource_active) |entry| if (entry == id) return error.InvalidResourceFrame;
                    const slot = for (self.resource_active, 0..) |entry, i| {
                        if (entry == null) break i;
                    } else return error.ResourceCallbackCapacity;
                    const task = try self.alloc.create(ResourceTask);
                    errdefer self.alloc.destroy(task);
                    task.* = .{ .id = id, .len = length, .bytes = undefined };
                    @memcpy(task.bytes[0..length], bytes);
                    self.resource_active[slot] = id;
                    errdefer self.resource_active[slot] = null;
                    try self.handlers.concurrent(self.io, handleResource, .{ self, task, slot });
                },
                .resource_reply => {
                    if (length != 16) return error.InvalidResourceFrame;
                    const pending = for (self.resource_pending) |entry| {
                        if (entry) |resource| if (resource.id == id) break resource;
                    } else return error.InvalidResourceFrame;
                    if (pending.reply != null) return error.InvalidResourceFrame;
                    pending.reply = .{
                        .code = std.mem.readInt(i32, bytes[0..4], .little),
                        .detail = std.mem.readInt(i32, bytes[4..8], .little),
                        .value = std.mem.readInt(u64, bytes[8..16], .little),
                    };
                    pending.ready.set(self.io);
                },
                .offer => {
                    if (length != 21 or self.incoming.contains(id)) return error.InvalidWorkerFrame;
                    const message_kind = std.enums.fromInt(MessageKind, bytes[0]) orelse return error.InvalidWorkerFrame;
                    const request_id = std.mem.readInt(u64, bytes[1..9], .little);
                    const metadata_len = std.mem.readInt(u32, bytes[9..13], .little);
                    const body_len = std.mem.readInt(u64, bytes[13..21], .little);
                    if (request_id == 0 or metadata_len > max_metadata_bytes or body_len > max_body_bytes)
                        return error.InvalidWorkerFrame;
                    if (message_kind == .request and (request_id % 2 == self.next_id % 2 or self.active.contains(request_id)))
                        return error.InvalidWorkerFrame;
                    if (self.incoming.count() >= max_requests or
                        (message_kind == .request and self.active.count() >= max_requests) or
                        (message_kind != .request and (!self.pending.contains(request_id) or
                            self.pending.get(request_id).?.messages.items.len + self.incoming.count() >= 1024)))
                    {
                        try self.replyOffer(.reject, id);
                        continue;
                    }
                    const payload = self.allocateLocked(metadata_len, @intCast(body_len)) catch {
                        try self.replyOffer(.reject, id);
                        continue;
                    };
                    self.incoming.put(self.alloc, id, .{ .kind = message_kind, .request_id = request_id, .payload = payload }) catch |err| {
                        payload.deinitLocked();
                        return err;
                    };
                    try self.replyOffer(.accept, id);
                },
                .accept, .reject => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    if (self.offers.get(id)) |offer| {
                        offer.accepted = kind == .accept;
                        offer.ready.set(self.io);
                    }
                },
                .chunk => {
                    const assembly = self.incoming.getPtr(id) orelse return error.InvalidWorkerFrame;
                    if (length == 0 or length > assembly.payload.bytes.len - assembly.received) return error.InvalidWorkerFrame;
                    @memcpy(assembly.payload.bytes[assembly.received..][0..length], bytes);
                    assembly.received += length;
                },
                .finish => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    const entry = self.incoming.fetchRemove(id) orelse return error.InvalidWorkerFrame;
                    const assembly = entry.value;
                    var owned = true;
                    defer if (owned) assembly.payload.deinitLocked();
                    if (assembly.received != assembly.payload.bytes.len) return error.InvalidWorkerFrame;
                    if (assembly.kind == .request) {
                        const request = try self.alloc.create(Request);
                        errdefer self.alloc.destroy(request);
                        request.* = .{ .endpoint = self, .id = assembly.request_id, .payload = assembly.payload };
                        if (self.active.contains(request.id)) return error.InvalidWorkerFrame;
                        try self.active.put(self.alloc, request.id, request);
                        errdefer _ = self.active.remove(request.id);
                        try self.handlers.concurrent(self.io, handle, .{ self, request });
                        owned = false;
                    } else if (self.pending.get(assembly.request_id)) |pending| {
                        // Bound empty-event floods as well as payload bytes.
                        std.debug.assert(pending.messages.items.len < 1024);
                        try pending.messages.append(self.alloc, .{ .kind = assembly.kind, .payload = assembly.payload });
                        pending.ready.set(self.io);
                        owned = false;
                    }
                },
                .abort => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    if (self.incoming.fetchRemove(id)) |entry| entry.value.payload.deinitLocked();
                },
                .failure, .capacity => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    if (self.pending.get(id)) |pending| {
                        pending.terminal = if (kind == .capacity) .capacity else .failure;
                        pending.ready.set(self.io);
                    }
                },
                .cancel => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    if (self.active.get(id)) |request| request.cancelled.store(true, .release);
                },
            }
        }
    }
    fn handle(self: *Endpoint, request: *Request) std.Io.Cancelable!void {
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.active.remove(request.id);
            request.payload.deinitLocked();
            self.mutex.unlock(self.io);
            self.alloc.destroy(request);
        }
        const response = self.handler(self.context, request) catch |err| {
            // Terminal failure never needs message credit or an offer slot.
            // Even a completely full recipient can finish this one request.
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable) .capacity else .failure, request.id, "") catch {};
            return;
        };
        defer response.deinit();
        self.sendMessage(.response, request.id, response.view(), .{ .ptr = request, .check = Request.check }) catch |err| {
            // Terminal failure never needs message credit or an offer slot.
            // Even a completely full recipient can finish this one request.
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable) .capacity else .failure, request.id, "") catch {};
        };
    }
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
    fn handle(raw: *anyopaque, request: *Request) !OwnedPayload {
        const self: *TestPair = @ptrCast(@alignCast(raw));
        if (std.mem.eql(u8, request.payload.view().body, "nested"))
            return request.endpoint.call(.{ .body = "echo" }, .{});
        if (std.mem.eql(u8, request.payload.view().body, "cancel")) {
            try request.event(.{ .body = "started" });
            while (!request.cancelled.load(.acquire)) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            self.cancel_seen.set(std.testing.io);
        }
        return request.endpoint.copyPayload(request.payload.view());
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *TestPair = @ptrCast(@alignCast(raw.?));
        if (self.cancelled.load(.acquire)) return error.Cancelled;
    }
    fn event(raw: ?*anyopaque, _: Payload) !void {
        const self: *TestPair = @ptrCast(@alignCast(raw.?));
        self.cancelled.store(true, .release);
    }
};

test "inference worker RPC permits nested bidirectional callbacks" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const result = try pair.parent.call(.{ .body = "nested" }, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("echo", result.view().body);
}

test "inference worker RPC cancellation reaches the active child request" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    try std.testing.expectError(error.Cancelled, pair.parent.call(.{ .body = "cancel" }, .{
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
    try pair.parent.sendFrame(.cancel, 1, "invalid payload");
    var elapsed: usize = 0;
    while (!pair.child.closed.load(.acquire) and elapsed < 5000) : (elapsed += 1)
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(pair.child.closed.load(.acquire));
}

test "inference worker raw payloads round trip at 48 MiB and the public limit" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const body = try std.testing.allocator.alloc(u8, max_body_bytes + 1);
    defer std.testing.allocator.free(body);
    for (body, 0..) |*byte, i| byte.* = @truncate(i);
    for ([_]usize{ 48 * 1024 * 1024, max_body_bytes }) |size| {
        const response = try pair.parent.call(.{ .metadata = "\x00\xffmetadata", .body = body[0..size] }, .{});
        defer response.deinit();
        try std.testing.expectEqualSlices(u8, body[0..size], response.view().body);
        try std.testing.expectEqualStrings("\x00\xffmetadata", response.view().metadata);
    }
    try std.testing.expectError(error.BodyTooLarge, pair.parent.call(.{ .body = body }, .{}));
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker bulk capacity rejects one request and leaves nested resource callbacks available" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const body = try std.testing.allocator.alloc(u8, 2 * max_frame_bytes);
    defer std.testing.allocator.free(body);
    @memset(body, 0x80);
    // Occupy the complete bulk allowance without occupying control capacity.
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.retained_limit = body.len;
    const occupied = try pair.child.allocateLocked(0, body.len);
    pair.child.mutex.unlock(pair.child.io);
    defer occupied.deinit();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = body }, .{}));
    const nested = try pair.parent.call(.{ .body = "nested" }, .{});
    defer nested.deinit();
    try std.testing.expectEqualStrings("echo", nested.view().body);
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker bulk response capacity failure leaves transport healthy" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const body = try std.testing.allocator.alloc(u8, 2 * max_frame_bytes);
    defer std.testing.allocator.free(body);
    @memset(body, 0x81);
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.retained_limit = body.len; // receive fits; allocating its echo does not
    pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = body }, .{}));
    const response = try pair.parent.call(.{ .body = "nested" }, .{});
    defer response.deinit();
    try std.testing.expectEqualStrings("echo", response.view().body);
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker mid-transfer cancellation releases reservation and permits nested calls between chunks" {
    const Probe = struct {
        pair: *TestPair,
        nested_completed: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.pair.child.mutex.lockUncancelable(self.pair.child.io);
            var transfers = self.pair.child.incoming.valueIterator();
            const partial = if (transfers.next()) |entry| entry.received >= max_frame_bytes else false;
            self.pair.child.mutex.unlock(self.pair.child.io);
            if (partial) {
                const response = try self.pair.parent.call(.{ .body = "nested" }, .{});
                defer response.deinit();
                try std.testing.expectEqualStrings("echo", response.view().body);
                self.nested_completed = true;
                return error.Cancelled;
            }
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const body = try std.testing.allocator.alloc(u8, 8 * max_frame_bytes);
    defer std.testing.allocator.free(body);
    @memset(body, 0x82);
    var probe: Probe = .{ .pair = &pair };
    try std.testing.expectError(error.Cancelled, pair.parent.call(.{ .body = body }, .{ .ptr = &probe, .check = Probe.check }));
    try std.testing.expect(probe.nested_completed);
    // The subsequent request is ordered after abort on the same pipe.
    const response = try pair.parent.call(.{ .body = "echo" }, .{});
    defer response.deinit();
    pair.child.mutex.lockUncancelable(pair.child.io);
    defer pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectEqual(@as(usize, 0), pair.child.incoming.count());
    try std.testing.expectEqual(@as(usize, 0), pair.child.retained_bytes);
}

test "inference worker concurrent bulk transfers preserve message boundaries" {
    const Probe = struct {
        endpoint: *Endpoint,
        body: []const u8,
        fn run(self: @This()) !void {
            const response = try self.endpoint.call(.{ .body = self.body }, .{});
            defer response.deinit();
            try std.testing.expectEqualSlices(u8, self.body, response.view().body);
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const body_a = try std.testing.allocator.alloc(u8, 16 * max_frame_bytes);
    defer std.testing.allocator.free(body_a);
    const body_b = try std.testing.allocator.alloc(u8, 16 * max_frame_bytes);
    defer std.testing.allocator.free(body_b);
    @memset(body_a, 0xa3);
    @memset(body_b, 0xb4);
    var a = try std.testing.io.concurrent(Probe.run, .{Probe{ .endpoint = &pair.parent, .body = body_a }});
    defer _ = a.cancel(std.testing.io) catch {};
    var b = try std.testing.io.concurrent(Probe.run, .{Probe{ .endpoint = &pair.parent, .body = body_b }});
    defer _ = b.cancel(std.testing.io) catch {};
    try a.await(std.testing.io);
    try b.await(std.testing.io);
}

test "inference worker truncated logical messages close transport and release receive credits" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var offer: [21]u8 = @splat(0);
    offer[0] = @intFromEnum(MessageKind.request);
    std.mem.writeInt(u64, offer[1..9], 1, .little);
    std.mem.writeInt(u64, offer[13..21], 2 * max_frame_bytes, .little);
    try pair.parent.sendFrame(.offer, 99, &offer);
    try pair.parent.sendFrame(.chunk, 99, "partial");
    try pair.parent.sendFrame(.finish, 99, "");
    var elapsed: usize = 0;
    while (!pair.child.closed.load(.acquire) and elapsed < 5000) : (elapsed += 1)
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(pair.child.closed.load(.acquire));
    pair.child.mutex.lockUncancelable(pair.child.io);
    defer pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectEqual(@as(usize, 0), pair.child.retained_bytes);
}

test "inference worker terminal capacity errors require no receive credits" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var held: [control_budget_bytes / max_frame_bytes]OwnedPayload = undefined;
    var held_count: usize = 0;
    defer for (held[0..held_count]) |payload| payload.deinit();
    {
        pair.parent.mutex.lockUncancelable(pair.parent.io);
        defer pair.parent.mutex.unlock(pair.parent.io);
        pair.parent.retained_limit = 0;
        while (held_count < held.len) : (held_count += 1)
            held[held_count] = try pair.parent.allocateLocked(0, max_frame_bytes);
    }
    const body = try std.testing.allocator.alloc(u8, 2 * max_frame_bytes);
    defer std.testing.allocator.free(body);
    @memset(body, 0xc5);
    // Child execution succeeds, but the parent has neither bulk nor control
    // payload credit for the response. Its caller must still finish promptly.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = body }, .{}));
    for (held[0..held_count]) |payload| payload.deinit();
    held_count = 0;
    const response = try pair.parent.call(.{ .body = "nested" }, .{});
    defer response.deinit();
    try std.testing.expectEqualStrings("echo", response.view().body);
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker response ownership survives endpoint replacement" {
    const response = blk: {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        const result = try pair.parent.call(.{ .body = "response" }, .{});
        break :blk result.detach();
    };
    defer response.deinit();
    try std.testing.expectEqualStrings("response", response.view().body);
}

test "inference worker resource callbacks bypass saturated ordinary budgets and slots" {
    const Probe = struct {
        fn resource(_: *anyopaque, bytes: []const u8) !ResourceReply {
            try std.testing.expectEqualStrings("reserve", bytes);
            return .{ .value = 7 };
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.resource_handler = Probe.resource;
    pair.child.control_bytes = control_budget_bytes;
    pair.child.retained_bytes = max_retained_bytes;
    pair.child.mutex.unlock(pair.child.io);
    // Fill even the ordinary offer, pending, and handler slots. Resource calls
    // must neither inspect these entries nor require an ordinary reply offer.
    var pending = Pending{};
    var offer = Offer{};
    var dummy_request: Request = undefined;
    pair.parent.mutex.lockUncancelable(pair.parent.io);
    for (0..max_requests) |i| {
        try pair.parent.pending.put(pair.parent.alloc, @intCast(i + 1000), &pending);
        try pair.parent.offers.put(pair.parent.alloc, @intCast(i + 1000), &offer);
    }
    pair.parent.control_bytes = control_budget_bytes;
    pair.parent.retained_bytes = max_retained_bytes;
    pair.parent.mutex.unlock(pair.parent.io);
    pair.child.mutex.lockUncancelable(pair.child.io);
    for (0..max_requests) |i| try pair.child.active.put(pair.child.alloc, @intCast(i + 1000), &dummy_request);
    pair.child.mutex.unlock(pair.child.io);
    defer {
        pair.parent.mutex.lockUncancelable(pair.parent.io);
        pair.parent.pending.clearRetainingCapacity();
        pair.parent.offers.clearRetainingCapacity();
        pair.parent.control_bytes = 0;
        pair.parent.retained_bytes = 0;
        pair.parent.mutex.unlock(pair.parent.io);
        pair.child.mutex.lockUncancelable(pair.child.io);
        pair.child.active.clearRetainingCapacity();
        pair.child.control_bytes = 0;
        pair.child.retained_bytes = 0;
        pair.child.mutex.unlock(pair.child.io);
    }
    const result = try pair.parent.callResource("reserve");
    try std.testing.expectEqual(@as(u64, 7), result.value);
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker uncertain resource replies fence cleanup and wake callers" {
    const Probe = struct {
        pair: *TestPair,
        leased: std.atomic.Value(bool) = .init(false),
        reaping: std.Io.Event = .unset,
        reaped: std.Io.Event = .unset,
        cleaned: std.Io.Event = .unset,
        cleanups: usize = 0,
        fn resource(raw: *anyopaque, _: []const u8) !ResourceReply {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.leased.store(true, .release);
            // The mutation succeeds, but the reply cannot be delivered.
            // Called inside a handler: teardown must never try to join itself.
            self.pair.child.fail();
            return .{ .value = 7 };
        }
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reaping.set(std.testing.io);
            self.reaped.wait(std.testing.io) catch unreachable;
            self.leased.store(false, .release);
            self.cleanups += 1;
            self.pair.parent.fail(); // Models the worker's pipe EOF.
            self.cleaned.set(std.testing.io);
        }
        fn call(self: *@This()) !void {
            try std.testing.expectError(error.InferenceWorkerUnavailable, self.pair.parent.callResource("reserve"));
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var probe: Probe = .{ .pair = &pair };
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.context = &probe;
    pair.child.resource_handler = Probe.resource;
    pair.child.on_closed = Probe.closed;
    pair.child.mutex.unlock(pair.child.io);
    var call = try std.testing.io.concurrent(Probe.call, .{&probe});
    defer _ = call.cancel(std.testing.io) catch {};
    // Always unblock lifecycle cleanup even if an assertion fails.
    defer probe.reaped.set(std.testing.io);
    try probe.reaping.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(probe.leased.load(.acquire));
    pair.child.fail(); // Repeated faults must not run cleanup twice.
    probe.reaped.set(std.testing.io);
    try probe.cleaned.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try call.await(std.testing.io);
    try std.testing.expect(!probe.leased.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), probe.cleanups);
}

test "inference worker resource dispatch failure closes the endpoint before mutation" {
    const Probe = struct {
        pair: *TestPair,
        fn resource(_: *anyopaque, _: []const u8) !ResourceReply {
            return error.OutOfMemory;
        }
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pair.parent.fail();
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var probe: Probe = .{ .pair = &pair };
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.context = &probe;
    pair.child.resource_handler = Probe.resource;
    pair.child.on_closed = Probe.closed;
    pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectError(error.InferenceWorkerUnavailable, pair.parent.callResource("release"));
    try std.testing.expect(pair.child.closed.load(.acquire));
}

test "inference worker malformed resource replies close the endpoint" {
    const Probe = struct {
        pair: *TestPair,
        fn resource(raw: *anyopaque, _: []const u8) !ResourceReply {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.pair.child.sendFrame(.resource_reply, 1, "truncated");
            return .{ .value = 7 };
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var probe: Probe = .{ .pair = &pair };
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.context = &probe;
    pair.child.resource_handler = Probe.resource;
    pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectError(error.InferenceWorkerUnavailable, pair.parent.callResource("reserve"));
    try std.testing.expect(pair.parent.closed.load(.acquire));
}

test "inference worker exhausted resource dispatch slots fail the owner instead of dropping release" {
    const Probe = struct {
        pair: *TestPair,
        fn resource(_: *anyopaque, _: []const u8) !ResourceReply {
            return error.UnexpectedDispatch;
        }
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pair.parent.fail();
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var probe: Probe = .{ .pair = &pair };
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.context = &probe;
    pair.child.resource_handler = Probe.resource;
    pair.child.on_closed = Probe.closed;
    for (&pair.child.resource_active, 0..) |*entry, i| entry.* = @intCast(i + 1000);
    pair.child.mutex.unlock(pair.child.io);
    defer {
        pair.child.mutex.lockUncancelable(pair.child.io);
        pair.child.resource_active = @splat(null);
        pair.child.mutex.unlock(pair.child.io);
    }
    try std.testing.expectError(error.InferenceWorkerUnavailable, pair.parent.callResource("release"));
    try std.testing.expect(pair.child.closed.load(.acquire));
}

test "inference worker concurrent resource callbacks route replies by ID" {
    const Probe = struct {
        pair: *TestPair,
        both: std.Io.Event = .unset,
        entered: std.atomic.Value(usize) = .init(0),
        fn resource(raw: *anyopaque, bytes: []const u8) !ResourceReply {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.entered.fetchAdd(1, .acq_rel) == 1) self.both.set(std.testing.io);
            try self.both.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            return .{ .value = bytes[0] };
        }
        fn call(self: *@This(), bytes: []const u8) !void {
            const result = try self.pair.parent.callResource(bytes);
            try std.testing.expectEqual(@as(u64, bytes[0]), result.value);
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var probe: Probe = .{ .pair = &pair };
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.context = &probe;
    pair.child.resource_handler = Probe.resource;
    pair.child.mutex.unlock(pair.child.io);
    var first = try std.testing.io.concurrent(Probe.call, .{ &probe, "first" });
    defer _ = first.cancel(std.testing.io) catch {};
    var second = try std.testing.io.concurrent(Probe.call, .{ &probe, "second" });
    defer _ = second.cancel(std.testing.io) catch {};
    try first.await(std.testing.io);
    try second.await(std.testing.io);
}
