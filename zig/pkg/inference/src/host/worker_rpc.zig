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
const Kind = enum(u8) { offer = 1, accept, reject, chunk, finish, abort, cancel, failure, capacity, resource_request, resource_reply, body_read, body_chunk };
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
    /// Send-only scatter/gather body. The receiver still owns one admitted slab.
    body_segments: ?[]const []const u8 = null,

    fn bodySize(self: Payload) !usize {
        var len = self.body.len;
        if (self.body_segments) |segments| {
            if (len != 0) return error.InvalidInput;
            for (segments) |segment| len = std.math.add(usize, len, segment.len) catch return error.BodyTooLarge;
        }
        if (len > max_body_bytes) return error.BodyTooLarge;
        return len;
    }

    fn size(self: Payload) !usize {
        if (self.metadata.len > max_metadata_bytes)
            return error.BodyTooLarge;
        return self.metadata.len + try self.bodySize();
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
    /// Runs concurrently with event/check callbacks; must honor Io cancellation.
    read_body: ?*const fn (?*anyopaque, []u8) anyerror!usize = null,
};

pub const Request = struct {
    endpoint: *Endpoint,
    id: u64,
    payload: OwnedPayload,
    cancelled: std.atomic.Value(bool) = .init(false),
    body_ready: std.Io.Event = .unset,
    body_waiting: bool = false,
    body_eof: bool = false,
    body_received: bool = false,
    body_failed: bool = false,
    body_len: usize = 0,
    body_buffer: [16384]u8 = undefined,

    /// Pull one bounded upload chunk. At most one chunk can be in flight;
    /// response events continue through the existing independently read pipe.
    pub fn readBody(self: *Request, dest: []u8) !usize {
        if (dest.len == 0) return 0;
        try check(self);
        const endpoint = self.endpoint;
        endpoint.mutex.lockUncancelable(endpoint.io);
        if (self.body_eof) {
            endpoint.mutex.unlock(endpoint.io);
            return 0;
        }
        if (self.body_waiting) {
            endpoint.mutex.unlock(endpoint.io);
            return error.InvalidWorkerFrame;
        }
        self.body_waiting = true;
        self.body_received = false;
        self.body_ready.reset();
        endpoint.mutex.unlock(endpoint.io);
        defer {
            endpoint.mutex.lockUncancelable(endpoint.io);
            self.body_waiting = false;
            endpoint.mutex.unlock(endpoint.io);
        }
        var capacity: [4]u8 = undefined;
        std.mem.writeInt(u32, &capacity, @intCast(@min(dest.len, self.body_buffer.len)), .little);
        try endpoint.sendFrame(.body_read, self.id, &capacity);
        while (true) {
            try check(self);
            endpoint.mutex.lockUncancelable(endpoint.io);
            if (self.body_received) {
                defer endpoint.mutex.unlock(endpoint.io);
                if (self.body_failed) return error.BodyReadFailed;
                if (self.body_len > dest.len) return error.InvalidWorkerFrame;
                @memcpy(dest[0..self.body_len], self.body_buffer[0..self.body_len]);
                self.body_eof = self.body_len == 0;
                return self.body_len;
            }
            const closed = endpoint.closed.load(.acquire);
            endpoint.mutex.unlock(endpoint.io);
            if (closed) return error.InferenceWorkerUnavailable;
            try endpoint.waitTick(&self.body_ready);
        }
    }

    fn check(raw: ?*anyopaque) !void {
        const self: *Request = @ptrCast(@alignCast(raw.?));
        if (self.cancelled.load(.acquire)) return error.Cancelled;
    }
    pub fn event(self: *Request, payload: Payload) !void {
        while (true) {
            try check(self);
            self.endpoint.sendMessage(.event, self.id, payload, .{ .ptr = self, .check = check }) catch |err| switch (err) {
                error.QueueFull => {
                    // A rejected offer transferred no payload or finish frame.
                    // Preserve bounded receiver credits and wait for the HTTP
                    // reader to drain them before offering this same event.
                    // Parent cancellation and endpoint closure remain checked
                    // between attempts; accepted events are never replayed.
                    try self.endpoint.io.sleep(.fromMilliseconds(10), .awake);
                    continue;
                },
                else => return err,
            };
            return;
        }
    }
};

const Message = struct { kind: MessageKind, payload: OwnedPayload };
const Pending = struct {
    body_capacity: ?usize = null,
    ready: std.Io.Event = .unset,
    terminal: ?Failure = null,
    messages: std.ArrayListUnmanaged(Message) = .empty,
};
const Offer = struct {
    ready: std.Io.Event = .unset,
    accepted: bool = false,
};
const Assembly = struct { kind: MessageKind, request_id: u64, payload: OwnedPayload, received: usize = 0 };

/// One request-scoped upload read. The dispatch loop remains responsive to
/// deadlines, terminal replies and response events while the source is idle.
/// Publication uses the endpoint mutex; cancellation always joins the reader
/// before the caller releases its source or pending request storage.
const BodyRead = struct {
    group: std.Io.Group = .init,
    active: bool = false,
    result: ?anyerror!usize = null,
    bytes: [16385]u8 = undefined,

    fn run(self: *BodyRead, endpoint: *Endpoint, pending: *Pending, control: Control, capacity: usize) std.Io.Cancelable!void {
        const result = if (control.read_body) |read|
            read(control.ptr, self.bytes[1..][0..capacity])
        else
            error.BodySourceUnavailable;
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        self.result = if (result) |count| if (count <= capacity) count else error.InvalidBodyReadCount else |err| err;
        pending.ready.set(endpoint.io);
    }
};

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
        const owned = try self.allocateLocked(value.metadata.len, try value.bodySize());
        @memcpy(owned.bytes[0..value.metadata.len], value.metadata);
        var offset = value.metadata.len;
        for (value.body_segments orelse &.{value.body}) |segment| {
            @memcpy(owned.bytes[offset..][0..segment.len], segment);
            offset += segment.len;
        }
        return owned;
    }

    pub fn call(self: *Endpoint, payload: Payload, control: Control) !OwnedPayload {
        _ = try payload.size();
        if (control.check) |check| try check(control.ptr);
        var pending = Pending{};
        self.mutex.lockUncancelable(self.io);
        if (self.closed.load(.acquire)) {
            self.mutex.unlock(self.io);
            return error.InferenceWorkerUnavailable;
        }
        if (self.pending.count() >= max_requests) {
            self.mutex.unlock(self.io);
            return error.QueueFull;
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
        var body_read: BodyRead = .{};
        defer body_read.group.cancel(self.io);
        errdefer self.sendFrame(.cancel, id, "") catch {};
        try self.sendMessage(.request, id, payload, control);
        while (true) {
            if (control.check) |check| try check(control.ptr);
            self.mutex.lockUncancelable(self.io);
            // Terminal replies and response events take priority over upload
            // progress, including a source that completed at the same instant.
            const message: ?Message = if (pending.messages.items.len > 0) pending.messages.orderedRemove(0) else null;
            const body_result = if (message == null) body_read.result else null;
            if (body_result != null) body_read.result = null;
            const closed = self.closed.load(.acquire);
            const terminal = pending.terminal;
            const body_capacity = if (message == null and body_result == null) pending.body_capacity else null;
            if (body_capacity != null) pending.body_capacity = null;
            if (message == null and body_capacity == null and body_result == null) pending.ready.reset();
            self.mutex.unlock(self.io);
            if (message == null) {
                if (terminal) |failure| return if (failure == .capacity) error.ResourceTemporarilyUnavailable else error.InferenceWorkerRequestFailed;
                if (closed) return error.InferenceWorkerUnavailable;
            }
            if (body_result) |result| {
                try body_read.group.await(self.io);
                body_read.active = false;
                if (result) |count| {
                    body_read.bytes[0] = 0;
                    try self.sendFrame(.body_chunk, id, body_read.bytes[0 .. count + 1]);
                } else |err| {
                    // Preserve source errors (including upload ceilings) on the
                    // caller and cancel only this request. The reader is joined.
                    return err;
                }
            } else if (message) |item| {
                switch (item.kind) {
                    .response => return item.payload,
                    .event => {
                        defer item.payload.deinit();
                        if (control.event) |event| try event(control.ptr, item.payload.view());
                    },
                    .request => unreachable,
                }
            } else if (body_capacity) |capacity| {
                if (body_read.active) return error.InvalidWorkerFrame;
                body_read.active = true;
                try body_read.group.concurrent(self.io, BodyRead.run, .{ &body_read, self, &pending, control, capacity });
            } else {
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
        if (self.closed.load(.acquire)) {
            self.mutex.unlock(self.io);
            return error.InferenceWorkerUnavailable;
        }
        if (self.offers.count() >= max_requests) {
            self.mutex.unlock(self.io);
            return error.QueueFull;
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
        description[0] = @backingInt(kind);
        std.mem.writeInt(u64, description[1..9], request_id, .little);
        std.mem.writeInt(u32, description[9..13], @intCast(payload.metadata.len), .little);
        std.mem.writeInt(u64, description[13..21], try payload.bodySize(), .little);
        errdefer self.sendFrame(.abort, transfer, "") catch {};
        try self.sendFrame(.offer, transfer, &description);
        while (!offer.ready.isSet()) {
            if (control.check) |check| try check(control.ptr);
            if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
            try self.waitTick(&offer.ready);
        }
        // No payload or finish frame has crossed this boundary. A denied
        // request has not executed and can be retried after sibling admission
        // drains. Response denial is translated to terminal capacity below:
        // the original request may already have executed successfully.
        if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        if (!offer.accepted) return error.QueueFull;
        const parts = payload.body_segments orelse &.{payload.body};
        for (0..parts.len + 1) |index| {
            const part = if (index == 0) payload.metadata else parts[index - 1];
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
        @memcpy(header[0..4], "AFW4");
        header[4] = @backingInt(kind);
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
            if (!std.mem.eql(u8, header[0..4], "AFW4")) return error.InvalidWorkerFrame;
            const kind = std.enums.fromInt(Kind, header[4]) orelse return error.InvalidWorkerFrame;
            const id = std.mem.readInt(u64, header[5..13], .little);
            const length = std.mem.readInt(u32, header[13..17], .little);
            if (length > max_frame_bytes or id == 0) return error.InvalidWorkerFrame;
            const bytes = buffer[0..length];
            try self.readExact(bytes);
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            switch (kind) {
                .body_read => {
                    if (length != 4) return error.InvalidWorkerFrame;
                    const capacity = std.mem.readInt(u32, bytes[0..4], .little);
                    if (capacity == 0 or capacity > 16384) return error.InvalidWorkerFrame;
                    if (self.pending.get(id)) |pending| {
                        if (pending.body_capacity != null) return error.InvalidWorkerFrame;
                        pending.body_capacity = capacity;
                        pending.ready.set(self.io);
                    } else {
                        try self.replyOffer(.cancel, id);
                    }
                },
                .body_chunk => {
                    if (length == 0 or length > 16385 or bytes[0] > 1) return error.InvalidWorkerFrame;
                    if (self.active.get(id)) |request| {
                        if (request.cancelled.load(.acquire)) continue;
                        if (!request.body_waiting or request.body_received) return error.InvalidWorkerFrame;
                        request.body_len = bytes.len - 1;
                        @memcpy(request.body_buffer[0..request.body_len], bytes[1..]);
                        request.body_failed = bytes[0] != 0;
                        request.body_received = true;
                        request.body_ready.set(self.io);
                    }
                },
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
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable or err == error.QueueFull) .capacity else .failure, request.id, "") catch {};
            return;
        };
        defer response.deinit();
        self.sendMessage(.response, request.id, response.view(), .{ .ptr = request, .check = Request.check }) catch |err| {
            // Terminal failure never needs message credit or an offer slot.
            // Even a completely full recipient can finish this one request.
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable or err == error.QueueFull) .capacity else .failure, request.id, "") catch {};
        };
    }
};

const TestPair = struct {
    parent: Endpoint,
    child: Endpoint,
    files: [4]std.Io.File,
    cancelled: std.atomic.Value(bool) = .init(false),
    handled: std.atomic.Value(usize) = .init(0),
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

    pub fn deinit(self: *TestPair) void {
        self.parent.deinit();
        self.child.deinit();
        for (self.files) |file| file.close(std.testing.io);
    }

    fn closed(_: *anyopaque) void {}
    fn handle(raw: *anyopaque, request: *Request) !OwnedPayload {
        const self: *TestPair = @ptrCast(@alignCast(raw));
        _ = self.handled.fetchAdd(1, .monotonic);
        if (std.mem.eql(u8, request.payload.view().body, "duplex")) {
            try request.event(.{ .body = "opening" });
            var chunk: [4]u8 = undefined;
            const n = try request.readBody(&chunk);
            try request.event(.{ .body = chunk[0..n] });
            if (try request.readBody(&chunk) != 0) return error.InvalidWorkerFrame;
            return request.endpoint.copyPayload(.{ .body = "complete" });
        }
        if (std.mem.eql(u8, request.payload.view().body, "idle-upload")) {
            const Heartbeat = struct {
                fn run(pair: *TestPair, req: *Request) std.Io.Cancelable!void {
                    try pair.cancel_seen.wait(std.testing.io);
                    req.event(.{ .body = "heartbeat" }) catch {};
                }
            };
            var group: std.Io.Group = .init;
            defer group.cancel(std.testing.io);
            try group.concurrent(std.testing.io, Heartbeat.run, .{ self, request });
            var chunk: [4]u8 = undefined;
            _ = try request.readBody(&chunk);
            try group.await(std.testing.io);
            return request.endpoint.copyPayload(.{ .body = "complete" });
        }
        if (std.mem.eql(u8, request.payload.view().body, "early-upload")) {
            const Reader = struct {
                fn run(req: *Request) std.Io.Cancelable!void {
                    var chunk: [4]u8 = undefined;
                    _ = req.readBody(&chunk) catch {};
                }
            };
            var group: std.Io.Group = .init;
            defer group.cancel(std.testing.io);
            try group.concurrent(std.testing.io, Reader.run, .{request});
            try self.cancel_seen.wait(std.testing.io);
            return request.endpoint.copyPayload(.{ .body = "early" });
        }
        if (std.mem.eql(u8, request.payload.view().body, "blocked-upload")) {
            var chunk: [4]u8 = undefined;
            _ = request.readBody(&chunk) catch |err| {
                self.cancel_seen.set(std.testing.io);
                return err;
            };
        }
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

test "inference worker segmented media crosses the actual RPC boundary without sender copies" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const envelope = @import("httpx").attachment_envelope;
    var encoded = try envelope.encodeSegmentsAlloc(std.testing.allocator, "{\"image_count\":2}", &.{
        .{ .mime_type = "image/png", .data = "\x89PNG\x00\xff" },
        .{ .mime_type = "application/x-antfly-raster", .data = "\x00\xff\x00\x80" },
    });
    defer encoded.deinit();
    const result = try pair.parent.call(.{ .metadata = "provider options", .body_segments = encoded.segments }, .{});
    defer result.deinit();
    var decoded = try envelope.parseAlloc(std.testing.allocator, result.view().body, .{});
    defer decoded.deinit();
    try std.testing.expectEqualStrings("provider options", result.view().metadata);
    try std.testing.expectEqualStrings("{\"image_count\":2}", decoded.metadata);
    try std.testing.expectEqualStrings("\x00\xff\x00\x80", decoded.attachments[1].data);
    try std.testing.expectError(error.InvalidInput, pair.parent.call(.{ .body = "ambiguous", .body_segments = encoded.segments }, .{}));
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
    try std.testing.expectError(error.QueueFull, pair.parent.call(.{ .body = body }, .{}));
    try std.testing.expectEqual(@as(usize, 0), pair.handled.load(.monotonic));
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
    offer[0] = @backingInt(MessageKind.request);
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

const PullFixture = struct {
    remaining: usize,
    sent: usize = 0,
    reads: std.atomic.Value(usize) = .init(0),
    fail_at: usize = std.math.maxInt(usize),
    cancel_after: usize = std.math.maxInt(usize),
    oversized: bool = false,

    fn read(raw: ?*anyopaque, destination: []u8) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.reads.load(.acquire) == self.fail_at) return error.TestBodyReadFailed;
        const reads = self.reads.fetchAdd(1, .acq_rel) + 1;
        if (self.oversized) return destination.len + 1;
        const n = @min(self.remaining, destination.len, if (reads == 3) @as(usize, 7) else max_frame_bytes);
        for (destination[0..n], 0..) |*byte, i| byte.* = @truncate((self.sent + i) *% 31);
        self.sent += n;
        self.remaining -= n;
        return n;
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.reads.load(.acquire) >= self.cancel_after) return error.Cancelled;
    }
    fn handler(_: *anyopaque, request: *Request) !OwnedPayload {
        var buffer: [max_frame_bytes]u8 = undefined;
        var count: usize = 0;
        while (true) {
            const n = try request.readBody(&buffer);
            if (n == 0) break;
            for (buffer[0..n], 0..) |byte, i| {
                if (byte != @as(u8, @truncate((count + i) *% 31))) return error.BodyContentMismatch;
            }
            count += n;
        }
        // EOF stays terminal without another callback into the caller.
        if (try request.readBody(&buffer) != 0) return error.BodyContentAfterEof;
        var result: [8]u8 = undefined;
        std.mem.writeInt(u64, &result, @intCast(count), .little);
        return request.endpoint.copyPayload(.{ .body = &result });
    }
};

test "inference worker pull body exceeds buffered limit with bounded chunks and short reads" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    pair.child.handler = PullFixture.handler;
    const size = max_body_bytes + 19;
    var source: PullFixture = .{ .remaining = size };
    const response = try pair.parent.call(.{}, .{ .ptr = &source, .read_body = PullFixture.read });
    defer response.deinit();
    try std.testing.expectEqual(@as(u64, size), std.mem.readInt(u64, response.view().body[0..8], .little));
    try std.testing.expectEqual(size, source.sent);
    try std.testing.expect(source.reads.load(.acquire) > 1024);
    pair.child.mutex.lockUncancelable(pair.child.io);
    const peak_retained_now = pair.child.retained_bytes;
    pair.child.mutex.unlock(pair.child.io);
    try std.testing.expectEqual(@as(usize, 0), peak_retained_now);
}

test "inference worker pull body failures cancel only their request and preserve transport" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    pair.child.handler = PullFixture.handler;
    var failed: PullFixture = .{ .remaining = 1024 * 1024, .fail_at = 2 };
    try std.testing.expectError(error.TestBodyReadFailed, pair.parent.call(.{}, .{ .ptr = &failed, .read_body = PullFixture.read }));
    var oversized: PullFixture = .{ .remaining = 1024, .oversized = true };
    try std.testing.expectError(error.InvalidBodyReadCount, pair.parent.call(.{}, .{ .ptr = &oversized, .read_body = PullFixture.read }));
    var cancelled: PullFixture = .{ .remaining = 1024 * 1024, .cancel_after = 2 };
    try std.testing.expectError(error.Cancelled, pair.parent.call(.{}, .{ .ptr = &cancelled, .read_body = PullFixture.read, .check = PullFixture.check }));
    try std.testing.expectError(error.BodySourceUnavailable, pair.parent.call(.{}, .{}));
    var healthy: PullFixture = .{ .remaining = 100123 };
    const response = try pair.parent.call(.{}, .{ .ptr = &healthy, .read_body = PullFixture.read });
    defer response.deinit();
    try std.testing.expectEqual(@as(u64, 100123), std.mem.readInt(u64, response.view().body[0..8], .little));
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker pull body refuses requests above one frame" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var capacity: [4]u8 = undefined;
    std.mem.writeInt(u32, &capacity, max_frame_bytes + 1, .little);
    try pair.child.sendFrame(.body_read, 1, &capacity);
    while (!pair.parent.closed.load(.acquire)) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
}

test "inference worker pull bodies keep concurrent requests separate" {
    const Probe = struct {
        endpoint: *Endpoint,
        bytes: usize,
        fn run(self: @This()) !void {
            var source: PullFixture = .{ .remaining = self.bytes };
            const response = try self.endpoint.call(.{}, .{ .ptr = &source, .read_body = PullFixture.read });
            defer response.deinit();
            try std.testing.expectEqual(@as(u64, @intCast(self.bytes)), std.mem.readInt(u64, response.view().body[0..8], .little));
            try std.testing.expectEqual(self.bytes, source.sent);
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    pair.child.handler = PullFixture.handler;
    var first = try std.testing.io.concurrent(Probe.run, .{Probe{ .endpoint = &pair.parent, .bytes = 1024 * 1024 + 13 }});
    defer _ = first.cancel(std.testing.io) catch {};
    var second = try std.testing.io.concurrent(Probe.run, .{Probe{ .endpoint = &pair.parent, .bytes = 512 * 1024 + 99 }});
    defer _ = second.cancel(std.testing.io) catch {};
    try first.await(std.testing.io);
    try second.await(std.testing.io);
}

test "inference worker pull body drains queued events before borrowing the reader" {
    const Probe = struct {
        source: PullFixture = .{ .remaining = 70123 },
        events: usize = 0,
        fn handler(raw: *anyopaque, request: *Request) !OwnedPayload {
            try request.event(.{ .body = "before-body" });
            return PullFixture.handler(raw, request);
        }
        fn event(raw: ?*anyopaque, payload: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("before-body", payload.body);
            self.events += 1;
        }
        fn read(raw: ?*anyopaque, destination: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.events != 1) return error.EventOrderChanged;
            return PullFixture.read(&self.source, destination);
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    pair.child.handler = Probe.handler;
    var probe: Probe = .{};
    const response = try pair.parent.call(.{}, .{ .ptr = &probe, .read_body = Probe.read, .event = Probe.event });
    defer response.deinit();
    try std.testing.expectEqual(@as(u64, 70123), std.mem.readInt(u64, response.view().body[0..8], .little));
}

test "inference worker streaming events wait for receive credit and remain cancellable" {
    const Probe = struct {
        pair: *TestPair,
        entered: std.Io.Event = .unset,
        finished: std.atomic.Value(bool) = .init(false),
        cancelled: std.atomic.Value(bool) = .init(false),
        events: usize = 0,

        fn handle(raw: *anyopaque, request: *Request) !OwnedPayload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.eql(u8, request.payload.view().body, "stream")) {
                defer self.finished.store(true, .release);
                self.entered.set(std.testing.io);
                try request.event(.{ .body = "checkpoint-part" });
            }
            return request.endpoint.copyPayload(request.payload.view());
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled.load(.acquire)) return error.Cancelled;
        }
        fn event(raw: ?*anyopaque, payload: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("checkpoint-part", payload.body);
            self.events += 1;
        }
        fn call(self: *@This()) !void {
            const result = try self.pair.parent.call(.{ .body = "stream" }, .{ .ptr = self, .check = check, .event = event });
            defer result.deinit();
            try std.testing.expectEqualStrings("stream", result.view().body);
        }
    };
    for ([_]bool{ false, true }) |cancel| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var probe: Probe = .{ .pair = &pair };
        pair.child.context = &probe;
        pair.child.handler = Probe.handle;
        var held: [control_budget_bytes / max_frame_bytes]OwnedPayload = undefined;
        var held_count: usize = 0;
        defer for (held[0..held_count]) |payload| payload.deinit();
        {
            pair.parent.mutex.lockUncancelable(pair.parent.io);
            defer pair.parent.mutex.unlock(pair.parent.io);
            while (held_count < held.len) : (held_count += 1)
                held[held_count] = try pair.parent.allocateLocked(0, max_frame_bytes);
        }
        pair.child.mutex.lockUncancelable(pair.child.io);
        const initial_transfer = pair.child.next_transfer;
        pair.child.mutex.unlock(pair.child.io);
        var call = try std.testing.io.concurrent(Probe.call, .{&probe});
        defer _ = call.cancel(std.testing.io) catch {};
        try probe.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        // The receiver owns every control credit. No stream event can be
        // admitted until those credits drain, but the request remains live.
        var retried_without_credit = false;
        for (0..500) |_| {
            if (probe.finished.load(.acquire)) break;
            pair.child.mutex.lockUncancelable(pair.child.io);
            retried_without_credit = pair.child.next_transfer >= initial_transfer + 2;
            pair.child.mutex.unlock(pair.child.io);
            if (retried_without_credit) break;
            try std.testing.io.sleep(.fromMilliseconds(10), .awake);
        }
        const finished_without_credit = probe.finished.load(.acquire);
        if (cancel) {
            probe.cancelled.store(true, .release);
            try std.testing.expectError(error.Cancelled, call.await(std.testing.io));
        }
        for (held[0..held_count]) |payload| payload.deinit();
        held_count = 0;
        if (!cancel) {
            try call.await(std.testing.io);
            try std.testing.expectEqual(@as(usize, 1), probe.events);
        }
        try std.testing.expect(!finished_without_credit);
        try std.testing.expect(retried_without_credit);
        {
            const result = try pair.parent.call(.{ .body = "healthy" }, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings("healthy", result.view().body);
        }
        pair.parent.mutex.lockUncancelable(pair.parent.io);
        const control_bytes = pair.parent.control_bytes;
        const retained_bytes = pair.parent.retained_bytes;
        pair.parent.mutex.unlock(pair.parent.io);
        try std.testing.expectEqual(@as(usize, 0), control_bytes);
        try std.testing.expectEqual(@as(usize, 0), retained_bytes);
        try std.testing.expect(!pair.parent.closed.load(.acquire));
        try std.testing.expect(!pair.child.closed.load(.acquire));
    }
}

test "inference worker RPC pulls bounded audio while response events remain incremental" {
    const Capture = struct {
        events: usize = 0,
        reads: usize = 0,
        fn event(raw: ?*anyopaque, payload: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings(if (self.events == 0) "opening" else "pcm!", payload.body);
            self.events += 1;
        }
        fn read(raw: ?*anyopaque, dest: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqual(self.reads + 1, self.events);
            self.reads += 1;
            if (self.reads > 1) return 0;
            try std.testing.expectEqual(@as(usize, 4), dest.len);
            @memcpy(dest, "pcm!");
            return dest.len;
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var capture: Capture = .{};
    const response = try pair.parent.call(.{ .body = "duplex" }, .{ .ptr = &capture, .event = Capture.event, .read_body = Capture.read });
    defer response.deinit();
    try std.testing.expectEqualStrings("complete", response.view().body);
    try std.testing.expectEqual(@as(usize, 2), capture.events);
    try std.testing.expectEqual(@as(usize, 2), capture.reads);
}

test "inference worker blocked upload honors deadline and joins source before returning" {
    const Source = struct {
        started: std.atomic.Value(i64) = .init(0),
        stopped: std.atomic.Value(bool) = .init(false),
        fn now() i64 {
            return @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds);
        }
        fn read(raw: ?*anyopaque, _: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.started.store(now(), .release);
            defer self.stopped.store(true, .release);
            try std.testing.io.sleep(.fromSeconds(10), .awake);
            return 0;
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const started = self.started.load(.acquire);
            if (started != 0 and now() - started >= 50 * std.time.ns_per_ms) return error.Timeout;
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var source: Source = .{};
    try std.testing.expectError(error.Timeout, pair.parent.call(.{ .body = "blocked-upload" }, .{
        .ptr = &source,
        .check = Source.check,
        .read_body = Source.read,
    }));
    try std.testing.expect(source.stopped.load(.acquire));
    try std.testing.expect(Source.now() - source.started.load(.acquire) < std.time.ns_per_s);
    try pair.cancel_seen.wait(std.testing.io);
    const response = try pair.parent.call(.{ .body = "echo" }, .{});
    defer response.deinit();
    try std.testing.expectEqualStrings("echo", response.view().body);
}

test "inference worker idle uploads allow response events and early terminal replies" {
    const Source = struct {
        pair: *TestPair,
        release: std.Io.Event = .unset,
        stopped: std.atomic.Value(bool) = .init(false),
        deadline: i64,
        fn read(raw: ?*anyopaque, _: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            defer self.stopped.store(true, .release);
            self.pair.cancel_seen.set(std.testing.io);
            try self.release.wait(std.testing.io);
            return 0;
        }
        fn event(raw: ?*anyopaque, payload: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("heartbeat", payload.body);
            self.release.set(std.testing.io);
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= self.deadline) return error.Timeout;
        }
    };
    for ([_][]const u8{ "idle-upload", "early-upload" }) |operation| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var source: Source = .{ .pair = &pair, .deadline = @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds + std.time.ns_per_s) };
        const response = try pair.parent.call(.{ .body = operation }, .{ .ptr = &source, .check = Source.check, .event = Source.event, .read_body = Source.read });
        defer response.deinit();
        try std.testing.expectEqualStrings(if (std.mem.eql(u8, operation, "idle-upload")) "complete" else "early", response.view().body);
        try std.testing.expect(source.stopped.load(.acquire));
    }
}
