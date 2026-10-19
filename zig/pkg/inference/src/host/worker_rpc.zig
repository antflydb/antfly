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
pub const Progress = @import("worker_progress.zig");

pub const max_frame_bytes = 64 * 1024;
pub const max_metadata_bytes = 1024 * 1024;
pub const max_body_bytes = 64 * 1024 * 1024;
const max_retained_bytes = 256 * 1024 * 1024;
const control_budget_bytes = 4 * 1024 * 1024;
const max_requests = 128;
pub const max_peer_payloads = 256;
pub const child_metadata_limit = 2 * 1024 * 1024;
const max_queued_messages = 1024;
const PeerGrant = struct { id: u64, bytes: usize };
const header_len = 17;
const Kind = enum(u8) { offer = 1, accept, reject, chunk, finish, abort, cancel, failure, capacity, resource_request, resource_reply, phase_request, phase_reply, event_reply, event_cancel, retired };
const MessageKind = enum(u8) { request = 1, response, event, acknowledged_event };
const Failure = enum { failure, capacity };

// Resource mutations use dedicated frames and slots, never payload offers.
// The caller registers fixed reply storage before the peer may mutate a budget.
pub const max_resource_bytes = 1024;
const max_resource_requests = 128;
pub const ResourceReply = struct { code: i32 = 0, detail: i32 = 0, value: u64 = 0 };
const ResourcePending = struct { id: u64, ready: std.Io.Event = .unset, reply: ?ResourceReply = null };
const ResourceTask = struct { id: u64, len: usize, bytes: [max_resource_bytes]u8 };

/// Host response bytes remain charged after detach and across worker replacement.
/// One Client anchor owns the budget; each live payload retains another reference.
/// Small messages retain a separate allowance so bulk cannot consume their credit.
pub const PayloadBudget = struct {
    pub const Limits = struct { retained: usize = max_retained_bytes, control: usize = control_budget_bytes };
    pub const External = struct {
        context: *anyopaque,
        retain: *const fn (*anyopaque) callconv(.c) u8,
        release: *const fn (*anyopaque) callconv(.c) void,
        adjust: *const fn (*anyopaque, usize, u64, u64) callconv(.c) u8,
    };
    alloc: std.mem.Allocator,
    object_allocator: std.mem.Allocator,
    limits: Limits,
    external: ?External = null,
    external_mutex: std.atomic.Mutex = .unlocked,
    external_bytes: u64 = 0,
    references: std.atomic.Value(usize) = .init(1),
    retained: std.atomic.Value(usize) = .init(0),
    control: std.atomic.Value(usize) = .init(0),

    pub fn create(alloc: std.mem.Allocator, limits: Limits) !*PayloadBudget {
        return createWithObjectAllocator(alloc, alloc, limits);
    }
    pub fn createWithObjectAllocator(backing: std.mem.Allocator, object_allocator: std.mem.Allocator, limits: Limits) !*PayloadBudget {
        const self = try object_allocator.create(PayloadBudget);
        self.* = .{ .alloc = backing, .object_allocator = object_allocator, .limits = limits };
        return self;
    }
    /// Configure only before any payload/derived allocations. The independent
    /// context edge remains until the final allocation releases this budget.
    pub fn configureExternal(self: *PayloadBudget, external: External) !void {
        if (self.external != null or self.references.load(.acquire) != 1 or
            self.retained.load(.acquire) != 0 or self.control.load(.acquire) != 0) return error.InvalidArgument;
        if (external.retain(external.context) == 0) return error.ResourceOwnerShuttingDown;
        self.external = external;
    }
    fn lockExternal(self: *PayloadBudget) void {
        while (!self.external_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn used(self: *const PayloadBudget) Limits {
        return .{ .retained = self.retained.load(.acquire), .control = self.control.load(.acquire) };
    }
    const Class = enum { retained, control };
    /// All derived output allocations use bulk credit, regardless of individual
    /// row size. They cannot consume the ordinary small-message allowance.
    pub fn retainedAllocator(self: *PayloadBudget) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = allocateRetained,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = freeRetained,
        } };
    }
    fn allocateRetained(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PayloadBudget = @ptrCast(@alignCast(raw));
        self.reserveClass(len, .retained) catch return null;
        return self.alloc.rawAlloc(len, alignment, ret_addr) orelse {
            self.releaseClass(len, .retained);
            return null;
        };
    }
    fn freeRetained(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PayloadBudget = @ptrCast(@alignCast(raw));
        self.alloc.rawFree(memory, alignment, ret_addr);
        self.releaseClass(memory.len, .retained);
    }
    fn reserve(self: *PayloadBudget, size: usize) !void {
        return self.reserveClass(size, if (size <= max_frame_bytes) .control else .retained);
    }
    fn reserveClass(self: *PayloadBudget, size: usize, class: Class) !void {
        var count = self.references.load(.acquire);
        while (true) {
            if (count == 0 or count == std.math.maxInt(usize)) return error.ResourceTemporarilyUnavailable;
            count = self.references.cmpxchgWeak(count, count + 1, .acq_rel, .acquire) orelse break;
        }
        errdefer self.releaseOwner();
        const counter = if (class == .control) &self.control else &self.retained;
        const limit = if (class == .control) self.limits.control else self.limits.retained;
        var previous = counter.load(.acquire);
        while (true) {
            if (previous > limit or size > limit - previous) return error.ResourceTemporarilyUnavailable;
            previous = counter.cmpxchgWeak(previous, previous + size, .acq_rel, .acquire) orelse break;
        }
        errdefer _ = counter.fetchSub(size, .acq_rel);
        if (self.external) |external| {
            if (!self.external_mutex.tryLock()) return error.ResourceTemporarilyUnavailable;
            defer self.external_mutex.unlock();
            const next = std.math.add(u64, self.external_bytes, size) catch return error.ResourceTemporarilyUnavailable;
            if (external.adjust(external.context, @intFromPtr(self), self.external_bytes, next) == 0) return error.ResourceTemporarilyUnavailable;
            self.external_bytes = next;
        }
    }
    fn release(self: *PayloadBudget, size: usize) void {
        self.releaseClass(size, if (size <= max_frame_bytes) .control else .retained);
    }
    fn releaseClass(self: *PayloadBudget, size: usize, class: Class) void {
        if (self.external) |external| {
            self.lockExternal();
            const next = std.math.sub(u64, self.external_bytes, size) catch @panic("worker transient accounting underflow");
            if (external.adjust(external.context, @intFromPtr(self), self.external_bytes, next) == 0)
                @panic("worker transient physical retirement rejected by node owner");
            self.external_bytes = next;
            self.external_mutex.unlock();
        }
        const counter = if (class == .control) &self.control else &self.retained;
        const previous = counter.fetchSub(size, .acq_rel);
        std.debug.assert(previous >= size);
        self.releaseOwner();
    }
    pub fn releaseOwner(self: *PayloadBudget) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous == 1) {
            std.debug.assert(self.retained.load(.acquire) == 0 and self.control.load(.acquire) == 0);
            if (self.external_bytes != 0) @panic("worker transient budget closed with accounting debt");
            const external = self.external;
            const object_allocator = self.object_allocator;
            object_allocator.destroy(self);
            // Object destruction may retire bootstrap-parent credit. Keep its
            // configured node capability alive through that physical release.
            if (external) |owner| owner.release(owner.context);
        }
    }
};

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
    budget: ?*PayloadBudget = null,
    retirement_id: u64 = 0,
    free_outside_mutex: bool = false,

    pub fn view(self: OwnedPayload) Payload {
        return .{ .metadata = self.bytes[0..self.metadata_len], .body = self.bytes[self.metadata_len..] };
    }
    pub fn deinit(self: OwnedPayload) void {
        if (self.endpoint) |endpoint| {
            if (self.free_outside_mutex) {
                self.alloc.free(self.bytes);
                endpoint.mutex.lockUncancelable(endpoint.io);
                endpoint.releaseBytesLocked(self.bytes.len);
                endpoint.mutex.unlock(endpoint.io);
                return;
            }
            endpoint.mutex.lockUncancelable(endpoint.io);
            defer endpoint.mutex.unlock(endpoint.io);
            self.deinitLocked();
        } else {
            self.alloc.free(self.bytes);
            if (self.budget) |budget| budget.release(self.bytes.len);
        }
    }
    // Transfer response ownership to the host before releasing its worker
    // lifetime lease. No payload copy or pointer into a replaceable endpoint.
    pub fn detach(self: OwnedPayload) OwnedPayload {
        if (self.retirement_id != 0 or self.free_outside_mutex) @panic("worker peer/derived payload requires endpoint-bound retirement");
        const endpoint = self.endpoint.?;
        endpoint.mutex.lockUncancelable(endpoint.io);
        defer endpoint.mutex.unlock(endpoint.io);
        endpoint.releaseBytesLocked(self.bytes.len);
        var result = self;
        result.endpoint = null;
        return result;
    }
    fn deinitLocked(self: OwnedPayload) void {
        if (self.free_outside_mutex) @panic("worker derived payload freed under reader lock");
        self.alloc.free(self.bytes);
        self.endpoint.?.releaseBytesLocked(self.bytes.len);
        if (self.budget) |budget| budget.release(self.bytes.len);
        if (self.retirement_id != 0) self.endpoint.?.retirePayloadLocked(self.retirement_id);
    }
};

/// A delegated worker must wait for the owning caller's acknowledgment before
/// entering either a resource wait or a new runnable interval.
pub const Phase = enum(u8) { resource_wait = 1, runnable = 2, checkpoint = 3 };
const PhaseStatus = enum(u8) { accepted = 0, canceled, timeout, denied };
const PhaseRequest = struct { sequence: u64, phase: Phase };

/// Checks only the lifetime of one acknowledged event. This synchronous borrow
/// may be polled by transport monitors; it never changes execution ownership.
pub const EventControl = struct {
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    pub fn check(self: @This()) !void {
        if (self.check_fn) |call| try call(self.ptr);
    }
};

pub const Control = struct {
    ptr: ?*anyopaque = null,
    check: ?*const fn (?*anyopaque) anyerror!void = null,
    event: ?*const fn (?*anyopaque, Payload) anyerror!void = null,
    event_controlled: ?*const fn (?*anyopaque, Payload, EventControl) anyerror!void = null,
    phase: ?*const fn (?*anyopaque, Phase) anyerror!void = null,
    /// The acknowledged event's sink releases execution only at physical IO
    /// waits. Its parsing and formatting must retain the current grant.
    event_owns_waits: bool = false,
    /// A caller lending physical capacity cannot retire it until an uncertain
    /// child has been reaped by the endpoint's lifetime owner.
    fence_on_error: bool = false,
};

pub const Request = struct {
    endpoint: *Endpoint,
    id: u64,
    payload: OwnedPayload,
    cancelled: std.atomic.Value(bool) = .init(false),
    progress_gate: Progress.Gate = .{},
    phase_sequence: u64 = 0,
    phase_waiting: bool = false,
    phase_reply: ?PhaseStatus = null,
    phase_ready: std.Io.Event = .unset,
    event_sequence: u64 = 0,
    event_waiting: bool = false,
    event_reply: ?PhaseStatus = null,
    event_ready: std.Io.Event = .unset,

    fn check(raw: ?*anyopaque) !void {
        const self: *Request = @ptrCast(@alignCast(raw.?));
        if (self.cancelled.load(.acquire)) return error.Cancelled;
    }
    pub fn transition(self: *Request, phase: Phase) !void {
        try check(self);
        const endpoint = self.endpoint;
        endpoint.mutex.lockUncancelable(endpoint.io);
        if (self.phase_waiting) {
            endpoint.mutex.unlock(endpoint.io);
            return error.InvalidWorkerPhase;
        }
        const sequence = std.math.add(u64, self.phase_sequence, 1) catch {
            endpoint.mutex.unlock(endpoint.io);
            return error.InvalidWorkerPhase;
        };
        self.phase_sequence = sequence;
        self.phase_waiting = true;
        self.phase_reply = null;
        self.phase_ready.reset();
        endpoint.mutex.unlock(endpoint.io);
        defer {
            endpoint.mutex.lockUncancelable(endpoint.io);
            self.phase_waiting = false;
            endpoint.mutex.unlock(endpoint.io);
        }
        var bytes: [9]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], sequence, .little);
        bytes[8] = @backingInt(phase);
        try endpoint.sendFrame(.phase_request, self.id, &bytes);
        while (true) {
            try check(self);
            endpoint.mutex.lockUncancelable(endpoint.io);
            const status = self.phase_reply;
            const closed = endpoint.closed.load(.acquire);
            endpoint.mutex.unlock(endpoint.io);
            if (status) |value| return switch (value) {
                .accepted => {},
                .canceled => error.Cancelled,
                .timeout => error.Timeout,
                .denied => error.InferenceExecutionHandoffFailed,
            };
            if (closed) return error.InferenceWorkerUnavailable;
            try endpoint.waitTick(&self.phase_ready);
        }
    }

    /// Use only at a synchronous boundary where this request's physical
    /// computation is stopped. The recipient may park borrowed CPU while its
    /// sink runs; this call cannot return successfully before reacquisition.
    pub fn eventAcknowledged(self: *Request, payload: Payload) !void {
        return self.eventAcknowledgedControlled(payload, .{});
    }

    pub fn eventAcknowledgedControlled(self: *Request, payload: Payload, control: EventControl) !void {
        try control.check();
        try check(self);
        const endpoint = self.endpoint;
        endpoint.mutex.lockUncancelable(endpoint.io);
        if (self.event_waiting) {
            endpoint.mutex.unlock(endpoint.io);
            return error.InvalidWorkerEvent;
        }
        const sequence = std.math.add(u64, self.event_sequence, 1) catch {
            endpoint.mutex.unlock(endpoint.io);
            return error.InvalidWorkerEvent;
        };
        self.event_sequence = sequence;
        self.event_waiting = true;
        self.event_reply = null;
        self.event_ready.reset();
        endpoint.mutex.unlock(endpoint.io);
        defer {
            endpoint.mutex.lockUncancelable(endpoint.io);
            self.event_waiting = false;
            endpoint.mutex.unlock(endpoint.io);
        }
        errdefer self.cancelled.store(true, .release);
        const Sending = struct {
            request: *Request,
            control: EventControl,
            fn check(raw: ?*anyopaque) !void {
                const self_sending: *@This() = @ptrCast(@alignCast(raw.?));
                try Request.check(self_sending.request);
                try self_sending.control.check();
            }
        };
        var sending = Sending{ .request = self, .control = control };
        try endpoint.sendMessage(.acknowledged_event, self.id, payload, .{ .ptr = &sending, .check = Sending.check }, null, sequence);
        // Once dispatched, a failed child control must not release this
        // synchronous borrow before the parent's physical sink has stopped.
        var failure: ?anyerror = null;
        while (true) {
            if (failure == null) {
                control.check() catch |err| {
                    failure = err;
                };
                if (failure == null) check(self) catch |err| {
                    failure = err;
                };
                if (failure) |err| {
                    var cancel: [9]u8 = undefined;
                    std.mem.writeInt(u64, cancel[0..8], sequence, .little);
                    cancel[8] = @backingInt(switch (err) {
                        error.Timeout, error.DeadlineExceeded => PhaseStatus.timeout,
                        else => PhaseStatus.canceled,
                    });
                    try endpoint.sendFrame(.event_cancel, self.id, &cancel);
                }
            }
            endpoint.mutex.lockUncancelable(endpoint.io);
            const status = self.event_reply;
            const closed = endpoint.closed.load(.acquire);
            endpoint.mutex.unlock(endpoint.io);
            if (status) |value| {
                // Caller cleanup can mark this request canceled immediately
                // after acknowledging its sink failure. Preserve that typed
                // outcome over generic cancellation; a local non-cancellation
                // cause (including its original deadline) remains authoritative.
                if (failure) |err| {
                    if (value == .accepted or value == .canceled or
                        (err != error.Cancelled and err != error.Canceled)) return err;
                }
                return switch (value) {
                    .accepted => {
                        try control.check();
                        try check(self);
                    },
                    .canceled => error.Cancelled,
                    .timeout => error.Timeout,
                    .denied => error.InferenceExecutionHandoffFailed,
                };
            }
            if (closed) return error.InferenceWorkerUnavailable;
            try endpoint.waitTick(&self.event_ready);
        }
    }

    pub fn event(self: *Request, payload: Payload) !void {
        try self.endpoint.sendMessage(.event, self.id, payload, .{ .ptr = self, .check = check }, null, 0);
    }
};

const Message = struct { kind: MessageKind, payload: OwnedPayload, event_sequence: u64 = 0 };
const Pending = struct {
    ready: std.Io.Event = .unset,
    terminal: ?Failure = null,
    next_phase_sequence: u64 = 1,
    next_event_sequence: u64 = 1,
    canceled_event_sequence: std.atomic.Value(u64) = .init(0),
    canceled_event_status: std.atomic.Value(PhaseStatus) = .init(.accepted),
    phase: ?PhaseRequest = null,
    phase_inflight: bool = false,
    messages: std.ArrayListUnmanaged(Message) = .empty,
};
const Offer = struct {
    ready: std.Io.Event = .unset,
    accepted: bool = false,
};
const Assembly = struct { kind: MessageKind, request_id: u64, payload: OwnedPayload, received: usize = 0, event_sequence: u64 = 0 };

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
    resource_tasks: [max_resource_requests]ResourceTask = undefined,
    failed: std.Io.Event = .unset,
    lifecycle_group: std.Io.Group = .init,
    closed: std.atomic.Value(bool) = .init(false),
    closed_fence: std.Io.Event = .unset,
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
    payload_budget: ?*PayloadBudget = null,
    /// Parent precharges a peer slab before its offer can cross the pipe.
    peer_payload_budget: ?*PayloadBudget = null,
    peer_grants: [max_peer_payloads]?PeerGrant = @splat(null),
    /// Child receipts are emitted only after physical payload free.
    report_retirement: bool = false,
    retirement_inflight: usize = 0,
    /// Handler allocations may synchronously ask the peer for memory credit.
    /// Never use this allocator while holding the reader mutex.
    compute_allocator: ?std.mem.Allocator = null,
    /// Must not perform synchronous peer RPC: the reader uses it under mutex.
    metadata_allocator: ?std.mem.Allocator = null,
    reader_group: std.Io.Group = .init,
    handlers: std.Io.Group = .init,
    control_writes: std.Io.Group = .init,

    fn metadataAllocator(self: *Endpoint) std.mem.Allocator {
        return self.metadata_allocator orelse self.alloc;
    }
    pub fn computeAllocator(self: *Endpoint) std.mem.Allocator {
        return self.compute_allocator orelse self.alloc;
    }
    fn reservePeerLocked(self: *Endpoint, id: u64, bytes: usize) !void {
        const budget = self.peer_payload_budget orelse return;
        for (&self.peer_grants) |*slot| {
            if (slot.* != null) continue;
            try budget.reserve(bytes);
            slot.* = .{ .id = id, .bytes = bytes };
            return;
        }
        return error.ResourceTemporarilyUnavailable;
    }
    fn releasePeerLocked(self: *Endpoint, id: u64) bool {
        const budget = self.peer_payload_budget orelse return false;
        for (&self.peer_grants) |*slot| {
            const grant = slot.* orelse continue;
            if (grant.id != id) continue;
            slot.* = null;
            budget.release(grant.bytes);
            return true;
        }
        return false;
    }
    /// Trusted physical fence only: a pipe close or empty replacement process
    /// cannot authorize this. Production calls it AFTER kill/wait reaps child.
    pub fn releasePeerAfterPhysicalJoin(self: *Endpoint) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.peer_grants) |slot| if (slot) |grant| {
            if (!self.releasePeerLocked(grant.id)) @panic("worker peer grant disappeared during physical retirement");
        };
    }
    fn retirePayloadLocked(self: *Endpoint, id: u64) void {
        if (self.closed.load(.acquire)) {
            self.retirement_inflight -= 1;
            return; // The physical owner must reap instead of trusting EOF.
        }
        self.control_writes.concurrent(self.io, writeRetirement, .{ self, id }) catch {
            self.retirement_inflight -= 1;
            self.fail();
        };
    }
    fn writeRetirement(self: *Endpoint, id: u64) std.Io.Cancelable!void {
        defer {
            self.mutex.lockUncancelable(self.io);
            self.retirement_inflight -= 1;
            self.mutex.unlock(self.io);
        }
        self.sendFrame(.retired, id, "") catch self.fail();
    }
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
    /// Only ordinary callers may wait here. The reader owns on_closed and its
    /// process-reaping fence; handlers/readers must never join themselves.
    pub fn fence(self: *Endpoint) void {
        self.fail();
        self.closed_fence.waitUncancelable(self.io);
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
        for (self.peer_grants) |slot| if (slot != null) @panic("worker endpoint closed before physical peer retirement");
        std.debug.assert(self.retirement_inflight == 0);
        for (self.resource_pending) |entry| std.debug.assert(entry == null);
        for (self.resource_active) |entry| std.debug.assert(entry == null);
        self.pending.deinit(self.metadataAllocator());
        self.active.deinit(self.metadataAllocator());
        self.offers.deinit(self.metadataAllocator());
        self.incoming.deinit(self.metadataAllocator());
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
        if (self.payload_budget) |budget| try budget.reserve(size);
        errdefer if (self.payload_budget) |budget| budget.release(size);
        return .{ .endpoint = self, .alloc = self.alloc, .bytes = try self.alloc.alloc(u8, size), .metadata_len = metadata_len, .budget = self.payload_budget };
    }
    pub fn copyPayload(self: *Endpoint, value: Payload) !OwnedPayload {
        const body_len = try value.bodySize();
        if (value.metadata.len > max_metadata_bytes) return error.BodyTooLarge;
        var owned: OwnedPayload = undefined;
        if (self.compute_allocator) |alloc| {
            // Reader/resource replies must remain runnable during admission.
            const size = value.metadata.len + body_len;
            {
                self.mutex.lockUncancelable(self.io);
                defer self.mutex.unlock(self.io);
                try self.reserveBytesLocked(size);
            }
            errdefer {
                self.mutex.lockUncancelable(self.io);
                self.releaseBytesLocked(size);
                self.mutex.unlock(self.io);
            }
            owned = .{ .endpoint = self, .alloc = alloc, .bytes = try alloc.alloc(u8, size), .metadata_len = value.metadata.len, .free_outside_mutex = true };
        } else {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            owned = try self.allocateLocked(value.metadata.len, body_len);
        }
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
        self.pending.put(self.metadataAllocator(), id, &pending) catch {
            self.mutex.unlock(self.io);
            return error.ResourceTemporarilyUnavailable;
        };
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.pending.remove(id);
            for (pending.messages.items) |message| message.payload.deinitLocked();
            pending.messages.deinit(self.metadataAllocator());
            self.mutex.unlock(self.io);
        }
        var dispatch_possible = false;
        errdefer if (control.fence_on_error and dispatch_possible) self.fence();
        errdefer self.sendFrame(.cancel, id, "") catch {};
        try self.sendMessage(.request, id, payload, control, &dispatch_possible, 0);
        while (true) {
            if (control.check) |check| try check(control.ptr);
            self.mutex.lockUncancelable(self.io);
            const phase = pending.phase;
            if (phase != null) {
                pending.phase = null;
                pending.phase_inflight = true;
            }
            const message: ?Message = if (phase == null and pending.messages.items.len > 0) pending.messages.orderedRemove(0) else null;
            const closed = self.closed.load(.acquire);
            const terminal = pending.terminal;
            if (message == null and phase == null) pending.ready.reset();
            self.mutex.unlock(self.io);
            if (phase) |transition| {
                // Execute on the original call's OS worker, never the reader
                // or a reverse-RPC dispatch task that cannot own its capsule.
                const result = if (control.phase) |callback|
                    callback(control.ptr, transition.phase)
                else
                    @as(anyerror!void, error.UnsupportedWorkerPhase);
                const status: PhaseStatus = if (result) |_| .accepted else |err| switch (err) {
                    error.Cancelled, error.Canceled => .canceled,
                    error.Timeout, error.DeadlineExceeded => .timeout,
                    else => .denied,
                };
                var bytes: [9]u8 = undefined;
                std.mem.writeInt(u64, bytes[0..8], transition.sequence, .little);
                bytes[8] = @backingInt(status);
                self.mutex.lockUncancelable(self.io);
                pending.phase_inflight = false;
                self.mutex.unlock(self.io);
                self.sendFrame(.phase_reply, id, &bytes) catch |err| {
                    // Reacquisition may have succeeded, and a partial write
                    // makes acknowledgment delivery uncertain. Fence the peer
                    // before that capacity can be reused by another operation.
                    self.fence();
                    return err;
                };
                try result;
            }
            if (message) |item| {
                switch (item.kind) {
                    .response => return item.payload,
                    .event => {
                        defer item.payload.deinit();
                        if (control.event) |event| try event(control.ptr, item.payload.view());
                    },
                    .acknowledged_event => {
                        defer item.payload.deinit();
                        var event_scope = EventScope{ .endpoint = self, .pending = &pending, .sequence = item.event_sequence };
                        const result = deliverAcknowledgedEventControlled(control, item.payload.view(), .{ .ptr = &event_scope, .check_fn = EventScope.check });
                        const status: PhaseStatus = if (result) |_| .accepted else |err| switch (err) {
                            error.Cancelled, error.Canceled => .canceled,
                            error.Timeout, error.DeadlineExceeded => .timeout,
                            else => .denied,
                        };
                        var bytes: [9]u8 = undefined;
                        std.mem.writeInt(u64, bytes[0..8], item.event_sequence, .little);
                        bytes[8] = @backingInt(status);
                        self.sendFrame(.event_reply, id, &bytes) catch |err| {
                            // The sink and reacquisition may have completed.
                            // Fence uncertain delivery before retiring capacity.
                            self.fence();
                            return err;
                        };
                        try result;
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

    const EventScope = struct {
        endpoint: *Endpoint,
        pending: *Pending,
        sequence: u64,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.pending.canceled_event_sequence.load(.acquire) == self.sequence) {
                return switch (self.pending.canceled_event_status.load(.monotonic)) {
                    .timeout => error.Timeout,
                    else => error.Cancelled,
                };
            }
            if (self.endpoint.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        }
    };

    fn deliverAcknowledgedEvent(control: Control, payload: Payload) !void {
        return deliverAcknowledgedEventControlled(control, payload, .{});
    }

    fn dispatchAcknowledgedEvent(control: Control, payload: Payload, event_control: EventControl) !void {
        try event_control.check();
        if (control.event_controlled) |event| try event(control.ptr, payload, event_control) else if (control.event) |event| try event(control.ptr, payload);
        try event_control.check();
    }

    fn deliverAcknowledgedEventControlled(control: Control, payload: Payload, event_control: EventControl) !void {
        if (control.check) |check| try check(control.ptr);
        if (control.event_owns_waits) {
            // The child cannot compute until this event is acknowledged, so
            // cooperative checkpoints are safe at these formatter boundaries.
            if (control.phase) |phase| try phase(control.ptr, .checkpoint);
            try dispatchAcknowledgedEvent(control, payload, event_control);
            if (control.check) |check| try check(control.ptr);
            if (control.phase) |phase| try phase(control.ptr, .checkpoint);
            return;
        }
        if (control.phase) |phase| try phase(control.ptr, .resource_wait);
        if (control.check) |check| try check(control.ptr);
        // On error the lending caller fences the child, then its existing
        // invocation cleanup finishes this outstanding resource wait.
        try dispatchAcknowledgedEvent(control, payload, event_control);
        if (control.check) |check| try check(control.ptr);
        if (control.phase) |phase| try phase(control.ptr, .runnable);
        if (control.check) |check| try check(control.ptr);
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

    fn sendMessage(self: *Endpoint, kind: MessageKind, request_id: u64, payload: Payload, control: Control, dispatch_possible: ?*bool, event_sequence: u64) !void {
        const payload_bytes = try payload.size();
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
        self.reservePeerLocked(transfer, payload_bytes) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.offers.put(self.metadataAllocator(), transfer, &offer) catch {
            _ = self.releasePeerLocked(transfer);
            self.mutex.unlock(self.io);
            return error.ResourceTemporarilyUnavailable;
        };
        self.mutex.unlock(self.io);
        var offer_may_have_crossed = false;
        defer {
            self.mutex.lockUncancelable(self.io);
            _ = self.offers.remove(transfer);
            if (!offer_may_have_crossed) _ = self.releasePeerLocked(transfer);
            self.mutex.unlock(self.io);
        }
        var description: [29]u8 = undefined;
        description[0] = @backingInt(kind);
        std.mem.writeInt(u64, description[1..9], request_id, .little);
        std.mem.writeInt(u32, description[9..13], @intCast(payload.metadata.len), .little);
        std.mem.writeInt(u64, description[13..21], try payload.bodySize(), .little);
        errdefer self.sendFrame(.abort, transfer, "") catch {};
        if (kind == .acknowledged_event) std.mem.writeInt(u64, description[21..29], event_sequence, .little);
        offer_may_have_crossed = true;
        try self.sendFrame(.offer, transfer, description[0..if (kind == .acknowledged_event) @as(usize, 29) else 21]);
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
        // Only a finish frame can dispatch the assembled request. Register the
        // fence requirement before writing it, including uncertain partial I/O.
        if (dispatch_possible) |possible| possible.* = true;
        try self.sendFrame(.finish, transfer, "");
    }

    fn sendFrame(self: *Endpoint, kind: Kind, id: u64, payload: []const u8) !void {
        if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        if (payload.len > max_frame_bytes) return error.InferenceWorkerFrameTooLarge;
        // Cancellation is checked between frames. Never truncate a frame and
        // leave the next request's header inside this frame's payload.
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        self.writer_mutex.lockUncancelable(self.io);
        defer self.writer_mutex.unlock(self.io);
        if (self.closed.load(.acquire)) return error.InferenceWorkerUnavailable;
        var header: [header_len]u8 = undefined;
        @memcpy(header[0..4], "AFW6");
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
        defer self.closed_fence.set(self.io);
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
            if (!std.mem.eql(u8, header[0..4], "AFW6")) return error.InvalidWorkerFrame;
            const kind = std.enums.fromInt(Kind, header[4]) orelse return error.InvalidWorkerFrame;
            const id = std.mem.readInt(u64, header[5..13], .little);
            const length = std.mem.readInt(u32, header[13..17], .little);
            if (length > max_frame_bytes or id == 0) return error.InvalidWorkerFrame;
            const bytes = buffer[0..length];
            try self.readExact(bytes);
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            switch (kind) {
                .retired => {
                    if (length != 0 or !self.releasePeerLocked(id)) return error.InvalidWorkerRetirement;
                },
                .phase_request => {
                    if (length != 9) return error.InvalidWorkerPhase;
                    const sequence = std.mem.readInt(u64, bytes[0..8], .little);
                    const phase = std.enums.fromInt(Phase, bytes[8]) orelse return error.InvalidWorkerPhase;
                    // A retired caller may have already sent cancellation.
                    // No ownership action can occur without its pending slot.
                    if (self.pending.get(id)) |pending| {
                        if (pending.phase != null or pending.phase_inflight or sequence != pending.next_phase_sequence)
                            return error.InvalidWorkerPhase;
                        pending.next_phase_sequence = try std.math.add(u64, sequence, 1);
                        pending.phase = .{ .sequence = sequence, .phase = phase };
                        pending.ready.set(self.io);
                    }
                },
                .phase_reply => {
                    if (length != 9) return error.InvalidWorkerPhase;
                    const sequence = std.mem.readInt(u64, bytes[0..8], .little);
                    const status = std.enums.fromInt(PhaseStatus, bytes[8]) orelse return error.InvalidWorkerPhase;
                    if (self.active.get(id)) |request| {
                        if (!request.phase_waiting or request.phase_reply != null or sequence != request.phase_sequence)
                            return error.InvalidWorkerPhase;
                        request.phase_reply = status;
                        request.phase_ready.set(self.io);
                    }
                },
                .event_cancel => {
                    if (length != 9) return error.InvalidWorkerEvent;
                    const sequence = std.mem.readInt(u64, bytes[0..8], .little);
                    const status = std.enums.fromInt(PhaseStatus, bytes[8]) orelse return error.InvalidWorkerEvent;
                    if (sequence == 0 or (status != .timeout and status != .canceled)) return error.InvalidWorkerEvent;
                    if (self.pending.get(id)) |pending| {
                        if (sequence != pending.next_event_sequence - 1 or pending.canceled_event_sequence.load(.acquire) == sequence) return error.InvalidWorkerEvent;
                        pending.canceled_event_status.store(status, .monotonic);
                        pending.canceled_event_sequence.store(sequence, .release);
                        pending.ready.set(self.io);
                    }
                },
                .event_reply => {
                    if (length != 9) return error.InvalidWorkerEvent;
                    const sequence = std.mem.readInt(u64, bytes[0..8], .little);
                    const status = std.enums.fromInt(PhaseStatus, bytes[8]) orelse return error.InvalidWorkerEvent;
                    if (self.active.get(id)) |request| {
                        if (!request.event_waiting or request.event_reply != null or sequence != request.event_sequence)
                            return error.InvalidWorkerEvent;
                        request.event_reply = status;
                        request.event_ready.set(self.io);
                    }
                },
                .resource_request => {
                    if (self.resource_handler == null or length == 0 or length > max_resource_bytes)
                        return error.InvalidResourceFrame;
                    for (self.resource_active) |entry| if (entry == id) return error.InvalidResourceFrame;
                    const slot = for (self.resource_active, 0..) |entry, i| {
                        if (entry == null) break i;
                    } else return error.ResourceCallbackCapacity;
                    const task = &self.resource_tasks[slot];
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
                    if ((length != 21 and length != 29) or self.incoming.contains(id)) return error.InvalidWorkerFrame;
                    const message_kind = std.enums.fromInt(MessageKind, bytes[0]) orelse return error.InvalidWorkerFrame;
                    if (length != (if (message_kind == .acknowledged_event) @as(u32, 29) else 21)) return error.InvalidWorkerFrame;
                    const event_sequence = if (message_kind == .acknowledged_event) std.mem.readInt(u64, bytes[21..29], .little) else 0;
                    if (message_kind == .acknowledged_event and event_sequence == 0) return error.InvalidWorkerEvent;
                    const request_id = std.mem.readInt(u64, bytes[1..9], .little);
                    const metadata_len = std.mem.readInt(u32, bytes[9..13], .little);
                    const body_len = std.mem.readInt(u64, bytes[13..21], .little);
                    if (request_id == 0 or metadata_len > max_metadata_bytes or body_len > max_body_bytes)
                        return error.InvalidWorkerFrame;
                    if (message_kind == .request and (request_id % 2 == self.next_id % 2 or self.active.contains(request_id)))
                        return error.InvalidWorkerFrame;
                    if ((self.report_retirement and self.retirement_inflight >= max_peer_payloads) or self.incoming.count() >= max_requests or
                        (message_kind == .request and self.active.count() >= max_requests) or
                        (message_kind != .request and (!self.pending.contains(request_id) or
                            self.pending.get(request_id).?.messages.items.len + self.incoming.count() >= max_queued_messages)))
                    {
                        try self.replyOffer(.reject, id);
                        continue;
                    }
                    self.incoming.ensureUnusedCapacity(self.metadataAllocator(), 1) catch {
                        try self.replyOffer(.reject, id);
                        continue;
                    };
                    var payload = self.allocateLocked(metadata_len, @intCast(body_len)) catch {
                        try self.replyOffer(.reject, id);
                        continue;
                    };
                    if (self.report_retirement) {
                        self.retirement_inflight += 1;
                        payload.retirement_id = id;
                    }
                    self.incoming.putAssumeCapacity(id, .{ .kind = message_kind, .request_id = request_id, .payload = payload, .event_sequence = event_sequence });
                    try self.replyOffer(.accept, id);
                },
                .accept, .reject => {
                    if (length != 0) return error.InvalidWorkerFrame;
                    if (kind == .reject and self.peer_payload_budget != null and !self.releasePeerLocked(id)) return error.InvalidWorkerRetirement;
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
                        const request = try self.metadataAllocator().create(Request);
                        errdefer self.metadataAllocator().destroy(request);
                        request.* = .{ .endpoint = self, .id = assembly.request_id, .payload = assembly.payload };
                        if (self.active.contains(request.id)) return error.InvalidWorkerFrame;
                        try self.active.put(self.metadataAllocator(), request.id, request);
                        errdefer _ = self.active.remove(request.id);
                        try self.handlers.concurrent(self.io, handle, .{ self, request });
                        owned = false;
                    } else if (self.pending.get(assembly.request_id)) |pending| {
                        // Bound empty-event floods as well as payload bytes.
                        if (pending.messages.items.len >= max_queued_messages) return error.WorkerMessageCapacity;
                        if (assembly.kind == .acknowledged_event) {
                            if (assembly.event_sequence != pending.next_event_sequence) return error.InvalidWorkerEvent;
                            pending.next_event_sequence = try std.math.add(u64, assembly.event_sequence, 1);
                        }
                        try pending.messages.append(self.metadataAllocator(), .{ .kind = assembly.kind, .payload = assembly.payload, .event_sequence = assembly.event_sequence });
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
            self.metadataAllocator().destroy(request);
        }
        const response = self.handler(self.context, request) catch |err| {
            // Terminal failure never needs message credit or an offer slot.
            // Even a completely full recipient can finish this one request.
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable or err == error.QueueFull or err == error.OutOfMemory) .capacity else .failure, request.id, "") catch {};
            return;
        };
        defer response.deinit();
        self.sendMessage(.response, request.id, response.view(), .{ .ptr = request, .check = Request.check }, null, 0) catch |err| {
            // Terminal failure never needs message credit or an offer slot.
            // Even a completely full recipient can finish this one request.
            self.sendFrame(if (err == error.ResourceTemporarilyUnavailable or err == error.QueueFull or err == error.OutOfMemory) .capacity else .failure, request.id, "") catch {};
        };
    }
};

pub const TestPair = struct {
    parent: Endpoint,
    child: Endpoint,
    files: [4]std.Io.File,
    cancelled: std.atomic.Value(bool) = .init(false),
    handled: std.atomic.Value(usize) = .init(0),
    cancel_seen: std.Io.Event = .unset,
    phase_resumed: std.atomic.Value(bool) = .init(false),
    phase_denied: std.Io.Event = .unset,

    pub fn init(self: *TestPair) !void {
        try self.initUnstarted();
        try self.parent.start();
        try self.child.start();
    }
    pub fn initUnstarted(self: *TestPair) !void {
        if (!@import("builtin").is_test) @compileError("worker pipe fixture is test-only");
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
        if (std.mem.eql(u8, request.payload.view().body, "phase")) {
            request.transition(.resource_wait) catch |err| {
                self.phase_denied.set(std.testing.io);
                return err;
            };
            request.transition(.runnable) catch |err| {
                self.phase_denied.set(std.testing.io);
                return err;
            };
            self.phase_resumed.store(true, .release);
        }
        if (std.mem.startsWith(u8, request.payload.view().body, "controlled-")) {
            const Check = struct {
                pair: *TestPair,
                timeout: bool,
                fn check(raw_control: ?*anyopaque) !void {
                    const control: *@This() = @ptrCast(@alignCast(raw_control.?));
                    if (control.pair.cancelled.load(.acquire)) {
                        if (control.timeout) return error.Timeout;
                        return error.Cancelled;
                    }
                }
            };
            var control = Check{ .pair = self, .timeout = std.mem.eql(u8, request.payload.view().body, "controlled-timeout") };
            request.eventAcknowledgedControlled(.{ .body = "chunk" }, .{ .ptr = &control, .check_fn = Check.check }) catch |err| {
                self.phase_denied.set(std.testing.io);
                return err;
            };
            self.phase_resumed.store(true, .release);
        }
        if (std.mem.eql(u8, request.payload.view().body, "output")) {
            request.eventAcknowledged(.{ .body = "chunk" }) catch |err| {
                self.phase_denied.set(std.testing.io);
                return err;
            };
            self.phase_resumed.store(true, .release);
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

test "inference worker phase acknowledgments run on the owner thread under saturated payload budgets" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const Owner = struct {
        pair: *TestPair,
        thread: std.Thread.Id,
        transitions: usize = 0,
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqual(self.thread, std.Thread.getCurrentId());
            try std.testing.expect(!self.pair.phase_resumed.load(.acquire));
            try std.testing.expectEqual(if (self.transitions == 0) Phase.resource_wait else Phase.runnable, value);
            self.transitions += 1;
            const endpoint = &self.pair.parent;
            endpoint.mutex.lockUncancelable(endpoint.io);
            defer endpoint.mutex.unlock(endpoint.io);
            // Phase frames bypass both allocation counters. Restore them only
            // at reacquisition, before the final response can be offered.
            endpoint.control_bytes = if (value == .resource_wait) control_budget_bytes else 0;
            endpoint.retained_bytes = if (value == .resource_wait) endpoint.retained_limit else 0;
        }
    };
    var owner = Owner{ .pair = &pair, .thread = std.Thread.getCurrentId() };
    defer {
        pair.parent.control_bytes = 0;
        pair.parent.retained_bytes = 0;
    }
    const result = try pair.parent.call(.{ .body = "phase" }, .{ .ptr = &owner, .phase = Owner.phase });
    defer result.deinit();
    try std.testing.expectEqualStrings("phase", result.view().body);
    try std.testing.expectEqual(@as(usize, 2), owner.transitions);
    try std.testing.expect(pair.phase_resumed.load(.acquire));
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker rejected reacquisition preserves the owner's cause and prevents child computation" {
    const Owner = struct {
        failure: anyerror,
        transitions: usize = 0,
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.transitions += 1;
            if (value == .runnable) return self.failure;
        }
    };
    for ([_]anyerror{ error.Forbidden, error.Canceled, error.DeadlineExceeded }) |failure| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var owner = Owner{ .failure = failure };
        try std.testing.expectError(failure, pair.parent.call(.{ .body = "phase" }, .{ .ptr = &owner, .phase = Owner.phase }));
        try pair.phase_denied.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expectEqual(@as(usize, 2), owner.transitions);
        try std.testing.expect(!pair.phase_resumed.load(.acquire));
        const result = try pair.parent.call(.{ .body = "echo" }, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("echo", result.view().body);
    }
}

test "inference worker missing phase owner rejects before delegated computation" {
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    try std.testing.expectError(error.UnsupportedWorkerPhase, pair.parent.call(.{ .body = "phase" }, .{}));
    try pair.phase_denied.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(!pair.phase_resumed.load(.acquire));
}

test "inference worker borrowed cancellation and lost acknowledgments wait for the reap fence" {
    const Probe = struct {
        pair: *TestPair,
        lose_ack: bool,
        reaping: std.Io.Event = .unset,
        release_reaper: std.Io.Event = .unset,
        owner_held: std.atomic.Value(bool) = .init(true),
        finished: std.atomic.Value(bool) = .init(false),
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reaping.set(std.testing.io);
            self.release_reaper.waitUncancelable(std.testing.io);
            // Models the real worker owner's kill/reap before returning from
            // on_closed. No original caller capacity can be retired sooner.
            self.pair.child.fail();
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.pair.cancelled.load(.acquire)) return error.Cancelled;
        }
        fn event(raw: ?*anyopaque, _: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.pair.cancelled.store(true, .release);
        }
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            // Reacquisition succeeds but transport closes before its ack can
            // be delivered. The child must not compute on an uncertain grant.
            if (value == .runnable) self.pair.parent.fail();
        }
        fn call(self: *@This()) !void {
            try std.testing.expectError(if (self.lose_ack) error.InferenceWorkerUnavailable else error.Cancelled, self.pair.parent.call(
                .{ .body = if (self.lose_ack) "phase" else "cancel" },
                .{ .ptr = self, .check = check, .event = event, .phase = phase, .fence_on_error = true },
            ));
            self.owner_held.store(false, .release);
            self.finished.store(true, .release);
        }
    };
    for ([_]bool{ false, true }) |lose_ack| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var probe = Probe{ .pair = &pair, .lose_ack = lose_ack };
        pair.parent.mutex.lockUncancelable(pair.parent.io);
        pair.parent.context = &probe;
        pair.parent.on_closed = Probe.closed;
        pair.parent.mutex.unlock(pair.parent.io);
        var call = try std.testing.io.concurrent(Probe.call, .{&probe});
        defer _ = call.cancel(std.testing.io) catch {};
        defer probe.release_reaper.set(std.testing.io);
        try probe.reaping.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expect(probe.owner_held.load(.acquire));
        try std.testing.expect(!probe.finished.load(.acquire));
        try std.testing.expect(!pair.parent.closed_fence.isSet());
        if (lose_ack) try std.testing.expect(!pair.phase_resumed.load(.acquire));
        probe.release_reaper.set(std.testing.io);
        try call.await(std.testing.io);
        try std.testing.expect(pair.parent.closed_fence.isSet());
        try std.testing.expect(!probe.owner_held.load(.acquire));
        try std.testing.expect(probe.finished.load(.acquire));
    }
}

test "inference worker rejected offers retain a healthy borrowed-capacity transport" {
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    const peer_budget = try PayloadBudget.create(std.testing.allocator, .{});
    defer peer_budget.releaseOwner();
    pair.parent.peer_payload_budget = peer_budget;
    pair.child.report_retirement = true;
    defer {
        pair.parent.fail();
        pair.child.deinit();
        pair.parent.releasePeerAfterPhysicalJoin();
        pair.parent.deinit();
        for (pair.files) |file| file.close(std.testing.io);
    }
    try pair.parent.start();
    try pair.child.start();
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.control_bytes = control_budget_bytes;
    pair.child.mutex.unlock(pair.child.io);
    defer pair.child.control_bytes = 0;
    try std.testing.expectError(error.QueueFull, pair.parent.call(.{ .body = "echo" }, .{ .fence_on_error = true }));
    try std.testing.expectEqual(@as(usize, 0), peer_budget.used().retained);
    try std.testing.expectEqual(@as(usize, 0), peer_budget.used().control);
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pair.handled.load(.acquire));
    pair.child.mutex.lockUncancelable(pair.child.io);
    pair.child.control_bytes = 0;
    pair.child.mutex.unlock(pair.child.io);
    const response = try pair.parent.call(.{ .body = "echo" }, .{ .fence_on_error = true });
    defer response.deinit();
    try std.testing.expectEqualStrings("echo", response.view().body);
}

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
    try pair.initUnstarted();
    const peer_budget = try PayloadBudget.create(std.testing.allocator, .{});
    defer peer_budget.releaseOwner();
    pair.parent.peer_payload_budget = peer_budget;
    pair.child.report_retirement = true;
    defer {
        pair.parent.fail();
        pair.child.deinit();
        pair.parent.releasePeerAfterPhysicalJoin();
        pair.parent.deinit();
        for (pair.files) |file| file.close(std.testing.io);
    }
    try pair.parent.start();
    try pair.child.start();
    const body = try std.testing.allocator.alloc(u8, 8 * max_frame_bytes);
    defer std.testing.allocator.free(body);
    @memset(body, 0x82);
    var probe: Probe = .{ .pair = &pair };
    try std.testing.expectError(error.Cancelled, pair.parent.call(.{ .body = body }, .{ .ptr = &probe, .check = Probe.check }));
    try std.testing.expect(probe.nested_completed);
    // The subsequent request is ordered after abort on the same pipe.
    const response = try pair.parent.call(.{ .body = "echo" }, .{});
    defer response.deinit();
    const deadline = std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() + 3 * std.time.ns_per_s;
    while (peer_budget.used().retained != 0 or peer_budget.used().control != 0) {
        if (std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() >= deadline) return error.TestTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
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

test "inference worker output acknowledgment blocks child compute through sink delivery and reacquisition" {
    const Sink = struct {
        pair: *TestPair,
        thread: std.Thread.Id,
        parked: bool = false,
        writes: usize = 0,
        transitions: usize = 0,
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqual(self.thread, std.Thread.getCurrentId());
            try std.testing.expect(!self.pair.phase_resumed.load(.acquire));
            try std.testing.expectEqual(if (self.parked) Phase.runnable else Phase.resource_wait, value);
            self.parked = !self.parked;
            self.transitions += 1;
        }
        fn event(raw: ?*anyopaque, payload: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            try std.testing.expectEqualStrings("chunk", payload.body);
            // A second request can traverse the actual same pipes while the
            // first child is held at its output boundary.
            const peer = try self.pair.parent.call(.{ .body = "echo" }, .{});
            defer peer.deinit();
            try std.testing.expectEqualStrings("echo", peer.view().body);
            for (0..15) |_| {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                try std.testing.expect(!self.pair.phase_resumed.load(.acquire));
            }
            self.writes += 1;
        }
    };
    var pair: TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    var sink = Sink{ .pair = &pair, .thread = std.Thread.getCurrentId() };
    const result = try pair.parent.call(.{ .body = "output" }, .{ .ptr = &sink, .event = Sink.event, .phase = Sink.phase, .fence_on_error = true });
    defer result.deinit();
    try std.testing.expectEqualStrings("output", result.view().body);
    try std.testing.expectEqual(@as(usize, 1), sink.writes);
    try std.testing.expectEqual(@as(usize, 2), sink.transitions);
    try std.testing.expect(!sink.parked);
    try std.testing.expect(pair.phase_resumed.load(.acquire));
}

test "inference worker output failures preserve parent cause and fence child before capacity retirement" {
    const Mode = enum { sink_failure, canceled, timeout, reacquire_failure, lost_ack };
    const Sink = struct {
        pair: *TestPair,
        mode: Mode,
        parked: bool = false,
        writes: usize = 0,
        reaped: bool = false,
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (value == .resource_wait) {
                self.parked = true;
            } else if (value == .runnable) {
                if (self.mode == .reacquire_failure) return error.Forbidden;
                self.parked = false;
            } else return error.InvalidWorkerPhase;
        }
        fn event(raw: ?*anyopaque, _: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.parked);
            try std.testing.expect(!self.pair.phase_resumed.load(.acquire));
            self.writes += 1;
            switch (self.mode) {
                .sink_failure => return error.InferenceTransportFailed,
                .canceled => return error.Canceled,
                .timeout => return error.DeadlineExceeded,
                .reacquire_failure => {},
                .lost_ack => self.pair.parent.fail(),
            }
        }
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pair.child.fail();
            // This fixture joins the actual handler at the mock process-reap
            // boundary. It does not claim to test an OS child process.
            self.pair.phase_denied.waitUncancelable(std.testing.io);
            self.reaped = true;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var sink = Sink{ .pair = &pair, .mode = mode };
        pair.parent.context = &sink;
        pair.parent.on_closed = Sink.closed;
        const expected: anyerror = switch (mode) {
            .sink_failure => error.InferenceTransportFailed,
            .canceled => error.Canceled,
            .timeout => error.DeadlineExceeded,
            .reacquire_failure => error.Forbidden,
            .lost_ack => error.InferenceWorkerUnavailable,
        };
        try std.testing.expectError(expected, pair.parent.call(.{ .body = "output" }, .{ .ptr = &sink, .event = Sink.event, .phase = Sink.phase, .fence_on_error = true }));
        try std.testing.expect(sink.reaped);
        try std.testing.expectEqual(@as(usize, 1), sink.writes);
        try std.testing.expect(!pair.phase_resumed.load(.acquire));
        // A failed sink/reentry leaves the loan parked until the caller's
        // existing cleanup, after the fence, retires or resumes it.
        // Endpoint closure now rejects the event lifetime before resumption,
        // including an otherwise successful sink whose acknowledgment is lost.
        try std.testing.expect(sink.parked);
    }
}

test "inference worker acknowledged output formatting retains execution while its sink owns physical waits" {
    const State = struct {
        parked: bool = false,
        checkpoints: usize = 0,
        fn phase(raw: ?*anyopaque, value: Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            switch (value) {
                .checkpoint => {
                    try std.testing.expect(!self.parked);
                    self.checkpoints += 1;
                },
                .resource_wait => {
                    try std.testing.expect(!self.parked);
                    self.parked = true;
                },
                .runnable => {
                    try std.testing.expect(self.parked);
                    self.parked = false;
                },
            }
        }
        fn event(raw: ?*anyopaque, _: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            // JSON/header formatting is CPU work and cannot begin parked.
            try std.testing.expect(!self.parked);
            try phase(raw, .resource_wait);
            try std.testing.expect(self.parked);
            try phase(raw, .runnable);
            try std.testing.expect(!self.parked);
        }
    };
    var state = State{};
    try Endpoint.deliverAcknowledgedEvent(.{ .ptr = &state, .event = State.event, .phase = State.phase, .event_owns_waits = true }, .{ .metadata = "{}", .body = "payload" });
    try std.testing.expect(!state.parked);
    try std.testing.expectEqual(@as(usize, 2), state.checkpoints);
}

test "inference worker controlled event cancellation reaches a blocked parent before child retirement" {
    const Owner = struct {
        pair: *TestPair,
        observed: bool = false,
        fn event(raw: ?*anyopaque, _: Payload, control: EventControl) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const endpoint = &self.pair.parent;
            endpoint.mutex.lockUncancelable(endpoint.io);
            const old_control = endpoint.control_bytes;
            const old_retained = endpoint.retained_bytes;
            endpoint.control_bytes = control_budget_bytes;
            endpoint.retained_bytes = endpoint.retained_limit;
            endpoint.mutex.unlock(endpoint.io);
            defer {
                endpoint.mutex.lockUncancelable(endpoint.io);
                endpoint.control_bytes = old_control;
                endpoint.retained_bytes = old_retained;
                endpoint.mutex.unlock(endpoint.io);
            }
            self.pair.cancelled.store(true, .release);
            // The reader must deliver the dedicated cancellation frame even
            // though neither ordinary payload allowance has any capacity.
            for (0..1000) |_| {
                control.check() catch |err| {
                    self.observed = true;
                    try std.testing.expect(!self.pair.phase_resumed.load(.acquire));
                    try std.testing.expect(!self.pair.phase_denied.isSet());
                    try std.testing.io.sleep(.fromMilliseconds(20), .awake);
                    // A canceled child still owns its callback borrow until
                    // this sink returns and its acknowledgment is delivered.
                    try std.testing.expect(!self.pair.phase_denied.isSet());
                    return err;
                };
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            return error.TestEventCancellationWasLost;
        }
    };
    for ([_][]const u8{ "controlled-timeout", "controlled-cancel" }) |body| {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        var owner = Owner{ .pair = &pair };
        const result = pair.parent.call(.{ .body = body }, .{ .ptr = &owner, .event_controlled = Owner.event });
        if (std.mem.eql(u8, body, "controlled-timeout")) try std.testing.expectError(error.Timeout, result) else try std.testing.expectError(error.Cancelled, result);
        try std.testing.expect(owner.observed);
        try pair.phase_denied.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        try std.testing.expect(!pair.phase_resumed.load(.acquire));
        const healthy = try pair.parent.call(.{ .body = "echo" }, .{});
        defer healthy.deinit();
        try std.testing.expectEqualStrings("echo", healthy.view().body);
        try std.testing.expect(!pair.parent.closed.load(.acquire));
    }
}

test "inference worker detached responses retain shared credit across actual endpoint generations" {
    const alloc = std.testing.allocator;
    const body = try alloc.alloc(u8, 2 * max_frame_bytes);
    defer alloc.free(body);
    @memset(body, 0xa3);
    const budget = try PayloadBudget.create(alloc, .{ .retained = body.len, .control = 32 });
    defer budget.releaseOwner();
    const old = blk: {
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        pair.parent.payload_budget = budget;
        const response = try pair.parent.call(.{ .body = body }, .{});
        break :blk response.detach();
    };
    var old_live = true;
    defer if (old_live) old.deinit();
    try std.testing.expectEqual(body.len, budget.used().retained);
    var next: TestPair = undefined;
    try next.init();
    defer next.deinit();
    next.parent.payload_budget = budget;
    // The old endpoint no longer exists. Its retained host result must still
    // deny this generation's receive allocation before additional bytes exist.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, next.parent.call(.{ .body = body }, .{}));
    try std.testing.expect(!next.parent.closed.load(.acquire));
    const small = try next.parent.call(.{ .body = "small" }, .{});
    try std.testing.expectEqualStrings("small", small.view().body);
    small.deinit();
    try std.testing.expectEqual(body.len, budget.used().retained);
    try std.testing.expectEqual(@as(usize, 0), budget.used().control);
    try std.testing.expectEqualSlices(u8, body, old.view().body);
    old.deinit();
    old_live = false;
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    const resumed = try next.parent.call(.{ .body = body }, .{});
    try std.testing.expectEqualSlices(u8, body, resumed.view().body);
    resumed.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker detached payload owns its budget beyond client anchor retirement" {
    const response = blk: {
        const budget = try PayloadBudget.create(std.testing.allocator, .{});
        defer budget.releaseOwner();
        var pair: TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        pair.parent.payload_budget = budget;
        const result = try pair.parent.call(.{ .body = "owned after teardown" }, .{});
        break :blk result.detach();
    };
    defer response.deinit();
    try std.testing.expectEqualStrings("owned after teardown", response.view().body);
}

test "inference worker payload allocation refusal rolls back shared and endpoint credit" {
    const budget = try PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var endpoint: Endpoint = .{
        .alloc = failing.allocator(),
        .io = std.testing.io,
        .input = undefined,
        .output = undefined,
        .context = undefined,
        .handler = undefined,
        .on_closed = undefined,
        .next_id = 1,
        .payload_budget = budget,
    };
    const body: [max_frame_bytes + 1]u8 = @splat(0xa4);
    try std.testing.expectError(error.OutOfMemory, endpoint.copyPayload(.{ .body = &body }));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqual(@as(usize, 0), endpoint.retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
    failing.fail_index = std.math.maxInt(usize);
    const retry = try endpoint.copyPayload(.{ .body = &body });
    retry.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker concurrent endpoint allocations obey one shared retained ceiling" {
    const alloc = std.testing.allocator;
    const body = try alloc.alloc(u8, 2 * max_frame_bytes);
    defer alloc.free(body);
    @memset(body, 0xa5);
    const budget = try PayloadBudget.create(alloc, .{ .retained = 2 * body.len });
    defer budget.releaseOwner();
    const Probe = struct {
        body: []const u8,
        budget: *PayloadBudget,
        started: std.atomic.Value(bool) = .init(false),
        finish: std.atomic.Value(bool) = .init(false),
        attempts: std.atomic.Value(usize) = .init(0),
        successes: std.atomic.Value(usize) = .init(0),
        unexpected: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            var endpoint: Endpoint = .{
                .alloc = std.testing.allocator,
                .io = std.testing.io,
                .input = undefined,
                .output = undefined,
                .context = undefined,
                .handler = undefined,
                .on_closed = undefined,
                .next_id = 1,
                .payload_budget = self.budget,
            };
            while (!self.started.load(.acquire)) std.atomic.spinLoopHint();
            const payload = endpoint.copyPayload(.{ .body = self.body }) catch |err| {
                if (err != error.ResourceTemporarilyUnavailable) self.unexpected.store(true, .release);
                _ = self.attempts.fetchAdd(1, .acq_rel);
                return;
            };
            _ = self.successes.fetchAdd(1, .acq_rel);
            _ = self.attempts.fetchAdd(1, .acq_rel);
            while (!self.finish.load(.acquire)) std.atomic.spinLoopHint();
            payload.deinit();
        }
    };
    var probe = Probe{ .body = body, .budget = budget };
    var threads: [8]std.Thread = undefined;
    var count: usize = 0;
    defer {
        probe.started.store(true, .release);
        probe.finish.store(true, .release);
        for (threads[0..count]) |thread| thread.join();
    }
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Probe.run, .{&probe});
        count += 1;
    }
    probe.started.store(true, .release);
    const deadline = std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() + 5 * std.time.ns_per_s;
    while (probe.attempts.load(.acquire) != threads.len) {
        if (std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() >= deadline) return error.TestTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!probe.unexpected.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), probe.successes.load(.acquire));
    try std.testing.expectEqual(2 * body.len, budget.used().retained);
    probe.finish.store(true, .release);
    for (threads[0..count]) |thread| thread.join();
    count = 0;
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
}

test "inference worker derived allocator holds bulk credit and preserves small-message progress" {
    const budget = try PayloadBudget.create(std.testing.allocator, .{ .retained = 16, .control = 1 });
    var owner_live = true;
    defer if (owner_live) budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.dupe(u8, "0123456789abcdef");
    var bytes_live = true;
    defer if (bytes_live) alloc.free(bytes);
    try std.testing.expectEqual(@as(usize, 16), budget.used().retained);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    var endpoint: Endpoint = .{
        .alloc = std.testing.allocator,
        .io = std.testing.io,
        .input = undefined,
        .output = undefined,
        .context = undefined,
        .handler = undefined,
        .on_closed = undefined,
        .next_id = 1,
        .payload_budget = budget,
    };
    const small = try endpoint.copyPayload(.{ .body = "x" });
    try std.testing.expectEqual(@as(usize, 1), budget.used().control);
    small.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().control);
    budget.releaseOwner();
    owner_live = false;
    try std.testing.expectEqualStrings("0123456789abcdef", bytes);
    alloc.free(bytes);
    bytes_live = false;
}

test "inference worker derived allocator rolls back backing refusal and bounds realloc peak" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const budget = try PayloadBudget.create(failing.allocator(), .{ .retained = 32 });
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 4));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
    failing.fail_index = std.math.maxInt(usize);
    const bytes = try alloc.dupe(u8, "0123456789abcdef");
    try std.testing.expectError(error.OutOfMemory, alloc.realloc(bytes, 32));
    try std.testing.expectEqualStrings("0123456789abcdef", bytes);
    try std.testing.expectEqual(@as(usize, 16), budget.used().retained);
    alloc.free(bytes);
    const retry = try alloc.alloc(u8, 32);
    alloc.free(retry);
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker peer grant survives response arrival until physical receive free" {
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    const body = try alloc.alloc(u8, 2 * max_frame_bytes);
    defer alloc.free(body);
    @memset(body, 0xa8);
    const budget = try PayloadBudget.create(alloc, .{ .retained = body.len });
    defer budget.releaseOwner();
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    const Probe = struct {
        target: ?[*]u8 = null,
        freeing: std.Io.Event = .unset,
        allow_free: std.Io.Event = .unset,
        fn allocate(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ret);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.target == memory.ptr) {
                self.freeing.set(io);
                self.allow_free.waitUncancelable(io);
            }
            std.testing.allocator.rawFree(memory, alignment, ret);
        }
        fn handle(raw: *anyopaque, request: *Request) !OwnedPayload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.target = request.payload.bytes.ptr;
            return request.endpoint.copyPayload(.{ .body = "done" });
        }
    };
    var probe: Probe = .{};
    pair.parent.peer_payload_budget = budget;
    pair.child.report_retirement = true;
    pair.child.context = &probe;
    pair.child.handler = Probe.handle;
    pair.child.alloc = .{ .ptr = &probe, .vtable = &.{ .alloc = Probe.allocate, .free = Probe.free, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap } };
    defer {
        probe.allow_free.set(io);
        pair.parent.fail();
        pair.child.deinit();
        pair.parent.releasePeerAfterPhysicalJoin();
        pair.parent.deinit();
        for (pair.files) |file| file.close(io);
    }
    try pair.parent.start();
    try pair.child.start();
    const Task = struct {
        response: ?OwnedPayload = null,
        failure: ?anyerror = null,
        fn run(self: *@This(), parent: *Endpoint, bytes: []const u8) void {
            self.response = parent.call(.{ .body = bytes }, .{}) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var task: Task = .{};
    var future = try io.concurrent(Task.run, .{ &task, &pair.parent, body });
    defer {
        probe.allow_free.set(io);
        future.cancel(io);
        if (task.response) |response| response.deinit();
    }
    future.await(io);
    if (task.failure) |err| return err;
    try std.testing.expectEqualStrings("done", task.response.?.view().body);
    try probe.freeing.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(body.len, budget.used().retained);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = body }, .{}));
    probe.allow_free.set(io);
    const deadline = std.Io.Timestamp.now(io, .awake).toNanoseconds() + 5 * std.time.ns_per_s;
    while (budget.used().retained != 0) {
        if (std.Io.Timestamp.now(io, .awake).toNanoseconds() >= deadline) return error.TestTimeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "inference worker zero-byte peer grants have fixed capacity and require explicit physical join" {
    const budget = try PayloadBudget.create(std.testing.allocator, .{ .retained = 0, .control = 0 });
    defer budget.releaseOwner();
    var endpoint: Endpoint = .{ .alloc = std.testing.allocator, .io = std.testing.io, .input = undefined, .output = undefined, .context = undefined, .handler = undefined, .on_closed = undefined, .next_id = 1, .peer_payload_budget = budget };
    defer endpoint.releasePeerAfterPhysicalJoin();
    for (0..max_peer_payloads) |i| try endpoint.reservePeerLocked(i + 1, 0);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, endpoint.reservePeerLocked(max_peer_payloads + 1, 0));
    endpoint.closed.store(true, .release);
    try std.testing.expectEqual(@as(usize, max_peer_payloads + 1), budget.references.load(.acquire));
    endpoint.releasePeerAfterPhysicalJoin();
    try std.testing.expectEqual(@as(usize, 1), budget.references.load(.acquire));
}

test "inference worker malformed retirement closes transport without refunding another grant" {
    const io = std.testing.io;
    const budget = try PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    pair.parent.peer_payload_budget = budget;
    try pair.parent.reservePeerLocked(77, 2 * max_frame_bytes);
    defer {
        pair.parent.fail();
        pair.child.deinit();
        pair.parent.releasePeerAfterPhysicalJoin();
        pair.parent.deinit();
        for (pair.files) |file| file.close(io);
    }
    try pair.parent.start();
    try pair.child.start();
    try pair.child.sendFrame(.retired, 999, "");
    try pair.parent.closed_fence.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, 2 * max_frame_bytes), budget.used().retained);
}

test "inference worker resource dispatch survives endpoint allocator exhaustion" {
    const Probe = struct {
        fn resource(_: *anyopaque, bytes: []const u8) !ResourceReply {
            try std.testing.expectEqualStrings("release", bytes);
            return .{ .value = 42 };
        }
    };
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    pair.child.alloc = failing.allocator();
    pair.child.resource_handler = Probe.resource;
    defer pair.deinit();
    try pair.parent.start();
    try pair.child.start();
    for (0..3) |_| {
        const result = try pair.parent.callResource("release");
        try std.testing.expectEqual(@as(u64, 42), result.value);
    }
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!pair.parent.closed.load(.acquire));
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker transport metadata quotas refuse before publication and preserve resource replies" {
    const Probe = struct {
        fn resource(_: *anyopaque, _: []const u8) !ResourceReply {
            return .{ .value = 7 };
        }
    };
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    const parent = try PayloadBudget.create(std.testing.allocator, .{ .retained = 0, .control = 0 });
    defer parent.releaseOwner();
    const child = try PayloadBudget.create(std.testing.allocator, .{ .retained = 0, .control = 0 });
    defer child.releaseOwner();
    pair.parent.metadata_allocator = parent.retainedAllocator();
    pair.child.metadata_allocator = child.retainedAllocator();
    pair.child.resource_handler = Probe.resource;
    {
        defer pair.deinit();
        try pair.parent.start();
        try pair.child.start();
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, pair.parent.call(.{ .body = "echo" }, .{}));
        try std.testing.expectEqual(@as(usize, 0), pair.handled.load(.acquire));
        try std.testing.expectEqual(@as(u64, 7), (try pair.parent.callResource("release")).value);
        parent.limits.retained = 1024 * 1024;
        // The receiver reserves its metadata before accepting or allocating a slab.
        try std.testing.expectError(error.QueueFull, pair.parent.call(.{ .body = "echo" }, .{}));
        try std.testing.expectEqual(@as(usize, 0), pair.handled.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), child.used().retained);
        try std.testing.expect(!pair.parent.closed.load(.acquire) and !pair.child.closed.load(.acquire));
        child.limits.retained = child_metadata_limit;
        const response = try pair.parent.call(.{ .body = "echo" }, .{});
        defer response.deinit();
        try std.testing.expectEqualStrings("echo", response.view().body);
        try std.testing.expect(parent.used().retained > 0);
    }
    try std.testing.expectEqual(@as(usize, 0), parent.used().retained);
    try std.testing.expectEqual(@as(usize, 0), child.used().retained);
    try std.testing.expectEqual(@as(usize, 1), parent.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), child.references.load(.acquire));
}

test "inference worker empty event queues charge metadata and preserve independent resource progress" {
    const io = std.testing.io;
    const Probe = struct {
        fn resource(_: *anyopaque, _: []const u8) !ResourceReply {
            return .{ .value = 9 };
        }
    };
    var pair: TestPair = undefined;
    try pair.initUnstarted();
    const metadata = try PayloadBudget.create(std.testing.allocator, .{ .retained = 1024 * 1024, .control = 0 });
    defer metadata.releaseOwner();
    pair.parent.metadata_allocator = metadata.retainedAllocator();
    pair.parent.resource_handler = Probe.resource;
    var pending: Pending = .{};
    try pair.parent.pending.put(metadata.retainedAllocator(), 1, &pending);
    defer {
        pair.parent.mutex.lockUncancelable(io);
        _ = pair.parent.pending.remove(1);
        for (pending.messages.items) |message| message.payload.deinitLocked();
        pending.messages.deinit(metadata.retainedAllocator());
        pair.parent.mutex.unlock(io);
        pair.deinit();
    }
    try pair.parent.start();
    try pair.child.start();
    for (0..max_queued_messages) |_| try pair.child.sendMessage(.event, 1, .{}, .{}, null, 0);
    try std.testing.expectError(error.QueueFull, pair.child.sendMessage(.event, 1, .{}, .{}, null, 0));
    pair.parent.mutex.lockUncancelable(io);
    const queued = pending.messages.items.len;
    const ordinary_bytes = pair.parent.control_bytes + pair.parent.retained_bytes;
    pair.parent.mutex.unlock(io);
    try std.testing.expectEqual(max_queued_messages, queued);
    try std.testing.expectEqual(@as(usize, 0), ordinary_bytes);
    try std.testing.expect(metadata.used().retained > 0);
    try std.testing.expectEqual(@as(u64, 9), (try pair.child.callResource("release")).value);
    try std.testing.expect(!pair.parent.closed.load(.acquire) and !pair.child.closed.load(.acquire));
}

test "inference worker sink acknowledgment preserves timeout and denial across request cancellation" {
    const io = std.testing.io;
    const Probe = struct {
        pair: *TestPair,
        timeout: ?bool,
        completed: std.Io.Event = .unset,
        outcome: std.atomic.Value(u8) = .init(0),
        fn handle(raw: *anyopaque, request: *Request) !OwnedPayload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            defer self.completed.set(std.testing.io);
            request.eventAcknowledged(.{ .body = "blocked sink" }) catch |err| {
                self.outcome.store(if (err == error.Timeout) 1 else if (err == error.InferenceExecutionHandoffFailed) 2 else 3, .release);
                return err;
            };
            self.outcome.store(4, .release);
            return request.endpoint.copyPayload(.{ .body = "must not resume" });
        }
        fn event(raw: ?*anyopaque, _: Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            // Force the same ordering as caller cleanup racing the acknowledged
            // sink error: the child sees request cancellation before its reply.
            self.pair.child.mutex.lockUncancelable(std.testing.io);
            var active = self.pair.child.active.valueIterator();
            const request = active.next().?.*;
            request.cancelled.store(true, .release);
            self.pair.child.mutex.unlock(std.testing.io);
            if (self.timeout) |timeout| return if (timeout) error.Timeout else error.TestSinkDenied;
        }
    };
    for ([_]?bool{ true, false, null }) |timeout| {
        var pair: TestPair = undefined;
        try pair.initUnstarted();
        var probe = Probe{ .pair = &pair, .timeout = timeout };
        pair.child.context = &probe;
        pair.child.handler = Probe.handle;
        defer pair.deinit();
        try pair.parent.start();
        try pair.child.start();
        if (timeout) |value| {
            try std.testing.expectError(if (value) error.Timeout else error.TestSinkDenied, pair.parent.call(.{ .body = "output" }, .{ .ptr = &probe, .event = Probe.event }));
        } else {
            // The parent's final event check may observe cancellation before
            // acknowledging success; otherwise the child rejects that success.
            if (pair.parent.call(.{ .body = "output" }, .{ .ptr = &probe, .event = Probe.event })) |response| {
                response.deinit();
                return error.TestCancelledChildResumed;
            } else |err| if (err != error.Cancelled and err != error.InferenceWorkerRequestFailed) return err;
        }
        try probe.completed.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
        try std.testing.expectEqual(@as(u8, if (timeout) |value| (if (value) 1 else 2) else 3), probe.outcome.load(.acquire));
    }
}
