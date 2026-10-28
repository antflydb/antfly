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

//! Bounded node-owned obligations, reserved before remote publication. The
//! publisher supplies an owned parent pin/charge, never a request-stack Port.
//! One node consumer polls idempotent retirement; no worker is created per read.
const std = @import("std");
const admission = @import("read_admission.zig");
const callbacks = @import("../runtime_callback_abi.zig");
const PinCallbacks = struct {
    retain: *const fn (*anyopaque) anyerror!void,
};
const PinBoundary = callbacks.Boundary(PinCallbacks);
const ParentCallbacks = struct {
    retain: *const fn (*anyopaque, usize) anyerror!void,
};
const ParentBoundary = callbacks.Boundary(ParentCallbacks);
const ResourceCallbacks = struct {
    poll: *const fn (*anyopaque, Poll) anyerror!Outcome,
    reconcile: *const fn (*anyopaque, Poll) anyerror!Outcome,
};
const ResourceBoundary = callbacks.Boundary(ResourceCallbacks);

pub const Pin = struct {
    boundary_dispatch: PinBoundary.Dispatch = PinBoundary.local_dispatch,
    ptr: *anyopaque,
    retain: *const fn (*anyopaque) anyerror!void,
    release: *const fn (*anyopaque) void,
    pub fn acquire(self: Pin) !void {
        try PinBoundary.call("retain", self.boundary_dispatch, self.retain, .{self.ptr});
    }
    pub fn retire(self: Pin) void {
        self.release(self.ptr);
    }
};
pub const Parent = struct {
    boundary_dispatch: ParentBoundary.Dispatch = ParentBoundary.local_dispatch,
    ptr: *anyopaque,
    /// Charge bytes and pin the complete execution/allocator/transport parent
    /// chain atomically. Failure must roll back all partial charges and pins.
    retain: *const fn (*anyopaque, usize) anyerror!void,
    /// Last access to the parent, after resource destruction and before the
    /// charged ledger slot is released.
    release: *const fn (*anyopaque, usize) void,
    /// Optional node-host lifetime, copied with the full parent. Outbox refs
    /// acquire it too, even when the originating registry ticket is gone.
    pin: ?Pin = null,
    pub fn acquire(self: Parent, bytes: usize) !void {
        if (self.pin) |node| try node.acquire();
        errdefer if (self.pin) |node| node.retire();
        try ParentBoundary.call("retain", self.boundary_dispatch, self.retain, .{ self.ptr, bytes });
    }
    pub fn retire(self: Parent, bytes: usize) void {
        self.release(self.ptr, bytes);
        if (self.pin) |node| node.retire();
    }
};
pub const Outcome = enum { retired, pending, needs_fence };
pub const Poll = struct {
    allocator: std.mem.Allocator,
    control: ?admission.Port = null,
    /// Borrowed only through this synchronous checked resource callback.
    control_permit: ?@import("http/http_common.zig").BorrowedControlPermit = null,
};
pub const Resource = struct {
    boundary_dispatch: ResourceBoundary.Dispatch = ResourceBoundary.local_dispatch,
    ptr: *anyopaque,
    /// May repeat only exact idempotent close. Unknown creation/process changes
    /// return needs_fence; they cannot be retired by time or connection cancel.
    poll: *const fn (*anyopaque, Poll) anyerror!Outcome,
    destroy: *const fn (*anyopaque) void,
    /// Bounded metadata observation of an authenticated creation response. It
    /// grants no query authority and must allocate nothing from the request.
    observe: ?*const fn (*anyopaque, []const u8) void = null,
    /// Explicit recovery may re-observe the original process or validate a
    /// signed attempt/generation fence. No automatic success fallback exists.
    reconcile: ?*const fn (*anyopaque, Poll) anyerror!Outcome = null,
};
pub const Binding = struct { registry: *Registry, parent: Parent };

pub const Registry = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,
    byte_limit: usize,
    max_item_bytes: usize,
    mutex: std.atomic.Mutex = .unlocked,
    used: usize = 0,
    bytes: usize = 0,
    sequence: u64 = 0,
    cursor: usize = 0,
    closed: bool = false,

    pub const slot_bytes = @sizeOf(Slot);
    pub fn metadataBytes(capacity: usize) !usize {
        if (capacity == 0 or capacity > 4096) return error.InvalidArgument;
        return std.math.mul(usize, capacity, slot_bytes);
    }

    const State = enum { free, constructing, foreground, queued, polling, needs_fence, retiring };
    const Slot = struct {
        state: State = .free,
        sequence: u64 = 0,
        parent: Parent = undefined,
        bytes: usize = 0,
        resource: ?Resource = null,
        published: bool = false,
    };
    pub const Ticket = struct {
        owner: *Registry,
        index: usize,
        sequence: u64,
        pub fn allocator(self: Ticket) std.mem.Allocator {
            return self.owner.allocator;
        }
        fn slot(self: Ticket) !*Slot {
            if (self.index >= self.owner.slots.len) return error.InvalidRunnableTransition;
            const value = &self.owner.slots[self.index];
            if (value.state == .free or value.sequence != self.sequence) return error.InvalidRunnableTransition;
            return value;
        }
        pub fn bind(self: Ticket, resource: Resource) !void {
            self.owner.lock();
            defer self.owner.mutex.unlock();
            const value = try self.slot();
            if (self.owner.closed) return error.CleanupCapacityUnavailable;
            if (value.state != .foreground or value.resource != null or value.published) return error.InvalidRunnableTransition;
            value.resource = resource;
        }
        pub fn publish(self: Ticket) !void {
            self.owner.lock();
            defer self.owner.mutex.unlock();
            const value = try self.slot();
            if (self.owner.closed) return error.CleanupCapacityUnavailable;
            if (value.state != .foreground or value.resource == null or value.published) return error.InvalidRunnableTransition;
            value.published = true;
        }
        /// Only caller-side setup rollback, before any executor invocation.
        pub fn abort(self: Ticket) !void {
            self.owner.lock();
            const value = self.slot() catch |err| {
                self.owner.mutex.unlock();
                return err;
            };
            if (value.state != .foreground or value.published) {
                self.owner.mutex.unlock();
                return error.InvalidRunnableTransition;
            }
            value.state = .retiring;
            self.owner.mutex.unlock();
            self.owner.releaseSlot(self.index);
        }
        pub fn observe(self: Ticket, bytes: []const u8) !void {
            self.owner.lock();
            const value = self.slot() catch |err| {
                self.owner.mutex.unlock();
                return err;
            };
            if (value.state != .foreground or !value.published) {
                self.owner.mutex.unlock();
                return error.InvalidRunnableTransition;
            }
            const resource = value.resource.?;
            self.owner.mutex.unlock();
            if (resource.observe) |callback| callback(resource.ptr, bytes);
        }
        pub fn abandon(self: Ticket) !void {
            self.owner.lock();
            const value = self.slot() catch |err| {
                self.owner.mutex.unlock();
                return err;
            };
            if (value.state != .foreground) {
                self.owner.mutex.unlock();
                return error.InvalidRunnableTransition;
            }
            const published = value.published;
            value.state = if (published) .queued else .retiring;
            self.owner.mutex.unlock();
            if (!published) self.owner.releaseSlot(self.index);
        }
        /// Only a checked creation response can prove that no capability was
        /// created. No timeout, transport error or connection cancel may use this.
        pub fn acknowledgeNotCreated(self: Ticket) !void {
            self.owner.lock();
            const value = self.slot() catch |err| {
                self.owner.mutex.unlock();
                return err;
            };
            if (value.state != .foreground or !value.published) {
                self.owner.mutex.unlock();
                return error.InvalidRunnableTransition;
            }
            value.state = .retiring;
            self.owner.mutex.unlock();
            self.owner.releaseSlot(self.index);
        }
        /// Move physical ownership without allocating or borrowing foreground
        /// cancellation, deadlines, Io or admission callbacks.
        pub fn transfer(self: Ticket) !void {
            self.owner.lock();
            defer self.owner.mutex.unlock();
            const value = try self.slot();
            if (value.state != .foreground or value.resource == null) return error.InvalidRunnableTransition;
            value.state = .queued;
        }
    };

    /// The node's thread-safe charged allocator and runtime outlive all tickets.
    pub fn init(allocator: std.mem.Allocator, capacity: usize, byte_limit: usize, max_item_bytes: usize) !Registry {
        if (capacity == 0 or capacity > 4096 or max_item_bytes < @sizeOf(Slot) or max_item_bytes > byte_limit) return error.InvalidArgument;
        const slots = try allocator.alloc(Slot, capacity);
        @memset(slots, .{});
        return .{ .allocator = allocator, .slots = slots, .byte_limit = byte_limit, .max_item_bytes = max_item_bytes };
    }
    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn reserve(self: *Registry, parent: Parent, resource_bytes: usize) !Ticket {
        const bytes = std.math.add(usize, @sizeOf(Slot), resource_bytes) catch return error.CleanupCapacityUnavailable;
        self.lock();
        if (self.closed or bytes > self.max_item_bytes or bytes > self.byte_limit - self.bytes or self.used == self.slots.len) {
            self.mutex.unlock();
            return error.CleanupCapacityUnavailable;
        }
        const index = for (self.slots, 0..) |slot, i| {
            if (slot.state == .free) break i;
        } else unreachable;
        const sequence = std.math.add(u64, self.sequence, 1) catch {
            self.mutex.unlock();
            return error.CleanupCapacityUnavailable;
        };
        self.sequence = sequence;
        self.used += 1;
        self.bytes += bytes;
        self.slots[index] = .{ .state = .constructing, .sequence = sequence, .parent = parent, .bytes = bytes };
        self.mutex.unlock();
        parent.acquire(bytes) catch |err| {
            self.lock();
            self.slots[index] = .{};
            self.used -= 1;
            self.bytes -= bytes;
            self.mutex.unlock();
            return err;
        };
        self.lock();
        // An in-flight reservation stays charged through shutdown, but cannot
        // publish after it. Parent release must happen outside the mutex.
        if (self.closed) {
            self.slots[index].state = .retiring;
            self.mutex.unlock();
            self.releaseSlot(index);
            return error.CleanupCapacityUnavailable;
        }
        self.slots[index].state = .foreground;
        self.mutex.unlock();
        return .{ .owner = self, .index = index, .sequence = sequence };
    }
    pub const Progress = enum { idle, pending, retired, needs_fence };
    /// One bounded scan and at most one cleanup RPC. Cursor advances even on
    /// transport failure, so one unavailable peer cannot monopolize recovery.
    pub fn pollOne(self: *Registry, poll: Poll) Progress {
        return self.pollState(poll, false);
    }
    pub fn reconcileOne(self: *Registry, poll: Poll) Progress {
        return self.pollState(poll, true);
    }
    fn pollState(self: *Registry, poll: Poll, reconcile: bool) Progress {
        self.lock();
        const index = find: {
            for (0..self.slots.len) |_| {
                const current = self.cursor;
                self.cursor = (current + 1) % self.slots.len;
                if (self.slots[current].state == (if (reconcile) State.needs_fence else State.queued) and
                    (!reconcile or self.slots[current].resource.?.reconcile != null)) break :find current;
            }
            self.mutex.unlock();
            return .idle;
        };
        self.slots[index].state = .polling;
        const resource = self.slots[index].resource.?;
        self.mutex.unlock();
        const callback = if (reconcile) resource.reconcile.? else resource.poll;
        const result = if (reconcile)
            ResourceBoundary.call("reconcile", resource.boundary_dispatch, callback, .{ resource.ptr, poll })
        else
            ResourceBoundary.call("poll", resource.boundary_dispatch, callback, .{ resource.ptr, poll });
        const outcome = result catch (if (reconcile) Outcome.needs_fence else Outcome.pending);
        self.lock();
        self.slots[index].state = switch (outcome) {
            .retired => .retiring,
            .pending => if (reconcile) .needs_fence else .queued,
            .needs_fence => .needs_fence,
        };
        self.mutex.unlock();
        if (outcome == .retired) self.releaseSlot(index);
        return switch (outcome) {
            .retired => .retired,
            .pending => .pending,
            .needs_fence => .needs_fence,
        };
    }
    fn releaseSlot(self: *Registry, index: usize) void {
        self.lock();
        const value = self.slots[index];
        std.debug.assert(value.state == .retiring);
        self.mutex.unlock();
        if (value.resource) |resource| resource.destroy(resource.ptr);
        // Keep quota through parent release too. Neither callback may access
        // request-stack borrows; the node runtime itself remains independently pinned.
        value.parent.retire(value.bytes);
        self.lock();
        self.slots[index] = .{};
        self.used -= 1;
        self.bytes -= value.bytes;
        self.mutex.unlock();
    }
    pub const Snapshot = struct { retained: usize, bytes: usize, foreground: usize = 0, queued: usize = 0, needs_fence: usize = 0, in_flight: usize = 0, closed: bool };
    pub fn snapshot(self: *Registry) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var result: Snapshot = .{ .retained = self.used, .bytes = self.bytes, .closed = self.closed };
        for (self.slots) |slot| switch (slot.state) {
            .foreground => result.foreground += 1,
            .queued => result.queued += 1,
            .needs_fence => result.needs_fence += 1,
            .free => {},
            else => result.in_flight += 1,
        };
        return result;
    }
    pub fn close(self: *Registry) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
    }
    /// A lost peer is not retirement. Shutdown/restart must preserve the node
    /// owner and its obligations until signed recovery can establish a fence.
    pub fn deinit(self: *Registry) !void {
        self.lock();
        if (!self.closed or self.used != 0) {
            self.mutex.unlock();
            return error.CleanupPending;
        }
        self.mutex.unlock();
        self.allocator.free(self.slots);
        self.* = undefined;
    }
};
