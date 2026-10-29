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

//! One joined lease-renewal worker, funded by the original maintenance root.
//! Watchdogs inspect its published authority; only this worker performs renewal.
const std = @import("std");
const maintenance = @import("../maintenance_cancellation.zig");
const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const platform = @import("antfly_platform");
const Rejection = @import("antfly_cancellation").Rejection;

/// Intrusive registration owned by a joined source scope. Detach removes it
/// before waiting for the one in-flight callback, so no callback outlives it.
pub const Registration = struct {
    owner: ?*Renewal = null,
    next: ?*Registration = null,
    ptr: *anyopaque = undefined,
    callback: *const fn (*anyopaque, Context) anyerror!void = undefined,
    generation: u64 = 0,
    busy: std.atomic.Value(bool) = .init(false),
    idle: std.Io.Event = .is_set,
    refusal: Rejection = .{},

    pub fn check(self: *const Registration) !void {
        try self.refusal.check();
    }
    pub fn stop(self: *Registration) void {
        const owner = self.owner orelse return;
        platform.sync.lockYielding(&owner.registry_mutex);
        var link = &owner.registrations;
        while (link.* != self) link = &link.*.?.next;
        link.* = self.next;
        owner.registration_count -= 1;
        const busy = self.busy.load(.acquire);
        self.owner = null;
        owner.registry_mutex.unlock();
        // Cleanup may run after refusal. Park an extant grant before joining;
        // failed reentry remains terminal and never manufactures new authority.
        var restore: ?Runnable.Class = null;
        if (busy) {
            if (owner.parent.execution) |execution| {
                const class = execution.retainedClass() catch null;
                if (class) |value| {
                    if (execution.park(.resource_blocked)) |_| {
                        restore = value;
                    } else |_| {}
                }
            }
            self.idle.waitUncancelable(owner.parent.io);
            if (restore) |class| owner.parent.execution.?.enter(class) catch {};
        }
    }
};

pub const Renewal = struct {
    parent: maintenance.Token = undefined,
    clock_io: std.Io = undefined,
    execution: Runnable.Execution = .{},
    callback_ptr: *anyopaque = undefined,
    callback: *const fn (*anyopaque, Context) anyerror!u64 = undefined,
    ttl_ns: std.atomic.Value(u64) = .init(0),
    mutex: std.atomic.Mutex = .unlocked,
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    stopping: std.atomic.Value(bool) = .init(false),
    refusal: Rejection = .{},
    ready: std.Io.Event = .unset,
    activate: std.Io.Event = .unset,
    future: ?std.Io.Future(void) = null,
    registry_mutex: std.atomic.Mutex = .unlocked,
    registrations: ?*Registration = null,
    registration_count: usize = 0,
    registration_generation: u64 = 0,

    pub fn attach(self: *Renewal, registration: *Registration, ptr: *anyopaque, callback: *const fn (*anyopaque, Context) anyerror!void) !void {
        try self.check();
        platform.sync.lockYielding(&self.registry_mutex);
        defer self.registry_mutex.unlock();
        if (registration.owner != null or self.registration_count >= 64) return error.SchedulerFull;
        registration.ptr = ptr;
        registration.callback = callback;
        registration.next = self.registrations;
        registration.generation = self.registration_generation;
        registration.owner = self;
        self.registrations = registration;
        self.registration_count += 1;
    }
    fn runRegistrations(self: *Renewal, call_context: Context) void {
        platform.sync.lockYielding(&self.registry_mutex);
        self.registration_generation +%= 1;
        const generation = self.registration_generation;
        self.registry_mutex.unlock();
        while (true) {
            platform.sync.lockYielding(&self.registry_mutex);
            var cursor = self.registrations;
            while (cursor) |item| : (cursor = item.next) {
                if (item.generation != generation) break;
            }
            const item = cursor orelse {
                self.registry_mutex.unlock();
                return;
            };
            item.generation = generation;
            item.idle.reset();
            item.busy.store(true, .release);
            self.registry_mutex.unlock();
            if (item.check()) |_| {
                item.callback(item.ptr, call_context) catch |err| {
                    _ = @intFromError(item.refusal.remember(err));
                };
            } else |_| {}
            // Detach observes busy under this mutex. Publish the event before
            // unlocking, then never touch the borrowed registration again.
            platform.sync.lockYielding(&self.registry_mutex);
            item.busy.store(false, .release);
            item.idle.set(self.parent.io);
            self.registry_mutex.unlock();
        }
    }
    pub fn active(self: *const Renewal) bool {
        return self.future != null;
    }
    pub fn check(self: *const Renewal) !void {
        try self.refusal.check();
        try self.parent.checkLifetime();
        if (self.parent.execution) |parent| try parent.checkLifetime();
        if (self.stopping.load(.acquire)) return error.Canceled;
        const unix = self.unixNow();
        const authority = platform.time.authorityNs();
        if (unix == 0 or authority == 0 or unix >= self.unix_deadline.load(.acquire) or authority >= self.authority_deadline.load(.acquire)) return error.WorkLeaseLost;
    }
    fn fence(raw: *anyopaque) !void {
        try @as(*Renewal, @ptrCast(@alignCast(raw))).check();
    }
    fn canceled(raw: *const anyopaque) bool {
        @as(*const Renewal, @ptrCast(@alignCast(raw))).check() catch return true;
        return false;
    }
    fn unixNow(self: *const Renewal) u64 {
        return @intCast(@max(0, std.Io.Timestamp.now(self.clock_io, .real).toNanoseconds()));
    }
    pub fn publish(self: *Renewal, expiry: u64) void {
        const unix = self.unixNow();
        const authority = platform.time.authorityNs();
        _ = self.unix_deadline.fetchMax(expiry, .acq_rel);
        _ = self.authority_deadline.fetchMax(authority +| @min(self.ttl_ns.load(.acquire), expiry -| unix), .acq_rel);
    }
    pub fn context(self: *Renewal, execution: ?*Runnable.Execution) Context {
        var result: Context = .{ .io = self.parent.io, .deadline_ns = self.parent.deadline_ns, .cancellation = .{ .ptr = self, .is_cancelled_fn = canceled } };
        if (execution) |owner| result = result.withExecution(owner);
        return result;
    }
    pub fn start(self: *Renewal, parent: maintenance.Token, ptr: *anyopaque, callback: *const fn (*anyopaque, Context) anyerror!u64, ttl: u64, expiry: u64, clock_io: std.Io) !void {
        if (self.active() or ttl == 0) return error.InvalidLeaseTtl;
        self.parent = parent;
        self.clock_io = clock_io;
        self.callback_ptr = ptr;
        self.callback = callback;
        self.ttl_ns.store(ttl, .release);
        self.publish(expiry);
        try self.check();
        if (parent.execution) |owner| if (owner.entry != null or owner.borrowed != null) {
            try self.execution.reserveControlChild(owner, parent.io, @sizeOf(Renewal));
        };
        errdefer self.execution.finish();
        self.future = try parent.io.concurrent(run, .{self});
        errdefer self.stop();
        // Let the reserved child activate before restoring the parent team.
        // It parks before publishing readiness, so startup cannot deadlock on
        // a parent that occupies all ordinary CPU capacity.
        if (parent.execution) |owner| {
            if (owner.entry == null and owner.borrowed == null) {
                self.activate.set(parent.io);
                try self.ready.wait(parent.io);
                try self.check();
                return;
            }
            const class = try owner.retainedClass();
            try owner.park(.resource_blocked);
            self.activate.set(parent.io);
            const result = self.ready.wait(parent.io);
            try owner.enterWithCheck(class, .{ .context = self, .check = fence });
            try result;
        } else {
            self.activate.set(parent.io);
            try self.ready.wait(parent.io);
        }
        try self.check();
    }
    fn run(self: *Renewal) void {
        defer self.execution.finish();
        defer self.ready.set(self.parent.io);
        self.runOwned() catch |err| {
            if (!self.stopping.load(.acquire)) {
                const cause = blk: {
                    self.check() catch |original| break :blk original;
                    break :blk err;
                };
                _ = @intFromError(self.refusal.remember(cause));
            }
        };
    }
    fn runOwned(self: *Renewal) !void {
        try self.activate.wait(self.parent.io);
        const scheduled = self.execution.entry != null or self.execution.borrowed != null;
        if (scheduled) {
            try self.execution.activateChildWithCheck(.{ .context = self, .check = fence });
            try self.execution.park(.resource_blocked);
        }
        self.ready.set(self.parent.io);
        while (!self.stopping.load(.acquire)) {
            const interval = @max(@as(u64, 1), @min(self.ttl_ns.load(.acquire) / 4, std.time.ns_per_s));
            try self.parent.io.sleep(.fromNanoseconds(@intCast(interval)), .awake);
            try self.check();
            if (scheduled) try self.execution.enterWithCheck(.control, .{ .context = self, .check = fence });
            const call_context = self.context(if (scheduled) &self.execution else null);
            _ = try self.renew(call_context);
            self.runRegistrations(call_context);
            if (scheduled) try self.execution.park(.resource_blocked);
        }
    }
    pub fn renew(self: *Renewal, call_context: Context) !u64 {
        try self.check();
        const operation: @import("../provider_operation.zig").Operation = .{ .context = call_context };
        try operation.ready();
        // Every contender lends its own capacity while waiting. Watchdog
        // checks never take this mutex or enter the provider.
        while (!self.mutex.tryLock()) try operation.sleep(1);
        defer self.mutex.unlock();
        try operation.check();
        const expiry = self.callback(self.callback_ptr, call_context) catch |err| {
            try self.check();
            return err;
        };
        operation.check() catch |err| {
            try self.check();
            return err;
        };
        self.publish(expiry);
        return expiry;
    }
    pub fn stop(self: *Renewal) void {
        self.stopping.store(true, .release);
        if (self.future) |*future| future.cancel(self.parent.io);
        self.future = null;
    }
};
