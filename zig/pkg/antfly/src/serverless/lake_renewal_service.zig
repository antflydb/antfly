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

//! One bounded server-owned lane for snapshot and native reader heartbeats. Reader
//! authority remains with each source; this service supplies only execution.
const std = @import("std");
const local = @import("antfly_local_sources");
const Runnable = local.common_request_admission.runnable_scheduler;
const Context = local.serverless_query_lake_read_context.Context;
const Owner = @import("lake_snapshot_pins.zig").Owner;
const platform = @import("antfly_platform");

pub const Service = struct {
    pub const limit = 64;
    pub const Link = struct {
        next: ?*Link = null,
        ptr: *anyopaque = undefined,
        renew: *const fn (*anyopaque, Context) anyerror!void = undefined,
        check: *const fn (*anyopaque) anyerror!void = undefined,
        busy: bool = false,
        idle: std.Io.Event = .is_set,
        sweep: u64 = 0,
        epoch: u64 = 0,
    };
    io: ?std.Io = null,
    scheduler: ?*Runnable.Scheduler = null,
    configured: bool = false,
    lifecycle: std.Io.Mutex = .init,
    mutex: std.atomic.Mutex = .unlocked,
    execution: Runnable.Execution = .{},
    future: ?std.Io.Future(void) = null,
    head: ?*Link = null,
    count: usize = 0,
    sweep: u64 = 0,
    epoch: std.atomic.Value(u64) = .init(0),
    failed: std.atomic.Value(bool) = .init(false),
    closed: std.atomic.Value(bool) = .init(false),
    stopping: std.atomic.Value(bool) = .init(false),
    interval_ns: u64 = 30 * std.time.ns_per_s,

    pub fn configure(self: *Service, io: std.Io, scheduler: ?*Runnable.Scheduler) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.configured and !self.closed.load(.acquire)) {
            self.io = io;
            self.scheduler = scheduler;
            self.configured = true;
        }
    }
    const Loan = struct {
        execution: ?*Runnable.Execution = null,
        class: ?Runnable.Class = null,
        fn begin(context: Context) Loan {
            if (context.execution) |execution| {
                const class = execution.retainedClass() catch return .{};
                execution.park(.resource_blocked) catch return .{};
                return .{ .execution = execution, .class = class };
            }
            return .{};
        }
        fn finish(self: Loan) !void {
            if (self.execution) |execution| try execution.enter(self.class.?);
        }
    };
    fn sourceFence(raw: *anyopaque) !void {
        const link: *Link = @ptrCast(@alignCast(raw));
        try link.check(link.ptr);
    }
    fn lockLifecycle(self: *Service, link: *Link, parent: Context) !void {
        var context = parent;
        context.authority = .{ .ptr = link, .check = sourceFence };
        const operation: @import("provider_operation.zig").Operation = .{ .context = context };
        // New registrations can abandon their wait at the original deadline;
        // they do not yet own a callback that requires an uncancelable join.
        while (!self.lifecycle.tryLock()) try operation.sleep(1);
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Service = @ptrCast(@alignCast(raw));
        return self.stopping.load(.acquire) or self.closed.load(.acquire);
    }
    fn check(raw: *anyopaque) !void {
        if (canceled(raw)) return error.Canceled;
    }
    fn start(self: *Service) !void {
        const io = self.io orelse return error.BackendRuntimeIoUnavailable;
        if (self.future != null and !self.failed.load(.acquire)) return;
        if (self.future) |*future| future.cancel(io);
        self.future = null;
        self.execution = .{};
        self.failed.store(false, .release);
        self.stopping.store(false, .release);
        _ = self.epoch.fetchAdd(1, .acq_rel);
        if (self.scheduler) |scheduler| {
            try self.execution.reserveControlService(scheduler, .{
                .io = io,
                .retained_bytes = @sizeOf(Service) + limit * @sizeOf(Link),
                .cancellation = .{ .ptr = self, .requested = canceled },
            });
        }
        errdefer self.execution.finish();
        self.future = try io.concurrent(run, .{self});
    }
    pub fn attach(self: *Service, owner: *Owner) !void {
        const Call = struct {
            fn check(raw: *anyopaque) !void {
                try @as(*Owner, @ptrCast(@alignCast(raw))).checkSource();
            }
            fn renew(raw: *anyopaque, context: Context) !void {
                try @as(*Owner, @ptrCast(@alignCast(raw))).heartbeatFor(context);
            }
        };
        try self.attachLink(&owner.link, owner, Call.renew, Call.check, owner.parent);
    }
    pub fn attachLink(self: *Service, link: *Link, ptr: *anyopaque, renew: *const fn (*anyopaque, Context) anyerror!void, authority: *const fn (*anyopaque) anyerror!void, parent: Context) !void {
        if (self.closed.load(.acquire)) return error.SchedulerClosed;
        const io = self.io orelse return error.BackendRuntimeIoUnavailable;
        try parent.ensureActive();
        link.ptr = ptr;
        link.renew = renew;
        link.check = authority;
        try self.lockLifecycle(link, parent);
        defer self.lifecycle.unlock(io);
        try authority(ptr);
        if (self.closed.load(.acquire)) return error.SchedulerClosed;
        platform.sync.lockYielding(&self.mutex);
        const full = self.count >= limit;
        self.mutex.unlock();
        if (full) return error.SchedulerFull;
        try self.start();
        errdefer if (self.count == 0) self.stopWorker();
        try authority(ptr);
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        link.next = self.head;
        link.sweep = self.sweep;
        link.epoch = self.epoch.load(.acquire);
        self.head = link;
        self.count += 1;
    }
    pub fn detach(self: *Service, owner: *Owner) void {
        self.detachLink(&owner.link, owner.parent);
    }
    pub fn detachLink(self: *Service, entry: *Link, parent: Context) void {
        if (!self.lifecycle.tryLock()) {
            const loan = Loan.begin(parent);
            self.lifecycle.lockUncancelable(self.io.?);
            loan.finish() catch {};
        }
        defer self.lifecycle.unlock(self.io.?);
        platform.sync.lockYielding(&self.mutex);
        var link = &self.head;
        while (link.* != entry) link = &link.*.?.next;
        link.* = entry.next;
        self.count -= 1;
        const busy = entry.busy;
        self.mutex.unlock();
        if (busy) {
            const loan = Loan.begin(parent);
            entry.idle.waitUncancelable(self.io.?);
            loan.finish() catch {};
        }
        if (self.count == 0) {
            const loan = Loan.begin(parent);
            self.stopWorker();
            loan.finish() catch {};
        }
    }
    fn stopWorker(self: *Service) void {
        self.stopping.store(true, .release);
        if (self.future) |*future| future.cancel(self.io.?);
        self.future = null;
        self.execution.finish();
    }

    pub fn checkOwner(self: *const Service, owner: *const Owner) !void {
        try self.checkLink(&owner.link);
    }
    pub fn checkLink(self: *const Service, link: *const Link) !void {
        if (self.closed.load(.acquire) or self.failed.load(.acquire) or link.epoch != self.epoch.load(.acquire)) return error.LakeSnapshotReadLeaseExpired;
    }
    fn run(self: *Service) void {
        defer self.execution.finish();
        self.runOwned() catch {
            if (!self.stopping.load(.acquire)) self.failed.store(true, .release);
        };
    }
    fn runOwned(self: *Service) !void {
        const io = self.io.?;
        const admitted = self.scheduler != null;
        while (!self.stopping.load(.acquire)) {
            try io.sleep(.fromNanoseconds(@intCast(self.interval_ns)), .awake);
            if (admitted) try self.execution.enterWithCheck(.control, .{ .context = self, .check = check });
            platform.sync.lockYielding(&self.mutex);
            self.sweep +%= 1;
            const sweep = self.sweep;
            self.mutex.unlock();
            while (!self.stopping.load(.acquire)) {
                platform.sync.lockYielding(&self.mutex);
                var cursor = self.head;
                while (cursor) |owner| : (cursor = owner.next) {
                    if (owner.sweep != sweep) break;
                }
                const owner = cursor orelse {
                    self.mutex.unlock();
                    break;
                };
                owner.sweep = sweep;
                owner.busy = true;
                owner.idle.reset();
                self.mutex.unlock();
                if (owner.epoch == self.epoch.load(.acquire)) {
                    var context: Context = .{ .io = io };
                    if (admitted) context = context.withExecution(&self.execution);
                    owner.renew(owner.ptr, context) catch {};
                }
                platform.sync.lockYielding(&self.mutex);
                owner.busy = false;
                owner.idle.set(io);
                self.mutex.unlock();
            }
            if (admitted) try self.execution.park(.resource_blocked);
        }
    }
    pub fn deinit(self: *Service) void {
        self.closed.store(true, .release);
        platform.sync.lockYielding(&self.mutex);
        const configured_io = self.io;
        self.mutex.unlock();
        const io = configured_io orelse return;
        self.lifecycle.lockUncancelable(io);
        defer self.lifecycle.unlock(io);
        self.closed.store(true, .release);
        self.stopping.store(true, .release);
        if (self.future) |*future| future.cancel(io);
        self.future = null;
        self.execution.finish();
    }
};
