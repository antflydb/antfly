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

//! Stable, joined read protection. Checks are passive on model/graph workers;
//! only the admitted renewal worker touches the durable pin provider.
const std = @import("std");
const lease = @import("read_lease.zig");
const maintenance = @import("../maintenance_cancellation.zig");
const renewal = @import("../build/lease_renewal.zig");
const ProgressStore = @import("../catalog/progress_store.zig").ProgressStore;
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const platform = @import("antfly_platform");

/// Initialize and start at the final address; deinit before the parent retires.
pub const Pin = struct {
    progress: *ProgressStore,
    namespace: []const u8,
    version: u64,
    parent: maintenance.Token,
    cancellation: Cancellation = .none,
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    registration: renewal.Registration = .{},
    owned: renewal.Renewal = .{},
    fallback_io: ?std.Io.Threaded = null,
    worker: ?*renewal.Renewal = null,

    fn bounds(self: *const Pin) lease.Lease {
        return .{ .unix_deadline = self.unix_deadline.load(.acquire), .authority_deadline = self.authority_deadline.load(.acquire) };
    }
    fn publish(self: *Pin, value: lease.Lease) void {
        self.unix_deadline.store(value.unix_deadline, .release);
        self.authority_deadline.store(value.authority_deadline, .release);
    }
    fn checkSource(self: *const Pin) !void {
        try self.parent.check();
        try self.cancellation.check();
        try self.bounds().check();
    }
    pub fn check(self: *const Pin) !void {
        try self.registration.check();
        try self.checkSource();
        if (self.worker) |worker| try worker.check();
    }
    fn sourceFence(raw: *anyopaque) !void {
        try @as(*Pin, @ptrCast(@alignCast(raw))).checkSource();
    }
    fn refresh(raw: *anyopaque, context: Context) !void {
        const self: *Pin = @ptrCast(@alignCast(raw));
        try self.checkSource();
        const prior = self.bounds();
        const unix = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (prior.unix_deadline -| unix >= lease.reuse_min_ns and prior.authority_deadline -| authority >= lease.reuse_min_ns) return;
        // Every physical wait validates the old rights. A delayed response may
        // never reacquire a source whose existing rights have expired.
        var view = self.progress.*;
        var scoped = context;
        scoped.io = self.parent.io;
        scoped.authority = .{ .ptr = self, .check = sourceFence };
        view.context = scoped;
        const next = try lease.acquireAt(&view, self.namespace, self.version, unix, authority);
        try self.checkSource();
        try next.check();
        self.publish(next);
    }
    fn refreshOwned(raw: *anyopaque, context: Context) !u64 {
        try refresh(raw, context);
        return @as(*Pin, @ptrCast(@alignCast(raw))).unix_deadline.load(.acquire);
    }
    pub fn start(self: *Pin, initial: lease.Lease) !void {
        self.publish(initial);
        try self.checkSource();
        if (self.parent.renewals) |worker| {
            try worker.attach(&self.registration, self, refresh);
            self.worker = worker;
        } else {
            self.owned.start(self.parent, self, refreshOwned, lease.duration_ns, initial.unix_deadline, self.parent.io) catch |err| {
                try self.checkSource();
                const admitted = if (self.parent.execution) |execution| execution.entry != null or execution.borrowed != null else false;
                if (err != error.ConcurrencyUnavailable or admitted) return err;
                // Synchronous, explicitly unadmitted embedders may supply an
                // Io without concurrent tasks (including debug_io). Keep all
                // original cancellation and provider Io in checkSource/refresh;
                // only the timer task uses this joined local executor.
                self.owned = .{};
                self.fallback_io = .init(self.progress.allocator, .{});
                errdefer {
                    self.fallback_io.?.deinit();
                    self.fallback_io = null;
                }
                var task_parent = self.parent;
                task_parent.io = self.fallback_io.?.io();
                task_parent.cooperative = .{ .ptr = self, .check_fn = checkSourceToken, .is_cancelled_fn = sourceCanceled };
                try self.owned.start(task_parent, self, refreshOwned, lease.duration_ns, initial.unix_deadline, self.parent.io);
            };
            self.worker = &self.owned;
        }
    }
    pub fn deinit(self: *Pin) void {
        self.registration.stop();
        if (self.worker == &self.owned) self.owned.stop();
        self.worker = null;
        if (self.fallback_io) |*io| io.deinit();
        self.fallback_io = null;
    }
    fn checkSourceToken(raw: *const anyopaque) !void {
        try @as(*const Pin, @ptrCast(@alignCast(raw))).checkSource();
    }
    fn sourceCanceled(raw: *const anyopaque) bool {
        checkSourceToken(raw) catch return true;
        return false;
    }
    fn checkToken(raw: *const anyopaque) !void {
        try @as(*const Pin, @ptrCast(@alignCast(raw))).check();
    }
    fn canceled(raw: *const anyopaque) bool {
        checkToken(raw) catch return true;
        return false;
    }
    pub fn token(self: *Pin) Cancellation {
        return .{ .ptr = self, .check_fn = checkToken, .is_cancelled_fn = canceled };
    }
    pub fn maintenanceToken(self: *Pin) maintenance.Token {
        var result = self.parent;
        result.cooperative = self.token();
        // Pin.check includes the original checkpoint and cooperative token.
        result.checkpoint_ptr = null;
        result.checkpoint_fn = null;
        result.renewals = self.worker;
        return result;
    }
};
