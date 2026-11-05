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

//! Query-role recovery loop, deliberately independent of maintenance ticks.
//! Durable submissions are rediscovered after pod loss; notifications are hints.
const std = @import("std");
const runtime = @import("runtime.zig");
const state = @import("state.zig");
const A = std.mem.Allocator;
pub const Driver = struct {
    const worker_count = 16;
    const lease_workers = 2;
    const Hint = struct { tenant: state.Digest, query: state.Query, not_before_ms: u64 = 0, renew_by_ms: u64 = 0, discover: bool = false, failures: u8 = 0 };
    service: runtime.Service,
    dispatcher: runtime.Dispatcher,
    interval_ms: u32 = 100,
    principal_scopes: bool = false,
    idle_delay_ms: u32 = 100,
    delay_ms: u32 = 100,
    sweep_work: bool = false,
    after: ?[]u8 = null,
    group: std.Io.Group = .init,
    metadata_budget_ms: u32 = 10_000,
    worker_wake: [worker_count]std.Io.Event = @splat(.unset),
    active: [worker_count]?state.Query = @splat(null),
    started: bool = false,
    stopping: std.atomic.Value(bool) = .init(false),
    io: ?std.Io = null,
    wake: std.Io.Event = .unset,
    hint_mutex: std.atomic.Mutex = .unlocked,
    hints: [256]Hint = undefined,
    hint_head: usize = 0,
    hint_count: usize = 0,
    pub fn notification(self: *Driver) runtime.Notification {
        return .{ .ptr = self, .notify = struct {
            fn notify(raw: *anyopaque, tenant: state.Digest, query: state.Query) void {
                const driver: *Driver = @ptrCast(@alignCast(raw));
                driver.enqueue(.{ .tenant = tenant, .query = query }, true);
            }
        }.notify };
    }
    fn enqueue(self: *Driver, hint: Hint, signal: bool) void {
        self.enqueueWithPolicy(hint, signal, false);
    }
    fn enqueueDiscovered(self: *Driver, query: state.Query) void {
        self.enqueueWithPolicy(.{ .tenant = self.service.tenant, .query = query, .discover = true }, false, true);
    }
    fn enqueueWithPolicy(self: *Driver, hint: Hint, signal: bool, discovery_only: bool) void {
        if (self.stopping.load(.acquire)) return;
        while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
        // Discovery restores missing work; it is not a progress notification.
        // Selection and this check share a lock so an active turn cannot gain
        // an immediate duplicate between dequeue and its delayed requeue.
        if (discovery_only) {
            const known = for (self.active) |active| {
                if (active) |query| if (std.meta.eql(query, hint.query)) break true;
            } else for (0..self.hint_count) |i| {
                if (std.meta.eql(self.hints[(self.hint_head + i) % self.hints.len].query, hint.query)) break true;
            } else false;
            if (known) {
                self.hint_mutex.unlock();
                return;
            }
        }
        const duplicate = for (0..self.hint_count) |i| {
            const queued = &self.hints[(self.hint_head + i) % self.hints.len];
            if (std.meta.eql(queued.query, hint.query)) {
                // External progress/cancellation wins over a heartbeat delay,
                // including a notification racing this turn's requeue.
                queued.not_before_ms = @min(queued.not_before_ms, hint.not_before_ms);
                if (!hint.discover) {
                    queued.tenant = hint.tenant;
                    queued.discover = false;
                }
                if (hint.renew_by_ms != 0) queued.renew_by_ms = if (queued.renew_by_ms == 0) hint.renew_by_ms else @min(queued.renew_by_ms, hint.renew_by_ms);
                break true;
            }
        } else false;
        if (!duplicate and self.hint_count < self.hints.len) {
            self.hints[(self.hint_head + self.hint_count) % self.hints.len] = hint;
            self.hint_count += 1;
        }
        self.hint_mutex.unlock();
        // A full queue still wakes discovery. Overflow loses only the hint,
        // never the durable query or cancellation record.
        if (self.io orelse self.service.context.io) |io| {
            if (signal) self.wake.set(io);
            // Also update sleeping workers when a turn establishes an earlier
            // lease deadline. Events have exactly one consumer per lane.
            for (&self.worker_wake) |*event| event.set(io);
        }
    }
    fn drain(self: *Driver, out: []Hint) usize {
        while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.hint_mutex.unlock();
        const count = @min(out.len, self.hint_count);
        for (out[0..count], 0..) |*hint, i| hint.* = self.hints[(self.hint_head + i) % self.hints.len];
        self.hint_head = (self.hint_head + count) % self.hints.len;
        self.hint_count -= count;
        return count;
    }
    fn hasHint(self: *Driver, query: state.Query) bool {
        while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.hint_mutex.unlock();
        for (0..self.hint_count) |i| {
            if (std.meta.eql(self.hints[(self.hint_head + i) % self.hints.len].query, query)) return true;
        }
        return false;
    }
    pub fn start(self: *Driver, io: std.Io) !void {
        if (self.started) return;
        if (self.service.role != .query_only and self.service.role != .combined) return error.SqlQueryRoleRequired;
        self.io = io;
        self.stopping.store(false, .release);
        errdefer {
            self.stopping.store(true, .release);
            self.group.cancel(io);
        }
        for (0..worker_count) |index| try self.group.concurrent(io, worker, .{ self, index });
        try self.group.concurrent(io, run, .{self});
        self.started = true;
    }
    pub fn stop(self: *Driver) void {
        self.stopping.store(true, .release);
        if (self.started) self.group.cancel(self.io.?);
        self.started = false;
        if (self.after) |value| self.service.a.free(value);
        self.after = null;
    }
    fn run(self: *Driver) void {
        while (!self.stopping.load(.acquire)) {
            // Reset before draining: a notification during a turn remains set
            // and prevents sleeping. A notification before reset is in the queue.
            self.wake.reset();
            _ = self.discover() catch |err| switch (err) {
                error.Canceled, error.Cancelled => return,
                else => std.log.warn("distributed SQL recovery turn deferred: {s}", .{@errorName(err)}),
            };
            self.wake.waitTimeout(self.io.?, .{ .duration = .{ .raw = .fromMilliseconds(self.sleepDelay()), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return,
            };
        }
    }
    pub fn sleepDelay(self: *const Driver) u32 {
        if (self.delay_ms <= self.interval_ms) return self.interval_ms;
        // Boot-specific jitter prevents idle replicas from polling in lockstep.
        return self.delay_ms + @as(u32, @intCast(std.hash.Wyhash.hash(0, self.service.owner) % @max(@as(u32, 1), self.delay_ms / 8)));
    }
    /// One bounded round-robin page. A long-lived query never monopolizes the
    /// head of the discovery index, and the cursor itself need not be durable.
    fn context(self: *Driver) @import("antfly_local_sources").serverless_query_lake_read_context.Context {
        var value = self.service.context;
        const deadline = @import("antfly_platform").time.monotonicNs() +| @as(u64, self.metadata_budget_ms) * std.time.ns_per_ms;
        value.deadline_ns = if (value.deadline_ns) |parent| @min(parent, deadline) else deadline;
        value.cancellation = .{ .ptr = self, .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const driver: *const Driver = @ptrCast(@alignCast(raw));
                return driver.stopping.load(.acquire) or (if (driver.service.context.cancellation) |parent| parent.isCancelled() else false);
            }
        }.canceled };
        return value;
    }
    fn discover(self: *Driver) !usize {
        var arena = std.heap.ArenaAllocator.init(self.service.a);
        defer arena.deinit();
        const a = arena.allocator();
        const ctx = self.context();
        var client = self.service.state_store.client;
        client.allocator = a;
        const prefix = try std.fmt.allocPrint(a, "{s}/sql-query-expiry/", .{self.service.state_store.prefix});
        var listed = try client.listObjects(self.service.state_store.bucket, .{ .prefix = prefix, .start_after = self.after, .max_keys = 16, .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&ctx) });
        defer listed.deinit(a);
        for (listed.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len != prefix.len + 90) return error.InvalidSqlExecutionState;
            const expires = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
            if (expires <= self.service.clock.now()) continue;
            const query: state.Query = .{ .id = entry.key[prefix.len + 21 ..][0..64].*, .expires_ms = expires };
            self.enqueueDiscovered(query);
        }
        const next = if (listed.entries.len != 0) try self.service.a.dupe(u8, listed.entries[listed.entries.len - 1].key) else null;
        if (self.after) |old| self.service.a.free(old);
        self.after = next;
        self.sweep_work = self.sweep_work or listed.entries.len != 0;
        self.delay_ms = self.interval_ms;
        if (next == null) {
            self.idle_delay_ms = if (self.sweep_work) self.interval_ms else @min(@as(u32, 30_000), self.idle_delay_ms *| 2);
            self.delay_ms = self.idle_delay_ms;
            self.sweep_work = false;
        }
        return listed.entries.len;
    }
    fn take(self: *Driver, lane: usize, wait_ms: *u64) ?Hint {
        while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.hint_mutex.unlock();
        const now = self.service.clock.now();
        var selected: ?usize = null;
        var urgent: u64 = std.math.maxInt(u64);
        wait_ms.* = 30_000;
        for (0..self.hint_count) |offset| {
            const hint = self.hints[(self.hint_head + offset) % self.hints.len];
            const busy = for (self.active) |entry| {
                if (entry) |query| if (std.meta.eql(query, hint.query)) break true;
            } else false;
            if (busy) continue;
            const lease_due = hint.renew_by_ms != 0 and hint.renew_by_ms <= now;
            const due = if (lane < lease_workers) (if (hint.renew_by_ms == 0) continue else hint.renew_by_ms) else @min(hint.query.expires_ms, @min(hint.not_before_ms, if (hint.renew_by_ms == 0) std.math.maxInt(u64) else hint.renew_by_ms));
            wait_ms.* = @min(wait_ms.*, due -| now);
            if (due > now) continue;
            // FIFO for ordinary work; lease deadlines take priority once due.
            const priority = if (lease_due) hint.renew_by_ms else std.math.maxInt(u64);
            if (selected == null or priority < urgent) {
                selected = offset;
                urgent = priority;
            }
        }
        const offset = selected orelse return null;
        const hint = self.hints[(self.hint_head + offset) % self.hints.len];
        for (offset..self.hint_count - 1) |i| self.hints[(self.hint_head + i) % self.hints.len] = self.hints[(self.hint_head + i + 1) % self.hints.len];
        self.hint_count -= 1;
        self.active[lane] = hint.query;
        return hint;
    }
    fn worker(self: *Driver, lane: usize) void {
        const io = self.io.?;
        while (!self.stopping.load(.acquire)) {
            self.worker_wake[lane].reset();
            var wait_ms: u64 = 0;
            if (self.take(lane, &wait_ms)) |hint| {
                self.process(hint, lane < lease_workers) catch |err| {
                    if (!self.stopping.load(.acquire) and err != error.Canceled and err != error.Cancelled and err != error.DeadlineExceeded and err != error.SqlWorkerUnavailable) std.log.warn("distributed SQL metadata turn deferred: {s}", .{@errorName(err)});
                };
                while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
                self.active[lane] = null;
                self.hint_mutex.unlock();
                // A notification that arrived during this turn can now run.
                for (&self.worker_wake) |*event| event.set(io);
                continue;
            }
            self.worker_wake[lane].waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(@intCast(@max(@as(u64, 1), wait_ms))), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return,
            };
        }
    }
    fn process(self: *Driver, original: Hint, lease_only: bool) !void {
        if (original.query.expires_ms <= self.service.clock.now()) return;
        var hint = original;
        errdefer if (!self.stopping.load(.acquire)) {
            const now = self.service.clock.now();
            hint.failures = @min(@as(u8, 6), hint.failures +| 1);
            const backoff = @min(@as(u32, 5000), self.interval_ms *| (@as(u32, 1) << @as(u5, @intCast(hint.failures))));
            const delay = if (hint.renew_by_ms != 0 and hint.renew_by_ms <= now +| 10_000) @min(backoff, 1000) else backoff;
            hint.not_before_ms = now +| delay;
            if (hint.renew_by_ms == 0 and hint.query.expires_ms > now +| 60_000) hint.renew_by_ms = now +| 30_000;
            if (hint.renew_by_ms != 0 and hint.renew_by_ms <= now) hint.renew_by_ms = hint.not_before_ms;
            self.enqueue(hint, false);
        };
        var arena = std.heap.ArenaAllocator.init(self.service.a);
        defer arena.deinit();
        const a = arena.allocator();
        var service = self.service;
        service.notification = null;
        service.context = self.context();
        var records = service.state_store;
        records.context = service.context;
        const key = try records.key(a, hint.query, null, 0);
        var current = (try records.load(a, key)) orelse return;
        defer current.deinit(a);
        if (current.value.terminal()) {
            service.retire(hint.query);
            var client = records.client;
            client.allocator = a;
            const index = try std.fmt.allocPrint(a, "{s}/sql-query-expiry/{d:0>20}/{s}.json", .{ records.prefix, hint.query.expires_ms, hint.query.id });
            client.deleteObject(records.bucket, index, .{ .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) }) catch |err| switch (err) {
                error.FileNotFound, error.NotFound, error.ObjectNotFound => {},
                else => return err,
            };
            return;
        }
        const now = service.clock.now();
        if (current.value.status == .running and !std.mem.eql(u8, current.value.owner, service.owner) and current.value.until_ms +| 30_000 >= now) return;
        if (hint.discover and self.principal_scopes) {
            if (current.value.tenant) |tenant| hint.tenant = tenant else {
                const plan_key = try std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/plan.json", .{ records.prefix, hint.query.expires_ms, hint.query.id });
                var client = records.client;
                client.allocator = a;
                var object = try client.getObject(records.bucket, plan_key, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) });
                defer object.deinit(a);
                hint.tenant = (try std.json.parseFromSliceLeaky(struct { tenant: state.Digest }, a, object.body, .{ .ignore_unknown_fields = true })).tenant;
            }
        }
        hint.discover = false;
        service.tenant = hint.tenant;
        const owned = current.value.status == .running and std.mem.eql(u8, current.value.owner, service.owner);
        hint.renew_by_ms = if (owned and current.value.until_ms < hint.query.expires_ms) current.value.until_ms -| 20_000 else 0;
        if (owned and current.value.until_ms > now and hint.renew_by_ms != 0 and hint.renew_by_ms <= now and !current.value.cancel_requested) {
            // Renewal is independent of task enumeration and has reserved lanes.
            try service.authorize(a, hint.query, current.value);
            if (service.coordination) |coordination| {
                if (!coordination.enter(coordination.ptr, hint.query)) {
                    hint.not_before_ms = now +| self.interval_ms;
                    hint.renew_by_ms = hint.not_before_ms;
                    self.enqueue(hint, false);
                    return;
                }
            }
            defer if (service.coordination) |coordination| coordination.leave(coordination.ptr, hint.query);
            records.tenant = hint.tenant;
            records.now_ms = service.clock.now();
            try records.renew(a, key, .{ .owner = service.owner, .epoch = current.value.epoch }, 60_000);
            hint.renew_by_ms = @min(hint.query.expires_ms, records.now_ms +| 60_000) -| 20_000;
            hint.not_before_ms = now;
            self.enqueue(hint, false);
            return;
        }
        if (lease_only) {
            // Expired leases and cancellation need a normal state transition,
            // rather than another immediate reservation of a renewal lane.
            hint.renew_by_ms = 0;
            hint.not_before_ms = now;
            self.enqueue(hint, false);
            return;
        }
        const complete = service.turn(hint.query, self.dispatcher) catch |err| switch (err) {
            error.SqlQueryExpired, error.SqlExecutionFailed, error.SqlExecutionNotFound, error.SqlExecutionUnauthorized => return,
            else => return err,
        };
        if (!complete) {
            hint.failures = 0;
            if (!owned) hint.renew_by_ms = if (hint.query.expires_ms > now +| 60_000) now +| 40_000 else 0;
            hint.not_before_ms = service.clock.now() +| self.interval_ms;
            if (service.coordination) |coordination| if (coordination.delivery_pending(coordination.ptr, hint.query) and !coordination.has_progress(coordination.ptr, hint.query)) {
                hint.not_before_ms = service.clock.now() +| 10_000;
            };
            self.enqueue(hint, false);
        }
    }
    /// Synchronous deterministic embedding/test hook. Native deployments use
    /// start(), whose discovery loop never executes coordinator metadata I/O.
    pub fn tick(self: *Driver) !usize {
        return if (self.started) self.discover() else self.tickSerial();
    }
    fn tickSerial(self: *Driver) !usize {
        if (self.service.role != .query_only and self.service.role != .combined) return error.SqlQueryRoleRequired;
        var service = self.service;
        // Internal progress must not recursively notify its own driver.
        service.notification = null;
        service.context.cancellation = .{ .ptr = self, .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const driver: *const Driver = @ptrCast(@alignCast(raw));
                return driver.stopping.load(.acquire) or (if (driver.service.context.cancellation) |parent| parent.isCancelled() else false);
            }
        }.canceled };
        var arena = std.heap.ArenaAllocator.init(self.service.a);
        defer arena.deinit();
        const a = arena.allocator();
        var hinted: [16]Hint = undefined;
        const hinted_count = self.drain(&hinted);
        var turns: usize = 0;
        // Reset backoff before any I/O: a transient read failure must not put
        // newly admitted work back into the previous thirty-second sleep.
        if (hinted_count != 0) {
            self.idle_delay_ms = self.interval_ms;
            self.delay_ms = self.interval_ms;
            self.sweep_work = true;
        }
        var consumed: usize = 0;
        errdefer for (hinted[consumed..hinted_count]) |hint| self.enqueue(hint, false);
        for (hinted[0..hinted_count]) |hint| {
            consumed += 1;
            errdefer self.enqueue(hint, false);
            if (hint.query.expires_ms <= service.clock.now()) continue;
            if (hint.not_before_ms > service.clock.now()) {
                self.enqueue(hint, false);
                continue;
            }
            var records = service.state_store;
            records.context = service.context;
            const key = try records.key(a, hint.query, null, 0);
            var current = (try records.load(a, key)) orelse continue;
            defer current.deinit(a);
            if (current.value.status == .running and !std.mem.eql(u8, current.value.owner, service.owner) and current.value.until_ms +| 30_000 >= service.clock.now()) continue;
            var scoped = service;
            scoped.tenant = hint.tenant;
            const complete = scoped.turn(hint.query, self.dispatcher) catch |err| switch (err) {
                error.SqlQueryExpired, error.SqlExecutionFailed, error.SqlExecutionNotFound, error.SqlExecutionUnauthorized => continue,
                error.Canceled, error.Cancelled => return err,
                else => {
                    self.enqueue(hint, false);
                    continue;
                },
            };
            if (!complete) {
                var next_hint = hint;
                next_hint.not_before_ms = 0;
                if (service.coordination) |coordination| if (coordination.delivery_pending(coordination.ptr, hint.query) and !coordination.has_progress(coordination.ptr, hint.query)) {
                    next_hint.not_before_ms = service.clock.now() +| 10_000;
                };
                self.enqueue(next_hint, false);
            }
            turns += 1;
        }
        var client = self.service.state_store.client;
        client.allocator = a;
        const prefix = try std.fmt.allocPrint(a, "{s}/sql-query-expiry/", .{self.service.state_store.prefix});
        var listed = try client.listObjects(self.service.state_store.bucket, .{ .prefix = prefix, .start_after = self.after, .max_keys = 16, .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) });
        defer listed.deinit(a);
        for (listed.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len != prefix.len + 90) return error.InvalidSqlExecutionState;
            const expiry = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
            if (expiry <= self.service.clock.now()) continue;
            const query: @import("state.zig").Query = .{ .id = entry.key[prefix.len + 21 ..][0..64].*, .expires_ms = expiry };
            const just_processed = for (hinted[0..hinted_count]) |hint| {
                if (std.meta.eql(hint.query, query)) break true;
            } else false;
            if (just_processed or self.hasHint(query)) continue;
            const key = try self.service.state_store.key(a, query, null, 0);
            var records = self.service.state_store;
            records.context = service.context;
            var current = (try records.load(a, key)) orelse continue;
            defer current.deinit(a);
            if (current.value.terminal()) {
                try client.deleteObject(self.service.state_store.bucket, entry.key, .{ .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) });
                continue;
            }
            // A live foreign owner drives its own fast loop. Its lease remains
            // discoverable for takeover after failure, without reopening plans.
            if (current.value.status == .running and !std.mem.eql(u8, current.value.owner, service.owner) and current.value.until_ms +| 30_000 >= service.clock.now()) continue;
            var scoped = service;
            if (self.principal_scopes and current.value.tenant != null) {
                scoped.tenant = current.value.tenant.?;
            } else if (self.principal_scopes) {
                const plan_key = try std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/plan.json", .{ service.state_store.prefix, query.expires_ms, query.id });
                var object = client.getObject(service.state_store.bucket, plan_key, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) }) catch continue;
                defer object.deinit(a);
                const identity = std.json.parseFromSliceLeaky(struct { tenant: @import("state.zig").Digest }, a, object.body, .{ .ignore_unknown_fields = true }) catch continue;
                scoped.tenant = identity.tenant;
            }
            _ = scoped.turn(query, self.dispatcher) catch |err| switch (err) {
                error.SqlExecutionFenced, error.PreconditionFailed, error.SqlExecutionNotFound => continue,
                error.Canceled, error.Cancelled => return err,
                else => {
                    std.log.warn("distributed SQL recovery deferred query={s}: {s}", .{ query.id, @errorName(err) });
                    continue;
                },
            };
            turns += 1;
        }
        const next = if (listed.entries.len != 0) try self.service.a.dupe(u8, listed.entries[listed.entries.len - 1].key) else null;
        if (self.after) |value| self.service.a.free(value);
        self.after = next;
        self.sweep_work = self.sweep_work or turns != 0;
        self.delay_ms = self.interval_ms;
        if (next == null) {
            self.idle_delay_ms = if (self.sweep_work) self.interval_ms else @min(@as(u32, 30_000), @max(self.interval_ms, self.idle_delay_ms *| 2));
            self.delay_ms = self.idle_delay_ms;
            self.sweep_work = false;
        }
        return turns;
    }
};

test "distributed SQL coordinator reserves lease lanes without executing queued ordinary turns" {
    const Clock = struct {
        fn read(_: ?*anyopaque) u64 {
            return 10000;
        }
    };
    var driver: Driver = .{ .service = undefined, .dispatcher = undefined };
    // Only the clock is needed by selection; selection performs no provider I/O.
    driver.service.clock = .{ .read = Clock.read };
    const ordinary: Driver.Hint = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 100000 } };
    var renewal = ordinary;
    renewal.query.id = @splat('c');
    renewal.renew_by_ms = 9000;
    var another = ordinary;
    another.query.id = @splat('d');
    driver.hints[0] = ordinary;
    driver.hints[1] = renewal;
    driver.hints[2] = another;
    driver.hint_count = 3;
    var delay: u64 = 0;
    const selected = driver.take(0, &delay).?;
    try std.testing.expectEqual(renewal.query, selected.query);
    try std.testing.expect(driver.take(1, &delay) == null);
    const ready = driver.take(Driver.lease_workers, &delay).?;
    try std.testing.expectEqual(ordinary.query, ready.query);
}

test "distributed SQL renewal-only turns do not load plans or pending tasks" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var records: state.Store = .{ .client = memory.client(), .bucket = "state", .prefix = "sql", .now_ms = 1000, .tenant = @splat('a') };
    const query: state.Query = .{ .id = @splat('b'), .expires_ms = 200000 };
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    try records.create(a, key, query.id, query.expires_ms);
    const claim = (try records.claim(a, key, "owner", 60000)).?;
    try records.checkpointTasks(a, key, claim, 0, 1, &.{0});
    const Clock = struct {
        fn read(_: ?*anyopaque) u64 {
            return 45000;
        }
    };
    var driver: Driver = .{ .service = .{ .a = a, .state_store = records, .exchange_store = undefined, .source = undefined, .retention = undefined, .role = .query_only, .owner = "owner", .tenant = @splat('a'), .clock = .{ .read = Clock.read } }, .dispatcher = undefined };
    defer driver.stop();
    const metrics = @import("objectstore").operation_metrics;
    const before = metrics.runtime.snapshot().calls[@backingInt(metrics.Operation.get)];
    try driver.process(.{ .tenant = @splat('a'), .query = query, .renew_by_ms = 41000 }, true);
    try std.testing.expectEqual(@as(u64, 2), metrics.runtime.snapshot().calls[@backingInt(metrics.Operation.get)] - before);
    var value = (try records.load(a, key)).?;
    defer value.deinit(a);
    try std.testing.expectEqual(@as(u64, 105000), value.value.until_ms);
    try std.testing.expectEqualSlices(u32, &.{0}, value.value.pending);
    try records.requestCancel(a, key);
    driver.hint_count = 0;
    try driver.process(.{ .tenant = @splat('a'), .query = query, .renew_by_ms = 41000 }, true);
    var delay: u64 = 0;
    try std.testing.expect(driver.take(0, &delay) == null);
    try std.testing.expectEqual(query, driver.take(Driver.lease_workers, &delay).?.query);
}

test "distributed SQL discovery preserves active and queued backoff while progress wakes immediately" {
    const Clock = struct {
        fn read(_: ?*anyopaque) u64 {
            return 1000;
        }
    };
    var driver: Driver = .{ .service = undefined, .dispatcher = undefined, .io = std.testing.io };
    driver.service.clock = .{ .read = Clock.read };
    driver.service.tenant = @splat('a');
    const hint: Driver.Hint = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 100000 }, .not_before_ms = 11000 };
    const lane = Driver.lease_workers;
    driver.active[lane] = hint.query;
    driver.enqueueDiscovered(hint.query);
    try std.testing.expectEqual(@as(usize, 0), driver.hint_count);
    driver.enqueue(hint, false);
    driver.active[lane] = null;
    driver.enqueueDiscovered(hint.query);
    var delay: u64 = 0;
    try std.testing.expect(driver.take(lane, &delay) == null);
    try std.testing.expectEqual(@as(u64, 10000), delay);
    driver.notification().notify(driver.notification().ptr, hint.tenant, hint.query);
    try std.testing.expectEqual(hint.query, driver.take(lane, &delay).?.query);
    // A real notification arriving during execution must also survive requeue.
    driver.enqueue(.{ .tenant = hint.tenant, .query = hint.query }, false);
    driver.enqueue(hint, false);
    driver.active[lane] = null;
    try std.testing.expectEqual(hint.query, driver.take(lane, &delay).?.query);
    driver.active[lane] = null;
    driver.enqueueDiscovered(hint.query);
    try std.testing.expectEqual(hint.query, driver.take(lane, &delay).?.query);
}
