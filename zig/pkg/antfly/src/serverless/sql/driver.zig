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
    const Hint = struct { tenant: state.Digest, query: state.Query, not_before_ms: u64 = 0 };
    service: runtime.Service,
    dispatcher: runtime.Dispatcher,
    interval_ms: u32 = 100,
    principal_scopes: bool = false,
    idle_delay_ms: u32 = 100,
    delay_ms: u32 = 100,
    sweep_work: bool = false,
    after: ?[]u8 = null,
    group: std.Io.Group = .init,
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
        if (self.stopping.load(.acquire)) return;
        while (!self.hint_mutex.tryLock()) std.atomic.spinLoopHint();
        const duplicate = for (0..self.hint_count) |i| {
            const queued = &self.hints[(self.hint_head + i) % self.hints.len];
            if (std.meta.eql(queued.query, hint.query) and std.meta.eql(queued.tenant, hint.tenant)) {
                // External progress/cancellation wins over a heartbeat delay,
                // including a notification racing this turn's requeue.
                queued.not_before_ms = @min(queued.not_before_ms, hint.not_before_ms);
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
        if (signal) if (self.io orelse self.service.context.io) |io| self.wake.set(io);
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
            _ = self.tick() catch |err| switch (err) {
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
    pub fn tick(self: *Driver) !usize {
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
                if (service.coordination) |coordination| if (coordination.delivery_pending(coordination.ptr, hint.query)) {
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
            if (self.principal_scopes) {
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
