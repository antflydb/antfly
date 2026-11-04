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
const A = std.mem.Allocator;
pub const Driver = struct {
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
        while (true) {
            _ = self.tick() catch |err| switch (err) {
                error.Canceled, error.Cancelled => return,
                else => std.log.warn("distributed SQL recovery turn deferred: {s}", .{@errorName(err)}),
            };
            std.Io.sleep(self.io.?, .fromMilliseconds(self.sleepDelay()), .awake) catch return;
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
        service.context.cancellation = .{ .ptr = self, .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const driver: *const Driver = @ptrCast(@alignCast(raw));
                return driver.stopping.load(.acquire) or (if (driver.service.context.cancellation) |parent| parent.isCancelled() else false);
            }
        }.canceled };
        var arena = std.heap.ArenaAllocator.init(self.service.a);
        defer arena.deinit();
        const a = arena.allocator();
        var client = self.service.state_store.client;
        client.allocator = a;
        const prefix = try std.fmt.allocPrint(a, "{s}/sql-query-expiry/", .{self.service.state_store.prefix});
        var listed = try client.listObjects(self.service.state_store.bucket, .{ .prefix = prefix, .start_after = self.after, .max_keys = 16, .cancellation = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.types.contextCancellation(&service.context) });
        defer listed.deinit(a);
        var turns: usize = 0;
        for (listed.entries) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix) or entry.key.len != prefix.len + 90) return error.InvalidSqlExecutionState;
            const expiry = try std.fmt.parseInt(u64, entry.key[prefix.len..][0..20], 10);
            if (expiry <= self.service.clock.now()) continue;
            const query: @import("state.zig").Query = .{ .id = entry.key[prefix.len + 21 ..][0..64].*, .expires_ms = expiry };
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
