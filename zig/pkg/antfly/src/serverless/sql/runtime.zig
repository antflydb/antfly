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

//! Query-role execution service. Shared records, rather than a particular pod,
//! own accepted outputs. Dispatch can be HTTP, a queue, or in-process calls.
const std = @import("std");
const storage = @import("objectstore");
const local = @import("antfly_local_sources");
const plan = @import("plan.zig");
const state = @import("state.zig");
const exchange = @import("exchange.zig");
const runner = @import("runner.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const digest = local.serverless_external_source_mod.lake_catalog.types.digestHex;
pub const Clock = struct {
    ptr: ?*anyopaque = null,
    read: *const fn (?*anyopaque) u64 = realtime,
    fn realtime(_: ?*anyopaque) u64 {
        return @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    }
    pub fn now(self: Clock) u64 {
        return self.read(self.ptr);
    }
};
pub const Retention = struct {
    ptr: *anyopaque,
    /// Resolve and validate every authorized input capability; renew its base
    /// snapshot pins. Accepted WAL images belong to the retained capability.
    ensure: *const fn (*anyopaque, A, plan.Plan, Context) anyerror!void,
};
pub const Dispatcher = struct {
    ptr: *anyopaque,
    /// Bounded fan-out; delivery may duplicate or lose a response. Replaying
    /// deterministic tickets recovers dispatch without an external queue.
    dispatch: *const fn (*anyopaque, []const plan.Ticket, Context) anyerror!void,
};
pub const Result = struct { version: u16 = 1, query: state.Digest, pages: []const state.Output, rows: u64 };
pub const Factory = struct {
    ptr: *anyopaque,
    /// Returns a borrowed process-owned service for this authenticated scope.
    /// owner is a unique boot incarnation; stores must be scope-isolated.
    open: *const fn (*anyopaque, A, plan.Digest, Context) anyerror!Service,
    dispatcher: Dispatcher,
};
pub const Service = struct {
    a: A,
    state_store: state.Store,
    exchange_store: exchange.Store,
    tenant: state.Digest,
    role: @import("../api/types.zig").RuntimeRole,
    owner: []const u8,
    clock: Clock = .{},
    context: Context = .{},
    retention: Retention,
    source: runner.Source,
    spill: ?*local.sql_spill.Manager = null,
    task_memory_ceiling: u64 = 1024 * 1024 * 1024,
    fn store(self: Service) state.Store {
        var result = self.state_store;
        result.now_ms = self.clock.now();
        result.context = self.context;
        return result;
    }
    fn requireQueryRole(self: Service) !void {
        if (self.role != .query_only and self.role != .combined) return error.SqlQueryRoleRequired;
    }
    fn planKey(self: Service, a: A, query: state.Digest) ![]const u8 {
        try state.validateDigest(query);
        return std.fmt.allocPrint(a, "{s}/sql-execution/{s}/plan.json", .{ self.state_store.prefix, query });
    }
    fn immutable(self: Service, a: A, key: []const u8, bytes: []const u8, limit: usize) !void {
        try self.context.ensureActive();
        var client = self.state_store.client;
        client.allocator = a;
        var stored = client.putObject(self.state_store.bucket, key, bytes, .{ .if_none_match = true, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) }) catch |err| {
            var existing = client.getObject(self.state_store.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) }) catch return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, existing.body, bytes)) return error.SqlExecutionIdReused;
            return;
        };
        stored.deinit(a);
    }
    pub fn submit(self: Service, specification: plan.Plan) !state.Digest {
        try self.requireQueryRole();
        try specification.validate(self.clock.now());
        if (specification.reduce_boundaries.len != 0) return error.InvalidSqlDistributedPlan;
        if (specification.limits.task_memory > self.task_memory_ceiling) return error.SqlJoinBudgetExceeded;
        if (!std.mem.eql(u8, &self.tenant, &specification.tenant)) return error.SqlExecutionUnauthorized;
        try self.retention.ensure(self.retention.ptr, self.a, specification, self.context);
        const bytes = try specification.encode(self.a);
        defer self.a.free(bytes);
        if (bytes.len > 1024 * 1024) return error.SqlJoinBudgetExceeded;
        const query = digest(bytes);
        const key = try self.planKey(self.a, query);
        defer self.a.free(key);
        // Expiry index precedes every durable write so failed submissions are
        // discoverable without listing every active query or exchange object.
        const expiry_key = try std.fmt.allocPrint(self.a, "{s}/sql-query-expiry/{d:0>20}/{s}.json", .{ self.state_store.prefix, specification.expires_ms, query });
        defer self.a.free(expiry_key);
        try self.immutable(self.a, expiry_key, "{}", 1024);
        try self.immutable(self.a, key, bytes, 1024 * 1024);
        const query_key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(query_key);
        try self.store().create(self.a, query_key, query, specification.expires_ms);
        return query;
    }
    pub fn loadPlan(self: Service, a: A, query: state.Digest) !plan.Plan {
        const key = try self.planKey(a, query);
        defer a.free(key);
        var client = self.state_store.client;
        client.allocator = a;
        var object = try client.getObject(self.state_store.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) });
        defer object.deinit(a);
        if (!std.mem.eql(u8, &digest(object.body), &query)) return error.InvalidSqlDistributedPlan;
        const specification = try std.json.parseFromSliceLeaky(plan.Plan, a, object.body, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, &self.tenant, &specification.tenant)) return error.SqlExecutionUnauthorized;
        try specification.validate(self.clock.now());
        if (specification.limits.task_memory > self.task_memory_ceiling) return error.SqlJoinBudgetExceeded;
        return specification;
    }
    pub fn acquire(self: Service, query: state.Digest) !?state.Claim {
        try self.requireQueryRole();
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        return self.store().claim(self.a, key, self.owner, 60_000);
    }
    fn tasks(spec: plan.Plan, stage: u32) !u32 {
        return spec.stageTasks(stage);
    }
    fn taskFor(spec: plan.Plan, stage: u32, task: u32) !plan.Task {
        if (task >= try tasks(spec, stage)) return error.InvalidSqlDistributedPlan;
        if (stage >= 2) return .{ .stage = stage, .task = task, .kind = .aggregate, .finalize = stage + 1 == spec.finalStage() };
        if (stage == 0) return .{ .stage = stage, .task = task, .kind = if (spec.strategy == .broadcast) .scan_build else if (task < spec.right.splits) .map_right else .map_left, .split = if (task < spec.right.splits) task else task - spec.right.splits };
        return .{ .stage = stage, .task = task, .kind = if (spec.strategy == .broadcast) .broadcast_probe else .reduce, .split = task, .partition = if (spec.strategy == .shuffle) (if (spec.reduce_boundaries.len != 0) spec.reduce_boundaries[task] else @intCast(task)) else 0, .partition_end = if (spec.strategy == .shuffle) (if (spec.reduce_boundaries.len != 0) spec.reduce_boundaries[task + 1] else @intCast(task + 1)) else 1 };
    }
    /// One bounded coordinator turn. A caller schedules subsequent turns while
    /// pending, renews ownership, and may move ownership to any query pod.
    pub fn tick(self: Service, query: state.Digest, claim: state.Claim, dispatcher: Dispatcher) !bool {
        try self.requireQueryRole();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var spec = try self.loadPlan(a, query);
        const query_key = try self.store().key(a, query, null, 0);
        try self.store().renew(a, query_key, claim, 60_000);
        try self.retention.ensure(self.retention.ptr, a, spec, self.context);
        var current = (try self.store().load(a, query_key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(a);
        spec = try spec.withReducers(current.value.reduce_boundaries);
        if (current.value.stage == spec.finalStage()) {
            const count = try tasks(spec, spec.finalStage() - 1);
            const pages = try a.alloc(state.Output, count);
            var rows: u64 = 0;
            for (pages, 0..) |*page, task| {
                const key = try self.store().key(a, query, spec.finalStage() - 1, @intCast(task));
                var result = (try self.store().load(a, key)) orelse return error.InvalidSqlExecutionState;
                defer result.deinit(a);
                if (result.value.status != .succeeded) return error.InvalidSqlExecutionState;
                page.* = result.value.output.?;
                page.key = try a.dupe(u8, page.key);
                rows = try std.math.add(u64, rows, page.rows);
            }
            const bytes = try std.json.Stringify.valueAlloc(a, Result{ .query = query, .pages = pages, .rows = rows }, .{});
            if (bytes.len > 4 * 1024 * 1024) return error.SqlJoinBudgetExceeded;
            const key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.result.json", .{ self.exchange_store.prefix, spec.expires_ms, query, digest(bytes) });
            var result_service = self;
            result_service.state_store.client = self.exchange_store.client;
            result_service.state_store.bucket = self.exchange_store.bucket;
            try result_service.immutable(a, key, bytes, 4 * 1024 * 1024);
            try self.store().complete(a, query_key, claim, .{ .key = key, .hash = digest(bytes), .bytes = bytes.len, .rows = rows });
            return true;
        }
        const count = try tasks(spec, current.value.stage);
        var cursor = current.value.cursor;
        var tickets: std.ArrayList(plan.Ticket) = .empty;
        var pending: std.ArrayList(u32) = .empty;
        // Only unresolved tasks are revisited. A slow first task cannot pin a
        // sliding window or force repeated GETs of an ever-growing prefix.
        for (current.value.pending) |id| {
            if (id >= count) return error.InvalidSqlExecutionState;
            const key = try self.store().key(a, query, current.value.stage, id);
            var value = (try self.store().load(a, key)) orelse return error.InvalidSqlExecutionState;
            defer value.deinit(a);
            if (value.value.status == .failed or value.value.status == .cancelled) {
                try self.store().terminate(a, query_key, claim, .failed, value.value.failure orelse "TaskFailed");
                return true;
            }
            if (value.value.status == .succeeded) continue;
            try pending.append(a, id);
            if (value.value.status != .running or value.value.until_ms +| 30_000 < self.clock.now()) try tickets.append(a, .{ .tenant = self.tenant, .query = query, .stage = current.value.stage, .task = id });
        }
        while (cursor < count and pending.items.len < spec.limits.concurrency) : (cursor += 1) {
            const key = try self.store().key(a, query, current.value.stage, cursor);
            if (try self.store().ensure(a, key, query, spec.expires_ms)) {
                try pending.append(a, cursor);
                try tickets.append(a, .{ .tenant = self.tenant, .query = query, .stage = current.value.stage, .task = cursor });
                continue;
            }
            var value = (try self.store().load(a, key)) orelse return error.InvalidSqlExecutionState;
            defer value.deinit(a);
            if (value.value.status == .failed or value.value.status == .cancelled) {
                try self.store().terminate(a, query_key, claim, .failed, value.value.failure orelse "TaskFailed");
                return true;
            }
            if (value.value.status == .succeeded) continue;
            try pending.append(a, cursor);
            if (value.value.status != .running or value.value.until_ms +| 30_000 < self.clock.now()) try tickets.append(a, .{ .tenant = self.tenant, .query = query, .stage = current.value.stage, .task = cursor });
        }
        // Persist the dispatch intent first. Lost acknowledgements and process
        // loss replay only this bounded set, without relying on queue delivery.
        if (pending.items.len == 0 and cursor == count) {
            var boundaries: ?[]const u16 = null;
            if (current.value.stage == 0 and spec.strategy == .shuffle) {
                // First persist the completed scan cursor, then checkpoint at
                // most sixteen authenticated map directories per turn.
                try self.store().checkpointTasks(a, query_key, claim, 0, cursor, &.{});
                const resolution = try self.coalesce(a, spec, query, current.value.adaptive);
                switch (resolution) {
                    .pending => |output| {
                        try self.store().saveAdaptive(a, query_key, claim, output);
                        return false;
                    },
                    .done => |value| boundaries = value,
                }
            }
            try self.store().checkpointTasksAndReducers(a, query_key, claim, current.value.stage + 1, 0, &.{}, boundaries);
        } else try self.store().checkpointTasks(a, query_key, claim, current.value.stage, cursor, pending.items);
        if (tickets.items.len != 0) try dispatcher.dispatch(dispatcher.ptr, tickets.items, self.context);
        return false;
    }
    /// Invoke only after authenticating the internal caller. Tickets contain no
    /// caller-controlled scan URLs or credentials; the shared plan is authority.
    pub fn execute(self: Service, ticket: plan.Ticket) !void {
        try self.requireQueryRole();
        if (!std.mem.eql(u8, &ticket.tenant, &self.tenant)) return error.SqlExecutionUnauthorized;
        var task_memory: local.sql_memory_budget = .{ .backing = self.a, .limit = @intCast(self.task_memory_ceiling) };
        const task_allocator = task_memory.allocator();
        defer std.debug.assert(task_memory.live == 0);
        var arena = std.heap.ArenaAllocator.init(task_allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const query_key = try self.store().key(a, ticket.query, null, 0);
        var query = (try self.store().load(a, query_key)) orelse return error.SqlExecutionNotFound;
        defer query.deinit(a);
        if (query.value.terminal()) return;
        if (query.value.stage != ticket.stage or !std.mem.eql(u8, &query.value.plan, &ticket.query)) return error.SqlExecutionFenced;
        const key = try self.store().key(a, ticket.query, ticket.stage, ticket.task);
        const claim_started = self.clock.now();
        const claim = (try self.store().claim(a, key, self.owner, 60_000)) orelse return;
        var context = self.context;
        const remaining = @min(query.value.expires_ms -| self.clock.now(), 55_000 -| (self.clock.now() -| claim_started));
        const deadline = @import("antfly_platform").time.monotonicNs() +| remaining *| std.time.ns_per_ms;
        context.deadline_ns = if (context.deadline_ns) |parent| @min(parent, deadline) else deadline;
        var scoped_service = self;
        scoped_service.context = context;
        var spec = scoped_service.loadPlan(a, ticket.query) catch |err| {
            if (err == error.OutOfMemory and task_memory.exhausted) {
                try self.store().terminate(self.a, key, claim, .failed, "SqlJoinBudgetExceeded");
                return;
            }
            return err;
        };
        spec = try spec.withReducers(query.value.reduce_boundaries);
        const task = try taskFor(spec, ticket.stage, ticket.task);
        if (task_memory.live > spec.limits.task_memory) {
            try self.store().terminate(self.a, key, claim, .failed, "SqlJoinBudgetExceeded");
            return;
        }
        task_memory.limit = @intCast(spec.limits.task_memory);
        if (claim.epoch > spec.limits.max_attempts) {
            try self.store().terminate(self.a, key, claim, .failed, "TaskRetryBudgetExceeded");
            return;
        }
        const inputs = scoped_service.loadDependencies(a, spec, ticket.query, ticket.stage, ticket.task) catch |err| {
            if (err == error.OutOfMemory and task_memory.exhausted) {
                try self.store().terminate(self.a, key, claim, .failed, "SqlJoinBudgetExceeded");
                return;
            }
            return err;
        };
        var objects = self.exchange_store;
        objects.context = context;
        var task_spill: ?local.sql_spill.Manager = if (self.spill) |parent| parent.scoped(task_allocator) else null;
        defer if (task_spill) |*manager| manager.deinit();
        if (task_spill) |*manager| {
            manager.max_bytes = @min(manager.max_bytes, spec.limits.task_spill_bytes);
            manager.context = &context;
            manager.checkpoint = struct {
                fn check(raw: *anyopaque) !void {
                    const value: *const Context = @ptrCast(@alignCast(raw));
                    try value.ensureActive();
                }
            }.check;
        }
        const executor: runner.Runner = .{ .a = task_allocator, .source = self.source, .exchange_store = objects, .context = context, .spill = if (task_spill) |*manager| manager else null };
        const output = executor.run(ticket.query, spec, task, inputs) catch |raw_error| {
            const err = if (raw_error == error.OutOfMemory and task_memory.exhausted) error.SqlJoinBudgetExceeded else raw_error;
            switch (err) {
                error.SqlJoinBudgetExceeded, error.SqlProgramLimitExceeded, error.InvalidSqlBackendResponse, error.InvalidSqlExchange, error.SqlExecutionUnauthorized => {
                    std.log.warn("distributed SQL task failed query={s} stage={d} task={d} err={s}", .{ ticket.query, ticket.stage, ticket.task, @errorName(err) });
                    try self.store().terminate(self.a, key, claim, .failed, @errorName(err));
                    return;
                },
                else => return err,
            }
        };
        defer task_allocator.free(output.key);
        try context.ensureActive();
        // Uses a fresh wall-clock reading, never the claim's stale timestamp.
        try scoped_service.store().complete(self.a, key, claim, output);
    }
    /// Resolve adjacent partitions once from accepted map statistics. The
    /// boundary list is committed with the stage transition under the same CAS.
    /// Worker count and subsequent coordinator placement cannot change it.
    const Adaptive = struct { cursor: u32 = 0, bytes: []u64, rows: []u64 };
    const Resolution = union(enum) { pending: state.Output, done: []const u16 };
    fn coalesce(self: Service, a: A, spec: plan.Plan, query: state.Digest, previous: ?state.Output) !Resolution {
        var progress: Adaptive = if (previous) |output| blk: {
            var client = self.exchange_store.client;
            client.allocator = a;
            var object = try client.getObject(self.exchange_store.bucket, output.key, .{ .skip_metadata_probe = true, .max_response_bytes = 128 * 1024, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) });
            defer object.deinit(a);
            if (!std.mem.eql(u8, &digest(object.body), &output.hash)) return error.InvalidSqlExchange;
            break :blk try std.json.parseFromSliceLeaky(Adaptive, a, object.body, .{ .allocate = .alloc_always });
        } else blk: {
            const bytes = try a.alloc(u64, spec.partitions);
            const rows = try a.alloc(u64, spec.partitions);
            @memset(bytes, 0);
            @memset(rows, 0);
            break :blk .{ .bytes = bytes, .rows = rows };
        };
        const bytes = progress.bytes;
        const rows = progress.rows;
        const count = try spec.stageTasks(0);
        if (bytes.len != spec.partitions or rows.len != spec.partitions or progress.cursor > count) return error.InvalidSqlExecutionState;
        const end = @min(count, progress.cursor + 16);
        for (progress.cursor..end) |id| {
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            const sa = scratch.allocator();
            const key = try self.store().key(sa, query, 0, @intCast(id));
            var value = (try self.store().load(sa, key)) orelse return error.InvalidSqlExecutionState;
            defer value.deinit(sa);
            if (value.value.status != .succeeded) return error.InvalidSqlExecutionState;
            var objects = self.exchange_store;
            objects.context = self.context;
            const directory = try objects.footer(sa, value.value.output.?);
            if (directory.partitions.len != spec.partitions) return error.InvalidSqlExchange;
            for (directory.partitions, bytes, rows) |partition, *size, *row_count| {
                size.* +|= partition.retained_bytes;
                row_count.* +|= partition.rows;
            }
        }
        progress.cursor = end;
        if (end != count) {
            const encoded = try std.json.Stringify.valueAlloc(a, progress, .{});
            const hash = digest(encoded);
            const key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.adaptive.json", .{ self.exchange_store.prefix, spec.expires_ms, query, hash });
            var writer = self;
            writer.state_store.client = self.exchange_store.client;
            writer.state_store.bucket = self.exchange_store.bucket;
            try writer.immutable(a, key, encoded, 128 * 1024);
            return .{ .pending = .{ .key = key, .hash = hash, .bytes = encoded.len } };
        }
        var boundaries: std.ArrayList(u16) = .empty;
        try boundaries.append(a, 0);
        var total_bytes: u64 = 0;
        var total_rows: u64 = 0;
        for (bytes, rows, 0..) |size, row_count, index| {
            // Reserve ample headroom for hash metadata, output and decoding.
            if (index > boundaries.items[boundaries.items.len - 1] and (total_bytes +| size > spec.limits.task_memory / 8 or total_rows +| row_count > spec.limits.task_scan_rows / 2)) {
                try boundaries.append(a, @intCast(index));
                total_bytes = 0;
                total_rows = 0;
            }
            total_bytes +|= size;
            total_rows +|= row_count;
        }
        try boundaries.append(a, spec.partitions);
        return .{ .done = boundaries.items };
    }
    fn loadDependencies(self: Service, a: A, spec: plan.Plan, query: state.Digest, stage: u32, task: u32) ![]const state.Output {
        var dependencies: std.ArrayList(state.Output) = .empty;
        if (stage >= 1) {
            const previous = stage - 1;
            const count = try tasks(spec, previous);
            const begin = if (stage >= 2) task * 32 else 0;
            const end = if (stage >= 2) @min(count, begin + 32) else count;
            for (begin..end) |id| {
                const dependency_key = try self.store().key(a, query, previous, @intCast(id));
                var dependency = (try self.store().load(a, dependency_key)) orelse return error.InvalidSqlExecutionState;
                defer dependency.deinit(a);
                if (dependency.value.status != .succeeded) return error.InvalidSqlExecutionState;
                var output = dependency.value.output.?;
                output.key = try a.dupe(u8, output.key);
                try dependencies.append(a, output);
            }
        }
        return dependencies.items;
    }
    pub fn turn(self: Service, query: state.Digest, dispatcher: Dispatcher) !bool {
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        var current = (try self.store().load(self.a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(self.a);
        if (current.value.status == .succeeded) return true;
        if (current.value.status == .failed or current.value.status == .cancelled) return error.SqlExecutionFailed;
        const claim: state.Claim = if (current.value.status == .running and std.mem.eql(u8, current.value.owner, self.owner) and current.value.until_ms > self.clock.now()) .{ .owner = self.owner, .epoch = current.value.epoch } else (try self.acquire(query)) orelse return false;
        return self.tick(query, claim, dispatcher);
    }
    pub fn cancel(self: Service, query: state.Digest, claim: state.Claim) !void {
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        try self.store().terminate(self.a, key, claim, .cancelled, "Cancelled");
    }
    pub fn pollResult(self: Service, a: A, query: state.Digest) !?Result {
        const key = try self.store().key(a, query, null, 0);
        defer a.free(key);
        var current = (try self.store().load(a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(a);
        if (current.value.expires_ms <= self.clock.now()) return error.SqlQueryExpired;
        if (current.value.status == .failed) {
            if (current.value.failure) |failure| {
                if (std.mem.eql(u8, failure, "SqlJoinBudgetExceeded") or std.mem.eql(u8, failure, "SqlProgramLimitExceeded")) return error.SqlJoinBudgetExceeded;
            }
            return error.SqlExecutionFailed;
        }
        if (current.value.status == .cancelled) return error.Canceled;
        if (current.value.status != .succeeded) return null;
        const output = current.value.output.?;
        var client = self.exchange_store.client;
        client.allocator = a;
        var object = try client.getObject(self.exchange_store.bucket, output.key, .{ .skip_metadata_probe = true, .max_response_bytes = 4 * 1024 * 1024, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) });
        defer object.deinit(a);
        if (!std.mem.eql(u8, &digest(object.body), &output.hash)) return error.InvalidSqlExchange;
        const value = try std.json.parseFromSliceLeaky(Result, a, object.body, .{ .allocate = .alloc_always });
        if (value.version != 1 or !std.mem.eql(u8, &value.query, &query) or value.rows != output.rows) return error.InvalidSqlExchange;
        return value;
    }
};
