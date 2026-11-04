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
const physical = @import("physical.zig");
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
    /// Resolve and validate every authorized input capability and retain its
    /// base through spec.expires_ms. Accepted WAL images belong to the durable
    /// capability. Retention survives coordinator loss without periodic renewal.
    ensure: *const fn (*anyopaque, A, plan.Plan, Context) anyerror!void,
};
pub const Dispatcher = struct {
    ptr: *anyopaque,
    /// Bounded fan-out; delivery may duplicate or lose a response. Replaying
    /// deterministic tickets recovers dispatch without an external queue.
    dispatch: *const fn (*anyopaque, []const plan.Ticket, Context) anyerror!void,
};
pub const Result = struct { version: u16 = 2, query: state.Query, pages: []const state.Output = &.{}, manifests: []const state.Output = &.{}, rows: u64 };
pub const Factory = struct {
    ptr: *anyopaque,
    /// Returns a borrowed process-owned service for this authenticated scope.
    /// owner is a unique boot incarnation; stores must be scope-isolated.
    open: *const fn (*anyopaque, A, plan.Digest, Context) anyerror!Service,
    dispatcher: Dispatcher,
};
/// Best-effort local scheduling hint. Durable records remain authoritative;
/// notifications must not allocate or make an accepted write fail.
pub const Notification = struct {
    ptr: *anyopaque,
    notify: *const fn (*anyopaque, state.Digest, state.Query) void,
};
/// Process-local exclusion supplements durable epoch fencing: foreground and
/// recovery turns must not race using the same boot owner and lease epoch.
pub const Coordination = struct {
    ptr: *anyopaque,
    enter: *const fn (*anyopaque, state.Query) bool,
    leave: *const fn (*anyopaque, state.Query) void,
    delivery_pending: *const fn (*anyopaque, state.Query) bool,
    delivery_ready: *const fn (*anyopaque, state.Query) bool,
    retry: *const fn (*anyopaque, state.Query) void,
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
    notification: ?Notification = null,
    coordination: ?Coordination = null,
    fn notify(self: Service, query: state.Query) void {
        if (self.notification) |notification| notification.notify(notification.ptr, self.tenant, query);
    }
    fn store(self: Service) state.Store {
        var result = self.state_store;
        result.now_ms = self.clock.now();
        result.tenant = self.tenant;
        result.context = self.context;
        return result;
    }
    fn authorize(self: Service, a: A, query: state.Query, record: state.Record) !void {
        if (!std.meta.eql(record.plan, query.id) or record.expires_ms != query.expires_ms) return error.InvalidSqlExecutionState;
        if (record.tenant) |tenant| {
            if (!std.meta.eql(tenant, self.tenant)) return error.SqlExecutionUnauthorized;
        } else {
            // Compatibility is limited to pre-scope jobs' immutable lifetime.
            // Newly admitted jobs use only the compact record on polling paths.
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            _ = try self.loadPlan(arena.allocator(), query);
        }
    }
    fn requireQueryRole(self: Service) !void {
        if (self.role != .query_only and self.role != .combined) return error.SqlQueryRoleRequired;
    }
    fn planKey(self: Service, a: A, query: state.Query) ![]const u8 {
        try query.validate();
        return std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/plan.json", .{ self.state_store.prefix, query.expires_ms, query.id });
    }
    fn immutable(self: Service, a: A, key: []const u8, bytes: []const u8, limit: usize) !void {
        if (bytes.len > limit) return error.SqlJoinBudgetExceeded;
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
    pub fn submit(self: Service, specification: plan.Plan) !state.Query {
        try self.requireQueryRole();
        try specification.validate(self.clock.now());
        if (specification.reduce_boundaries.len != 0 or specification.skew.len != 0) return error.InvalidSqlDistributedPlan;
        if (specification.limits.task_memory > self.task_memory_ceiling) return error.SqlJoinBudgetExceeded;
        if (!std.mem.eql(u8, &self.tenant, &specification.tenant)) return error.SqlExecutionUnauthorized;
        try self.retention.ensure(self.retention.ptr, self.a, specification, self.context);
        const bytes = try specification.encode(self.a);
        defer self.a.free(bytes);
        if (bytes.len > 1024 * 1024) return error.SqlJoinBudgetExceeded;
        const query: state.Query = .{ .id = digest(bytes), .expires_ms = specification.expires_ms };
        const key = try self.planKey(self.a, query);
        defer self.a.free(key);
        // Expiry index precedes every durable write so failed submissions are
        // discoverable without listing every active query or exchange object.
        const expiry_key = try std.fmt.allocPrint(self.a, "{s}/sql-query-expiry/{d:0>20}/{s}.json", .{ self.state_store.prefix, specification.expires_ms, query.id });
        defer self.a.free(expiry_key);
        try self.immutable(self.a, expiry_key, "{}", 1024);
        try self.immutable(self.a, key, bytes, 1024 * 1024);
        const query_key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(query_key);
        try self.store().create(self.a, query_key, query.id, specification.expires_ms);
        self.notify(query);
        return query;
    }
    pub fn loadPlan(self: Service, a: A, query: state.Query) !plan.Plan {
        const key = try self.planKey(a, query);
        defer a.free(key);
        var client = self.state_store.client;
        client.allocator = a;
        var object = try client.getObject(self.state_store.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) });
        defer object.deinit(a);
        if (!std.mem.eql(u8, &digest(object.body), &query.id)) return error.InvalidSqlDistributedPlan;
        const specification = try std.json.parseFromSliceLeaky(plan.Plan, a, object.body, .{ .allocate = .alloc_always });
        if (specification.expires_ms != query.expires_ms) return error.InvalidSqlDistributedPlan;
        if (!std.mem.eql(u8, &self.tenant, &specification.tenant)) return error.SqlExecutionUnauthorized;
        try specification.validate(self.clock.now());
        if (specification.limits.task_memory > self.task_memory_ceiling) return error.SqlJoinBudgetExceeded;
        return specification;
    }
    pub fn acquire(self: Service, query: state.Query) !?state.Claim {
        try self.requireQueryRole();
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        var current = (try self.store().load(self.a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(self.a);
        try self.authorize(self.a, query, current.value);
        return self.store().claim(self.a, key, self.owner, 60_000);
    }
    fn tasks(spec: plan.Plan, stage: u32) !u32 {
        return spec.stageTasks(stage);
    }
    fn taskFor(spec: plan.Plan, stage: u32, task: u32) !plan.Task {
        if (task >= try tasks(spec, stage)) return error.InvalidSqlDistributedPlan;
        if (stage > spec.outputStage()) return .{ .stage = stage, .task = task, .kind = .aggregate, .finalize = stage + 1 == spec.finalStage() };
        if (spec.skew.len != 0 and stage == spec.outputStage()) {
            const pairs = try physical.pairCount(spec.skew, spec.reduceCount());
            if (task < pairs) return .{ .stage = stage, .task = task, .kind = .passthrough, .dependent = task };
            var next = task - pairs;
            for (spec.skew, 0..) |part, index| {
                if (spec.join.preservesProbe() or spec.join.kind == .semi) {
                    if (next < part.leftCount()) return .{ .stage = stage, .task = task, .kind = .finalize_probe, .physical_part = @intCast(index), .tile_left = next };
                    next -= @intCast(part.leftCount());
                }
                if (spec.join.preservesBuild()) {
                    if (next < part.rightCount()) return .{ .stage = stage, .task = task, .kind = .finalize_build, .physical_part = @intCast(index), .tile_right = next };
                    next -= @intCast(part.rightCount());
                }
            }
            return error.InvalidSqlExecutionState;
        }
        if (spec.skew.len != 0 and stage > spec.joinStage() and stage < spec.outputStage()) {
            var next = task;
            for (spec.skew, 0..) |part, index| {
                inline for (.{ false, true }) |build| {
                    if (if (build) spec.join.preservesBuild() else (spec.join.preservesProbe() or spec.join.kind == .semi)) {
                        const groups = plan.Plan.witnessGroups(if (build) part.leftCount() else part.rightCount(), stage - spec.joinStage());
                        const tiles = if (build) part.rightCount() else part.leftCount();
                        const count = try std.math.mul(u32, groups, tiles);
                        if (next < count) return .{ .stage = stage, .task = task, .kind = if (build) .merge_build_matches else .merge_probe_matches, .physical_part = @intCast(index), .tile_left = if (build) 0 else next / groups, .tile_right = if (build) next / groups else 0, .witness_group = next % groups };
                        next -= count;
                    }
                }
            }
            return error.InvalidSqlExecutionState;
        }
        if (spec.skew.len != 0 and stage == spec.joinStage()) {
            switch (try physical.work(spec.skew, spec.reduce_boundaries, spec.reduceCount(), task)) {
                .pair => |pair| return .{ .stage = stage, .task = task, .kind = .tile_pair, .physical_part = pair.part, .tile_left = pair.left, .tile_right = pair.right },
                .reduce => |reducer| return .{ .stage = stage, .task = task, .kind = .reduce, .reducer = try spec.ordinaryOrdinal(reducer), .build_count = if (spec.inputLevels() != 0) 1 else null, .partition = spec.reduce_boundaries[reducer], .partition_end = spec.reduce_boundaries[reducer + 1] },
            }
        }
        if (stage > 0 and stage < spec.joinStage()) {
            const right = plan.Plan.inputGroups(spec.right.splits, stage);
            const left = if (spec.strategy == .broadcast) 0 else plan.Plan.inputGroups(spec.left.splits, stage);
            const ordinal = task / (right + left);
            const group = task % (right + left);
            const reducer = if (spec.strategy == .broadcast) 0 else try spec.ordinaryReducer(ordinal);
            return .{ .stage = stage, .task = task, .kind = .consolidate, .build = group < right, .dependent = if (group < right) group else group - right, .reducer = ordinal, .partition = if (spec.reduce_boundaries.len == 0) @intCast(reducer) else spec.reduce_boundaries[reducer], .partition_end = if (spec.reduce_boundaries.len == 0) @intCast(reducer + 1) else spec.reduce_boundaries[reducer + 1] };
        }
        if (spec.strategy == .bucket) return .{ .stage = stage, .task = task, .kind = .bucket_join, .partition = @intCast(task), .partition_end = @intCast(task + 1) };
        if (stage == 0) return .{ .stage = stage, .task = task, .kind = if (spec.strategy == .broadcast) .scan_build else if (task < spec.right.splits) .map_right else .map_left, .split = if (task < spec.right.splits) task else task - spec.right.splits };
        return .{ .stage = stage, .task = task, .kind = if (spec.strategy == .broadcast) .broadcast_probe else .reduce, .reducer = if (spec.strategy == .broadcast) 0 else task, .build_count = if (spec.inputLevels() != 0) 1 else null, .split = task, .partition = if (spec.strategy == .shuffle) (if (spec.reduce_boundaries.len != 0) spec.reduce_boundaries[task] else @intCast(task)) else 0, .partition_end = if (spec.strategy == .shuffle) (if (spec.reduce_boundaries.len != 0) spec.reduce_boundaries[task + 1] else @intCast(task + 1)) else 1 };
    }
    /// One bounded coordinator turn. A caller schedules subsequent turns while
    /// pending, renews ownership, and may move ownership to any query pod.
    pub fn tick(self: Service, query: state.Query, claim: state.Claim, dispatcher: Dispatcher) !bool {
        try self.requireQueryRole();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var spec = try self.loadPlan(a, query);
        const query_key = try self.store().key(a, query, null, 0);
        try self.store().renew(a, query_key, claim, 60_000);
        var current = (try self.store().load(a, query_key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(a);
        if (current.value.cancel_requested) {
            try self.store().terminate(a, query_key, claim, .cancelled, "Cancelled");
            return true;
        }
        // submit() retained both inputs through the immutable query expiry.
        // Workers revalidate their scanned capability on open. Reducer/control
        // turns consume accepted exchange, without reopening source catalogs.
        spec = self.resolvePhysical(a, spec, current.value) catch |err| {
            if (err != error.SqlJoinBudgetExceeded) return err;
            try self.store().terminate(a, query_key, claim, .failed, "SqlJoinBudgetExceeded");
            return true;
        };
        if (current.value.stage == spec.finalStage()) {
            return self.assembleResult(a, spec, query, query_key, claim, current.value.result_progress);
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
            if (try self.store().ensure(a, key, query.id, spec.expires_ms)) {
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
            var resolved: ?state.Output = null;
            if (current.value.stage == 0 and spec.strategy == .shuffle) {
                // First persist the completed scan cursor, then checkpoint at
                // most sixteen authenticated map directories per turn.
                try self.store().checkpointTasks(a, query_key, claim, 0, cursor, &.{});
                const resolution = self.coalesce(a, spec, query, current.value.adaptive) catch |err| {
                    if (err != error.SqlJoinBudgetExceeded) return err;
                    try self.store().terminate(a, query_key, claim, .failed, "SqlJoinBudgetExceeded");
                    return true;
                };
                switch (resolution) {
                    .pending => |output| {
                        try self.store().saveAdaptive(a, query_key, claim, output);
                        return false;
                    },
                    .done => |value| {
                        resolved = value.reference;
                        _ = spec.withPhysical(value.index.boundaries, value.index.skew) catch |err| {
                            if (err != error.SqlJoinBudgetExceeded) return err;
                            try self.store().terminate(a, query_key, claim, .failed, "SqlJoinBudgetExceeded");
                            return true;
                        };
                    },
                }
            }
            try self.store().checkpointResolved(a, query_key, claim, current.value.stage + 1, 0, &.{}, null, null, resolved);
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
        try self.authorize(a, ticket.query, query.value);
        if (query.value.terminal()) return;
        if (query.value.stage != ticket.stage or !std.mem.eql(u8, &query.value.plan, &ticket.query.id)) return error.SqlExecutionFenced;
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
        spec = try scoped_service.resolvePhysical(a, spec, query.value);
        const task = try scoped_service.hydrateTask(a, spec, try taskFor(spec, ticket.stage, ticket.task));
        if (task_memory.live > spec.limits.task_memory) {
            try self.store().terminate(self.a, key, claim, .failed, "SqlJoinBudgetExceeded");
            return;
        }
        task_memory.limit = @intCast(spec.limits.task_memory);
        if (claim.epoch > spec.limits.max_attempts) {
            try self.store().terminate(self.a, key, claim, .failed, "TaskRetryBudgetExceeded");
            return;
        }
        const inputs = scoped_service.loadDependencies(a, spec, ticket.query, task) catch |err| {
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
        const output = executor.run(ticket.query.id, spec, task, inputs) catch |raw_error| {
            const err = if (raw_error == error.OutOfMemory and task_memory.exhausted) error.SqlJoinBudgetExceeded else raw_error;
            switch (err) {
                error.SqlJoinBudgetExceeded, error.SqlProgramLimitExceeded, error.InvalidSqlBackendResponse, error.InvalidSqlExchange, error.SqlExecutionUnauthorized => {
                    std.log.warn("distributed SQL task failed query={s} stage={d} task={d} err={s}", .{ ticket.query.id, ticket.stage, ticket.task, @errorName(err) });
                    try self.store().terminate(self.a, key, claim, .failed, @errorName(err));
                    return;
                },
                else => return err,
            }
        };
        defer task_allocator.free(output.key);
        defer if (output.matches) |witness| task_allocator.free(witness.key);
        try context.ensureActive();
        // Uses a fresh wall-clock reading, never the claim's stale timestamp.
        try scoped_service.store().complete(self.a, key, claim, output);
    }
    // Mutable CAS records hold only hashes/references. Statistics are fixed by
    // virtual partition count; tile pages are immutable and capped at 128 rows.
    const Adaptive = struct {
        cursor: u32 = 0,
        bytes: []u64,
        rows: []u64,
        right_bytes: []u64,
        left_counts: []u32,
        right_counts: []u32,
        boundaries: []const u16 = &.{},
        parts: []physical.Part = &.{},
        part: u16 = 0,
        map: u32 = 0,
        left_buffer: []const physical.Tile = &.{},
        right_buffer: []const physical.Tile = &.{},
    };
    const Resolution = union(enum) { pending: state.Output, done: struct { reference: state.Output, index: physical.Index } };
    fn putMetadata(self: Service, a: A, query: state.Query, label: []const u8, bytes: []const u8, limit: usize) !state.Output {
        if (bytes.len > limit) return error.SqlJoinBudgetExceeded;
        const hash = digest(bytes);
        const key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.{s}.json", .{ self.exchange_store.prefix, query.expires_ms, query.id, hash, label });
        var writer = self;
        writer.state_store.client = self.exchange_store.client;
        writer.state_store.bucket = self.exchange_store.bucket;
        try writer.immutable(a, key, bytes, limit);
        return .{ .key = key, .hash = hash, .bytes = bytes.len };
    }
    fn getMetadata(self: Service, a: A, ref: state.Output, limit: usize) ![]const u8 {
        if (ref.bytes > limit) return error.SqlJoinBudgetExceeded;
        var client = self.exchange_store.client;
        client.allocator = a;
        var object = try client.getObject(self.exchange_store.bucket, ref.key, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = local.serverless_external_source_mod.lake_catalog.types.contextCancellation(&self.context) });
        defer object.deinit(a);
        if (object.body.len != ref.bytes or !std.mem.eql(u8, &digest(object.body), &ref.hash)) return error.InvalidSqlExchange;
        return a.dupe(u8, object.body);
    }
    pub fn resolvePhysical(self: Service, a: A, spec: plan.Plan, record: state.Record) !plan.Plan {
        if (record.physical) |ref| {
            const index = try std.json.parseFromSliceLeaky(physical.Index, a, try self.getMetadata(a, ref, physical.max_index_bytes), .{ .allocate = .alloc_always });
            return spec.withPhysical(index.boundaries, index.skew);
        }
        return spec.withPhysical(record.reduce_boundaries, record.skew);
    }
    fn mapDirectory(self: Service, a: A, query: state.Query, id: u32) !exchange.Footer {
        const ref = try self.loadAccepted(a, query, 0, id);
        var objects = self.exchange_store;
        objects.context = self.context;
        return objects.footer(a, ref);
    }
    fn loadTile(self: Service, a: A, part: physical.Part, build: bool, ordinal: u32) !physical.Tile {
        const inline_tiles = if (build) part.right else part.left;
        if (inline_tiles.len != 0) {
            if (ordinal >= inline_tiles.len) return error.InvalidSqlExecutionState;
            return inline_tiles[ordinal];
        }
        const pages = if (build) part.right_pages else part.left_pages;
        const page_id = ordinal / physical.page_tiles;
        if (page_id >= pages.len) return error.InvalidSqlExecutionState;
        const page = pages[page_id];
        if (page.first > ordinal or ordinal - page.first >= page.count or page.count > physical.page_tiles) return error.InvalidSqlExecutionState;
        const tiles = try std.json.parseFromSliceLeaky([]const physical.Tile, a, try self.getMetadata(a, page.output, 8192), .{ .allocate = .alloc_always });
        if (tiles.len != page.count) return error.InvalidSqlExecutionState;
        return tiles[ordinal - page.first];
    }
    fn hydrateTask(self: Service, a: A, spec: plan.Plan, original: plan.Task) !plan.Task {
        var task = original;
        if (task.kind == .tile_pair or task.kind == .finalize_probe or task.kind == .merge_probe_matches) task.left_tile = try self.loadTile(a, spec.skew[task.physical_part], false, task.tile_left);
        if (task.kind == .tile_pair or task.kind == .finalize_build or task.kind == .merge_build_matches) task.right_tile = try self.loadTile(a, spec.skew[task.physical_part], true, task.tile_right);
        return task;
    }
    fn flushTiles(self: Service, a: A, query: state.Query, part: *physical.Part, build: bool, buffer: *[]const physical.Tile) !void {
        if (buffer.len == 0) return;
        const pages = if (build) &part.right_pages else &part.left_pages;
        const first: u32 = if (pages.len == 0) 0 else pages.*[pages.len - 1].first + pages.*[pages.len - 1].count;
        const ref = try self.putMetadata(a, query, "tiles", try std.json.Stringify.valueAlloc(a, buffer.*, .{}), 8192);
        const appended = try a.alloc(physical.TilePage, pages.len + 1);
        @memcpy(appended[0..pages.len], pages.*);
        appended[pages.len] = .{ .first = first, .count = @intCast(buffer.len), .output = ref };
        pages.* = appended;
        buffer.* = &.{};
    }
    fn coalesce(self: Service, a: A, spec: plan.Plan, query: state.Query, previous: ?state.Output) !Resolution {
        var progress: Adaptive = if (previous) |ref|
            try std.json.parseFromSliceLeaky(Adaptive, a, try self.getMetadata(a, ref, physical.max_index_bytes), .{ .allocate = .alloc_always })
        else blk: {
            const bytes = try a.alloc(u64, spec.partitions);
            @memset(bytes, 0);
            const rows = try a.alloc(u64, spec.partitions);
            @memset(rows, 0);
            const right_bytes = try a.alloc(u64, spec.partitions);
            @memset(right_bytes, 0);
            const left_counts = try a.alloc(u32, spec.partitions);
            @memset(left_counts, 0);
            const right_counts = try a.alloc(u32, spec.partitions);
            @memset(right_counts, 0);
            break :blk .{ .bytes = bytes, .rows = rows, .right_bytes = right_bytes, .left_counts = left_counts, .right_counts = right_counts };
        };
        const count = try spec.stageTasks(0);
        if (progress.bytes.len != spec.partitions or progress.rows.len != spec.partitions or progress.right_bytes.len != spec.partitions or progress.left_counts.len != spec.partitions or progress.right_counts.len != spec.partitions or progress.cursor > count) return error.InvalidSqlExecutionState;
        if (progress.boundaries.len == 0) {
            const end = @min(count, progress.cursor + 16);
            for (progress.cursor..end) |id| {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const directory = try self.mapDirectory(arena.allocator(), query, @intCast(id));
                if (directory.partitions.len != spec.partitions) return error.InvalidSqlExchange;
                for (directory.partitions, 0..) |partition, index| {
                    progress.bytes[index] +|= partition.retained_bytes;
                    progress.rows[index] +|= partition.rows;
                    if (id < spec.right.splits) progress.right_bytes[index] +|= partition.retained_bytes;
                    if (partition.rows != 0) {
                        if (id < spec.right.splits) progress.right_counts[index] += 1 else progress.left_counts[index] += 1;
                    }
                }
            }
            progress.cursor = end;
            if (end == count) {
                var boundaries: std.ArrayList(u16) = .empty;
                try boundaries.append(a, 0);
                var total_bytes: u64 = 0;
                var total_rows: u64 = 0;
                for (progress.bytes, progress.rows, 0..) |size, row_count, index| {
                    if (index > boundaries.items[boundaries.items.len - 1] and (total_bytes +| size > spec.limits.task_memory / 8 or total_rows +| row_count > spec.limits.task_scan_rows / 2)) {
                        try boundaries.append(a, @intCast(index));
                        total_bytes = 0;
                        total_rows = 0;
                    }
                    total_bytes +|= size;
                    total_rows +|= row_count;
                }
                try boundaries.append(a, spec.partitions);
                progress.boundaries = boundaries.items;
                var parts: std.ArrayList(physical.Part) = .empty;
                for (0..spec.partitions) |index| {
                    if (progress.bytes[index] <= spec.limits.task_memory / 8 and progress.rows[index] <= spec.limits.task_scan_rows / 2) continue;
                    const left = progress.left_counts[index];
                    const right = progress.right_counts[index];
                    try parts.append(a, .{ .partition = @intCast(index), .left_count = if (left == 0) null else left, .right_count = if (right == 0) null else right, .left = if (left == 0) &.{.{}} else &.{}, .right = if (right == 0) &.{.{}} else &.{} });
                }
                progress.parts = parts.items;
                _ = spec.withPhysical(progress.boundaries, progress.parts) catch |err| switch (err) {
                    error.Overflow => return error.SqlJoinBudgetExceeded,
                    else => return err,
                };
            }
        } else if (progress.part < progress.parts.len) {
            const part = &progress.parts[progress.part];
            const end = @min(count, progress.map + 16);
            for (progress.map..end) |id| {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const directory = try self.mapDirectory(arena.allocator(), query, @intCast(id));
                const rows = directory.partitions[part.partition].rows;
                if (rows == 0) continue;
                const build = id < spec.right.splits;
                const buffer = if (build) &progress.right_buffer else &progress.left_buffer;
                const appended = try a.alloc(physical.Tile, buffer.len + 1);
                @memcpy(appended[0..buffer.len], buffer.*);
                appended[buffer.len] = .{ .map = @intCast(id), .rows = rows };
                buffer.* = appended;
                if (buffer.len == physical.page_tiles) try self.flushTiles(a, query, part, build, buffer);
            }
            progress.map = end;
            if (end == count) {
                try self.flushTiles(a, query, part, false, &progress.left_buffer);
                try self.flushTiles(a, query, part, true, &progress.right_buffer);
                progress.part += 1;
                progress.map = 0;
            }
        }
        if (progress.boundaries.len != 0 and progress.part == progress.parts.len) {
            const index: physical.Index = .{ .boundaries = progress.boundaries, .skew = progress.parts };
            const ref = try self.putMetadata(a, query, "physical", try std.json.Stringify.valueAlloc(a, index, .{}), physical.max_index_bytes);
            return .{ .done = .{ .reference = ref, .index = index } };
        }
        return .{ .pending = try self.putMetadata(a, query, "adaptive", try std.json.Stringify.valueAlloc(a, progress, .{}), physical.max_index_bytes) };
    }
    pub const ResultPage = struct { pages: []const state.Output, rows: u64, previous: ?state.Output = null };
    const ResultProgress = struct { cursor: u32 = 0, rows: u64 = 0, head: ?state.Output = null };
    pub fn readResultPage(self: Service, a: A, ref: state.Output) !ResultPage {
        return std.json.parseFromSliceLeaky(ResultPage, a, try self.getMetadata(a, ref, 128 * 1024), .{ .allocate = .alloc_always });
    }
    fn assembleResult(self: Service, a: A, spec: plan.Plan, query: state.Query, query_key: []const u8, claim: state.Claim, previous: ?state.Output) !bool {
        var progress: ResultProgress = if (previous) |ref| try std.json.parseFromSliceLeaky(ResultProgress, a, try self.getMetadata(a, ref, physical.max_index_bytes), .{ .allocate = .alloc_always }) else .{};
        const count = try tasks(spec, spec.finalStage() - 1);
        if (progress.cursor > count) return error.InvalidSqlExecutionState;
        const end = @min(count, progress.cursor + 64);
        const pages = try a.alloc(state.Output, end - progress.cursor);
        var rows: u64 = 0;
        for (pages, progress.cursor..) |*page, task| {
            page.* = try self.loadAccepted(a, query, spec.finalStage() - 1, @intCast(task));
            page.matches = null;
            rows = try std.math.add(u64, rows, page.rows);
        }
        progress.rows = try std.math.add(u64, progress.rows, rows);
        if (count <= 64) return self.completeResult(a, query, query_key, claim, .{ .query = query, .pages = pages, .rows = progress.rows });
        const page_ref = try self.putMetadata(a, query, "result-page", try std.json.Stringify.valueAlloc(a, ResultPage{ .pages = pages, .rows = rows, .previous = progress.head }, .{}), 128 * 1024);
        progress.head = page_ref;
        progress.cursor = end;
        if (end == count) return self.completeResult(a, query, query_key, claim, .{ .query = query, .manifests = &.{page_ref}, .rows = progress.rows });
        const checkpoint = try self.putMetadata(a, query, "result-progress", try std.json.Stringify.valueAlloc(a, progress, .{}), physical.max_index_bytes);
        try self.store().saveResultProgress(a, query_key, claim, checkpoint);
        return false;
    }
    fn completeResult(self: Service, a: A, query: state.Query, query_key: []const u8, claim: state.Claim, result: Result) !bool {
        const bytes = try std.json.Stringify.valueAlloc(a, result, .{});
        const ref = try self.putMetadata(a, query, "result", bytes, physical.max_index_bytes);
        var output = ref;
        output.rows = result.rows;
        try self.store().complete(a, query_key, claim, output);
        return true;
    }
    fn loadAccepted(self: Service, a: A, query: state.Query, stage: u32, id: u32) !state.Output {
        const key = try self.store().key(a, query, stage, id);
        var loaded = (try self.store().load(a, key)) orelse return error.InvalidSqlExecutionState;
        defer loaded.deinit(a);
        if (loaded.value.status != .succeeded) return error.InvalidSqlExecutionState;
        var result = loaded.value.output.?;
        result.key = try a.dupe(u8, result.key);
        if (result.matches) |*witness| witness.key = try a.dupe(u8, witness.key);
        return result;
    }
    fn loadDependencies(self: Service, a: A, spec: plan.Plan, query: state.Query, descriptor: plan.Task) ![]const state.Output {
        const stage = descriptor.stage;
        const task = descriptor.task;
        if (descriptor.kind == .passthrough) {
            const refs = try a.alloc(state.Output, 1);
            refs[0] = try self.loadAccepted(a, query, spec.joinStage(), descriptor.dependent);
            return refs;
        }
        if (descriptor.kind == .tile_pair or descriptor.kind == .finalize_probe or descriptor.kind == .finalize_build or descriptor.kind == .merge_probe_matches or descriptor.kind == .merge_build_matches) {
            var refs: std.ArrayList(state.Output) = .empty;
            const part = spec.skew[descriptor.physical_part];
            if (descriptor.kind == .tile_pair) {
                if (descriptor.left_tile.map) |id| try refs.append(a, try self.loadAccepted(a, query, 0, id));
                if (descriptor.right_tile.map) |id| try refs.append(a, try self.loadAccepted(a, query, 0, id));
            } else {
                const build = descriptor.kind == .finalize_build or descriptor.kind == .merge_build_matches;
                const merging = descriptor.kind == .merge_probe_matches or descriptor.kind == .merge_build_matches;
                const tile = if (build) descriptor.right_tile else descriptor.left_tile;
                if (!merging) if (tile.map) |id| try refs.append(a, try self.loadAccepted(a, query, 0, id));
                const opposite = if (build) part.leftCount() else part.rightCount();
                const level = if (merging) descriptor.stage - spec.joinStage() - 1 else plan.Plan.witnessLevels(opposite);
                const groups = plan.Plan.witnessGroups(opposite, level);
                const begin = if (merging) descriptor.witness_group * 32 else 0;
                const end = if (merging) @min(groups, begin + 32) else groups;
                const ordinal = if (build) descriptor.tile_right else descriptor.tile_left;
                for (begin..end) |index| {
                    if (level == 0) {
                        const pair_id = try physical.pairId(spec.skew, spec.reduce_boundaries, spec.reduceCount(), .{ .part = descriptor.physical_part, .left = if (build) @intCast(index) else ordinal, .right = if (build) ordinal else @intCast(index) });
                        try refs.append(a, try self.loadAccepted(a, query, spec.joinStage(), pair_id));
                    } else {
                        const id = try spec.witnessOffset(descriptor.physical_part, build, level) + ordinal * groups + @as(u32, @intCast(index));
                        try refs.append(a, try self.loadAccepted(a, query, spec.joinStage() + level, id));
                    }
                }
            }
            return refs.items;
        }
        if (descriptor.kind == .consolidate or (stage == spec.joinStage() and spec.inputLevels() != 0 and descriptor.kind != .bucket_join)) {
            var refs: std.ArrayList(state.Output) = .empty;
            if (descriptor.kind == .consolidate) {
                const previous = stage - 1;
                const right = plan.Plan.inputGroups(spec.right.splits, previous);
                const left = if (spec.strategy == .broadcast) 0 else plan.Plan.inputGroups(spec.left.splits, previous);
                const begin = descriptor.dependent * 32;
                const end = @min(if (descriptor.build) right else left, begin + 32);
                const base = (if (previous == 0) @as(u32, 0) else descriptor.reducer * (right + left)) + (if (descriptor.build) @as(u32, 0) else right);
                for (begin..end) |id| try refs.append(a, try self.loadAccepted(a, query, previous, base + @as(u32, @intCast(id))));
            } else {
                const reducer = descriptor.reducer;
                const width: u32 = if (spec.strategy == .broadcast) 1 else 2;
                for (0..width) |id| try refs.append(a, try self.loadAccepted(a, query, stage - 1, reducer * width + @as(u32, @intCast(id))));
            }
            return refs.items;
        }
        var dependencies: std.ArrayList(state.Output) = .empty;
        if (stage >= 1) {
            const previous = stage - 1;
            const count = try tasks(spec, previous);
            const begin = if (stage > spec.outputStage()) task * 32 else 0;
            const end = if (stage > spec.outputStage()) @min(count, begin + 32) else count;
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
    pub fn turn(self: Service, query: state.Query, dispatcher: Dispatcher) !bool {
        if (self.coordination) |coordination| if (!coordination.enter(coordination.ptr, query)) return false;
        defer if (self.coordination) |coordination| coordination.leave(coordination.ptr, query);
        errdefer if (self.coordination) |coordination| coordination.retry(coordination.ptr, query);
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        var current = (try self.store().load(self.a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(self.a);
        try self.authorize(self.a, query, current.value);
        if (current.value.status == .succeeded) return true;
        if (current.value.status == .failed or current.value.status == .cancelled) return error.SqlExecutionFailed;
        // Native delivery completion wakes orchestration. While a bounded
        // dispatch window is outstanding, avoid rereading every task on each
        // foreground poll. Keep twenty seconds of lease margin and always let
        // durable cancellation through. Hosted dispatch has no local registry.
        if (!current.value.cancel_requested and current.value.pending.len != 0 and current.value.status == .running and std.mem.eql(u8, current.value.owner, self.owner) and current.value.until_ms > self.clock.now() +| 20_000) {
            if (self.coordination) |coordination| if (!coordination.delivery_ready(coordination.ptr, query)) return false;
        }
        const claim: state.Claim = if (current.value.status == .running and std.mem.eql(u8, current.value.owner, self.owner) and current.value.until_ms > self.clock.now()) .{ .owner = self.owner, .epoch = current.value.epoch } else (try self.store().claim(self.a, key, self.owner, 60_000)) orelse return false;
        self.notify(query);
        return self.tick(query, claim, dispatcher);
    }
    pub fn requestCancel(self: Service, query: state.Query) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const key = try self.store().key(a, query, null, 0);
        var current = (try self.store().load(a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(a);
        try self.authorize(a, query, current.value);
        try self.store().requestCancel(a, key);
        self.notify(query);
    }
    pub fn cancel(self: Service, query: state.Query, claim: state.Claim) !void {
        const key = try self.store().key(self.a, query, null, 0);
        defer self.a.free(key);
        var current = (try self.store().load(self.a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(self.a);
        try self.authorize(self.a, query, current.value);
        try self.store().terminate(self.a, key, claim, .cancelled, "Cancelled");
    }
    pub fn pollResult(self: Service, a: A, query: state.Query) !?Result {
        const key = try self.store().key(a, query, null, 0);
        defer a.free(key);
        var current = (try self.store().load(a, key)) orelse return error.SqlExecutionNotFound;
        defer current.deinit(a);
        try self.authorize(a, query, current.value);
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
        if (value.version != 2 or !std.meta.eql(value.query, query) or value.rows != output.rows) return error.InvalidSqlExchange;
        return value;
    }
};
