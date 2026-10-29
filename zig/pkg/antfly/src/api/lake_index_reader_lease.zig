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

//! Amortized native metadata reader sessions. Queries borrow one durable
//! per-process session; payload cache eviction never changes read authority.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Request = local.api_operation.RequestContext;
const A = std.mem.Allocator;
const Service = @import("../serverless/lake_renewal_service.zig").Service;
const Operation = @import("../serverless/provider_operation.zig").Operation;
pub const Authority = struct {
    ptr: *anyopaque,
    context: Request,
    read: *const fn (*anyopaque, A, u64, Request) anyerror![]u8,
    mutate: *const fn (*anyopaque, u64, u64, lifecycle.Mutation, Request) anyerror!void,
    pub fn readState(self: Authority, a: A, table: u64) !lifecycle.State {
        try self.context.ensureExecutionActive();
        const bytes = try self.read(self.ptr, a, table, self.context);
        defer a.free(bytes);
        try self.context.ensureExecutionActive();
        return lifecycle.parse(a, bytes);
    }
    /// Retry only a proven pre-admission revision conflict. Ambiguous append
    /// outcomes terminate the session; a renewal must never replay them.
    pub fn apply(self: Authority, table: u64, mutation: lifecycle.Mutation) !void {
        for (0..8) |_| {
            try self.context.ensureExecutionActive();
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const state = try self.readState(arena.allocator(), table);
            try self.context.ensureExecutionActive();
            self.mutate(self.ptr, table, state.revision, mutation, self.context) catch |err| {
                if (err == error.CatalogGenerationChanged) continue;
                return err;
            };
            try self.context.ensureExecutionActive();
            return;
        }
        return error.LakeIndexReaderLeaseContended;
    }
};
pub const Handle = struct {
    owner: *Owner,
    parent: Context,
    admission: Request,
    pub fn retainedToken(self: *const Handle) local.metadata_lake_index_catalog.Token {
        return self.owner.token;
    }
    pub fn readContext(self: *Handle) Context {
        var context = self.parent;
        context.authority = .{ .ptr = self, .check = checkpoint };
        return context;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Handle = @ptrCast(@alignCast(raw));
        try self.parent.checkLifetime();
        try self.admission.ensureExecutionActive();
        try self.owner.check();
    }
    pub fn retain(self: *const Handle) !*Handle {
        try self.parent.ensureActive();
        try self.admission.ensureExecutionActive();
        try self.owner.check();
        const copy = try std.heap.page_allocator.create(Handle);
        copy.* = self.*;
        _ = self.owner.users.fetchAdd(1, .acq_rel);
        return copy;
    }
    pub fn deinit(self: *Handle) void {
        self.owner.release(self.parent);
        std.heap.page_allocator.destroy(self);
    }
};
const Owner = struct {
    table: u64,
    generation: u64,
    authority: Authority,
    io: std.Io,
    token: [16]u8,
    users: std.atomic.Value(usize) = .init(1),
    retiring: std.atomic.Value(usize) = .init(0),
    initializing: std.atomic.Value(bool) = .init(true),
    failed: std.atomic.Value(bool) = .init(false),
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    heartbeat: ?std.Io.Future(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),
    service: ?*Service = null,
    link: Service.Link = .{},
    subscription: std.Io.Mutex = .init,
    attached: std.atomic.Value(bool) = .init(false),
    interval_ns: u64 = lifecycle.lease_ms / 3 * std.time.ns_per_ms,
    next_renewal: std.atomic.Value(u64) = .init(0),
    fn stopped(raw: *const anyopaque) bool {
        const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
        return flag.load(.acquire);
    }
    fn checkBase(self: *Owner) !void {
        if (self.stopping.load(.acquire)) return error.Canceled;
        if (self.failed.load(.acquire)) return error.LakeIndexReaderLeaseExpired;
        const unix = platform.time.realtimeNs() / std.time.ns_per_ms;
        const authority = platform.time.authorityNs();
        if (unix == 0 or authority == 0 or unix >= self.unix_deadline.load(.acquire) or authority >= self.authority_deadline.load(.acquire)) return error.LakeIndexReaderLeaseExpired;
    }
    fn check(self: *Owner) !void {
        try self.checkBase();
        if (self.attached.load(.acquire)) {
            self.service.?.checkLink(&self.link) catch {
                self.failed.store(true, .release);
                return error.LakeIndexReaderLeaseExpired;
            };
        }
    }
    fn checkErased(raw: *anyopaque) !void {
        try @as(*Owner, @ptrCast(@alignCast(raw))).checkBase();
    }
    fn serviceRenew(raw: *anyopaque, context: Context) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.users.load(.acquire) == 0 or platform.time.authorityNs() < self.next_renewal.load(.acquire)) return;
        self.renewFor(context) catch |err| {
            self.failed.store(true, .release);
            return err;
        };
    }
    fn subscribe(self: *Owner, parent: Context) !void {
        try self.checkBase();
        const service = self.service orelse {
            if (parent.execution != null or parent.wait != null) return error.ReaderRenewalServiceRequired;
            return;
        };
        const operation: Operation = .{ .context = parent };
        while (!self.subscription.tryLock()) try operation.sleep(1);
        defer self.subscription.unlock(self.io);
        try self.check();
        if (!self.attached.load(.acquire)) {
            try service.attachLink(&self.link, self, serviceRenew, checkErased, parent);
            self.attached.store(true, .release);
        }
    }
    fn release(self: *Owner, parent: Context) void {
        // A zero-user cache entry is not replaceable until the physical last
        // release has left its subscription and callback completely behind.
        _ = self.retiring.fetchAdd(1, .acq_rel);
        defer _ = self.retiring.fetchSub(1, .acq_rel);
        if (self.users.fetchSub(1, .acq_rel) != 1 or self.service == null) return;
        // The owner remains cached. Only its live-reader renewal subscription
        // retires here, and a later reader can reuse still-live cached rights.
        var restore: ?local.common_request_admission.runnable_scheduler.Class = null;
        if (!self.subscription.tryLock()) {
            if (parent.execution) |execution| {
                const class = execution.retainedClass() catch null;
                if (class) |value| if (execution.park(.resource_blocked)) |_| {
                    restore = value;
                } else |_| {};
            }
            self.subscription.lockUncancelable(self.io);
            if (restore) |class| parent.execution.?.enter(class) catch {};
        }
        defer self.subscription.unlock(self.io);
        if (self.users.load(.acquire) != 0 or !self.attached.load(.acquire)) return;
        self.service.?.checkLink(&self.link) catch {
            self.failed.store(true, .release);
        };
        self.service.?.detachLink(&self.link, parent);
        self.attached.store(false, .release);
    }
    fn deadline(self: *Owner, now: u64, authority: u64) !void {
        self.unix_deadline.store(std.math.add(u64, now, lifecycle.lease_ms) catch return error.LakeIndexReaderLeaseExpired, .release);
        self.authority_deadline.store(std.math.add(u64, authority, lifecycle.lease_ms * std.time.ns_per_ms) catch return error.LakeIndexReaderLeaseExpired, .release);
        self.next_renewal.store(authority +| self.interval_ns, .release);
    }
    fn start(self: *Owner, admission: Authority, retained: bool) !void {
        const now = platform.time.realtimeNs() / std.time.ns_per_ms;
        const authority = platform.time.authorityNs();
        if (now == 0 or authority == 0) return error.LakeIndexReaderLeaseExpired;
        if (retained) {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const state = try admission.readState(arena.allocator(), self.table);
            const generation = for (state.readers) |reader| {
                if (std.mem.eql(u8, &reader.token, &self.token)) break reader.generation;
            } else return error.LakeIndexReaderLeaseExpired;
            if (generation != self.generation) return error.LakeIndexReaderLeaseExpired;
            try admission.apply(self.table, .{ .renew = .{ .token = self.token, .now_ms = now } });
        } else try admission.apply(self.table, .{ .acquire = .{ .token = self.token, .generation = self.generation, .now_ms = now } });
        try self.deadline(now, authority);
        try self.check();
        if (self.service == null) self.heartbeat = try self.io.concurrent(run, .{self});
    }
    fn run(self: *Owner) void {
        while (true) {
            self.io.sleep(.fromNanoseconds(@intCast(self.interval_ns)), .awake) catch return;
            if (self.users.load(.acquire) == 0) continue;
            self.renew() catch {
                self.failed.store(true, .release);
                return;
            };
        }
    }
    fn renew(self: *Owner) !void {
        return self.renewFor(.{});
    }
    fn sourceCanceled(raw: *const anyopaque) bool {
        @as(*Owner, @ptrCast(@alignCast(@constCast(raw)))).checkBase() catch return true;
        return false;
    }
    fn renewFor(self: *Owner, context: Context) !void {
        try self.checkBase();
        const now = platform.time.realtimeNs() / std.time.ns_per_ms;
        const authority = platform.time.authorityNs();
        var bounded = self.authority;
        bounded.context = .{
            .deadline_ns = platform.time.monotonicNs() +| @min(self.authority_deadline.load(.acquire) -| authority, 5 * std.time.ns_per_s),
            .cancellation = .{ .ptr = self, .is_cancelled_fn = sourceCanceled },
        };
        if (context.deadline_ns) |limit| bounded.context.deadline_ns = @min(limit, bounded.context.deadline_ns.?);
        var read: ?@import("runnable_local_read.zig").Read = if (context.execution) |execution| .{ .execution = execution, .allocator = std.heap.page_allocator } else null;
        var team = if (read) |*value| try value.team() else null;
        if (team) |*value| bounded.context.execution_team = value;
        try bounded.apply(self.table, .{ .renew = .{ .token = self.token, .now_ms = now } });
        // The old rights must remain valid through RPC/CPU return. A delayed
        // metadata response never revives an expired local or durable session.
        try self.checkBase();
        try self.deadline(now, authority);
        try self.checkBase();
    }
    fn destroy(self: *Owner, parent: Context) void {
        if (self.attached.load(.acquire)) {
            self.service.?.detachLink(&self.link, parent);
            self.attached.store(false, .release);
        }
        self.stopping.store(true, .release);
        if (self.heartbeat) |*heartbeat| heartbeat.cancel(self.io);
        // No release I/O on close: crashed and gracefully stopped processes
        // have identical bounded lease expiry. New admission prunes old pins.
        std.heap.page_allocator.destroy(self);
    }
};
pub const Pool = struct {
    mutex: std.Io.Mutex = .init,
    entries: [64]?*Owner = @splat(null),
    closing: bool = false,
    service: std.atomic.Value(?*Service) = .init(null),
    renewal_interval_ns: u64 = lifecycle.lease_ms / 3 * std.time.ns_per_ms,

    pub fn setService(self: *Pool, service: *Service) void {
        const previous = self.service.swap(service, .acq_rel);
        std.debug.assert(previous == null or previous == service);
    }

    pub fn acquire(self: *Pool, io: std.Io, authority: Authority, table: u64, generation: u64, parent: Context) !*Handle {
        return self.acquireRetained(io, authority, table, generation, parent, null);
    }
    pub fn acquireRetained(self: *Pool, io: std.Io, authority: Authority, table: u64, generation: u64, parent: Context, token: ?local.metadata_lake_index_catalog.Token) !*Handle {
        var scoped = parent;
        if (scoped.io == null) scoped.io = io;
        const incoming = try authority.context.platformDeadline();
        if (incoming.deadline_ns) |deadline| scoped.deadline_ns = @min(scoped.deadline_ns orelse deadline, deadline);
        const operation: Operation = .{ .context = scoped };
        while (true) {
            try scoped.ensureActive();
            try authority.context.ensureExecutionActive();
            while (!self.mutex.tryLock()) try operation.sleep(1);
            if (self.closing) {
                self.mutex.unlock(io);
                return error.Canceled;
            }
            var pending = false;
            var retiring_pending = false;
            var available: ?usize = null;
            var selected: ?*Owner = null;
            for (self.entries, 0..) |entry, index| {
                const owner = entry orelse {
                    if (available == null) available = index;
                    continue;
                };
                if (owner.initializing.load(.acquire)) {
                    if (owner.table == table and owner.generation == generation) pending = true;
                    continue;
                }
                const active = if (owner.check()) |_| true else |_| false;
                if (active and owner.table == table and owner.generation == generation) {
                    _ = owner.users.fetchAdd(1, .acq_rel);
                    selected = owner;
                    break;
                }
                if (owner.users.load(.acquire) == 0) {
                    if (owner.retiring.load(.acquire) != 0) {
                        retiring_pending = true;
                    } else if (available == null) available = index;
                }
            }
            if (selected) |owner| {
                self.mutex.unlock(io);
                errdefer owner.release(scoped);
                const handle = try std.heap.page_allocator.create(Handle);
                errdefer std.heap.page_allocator.destroy(handle);
                handle.* = .{ .owner = owner, .parent = scoped, .admission = authority.context };
                try owner.subscribe(scoped);
                try owner.check();
                try parent.ensureActive();
                try authority.context.ensureExecutionActive();
                return handle;
            }
            if (pending or (available == null and retiring_pending)) {
                self.mutex.unlock(io);
                // Single-flight admission, with no network I/O under the lock.
                try operation.sleep(1);
                continue;
            }
            const index = available orelse {
                self.mutex.unlock(io);
                return error.LakeIndexReaderCapacityExceeded;
            };
            const prior = self.entries[index];
            const owner = std.heap.page_allocator.create(Owner) catch |err| {
                self.mutex.unlock(io);
                return err;
            };
            var session_authority = authority;
            // A shared session outlives individual query deadlines. Its
            // heartbeat is stopped by Pool.deinit after queries drain.
            session_authority.context = .{};
            owner.* = .{ .table = table, .generation = generation, .authority = session_authority, .io = io, .token = undefined, .service = self.service.load(.acquire), .interval_ns = self.renewal_interval_ns };
            if (token) |retained| owner.token = retained else while (true) {
                io.random(&owner.token);
                if (!std.mem.allEqual(u8, &owner.token, 0)) break;
            }
            self.entries[index] = owner;
            self.mutex.unlock(io);
            if (prior) |old| old.destroy(scoped);
            const admitted = scoped.execution != null or scoped.wait != null or authority.context.execution_team != null;
            const initialization = if (owner.service == null and admitted) error.ReaderRenewalServiceRequired else owner.start(authority, token != null);
            initialization catch |err| {
                owner.failed.store(true, .release);
                owner.initializing.store(false, .release);
                _ = owner.users.fetchSub(1, .acq_rel);
                return err;
            };
            owner.initializing.store(false, .release);
            errdefer owner.release(scoped);
            const handle = try std.heap.page_allocator.create(Handle);
            errdefer std.heap.page_allocator.destroy(handle);
            handle.* = .{ .owner = owner, .parent = scoped, .admission = authority.context };
            try owner.subscribe(scoped);
            try parent.ensureActive();
            try owner.check();
            try authority.context.ensureExecutionActive();
            return handle;
        }
    }
    /// Server shutdown drains query/build workers before destroying the pool.
    pub fn deinit(self: *Pool, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        self.closing = true;
        const entries = self.entries;
        self.entries = @splat(null);
        self.mutex.unlock(io);
        for (entries) |entry| if (entry) |owner| owner.destroy(.{ .io = io });
    }
};

test "external lake native reader pool amortizes admission and fences every cached read" {
    const a = std.testing.allocator;
    const Harness = struct {
        arena: std.heap.ArenaAllocator,
        state: lifecycle.State,
        mutations: usize = 0,
        fn read(raw: *anyopaque, alloc: A, _: u64, _: Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return lifecycle.encode(alloc, self.state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, mutation: lifecycle.Mutation, _: Request) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (revision != self.state.revision) return error.CatalogGenerationChanged;
            self.state = try mutation.apply(self.arena.allocator(), self.state);
            self.mutations += 1;
        }
    };
    const publication: local.metadata_lake_index_catalog.Publication = .{
        .generation = 1,
        .token = @splat(1),
        .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) },
        .published_at_ms = 1,
        .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } },
        .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    };
    var harness: Harness = .{ .arena = .init(a), .state = .{} };
    defer harness.arena.deinit();
    harness.state = try harness.state.synchronize(harness.arena.allocator(), .{ .generation = 1, .published = publication });
    const authority: Authority = .{ .ptr = &harness, .context = .{}, .read = Harness.read, .mutate = Harness.mutate };
    var pool: Pool = .{};
    defer pool.deinit(std.testing.io);
    const first = try pool.acquire(std.testing.io, authority, 4, 1, .{});
    defer first.deinit();
    for (0..64) |_| {
        const handle = try pool.acquire(std.testing.io, authority, 4, 1, .{});
        defer handle.deinit();
        try std.testing.expect(first.owner == handle.owner);
        try handle.readContext().ensureActive();
    }
    const short = try pool.acquire(std.testing.io, authority, 4, 1, .{});
    const retained_copy = try short.retain();
    defer retained_copy.deinit();
    short.deinit();
    // A source can retain authority after its short index cursor closes.
    try retained_copy.readContext().ensureActive();
    try std.testing.expect(first.owner == retained_copy.owner);
    try std.testing.expectEqual(@as(usize, 1), harness.mutations);
    var replacement = publication;
    replacement.generation = 2;
    replacement.token = @splat(2);
    harness.state = try harness.state.synchronize(harness.arena.allocator(), .{ .generation = 2, .published = replacement });
    var restarted: Pool = .{};
    defer restarted.deinit(std.testing.io);
    try std.testing.expectError(error.LakeIndexGenerationRetired, restarted.acquire(std.testing.io, authority, 4, 1, .{}));
    const resumed = try restarted.acquireRetained(std.testing.io, authority, 4, 1, .{}, first.retainedToken());
    defer resumed.deinit();
    try resumed.readContext().ensureActive();
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, restarted.acquireRetained(std.testing.io, authority, 4, 2, .{}, first.retainedToken()));
    harness.state = try harness.state.drop();
    // Existing sessions retain their exact snapshot even after logical DROP.
    try first.owner.renew();
    try first.readContext().ensureActive();
    const Runnable = local.common_request_admission.runnable_scheduler;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    var execution: Runnable.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer execution.finish();
    const active = first.readContext().withExecution(&execution);
    try active.ensureActive();
    try execution.park(.resource_blocked);
    try active.checkLifetime();
    first.owner.authority_deadline.store(0, .release);
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, first.readContext().ensureActive());
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, active.checkLifetime());
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, active.ensureActive());
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
}

test "external lake cold reader admission retains deadline and cancellation" {
    const Harness = struct {
        fn read(_: *anyopaque, _: A, _: u64, request: Request) ![]u8 {
            try request.ensureActive();
            return error.UnexpectedUnboundedReaderAdmission;
        }
        fn mutate(_: *anyopaque, _: u64, _: u64, _: lifecycle.Mutation, _: Request) !void {
            return error.UnexpectedUnboundedReaderAdmission;
        }
        fn canceled(_: *const anyopaque) bool {
            return true;
        }
    };
    var dummy: u8 = 0;
    var pool: Pool = .{};
    defer pool.deinit(std.testing.io);
    var authority: Authority = .{ .ptr = &dummy, .context = .{ .deadline_ns = 1 }, .read = Harness.read, .mutate = Harness.mutate };
    try std.testing.expectError(error.DeadlineExceeded, pool.acquire(std.testing.io, authority, 4, 1, .{}));
    authority.context = .{ .cancellation = .{ .ptr = &dummy, .is_cancelled_fn = Harness.canceled } };
    try std.testing.expectError(error.Canceled, pool.acquire(std.testing.io, authority, 4, 1, .{}));
}

const HttpRenewalFixture = struct {
    const httpx = @import("httpx");
    const Call = @import("../system_catalog/server_call.zig").Call;
    const Client = @import("../metadata/http_client.zig").MetadataHttpClient;
    arena: std.heap.ArenaAllocator,
    state: lifecycle.State,
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    executor: @import("../raft/transport/http_common.zig").RequestExecutor,
    base: []const u8 = "",
    mutations: std.atomic.Value(usize) = .init(0),
    renewals: std.atomic.Value(usize) = .init(0),
    admitted: std.atomic.Value(usize) = .init(0),
    stall: std.atomic.Value(bool) = .init(false),
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    refuse: std.atomic.Value(bool) = .init(false),

    fn serve(self: *@This(), ctx: *httpx.Context) !httpx.Response {
        const call = try std.json.parseFromSlice(Call, ctx.allocator, ctx.request.body orelse "", .{ .allocate = .alloc_always });
        defer call.deinit();
        self.mutex.lockUncancelable(self.io);
        var locked = true;
        defer if (locked) self.mutex.unlock(self.io);
        var response = try httpx.Response.fromText(ctx.allocator, 200, "");
        errdefer response.deinit();
        switch (call.value) {
            .lake_index_lifecycle_read => {
                const body = try lifecycle.encode(ctx.allocator, self.state);
                defer ctx.allocator.free(body);
                response.deinit();
                response = try httpx.Response.fromText(ctx.allocator, 200, body);
                try response.headers.set("x-antfly-catalog-metadata-group", "1");
                try response.headers.set("x-antfly-catalog-metadata-incarnation", "11111111111111111111111111111111");
            },
            .lake_index_lifecycle_mutate => |write| {
                if (write.expected_revision != self.state.revision) return error.UnexpectedRevision;
                if (write.mutation == .renew) {
                    _ = self.renewals.fetchAdd(1, .acq_rel);
                    if (self.refuse.load(.acquire)) {
                        response.status = httpx.Status.fromCode(409);
                        try response.headers.set("x-antfly-raft-mutation-outcome", "unknown-v1");
                        return response;
                    }
                }
                self.state = try write.mutation.apply(self.arena.allocator(), self.state);
                _ = self.mutations.fetchAdd(1, .acq_rel);
                try response.headers.set("x-antfly-raft-mutation-outcome", "committed-v1");
                if (write.mutation == .renew and self.stall.swap(false, .acq_rel)) {
                    self.mutex.unlock(self.io);
                    locked = false;
                    self.entered.set(self.io);
                    self.release.waitUncancelable(self.io);
                }
            },
            else => return error.UnexpectedCatalogCall,
        }
        return response;
    }
    fn read(raw: *anyopaque, a: A, table: u64, request: Request) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var scope: @import("request_http_execution.zig").Scope = .{ .request = request, .inner = self.executor };
        var client = Client.init(a, scope.executor());
        _ = client.withSettingAuthority("reader-http-fixture", "reader-fixture");
        var cancellation = @import("../raft/transport/http_common.zig").RequestCancellation.fromToken(request.cancellation);
        const result = try client.readSystemCatalog(self.base, .{ .lake_index_lifecycle_read = table }, 5000, &cancellation);
        return result.body;
    }
    fn mutate(raw: *anyopaque, table: u64, revision: u64, mutation: lifecycle.Mutation, request: Request) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (mutation == .renew) {
            const team = request.execution_team orelse return error.UnadmittedRenewal;
            if (try team.borrow.currentClass() != .control) return error.WrongRenewalClass;
            _ = self.admitted.fetchAdd(1, .acq_rel);
        }
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        var scope: @import("request_http_execution.zig").Scope = .{ .request = request, .inner = self.executor };
        var client = Client.init(arena.allocator(), scope.executor());
        _ = client.withSettingAuthority("reader-http-fixture", "reader-fixture");
        _ = try client.forwardSystemCatalog(self.base, .{ .lake_index_lifecycle_mutate = .{ .table_id = table, .expected_revision = revision, .mutation = mutation } }, .{ .remaining_ms = 5000, .forwards_remaining = 1, .campaign_allowed = false }, true);
    }
    fn await(self: *@This(), predicate: *const fn (*@This()) bool) !void {
        const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
        while (!predicate(self)) {
            if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
            try self.io.sleep(.fromMilliseconds(1), .awake);
        }
    }
    fn hasEntered(self: *@This()) bool {
        return self.entered.isSet();
    }
    fn hasRenewal(self: *@This()) bool {
        return self.renewals.load(.acquire) != 0;
    }
};

test "external lake native reader real HTTP renewal coalesces and retires with live readers" {
    try nativeReaderHttpCase(.coalesce);
}
test "external lake native reader real HTTP delayed expiry never revives and leaves control progress" {
    try nativeReaderHttpCase(.delayed_expiry);
}
test "external lake native reader real HTTP ambiguous renewal never replays" {
    try nativeReaderHttpCase(.refused);
}
test "external lake native reader real HTTP last close joins callback while lending reader CPU" {
    try nativeReaderHttpCase(.joining);
}
const NativeReaderHttpMode = enum { coalesce, delayed_expiry, refused, joining };
fn nativeReaderHttpCase(mode: NativeReaderHttpMode) !void {
    const httpx = @import("httpx");
    const Runnable = local.common_request_admission.runnable_scheduler;
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var transport = @import("../raft/transport/std_http_executor.zig").StdHttpExecutor.init(a, .{});
    defer transport.deinit();
    var fixture: HttpRenewalFixture = .{ .arena = .init(a), .state = .{}, .io = io, .executor = transport.executor() };
    defer fixture.arena.deinit();
    const publication: local.metadata_lake_index_catalog.Publication = .{
        .generation = 1,
        .token = @splat(1),
        .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) },
        .published_at_ms = 1,
        .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } },
        .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
    };
    fixture.state = try fixture.state.synchronize(fixture.arena.allocator(), .{ .generation = 1, .published = publication });
    var server = httpx.Server.initWithConfig(a, io, .{ .port = 0 });
    defer server.deinit();
    try server.post("/internal/v1/system-catalog", httpx.Handler.bind(&fixture, HttpRenewalFixture.serve));
    var listener = httpx.Server.ListenerTask.init(&server);
    try listener.start();
    defer {
        server.stop();
        listener.join() catch {};
    }
    defer fixture.release.set(io);
    const base = try std.fmt.allocPrint(a, "http://{f}", .{server.boundAddress().?});
    defer a.free(base);
    fixture.base = base;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 4, .control_slots = 2 });
    defer scheduler.close();
    var service: Service = .{ .interval_ns = 2 * std.time.ns_per_ms };
    service.configure(io, &scheduler);
    defer service.deinit();
    var pool: Pool = .{ .renewal_interval_ns = 5 * std.time.ns_per_ms };
    pool.setService(&service);
    defer pool.deinit(io);
    var query: Runnable.Execution = .{};
    try query.start(&scheduler, .{ .io = io }, .query);
    defer query.finish();
    var read: @import("runnable_local_read.zig").Read = .{ .execution = &query, .allocator = a };
    var team = try read.team();
    const authority: Authority = .{ .ptr = &fixture, .context = .{ .execution_team = &team }, .read = HttpRenewalFixture.read, .mutate = HttpRenewalFixture.mutate };
    if (mode == .delayed_expiry or mode == .joining) fixture.stall.store(true, .release);
    if (mode == .refused) fixture.refuse.store(true, .release);
    const first = try pool.acquire(io, authority, 4, 1, .{ .io = io, .execution = &query });
    var first_open = true;
    defer if (first_open) first.deinit();
    if (mode == .joining) {
        try query.park(.resource_blocked);
        try fixture.await(HttpRenewalFixture.hasEntered);
        try query.enter(.query);
        const cached = first.owner;
        const Close = struct {
            handle: *Handle,
            done: std.atomic.Value(bool) = .init(false),
            fn run(self: *@This()) void {
                self.handle.deinit();
                self.done.store(true, .release);
            }
        };
        var close: Close = .{ .handle = first };
        var task = try io.concurrent(Close.run, .{&close});
        first_open = false;
        defer {
            fixture.release.set(io);
            task.cancel(io);
        }
        const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
        while (scheduler.stats().runnable_slots != 0) {
            if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expect(!close.done.load(.acquire));
        try std.testing.expect(cached.retiring.load(.acquire) != 0);
        var unrelated: Runnable.Execution = .{};
        try unrelated.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
        unrelated.finish();
        var health: Runnable.Execution = .{};
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        fixture.release.set(io);
        task.await(io);
        try std.testing.expect(close.done.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), cached.retiring.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), service.count);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().control_services);
        try query.checkpoint();
        return;
    }
    if (mode != .coalesce) {
        try query.park(.resource_blocked);
        if (mode == .delayed_expiry) {
            try fixture.await(HttpRenewalFixture.hasEntered);
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
            var unrelated: Runnable.Execution = .{};
            try unrelated.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
            unrelated.finish();
            var health: Runnable.Execution = .{};
            try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
            health.finish();
            const previous = first.owner.unix_deadline.load(.acquire);
            first.owner.authority_deadline.store(0, .release);
            fixture.release.set(io);
            const terminal = struct {
                fn failed(owner: *Owner) bool {
                    return owner.failed.load(.acquire);
                }
            };
            const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
            while (!terminal.failed(first.owner)) {
                if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expectEqual(previous, first.owner.unix_deadline.load(.acquire));
        } else {
            try fixture.await(HttpRenewalFixture.hasRenewal);
            const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
            while (!first.owner.failed.load(.acquire)) {
                if (platform.time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            try io.sleep(.fromMilliseconds(20), .awake);
            try std.testing.expectEqual(@as(usize, 1), fixture.renewals.load(.acquire));
        }
        try query.enter(.query);
        try std.testing.expectError(error.LakeIndexReaderLeaseExpired, first.readContext().ensureActive());
        try std.testing.expect(!service.failed.load(.acquire));
        fixture.refuse.store(false, .release);
        const healthy = try pool.acquire(io, authority, 5, 1, .{ .io = io, .execution = &query });
        defer healthy.deinit();
        try healthy.readContext().ensureActive();
        try std.testing.expectError(error.LakeIndexReaderLeaseExpired, first.readContext().ensureActive());
        return;
    }
    const second = try pool.acquire(io, authority, 4, 1, .{ .io = io, .execution = &query });
    var second_open = true;
    defer if (second_open) second.deinit();
    try std.testing.expect(first.owner == second.owner);
    try query.park(.resource_blocked);
    try fixture.await(HttpRenewalFixture.hasRenewal);
    try query.enter(.query);
    try first.readContext().ensureActive();
    try std.testing.expect(fixture.admitted.load(.acquire) > 0);
    try std.testing.expectEqual(@as(usize, 1), service.count);
    first.deinit();
    first_open = false;
    try std.testing.expectEqual(@as(usize, 1), service.count);
    const Cancel = struct {
        flag: std.atomic.Value(bool) = .init(false),
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).flag.load(.acquire);
        }
    };
    var cancel: Cancel = .{};
    var private_authority = authority;
    private_authority.context.cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.requested };
    const private = try pool.acquire(io, private_authority, 4, 1, .{ .io = io });
    cancel.flag.store(true, .release);
    try std.testing.expectError(error.Canceled, private.readContext().ensureActive());
    try std.testing.expectError(error.Canceled, private.retain());
    private.deinit();
    try second.readContext().ensureActive();
    private_authority = authority;
    private_authority.context.deadline_ns = platform.time.monotonicNs() + 10 * std.time.ns_per_ms;
    const short = try pool.acquire(io, private_authority, 4, 1, .{ .io = io });
    try query.park(.resource_blocked);
    try io.sleep(.fromMilliseconds(15), .awake);
    try query.enter(.query);
    try std.testing.expectError(error.DeadlineExceeded, short.readContext().ensureActive());
    short.deinit();
    try second.readContext().ensureActive();
    const cached = second.owner;
    cached.next_renewal.store(std.math.maxInt(u64), .release);
    second.deinit();
    second_open = false;
    try std.testing.expectEqual(@as(usize, 0), service.count);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().control_services);
    const before = fixture.mutations.load(.acquire);
    const reused = try pool.acquire(io, authority, 4, 1, .{ .io = io, .execution = &query });
    defer reused.deinit();
    try std.testing.expect(reused.owner == cached);
    try std.testing.expectEqual(before, fixture.mutations.load(.acquire));
    try reused.readContext().ensureActive();
}
