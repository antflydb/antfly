// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Amortized native metadata reader sessions. Queries borrow one durable
//! per-process session; payload cache eviction never changes read authority.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const Context = local.serverless_query_lake_read_context.Context;
const Request = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Authority = struct {
    ptr: *anyopaque,
    context: Request,
    read: *const fn (*anyopaque, A, u64, Request) anyerror![]u8,
    mutate: *const fn (*anyopaque, u64, u64, lifecycle.Mutation, Request) anyerror!void,
    pub fn readState(self: Authority, a: A, table: u64) !lifecycle.State {
        return lifecycle.parse(a, try self.read(self.ptr, a, table, self.context));
    }
    /// Retry only a proven pre-admission revision conflict. Ambiguous append
    /// outcomes terminate the session; a renewal must never replay them.
    pub fn apply(self: Authority, table: u64, mutation: lifecycle.Mutation) !void {
        for (0..8) |_| {
            try self.context.ensureActive();
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const state = try self.readState(arena.allocator(), table);
            try self.context.ensureActive();
            self.mutate(self.ptr, table, state.revision, mutation, self.context) catch |err| {
                if (err == error.CatalogGenerationChanged) continue;
                return err;
            };
            return;
        }
        return error.LakeIndexReaderLeaseContended;
    }
};
pub const Handle = struct {
    owner: *Owner,
    parent: Context,
    pub fn retainedToken(self: *const Handle) local.metadata_lake_index_catalog.Token {
        return self.owner.token;
    }
    pub fn readContext(self: *Handle) Context {
        var context = self.parent;
        context.checkpoint = .{ .ptr = self, .check = checkpoint };
        return context;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Handle = @ptrCast(@alignCast(raw));
        try self.parent.ensureActive();
        try self.owner.check();
    }
    pub fn deinit(self: *Handle) void {
        _ = self.owner.users.fetchSub(1, .acq_rel);
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
    initializing: std.atomic.Value(bool) = .init(true),
    failed: std.atomic.Value(bool) = .init(false),
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    heartbeat: ?std.Io.Future(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),
    fn stopped(raw: *const anyopaque) bool {
        const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
        return flag.load(.acquire);
    }
    fn check(self: *Owner) !void {
        if (self.failed.load(.acquire)) return error.LakeIndexReaderLeaseExpired;
        const unix = platform.time.realtimeNs() / std.time.ns_per_ms;
        const authority = platform.time.authorityNs();
        if (unix == 0 or authority == 0 or unix >= self.unix_deadline.load(.acquire) or authority >= self.authority_deadline.load(.acquire)) return error.LakeIndexReaderLeaseExpired;
    }
    fn deadline(self: *Owner, now: u64, authority: u64) !void {
        self.unix_deadline.store(std.math.add(u64, now, lifecycle.lease_ms) catch return error.LakeIndexReaderLeaseExpired, .release);
        self.authority_deadline.store(std.math.add(u64, authority, lifecycle.lease_ms * std.time.ns_per_ms) catch return error.LakeIndexReaderLeaseExpired, .release);
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
        self.heartbeat = try self.io.concurrent(run, .{self});
    }
    fn run(self: *Owner) void {
        while (true) {
            self.io.sleep(.fromMilliseconds(@intCast(lifecycle.lease_ms / 3)), .awake) catch return;
            if (self.users.load(.acquire) == 0) continue;
            self.renew() catch {
                self.failed.store(true, .release);
                return;
            };
        }
    }
    fn renew(self: *Owner) !void {
        try self.check();
        const now = platform.time.realtimeNs() / std.time.ns_per_ms;
        const authority = platform.time.authorityNs();
        var bounded = self.authority;
        bounded.context = .{
            .deadline_ns = platform.time.monotonicNs() +| @min(self.authority_deadline.load(.acquire) -| authority, 5 * std.time.ns_per_s),
            .cancellation = .{ .ptr = &self.stopping, .is_cancelled_fn = stopped },
        };
        try bounded.apply(self.table, .{ .renew = .{ .token = self.token, .now_ms = now } });
        try self.deadline(now, authority);
        try self.check();
    }
    fn destroy(self: *Owner) void {
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

    pub fn acquire(self: *Pool, io: std.Io, authority: Authority, table: u64, generation: u64, parent: Context) !*Handle {
        return self.acquireRetained(io, authority, table, generation, parent, null);
    }
    pub fn acquireRetained(self: *Pool, io: std.Io, authority: Authority, table: u64, generation: u64, parent: Context, token: ?local.metadata_lake_index_catalog.Token) !*Handle {
        while (true) {
            try parent.ensureActive();
            try self.mutex.lock(io);
            if (self.closing) {
                self.mutex.unlock(io);
                return error.Cancelled;
            }
            var pending = false;
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
                if (owner.users.load(.acquire) == 0 and available == null) available = index;
            }
            if (selected) |owner| {
                self.mutex.unlock(io);
                errdefer _ = owner.users.fetchSub(1, .acq_rel);
                const handle = try std.heap.page_allocator.create(Handle);
                errdefer std.heap.page_allocator.destroy(handle);
                handle.* = .{ .owner = owner, .parent = parent };
                try owner.check();
                try parent.ensureActive();
                return handle;
            }
            if (pending) {
                self.mutex.unlock(io);
                // Single-flight admission, with no network I/O under the lock.
                try io.sleep(.fromMilliseconds(1), .awake);
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
            owner.* = .{ .table = table, .generation = generation, .authority = session_authority, .io = io, .token = undefined };
            if (token) |retained| owner.token = retained else while (true) {
                io.random(&owner.token);
                if (!std.mem.allEqual(u8, &owner.token, 0)) break;
            }
            self.entries[index] = owner;
            self.mutex.unlock(io);
            if (prior) |old| old.destroy();
            owner.start(authority, token != null) catch |err| {
                owner.failed.store(true, .release);
                owner.initializing.store(false, .release);
                _ = owner.users.fetchSub(1, .acq_rel);
                return err;
            };
            owner.initializing.store(false, .release);
            errdefer _ = owner.users.fetchSub(1, .acq_rel);
            const handle = try std.heap.page_allocator.create(Handle);
            errdefer std.heap.page_allocator.destroy(handle);
            handle.* = .{ .owner = owner, .parent = parent };
            try parent.ensureActive();
            try owner.check();
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
        for (entries) |entry| if (entry) |owner| owner.destroy();
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
    first.owner.authority_deadline.store(0, .release);
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, first.readContext().ensureActive());
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
