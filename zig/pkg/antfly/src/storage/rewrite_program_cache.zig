// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Resident-owner, immutable rewrite programs. Acquire before source-generation
//! or frame locks; compilation is single-flight and never holds either lock.
//! A lease pins the program through page preparation. Replicated staging state,
//! not this disposable cache, remains the publication/progress authority.
const std = @import("std");
const programs = @import("db/relational_rewrite_program.zig");
const contract = @import("db/relational_rewrite_contract.zig");
const staging = @import("db/restore_staging_contract.zig");
const resources = @import("resource_manager.zig");
const Cancellation = @import("db/types.zig").CancellationToken;

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    io: ?std.Io = null,
    manager: ?*resources.ResourceManager = null,
    reclaimer: u64 = 0,
    // Stable-address allocator: the owning DB must not move after first use.
    budget: ?resources.BudgetedAllocator = null,
    entry: ?Entry = null,
    compilations: u64 = 0,
    hits: u64 = 0,

    const Entry = struct {
        scope: [32]u8,
        intent: [32]u8,
        program: programs.ProgramSet,
    };

    pub const Lease = struct {
        cache: *Cache,

        pub fn program(self: Lease) *const programs.ProgramSet {
            return &self.cache.entry.?.program;
        }

        pub fn deinit(self: *Lease) void {
            self.cache.mutex.unlock(self.cache.io.?);
            self.* = undefined;
        }
    };

    fn clear(self: *Cache) void {
        if (self.entry) |*entry| entry.program.deinit();
        self.entry = null;
        if (self.budget) |*budget| budget.deinit();
        self.budget = null;
    }

    pub fn evict(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.clear();
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        // Drain callbacks before taking the owner lock or releasing its allocator.
        if (self.manager) |manager| manager.unregisterReclaimer(self.reclaimer);
        self.evict(io);
        self.reclaimer = 0;
        self.manager = null;
        self.io = null;
    }

    fn reclaim(ptr: *anyopaque, _: u64) u64 {
        const self: *Cache = @ptrCast(@alignCast(ptr));
        if (!self.mutex.tryLock()) return 0;
        defer self.mutex.unlock(self.io.?);
        const bytes = if (self.budget) |budget| budget.reservation.bytes else 0;
        self.clear();
        return bytes;
    }

    pub fn acquire(self: *Cache, io: std.Io, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, scope: staging.Scope, intent: contract.Intent, cancellation: Cancellation) !Lease {
        try cancellation.check();
        try scope.validate();
        try intent.validate();
        const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
        if (!std.mem.eql(u8, &binding.program_digest, &intent.program_digest)) return error.RestoreStagingScopeChanged;
        // Never trust a caller-supplied program digest as proof that changed
        // schema bytes/policies were compiled. Hash framed input without parsing
        // or allocating; exact repeats reuse the fully validated program.
        const input_digest = intentDigest(intent);
        const scope_digest = scope.digest();
        try self.mutex.lock(io);
        errdefer self.mutex.unlock(io);
        try cancellation.check();
        if (self.io != null and self.manager != manager) return error.InvalidRestoreSourceCheckpoint;
        self.io = io;
        self.manager = manager;
        if (manager) |value| if (self.reclaimer == 0) {
            self.reclaimer = try value.registerReclaimer(.relational_preparation_working_set, self, reclaim);
        };
        if (self.entry) |entry| {
            if (std.mem.eql(u8, &entry.scope, &scope_digest) and std.mem.eql(u8, &entry.intent, &input_digest)) {
                self.hits +|= 1;
                return .{ .cache = self };
            }
        }
        self.clear();
        errdefer self.clear();
        if (manager) |value| self.budget = resources.BudgetedAllocator.initReclaiming(value, .relational_preparation_working_set, backing, 1);
        const alloc = if (self.budget) |*budget| budget.allocator() else backing;
        var program = programs.ProgramSet.initIntent(alloc, intent) catch |err| {
            if (self.budget) |budget| if (budget.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
        errdefer program.deinit();
        try program.requireScope(scope);
        try cancellation.check();
        if (self.budget) |*budget| _ = budget.releaseUnusedCredit();
        self.entry = .{ .scope = scope_digest, .intent = input_digest, .program = program };
        self.compilations +|= 1;
        return .{ .cache = self };
    }
};

fn intentDigest(intent: contract.Intent) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.rewrite-program-cache.input.v1");
    hash.update(&.{ intent.version, @intFromBool(intent.preserve_document), @intFromBool(intent.apply_defaults_to_absent), @intFromBool(intent.allow_column_drops) });
    hash.update(&intent.program_digest);
    hashPart(&hash, intent.target_schema);
    hashPart(&hash, intent.target_read_schema);
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, intent.source_schemas.len, .little);
    hash.update(&count);
    for (intent.source_schemas) |schema| hashPart(&hash, schema);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

fn hashPart(hash: *std.crypto.hash.Blake3, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}

test "relational index system rewrite program cache owns validates and budgets immutable programs" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    var reference = try programs.ProgramSet.init(alloc, &.{schema}, schema, .{});
    defer reference.deinit();
    const scope: staging.Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .target_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 },
        .target_schema_digest = reference.target_runtime_digest,
        .rewrite = .{ .program_digest = reference.identity, .retained_pin = @splat(6), .snapshot_certificate = @splat(7), .retained_epoch = 1, .retained_start = 0, .source_applied_index = 1 },
    };
    const intent: contract.Intent = .{ .source_schemas = &.{schema}, .target_schema = schema, .program_digest = reference.identity };
    var options: resources.Options = .{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 8 * 1024 * 1024 } };
    options.budgets[@intFromEnum(resources.Slice.relational_preparation_working_set)] = .{ .hard_limit_bytes = 4 * 1024 * 1024 };
    var manager = resources.ResourceManager.init(options);
    defer manager.deinit(alloc);
    var cache: Cache = .{};
    defer cache.deinit(io);
    {
        // Input bytes may disappear with the RPC. The compiled epoch owns them.
        const temporary = try alloc.dupe(u8, schema);
        var borrowed = intent;
        borrowed.source_schemas = &.{temporary};
        borrowed.target_schema = temporary;
        var lease = try cache.acquire(io, alloc, &manager, scope, borrowed, .none);
        defer lease.deinit();
        alloc.free(temporary);
        try lease.program().requireSourceManifest(alloc, &.{schema}, schema);
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, 1));
    }
    for (0..32) |_| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        var lease = try cache.acquire(io, failing.allocator(), &manager, scope, intent, .none);
        defer lease.deinit();
        try lease.program().requireScope(scope);
    }
    try std.testing.expectEqual(@as(u64, 1), cache.compilations);
    try std.testing.expectEqual(@as(u64, 32), cache.hits);
    try std.testing.expect(manager.snapshot().memory.used_bytes > 0);
    // Local-slice contention and unrelated aggregate pressure can both evict.
    var local = try manager.reserve(.relational_preparation_working_set, 4 * 1024 * 1024);
    local.release();
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    {
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .none);
        lease.deinit();
    }
    var foreground = try manager.reserve(.dense_apply_working_set, 8 * 1024 * 1024);
    foreground.release();
    try std.testing.expect(cache.entry == null);
    var changed_scope = scope;
    changed_scope.plan_id[0] ^= 1;
    {
        var lease = try cache.acquire(io, alloc, &manager, changed_scope, intent, .none);
        lease.deinit();
    }
    try std.testing.expectEqual(@as(u64, 3), cache.compilations);
    var changed = intent;
    changed.allow_column_drops = true;
    try std.testing.expectError(error.RestoreStagingScopeChanged, cache.acquire(io, alloc, &manager, changed_scope, changed, .none));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    {
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .none);
        lease.deinit();
    }
    const substituted = try std.mem.replaceOwned(u8, alloc, schema, "\"version\":1", "\"version\":2");
    defer alloc.free(substituted);
    changed = intent;
    changed.source_schemas = &.{substituted};
    try std.testing.expectError(error.RestoreStagingScopeChanged, cache.acquire(io, alloc, &manager, scope, changed, .none));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, cache.acquire(io, alloc, &manager, scope, intent, .fromAtomic(&canceled)));
    var denied = try manager.reserve(.relational_preparation_working_set, 4 * 1024 * 1024);
    try std.testing.expectError(error.ResourceBudgetExceeded, cache.acquire(io, alloc, &manager, scope, intent, .none));
    denied.release();
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, cache.acquire(io, failing.allocator(), &manager, scope, intent, .none));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    {
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .none);
        lease.deinit();
    }
    cache.evict(io);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    const Failure = struct {
        fn attempt(failing_alloc: std.mem.Allocator, bound_scope: staging.Scope, request: contract.Intent) !void {
            var candidate: Cache = .{};
            defer candidate.deinit(std.testing.io);
            var lease = try candidate.acquire(std.testing.io, failing_alloc, null, bound_scope, request, .none);
            lease.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Failure.attempt, .{ scope, intent });
    // Contending RPCs share one compile; no std.Thread lifecycle or spin waits.
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    const concurrent_io = runtime.io();
    var parallel: Cache = .{};
    defer parallel.deinit(concurrent_io);
    var start: std.Io.Event = .unset;
    const Worker = struct {
        io: std.Io,
        cache: *Cache,
        manager: *resources.ResourceManager,
        scope: staging.Scope,
        intent: contract.Intent,
        start: *std.Io.Event,
        err: ?anyerror = null,
        fn run(self: *@This()) std.Io.Cancelable!void {
            try self.start.wait(self.io);
            var lease = self.cache.acquire(self.io, std.testing.allocator, self.manager, self.scope, self.intent, .none) catch |err| {
                self.err = err;
                return;
            };
            lease.deinit();
        }
    };
    var workers: [8]Worker = undefined;
    var group: std.Io.Group = .init;
    defer group.cancel(concurrent_io);
    for (&workers) |*worker| {
        worker.* = .{ .io = concurrent_io, .cache = &parallel, .manager = &manager, .scope = scope, .intent = intent, .start = &start };
        try group.concurrent(concurrent_io, Worker.run, .{worker});
    }
    start.set(concurrent_io);
    try group.await(concurrent_io);
    for (workers) |worker| if (worker.err) |err| return err;
    try std.testing.expectEqual(@as(u64, 1), parallel.compilations);
    try std.testing.expectEqual(@as(u64, 7), parallel.hits);
}
