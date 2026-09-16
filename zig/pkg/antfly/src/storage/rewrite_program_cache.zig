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
const RequestContext = @import("../api/operation.zig").RequestContext;
const Waiters = @import("admission_waiter.zig").Fifo(void);

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    io: ?std.Io = null,
    manager: ?*resources.ResourceManager = null,
    reclaimer: u64 = 0,
    entry: ?*Entry = null,
    compiling: bool = false,
    epoch: u64 = 0,
    waiters: Waiters = .{},
    compilations: u64 = 0,
    hits: u64 = 0,

    const Entry = struct {
        backing: std.mem.Allocator,
        header: ?resources.Reservation,
        // Each generation owns its stable-address allocator. Publication ends
        // allocation; only the last reference can deinitialize it.
        budget: ?resources.BudgetedAllocator,
        refs: std.atomic.Value(usize) = .init(1),
        scope: [32]u8,
        intent: [32]u8,
        program: ?programs.ProgramSet = null,

        fn create(backing: std.mem.Allocator, manager: ?*resources.ResourceManager, scope: [32]u8, intent: [32]u8) !*Entry {
            var header = if (manager) |value| try value.reserve(.relational_preparation_working_set, @sizeOf(Entry)) else null;
            errdefer if (header) |*reservation| reservation.release();
            const entry = try backing.create(Entry);
            entry.* = .{
                .backing = backing,
                .header = header,
                .budget = if (manager) |value| resources.BudgetedAllocator.initReclaiming(value, .relational_preparation_working_set, backing, 1) else null,
                .scope = scope,
                .intent = intent,
            };
            return entry;
        }

        fn retain(self: *Entry) void {
            const previous = self.refs.fetchAdd(1, .monotonic);
            std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
        }

        fn release(self: *Entry) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.program) |*program| program.deinit();
            if (self.budget) |*budget| budget.deinit();
            const backing = self.backing;
            var header = self.header;
            backing.destroy(self);
            if (header) |*reservation| reservation.release();
        }

        fn bytes(self: *const Entry) u64 {
            return (if (self.header) |header| header.bytes else @as(u64, 0)) +
                (if (self.budget) |budget| budget.reservation.bytes else @as(u64, 0));
        }
    };

    pub const Lease = struct {
        entry: *Entry,

        pub fn program(self: Lease) *const programs.ProgramSet {
            return &self.entry.program.?;
        }

        pub fn deinit(self: *Lease) void {
            self.entry.release();
            self.* = undefined;
        }
    };

    pub fn evict(self: *Cache, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        const entry = self.entry;
        self.entry = null;
        // A compiler already in flight may finish for its caller, but cannot
        // repopulate the cache after terminal cleanup or explicit eviction.
        self.epoch +%= 1;
        self.mutex.unlock(io);
        if (entry) |value| value.release();
    }

    pub fn deinit(self: *Cache, io: std.Io) void {
        // The resident owner drains requests before close. Leases may outlive
        // eviction, but must not outlive the backing allocator/ResourceManager.
        std.debug.assert(!self.compiling and self.waiters.head == null);
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
        const entry = self.entry orelse {
            self.mutex.unlock(self.io.?);
            return 0;
        };
        // Report only memory that can actually be freed, not pinned bytes.
        if (entry.refs.load(.acquire) != 1) {
            self.mutex.unlock(self.io.?);
            return 0;
        }
        const bytes = entry.bytes();
        self.entry = null;
        self.mutex.unlock(self.io.?);
        entry.release();
        return bytes;
    }

    /// Called with the short publication lock held. Waiters are stack-owned;
    /// cancellation rejoins that lock before retiring them.
    fn finishCompilation(self: *Cache) void {
        self.compiling = false;
        while (self.waiters.pop()) |waiter| waiter.handoff.publish();
    }

    fn wait(self: *Cache, io: std.Io, waiter: *Waiters.Waiter, context: RequestContext) !void {
        defer {
            self.mutex.lockUncancelable(io);
            _ = self.waiters.remove(waiter);
            self.mutex.unlock(io);
        }
        while (!waiter.handoff.isAdmitted()) {
            try context.ensureActive();
            // Semantic request cancellation can originate outside this Io task
            // (including across the C ABI). Bound its observation latency while
            // waking immediately on publication, without OS-thread spin waits.
            waiter.handoff.ready.waitTimeout(io, .{ .duration = .{
                .raw = std.Io.Duration.fromMilliseconds(5),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return err,
            };
        }
        try context.ensureActive();
    }

    pub fn acquire(self: *Cache, io: std.Io, backing: std.mem.Allocator, manager: ?*resources.ResourceManager, scope: staging.Scope, intent: contract.Intent, context: RequestContext) !Lease {
        try context.ensureActive();
        try scope.validate();
        try intent.validate();
        const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
        if (!std.mem.eql(u8, &binding.program_digest, &intent.program_digest)) return error.RestoreStagingScopeChanged;
        // Never trust a caller-supplied program digest as proof that changed
        // schema bytes/policies were compiled. Hash framed input without parsing
        // or allocating; exact repeats reuse the fully validated program.
        const input_digest = intentDigest(intent);
        const scope_digest = scope.digest();
        const epoch = while (true) {
            try self.mutex.lock(io);
            var locked = true;
            errdefer if (locked) self.mutex.unlock(io);
            try context.ensureActive();
            if (self.io != null and self.manager != manager) return error.InvalidRestoreSourceCheckpoint;
            self.io = io;
            self.manager = manager;
            if (manager) |value| if (self.reclaimer == 0) {
                self.reclaimer = try value.registerReclaimer(.relational_preparation_working_set, self, reclaim);
            };
            if (self.entry) |entry| {
                if (std.mem.eql(u8, &entry.scope, &scope_digest) and std.mem.eql(u8, &entry.intent, &input_digest)) {
                    entry.retain();
                    self.hits +|= 1;
                    self.mutex.unlock(io);
                    return .{ .entry = entry };
                }
            }
            if (self.compiling) {
                var waiter: Waiters.Waiter = .{ .handoff = .{ .io = io }, .payload = {} };
                self.waiters.enqueue(&waiter);
                self.mutex.unlock(io);
                locked = false;
                try self.wait(io, &waiter, context);
                continue;
            }
            self.compiling = true;
            const generation = self.epoch;
            const old = self.entry;
            self.entry = null;
            self.mutex.unlock(io);
            if (old) |entry| entry.release();
            break generation;
        };
        errdefer {
            self.mutex.lockUncancelable(io);
            self.finishCompilation();
            self.mutex.unlock(io);
        }
        const entry = try Entry.create(backing, manager, scope_digest, input_digest);
        errdefer entry.release();
        const alloc = if (entry.budget) |*budget| budget.allocator() else backing;
        entry.program = programs.ProgramSet.initIntent(alloc, intent) catch |err| {
            if (entry.budget) |budget| if (budget.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
        try entry.program.?.requireScope(scope);
        try context.ensureActive();
        if (entry.budget) |*budget| _ = budget.releaseUnusedCredit();
        self.mutex.lockUncancelable(io);
        if (self.epoch == epoch) {
            entry.retain(); // Cache reference is independent of every reader.
            self.entry = entry;
        }
        self.compilations +|= 1;
        self.finishCompilation();
        self.mutex.unlock(io);
        return .{ .entry = entry };
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
        var lease = try cache.acquire(io, alloc, &manager, scope, borrowed, .{});
        defer lease.deinit();
        alloc.free(temporary);
        try lease.program().requireSourceManifest(alloc, &.{schema}, schema);
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, 1));
    }
    for (0..32) |_| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        var lease = try cache.acquire(io, failing.allocator(), &manager, scope, intent, .{});
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
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .{});
        lease.deinit();
    }
    var foreground = try manager.reserve(.dense_apply_working_set, 8 * 1024 * 1024);
    foreground.release();
    try std.testing.expect(cache.entry == null);
    var changed_scope = scope;
    changed_scope.plan_id[0] ^= 1;
    {
        var lease = try cache.acquire(io, alloc, &manager, changed_scope, intent, .{});
        lease.deinit();
    }
    try std.testing.expectEqual(@as(u64, 3), cache.compilations);
    var changed = intent;
    changed.allow_column_drops = true;
    try std.testing.expectError(error.RestoreStagingScopeChanged, cache.acquire(io, alloc, &manager, changed_scope, changed, .{}));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    {
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .{});
        lease.deinit();
    }
    const substituted = try std.mem.replaceOwned(u8, alloc, schema, "\"version\":1", "\"version\":2");
    defer alloc.free(substituted);
    changed = intent;
    changed.source_schemas = &.{substituted};
    try std.testing.expectError(error.RestoreStagingScopeChanged, cache.acquire(io, alloc, &manager, scope, changed, .{}));
    try std.testing.expect(cache.entry == null);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, cache.acquire(io, alloc, &manager, scope, intent, .{ .cancellation = .fromAtomic(&canceled) }));
    var denied = try manager.reserve(.relational_preparation_working_set, 4 * 1024 * 1024);
    try std.testing.expectError(error.ResourceBudgetExceeded, cache.acquire(io, alloc, &manager, scope, intent, .{}));
    denied.release();
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, cache.acquire(io, failing.allocator(), &manager, scope, intent, .{}));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    {
        var lease = try cache.acquire(io, alloc, &manager, scope, intent, .{});
        lease.deinit();
    }
    cache.evict(io);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    const Failure = struct {
        fn attempt(failing_alloc: std.mem.Allocator, bound_scope: staging.Scope, request: contract.Intent) !void {
            var candidate: Cache = .{};
            defer candidate.deinit(std.testing.io);
            var lease = try candidate.acquire(std.testing.io, failing_alloc, null, bound_scope, request, .{});
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
            var lease = self.cache.acquire(self.io, std.testing.allocator, self.manager, self.scope, self.intent, .{}) catch |err| {
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

test "relational index system rewrite program cache leases survive eviction and waiters cancel independently" {
    const alloc = std.testing.allocator;
    var runtime = std.Io.Threaded.init(alloc, .{});
    defer runtime.deinit();
    const io = runtime.io();
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
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var cache: Cache = .{};
    defer cache.deinit(io);

    // Immutable readers do not retain the publication mutex or serialize other
    // readers. A detached old generation stays charged until its final reader.
    {
        var first = try cache.acquire(io, alloc, &manager, scope, intent, .{});
        defer first.deinit();
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        var second = try cache.acquire(io, failing.allocator(), &manager, scope, intent, .{});
        defer second.deinit();
        try std.testing.expect(first.program() == second.program());
        const pinned = manager.snapshot().memory.used_bytes;
        try std.testing.expect(pinned > 0);
        try std.testing.expectEqual(@as(u64, 0), Cache.reclaim(&cache, 1));
        cache.evict(io);
        try std.testing.expect(cache.entry == null);
        try std.testing.expectEqual(pinned, manager.snapshot().memory.used_bytes);
        var successor_scope = scope;
        successor_scope.plan_id[0] ^= 1;
        var successor = try cache.acquire(io, alloc, &manager, successor_scope, intent, .{});
        try std.testing.expect(first.program() != successor.program());
        try first.program().requireScope(scope);
        try second.program().requireScope(scope);
        try successor.program().requireScope(successor_scope);
        successor.deinit();
        try std.testing.expect(Cache.reclaim(&cache, 1) > 0);
        try std.testing.expectEqual(pinned, manager.snapshot().memory.used_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    const Control = struct {
        io: std.Io,
        checkpoint: usize = 0,
        reached: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        block_compiler: bool = false,
        canceled: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,

        fn check(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.checkpoint += 1;
            // Third checkpoint: either compilation is complete but unpublished,
            // or a contending caller is enqueued and about to wait.
            if (self.checkpoint == 3) {
                self.reached.set(self.io);
                if (self.block_compiler) self.release.waitTimeout(self.io, .{ .duration = .{
                    .raw = std.Io.Duration.fromSeconds(10),
                    .clock = .awake,
                } }) catch |err| {
                    self.failure = err;
                    return true;
                };
            }
            return self.canceled.load(.acquire);
        }
    };
    const Worker = struct {
        io: std.Io,
        cache: *Cache,
        manager: *resources.ResourceManager,
        scope: staging.Scope,
        intent: contract.Intent,
        context: RequestContext,
        deadline_after_start: ?u64 = null,
        done: std.Io.Event = .unset,
        err: ?anyerror = null,
        lease: ?Cache.Lease = null,

        fn run(self: *@This()) void {
            defer self.done.set(self.io);
            if (self.deadline_after_start) |duration| self.context.deadline_ns = @import("antfly_platform").time.monotonicNs() + duration;
            self.lease = self.cache.acquire(self.io, std.testing.allocator, self.manager, self.scope, self.intent, self.context) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = std.Io.Duration.fromSeconds(5), .clock = .awake } };
    var leader_control: Control = .{ .io = io, .block_compiler = true };
    var leader: Worker = .{ .io = io, .cache = &cache, .manager = &manager, .scope = scope, .intent = intent, .context = .{ .cancellation = .{ .ptr = &leader_control, .is_cancelled_fn = Control.check } } };
    var leaders: std.Io.Group = .init;
    defer {
        leader_control.release.set(io);
        leaders.cancel(io);
        if (leader.lease) |*lease| lease.deinit();
    }
    try leaders.concurrent(io, Worker.run, .{&leader});
    try leader_control.reached.waitTimeout(io, timeout);

    // Callback cancellation, elapsed request deadline, and native Io task
    // cancellation all retire the waiter before the compiler is released.
    for (0..3) |mode| {
        var control: Control = .{ .io = io };
        var waiter: Worker = .{
            .io = io,
            .cache = &cache,
            .manager = &manager,
            .scope = scope,
            .intent = intent,
            .deadline_after_start = if (mode == 1) 250 * std.time.ns_per_ms else null,
            .context = .{
                .cancellation = .{ .ptr = &control, .is_cancelled_fn = Control.check },
            },
        };
        var waiters: std.Io.Group = .init;
        defer {
            waiters.cancel(io);
            if (waiter.lease) |*lease| lease.deinit();
        }
        try waiters.concurrent(io, Worker.run, .{&waiter});
        try control.reached.waitTimeout(io, timeout);
        if (mode == 0) control.canceled.store(true, .release);
        if (mode == 2) waiters.cancel(io);
        try waiter.done.waitTimeout(io, timeout);
        try std.testing.expectEqual(@as(?anyerror, if (mode == 1) error.DeadlineExceeded else error.Canceled), waiter.err);
        try std.testing.expect(waiter.lease == null);
        cache.mutex.lockUncancelable(io);
        const empty = cache.waiters.head == null;
        cache.mutex.unlock(io);
        try std.testing.expect(empty);
        try std.testing.expect(!leader.done.isSet());
    }
    // A live waiter still succeeds after its neighbors depart. Eviction during
    // compilation must not let the completing compiler resurrect retained state.
    var survivor_control: Control = .{ .io = io };
    var survivor: Worker = .{ .io = io, .cache = &cache, .manager = &manager, .scope = scope, .intent = intent, .context = .{ .cancellation = .{ .ptr = &survivor_control, .is_cancelled_fn = Control.check } } };
    var survivors: std.Io.Group = .init;
    defer {
        survivors.cancel(io);
        if (survivor.lease) |*lease| lease.deinit();
    }
    try survivors.concurrent(io, Worker.run, .{&survivor});
    try survivor_control.reached.waitTimeout(io, timeout);
    cache.evict(io);
    leader_control.release.set(io);
    try leader.done.waitTimeout(io, timeout);
    try survivor.done.waitTimeout(io, timeout);
    try std.testing.expect(leader.err == null and leader_control.failure == null);
    try std.testing.expect(survivor.err == null);
    // The old, invalidated compiler returned a valid private lease; the waiter
    // had to compile a fresh generation instead of hitting a resurrected one.
    try std.testing.expect(leader.lease.?.program() != survivor.lease.?.program());
    try leader.lease.?.program().requireScope(scope);
    try survivor.lease.?.program().requireScope(scope);
    cache.evict(io);
    leader.lease.?.deinit();
    leader.lease = null;
    survivor.lease.?.deinit();
    survivor.lease = null;
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);

    // A failed leader relinquishes single-flight ownership and wakes live
    // waiters; its request cancellation must not poison their independent work.
    var failed_control: Control = .{ .io = io, .block_compiler = true };
    var failed: Worker = .{ .io = io, .cache = &cache, .manager = &manager, .scope = scope, .intent = intent, .context = .{ .cancellation = .{ .ptr = &failed_control, .is_cancelled_fn = Control.check } } };
    var failing_group: std.Io.Group = .init;
    defer {
        failed_control.release.set(io);
        failing_group.cancel(io);
        if (failed.lease) |*lease| lease.deinit();
    }
    try failing_group.concurrent(io, Worker.run, .{&failed});
    try failed_control.reached.waitTimeout(io, timeout);
    var retry_control: Control = .{ .io = io };
    var retry: Worker = .{ .io = io, .cache = &cache, .manager = &manager, .scope = scope, .intent = intent, .context = .{ .cancellation = .{ .ptr = &retry_control, .is_cancelled_fn = Control.check } } };
    var retry_group: std.Io.Group = .init;
    defer {
        retry_group.cancel(io);
        if (retry.lease) |*lease| lease.deinit();
    }
    try retry_group.concurrent(io, Worker.run, .{&retry});
    try retry_control.reached.waitTimeout(io, timeout);
    failed_control.canceled.store(true, .release);
    failed_control.release.set(io);
    try failed.done.waitTimeout(io, timeout);
    try retry.done.waitTimeout(io, timeout);
    try std.testing.expectEqual(@as(?anyerror, error.Canceled), failed.err);
    try std.testing.expect(failed.lease == null and failed_control.failure == null);
    try std.testing.expect(retry.err == null);
    try retry.lease.?.program().requireScope(scope);
    retry.lease.?.deinit();
    retry.lease = null;
    cache.evict(io);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}
