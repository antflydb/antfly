// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const cache_budget = @import("../common/cache_budget.zig");

pub const Key = [32]u8;

pub const Config = struct {
    enabled: bool = true,
    max_bytes: usize = 64 * 1024 * 1024,
    ttl_ns: u64 = 5 * std.time.ns_per_min,
    max_inflight: usize = 16,
};

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    coalesced_waiters: u64 = 0,
    producer_computations: u64 = 0,
    uncached_computations: u64 = 0,
    producer_compute_ns_total: u64 = 0,
    inflight_rejections: u64 = 0,
    waiter_timeouts: u64 = 0,
    evictions: u64 = 0,
    expirations: u64 = 0,
    rejected_admissions: u64 = 0,
    entries: usize = 0,
    live_bytes: usize = 0,
    inflight: usize = 0,
    max_inflight: usize = 0,
};

pub const ComputeFn = *const fn (context: *anyopaque, alloc: std.mem.Allocator) anyerror![]f32;

/// Stable production work boundaries at which a deployment may account for
/// or delay cache work. The cache does not depend on VOPR: production owners
/// can install any thread-safe implementation, while deterministic scenarios
/// adapt these operations to a node-bound logical service-rate model. An
/// installed implementation must be thread-safe and outlive every cache call
/// that can reach it.
pub const WorkKind = enum {
    request,
    hit_copy,
    coalesced_wait,
    producer_compute,
};

pub const WorkCostPort = struct {
    ptr: *anyopaque,
    charge_fn: *const fn (*anyopaque, WorkKind, u64) anyerror!void,

    pub fn charge(self: WorkCostPort, kind: WorkKind, units: u64) !void {
        try self.charge_fn(self.ptr, kind, units);
    }
};

/// Optional production-safe lifecycle seam used by deterministic schedules to
/// suspend after an entry is pinned but before its value is copied. The hook
/// runs outside the cache mutex and is unset in production.
pub const LifecycleHook = struct {
    ptr: *anyopaque,
    after_pin: *const fn (*anyopaque, Key) void,

    fn afterPin(self: LifecycleHook, key: Key) void {
        self.after_pin(self.ptr, key);
    }
};

const Entry = struct {
    key: Key,
    vector: []f32,
    charge_bytes: usize,
    expires_at_ns: u64,
    pins: usize = 0,
    retired: bool = false,
    newer: ?*Entry = null,
    older: ?*Entry = null,
};

const Flight = struct {
    refs: usize = 1,
    done: bool = false,
    result: ?[]f32 = null,
    err: ?anyerror = null,
    ready: std.Io.Event = .unset,
};

/// Thread-safe byte-bounded LRU with per-key in-flight request coalescing.
/// Returned vectors are always owned by the caller.
pub const QueryEmbeddingCache = struct {
    const admission_expire_batch: usize = 64;
    const metrics_expire_batch: usize = 256;

    alloc: std.mem.Allocator,
    io: std.Io,
    config: Config,
    mutex: std.Io.Mutex = .init,
    entries: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    flights: std.AutoHashMapUnmanaged(Key, *Flight) = .empty,
    newest: ?*Entry = null,
    oldest: ?*Entry = null,
    live_bytes: usize = 0,
    active_pins: usize = 0,
    uncached_inflight: usize = 0,
    counters: Stats = .{},
    lifecycle_hook: ?LifecycleHook = null,
    work_cost_port: ?WorkCostPort = null,

    pub fn init(alloc: std.mem.Allocator, io: std.Io, config: Config) QueryEmbeddingCache {
        return .{
            .alloc = alloc,
            .io = io,
            .config = config,
        };
    }

    pub fn setLifecycleHook(self: *QueryEmbeddingCache, hook: ?LifecycleHook) void {
        self.lifecycle_hook = hook;
    }

    pub fn setWorkCostPort(self: *QueryEmbeddingCache, port: ?WorkCostPort) void {
        // Configuration is owner-controlled. Install or remove the port only
        // while no cache request is concurrently executing.
        self.work_cost_port = port;
    }

    /// Returns the logical cache charge used for admission. Exposing the same
    /// calculation lets resource envelopes and deterministic tests configure
    /// exact one-entry or N-entry budgets without duplicating layout math.
    pub fn entryChargeBytes(vector_len: usize) usize {
        return entryCharge(vector_len);
    }

    pub fn deinit(self: *QueryEmbeddingCache, budget: *cache_budget.CacheBudget) void {
        self.mutex.lockUncancelable(self.io);
        std.debug.assert(self.flights.count() == 0);
        std.debug.assert(self.uncached_inflight == 0);
        std.debug.assert(self.active_pins == 0);
        while (self.oldest) |entry| self.removeEntryLocked(entry, budget, false);
        self.entries.deinit(self.alloc);
        self.flights.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    pub fn getOrCompute(
        self: *QueryEmbeddingCache,
        budget: *cache_budget.CacheBudget,
        caller_alloc: std.mem.Allocator,
        key: Key,
        deadline_ns: ?u64,
        context: *anyopaque,
        compute: ComputeFn,
    ) ![]f32 {
        try self.chargeWork(.request, 1);
        if (deadlineExpiredAt(self.nowNs(), deadline_ns)) return error.Timeout;
        if (!self.config.enabled) return self.computeUncachedInner(caller_alloc, deadline_ns, context, compute);

        const io = self.io;
        self.mutex.lockUncancelable(io);
        if (self.entries.get(key)) |entry| {
            const now = self.nowNs();
            if (now < entry.expires_at_ns) {
                self.touchLocked(entry, now);
                self.counters.hits +|= 1;
                self.pinEntryLocked(entry);
                self.mutex.unlock(io);

                if (self.lifecycle_hook) |hook| hook.afterPin(key);

                self.chargeWork(.hit_copy, entry.vector.len) catch |err| {
                    self.mutex.lockUncancelable(io);
                    self.unpinEntryLocked(entry, budget);
                    self.mutex.unlock(io);
                    return err;
                };
                if (deadlineExpiredAt(self.nowNs(), deadline_ns)) {
                    self.mutex.lockUncancelable(io);
                    self.unpinEntryLocked(entry, budget);
                    self.mutex.unlock(io);
                    return error.Timeout;
                }

                const result = caller_alloc.dupe(f32, entry.vector) catch |err| {
                    self.mutex.lockUncancelable(io);
                    self.unpinEntryLocked(entry, budget);
                    self.mutex.unlock(io);
                    return err;
                };
                self.mutex.lockUncancelable(io);
                self.unpinEntryLocked(entry, budget);
                self.mutex.unlock(io);
                return result;
            }
            self.removeEntryLocked(entry, budget, true);
        }

        if (self.flights.get(key)) |flight| {
            flight.refs += 1;
            self.counters.coalesced_waiters +|= 1;
            self.mutex.unlock(io);
            self.chargeWork(.coalesced_wait, 1) catch |err| {
                self.mutex.lockUncancelable(io);
                self.releaseFlightLocked(key, flight);
                self.mutex.unlock(io);
                return err;
            };
            self.waitForFlight(flight, deadline_ns) catch |err| {
                self.mutex.lockUncancelable(io);
                if (err == error.Timeout) self.counters.waiter_timeouts +|= 1;
                self.releaseFlightLocked(key, flight);
                self.mutex.unlock(io);
                return err;
            };
            const result = copyFlightResult(caller_alloc, flight) catch |err| {
                self.mutex.lockUncancelable(io);
                self.releaseFlightLocked(key, flight);
                self.mutex.unlock(io);
                return err;
            };
            self.mutex.lockUncancelable(io);
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return result;
        }

        if (deadlineExpiredAt(self.nowNs(), deadline_ns)) {
            self.mutex.unlock(io);
            return error.Timeout;
        }

        if (self.totalInflightLocked() >= self.config.max_inflight) {
            self.counters.inflight_rejections +|= 1;
            self.mutex.unlock(io);
            return error.QueryEmbeddingOverloaded;
        }

        const flight = self.alloc.create(Flight) catch |err| {
            self.counters.rejected_admissions +|= 1;
            self.mutex.unlock(io);
            return err;
        };
        flight.* = .{};
        self.flights.put(self.alloc, key, flight) catch |err| {
            self.alloc.destroy(flight);
            self.counters.rejected_admissions +|= 1;
            self.mutex.unlock(io);
            return err;
        };
        self.counters.misses +|= 1;
        self.counters.producer_computations +|= 1;
        self.mutex.unlock(io);

        const compute_started_ns = self.nowNs();
        self.chargeWork(.producer_compute, 1) catch |err| {
            self.mutex.lockUncancelable(io);
            self.recordProducerDurationLocked(compute_started_ns);
            flight.err = err;
            flight.done = true;
            flight.ready.set(io);
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return err;
        };
        const computed = compute(context, self.alloc) catch |err| {
            self.mutex.lockUncancelable(io);
            self.recordProducerDurationLocked(compute_started_ns);
            flight.err = err;
            flight.done = true;
            flight.ready.set(io);
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return err;
        };

        self.mutex.lockUncancelable(io);
        self.recordProducerDurationLocked(compute_started_ns);
        flight.result = computed;
        flight.done = true;
        self.admitLocked(key, computed, budget) catch {
            self.counters.rejected_admissions +|= 1;
        };
        flight.ready.set(io);
        self.mutex.unlock(io);

        const result = copyFlightResult(caller_alloc, flight) catch |err| {
            self.mutex.lockUncancelable(io);
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return err;
        };
        self.mutex.lockUncancelable(io);
        self.releaseFlightLocked(key, flight);
        self.mutex.unlock(io);
        return result;
    }

    /// Run a query embedding that is unsafe to retain or coalesce while still
    /// sharing the provider admission bound with cacheable producers.
    pub fn computeUncached(
        self: *QueryEmbeddingCache,
        caller_alloc: std.mem.Allocator,
        deadline_ns: ?u64,
        context: *anyopaque,
        compute: ComputeFn,
    ) ![]f32 {
        try self.chargeWork(.request, 1);
        return self.computeUncachedInner(caller_alloc, deadline_ns, context, compute);
    }

    fn computeUncachedInner(
        self: *QueryEmbeddingCache,
        caller_alloc: std.mem.Allocator,
        deadline_ns: ?u64,
        context: *anyopaque,
        compute: ComputeFn,
    ) ![]f32 {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        if (deadlineExpiredAt(self.nowNs(), deadline_ns)) {
            self.mutex.unlock(io);
            return error.Timeout;
        }
        if (self.totalInflightLocked() >= self.config.max_inflight) {
            self.counters.inflight_rejections +|= 1;
            self.mutex.unlock(io);
            return error.QueryEmbeddingOverloaded;
        }
        self.uncached_inflight += 1;
        self.counters.producer_computations +|= 1;
        self.counters.uncached_computations +|= 1;
        self.mutex.unlock(io);

        const compute_started_ns = self.nowNs();
        self.chargeWork(.producer_compute, 1) catch |err| {
            self.finishUncachedCompute(compute_started_ns);
            return err;
        };
        const result = compute(context, caller_alloc) catch |err| {
            self.finishUncachedCompute(compute_started_ns);
            return err;
        };
        self.finishUncachedCompute(compute_started_ns);
        return result;
    }

    pub fn stats(self: *QueryEmbeddingCache, budget: *cache_budget.CacheBudget) Stats {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.expireOldestLocked(self.nowNs(), budget, metrics_expire_batch);
        var result = self.counters;
        result.entries = self.entries.count();
        result.live_bytes = self.live_bytes;
        result.inflight = self.totalInflightLocked();
        result.max_inflight = self.config.max_inflight;
        return result;
    }

    fn recordProducerDurationLocked(self: *QueryEmbeddingCache, started_ns: u64) void {
        const elapsed_ns = self.nowNs() -| started_ns;
        self.counters.producer_compute_ns_total +|= elapsed_ns;
    }

    fn totalInflightLocked(self: *const QueryEmbeddingCache) usize {
        return self.flights.count() + self.uncached_inflight;
    }

    fn finishUncachedCompute(self: *QueryEmbeddingCache, started_ns: u64) void {
        const io = self.io;
        self.mutex.lockUncancelable(io);
        std.debug.assert(self.uncached_inflight > 0);
        self.uncached_inflight -= 1;
        self.recordProducerDurationLocked(started_ns);
        self.mutex.unlock(io);
    }

    fn waitForFlight(self: *QueryEmbeddingCache, flight: *Flight, deadline_ns: ?u64) !void {
        const deadline = deadline_ns orelse {
            // A coalesced waiter is not the producer and owns no shared state
            // that requires an uncancelable region. Let its enclosing std.Io
            // task be canceled, then release its flight reference through the
            // caller's existing error path.
            try flight.ready.wait(self.io);
            return;
        };
        while (!flight.ready.isSet()) {
            const now = self.nowNs();
            if (now >= deadline) return error.Timeout;
            flight.ready.waitTimeout(self.io, .{
                .duration = .{
                    .raw = std.Io.Duration.fromNanoseconds(@intCast(deadline - now)),
                    .clock = .awake,
                },
            }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return error.Canceled,
            };
        }
    }

    fn releaseFlightLocked(self: *QueryEmbeddingCache, key: Key, flight: *Flight) void {
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) return;
        std.debug.assert(flight.done);
        _ = self.flights.remove(key);
        if (flight.result) |result| self.alloc.free(result);
        self.alloc.destroy(flight);
    }

    fn admitLocked(self: *QueryEmbeddingCache, key: Key, vector: []const f32, budget: *cache_budget.CacheBudget) !void {
        if (self.config.max_bytes == 0 or self.config.ttl_ns == 0) return;
        const charge = entryCharge(vector.len);
        if (charge > self.config.max_bytes) {
            self.counters.rejected_admissions +|= 1;
            return;
        }
        self.expireOldestLocked(self.nowNs(), budget, admission_expire_batch);
        while (self.live_bytes > self.config.max_bytes - charge) {
            const victim = self.oldest orelse break;
            self.removeEntryLocked(victim, budget, false);
        }
        while (!budget.tryReserve(charge)) {
            const victim = self.oldest orelse {
                self.counters.rejected_admissions +|= 1;
                return;
            };
            self.removeEntryLocked(victim, budget, false);
        }
        errdefer budget.release(charge);

        const owned_vector = try self.alloc.dupe(f32, vector);
        errdefer self.alloc.free(owned_vector);
        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);
        entry.* = .{
            .key = key,
            .vector = owned_vector,
            .charge_bytes = charge,
            .expires_at_ns = self.nowNs() +| self.config.ttl_ns,
        };
        try self.entries.put(self.alloc, key, entry);
        self.linkNewestLocked(entry);
        self.live_bytes += charge;
    }

    fn entryCharge(vector_len: usize) usize {
        // Include the owned vector, entry, map key/value pair, and conservative
        // hash-table occupancy overhead so the logical cap bounds allocator use.
        const vector_bytes = std.math.mul(usize, vector_len, @sizeOf(f32)) catch return std.math.maxInt(usize);
        return std.math.add(usize, vector_bytes, @sizeOf(Entry) + 2 * (@sizeOf(Key) + @sizeOf(*Entry))) catch std.math.maxInt(usize);
    }

    fn touchLocked(self: *QueryEmbeddingCache, entry: *Entry, now_ns: u64) void {
        entry.expires_at_ns = now_ns +| self.config.ttl_ns;
        if (self.newest == entry) return;
        self.unlinkLocked(entry);
        self.linkNewestLocked(entry);
    }

    fn linkNewestLocked(self: *QueryEmbeddingCache, entry: *Entry) void {
        entry.newer = null;
        entry.older = self.newest;
        if (self.newest) |current| current.newer = entry else self.oldest = entry;
        self.newest = entry;
    }

    fn unlinkLocked(self: *QueryEmbeddingCache, entry: *Entry) void {
        if (entry.newer) |newer| newer.older = entry.older else self.newest = entry.older;
        if (entry.older) |older| older.newer = entry.newer else self.oldest = entry.newer;
        entry.newer = null;
        entry.older = null;
    }

    fn expireOldestLocked(self: *QueryEmbeddingCache, now_ns: u64, budget: *cache_budget.CacheBudget, max_entries: usize) void {
        var expired: usize = 0;
        while (expired < max_entries) : (expired += 1) {
            const entry = self.oldest orelse return;
            if (now_ns < entry.expires_at_ns) return;
            self.removeEntryLocked(entry, budget, true);
        }
    }

    fn removeEntryLocked(self: *QueryEmbeddingCache, entry: *Entry, budget: *cache_budget.CacheBudget, expired: bool) void {
        std.debug.assert(!entry.retired);
        _ = self.entries.remove(entry.key);
        self.unlinkLocked(entry);
        entry.retired = true;
        if (entry.pins == 0) self.destroyEntryLocked(entry, budget);
        if (expired) self.counters.expirations +|= 1 else self.counters.evictions +|= 1;
    }

    fn pinEntryLocked(self: *QueryEmbeddingCache, entry: *Entry) void {
        std.debug.assert(!entry.retired);
        entry.pins += 1;
        self.active_pins += 1;
    }

    fn unpinEntryLocked(self: *QueryEmbeddingCache, entry: *Entry, budget: *cache_budget.CacheBudget) void {
        std.debug.assert(entry.pins > 0);
        std.debug.assert(self.active_pins > 0);
        entry.pins -= 1;
        self.active_pins -= 1;
        if (entry.retired and entry.pins == 0) self.destroyEntryLocked(entry, budget);
    }

    fn destroyEntryLocked(self: *QueryEmbeddingCache, entry: *Entry, budget: *cache_budget.CacheBudget) void {
        std.debug.assert(entry.retired);
        std.debug.assert(entry.pins == 0);
        self.live_bytes -= entry.charge_bytes;
        budget.release(entry.charge_bytes);
        self.alloc.free(entry.vector);
        self.alloc.destroy(entry);
    }

    fn nowNs(self: *const QueryEmbeddingCache) u64 {
        return @intCast(@max(std.Io.Timestamp.now(self.io, .awake).toNanoseconds(), 0));
    }

    fn chargeWork(self: *QueryEmbeddingCache, kind: WorkKind, units: usize) !void {
        if (self.work_cost_port) |port| try port.charge(kind, @intCast(units));
    }
};

fn copyFlightResult(alloc: std.mem.Allocator, flight: *const Flight) ![]f32 {
    std.debug.assert(flight.done);
    if (flight.err) |err| return err;
    return try alloc.dupe(f32, flight.result orelse return error.QueryEmbeddingProducerFailed);
}

fn deadlineExpiredAt(now_ns: u64, deadline_ns: ?u64) bool {
    const deadline = deadline_ns orelse return false;
    return now_ns >= deadline;
}

const TestCompute = struct {
    calls: std.atomic.Value(u64) = .init(0),
    value: f32,

    fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
        const self: *TestCompute = @ptrCast(@alignCast(ptr));
        _ = self.calls.fetchAdd(1, .monotonic);
        const result = try alloc.alloc(f32, 2);
        result[0] = self.value;
        result[1] = self.value + 1;
        return result;
    }
};

pub fn testOwnedValuesAndHits() !void {
    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, std.Io.Threaded.global_single_threaded.io(), .{});
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 4 };
    const key: Key = [_]u8{7} ** 32;

    const first = try cache.getOrCompute(&budget, std.testing.allocator, key, null, &compute, TestCompute.run);
    defer std.testing.allocator.free(first);
    first[0] = 99;
    const second = try cache.getOrCompute(&budget, std.testing.allocator, key, null, &compute, TestCompute.run);
    defer std.testing.allocator.free(second);

    try std.testing.expectEqual(@as(f32, 4), second[0]);
    try std.testing.expectEqual(@as(u64, 1), compute.calls.load(.monotonic));
    const current = cache.stats(&budget);
    try std.testing.expectEqual(@as(u64, 1), current.hits);
    try std.testing.expectEqual(@as(u64, 1), current.misses);
}

test "query embedding cache owns values and serves LRU hits" {
    try testOwnedValuesAndHits();
}

pub fn testConcurrentCoalescing() !void {
    const SlowCompute = struct {
        calls: std.atomic.Value(u64) = .init(0),
        io: std.Io,

        fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .release);
            self.io.sleep(.fromNanoseconds(50 * std.time.ns_per_ms), .awake) catch {};
            return try alloc.dupe(f32, &.{ 1, 2, 3 });
        }
    };
    const Worker = struct {
        cache: *QueryEmbeddingCache,
        budget: *cache_budget.CacheBudget,
        compute: *SlowCompute,
        key: Key,
        result: ?[]f32 = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.result = self.cache.getOrCompute(self.budget, std.heap.page_allocator, self.key, null, self.compute, SlowCompute.run) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var compute_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer compute_io.deinit();
    var budget = cache_budget.CacheBudget.init(0);
    var cache = QueryEmbeddingCache.init(std.heap.page_allocator, compute_io.io(), .{ .max_bytes = 0 });
    defer cache.deinit(&budget);
    var compute = SlowCompute{ .io = compute_io.io() };
    const key: Key = [_]u8{9} ** 32;
    var first = Worker{ .cache = &cache, .budget = &budget, .compute = &compute, .key = key };
    var second = Worker{ .cache = &cache, .budget = &budget, .compute = &compute, .key = key };

    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    while (compute.calls.load(.acquire) == 0) std.atomic.spinLoopHint();
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});
    first_thread.join();
    second_thread.join();
    defer if (first.result) |result| std.heap.page_allocator.free(result);
    defer if (second.result) |result| std.heap.page_allocator.free(result);

    try std.testing.expectEqual(@as(?anyerror, null), first.err);
    try std.testing.expectEqual(@as(?anyerror, null), second.err);
    try std.testing.expectEqual(@as(u64, 1), compute.calls.load(.monotonic));
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, first.result.?);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, second.result.?);
    const current = cache.stats(&budget);
    try std.testing.expectEqual(@as(u64, 1), current.coalesced_waiters);
    try std.testing.expect(current.producer_compute_ns_total > 0);
    try std.testing.expectEqual(@as(usize, 0), current.entries);
    try std.testing.expectEqual(@as(u64, 0), current.rejected_admissions);
}

test "query embedding cache coalesces concurrent misses" {
    try testConcurrentCoalescing();
}

pub fn testInflightAdmissionBound() !void {
    const BlockingCompute = struct {
        calls: std.atomic.Value(u64) = .init(0),
        release: std.atomic.Value(bool) = .init(false),
        io: std.Io,

        fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
            return try alloc.dupe(f32, &.{1});
        }
    };
    const Worker = struct {
        cache: *QueryEmbeddingCache,
        budget: *cache_budget.CacheBudget,
        compute: *BlockingCompute,
        key: Key,
        result: ?[]f32 = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.result = self.cache.getOrCompute(self.budget, std.heap.page_allocator, self.key, null, self.compute, BlockingCompute.run) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    const Releaser = struct {
        fn run(compute: *BlockingCompute) void {
            compute.io.sleep(.fromNanoseconds(100 * std.time.ns_per_ms), .awake) catch {};
            compute.release.store(true, .release);
        }
    };

    var compute_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer compute_io.deinit();
    var budget = cache_budget.CacheBudget.init(0);
    var cache = QueryEmbeddingCache.init(std.heap.page_allocator, compute_io.io(), .{ .max_bytes = 0, .max_inflight = 1 });
    defer cache.deinit(&budget);
    var compute = BlockingCompute{ .io = compute_io.io() };
    var producer = Worker{ .cache = &cache, .budget = &budget, .compute = &compute, .key = [_]u8{1} ** 32 };
    const producer_thread = try std.Thread.spawn(.{}, Worker.run, .{&producer});
    while (compute.calls.load(.acquire) == 0) std.atomic.spinLoopHint();
    const releaser_thread = try std.Thread.spawn(.{}, Releaser.run, .{&compute});

    const producer_key: Key = [_]u8{1} ** 32;
    try std.testing.expectError(
        error.Timeout,
        cache.getOrCompute(
            &budget,
            std.testing.allocator,
            producer_key,
            @as(u64, @intCast(@max(std.Io.Timestamp.now(compute_io.io(), .awake).toNanoseconds(), 0))) +| std.time.ns_per_ms,
            &compute,
            BlockingCompute.run,
        ),
    );
    const rejected_key: Key = [_]u8{2} ** 32;
    try std.testing.expectError(
        error.QueryEmbeddingOverloaded,
        cache.getOrCompute(&budget, std.testing.allocator, rejected_key, null, &compute, BlockingCompute.run),
    );
    try std.testing.expectError(
        error.QueryEmbeddingOverloaded,
        cache.computeUncached(std.testing.allocator, null, &compute, BlockingCompute.run),
    );
    const current = cache.stats(&budget);
    try std.testing.expectEqual(@as(u64, 2), current.inflight_rejections);
    try std.testing.expectEqual(@as(u64, 1), current.waiter_timeouts);
    try std.testing.expectEqual(@as(u64, 1), current.coalesced_waiters);
    try std.testing.expectEqual(@as(usize, 1), current.inflight);
    try std.testing.expectEqual(@as(u64, 1), compute.calls.load(.monotonic));

    compute.release.store(true, .release);
    releaser_thread.join();
    producer_thread.join();
    defer if (producer.result) |result| std.heap.page_allocator.free(result);
    try std.testing.expectEqual(@as(?anyerror, null), producer.err);

    const uncached = try cache.computeUncached(std.testing.allocator, null, &compute, BlockingCompute.run);
    defer std.testing.allocator.free(uncached);
    const completed = cache.stats(&budget);
    try std.testing.expectEqual(@as(u64, 1), completed.uncached_computations);
    try std.testing.expectEqual(@as(usize, 0), completed.inflight);
    try std.testing.expectEqual(@as(u64, 2), compute.calls.load(.monotonic));
}

pub fn testDisabledCacheRetainsAdmissionBound() !void {
    const BlockingCompute = struct {
        calls: std.atomic.Value(u64) = .init(0),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
            return try alloc.dupe(f32, &.{1});
        }
    };
    const Worker = struct {
        cache: *QueryEmbeddingCache,
        budget: *cache_budget.CacheBudget,
        compute: *BlockingCompute,
        result: ?[]f32 = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.result = self.cache.getOrCompute(
                self.budget,
                std.heap.page_allocator,
                [_]u8{1} ** 32,
                null,
                self.compute,
                BlockingCompute.run,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    const io = std.Io.Threaded.global_single_threaded.io();
    var budget = cache_budget.CacheBudget.init(0);
    var cache = QueryEmbeddingCache.init(std.heap.page_allocator, io, .{
        .enabled = false,
        .max_bytes = 0,
        .max_inflight = 1,
    });
    defer cache.deinit(&budget);
    var compute = BlockingCompute{};
    var worker = Worker{ .cache = &cache, .budget = &budget, .compute = &compute };
    const producer_thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    while (compute.calls.load(.acquire) == 0) std.atomic.spinLoopHint();

    try std.testing.expectError(
        error.QueryEmbeddingOverloaded,
        cache.getOrCompute(&budget, std.testing.allocator, [_]u8{2} ** 32, null, &compute, BlockingCompute.run),
    );
    try std.testing.expectEqual(@as(usize, 1), cache.stats(&budget).inflight);

    compute.release.store(true, .release);
    producer_thread.join();
    defer if (worker.result) |result| std.heap.page_allocator.free(result);
    try std.testing.expectEqual(@as(?anyerror, null), worker.err);
    try std.testing.expectEqual(@as(usize, 0), cache.stats(&budget).entries);
}

test "disabled query embedding cache retains provider admission" {
    try testDisabledCacheRetainsAdmissionBound();
}

test "query embedding cache bounds distinct in-flight misses" {
    try testInflightAdmissionBound();
}

pub fn testFlightBookkeepingOOMFailsClosed() !void {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(failing.allocator(), std.Io.Threaded.global_single_threaded.io(), .{});
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 1 };

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(
        error.OutOfMemory,
        cache.getOrCompute(&budget, std.testing.allocator, [_]u8{8} ** 32, null, &compute, TestCompute.run),
    );
    try std.testing.expectEqual(@as(u64, 0), compute.calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), cache.stats(&budget).rejected_admissions);
}

test "query embedding cache fails closed when flight bookkeeping allocation fails" {
    try testFlightBookkeepingOOMFailsClosed();
}

pub fn testByteBudgetEviction() !void {
    const one_entry_bytes = QueryEmbeddingCache.entryCharge(2);
    var budget = cache_budget.CacheBudget.init(one_entry_bytes);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, std.Io.Threaded.global_single_threaded.io(), .{ .max_bytes = one_entry_bytes });
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 8 };
    const first_key: Key = [_]u8{1} ** 32;
    const second_key: Key = [_]u8{2} ** 32;

    const first = try cache.getOrCompute(&budget, std.testing.allocator, first_key, null, &compute, TestCompute.run);
    std.testing.allocator.free(first);
    const second = try cache.getOrCompute(&budget, std.testing.allocator, second_key, null, &compute, TestCompute.run);
    std.testing.allocator.free(second);
    const second_hit = try cache.getOrCompute(&budget, std.testing.allocator, second_key, null, &compute, TestCompute.run);
    std.testing.allocator.free(second_hit);
    const first_again = try cache.getOrCompute(&budget, std.testing.allocator, first_key, null, &compute, TestCompute.run);
    std.testing.allocator.free(first_again);

    const current = cache.stats(&budget);
    try std.testing.expectEqual(@as(usize, 1), current.entries);
    try std.testing.expectEqual(one_entry_bytes, current.live_bytes);
    try std.testing.expectEqual(@as(u64, 2), current.evictions);
    try std.testing.expectEqual(@as(u64, 3), compute.calls.load(.monotonic));
    try std.testing.expectEqual(one_entry_bytes, budget.stats().used_bytes);
}

test "query embedding cache enforces byte budget with LRU eviction" {
    try testByteBudgetEviction();
}

pub fn testPinnedHitRetainsBudgetUntilCopyCompletes() !void {
    const io = std.Io.Threaded.global_single_threaded.io();
    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, io, .{});
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 5 };
    const key: Key = [_]u8{4} ** 32;

    const result = try cache.getOrCompute(&budget, std.testing.allocator, key, null, &compute, TestCompute.run);
    std.testing.allocator.free(result);
    const charge = cache.live_bytes;

    cache.mutex.lockUncancelable(io);
    const entry = cache.entries.get(key).?;
    cache.pinEntryLocked(entry);
    cache.removeEntryLocked(entry, &budget, false);
    const retained_entries = cache.entries.count();
    const retained_live_bytes = cache.live_bytes;
    const retained_budget_bytes = budget.stats().used_bytes;
    cache.mutex.unlock(io);
    try std.testing.expectEqual(@as(usize, 0), retained_entries);
    try std.testing.expectEqual(charge, retained_live_bytes);
    try std.testing.expectEqual(charge, retained_budget_bytes);

    cache.mutex.lockUncancelable(io);
    cache.unpinEntryLocked(entry, &budget);
    const released_live_bytes = cache.live_bytes;
    const released_budget_bytes = budget.stats().used_bytes;
    cache.mutex.unlock(io);
    try std.testing.expectEqual(@as(usize, 0), released_live_bytes);
    try std.testing.expectEqual(@as(usize, 0), released_budget_bytes);
}

test "query embedding cache pins hit values outside the LRU lock" {
    try testPinnedHitRetainsBudgetUntilCopyCompletes();
}

pub fn testStatsExpireIdleEntries() !void {
    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, std.Io.Threaded.global_single_threaded.io(), .{ .ttl_ns = 0 });
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 3 };
    const key: Key = [_]u8{3} ** 32;

    const result = try cache.getOrCompute(&budget, std.testing.allocator, key, null, &compute, TestCompute.run);
    std.testing.allocator.free(result);
    const current = cache.stats(&budget);
    try std.testing.expectEqual(@as(usize, 0), current.entries);
    try std.testing.expectEqual(@as(usize, 0), current.live_bytes);
    try std.testing.expectEqual(@as(u64, 0), current.expirations);
    try std.testing.expectEqual(@as(u64, 0), current.rejected_admissions);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().used_bytes);
}

test "query embedding cache skips zero TTL retention" {
    try testStatsExpireIdleEntries();
}

pub fn testStatsBoundExpirationWork() !void {
    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, std.Io.Threaded.global_single_threaded.io(), .{});
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 3 };

    for (0..300) |i| {
        var key: Key = [_]u8{0} ** 32;
        key[0] = @intCast(i & 0xff);
        key[1] = @intCast(i >> 8);
        const result = try cache.getOrCompute(&budget, std.testing.allocator, key, null, &compute, TestCompute.run);
        std.testing.allocator.free(result);
    }
    var cursor = cache.oldest;
    while (cursor) |entry| {
        entry.expires_at_ns = 0;
        cursor = entry.newer;
    }

    const first = cache.stats(&budget);
    try std.testing.expectEqual(@as(usize, 44), first.entries);
    try std.testing.expectEqual(@as(u64, 256), first.expirations);
    const second = cache.stats(&budget);
    try std.testing.expectEqual(@as(usize, 0), second.entries);
    try std.testing.expectEqual(@as(u64, 300), second.expirations);
}

test "query embedding cache bounds metrics expiration work" {
    try testStatsBoundExpirationWork();
}
