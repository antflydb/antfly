// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const cache_budget = @import("../common/cache_budget.zig");
const platform_time = @import("../platform/time.zig");

pub const Key = [32]u8;

pub const Config = struct {
    enabled: bool = true,
    max_bytes: usize = 64 * 1024 * 1024,
    ttl_ns: u64 = 5 * std.time.ns_per_min,
};

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    coalesced_waiters: u64 = 0,
    producer_computations: u64 = 0,
    evictions: u64 = 0,
    expirations: u64 = 0,
    rejected_admissions: u64 = 0,
    entries: usize = 0,
    live_bytes: usize = 0,
    inflight: usize = 0,
};

pub const ComputeFn = *const fn (context: *anyopaque, alloc: std.mem.Allocator) anyerror![]f32;

const Entry = struct {
    key: Key,
    vector: []f32,
    charge_bytes: usize,
    expires_at_ns: u64,
    newer: ?*Entry = null,
    older: ?*Entry = null,
};

const Flight = struct {
    refs: usize = 1,
    done: bool = false,
    result: ?[]f32 = null,
    err: ?anyerror = null,
};

/// Thread-safe byte-bounded LRU with per-key in-flight request coalescing.
/// Returned vectors are always owned by the caller.
pub const QueryEmbeddingCache = struct {
    alloc: std.mem.Allocator,
    config: Config,
    threaded: std.Io.Threaded,
    mutex: std.Io.Mutex = .init,
    ready: std.Io.Condition = .init,
    entries: std.AutoHashMapUnmanaged(Key, *Entry) = .empty,
    flights: std.AutoHashMapUnmanaged(Key, *Flight) = .empty,
    newest: ?*Entry = null,
    oldest: ?*Entry = null,
    live_bytes: usize = 0,
    counters: Stats = .{},

    pub fn init(alloc: std.mem.Allocator, config: Config) QueryEmbeddingCache {
        return .{
            .alloc = alloc,
            .config = config,
            .threaded = std.Io.Threaded.init(alloc, .{}),
        };
    }

    pub fn deinit(self: *QueryEmbeddingCache, budget: *cache_budget.CacheBudget) void {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        std.debug.assert(self.flights.count() == 0);
        while (self.oldest) |entry| self.removeEntryLocked(entry, budget, false);
        self.entries.deinit(self.alloc);
        self.flights.deinit(self.alloc);
        self.mutex.unlock(io);
        self.threaded.deinit();
        self.* = undefined;
    }

    pub fn getOrCompute(
        self: *QueryEmbeddingCache,
        budget: *cache_budget.CacheBudget,
        caller_alloc: std.mem.Allocator,
        key: Key,
        context: *anyopaque,
        compute: ComputeFn,
    ) ![]f32 {
        if (!self.config.enabled or self.config.max_bytes == 0) return compute(context, caller_alloc);

        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        if (self.entries.get(key)) |entry| {
            const now = platform_time.monotonicNs();
            if (now < entry.expires_at_ns) {
                self.touchLocked(entry, now);
                self.counters.hits += 1;
                const result = caller_alloc.dupe(f32, entry.vector) catch |err| {
                    self.mutex.unlock(io);
                    return err;
                };
                self.mutex.unlock(io);
                return result;
            }
            self.removeEntryLocked(entry, budget, true);
        }

        if (self.flights.get(key)) |flight| {
            flight.refs += 1;
            self.counters.coalesced_waiters += 1;
            while (!flight.done) self.ready.waitUncancelable(io, &self.mutex);
            const result = self.copyFlightResultLocked(caller_alloc, flight) catch |err| {
                self.releaseFlightLocked(key, flight);
                self.mutex.unlock(io);
                return err;
            };
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return result;
        }

        const flight = self.alloc.create(Flight) catch {
            self.counters.rejected_admissions += 1;
            self.mutex.unlock(io);
            return compute(context, caller_alloc);
        };
        flight.* = .{};
        self.flights.put(self.alloc, key, flight) catch {
            self.alloc.destroy(flight);
            self.counters.rejected_admissions += 1;
            self.mutex.unlock(io);
            return compute(context, caller_alloc);
        };
        self.counters.misses += 1;
        self.counters.producer_computations += 1;
        self.mutex.unlock(io);

        const computed = compute(context, self.alloc) catch |err| {
            self.mutex.lockUncancelable(io);
            flight.err = err;
            flight.done = true;
            self.ready.broadcast(io);
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return err;
        };

        self.mutex.lockUncancelable(io);
        flight.result = computed;
        flight.done = true;
        self.admitLocked(key, computed, budget) catch {
            self.counters.rejected_admissions += 1;
        };
        self.ready.broadcast(io);
        const result = self.copyFlightResultLocked(caller_alloc, flight) catch |err| {
            self.releaseFlightLocked(key, flight);
            self.mutex.unlock(io);
            return err;
        };
        self.releaseFlightLocked(key, flight);
        self.mutex.unlock(io);
        return result;
    }

    pub fn stats(self: *QueryEmbeddingCache) Stats {
        const io = self.threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var result = self.counters;
        result.entries = self.entries.count();
        result.live_bytes = self.live_bytes;
        result.inflight = self.flights.count();
        return result;
    }

    fn copyFlightResultLocked(self: *QueryEmbeddingCache, alloc: std.mem.Allocator, flight: *Flight) ![]f32 {
        _ = self;
        if (flight.err) |err| return err;
        return try alloc.dupe(f32, flight.result orelse return error.QueryEmbeddingProducerFailed);
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
        const charge = entryCharge(vector.len);
        if (charge > self.config.max_bytes) {
            self.counters.rejected_admissions += 1;
            return;
        }
        self.expireOldestLocked(platform_time.monotonicNs(), budget);
        while (self.live_bytes > self.config.max_bytes - charge) {
            const victim = self.oldest orelse break;
            self.removeEntryLocked(victim, budget, false);
        }
        while (!budget.tryReserve(charge)) {
            const victim = self.oldest orelse {
                self.counters.rejected_admissions += 1;
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
            .expires_at_ns = platform_time.monotonicNs() +| self.config.ttl_ns,
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

    fn expireOldestLocked(self: *QueryEmbeddingCache, now_ns: u64, budget: *cache_budget.CacheBudget) void {
        while (self.oldest) |entry| {
            if (now_ns < entry.expires_at_ns) return;
            self.removeEntryLocked(entry, budget, true);
        }
    }

    fn removeEntryLocked(self: *QueryEmbeddingCache, entry: *Entry, budget: *cache_budget.CacheBudget, expired: bool) void {
        _ = self.entries.remove(entry.key);
        self.unlinkLocked(entry);
        self.live_bytes -= entry.charge_bytes;
        budget.release(entry.charge_bytes);
        self.alloc.free(entry.vector);
        self.alloc.destroy(entry);
        if (expired) self.counters.expirations += 1 else self.counters.evictions += 1;
    }
};

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
    var cache = QueryEmbeddingCache.init(std.testing.allocator, .{});
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 4 };
    const key: Key = [_]u8{7} ** 32;

    const first = try cache.getOrCompute(&budget, std.testing.allocator, key, &compute, TestCompute.run);
    defer std.testing.allocator.free(first);
    first[0] = 99;
    const second = try cache.getOrCompute(&budget, std.testing.allocator, key, &compute, TestCompute.run);
    defer std.testing.allocator.free(second);

    try std.testing.expectEqual(@as(f32, 4), second[0]);
    try std.testing.expectEqual(@as(u64, 1), compute.calls.load(.monotonic));
    const current = cache.stats();
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
            self.result = self.cache.getOrCompute(self.budget, std.heap.page_allocator, self.key, self.compute, SlowCompute.run) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = QueryEmbeddingCache.init(std.heap.page_allocator, .{});
    defer cache.deinit(&budget);
    var compute_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer compute_io.deinit();
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
    try std.testing.expectEqual(@as(u64, 1), cache.stats().coalesced_waiters);
}

test "query embedding cache coalesces concurrent misses" {
    try testConcurrentCoalescing();
}

pub fn testByteBudgetEviction() !void {
    const one_entry_bytes = QueryEmbeddingCache.entryCharge(2);
    var budget = cache_budget.CacheBudget.init(one_entry_bytes);
    var cache = QueryEmbeddingCache.init(std.testing.allocator, .{ .max_bytes = one_entry_bytes });
    defer cache.deinit(&budget);
    var compute = TestCompute{ .value = 8 };
    const first_key: Key = [_]u8{1} ** 32;
    const second_key: Key = [_]u8{2} ** 32;

    const first = try cache.getOrCompute(&budget, std.testing.allocator, first_key, &compute, TestCompute.run);
    std.testing.allocator.free(first);
    const second = try cache.getOrCompute(&budget, std.testing.allocator, second_key, &compute, TestCompute.run);
    std.testing.allocator.free(second);
    const second_hit = try cache.getOrCompute(&budget, std.testing.allocator, second_key, &compute, TestCompute.run);
    std.testing.allocator.free(second_hit);
    const first_again = try cache.getOrCompute(&budget, std.testing.allocator, first_key, &compute, TestCompute.run);
    std.testing.allocator.free(first_again);

    const current = cache.stats();
    try std.testing.expectEqual(@as(usize, 1), current.entries);
    try std.testing.expectEqual(one_entry_bytes, current.live_bytes);
    try std.testing.expectEqual(@as(u64, 2), current.evictions);
    try std.testing.expectEqual(@as(u64, 3), compute.calls.load(.monotonic));
    try std.testing.expectEqual(one_entry_bytes, budget.stats().used_bytes);
}

test "query embedding cache enforces byte budget with LRU eviction" {
    try testByteBudgetEviction();
}
