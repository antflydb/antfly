// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Exact-replay schedules for the production query-embedding cache. The cache
//! borrows one VoprIo clock and task kernel; no native test thread participates
//! in coalescing, timeout, cancellation, admission, TTL, LRU, or pin races.

const std = @import("std");
const vopr = @import("vopr");
const cache_budget = @import("../common/cache_budget.zig");
const query_cache = @import("../inference/query_embedding_cache.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "query-embedding-cache";
    pub const version: u32 = 1;

    const sound_id = vopr.id.stable(name, "outcomes-sound");
    const single_producer_id = vopr.id.stable(name, "same-key-single-producer");
    const waiter_cleanup_id = vopr.id.stable(name, "waiter-cleanup");
    const bounded_id = vopr.id.stable(name, "distinct-key-admission-bounded");
    const expiry_id = vopr.id.stable(name, "ttl-expiry-uses-borrowed-clock");
    const eviction_id = vopr.id.stable(name, "lru-eviction-bounded");
    const pinned_id = vopr.id.stable(name, "pinned-copy-survives-eviction");
    const complete_id = vopr.id.stable(name, "mode-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".outcomes-sound", .kind = .always },
        .{ .id = single_producer_id, .name = name ++ ".same-key-single-producer", .kind = .always },
        .{ .id = waiter_cleanup_id, .name = name ++ ".waiter-cleanup", .kind = .always },
        .{ .id = bounded_id, .name = name ++ ".distinct-key-admission-bounded", .kind = .always },
        .{ .id = expiry_id, .name = name ++ ".ttl-expiry-uses-borrowed-clock", .kind = .always },
        .{ .id = eviction_id, .name = name ++ ".lru-eviction-bounded", .kind = .always },
        .{ .id = pinned_id, .name = name ++ ".pinned-copy-survives-eviction", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum { coalesce, timeout, cancellation, overload, ttl, lru, pinned_eviction };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "coalesce"),
        vopr.id.stable(name, "timeout"),
        vopr.id.stable(name, "cancellation"),
        vopr.id.stable(name, "overload"),
        vopr.id.stable(name, "ttl"),
        vopr.id.stable(name, "lru"),
        vopr.id.stable(name, "pinned-eviction"),
    };
    const mode_names = [_][]const u8{
        name ++ ".coalesce",
        name ++ ".timeout",
        name ++ ".cancellation",
        name ++ ".overload",
        name ++ ".ttl",
        name ++ ".lru",
        name ++ ".pinned-eviction",
    };

    const key_a: query_cache.Key = [_]u8{0xa1} ** 32;
    const key_b: query_cache.Key = [_]u8{0xb2} ** 32;

    const State = struct {
        owner_allocator: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        budget: cache_budget.CacheBudget,
        cache: ?query_cache.QueryEmbeddingCache = null,
        mode: ?Mode = null,
        compute_release: std.Io.Event = .unset,
        pin_release: std.Io.Event = .unset,
        compute_blocked: bool = false,
        producer_started: bool = false,
        waiter_entered: bool = false,
        pin_observed: bool = false,
        compute_calls: u64 = 0,
        successful_results: u64 = 0,
        sound: bool = true,
        single_producer: bool = true,
        waiter_clean: bool = true,
        bounded: bool = true,
        expiry_sound: bool = true,
        eviction_sound: bool = true,
        pinned_sound: bool = true,
        complete: bool = false,

        fn init(allocator: std.mem.Allocator) !*State {
            const self = try allocator.create(State);
            errdefer allocator.destroy(self);
            self.owner_allocator = allocator;
            self.fixture_allocator = .init;
            errdefer _ = self.fixture_allocator.deinit();
            const fixture_allocator = self.fixture_allocator.allocator();
            self.* = .{
                .owner_allocator = allocator,
                .fixture_allocator = self.fixture_allocator,
                .allocator = fixture_allocator,
                .sim = try vopr.vopr_io.VoprIo.init(.{
                    .seed = 0x5145_4348,
                    .required = .of(&.{ .clock_read, .task_scheduling, .synchronization, .sleep }),
                }),
                .budget = cache_budget.CacheBudget.init(0),
            };
            return self;
        }

        fn deinit(self: *State) void {
            if (self.cache) |*cache| cache.deinit(&self.budget);
            self.sim.deinit();
            const owner_allocator = self.owner_allocator;
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            owner_allocator.destroy(self);
        }

        fn start(self: *State, mode: Mode) void {
            self.mode = mode;
            const charge = query_cache.QueryEmbeddingCache.entryChargeBytes(3);
            const config: query_cache.Config = switch (mode) {
                .coalesce, .timeout, .cancellation => .{ .max_bytes = 0, .max_inflight = 2 },
                .overload => .{ .max_bytes = 0, .max_inflight = 1 },
                .ttl => .{ .max_bytes = charge * 2, .ttl_ns = 5, .max_inflight = 1 },
                .lru, .pinned_eviction => .{ .max_bytes = charge, .ttl_ns = 1_000, .max_inflight = 1 },
            };
            self.budget = cache_budget.CacheBudget.init(config.max_bytes);
            self.cache = query_cache.QueryEmbeddingCache.init(self.allocator, self.sim.io(), config);
            if (mode == .pinned_eviction) self.cache.?.setLifecycleHook(.{
                .ptr = self,
                .after_pin = afterPin,
            });
            _ = self.sim.io().async(runRoot, .{self});
        }

        fn nowNs(self: *State) u64 {
            return @intCast(@max(std.Io.Timestamp.now(self.sim.io(), .awake).toNanoseconds(), 0));
        }

        fn compute(raw: *anyopaque, allocator: std.mem.Allocator) ![]f32 {
            const self: *State = @ptrCast(@alignCast(raw));
            self.compute_calls +|= 1;
            self.producer_started = true;
            if (self.compute_blocked) self.compute_release.waitUncancelable(self.sim.io());
            return try allocator.dupe(f32, &.{ 1, 2, 3 });
        }

        fn afterPin(raw: *anyopaque, _: query_cache.Key) void {
            const self: *State = @ptrCast(@alignCast(raw));
            self.pin_observed = true;
            self.pin_release.waitUncancelable(self.sim.io());
        }

        fn fetch(self: *State, key: query_cache.Key, deadline_ns: ?u64) !void {
            const result = try self.cache.?.getOrCompute(
                &self.budget,
                self.allocator,
                key,
                deadline_ns,
                self,
                compute,
            );
            defer self.allocator.free(result);
            if (!std.mem.eql(f32, result, &.{ 1, 2, 3 })) return error.InvalidEmbeddingCacheResult;
            self.successful_results +|= 1;
        }

        fn producerTask(self: *State) void {
            self.fetch(key_a, null) catch {
                self.sound = false;
            };
        }

        fn waiterTask(self: *State) void {
            self.waiter_entered = true;
            self.fetch(key_a, null) catch {
                self.sound = false;
            };
        }

        fn cancellableWaiter(self: *State) std.Io.Cancelable!void {
            self.waiter_entered = true;
            self.fetch(key_a, null) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    self.sound = false;
                    return;
                },
            };
        }

        fn hitTask(self: *State) void {
            self.fetch(key_a, null) catch {
                self.pinned_sound = false;
            };
        }

        fn waitFor(self: *State, value: *const bool) !void {
            while (!value.*) try self.sim.io().sleep(.fromNanoseconds(1), .awake);
        }

        fn runRoot(self: *State) void {
            self.runMode() catch {
                self.sound = false;
            };
            const stats = self.cache.?.stats(&self.budget);
            self.waiter_clean = stats.inflight == 0;
            self.complete = true;
        }

        fn runMode(self: *State) !void {
            switch (self.mode.?) {
                .coalesce => try self.runCoalesce(),
                .timeout => try self.runTimeout(),
                .cancellation => try self.runCancellation(),
                .overload => try self.runOverload(),
                .ttl => try self.runTtl(),
                .lru => try self.runLru(),
                .pinned_eviction => try self.runPinnedEviction(),
            }
        }

        fn runCoalesce(self: *State) !void {
            self.compute_blocked = true;
            var producer = self.sim.io().async(producerTask, .{self});
            try self.waitFor(&self.producer_started);
            var waiter = self.sim.io().async(waiterTask, .{self});
            try self.waitFor(&self.waiter_entered);
            self.compute_release.set(self.sim.io());
            producer.await(self.sim.io());
            waiter.await(self.sim.io());
            const stats = self.cache.?.stats(&self.budget);
            self.single_producer = self.compute_calls == 1 and stats.coalesced_waiters == 1 and self.successful_results == 2;
        }

        fn runTimeout(self: *State) !void {
            self.compute_blocked = true;
            var producer = self.sim.io().async(producerTask, .{self});
            try self.waitFor(&self.producer_started);
            self.fetch(key_a, self.nowNs() +| 5) catch |err| {
                if (err != error.Timeout) self.sound = false;
            };
            self.compute_release.set(self.sim.io());
            producer.await(self.sim.io());
            const stats = self.cache.?.stats(&self.budget);
            self.waiter_clean = stats.waiter_timeouts == 1 and stats.inflight == 0;
        }

        fn runCancellation(self: *State) !void {
            self.compute_blocked = true;
            var producer = self.sim.io().async(producerTask, .{self});
            try self.waitFor(&self.producer_started);
            var waiter = self.sim.io().async(cancellableWaiter, .{self});
            try self.waitFor(&self.waiter_entered);
            _ = waiter.cancel(self.sim.io()) catch |err| switch (err) {
                error.Canceled => {},
            };
            self.compute_release.set(self.sim.io());
            producer.await(self.sim.io());
            const stats = self.cache.?.stats(&self.budget);
            self.waiter_clean = stats.coalesced_waiters == 1 and stats.inflight == 0;
        }

        fn runOverload(self: *State) !void {
            self.compute_blocked = true;
            var producer = self.sim.io().async(producerTask, .{self});
            try self.waitFor(&self.producer_started);
            self.fetch(key_b, null) catch |err| {
                if (err != error.QueryEmbeddingOverloaded) self.sound = false;
            };
            self.compute_release.set(self.sim.io());
            producer.await(self.sim.io());
            const stats = self.cache.?.stats(&self.budget);
            self.bounded = stats.inflight_rejections == 1 and stats.max_inflight == 1 and stats.inflight == 0;
        }

        fn runTtl(self: *State) !void {
            try self.fetch(key_a, null);
            try self.sim.io().sleep(.fromNanoseconds(10), .awake);
            try self.fetch(key_a, null);
            const stats = self.cache.?.stats(&self.budget);
            self.expiry_sound = self.compute_calls == 2 and stats.expirations == 1 and stats.entries == 1;
        }

        fn runLru(self: *State) !void {
            try self.fetch(key_a, null);
            try self.fetch(key_b, null);
            try self.fetch(key_a, null);
            const stats = self.cache.?.stats(&self.budget);
            self.eviction_sound = self.compute_calls == 3 and stats.evictions == 2 and stats.entries == 1 and
                stats.live_bytes <= query_cache.QueryEmbeddingCache.entryChargeBytes(3);
        }

        fn runPinnedEviction(self: *State) !void {
            try self.fetch(key_a, null);
            var hit = self.sim.io().async(hitTask, .{self});
            try self.waitFor(&self.pin_observed);
            try self.fetch(key_b, null);
            self.pin_release.set(self.sim.io());
            hit.await(self.sim.io());
            const stats = self.cache.?.stats(&self.budget);
            self.pinned_sound = self.pinned_sound and self.successful_results == 3 and
                stats.entries == 0 and stats.live_bytes == 0 and self.budget.stats().used_bytes == 0;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        return .{ .state = try State.init(allocator) };
    }

    pub fn deinit(world: *World, _: std.mem.Allocator) void {
        world.state.deinit();
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        const state = world.state;
        if (state.mode == null) {
            inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
                .id = id,
                .name = mode_name,
                .kind = switch (mode) {
                    .coalesce, .ttl, .lru => .workload,
                    .timeout, .cancellation, .overload, .pinned_eviction => .fault,
                },
            });
            return;
        }
        if (!state.sim.scheduler().quiescent()) try state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (state.mode == null) {
            var found = false;
            inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
                state.start(mode);
                found = true;
            };
            if (!found) return error.InvalidQueryEmbeddingCacheMode;
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        try events.emitNamed(allocator, .domain, selected.name, state.compute_calls);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const stats = if (state.cache) |*cache| cache.stats(&state.budget) else query_cache.Stats{};
        try builder.addNamed(allocator, name ++ ".mode", if (state.mode) |mode| @as(i64, @intFromEnum(mode)) + 1 else 0);
        try builder.addNamed(allocator, name ++ ".compute-calls", @intCast(state.compute_calls));
        try builder.addNamed(allocator, name ++ ".results", @intCast(state.successful_results));
        try builder.addNamed(allocator, name ++ ".inflight", @intCast(stats.inflight));
        try builder.addNamed(allocator, name ++ ".entries", @intCast(stats.entries));
        try builder.addNamed(allocator, name ++ ".budget-used", @intCast(state.budget.stats().used_bytes));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, sound_id, state.sound);
        try sink.check(allocator, single_producer_id, !state.complete or state.single_producer);
        try sink.check(allocator, waiter_cleanup_id, !state.complete or state.waiter_clean);
        try sink.check(allocator, bounded_id, !state.complete or state.bounded);
        try sink.check(allocator, expiry_id, !state.complete or state.expiry_sound);
        try sink.check(allocator, eviction_id, !state.complete or state.eviction_sound);
        try sink.check(allocator, pinned_id, !state.complete or state.pinned_sound);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const stats = if (state.cache) |*cache| cache.stats(&state.budget) else query_cache.Stats{};
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = state.compute_calls + state.successful_results,
            .recovery_expected = state.mode == .timeout or state.mode == .cancellation or state.mode == .overload,
            .recovery_complete = state.complete and stats.inflight == 0,
            .consistency_valid = state.sound and state.single_producer and state.waiter_clean and
                state.bounded and state.expiry_sound and state.eviction_sound and state.pinned_sound,
            .cleanup_complete = state.complete and stats.inflight == 0,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "query embedding cache VOPR exact replays coalescing cancellation admission TTL LRU and pin races" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, mode_ordinal| {
        for (0..4) |schedule_ordinal| {
            var choices = vopr.choice.PrefixedSeeded.init(
                &.{mode_id},
                0x5145_4348 + mode_ordinal * 16 + schedule_ordinal,
            );
            var recorded = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
                .system = "antfly",
                .transition_budget = 512,
                .backend_ids = &backend_ids,
                .source_revision = "query-embedding-cache-vopr-v1",
                .target = "native",
                .optimize = @tagName(@import("builtin").mode),
            });
            defer recorded.deinit();
            try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
