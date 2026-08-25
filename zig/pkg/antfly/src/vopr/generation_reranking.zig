// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Generation-chain and reranking boundary histories on one borrowed VoprIo.
//! Retry backoff uses the production antfly-independent chain executor; local
//! reranking goes through the production runtime validation boundary.

const std = @import("std");
const httpx = @import("httpx");
const generating = @import("antfly_generating");
const vopr = @import("vopr");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const reranking = @import("../reranking/mod.zig");

const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "generation-reranking-chain";
    pub const version: u32 = 1;

    const generation_id = vopr.id.stable(name, "generation-chain-sound");
    const retry_id = vopr.id.stable(name, "retry-uses-borrowed-io");
    const reranking_id = vopr.id.stable(name, "reranking-response-validated");
    const error_id = vopr.id.stable(name, "provider-errors-propagated");
    const cleanup_id = vopr.id.stable(name, "resources-cleaned");
    const complete_id = vopr.id.stable(name, "history-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = generation_id, .name = name ++ ".generation-chain-sound", .kind = .always },
        .{ .id = retry_id, .name = name ++ ".retry-uses-borrowed-io", .kind = .always },
        .{ .id = reranking_id, .name = name ++ ".reranking-response-validated", .kind = .always },
        .{ .id = error_id, .name = name ++ ".provider-errors-propagated", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".resources-cleaned", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".history-completes", .kind = .reachable },
    };

    const Mode = enum {
        generation_success,
        generation_retry,
        generation_timeout_fallback,
        generation_rate_limit_fallback,
        generation_cancel,
        reranking_success,
        reranking_malformed_count,
        reranking_malformed_nan,
        reranking_timeout,
        reranking_cancel,
    };

    const mode_ids = ids: {
        var values: [@typeInfo(Mode).@"enum".fields.len]vopr.id.StableId = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = vopr.id.stable(name, @tagName(mode));
        break :ids values;
    };
    const mode_names = names: {
        var values: [mode_ids.len][]const u8 = undefined;
        for (std.meta.tags(Mode), 0..) |mode, index|
            values[index] = name ++ "." ++ @tagName(mode);
        break :names values;
    };

    const State = struct {
        owner_allocator: std.mem.Allocator,
        fixture_allocator: FixtureAllocator,
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        http: httpx.Client,
        mode: ?Mode = null,
        attempts: u32 = 0,
        fallback_used: bool = false,
        retry_wait_observed: bool = false,
        generation_sound: bool = true,
        reranking_sound: bool = true,
        error_sound: bool = true,
        complete: bool = false,
        task_error: ?anyerror = null,

        fn init(owner_allocator: std.mem.Allocator) !*State {
            const self = try owner_allocator.create(State);
            errdefer owner_allocator.destroy(self);
            self.* = .{
                .owner_allocator = owner_allocator,
                .fixture_allocator = .init,
                .allocator = undefined,
                .sim = undefined,
                .http = undefined,
            };
            errdefer _ = self.fixture_allocator.deinit();
            self.allocator = self.fixture_allocator.allocator();
            self.sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0x4745_4e52,
                .instrumentation = .{ .enabled = false, .map_digest = 0x4745_4e52 },
            });
            errdefer self.sim.deinit();
            self.http = httpx.Client.initWithConfig(self.allocator, self.sim.io(), .{ .keep_alive = false });
            return self;
        }

        fn deinit(self: *State) void {
            self.http.deinit();
            self.sim.deinit();
            const owner_allocator = self.owner_allocator;
            std.debug.assert(self.fixture_allocator.deinit() == .ok);
            owner_allocator.destroy(self);
        }

        fn factory(self: *State) generating.GeneratorFactory {
            return .{ .ptr = self, .vtable = &.{ .create = createGenerator } };
        }

        fn createGenerator(ptr: *anyopaque, _: std.mem.Allocator, _: generating.GeneratorConfig) !generating.Generator {
            return .{ .ptr = ptr, .vtable = &.{ .generate = generate } };
        }

        fn generate(ptr: *anyopaque, alloc: std.mem.Allocator, model: []const u8, _: []const generating.ChatMessage) !generating.GenerateResult {
            const self: *State = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            const primary = std.mem.eql(u8, model, "primary");
            switch (self.mode.?) {
                .generation_retry => if (self.attempts == 1) return error.Timeout,
                .generation_timeout_fallback => if (primary) return error.Timeout,
                .generation_rate_limit_fallback => if (primary) return error.RateLimit,
                .generation_cancel => return error.Canceled,
                else => {},
            }
            if (!primary) self.fallback_used = true;
            return .{
                .content = try alloc.dupe(u8, if (primary) "primary-result" else "fallback-result"),
                .allocator = alloc,
            };
        }

        fn dense(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return alloc.alloc([]f32, 0);
        }

        fn sparse(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return alloc.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn rerank(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, _: []const []const u8) ![]f32 {
            const self: *State = @ptrCast(@alignCast(ptr));
            return switch (self.mode.?) {
                .reranking_malformed_count => try alloc.dupe(f32, &.{0.5}),
                .reranking_malformed_nan => try alloc.dupe(f32, &.{ std.math.nan(f32), 0.5 }),
                .reranking_timeout => error.Timeout,
                .reranking_cancel => error.Canceled,
                else => try alloc.dupe(f32, &.{ 0.2, 0.8 }),
            };
        }

        fn runGeneration(self: *State) !void {
            const primary = generating.ChainLink{
                .generator = generating.GeneratorConfig.fromOpenAI(.{ .model = "primary" }),
                .condition = switch (self.mode.?) {
                    .generation_timeout_fallback => .on_timeout,
                    .generation_rate_limit_fallback => .on_rate_limit,
                    else => .on_error,
                },
                .retry = if (self.mode.? == .generation_retry)
                    .{ .max_attempts = 2, .initial_backoff_ms = 1, .max_backoff_ms = 1 }
                else
                    null,
            };
            const fallback = generating.ChainLink{
                .generator = generating.GeneratorConfig.fromAntfly(.{ .model = "fallback" }),
            };
            const chain = if (self.mode.? == .generation_timeout_fallback or self.mode.? == .generation_rate_limit_fallback)
                &[_]generating.ChainLink{ primary, fallback }
            else
                &[_]generating.ChainLink{primary};
            var result = generating.executeChainWithIo(
                self.allocator,
                self.sim.io(),
                chain,
                self.factory(),
                &.{.{ .role = .user, .content = .{ .text = "query" } }},
            ) catch |err| {
                self.error_sound = self.mode.? == .generation_cancel and err == error.Canceled;
                if (!self.error_sound) return err;
                return;
            };
            defer result.deinit();
            self.retry_wait_observed = self.mode.? != .generation_retry or
                std.Io.Clock.awake.now(self.sim.io()).toNanoseconds() >= std.time.ns_per_ms;
            self.generation_sound = if (self.mode.? == .generation_timeout_fallback or self.mode.? == .generation_rate_limit_fallback)
                self.fallback_used and std.mem.eql(u8, result.content, "fallback-result")
            else
                !self.fallback_used and std.mem.eql(u8, result.content, "primary-result") and
                    (self.mode.? != .generation_retry or self.attempts == 2);
        }

        fn runReranking(self: *State) !void {
            const local = managed_embedder.AntflyProvider{
                .ptr = self,
                .embed_dense_texts = dense,
                .embed_sparse_texts = sparse,
                .rerank_texts = rerank,
            };
            const scores = reranking.rerankDocumentsWithAntflyProvider(
                self.allocator,
                &self.http,
                .{ .provider = .antfly, .field = "body" },
                local,
                "query",
                &.{ "doc-a", "doc-b" },
            ) catch |err| {
                self.reranking_sound = switch (self.mode.?) {
                    .reranking_malformed_count, .reranking_malformed_nan => err == error.InvalidRerankerResponse,
                    .reranking_timeout => err == error.Timeout,
                    .reranking_cancel => err == error.Canceled,
                    else => false,
                };
                self.error_sound = self.reranking_sound;
                return;
            };
            defer self.allocator.free(scores);
            self.reranking_sound = self.mode.? == .reranking_success and scores.len == 2 and scores[1] > scores[0];
        }

        fn runTask(self: *State) void {
            const mode = self.mode.?;
            const result = switch (mode) {
                .generation_success,
                .generation_retry,
                .generation_timeout_fallback,
                .generation_rate_limit_fallback,
                .generation_cancel,
                => self.runGeneration(),
                else => self.runReranking(),
            };
            result catch |err| {
                self.task_error = err;
            };
            self.complete = true;
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
                    .generation_success, .generation_retry, .reranking_success => .workload,
                    else => .fault,
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
                state.mode = mode;
                _ = state.sim.io().async(State.runTask, .{state});
                found = true;
            };
            if (!found) return error.InvalidGenerationRerankingMode;
        } else {
            try state.sim.scheduler().executeReady(selected.id, events, allocator);
        }
        try events.emitNamed(allocator, .domain, selected.name, state.attempts);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".attempts", @intCast(state.attempts));
        try builder.addNamed(allocator, name ++ ".fallback-used", @intFromBool(state.fallback_used));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, generation_id, state.generation_sound);
        try sink.check(allocator, retry_id, state.mode == null or state.mode.? != .generation_retry or
            !state.complete or state.retry_wait_observed);
        try sink.check(allocator, reranking_id, state.reranking_sound);
        try sink.check(allocator, error_id, state.error_sound and state.task_error == null);
        try sink.check(allocator, cleanup_id, !state.complete or state.sim.resourceSnapshot().active_tasks == 0);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        return state.sim.healthSnapshot(.{
            .progress_expected = state.mode != null,
            .progress_units = @intFromBool(state.complete),
            .consistency_valid = state.generation_sound and state.reranking_sound and state.error_sound and state.task_error == null,
            .cleanup_complete = state.complete and state.sim.resourceSnapshot().active_tasks == 0,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "generation and reranking chain VOPR exact replays fallback retry malformed timeout and cancellation" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids, 0..) |mode_id, ordinal| {
        errdefer std.debug.print("generation/reranking VOPR failed mode={s}\n", .{Scenario.mode_names[ordinal]});
        var choices = vopr.choice.PrefixedFairSeeded.init(&.{mode_id}, 0x4745_4e52 + ordinal);
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, choices.source(), .{
            .system = "antfly",
            .transition_budget = 128,
            .backend_ids = &backend_ids,
            .source_revision = "generation-reranking-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
        replayed.deinit();
    }
}
