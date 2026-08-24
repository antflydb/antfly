// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deterministic provider-boundary histories. These tests execute the real
//! ManagedEmbedder validation/error normalization and PostgreSQL Source SQL
//! construction around controlled adapters. Model/GPU execution and libpq
//! internals remain differential and integration-test concerns.

const std = @import("std");
const vopr = @import("vopr");
const cancellation_mod = @import("../common/cancellation.zig");
const request_admission = @import("../common/request_admission.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const foreign_source = @import("../foreign/source.zig");
const postgres_source = @import("../foreign/postgres_source.zig");
const sql = @import("../foreign/sql.zig");

pub const Scenario = struct {
    pub const name: []const u8 = "provider-boundaries";
    pub const version: u32 = 1;

    const result_id = vopr.id.stable(name, "outcome-classified");
    const validation_id = vopr.id.stable(name, "invalid-results-rejected");
    const cancellation_id = vopr.id.stable(name, "cancellation-propagated");
    const timeout_id = vopr.id.stable(name, "timeout-propagated");
    const retry_id = vopr.id.stable(name, "retry-recovers");
    const admission_id = vopr.id.stable(name, "admission-released");
    const sql_id = vopr.id.stable(name, "postgres-sql-constructed");
    const complete_id = vopr.id.stable(name, "mode-completes");

    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = result_id, .name = name ++ ".outcome-classified", .kind = .always },
        .{ .id = validation_id, .name = name ++ ".invalid-results-rejected", .kind = .always },
        .{ .id = cancellation_id, .name = name ++ ".cancellation-propagated", .kind = .always },
        .{ .id = timeout_id, .name = name ++ ".timeout-propagated", .kind = .always },
        .{ .id = retry_id, .name = name ++ ".retry-recovers", .kind = .always },
        .{ .id = admission_id, .name = name ++ ".admission-released", .kind = .always },
        .{ .id = sql_id, .name = name ++ ".postgres-sql-constructed", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
    };

    const Mode = enum {
        inference_success,
        inference_partial,
        inference_malformed,
        inference_timeout,
        inference_cancel,
        inference_transient,
        inference_retry,
        postgres_success,
        postgres_partial,
        postgres_malformed,
        postgres_timeout,
        postgres_cancel,
        postgres_transient,
        postgres_retry,
    };

    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "inference-success"),
        vopr.id.stable(name, "inference-partial"),
        vopr.id.stable(name, "inference-malformed"),
        vopr.id.stable(name, "inference-timeout"),
        vopr.id.stable(name, "inference-cancel"),
        vopr.id.stable(name, "inference-transient"),
        vopr.id.stable(name, "inference-retry"),
        vopr.id.stable(name, "postgres-success"),
        vopr.id.stable(name, "postgres-partial"),
        vopr.id.stable(name, "postgres-malformed"),
        vopr.id.stable(name, "postgres-timeout"),
        vopr.id.stable(name, "postgres-cancel"),
        vopr.id.stable(name, "postgres-transient"),
        vopr.id.stable(name, "postgres-retry"),
    };

    const mode_names = [_][]const u8{
        name ++ ".inference-success",
        name ++ ".inference-partial",
        name ++ ".inference-malformed",
        name ++ ".inference-timeout",
        name ++ ".inference-cancel",
        name ++ ".inference-transient",
        name ++ ".inference-retry",
        name ++ ".postgres-success",
        name ++ ".postgres-partial",
        name ++ ".postgres-malformed",
        name ++ ".postgres-timeout",
        name ++ ".postgres-cancel",
        name ++ ".postgres-transient",
        name ++ ".postgres-retry",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        inference_admission: request_admission.RequestAdmission,
        postgres_admission: request_admission.RequestAdmission,
        managed: managed_embedder.ManagedEmbedder,
        source: foreign_source.Source,
        mode: Mode = .inference_success,
        complete: bool = false,
        outcome_classified: bool = true,
        invalid_rejected: bool = true,
        cancellation_propagated: bool = true,
        timeout_propagated: bool = true,
        retry_recovered: bool = true,
        sql_constructed: bool = true,
        inference_attempts: u32 = 0,
        postgres_attempts: u32 = 0,
        cancellation_checks: u32 = 0,

        fn cancellationToken(self: *@This()) cancellation_mod.CancellationToken {
            return .{ .ptr = self, .is_cancelled_fn = cancellationCheck };
        }

        fn cancellationCheck(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (self.mode != .inference_cancel and self.mode != .postgres_cancel) return false;
            self.cancellation_checks += 1;
            // Let the production boundary enter the adapter, then cancel at
            // the adapter's first explicit context check.
            return self.cancellation_checks >= if (self.mode == .inference_cancel) 2 else 1;
        }

        fn denseFallback(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.ProviderContextRequired;
        }

        fn sparseFallback(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try alloc.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn denseWithContext(
            raw: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            texts: []const []const u8,
            context: managed_embedder.EmbeddingRequestContext,
        ) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var lease = self.inference_admission.tryAcquireLease() orelse return error.QueueFull;
            defer lease.release();
            self.inference_attempts += 1;
            try context.check();

            switch (self.mode) {
                .inference_timeout => return error.Timeout,
                .inference_transient => return error.ResourceTemporarilyUnavailable,
                .inference_retry => if (self.inference_attempts == 1) return error.QueueFull,
                else => {},
            }

            const count: usize = if (self.mode == .inference_partial) texts.len - 1 else texts.len;
            const vectors = try alloc.alloc([]f32, count);
            errdefer alloc.free(vectors);
            var initialized: usize = 0;
            errdefer for (vectors[0..initialized]) |vector| alloc.free(vector);
            for (vectors) |*vector| {
                vector.* = if (self.mode == .inference_malformed)
                    try alloc.dupe(f32, &.{std.math.nan(f32)})
                else
                    try alloc.dupe(f32, &.{ 0.25, 0.75 });
                initialized += 1;
            }
            return vectors;
        }

        fn query(
            raw: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            prepared: sql.PreparedQuery,
            _: ?u64,
            cancellation: ?cancellation_mod.CancellationToken,
        ) !foreign_source.QueryResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var owned = prepared;
            defer owned.deinit(alloc);
            var lease = self.postgres_admission.tryAcquireLease() orelse return error.ResourceTemporarilyUnavailable;
            defer lease.release();
            self.postgres_attempts += 1;
            self.sql_constructed = std.mem.eql(u8, owned.sql_text, "SELECT * FROM \"docs\" LIMIT 2");
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;

            switch (self.mode) {
                .postgres_partial => return error.IncompleteResponse,
                .postgres_malformed => return error.MalformedData,
                .postgres_timeout => return error.Timeout,
                .postgres_transient => return error.ResourceTemporarilyUnavailable,
                .postgres_retry => if (self.postgres_attempts == 1) return error.ResourceTemporarilyUnavailable,
                else => {},
            }

            const rows = try alloc.alloc(std.json.Value, 1);
            rows[0] = .{ .integer = 7 };
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !foreign_source.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 8 };
        }

        const executor_vtable: postgres_source.QueryExecutor.VTable = .{
            .query = query,
            .statistics = statistics,
        };

        fn runInferenceOnce(self: *@This()) !void {
            const dense = self.managed.denseInterface();
            const batch = dense.embedDenseBatch(
                self.allocator,
                "semantic_idx",
                &.{ "first", "second" },
                2,
            ) catch |err| return err;
            defer db_embedder.freeDenseEmbeddingBatch(self.allocator, batch);
            if (batch.len != 2 or batch[0].len != 2 or batch[1].len != 2)
                return error.InvalidAcceptedEmbedding;
        }

        fn runPostgresOnce(self: *@This()) !void {
            var result = try self.source.query(self.allocator, .{
                .table = @constCast("docs"),
                .limit = 2,
                .cancellation = self.cancellationToken(),
            });
            defer result.deinit(self.allocator);
            if (result.total != 1 or result.rows.len != 1 or result.rows[0] != .integer)
                return error.InvalidAcceptedPostgresResult;
        }

        fn expectInferenceError(self: *@This(), expected: anyerror) !void {
            self.runInferenceOnce() catch |err| {
                if (err != expected) self.outcome_classified = false;
                return;
            };
            self.outcome_classified = false;
        }

        fn expectPostgresError(self: *@This(), expected: anyerror) !void {
            self.runPostgresOnce() catch |err| {
                if (err != expected) self.outcome_classified = false;
                return;
            };
            self.outcome_classified = false;
        }

        fn run(self: *@This()) !void {
            self.cancellation_checks = 0;
            switch (self.mode) {
                .inference_success => try self.runInferenceOnce(),
                .inference_partial => {
                    try self.expectInferenceError(error.InvalidEmbeddingResponse);
                    self.invalid_rejected = self.outcome_classified;
                },
                .inference_malformed => {
                    try self.expectInferenceError(error.InvalidEmbeddingDimensions);
                    self.invalid_rejected = self.outcome_classified;
                },
                .inference_timeout => {
                    try self.expectInferenceError(error.Timeout);
                    self.timeout_propagated = self.outcome_classified;
                },
                .inference_cancel => {
                    try self.expectInferenceError(error.Cancelled);
                    self.cancellation_propagated = self.outcome_classified and self.cancellation_checks >= 2;
                },
                .inference_transient => try self.expectInferenceError(error.EmbedTransientFailure),
                .inference_retry => {
                    try self.expectInferenceError(error.EmbedTransientFailure);
                    try self.runInferenceOnce();
                    self.retry_recovered = self.inference_attempts == 2;
                },
                .postgres_success => try self.runPostgresOnce(),
                .postgres_partial => {
                    try self.expectPostgresError(error.IncompleteResponse);
                    self.invalid_rejected = self.outcome_classified;
                },
                .postgres_malformed => {
                    try self.expectPostgresError(error.MalformedData);
                    self.invalid_rejected = self.outcome_classified;
                },
                .postgres_timeout => {
                    try self.expectPostgresError(error.Timeout);
                    self.timeout_propagated = self.outcome_classified;
                },
                .postgres_cancel => {
                    try self.expectPostgresError(error.Cancelled);
                    self.cancellation_propagated = self.outcome_classified and self.cancellation_checks >= 1;
                },
                .postgres_transient => try self.expectPostgresError(error.ResourceTemporarilyUnavailable),
                .postgres_retry => {
                    try self.expectPostgresError(error.ResourceTemporarilyUnavailable);
                    try self.runPostgresOnce();
                    self.retry_recovered = self.postgres_attempts == 2;
                },
            }
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = undefined;
        state.allocator = allocator;
        state.sim = try vopr.vopr_io.VoprIo.init(.{
            .seed = 0x50524f56,
            .required = .of(&.{.clock_read}),
        });
        errdefer state.sim.deinit();
        state.inference_admission = request_admission.RequestAdmission.init(1);
        state.postgres_admission = request_admission.RequestAdmission.init(1);
        state.mode = .inference_success;
        state.complete = false;
        state.outcome_classified = true;
        state.invalid_rejected = true;
        state.cancellation_propagated = true;
        state.timeout_propagated = true;
        state.retry_recovered = true;
        state.sql_constructed = true;
        state.inference_attempts = 0;
        state.postgres_attempts = 0;
        state.cancellation_checks = 0;
        state.managed = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(allocator,
            \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":2,"embedder":{"provider":"antfly","model":"antflydb/vopr"}}}
        , .{
            .antfly_provider = .{
                .ptr = state,
                .embed_dense_texts = State.denseFallback,
                .embed_dense_texts_with_context = State.denseWithContext,
                .embed_sparse_texts = State.sparseFallback,
            },
            .io = state.sim.io(),
            .cancellation = state.cancellationToken(),
        });
        errdefer state.managed.deinit();
        const runtime = try allocator.create(postgres_source.RuntimeSource);
        errdefer allocator.destroy(runtime);
        runtime.* = .{
            .alloc = allocator,
            .executor = .{ .ptr = state, .vtable = &State.executor_vtable },
            .dsn = try allocator.dupe(u8, "postgres://vopr.invalid/test"),
        };
        state.source = runtime.asSource();
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        const state = world.state;
        state.source.deinit(allocator);
        state.managed.deinit();
        state.sim.deinit();
        allocator.destroy(state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
            .id = id,
            .name = mode_name,
            .kind = switch (mode) {
                .inference_success, .postgres_success => .workload,
                else => .fault,
            },
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            world.state.mode = mode;
            found = true;
        };
        if (!found) return error.InvalidProviderBoundaryTransition;
        try world.state.run();
        try events.emitNamed(allocator, .domain, selected.name, world.state.inference_attempts + world.state.postgres_attempts);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
        try builder.addNamed(allocator, name ++ ".inference-attempts", state.inference_attempts);
        try builder.addNamed(allocator, name ++ ".postgres-attempts", state.postgres_attempts);
        try builder.addNamed(allocator, name ++ ".admission-in-flight", state.inference_admission.stats().in_flight + state.postgres_admission.stats().in_flight);
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        try sink.check(allocator, result_id, state.outcome_classified);
        try sink.check(allocator, validation_id, state.invalid_rejected);
        try sink.check(allocator, cancellation_id, state.cancellation_propagated);
        try sink.check(allocator, timeout_id, state.timeout_propagated);
        try sink.check(allocator, retry_id, state.retry_recovered);
        try sink.check(allocator, admission_id, state.inference_admission.stats().in_flight == 0 and state.postgres_admission.stats().in_flight == 0);
        try sink.check(allocator, sql_id, state.sql_constructed);
        try sink.check(allocator, complete_id, state.complete);
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const admissions_clean = state.inference_admission.stats().in_flight == 0 and
            state.postgres_admission.stats().in_flight == 0;
        return state.sim.healthSnapshot(.{
            .progress_expected = true,
            .progress_units = state.inference_attempts + state.postgres_attempts,
            .recovery_expected = state.mode == .inference_retry or state.mode == .postgres_retry,
            .recovery_complete = state.complete and state.retry_recovered,
            .consistency_valid = state.outcome_classified and state.invalid_rejected and
                state.cancellation_propagated and state.timeout_propagated and state.sql_constructed,
            .cleanup_complete = state.complete and admissions_clean,
        });
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

test "provider boundary VOPR exact replays inference and PostgreSQL adapter outcomes" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var recorded = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
            .source_revision = "provider-boundaries-vopr-v1",
            .target = "native",
            .optimize = @tagName(@import("builtin").mode),
        });
        defer recorded.deinit();
        try std.testing.expectEqual(@as(u64, 0), recorded.summary.property_failures);
        for (0..3) |_| {
            var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &recorded);
            replayed.deinit();
        }
    }
}
