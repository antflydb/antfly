// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deterministic DB/index request-race compositions. Operations are split only
//! at production-safe boundaries: complete public operations or nonblocking
//! microsteps of the lock-free dense catalog admission protocol. No native
//! test thread or suspension while holding an apply/structural mutex is
//! reproduced here.

const std = @import("std");
const builtin = @import("builtin");
const vopr = @import("vopr");
const db_mod = @import("../storage/db/db.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const text_merge_runtime = @import("../storage/db/maintenance/text_merge_runtime.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const VoprTestAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

const Fixture = struct {
    allocator: std.mem.Allocator,
    tmp: std.testing.TmpDir,
    root: [:0]u8,
    // BackendRuntime retains the `std.Io.userdata` pointer, so VoprIo must not
    // move after lending that interface. Heap ownership gives the fixture a
    // stable address across construction and runner-world moves.
    sim: *vopr.vopr_io.VoprIo,
    backend: background_runtime.BackendRuntimeHandle,
    repair_storage: *lsm_backend.MemoryStorage,
    db: db_mod.DB,

    fn init(allocator: std.mem.Allocator) !Fixture {
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
        errdefer allocator.free(root);
        const sim = try allocator.create(vopr.vopr_io.VoprIo);
        errdefer allocator.destroy(sim);
        sim.* = try vopr.vopr_io.VoprIo.init(.{
            .seed = 0x4442_494e_4445_5852,
            .required = .of(&.{ .clock_read, .task_scheduling, .synchronization, .sleep }),
            // DB admission crosses the production index manager and the debug
            // allocator's stack-capture path. Give those fibers the same
            // headroom as the production-shaped DataServer campaign instead
            // of relying on VoprIo's deliberately small generic default.
            .tasks = .{ .stack_size = 8 * 1024 * 1024 },
        });
        errdefer sim.deinit();
        var backend = try background_runtime.BackendRuntimeHandle.init(allocator, .{
            .backend = .manual,
            .borrowed_io = .{ .general = sim.io() },
        });
        errdefer backend.deinit();
        const repair_storage = try allocator.create(lsm_backend.MemoryStorage);
        errdefer allocator.destroy(repair_storage);
        repair_storage.* = lsm_backend.MemoryStorage.init(allocator);
        errdefer repair_storage.deinit();
        var db = try db_mod.DB.open(allocator, root, .{
            .backend_runtime = backend.ptr(),
            .executor = .{ .backend = .manual },
            .primary_backend = .{ .mem = .{} },
            .physical_root_mode = .external_backend,
            .index_repair_checkpoint_storage = repair_storage.storage(),
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .start_optional_runtime_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        errdefer db.close();
        // The campaign controls request scheduling, catalog atomics, and
        // admission waits. Physical index bytes remain the real-backend
        // differential boundary and therefore use the host test I/O.
        db.core.index_manager.setIo(std.testing.io);
        return .{
            .allocator = allocator,
            .tmp = tmp,
            .root = root,
            .sim = sim,
            .backend = backend,
            .repair_storage = repair_storage,
            .db = db,
        };
    }

    fn deinit(self: *Fixture) void {
        self.db.close();
        self.repair_storage.deinit();
        self.allocator.destroy(self.repair_storage);
        self.backend.deinit();
        self.sim.deinit();
        self.allocator.destroy(self.sim);
        self.allocator.free(self.root);
        self.tmp.cleanup();
        self.* = undefined;
    }
};

const managed_name = "managed_ft";
const materialize_id = vopr.id.stable("transition", "db-index.materialize-managed-admission");
const delete_managed_id = vopr.id.stable("transition", "db-index.delete-managed-index");
const managed_linearizable_id = vopr.id.stable("property", "db-index.managed-delete-materialize-linearizable");
const managed_cleanup_id = vopr.id.stable("property", "db-index.managed-delete-cleans-repair");
const managed_complete_id = vopr.id.stable("property", "db-index.managed-race-completes");

pub const ManagedAdmissionScenario = struct {
    pub const name: []const u8 = "db-index-managed-admission-race";
    pub const version: u32 = 2;
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = managed_linearizable_id, .name = name ++ ".linearizable", .kind = .always },
        .{ .id = managed_cleanup_id, .name = name ++ ".cleanup", .kind = .always },
        .{ .id = managed_complete_id, .name = name ++ ".complete", .kind = .reachable },
    };

    pub const World = struct {
        fixture: *Fixture,
        materialized: bool = false,
        materialize_saw_marker: bool = false,
        materialize_expected_marker: bool = false,
        deleted: bool = false,
        actions: u8 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !World {
        const fixture = try allocator.create(Fixture);
        errdefer allocator.destroy(fixture);
        fixture.* = try Fixture.init(allocator);
        errdefer fixture.deinit();
        try fixture.db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try fixture.db.prepareManagedIndexAdmissionForVopr(.{
            .name = managed_name,
            .kind = .full_text,
            .config_json = "{}",
        });
        return .{ .fixture = fixture };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.fixture.deinit();
        allocator.destroy(world.fixture);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (!world.materialized) try list.append(allocator, .{
            .id = materialize_id,
            .name = name ++ ".materialize",
            .kind = .maintenance,
        });
        if (!world.deleted) try list.append(allocator, .{
            .id = delete_managed_id,
            .name = name ++ ".delete",
            .kind = .workload,
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        if (selected.id == materialize_id) {
            world.materialize_expected_marker = !world.deleted;
            world.materialize_saw_marker = (try world.fixture.db.materializeManagedIndexAdmission(
                allocator,
                managed_name,
            )) != null;
            world.materialized = true;
        } else if (selected.id == delete_managed_id) {
            world.deleted = try world.fixture.db.deleteIndex(managed_name);
        } else return error.InvalidDbIndexManagedTransition;
        world.actions += 1;
        try events.emitNamed(allocator, .state_change, selected.name, world.actions);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".actions", world.actions);
        try builder.addNamed(allocator, name ++ ".materialized", @intFromBool(world.materialized));
        try builder.addNamed(allocator, name ++ ".materialize-saw-marker", @intFromBool(world.materialize_saw_marker));
        try builder.addNamed(allocator, name ++ ".materialize-expected-marker", @intFromBool(world.materialize_expected_marker));
        try builder.addNamed(allocator, name ++ ".deleted", @intFromBool(world.deleted));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const pending = try world.fixture.db.hasPendingIndexRepairIntents(allocator);
        const complete = world.materialized and world.deleted;
        // Materialization observes the marker iff deletion has not already
        // committed catalog absence. Both orders converge to the same cleanup.
        try sink.check(
            allocator,
            managed_linearizable_id,
            !world.materialized or world.materialize_saw_marker == world.materialize_expected_marker,
        );
        try sink.check(allocator, managed_cleanup_id, !complete or !pending);
        try sink.check(allocator, managed_complete_id, complete);
    }

    pub fn done(world: *World) bool {
        return world.materialized and world.deleted;
    }
};

const capture_begin_id = vopr.id.stable("transition", "db-index.dense-capture-begin");
const capture_end_id = vopr.id.stable("transition", "db-index.dense-capture-end");
const barrier_begin_id = vopr.id.stable("transition", "db-index.catalog-barrier-begin");
const barrier_end_id = vopr.id.stable("transition", "db-index.catalog-barrier-end");
const delete_dense_id = vopr.id.stable("transition", "db-index.delete-dense-index");
const capture_fenced_id = vopr.id.stable("property", "db-index.catalog-closure-fences-new-capture");
const capture_drain_id = vopr.id.stable("property", "db-index.catalog-writer-waits-for-capture");
const capture_complete_id = vopr.id.stable("property", "db-index.capture-delete-completes");

pub const PublishedCaptureScenario = struct {
    pub const name: []const u8 = "db-index-published-capture-race";
    pub const version: u32 = 1;
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = capture_fenced_id, .name = name ++ ".closure-fences-new-capture", .kind = .always },
        .{ .id = capture_drain_id, .name = name ++ ".writer-waits-for-capture", .kind = .always },
        .{ .id = capture_complete_id, .name = name ++ ".complete", .kind = .reachable },
    };

    pub const World = struct {
        fixture: *Fixture,
        capture_attempted: bool = false,
        capture_held: bool = false,
        capture_rejected_after_closure: bool = false,
        barrier_started: bool = false,
        barrier_observed_reader: bool = false,
        barrier_ended: bool = false,
        deleted: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator) !World {
        const fixture = try allocator.create(Fixture);
        errdefer allocator.destroy(fixture);
        fixture.* = try Fixture.init(allocator);
        errdefer fixture.deinit();
        try fixture.db.addIndex(.{
            .name = "dense",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
        });
        return .{ .fixture = fixture };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        if (world.capture_held) world.fixture.db.endPublishedDenseCaptureForVopr();
        if (world.barrier_started and !world.barrier_ended and
            world.fixture.db.indexCatalogBarrierDrainedForVopr())
        {
            world.fixture.db.endIndexCatalogBarrierForVopr();
        }
        world.fixture.deinit();
        allocator.destroy(world.fixture);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (!world.capture_attempted) try list.append(allocator, .{
            .id = capture_begin_id,
            .name = name ++ ".capture-begin",
            .kind = .workload,
        });
        if (!world.barrier_started) try list.append(allocator, .{
            .id = barrier_begin_id,
            .name = name ++ ".barrier-begin",
            .kind = .maintenance,
        });
        if (world.capture_held and world.barrier_started) try list.append(allocator, .{
            .id = capture_end_id,
            .name = name ++ ".capture-end",
            .kind = .workload,
        });
        if (world.barrier_started and !world.barrier_ended and !world.capture_held and
            world.fixture.db.indexCatalogBarrierDrainedForVopr())
        {
            try list.append(allocator, .{
                .id = barrier_end_id,
                .name = name ++ ".barrier-end",
                .kind = .maintenance,
            });
        }
        if (world.barrier_ended and !world.deleted) try list.append(allocator, .{
            .id = delete_dense_id,
            .name = name ++ ".delete",
            .kind = .workload,
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        if (selected.id == capture_begin_id) {
            world.capture_attempted = true;
            world.capture_held = world.fixture.db.beginPublishedDenseCaptureForVopr("dense");
            if (world.barrier_started) world.capture_rejected_after_closure = !world.capture_held;
        } else if (selected.id == barrier_begin_id) {
            if (!world.fixture.db.beginIndexCatalogBarrierForVopr()) return error.CatalogBarrierAlreadyActive;
            world.barrier_started = true;
            world.barrier_observed_reader = !world.fixture.db.indexCatalogBarrierDrainedForVopr();
        } else if (selected.id == capture_end_id) {
            world.fixture.db.endPublishedDenseCaptureForVopr();
            world.capture_held = false;
        } else if (selected.id == barrier_end_id) {
            if (!world.fixture.db.indexCatalogBarrierDrainedForVopr()) return error.CatalogBarrierNotDrained;
            world.fixture.db.endIndexCatalogBarrierForVopr();
            world.barrier_ended = true;
        } else if (selected.id == delete_dense_id) {
            world.deleted = try world.fixture.db.deleteIndex("dense");
        } else return error.InvalidDbIndexCaptureTransition;
        try events.emitNamed(allocator, .state_change, selected.name, @intFromBool(world.deleted));
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".capture-held", @intFromBool(world.capture_held));
        try builder.addNamed(allocator, name ++ ".barrier-started", @intFromBool(world.barrier_started));
        try builder.addNamed(allocator, name ++ ".deleted", @intFromBool(world.deleted));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, capture_fenced_id, !world.barrier_started or !world.capture_attempted or
            world.barrier_observed_reader or world.capture_rejected_after_closure);
        try sink.check(allocator, capture_drain_id, !world.barrier_observed_reader or !world.barrier_ended or !world.capture_held);
        try sink.check(allocator, capture_complete_id, world.deleted);
    }

    pub fn done(world: *World) bool {
        return world.deleted;
    }
};

const admission_cross_index_id = vopr.id.stable("transition", "db-index.admission-cross-index");
const admission_fifo_id = vopr.id.stable("transition", "db-index.admission-fifo");
const admission_cancel_id = vopr.id.stable("transition", "db-index.admission-cancel");
const admission_shutdown_id = vopr.id.stable("transition", "db-index.admission-shutdown");
const admission_isolation_id = vopr.id.stable("property", "db-index.admission-cross-index-isolated");
const admission_fair_id = vopr.id.stable("property", "db-index.admission-fifo-fair");
const admission_cleanup_id = vopr.id.stable("property", "db-index.admission-cancel-shutdown-clean");
const admission_complete_id = vopr.id.stable("property", "db-index.admission-mode-completes");

pub const TextAdmissionScenario = struct {
    pub const name: []const u8 = "db-index-text-admission-races";
    pub const version: u32 = 1;
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = admission_isolation_id, .name = name ++ ".cross-index-isolated", .kind = .always },
        .{ .id = admission_fair_id, .name = name ++ ".fifo-fair", .kind = .always },
        .{ .id = admission_cleanup_id, .name = name ++ ".cancel-shutdown-clean", .kind = .always },
        .{ .id = admission_complete_id, .name = name ++ ".complete", .kind = .reachable },
    };

    const Mode = enum { cross_index, fifo, cancellation, shutdown };
    const mode_ids = [_]u64{
        admission_cross_index_id,
        admission_fifo_id,
        admission_cancel_id,
        admission_shutdown_id,
    };
    const mode_names = [_][]const u8{
        name ++ ".cross-index",
        name ++ ".fifo",
        name ++ ".cancellation",
        name ++ ".shutdown",
    };

    const State = struct {
        allocator: std.mem.Allocator,
        fixture: *Fixture,
        runtime: *text_merge_runtime.TextMergeRuntime,
        complete: bool = false,
        cross_index_isolated: bool = true,
        fifo_fair: bool = true,
        cleanup_safe: bool = true,
        task_failure: ?anyerror = null,

        fn runOne(self: *@This()) !void {
            var enabled: vopr.transition.List = .{};
            defer enabled.deinit(self.allocator);
            var events: vopr.event.Sink = .{};
            defer events.deinit(self.allocator);
            const scheduler = self.fixture.sim.scheduler();
            try scheduler.enumerateReady(&enabled, self.allocator);
            try enabled.canonicalize();
            if (enabled.items.items.len == 0) return error.DbIndexAdmissionDeadlock;
            const selected = for (enabled.items.items) |candidate| {
                if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) break candidate;
            } else enabled.items.items[0];
            try scheduler.executeReady(selected.id, &events, self.allocator);
        }

        fn drive(self: *@This()) !void {
            var transitions: usize = 0;
            while (!self.fixture.sim.scheduler().quiescent()) : (transitions += 1) {
                if (transitions > 128) return error.DbIndexAdmissionTransitionBudgetExceeded;
                try self.runOne();
            }
        }

        fn crossIndexTask(self: *@This()) void {
            var blocker = self.runtime.acquireProducerPermit("a", 100, 0) catch |err| {
                self.task_failure = err;
                return;
            };
            const Waiter = struct {
                runtime: *text_merge_runtime.TextMergeRuntime,
                acquired: bool = false,
                failure: ?anyerror = null,

                fn run(waiter: *@This()) void {
                    var permit = waiter.runtime.acquireProducerPermit("a", 1, 0) catch |err| {
                        waiter.failure = err;
                        return;
                    };
                    waiter.acquired = true;
                    permit.release();
                }
            };
            var waiter = Waiter{ .runtime = self.runtime };
            const io = self.fixture.sim.io();
            var group: std.Io.Group = .init;
            group.async(io, Waiter.run, .{&waiter});
            io.sleep(.fromNanoseconds(1), .awake) catch {};
            var independent = self.runtime.acquireProducerPermit("b", 100, 0) catch |err| {
                blocker.release();
                group.cancel(io);
                self.task_failure = err;
                return;
            };
            independent.release();
            self.cross_index_isolated = !waiter.acquired and waiter.failure == null;
            blocker.release();
            group.await(io) catch |err| {
                self.task_failure = err;
                return;
            };
            self.cross_index_isolated = self.cross_index_isolated and waiter.acquired and waiter.failure == null;
        }

        fn fifoTask(self: *@This()) void {
            var blocker = self.runtime.acquireProducerPermit("shared", 80, 0) catch |err| {
                self.task_failure = err;
                return;
            };
            const Waiter = struct {
                runtime: *text_merge_runtime.TextMergeRuntime,
                segments: u64,
                counter: *u8,
                order: u8 = 0,
                failure: ?anyerror = null,

                fn run(waiter: *@This()) void {
                    var permit = waiter.runtime.acquireProducerPermit("shared", waiter.segments, 0) catch |err| {
                        waiter.failure = err;
                        return;
                    };
                    waiter.counter.* += 1;
                    waiter.order = waiter.counter.*;
                    permit.release();
                }
            };
            var counter: u8 = 0;
            var older = Waiter{ .runtime = self.runtime, .segments = 95, .counter = &counter };
            var younger = Waiter{ .runtime = self.runtime, .segments = 10, .counter = &counter };
            const io = self.fixture.sim.io();
            var group: std.Io.Group = .init;
            group.async(io, Waiter.run, .{&older});
            io.sleep(.fromNanoseconds(1), .awake) catch {};
            group.async(io, Waiter.run, .{&younger});
            io.sleep(.fromNanoseconds(1), .awake) catch {};
            self.fifo_fair = older.order == 0 and younger.order == 0;
            blocker.release();
            group.await(io) catch |err| {
                self.task_failure = err;
                return;
            };
            self.fifo_fair = self.fifo_fair and older.order == 1 and younger.order == 2 and
                older.failure == null and younger.failure == null;
        }

        fn cancellationTask(self: *@This()) void {
            var blocker = self.runtime.acquireProducerPermit("cancel", 100, 0) catch |err| {
                self.task_failure = err;
                return;
            };
            const Waiter = struct {
                runtime: *text_merge_runtime.TextMergeRuntime,
                outcome: u8 = 0,

                fn run(waiter: *@This()) void {
                    var permit = waiter.runtime.acquireProducerPermit("cancel", 1, 0) catch |err| {
                        waiter.outcome = if (err == error.Canceled) 1 else 2;
                        return;
                    };
                    permit.release();
                    waiter.outcome = 3;
                }
            };
            var waiter = Waiter{ .runtime = self.runtime };
            const io = self.fixture.sim.io();
            var group: std.Io.Group = .init;
            group.async(io, Waiter.run, .{&waiter});
            io.sleep(.fromNanoseconds(1), .awake) catch {};
            group.cancel(io);
            blocker.release();
            self.cleanup_safe = waiter.outcome == 1;
            var post_cancel = self.runtime.acquireProducerPermit("cancel", 1, 0) catch |err| {
                self.task_failure = err;
                return;
            };
            post_cancel.release();
        }

        fn shutdownTask(self: *@This()) void {
            var blocker = self.runtime.acquireProducerPermit("shutdown", 100, 0) catch |err| {
                self.task_failure = err;
                return;
            };
            const Waiter = struct {
                runtime: *text_merge_runtime.TextMergeRuntime,
                outcome: u8 = 0,

                fn run(waiter: *@This()) void {
                    var permit = waiter.runtime.acquireProducerPermit("shutdown", 1, 0) catch |err| {
                        waiter.outcome = if (err == error.TextMergeRuntimeShutdown) 1 else 2;
                        return;
                    };
                    permit.release();
                    waiter.outcome = 3;
                }
            };
            var waiter = Waiter{ .runtime = self.runtime };
            const io = self.fixture.sim.io();
            var group: std.Io.Group = .init;
            group.async(io, Waiter.run, .{&waiter});
            io.sleep(.fromNanoseconds(1), .awake) catch {};
            _ = self.runtime.stop();
            blocker.release();
            group.await(io) catch |err| {
                self.task_failure = err;
                return;
            };
            self.cleanup_safe = waiter.outcome == 1;
        }

        fn run(self: *@This(), mode: Mode) !void {
            const io = self.fixture.sim.io();
            _ = switch (mode) {
                .cross_index => io.async(crossIndexTask, .{self}),
                .fifo => io.async(fifoTask, .{self}),
                .cancellation => io.async(cancellationTask, .{self}),
                .shutdown => io.async(shutdownTask, .{self}),
            };
            try self.drive();
            if (self.task_failure) |err| return err;
            try self.fixture.sim.ensureNoCapabilityViolation();
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: std.mem.Allocator) !World {
        const fixture = try allocator.create(Fixture);
        errdefer allocator.destroy(fixture);
        fixture.* = try Fixture.init(allocator);
        errdefer fixture.deinit();
        const resources = fixture.db.core.batchExecutionResources();
        const runtime = try allocator.create(text_merge_runtime.TextMergeRuntime);
        errdefer allocator.destroy(runtime);
        runtime.* = try text_merge_runtime.TextMergeRuntime.init(
            allocator,
            resources.index_manager,
            resources.apply_mutex,
            fixture.backend.ptr(),
            .{
                .enabled = true,
                .max_pending_segments = 100,
                .resume_pending_segments = 50,
                .max_pending_bytes = 100,
                .backpressure_max_wait_ms = 10_000,
            },
        );
        errdefer runtime.deinit();
        const state = try allocator.create(State);
        state.* = .{ .allocator = allocator, .fixture = fixture, .runtime = runtime };
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.runtime.deinit();
        allocator.destroy(world.state.runtime);
        world.state.fixture.deinit();
        allocator.destroy(world.state.fixture);
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, transition_name| try list.append(allocator, .{
            .id = id,
            .name = transition_name,
            .kind = if (mode == .cancellation or mode == .shutdown) .fault else .workload,
        });
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            try world.state.run(mode);
            found = true;
        };
        if (!found) return error.InvalidDbIndexAdmissionTransition;
        try events.emitNamed(allocator, .domain, selected.name, 1);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
        try builder.addNamed(allocator, name ++ ".cross-index", @intFromBool(world.state.cross_index_isolated));
        try builder.addNamed(allocator, name ++ ".fifo", @intFromBool(world.state.fifo_fair));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, admission_isolation_id, world.state.cross_index_isolated);
        try sink.check(allocator, admission_fair_id, world.state.fifo_fair);
        try sink.check(allocator, admission_cleanup_id, world.state.cleanup_safe);
        try sink.check(allocator, admission_complete_id, world.state.complete);
    }

    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

fn runManagedOrder(allocator: std.mem.Allocator, order: []const u64) !void {
    var scripted = vopr.choice.Scripted{ .selections = order };
    var artifact = try vopr.runner.run(ManagedAdmissionScenario, allocator, scripted.source(), .{
        .system = "antfly",
        .transition_budget = 2,
        .source_revision = "db-index-managed-vopr-v1",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    for (0..5) |_| {
        var replayed = try vopr.replay.exact(ManagedAdmissionScenario, allocator, &artifact);
        replayed.deinit();
    }
}

fn runCaptureOrder(allocator: std.mem.Allocator, order: []const u64) !void {
    var scripted = vopr.choice.Scripted{ .selections = order };
    var artifact = try vopr.runner.run(PublishedCaptureScenario, allocator, scripted.source(), .{
        .system = "antfly",
        .transition_budget = 5,
        .source_revision = "db-index-capture-vopr-v1",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    for (0..5) |_| {
        var replayed = try vopr.replay.exact(PublishedCaptureScenario, allocator, &artifact);
        replayed.deinit();
    }
}

fn runAdmissionMode(allocator: std.mem.Allocator, mode_id: u64) !void {
    var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
    var artifact = try vopr.runner.run(TextAdmissionScenario, allocator, scripted.source(), .{
        .system = "antfly",
        .transition_budget = 1,
        .source_revision = "db-index-admission-vopr-v1",
        .target = "native",
        .optimize = @tagName(builtin.mode),
    });
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    for (0..5) |_| {
        var replayed = try vopr.replay.exact(TextAdmissionScenario, allocator, &artifact);
        replayed.deinit();
    }
}

test "DB index request races VOPR exact replays delete materialize and capture handoff" {
    // Zig's debug allocator captures native stack traces by default, which is
    // not defined while executing on a switched fiber stack. Retain allocator
    // safety and leak checking while disabling only that host-only diagnostic.
    var alloc_state: VoprTestAllocator = .init;
    defer _ = alloc_state.deinit();
    const allocator = alloc_state.allocator();

    try runManagedOrder(allocator, &.{ materialize_id, delete_managed_id });
    try runManagedOrder(allocator, &.{ delete_managed_id, materialize_id });
    try runCaptureOrder(allocator, &.{ capture_begin_id, barrier_begin_id, capture_end_id, barrier_end_id, delete_dense_id });
    try runCaptureOrder(allocator, &.{ barrier_begin_id, capture_begin_id, barrier_end_id, delete_dense_id });
    for (TextAdmissionScenario.mode_ids) |mode_id| try runAdmissionMode(allocator, mode_id);
}
