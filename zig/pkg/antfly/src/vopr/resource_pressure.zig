// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One node-wide resource envelope composed across foreground admission,
//! DB-shaped work, a production durable-job lane, provider cancellation, and
//! the persistent lake range cache. VoprIo supplies task, descriptor, and
//! storage quotas so overload and recovery are part of the same replayable
//! schedule as ResourceManager ownership.

const std = @import("std");
const vopr = @import("vopr");
const request_admission = @import("../common/request_admission.zig");
const resource_manager = @import("../storage/resource_manager.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const vopr_durable_job_lane = @import("../storage/vopr_durable_job_lane.zig");
const db_mod = @import("../storage/db/db.zig");
const lite_backend = @import("../storage/lite/backend.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const lake = @import("../serverless/query/lake_parquet_rowgroup.zig");
const FixtureAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

pub const Scenario = struct {
    pub const name: []const u8 = "cross-service-resource-pressure";
    pub const version: u32 = 1;

    const bounded_id = vopr.id.stable(name, "bounded-overload");
    const denial_id = vopr.id.stable(name, "all-limits-observed");
    const recovery_id = vopr.id.stable(name, "progress-after-pressure");
    const cancellation_id = vopr.id.stable(name, "provider-cancellation-clean");
    const cleanup_id = vopr.id.stable(name, "all-ownership-released");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = bounded_id, .name = name ++ ".bounded-overload", .kind = .always },
        .{ .id = denial_id, .name = name ++ ".all-limits-observed", .kind = .reachable },
        .{ .id = recovery_id, .name = name ++ ".progress-after-pressure", .kind = .reachable },
        .{ .id = cancellation_id, .name = name ++ ".provider-cancellation-clean", .kind = .always },
        .{ .id = cleanup_id, .name = name ++ ".all-ownership-released", .kind = .always },
    };

    const memory_hard: u64 = 1_200_000;
    const storage_capacity: u64 = 260;
    const storage_floor: u64 = 16;
    const capacity_domain = vopr.id.stable(name, "node-volume");
    const background_owner: u64 = 73;

    const State = struct {
        allocator: std.mem.Allocator,
        fixture_allocator: *FixtureAllocator,
        sim: vopr.vopr_io.VoprIo,
        resources: resource_manager.ResourceManager,
        durable_jobs: vopr_durable_job_lane.Lane,
        managed: managed_embedder.ManagedEmbedder,
        query_admission: request_admission.RequestAdmission,
        write_admission: request_admission.RequestAdmission,
        inference_admission: request_admission.RequestAdmission,
        mutex: std.Io.Mutex = .init,
        holders_condition: std.Io.Condition = .init,
        release_condition: std.Io.Condition = .init,
        background_condition: std.Io.Condition = .init,
        provider_condition: std.Io.Condition = .init,

        reader_parked: bool = false,
        writer_parked: bool = false,
        release_holders: bool = false,
        provider_parked: bool = false,
        provider_canceled: bool = false,
        background_attempts: u64 = 0,
        background_denied: bool = false,
        background_progressed: bool = false,
        background_deinits: u64 = 0,

        memory_denied: bool = false,
        request_denied: bool = false,
        task_denied: bool = false,
        socket_denied: bool = false,
        file_denied: bool = false,
        storage_denied: bool = false,
        cache_pressure_denied: bool = false,
        storage_baseline: u64 = 0,
        progress_after_pressure: u64 = 0,
        sound: bool = true,
        complete: bool = false,
        failure: ?anyerror = null,

        fn runtimeIo(self: *@This()) std.Io {
            return self.sim.io();
        }

        fn capacityObservation(raw: *anyopaque) anyerror!resource_manager.CapacityObservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const snapshot = self.sim.resourceSnapshot();
            const resource_growth = snapshot.storage_bytes -| self.storage_baseline;
            return .{
                .available_bytes = storage_capacity -| resource_growth,
                .capacity_bytes = storage_capacity,
                .observed_at_ns = 0,
                .valid_for_ns = 0,
            };
        }

        fn fail(self: *@This(), err: anyerror) void {
            if (self.failure == null) self.failure = err;
            self.sound = false;
            std.debug.print("resource pressure failure={}\n", .{err});
        }

        fn readerHolder(self: *@This()) void {
            const io = self.runtimeIo();
            var request = self.query_admission.tryAcquireLease() orelse {
                self.fail(error.QueryAdmissionUnexpectedlyDenied);
                return;
            };
            defer request.release();
            var memory = self.resources.reserve(.dense_search_working_set, 600_000) catch |err| {
                self.fail(err);
                return;
            };
            defer memory.release();
            const sockets = std.Io.net.Socket.createPair(io, .{}) catch |err| {
                self.fail(err);
                return;
            };
            defer {
                sockets[0].close(io);
                sockets[1].close(io);
            }
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.reader_parked = true;
            self.holders_condition.broadcast(io);
            while (!self.release_holders) self.release_condition.wait(io, &self.mutex) catch return;
        }

        fn writerHolder(self: *@This()) void {
            const io = self.runtimeIo();
            var request = self.write_admission.tryAcquireLease() orelse {
                self.fail(error.WriteAdmissionUnexpectedlyDenied);
                return;
            };
            defer request.release();
            var memory = self.resources.reserve(.lsm_wal_write_working_set, 600_000) catch |err| {
                self.fail(err);
                return;
            };
            defer memory.release();
            var file = std.Io.Dir.cwd().createFile(io, "pressure.db", .{ .read = true }) catch |err| {
                self.fail(err);
                return;
            };
            defer file.close(io);
            file.writePositionalAll(io, "0123456789abcdef0123456789abcdef", 0) catch |err| {
                self.fail(err);
                return;
            };
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.writer_parked = true;
            self.holders_condition.broadcast(io);
            while (!self.release_holders) self.release_condition.wait(io, &self.mutex) catch return;
        }

        fn denseFallback(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.ProviderContextRequired;
        }

        fn sparseFallback(
            _: *anyopaque,
            allocator: std.mem.Allocator,
            _: []const u8,
            _: []const []const u8,
        ) ![]db_embedder.SparseEmbedding {
            return try allocator.alloc(db_embedder.SparseEmbedding, 0);
        }

        fn denseWithContext(
            raw: *anyopaque,
            allocator: std.mem.Allocator,
            _: []const u8,
            texts: []const []const u8,
            _: managed_embedder.EmbeddingRequestContext,
        ) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const io = self.runtimeIo();
            var request = self.inference_admission.tryAcquireLease() orelse {
                return error.QueueFull;
            };
            defer request.release();
            var memory = self.resources.reserve(.inference_scratch_working_set, 400_000) catch |err| {
                return err;
            };
            defer memory.release();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.provider_parked = true;
            self.provider_condition.broadcast(io);
            self.provider_condition.wait(io, &self.mutex) catch |err| switch (err) {
                error.Canceled => {
                    self.provider_canceled = true;
                    return error.Canceled;
                },
            };

            const vectors = try allocator.alloc([]f32, texts.len);
            errdefer allocator.free(vectors);
            var initialized: usize = 0;
            errdefer for (vectors[0..initialized]) |vector| allocator.free(vector);
            for (vectors) |*vector| {
                vector.* = try allocator.dupe(f32, &.{ 0.25, 0.75 });
                initialized += 1;
            }
            return vectors;
        }

        fn providerCancelable(self: *@This()) void {
            const dense = self.managed.denseInterface();
            const batch = dense.embedDenseBatch(
                self.allocator,
                "semantic_idx",
                &.{"cancel-me"},
                2,
            ) catch {
                if (!self.provider_canceled) self.fail(error.ManagedProviderCancellationUnexpectedFailure);
                return;
            };
            db_embedder.freeDenseEmbeddingBatch(self.allocator, batch);
            self.fail(error.ManagedProviderCancellationUnexpectedSuccess);
        }

        fn noOp() void {}

        fn exerciseDb(self: *@This()) !void {
            const io = self.runtimeIo();
            var runtime = try background_runtime.BackendRuntime.init(self.allocator, .{
                .backend = .manual,
                .borrowed_io = .{ .general = io },
            });
            defer runtime.deinit();
            var backend = try lite_backend.Handle.openOrCreate(self.allocator, "resource-db.aflite", .{
                .resource_manager = &self.resources,
                .io = io,
            });
            defer backend.deinit();
            var options = db_mod.OpenOptions{
                .open_mode = .writer,
                .executor = .{ .backend = .manual },
                .backend_runtime = &runtime,
                .resource_manager = &self.resources,
                .external_derived_checkpoints = false,
                .ttl_cleanup = .{ .enabled = false },
                .transaction_recovery = .{ .enabled = false },
                .text_merge = .{ .enabled = false },
                .sparse_compaction = .{ .enabled = false },
            };
            try backend.configureDbOpenOptions(&options);
            var db = db_mod.DB.open(self.allocator, "resource-db", options) catch |err| {
                std.debug.print("resource pressure db=open err={}\n", .{err});
                return err;
            };
            defer db.close();
            db.batch(.{
                .writes = &.{.{ .key = "doc:pressure", .value = "{\"title\":\"shared-envelope\"}" }},
                .sync_level = .write,
            }) catch |err| {
                std.debug.print("resource pressure db=batch err={}\n", .{err});
                return err;
            };
            var result = (db.lookup(self.allocator, "doc:pressure", .{}) catch |err| {
                std.debug.print("resource pressure db=lookup err={}\n", .{err});
                return err;
            }) orelse
                return error.ResourcePressureDbLookupMissing;
            defer result.deinit(self.allocator);
            if (std.mem.indexOf(u8, result.json, "shared-envelope") == null)
                return error.ResourcePressureDbLookupInvalid;
            self.progress_after_pressure += 1;
        }

        fn backgroundRun(raw: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.background_attempts += 1;
            var reservation = self.resources.reserve(.lsm_compaction_work, 400_000) catch |err| switch (err) {
                error.ResourceBudgetExceeded => {
                    self.background_denied = true;
                    return;
                },
                else => return err,
            };
            defer reservation.release();
            self.background_progressed = true;
            self.progress_after_pressure += 1;
        }

        fn backgroundDeinit(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const io = self.runtimeIo();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.background_deinits += 1;
            self.background_condition.broadcast(io);
        }

        fn submitBackground(self: *@This()) !void {
            try self.durable_jobs.lane().submit(.{
                .owner_id = background_owner,
                .class = .maintenance,
                .ptr = self,
                .run = backgroundRun,
                .deinit = backgroundDeinit,
            });
        }

        fn yieldUntil(
            self: *@This(),
            condition: *std.Io.Condition,
            predicate: *const fn (*State) bool,
        ) !void {
            const io = self.runtimeIo();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            while (!predicate(self)) try condition.wait(io, &self.mutex);
        }

        fn holdersReady(self: *State) bool {
            return self.reader_parked and self.writer_parked;
        }

        fn firstBackgroundDone(self: *State) bool {
            return self.background_deinits >= 1;
        }

        fn secondBackgroundDone(self: *State) bool {
            return self.background_deinits >= 2;
        }

        fn providerReady(self: *State) bool {
            return self.provider_parked;
        }

        fn verifyPressure(self: *@This(), cache: *lake.PersistentObjectRangeCache) !void {
            const io = self.runtimeIo();
            const memory = self.resources.snapshot().memory;
            if (memory.used_bytes != memory_hard)
                return error.SharedMemoryEnvelopeNotHeld;

            var inference_request = self.inference_admission.tryAcquireLease() orelse
                return error.InferenceRequestAdmissionUnexpectedlyDenied;
            if (self.resources.reserve(.inference_scratch_working_set, 400_000)) |unexpected| {
                var reservation = unexpected;
                reservation.release();
                inference_request.release();
                return error.ProviderMemoryUnexpectedlyAdmitted;
            } else |err| switch (err) {
                error.ResourceBudgetExceeded => self.memory_denied = true,
                else => return err,
            }
            inference_request.release();

            if (self.query_admission.tryAcquireLease()) |unexpected| {
                var lease = unexpected;
                lease.release();
                return error.ConcurrentQueryUnexpectedlyAdmitted;
            }
            if (self.write_admission.tryAcquireLease()) |unexpected| {
                var lease = unexpected;
                lease.release();
                return error.ConcurrentWriteUnexpectedlyAdmitted;
            }
            self.request_denied = true;

            const cache_result = cache.enqueueWrite("pressure", "cache-memory-under-pressure");
            self.cache_pressure_denied = cache_result == .dropped;
            if (!self.cache_pressure_denied) return error.CacheQueueMemoryUnexpectedlyAdmitted;

            if (io.concurrent(noOp, .{})) |unexpected| {
                var future = unexpected;
                future.cancel(io);
                return error.TaskQuotaUnexpectedlyAdmitted;
            } else |_| self.task_denied = true;

            if (std.Io.net.Socket.createPair(io, .{})) |unexpected| {
                unexpected[0].close(io);
                unexpected[1].close(io);
                return error.SocketQuotaUnexpectedlyAdmitted;
            } else |err| switch (err) {
                error.ProcessFdQuotaExceeded => self.socket_denied = true,
                else => return err,
            }

            const probe_names = [_][]const u8{
                "descriptor-probe-a",
                "descriptor-probe-b",
                "descriptor-probe-c",
                "descriptor-probe-d",
                "descriptor-probe-e",
                "descriptor-probe-f",
                "descriptor-probe-g",
                "descriptor-probe-h",
            };
            var probes: [probe_names.len]?std.Io.File = @splat(null);
            defer for (&probes) |*probe| if (probe.*) |*file| file.close(io);
            for (probe_names, 0..) |probe_name, index| {
                if (std.Io.Dir.cwd().createFile(io, probe_name, .{})) |file| {
                    probes[index] = file;
                } else |err| switch (err) {
                    error.ProcessFdQuotaExceeded => {
                        self.file_denied = true;
                        break;
                    },
                    else => return err,
                }
            }
            if (!self.file_denied) return error.FileQuotaUnexpectedlyAdmitted;

            try self.submitBackground();
            try self.yieldUntil(&self.background_condition, firstBackgroundDone);
            if (!self.background_denied) return error.BackgroundMemoryUnexpectedlyAdmitted;
        }

        fn releasePressure(self: *@This()) void {
            const io = self.runtimeIo();
            self.mutex.lockUncancelable(io);
            self.release_holders = true;
            self.release_condition.broadcast(io);
            self.mutex.unlock(io);
        }

        fn verifyRecovery(self: *@This(), cache: *lake.PersistentObjectRangeCache) !void {
            const io = self.runtimeIo();
            if (self.resources.snapshot().memory.used_bytes != 0) return error.MemoryDidNotRecover;
            if (self.query_admission.stats().in_flight != 0 or self.write_admission.stats().in_flight != 0)
                return error.ForegroundAdmissionDidNotRecover;

            var provider_memory = try self.resources.reserve(.inference_scratch_working_set, 400_000);
            provider_memory.release();
            self.progress_after_pressure += 1;

            try self.submitBackground();
            try self.yieldUntil(&self.background_condition, secondBackgroundDone);
            if (!self.background_progressed) return error.BackgroundDidNotRecover;

            const sockets = try std.Io.net.Socket.createPair(io, .{});
            sockets[0].close(io);
            sockets[1].close(io);
            self.progress_after_pressure += 1;

            var task = try io.concurrent(noOp, .{});
            task.await(io);
            self.progress_after_pressure += 1;

            const payload = "abcdefghijklmnopqrstuvwx";
            if (cache.enqueueWrite("cache-a", payload) != .enqueued) return error.FirstCacheWriteNotAdmitted;
            cache.flush();
            if (cache.enqueueWrite("cache-b", payload) != .enqueued) return error.SecondCacheWriteNotAdmitted;
            cache.flush();
            if (cache.enqueueWrite("cache-c", payload) != .enqueued) return error.ThirdCacheWriteNotQueued;
            cache.flush();
            const cache_stats = cache.statsSnapshot();
            self.storage_denied = cache_stats.writes_completed == 2 and
                cache_stats.writes_dropped >= 2 and
                self.resources.capacityStats().denials >= 1;
            if (!self.storage_denied) return error.StorageCapacityWasNotEnforcedBeforeWrite;
            self.progress_after_pressure += cache_stats.writes_completed;

            var provider = try io.concurrent(providerCancelable, .{self});
            try self.yieldUntil(&self.provider_condition, providerReady);
            provider.cancel(io);
            if (!self.provider_canceled or self.inference_admission.stats().in_flight != 0 or
                self.resources.sliceStats(.inference_scratch_working_set).used_bytes != 0)
                return error.ProviderCancellationLeakedOwnership;
            self.progress_after_pressure += 1;
        }

        fn runRootInner(self: *@This()) !void {
            const io = self.runtimeIo();
            self.exerciseDb() catch |err| {
                std.debug.print("resource pressure stage=exercise-db err={}\n", .{err});
                return err;
            };
            self.storage_baseline = self.sim.resourceSnapshot().storage_bytes;
            var cache = try lake.PersistentObjectRangeCache.initWithPolicyAndResources(io, "/resource-cache", .{
                .max_total_bytes = 4096,
                .max_entries = 16,
                .max_write_queue_bytes = 64,
                .max_write_queue_entries = 4,
                .durability = .durable,
            }, .{ .resource_manager = &self.resources });
            defer cache.deinit();

            var reader = try io.concurrent(readerHolder, .{self});
            var reader_live = true;
            defer if (reader_live) reader.cancel(io);
            var writer = try io.concurrent(writerHolder, .{self});
            var writer_live = true;
            defer if (writer_live) writer.cancel(io);

            try self.yieldUntil(&self.holders_condition, holdersReady);
            self.verifyPressure(&cache) catch |err| {
                std.debug.print("resource pressure stage=verify-pressure err={}\n", .{err});
                return err;
            };
            self.releasePressure();
            reader.await(io);
            reader_live = false;
            writer.await(io);
            writer_live = false;
            self.verifyRecovery(&cache) catch |err| {
                std.debug.print("resource pressure stage=verify-recovery err={}\n", .{err});
                return err;
            };
            self.durable_jobs.lane().closeOwner(background_owner);
        }

        fn runRoot(self: *@This()) void {
            self.runRootInner() catch |err| self.fail(err);
            self.complete = true;
        }

        fn allDenials(self: *const @This()) bool {
            return self.memory_denied and self.request_denied and self.task_denied and
                self.socket_denied and self.file_denied and self.storage_denied and
                self.cache_pressure_denied and self.background_denied;
        }

        fn ownershipClean(self: *@This()) bool {
            if (!self.complete) return true;
            const runtime = self.sim.resourceSnapshot();
            return self.resources.snapshot().memory.used_bytes == 0 and
                self.resources.capacityStats().reserved_bytes == 0 and
                self.query_admission.stats().in_flight == 0 and
                self.write_admission.stats().in_flight == 0 and
                self.inference_admission.stats().in_flight == 0 and
                self.durable_jobs.stats().pending_jobs == 0 and
                runtime.active_tasks == 0 and runtime.open_file_handles == 0 and
                runtime.open_sockets == 0;
        }
    };

    pub const World = struct { state: *State };

    fn resourceOptions(allocator: std.mem.Allocator) resource_manager.Options {
        var options = resource_manager.Options{
            .identity_allocator = allocator,
            .memory_budget = .{ .soft_limit_bytes = 900_000, .hard_limit_bytes = memory_hard },
            .disk_safety_floor_bytes = storage_floor,
            .disk_safety_floor_divisor = 0,
        };
        inline for (&.{
            resource_manager.Slice.dense_search_working_set,
            resource_manager.Slice.lsm_wal_write_working_set,
            resource_manager.Slice.lsm_compaction_work,
            resource_manager.Slice.inference_scratch_working_set,
            resource_manager.Slice.lake_range_cache_queue,
        }) |slice| options.budgets[@intFromEnum(slice)] = .{ .soft_limit_bytes = 480_000, .hard_limit_bytes = 640_000 };
        return options;
    }

    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        const fixture_allocator = try allocator.create(FixtureAllocator);
        errdefer allocator.destroy(fixture_allocator);
        fixture_allocator.* = .init;
        errdefer _ = fixture_allocator.deinit();
        const fixture_alloc = fixture_allocator.allocator();
        var sim = try vopr.vopr_io.VoprIo.init(.{
            .seed = 0x52e5_50e5,
            .required = .of(&.{ .files, .sockets, .task_scheduling, .synchronization, .clock_read }),
            .tasks = .{ .stack_size = 8 * 1024 * 1024, .max_tasks = 4 },
            .files = .{ .max_open_handles = 8, .capacity_bytes = 1024 * 1024 },
            .network = .{ .max_sockets = 3, .stream_capacity = 128 },
        });
        errdefer sim.deinit();
        state.* = .{
            .allocator = fixture_alloc,
            .fixture_allocator = fixture_allocator,
            .sim = sim,
            .resources = resource_manager.ResourceManager.init(resourceOptions(fixture_alloc)),
            .durable_jobs = undefined,
            .managed = undefined,
            .query_admission = request_admission.RequestAdmission.init(1),
            .write_admission = request_admission.RequestAdmission.init(1),
            .inference_admission = request_admission.RequestAdmission.init(1),
        };
        errdefer state.resources.deinit(fixture_alloc);
        state.durable_jobs = vopr_durable_job_lane.Lane.init(fixture_alloc, state.sim.executor());
        errdefer state.durable_jobs.deinit();
        try state.durable_jobs.registerOwner(background_owner);
        state.managed = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(fixture_alloc,
            \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":2,"embedder":{"provider":"antfly","model":"antflydb/vopr-pressure"}}}
        , .{
            .antfly_provider = .{
                .ptr = state,
                .embed_dense_texts = State.denseFallback,
                .embed_dense_texts_with_context = State.denseWithContext,
                .embed_sparse_texts = State.sparseFallback,
            },
            .io = state.sim.io(),
        });
        errdefer state.managed.deinit();
        try state.resources.installCapacitySource(.{
            .ptr = state,
            .domain_id = capacity_domain,
            .observe = State.capacityObservation,
        });
        _ = state.sim.io().async(State.runRoot, .{state});
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        const state = world.state;
        if (!state.complete or !state.ownershipClean()) std.debug.print(
            "resource pressure dirty complete={} active={d} reader={} writer={} released={} provider_parked={} provider_canceled={} background_attempts={d} background_deinits={d} memory={d} progress={d} failure={?}\n",
            .{
                state.complete,
                state.sim.resourceSnapshot().active_tasks,
                state.reader_parked,
                state.writer_parked,
                state.release_holders,
                state.provider_parked,
                state.provider_canceled,
                state.background_attempts,
                state.background_deinits,
                state.resources.snapshot().memory.used_bytes,
                state.progress_after_pressure,
                state.failure,
            },
        );
        state.durable_jobs.deinit();
        state.managed.deinit();
        state.sim.deinit();
        state.resources.deinit(state.allocator);
        const fixture_allocator = state.fixture_allocator;
        std.debug.assert(fixture_allocator.deinit() == .ok);
        allocator.destroy(fixture_allocator);
        allocator.destroy(state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete and world.state.sim.scheduler().quiescent()) return;
        try world.state.sim.scheduler().enumerateReady(list, allocator);
    }

    pub fn execute(
        world: *World,
        selected: vopr.transition.Transition,
        events: *vopr.event.Sink,
        allocator: std.mem.Allocator,
    ) !vopr.outcome.TransitionOutcome {
        try world.state.sim.scheduler().executeReady(selected.id, events, allocator);
        return .applied();
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const runtime = state.sim.resourceSnapshot();
        try builder.addNamed(allocator, name ++ ".progress", @intCast(state.progress_after_pressure));
        try builder.addNamed(allocator, name ++ ".memory-used", @intCast(state.resources.snapshot().memory.used_bytes));
        try builder.addNamed(allocator, name ++ ".capacity-reserved", @intCast(state.resources.capacityStats().reserved_bytes));
        try builder.addNamed(allocator, name ++ ".active-tasks", @intCast(runtime.active_tasks));
        try builder.addNamed(allocator, name ++ ".open-files", @intCast(runtime.open_file_handles));
        try builder.addNamed(allocator, name ++ ".open-sockets", @intCast(runtime.open_sockets));
        try builder.addNamed(allocator, name ++ ".denials", @intFromBool(state.allDenials()));
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(state.complete));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        const state = world.state;
        const runtime = state.sim.resourceSnapshot();
        const memory = state.resources.snapshot().memory;
        try sink.check(allocator, bounded_id, state.sound and memory.used_bytes <= memory_hard and
            runtime.open_file_handles <= 8 and runtime.open_sockets <= 3 and runtime.active_tasks <= 4);
        try sink.check(allocator, denial_id, state.allDenials());
        try sink.check(allocator, recovery_id, state.progress_after_pressure >= 7);
        try sink.check(allocator, cancellation_id, !state.complete or state.provider_canceled);
        try sink.check(allocator, cleanup_id, state.ownershipClean());
    }

    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const state = world.state;
        const runtime = state.sim.resourceSnapshot();
        return .{
            .progress_expected = true,
            .progress_units = state.progress_after_pressure,
            .active_tasks = @intCast(runtime.active_tasks),
            .open_descriptors = @intCast(runtime.open_file_handles + runtime.open_sockets),
            .allocator_exhausted = false,
            // Expected, classified capacity denial is scenario evidence, not
            // an unhandled harness/storage exhaustion.
            .storage_exhausted = false,
            .cleanup_complete = state.complete and state.ownershipClean(),
        };
    }

    pub fn done(world: *World) bool {
        return world.state.complete and world.state.sim.scheduler().quiescent();
    }
};

test "cross-service resource pressure VOPR exact replays overload cancellation cleanup and recovery" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (0..8) |ordinal| {
        var seeded = vopr.choice.Seeded.init(0x5052_3555 + ordinal);
        var artifact = vopr.runner.run(Scenario, std.testing.allocator, seeded.source(), .{
            .system = "antfly",
            .transition_budget = 200,
            .backend_ids = &backend_ids,
            .source_revision = "cross-service-resource-pressure-v1",
        }) catch |err| {
            std.debug.print("resource pressure seed={d} runner error={}\n", .{ ordinal, err });
            return err;
        };
        defer artifact.deinit();
        if (artifact.summary.?.property_failures != 0) {
            for (artifact.failures.items) |failure| std.debug.print(
                "resource pressure seed={d} failure={s} class={s}\n",
                .{ ordinal, failure.identity, @tagName(failure.class) },
            );
        }
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
