// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const model_manager_mod = @import("model_manager.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const runtime = @import("../runtime/root.zig");
const ops = @import("../ops/ops.zig");
const reader_execution_pool = @import("reader_execution_pool.zig");
const execution_control_mod = @import("../execution_control.zig");
const executor_microbatch = @import("executor_microbatch.zig");
const generation = @import("../pipelines/generation.zig");
const session_factory = @import("../architectures/session_factory.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");

const InferenceExecutionControl = execution_control_mod.InferenceExecutionControl;
const banks = @import("../execution/prepared_kv_banks.zig");
const cold_target = @import("../backends/cold_compute_target.zig");
const compute_factory = @import("../backends/compute_factory.zig");
const capacity = @import("../runtime/kv/device_capacity.zig");
const storage_admission = @import("../execution/storage_admission.zig");
const cleanup_owner = @import("../execution/cleanup_owner.zig");
var lighton_stream_execution_id: std.atomic.Value(u64) = .init(1);

/// Metadata reservation, not numerical exclusion. Suppliers execute with no
/// mutex held. A counted outside borrower keeps restoration from reacquiring
/// real lanes until that borrower's own qualified PAGE callback has returned.
const AdmissionWindow = struct {
    const busy: u64 = @as(u64, 1) << 63;
    const outside: u64 = @as(u64, 1) << 62;
    const closing: u64 = @as(u64, 1) << 61;
    const count_mask: u64 = closing - 1;
    state: std.atomic.Value(u64) = .init(0),
    const Borrow = struct {
        window: *AdmissionWindow,
        fn finish(self: Borrow) void {
            const old = self.window.state.fetchSub(1, .acq_rel);
            std.debug.assert((old & count_mask) != 0 and (old & busy) != 0);
        }
    };
    fn reserve(self: *AdmissionWindow) bool {
        return self.state.cmpxchgStrong(0, busy, .acq_rel, .acquire) == null;
    }
    fn abandonUnpublished(self: *AdmissionWindow) void {
        std.debug.assert(self.state.swap(0, .acq_rel) == busy);
    }
    fn publishOutside(self: *AdmissionWindow) void {
        const old = self.state.swap(busy | outside, .acq_rel);
        std.debug.assert(old == busy or old == (busy | closing));
    }
    fn borrowOutside(self: *AdmissionWindow) !?Borrow {
        var old = self.state.load(.acquire);
        while ((old & (busy | outside | closing)) == (busy | outside)) {
            if ((old & count_mask) == count_mask) return error.AdmissionWindowReferenceOverflow;
            old = self.state.cmpxchgWeak(old, old + 1, .acq_rel, .acquire) orelse return .{ .window = self };
        }
        return null;
    }
    fn closeOutside(self: *AdmissionWindow) void {
        var old = self.state.load(.acquire);
        while (true) {
            std.debug.assert((old & (busy | outside | closing)) == (busy | outside));
            old = self.state.cmpxchgWeak(old, (old & ~outside) | closing, .acq_rel, .acquire) orelse break;
        }
        // No actual step/provider/model lane has been reacquired yet. Joined
        // quota callbacks may still perform their independently owned numerics.
        while ((self.state.load(.acquire) & count_mask) != 0) platform.time.sleepNs(100 * std.time.ns_per_us);
    }
    fn finishRestored(self: *AdmissionWindow) void {
        std.debug.assert(self.state.swap(0, .acq_rel) == (busy | closing));
    }
};

/// Owns a reader epoch, its worker, page controls and all shared leases.
pub fn Epoch(comptime Node: type, comptime Hooks: type) type {
    return struct {
        const LightOnStreamContext = @This();
        allocator: std.mem.Allocator,
        model: *model_manager_mod.LoadedModel,
        cfg: gpt_model_mod.Config,
        features: ExecutionFeatures = .{},
        auxiliary_lease: ?model_manager_mod.GenerationAdmissionLease = null,
        auxiliary_ready: std.atomic.Value(bool) = .init(false),
        auxiliary_acquisitions: usize = 0,
        budget: runtime.tier.memory.RunBudget,
        lane: execution_control_mod.ExecutionLane.Lease,
        cleanup_owner: @import("../execution/cleanup_owner.zig").Owner = .{},
        coordinator: runtime.scheduler.native_generate.NativeGenerateCoordinator,
        manager: runtime.kv.manager.KvManager,
        storage: runtime.kv.storage_runtime.KvStorageRuntime = undefined,
        pool: runtime.kv.block.KvPoolId = undefined,
        cb: ?ops.ComputeBackend = null,
        kv_bank: ?banks.Owned = null,
        creator_admission: ?model_manager_mod.GenerationAdmissionLease = null,
        norm_preparation: if (build_options.enable_cuda) ?*NormPreparation else void = if (build_options.enable_cuda) null else {},
        // Frozen outside Poolmutex, points into the actual bank retained here.
        bank_coverage: ?banks.Coverage = null,
        bank_adopted: bool = false,
        execution_gate_wait_ns: u64 = 0,
        phase_wait_ns: u64 = 0,
        phase_fair: bool = false,
        phase_waiters: std.atomic.Value(usize) = .init(0),
        phase_priority_yields: usize = 0,
        published_vision_peak: std.atomic.Value(usize) = .init(0),
        published_vision_host_peak: std.atomic.Value(usize) = .init(0),
        published_decoder_peak: std.atomic.Value(usize) = .init(0),
        step_mutex: std.atomic.Mutex = .unlocked,
        admission_window: AdmissionWindow = .{},
        execution_pause: execution_control_mod.ExecutionPause = .{},
        captured_root_slots: if (build_options.enable_cuda) []@import("../ops/cuda/cuda_compute.zig").CudaCompute.CapturedCacheRoots.Slot else void = if (build_options.enable_cuda) &.{} else {},
        captured_roots: if (build_options.enable_cuda) ?@import("../ops/cuda/cuda_compute.zig").CudaCompute.CapturedCacheRoots else void = if (build_options.enable_cuda) null else {},
        cohort_waiter: @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter = .{},
        cohort_owned: bool = false,
        final_page_admission: ?model_manager_mod.GenerationAdmissionLease = null,
        metal_park: if (build_options.enable_metal) ?@import("../ops/metal_compute.zig").MetalCompute.ReaderEpochPark else void = if (build_options.enable_metal) null else {},
        custody_metadata_lease: ?runtime.tier.memory.AdmissionLease = null,
        step_gate: execution_control_mod.ExecutionLane = .{},
        gate_events: bool = false,
        cpu_preprocess_items: usize = 0,
        cpu_preprocess_ns: u64 = 0,
        kv_mutex: std.atomic.Mutex = .unlocked,
        controls_mutex: std.atomic.Mutex = .unlocked,
        controls: [reader_execution_pool.Pool.max_capacity]?InferenceExecutionControl = @splat(null),
        registered_pages: usize = 0,
        admitted_pages: usize = 0,
        admitted_controls: [reader_execution_pool.Pool.max_capacity]bool = @splat(false),
        admitted_vision: [reader_execution_pool.Pool.max_capacity]usize = @splat(0),
        admitted_host: [reader_execution_pool.Pool.max_capacity]usize = @splat(0),
        admitted_workspace: [reader_execution_pool.Pool.max_capacity]?@import("reader_workspace.zig").Decoder = @splat(null),
        registered_window_peak: usize = 0,
        execution_id: u64,
        completed: usize = 0,
        recurrent_cache_pages: usize = 0,
        host_recurrent_bytes_total: usize = 0,
        vision_broker: executor_microbatch.Broker,
        prefill_broker: executor_microbatch.Broker,
        vision_seal_mutex: std.Io.Mutex = .init,
        prefill_seal_mutex: std.Io.Mutex = .init,
        prefill_await_groups: usize = 0,
        prefill_await_events: bool = false,
        prefill_host_scratch_acquisitions: usize = 0,
        prefill_host_scratch_releases: usize = 0,
        prefill_host_scratch_peak_bytes: usize = 0,
        vision_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        vision_host_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        decoder_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        decoder_workspace: ?@import("reader_workspace.zig").Decoder = null,
        vision_per_item_peak: usize = 0,
        vision_host_per_item_peak: usize = 0,
        worker: ?std.Thread = null,
        worker_io: ?std.Io.Threaded = null,
        worker_wake: std.Io.Event = .unset,
        worker_stop: std.atomic.Value(bool) = .init(false),
        worker_control: InferenceExecutionControl = .{},
        worker_kv_dtype: runtime.kv.pool.KvDType = undefined,
        worker_steps: usize = 0,
        worker_tail_steps: usize = 0,

        const NormPreparation = @import("../execution/architecture_session.zig").PreparedComputeAdmission;

        pub const ExecutionFeatures = @import("reader_execution_config.zig").Features(Hooks);
        pub const Factory = struct {
            node: *Node,
            model: *model_manager_mod.LoadedModel,
            cfg: gpt_model_mod.Config,
            limits: runtime.tier.memory.Limits,
            physical_rows_limit: usize,
            epoch_page_limit: usize = 64,
            cuda_batch_enabled: bool = false,
            decoder_peak: usize = 0,
            decoder_workspace: ?@import("reader_workspace.zig").Decoder = null,
            dynamic_vision_batch_items: usize = 0,
            features: ExecutionFeatures = .{},
            control: InferenceExecutionControl,
            kv_target: ?cold_target.Retained = null,
            kv_descriptor: ?banks.Descriptor = null,
            kv_geometry: capacity.Geometry = .{},
            kv_domain: ?runtime.tier.admission_domain.Pin = null,
            creator_page: ?CreatorPage = null,
            created_context: ?*LightOnStreamContext = null,
            pub const CreatorPage = struct {
                amounts: runtime.tier.memory.AdmissionAmounts,
                peak: ?usize,
                host_peak: usize,
            };

            fn acquireCustodyMetadata(self: *Factory, bytes: usize) !runtime.tier.memory.AdmissionLease {
                return self.node.model_manager.acquireRunResourceEstimates(&.{.{ .backend_class = .gpu, .limits = self.limits, .amounts = .{ .host_scratch_bytes = bytes } }});
            }

            pub fn describeKv(self: *Factory, prefix: usize, max_new: usize, live: usize, dtype: runtime.kv.pool.KvDType) !void {
                const session = self.model.session;
                const policy = session.run_admission orelse return;
                const model = session.model_lifetime orelse return;
                if (self.control.native_call_policy == .require_supervision and model.close_protection == null) return;
                const config = generation.requestKvManagerPoolConfig(generation.kvPoolConfig(if (session.backend() == .cuda) .cuda else .metal, dtype, self.cfg, Hooks.generationKvSlidingTrimForced()));
                // Cyclic/selected stores require separate proven owners. This
                // path describes only full-history ordinary request storage.
                if (config.sliding_window_size != null) return;
                self.kv_geometry = try @import("../execution/decoder_storage.zig").requiredKvGeometry(&self.cfg, .{});
                if (self.kv_geometry.count == 0) return;
                var target = (try cold_target.Retained.acquire(session, self.control)) orelse return; // lazy Metal honestly falls back
                defer target.deinit();
                const mode: banks.Mode = if (session.backend() == .cuda and dtype == .f32) .sequence_private else .shared_physical_pages;
                // Scheduler withholds one spare physical page from a global
                // pool; private planes have no shared-COW/spare-page contract.
                const window = try capacity.Window.fromRequest(prefix, max_new, live, config.page_size_tokens, if (mode == .shared_physical_pages) 1 else 0);
                const descriptor = (try compute_factory.describeKvCoverage(target.target, dtype, window, mode, self.kv_geometry.items())) orelse return;
                const domain = model.retainAdmissionDomain(policy.controller) orelse return;
                self.kv_domain = domain;
                self.kv_descriptor = descriptor;
                self.kv_target = target;
                target.model = null;
            }
            pub fn deinitKv(self: *Factory) void {
                self.created_context = null; // end the synchronous creator borrow
                if (self.kv_target) |*target| target.deinit();
                self.kv_target = null;
                if (self.kv_domain) |pin| pin.release();
                self.kv_domain = null;
            }
            fn reserveKv(raw: *anyopaque, _: std.mem.Allocator, bytes: usize) !storage_admission.Reservation {
                const self: *Factory = @ptrCast(@alignCast(raw));
                const policy = self.model.session.run_admission orelse return error.InvalidDeviceCapacityAdmission;
                const pin = self.kv_domain orelse return error.InvalidDeviceCapacityAdmission;
                const lifetime = storage_admission.Lifetime.forAdmissionDomain(pin);
                if (!lifetime.acquire()) return error.InvalidDeviceCapacityAdmission;
                errdefer lifetime.release(lifetime.context);
                const lease = try policy.controller.tryAcquire(policy.backend_class, self.limits, .{ .backend_kv_bytes = bytes }, policy.check_live_memory);
                return .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .leases = .{ lease, null }, .lifetime = lifetime, .domain = .backend_kv };
            }
            fn prepareNorm(self: *Factory) !(if (build_options.enable_cuda) ?*NormPreparation else void) {
                if (comptime build_options.enable_cuda) return NormPreparation.prepare(self.model.session, self.cfg, self.control);
            }
            fn prepareKv(self: *Factory) !?banks.Owned {
                const descriptor = if (self.kv_descriptor) |*value| value else return null;
                const target = if (self.kv_target) |*value| value else return null;
                const policy = self.model.session.run_admission orelse return null;
                var preparation = try cleanup_owner.Owner.beginAdmitted(self.model.session);
                defer preparation.finishAfterUnlock();
                const epoch = preparation.epoch() orelse return null;
                // The Pool invokes create only for an empty epoch, outside its
                // mutex. Neither real model nor reader lane has been acquired.
                return compute_factory.prepareKvBanks(target, descriptor.dtype, descriptor.window, descriptor.mode, self.kv_geometry.items(), .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .context = self, .reserve = reserveKv, .domain = self.kv_domain }, self.control, epoch);
            }
        };
        pub fn compatible(existing: *const anyopaque, requested: *const anyopaque) bool {
            const ctx: *const LightOnStreamContext = @ptrCast(@alignCast(existing));
            const factory: *const Factory = @ptrCast(@alignCast(requested));
            if (!std.meta.eql(ctx.features, factory.features) or ctx.cfg.compact_qwen35_kv != factory.cfg.compact_qwen35_kv or ctx.cfg.qwen35_device_only_state != factory.cfg.qwen35_device_only_state) return false;
            if (ctx.bank_coverage) |available| return if (factory.kv_descriptor) |*request| available.covers(request.coverage()) else false;
            return factory.kv_descriptor == null;
        }
        pub fn pageAmounts(ctx: *const LightOnStreamContext, factory: *const Factory, original: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionAmounts {
            var result = original;
            if (ctx.bank_coverage) |available| if (factory.kv_descriptor) |*request| if (available.covers(request.coverage())) {
                result.backend_kv_bytes = 0;
            };
            return result;
        }
        fn enterSessionCohort(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !void {
            std.debug.assert(!ctx.cohort_owned);
            const gate = ctx.model.session.execution_gate orelse return;
            if (gate == ctx.model.nativeGenerationMutex()) return;
            try ctx.model.session.enterExecutionCohort(gate, control, &ctx.cohort_waiter);
            ctx.cohort_owned = true;
        }
        fn leaveSessionCohort(ctx: *LightOnStreamContext) void {
            if (!ctx.cohort_owned) return;
            ctx.cohort_owned = false;
            ctx.model.session.leaveExecutionCohort(ctx.model.session.execution_gate, &ctx.cohort_waiter);
        }

        pub fn create(raw: *anyopaque) !*anyopaque {
            return createWithAllocator(raw, std.heap.smp_allocator);
        }
        fn createWithAllocator(raw: *anyopaque, a: std.mem.Allocator) !*anyopaque {
            const factory: *Factory = @ptrCast(@alignCast(raw));
            // Reserve the exact epoch BEFORE either real outer execution lane.
            // The model's generation mutex remains owned until destroy, so a
            // per-step boundary would be too short for provider/cache cleanup.
            // Reserve lifetime metadata first so every successful cold bank
            // has a guaranteed current cleanup epoch on later create failure.
            // Each Owner has a distinct epoch, even on the same model sink.
            var cleanup = try cleanup_owner.Owner.beginResident(factory.model.session);
            errdefer cleanup.finishAfterUnlock();
            var bank = if (cleanup.epoch() != null) try factory.prepareKv() else null;
            errdefer if (bank) |*value| value.abortOutsideLanes(cleanup.epoch().?);
            const coverage = if (bank) |value| try value.snapshotCoverage() else null;
            const ctx = try a.create(LightOnStreamContext);
            ctx.* = .{ .allocator = a, .model = factory.model, .cfg = factory.cfg, .features = factory.features, .auxiliary_ready = .init(!factory.features.needsAuxiliary()), .budget = runtime.tier.memory.RunBudget.init(factory.limits), .lane = undefined, .coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(a), .manager = runtime.kv.manager.KvManager.init(a), .vision_peak = .{ .max_increments = factory.epoch_page_limit }, .vision_host_peak = .{ .max_increments = factory.epoch_page_limit }, .decoder_peak = .{ .max_increments = factory.epoch_page_limit }, .vision_broker = executor_microbatch.Broker.init(a), .prefill_broker = executor_microbatch.Broker.init(a), .execution_id = lighton_stream_execution_id.fetchAdd(1, .monotonic) };
            errdefer ctx.destroyUnpublished();
            ctx.bank_coverage = coverage;
            if (factory.features.vision_late_seal) ctx.vision_broker.execution_mutex = &ctx.vision_seal_mutex;
            if (factory.features.prefill_late_seal) ctx.prefill_broker.execution_mutex = &ctx.prefill_seal_mutex;
            ctx.coordinator.setPolicy(factory.node.lightOnOcrPolicy(factory.physical_rows_limit));
            if (factory.features.packed_mixed) {
                ctx.coordinator.policy.max_step_prefill_items = factory.features.packed_prefill_items;
                ctx.coordinator.policy.max_step_query_tokens = generation.NativeGenerationPipeline.max_prepared_qwen_prefill_rows;
            }
            ctx.execution_gate_wait_ns = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_DISABLE_GATE_PARK")) 0 else @as(u64, @min(ctx.coordinator.policy.contention_wait_us, 1000)) * std.time.ns_per_us;
            ctx.phase_wait_ns = if ((factory.cuda_batch_enabled and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHASE_PARK")) or metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PHASE_PARK")) ctx.execution_gate_wait_ns else 0;
            ctx.phase_fair = (factory.cuda_batch_enabled and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHASE_FAIR")) or metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PHASE_FAIR");
            ctx.gate_events = ctx.coordinator.usesStepEvents() and platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_GATE_EVENTS", true);
            // Keep the latency-oriented default; bulk-throughput profiles can
            // prioritize runnable prefills to fill the physical decode batch.
            ctx.coordinator.max_decode_streak_before_prefill = factory.features.prefill_decode_streak;
            if (comptime build_options.enable_cuda) if (factory.model.session.backend() == .cuda) {
                ctx.captured_root_slots = try a.alloc(@import("../ops/cuda/cuda_compute.zig").CudaCompute.CapturedCacheRoots.Slot, try std.math.add(usize, 2, try std.math.mul(usize, factory.cfg.num_hidden_layers, 4)));
                @memset(ctx.captured_root_slots, .{});
            };
            if (comptime build_options.enable_metal) if (factory.model.session.backend() == .metal) {
                ctx.metal_park = try @import("../ops/metal_compute.zig").MetalCompute.ReaderEpochPark.prepare();
                // Exact authored metadata declaration (two C arrays + process
                // Cell), once per cold epoch, BEFORE every actual lane. This
                // is new metadata, not another numerical/workspace envelope.
                const bytes = try @import("../ops/metal_compute.zig").MetalCompute.ReaderEpochPark.metadataBytes();
                // Persistent SDK/host custody is independent of reserved
                // numerical generation workspace; never borrow/subtract that
                // separate owner before partitioning this metadata credit.
                ctx.custody_metadata_lease = try factory.acquireCustodyMetadata(bytes);
                const source = if (ctx.custody_metadata_lease) |*lease| lease else return error.MissingCustodyMetadataAdmission;
                const domain = factory.node.model_manager.resource_domain.?.admission_owner.pin();
                defer domain.release();
                // SAME declared lease partitions into independently retained
                // Cell metadata and the Reader's reusable C-bank metadata.
                try ctx.metal_park.?.adoptMetadata(source, &domain);
            };
            if (factory.creator_page) |page| {
                try factory.control.check();
                const actual = ctx.pageAmounts(factory, page.amounts);
                var admission_wait = @import("../execution/reader_pool.zig").AdmissionWait{ .last_progress_ns = platform.time.monotonicNs() };
                ctx.creator_admission = while (true) {
                    try factory.control.check();
                    const admitted = ctx.admitPageInternal(factory, null, page.peak, page.host_peak, actual, true) catch |err| {
                        if (err != error.ResourceTemporarilyUnavailable) return err;
                        if (admission_wait.expired(platform.time.monotonicNs(), null)) return err;
                        platform.time.sleepNs(admission_wait.nextRetryDelayNs());
                        continue;
                    };
                    break admitted;
                };
            }
            // Only the empty epoch creator reaches this cold producer. The
            // independent prepared KV bank stays intact across metadata races.
            var norms: if (build_options.enable_cuda) ?*NormPreparation else void = if (build_options.enable_cuda) null else {};
            errdefer if (comptime build_options.enable_cuda) if (norms) |value| value.finishAfterUnlock(cleanup.epoch());
            var lane: execution_control_mod.ExecutionLane.Lease = undefined;
            var lane_owned = false;
            errdefer if (lane_owned) lane.release();
            var model_owned = false;
            errdefer if (model_owned) factory.model.unlockNativeGeneration();
            errdefer ctx.leaveSessionCohort();
            for (0..8) |_| {
                try factory.control.check();
                if (comptime build_options.enable_cuda) if (cleanup.epoch() != null) {
                    norms = try factory.prepareNorm();
                    if (norms) |value| {
                        ctx.captured_roots = try @import("../ops/cuda/cuda_compute.zig").CudaCompute.CapturedCacheRoots.init(ctx.captured_root_slots, value.normRequests());
                        try ctx.prepareLegacyQuotasOutside(value);
                    }
                };
                lane = try factory.model.native_reader_execution_lane.acquire(factory.control);
                lane_owned = true;
                try factory.control.lock(factory.model.nativeGenerationMutex());
                model_owned = true;
                try ctx.enterSessionCohort(factory.control);
                if (comptime build_options.enable_cuda) if (norms) |value| {
                    ctx.validateNormsAndLegacyQuotas(value) catch |err| {
                        if (err != error.PreparedCachePlanChanged) return err;
                        // No ctx/consumer/bank adoption or numerical SDK work
                        // exists yet. Retry only after BOTH real lanes leave.
                        ctx.leaveSessionCohort();
                        factory.model.unlockNativeGeneration();
                        model_owned = false;
                        lane.release();
                        lane_owned = false;
                        if (comptime build_options.enable_cuda) if (ctx.captured_roots) |*roots| {
                            roots.finishLegacyPreparationOutside();
                            ctx.captured_roots = null;
                        };
                        value.finishAfterUnlock(cleanup.epoch());
                        norms = null;
                        continue;
                    };
                };
                break;
            }
            if (!model_owned) return error.ResourceTemporarilyUnavailable;
            ctx.lane = lane;
            ctx.cleanup_owner = cleanup;
            cleanup = .{};
            ctx.kv_bank = bank;
            bank = null;
            if (comptime build_options.enable_cuda) {
                ctx.norm_preparation = norms;
                norms = null;
            }
            factory.created_context = ctx;
            return ctx;
        }
        fn destroyUnpublished(ctx: *LightOnStreamContext) void {
            if (comptime build_options.enable_cuda) {
                if (ctx.captured_roots) |*roots| roots.finishLegacyPreparationOutside();
                ctx.allocator.free(ctx.captured_root_slots);
            }
            if (comptime build_options.enable_metal) if (ctx.metal_park) |*park| park.deinit();
            if (ctx.custody_metadata_lease) |*lease| {
                lease.release();
                ctx.custody_metadata_lease = null;
            }
            std.debug.assert(ctx.cb == null and ctx.worker == null and ctx.registered_pages == 0);
            if (ctx.creator_admission) |*lease| lease.release();
            if (ctx.auxiliary_lease) |*lease| lease.release();
            ctx.vision_peak.deinit();
            ctx.vision_host_peak.deinit();
            ctx.decoder_peak.deinit();
            ctx.vision_broker.deinit();
            ctx.prefill_broker.deinit();
            ctx.manager.deinit();
            ctx.coordinator.deinit();
            ctx.allocator.destroy(ctx);
        }
        /// The creating Factory is a synchronous borrower of this exact ctx.
        /// Ownership moves into its actual registered page before cancellation
        /// is observed; a different joining Factory cannot steal this lease.
        pub fn takeCreatorAdmission(ctx: *LightOnStreamContext, factory: *Factory, index: usize, peak: ?usize, host_peak: usize, amounts: runtime.tier.memory.AdmissionAmounts) !?model_manager_mod.GenerationAdmissionLease {
            if (factory.created_context != ctx) return null;
            const page = factory.creator_page orelse return null;
            const lease = ctx.creator_admission orelse return error.CreatorAdmissionAlreadyConsumed;
            if (ctx.model != factory.model or page.peak != peak or page.host_peak != host_peak or !std.meta.eql(ctx.pageAmounts(factory, page.amounts), amounts)) return error.InvalidCreatorAdmissionHandoff;
            ctx.noteAdmitted(index, peak, host_peak, factory.decoder_workspace);
            ctx.creator_admission = null;
            factory.created_context = null;
            return lease;
        }
        pub fn destroy(raw: *anyopaque) void {
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            std.debug.assert(ctx.registered_pages == 0);
            // Published results can let the last caller retire while the
            // executor is still freeing its own output. Join before the CB,
            // coordinator, model lane, or worker-owned Io can be destroyed.
            if (ctx.worker) |worker| {
                ctx.worker_stop.store(true, .release);
                ctx.worker_wake.set(ctx.worker_io.?.io());
                worker.join();
                ctx.coordinator.unregisterExecutor();
                ctx.worker_io.?.deinit();
                ctx.worker_io = null;
            }
            // Worker numerical/error cleanup has joined. Clear the borrowed
            // Funding epoch before moving its actual stable owner out of ctx.
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| if (ctx.cb) |*cb| norms.unbind(cb);
            var cleanup = ctx.cleanup_owner;
            ctx.cleanup_owner = .{};
            const cleanup_epoch = cleanup.epoch();
            defer cleanup.finishAfterUnlock();
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) {
                std.debug.print("LIGHTONOCR3 device_recurrent_state={d} recurrent_cache_pages={d} host_recurrent_bytes_total={d}\n", .{ @intFromBool(ctx.cfg.qwen35_device_only_state), ctx.recurrent_cache_pages, ctx.host_recurrent_bytes_total });
                std.debug.print("LIGHTONOCR3 prefill_await_groups={d} prefill_await_events={d} prefill_decode_streak={d}\n", .{ ctx.prefill_await_groups, @intFromBool(ctx.prefill_await_events), ctx.features.prefill_decode_streak });
                std.debug.print("LIGHTONOCR3 cpu_preprocess={d} cpu_preprocess_items={d} cpu_preprocess_ns={d} vision_batch_limit={d}\n", .{ @intFromBool(ctx.features.cpu_preprocess), ctx.cpu_preprocess_items, ctx.cpu_preprocess_ns, ctx.features.vision_batch_items });
                std.debug.print("LIGHTONOCR3 shared_pool_event_waits={d} shared_pool_poll_waits={d}\n", .{ ctx.model.native_reader_stream.event_waits.load(.monotonic), ctx.model.native_reader_stream.poll_waits.load(.monotonic) });
                std.debug.print("LIGHTONOCR3 gate_events={d} gate_event_acquisitions={d} gate_event_waits={d}\n", .{ @intFromBool(ctx.gate_events), ctx.step_gate.acquisitions, ctx.step_gate.event_waits });
                std.debug.print("LIGHTONOCR3 executor_steps={d}\n", .{ctx.worker_steps});
                std.debug.print("LIGHTONOCR3 executor_tail_steps={d}\n", .{ctx.worker_tail_steps});
                std.debug.print("LIGHTONOCR3 phase_priority_yields={d} phase_fair={d}\n", .{ ctx.phase_priority_yields, @intFromBool(ctx.phase_fair) });
                const auxiliary = ctx.features.auxiliaryAmounts();
                std.debug.print("LIGHTONOCR3 epoch_auxiliary={d} auxiliary_acquisitions={d} auxiliary_host_scratch={d} auxiliary_backend_scratch={d}\n", .{ @intFromBool(ctx.features.epoch_auxiliary), ctx.auxiliary_acquisitions, auxiliary.host_scratch_bytes, auxiliary.backend_scratch_bytes });
                std.debug.print("LIGHTONOCR3 phase_gate_wait_ns={d}\n", .{ctx.phase_wait_ns});
                const s = ctx.coordinator.stats;
                if (ctx.cb) |cb| {
                    const t = cb.debugTimingSnapshot().provider;
                    const pool_stats = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnActivationPoolStats(&cb) else @as([4]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 activation_pool_hits={d}\nLIGHTONOCR3 activation_pool_peak_bytes={d}\nLIGHTONOCR3 activation_pool_duplicate_retirements={d}\n", .{ pool_stats[0], pool_stats[1], pool_stats[3] });
                    const prefill_pool_stats = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillActivationPoolStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 prefill_activation_pool={d} prefill_activation_pool_hits={d} prefill_activation_pool_hit_bytes={d}\n", .{ @intFromBool(ctx.features.prefill_activation_pool), prefill_pool_stats[0], prefill_pool_stats[1] });
                    const prepared_blocks = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPreparedBlockCount(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prepared_blocks={d}\n", .{prepared_blocks});
                    const wide_tiles = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideProjectionTileCount(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 wide_projection_tiles={d}\n", .{wide_tiles});
                    const prefill_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 prefill_pack_calls={d} prefill_pack_reuse_hits={d}\n", .{ prefill_pack[0], prefill_pack[1] });
                    const decode_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnDecodePackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 decode_pack_calls={d} decode_pack_reuse_hits={d}\n", .{ decode_pack[0], decode_pack[1] });
                    const vision_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnVisionPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_pack_calls={d} vision_pack_reuse_hits={d}\n", .{ vision_pack[0], vision_pack[1] });
                    const mpp_encoder = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnMppEncoderStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 mpp_planned_calls={d} mpp_dedicated_calls={d}\n", .{ mpp_encoder[0], mpp_encoder[1] });
                    const attention_batch = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBatchAttentionStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 attention_batch_calls={d} attention_batch_rows={d}\n", .{ attention_batch[0], attention_batch[1] });
                    std.debug.print("LIGHTONOCR3 attention_batch_kernel_calls={d}\n", .{attention_batch[2]});
                    const stream_splits = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStreamSplits(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 stream_forward_splits={d} stream_first_layers={d}\n", .{ stream_splits, ctx.features.stream_first_layers });
                    const prefill_splits = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillStreamSplits(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prefill_stream_layers={d} prefill_stream_splits={d}\n", .{ ctx.features.prefill_stream_layers, prefill_splits });
                    const prefill_ffn_calls = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillFfnCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prefill_fused_ffn_calls={d}\n", .{prefill_ffn_calls});
                    const core_weights = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnSharedCoreWeightStats(&cb) else @as([3]usize, @splat(0));
                    std.debug.print("LIGHTONOCR3 shared_core_weight_hits={d} shared_core_weight_misses={d} shared_core_weight_bytes={d}\n", .{ core_weights[0], core_weights[1], core_weights[2] });
                    const wide_mpp = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideMppStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 wide_mpp_projections={d}\nLIGHTONOCR3 wide_mpp_heads={d}\n", .{ wide_mpp[0], wide_mpp[1] });
                    const wide_gate_up = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideGateUpCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 wide_gate_up_calls={d}\n", .{wide_gate_up});
                    const full64_gate_up = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnFull64GateUpCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 full64_gate_up_calls={d}\n", .{full64_gate_up});
                    const short_prefill = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnShortPrefillStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 short_prefill_core_dispatches={d} short_direct_attention_dispatches={d}\n", .{ short_prefill[0], short_prefill[1] });
                    const wide_recurrent = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideRecurrentStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 wide_recurrent_dispatches={d} wide_recurrent_lifetime_max_rows={d} wide_recurrent_staged_bytes={d}\n", .{ wide_recurrent[0], wide_recurrent[1], wide_recurrent[2] });
                    const batch_argmax = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBatchedArgmaxStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 batch_argmax_calls={d}\nLIGHTONOCR3 batch_argmax_rows={d}\n", .{ batch_argmax[0], batch_argmax[1] });
                    const vision_workspace = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnVisionWorkspaceStats(&cb) else @as([3]u64, @splat(0));
                    const large_vision_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnLargeVisionPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_workspace_cache={d}\nLIGHTONOCR3 vision_workspace_allocations={d}\nLIGHTONOCR3 vision_workspace_reuses={d}\nLIGHTONOCR3 vision_workspace_bytes={d}\n", .{ @intFromBool(ctx.features.vision_workspace_cache), vision_workspace[0], vision_workspace[1], vision_workspace[2] });
                    std.debug.print("LIGHTONOCR3 large_vision_pack_calls={d}\nLIGHTONOCR3 large_vision_pack_reuse_hits={d}\n", .{ large_vision_pack[0], large_vision_pack[1] });
                    std.debug.print("LIGHTONOCR3 resident_vision_budget={d}\n", .{@intFromBool(ctx.features.resident_vision_budget)});
                    std.debug.print("LIGHTONOCR3 vision_late_seal={d}\nLIGHTONOCR3 prefill_late_seal={d}\nLIGHTONOCR3 queued_phase_priority={d}\n", .{ @intFromBool(ctx.features.vision_late_seal), @intFromBool(ctx.features.prefill_late_seal), @intFromBool(ctx.features.queued_phase_priority) });
                    const standalone_heads = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStandaloneWideHeads(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 standalone_wide_heads={d}\n", .{standalone_heads});
                    var burst = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBurstStats(&cb) else @as([5]u64, @splat(0));
                    if (comptime build_options.enable_cuda) if (cb.kind() == .cuda) {
                        burst = @import("../ops/cuda/cuda_compute.zig").CudaCompute.lightOnCudaBurstStats(&cb);
                    };
                    std.debug.print("LIGHTONOCR3 gpu_token_burst_enabled={d} gpu_token_burst_steps={d} gpu_token_burst_topology_mismatches={d}\n", .{ @intFromBool(ctx.features.gpu_token_burst), burst[0], burst[1] });
                    std.debug.print("LIGHTONOCR3 gpu_token_burst_extra_steps={d} gpu_token_burst_extra_rows={d} gpu_token_burst_max_width={d}\n", .{ burst[2], burst[3], burst[4] });
                    const single_frame = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnSingleFrameStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 single_frame_steps={d} single_frame_topology_mismatches={d}\n", .{ single_frame[0], single_frame[1] });
                    const greedy_stream = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnGpuGreedyStreamStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 gpu_greedy_stream_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_two_frame_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_topology_mismatches={d}\n", .{ greedy_stream[0], greedy_stream[1], greedy_stream[2] });
                    const stream_frames: [2]u64 = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStreamFrameStats(&cb) else @splat(0);
                    std.debug.print("LIGHTONOCR3 gpu_greedy_stream_multi_frame_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_frame_submits={d}\n", .{ stream_frames[0], stream_frames[1] });
                    const token_scopes = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnTokenEncoderScopeSteps(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 token_encoder_scope_steps={d}\n", .{token_scopes});
                    const encoder_scopes = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.ocrEncoderScopeOpens(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_encoder_scope_opens={d} prefill_encoder_scope_opens={d}\n", .{ encoder_scopes[0], encoder_scopes[1] });
                    const snapshots = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnFramePolicySnapshots(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 frame_policy_snapshots={d}\n", .{snapshots});
                    std.debug.print("LIGHTONOCR3 runtime_frames={d} frame_gpu_ns={d} frame_wait_ns={d}\n", .{ t.decoder_runtime_frame_submits, t.decoder_runtime_frame_gpu_nanos, t.decoder_runtime_frame_wait_nanos });
                }
                std.debug.print("LIGHTONOCR3 execution_gate_wait_ns={d}\n", .{ctx.execution_gate_wait_ns});
                std.debug.print("LIGHTONOCR3 kv_storage_layers={d} model_layers={d}\n", .{ ctx.cfg.kvStorageLayerCount(), ctx.cfg.num_hidden_layers });
                std.debug.print("LIGHTONOCR3 registered_window_peak={d} physical_rows_limit={d}\n", .{ ctx.registered_window_peak, ctx.coordinator.policy.max_step_items });
                std.debug.print("LIGHTONOCR3 cohort_items={d} decode_steps={d} batched_steps={d} singleton_steps={d} decode_items={d}\n", .{ ctx.completed, s.step_batches_total, s.step_batch_size_2_total + s.step_batch_size_3_4_total + s.step_batch_size_5_8_total, s.step_singleton_batches_total, s.step_decode_items_total });
                std.debug.print("LIGHTONOCR3 turn_coalescing={d} coalesce_turn_boundaries={d} coalesce_rebased_items={d} coalesce_waits={d} coalesce_wait_us={d} kv_block_skips={d}\n", .{ @intFromBool(ctx.coordinator.policy.decode_coalesce_after_turn), s.decode_coalesce_turn_boundaries_total, s.decode_coalesce_rebased_items_total, s.decode_coalesce_waits_total, s.decode_coalesce_wait_us_total, s.step_kv_block_skips_total });
            }
            ctx.vision_broker.deinit();
            ctx.prefill_broker.deinit();
            // cleanup_owner was moved above: use its ACTUAL local current B,
            // not ctx's now-empty getter. Destroy captured addresses BEFORE KV
            // teardown invokes strict raw recycle callbacks.
            if (comptime build_options.enable_cuda) if (ctx.cb) |*cb| if (cb.kind() == .cuda) {
                @import("../ops/cuda/cuda_compute.zig").CudaCompute.invalidateLightOnDecodeGraphWithCleanup(cb, .{ .boundary = cleanup_epoch, .operation = cb.operationToken() }) catch platform.inference_process_supervisor.restartWorker();
            };
            if (ctx.cb != null) ctx.storage.deinitWithBoundary(cleanup_epoch);
            ctx.manager.deinit();
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| if (ctx.cb) |*cb| {
                if (ctx.captured_roots) |*roots| {
                    roots.unbind(cb, norms.expectedCudaCompute()) catch platform.inference_process_supervisor.restartWorker();
                    roots.finishAfterGraph(cb, norms.expectedCudaCompute(), .{ .boundary = cleanup_epoch }) catch platform.inference_process_supervisor.restartWorker();
                    ctx.captured_roots = null;
                }
            };
            if (ctx.cb) |*cb| cb.deinitWithCleanup(.{ .boundary = cleanup_epoch });
            // The worker has joined and the backend has released plans and
            // cached activations. Admission waiters may have kept this epoch
            // alive after its last admitted page retired; retain its own lease
            // through that interval, independently of every page's lease.
            ctx.coordinator.deinit();
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_vision_peak_bytes={d} peak_segments={d}\n", .{ ctx.vision_peak.bytes, ctx.vision_peak.count });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_vision_host_peak_bytes={d} peak_segments={d}\n", .{ ctx.vision_host_peak.bytes, ctx.vision_host_peak.count });
            const decoder_bytes = ctx.decoder_peak.bytes;
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_decoder_scratch={d} shared_decoder_peak_bytes={d} peak_segments={d}\n", .{ @intFromBool(ctx.features.shared_decoder_scratch), decoder_bytes, ctx.decoder_peak.count });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 prefill_host_scratch_acquisitions={d} prefill_host_scratch_releases={d} prefill_host_scratch_peak_bytes={d}\n", .{ ctx.prefill_host_scratch_acquisitions, ctx.prefill_host_scratch_releases, ctx.prefill_host_scratch_peak_bytes });
            // SDK/cache cleanup is complete. Release BOTH actual outer lanes
            // before external admission callbacks or current-epoch retirement.
            ctx.leaveSessionCohort();
            ctx.model.unlockNativeGeneration();
            ctx.lane.release();
            if (comptime build_options.enable_metal) if (ctx.metal_park) |*park| park.deinit();
            if (ctx.custody_metadata_lease) |*lease| {
                lease.release();
                ctx.custody_metadata_lease = null;
            }
            if (ctx.final_page_admission) |*lease| {
                lease.release();
                ctx.final_page_admission = null;
            }
            if (ctx.creator_admission) |*lease| {
                lease.release();
                ctx.creator_admission = null;
            }
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                norms.finishAfterUnlock(cleanup_epoch);
                ctx.norm_preparation = null;
            };
            // Physical storage was destroyed under the CURRENT lifetime epoch;
            // the target pin/quota wrapper retires only after both real lanes.
            if (ctx.kv_bank) |*bank| {
                if (ctx.bank_adopted) bank.finishAfterUnlock(cleanup_epoch.?) else bank.abortOutsideLanes(cleanup_epoch.?);
                ctx.kv_bank = null;
            }
            if (ctx.auxiliary_lease) |*auxiliary| {
                auxiliary.release();
                ctx.auxiliary_lease = null;
                if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 auxiliary_release_after_backend=1\n", .{});
            }
            ctx.vision_peak.deinit();
            ctx.vision_host_peak.deinit();
            ctx.decoder_peak.deinit();
            if (decoder_bytes != 0 and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 decoder_scratch_release_after_backend=1\n", .{});
            cleanup.finishAfterUnlock();
            if (comptime build_options.enable_cuda) {
                // Roots died under current B above. Only inert unused cold
                // metadata remains; release it after every actual gate left.
                for (ctx.captured_root_slots) |*slot| if (slot.upgrade) |*upgrade| upgrade.deinitOutsideExecution();
                ctx.allocator.free(ctx.captured_root_slots);
            }
            ctx.allocator.destroy(ctx);
        }
        pub fn acquireVisionPeak(factory: *Factory, bytes: usize) !model_manager_mod.GenerationAdmissionLease {
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = .{ .backend_scratch_bytes = bytes } } }});
        }
        pub fn acquireVisionHostPeak(factory: *Factory, bytes: usize) !model_manager_mod.GenerationAdmissionLease {
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = .{ .host_scratch_bytes = bytes } } }});
        }
        pub fn finishPhaseWait(ctx: *LightOnStreamContext) void {
            if (ctx.phase_waiters.fetchSub(1, .acq_rel) == 1) if (ctx.worker_io) |*owner| {
                ctx.worker_wake.set(owner.io());
            };
        }
        // Keep admitted, mergeable broker work visible to the decode worker.
        // A prefill that requires decode progress must never park that worker.
        pub fn beginQueuedPhase(ctx: *LightOnStreamContext, vision: bool) bool {
            const enabled = ctx.phase_fair and ctx.features.queued_phase_priority and (if (vision)
                ctx.features.vision_late_seal
            else
                ctx.features.prefill_late_seal and ctx.features.prefill_decode_streak == 0);
            if (enabled) _ = ctx.phase_waiters.fetchAdd(1, .acq_rel);
            return enabled;
        }
        /// A cancelled receiver still owns a completed device result. Retire
        /// it under the same lane as its producer, before the page/epoch dies.
        pub fn retireProjected(ctx: *LightOnStreamContext, projected: *qwen3vl_projector.ProjectedImages) void {
            if (projected.device_embeddings != null) {
                ctx.lockStepForCleanup();
                defer ctx.unlockStep();
                projected.deinit();
            } else projected.deinit();
        }

        fn pageAccess(ctx: *LightOnStreamContext, control: InferenceExecutionControl, priority: bool) execution_control_mod.ExecutionAccess {
            return .{
                .mutex = &ctx.step_mutex,
                .gate = if (ctx.gate_events) &ctx.step_gate else null,
                .priority = priority,
                .pause = &ctx.execution_pause,
                .control = control,
                .io = control.io orelse if (ctx.worker_io) |*owner| owner.io() else null,
                .contention_wait_ns = ctx.phase_wait_ns,
                .phase_waiters = if (ctx.phase_fair) &ctx.phase_waiters else null,
                .phase_wake = &ctx.worker_wake,
                .phase_wake_io = if (ctx.worker_io) |*owner| owner.io() else null,
            };
        }
        /// Stack-local diagnostic interval. Start owns the caller's actual
        /// step; finish rejoins that SAME epoch before its PAGE/pool can retire.
        pub const CudaRequestProfile = struct {
            before: ?session_factory.CudaRuntimeStats = null,

            pub fn finish(self: *CudaRequestProfile, ctx: *LightOnStreamContext, complete: bool) void {
                if (comptime build_options.enable_cuda) {
                    const before = self.before orelse return;
                    self.before = null;
                    ctx.lockStepForCleanup();
                    defer ctx.unlockStep();
                    const cb = if (ctx.cb) |*value| value else {
                        std.log.warn("cuda_read_profile: status=unavailable reason=MissingReaderBackend", .{});
                        return;
                    };
                    const after = @import("../ops/cuda/cuda_compute.zig").CudaCompute.drainProfileSnapshot(cb, .{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() }) catch |err| {
                        std.log.warn("cuda_read_profile: status=unavailable reason={s} request_outcome={s}", .{ @errorName(err), if (complete) "complete" else "partial" });
                        return;
                    };
                    // Ring-full automatic drains may have failed BEFORE the
                    // successful final drain. Never publish their zeros as time.
                    if (after.profile_collection_failures == std.math.maxInt(usize) or after.profile_collection_failures != before.profile_collection_failures) {
                        std.log.warn("cuda_read_profile: status=unavailable reason=CudaProfileCollectionIncomplete request_outcome={s}", .{if (complete) "complete" else "partial"});
                        return;
                    }
                    // A peer may have shared the resident epoch during a yield.
                    // Never describe those aggregate counters as one request.
                    platform.sync.lockYielding(&ctx.controls_mutex);
                    const exclusive = ctx.registered_window_peak == 1;
                    ctx.controls_mutex.unlock();
                    const delta = session_factory.cudaStatsDelta(after, before);
                    var lines: [1536]u8 = undefined;
                    const decode = session_factory.formatCudaDecodeProfileLine(lines[0..768], delta);
                    const prefill = session_factory.formatCudaPrefillProfileLine(lines[768..], delta);
                    // One log record binds BOTH category sets to this scope/id;
                    // another model's epoch cannot interleave their provenance.
                    std.log.info("cuda_read_profile: scope={s} status=observed request_outcome={s} execution_id={d} {s} {s}", .{ if (exclusive) "request" else "shared_epoch_interval", if (complete) "complete" else "partial", ctx.execution_id, decode, prefill });
                }
            }
        };

        /// Called only with the initialized resident backend and step already
        /// owned. Draining first excludes cold preparation and older references.
        pub fn beginCudaRequestProfile(ctx: *LightOnStreamContext) !CudaRequestProfile {
            if (comptime build_options.enable_cuda) {
                if (!session_factory.cudaOpProfileLoggingEnabled()) return .{};
                const cb = if (ctx.cb) |*value| value else return error.MissingReaderBackend;
                if (cb.kind() != .cuda) return .{};
                return .{ .before = try @import("../ops/cuda/cuda_compute.zig").CudaCompute.drainProfileSnapshot(cb, .{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() }) };
            }
            return .{};
        }

        pub fn lockStep(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !void {
            try ctx.pageAccess(control, true).acquire();
            ctx.invalidateCudaDecodeGraph();
        }
        // Private park/restore owner only: it already owns the pause reservation.
        // This method neither invalidates the graph nor waits on its OWN pause.
        fn lockAdmissionRaw(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !void {
            var access = ctx.pageAccess(control, true);
            access.pause = null;
            try access.acquire();
        }
        fn lockAdmissionForCleanupRaw(ctx: *LightOnStreamContext) void {
            var access = ctx.pageAccess(.{}, true);
            access.pause = null;
            access.acquireForCleanup();
        }
        fn invalidateCudaDecodeGraph(ctx: *LightOnStreamContext) void {
            // Vision and other foreground phases may resize shared CUDA
            // scratch. Retire captured addresses before that phase executes.
            if (comptime build_options.enable_cuda) if (ctx.cb) |*cb| if (cb.kind() == .cuda) {
                @import("../ops/cuda/cuda_compute.zig").CudaCompute.invalidateLightOnDecodeGraphWithCleanup(cb, .{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() }) catch platform.inference_process_supervisor.restartWorker();
            };
        }
        pub fn lockStepForCleanup(ctx: *LightOnStreamContext) void {
            ctx.pageAccess(.{}, true).acquireForCleanup();
            ctx.invalidateCudaDecodeGraph();
        }
        pub fn unlockStep(ctx: *LightOnStreamContext) void {
            ctx.step_mutex.unlock();
            if (ctx.gate_events) (execution_control_mod.ExecutionLane.Lease{ .lane = &ctx.step_gate }).release();
        }
        pub const AdmissionPark = struct {
            ctx: *LightOnStreamContext,
            return_with_step: bool = false,
            cuda: if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute.ReaderEpochPark else void = if (build_options.enable_cuda) .{} else {},
            active: bool = false,

            fn dropRestoreLanes(self: *AdmissionPark) void {
                const ctx = self.ctx;
                ctx.unlockStep();
                ctx.leaveSessionCohort();
                ctx.model.unlockNativeGeneration();
                ctx.lane.release();
            }
            /// The synchronous pre-graph hook borrowed an ALREADY-owned step.
            /// Return with that SAME mutex/FIFO lease held, all backend custody
            /// restored and fresh current B published, avoiding a gap before math.
            pub fn restoreKeepingStep(self: *AdmissionPark) !void {
                if (!self.return_with_step) return error.ReaderStepNotBorrowed;
                try self.restore();
            }
            /// Numerical restoration cannot be canceled: old admitted pages
            /// own this worker/backend independently of the requesting page.
            fn restore(self: *AdmissionPark) !void {
                if (!self.active) return;
                const ctx = self.ctx;
                const control = InferenceExecutionControl{}; // cleanup polling must not inherit canceled worker Io
                const cleanup_io = if (ctx.worker_io) |*owner| owner.io() else null;
                ctx.admission_window.closeOutside();
                while (true) {
                    ctx.lane = ctx.model.native_reader_execution_lane.acquireForCleanup(cleanup_io, false);
                    try control.lock(ctx.model.nativeGenerationMutex());
                    try ctx.enterSessionCohort(control);
                    ctx.lockAdmissionForCleanupRaw();
                    var retry = false;
                    if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                        if (ctx.cb) |*cb| try self.cuda.prepareRestore(cb, norms.expectedCudaCompute());
                        const Resume = struct {
                            fn run(owner: *LightOnStreamContext, preparation: *NormPreparation) !void {
                                try owner.validateNormsAndLegacyQuotas(preparation);
                                try preparation.resumeRequestForCohort(owner.model.session, &owner.cohort_waiter, if (owner.cb) |*cb| cb else null, owner.cleanup_owner.epoch() orelse return error.MissingCurrentCleanupEpoch);
                            }
                        };
                        Resume.run(ctx, norms) catch |err| {
                            if (ctx.cb != null) self.cuda.suspendRestoreBinding(norms.expectedCudaCompute());
                            // Only an exact pre-submit metadata race retries.
                            // Actual SDK and authority failures remain fatal.
                            if (err != error.PreparedCachePlanChanged) return err;
                            retry = true;
                        };
                        if (!retry) if (ctx.cb) |*cb| try self.cuda.restore(cb, norms.expectedCudaCompute());
                    };
                    if (retry) {
                        self.dropRestoreLanes();
                        ctx.admission_window.publishOutside();
                        try ctx.refreshNormFundingOutside(control);
                        ctx.admission_window.closeOutside();
                        continue;
                    }
                    if (comptime build_options.enable_metal) if (ctx.metal_park) |*park| if (park.compute != null) {
                        try park.restore(&ctx.cb.?, .{ .boundary = ctx.cleanup_owner.epoch() });
                    };
                    // Every actual backend/graph/operation is restored under
                    // the current B before a queued foreground/worker can enter.
                    ctx.execution_pause.resumeWithStepLocked();
                    self.active = false;
                    ctx.admission_window.finishRestored();
                    if (!self.return_with_step) ctx.unlockStep();
                    return;
                }
            }
        };

        fn prepareLegacyQuotasOutside(ctx: *LightOnStreamContext, norms: *NormPreparation) !void {
            if (comptime build_options.enable_cuda) if (ctx.captured_roots) |*roots| {
                const model = ctx.model.session.model_lifetime orelse return error.MissingCacheRetirementModel;
                try roots.prepareLegacyQuotasOutside(norms.expectedCudaCompute(), model);
            };
        }
        fn validateNormsAndLegacyQuotas(ctx: *LightOnStreamContext, norms: *NormPreparation) !void {
            if (comptime build_options.enable_cuda) {
                try norms.validateForCohort(ctx.model.session, &ctx.cohort_waiter);
                if (ctx.captured_roots) |*roots| try roots.commitLegacyQuotas(norms.expectedCudaCompute());
            }
        }
        fn refreshNormFundingOutside(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !void {
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                norms.finishFundingOutsideExecution();
                norms.prepareFundingOutsideExecution(control) catch |err| switch (err) {
                    // PAGE/old admitted pages already own ordinary run floors.
                    // Existing compatibility funding declines only optional
                    // cache misses, with exact zero retention; no late supplier.
                    error.OutOfMemory, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => try norms.recoverOptionalFundingOutsideExecution(),
                    else => return err,
                };
                try ctx.prepareLegacyQuotasOutside(norms);
            };
        }

        /// Own one metadata admission transaction, pause every actual step
        /// entry, then release provider→Session→native→reader→step exclusion.
        /// Supplier callbacks run only after this function returns successfully.
        fn parkForAdmission(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !AdmissionPark {
            return ctx.parkPolicy(control, false);
        }
        /// ONLY the synchronous Executor pre-graph hook may call this: its
        /// actual ExecutionAccess already owns step/FIFO. A busy outside owner
        /// returns BEFORE pause/custody changes; caller keeps its original step
        /// and may precommit-decline graph instead of waiting while holding it.
        pub fn parkWithOwnedStep(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !AdmissionPark {
            return ctx.parkPolicy(control, true);
        }
        fn parkPolicy(ctx: *LightOnStreamContext, control: InferenceExecutionControl, comptime already_owned_step: bool) !AdmissionPark {
            try control.check();
            if (!ctx.admission_window.reserve()) return error.ResourceTemporarilyUnavailable;
            var serial_owned = true;
            errdefer if (serial_owned) ctx.admission_window.abandonUnpublished();
            if (!already_owned_step) try ctx.lockAdmissionRaw(control);
            var step_owned = true;
            // Before transfer, failures preserve the hook caller's original
            // step lease; ordinary PAGE acquisition releases its own new one.
            errdefer if (step_owned and !already_owned_step) ctx.unlockStep();
            try ctx.execution_pause.pauseWithStepLocked();
            var pause_owned = true;
            errdefer if (pause_owned) ctx.execution_pause.resumeWithStepLocked();
            var parked = AdmissionPark{ .ctx = ctx, .return_with_step = already_owned_step };
            if (ctx.cb) |*cb| switch (cb.kind()) {
                .cuda => if (comptime build_options.enable_cuda) {
                    const norms = ctx.norm_preparation orelse return error.UnsupportedReaderEpochBackend;
                    const roots = if (ctx.captured_roots) |*value| value else return error.UnsupportedReaderEpochBackend;
                    norms.unbind(cb);
                    parked.cuda.park(cb, norms.expectedCudaCompute(), roots) catch |err| {
                        // Failure has not moved backend custody. Prove the
                        // old stream idle before returning it to old pages.
                        norms.expectedCudaCompute().ctx.synchronize() catch platform.inference_process_supervisor.restartWorker();
                        norms.beginRequest(ctx.cleanup_owner.epoch().?) catch platform.inference_process_supervisor.restartWorker();
                        norms.bind(cb, ctx.cleanup_owner.epoch().?) catch platform.inference_process_supervisor.restartWorker();
                        return err;
                    };
                } else return error.CudaNotEnabled,
                .metal => if (comptime build_options.enable_metal) {
                    const park = if (ctx.metal_park) |*value| value else return error.UnsupportedReaderEpochBackend;
                    try park.park(cb, .{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() });
                } else return error.MetalNotEnabled,
                else => return error.UnsupportedReaderEpochBackend,
            };
            ctx.leaveSessionCohort();
            ctx.model.unlockNativeGeneration();
            ctx.lane.release();
            ctx.unlockStep();
            step_owned = false;
            pause_owned = false;
            parked.active = true;
            ctx.admission_window.publishOutside();
            serial_owned = false; // transferred to the returned metadata owner
            ctx.cleanup_owner.rotateAfterUnlock() catch |err| {
                parked.restore() catch platform.inference_process_supervisor.restartWorker();
                return err;
            };
            // Recovery is temporary. Every later outside opportunity retries
            // normal cache preparation instead of preserving a disabled policy.
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| if (norms.optionalRecoveryActive()) {
                ctx.refreshNormFundingOutside(control) catch |err| {
                    parked.restore() catch platform.inference_process_supervisor.restartWorker();
                    return err;
                };
            };
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                ctx.prepareLegacyQuotasOutside(norms) catch |err| {
                    parked.restore() catch platform.inference_process_supervisor.restartWorker();
                    return err;
                };
            };
            return parked;
        }

        /// The sole production owner is runLightOnContinuousPage, whose caller
        /// retains a LoadedModel handle and owns no foreign runtime/model stage.
        /// This value owns the raw PAGE quota. An origin witness is only a pure
        /// seal attestation; it is NEVER an SDK cleanup-B selector.
        pub const PageLease = struct {
            const State = enum { empty, active, retired };
            ctx: ?*LightOnStreamContext,
            owner_id: usize,
            execution_id: u64,
            quota: model_manager_mod.GenerationAdmissionLease,
            state: State = .empty,
            origin: ?@import("../execution/retirement_sink.zig").SealWitness = null,

            pub fn beginState(self: *PageLease) void {
                std.debug.assert(self.ctx != null and self.state == .empty);
                self.state = .active;
            }
            /// Called AFTER actual state.deinit, before its existing step unlock.
            /// A foreign/stale B changes neither the PAGE state nor origin. The
            /// exact current B is supplied NOW; no creation authority is reused.
            pub fn noteStateRetired(self: *PageLease, current: @import("../execution/cleanup_context.zig").Context) !void {
                const ctx = self.ctx orelse return error.PageLeaseConsumed;
                if (self.state == .retired) return error.PageAlreadyRetired;
                const actual = ctx.cleanup_owner.epoch() orelse return error.MissingCurrentCleanupEpoch;
                const supplied = current.boundary orelse return error.MissingCurrentCleanupEpoch;
                if (current.outside_execution or supplied.record != actual.record or supplied.nonce != actual.nonce) return error.ForeignPageCleanupEpoch;
                const scheduler = ctx.model.session.cohort_scheduler orelse return error.MissingExecutionCohortScheduler;
                const gate = ctx.model.session.execution_gate orelse return error.MissingExecutionCohortGate;
                if (!ctx.cohort_owned or !scheduler.ownsAcquired(&ctx.cohort_waiter, gate)) return error.ExecutionCohortNotOwned;
                if (ctx.cb) |*cb| {
                    if (cb.operationToken() != null) _ = try current.operationFor(cb);
                }
                self.origin = try actual.sealWitness();
                self.state = .retired;
            }
            fn finishEmpty(self: *PageLease) void {
                if (self.state != .empty) return;
                const ctx = self.ctx.?;
                // Early preparation failure created no per-page state. Still
                // make the retirement transition under the actual current scope.
                ctx.pageAccess(.{}, true).acquireForCleanup();
                defer ctx.unlockStep();
                self.noteStateRetired(.{ .boundary = ctx.cleanup_owner.epoch(), .operation = if (ctx.cb) |*cb| cb.operationToken() else null }) catch platform.inference_process_supervisor.restartWorker();
            }
            fn originSealed(self: *const PageLease, ctx: *const LightOnStreamContext) !bool {
                if (self.owner_id != @intFromPtr(ctx) or self.execution_id != ctx.execution_id or self.state != .retired) return error.ForeignPageRetirement;
                const origin = if (self.origin) |*value| value else return error.MissingPageRetirementOrigin;
                return origin.wasSealed();
            }
            fn finishOrigin(self: *PageLease) void {
                if (self.origin) |*value| value.release();
                self.origin = null;
            }
            pub fn release(self: *PageLease, pool_lease: *reader_execution_pool.Pool.Lease) void {
                const ctx = self.ctx orelse return;
                self.finishEmpty();
                if (self.state != .retired or pool_lease.context != @as(*anyopaque, @ptrCast(ctx))) platform.inference_process_supervisor.restartWorker();
                self.ctx = null; // consume before any reentrant quota callback
                if (pool_lease.depart(&self.quota, adoptFinalPageAdmission)) |retirement| {
                    var owned = retirement;
                    while (true) {
                        // This PAGE's own real state retirement must precede
                        // a join; another thread's outside flag is insufficient.
                        if (self.originSealed(ctx) catch platform.inference_process_supervisor.restartWorker()) {
                            if (ctx.admission_window.borrowOutside() catch platform.inference_process_supervisor.restartWorker()) |borrow| {
                                self.quota.release();
                                self.quota = .{};
                                self.finishOrigin();
                                borrow.finish();
                                owned.finish(); // may destroy ctx
                                return;
                            }
                        }
                        var parked = ctx.parkForAdmission(.{}) catch |err| {
                            if (err != error.ResourceTemporarilyUnavailable) platform.inference_process_supervisor.restartWorker();
                            platform.time.sleepNs(100 * std.time.ns_per_us);
                            continue;
                        };
                        if (!(self.originSealed(ctx) catch platform.inference_process_supervisor.restartWorker())) platform.inference_process_supervisor.restartWorker();
                        self.quota.release();
                        self.quota = .{};
                        self.finishOrigin();
                        parked.restore() catch platform.inference_process_supervisor.restartWorker();
                        owned.finish();
                        return;
                    }
                } else {
                    // Context already died, or owns the moved final quota.
                    // Only pure origin metadata may outlive that destruction.
                    self.quota.release();
                    self.quota = .{};
                    self.finishOrigin();
                }
            }
        };
        pub fn bindPageLease(ctx: *LightOnStreamContext, quota: model_manager_mod.GenerationAdmissionLease) PageLease {
            return .{ .ctx = ctx, .owner_id = @intFromPtr(ctx), .execution_id = ctx.execution_id, .quota = quota };
        }
        fn adoptFinalPageAdmission(raw: *anyopaque, owned: *model_manager_mod.GenerationAdmissionLease) void {
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            std.debug.assert(ctx.final_page_admission == null);
            ctx.final_page_admission = owned.*;
            owned.* = .{};
        }

        pub fn admitPage(ctx: *LightOnStreamContext, factory: *Factory, peak: ?usize, host_peak: usize, amounts: runtime.tier.memory.AdmissionAmounts) !model_manager_mod.GenerationAdmissionLease {
            return ctx.admitPageInternal(factory, null, peak, host_peak, amounts, false);
        }
        pub fn admitRegisteredPage(ctx: *LightOnStreamContext, factory: *Factory, index: usize, peak: ?usize, host_peak: usize, amounts: runtime.tier.memory.AdmissionAmounts) !model_manager_mod.GenerationAdmissionLease {
            return ctx.admitPageInternal(factory, index, peak, host_peak, amounts, false);
        }
        fn acquirePageOutside(ctx: *LightOnStreamContext, factory: *Factory, amounts: runtime.tier.memory.AdmissionAmounts, comptime unpublished: bool) !model_manager_mod.GenerationAdmissionLease {
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = amounts } }}) catch |err| {
                if (unpublished or (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable)) return err;
                if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                    if (try norms.releaseUnusedFundingForPageOutside()) {
                        // SAME PAGE amounts, exactly one retry, no math replay.
                        return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = amounts } }});
                    }
                };
                return err;
            };
        }
        fn admitPageInternal(ctx: *LightOnStreamContext, factory: *Factory, index: ?usize, peak: ?usize, host_peak: usize, amounts: runtime.tier.memory.AdmissionAmounts, comptime unpublished: bool) !model_manager_mod.GenerationAdmissionLease {
            if (unpublished) std.debug.assert(ctx.cb == null and ctx.worker == null and ctx.registered_pages == 0);
            var parked: ?AdmissionPark = if (!unpublished) try ctx.parkForAdmission(factory.control) else null;
            defer if (parked) |*owner| owner.restore() catch platform.inference_process_supervisor.restartWorker();
            // Resource-bearing choices are immutable for a shared backend.
            if (!std.meta.eql(ctx.features, factory.features) or ctx.cfg.compact_qwen35_kv != factory.cfg.compact_qwen35_kv or ctx.cfg.qwen35_device_only_state != factory.cfg.qwen35_device_only_state) return error.InvalidBatchDecodeState;
            const needs_auxiliary = !ctx.auxiliary_ready.load(.acquire);
            const needs_peak = if (peak) |bytes| ctx.phase_wait_ns == 0 or bytes > ctx.published_vision_peak.load(.acquire) else false;
            const needs_host_peak = host_peak > 0 and (ctx.phase_wait_ns == 0 or host_peak > ctx.published_vision_host_peak.load(.acquire));
            const needs_decoder_peak = factory.decoder_workspace != null or (factory.decoder_peak != 0 and (ctx.phase_wait_ns == 0 or factory.decoder_peak > ctx.published_decoder_peak.load(.acquire)));
            if (needs_auxiliary or needs_peak or needs_host_peak or needs_decoder_peak or factory.dynamic_vision_batch_items != 0) {
                const vision_mark = if (peak != null) ctx.vision_peak.mark() else null;
                const host_mark = if (host_peak != 0) ctx.vision_host_peak.mark() else null;
                const decoder_mark = if (factory.decoder_peak != 0 or factory.decoder_workspace != null) ctx.decoder_peak.mark() else null;
                const old_workspace = if (factory.decoder_workspace != null) ctx.decoder_workspace else null;
                const old_vision = if (factory.dynamic_vision_batch_items != 0) ctx.vision_per_item_peak else 0;
                const old_host = if (factory.dynamic_vision_batch_items != 0) ctx.vision_host_per_item_peak else 0;
                var committed = false;
                defer if (!committed) {
                    // No GPU consumer can use these increments before this
                    // gate returns. A denied page must not strand shared leases
                    // that keep all pending pages from ever entering the epoch.
                    if (vision_mark) |before| ctx.vision_peak.rollback(before);
                    if (host_mark) |before| ctx.vision_host_peak.rollback(before);
                    if (decoder_mark) |before| ctx.decoder_peak.rollback(before);
                    ctx.decoder_workspace = old_workspace;
                    ctx.vision_per_item_peak = old_vision;
                    ctx.vision_host_per_item_peak = old_host;
                    if (vision_mark != null) ctx.published_vision_peak.store(ctx.vision_peak.bytes, .release);
                    if (host_mark != null) ctx.published_vision_host_peak.store(ctx.vision_host_peak.bytes, .release);
                    if (decoder_mark != null) ctx.published_decoder_peak.store(ctx.decoder_peak.bytes, .release);
                };
                if (!ctx.auxiliary_ready.load(.acquire)) {
                    ctx.auxiliary_lease = try factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = ctx.features.auxiliaryAmounts() } }});
                    ctx.auxiliary_acquisitions += 1;
                    ctx.auxiliary_ready.store(true, .release);
                }
                const ActivePlan = struct { pages: usize, vision: usize, host: usize, decoder: ?@import("reader_workspace.zig").Decoder };
                const active: ActivePlan = blk: {
                    if (factory.dynamic_vision_batch_items == 0 and factory.decoder_workspace == null)
                        break :blk .{ .pages = @as(usize, 1), .vision = peak orelse 0, .host = host_peak, .decoder = factory.decoder_workspace };
                    platform.sync.lockYielding(&ctx.controls_mutex);
                    defer ctx.controls_mutex.unlock();
                    var plan: ActivePlan = .{ .pages = ctx.admitted_pages + 1, .vision = peak orelse 0, .host = host_peak, .decoder = factory.decoder_workspace };
                    for (ctx.admitted_controls, 0..) |admitted, slot| if (admitted) {
                        plan.vision = @max(plan.vision, ctx.admitted_vision[slot]);
                        plan.host = @max(plan.host, ctx.admitted_host[slot]);
                        if (ctx.admitted_workspace[slot]) |other| plan.decoder = if (plan.decoder) |candidate| try candidate.merge(other) else other;
                    };
                    break :blk plan;
                };
                const active_pages = active.pages;
                if (peak) |per_item| {
                    ctx.vision_per_item_peak = active.vision;
                    const bytes = if (factory.dynamic_vision_batch_items != 0)
                        try @import("reader_workspace.zig").batchPeak(ctx.vision_per_item_peak, active_pages, factory.dynamic_vision_batch_items)
                    else
                        per_item;
                    try ctx.vision_peak.grow(bytes, factory, acquireVisionPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_vision_peak.store(ctx.vision_peak.bytes, .release);
                }
                if (factory.decoder_peak != 0 or factory.decoder_workspace != null) {
                    var bytes = factory.decoder_peak;
                    if (factory.decoder_workspace) |plan| {
                        _ = plan;
                        ctx.decoder_workspace = active.decoder;
                        bytes = try ctx.decoder_workspace.?.peak(active_pages);
                    }
                    try ctx.decoder_peak.grow(bytes, factory, acquireVisionPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_decoder_peak.store(ctx.decoder_peak.bytes, .release);
                }
                if (host_peak != 0) {
                    ctx.vision_host_per_item_peak = active.host;
                    const bytes = if (factory.dynamic_vision_batch_items != 0)
                        try @import("reader_workspace.zig").batchPeak(ctx.vision_host_per_item_peak, active_pages, factory.dynamic_vision_batch_items)
                    else
                        host_peak;
                    try ctx.vision_host_peak.grow(bytes, factory, acquireVisionHostPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_vision_host_peak.store(ctx.vision_host_peak.bytes, .release);
                }
                const lease = try ctx.acquirePageOutside(factory, amounts, unpublished);
                if (index) |slot| ctx.noteAdmitted(slot, peak, host_peak, factory.decoder_workspace);
                committed = true;
                return lease;
            }
            return ctx.acquirePageOutside(factory, amounts, unpublished);
        }

        fn noteAdmitted(ctx: *LightOnStreamContext, slot: usize, peak: ?usize, host_peak: usize, workspace: ?@import("reader_workspace.zig").Decoder) void {
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            std.debug.assert(ctx.controls[slot] != null and !ctx.admitted_controls[slot]);
            ctx.admitted_controls[slot] = true;
            ctx.admitted_vision[slot] = peak orelse 0;
            ctx.admitted_host[slot] = host_peak;
            ctx.admitted_workspace[slot] = workspace;
            ctx.admitted_pages += 1;
        }

        pub fn register(ctx: *LightOnStreamContext, control: InferenceExecutionControl) usize {
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            for (&ctx.controls, 0..) |*slot, index| if (slot.* == null) {
                slot.* = control;
                ctx.registered_pages += 1;
                ctx.registered_window_peak = @max(ctx.registered_window_peak, ctx.registered_pages);
                return index;
            };
            unreachable; // Pool capacity and the control table share this bound.
        }
        pub fn unregister(ctx: *LightOnStreamContext, index: usize) void {
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            std.debug.assert(ctx.controls[index] != null and ctx.registered_pages > 0);
            ctx.controls[index] = null;
            ctx.registered_pages -= 1;
            if (ctx.admitted_controls[index]) {
                std.debug.assert(ctx.admitted_pages > 0);
                ctx.admitted_controls[index] = false;
                ctx.admitted_vision[index] = 0;
                ctx.admitted_host[index] = 0;
                ctx.admitted_workspace[index] = null;
                ctx.admitted_pages -= 1;
            }
        }
        pub fn check(raw: ?*anyopaque) !void {
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw.?));
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            var timeout = false;
            for (ctx.controls) |slot| if (slot) |control| {
                control.check() catch |err| {
                    timeout = timeout or err == error.Timeout or err == error.DeadlineExceeded;
                    continue;
                };
                return;
            };
            return if (timeout) error.Timeout else error.Cancelled;
        }
        /// Called under step_mutex before this epoch publishes its first page.
        pub fn startExecutor(ctx: *LightOnStreamContext, node: *Node, kv_dtype: runtime.kv.pool.KvDType) !void {
            if (ctx.worker != null or !ctx.features.worker) return;
            ctx.worker_io = std.Io.Threaded.init(ctx.allocator, .{});
            errdefer {
                ctx.worker_io.?.deinit();
                ctx.worker_io = null;
            }
            ctx.worker_control = node.bindExecutionControl(ctx.worker_io.?.io(), .{ .ptr = ctx, .check_fn = check });
            ctx.worker_kv_dtype = kv_dtype;
            ctx.coordinator.registerExecutor(&ctx.worker_wake, ctx.worker_io.?.io());
            errdefer ctx.coordinator.unregisterExecutor();
            ctx.worker = try std.Thread.spawn(.{}, runExecutor, .{ctx});
        }

        /// Actual PAGE already owns the CPU image working set. The exact
        /// prepared/resize-plan upload value can add optional pinned backing
        /// only through the SAME all-lane park/funding/current-B restore owner.
        pub fn pinnedInputUploadCapacity() usize {
            if (comptime build_options.enable_cuda) return NormPreparation.pinned_upload_capacity;
            return 0;
        }
        pub fn preparePinnedInputUpload(ctx: *LightOnStreamContext, max_upload_bytes: usize, control: InferenceExecutionControl) !void {
            if (comptime !build_options.enable_cuda) return;
            const norms = ctx.norm_preparation orelse return;
            try ctx.pageAccess(control, true).acquire(); // admission access never invalidates a live graph
            var step_owned = true;
            defer if (step_owned) ctx.unlockStep();
            if (!try norms.needsPinnedInputUpload(max_upload_bytes)) return;
            var parked = ctx.parkWithOwnedStep(control) catch |err| switch (err) {
                // Busy reservation is pre-transfer: original step/custody stay
                // owned and ordinary upload remains safe before any SDK copy.
                error.ResourceTemporarilyUnavailable => return,
                else => return err,
            };
            step_owned = false;
            defer if (parked.active) {
                parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                step_owned = true;
            };
            try norms.includePinnedInputUploadOutside(max_upload_bytes);
            try ctx.refreshNormFundingOutside(control);
            try parked.restoreKeepingStep();
            step_owned = true;
        }

        /// Synchronous pre-graph hook. Executor owns the actual step/FIFO;
        /// Page/State storage remains owned while pause excludes every other
        /// consumer. No completed forward work is replayed on a metadata race.
        fn prepareCapturedGraph(raw: *anyopaque, cb: *const ops.ComputeBackend, descriptor: @import("../ops/cuda/lighton_decode_graph.zig").Descriptor, phase: *@import("../ops/cuda/lighton_decode_graph.zig").GraphPhase, outside: @import("../graph/execution.zig").CapturedGraphOutside, control: InferenceExecutionControl) !bool {
            if (comptime !build_options.enable_cuda) return false;
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            const Cuda = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
            const actual = if (ctx.cb) |*value| value else return error.UnsupportedReaderEpochBackend;
            if (cb.ptr != actual.ptr or cb.vtable != actual.vtable or actual.kind() != .cuda) return error.ForeignReaderEpochBackend;
            const model = ctx.model.session.model_lifetime orelse return error.MissingCacheRetirementModel;
            const preparation = ctx.norm_preparation orelse return error.UnsupportedReaderEpochBackend;
            try preparation.validateForCohort(ctx.model.session, &ctx.cohort_waiter);
            if (@as(*anyopaque, @ptrCast(preparation.expectedCudaCompute())) != cb.ptr) return error.ForeignReaderEpochBackend;
            for (0..8) |_| {
                try control.check();
                const current = @import("../execution/cleanup_context.zig").Context{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() };
                switch (try Cuda.inspectCapturedGraphPhase(cb, phase, current)) {
                    .ready => return true,
                    .unsupported => return false,
                    .outside => outside.release(),
                }
                var parked = ctx.parkWithOwnedStep(control) catch |err| {
                    // Another owner reserved the outside window but has not
                    // taken our step. Decline BEFORE transfer, never wait here.
                    if (err == error.ResourceTemporarilyUnavailable) return false;
                    return err;
                };
                phase.prepareOutside(model) catch |err| {
                    phase.destroyOutside(); // inert failed phase, outside all gates
                    parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                    switch (err) {
                        error.OutOfMemory, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => return false,
                        else => return err,
                    }
                };
                parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                // NEVER use the pre-pause current variable for bind/retirement.
                const now = @import("../execution/cleanup_context.zig").Context{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() };
                const bound = Cuda.bindCapturedGraphPhase(cb, phase, now) catch |err| {
                    try phase.close(now); // process-owned node, not this stack
                    if (err != error.PreparedCachePlanChanged) return err;
                    try phase.initInto(descriptor);
                    continue;
                };
                if (bound) return true;
                // Legacy raw metadata upgrade consumed no graph math. Retire
                // its inert process phase under NOW B, then inspect again;
                // the next park seals B before any new graph funding supplier.
                try phase.close(now);
                try phase.initInto(descriptor);
            }
            return error.PreparedCachePlanChanged;
        }

        /// Before any forward work, prepare the optional external producer's
        /// exact values-only storage. Like graph admission this borrows the
        /// actual step, restores every owner on error, and binds only fresh B.
        fn prepareExternalPrefill(raw: *anyopaque, cb: *const ops.ComputeBackend, phase: *@import("../ops/cuda/prefill_phase.zig").Phase, control: InferenceExecutionControl) !void {
            if (comptime !build_options.enable_cuda) return;
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            const Cuda = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
            const actual = if (ctx.cb) |*value| value else return error.UnsupportedReaderEpochBackend;
            if (cb.ptr != actual.ptr or cb.vtable != actual.vtable or actual.kind() != .cuda) return error.ForeignReaderEpochBackend;
            const model = ctx.model.session.model_lifetime orelse return error.MissingCacheRetirementModel;
            const preparation = ctx.norm_preparation orelse return error.UnsupportedReaderEpochBackend;
            try preparation.validateForCohort(ctx.model.session, &ctx.cohort_waiter);
            if (@as(*anyopaque, @ptrCast(preparation.expectedCudaCompute())) != cb.ptr) return error.ForeignReaderEpochBackend;
            const descriptor = phase.descriptor;
            for (0..8) |_| {
                try control.check();
                const current = @import("../execution/cleanup_context.zig").Context{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() };
                switch (try Cuda.inspectExternalPrefillPhase(cb, phase, current)) {
                    .ready, .unavailable => return,
                    .outside => {},
                }
                var parked = ctx.parkWithOwnedStep(control) catch |err| {
                    // No ownership transferred on a busy outside window. Only
                    // optional query/funding work may decline here; raw
                    // ownership upgrade/geometry authority is never fabricated.
                    if (err == error.ResourceTemporarilyUnavailable and (phase.mode == .query or phase.mode == .fund)) {
                        phase.declined = true;
                        return switch (try Cuda.inspectExternalPrefillPhase(cb, phase, current)) {
                            .ready => {},
                            else => error.PreparedCachePlanChanged,
                        };
                    }
                    return err;
                };
                phase.prepareOutside(model) catch |err| {
                    const optional = phase.mode == .fund and switch (err) {
                        error.OutOfMemory, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => true,
                        else => false,
                    };
                    phase.destroyOutside(); // all lanes and old B are gone
                    if (optional) phase.declined = true;
                    parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                    if (optional) continue; // inspect binds managed decline under fresh B
                    return err;
                };
                parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                const now = @import("../execution/cleanup_context.zig").Context{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() };
                const bound = Cuda.bindExternalPrefillPhase(cb, phase, now) catch |err| {
                    try phase.close(now);
                    if (err != error.PreparedCachePlanChanged) return err;
                    try phase.initInto(descriptor);
                    continue;
                };
                if (bound) return; // actual backing published, Funding closed before math
                // A metadata query keeps its exact values in this synchronous
                // phase. A raw upgrade already consumed its pure Node under B.
                // Inspect again; do not discard the successful query and replay it.
                try phase.close(now);
            }
            return error.PreparedCachePlanChanged;
        }

        /// Mandatory packed CPU arrays precede ALL packet KV mutation/math,
        /// regardless of whether an optional external DSO exists or declines.
        /// This is the SAME actual owned-step outside window as graph/DSO
        /// preparation. Busy/cancel/admission errors restore and propagate.
        fn preparePackedHost(raw: *anyopaque, cb: *const ops.ComputeBackend, bytes: usize, control: InferenceExecutionControl) !@import("../execution/packed_host_loan.zig").Loan {
            if (comptime !build_options.enable_cuda) return error.UnsupportedReaderEpochBackend;
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            const actual = if (ctx.cb) |*value| value else return error.UnsupportedReaderEpochBackend;
            if (cb.ptr != actual.ptr or cb.vtable != actual.vtable or actual.kind() != .cuda) return error.ForeignReaderEpochBackend;
            const preparation = ctx.norm_preparation orelse return error.UnsupportedReaderEpochBackend;
            try preparation.validateForCohort(ctx.model.session, &ctx.cohort_waiter);
            if (@as(*anyopaque, @ptrCast(preparation.expectedCudaCompute())) != cb.ptr) return error.ForeignReaderEpochBackend;
            const identity = @import("../execution/kernel_route.zig").Implementation{ .context = cb.ptr, .table = cb.vtable };
            var parked = try ctx.parkWithOwnedStep(control);
            var loan = @import("../execution/packed_host_loan.zig").Loan.prepareOutsideExecution(ctx.model.session, identity, bytes) catch |err| {
                parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
                return err;
            };
            parked.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
            const now = @import("../execution/cleanup_context.zig").Context{ .boundary = ctx.cleanup_owner.epoch(), .operation = cb.operationToken() };
            errdefer loan.finishWithCleanup(now) catch platform.inference_process_supervisor.restartWorker();
            // Cancellation during the cold supplier still restores all actual
            // owners, then retires this loan under NOW B before any packet math.
            try control.check();
            try preparation.validateForCohort(ctx.model.session, &ctx.cohort_waiter);
            try loan.bind(ctx.model.session, identity, bytes, now);
            return loan;
        }

        /// Bind the SAME actual Reader-owned hook for its worker and owned
        /// runtime harness. This metadata assignment grants no execution or
        /// outside authority: caller must still use the real step/cohort and
        /// satisfy the callback's exact backend/Model proof before SDK work.
        pub fn configureGraphAdmission(ctx: *LightOnStreamContext, executor: *@import("../graph/decoder_step_executor.zig").Executor) void {
            executor.context.cleanup_owner = &ctx.cleanup_owner;
            executor.context.before_owned_forward = if (comptime build_options.enable_cuda) .{ .ptr = ctx, .prepare_fn = preparePackedHost } else null;
            executor.context.graph_preparation = if (comptime build_options.enable_cuda) .{ .ptr = ctx, .prepare_fn = prepareCapturedGraph } else null;
            executor.context.external_prefill_preparation = if (comptime build_options.enable_cuda) .{ .ptr = ctx, .prepare_fn = prepareExternalPrefill } else null;
        }

        pub fn runExecutor(ctx: *LightOnStreamContext) void {
            const io = ctx.worker_io.?.io();
            // Physical execution borrows the epoch's backend/state. Task
            // publication is a separate adapter and retains no request pipeline.
            var executor = @import("../graph/decoder_step_executor.zig").Executor{
                .context = .{
                    .allocator = ctx.allocator,
                    .cb = &ctx.cb.?,
                    .gpt_config = &ctx.cfg,
                    .session = ctx.model.session,
                    .kv_dtype = ctx.worker_kv_dtype,
                    .execution_control = ctx.worker_control,
                },
                .pad_token = ctx.model.getTokenizer().specialTokens().pad_id,
                .continuation = ctx.worker_control,
                .access = .{
                    .mutex = &ctx.step_mutex,
                    .pause = &ctx.execution_pause,
                    .gate = if (ctx.gate_events) &ctx.step_gate else null,
                    .io = io,
                    .control = ctx.worker_control,
                    .contention_wait_ns = ctx.execution_gate_wait_ns,
                },
                .prepared_lighton_decode = ctx.features.prepared,
                .packed_prefill_items = ctx.features.packed_prefill_items,
                .prepared_qwen_prefill_chunk_rows = ctx.features.prepared_prefill_chunk_rows,
                .packed_shared_frame = ctx.features.packed_shared_frame,
                .single_frame_lighton_decode = ctx.features.single_frame,
                .gpu_token_burst_lighton_decode = ctx.features.gpu_token_burst,
                .gpu_token_burst_width = ctx.features.gpu_token_burst_width,
                .gpu_greedy_stream_lighton_decode = ctx.features.gpu_greedy_stream,
                .lighton_cuda_execution_id = ctx.execution_id,
            };
            defer executor.endEpoch();
            var storage: [reader_execution_pool.Pool.max_capacity]runtime.scheduler.native_generate.StepItem = undefined;
            var step = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
            ctx.configureGraphAdmission(&executor);
            while (!ctx.worker_stop.load(.acquire)) {
                // Request phases cannot enqueue decode until vision/prompt
                // preparation obtains this same GPU gate. Do not immediately
                // reacquire it forever just because continuations stay queued.
                // Reset before observing phase/queue predicates. A phase
                // releasing its last waiter always signals this same event.
                ctx.worker_wake.reset();
                if (ctx.phase_fair and ctx.phase_waiters.load(.acquire) != 0) {
                    ctx.phase_priority_yields += 1;
                    if (ctx.gate_events) {
                        if (!ctx.worker_stop.load(.acquire)) ctx.worker_wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch {};
                        continue;
                    }
                    // Match the admitted epoch's gate parking interval. The
                    // old fixed 100us loop kept waking during long prefill
                    // phases even when the configured interval was 1ms.
                    const wait_us = @max(@as(u64, 100), @min(ctx.execution_gate_wait_ns / std.time.ns_per_us, 1000));
                    io.sleep(std.Io.Duration.fromMicroseconds(@intCast(wait_us)), .awake) catch {};
                    continue;
                }
                // Both predicates have now been observed after the reset.
                var budget = ctx.coordinator.defaultStepBudget();
                platform.sync.lockYielding(&ctx.kv_mutex);
                if (ctx.manager.poolAvailableBlocks(ctx.pool)) |available|
                    budget.max_kv_blocks = if (available > 1) available - 1 else 0;
                ctx.kv_mutex.unlock();
                if (ctx.coordinator.claimNextStep(budget, &storage, &step)) |claimed_lease| {
                    var lease = claimed_lease;
                    var publication = generation.DecoderStepPublication{ .scheduler = &ctx.coordinator, .lease = &lease, .vocab_size = ctx.cfg.vocab_size };
                    executor.execute(step.items, ctx.coordinator.usesStepEvents(), publication.interface());
                    ctx.worker_steps += 1;
                    continue;
                }
                if (!ctx.worker_stop.load(.acquire)) ctx.worker_wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMicroseconds(ctx.coordinator.eventWaitUs()), .clock = .awake } }) catch {};
            }
            ctx.worker_tail_steps = executor.tail_steps;
        }

        pub fn prepare(ctx: *LightOnStreamContext, kv_dtype: runtime.kv.pool.KvDType) !void {
            if (ctx.cb != null) return;
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                try norms.beginRequest(ctx.cleanup_owner.epoch() orelse return error.MissingCurrentCleanupEpoch);
            };
            errdefer if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| norms.unbindExpected();
            var cb = try session_factory.getComputeBackendWithBudgetAndCleanup(ctx.model.session, ctx.allocator, &ctx.budget, .{ .boundary = ctx.cleanup_owner.epoch() });
            errdefer cb.deinitWithCleanup(.{ .boundary = ctx.cleanup_owner.epoch() });
            if (ctx.cleanup_owner.epoch() != null) {
                const bound = try ctx.cleanup_owner.bindBackend(&cb);
                // An acquired engine token must bind its exact owned record.
                // Canonical model-owned/private targets without an engine token
                // remain under this epoch's actual generation lane.
                if (!bound and cb.operationToken() != null) return error.BackendOperationCleanupUnavailable;
            }
            // The device hook owns CUDA KV bytes. Both epoch pools retain
            // logical pages without allocating a second pair of host mirrors.
            const config = generation.requestKvManagerPoolConfig(generation.kvPoolConfig(if (ctx.model.session.backend() == .cuda) .cuda else .metal, kv_dtype, ctx.cfg, Hooks.generationKvSlidingTrimForced()));
            ctx.pool = try ctx.manager.addPool(config);
            ctx.storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, config);
            errdefer ctx.storage.deinitWithBoundary(ctx.cleanup_owner.epoch());
            if (ctx.kv_bank == null) try cb.provisionKvDeviceWriteHook(&ctx.storage);
            if (comptime build_options.enable_metal) {
                if (cb.kind() == .metal) {
                    const M = @import("../ops/metal_compute.zig").MetalCompute;
                    if (ctx.features.preparedRowCapacity()) |rows| try M.setLightOnPreparedRows(&cb, rows);
                    try M.setLightOnActivationPoolConfigured(&cb, ctx.features.activation_pool, ctx.features.prefill_activation_pool);
                    try M.setLightOnSharedCoreWeights(&cb, ctx.features.epoch_auxiliary and ctx.features.shared_core_weights);
                    try M.setLightOnStreamFirstLayers(&cb, ctx.features.stream_first_layers);
                    try M.setLightOnBatchAttention(&cb, ctx.features.batch_attention);
                    try M.setLightOnGpuGreedyStream(&cb, ctx.features.gpu_greedy_stream);
                    try M.setLightOnStreamChunks(&cb, ctx.features.stream_chunks);
                    try M.setLightOnPrefillStreamLayers(&cb, ctx.features.prefill_stream_layers);
                    // admitPage has already acquired the shared vision peak.
                    // It outlives cb.deinit and its explicit workspace clear.
                    try M.setLightOnVisionWorkspaceCache(&cb, ctx.features.vision_workspace_cache);
                }
            }
            // beginRequest and all existing feature policy changes have
            // finished under the actual reader lanes. Only now bind the exact
            // already-funded norm producers to this stable epoch owner.
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| {
                try norms.bind(&cb, ctx.cleanup_owner.epoch() orelse return error.MissingCurrentCleanupEpoch);
            };
            // LAST fallible validation/transfer. Leaf adoption publishes its
            // already-owned hook only after all checks and ledger handoff.
            if (ctx.kv_bank) |*bank| {
                const blocks = try ctx.bank_coverage.?.window.sharedBlocks();
                try bank.adopt(&cb, &ctx.storage);
                ctx.bank_adopted = true;
                ctx.manager.setPoolTargetMaxBlocks(ctx.pool, blocks);
                ctx.storage.storage.setTargetMaxBlocks(blocks);
            }
            if (comptime build_options.enable_cuda) if (ctx.norm_preparation) |norms| if (ctx.captured_roots) |*roots| try roots.bind(&cb, norms.expectedCudaCompute());
            ctx.cb = cb;
        }
    };
}

fn coldCreatorPageCase(a: std.mem.Allocator, cancel_after_reserve: bool, transient_first: bool) !void {
    const Node = struct {
        model_manager: model_manager_mod.ModelManager,
        pub fn lightOnOcrPolicy(_: *const @This(), rows: usize) runtime.scheduler.native_generate.Policy {
            return .{ .max_step_items = rows };
        }
    };
    const Hooks = struct {};
    const Context = Epoch(Node, Hooks);
    var node = Node{ .model_manager = model_manager_mod.ModelManager.init(std.testing.allocator, @import("../backends/backends.zig").SessionManager.init(std.testing.allocator)) };
    defer node.model_manager.deinit();
    try node.model_manager.ensureResourceOwnerReady();
    const controller = node.model_manager.resource_domain.?.admission;
    controller.configureSharedLimits(.{});
    const ColdSession = struct {
        const Tensor = @import("../backends/tensor.zig").Tensor;
        const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) anyerror![]Tensor {
            return error.UnexpectedNumericalCall;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        const table = @import("../backends/session.zig").Session.VTable{ .backend = backend, .run = run, .inputInfo = info, .outputInfo = info, .close = close };
    };
    var marker: u8 = 0;
    var model: model_manager_mod.LoadedModel = undefined;
    model.session = .{ .ptr = &marker, .vtable = &ColdSession.table };
    model.native_generate_lock = .unlocked;
    model.native_reader_execution_lane = .{};
    model.native_reader_stream = .{};
    const Probe = struct {
        model: *model_manager_mod.LoadedModel,
        cancelled: bool = false,
        cancel_after_reserve: bool,
        transient_first: bool,
        attempts: usize = 0,
        supplier_under_lane: bool = false,
        reserves: usize = 0,
        releases: usize = 0,
        fn observe(self: *@This()) void {
            if (self.model.native_reader_execution_lane.owned) self.supplier_under_lane = true;
            if (self.model.nativeGenerationMutex().tryLock()) self.model.nativeGenerationMutex().unlock() else self.supplier_under_lane = true;
        }
        fn reserve(raw: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.attempts += 1;
            if (self.transient_first and self.attempts == 1) return error.ResourceTemporarilyUnavailable;
            self.reserves += 1;
            if (self.cancel_after_reserve) self.cancelled = true;
            return self.reserves;
        }
        fn retain(raw: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.releases += 1;
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
    };
    var probe = Probe{ .model = &model, .cancel_after_reserve = cancel_after_reserve, .transient_first = transient_first };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var factory = Context.Factory{ .node = &node, .model = &model, .cfg = .{}, .limits = .{ .backend_limit_bytes = 1024, .host_limit_bytes = 1024, .scratch_limit_bytes = 2048 }, .physical_rows_limit = 1, .decoder_peak = 64, .control = .{ .ptr = &probe, .check_fn = Probe.check }, .creator_page = .{ .amounts = .{ .backend_scratch_bytes = 128, .host_scratch_bytes = 16 }, .peak = 64, .host_peak = 32 } };
    if (!cancel_after_reserve and !transient_first) {
        var workspace = runtime.tier.memory.ReservedGenerationWorkspace{ .floor = .{}, .capacity = .{ .host_scratch_bytes = 64, .backend_scratch_bytes = 64 } };
        defer workspace.borrowed.deinit();
        model.session.generation_workspace = &workspace;
        defer model.session.generation_workspace = null;
        // Call the ACTUAL metadata producer while the real Session advertises
        // separately prepaid numerical workspace. All metadata must remain in
        // its own partitionable direct credit; the workspace ledger stays idle.
        var metadata = try factory.acquireCustodyMetadata(64);
        defer metadata.release();
        try std.testing.expectEqual(@as(usize, 64), metadata.amounts.host_scratch_bytes);
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes);
    }
    const raw = Context.createWithAllocator(&factory, a) catch |err| {
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
        try std.testing.expect(!model.native_reader_execution_lane.owned and !probe.supplier_under_lane);
        try std.testing.expectEqual(probe.reserves, probe.releases);
        try std.testing.expect(model.nativeGenerationMutex().tryLock());
        model.nativeGenerationMutex().unlock();
        if (cancel_after_reserve and err == error.Cancelled) return;
        return err;
    };
    const ctx: *Context = @ptrCast(@alignCast(raw));
    var ctx_live = true;
    defer if (ctx_live) Context.destroy(raw);
    try std.testing.expect(!cancel_after_reserve);
    {
        // The actual cold Reader installs both synchronous callbacks through
        // its one worker/harness configuration seam. With no actual backend
        // yet, the prefill callback must refuse before touching phase/Model
        // metadata, reserving anything, or changing the active epoch.
        var configured: @import("../graph/decoder_step_executor.zig").Executor = undefined;
        ctx.configureGraphAdmission(&configured);
        try std.testing.expect(configured.context.cleanup_owner == &ctx.cleanup_owner);
        if (comptime build_options.enable_cuda) {
            const graph_hook = configured.context.graph_preparation orelse return error.MissingReaderGraphPreparation;
            const prefill_hook = configured.context.external_prefill_preparation orelse return error.MissingReaderPrefillPreparation;
            const host_hook = configured.context.before_owned_forward orelse return error.MissingPackedHostPreparation;
            try std.testing.expect(graph_hook.ptr == @as(*anyopaque, @ptrCast(ctx)) and prefill_hook.ptr == @as(*anyopaque, @ptrCast(ctx)));
            const F = @import("../ops/cuda/prefill_phase.zig");
            var phase: F.Phase = undefined;
            try phase.initInto(.{});
            const before = controller.snapshot();
            const epoch_nonce = ctx.cleanup_owner.epoch().?.nonce;
            var no_backend: ops.ComputeBackend = undefined; // refusal precedes any read
            try std.testing.expectError(error.UnsupportedReaderEpochBackend, prefill_hook.prepare_fn(prefill_hook.ptr, &no_backend, &phase, .{}));
            try std.testing.expectError(error.UnsupportedReaderEpochBackend, host_hook.prepare_fn(host_hook.ptr, &no_backend, 35028, .{}));
            try std.testing.expect(phase.cold == null and !phase.bound and !phase.declined and phase.mode == .none);
            try std.testing.expectEqual(epoch_nonce, ctx.cleanup_owner.epoch().?.nonce);
            try std.testing.expectEqualDeep(before, controller.snapshot());
        } else {
            try std.testing.expect(configured.context.graph_preparation == null);
            try std.testing.expect(configured.context.external_prefill_preparation == null);
            try std.testing.expect(configured.context.before_owned_forward == null);
        }
    }
    {
        // Actual cold creator owns native+reader lanes. This finite metadata
        // oracle proves the graph seam returns the SAME borrowed step held;
        // nonempty CUDA/Metal graph custody needs its separate SDK fixture.
        var origin = try ctx.cleanup_owner.epoch().?.sealWitness();
        defer origin.release();
        try ctx.lockAdmissionRaw(.{});
        var step_owned = true;
        defer if (step_owned) ctx.unlockStep();
        var paused = try ctx.parkWithOwnedStep(.{});
        step_owned = false;
        defer if (paused.active) {
            paused.restoreKeepingStep() catch platform.inference_process_supervisor.restartWorker();
            ctx.unlockStep();
        };
        try std.testing.expect(try origin.wasSealed());
        try std.testing.expect(!model.native_reader_execution_lane.owned);
        try std.testing.expect(model.nativeGenerationMutex().tryLock());
        model.nativeGenerationMutex().unlock();
        try std.testing.expect(ctx.step_mutex.tryLock());
        ctx.step_mutex.unlock();
        try paused.restoreKeepingStep();
        step_owned = true;
        try std.testing.expect(!ctx.step_mutex.tryLock());
        try std.testing.expect(model.native_reader_execution_lane.owned);
        try std.testing.expect(!ctx.execution_pause.paused.load(.acquire));
    }
    const before_adoption = controller.snapshot();
    try std.testing.expectEqual(@as(usize, 256), before_adoption.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 48), before_adoption.host_scratch_bytes);
    const index = ctx.register(.{});
    var registered = true;
    defer if (registered) ctx.unregister(index);
    try std.testing.expectError(error.InvalidCreatorAdmissionHandoff, ctx.takeCreatorAdmission(&factory, index, 64, 33, factory.creator_page.?.amounts));
    var page = (try ctx.takeCreatorAdmission(&factory, index, 64, 32, factory.creator_page.?.amounts)) orelse return error.MissingCreatorPage;
    var page_live = true;
    defer if (page_live) {
        if (registered) {
            ctx.unregister(index);
            registered = false;
        }
        if (ctx_live) {
            Context.destroy(raw);
            ctx_live = false;
        }
        page.release();
    };
    try std.testing.expectEqualDeep(before_adoption, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), ctx.admitted_pages);
    try std.testing.expect((try ctx.takeCreatorAdmission(&factory, index, 64, 32, factory.creator_page.?.amounts)) == null);
    ctx.unregister(index);
    registered = false;
    Context.destroy(raw);
    ctx_live = false;
    try std.testing.expectEqualDeep(factory.creator_page.?.amounts, controller.snapshot());
    page.release();
    page_live = false;
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(!probe.supplier_under_lane);
    try std.testing.expectEqual(probe.reserves, probe.releases);
}

test "reader unpublished creator page and SharedPeaks preserve exact quota through adoption all OOM and cancellation" {
    const Oom = struct {
        fn run(a: std.mem.Allocator) !void {
            return coldCreatorPageCase(a, false, false);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Oom.run, .{});
    try coldCreatorPageCase(std.testing.allocator, true, false);
    try coldCreatorPageCase(std.testing.allocator, false, true);
}

test "reader CUDA request profile disabled finish never touches retired epoch metadata" {
    const Context = Epoch(struct {}, struct {});
    var profile = Context.CudaRequestProfile{};
    const unavailable: *Context = @ptrFromInt(16);
    profile.finish(unavailable, true);
    profile.finish(unavailable, false);
    try std.testing.expect(profile.before == null);
}
