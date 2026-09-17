// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Managed CPU/resident-Metal training over immutable named JSONL snapshots. The caller
//! holds the verified source store/tokenizer and dataset leases for this job.
//! One controller owns all trainable weights/moments; one bounded graph cache
//! survives matching batch geometries. A failed step never advances data order.
const std = @import("std");
const ml = @import("ml").graph;
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const controller = @import("seeded_gradient_trainer.zig");
const run = @import("gliner_boundary_run.zig");
const data = @import("gliner_boundary_dataset.zig");
const step = @import("gliner_boundary_train_step.zig");
const encoder = @import("gliner_boundary_encoder_graph.zig");
const peft = @import("gliner_boundary_peft_graph.zig");
const adapters = @import("gliner_boundary_adapter_layout.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const targets = @import("gliner_boundary_targets.zig");
const seeded = @import("../graph/seeded_training.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");
const regex = @import("../pipelines/extraction_regex.zig");
const export_mod = @import("gliner_boundary_training_export.zig");
const source_mod = @import("gliner_boundary_training_source.zig");
const backend_mod = @import("gliner_boundary_training_backend.zig");
const transfer_mod = @import("gliner_boundary_training_transfer.zig");
const synthetic = @import("gliner_boundary_synthetic_adapter_fixture.zig");
const SyntheticFixture = if (@import("builtin").is_test) ?*const synthetic.Fixture else void;
const Allocator = std.mem.Allocator;

pub const Limits = struct {
    max_host_bytes: usize = 6 * 1024 * 1024 * 1024,
    max_backend_bytes: usize = 4 * 1024 * 1024 * 1024,
    /// Resident device payloads are admitted explicitly. This independent
    /// backend heap cap includes CT handles, shapes, runtime and driver metadata.
    max_backend_host_bytes: usize = 128 * 1024 * 1024,
    max_combined_bytes: usize = 12 * 1024 * 1024 * 1024,
    /// Regional jobs reserve this complete caller-side arena, including its
    /// growth slack, draws, binding vectors and CPU gradient readbacks.
    max_recomputed_batch_scratch_bytes: usize = 512 * 1024 * 1024,
    optimizer: controller.Limits = .{},
    run: run.Limits = .{},
    step: step.Limits = .{},
    peft: peft.Limits = .{},
    differentiation: seeded.Options = .{},
};
pub const Options = struct {
    run: run.Config,
    execution: controller.Execution = .native,
    attention_profile: step.AttentionProfile = .materialized_v1,
    activation_profile: step.ActivationProfile = .retained_v1,
    /// Reservation of the verified source owner, including immutable files,
    /// decoded tokenizer, named tensor metadata and any alignment copies.
    source_reserved_bytes: usize,
    calibration_sha256: ?[32]u8 = null,
    test_sha256: ?[32]u8 = null,
    peft: ?peft.Config = null,
    processor: processor.Options = .{},
    capacities: step.Capacities = .{},
    weights: objectives.Weights = .{},
    gold_start: f32 = 1,
    gold_end: f32 = 0.25,
    gold_hold_fraction: f32 = 0.15,
    require_gold_relation_coverage: bool = false,
    regex: regex.ContextOptions = .{},
    limits: Limits = .{},
};
pub const Report = struct {
    epoch: u64,
    batch: u32,
    examples: u32,
    terms: ?objectives.Terms,
    coverage: step.Coverage = .{},
    optimizer: controller.Result,
    decision_fingerprint: ?[32]u8 = null,
    host_peak_bytes: usize,
    backend_peak_bytes: usize,
    resident_device_upper_bound_bytes: usize = 0,
    transfers: transfer_mod.Diagnostics = .{},
    resident_gradient_control_bytes: usize = 0,
    /// Internal kernel scalar observations are conservatively admitted. This
    /// is separate from measured objective/gradient transfer diagnostics.
    resident_instruction_control_readback_upper_bound_bytes: usize = 0,
    /// Terms retain the model's component diagnostics. In the pinned trainer's
    /// wholly disconnected case the actual optimizer objective is zero and
    /// every selected parameter receives an explicit zero gradient.
    zero_loss_fallback: bool = false,
};

pub const MemoryPhase = enum { initialization, batch_preparation, graph_preparation, forward_backward, gradient_readback, optimizer, checkpoint, restore, export_snapshot, job };
pub const MemoryDomain = enum { host, backend, job };
pub const MemoryFailure = struct {
    phase: MemoryPhase,
    domain: MemoryDomain,
    allocation: Budget.AllocationFailure,
};

/// One synchronous operation's terminal allocation failures. This is separate
/// from the allocator's sticky `denied` statistic: a prior failed resize or
/// retry must not turn a genuine backing allocator OOM into a declared denial.
pub const MemoryFailures = struct {
    mutex: std.atomic.Mutex = .unlocked,
    phase: MemoryPhase = .initialization,
    last: ?MemoryFailure = null,

    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn begin(self: *@This(), phase: MemoryPhase) void {
        self.lock();
        defer self.mutex.unlock();
        self.phase = phase;
        self.last = null;
    }
    pub fn snapshot(self: *@This()) ?MemoryFailure {
        self.lock();
        defer self.mutex.unlock();
        return self.last;
    }
    pub fn translate(self: *@This(), err: anyerror) anyerror {
        if (err != error.OutOfMemory) return err;
        const failure = self.snapshot() orelse return err;
        if (failure.allocation.kind != .declared_limit) return err;
        return switch (failure.domain) {
            .host => error.BoundaryTrainingHostMemoryLimitExceeded,
            .backend => error.BoundaryTrainingBackendMemoryLimitExceeded,
            .job => error.BoundaryTrainingJobMemoryLimitExceeded,
        };
    }
    pub fn report(self: *@This(), err: anyerror) anyerror {
        const translated = self.translate(err);
        if (err == error.OutOfMemory) if (self.snapshot()) |failure| {
            const details = failure.allocation;
            std.log.warn("GLiNER2.5 training allocation failed: phase={s} domain={s} reason={s} requested_bytes={d} live_bytes={d} peak_bytes={d} limit_bytes={d}", .{
                @tagName(failure.phase), @tagName(failure.domain), @tagName(details.kind), details.requested_bytes, details.live_bytes, details.peak_bytes, details.limit_bytes,
            });
        };
        return translated;
    }
};

pub const MemoryObserver = struct {
    failures: *MemoryFailures,
    domain: MemoryDomain,

    pub fn attach(self: *@This(), budget: *Budget) void {
        budget.failure_context = self;
        budget.allocation_failed = observe;
    }
    fn observe(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.failures.lock();
        defer self.failures.mutex.unlock();
        self.failures.last = .{ .phase = self.failures.phase, .domain = self.domain, .allocation = failure };
    }
    fn observeDeclared(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
        // The enclosing host allocator already reported a backing failure.
        // Preserve its precise declared/backing cause through this child.
        if (failure.kind == .declared_limit) observe(raw, failure);
    }
};

/// A process-level resource manager must reserve this conservative amount
/// before constructing the trainer. Allocations inside the job are additionally
/// enforced by the independent host and backend budgets below.
pub fn admissionBytes(source: bundle.Identity, dataset: *const data.Dataset, source_reserved_bytes: usize, limits: Limits) !usize {
    var source_bytes = source.weight.size_bytes;
    for (source.sidecars) |sidecar| source_bytes = std.math.add(u64, source_bytes, sidecar.size_bytes) catch return error.BoundaryTrainingRunLimitExceeded;
    if (source_reserved_bytes < source_bytes) return error.InvalidBoundaryTrainingSourceReservation;
    var total = std.math.add(usize, limits.max_host_bytes, limits.max_backend_bytes) catch return error.BoundaryTrainingRunLimitExceeded;
    total = std.math.add(usize, total, source_reserved_bytes) catch return error.BoundaryTrainingRunLimitExceeded;
    total = std.math.add(usize, total, dataset.options.limits.max_host_bytes) catch return error.BoundaryTrainingRunLimitExceeded;
    if (total > limits.max_combined_bytes) return error.BoundaryTrainingRunLimitExceeded;
    return total;
}

pub const Trainer = struct {
    backing: Allocator,
    host_budget: Budget,
    backend_budget: Budget,
    memory_failures: MemoryFailures = .{},
    host_observer: MemoryObserver = undefined,
    backend_observer: MemoryObserver = undefined,
    backend: *backend_mod.Owner,
    cb: ops.ComputeBackend,
    optimizer: controller.Trainer,
    validators: regex.Context,
    model_config: model.Config,
    options: Options,
    run_plan: run.Plan,
    dataset: *const data.Dataset,
    tokenizer: @import("inference_tokenizer").Tokenizer,
    adapter_layout: ?adapters.Layout = null,
    plan: ?step.Plan = null,
    plan_key: ?[32]u8 = null,
    recomputed_future_host_bytes: usize = 0,
    order: []u32 = &.{},
    order_epoch: ?u64 = null,
    fingerprint: [32]u8,
    resident_device_upper_bound_bytes: usize = 0,
    busy: std.atomic.Value(bool) = .init(false),
    synthetic_fixture: SyntheticFixture = if (@import("builtin").is_test) null else {},

    /// Original parameters borrow the verified named source store. All chosen
    /// trainables are copied into the sole controller; frozen weights retain
    /// the caller's immutable model lease. Adapter slots are enrolled globally,
    /// including modules absent from the first batch's task schema.
    pub fn init(a: Allocator, store: *native.WeightStore, tokenizer: @import("inference_tokenizer").Tokenizer, source: bundle.Identity, config: model.Config, dataset: *const data.Dataset, original: []const run.Parameter, options: Options, control: ?Control) !*Trainer {
        return initInternal(a, store, tokenizer, source, config, dataset, original, options, control, if (@import("builtin").is_test) null else {});
    }

    /// Synthetic source-oracle construction only. No JSON/env option or live
    /// owner mutation API can select this path in a production executable.
    pub fn initWithSyntheticAdapterFixtureForTest(a: Allocator, store: *native.WeightStore, tokenizer: @import("inference_tokenizer").Tokenizer, source: bundle.Identity, config: model.Config, dataset: *const data.Dataset, original: []const run.Parameter, options: Options, control: ?Control, fixture: *const synthetic.Fixture) !*Trainer {
        if (!@import("builtin").is_test) @compileError("Synthetic NativeTrainer construction is test-only");
        if (options.peft == null) return error.InvalidSyntheticAdapterFixture;
        return initInternal(a, store, tokenizer, source, config, dataset, original, options, control, fixture);
    }

    fn initInternal(a: Allocator, store: *native.WeightStore, tokenizer: @import("inference_tokenizer").Tokenizer, source: bundle.Identity, config: model.Config, dataset: *const data.Dataset, original: []const run.Parameter, options: Options, control: ?Control, fixture: SyntheticFixture) !*Trainer {
        try check(control);
        _ = try admissionBytes(source, dataset, options.source_reserved_bytes, options.limits);
        if (options.activation_profile == .layer_recompute_v1 and options.limits.max_recomputed_batch_scratch_bytes == 0) return error.InvalidBoundaryTrainingRun;
        if (source.backbone != config.backbone or options.processor.control != null or options.regex.compile_options.control != null or options.regex.match_options.control != null or options.limits.step.targets.regex_context != null or options.limits.step.targets.validate_value_fn != null) return error.InvalidBoundaryTrainingRun;
        const is_adapter = options.run.mode == .lora or options.run.mode == .dora;
        if (is_adapter != (options.peft != null)) return error.InvalidBoundaryTrainingRun;
        if (options.peft) |p| if (p.mode != .train or (p.kind == .dora) != (options.run.mode == .dora)) return error.InvalidBoundaryTrainingRun;
        for (original) |parameter| if (parameter.kind != .original) return error.InvalidBoundaryTrainingParameters;
        const self = try a.create(Trainer);
        errdefer a.destroy(self);
        if (options.execution == .resident_metal and (options.limits.max_backend_host_bytes == 0 or options.limits.max_backend_host_bytes >= options.limits.max_backend_bytes)) return error.BoundaryTrainingRunLimitExceeded;
        self.* = .{ .backing = a, .host_budget = .{ .backing = a, .limit = options.limits.max_host_bytes }, .backend_budget = .{ .backing = a, .limit = if (options.execution == .resident_metal) options.limits.max_backend_host_bytes else options.limits.max_backend_bytes }, .backend = undefined, .cb = undefined, .optimizer = undefined, .validators = undefined, .model_config = config, .options = options, .run_plan = undefined, .dataset = dataset, .tokenizer = tokenizer, .fingerprint = undefined };
        self.synthetic_fixture = fixture;
        errdefer std.debug.assert(self.host_budget.live == 0 and self.backend_budget.live == 0);
        self.host_observer = .{ .failures = &self.memory_failures, .domain = .host };
        self.host_observer.attach(&self.host_budget);
        self.backend_observer = .{ .failures = &self.memory_failures, .domain = .backend };
        self.backend_observer.attach(&self.backend_budget);
        self.initialize(store, source, original, control) catch |err| return self.memory_failures.report(err);
        return self;
    }

    fn initialize(self: *Trainer, store: *native.WeightStore, source: bundle.Identity, original: []const run.Parameter, control: ?Control) !void {
        const options = self.options;
        const config = self.model_config;
        const dataset = self.dataset;
        const tokenizer = self.tokenizer;
        const host = self.host_budget.allocator();
        self.validators = regex.Context.init(host, options.regex);
        errdefer self.validators.deinit();
        var run_config = options.run;
        if (options.peft) |p| {
            self.adapter_layout = try self.initializeAdapterLayout(host, source, p, original);
            run_config.adapter_config_sha256 = self.adapter_layout.?.fingerprint;
        }
        errdefer if (self.adapter_layout) |*layout| layout.deinit();
        self.run_plan = try run.Plan.init(run_config, source, .{ .train_sha256 = dataset.sha256, .schema_sha256 = dataset.schemas_sha256, .calibration_sha256 = options.calibration_sha256, .test_sha256 = options.test_sha256, .examples = std.math.cast(u32, dataset.index.len) orelse return error.BoundaryTrainingRunLimitExceeded }, options.limits.run);
        // Validate the actual schedule before reading or updating any batch.
        _ = try objectives.scales(config.head, .{ .optimizer_step = 0, .total_optimizer_steps = self.run_plan.total_optimizer_steps, .gold_start = options.gold_start, .gold_end = options.gold_end, .gold_hold_fraction = options.gold_hold_fraction });
        inline for (std.meta.fields(objectives.Weights)) |field| {
            const weight = @field(options.weights, field.name);
            if (!std.math.isFinite(weight) or weight < 0) return error.InvalidBoundaryTrainingRun;
        }
        var arena = std.heap.ArenaAllocator.init(host);
        defer arena.deinit();
        const scratch = arena.allocator();
        var parameters = std.ArrayListUnmanaged(run.Parameter).empty;
        try parameters.appendSlice(scratch, original);
        if (self.adapter_layout) |layout| for (layout.modules) |descriptor| {
            try check(control);
            const base = for (original) |parameter| {
                if (std.mem.eql(u8, parameter.name, descriptor.base_name)) break parameter.values;
            } else return error.MissingBoundaryAdapterBase;
            // Initialization storage is temporary. Trainer.init owns its own
            // final A/B/magnitude slots and checkpoint state.
            const initialized = try self.initializeAdapterWeights(scratch, descriptor, base, run_config.seed);
            const a_dims = try scratch.dupe(i32, &.{ @intCast(descriptor.rank), @intCast(descriptor.target.in_dim) });
            const b_dims = try scratch.dupe(i32, &.{ @intCast(descriptor.target.out_dim), @intCast(descriptor.rank) });
            try parameters.append(scratch, .{ .name = descriptor.a_name, .canonical_name = descriptor.a_name, .dimensions = a_dims, .values = initialized.a, .kind = .adapter });
            try parameters.append(scratch, .{ .name = descriptor.b_name, .canonical_name = descriptor.b_name, .dimensions = b_dims, .values = initialized.b, .kind = .adapter });
            if (initialized.magnitude) |values| try parameters.append(scratch, .{ .name = descriptor.magnitude_name.?, .canonical_name = descriptor.magnitude_name.?, .dimensions = try scratch.dupe(i32, &.{@intCast(descriptor.target.out_dim)}), .values = values, .kind = .adapter });
        };
        const selected = try self.run_plan.selectParameters(scratch, parameters.items);
        const backend_limits = backend_mod.Limits{ .max_frozen_device_bytes = options.limits.max_backend_bytes, .max_combined_bytes = options.limits.max_backend_bytes };
        const backend_admission = try backend_mod.estimate(original, selected, options.execution, backend_limits);
        if (options.execution == .resident_metal) {
            var device_state_bytes: usize = 0;
            var upload_bytes = backend_admission.upload_staging_bytes;
            for (selected) |parameter| {
                device_state_bytes = try std.math.add(usize, device_state_bytes, try std.math.mul(usize, parameter.values.len, 16));
                upload_bytes = @max(upload_bytes, try std.math.mul(usize, parameter.values.len, 4));
            }
            const fixed = try std.math.add(usize, backend_admission.frozen_device_bytes, device_state_bytes);
            const bound = try std.math.add(usize, fixed, try std.math.add(usize, options.limits.max_backend_host_bytes, @max(upload_bytes, 2 * 1024 * 1024)));
            if (bound > options.limits.max_backend_bytes) return error.BoundaryTrainingRunLimitExceeded;
            self.resident_device_upper_bound_bytes = bound;
        }
        self.backend = try backend_mod.Owner.init(self.backend_budget.allocator(), store, original, selected, options.execution, backend_limits, control);
        errdefer self.backend.deinit();
        self.cb = self.backend.cb;
        self.cb.execution_control = control;
        defer self.cb.execution_control = null;
        var optimizer_config = self.run_plan.optimizerConfig(options.limits.optimizer);
        optimizer_config.execution = options.execution;
        self.optimizer = try controller.Trainer.init(host, &self.cb, selected, optimizer_config);
        errdefer self.optimizer.deinit();
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.boundary-native-trainer.v1\x00");
        hash.update("pinned_all_trainable_zero_loss_fallback_v1\x00");
        if (options.execution == .resident_metal) hash.update("resident_f32_optimizer_v1\x00");
        if (options.attention_profile != .materialized_v1) {
            hash.update("attention_profile\x00");
            hash.update(@tagName(options.attention_profile));
            hash.update("\x00");
        }
        hash.update(&try self.run_plan.fingerprint(scratch));
        if (options.activation_profile != .retained_v1) {
            hash.update("activation_profile\x00");
            hash.update(@tagName(options.activation_profile));
            hash.update("\x00");
        }
        const settings = try std.json.Stringify.valueAlloc(scratch, .{ .model = config, .capacities = options.capacities, .weights = options.weights, .gold_start = options.gold_start, .gold_end = options.gold_end, .gold_hold_fraction = options.gold_hold_fraction, .require_gold_relation_coverage = options.require_gold_relation_coverage, .word_splitter = options.processor.word_splitter, .validators = "native_python312_unicode15_v1" }, .{});
        hash.update(settings);
        self.fingerprint = hash.finalResult();
        var processor_options = options.processor;
        processor_options.control = control;
        var target_options = options.limits.step.targets;
        target_options.gold_capacity = options.capacities.gold_per_query orelse config.head.max_gold_per_query;
        try dataset.preflight(tokenizer, processor_options, target_options, control, null);
        try check(control);
    }

    fn initializeAdapterLayout(self: *const Trainer, a: Allocator, source: bundle.Identity, config: peft.Config, original: []const run.Parameter) !adapters.Layout {
        if (comptime @import("builtin").is_test) {
            if (self.synthetic_fixture) |fixture| return fixture.layout(a, self.model_config, config, original, self.options.limits.peft);
        }
        return adapters.init(a, source.backbone, config, self.options.limits.peft);
    }

    fn initializeAdapterWeights(self: *const Trainer, a: Allocator, descriptor: adapters.Descriptor, base: []const f32, seed: u64) !peft.InitialWeights {
        if (comptime @import("builtin").is_test) {
            if (self.synthetic_fixture) |fixture| return fixture.initialFor(a, descriptor);
        }
        return descriptor.initialize(a, base, seed);
    }

    pub fn deinit(self: *Trainer) void {
        std.debug.assert(!self.busy.load(.acquire));
        const a = self.host_budget.allocator();
        if (self.plan) |*plan| plan.deinit();
        self.validators.deinit();
        a.free(self.order);
        self.optimizer.deinit();
        if (self.adapter_layout) |*layout| layout.deinit();
        self.backend.deinit();
        std.debug.assert(self.host_budget.live == 0 and self.backend_budget.live == 0);
        self.backing.destroy(self);
    }

    pub fn save(self: *Trainer, path: []const u8, control: ?Control) !void {
        try self.enter();
        defer self.leave();
        self.memory_failures.begin(.checkpoint);
        self.optimizer.save(path, self.fingerprint, control) catch |err| return self.memory_failures.report(err);
    }

    pub fn restore(self: *Trainer, path: []const u8, control: ?Control) !void {
        _ = try self.restorePinned(path, null, control);
    }

    pub fn restorePinned(self: *Trainer, path: []const u8, expected_state_sha256: ?[32]u8, control: ?Control) !controller.RestoreReceipt {
        try self.enter();
        defer self.leave();
        self.memory_failures.begin(.restore);
        return self.restorePinnedOwned(path, expected_state_sha256, control) catch |err| return self.memory_failures.report(err);
    }

    fn restorePinnedOwned(self: *Trainer, path: []const u8, expected_state_sha256: ?[32]u8, control: ?Control) !controller.RestoreReceipt {
        if (self.options.execution == .resident_metal) {
            var upload: usize = 2 * 1024 * 1024;
            for (self.optimizer.owner.regular_params.items) |slot| upload = @max(upload, try std.math.mul(usize, slot.weights.len, 4));
            try self.admitResident(try std.math.add(usize, self.optimizer.owner.device_trainable_bytes, upload));
        }
        const Validate = struct {
            fn apply(raw: ?*const anyopaque, identity: controller.Identity, accumulated: u32) !void {
                const plan: *const run.Plan = @ptrCast(@alignCast(raw.?));
                _ = try plan.position(identity, accumulated);
            }
        };
        try self.optimizer.restoreValidated(path, self.fingerprint, control, .{ .context = &self.run_plan, .validate = Validate.apply, .expected_state_sha256 = expected_state_sha256 });
        if (self.plan) |*plan| plan.deinit();
        self.plan = null;
        self.plan_key = null;
        self.host_budget.allocator().free(self.order);
        self.order = &.{};
        self.order_epoch = null;
        return self.optimizer.last_restore_receipt.?;
    }

    pub fn position(self: *Trainer) !run.Position {
        try self.enter();
        defer self.leave();
        return self.run_plan.position(self.optimizer.identity(), self.optimizer.owner.accum_count);
    }

    /// The caller reserves export scratch in addition to this trainer's live
    /// budgets. The busy lock keeps all slot values at one immutable epoch
    /// through validation, streaming, and atomic directory publication.
    pub fn exportSnapshot(self: *Trainer, a: Allocator, io: std.Io, source: *const source_mod.Source, output: []const u8, source_locator: ?[]const u8, limits: export_mod.Limits, control: ?Control) !export_mod.Result {
        try self.enter();
        defer self.leave();
        self.memory_failures.begin(.export_snapshot);
        return self.exportSnapshotOwned(a, io, source, output, source_locator, limits, control) catch |err| return self.memory_failures.report(err);
    }

    fn exportSnapshotOwned(self: *Trainer, a: Allocator, io: std.Io, source: *const source_mod.Source, output: []const u8, source_locator: ?[]const u8, limits: export_mod.Limits, control: ?Control) !export_mod.Result {
        if (!std.meta.eql(source.identity, self.run_plan.source)) return error.GlinerBoundaryArtifactMismatch;
        try self.optimizer.ensureHostState(control);
        const slots = try a.alloc(export_mod.Slot, self.optimizer.owner.regular_params.items.len);
        defer a.free(slots);
        for (slots, self.optimizer.owner.regular_params.items) |*slot, parameter| slot.* = .{ .name = parameter.name, .dimensions = parameter.dims, .values = parameter.weights };
        return export_mod.exportSnapshot(a, io, source, output, .{
            .mode = self.run_plan.config.mode,
            .slots = slots,
            .adapter_layout = if (self.adapter_layout) |*layout| layout else null,
            .base_model_name_or_path = source_locator,
            .provenance = .{ .run_fingerprint = self.fingerprint, .dataset_sha256 = self.dataset.sha256, .schemas_sha256 = self.dataset.schemas_sha256, .optimizer_identity = self.optimizer.identity(), .accumulated_microbatches = self.optimizer.owner.accum_count },
        }, limits, control);
    }

    /// Estimate the final flushed artifact using current parameter metadata.
    /// Dimensions and slot inventory are stable across updates. Clearing the
    /// local estimate's accumulation field neither updates the owner nor allows
    /// export of an unfinished window; this method cannot publish files.
    pub fn estimateExportSnapshot(self: *Trainer, a: Allocator, source: *const source_mod.Source, source_locator: ?[]const u8, limits: export_mod.Limits, control: ?Control) !export_mod.Estimate {
        try self.enter();
        defer self.leave();
        if (!std.meta.eql(source.identity, self.run_plan.source)) return error.GlinerBoundaryArtifactMismatch;
        const slots = try a.alloc(export_mod.Slot, self.optimizer.owner.regular_params.items.len);
        defer a.free(slots);
        for (slots, self.optimizer.owner.regular_params.items) |*slot, parameter| slot.* = .{ .name = parameter.name, .dimensions = parameter.dims, .values = parameter.weights };
        return export_mod.estimateSnapshot(a, source, .{
            .mode = self.run_plan.config.mode,
            .slots = slots,
            .adapter_layout = if (self.adapter_layout) |*layout| layout else null,
            .base_model_name_or_path = source_locator,
            .provenance = .{ .run_fingerprint = self.fingerprint, .dataset_sha256 = self.dataset.sha256, .schemas_sha256 = self.dataset.schemas_sha256, .optimizer_identity = self.optimizer.identity(), .accumulated_microbatches = 0 },
        }, limits, control);
    }

    /// One batch or one end-of-epoch partial flush. Null means the configured
    /// horizon is complete. Report production and checkpoint writes happen
    /// outside this transaction so callers can distinguish update/publication.
    pub fn next(self: *Trainer, control: ?Control) !?Report {
        try self.enter();
        defer self.leave();
        self.memory_failures.begin(.batch_preparation);
        return self.nextOwned(control) catch |err| return self.memory_failures.report(err);
    }

    fn nextOwned(self: *Trainer, control: ?Control) !?Report {
        try check(control);
        const pos = try self.run_plan.position(self.optimizer.identity(), self.optimizer.owner.accum_count);
        if (pos.complete) return null;
        if (pos.requires_flush) {
            self.memory_failures.begin(.optimizer);
            if (self.options.activation_profile == .layer_recompute_v1) try self.admitRecomputedFlush();
            // A restored epoch-end partial window can flush before a plan has
            // been built. Admit the pending optimizer state independently of
            // the forward/backward admission performed by ensurePlan.
            if (self.options.execution == .resident_metal) {
                const update = try self.optimizer.residentUpdateAdmission(self.host_budget.allocator());
                try self.admitResident(update.device_upper_bound_bytes);
            }
            const updated = try self.optimizer.flush(self.optimizer.identity(), control);
            return .{ .epoch = pos.epoch - 1, .batch = self.run_plan.batches_per_epoch, .examples = 0, .terms = null, .optimizer = updated, .host_peak_bytes = self.host_budget.peak, .backend_peak_bytes = self.backend_budget.peak, .resident_device_upper_bound_bytes = self.resident_device_upper_bound_bytes };
        }
        self.cb.execution_control = control;
        defer self.cb.execution_control = null;
        self.validators.options.compile_options.control = control;
        self.validators.options.match_options.control = control;
        self.validators.match_steps = 0;
        defer {
            self.validators.options.compile_options.control = null;
            self.validators.options.match_options.control = null;
        }
        const a = self.host_budget.allocator();
        if (self.order_epoch != pos.epoch) {
            a.free(self.order);
            self.order = &.{};
            self.order_epoch = null;
            self.order = try self.run_plan.epochOrder(a, pos.epoch, control);
            self.order_epoch = pos.epoch;
        }
        const samples = try a.alloc(data.Sample, pos.count);
        var initialized: usize = 0;
        defer {
            for (samples[0..initialized]) |*sample| sample.deinit();
            a.free(samples);
        }
        var caller_budget = Budget{ .backing = a, .limit = self.options.limits.max_recomputed_batch_scratch_bytes };
        var caller_observer = MemoryObserver{ .failures = &self.memory_failures, .domain = .host };
        caller_observer.attach(&caller_budget);
        caller_budget.allocation_failed = MemoryObserver.observeDeclared;
        defer std.debug.assert(caller_budget.live == 0);
        var arena = std.heap.ArenaAllocator.init(if (self.options.activation_profile == .layer_recompute_v1) caller_budget.allocator() else a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const schemas = try scratch.alloc(*const schema.CompiledSchema, pos.count);
        const annotations = try scratch.alloc(targets.Annotations, pos.count);
        const inputs = try scratch.alloc(processor.Item, pos.count);
        for (samples, schemas, annotations, inputs, self.order[pos.offset..][0..pos.count]) |*sample, *s, *annotation, *input, index| {
            sample.* = try self.dataset.sample(index, control, null);
            initialized += 1;
            s.* = &sample.schema;
            annotation.* = sample.annotations;
            input.* = .{ .text = sample.row.text, .schema = &sample.schema };
        }
        var processor_options = self.options.processor;
        processor_options.control = control;
        var prepared = try processor.prepare(a, self.tokenizer, inputs, processor_options);
        defer prepared.deinit();
        self.memory_failures.begin(.graph_preparation);
        try self.ensurePlan(&prepared, schemas, control);
        self.memory_failures.begin(.batch_preparation);
        const plan = &self.plan.?;
        const instruction_control_readback_upper_bound = try plan.instructionControlReadbackUpperBound();
        const identity = self.optimizer.identity();
        const draws = try scratch.alloc(f32, @as(usize, plan.encoder.layout.batch) * plan.encoder.layout.queries * plan.gold_capacity);
        var injection_rng = run.Random.init(self.run_plan.config.seed, identity.microbatch_step, "gold_injection");
        for (draws) |*value| value.* = injection_rng.uniform();
        const negative_draws = try scratch.alloc(f32, @as(usize, plan.encoder.layout.batch) * plan.encoder.layout.queries);
        var negative_rng = run.Random.init(self.run_plan.config.seed, identity.microbatch_step, "negative_queries");
        for (negative_draws) |*value| value.* = negative_rng.uniform();
        const context = step.StepContext{ .identity = .{ .binding = self.fingerprint, .optimizer_step = identity.optimizer_step, .microbatch = identity.microbatch_step }, .replay = .{ .seed = self.run_plan.config.seed, .micro_batch = identity.microbatch_step }, .progress = .{ .optimizer_step = identity.optimizer_step, .total_optimizer_steps = self.run_plan.total_optimizer_steps, .gold_start = self.options.gold_start, .gold_end = self.options.gold_end, .gold_hold_fraction = self.options.gold_hold_fraction }, .weights = self.options.weights, .injection_draws = draws, .negative_query_draws = negative_draws, .require_gold_relation_coverage = self.options.require_gold_relation_coverage };
        var result = blk: {
            self.memory_failures.begin(.forward_backward);
            var bindings = try self.optimizer.bind(plan.graph, control);
            defer bindings.deinit();
            var frozen = try self.backend.bindFrozen(plan.graph, control);
            defer frozen.deinit();
            const parameters = if (frozen.inputs.len == 0) bindings.inputs else combine: {
                const all = try scratch.alloc(@import("../graph/interpreter.zig").RuntimeInput, bindings.inputs.len + frozen.inputs.len);
                @memcpy(all[0..bindings.inputs.len], bindings.inputs);
                @memcpy(all[bindings.inputs.len..], frozen.inputs);
                break :combine all;
            };
            try self.checkRecomputedHost();
            break :blk try plan.run(&self.cb, parameters, &prepared, schemas, annotations, context, control);
        };
        var result_live = true;
        defer if (result_live) result.deinit(&self.cb);
        const terms = result.terms;
        const coverage = result.coverage;
        const decisions = result.decision_fingerprint;
        const transfers = result.transfers;
        self.memory_failures.begin(.gradient_readback);
        const slots = self.optimizer.owner.regular_params.items;
        const routes = try scratch.alloc(GradientRoute, slots.len);
        var source_has_gradient = false;
        for (slots, routes) |slot, *route| {
            route.* = try gradientRoute(plan, &result, slot.name);
            source_has_gradient = source_has_gradient or route.kind != .absent;
        }
        // Trainer._backward_one uses model._zero_loss only when the complete
        // objective lacks a trainable path. Optional-head zero touches already
        // establish such a path; an absent classifier alone does not.
        const zero_loss_fallback = !source_has_gradient;
        const optimizer_loss: f32 = if (zero_loss_fallback) 0 else terms.total;
        if (zero_loss_fallback) {
            for (routes) |*route| route.* = .{ .kind = .computed_zero };
        }
        if (comptime @import("builtin").is_test) {
            if (self.synthetic_fixture) |fixture| if (fixture.observe) |observe| {
                const gradients = try scratch.alloc(synthetic.Gradient, slots.len);
                for (slots, routes, gradients) |slot, route, *gradient| gradient.* = .{
                    .name = slot.name,
                    .kind = route.kind,
                    .tensor = if (route.kind != .absent) if (route.gradient_index) |index| result.backward.gradients.outputs[index] else null else null,
                    .elements = slot.weights.len,
                };
                try observe(fixture.observer_context, .{ .backend = &self.cb, .prepared = &prepared, .result = &result, .gradients = gradients, .optimizer_loss = optimizer_loss, .zero_loss_fallback = zero_loss_fallback });
            };
        }
        var gradient_control_bytes: usize = 0;
        const updated = if (self.options.execution == .native) cpu: {
            var gradients = std.ArrayListUnmanaged(controller.Gradient).empty;
            for (slots, routes) |slot, route| {
                if (route.kind == .absent) continue;
                const values = if (route.gradient_index) |index|
                    try self.cb.toFloat32(result.backward.gradients.outputs[index], scratch)
                else if (route.kind == .computed_zero) zero: {
                    const zeros = try scratch.alloc(f32, slot.weights.len);
                    @memset(zeros, 0);
                    break :zero zeros;
                } else return error.MissingBoundaryTrainingGradient;
                if (values.len != slot.weights.len) return error.TrainingBindingShapeMismatch;
                if (route.kind == .computed_zero) for (values) |value| if (value != 0) return error.InvalidBoundaryTrainingGradientPresence;
                try gradients.append(scratch, .{ .name = slot.name, .values = values });
            }
            result.deinit(&self.cb);
            result_live = false;
            try plan.advanceParameterEpoch();
            self.memory_failures.begin(.optimizer);
            break :cpu try self.optimizer.submit(identity, optimizer_loss, gradients.items, control);
        } else gpu: {
            var gradients = std.ArrayListUnmanaged(controller.ResidentGradient).empty;
            for (slots, routes) |slot, route| {
                if (route.kind == .absent) continue;
                if (route.kind == .computed_zero) {
                    if (route.gradient_index) |found| {
                        const norm = try self.cb.residentTrainingNorm(&.{.{ .tensor = result.backward.gradients.outputs[found], .elem_count = slot.weights.len }}, .{});
                        if (!norm.finite or norm.norm != 0) return error.InvalidBoundaryTrainingGradientPresence;
                        gradient_control_bytes = try std.math.add(usize, gradient_control_bytes, norm.download_bytes);
                    }
                    try gradients.append(scratch, .{ .name = slot.name, .value = .zero });
                } else {
                    const found = route.gradient_index orelse return error.MissingBoundaryTrainingGradient;
                    try gradients.append(scratch, .{ .name = slot.name, .value = .{ .tensor = result.backward.gradients.outputs[found] } });
                }
            }
            // Forward/backward and parameter leases have ended. The result
            // still owns its gradient CTs throughout atomic optimizer staging.
            try plan.advanceParameterEpoch();
            self.memory_failures.begin(.optimizer);
            break :gpu try self.optimizer.submitResident(identity, optimizer_loss, gradients.items, control);
        };
        return .{ .epoch = pos.epoch, .batch = pos.batch, .examples = pos.count, .terms = terms, .coverage = coverage, .optimizer = updated, .decision_fingerprint = decisions, .host_peak_bytes = self.host_budget.peak, .backend_peak_bytes = self.backend_budget.peak, .resident_device_upper_bound_bytes = self.resident_device_upper_bound_bytes, .transfers = transfers, .resident_gradient_control_bytes = gradient_control_bytes, .resident_instruction_control_readback_upper_bound_bytes = instruction_control_readback_upper_bound, .zero_loss_fallback = zero_loss_fallback };
    }

    fn ensurePlan(self: *Trainer, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, control: ?Control) !void {
        try check(control);
        const a = self.host_budget.allocator();
        const layout = try encoder.layoutFromPrepared(&self.model_config, prepared, self.options.limits.step.encoder);
        const layout_json = try std.json.Stringify.valueAlloc(a, layout, .{});
        defer a.free(layout_json);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(layout_json);
        for (schemas) |s| hash.update(&s.fingerprint);
        const key = hash.finalResult();
        if (self.plan_key) |cached| if (std.mem.eql(u8, &cached, &key)) {
            // Equal tensor geometry can still carry different UTF-8 text,
            // schema and annotation allocations. Re-admit the live parent.
            try self.checkRecomputedHost();
            return;
        };
        if (self.plan) |*old| old.deinit();
        self.plan = null;
        self.plan_key = null;
        self.recomputed_future_host_bytes = 0;
        self.validators.deinit();
        self.validators = regex.Context.init(a, self.options.regex);
        self.validators.options.compile_options.control = control;
        self.validators.options.match_options.control = control;
        for (schemas) |compiled| {
            for (compiled.schema.entities) |entity| for (entity.validators) |validator| try regex.Context.validateCompile(&self.validators, validator);
            for (compiled.schema.structures) |structure| for (structure.fields) |field| for (field.validators) |validator| try regex.Context.validateCompile(&self.validators, validator);
        }
        var step_limits = self.options.limits.step;
        step_limits.targets.regex_context = &self.validators;
        step_limits.targets.validate_value_fn = regex.Context.validateValue;
        step_limits.recomputation.max_backend_bytes = @min(step_limits.recomputation.max_backend_bytes, self.options.limits.max_backend_bytes);
        step_limits.recomputation.max_host_bytes = @min(step_limits.recomputation.max_host_bytes, try std.math.add(usize, self.options.source_reserved_bytes, self.options.limits.max_host_bytes));
        var plan = try step.buildWithProfiles(a, self.model_config, prepared, schemas, self.options.capacities, .training, self.options.attention_profile, self.options.activation_profile, step_limits);
        errdefer plan.deinit();
        if (self.adapter_layout) |adapter_layout| {
            const selected = try adapter_layout.targetsForGraph(a, plan.graph);
            defer a.free(selected);
            if (selected.len != 0) {
                var config = adapter_layout.config;
                config.targets = selected;
                try plan.applyPeft(config, self.options.limits.peft);
            }
        }
        var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
        defer wrt.deinit(a);
        for (plan.graph.parameters.items) |id| {
            const name = plan.graph.parameterName(plan.graph.node(id));
            for (self.optimizer.owner.regular_params.items) |slot| if (std.mem.eql(u8, name, slot.name)) {
                try wrt.append(a, id);
                break;
            };
        }
        var differentiation = self.options.limits.differentiation;
        differentiation.allow_no_gradients = true;
        differentiation.execution = if (self.options.execution == .resident_metal) .resident_metal else .native;
        if (self.options.execution == .resident_metal) differentiation.resident.max_device_bytes = @min(differentiation.resident.max_device_bytes, self.options.limits.max_backend_bytes);
        try plan.finalizeWithActivationProfile(wrt.items, differentiation, self.options.activation_profile);
        if (self.options.activation_profile == .layer_recompute_v1) {
            try self.admitRecomputed(&plan, control);
        } else if (self.options.execution == .resident_metal) {
            const execution = try plan.transferAdmission();
            const transaction = try self.optimizer.residentUpdateAdmission(a);
            // Conservatively retain the complete Step bound while staging an
            // all-slot optimizer flush. Actual lifetimes are often disjoint.
            try self.admitResident(try std.math.add(usize, execution.device_upper_bound_bytes, transaction.device_upper_bound_bytes));
        }
        try check(control);
        self.plan = plan;
        self.plan_key = key;
    }

    fn admitRecomputed(self: *Trainer, plan: *step.Plan, control: ?Control) !void {
        try check(control);
        _ = try plan.recomputedHeadAdmission();
        const regional = &plan.recomputation.?.graph.regional;
        const host_live = self.host_budget.live;
        if (regional.budget.live > host_live) return error.InvalidRecomputeAdmission;
        var owner = @import("../graph/recomputed_training.zig").EnclosingAdmission{
            // Regional compiled allocations are already inside the parent.
            // Replace that live portion with its complete bounded reservation.
            .fixed_host_bytes = try std.math.add(usize, self.options.source_reserved_bytes, host_live - regional.budget.live),
            // The caller arena also includes its backing allocation growth.
            // Its full cap remains reserved even if some capacity is live in H.
            .head_host_metadata_bytes = try std.math.add(usize, self.options.limits.step.max_step_host_bytes, self.options.limits.max_recomputed_batch_scratch_bytes),
            .head_work = self.options.limits.step.max_step_work,
        };
        if (self.options.execution == .resident_metal) {
            const update = try self.optimizer.residentUpdateAdmission(self.host_budget.allocator());
            owner.fixed_backend_bytes = try std.math.add(usize, self.options.limits.max_backend_host_bytes, try std.math.add(usize, self.backend.admission.frozen_device_bytes, self.optimizer.owner.device_trainable_bytes));
            owner.optimizer_transaction_backend_bytes = update.device_upper_bound_bytes;
            owner.optimizer_transaction_host_bytes = update.host_metadata_upper_bound_bytes;
            owner.optimizer_transaction_work = update.total_work;
        } else {
            const update = try self.optimizer.nativeUpdateAdmission();
            // Frozen native payloads borrow Source; mutable weights/moments
            // already live in the host parent. Backend CT/shape metadata stays
            // under the independent backend allocator and this reservation.
            const parameter_bindings = try std.math.mul(usize, update.elements, @sizeOf(f32));
            // Controller.bind creates owned native copies after this preflight;
            // the host optimizer arrays are a distinct, already-live owner.
            owner.fixed_backend_bytes = try std.math.add(usize, parameter_bindings, try std.math.add(usize, self.backend_budget.live, self.options.limits.max_backend_host_bytes));
            owner.optimizer_transaction_host_bytes = update.host_upper_bound_bytes;
            owner.optimizer_transaction_work = update.total_work;
        }
        const admitted = try plan.sealRecomputedAdmission(owner);
        const local_host = admitted.host_upper_bound_bytes - self.options.source_reserved_bytes;
        if (local_host > self.options.limits.max_host_bytes or local_host < host_live) return error.BoundaryTrainingRunLimitExceeded;
        self.recomputed_future_host_bytes = local_host - host_live;
        if (self.options.execution == .resident_metal) self.resident_device_upper_bound_bytes = @max(self.resident_device_upper_bound_bytes, admitted.backend_upper_bound_bytes);
        try self.checkRecomputedHost();
        try check(control);
    }

    fn checkRecomputedHost(self: *const Trainer) !void {
        if (self.options.activation_profile != .layer_recompute_v1) return;
        const bound = try std.math.add(usize, self.host_budget.live, self.recomputed_future_host_bytes);
        if (bound > self.options.limits.max_host_bytes) return error.BoundaryTrainingRunLimitExceeded;
    }

    /// A restored partial window can reach the epoch flush before any graph
    /// exists. Its optimizer still requires host and work admission first.
    fn admitRecomputedFlush(self: *Trainer) !void {
        const limits = self.options.limits;
        const host_bytes, const work = if (self.options.execution == .resident_metal) resident: {
            const update = try self.optimizer.residentUpdateAdmission(self.host_budget.allocator());
            break :resident .{ update.host_metadata_upper_bound_bytes, update.total_work };
        } else native_update: {
            const update = try self.optimizer.nativeUpdateAdmission();
            break :native_update .{ update.host_upper_bound_bytes, update.total_work };
        };
        const local = try std.math.add(usize, self.host_budget.live, host_bytes);
        const aggregate = try std.math.add(usize, local, self.options.source_reserved_bytes);
        if (local > limits.max_host_bytes or aggregate > limits.step.recomputation.max_host_bytes or
            work > limits.step.recomputation.max_total_work) return error.BoundaryTrainingRunLimitExceeded;
    }

    fn admitResident(self: *Trainer, additional_bytes: usize) !void {
        const fixed = try std.math.add(usize, self.backend.admission.frozen_device_bytes, self.optimizer.owner.device_trainable_bytes);
        const bound = try std.math.add(usize, fixed, try std.math.add(usize, self.options.limits.max_backend_host_bytes, additional_bytes));
        if (bound > self.options.limits.max_backend_bytes) return error.BoundaryTrainingRunLimitExceeded;
        self.resident_device_upper_bound_bytes = @max(self.resident_device_upper_bound_bytes, bound);
    }

    fn enter(self: *Trainer) !void {
        if (self.busy.cmpxchgStrong(false, true, .acquire, .monotonic) != null) return error.BoundaryTrainingJobBusy;
    }
    fn leave(self: *Trainer) void {
        self.busy.store(false, .release);
    }
};

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

const GradientRoute = struct { kind: step.GradientPresence, gradient_index: ?usize = null };

/// Resolve against the complete persistent slot inventory. A schema can omit
/// an optional head's entire PEFT graph while upstream _head_touch still gives
/// every parameter in that head an explicit zero gradient.
fn gradientRoute(plan: *const step.Plan, result: *const step.StepResult, name: []const u8) !GradientRoute {
    var route = GradientRoute{ .kind = .absent };
    var found = false;
    for (result.presence) |presence| {
        const parameter = plan.graph.node(presence.parameter);
        if (!std.mem.eql(u8, plan.graph.parameterName(parameter), name)) continue;
        if (found) return error.DuplicateTrainingGradient;
        found = true;
        route = .{ .kind = presence.kind, .gradient_index = std.mem.indexOfScalar(ml.NodeId, result.backward.parameter_ids, presence.parameter) };
        if (route.kind == .computed and route.gradient_index == null) return error.MissingBoundaryTrainingGradient;
    }
    if (route.kind == .absent and step.isTouchParameter(name, plan.config.head)) route.kind = .computed_zero;
    return route;
}
