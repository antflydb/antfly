// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One admitted native training job. Each invocation owns a new output
//! directory; resume reads the durable optimizer checkpoint into a fresh owner.
//! The checkpoint is authoritative when progress output is interrupted.
const std = @import("std");
const platform = @import("antfly_platform");
const run = @import("gliner_boundary_run.zig");
const native = @import("gliner_boundary_native_trainer.zig");
const limits_mod = @import("gliner_boundary_training_limits.zig");
const source_mod = @import("gliner_boundary_training_source.zig");
const dataset_mod = @import("gliner_boundary_dataset.zig");
const peft = @import("gliner_boundary_peft_graph.zig");
const step = @import("gliner_boundary_train_step.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const memory = @import("../runtime/tier/memory.zig");
const snapshots = @import("../runtime/file_snapshot.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const publication = @import("../gliner_boundary_export.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Identity = @import("seeded_gradient_trainer.zig").Identity;
const export_mod = @import("gliner_boundary_training_export.zig");
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;

pub const Memory = struct {
    // Reserve the job envelope from the native host default; adding it on top
    // would reject the multilingual source under the same combined ceiling.
    host_bytes: usize = 6 * 1024 * mib - 128 * mib,
    backend_bytes: usize = 4 * 1024 * mib,
    backend_metadata_bytes: usize = 128 * mib,
    combined_bytes: usize = 12 * 1024 * mib,
    optimizer_state_bytes: usize = 4 * 1024 * mib,
    optimizer_transaction_bytes: usize = 4 * 1024 * mib,
    job_bytes: usize = 128 * mib,
};
pub const Tokenization = struct {
    max_text_words: usize = 128,
    max_sequence_tokens: usize = 512,
    max_queries: usize = 64,
    word_splitter: processor.WordSplitter = .whitespace,
};
pub const Config = struct {
    version: u32,
    source_dir: []const u8,
    train_file: []const u8,
    output_dir: []const u8,
    calibration_file: ?[]const u8 = null,
    test_file: ?[]const u8 = null,
    resume_from: ?[]const u8 = null,
    expected_restore_state_sha256: ?[32]u8 = null,
    expected_source: ?bundle.Identity = null,
    run: run.Config,
    execution: @import("seeded_gradient_trainer.zig").Execution = .native,
    /// Versioned mathematics and dropout replay, included in checkpoints.
    /// Resource ceilings remain in training_limits and memory.
    attention_profile: step.AttentionProfile = .materialized_v1,
    activation_profile: step.ActivationProfile = .retained_v1,
    peft: ?peft.Config = null,
    tokenization: Tokenization = .{},
    capacities: step.Capacities = .{},
    weights: objectives.Weights = .{},
    gold_start: f32 = 1,
    gold_end: f32 = 0.25,
    gold_hold_fraction: f32 = 0.15,
    require_gold_relation_coverage: bool = false,
    source_limits: source_mod.Limits = .{},
    export_limits: export_mod.Limits = .{},
    dataset_limits: dataset_mod.Limits = .{},
    memory: Memory = .{},
    training_limits: limits_mod.Config = .{},
    checkpoint_every_microbatches: u32 = 100,
    timeout_seconds: u32 = 4 * 60 * 60,
    max_progress_bytes: usize = 64 * mib,
    disk_headroom_bytes: u64 = 256 * mib,
};
pub const Result = struct { version: u32 = 1, status: enum { paused, complete }, identity: Identity, accumulated_microbatches: u32, run_fingerprint: [32]u8, state_sha256: [32]u8, portable_model: ?export_mod.Result = null };
pub const Execution = struct {
    /// A cooperative pause saves the exact unfinished accumulation window.
    /// This invocation limit does not change the training schedule/fingerprint.
    stop_after_microbatches: ?u64 = null,
    pause_context: ?*const anyopaque = null,
    pause_requested: ?*const fn (?*const anyopaque) bool = null,
    report_context: ?*anyopaque = null,
    report_fn: ?*const fn (?*anyopaque, native.Report) anyerror!void = null,
};

pub const ConfigSnapshot = struct {
    parsed: std.json.Parsed(Config),
    sha256: [32]u8,
    size_bytes: u64,

    pub fn deinit(self: *ConfigSnapshot) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

/// Hash and parse the same bounded owned bytes from one file descriptor. A
/// supervising parent and worker can require exact configuration identity;
/// changing the path afterward cannot alter this returned parsed snapshot.
pub fn loadConfigSnapshot(a: Allocator, io: std.Io, path: []const u8) !ConfigSnapshot {
    const bytes = try snapshots.read(a, io, std.Io.Dir.cwd(), path, 64 * 1024, null);
    defer a.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return .{ .parsed = try parse(a, bytes), .sha256 = digest, .size_bytes = @intCast(bytes.len) };
}

pub fn loadConfig(a: Allocator, io: std.Io, path: []const u8) !std.json.Parsed(Config) {
    // Transfer parsed ownership to the existing API's caller.
    return (try loadConfigSnapshot(a, io, path)).parsed;
}

pub fn parse(a: Allocator, bytes: []const u8) !std.json.Parsed(Config) {
    if (bytes.len == 0 or bytes.len > 64 * 1024) return error.BoundaryTrainingJobConfigLimitExceeded;
    // Typed parsing still allocates parser nesting before rejecting an unknown
    // field. Bound that work independently of the semantic configuration.
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes) |byte| {
        if (quoted) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') quoted = false;
        } else switch (byte) {
            '"' => quoted = true,
            '{', '[' => {
                depth += 1;
                if (depth > 32) return error.BoundaryTrainingJobConfigLimitExceeded;
            },
            '}', ']' => {
                if (depth == 0) return error.InvalidBoundaryTrainingJob;
                depth -= 1;
            },
            else => {},
        }
    }
    const parsed = try std.json.parseFromSlice(Config, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try validate(parsed.value);
    return parsed;
}

fn pathValid(path: []const u8) bool {
    if (!std.fs.path.isAbsolute(path) or path.len > std.fs.max_path_bytes or path.len == 1) return false;
    for (path) |byte| if (byte < 32 or byte == 127) return false;
    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |part| if (std.mem.eql(u8, part, "..") or std.mem.eql(u8, part, ".")) return false;
    return true;
}
pub fn validate(config: Config) !void {
    try limits_mod.validate(config.training_limits);
    if (config.version != 1 or config.checkpoint_every_microbatches == 0 or config.timeout_seconds == 0 or config.max_progress_bytes < 8192 or config.memory.job_bytes < 4 * mib) return error.InvalidBoundaryTrainingJob;
    if (try std.math.add(usize, config.export_limits.max_scratch_bytes, 4 * mib) > config.memory.job_bytes) return error.BoundaryTrainingRunLimitExceeded;
    for ([_][]const u8{ config.source_dir, config.train_file, config.output_dir }) |path| if (!pathValid(path)) return error.InvalidBoundaryTrainingJobPath;
    for ([_]?[]const u8{ config.calibration_file, config.test_file, config.resume_from }) |path| if (path) |value| if (!pathValid(value)) return error.InvalidBoundaryTrainingJobPath;
    if (config.run.adapter_config_sha256 != null) return error.InvalidBoundaryTrainingJob;
    if (config.resume_from == null and config.expected_restore_state_sha256 != null) return error.InvalidBoundaryTrainingJob;
    const adapter = config.run.mode == .lora or config.run.mode == .dora;
    if (adapter != (config.peft != null)) return error.InvalidBoundaryTrainingJob;
    if (config.peft) |p| {
        if (p.mode != .train or (p.kind == .dora) != (config.run.mode == .dora)) return error.InvalidBoundaryTrainingJob;
        try peft.validateConfig(p, .{});
    }
    if (config.expected_source) |expected| if (expected.precision != .fp32) return error.QuantizedBoundaryTrainingUnsupported;
    if (config.memory.host_bytes == 0 or config.memory.backend_bytes == 0 or config.memory.combined_bytes == 0 or config.memory.optimizer_state_bytes == 0 or config.memory.optimizer_transaction_bytes == 0 or config.dataset_limits.max_host_bytes == 0) return error.InvalidBoundaryTrainingJob;
    if (config.execution == .resident_metal and (config.memory.backend_metadata_bytes == 0 or config.memory.backend_metadata_bytes >= config.memory.backend_bytes)) return error.InvalidBoundaryTrainingJob;
    if (config.run.batch_size == 0 or config.run.batch_size > 64 or config.tokenization.max_text_words == 0 or config.tokenization.max_sequence_tokens == 0 or config.tokenization.max_queries == 0) return error.InvalidBoundaryTrainingJob;
    const input = config.training_limits.encoder.input;
    if (config.run.batch_size > input.max_batch or config.tokenization.max_text_words > input.max_text_words or
        config.tokenization.max_sequence_tokens > input.max_sequence_tokens or config.tokenization.max_queries > input.max_queries or
        try std.math.mul(usize, config.run.batch_size, config.tokenization.max_sequence_tokens) > input.max_batch_tokens)
        return error.BoundaryTrainingLimitsExceeded;
}

/// Resource-only settings are forwarded separately from semantic run options.
/// They are recorded in config receipts and intentionally do not enter the
/// optimizer/run fingerprint: an admitted resume can raise resource ceilings.
pub fn trainerLimits(config: Config) !native.Limits {
    return limits_mod.apply(config.training_limits, .{
        .max_host_bytes = config.memory.host_bytes,
        .max_backend_bytes = config.memory.backend_bytes,
        .max_backend_host_bytes = config.memory.backend_metadata_bytes,
        .max_combined_bytes = config.memory.combined_bytes,
        .optimizer = .{ .max_state_bytes = config.memory.optimizer_state_bytes, .max_transaction_bytes = config.memory.optimizer_transaction_bytes },
    });
}

pub fn admissionBytes(config: Config, source_reserved: usize) !usize {
    const amounts = try admissionAmounts(config, source_reserved);
    return std.math.add(usize, amounts.hostTotalBytes(), amounts.backendTotalBytes());
}

fn admissionAmounts(config: Config, source_reserved: usize) !memory.AdmissionAmounts {
    try validate(config);
    const fixed = try std.math.add(usize, source_reserved, config.memory.job_bytes);
    const datasets: usize = 1 + @as(usize, @intFromBool(config.calibration_file != null)) + @as(usize, @intFromBool(config.test_file != null));
    // Declared holdouts are released after preflight and before the mutable
    // trainer is built. Charge the larger phase, never sum disjoint lifetimes.
    const preflight = try std.math.add(usize, fixed, try std.math.mul(usize, datasets, config.dataset_limits.max_host_bytes));
    const training = try std.math.add(usize, fixed, try std.math.add(usize, config.dataset_limits.max_host_bytes, try std.math.add(usize, config.memory.host_bytes, config.memory.backend_bytes)));
    const amounts: memory.AdmissionAmounts = if (config.execution == .native)
        .{ .host_scratch_bytes = @max(preflight, training) }
    else
        .{
            .host_scratch_bytes = @max(preflight, try std.math.add(usize, training - config.memory.backend_bytes, config.memory.backend_metadata_bytes)),
            .backend_scratch_bytes = config.memory.backend_bytes - config.memory.backend_metadata_bytes,
        };
    const total = try std.math.add(usize, amounts.hostTotalBytes(), amounts.backendTotalBytes());
    if (total > config.memory.combined_bytes) return error.BoundaryTrainingRunLimitExceeded;
    return amounts;
}

/// Caller supplies the process-wide admission owner; standalone CLI creates
/// exactly one. Library/server integrations share their existing controller.
pub fn execute(a: Allocator, io: std.Io, config: Config, admission: *memory.AdmissionController, execution: Execution, outer_control: ?Control) !Result {
    var failures = native.MemoryFailures{};
    failures.begin(.job);
    var observer = native.MemoryObserver{ .failures = &failures, .domain = .job };
    var budget = Budget{ .backing = a, .limit = config.memory.job_bytes };
    observer.attach(&budget);
    defer std.debug.assert(budget.live == 0);
    return executeOwned(a, io, config, admission, execution, outer_control, &budget) catch |err| return failures.report(err);
}

fn executeOwned(a: Allocator, io: std.Io, config: Config, admission: *memory.AdmissionController, execution: Execution, outer_control: ?Control, budget: *Budget) !Result {
    try validate(config);
    if (comptime !@import("build_options").enable_metal) if (config.execution == .resident_metal) return error.UnsupportedBoundaryTrainingBackend;
    var bounded_control = outer_control orelse Control{};
    bounded_control.io = io;
    const deadline = try std.math.add(u64, platform.time.monotonicNs(), try std.math.mul(u64, config.timeout_seconds, std.time.ns_per_s));
    bounded_control.deadline_ns = @min(deadline, bounded_control.deadline_ns orelse deadline);
    const control: ?Control = bounded_control;
    try check(control);
    // Reject reuse before acquiring a model reservation or reading datasets.
    // createDir below remains the exclusive publication guard against races.
    if (std.Io.Dir.cwd().access(io, config.output_dir, .{})) |_| {
        return error.PathAlreadyExists;
    } else |err| if (err != error.FileNotFound) return err;
    const source_reserved = try source_mod.reservation(io, config.source_dir, config.source_limits, control);
    const combined = try admissionBytes(config, source_reserved);
    var lease = try admission.tryAcquire(if (config.execution == .resident_metal) .gpu else .cpu, .{ .host_limit_bytes = config.memory.combined_bytes, .backend_limit_bytes = config.memory.backend_bytes, .combined_limit_bytes = config.memory.combined_bytes }, try admissionAmounts(config, source_reserved), true);
    defer lease.release();
    const scratch = budget.allocator();
    var train = try openDataset(a, config.train_file, config.dataset_limits, .train, control);
    defer train.deinit();
    var calibration: ?dataset_mod.Dataset = if (config.calibration_file) |path| try openDataset(a, path, config.dataset_limits, .calibration, control) else null;
    defer if (calibration) |*value| value.deinit();
    var heldout: ?dataset_mod.Dataset = if (config.test_file) |path| try openDataset(a, path, config.dataset_limits, .test_holdout, control) else null;
    defer if (heldout) |*value| value.deinit();
    if (calibration) |*value| try train.requireDisjoint(value, control);
    if (heldout) |*value| {
        try train.requireDisjoint(value, control);
        if (calibration) |*other| try other.requireDisjoint(value, control);
    }
    var source_limits = config.source_limits;
    source_limits.max_source_bytes = source_reserved;
    const source = try source_mod.Source.open(a, io, config.source_dir, .{ .limits = source_limits, .expected_identity = config.expected_source }, control);
    defer source.deinit();
    const tokenization = processor.Options{
        .max_batch_items = config.run.batch_size,
        .max_text_words = config.tokenization.max_text_words,
        .max_total_words = try std.math.mul(usize, config.run.batch_size, config.tokenization.max_text_words),
        .max_sequence_tokens = config.tokenization.max_sequence_tokens,
        .max_batch_tokens = try std.math.mul(usize, config.run.batch_size, config.tokenization.max_sequence_tokens),
        .max_queries = config.tokenization.max_queries,
        .word_splitter = config.tokenization.word_splitter,
    };
    if (calibration) |*value| try value.preflight(source.tokenizer(), tokenization, .{ .gold_capacity = source.config.head.max_gold_per_query }, control, null);
    if (heldout) |*value| try value.preflight(source.tokenizer(), tokenization, .{ .gold_capacity = source.config.head.max_gold_per_query }, control, null);
    const calibration_sha256: ?[32]u8 = if (calibration) |value| value.sha256 else null;
    const test_sha256: ?[32]u8 = if (heldout) |value| value.sha256 else null;
    if (calibration) |*value| value.deinit();
    calibration = null;
    if (heldout) |*value| value.deinit();
    heldout = null;
    const trainer = try native.Trainer.init(a, &source.store, source.tokenizer(), source.identity, source.config, &train, source.parameters, .{
        .run = config.run,
        .execution = config.execution,
        .attention_profile = config.attention_profile,
        .activation_profile = config.activation_profile,
        .source_reserved_bytes = source.reservedBytes(),
        .calibration_sha256 = calibration_sha256,
        .test_sha256 = test_sha256,
        .peft = config.peft,
        .processor = tokenization,
        .capacities = config.capacities,
        .weights = config.weights,
        .gold_start = config.gold_start,
        .gold_end = config.gold_end,
        .gold_hold_fraction = config.gold_hold_fraction,
        .require_gold_relation_coverage = config.require_gold_relation_coverage,
        .limits = try trainerLimits(config),
    }, control);
    defer trainer.deinit();
    const restore_receipt = if (config.resume_from) |path| try trainer.restorePinned(path, config.expected_restore_state_sha256, control) else null;
    const executable_path = try std.process.executablePathAlloc(io, scratch);
    defer scratch.free(executable_path);
    const executable_digest = try snapshots.digest(io, std.Io.Dir.cwd(), executable_path, 1024 * mib, control);
    const checkpoint_bytes = try checkpointSize(trainer);
    // Old and replacement checkpoint coexist until atomic publication. The
    // final model/adapter is separately staged; include its conservative size.
    const export_plan = try trainer.estimateExportSnapshot(scratch, source, config.source_dir, config.export_limits, control);
    const export_bytes = export_plan.output_bytes_upper_bound;
    try disk(config.output_dir, try std.math.add(u64, try std.math.mul(u64, checkpoint_bytes, 2), export_bytes), config.disk_headroom_bytes);
    try check(control);
    try std.Io.Dir.cwd().createDir(io, config.output_dir, .default_dir);
    // Do not erase a failed run: its immutable manifest and last checkpoint are
    // useful for diagnosis/recovery. Existing output directories are rejected.
    try publication.syncDirectory(io, std.fs.path.dirname(config.output_dir).?);
    const manifest_path = try std.fs.path.join(scratch, &.{ config.output_dir, "run.json" });
    defer scratch.free(manifest_path);
    const checkpoint_path = try std.fs.path.join(scratch, &.{ config.output_dir, "latest.safetensors" });
    defer scratch.free(checkpoint_path);
    const progress_path = try std.fs.path.join(scratch, &.{ config.output_dir, "progress.jsonl" });
    defer scratch.free(progress_path);
    const result_path = try std.fs.path.join(scratch, &.{ config.output_dir, "result.json" });
    defer scratch.free(result_path);
    try writeJson(scratch, io, manifest_path, .{
        .format = "antfly.gliner25-training-run/v1",
        .config = config,
        .backend = if (config.execution == .resident_metal) "metal" else "native",
        .math_policy = "strict_f32_activations_v1",
        .training_contract = if (config.execution == .resident_metal) "boundary-resident-training-v1" else "boundary-native-training-v1",
        .observed_executable = .{ .path = executable_path, .digest = executable_digest },
        .zig_version = @import("builtin").zig_version_string,
        .architecture = @tagName(@import("builtin").cpu.arch),
        .operating_system = @tagName(@import("builtin").os.tag),
        .source = source.identity,
        .train_sha256 = train.sha256,
        .schema_sha256 = train.schemas_sha256,
        .calibration_sha256 = trainer.run_plan.data.calibration_sha256,
        .test_sha256 = trainer.run_plan.data.test_sha256,
        .run_fingerprint = trainer.fingerprint,
        .initial_identity = trainer.optimizer.identity(),
        .restore_receipt = restore_receipt,
        .admitted_bytes = combined,
        .source_usage = source.usage(),
        .portable_export = "model",
        .declared_split_preflight = "all_splits",
        .evaluation_performed = false,
    }, control);
    const progress = try std.Io.Dir.cwd().createFile(io, progress_path, .{ .exclusive = true });
    defer progress.close(io);
    const initial_microbatch = trainer.optimizer.identity().microbatch_step;
    var log_bytes: usize = 0;
    var last_checkpoint: ?Identity = null;
    while (true) {
        try check(control);
        const identity = trainer.optimizer.identity();
        if ((try trainer.position()).complete) break;
        if (pauseRequested(execution, identity.microbatch_step - initial_microbatch)) {
            try save(trainer, checkpoint_path, checkpoint_bytes, config.disk_headroom_bytes, control);
            try progress.sync(io);
            const result = Result{ .status = .paused, .identity = identity, .accumulated_microbatches = trainer.optimizer.owner.accum_count, .run_fingerprint = trainer.fingerprint, .state_sha256 = try trainer.optimizer.stateFingerprint(trainer.fingerprint, control) };
            try writeJson(scratch, io, result_path, result, control);
            return result;
        }
        if (config.max_progress_bytes - log_bytes < 8192) return error.BoundaryTrainingProgressLimitExceeded;
        const report = try trainer.next(control) orelse break;
        const bytes = try std.json.Stringify.valueAlloc(scratch, report, .{});
        defer scratch.free(bytes);
        if (bytes.len + 1 > 8192) return error.BoundaryTrainingProgressLimitExceeded;
        try progress.writeStreamingAll(io, bytes);
        try progress.writeStreamingAll(io, "\n");
        log_bytes += bytes.len + 1;
        const current = trainer.optimizer.identity();
        if (current.microbatch_step % config.checkpoint_every_microbatches == 0) {
            try save(trainer, checkpoint_path, checkpoint_bytes, config.disk_headroom_bytes, control);
            last_checkpoint = current;
            try progress.sync(io);
        }
        if (execution.report_fn) |callback| try callback(execution.report_context, report);
    }
    const final_identity = trainer.optimizer.identity();
    if (last_checkpoint == null or !std.meta.eql(last_checkpoint.?, final_identity)) try save(trainer, checkpoint_path, checkpoint_bytes, config.disk_headroom_bytes, control);
    try progress.sync(io);
    const model_path = try std.fs.path.join(scratch, &.{ config.output_dir, "model" });
    defer scratch.free(model_path);
    try disk(model_path, export_bytes, config.disk_headroom_bytes);
    const portable = try trainer.exportSnapshot(scratch, io, source, model_path, config.source_dir, config.export_limits, control);
    const result = Result{ .status = .complete, .identity = final_identity, .accumulated_microbatches = 0, .run_fingerprint = trainer.fingerprint, .state_sha256 = try trainer.optimizer.stateFingerprint(trainer.fingerprint, control), .portable_model = portable };
    try writeJson(scratch, io, result_path, result, control);
    return result;
}

const DatasetSplit = enum { train, calibration, test_holdout };

fn openDataset(a: Allocator, path: []const u8, limits: dataset_mod.Limits, split: DatasetSplit, control: ?Control) !dataset_mod.Dataset {
    var failure = dataset_mod.Failure{};
    return dataset_mod.Dataset.open(a, path, .{ .limits = limits }, control, &failure) catch |err| {
        if (failure.allocation) |details| std.log.warn("GLiNER2.5 dataset allocation failed: split={s} stage={s} line={?} reason={s} requested_bytes={d} live_bytes={d} peak_bytes={d} limit_bytes={d}", .{
            @tagName(split), @tagName(failure.stage), failure.line, @tagName(details.kind), details.requested_bytes, details.live_bytes, details.peak_bytes, details.limit_bytes,
        });
        return err;
    };
}

fn pauseRequested(execution: Execution, completed_microbatches: u64) bool {
    const bounded = if (execution.stop_after_microbatches) |maximum| completed_microbatches >= maximum else false;
    return bounded or if (execution.pause_requested) |callback| callback(execution.pause_context) else false;
}

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn checkpointSize(trainer: *const native.Trainer) !u64 {
    var bytes: u64 = 8 * mib;
    for (trainer.optimizer.owner.regular_params.items) |slot| bytes = try std.math.add(u64, bytes, try std.math.mul(u64, slot.weights.len, 16));
    return bytes;
}
fn disk(output: []const u8, additional: u64, reserve: u64) !void {
    const parent = std.fs.path.dirname(output) orelse return error.InvalidBoundaryTrainingJobPath;
    const available = try platform.filesystem.capacity(parent);
    if (try std.math.add(u64, additional, reserve) > available.available_bytes) return error.BoundaryTrainingDiskLimitExceeded;
}
fn save(trainer: *native.Trainer, path: []const u8, required: u64, reserve: u64, control: ?Control) !void {
    try check(control);
    try disk(path, required, reserve);
    try trainer.save(path, control);
}
fn writeJson(a: Allocator, io: std.Io, path: []const u8, value: anytype, control: ?Control) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{ .whitespace = .indent_2 });
    defer a.free(bytes);
    try check(control);
    const parent = std.fs.path.dirname(path) orelse return error.InvalidBoundaryTrainingJobPath;
    var nonce: [16]u8 = undefined;
    try std.Io.randomSecure(io, &nonce);
    const temporary = try std.fmt.allocPrint(a, "{s}/.training-receipt-{s}.tmp", .{ parent, std.fmt.bytesToHex(nonce, .lower) });
    defer a.free(temporary);
    const file = try std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true });
    defer file.close(io);
    var published = false;
    defer if (!published) publication.cleanupPrivateFile(io, temporary);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        try file.writeStreamingAll(io, bytes[offset..end]);
        offset = end;
    }
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
    try check(control);
    try publication.publishDirectory(a, io, temporary, path);
    published = true;
    try publication.syncDirectory(io, parent);
}

test "boundary training job rejects invalid paths modes precision and combined admission" {
    const a = std.testing.allocator;
    const json = "{\"version\":1,\"source_dir\":\"/tmp/source\",\"train_file\":\"/tmp/train.jsonl\",\"output_dir\":\"/tmp/new-run\",\"run\":{\"mode\":\"heads\"}}";
    var parsed = try parse(a, json);
    defer parsed.deinit();
    try std.testing.expect(try admissionBytes(parsed.value, 512 * mib) < parsed.value.memory.combined_bytes);
    var multilingual = parsed.value;
    multilingual.calibration_file = "/tmp/calibration.jsonl";
    multilingual.test_file = "/tmp/test.jsonl";
    try std.testing.expect(try admissionBytes(multilingual, 1568157737) <= multilingual.memory.combined_bytes);
    var invalid = parsed.value;
    invalid.output_dir = "/tmp/../source";
    try std.testing.expectError(error.InvalidBoundaryTrainingJobPath, validate(invalid));
    invalid = parsed.value;
    invalid.run.mode = .dora;
    try std.testing.expectError(error.InvalidBoundaryTrainingJob, validate(invalid));
    invalid = parsed.value;
    invalid.memory.combined_bytes = 1;
    try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, admissionBytes(invalid, 1));
    invalid = parsed.value;
    invalid.version = 2;
    var admission = memory.AdmissionController{};
    defer admission.deinit();
    try std.testing.expectError(error.InvalidBoundaryTrainingJob, execute(a, std.testing.io, invalid, &admission, .{}, null));
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().hostTotalBytes());
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const relative = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{temporary.sub_path});
    defer a.free(relative);
    const existing = try std.Io.Dir.cwd().realPathFileAlloc(std.testing.io, relative, a);
    defer a.free(existing);
    var reuse = parsed.value;
    reuse.source_dir = "/nonexistent-gliner25-source";
    reuse.output_dir = existing;
    try std.testing.expectError(error.PathAlreadyExists, execute(a, std.testing.io, reuse, &admission, .{}, null));
    try std.testing.expectEqual(@as(usize, 0), admission.snapshot().hostTotalBytes());
}

fn exerciseReceipt(a: Allocator) !void {
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/result.json", .{temporary.sub_path});
    defer a.free(path);
    try writeJson(a, io, path, .{ .status = "paused", .step = 1 }, null);
    const bytes = try snapshots.read(a, io, std.Io.Dir.cwd(), path, 1024, null);
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "paused") != null);
    writeJson(a, io, path, .{ .status = "replaced" }, null) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    const preserved = try snapshots.read(a, io, std.Io.Dir.cwd(), path, 1024, null);
    defer a.free(preserved);
    try std.testing.expectEqualSlices(u8, bytes, preserved);
    var iterator = temporary.dir.iterate();
    var count: usize = 0;
    while (try iterator.next(io)) |entry| {
        try std.testing.expectEqualStrings("result.json", entry.name);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "boundary training job receipts publish atomically without replacement and clean allocation failures" {
    try exerciseReceipt(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseReceipt, .{});
}

fn admissionTestConfig() Config {
    return .{
        .version = 1,
        .source_dir = "/tmp/source",
        .train_file = "/tmp/train.jsonl",
        .output_dir = "/tmp/new-run",
        .run = .{ .mode = .heads },
        .dataset_limits = .{ .max_host_bytes = 256 * mib },
        .memory = .{
            .host_bytes = 384 * mib,
            .backend_bytes = 512 * mib,
            .backend_metadata_bytes = 64 * mib,
            .combined_bytes = 4 * 1024 * mib,
        },
    };
}

test "boundary training job admission splits resident payload from host metadata without losing bytes" {
    var config = admissionTestConfig();
    const source_reserved = 512 * mib;
    const expected = source_reserved + config.memory.job_bytes + config.dataset_limits.max_host_bytes + config.memory.host_bytes + config.memory.backend_bytes;
    const cpu = try admissionAmounts(config, source_reserved);
    try std.testing.expectEqual(expected, cpu.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), cpu.backendTotalBytes());
    config.execution = .resident_metal;
    const gpu = try admissionAmounts(config, source_reserved);
    try std.testing.expectEqual(expected - config.memory.backend_bytes + config.memory.backend_metadata_bytes, gpu.hostTotalBytes());
    try std.testing.expectEqual(config.memory.backend_bytes - config.memory.backend_metadata_bytes, gpu.backendTotalBytes());
    try std.testing.expectEqual(expected, try admissionBytes(config, source_reserved));
    config.memory.combined_bytes = expected - 1;
    try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, admissionAmounts(config, source_reserved));
    config = admissionTestConfig();
    config.execution = .resident_metal;
    for ([_]usize{ 0, config.memory.backend_bytes, config.memory.backend_bytes + 1 }) |metadata| {
        config.memory.backend_metadata_bytes = metadata;
        try std.testing.expectError(error.InvalidBoundaryTrainingJob, admissionAmounts(config, source_reserved));
    }
}

test "boundary training job admission retains every preflight split and checks arithmetic overflow" {
    var config = admissionTestConfig();
    config.calibration_file = "/tmp/calibration.jsonl";
    config.test_file = "/tmp/test.jsonl";
    config.memory.host_bytes = 32 * mib;
    config.memory.backend_bytes = 64 * mib;
    config.memory.backend_metadata_bytes = 16 * mib;
    const source_reserved = 512 * mib;
    const preflight = source_reserved + config.memory.job_bytes + 3 * config.dataset_limits.max_host_bytes;
    const cpu = try admissionAmounts(config, source_reserved);
    try std.testing.expectEqual(preflight, cpu.hostTotalBytes());
    config.execution = .resident_metal;
    const gpu = try admissionAmounts(config, source_reserved);
    try std.testing.expectEqual(preflight, gpu.hostTotalBytes());
    try std.testing.expectEqual(@as(usize, 48 * mib), gpu.backendTotalBytes());
    // The lease holds separate host/device maxima across both phases. GPU
    // bytes cannot cover simultaneously retained CPU preflight snapshots.
    try std.testing.expectEqual(preflight + 48 * mib, try admissionBytes(config, source_reserved));
    var fewer = config;
    fewer.calibration_file = null;
    fewer.test_file = null;
    try std.testing.expect(try admissionBytes(fewer, source_reserved) < try admissionBytes(config, source_reserved));

    config.memory.combined_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.Overflow, admissionAmounts(config, std.math.maxInt(usize)));
    var overflow = config;
    overflow.dataset_limits.max_host_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.Overflow, admissionAmounts(overflow, 0));
    overflow = config;
    overflow.memory.host_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.Overflow, admissionAmounts(overflow, 0));
    overflow = config;
    overflow.dataset_limits.max_host_bytes = (std.math.maxInt(usize) - config.memory.job_bytes) / 3;
    try std.testing.expectError(error.Overflow, admissionAmounts(overflow, 0));
}

test "boundary training job forwards resource overrides without changing semantic run options" {
    const a = std.testing.allocator;
    const parsed = try parse(a,
        \\{"version":1,"source_dir":"/tmp/source","train_file":"/tmp/train.jsonl","output_dir":"/tmp/new-run","run":{"mode":"full"},"training_limits":{"encoder":{"max_forward_tensor_bytes":17179869184},"resident":{"program":{"max_capture_bytes":2147483648,"max_working_bytes":2147483648,"instruction":{"primitive":{"max_scatter_work":268435456}}}}}}
    );
    defer parsed.deinit();
    const resolved = try trainerLimits(parsed.value);
    try std.testing.expectEqual(@as(u64, 16 * 1024 * mib), resolved.step.encoder.max_forward_tensor_bytes);
    try std.testing.expectEqual(@as(usize, 2 * 1024 * mib), resolved.differentiation.resident.program.max_capture_bytes);
    try std.testing.expectEqual(@as(usize, 256 * mib), resolved.differentiation.resident.program.instruction.primitive.max_scatter_work);
    try std.testing.expectEqual(parsed.value.memory.host_bytes, resolved.max_host_bytes);
    try std.testing.expectEqual(parsed.value.memory.optimizer_transaction_bytes, resolved.optimizer.max_transaction_bytes);
    var defaults = parsed.value;
    defaults.training_limits = .{};
    try std.testing.expectEqual(parsed.value.run, defaults.run);
    try std.testing.expectEqual(try admissionBytes(defaults, 512 * mib), try admissionBytes(parsed.value, 512 * mib));
    // Configured tokenization/batch maxima are admitted together; defaults
    // remain unchanged and no input is silently truncated to an inner cap.
    defaults.run.batch_size = 9;
    try std.testing.expectError(error.BoundaryTrainingLimitsExceeded, validate(defaults));
    defaults.training_limits.encoder.input.max_batch = 9;
    try validate(defaults);
    defaults.training_limits.encoder.input.max_batch_tokens = 9 * defaults.tokenization.max_sequence_tokens - 1;
    try std.testing.expectError(error.BoundaryTrainingLimitsExceeded, validate(defaults));
    defaults = parsed.value;
    defaults.tokenization.max_queries = defaults.training_limits.encoder.input.max_queries + 1;
    try std.testing.expectError(error.BoundaryTrainingLimitsExceeded, validate(defaults));
}

test "boundary training job cooperative pause combines callback and invocation limit" {
    const Flag = struct {
        requested: bool = false,
        fn read(raw: ?*const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return self.requested;
        }
    };
    var flag = Flag{};
    const execution = Execution{ .stop_after_microbatches = 2, .pause_context = &flag, .pause_requested = Flag.read };
    try std.testing.expect(!pauseRequested(.{}, std.math.maxInt(u64)));
    try std.testing.expect(!pauseRequested(execution, 1));
    try std.testing.expect(pauseRequested(execution, 2));
    flag.requested = true;
    try std.testing.expect(pauseRequested(execution, 0));
    try std.testing.expect(pauseRequested(.{ .pause_context = &flag, .pause_requested = Flag.read }, 0));
}

fn exerciseConfigSnapshot(a: Allocator) !void {
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/job.json", .{temporary.sub_path});
    defer a.free(path);
    const first =
        \\{"version":1,"source_dir":"/tmp/source","train_file":"/tmp/train.jsonl","output_dir":"/tmp/run","run":{"mode":"heads"},"training_limits":{"resident":{"program":{"max_capture_bytes":2147483648}}}}
    ;
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = first });
    var snapshot = try loadConfigSnapshot(a, io, path);
    defer snapshot.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(first, &digest, .{});
    try std.testing.expectEqual(digest, snapshot.sha256);
    try std.testing.expectEqual(@as(u64, first.len), snapshot.size_bytes);
    try std.testing.expectEqual(.heads, snapshot.parsed.value.run.mode);
    try std.testing.expectEqual(@as(usize, 2 * 1024 * mib), (try trainerLimits(snapshot.parsed.value)).differentiation.resident.program.max_capture_bytes);
    const replacement =
        \\{"version":1,"source_dir":"/tmp/source","train_file":"/tmp/train.jsonl","output_dir":"/tmp/run","run":{"mode":"full"}}
    ;
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = replacement });
    var changed = try loadConfigSnapshot(a, io, path);
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &snapshot.sha256, &changed.sha256));
    try std.testing.expectEqual(.full, changed.parsed.value.run.mode);
    try std.testing.expectEqual(.heads, snapshot.parsed.value.run.mode);
    try std.testing.expectEqual(digest, snapshot.sha256);
    const legacy = try loadConfig(a, io, path);
    defer legacy.deinit();
    try std.testing.expectEqual(.full, legacy.value.run.mode);
}

test "boundary training job config snapshot binds consumed bytes and cleans allocation failures" {
    try exerciseConfigSnapshot(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseConfigSnapshot, .{});
}

test "boundary training job dataset open preserves declared and backing allocation errors before model loading" {
    const a = std.testing.allocator;
    // Both failures happen before opening the file or loading any checkpoint.
    try std.testing.expectError(error.BoundaryTrainingDatasetMemoryLimitExceeded, openDataset(a, "missing-dataset.jsonl", .{ .max_host_bytes = 1 }, .train, null));
    var backing_failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, openDataset(backing_failure.allocator(), "missing-dataset.jsonl", .{}, .train, null));
}
