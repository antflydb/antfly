// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One synchronous, strictly resident GLiNER2.5 request. The caller holds the
//! managed backend execution lease for this call. Only owned decoded values
//! and bounded scalar window evidence outlive the encoder and learned heads.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const artifact = @import("../models/gliner_boundary_artifact.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const engine = @import("gliner_boundary_engine_device.zig");
const native_engine = @import("gliner_boundary_engine.zig");
const head = @import("gliner_boundary_device.zig");
const adapter = @import("gliner_boundary_scorer_device.zig");
pub const ExecutionPolicy = @import("gliner_boundary_device_math.zig").ExecutionPolicy;

/// Allocation-free preflight uses bounded stack storage for ragged lengths.
pub const max_admission_batch = 64;

pub const Limits = struct {
    encoder: native_engine.Limits = .{},
    max_encoder_device_bytes: usize = 2 * 1024 * 1024 * 1024,
    head: head.Limits = .{},
    scorer: adapter.Limits = .{},
    /// Both independent owners enforce their caps before dispatch/allocation.
    /// Their sum also covers classifier/record/relation child scorer work.
    max_combined_device_bytes: usize = 3 * 1024 * 1024 * 1024,
};

pub const Options = struct {
    /// Obtain from the validated opened model artifact's immutable identity.
    precision: artifact.Precision = .fp32,
    execution_policy: ExecutionPolicy = .reference_v1,
    limits: Limits = .{},
    pipeline: pipeline.Options = .{},
    per_sample: []const pipeline.Options = &.{},
};

pub const Plan = struct {
    encoder: engine.Plan,
    head: ?head.Plan,
    encoder_budget_bytes: usize,
    head_budget_bytes: usize,
    /// Sum of enforced device allocation charges, including upload staging.
    /// This conservative cap is distinct from physical peak RSS and excludes
    /// host model mappings, tokenizer/processor storage, and decoded outputs.
    combined_device_upper_bound_bytes: usize,
    minimum_result_download_bytes: usize,
};

pub const Result = struct {
    outputs: pipeline.Result,
    stats: adapter.Stats,
    admission: Plan,
    pub fn deinit(self: *Result) void {
        self.outputs.deinit();
        self.* = undefined;
    }
};

pub const WindowResult = struct {
    outputs: pipeline.WindowResult,
    stats: adapter.Stats,
    admission: Plan,
    pub fn deinit(self: *WindowResult) void {
        self.outputs.deinit();
        self.* = undefined;
    }
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}

fn encoderOptions(options: Options) engine.Options {
    return .{ .precision = options.precision, .execution_policy = options.execution_policy, .admission = options.limits.encoder, .max_device_bytes = options.limits.max_encoder_device_bytes, .max_result_download_bytes = options.limits.scorer.max_result_download_bytes, .control = options.pipeline.control };
}

pub fn requiredHeadOutputs(schemas: []const *const schema.CompiledSchema) head.RequiredHeadOutputs {
    for (schemas) |compiled| for (compiled.schema.structures) |structure| {
        if (structure.mode != null) return .{ .record_candidates = true };
    };
    return .{ .record_candidates = false };
}

/// No allocator, model tensor read, backend dispatch, or upload is performed.
pub fn plan(config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: Options) !Plan {
    if (options.pipeline.control) |control| try control.check();
    try pipeline.validateOptions(options.pipeline);
    const batch = prepared.samples.len;
    if (batch > max_admission_batch) return error.ResourceLimitExceeded;
    if (batch == 0 or schemas.len != batch) return error.InvalidBoundaryPipelineInput;
    if (options.per_sample.len != 0 and options.per_sample.len != batch) return error.InvalidBoundaryPipelineOptions;
    for (options.per_sample) |item| try pipeline.validateOptions(item);
    const cpu_limits = options.pipeline.head_limits;
    if (batch > cpu_limits.max_batch or prepared.word_width > cpu_limits.max_text_words or prepared.query_width > cpu_limits.max_queries)
        return error.ResourceLimitExceeded;
    var lengths: [max_admission_batch]usize = undefined;
    for (prepared.samples, schemas, 0..) |sample, compiled, index| {
        if (!std.mem.eql(u8, &sample.schema_fingerprint, &compiled.fingerprint) or sample.words.len > prepared.word_width or
            sample.queries.len > prepared.query_width or sample.classification_labels.len > prepared.classification_width) return error.InvalidBoundaryPipelineRouting;
        lengths[index] = sample.words.len;
        if (options.pipeline.validate_value_fn == null) {
            for (compiled.schema.entities) |entity| if (entity.validators.len > 0) return error.MissingExtractionValidator;
            for (compiled.schema.structures) |structure| for (structure.fields) |field| if (field.validators.len > 0) return error.MissingExtractionValidator;
        }
    }
    const encoded = try engine.plan(config, prepared, encoderOptions(options));
    const headed: ?head.Plan = if (prepared.query_width != 0)
        try head.planShape(config, .{ .batch = batch, .text_length = prepared.word_width, .queries = prepared.query_width, .text_lengths = lengths[0..batch], .query_mask = prepared.query_marker_mask }, options.limits.head)
    else
        null;
    const query_count = try mul(batch, prepared.query_width);
    const pool_elements = try mul(query_count, config.head.pool_size);
    if (pool_elements > cpu_limits.max_pool_pair_elements) return error.ResourceLimitExceeded;
    const classification_elements = try mul(batch, prepared.classification_width);
    if (classification_elements > options.limits.scorer.tasks.max_classification_choices or
        classification_elements > options.pipeline.task_limits.max_classification_choices) return error.ResourceLimitExceeded;
    var head_download_elements = pool_elements;
    if (config.head.enable_abstention) head_download_elements = try add(head_download_elements, query_count);
    if (config.head.enable_count_head) head_download_elements = try add(head_download_elements, query_count);
    const head_download_bytes = try mul(head_download_elements, 4);
    const minimum_download = try add(head_download_bytes, try mul(classification_elements, 4));
    if (head_download_bytes > options.limits.head.max_result_download_bytes or minimum_download > options.limits.scorer.max_result_download_bytes)
        return error.ResourceLimitExceeded;
    const head_budget = if (headed != null) options.limits.head.max_device_bytes else 0;
    const combined = try add(options.limits.max_encoder_device_bytes, head_budget);
    if (combined > options.limits.max_combined_device_bytes) return error.ResourceLimitExceeded;
    return .{ .encoder = encoded, .head = headed, .encoder_budget_bytes = options.limits.max_encoder_device_bytes, .head_budget_bytes = head_budget, .combined_device_upper_bound_bytes = combined, .minimum_result_download_bytes = minimum_download };
}

fn checkBackend(cb: *const ops.ComputeBackend) !void {
    if (cb.kind() != .metal or cb.vtable.glinerBoundaryDevice == null or cb.vtable.glinerBoundaryDownload == null)
        return error.UnsupportedGlinerBoundaryDevice;
    if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    try cb.checkExecutionControl();
}

pub fn run(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: Options) !Result {
    return execute(false, cb, allocator, allocator, config, prepared, schemas, options);
}

/// The caller must globally merge and solve these internal window candidates
/// before returning a public long-document response.
pub fn runWindows(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: Options) !WindowResult {
    return runWindowsWithOutputAllocator(cb, allocator, allocator, config, prepared, schemas, options);
}

/// Device and encoder scratch is reclaimed per window. Retained scalar
/// evidence has a separate owner and cap spanning the complete document.
pub fn runWindowsWithOutputAllocator(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, output_allocator: std.mem.Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: Options) !WindowResult {
    return execute(true, cb, allocator, output_allocator, config, prepared, schemas, options);
}

fn execute(comptime windows: bool, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, output_allocator: std.mem.Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: Options) !(if (windows) WindowResult else Result) {
    try checkBackend(cb);
    const admitted = try plan(config, prepared, schemas, options);
    var encoded = try engine.encodeDevice(cb, allocator, config, prepared, encoderOptions(options));
    defer encoded.deinit();
    var headed: ?head.Result = if (admitted.head != null) blk: {
        var input = try encoded.asHeadInput(options.pipeline.control);
        if (options.execution_policy == .optimized_v2) input.required_outputs = requiredHeadOutputs(schemas);
        break :blk try head.forwardDevice(cb, allocator, config, input, options.limits.head);
    } else null;
    defer if (headed) |*value| value.deinit();
    var scorer = try adapter.Context.init(allocator, config, &encoded, if (headed) |*value| value else null, options.limits.scorer);
    defer scorer.deinit();
    var output = if (windows)
        try pipeline.runScoredWindows(output_allocator, config, prepared, schemas, scorer.scores, scorer.scorer(), options.pipeline, options.per_sample)
    else
        try pipeline.runScoredPerSample(output_allocator, config, prepared, schemas, scorer.scores, scorer.scorer(), options.pipeline, options.per_sample);
    errdefer output.deinit();
    try checkBackend(cb);
    if (options.pipeline.control) |control| try control.check();
    const stats = try scorer.stats();
    if (stats.peak_device_upper_bound_bytes > admitted.combined_device_upper_bound_bytes) return error.ResourceLimitExceeded;
    return .{ .outputs = output, .stats = stats, .admission = admitted };
}
