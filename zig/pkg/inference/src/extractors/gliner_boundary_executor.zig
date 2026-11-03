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

//! Schema-aware execution inside an admitted, managed model session. CUDA and
//! Metal group compatible inputs into bounded physical batches; native retains
//! its qualified serial request execution.
//! Output is buffered atomically and independently bounded across all inputs.
const std = @import("std");
const wire = @import("extraction_v2.zig");
const model = @import("../models/gliner_boundary.zig");
const engine = @import("../architectures/gliner/boundary_engine.zig");
const device_request = @import("../architectures/gliner/boundary_request_device.zig");
const device_engine = @import("../architectures/gliner/boundary_engine_device.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const compute = @import("../ops/ops.zig");
const long_executor = @import("gliner_boundary_long_executor.zig");
const artifact = @import("../models/gliner_boundary_bundle.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const observation = @import("extraction_observer.zig");
const qualification = @import("gliner_boundary_qualification.zig");

pub const PreparationStore = long_executor.PreparationStore;

pub const Options = struct {
    preparations: ?*PreparationStore = null,
    max_response_bytes: usize = 64 * 1024 * 1024,
    processor: processor.Options = .{},
    engine: engine.Options = .{},
    device: device_request.Limits = .{},
    metal_execution_policy: device_request.ExecutionPolicy = .reference_v1,
    /// Original declared ceiling, before admitted workspace is subtracted.
    /// Preserves long-document identity independently of cache capacity.
    profile_encoder_device_limit: ?usize = null,
    pipeline: pipeline.Options = .{},
    control: ?Control = null,
    failure: ?*wire.FailureContext = null,
    identity: ?artifact.Identity = null,
    long_document: long_executor.Limits = .{},
    observer: ?observation.Observer = null,
};

/// Both device owners enforce these caps independently before allocating.
/// Serial inputs/windows reuse this reservation; simultaneous encoder/head
/// ownership requires their sum, including protected weights and upload
/// staging. Host scratch and model-manager residency are separate amounts.
pub fn deviceScratchUpperBound(options: Options) !usize {
    const total = std.math.add(usize, options.device.max_encoder_device_bytes, options.device.head.max_device_bytes) catch return error.ResourceLimitExceeded;
    if (total == 0 or options.device.max_encoder_device_bytes == 0 or options.device.head.max_device_bytes == 0 or total > options.device.max_combined_device_bytes)
        return error.ResourceLimitExceeded;
    return total;
}

fn progress(options: Options, index: ?usize, stage: []const u8) !void {
    if (options.control) |control| try control.check();
    if (options.failure) |failure| failure.* = .{ .input_index = index, .stage = stage };
    if (observation.Stage.fromFailure(stage)) |phase| observation.emit(options.observer, .{ .phase = phase });
}

fn observeSample(observer: ?observation.Observer, sample: pipeline.Sample, tokens: usize, values: usize) void {
    const active = observer orelse return;
    var summary = observation.Sample{ .prompt_tokens = tokens, .output_values = values };
    for ([_]?pipeline.Diagnostics{ sample.classification_solver, sample.joint_solver, sample.record_solver }, &summary.solvers) |value, *out| if (value) |diagnostic| {
        out.* = .{ .status = switch (diagnostic.status) {
            .optimal => .optimal,
            .feasible => .feasible,
        }, .exhausted = diagnostic.exhausted, .visited_nodes = diagnostic.visited_nodes };
    };
    active.emit(.{ .sample_decoded = summary });
}

/// Runs before acquiring a backend or executing an input, so unsupported
/// features in later items cannot consume earlier items' model execution.
pub fn preflight(request: *const wire.Request, options: Options) !void {
    try progress(options, null, "preflight");
    if (request.items.len == 0 or options.max_response_bytes == 0) return error.InvalidExtractionRequest;
    for (request.items, 0..) |item, index| {
        try progress(options, index, "preflight");
        try item.options.validateNativeLimits(options.pipeline);
        try pipeline.validateOptions(item.options.native(options.pipeline));
    }
}

fn valueCount(value: pipeline.Value) !usize {
    var count: usize = 1;
    for (value.attributes) |attribute| count = try std.math.add(usize, count, attribute.labels.len);
    return count;
}
fn outputValues(sample: pipeline.Sample) !usize {
    var count: usize = 0;
    for (sample.entities) |group| for (group.values) |value| {
        count = try std.math.add(usize, count, try valueCount(value));
    };
    for (sample.classifications) |group| count = try std.math.add(usize, count, group.labels.len);
    for (sample.relations) |relation| {
        count = try std.math.add(usize, count, try valueCount(relation.head));
        count = try std.math.add(usize, count, try valueCount(relation.tail));
    }
    for (sample.structures) |group| for (group.instances) |record| {
        for (record.fields) |field| for (field.values) |value| {
            count = try std.math.add(usize, count, try valueCount(value));
        };
    };
    return count;
}

/// Allocator must reclaim scratch frees (or be backed by a bounded request
/// owner whose retained bytes are admitted). The returned JSON uses allocator.
pub fn executeNative(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    return execute(cb, allocator, config, tokenizer, request, options);
}

pub fn executeDevice(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    if ((cb.kind() != .metal and cb.kind() != .cuda)) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    return execute(cb, allocator, config, tokenizer, request, options);
}

/// Browser-owned, bounded session. This does not widen the native serving
/// qualification table: browser catalog artifacts are qualified separately.
pub fn executeBrowser(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    if (cb.kind() != .wasm) return error.UnsupportedGlinerBoundaryBackend;
    return executeChecked(cb, allocator, config, tokenizer, request, options, null);
}

/// The caller owns the managed backend, model lock and host/device admission
/// for the complete execution, including serialization and cleanup.
pub fn execute(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    return executeChecked(cb, allocator, config, tokenizer, request, options, null);
}

/// Serving admission verifies the exact live artifact and complete feature union.
/// Model/backend geometry and work budgets are planned before learned operations.
pub fn executeQualified(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    if (options.control) |control| try control.check();
    try cb.checkExecutionControl();
    if (cb.kind() != .native and (cb.kind() != .metal and cb.kind() != .cuda)) return error.UnsupportedGlinerBoundaryBackend;
    const identity = options.identity orelse return error.UnsupportedGlinerBoundaryRuntime;
    if (identity.backbone != config.backbone) return error.GlinerBoundaryArtifactMismatch;
    var gate = try qualification.Admission.initResolved(identity, switch (cb.kind()) {
        .metal => .metal,
        .cuda => .cuda,
        else => .native,
    }, request, options.pipeline, options.control);
    var local_store = PreparationStore.init(allocator);
    defer local_store.deinit();
    var prepared_options = options;
    if (prepared_options.preparations == null) prepared_options.preparations = &local_store;
    try qualifyRequestGeometry(cb, allocator, config, tokenizer, request, prepared_options, &gate);
    if (options.control) |control| try control.check();
    return executeChecked(cb, allocator, config, tokenizer, request, prepared_options, &gate);
}

/// The receiver is a compile-time parameter for the quiet scalar observation
/// path only. Learned entrypoints always require concrete runtime admission.
fn qualifyRequestGeometry(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: anytype) !void {
    return planRequestGeometry(cb.kind(), allocator, config, tokenizer, request, options, gate);
}

/// Maximum physical B*S over serial inputs and batched document windows. This uses
/// the same bounded processor/planning path as qualification, before the
/// accelerator mutex or any eviction-capable workspace admission is held.
pub fn workspaceGeometry(allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !usize {
    return workspaceGeometryFor(.metal, allocator, config, tokenizer, request, options);
}

pub fn browserGeometry(allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !usize {
    return workspaceGeometryFor(.wasm, allocator, config, tokenizer, request, options);
}

fn workspaceGeometryFor(backend: compute.BackendKind, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !usize {
    if (options.identity == null) return error.MissingGlinerBoundaryIdentity;
    const Receiver = struct {
        peak_tokens: usize = 0,
        pub fn observe(self: *@This(), _: usize, _: usize, _: usize, prepared: *const processor.PreparedBatch) !void {
            self.peak_tokens = @max(self.peak_tokens, prepared.input_ids.len);
        }
        pub fn observeSingle(self: *@This(), _: Allocator, _: []const u8, prepared: *const processor.PreparedBatch, _: processor.Options) !void {
            self.peak_tokens = @max(self.peak_tokens, prepared.input_ids.len);
        }
        pub fn observeWindowGroup(self: *@This(), prepared: *const processor.PreparedBatch) void {
            self.peak_tokens = @max(self.peak_tokens, prepared.input_ids.len);
        }
    };
    var receiver = Receiver{};
    try planRequestGeometry(backend, allocator, config, tokenizer, request, options, &receiver);
    if (receiver.peak_tokens == 0) return error.InvalidBoundaryPipelineInput;
    return receiver.peak_tokens;
}

/// Encoder device bytes the reference (non-resident) Metal/CUDA path needs for
/// this exact request: the largest encoder plan over the batches and windows it
/// will execute. Admission reserves this instead of the configured encoder
/// ceiling, so a short input does not hold the whole ceiling. Never exceeds
/// `options.device.max_encoder_device_bytes`; planning fails if it would.
pub fn referenceEncoderDeviceBytes(backend: compute.BackendKind, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !usize {
    if (backend != .metal and backend != .cuda) return error.UnsupportedGlinerBoundaryBackend;
    const identity = options.identity orelse return error.MissingGlinerBoundaryIdentity;
    const Precision = @TypeOf(identity.precision);
    const Receiver = struct {
        config: *const model.Config,
        options: Options,
        precision: Precision,
        peak: usize = 0,
        failure: ?anyerror = null,
        fn note(self: *@This(), prepared: *const processor.PreparedBatch) void {
            if (self.failure != null) return;
            const planned = device_engine.plan(self.config, prepared, .{
                .precision = self.precision,
                .execution_policy = .reference_v1,
                .admission = self.options.engine.limits,
                .max_device_bytes = self.options.device.max_encoder_device_bytes,
                .control = self.options.control,
            }) catch |err| {
                self.failure = err;
                return;
            };
            self.peak = @max(self.peak, planned.device_upper_bound_bytes);
        }
        pub fn observe(self: *@This(), _: usize, _: usize, _: usize, prepared: *const processor.PreparedBatch) !void {
            self.note(prepared);
            if (self.failure) |err| return err;
        }
        // observeGroupBatch already planned the physical batch. Per-item
        // qualification views retain its tensors but slice samples to one row,
        // so they are not complete batches accepted by the device planner.
        pub fn observeSingle(_: *@This(), _: Allocator, _: []const u8, _: *const processor.PreparedBatch, _: processor.Options) !void {}
        pub fn observeGroupBatch(self: *@This(), prepared: *const processor.PreparedBatch) !void {
            self.note(prepared);
            if (self.failure) |err| return err;
        }
        pub fn observeWindowGroup(self: *@This(), prepared: *const processor.PreparedBatch) void {
            self.note(prepared);
        }
    };
    var receiver = Receiver{ .config = config, .options = options, .precision = identity.precision };
    try planRequestGeometry(backend, allocator, config, tokenizer, request, options, &receiver);
    if (receiver.failure) |err| return err;
    if (receiver.peak == 0) return error.InvalidBoundaryPipelineInput;
    return receiver.peak;
}

/// Contiguous compatible items share one forward pass without reordering results.
/// The same preparation/planning routine runs before qualification and execution.
fn prepareGroup(backend: compute.BackendKind, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, items: []const wire.Item, options: Options) !processor.PreparedBatch {
    const limit = if (backend == .cuda or backend == .metal) @min(options.processor.max_batch_items, options.engine.limits.max_batch, options.pipeline.head_limits.max_batch, options.pipeline.task_limits.math.max_batch, options.device.head.max_batch) else 1;
    if (limit == 0 or items.len == 0) return error.ResourceLimitExceeded;
    var count: usize = 1;
    while (count < @min(limit, items.len) and items[count].options.long_document.mode != .window and std.meta.eql(items[0].options, items[count].options)) : (count += 1) {}
    var process_options = qualification.singleProcessor(config, &items[0], options.processor, options.engine.limits.max_queries, options.control);
    process_options.max_batch_tokens = @min(process_options.max_batch_tokens, options.engine.limits.max_batch_tokens);
    process_options.max_sequence_tokens = qualification.sequenceLimit(config, @min(process_options.max_sequence_tokens, options.engine.limits.max_sequence_tokens));
    var engine_options = options.engine;
    engine_options.control = options.control;
    while (true) {
        const inputs = try allocator.alloc(processor.Item, count);
        defer allocator.free(inputs);
        for (inputs, items[0..count]) |*input, *item| input.* = .{ .text = item.text, .schema = &item.compiled };
        var prepared = processor.prepare(allocator, tokenizer, inputs, process_options) catch |err| {
            if (err == error.BoundaryBatchLimitExceeded and count > 1) {
                count = (count + 1) / 2;
                continue;
            }
            return err;
        };
        errdefer prepared.deinit();
        // A larger padded group can exceed aggregate work/output limits even
        // when its individual inputs fit. Retry a smaller physical group.
        _ = engine.plan(config, &prepared, engine_options) catch |err| {
            if (err == error.ResourceLimitExceeded and count > 1) {
                prepared.deinit();
                count = (count + 1) / 2;
                continue;
            }
            return err;
        };
        if ((backend == .native or backend == .wasm) and prepared.query_width != 0)
            _ = try @import("../architectures/gliner/boundary_head.zig").planShape(config, .{ .batch = prepared.samples.len, .text_length = prepared.word_width, .queries = prepared.query_width }, options.pipeline.head_limits);
        return prepared;
    }
}

fn observeGroup(allocator: Allocator, config: *const model.Config, items: []const wire.Item, prepared: *const processor.PreparedBatch, options: Options, gate: anytype) !void {
    // Planners that size device work see the batch exactly as it executes.
    if (comptime @hasDecl(@TypeOf(gate.*), "observeGroupBatch")) try gate.observeGroupBatch(prepared);
    for (items[0..prepared.samples.len], 0..) |*item, index| {
        // Preserve the physical padded length in every item's policy check.
        var view = prepared.*;
        view.samples = prepared.samples[index..][0..1];
        try gate.observeSingle(allocator, item.text, &view, qualification.singleProcessor(config, item, options.processor, options.engine.limits.max_queries, options.control));
    }
}

fn planRequestGeometry(backend: compute.BackendKind, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: anytype) !void {
    if (backend != .native and backend != .wasm and (backend != .metal and backend != .cuda)) return error.UnsupportedGlinerBoundaryBackend;
    var quiet = options;
    quiet.observer = null;
    try preflight(request, quiet);
    var remaining_tokens = options.long_document.max_total_encoded_tokens;
    var remaining_attention = try options.long_document.attentionWorkLimit(config, options.engine.limits);
    var remaining_windows = options.long_document.max_request_windows;
    var index: usize = 0;
    while (index < request.items.len) {
        const item = &request.items[index];
        try progress(quiet, index, "tokenizing");
        if (item.options.long_document.mode == .window) {
            try progress(quiet, index, "windowing");
            if (remaining_windows == 0) return error.LongDocumentWindowLimitExceeded;
            if (remaining_tokens == 0 or remaining_attention == 0) return error.LongDocumentWorkLimitExceeded;
            var limits = options.long_document;
            limits.max_total_encoded_tokens = remaining_tokens;
            limits.max_total_attention_work = remaining_attention;
            limits.plan.max_windows = @min(limits.plan.max_windows, remaining_windows);
            var pipeline_options = item.options.native(options.pipeline);
            pipeline_options.control = options.control;
            const used = try long_executor.planGeometry(backend, allocator, config, tokenizer, item, .{
                .preparations = options.preparations,
                .identity = options.identity.?,
                .processor = item.options.preprocessing(options.processor),
                .engine = options.engine,
                .device = options.device,
                .metal_execution_policy = options.metal_execution_policy,
                .profile_encoder_device_limit = options.profile_encoder_device_limit,
                .pipeline = pipeline_options,
                .limits = limits,
                .control = options.control,
                .observer = null,
            }, gate);
            remaining_tokens -= used.encoded_tokens;
            remaining_attention -= used.attention_work_items;
            remaining_windows -= used.window_count;
            index += 1;
            continue;
        }
        var prepared = try prepareGroup(backend, allocator, config, tokenizer, request.items[index..], quiet);
        defer prepared.deinit();
        try observeGroup(allocator, config, request.items[index..], &prepared, quiet, gate);
        index += prepared.samples.len;
    }
    if (options.control) |control| try control.check();
}

fn executeChecked(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: ?*qualification.Admission) ![]u8 {
    if (cb.kind() != .native and cb.kind() != .wasm and (cb.kind() != .metal and cb.kind() != .cuda)) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.kind() != .native and options.identity == null) return error.MissingGlinerBoundaryIdentity;
    if (options.identity) |identity| if (identity.backbone != config.backbone) return error.GlinerBoundaryArtifactMismatch;
    try preflight(request, options);
    var writer = wire.ResponseWriter.init(allocator, options.max_response_bytes, request.items.len);
    defer writer.deinit();
    try writer.begin(request.model);
    var prompt_tokens: usize = 0;
    var remaining_values = options.pipeline.max_output_values;
    var remaining_window_tokens = options.long_document.max_total_encoded_tokens;
    var remaining_window_attention = try options.long_document.attentionWorkLimit(config, options.engine.limits);
    var remaining_windows = options.long_document.max_request_windows;
    var index: usize = 0;
    while (index < request.items.len) {
        const item = request.items[index];
        try progress(options, index, "preflight");
        if (remaining_values == 0) return error.ExtractionOutputLimitExceeded;
        if (item.options.long_document.mode == .window) {
            try progress(options, index, "windowing");
            if (remaining_windows == 0) return error.LongDocumentWindowLimitExceeded;
            if (remaining_window_tokens == 0 or remaining_window_attention == 0) return error.LongDocumentWorkLimitExceeded;
            var long_limits = options.long_document;
            long_limits.max_total_encoded_tokens = remaining_window_tokens;
            long_limits.max_total_attention_work = remaining_window_attention;
            long_limits.plan.max_windows = @min(long_limits.plan.max_windows, remaining_windows);
            var pipeline_options = item.options.native(options.pipeline);
            pipeline_options.control = options.control;
            pipeline_options.max_output_values = remaining_values;
            const long_options = long_executor.Options{
                .preparations = options.preparations,
                .identity = options.identity orelse return error.MissingGlinerBoundaryIdentity,
                .processor = item.options.preprocessing(options.processor),
                .engine = options.engine,
                .device = options.device,
                .pipeline = pipeline_options,
                .limits = long_limits,
                .metal_execution_policy = options.metal_execution_policy,
                .profile_encoder_device_limit = options.profile_encoder_device_limit,
                .control = options.control,
                .observer = options.observer,
            };
            var result = if (gate) |active|
                try long_executor.executeQualified(cb, allocator, config, tokenizer, &item, long_options, active)
            else switch (cb.kind()) {
                .native => try long_executor.executeNative(cb, allocator, config, tokenizer, &item, long_options),
                .wasm => try long_executor.executeBrowser(cb, allocator, config, tokenizer, &item, long_options),
                .metal, .cuda => try long_executor.executeDevice(cb, allocator, config, tokenizer, &item, long_options),
                else => return error.UnsupportedGlinerBoundaryBackend,
            };
            defer result.deinit();
            const used = try outputValues(result.sample);
            if (used > remaining_values) return error.ExtractionOutputLimitExceeded;
            remaining_values -= used;
            remaining_window_tokens -= result.encoded_tokens;
            remaining_window_attention -= result.attention_work_items;
            remaining_windows -= result.window_count;
            prompt_tokens = try std.math.add(usize, prompt_tokens, result.prompt_tokens);
            try progress(options, index, "serializing");
            try writer.append(item, result.sample);
            observeSample(options.observer, result.sample, result.prompt_tokens, used);
            index += 1;
            continue;
        }
        try progress(options, index, "tokenizing");
        var prepared = try prepareGroup(cb.kind(), allocator, config, tokenizer, request.items[index..], options);
        defer prepared.deinit();
        if (gate) |active| try observeGroup(allocator, config, request.items[index..], &prepared, options, active);
        const schemas = try allocator.alloc(*const @TypeOf(item.compiled), prepared.samples.len);
        defer allocator.free(schemas);
        for (schemas, request.items[index..][0..schemas.len]) |*out, *input| out.* = &input.compiled;
        for (prepared.samples) |sample| prompt_tokens = try std.math.add(usize, prompt_tokens, sample.input_ids.len);
        try progress(options, index, "encoder");
        var engine_options = options.engine;
        engine_options.control = options.control;
        var pipeline_options = item.options.native(options.pipeline);
        pipeline_options.control = options.control;
        pipeline_options.max_output_values = remaining_values;
        var result = switch (cb.kind()) {
            .native, .wasm => native: {
                var encoded = try engine.encodeNative(cb, allocator, config, &prepared, engine_options);
                defer encoded.deinit();
                try progress(options, index, "decoding");
                break :native try pipeline.runNative(cb, allocator, config, &prepared, schemas, .{
                    .text_states = encoded.text_states,
                    .query_states = encoded.query_states,
                    .classification_states = encoded.classification_states,
                    .text_lengths = encoded.text_lengths,
                }, pipeline_options);
            },
            .metal, .cuda => device: {
                var device_limits = options.device;
                device_limits.encoder = engine_options.limits;
                break :device (try device_request.run(cb, allocator, config, &prepared, schemas, .{
                    .precision = options.identity.?.precision,
                    .execution_policy = options.metal_execution_policy,
                    .limits = device_limits,
                    .pipeline = pipeline_options,
                })).outputs;
            },
            else => return error.UnsupportedGlinerBoundaryBackend,
        };
        defer result.deinit();
        if (result.samples.len != prepared.samples.len) return error.InvalidExtractionOutput;
        for (result.samples, prepared.samples, request.items[index..][0..result.samples.len], 0..) |sample, input, source, row| {
            const used = try outputValues(sample);
            if (used > remaining_values) return error.ExtractionOutputLimitExceeded;
            remaining_values -= used;
            try progress(options, index + row, "serializing");
            try writer.append(source, sample);
            observeSample(options.observer, sample, input.input_ids.len, used);
        }
        index += prepared.samples.len;
    }
    try progress(options, null, "serializing");
    return writer.finish(prompt_tokens);
}

test "extraction observability executor preserves scalar diagnostics without exposing outputs" {
    const Capture = struct {
        sample: ?observation.Sample = null,
        fn receive(raw: ?*anyopaque, event: observation.Event) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (event == .sample_decoded) self.sample = event.sample_decoded;
        }
    };
    var capture = Capture{};
    const sample = pipeline.Sample{ .classification_solver = .{ .status = .feasible, .visited_nodes = 17, .utility = 4, .exhausted = true }, .record_solver = .{ .status = .optimal, .visited_nodes = 3, .utility = 2 } };
    observeSample(null, sample, 5, 6);
    try std.testing.expect(capture.sample == null);
    observeSample(.{ .context = &capture, .emit_fn = Capture.receive }, sample, 5, 6);
    try std.testing.expectEqual(@as(usize, 5), capture.sample.?.prompt_tokens);
    try std.testing.expectEqual(@as(usize, 6), capture.sample.?.output_values);
    try std.testing.expect(capture.sample.?.solvers[0].?.exhausted);
    try std.testing.expect(capture.sample.?.solvers[1] == null);
    try std.testing.expectEqual(observation.Status.optimal, capture.sample.?.solvers[2].?.status);
    try std.testing.expectEqual(@as(usize, 3), capture.sample.?.solvers[2].?.visited_nodes);
}

test "gliner boundary executor preflight rejects unsupported later items and cancellation" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"boundary","schema":{"entities":["person"]},"inputs":[{"content":"John"},{"content":"Mary","options":{"decoder":{"beam_width":1024}}}]}
    , .{});
    defer request.deinit();
    var failure = wire.FailureContext{};
    try std.testing.expectError(error.ExtractionOptionLimitExceeded, preflight(&request, .{ .failure = &failure }));
    try std.testing.expectEqual(@as(?usize, 1), failure.input_index);
    try std.testing.expectEqualStrings("preflight", failure.stage);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, preflight(&request, .{ .control = .{ .check_fn = Cancel.check } }));
}

test "gliner boundary executor device scratch admits simultaneous owners and rejects invalid ceilings" {
    const options = Options{};
    const expected = options.device.max_encoder_device_bytes + options.device.head.max_device_bytes;
    try std.testing.expectEqual(expected, try deviceScratchUpperBound(options));
    var invalid = options;
    invalid.device.max_combined_device_bytes = expected - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, deviceScratchUpperBound(invalid));
    invalid = options;
    invalid.device.max_encoder_device_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.ResourceLimitExceeded, deviceScratchUpperBound(invalid));
}

test "gliner boundary executor pinned small production session and wire parity" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    try testPinnedSmallExecutor(directory, false, false);
}

test "gliner boundary executor pinned small converted FP32 session and wire parity" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_FP32_BUNDLE_DIR") orelse return error.SkipZigTest;
    try testPinnedSmallExecutor(directory, true, false);
}

test "gliner boundary executor pinned small Metal converted FP32 session and wire parity" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_FP32_BUNDLE_DIR") orelse return error.SkipZigTest;
    try testPinnedSmallExecutor(directory, true, true);
}

fn testPinnedSmallExecutor(directory: []const u8, converted: bool, metal: bool) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer fixture.deinit();
    const root = fixture.value.object;
    const pins = root.get("model_files").?.object;
    var bundle_manifest = if (converted) try @import("../models/manifest.zig").loadFromDir(a, directory) else null;
    defer if (bundle_manifest) |*manifest| manifest.deinit();
    if (bundle_manifest) |manifest| {
        const receipt = manifest.gliner_boundary_bundle orelse return error.InvalidGlinerBoundaryBundle;
        try std.testing.expectEqual(@import("../models/gliner_boundary_artifact.zig").Precision.fp32, receipt.value.precision);
        for (receipt.value.source_files) |source| {
            const expected_pin = pins.get(source.path) orelse return error.InvalidGlinerBoundaryBundle;
            try std.testing.expectEqual(@as(u64, @intCast(expected_pin.object.get("size_bytes").?.integer)), source.size_bytes);
            try std.testing.expectEqualStrings(expected_pin.object.get("sha256").?.string, source.sha256);
        }
    }
    for (pins.keys(), pins.values()) |name, pin| {
        if (converted and std.mem.eql(u8, name, "model.safetensors")) continue;
        const path = try std.fs.path.join(a, &.{ directory, name });
        defer a.free(path);
        var digest: [32]u8 = undefined;
        if (std.mem.eql(u8, name, "model.safetensors")) {
            var reader = try @import("../models/safetensors.zig").MMapReader.openFileAbsolute(a, path);
            defer reader.deinit();
            try std.testing.expectEqual(@as(usize, @intCast(pin.object.get("size_bytes").?.integer)), reader.file_bytes.len);
            std.crypto.hash.sha2.Sha256.hash(reader.file_bytes, &digest, .{});
        } else {
            const file_bytes = try @import("../util/c_file.zig").readFile(a, path);
            defer a.free(file_bytes);
            try std.testing.expectEqual(@as(usize, @intCast(pin.object.get("size_bytes").?.integer)), file_bytes.len);
            std.crypto.hash.sha2.Sha256.hash(file_bytes, &digest, .{});
        }
        try std.testing.expectEqualStrings(pin.object.get("sha256").?.string, &std.fmt.bytesToHex(digest, .lower));
    }
    const factory = @import("../architectures/session_factory.zig");
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerBoundaryConfig(session);
    try std.testing.expectEqual(model.Backbone.small, config.backbone);
    try std.testing.expectError(error.BoundaryExtractionRequiresSchema, session.run(&.{}, a));
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const case = root.get("cases").?.array.items[0].object;
    try std.testing.expectEqualStrings("mixed_tasks", case.get("id").?.string);
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "fastino/gliner2.5-small-v1",
        .schema = case.get("schema").?,
        .inputs = .{.{ .content = case.get("text").?.string }},
        .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
    }, .{});
    defer a.free(raw);
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 256 * 1024 * 1024 };
    const scratch = bounded.allocator();
    defer std.debug.assert(bounded.live == 0);
    var request = try wire.parseJson(scratch, raw, .{});
    defer request.deinit();
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = Control{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 180 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, scratch, null, control);
    defer managed.deinit();
    const response = try execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, .{ .identity = try factory.getGlinerBoundaryIdentity(session), .control = control });
    defer scratch.free(response);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, response, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("data").?.array.items[0].object;
    const entities = output.get("entities").?.array.items;
    try std.testing.expectEqual(@as(usize, 4), entities.len);
    var index: usize = 0;
    const expected = case.get("expected").?.object;
    for (expected.get("entities").?.array.items) |group| for (group.object.get("values").?.array.items) |value| {
        const actual = entities[index].object;
        index += 1;
        try std.testing.expectEqualStrings(group.object.get("name").?.string, actual.get("label").?.string);
        try std.testing.expectEqualStrings(value.object.get("text").?.string, actual.get("text").?.string);
        try std.testing.expectApproxEqAbs(value.object.get("confidence").?.float, actual.get("score").?.float, 5e-4);
        const source = value.object.get("source").?.object;
        inline for (.{ "start", "end" }) |key| try std.testing.expectEqual(source.get(key).?.integer, actual.get(key).?.integer);
    };
    const classifications = output.get("classifications").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), classifications.len);
    const classification = classifications[0].object;
    const wanted = expected.get("classifications").?.array.items[0].object;
    const label = wanted.get("labels").?.array.items[0].object;
    try std.testing.expectEqualStrings(wanted.get("name").?.string, classification.get("name").?.string);
    try std.testing.expectEqualStrings(label.get("label").?.string, classification.get("label").?.string);
    try std.testing.expectApproxEqAbs(label.get("confidence").?.float, classification.get("score").?.float, 5e-4);
    try std.testing.expect(!output.contains("relations"));
    try std.testing.expect(!bounded.denied);
}

test "gliner boundary executor output count includes attribute labels at every value occurrence" {
    const label = pipeline.Label{ .label = "selected", .confidence = 0.9 };
    const attributed = pipeline.Value{ .text = "item", .confidence = 0.9, .source = null, .token_span = null, .attributes = &.{.{ .name = "tags", .multi_label = true, .labels = &.{ label, label } }} };
    var entities = [_]pipeline.Value{attributed};
    var plain = attributed;
    plain.attributes = &.{};
    const sample = pipeline.Sample{
        .entities = &.{.{ .name = "product", .dtype = .list, .values = &entities }},
        .classifications = &.{.{ .name = "intent", .multi_label = false, .labels = &.{label} }},
        .relations = &.{.{ .name = "matches", .head = attributed, .tail = plain, .confidence = 0.9 }},
        .structures = &.{.{ .name = "record", .instances = &.{.{ .fields = &.{.{ .name = "field", .dtype = .str, .values = &.{attributed} }} }} }},
    };
    // Entity value + two labels, classification label, two relation endpoint
    // values + two labels, and structure value + two labels.
    try std.testing.expectEqual(@as(usize, 11), try outputValues(sample));
    try std.testing.expectEqual(@as(usize, 0), try outputValues(.{}));
}

test "gliner boundary executor pinned small window batch shares admission and recovers atomically" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    try testWindowBatch(directory, false);
}

test "gliner boundary executor pinned small Metal window batch shares admission and recovers atomically" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    try testWindowBatch(directory, true);
}

fn testWindowBatch(directory: []const u8, metal: bool) !void {
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, "pipeline_cases.json");
    defer a.free(bytes);
    var reference = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer reference.deinit();
    try std.testing.expectEqualStrings(reference.value.model_files.@"model.safetensors".sha256, &identity.weight.sha256);
    const config = try factory.getGlinerBoundaryConfig(session);
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const token_bytes = try @import("../util/c_file.zig").readFileMax(a, path, 32 * 1024 * 1024);
    defer a.free(token_bytes);
    try std.testing.expectEqualStrings(reference.value.model_files.@"tokenizer.json".sha256, &artifact.Digest.of(token_bytes).sha256);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, token_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const text = "John Smith works at Acme in London. Mary Johnson works at Fastino in Paris. David Brown works at Microsoft in Seattle.";
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "boundary",
        .schema = .{ .entities = .{ "person", "organization", "location" } },
        .inputs = .{ .{ .content = text }, .{ .content = text } },
        .options = .{ .include_confidence = true, .include_spans = true, .long_document = .{ .mode = "window", .window_words = @as(usize, 16), .overlap_words = @as(usize, 6) } },
    }, .{});
    defer a.free(raw);
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 256 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const scratch = bounded.allocator();
    var request = try wire.parseJson(scratch, raw, .{});
    defer request.deinit();
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = Control{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 180 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, scratch, null, control);
    defer managed.deinit();
    const options = Options{ .identity = identity, .control = control };
    const result = try execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options);
    defer scratch.free(result);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
    defer parsed.deinit();
    const outputs = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), outputs.len);
    var total_windows: usize = 0;
    for (outputs) |output| {
        const window_count: usize = @intCast(output.object.get("long_document").?.object.get("window_count").?.integer);
        try std.testing.expect(window_count > 1);
        total_windows += window_count;
        const entities = output.object.get("entities").?.array.items;
        try std.testing.expect(entities.len >= 3);
        var saw_late_mention = false;
        for (entities) |entity| {
            const value = entity.object;
            const start: usize = @intCast(value.get("start").?.integer);
            const end: usize = @intCast(value.get("end").?.integer);
            try std.testing.expect(start < end and end <= text.len);
            try std.testing.expectEqualStrings(text[start..end], value.get("text").?.string);
            saw_late_mention = saw_late_mention or start > text.len / 2;
        }
        try std.testing.expect(saw_late_mention);
    }
    var capped = options;
    capped.long_document.max_request_windows = total_windows - 1;
    try std.testing.expectError(error.LongDocumentWindowLimitExceeded, execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, capped));
    const prompt_tokens: usize = @intCast(parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    capped = options;
    capped.long_document.max_total_encoded_tokens = prompt_tokens - 1;
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, capped));
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw_context: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw_context.?));
            self.calls += 1;
            if (self.calls >= 100) return error.Cancelled;
        }
    };
    var cancellation = Cancel{};
    capped = options;
    capped.control = .{ .ptr = &cancellation, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, capped));
    const recovered = try execute(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options);
    defer scratch.free(recovered);
    try std.testing.expectEqualStrings(result, recovered);
    try std.testing.expect(!bounded.denied);
}

test "boundary qualification serving wrapper rejects before tokenizer model and scratch work" {
    const native = @import("../ops/native_compute.zig");
    var request = try wire.parseJson(std.testing.allocator,
        \\{"schema_version":2,"model":"boundary","schema":{"entities":["person"]},"inputs":[{"content":"Ada"}]}
    , .{});
    defer request.deinit();
    var store = native.WeightStore{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var context = native.NativeCompute.init(std.testing.allocator, &store, null);
    defer context.deinit();
    const cb = compute.ComputeBackend{ .ptr = &context, .vtable = &native.vtable_impl };
    const Probe = struct {
        calls: usize = 0,
        fn encode(raw: *anyopaque, _: Allocator, _: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedQualificationTokenization;
        }
        fn encodeInto(raw: *anyopaque, _: Allocator, _: []const u8, _: *std.ArrayListUnmanaged(i32)) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedQualificationTokenization;
        }
        fn specials(raw: *anyopaque) @import("inference_tokenizer").SpecialTokens {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return .{ .unk_id = 1 };
        }
        fn ids(raw: *anyopaque, _: Allocator) ![]u32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedQualificationTokenization;
        }
        fn vocabulary(raw: *anyopaque) usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return 512;
        }
    };
    var probe = Probe{};
    const tokenizer = Tokenizer{ .ptr = &probe, .vtable = &.{ .encode = Probe.encode, .encodeInto = Probe.encodeInto, .encodeForModel = undefined, .encodeGeneration = undefined, .decode = undefined, .specialTokens = Probe.specials, .allSpecialTokenIds = Probe.ids, .vocabSize = Probe.vocabulary, .deinit = undefined } };
    const config = model.Config{ .version = 3, .architecture_version = 1, .max_len = 64, .backbone = .small, .head = .{}, .encoder = std.mem.zeroes(model.EncoderConfig) };
    const identity = artifact.Identity{ .backbone = .small, .precision = .fp32, .weight = artifact.Digest.of("test weight"), .sidecars = @splat(artifact.Digest.of("test sidecar")) };
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = std.testing.allocator, .limit = 1 };
    const Count = struct {
        calls: usize = 0,
        fn observe(raw: ?*anyopaque, _: observation.Event) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
        }
    };
    var events = Count{};
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, executeQualified(&cb, bounded.allocator(), &config, tokenizer, &request, .{ .identity = identity, .observer = .{ .context = &events, .emit_fn = Count.observe } }));
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), events.calls);
    try std.testing.expectEqual(@as(usize, 0), bounded.peak);
    try std.testing.expectEqual(@as(usize, 0), context.weight_handles.count());
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, executeQualified(&cb, bounded.allocator(), &config, tokenizer, &request, .{ .identity = identity, .control = .{ .check_fn = Cancel.check } }));
    var cancelled_cb = cb;
    cancelled_cb.execution_control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, executeQualified(&cancelled_cb, bounded.allocator(), &config, tokenizer, &request, .{ .identity = identity }));
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
}

/// A test-only tokenizer supplies the real processor's ten structural markers
/// and bounded byte tokens. No vocabulary download or learned operation occurs.
const QualificationGeometryTest = struct {
    const policy = @import("../models/gliner_boundary_qualification.zig");
    const Mode = enum { success, late_item, late_window, cancelled, sequence_limit };
    const TokenizerProbe = struct {
        calls: usize = 0,
        one_token_words: bool = false,
        tokens_per_word: usize = 1,
        const markers = [_][]const u8{ "[P]", "[C]", "[E]", "[R]", "[L]", "[SEP_STRUCT]", "[SEP_TEXT]", "[DESCRIPTION]", "[EXAMPLE]", "[OUTPUT]" };
        fn tokenizer(self: *@This()) Tokenizer {
            return .{ .ptr = self, .vtable = &.{ .encode = encode, .encodeInto = encodeInto, .encodeForModel = undefined, .encodeGeneration = undefined, .decode = undefined, .specialTokens = specialTokens, .allSpecialTokenIds = specialIds, .vocabSize = vocabSize, .deinit = undefined } };
        }
        fn encode(raw: *anyopaque, a: Allocator, text: []const u8) ![]i32 {
            var out = std.ArrayListUnmanaged(i32).empty;
            errdefer out.deinit(a);
            try encodeInto(raw, a, text, &out);
            return out.toOwnedSlice(a);
        }
        fn encodeInto(raw: *anyopaque, a: Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            var position: usize = 0;
            while (position < text.len) {
                var matched = false;
                for (markers, 0..) |marker, i| if (std.mem.startsWith(u8, text[position..], marker)) {
                    try out.append(a, @intCast(300 + i));
                    position += marker.len;
                    matched = true;
                    break;
                };
                if (matched) continue;
                if (self.one_token_words) {
                    for (0..self.tokens_per_word) |_| try out.append(a, 64);
                    return;
                }
                try out.append(a, @as(i32, text[position]) + 10);
                position += 1;
            }
        }
        fn specialTokens(_: *anyopaque) @import("inference_tokenizer").SpecialTokens {
            return .{ .unk_id = 1 };
        }
        fn specialIds(_: *anyopaque, a: Allocator) ![]u32 {
            const ids = try a.alloc(u32, markers.len);
            for (ids, 0..) |*id, i| id.* = @intCast(300 + i);
            return ids;
        }
        fn vocabSize(_: *anyopaque) usize {
            return 512;
        }
    };
    const Receiver = struct {
        mode: Mode,
        count: usize = 0,
        cancelled: bool = false,
        events: usize = 0,
        values: [8]policy.LengthContract = undefined,
        prefix_words: [8]usize = undefined,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn event(raw: ?*anyopaque, _: observation.Event) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.events += 1;
        }
        // Compile-time receiver only for quiet tokenization/planning. It is
        // never accepted by executeQualified or the learned long executor.
        pub fn observe(self: *@This(), bytes: usize, words: usize, windows: usize, prepared: *const processor.PreparedBatch) !void {
            if (self.count == self.values.len) return error.TestUnexpectedResult;
            self.values[self.count] = try qualification.lengths(3, bytes, words, windows, prepared);
            self.prefix_words[self.count] = prepared.samples[0].prefix_word_count;
            try std.testing.expectEqual(prepared.samples[0].input_ids.len, prepared.sequence_length);
            self.count += 1;
            if ((self.mode == .late_item and self.count == 4) or (self.mode == .late_window and self.count == 3)) return error.UnsupportedGlinerBoundaryRuntime;
            if (self.mode == .cancelled and self.count == 2) self.cancelled = true;
        }
        pub fn observeSingle(self: *@This(), a: Allocator, text: []const u8, prepared: *const processor.PreparedBatch, supplied: processor.Options) !void {
            try self.observe(text.len, try qualification.sourceWords(a, text, supplied), 1, prepared);
        }
    };
    fn config() model.Config {
        return .{ .version = 3, .architecture_version = 1, .max_len = 32, .backbone = .small, .head = .{ .candidate_pool = .shared, .candidate_attention_layers = 0, .query_attention_layers = 0 }, .encoder = .{
            .hidden_size = 64,
            .intermediate_size = 128,
            .num_hidden_layers = 1,
            .num_attention_heads = 1,
            .vocab_size = 512,
            .max_position_embeddings = 512,
            .position_buckets = 256,
            .layer_norm_eps = 1e-7,
            .hidden_dropout_prob = 0,
            .attention_probs_dropout_prob = 0,
            .pad_token_id = 0,
        } };
    }
    fn exercise(backing: Allocator, mode: Mode) !void {
        const native = @import("../ops/native_compute.zig");
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = backing, .limit = 2 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const a = bounded.allocator();
        var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        defer store.deinitOwned();
        var context = native.NativeCompute.init(a, &store, null);
        defer context.deinit();
        const cb = compute.ComputeBackend{ .ptr = &context, .vtable = &native.vtable_impl };
        var request = try wire.parseJson(a,
            \\{"schema_version":2,"model":"geometry-only","schema":{"entities":["person"]},"inputs":[{"content":"Ada 東京"},{"content":"Α X 😀 Y Z","schema":{"structures":{"row":{"mode":"latent","fields":{"status":{"choices":["active","former"]}}}}},"options":{"long_document":{"mode":"window","window_words":4,"overlap_words":1}}},{"content":"tail","options":{"word_splitter":"char"}}]}
        , .{});
        defer request.deinit();
        var fingerprints: [3][32]u8 = undefined;
        for (request.items, &fingerprints) |item, *fingerprint| fingerprint.* = item.compiled.fingerprint;
        var tokenizer = TokenizerProbe{};
        var receiver = Receiver{ .mode = mode };
        const identity = artifact.Identity{ .backbone = .small, .precision = .fp32, .weight = artifact.Digest.of("geometry weight"), .sidecars = @splat(artifact.Digest.of("geometry sidecar")) };
        var options = Options{
            .identity = identity,
            .processor = .{ .max_text_words = 32, .max_total_words = 64, .max_sequence_tokens = 128, .max_batch_tokens = 128, .max_queries = 8, .max_groups = 4 },
            .engine = .{ .limits = .{ .max_sequence_tokens = 128, .max_batch_tokens = 128, .max_queries = 8, .max_groups = 4 } },
            .control = .{ .ptr = &receiver, .check_fn = Receiver.check },
            .observer = .{ .context = &receiver, .emit_fn = Receiver.event },
            .long_document = .{ .plan = .{ .max_memory_bytes = 256 * 1024, .max_document_bytes = 1024, .max_document_words = 64, .max_window_scan_bytes = 4096, .max_windows = 8 }, .max_request_windows = 8, .max_total_encoded_tokens = 256 },
        };
        if (mode == .sequence_limit) options.processor.max_sequence_tokens = 1;
        const cfg = config();
        const baseline = bounded.live;
        const outcome = qualifyRequestGeometry(&cb, a, &cfg, tokenizer.tokenizer(), &request, options, &receiver);
        if (mode == .success) {
            try outcome;
        } else {
            const expected: anyerror = switch (mode) {
                .late_item, .late_window => error.UnsupportedGlinerBoundaryRuntime,
                .cancelled => error.Cancelled,
                .sequence_limit => error.BoundarySequenceLimitExceeded,
                .success => unreachable,
            };
            // Preserve allocation-failure attribution for the shared exercise.
            outcome catch |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(expected, err);
                try std.testing.expectEqual(@as(usize, switch (mode) {
                    .late_item => 4,
                    .late_window => 3,
                    .cancelled => 2,
                    .sequence_limit => 0,
                    .success => unreachable,
                }), receiver.count);
            };
            // expectError catches an unexpected successful preflight too.
            try std.testing.expectError(expected, outcome);
        }
        try std.testing.expectEqual(baseline, bounded.live);
        try std.testing.expectEqual(@as(usize, 0), receiver.events);
        try std.testing.expectEqual(@as(usize, 0), context.weight_handles.count());
        for (request.items, fingerprints) |item, fingerprint| try std.testing.expectEqual(fingerprint, item.compiled.fingerprint);
        if (mode != .success) {
            // A rejected or cancelled request leaves no retained plan/prepare
            // owner. Retry reuses the same tokenizer, request, and empty backend.
            receiver.mode = .success;
            receiver.count = 0;
            receiver.cancelled = false;
            options.processor.max_sequence_tokens = 128;
            try qualifyRequestGeometry(&cb, a, &cfg, tokenizer.tokenizer(), &request, options, &receiver);
        }
        try std.testing.expectEqual(@as(usize, 4), receiver.count);
        const expected_bytes = [_]u64{ 10, 13, 13, 4 };
        const expected_words = [_]u64{ 2, 5, 5, 1 };
        const expected_windows = [_]u64{ 1, 2, 2, 1 };
        const expected_body = [_]u64{ 3, 4, 4, 1 };
        for (receiver.values[0..receiver.count], 0..) |value, i| {
            try std.testing.expectEqual(@as(u64, 3), value.request_items.min);
            try std.testing.expectEqual(expected_bytes[i], value.document_bytes.min);
            try std.testing.expectEqual(expected_words[i], value.document_words.min);
            try std.testing.expectEqual(expected_windows[i], value.window_count.min);
            try std.testing.expectEqual(expected_body[i], value.window_words.min);
            try std.testing.expect(value.padded_sequence_tokens.min > value.window_words.min);
            try std.testing.expect(value.padded_sequence_tokens.min < options.processor.max_sequence_tokens);
            inline for (comptime std.meta.fieldNames(policy.LengthContract)) |reflected_name| try std.testing.expectEqual(@field(value, reflected_name).min, @field(value, reflected_name).max);
        }
        try std.testing.expectEqual(@as(usize, 9), receiver.prefix_words[1]);
        try std.testing.expectEqual(@as(usize, 9), receiver.prefix_words[2]);
        var expected_workspace_tokens: usize = 0;
        for (receiver.values[0..receiver.count]) |value| expected_workspace_tokens = @max(expected_workspace_tokens, @as(usize, @intCast(value.padded_sequence_tokens.min)));
        // The original qualification probe also covers native-only head
        // configurations. Workspace planning uses the published Metal head.
        var metal_cfg = cfg;
        metal_cfg.head.candidate_pool = .shared;
        metal_cfg.head.candidate_attention_layers = 0;
        metal_cfg.head.query_attention_layers = 0;
        options.long_document.window_batch_size = 1;
        const serial_workspace_tokens = try workspaceGeometry(a, &metal_cfg, tokenizer.tokenizer(), &request, options);
        try std.testing.expectEqual(expected_workspace_tokens, serial_workspace_tokens);
        options.long_document.window_batch_size = 2;
        const batched_workspace_tokens = try workspaceGeometry(a, &metal_cfg, tokenizer.tokenizer(), &request, options);
        try std.testing.expect(batched_workspace_tokens > serial_workspace_tokens);

        try std.testing.expectEqual(baseline, bounded.live);
        try std.testing.expectEqual(@as(usize, 0), receiver.events);
        try std.testing.expectEqual(@as(usize, 0), context.weight_handles.count());
        try std.testing.expect(tokenizer.calls > 0);
        try std.testing.expect(!bounded.denied);
    }
};

test "boundary qualification quiet geometry covers complete request and all owned Unicode windows" {
    try QualificationGeometryTest.exercise(std.testing.allocator, .success);
}

test "boundary qualification serving uses full context beyond measured composition profiles" {
    const a = std.testing.allocator;
    const policy = @import("../models/gliner_boundary_qualification.zig");
    // Request the artifact's full word capacity, beyond both historical token
    // profiles. Two encoded tokens per word also exercise DeBERTa beyond its
    // relative-position table extent, which must not become a context cap.
    const word = "configuration_visibility_maintenance_acknowledgement ";
    const text = try a.alloc(u8, word.len * 8200);
    defer a.free(text);
    for (0..8200) |index| @memcpy(text[index * word.len ..][0..word.len], word);
    try std.testing.expect(text.len > 182000 and text.len < 1024 * 1024);
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "geometry-only",
        .schema = .{ .entities = .{"concept"} },
        .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 4096) } },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    var tokenizer = QualificationGeometryTest.TokenizerProbe{ .one_token_words = true, .tokens_per_word = 2 };
    var config = QualificationGeometryTest.config();
    config.max_len = 4096;
    config.backbone = policy.fastino_gliner25_base_v1.backbone;
    config.encoder.max_position_embeddings = 512;
    config.head.candidate_pool = .shared;
    config.head.candidate_attention_layers = 0;
    config.head.query_attention_layers = 0;
    var options = Options{ .identity = policy.fastino_gliner25_base_v1 };
    options.engine.limits.max_batch_tokens = 9000;
    var gate = try qualification.Admission.initResolved(options.identity.?, .metal, &request, options.pipeline, null);
    try planRequestGeometry(.metal, a, &config, tokenizer.tokenizer(), &request, options, &gate);
    try std.testing.expect(gate.long_document_supported);
    // Workspace sizing must use the identical bounded window geometry, even
    // though its receiver performs no qualification checks of its own.
    const workspace = try workspaceGeometry(a, &config, tokenizer.tokenizer(), &request, options);
    try std.testing.expect(workspace > 8192 and workspace <= 9000);
    // The measured profile remains an evidence checker, separate from serving.
    var coverage = try qualification.Gate.initResolved(options.identity.?, .metal, &request, options.pipeline, null);
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, planRequestGeometry(.metal, a, &config, tokenizer.tokenizer(), &request, options, &coverage));
    // A low trial width exceeds the window-count cap, while a larger width
    // fits both that cap and exact token capacity. Search must continue upward.
    options.processor.max_sequence_tokens = 6000;
    options.long_document.plan.max_windows = 3;
    const fitted = try workspaceGeometry(a, &config, tokenizer.tokenizer(), &request, options);
    try std.testing.expect(fitted > 5900 and fitted <= 6000);
    options.long_document.max_total_encoded_tokens = 128;
    try std.testing.expectError(error.LongDocumentWorkLimitExceeded, workspaceGeometry(a, &config, tokenizer.tokenizer(), &request, options));
}

test "boundary qualification exact prompt limit and model body limit remain distinct" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 4096 * 2);
    defer a.free(text);
    for (0..4096) |i| @memcpy(text[i * 2 ..][0..2], "x ");
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "geometry-only",
        .schema = .{ .entities = .{"concept"} },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    var tokenizer = QualificationGeometryTest.TokenizerProbe{ .one_token_words = true, .tokens_per_word = 2 };
    var config = QualificationGeometryTest.config();
    config.max_len = 4096;
    var options = Options{};
    // 4096 source words plus synthetic punctuation is over the model's word
    // limit; explicit windowing is required. No silent truncation is allowed.
    try std.testing.expectError(error.BoundaryTextLimitExceeded, prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, options));
    request.items[0].text = text[0 .. text.len - 2];
    var prepared = try prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, options);
    const exact = prepared.sequence_length;
    try std.testing.expectEqual(@as(usize, 4096), prepared.samples[0].body_word_count);
    try std.testing.expect(exact > 8192);
    prepared.deinit();
    options.processor.max_sequence_tokens = exact;
    prepared = try prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, options);
    prepared.deinit();
    options.processor.max_sequence_tokens = exact - 1;
    try std.testing.expectError(error.BoundarySequenceLimitExceeded, prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, options));
    // Relative-position DeBERTa accepted the full prompt above. The absolute
    // ModernBERT extent must instead constrain preparation before execution.
    options.processor.max_sequence_tokens = 16384;
    config.encoder.family = .modern_bert;
    try std.testing.expectError(error.BoundarySequenceLimitExceeded, prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, options));
}

test "boundary qualification real base tokenizer fits full encoded context" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const c_file = @import("../util/c_file.zig");
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const word = "configuration_visibility_maintenance_acknowledgement ";
    const text = try a.alloc(u8, word.len * 4096);
    defer a.free(text);
    for (0..4096) |i| @memcpy(text[i * word.len ..][0..word.len], word);
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "base",
        .schema = .{ .entities = .{"concept"} },
        .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 4096) } },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    const identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    var admission = try qualification.Admission.initResolved(identity, .native, &request, .{}, null);
    const Receiver = struct {
        admission: *qualification.Admission,
        peak: usize = 0,
        pub fn observe(self: *@This(), bytes: usize, words: usize, windows: usize, prepared: *const processor.PreparedBatch) !void {
            try self.admission.observe(bytes, words, windows, prepared);
            self.peak = @max(self.peak, prepared.sequence_length);
            try std.testing.expect(prepared.samples[0].body_word_count <= 4096);
        }
    };
    var receiver = Receiver{ .admission = &admission };
    const supplied = long_executor.Options{
        .identity = identity,
        .limits = .{
            // This explicitly larger work budget permits a full-context geometry
            // test; normal serving still enforces its configured cumulative budget.
            .max_total_attention_work = 512 * 1024 * 1024 * 1024,
            .window_batch_size = 1,
        },
    };
    const usage = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], supplied, &receiver);
    try std.testing.expect(usage.window_count > 1);
    try std.testing.expect(receiver.peak > 16000 and receiver.peak <= 16384);
    std.debug.print("full-context base tokenizer: peak_tokens={} windows={} total_tokens={}\n", .{ receiver.peak, usage.window_count, usage.prompt_tokens });
}

test "boundary full context learned Metal base serving uses word and token capacity" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    try learnedFullContext(.metal, .both, false);
}

test "boundary full context learned CUDA base serving uses word and token capacity" {
    if (!@import("build_options").enable_cuda) return error.SkipZigTest;
    try learnedFullContext(.cuda, .both, false);
}

test "boundary full context learned native base word capacity" {
    try learnedFullContext(.native, .words, false);
}

test "boundary full context learned native base token capacity" {
    try learnedFullContext(.native, .tokens, false);
}

test "boundary full context learned native grouped words" {
    try learnedFullContext(.native, .words, true);
}

fn learnedFullContext(comptime backend: enum { native, metal, cuda }, comptime extent: enum { words, tokens, both }, comptime grouped: bool) !void {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const session = switch (backend) {
        .native => try factory.createNativeSession(a, directory),
        .metal => try factory.createMetalSession(a, directory),
        .cuda => try factory.createCudaSession(a, directory),
    };
    defer session.close();
    const config = try factory.getGlinerBoundaryConfig(session);
    const identity = try factory.getGlinerBoundaryIdentity(session);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();
    const source_words = (config.max_len - 1) * (if (grouped) @as(usize, 4) else 1);
    const text = try a.alloc(u8, source_words * 4);
    defer a.free(text);
    for (0..source_words) |i| @memcpy(text[i * 4 ..][0..4], "the ");
    const schema_json = if (grouped)
        "{\"entities\":[\"concept\"],\"entity_definitions\":{\"concept\":{\"description\":\"A concept\"}}}"
    else
        "{\"entities\":[\"concept\"]}";
    const schema = try std.json.parseFromSlice(std.json.Value, a, schema_json, .{});
    defer schema.deinit();
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "base",
        .schema = schema.value,
        .options = .{ .long_document = .{ .mode = "window", .window_words = config.max_len, .overlap_words = if (grouped) @as(usize, 0) else 128 } },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    // Verify this is exactly the full body-word boundary before learned work.
    var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = text[0 .. (config.max_len - 1) * 4], .schema = &request.items[0].compiled }}, .{});
    try std.testing.expectEqual(@as(usize, config.max_len), prepared.samples[0].body_word_count);
    const exact_tokens = prepared.sequence_length;
    prepared.deinit();
    const watchdog = try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const control = Control{ .hard_cancellation = watchdog.boundary(), .deadline_ns = @import("antfly_platform").time.monotonicNs() + (if (backend == .native) @as(u64, 900) else 180) * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    if (extent != .tokens) {
        const response = try executeQualified(&managed.backend, a, &config, tok.tokenizer(), &request, .{ .identity = identity, .control = control });
        defer a.free(response);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, response, .{});
        defer parsed.deinit();
        const root = parsed.value.object;
        try std.testing.expectEqual(@as(i64, @intCast(exact_tokens * (if (grouped) @as(usize, 4) else 1))), root.get("usage").?.object.get("prompt_tokens").?.integer);
        const rows = root.get("data").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqual(if (grouped) @as(i64, 4) else 1, rows[0].object.get("long_document").?.object.get("window_count").?.integer);
        std.debug.print("full-context learned {s}: body_words={} prompt_tokens={} windows={}\n", .{ @tagName(backend), config.max_len, exact_tokens * (if (grouped) @as(usize, 4) else 1), if (grouped) @as(usize, 4) else 1 });
    }
    if (extent == .words) return;

    // Exercise the independent encoded-token boundary with the real tokenizer,
    // weights and default work budget too. Measure one word's token cost using
    // the same complete schema/punctuation grammar, then fill that capacity.
    const word = "configuration_visibility_maintenance_acknowledgement ";
    var probe = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = word, .schema = &request.items[0].compiled }}, .{});
    const first_word = probe.samples[0].words[0];
    const word_tokens = first_word.input_end - first_word.input_start;
    const overhead = probe.sequence_length - word_tokens;
    probe.deinit();
    const capacity = qualification.sequenceLimit(&config, (engine.Limits{}).max_sequence_tokens);
    const dense_source_words = (capacity - overhead) / word_tokens;
    try std.testing.expect(dense_source_words + 1 <= config.max_len);
    const dense_text = try a.alloc(u8, dense_source_words * word.len);
    defer a.free(dense_text);
    for (0..dense_source_words) |i| @memcpy(dense_text[i * word.len ..][0..word.len], word);
    request.items[0].text = dense_text;
    var dense = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = dense_text, .schema = &request.items[0].compiled }}, .{});
    const dense_tokens = dense.sequence_length;
    dense.deinit();
    try std.testing.expect(dense_tokens > 16000 and dense_tokens <= capacity);
    const dense_response = try executeQualified(&managed.backend, a, &config, tok.tokenizer(), &request, .{ .identity = identity, .control = control });
    defer a.free(dense_response);
    const dense_parsed = try std.json.parseFromSlice(std.json.Value, a, dense_response, .{});
    defer dense_parsed.deinit();
    const dense_root = dense_parsed.value.object;
    try std.testing.expectEqual(@as(i64, @intCast(dense_tokens)), dense_root.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 1), dense_root.get("data").?.array.items[0].object.get("long_document").?.object.get("window_count").?.integer);
    std.debug.print("full-context learned {s}: prompt_tokens={}/{} windows=1 default_work_budget\n", .{ @tagName(backend), dense_tokens, capacity });
}

const PreparationReuseTest = struct {
    const Receiver = struct {
        encoded: usize = 0,
        attention: u64 = 0,
        single_attention: u64 = 0,
        pub fn observe(self: *@This(), _: usize, _: usize, _: usize, prepared: *const processor.PreparedBatch) !void {
            self.single_attention += prepared.sequence_length * prepared.sequence_length;
        }
        pub fn observeWindowGroup(self: *@This(), prepared: *const processor.PreparedBatch) void {
            self.encoded += prepared.input_ids.len;
            self.attention += prepared.samples.len * prepared.sequence_length * prepared.sequence_length;
        }
    };
    fn exercise(a: Allocator) !void {
        var request = try wire.parseJson(a,
            \\{"schema_version":2,"model":"geometry-only","schema":{"entities":["concept"]},"options":{"long_document":{"mode":"window","window_words":4,"overlap_words":1}},"inputs":[{"content":"a b c d e f g h i j k l m n o p q r s t"}]}
        , .{});
        defer request.deinit();
        var tokenizer = QualificationGeometryTest.TokenizerProbe{};
        const config = QualificationGeometryTest.config();
        var store = PreparationStore.init(a);
        defer store.deinit();
        var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
        identity.backbone = config.backbone;
        const options = long_executor.Options{ .preparations = &store, .identity = identity };
        var receiver = Receiver{};
        const used = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
        try std.testing.expect(used.encoded_tokens > used.prompt_tokens);
        try std.testing.expectEqual(receiver.encoded, used.encoded_tokens);
        try std.testing.expectEqual(receiver.attention, used.attention_work_items);
        const calls = tokenizer.calls;
        receiver = .{};
        const repeated = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
        try std.testing.expectEqual(calls, tokenizer.calls);
        try std.testing.expectEqualDeep(used, repeated);
        // Decoder caps may change after earlier items produce output; they
        // must refresh semantic identity without re-tokenizing these windows.
        var decoded = options;
        decoded.pipeline.max_output_values -= 1;
        const adjusted = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], decoded, &receiver);
        try std.testing.expectEqualDeep(used, adjusted);
        try std.testing.expectEqual(calls, tokenizer.calls);

        receiver = .{};
        _ = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
        try std.testing.expect(receiver.single_attention < used.attention_work_items);
        var limited = options;
        limited.limits.max_total_attention_work = receiver.single_attention;
        const attention_failure = long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], limited, &receiver);
        if (attention_failure) |_| return error.TestUnexpectedResult else |err| {
            if (err == error.OutOfMemory) return err;
            try std.testing.expectEqual(@as(anyerror, error.LongDocumentWorkLimitExceeded), err);
        }
        limited = options;
        limited.limits.max_total_encoded_tokens = used.prompt_tokens;
        const token_failure = long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], limited, &receiver);
        if (token_failure) |_| return error.TestUnexpectedResult else |err| {
            if (err == error.OutOfMemory) return err;
            try std.testing.expectEqual(@as(anyerror, error.LongDocumentWorkLimitExceeded), err);
        }
        limited = options;
        limited.limits.max_prepared_bytes = 1;
        const memory_failure = long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], limited, &receiver);
        if (memory_failure) |_| return error.TestUnexpectedResult else |err| {
            if (err == error.OutOfMemory) return err;
            try std.testing.expectEqual(@as(anyerror, error.MemoryBudgetExceeded), err);
        }
        _ = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
        try std.testing.expectEqual(calls, tokenizer.calls);
    }
};

test "boundary qualification padded work and repeated planning reuse bounded samples" {
    try PreparationReuseTest.exercise(std.testing.allocator);
}

test "boundary qualification retained preparation releases every failed allocation" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, PreparationReuseTest.exercise, .{});
}

test "boundary preparation benchmark isolates request-local tokenizer reuse" {
    const env = @import("antfly_platform").env;
    if (!env.getenvBool("ANTFLY_GLINER_PREPARATION_BENCH")) return error.SkipZigTest;
    const directory = env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    defer a.free(bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const config_bytes = try fixtures.fixtureBytes(a, "models/base/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/base/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const sentence = "Alice met Bob at Antfly to discuss maintenance and visibility. ";
    const text = try a.alloc(u8, sentence.len * 120);
    defer a.free(text);
    for (0..120) |i| @memcpy(text[i * sentence.len ..][0..sentence.len], sentence);
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "base",
        .schema = .{ .entities = .{ "person", "organization", "concept" } },
        .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 128), .overlap_words = @as(usize, 16) } },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    const Receiver = struct {
        pub fn observe(_: *@This(), _: usize, _: usize, _: usize, _: *const processor.PreparedBatch) !void {}
    };
    var receiver = Receiver{};
    var expected: ?long_executor.GeometryUsage = null;
    var totals = [_]u64{ 0, 0 };
    // ABBA ordering reduces warmup/order bias. Each request has three quiet
    // geometry passes and a fresh store; tokenizer loading is outside timing.
    for ([_]bool{ false, true, true, false }) |reuse| {
        var store = PreparationStore.init(a);
        var store_live = true;
        defer if (store_live) store.deinit();
        const options = long_executor.Options{ .identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1, .preparations = if (reuse) &store else null };
        const started = @import("antfly_platform").time.monotonicNs();
        for (0..3) |_| {
            const used = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
            if (expected) |value| try std.testing.expectEqualDeep(value, used) else expected = used;
        }
        store.deinit();
        store_live = false;
        const elapsed = @import("antfly_platform").time.monotonicNs() - started;
        totals[@intFromBool(reuse)] += elapsed;
        std.debug.print("preparation_bench reuse={} passes=3 ns={}\n", .{ reuse, elapsed });
    }
    std.debug.print("preparation_bench total_without_ns={} total_with_ns={}\n", .{ totals[0], totals[1] });
}

test "boundary qualification later item and window rejection cancels quietly and retries cleanly" {
    for ([_]QualificationGeometryTest.Mode{ .late_item, .late_window, .cancelled, .sequence_limit }) |mode| try QualificationGeometryTest.exercise(std.testing.allocator, mode);
}

test "boundary qualification quiet geometry releases every failed allocation" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, QualificationGeometryTest.exercise, .{.success});
}

test "GPU boundary request groups distinct ragged inputs within physical limits" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"geometry-only","schema":{"entities":["person"]},"inputs":[{"content":"Ada"},{"content":"Ada visited 東京 with Grace"},{"content":"last","options":{"word_splitter":"char"}}]}
    , .{});
    defer request.deinit();
    var tokenizer = QualificationGeometryTest.TokenizerProbe{};
    const config = QualificationGeometryTest.config();
    var options = Options{};
    var group = try prepareGroup(.cuda, a, &config, tokenizer.tokenizer(), request.items, options);
    defer group.deinit();
    try std.testing.expectEqual(@as(usize, 2), group.samples.len);
    try std.testing.expect(group.samples[0].input_ids.len < group.sequence_length);
    try std.testing.expectEqual(group.sequence_length, group.samples[1].input_ids.len);
    for (group.attention_mask[group.samples[0].input_ids.len..group.sequence_length]) |value| try std.testing.expectEqual(@as(i64, 0), value);
    const Receiver = struct {
        count: usize = 0,
        padded: usize,
        pub fn observeSingle(self: *@This(), allocator: Allocator, source: []const u8, prepared: *const processor.PreparedBatch, supplied: processor.Options) !void {
            const contract = try qualification.lengths(3, source.len, try qualification.sourceWords(allocator, source, supplied), 1, prepared);
            try std.testing.expectEqual(@as(u64, @intCast(self.padded)), contract.padded_sequence_tokens.min);
            self.count += 1;
        }
    };
    var receiver = Receiver{ .padded = group.sequence_length };
    try observeGroup(a, &config, request.items, &group, options, &receiver);
    try std.testing.expectEqual(@as(usize, 2), receiver.count);
    options.processor.max_batch_tokens = group.input_ids.len - 1;
    var bounded = try prepareGroup(.cuda, a, &config, tokenizer.tokenizer(), request.items, options);
    defer bounded.deinit();
    try std.testing.expectEqual(@as(usize, 1), bounded.samples.len);
    var metal = try prepareGroup(.metal, a, &config, tokenizer.tokenizer(), request.items, .{});
    defer metal.deinit();
    try std.testing.expectEqual(@as(usize, 2), metal.samples.len);
    try std.testing.expectEqualSlices(i64, group.input_ids, metal.input_ids);
    try std.testing.expectEqualSlices(i64, group.attention_mask, metal.attention_mask);
    var metal_bounded = try prepareGroup(.metal, a, &config, tokenizer.tokenizer(), request.items, options);
    defer metal_bounded.deinit();
    try std.testing.expectEqual(@as(usize, 1), metal_bounded.samples.len);
    var serial = try prepareGroup(.native, a, &config, tokenizer.tokenizer(), request.items, .{});
    defer serial.deinit();
    try std.testing.expectEqual(@as(usize, 1), serial.samples.len);
    const geometry_options = Options{ .identity = .{ .backbone = .small, .precision = .fp32, .weight = artifact.Digest.of("geometry weight"), .sidecars = @splat(artifact.Digest.of("geometry sidecar")) } };
    var tail = try prepareGroup(.metal, a, &config, tokenizer.tokenizer(), request.items[2..], geometry_options);
    defer tail.deinit();
    const peak = try workspaceGeometry(a, &config, tokenizer.tokenizer(), &request, geometry_options);
    try std.testing.expectEqual(@max(metal.input_ids.len, tail.input_ids.len), peak);
    var serial_geometry_options = geometry_options;
    serial_geometry_options.engine.limits.max_batch = 1;
    try std.testing.expect(peak > try workspaceGeometry(a, &config, tokenizer.tokenizer(), &request, serial_geometry_options));
    // The reference device path reserves the request's own encoder plan, not
    // the configured ceiling, and refuses a ceiling the request cannot fit.
    const encoder_bytes = try referenceEncoderDeviceBytes(.metal, a, &config, tokenizer.tokenizer(), &request, geometry_options);
    try std.testing.expect(encoder_bytes > 0);
    try std.testing.expect(encoder_bytes < geometry_options.device.max_encoder_device_bytes);
    try std.testing.expect(encoder_bytes > try referenceEncoderDeviceBytes(.metal, a, &config, tokenizer.tokenizer(), &request, serial_geometry_options));
    var tight = geometry_options;
    tight.device.max_encoder_device_bytes = encoder_bytes - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, referenceEncoderDeviceBytes(.metal, a, &config, tokenizer.tokenizer(), &request, tight));
}

test "GPU boundary classification windows fit backend-specific reference reservations" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"geometry-only","schema":{"classifications":[{"name":"intent","labels":["refund","support"]}]},"inputs":[{"content":"Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada Ada"}]}
    , .{});
    defer request.deinit();
    request.items[0].options.long_document = .{ .mode = .window, .window_words = 4, .overlap_words = 1, .max_windows = 16 };
    var tokenizer = QualificationGeometryTest.TokenizerProbe{};
    const config = QualificationGeometryTest.config();
    const options = Options{ .identity = .{ .backbone = .small, .precision = .fp32, .weight = artifact.Digest.of("geometry weight"), .sidecars = @splat(artifact.Digest.of("geometry sidecar")) } };
    const metal_reserved = try referenceEncoderDeviceBytes(.metal, a, &config, tokenizer.tokenizer(), &request, options);
    const reserved = try referenceEncoderDeviceBytes(.cuda, a, &config, tokenizer.tokenizer(), &request, options);
    const Receiver = struct {
        config: *const model.Config,
        options: Options,
        peak: usize = 0,
        failure: ?anyerror = null,
        fn note(self: *@This(), prepared: *const processor.PreparedBatch) void {
            const planned = device_engine.plan(self.config, prepared, .{ .precision = .fp32, .max_device_bytes = self.options.device.max_encoder_device_bytes }) catch |err| {
                self.failure = err;
                return;
            };
            self.peak = @max(self.peak, planned.device_upper_bound_bytes);
        }
        pub fn observe(self: *@This(), _: usize, _: usize, _: usize, prepared: *const processor.PreparedBatch) !void {
            self.note(prepared);
        }
        pub fn observeSingle(_: *@This(), _: Allocator, _: []const u8, _: *const processor.PreparedBatch, _: processor.Options) !void {}
        pub fn observeWindowGroup(self: *@This(), prepared: *const processor.PreparedBatch) void {
            self.note(prepared);
        }
    };
    var receiver = Receiver{ .config = &config, .options = options };
    try planRequestGeometry(.cuda, a, &config, tokenizer.tokenizer(), &request, options, &receiver);
    if (receiver.failure) |err| return err;
    try std.testing.expectEqual(receiver.peak, reserved);
    try std.testing.expect(reserved > metal_reserved);
    var tight = options;
    tight.device.max_encoder_device_bytes = reserved - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, referenceEncoderDeviceBytes(.cuda, a, &config, tokenizer.tokenizer(), &request, tight));
}

test "CUDA boundary managed Multi-Decide batches windows and recovers after cancellation" {
    if (!@import("build_options").enable_cuda) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const precision = std.meta.stringToEnum(factory.GlinerCudaPrecision, @import("antfly_platform").env.getenv("ANTFLY_GLINER25_FAMILY_CUDA_PRECISION") orelse "fp32") orelse return error.InvalidPrecision;
    const session = try factory.createGlinerCudaSessionWithPrecision(a, directory, precision);
    defer session.close();
    std.debug.print("family_cuda_windows: precision={s}\n", .{@tagName(precision)});
    const config = try factory.getGlinerBoundaryConfig(session);
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try std.testing.expectEqual(model.Backbone.multi, config.backbone);
    try std.testing.expectEqualStrings("9efe0f88c99f2aa794452e9559dc60e98d60d9fa2bf1b60cf2710411b6da5b4e", &identity.weight.sha256);
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const token_bytes = try @import("../util/c_file.zig").readFileMax(a, path, 32 * 1024 * 1024);
    defer a.free(token_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytesWithOptions(a, token_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    const watchdog = try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const Cancel = struct {
        cancelled: bool = false,
        session: @TypeOf(session),
        after_kernel_launch: ?usize = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
            if (comptime factory.CudaRuntimeStats != void) {
                if (self.after_kernel_launch) |limit| {
                    if (factory.getCudaRuntimeStats(self.session).?.kernel_launches >= limit) return error.Cancelled;
                }
            }
        }
    };
    var cancellation = Cancel{ .session = session };
    const control = Control{ .ptr = &cancellation, .check_fn = Cancel.check, .hard_cancellation = watchdog.boundary(), .deadline_ns = @import("antfly_platform").time.monotonicNs() + 180 * std.time.ns_per_s };
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 256 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const scratch = bounded.allocator();
    var managed = try factory.getManagedComputeBackend(session, scratch, null, control);
    defer managed.deinit();
    const options = Options{ .identity = identity, .control = control };
    for ([_]usize{ 0, 2, 8, 32 }) |window_count| {
        var texts = [_][]const u8{
            "Please refund the duplicate charge. I do not need technical help.",
            "Help me fix the computer please. The program keeps crashing after I open a file. I need technical assistance to repair it.",
        };
        var owned: [2]?[]u8 = .{ null, null };
        defer for (owned) |text| if (text) |bytes| scratch.free(bytes);
        if (window_count != 0) {
            // Seven source words plus the synthetic terminal per window,
            // with two source words of overlap: exactly 2/8/32 windows.
            const words = 7 + 5 * (window_count - 1);
            for ([_][]const u8{ "refund ", "support " }, 0..) |word, index| {
                const text = try scratch.alloc(u8, word.len * words);
                owned[index] = text;
                for (0..words) |i| @memcpy(text[i * word.len ..][0..word.len], word);
                texts[index] = text;
            }
        }
        const request_json = try std.json.Stringify.valueAlloc(scratch, .{
            .schema_version = 2,
            .model = "multi-decide",
            .schema = .{ .classifications = .{.{ .name = "intent", .labels = [_][]const u8{ "refund", "technical_support", "sales" } }} },
            .options = .{ .include_confidence = true },
            .inputs = .{ .{ .content = texts[0] }, .{ .content = texts[1] } },
        }, .{});
        defer scratch.free(request_json);
        var request = try wire.parseJson(scratch, request_json, .{});
        defer request.deinit();
        if (window_count != 0) for (request.items) |*item| {
            item.options.long_document = .{ .mode = .window, .window_words = 8, .overlap_words = 2, .max_windows = 32 };
        };
        const batched = try executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options);
        defer scratch.free(batched);
        var serial_options = options;
        serial_options.engine.limits.max_batch = 1;
        serial_options.long_document.window_batch_size = 1;
        const serial = try executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, serial_options);
        defer scratch.free(serial);
        const expected = try std.json.parseFromSlice(std.json.Value, scratch, serial, .{});
        defer expected.deinit();
        const actual = try std.json.parseFromSlice(std.json.Value, scratch, batched, .{});
        defer actual.deinit();
        const expected_rows = expected.value.object.get("data").?.array.items;
        const actual_rows = actual.value.object.get("data").?.array.items;
        try std.testing.expectEqual(@as(usize, 2), actual_rows.len);
        for (expected_rows, actual_rows) |e, got| {
            if (window_count != 0) {
                try std.testing.expectEqual(@as(i64, @intCast(window_count)), got.object.get("long_document").?.object.get("window_count").?.integer);
                try std.testing.expectEqual(@as(i64, @intCast(window_count)), e.object.get("long_document").?.object.get("window_count").?.integer);
            }
            const expected_labels = e.object.get("classifications").?.array.items;
            const actual_labels = got.object.get("classifications").?.array.items;
            try std.testing.expectEqual(expected_labels.len, actual_labels.len);
            for (expected_labels, actual_labels) |label, other| {
                try std.testing.expectEqualStrings(label.object.get("label").?.string, other.object.get("label").?.string);
                try std.testing.expectApproxEqAbs(label.object.get("score").?.float, other.object.get("score").?.float, 5e-4);
            }
        }
        cancellation.cancelled = true;
        try std.testing.expectError(error.Cancelled, executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options));
        cancellation.cancelled = false;
        if (comptime factory.CudaRuntimeStats != void) {
            const before = factory.getCudaRuntimeStats(session).?.kernel_launches;
            cancellation.after_kernel_launch = before + 8;
            try std.testing.expectError(error.Cancelled, executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options));
            try std.testing.expect(factory.getCudaRuntimeStats(session).?.kernel_launches >= before + 8);
            cancellation.after_kernel_launch = null;
            const recovered = try executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options);
            defer scratch.free(recovered);
            try std.testing.expectEqualStrings(batched, recovered);
        }
        std.debug.print("family_cuda_windows: windows={d} batched_serial_equal=true cancellation_recovered=true\n", .{window_count});
    }
}

fn exerciseNonmonotonicFitting(a: Allocator) !void {
    const weights = [_]usize{ 1, 1, 1, 15, 30, 15, 1, 15, 15, 1, 30, 1, 30, 15, 1, 15, 1, 1, 1, 30, 15, 1, 1, 15 };
    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(a);
    for (weights) |weight| {
        for (0..weight) |_| try text.append(a, 'a');
        try text.append(a, ' ');
    }
    const raw = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "probe", .schema = .{ .entities = .{"x"} }, .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 12), .overlap_words = @as(usize, 0), .max_windows = @as(usize, 3) } }, .inputs = .{.{ .content = text.items }} }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    var tokenizer = QualificationGeometryTest.TokenizerProbe{};
    var empty = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "", .schema = &request.items[0].compiled }}, .{});
    const prefix_and_terminal = empty.sequence_length;
    empty.deinit();
    var config = QualificationGeometryTest.config();
    var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    identity.backbone = config.backbone;
    const options = long_executor.Options{ .identity = identity, .processor = .{ .max_sequence_tokens = prefix_and_terminal + 103 }, .limits = .{ .window_batch_size = 1 } };
    var receiver = PreparationReuseTest.Receiver{};
    const fitted = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
    request.items[0].options.long_document.window_words = 10;
    const valid = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(@as(usize, 3), fitted.window_count);
    try std.testing.expectEqual(valid.prompt_tokens, fitted.prompt_tokens);
    // Checking thousands of candidate widths must not multiply tokenizer work.
    config.max_len = 4096;
    request.items[0].options.long_document.window_words = 4096;
    const before = tokenizer.calls;
    const wide = try long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(valid.prompt_tokens, wide.prompt_tokens);
    try std.testing.expect(tokenizer.calls - before < 256);
}

test "boundary qualification fitting preserves valid nonmonotonic partitions" {
    try exerciseNonmonotonicFitting(std.testing.allocator);
}

test "boundary qualification token fitting releases every failed allocation" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exerciseNonmonotonicFitting, .{});
}

test "boundary qualification indexed fitting matches exhaustive tokenized partitions" {
    const a = std.testing.allocator;
    const documents = @import("../pipelines/gliner_boundary_long_document.zig");
    const texts = [_][]const u8{ "İΣ a bc def 😀 https://x.test @owner a@b.test a bb ccc dddd ", "a bb ccc dddd eeeee ffffff ggggggg h i j k l!", "https://x.test a b c d e f https://y.test" };
    for (texts) |text| for ([_]processor.WordSplitter{ .whitespace, .char }) |splitter| {
        const raw = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "probe", .schema = .{ .entities = .{"x"} }, .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 12), .overlap_words = @as(usize, 1) } }, .inputs = .{.{ .content = text }} }, .{});
        defer a.free(raw);
        var request = try wire.parseJson(a, raw, .{});
        defer request.deinit();
        const item = &request.items[0];
        item.options.word_splitter = splitter;
        var tokenizer = QualificationGeometryTest.TokenizerProbe{};
        var empty = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "", .schema = &item.compiled }}, .{ .word_splitter = splitter });
        const overhead = empty.sequence_length;
        empty.deinit();
        const config = QualificationGeometryTest.config();
        var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
        identity.backbone = config.backbone;
        for ([_]usize{ 4, 8, 16, 24 }) |body_budget| {
            const cap = overhead + body_budget;
            var expected: ?struct { tokens: usize, windows: usize } = null;
            var width: usize = 12;
            while (width >= 2) : (width -= 1) {
                var document = documents.plan(a, text, item.compiled.fingerprint, .{ .mode = .windowed, .word_splitter = splitter, .max_window_body_words = width, .overlap_words = 1 }) catch |err| switch (err) {
                    error.InvalidLongDocumentLimits => continue,
                    else => return err,
                };
                defer document.deinit();
                var total: usize = 0;
                var fits = true;
                for (document.windows) |window| {
                    var batch = processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = try document.windowText(window.index), .schema = &item.compiled }}, .{ .word_splitter = splitter, .max_text_words = width, .max_sequence_tokens = cap }) catch |err| switch (err) {
                        error.BoundarySequenceLimitExceeded => {
                            fits = false;
                            break;
                        },
                        else => return err,
                    };
                    total += batch.sequence_length;
                    batch.deinit();
                }
                if (fits) {
                    expected = .{ .tokens = total, .windows = document.windows.len };
                    break;
                }
            }
            var receiver = PreparationReuseTest.Receiver{};
            const result = long_executor.planGeometry(.native, a, &config, tokenizer.tokenizer(), item, .{ .identity = identity, .processor = .{ .max_sequence_tokens = cap }, .limits = .{ .window_batch_size = 1 } }, &receiver);
            if (expected) |value| {
                const actual = try result;
                try std.testing.expectEqual(value.tokens, actual.prompt_tokens);
                try std.testing.expectEqual(value.windows, actual.window_count);
            } else if (result) |_| return error.TestUnexpectedResult else |err| {
                if (err == error.OutOfMemory) return err;
            }
        }
    };
}

test "boundary end-to-end CUDA benchmark measures complete warm requests" {
    if (!@import("build_options").enable_cuda) return error.SkipZigTest;
    const env = @import("antfly_platform").env;
    if (!env.getenvBool("ANTFLY_GLINER_E2E_BENCH")) return error.SkipZigTest;
    const directory = env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const factory = @import("../architectures/session_factory.zig");
    const session = try factory.createCudaSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerBoundaryConfig(session);
    const identity = try factory.getGlinerBoundaryIdentity(session);
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    defer a.free(bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const sentence = "The library indexes records and answers search queries. ";
    const text = try a.alloc(u8, sentence.len * 64);
    defer a.free(text);
    for (0..64) |i| @memcpy(text[i * sentence.len ..][0..sentence.len], sentence);
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = "base",
        .schema = .{ .entities = .{"concept"} },
        .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 128), .overlap_words = @as(usize, 16) } },
        .inputs = .{.{ .content = text }},
    }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    const watchdog = try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const control = Control{ .hard_cancellation = watchdog.boundary(), .deadline_ns = @import("antfly_platform").time.monotonicNs() + 600 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var expected: ?[]u8 = null;
    defer if (expected) |response| a.free(response);
    var totals = [_]u64{ 0, 0 };
    // Warm both modes, then collect eight ABBA rounds (16 requests per mode). Identical learned dispatch policy and
    // grouping isolate preparation reuse. Include final preparation-store free;
    // exclude model/tokenizer loading, JSON parsing, transport and response free.
    for (0..34) |sample| {
        const reuse = if (sample < 2) sample == 1 else ((sample - 2) % 4 == 1 or (sample - 2) % 4 == 2);
        var store = PreparationStore.init(a);
        const options = Options{ .identity = identity, .control = control, .preparations = if (reuse) &store else null };
        const start = @import("antfly_platform").time.monotonicNs();
        const response = response: {
            errdefer store.deinit();
            _ = try workspaceGeometryFor(.cuda, a, &config, tokenizer.tokenizer(), &request, options);
            var gate = try qualification.Admission.initResolved(identity, .cuda, &request, options.pipeline, control);
            try qualifyRequestGeometry(&managed.backend, a, &config, tokenizer.tokenizer(), &request, options, &gate);
            break :response try executeChecked(&managed.backend, a, &config, tokenizer.tokenizer(), &request, options, &gate);
        };
        defer a.free(response);
        store.deinit();
        const elapsed = @import("antfly_platform").time.monotonicNs() - start;
        if (expected) |reference| try std.testing.expectEqualStrings(reference, response) else expected = try a.dupe(u8, response);
        if (sample < 2) continue;
        totals[@intFromBool(reuse)] += elapsed;
        std.debug.print("e2e_bench backend=cuda policy=reference_v1 reuse={} ns={} response_bytes={}\n", .{ reuse, elapsed, response.len });
    }
    std.debug.print("e2e_bench total_without_ns={} total_with_ns={} equal_responses=true\n", .{ totals[0], totals[1] });
}

fn exerciseFragmentFitting(a: Allocator) !void {
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"probe","schema":{"entities":["x"]},"options":{"long_document":{"mode":"window","window_words":6,"overlap_words":0}},"inputs":[{"content":"a b c d https://xxxx e f g"}]}
    , .{});
    defer request.deinit();
    var tok = QualificationGeometryTest.TokenizerProbe{ .one_token_words = true };
    const config = QualificationGeometryTest.config();
    var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    identity.backbone = config.backbone;
    var store = PreparationStore.init(a);
    defer store.deinit();
    const options = long_executor.Options{ .preparations = &store, .identity = identity, .processor = .{ .max_fragment_bytes = 12 } };
    var receiver = PreparationReuseTest.Receiver{};
    const first = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);

    request.items[0].options.long_document.window_words = 4;
    const fitting = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(fitting.window_count, first.window_count);
    try std.testing.expectEqual(fitting.prompt_tokens, first.prompt_tokens);
    request.items[0].options.long_document.window_words = 6;
    const before = tok.calls;
    const cached = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(first.prompt_tokens, cached.prompt_tokens);
    try std.testing.expectEqual(before, tok.calls);
    // A source word that already exceeds the cap cannot be fixed by fitting.
    request.items[0].text = "a b c d https://xxxxx e f g";
    const invalid = long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    if (invalid) |_| return error.TestUnexpectedResult else |err| {
        if (err == error.OutOfMemory) return err;
        try std.testing.expectEqual(@as(anyerror, error.BoundaryFragmentLimitExceeded), err);
    }
}

test "boundary qualification fragment fitting respects the default cap with pinned base tokenizer" {
    const a = std.testing.allocator;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    defer a.free(bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, bytes);
    defer tok.tokenizer().deinitTokenizer();
    var url = std.ArrayListUnmanaged(u8).empty;
    defer url.deinit(a);
    try url.appendSlice(a, "https://");
    while (url.items.len + "configuration".len <= 65536) try url.appendSlice(a, "configuration");
    while (url.items.len < 65536) try url.append(a, 'x');
    const text = try std.fmt.allocPrint(a, "a b c d {s} e f g", .{url.items});
    defer a.free(text);
    const raw = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "probe", .schema = .{ .entities = .{"x"} }, .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 6), .overlap_words = @as(usize, 0) } }, .inputs = .{.{ .content = text }} }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    var config = QualificationGeometryTest.config();
    config.encoder.vocab_size = @intCast(tok.tokenizer().vocabSize());
    var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    identity.backbone = config.backbone;
    const options = long_executor.Options{ .identity = identity };
    var receiver = PreparationReuseTest.Receiver{};
    const fitted = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);

    request.items[0].options.long_document.window_words = 4;
    const accepted = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(accepted.window_count, fitted.window_count);
    try std.testing.expectEqual(accepted.prompt_tokens, fitted.prompt_tokens);
    try std.testing.expectEqual(@as(usize, 5083), fitted.prompt_tokens);
}

test "boundary qualification fragment fitting preserves valid partitions" {
    try exerciseFragmentFitting(std.testing.allocator);
}

test "boundary qualification fragment fitting releases every failed allocation" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exerciseFragmentFitting, .{});
}

test "boundary qualification fragment fitting rejects invalid schema before source indexing" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"probe","schema":{"entities":["oversized_entity_label"]},"options":{"long_document":{"mode":"window","window_words":6,"overlap_words":0}},"inputs":[{"content":"a b c d https://xxxx e f g"}]}
    , .{});
    defer request.deinit();
    var tok = QualificationGeometryTest.TokenizerProbe{ .one_token_words = true };
    const config = QualificationGeometryTest.config();
    var identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    identity.backbone = config.backbone;
    var receiver = PreparationReuseTest.Receiver{};
    try std.testing.expectError(error.BoundaryFragmentLimitExceeded, long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], .{ .identity = identity, .processor = .{ .max_fragment_bytes = 12 } }, &receiver));
    // Only two bounded schema attempts: initial window, then empty prefix.
    try std.testing.expect(tok.calls < 32);
}

test "boundary qualification native full-context groups fit head budgets before encoding" {
    const a = std.testing.allocator;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const config_path = try std.fs.path.join(a, &.{ directory, "config.json" });
    defer a.free(config_path);
    const encoder_path = try std.fs.path.join(a, &.{ directory, "encoder_config/config.json" });
    defer a.free(encoder_path);
    const bytes = try @import("../util/c_file.zig").readFile(a, config_path);
    defer a.free(bytes);
    const encoder_bytes = try @import("../util/c_file.zig").readFile(a, encoder_path);
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, bytes, encoder_bytes);
    const text = try a.alloc(u8, 4095 * 4 * 4);
    defer a.free(text);
    for (0..4095 * 4) |i| @memcpy(text[i * 4 ..][0..4], "the ");
    const raw = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "base", .schema = .{ .entities = .{"concept"}, .entity_definitions = .{ .concept = .{ .description = "A concept" } } }, .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 4096), .overlap_words = @as(usize, 0) } }, .inputs = .{.{ .content = text }} }, .{});
    defer a.free(raw);
    var request = try wire.parseJson(a, raw, .{});
    defer request.deinit();
    const Receiver = struct {
        gate: qualification.Admission,
        batch: usize = 0,
        words: usize = 0,
        pub fn observe(self: *@This(), size: usize, words: usize, windows: usize, prepared: *const processor.PreparedBatch) !void {
            try self.gate.observe(size, words, windows, prepared);
        }
        pub fn observeWindowGroup(self: *@This(), prepared: *const processor.PreparedBatch) void {
            self.batch = @max(self.batch, prepared.samples.len);
            self.words = @max(self.words, prepared.word_width);
        }
    };
    const identity = @import("../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1;
    var receiver = Receiver{ .gate = try qualification.Admission.init(identity, .native, &request, null) };
    var tok = QualificationGeometryTest.TokenizerProbe{ .one_token_words = true };
    var preparations = PreparationStore.init(a);
    defer preparations.deinit();
    var options = long_executor.Options{ .identity = identity, .preparations = &preparations };
    const usage = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(@as(usize, 2), receiver.batch);
    try std.testing.expectEqual(@as(usize, 4096), receiver.words);
    const head = @import("../architectures/gliner/boundary_head.zig");
    const planned = try head.planShape(&config, .{ .batch = receiver.batch, .text_length = receiver.words, .queries = 1 }, .{});
    try std.testing.expect(planned.intermediate_bytes <= (head.Limits{}).max_intermediate_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, head.planShape(&config, .{ .batch = 4, .text_length = 4096, .queries = 1 }, .{}));
    try std.testing.expectEqual(@as(usize, 4), usage.window_count);
    // An impossible head cap must fail before the empty backend can read any
    // model weights. Cache reuse must re-check the current head limits.
    const native = @import("../ops/native_compute.zig");
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    var context = native.NativeCompute.init(a, &weights, null);
    defer context.deinit();
    const cb = compute.ComputeBackend{ .ptr = &context, .vtable = &native.vtable_impl };
    options.pipeline.head_limits.max_intermediate_bytes = 1;
    try std.testing.expectError(error.ResourceLimitExceeded, long_executor.executeNative(&cb, a, &config, tok.tokenizer(), &request.items[0], options));
    options.pipeline.head_limits = .{};
    const recovered = try long_executor.planGeometry(.native, a, &config, tok.tokenizer(), &request.items[0], options, &receiver);
    try std.testing.expectEqual(usage.prompt_tokens, recovered.prompt_tokens);
    options.pipeline.head_limits.max_intermediate_bytes = 1;
    request.items[0].options.long_document.mode = .reject;
    request.items[0].text = text[0 .. 4095 * 4];
    try std.testing.expectError(error.ResourceLimitExceeded, prepareGroup(.native, a, &config, tok.tokenizer(), request.items, .{ .identity = identity, .pipeline = options.pipeline }));
    std.debug.print("native-head admission: windows={} words={} admitted_batch={} planned_bytes={}\n", .{ usage.window_count, receiver.words, receiver.batch, planned.intermediate_bytes });
}
