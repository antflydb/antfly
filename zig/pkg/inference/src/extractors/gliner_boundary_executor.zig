// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Schema-aware execution inside an admitted, managed model session. A serial
//! physical batch bounds peak scratch independently of HTTP batch cardinality;
//! the native engine/pipeline retain their batched interfaces for the scheduler.
//! Output is buffered atomically and independently bounded across all inputs.
const std = @import("std");
const wire = @import("extraction_v2.zig");
const model = @import("../models/gliner_boundary.zig");
const engine = @import("../architectures/gliner_boundary_engine.zig");
const device_request = @import("../architectures/gliner_boundary_request_device.zig");
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

pub const Options = struct {
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
    if (cb.kind() != .metal) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    return execute(cb, allocator, config, tokenizer, request, options);
}

/// The caller owns the managed backend, model lock and host/device admission
/// for the complete execution, including serialization and cleanup.
pub fn execute(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    return executeChecked(cb, allocator, config, tokenizer, request, options, null);
}

/// Serving entrypoint: one exact live-artifact policy row must cover the full
/// request before the first learned operation. Diagnostic execute stays separate.
pub fn executeQualified(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) ![]u8 {
    if (options.control) |control| try control.check();
    try cb.checkExecutionControl();
    if (cb.kind() != .native and cb.kind() != .metal) return error.UnsupportedGlinerBoundaryBackend;
    const identity = options.identity orelse return error.UnsupportedGlinerBoundaryRuntime;
    if (identity.backbone != config.backbone) return error.GlinerBoundaryArtifactMismatch;
    var gate = try qualification.Gate.initResolved(identity, if (cb.kind() == .metal) .metal else .native, request, options.pipeline, options.control);
    try qualifyRequestGeometry(cb, allocator, config, tokenizer, request, options, &gate);
    if (options.control) |control| try control.check();
    return executeChecked(cb, allocator, config, tokenizer, request, options, &gate);
}

/// The receiver is a compile-time parameter for the quiet scalar observation
/// path only. Learned entrypoints always require the concrete closed Gate.
fn qualifyRequestGeometry(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: anytype) !void {
    return planRequestGeometry(cb.kind(), allocator, config, tokenizer, request, options, gate);
}

/// Maximum physical B*S over serial inputs and document windows. This uses
/// the same bounded processor/planning path as qualification, before the
/// accelerator mutex or any eviction-capable workspace admission is held.
pub fn workspaceGeometry(allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options) !usize {
    if (options.identity == null) return error.MissingGlinerBoundaryIdentity;
    const Receiver = struct {
        peak_tokens: usize = 0,
        pub fn observe(self: *@This(), _: usize, _: usize, _: usize, prepared: *const processor.PreparedBatch) !void {
            self.peak_tokens = @max(self.peak_tokens, prepared.input_ids.len);
        }
        pub fn observeSingle(self: *@This(), _: Allocator, _: []const u8, prepared: *const processor.PreparedBatch, _: processor.Options) !void {
            self.peak_tokens = @max(self.peak_tokens, prepared.input_ids.len);
        }
    };
    var receiver = Receiver{};
    try planRequestGeometry(.metal, allocator, config, tokenizer, request, options, &receiver);
    if (receiver.peak_tokens == 0) return error.InvalidBoundaryPipelineInput;
    return receiver.peak_tokens;
}

fn planRequestGeometry(backend: compute.BackendKind, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: anytype) !void {
    if (backend != .native and backend != .metal) return error.UnsupportedGlinerBoundaryBackend;
    var quiet = options;
    quiet.observer = null;
    try preflight(request, quiet);
    var remaining_tokens = options.long_document.max_total_encoded_tokens;
    var remaining_attention = options.long_document.max_total_attention_work;
    var remaining_windows = options.long_document.max_request_windows;
    for (request.items, 0..) |*item, index| {
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
            remaining_tokens -= used.prompt_tokens;
            remaining_attention -= used.attention_work_items;
            remaining_windows -= used.window_count;
            continue;
        }
        const process_options = qualification.singleProcessor(config, item, options.processor, options.engine.limits.max_queries, options.control);
        var prepared = try processor.prepare(allocator, tokenizer, &.{.{ .text = item.text, .schema = &item.compiled }}, process_options);
        defer prepared.deinit();
        try gate.observeSingle(allocator, item.text, &prepared, process_options);
    }
    if (options.control) |control| try control.check();
}

fn executeChecked(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const model.Config, tokenizer: Tokenizer, request: *const wire.Request, options: Options, gate: ?*qualification.Gate) ![]u8 {
    if (cb.kind() != .native and cb.kind() != .metal) return error.UnsupportedGlinerBoundaryBackend;
    if (cb.kind() == .metal and options.identity == null) return error.MissingGlinerBoundaryIdentity;
    if (options.identity) |identity| if (identity.backbone != config.backbone) return error.GlinerBoundaryArtifactMismatch;
    try preflight(request, options);
    var writer = wire.ResponseWriter.init(allocator, options.max_response_bytes, request.items.len);
    defer writer.deinit();
    try writer.begin(request.model);
    var prompt_tokens: usize = 0;
    var remaining_values = options.pipeline.max_output_values;
    var remaining_window_tokens = options.long_document.max_total_encoded_tokens;
    var remaining_window_attention = options.long_document.max_total_attention_work;
    var remaining_windows = options.long_document.max_request_windows;
    for (request.items, 0..) |item, index| {
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
                .metal => try long_executor.executeDevice(cb, allocator, config, tokenizer, &item, long_options),
                else => return error.UnsupportedGlinerBoundaryBackend,
            };
            defer result.deinit();
            const used = try outputValues(result.sample);
            if (used > remaining_values) return error.ExtractionOutputLimitExceeded;
            remaining_values -= used;
            remaining_window_tokens -= result.prompt_tokens;
            remaining_window_attention -= result.attention_work_items;
            remaining_windows -= result.window_count;
            prompt_tokens = try std.math.add(usize, prompt_tokens, result.prompt_tokens);
            try progress(options, index, "serializing");
            try writer.append(item, result.sample);
            observeSample(options.observer, result.sample, result.prompt_tokens, used);
            continue;
        }
        try progress(options, index, "tokenizing");
        const processor_options = qualification.singleProcessor(config, &item, options.processor, options.engine.limits.max_queries, options.control);
        var prepared = try processor.prepare(allocator, tokenizer, &.{.{ .text = item.text, .schema = &item.compiled }}, processor_options);
        defer prepared.deinit();
        if (gate) |active| try active.observeSingle(allocator, item.text, &prepared, processor_options);
        prompt_tokens = try std.math.add(usize, prompt_tokens, prepared.samples[0].input_ids.len);
        try progress(options, index, "encoder");
        var engine_options = options.engine;
        engine_options.control = options.control;
        var pipeline_options = item.options.native(options.pipeline);
        pipeline_options.control = options.control;
        pipeline_options.max_output_values = remaining_values;
        var result = switch (cb.kind()) {
            .native => native: {
                var encoded = try engine.encodeNative(cb, allocator, config, &prepared, engine_options);
                defer encoded.deinit();
                try progress(options, index, "decoding");
                break :native try pipeline.runNative(cb, allocator, config, &prepared, &.{&item.compiled}, .{
                    .text_states = encoded.text_states,
                    .query_states = encoded.query_states,
                    .classification_states = encoded.classification_states,
                    .text_lengths = encoded.text_lengths,
                }, pipeline_options);
            },
            .metal => device: {
                var device_limits = options.device;
                device_limits.encoder = engine_options.limits;
                break :device (try device_request.run(cb, allocator, config, &prepared, &.{&item.compiled}, .{
                    .precision = options.identity.?.precision,
                    .execution_policy = options.metal_execution_policy,
                    .limits = device_limits,
                    .pipeline = pipeline_options,
                })).outputs;
            },
            else => return error.UnsupportedGlinerBoundaryBackend,
        };
        defer result.deinit();
        if (result.samples.len != 1) return error.InvalidExtractionOutput;
        const used = try outputValues(result.samples[0]);
        if (used > remaining_values) return error.ExtractionOutputLimitExceeded;
        remaining_values -= used;
        try progress(options, index, "serializing");
        try writer.append(item, result.samples[0]);
        observeSample(options.observer, result.samples[0], prepared.samples[0].input_ids.len, used);
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
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
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
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "pipeline_cases.json");
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
        return .{ .version = 3, .architecture_version = 1, .max_len = 32, .backbone = .small, .head = .{}, .encoder = .{
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
            inline for (@typeInfo(policy.LengthContract).@"struct".fields) |field| try std.testing.expectEqual(@field(value, field.name).min, @field(value, field.name).max);
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
        try std.testing.expectEqual(expected_workspace_tokens, try workspaceGeometry(a, &metal_cfg, tokenizer.tokenizer(), &request, options));
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

test "boundary qualification later item and window rejection cancels quietly and retries cleanly" {
    for ([_]QualificationGeometryTest.Mode{ .late_item, .late_window, .cancelled, .sequence_limit }) |mode| try QualificationGeometryTest.exercise(std.testing.allocator, mode);
}

test "boundary qualification quiet geometry releases every failed allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, QualificationGeometryTest.exercise, .{.success});
}
