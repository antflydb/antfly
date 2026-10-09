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

//! Exact-artifact Metal parity for the published multilingual GLiNER2.5
//! boundary family. This stays separate from the generic device scorer tests:
//! every run binds the full checkpoint and all four sidecars to the immutable
//! upstream capture before executing the resident encoder, head and scorer.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const model = @import("../../models/gliner_boundary.zig");
const safetensors = @import("../../models/safetensors.zig");
const c_file = @import("../../util/c_file.zig");
const schema_mod = @import("../../pipelines/extraction_schema.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../../pipelines/gliner_boundary_pipeline.zig");
const decide = @import("antfly_decisions").legacy;
const fixtures = @import("boundary_parity_test.zig");
const engine = @import("boundary_engine_device.zig");
const head = @import("boundary_device.zig");
const adapter = @import("boundary_scorer_device.zig");
const head_tests = @import("boundary_device_test.zig");
const metal = @import("../../ops/metal_compute.zig");
const factory = @import("../session_factory.zig");
const bundle = @import("../../models/gliner_boundary_bundle.zig");
const admission_memory = @import("../../runtime/tier/memory.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const HardCancellationWatchdog = @import("../../hard_cancellation_watchdog.zig").HardCancellationWatchdog;

const disable_fused_ffn_env = "TERMITE_METAL_DISABLE_GLINER_BOUNDARY_FUSED_FFN";
extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;

const DecideCapture = struct {
    format_version: u32,
    status: []const u8,
    scope: []const u8,
    requests: []const struct {
        id: []const u8,
        text: []const u8,
        decide_request_json: []const u8,
        native_schema_json: []const u8,
        encoded: struct { input_ids: []const i64, attention_mask: []const i64 },
        native_classification: struct {
            input_ids: []const i64,
            tasks: []const struct { name: []const u8, labels: []const []const u8, raw_logits: []const f32 },
        },
        probabilities: std.json.Value,
        decide_expected: std.json.Value,
    },
};

fn expectJsonApprox(expected: std.json.Value, actual: std.json.Value) !void {
    if (expected == .integer and actual == .float)
        return std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(expected.integer)), actual.float, pipeline.fp32_confidence_tolerance);
    if (expected == .float and actual == .integer)
        return std.testing.expectApproxEqAbs(expected.float, @as(f64, @floatFromInt(actual.integer)), pipeline.fp32_confidence_tolerance);
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return error.TestExpectedEqual;
    switch (expected) {
        .null => {},
        .bool => |value| try std.testing.expectEqual(value, actual.bool),
        .integer => |value| try std.testing.expectEqual(value, actual.integer),
        .float => |value| try std.testing.expectApproxEqAbs(value, actual.float, pipeline.fp32_confidence_tolerance),
        .number_string => |value| try std.testing.expectEqualStrings(value, actual.number_string),
        .string => |value| try std.testing.expectEqualStrings(value, actual.string),
        .array => |values| {
            try std.testing.expectEqual(values.items.len, actual.array.items.len);
            for (values.items, actual.array.items) |want, got| try expectJsonApprox(want, got);
        },
        .object => |values| {
            try std.testing.expectEqual(values.count(), actual.object.count());
            var it = values.iterator();
            while (it.next()) |entry| {
                const got = actual.object.get(entry.key_ptr.*) orelse return error.TestExpectedEqual;
                try expectJsonApprox(entry.value_ptr.*, got);
            }
        },
    }
}

fn extractionResponseJson(owner_allocator: std.mem.Allocator, sample: pipeline.Sample, prompt_tokens: usize) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(owner_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var rows = std.array_list.Managed(std.json.Value).init(a);
    for (sample.classifications) |classification| for (classification.labels) |label| {
        var row: std.json.ObjectMap = .empty;
        try row.put(a, "name", .{ .string = classification.name });
        try row.put(a, "label", .{ .string = label.label });
        try row.put(a, "score", .{ .float = label.confidence });
        try rows.append(.{ .object = row });
    };
    var item: std.json.ObjectMap = .empty;
    try item.put(a, "classifications", .{ .array = rows });
    var data = std.array_list.Managed(std.json.Value).init(a);
    try data.append(.{ .object = item });
    var usage: std.json.ObjectMap = .empty;
    try usage.put(a, "prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
    try usage.put(a, "completion_tokens", .{ .integer = 0 });
    var root: std.json.ObjectMap = .empty;
    try root.put(a, "data", .{ .array = data });
    try root.put(a, "usage", .{ .object = usage });
    return std.json.Stringify.valueAlloc(owner_allocator, std.json.Value{ .object = root }, .{});
}

fn expectPin(pin: pipeline.PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(pin.size_bytes, bytes.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    try std.testing.expectEqualStrings(pin.sha256, &std.fmt.bytesToHex(digest, .lower));
}

fn readPinned(a: std.mem.Allocator, directory: []const u8, name: []const u8, pin: pipeline.PublishedModelPin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const bytes = try c_file.readFile(a, path);
    errdefer a.free(bytes);
    try expectPin(pin, bytes);
    return bytes;
}

fn expectReference(checkpoint: pipeline.FamilyCheckpoint, reference: pipeline.FamilyReference) !void {
    try std.testing.expectEqual(@as(u32, 1), reference.format_version);
    try std.testing.expectEqualStrings("captured", reference.status);
    try std.testing.expectEqualStrings(checkpoint.scope, reference.scope);
    try std.testing.expect(!reference.qualification and !reference.native_runtime_qualified and !reference.production_qualified);
    try std.testing.expectEqualStrings(checkpoint.profile, reference.model.profile);
    try std.testing.expectEqualStrings(checkpoint.repo, reference.model.repo);
    try std.testing.expectEqualStrings(checkpoint.revision, reference.model.revision);
    try std.testing.expectEqualStrings("boundary", reference.model.architecture);
    try std.testing.expectEqualStrings("deberta-v2", reference.model.encoder_family);
    try std.testing.expectEqual(checkpoint.model.size_bytes, reference.model.model_size_bytes);
    try std.testing.expectEqualStrings(checkpoint.model.sha256, reference.model.model_sha256);
    inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" }) |name| {
        const want = @field(checkpoint.sidecars, name);
        const got = @field(reference.model.sidecars, name);
        try std.testing.expectEqual(want.size_bytes, got.size_bytes);
        try std.testing.expectEqualStrings(want.sha256, got.sha256);
    }
    try std.testing.expectEqualStrings(checkpoint.generator_sha256, reference.artifacts.generator_sha256);
    try std.testing.expectEqualStrings(checkpoint.contract_sha256, reference.artifacts.contract_sha256);
    try std.testing.expectEqualStrings(checkpoint.requests_sha256, reference.artifacts.requests_sha256);
    try std.testing.expectEqual(checkpoint.request_count, reference.requests.len);
}

fn parity(comptime checkpoint: pipeline.FamilyCheckpoint) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const model_environment = checkpoint.environment_prefix ++ "_MODEL_DIR";
    const capture_environment = (checkpoint.capture_environment_prefix orelse checkpoint.environment_prefix) ++ "_CAPTURE";
    const directory = platform.env.getenv(model_environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;

    const capture_bytes = if (platform.env.getenv(capture_environment)) |path|
        try c_file.readFileMax(a, path, 2 * 1024 * 1024)
    else
        try fixtures.fixtureBytes(a, checkpoint.capture_fixture);
    defer a.free(capture_bytes);
    try expectPin(checkpoint.capture, capture_bytes);
    const capture = try std.json.parseFromSlice(pipeline.FamilyReference, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try expectReference(checkpoint, capture.value);

    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    try expectPin(checkpoint.model, weights.reader.file_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", checkpoint.sidecars.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinned(a, directory, "config.json", checkpoint.sidecars.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", checkpoint.sidecars.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const tokenizer_config = try readPinned(a, directory, "tokenizer_config.json", checkpoint.sidecars.@"tokenizer_config.json");
    defer a.free(tokenizer_config);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);

    var store = try head_tests.loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();

    for (capture.value.requests) |request| {
        errdefer std.debug.print("{s} resident family device case: {s}\n", .{ checkpoint.profile, request.id });
        var schema = try schema_mod.compile(a, request.native_schema_json, .{});
        defer schema.deinit();
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = request.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        try std.testing.expectEqualSlices(i64, request.encoded.input_ids, prepared.input_ids);
        try std.testing.expectEqualSlices(i64, request.encoded.attention_mask, prepared.attention_mask);

        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{}) else null;
        defer if (headed) |*value| value.deinit();
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        var scorer = try adapter.Context.init(a, &config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();

        if (request.native_classification) |reference| {
            var scores = try scorer.scorer().classify(a, .{}, null);
            defer scores.deinit();
            var offset: usize = 0;
            for (reference.tasks, 0..) |task, task_index| {
                const compiled = schema.schema.classifications[task_index].task;
                try std.testing.expectEqualStrings(task.name, compiled.name);
                try std.testing.expectEqual(task.labels.len, task.raw_logits.len);
                for (task.labels, task.raw_logits, 0..) |label, raw_logit, label_index| {
                    try std.testing.expectEqualStrings(label, compiled.labels[label_index]);
                    try std.testing.expectApproxEqAbs(raw_logit, scores.logits[offset + label_index], pipeline.fp32_confidence_tolerance);
                }
                offset += task.labels.len;
            }
            try std.testing.expectEqual(offset, scores.logits.len);
        }

        var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try pipeline.expectFamilySample(checkpoint, request.id, request.text, request.native_expected, result.samples[0], pipeline.fp32_confidence_tolerance, .unicode_codepoints);
        // Reuse resident encoded states and the scorer to exercise exact UTF-8
        // presentation without a second encoder pass.
        if (checkpoint.exercise_utf8_offsets) {
            var utf8_result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .utf8_bytes });
            defer utf8_result.deinit();
            try std.testing.expectEqual(@as(usize, 1), utf8_result.samples.len);
            try pipeline.expectFamilySample(checkpoint, request.id, request.text, request.native_expected, utf8_result.samples[0], pipeline.fp32_confidence_tolerance, .utf8_bytes);
        }
        const stats = try scorer.stats();
        try std.testing.expect(stats.result_download_bytes <= scorer.limits.max_result_download_bytes);
    }
}

const OptimizedObservation = struct {
    logits: []f32,
    decoded_json: []u8,
    fused_ffn_calls: u64,

    fn deinit(self: *OptimizedObservation, a: std.mem.Allocator) void {
        a.free(self.logits);
        a.free(self.decoded_json);
        self.* = undefined;
    }
};

fn optimizedClassificationArm(
    a: std.mem.Allocator,
    session: @import("../../backends/session.zig").Session,
    control: Control,
    controller: *admission_memory.AdmissionController,
    config: *const model.Config,
    tokenizer: @import("inference_tokenizer").Tokenizer,
    checkpoint: pipeline.FamilyCheckpoint,
    request: anytype,
    disable_fused_ffn: bool,
) !OptimizedObservation {
    var hard_cancellation = try control.enterUninterruptible(session.interruption());
    defer hard_cancellation.deinit();

    if (disable_fused_ffn) {
        if (setenv(disable_fused_ffn_env, "1", 1) != 0) return error.EnvironmentUpdateFailed;
    } else if (unsetenv(disable_fused_ffn_env) != 0) return error.EnvironmentUpdateFailed;

    var compiled = try schema_mod.compile(a, request.native_schema_json, .{});
    defer compiled.deinit();
    var prepared = try processor.prepare(a, tokenizer, &.{.{ .text = request.text, .schema = &compiled }}, .{});
    defer prepared.deinit();
    try std.testing.expectEqualSlices(i64, request.encoded.input_ids, prepared.input_ids);
    try std.testing.expectEqualSlices(i64, request.encoded.attention_mask, prepared.attention_mask);

    const workspace_plan = try factory.planGlinerBoundaryWorkspace(session, 1, prepared.sequence_length);
    var permit: ?admission_memory.AdmissionLease = null;
    defer if (permit) |*lease| lease.release();
    if (workspace_plan.replacement_amounts) |amounts| permit = try controller.tryAcquire(.gpu, .{
        .backend_limit_bytes = 3 * 1024 * 1024 * 1024,
        .scratch_limit_bytes = 3 * 1024 * 1024 * 1024,
    }, amounts, false);
    var managed = try factory.getManagedGlinerBoundaryComputeBackend(session, a, null, control, workspace_plan, if (permit) |*lease| lease else null);
    defer managed.deinit();
    try std.testing.expect(managed.backend.glinerBoundaryFusedFfnAvailable());
    const before = try managed.backend.glinerBoundaryScope(&.snapshot);

    var logits: ?[]f32 = null;
    errdefer if (logits) |values| a.free(values);
    var decoded_json: ?[]u8 = null;
    errdefer if (decoded_json) |bytes| a.free(bytes);
    {
        var encoded = try engine.encodeDevice(&managed.backend, a, config, &prepared, .{
            .execution_policy = .optimized_v2,
            .max_device_bytes = try workspace_plan.requestEncoderLimit(2 * 1024 * 1024 * 1024),
            .control = control,
        });
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&managed.backend, a, config, try encoded.asHeadInput(control), .{}) else null;
        defer if (headed) |*value| value.deinit();
        var scorer = try adapter.Context.init(a, config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();
        var raw = try scorer.scorer().classify(a, .{}, control);
        defer raw.deinit();
        logits = try a.dupe(f32, raw.logits);

        const reference = request.native_classification orelse return error.InvalidFamilyReference;
        try std.testing.expectEqualSlices(i64, reference.input_ids, prepared.samples[0].input_ids);
        var offset: usize = 0;
        for (reference.tasks, 0..) |task, task_index| {
            const task_schema = compiled.schema.classifications[task_index].task;
            try std.testing.expectEqualStrings(task.name, task_schema.name);
            try std.testing.expectEqual(task.labels.len, task.raw_logits.len);
            for (task.labels, task.raw_logits, 0..) |label, expected, label_index| {
                try std.testing.expectEqualStrings(label, task_schema.labels[label_index]);
                try std.testing.expectApproxEqAbs(expected, raw.logits[offset + label_index], pipeline.fp32_confidence_tolerance);
            }
            offset += task.labels.len;
        }
        try std.testing.expectEqual(offset, raw.logits.len);

        var result = try pipeline.runScored(a, config, &prepared, &.{&compiled}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints, .control = control });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try pipeline.expectFamilySample(checkpoint, request.id, request.text, request.native_expected, result.samples[0], pipeline.fp32_confidence_tolerance, .unicode_codepoints);
        try std.testing.expectEqual(@as(usize, 0), result.samples[0].entities.len);
        try std.testing.expectEqual(@as(usize, 0), result.samples[0].relations.len);
        try std.testing.expectEqual(@as(usize, 0), result.samples[0].structures.len);
        decoded_json = try extractionResponseJson(a, result.samples[0], prepared.samples[0].input_ids.len);
    }

    const after = try managed.backend.glinerBoundaryScope(&.snapshot);
    try std.testing.expect(!after.active);
    try std.testing.expectEqual(@as(usize, 0), after.pending_device_bytes);
    try std.testing.expectEqual(@as(usize, 0), after.workspace_pending_bytes);
    return .{
        .logits = logits.?,
        .decoded_json = decoded_json.?,
        .fused_ffn_calls = after.fused_ffn_calls -| before.fused_ffn_calls,
    };
}

fn expectWatchdogIdle(watchdog: *HardCancellationWatchdog) !void {
    while (!watchdog.mutex.tryLock()) std.atomic.spinLoopHint();
    defer watchdog.mutex.unlock();
    try std.testing.expect(watchdog.io != null);
    try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
}

fn optimizedFfnReleasedArtifactParity(comptime checkpoint: pipeline.FamilyCheckpoint) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    if (std.c.getenv(disable_fused_ffn_env) != null) return error.SkipZigTest;
    defer _ = unsetenv(disable_fused_ffn_env);
    const directory = platform.env.getenv(checkpoint.environment_prefix ++ "_MODEL_DIR") orelse return error.SkipZigTest;
    const capture_environment = (checkpoint.capture_environment_prefix orelse checkpoint.environment_prefix) ++ "_CAPTURE";
    const a = std.testing.allocator;
    const watchdog = try HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const control = Control{
        .io = std.testing.io,
        .deadline_ns = try std.math.add(u64, platform.time.monotonicNs(), 600 * std.time.ns_per_s),
        .cancellation_grace_ns = 5 * std.time.ns_per_s,
        .hard_cancellation = watchdog.boundary(),
    };

    const capture_bytes = if (platform.env.getenv(capture_environment)) |path|
        try c_file.readFileMax(a, path, 2 * 1024 * 1024)
    else
        try fixtures.fixtureBytes(a, checkpoint.capture_fixture);
    defer a.free(capture_bytes);
    try expectPin(checkpoint.capture, capture_bytes);
    const capture = try std.json.parseFromSlice(pipeline.FamilyReference, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try expectReference(checkpoint, capture.value);

    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", checkpoint.sidecars.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinned(a, directory, "config.json", checkpoint.sidecars.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", checkpoint.sidecars.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const tokenizer_config = try readPinned(a, directory, "tokenizer_config.json", checkpoint.sidecars.@"tokenizer_config.json");
    defer a.free(tokenizer_config);
    const parsed_config = try model.parseConfig(a, config_bytes, encoder_bytes);
    try std.testing.expectEqual(@as(u32, 3072), parsed_config.encoder.intermediate_size);

    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    try expectPin(checkpoint.model, weights.reader.file_bytes);

    var controller = admission_memory.AdmissionController{};
    defer controller.deinit();
    const session = try factory.createMetalSession(a, directory);
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try std.testing.expectEqual(.fp32, identity.precision);
    try identity.weight.verify(.{ .path = "model.safetensors", .size_bytes = checkpoint.model.size_bytes, .sha256 = checkpoint.model.sha256 });
    inline for (bundle.sidecar_names, 0..) |name, index| {
        const pin = @field(checkpoint.sidecars, name);
        try identity.sidecars[index].verify(.{ .path = name, .size_bytes = pin.size_bytes, .sha256 = pin.sha256 });
    }
    try factory.prepareGlinerBoundaryResident(session, control);
    try std.testing.expect(factory.isGlinerBoundaryResidentReady(session));
    const config = try factory.getGlinerBoundaryConfig(session);
    try std.testing.expectEqual(parsed_config.encoder.intermediate_size, config.encoder.intermediate_size);
    for (capture.value.requests) |request| {
        errdefer std.debug.print("{s} optimized fused FFN family case: {s}\n", .{ checkpoint.profile, request.id });
        var disabled = try optimizedClassificationArm(a, session, control, &controller, &config, tokenizer.tokenizer(), checkpoint, request, true);
        defer disabled.deinit(a);
        try std.testing.expectEqual(@as(u64, 0), disabled.fused_ffn_calls);
        var enabled = try optimizedClassificationArm(a, session, control, &controller, &config, tokenizer.tokenizer(), checkpoint, request, false);
        defer enabled.deinit(a);
        try std.testing.expectEqual(@as(u64, @intCast(config.encoder.num_hidden_layers)), enabled.fused_ffn_calls);
        try std.testing.expectEqual(disabled.logits.len, enabled.logits.len);
        for (disabled.logits, enabled.logits) |old, fused|
            try std.testing.expectApproxEqAbs(old, fused, pipeline.fp32_confidence_tolerance);
        const disabled_decoded = try std.json.parseFromSlice(std.json.Value, a, disabled.decoded_json, .{});
        defer disabled_decoded.deinit();
        const enabled_decoded = try std.json.parseFromSlice(std.json.Value, a, enabled.decoded_json, .{});
        defer enabled_decoded.deinit();
        try expectJsonApprox(disabled_decoded.value, enabled_decoded.value);
        try expectWatchdogIdle(watchdog);
    }
    _ = try factory.planGlinerBoundaryWorkspace(session, 1, 1);
    const owners = try factory.glinerBoundaryResidentStats(session);
    try std.testing.expect(owners.ready);
    try std.testing.expect(owners.model_live_bytes > 0);
}

test "GLiNER2.5 multilingual v1 optimized resident fused FFN exact classification parity" {
    try optimizedFfnReleasedArtifactParity(pipeline.multilingual_general_classification_checkpoints[0]);
}

test "GLiNER2.5 multilingual v1 exact family capture Metal resident parity" {
    try parity(pipeline.multilingual_family_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide exact family capture Metal resident parity" {
    try parity(pipeline.multilingual_family_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 mixed endpoint capture Metal resident parity" {
    try parity(pipeline.multilingual_endpoint_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide mixed endpoint capture Metal resident parity" {
    try parity(pipeline.multilingual_endpoint_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 classification floor capture Metal resident parity" {
    try parity(pipeline.multilingual_classification_floor_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide classification floor capture Metal resident parity" {
    try parity(pipeline.multilingual_classification_floor_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 general classification capture Metal resident parity" {
    try parity(pipeline.multilingual_general_classification_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide general classification capture Metal resident parity" {
    try parity(pipeline.multilingual_general_classification_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 general entity capture Metal resident parity" {
    try parity(pipeline.multilingual_general_entity_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide general entity capture Metal resident parity" {
    try parity(pipeline.multilingual_general_entity_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 clean short entity capture Metal resident parity" {
    try parity(pipeline.multilingual_clean_entity_short_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide clean short entity capture Metal resident parity" {
    try parity(pipeline.multilingual_clean_entity_short_checkpoints[1]);
}

test "GLiNER2.5 multilingual v1 source-word floor capture Metal resident parity" {
    try parity(pipeline.multilingual_source_word_floor_checkpoints[0]);
}

test "GLiNER2.5 multilingual Decide source-word floor capture Metal resident parity" {
    try parity(pipeline.multilingual_source_word_floor_checkpoints[1]);
}

test "GLiNER2.5 multilingual Decide public wire capture Metal resident parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const checkpoint = pipeline.multilingual_family_checkpoints[1];
    const directory = platform.env.getenv("ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;

    const capture_bytes = try fixtures.fixtureBytes(a, "family/multi_decide_decide_capture.json");
    defer a.free(capture_bytes);
    try expectPin(.{
        .sha256 = "04d9bbf219858166a1b0d7199b12673dd81eac7fae37454757d213debdd6e732",
        .size_bytes = 23_332,
    }, capture_bytes);
    const capture = try std.json.parseFromSlice(DecideCapture, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try std.testing.expectEqual(@as(u32, 1), capture.value.format_version);
    try std.testing.expectEqualStrings("captured", capture.value.status);
    try std.testing.expectEqualStrings("upstream_public_decide_reference", capture.value.scope);
    try std.testing.expectEqual(@as(usize, 2), capture.value.requests.len);

    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    try expectPin(checkpoint.model, weights.reader.file_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", checkpoint.sidecars.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinned(a, directory, "config.json", checkpoint.sidecars.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", checkpoint.sidecars.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const tokenizer_config = try readPinned(a, directory, "tokenizer_config.json", checkpoint.sidecars.@"tokenizer_config.json");
    defer a.free(tokenizer_config);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);

    var store = try head_tests.loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();

    for (capture.value.requests) |reference| {
        errdefer std.debug.print("multi_decide public wire Metal case: {s}\n", .{reference.id});
        var request_arena = std.heap.ArenaAllocator.init(a);
        defer request_arena.deinit();
        const request_allocator = request_arena.allocator();
        const wire_request = try decide.parse(request_allocator, reference.decide_request_json);
        try std.testing.expectEqualStrings(checkpoint.repo, wire_request.model);
        try std.testing.expectEqualStrings(reference.text, wire_request.state);
        const extraction = try decide.extractionInput(request_allocator, wire_request, .boundary);
        const extraction_value = try std.json.parseFromSlice(std.json.Value, request_allocator, extraction.json, .{});
        const wire_schema = extraction_value.value.object.get("schema") orelse return error.InvalidFamilyReference;
        const wire_schema_json = try std.json.Stringify.valueAlloc(request_allocator, wire_schema, .{});
        try std.testing.expectEqualStrings(reference.native_schema_json, wire_schema_json);

        var schema = try schema_mod.compile(a, wire_schema_json, .{});
        defer schema.deinit();
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = wire_request.state, .schema = &schema }}, .{});
        defer prepared.deinit();
        try std.testing.expectEqualSlices(i64, reference.encoded.input_ids, prepared.input_ids);
        try std.testing.expectEqualSlices(i64, reference.encoded.attention_mask, prepared.attention_mask);
        try std.testing.expectEqualSlices(i64, reference.native_classification.input_ids, prepared.samples[0].input_ids);

        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{}) else null;
        defer if (headed) |*value| value.deinit();
        var scorer = try adapter.Context.init(a, &config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();
        var raw = try scorer.scorer().classify(a, .{}, null);
        defer raw.deinit();

        var offset: usize = 0;
        for (reference.native_classification.tasks) |task| {
            const expected_task = reference.probabilities.object.get(task.name) orelse return error.InvalidFamilyReference;
            try std.testing.expectEqual(task.labels.len, task.raw_logits.len);
            var maximum = task.raw_logits[0];
            for (task.raw_logits[1..]) |logit| maximum = @max(maximum, logit);
            var denominator: f64 = 0;
            const temperature: f64 = @floatCast(config.head.classification_temperature);
            for (task.raw_logits) |logit| denominator += @exp(@as(f64, logit - maximum) / temperature);
            for (task.labels, task.raw_logits, 0..) |label, logit, label_index| {
                try std.testing.expectApproxEqAbs(logit, raw.logits[offset + label_index], pipeline.fp32_confidence_tolerance);
                const expected_probability = expected_task.object.get(label) orelse return error.InvalidFamilyReference;
                const probability = @exp(@as(f64, logit - maximum) / temperature) / denominator;
                try std.testing.expectApproxEqAbs(expected_probability.float, probability, pipeline.fp32_confidence_tolerance);
            }
            offset += task.labels.len;
        }
        try std.testing.expectEqual(offset, raw.logits.len);

        var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try std.testing.expectEqual(reference.native_classification.tasks.len, result.samples[0].classifications.len);
        for (result.samples[0].classifications) |classification| {
            const expected_task = reference.probabilities.object.get(classification.name) orelse return error.InvalidFamilyReference;
            for (classification.labels) |label| {
                const expected_probability = expected_task.object.get(label.label) orelse return error.InvalidFamilyReference;
                try std.testing.expectApproxEqAbs(expected_probability.float, label.confidence, pipeline.fp32_confidence_tolerance);
            }
        }

        const extraction_json = try extractionResponseJson(request_allocator, result.samples[0], prepared.samples[0].input_ids.len);
        const response_json = try decide.responseJson(request_allocator, wire_request, extraction_json, .boundary);
        const response = try std.json.parseFromSlice(std.json.Value, request_allocator, response_json, .{});
        const expected_model = reference.decide_expected.object.get("model") orelse return error.InvalidFamilyReference;
        const expected_answers = reference.decide_expected.object.get("answers") orelse return error.InvalidFamilyReference;
        try expectJsonApprox(expected_model, response.value.object.get("model") orelse return error.InvalidFamilyReference);
        try expectJsonApprox(expected_answers, response.value.object.get("answers") orelse return error.InvalidFamilyReference);
        // The explicit raw-logit oracle and the public presentation each
        // request the small classifier result once. Encoder activations stay
        // resident; only those bounded result vectors may download.
        try std.testing.expect(encoded.stats().result_download_calls <= 2);
        const stats = try scorer.stats();
        try std.testing.expect(stats.result_download_bytes <= scorer.limits.max_result_download_bytes);
    }
}
