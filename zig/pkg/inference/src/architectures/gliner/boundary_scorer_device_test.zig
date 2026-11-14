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
const build_options = @import("build_options");
const c_file = @import("../../util/c_file.zig");
const model = @import("../../models/gliner_boundary.zig");
const engine = @import("boundary_engine_device.zig");
const head = @import("boundary_device.zig");
const adapter = @import("boundary_scorer_device.zig");
const fixtures = @import("boundary_parity_test.zig");
const head_tests = @import("boundary_device_test.zig");
const metal = @import("../../ops/metal_compute.zig");
const pipeline = @import("../../pipelines/gliner_boundary_pipeline.zig");

fn expectPin(pin: pipeline.PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(pin.size_bytes, bytes.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    try std.testing.expectEqualStrings(pin.sha256, &std.fmt.bytesToHex(digest, .lower));
}
fn readPinned(a: std.mem.Allocator, directory: []const u8, name: []const u8, pin: pipeline.PublishedModelPin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const bytes = try @import("../../util/c_file.zig").readFile(a, path);
    errdefer a.free(bytes);
    try expectPin(pin, bytes);
    return bytes;
}
fn parity(comptime variant: []const u8, comptime environment: [:0]const u8, comptime fixture_name: []const u8) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv(environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, fixture_name);
    defer a.free(bytes);
    const capture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer capture.deinit();
    try std.testing.expectEqual(@as(u32, 2), capture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", capture.value.source_commit);
    try std.testing.expectEqualStrings(variant, capture.value.model);
    const reference = try fixtures.fixtureBytes(a, variant ++ "_reference/capture.json");
    defer a.free(reference);
    try expectPin(.{ .sha256 = capture.value.reference_sha256, .size_bytes = reference.len }, reference);
    const requests = try fixtures.fixtureBytes(a, "requests.json");
    defer a.free(requests);
    try expectPin(.{ .sha256 = capture.value.requests_sha256, .size_bytes = requests.len }, requests);
    const path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try @import("../../models/safetensors.zig").MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    c_file.MmapRegion.adviseBytesSequential(weights.reader.file_bytes);
    defer c_file.MmapRegion.adviseBytesRandom(weights.reader.file_bytes);
    try expectPin(capture.value.model_files.@"model.safetensors", weights.reader.file_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", capture.value.model_files.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinned(a, directory, "config.json", capture.value.model_files.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", capture.value.model_files.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var store = try head_tests.loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    for (capture.value.cases, 0..) |case, case_index| {
        errdefer std.debug.print(variant ++ " resident pipeline case: {s}\n", .{case.id});
        const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        defer a.free(schema_json);
        var schema = try @import("../../pipelines/extraction_schema.zig").compile(a, schema_json, .{});
        defer schema.deinit();
        var prepared = try @import("../../pipelines/gliner_boundary_processor.zig").prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{}) else null;
        defer if (headed) |*value| value.deinit();
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        var scorer = try adapter.Context.init(a, &config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();
        var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try pipeline.expectSample(case.expected, result.samples[0], pipeline.fp32_confidence_tolerance);
        // The only encoder-owner readback is the scalar classification output.
        try std.testing.expectEqual(@as(usize, @intFromBool(prepared.classification_width > 0)), encoded.stats().result_download_calls);
        const stats = try scorer.stats();
        try std.testing.expect(stats.result_download_bytes <= scorer.limits.max_result_download_bytes);
        if (case_index == 0) {
            // Admission rejects cumulative transfers before copying a tensor.
            const before = try scorer.stats();
            scorer.limits.max_result_download_bytes = before.result_download_bytes;
            if (prepared.classification_width > 0) try std.testing.expectError(error.ResourceLimitExceeded, scorer.scorer().classify(a, .{}, null));
            try std.testing.expectEqual(before.result_download_bytes, (try scorer.stats()).result_download_bytes);
        }
    }
}

test "gliner boundary device Metal pinned small full inference pipeline parity" {
    try parity("small", "ANTFLY_GLINER25_SMALL_MODEL_DIR", "pipeline_cases.json");
}
test "gliner boundary device Metal pinned base full inference pipeline parity" {
    try parity("base", "ANTFLY_GLINER25_BASE_MODEL_DIR", "pipeline_cases_base.json");
}
test "gliner boundary device Metal pinned multi full inference pipeline parity" {
    try parity("multi", "ANTFLY_GLINER25_MULTI_MODEL_DIR", "pipeline_cases_multi.json");
}

fn cudaParity(comptime variant: []const u8, comptime environment: [:0]const u8, comptime fixture_name: []const u8, comptime transformers_wrapper: bool) !void {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv(environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, fixture_name);
    defer a.free(bytes);
    const capture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer capture.deinit();
    try std.testing.expectEqual(@as(u32, 2), capture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", capture.value.source_commit);
    try std.testing.expectEqualStrings(variant, capture.value.model);
    const reference = try fixtures.fixtureBytes(a, variant ++ "_reference/capture.json");
    defer a.free(reference);
    try expectPin(.{ .sha256 = capture.value.reference_sha256, .size_bytes = reference.len }, reference);
    const requests = try fixtures.fixtureBytes(a, "requests.json");
    defer a.free(requests);
    try expectPin(.{ .sha256 = capture.value.requests_sha256, .size_bytes = requests.len }, requests);
    const path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try @import("../../models/safetensors.zig").MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    c_file.MmapRegion.adviseBytesSequential(weights.reader.file_bytes);
    defer c_file.MmapRegion.adviseBytesRandom(weights.reader.file_bytes);
    try expectPin(capture.value.model_files.@"model.safetensors", weights.reader.file_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", capture.value.model_files.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_pin: pipeline.PublishedModelPin = if (transformers_wrapper) .{
        .sha256 = &@import("../../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1_transformers.sidecars[0].sha256,
        .size_bytes = @import("../../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1_transformers.sidecars[0].size_bytes,
    } else capture.value.model_files.@"config.json";
    const config_bytes = try readPinned(a, directory, "config.json", config_pin);
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", capture.value.model_files.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const factory = @import("../session_factory.zig");
    const session = try factory.createGlinerCudaSessionWithPrecision(a, directory, .fp32);
    defer session.close();
    try std.testing.expectEqual(.cuda, session.backend());
    // Offline evidence gathering uses the same CUDA session and device pipeline;
    // this does not widen the production serving qualification table.
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    for (capture.value.cases, 0..) |case, case_index| {
        const cuda_before = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaRuntimeStats;
        errdefer std.debug.print(variant ++ " resident pipeline case: {s}\n", .{case.id});
        const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        defer a.free(schema_json);
        var schema = try @import("../../pipelines/extraction_schema.zig").compile(a, schema_json, .{});
        defer schema.deinit();
        var prepared = try @import("../../pipelines/gliner_boundary_processor.zig").prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{}) else null;
        defer if (headed) |*value| value.deinit();
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        var scorer = try adapter.Context.init(a, &config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();
        var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try pipeline.expectSample(case.expected, result.samples[0], pipeline.fp32_confidence_tolerance);
        const cuda_after = factory.getCudaRuntimeStats(session).?;
        try std.testing.expect(cuda_after.kernel_launches > cuda_before.kernel_launches);
        try std.testing.expectEqual(cuda_before.to_float32_calls, cuda_after.to_float32_calls);
        // The only encoder-owner readback is the scalar classification output.
        try std.testing.expectEqual(@as(usize, @intFromBool(prepared.classification_width > 0)), encoded.stats().result_download_calls);
        const stats = try scorer.stats();
        try std.testing.expect(stats.result_download_bytes <= scorer.limits.max_result_download_bytes);
        if (case_index == 0) {
            // Admission rejects cumulative transfers before copying a tensor.
            const before = try scorer.stats();
            scorer.limits.max_result_download_bytes = before.result_download_bytes;
            if (prepared.classification_width > 0) try std.testing.expectError(error.ResourceLimitExceeded, scorer.scorer().classify(a, .{}, null));
            try std.testing.expectEqual(before.result_download_bytes, (try scorer.stats()).result_download_bytes);
        }
    }
}

test "gliner boundary device CUDA pinned base full inference pipeline parity" {
    try cudaParity("base", "ANTFLY_GLINER25_BASE_MODEL_DIR", "pipeline_cases_base.json", false);
}

test "gliner boundary device CUDA pinned multi full inference pipeline parity" {
    try cudaParity("multi", "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", "pipeline_cases_multi.json", false);
}

test "GLiNER production CUDA required artifacts" {
    const platform = @import("antfly_platform");
    if (!platform.env.getenvBool("ANTFLY_GLINER25_REQUIRE_PRODUCTION_MODELS")) return error.SkipZigTest;
    if (!build_options.enable_cuda) return error.CudaNotEnabled;
    inline for (.{ "ANTFLY_GLINER25_BASE_MODEL_DIR", "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", "ANTFLY_GLINER_DECIDE_MODEL", "ANTFLY_GLINER_DECIDE_Q8_MODEL", "ANTFLY_GLINER25_FAMILY_MODEL_DIR" }) |name| {
        const path = platform.env.getenv(name) orelse {
            std.debug.print("production CUDA qualification requires {s}\n", .{name});
            return error.MissingProductionQualificationModel;
        };
        if (!std.fs.path.isAbsolute(path)) return error.QualificationModelPathMustBeAbsolute;
    }
    const backend = platform.env.getenv("ANTFLY_GLINER_DECIDE_BACKEND") orelse return error.MissingRequiredCudaBackend;
    try std.testing.expectEqualStrings("cuda", backend);
}

test "gliner boundary device CUDA pinned Transformers base full inference pipeline parity" {
    try cudaParity("base", "ANTFLY_GLINER25_BASE_TRANSFORMERS_MODEL_DIR", "pipeline_cases_base.json", true);
}

// Offline physical qualification only. The public feature table remains
// closed until this exact original full-model request passes on CUDA.
test "gliner boundary device CUDA pinned base allow overlap original reference" {
    const platform = @import("antfly_platform");
    if (!platform.env.getenvBool("ANTFLY_GLINER25_BASE_CUDA_OVERLAP_PARITY")) return error.SkipZigTest;
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    const directory = platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.MissingRequiredGlinerArtifact;
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, "base_overlap_allow_reference.json");
    defer a.free(bytes);
    try expectPin(.{ .size_bytes = 3684, .sha256 = "abb3791b6c4ba363cd6cb47a492c05f7fa3c49ee2869b7566484218062e0c98d" }, bytes);
    const Reference = struct {
        model: struct { revision: []const u8, files: pipeline.PublishedModelFiles },
        reference: struct { source_revision: []const u8 },
        request: struct {
            schema_version: u32,
            schema: std.json.Value,
            inputs: []const struct { content: []const u8 },
            options: struct { threshold: f32, overlap: []const u8, offset_unit: []const u8, include_spans: bool, include_confidence: bool },
        },
        expected_entities: []const struct { label: []const u8, text: []const u8, start: usize, end: usize, score: f32 },
        expected_classifications: []const std.json.Value,
    };
    const capture = try std.json.parseFromSlice(Reference, a, bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    const ref = capture.value;
    try std.testing.expectEqualStrings("72ac19b486cd4557424c8d61114e7530c243e9b0", ref.model.revision);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", ref.reference.source_revision);
    try std.testing.expectEqual(@as(u32, 2), ref.request.schema_version);
    try std.testing.expectEqual(@as(usize, 1), ref.request.inputs.len);
    try std.testing.expectEqualStrings("allow", ref.request.options.overlap);
    try std.testing.expectEqualStrings("unicode_codepoints", ref.request.options.offset_unit);
    try std.testing.expect(ref.request.options.include_spans and ref.request.options.include_confidence);
    try std.testing.expectEqual(@as(usize, 6), ref.expected_entities.len);
    try std.testing.expectEqual(@as(usize, 0), ref.expected_classifications.len);

    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try @import("../../models/safetensors.zig").MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    try expectPin(ref.model.files.@"model.safetensors", weights.reader.file_bytes);
    const config_bytes = try readPinned(a, directory, "config.json", ref.model.files.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", ref.model.files.@"encoder_config/config.json");
    defer a.free(encoder_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", ref.model.files.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer_config = try readPinned(a, directory, "tokenizer_config.json", ref.model.files.@"tokenizer_config.json");
    defer a.free(tokenizer_config);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    const factory = @import("../session_factory.zig");
    const session = try factory.createGlinerCudaSessionWithPrecision(a, directory, .fp32);
    defer session.close();
    try std.testing.expectEqual(.cuda, session.backend());
    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    const before = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaRuntimeStats;
    const schema_bytes = try std.json.Stringify.valueAlloc(a, ref.request.schema, .{});
    defer a.free(schema_bytes);
    var schema = try @import("../../pipelines/extraction_schema.zig").compile(a, schema_bytes, .{});
    defer schema.deinit();
    var prepared = try @import("../../pipelines/gliner_boundary_processor.zig").prepare(a, tokenizer.tokenizer(), &.{.{ .text = ref.request.inputs[0].content, .schema = &schema }}, .{});
    defer prepared.deinit();
    var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
    defer encoded.deinit();
    var headed = try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{});
    defer headed.deinit();
    var scorer = try adapter.Context.init(a, &config, &encoded, &headed, .{});
    defer scorer.deinit();
    var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .threshold = ref.request.options.threshold, .overlap = .allow, .offset_unit = .unicode_codepoints });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.samples.len);
    const sample = result.samples[0];
    try std.testing.expectEqual(@as(usize, 0), sample.classifications.len);
    try std.testing.expectEqual(@as(usize, 0), sample.relations.len);
    try std.testing.expectEqual(@as(usize, 0), sample.structures.len);
    var index: usize = 0;
    for (sample.entities) |entity| for (entity.values) |value| {
        try std.testing.expect(index < ref.expected_entities.len);
        const expected = ref.expected_entities[index];
        const source = value.source orelse return error.InvalidBoundarySpan;
        try std.testing.expectEqualStrings(expected.label, entity.name);
        try std.testing.expectEqualStrings(expected.text, value.text);
        try std.testing.expectEqual(expected.start, source.start);
        try std.testing.expectEqual(expected.end, source.end);
        try std.testing.expectEqual(.unicode_codepoints, source.unit);
        try std.testing.expectApproxEqAbs(expected.score, value.confidence, pipeline.fp32_confidence_tolerance);
        index += 1;
    };
    try std.testing.expectEqual(ref.expected_entities.len, index);
    // Composed independent singleton-oracle coverage, not an upstream batch
    // capture. The public release table remains singleton-only until this
    // diagnostic passes and its measured batch envelope is separately reviewed.
    const executor = @import("../../extractors/gliner_boundary_executor.zig");
    const wire = @import("../../extractors/extraction_v2.zig");
    const provider = @import("../../backends/compute_provider.zig");
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try std.testing.expectEqualDeep(@import("../../models/gliner_boundary_qualification.zig").fastino_gliner25_base_v1, identity);
    var bounded = @import("../../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 256 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const scratch = bounded.allocator();
    const watchdog = try @import("../../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const control = @import("../../execution_control.zig").InferenceExecutionControl{ .hard_cancellation = watchdog.boundary(), .deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s };
    var managed = try provider.acquire(session, scratch, null, control);
    defer managed.deinit();
    for ([_]usize{ 2, 4, 8 }) |batch| {
        var inputs: [8]struct { content: []const u8 } = undefined;
        for (inputs[0..batch]) |*input| input.* = .{ .content = ref.request.inputs[0].content };
        const raw = try std.json.Stringify.valueAlloc(scratch, .{
            .schema_version = ref.request.schema_version,
            .model = "pinned-base-overlap-diagnostic",
            .schema = ref.request.schema,
            .options = ref.request.options,
            .inputs = inputs[0..batch],
        }, .{});
        defer scratch.free(raw);
        var request = try wire.parseJson(scratch, raw, .{});
        defer request.deinit();
        var evidence: [8]executor.ItemExecution = undefined;
        const options = executor.Options{ .identity = identity, .control = control, .item_executions = evidence[0..batch] };
        // No release-table override: public multi-item overlap stays closed,
        // and a rejection must happen before any learned CUDA work executes.
        const prior = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaRuntimeStats;
        try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, executor.executeQualified(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options));
        const rejected = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaRuntimeStats;
        try std.testing.expectEqual(prior.kernel_launches, rejected.kernel_launches);
        // One actual composed execution per width. The canonical AA/ABBA
        // performance owner measures throughput after correctness closes.
        const response = try executor.executeDevice(&managed.backend, scratch, &config, tokenizer.tokenizer(), &request, options);
        defer scratch.free(response);
        var parsed = try std.json.parseFromSlice(std.json.Value, scratch, response, .{});
        defer parsed.deinit();
        const outputs = parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(batch, outputs.len);
        for (outputs, evidence[0..batch]) |output, physical| {
            try std.testing.expectEqual(batch, physical.rows);
            try std.testing.expect(physical.prompt_tokens > 0);
            const entities = output.object.get("entities").?.array.items;
            try std.testing.expectEqual(ref.expected_entities.len, entities.len);
            for (entities, ref.expected_entities) |entity, expected| {
                const value = entity.object;
                try std.testing.expectEqualStrings(expected.label, value.get("label").?.string);
                try std.testing.expectEqualStrings(expected.text, value.get("text").?.string);
                try std.testing.expectEqual(@as(i64, @intCast(expected.start)), value.get("start").?.integer);
                try std.testing.expectEqual(@as(i64, @intCast(expected.end)), value.get("end").?.integer);
                const score = value.get("score").?;
                const actual_score: f64 = if (score == .float) score.float else @floatFromInt(score.integer);
                try std.testing.expectApproxEqAbs(@as(f64, expected.score), actual_score, @as(f64, pipeline.fp32_confidence_tolerance));
            }
            inline for (.{ "classifications", "relations", "structures" }) |key| {
                if (output.object.get(key)) |values| try std.testing.expectEqual(@as(usize, 0), values.array.items.len);
            }
        }
        std.debug.print("GLiNER CUDA overlap composed: rows={d} source_fp32=true singleton_oracle=true serving_qualified=false\n", .{batch});
    }
    const after = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaRuntimeStats;
    try std.testing.expect(after.kernel_launches > before.kernel_launches);
    try std.testing.expectEqual(before.to_float32_calls, after.to_float32_calls);
    try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
    std.debug.print("GLiNER CUDA overlap original: entities={d} source_fp32=true policy_qualification=false\n", .{index});
}
