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

//! Full-checkpoint parity through the production session and span classifier.
//! This opt-in harness does not grant registry or serving qualification.
const std = @import("std");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const executor = @import("gliner_span_v2_executor.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const decide = @import("antfly_decisions").legacy;
const Tensor = @import("../backends/tensor.zig").Tensor;
const service = @import("../server/gliner_boundary_service_test.zig");
const qualification = @import("../models/gliner_decide_qualification.zig");

pub const files = pipeline.PublishedModelFiles{
    .@"config.json" = .{ .size_bytes = 464, .sha256 = "d2732928820b95649bc87f051345b2394fff87fc28c01f945610f943d6f402b2" },
    .@"encoder_config/config.json" = .{ .size_bytes = 2160, .sha256 = "c2cc4c15c7504b9e15651f2a32dff82b84fa9a99060a3218671a930da0a9b50d" },
    .@"model.safetensors" = .{ .size_bytes = 4755208228, .sha256 = "02c567d791aed26550d300064c7f0c0094fd65291503c65969b45b30786e33b3" },
    .@"tokenizer.json" = .{ .size_bytes = 3585055, .sha256 = "ddb379b6a4679ee16646bf0b726de9ec538d7c80ea9d4d4e33b069f19c9e1efb" },
    .@"tokenizer_config.json" = .{ .size_bytes = 559, .sha256 = "8bb8d094c7cde84942866ef98fb5b24f7387245573a7f707c8349729484c27b3" },
};

pub const capture_pin = pipeline.PublishedModelPin{
    .size_bytes = 73256,
    .sha256 = "de9fe72fd2038a2c62290a2f8f90aeb8081c5007b82f82a47ef137c33d3c5d61",
};

pub fn verifyCaptureBytes(bytes: []const u8) !void {
    if (bytes.len != capture_pin.size_bytes) return error.InvalidFamilyReference;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const hexadecimal = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &hexadecimal, capture_pin.sha256)) return error.InvalidFamilyReference;
}

/// Environment overrides must contain the same immutable reviewed oracle.
pub fn referenceBytes(a: std.mem.Allocator) ![]u8 {
    const bytes = if (platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_CAPTURE")) |path|
        try @import("../util/c_file.zig").readFileMax(a, path, 2 * 1024 * 1024)
    else
        try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, "family/decide_1b_capture.json");
    errdefer a.free(bytes);
    try verifyCaptureBytes(bytes);
    return bytes;
}

test "GLiNER2.5 Decide 1B reference binds whole capture bytes and rejects drift" {
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, "family/decide_1b_capture.json");
    defer a.free(bytes);
    try verifyCaptureBytes(bytes);
    bytes[bytes.len / 2] ^= 1;
    try std.testing.expectError(error.InvalidFamilyReference, verifyCaptureBytes(bytes));
    try std.testing.expectError(error.InvalidFamilyReference, verifyCaptureBytes(bytes[0 .. bytes.len - 1]));
}

const Reference = struct {
    format_version: u32,
    status: []const u8,
    scope: []const u8,
    qualification: bool,
    native_runtime_qualified: bool,
    production_qualified: bool,
    model: struct {
        status: []const u8,
        qualification: bool,
        profile: []const u8,
        repo: []const u8,
        revision: []const u8,
        architecture: []const u8,
        encoder_family: []const u8,
        model_sha256: []const u8,
        model_size_bytes: usize,
        tensor_count: usize,
        parameter_count: usize,
        tensor_header_sha256: []const u8,
        sidecars: struct {
            @"config.json": pipeline.PublishedModelPin,
            @"encoder_config/config.json": pipeline.PublishedModelPin,
            @"tokenizer.json": pipeline.PublishedModelPin,
            @"tokenizer_config.json": pipeline.PublishedModelPin,
        },
    },
    source: struct { repo: []const u8, revision: []const u8, package_version: []const u8 },
    artifacts: struct {
        generator_sha256: []const u8,
        contract_sha256: []const u8,
        runtime_contract_sha256: []const u8,
        shared_helper_sha256: []const u8,
        public_decide_helper_sha256: []const u8,
        requests_sha256: []const u8,
        public_requests_sha256: []const u8,
    },
    runtime: struct {
        python: []const u8,
        unicode: []const u8,
        isolated_packages: struct { transformers: []const u8, tokenizers: []const u8 },
        external_packages: struct { torch: []const u8, pydantic: []const u8 },
        transformers_distribution: struct { sha256: []const u8, source_revision: []const u8 },
        verified_tree: struct { files: usize, record_files: usize, tree_sha256: []const u8 },
        device: []const u8,
        dtype: []const u8,
        threads: usize,
    },
    rope: struct {
        head_dim: usize,
        layers: struct {
            full_attention: struct { rope_theta: f64, inv_freq_sha256_f32le: []const u8 },
            sliding_attention: struct { rope_theta: f64, inv_freq_sha256_f32le: []const u8 },
        },
    },
    requests: []const struct {
        id: []const u8,
        kind: []const u8,
        text: []const u8,
        native_schema_json: []const u8,
        native_expected: pipeline.ExpectedSample,
        encoded: struct { input_ids: []const i64, attention_mask: []const i64 },
        native_classification: struct {
            input_ids: []const i64,
            tasks: []const struct { name: []const u8, labels: []const []const u8, raw_logits: []const f32 },
        },
    },
    public_decide_requests: []const struct {
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

fn expectJsonApprox(expected: std.json.Value, actual: std.json.Value, tolerance: f64) !void {
    if (expected == .integer and actual == .float)
        return std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(expected.integer)), actual.float, tolerance);
    if (expected == .float and actual == .integer)
        return std.testing.expectApproxEqAbs(expected.float, @as(f64, @floatFromInt(actual.integer)), tolerance);
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return error.TestExpectedEqual;
    switch (expected) {
        .null => {},
        .bool => |value| try std.testing.expectEqual(value, actual.bool),
        .integer => |value| try std.testing.expectEqual(value, actual.integer),
        .float => |value| try std.testing.expectApproxEqAbs(value, actual.float, tolerance),
        .number_string => |value| try std.testing.expectEqualStrings(value, actual.number_string),
        .string => |value| try std.testing.expectEqualStrings(value, actual.string),
        .array => |values| {
            try std.testing.expectEqual(values.items.len, actual.array.items.len);
            for (values.items, actual.array.items) |want, got| try expectJsonApprox(want, got, tolerance);
        },
        .object => |values| {
            try std.testing.expectEqual(values.count(), actual.object.count());
            var it = values.iterator();
            while (it.next()) |entry| {
                const got = actual.object.get(entry.key_ptr.*) orelse return error.TestExpectedEqual;
                try expectJsonApprox(entry.value_ptr.*, got, tolerance);
            }
        },
    }
}

fn extractionResponseJson(a: std.mem.Allocator, classifications: []const pipeline.Classification, prompt_tokens: usize) ![]u8 {
    var rows = std.array_list.Managed(std.json.Value).init(a);
    for (classifications) |classification| for (classification.labels) |label| {
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
    return std.json.Stringify.valueAlloc(a, std.json.Value{ .object = root }, .{});
}

fn parity(metal: bool) !void {
    const directory = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR") orelse return error.SkipZigTest;
    if (metal and !@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const capture_bytes = try referenceBytes(a);
    defer a.free(capture_bytes);
    const capture = try std.json.parseFromSlice(Reference, a, capture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    const ref = capture.value;
    try std.testing.expectEqual(@as(u32, 1), ref.format_version);
    try std.testing.expectEqualStrings("captured", ref.status);
    try std.testing.expectEqualStrings("upstream_decide_1b_classification_reference", ref.scope);
    try std.testing.expect(!ref.qualification);
    try std.testing.expect(!ref.native_runtime_qualified);
    try std.testing.expect(!ref.production_qualified);
    try std.testing.expectEqualStrings("verified", ref.model.status);
    try std.testing.expect(!ref.model.qualification);
    try std.testing.expectEqualStrings("decide_1b", ref.model.profile);
    try std.testing.expectEqualStrings("fastino/GLiNER2.5-Decide-1B", ref.model.repo);
    try std.testing.expectEqualStrings("688cd7ba8917a0855ad3ce929cba5a9998932e79", ref.model.revision);
    try std.testing.expectEqualStrings("span", ref.model.architecture);
    try std.testing.expectEqualStrings("modernbert", ref.model.encoder_family);
    try std.testing.expectEqualStrings(files.@"model.safetensors".sha256, ref.model.model_sha256);
    try std.testing.expectEqual(files.@"model.safetensors".size_bytes, ref.model.model_size_bytes);
    try std.testing.expectEqual(@as(usize, 199), ref.model.tensor_count);
    try std.testing.expectEqual(@as(usize, 1188796693), ref.model.parameter_count);
    try std.testing.expectEqualStrings("449e7c096746bf06825e71d40d215de142f5f404f560c601a0d9bafc1bca0bae", ref.model.tensor_header_sha256);
    inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" }) |name| {
        try std.testing.expectEqual(@field(files, name).size_bytes, @field(ref.model.sidecars, name).size_bytes);
        try std.testing.expectEqualStrings(@field(files, name).sha256, @field(ref.model.sidecars, name).sha256);
    }
    try std.testing.expectEqualStrings("https://github.com/fastino-ai/GLiNER2", ref.source.repo);
    try std.testing.expectEqualStrings("55656fbfa01d3d4a77485e1a1eeeaf682990ccdf", ref.source.revision);
    try std.testing.expectEqualStrings("2.0.0", ref.source.package_version);
    try std.testing.expectEqualStrings("ad272a7ec1aec50c0156c824d8ea67d4e7d8a186201b3c1a768a1264075512dd", ref.artifacts.generator_sha256);
    try std.testing.expectEqualStrings("0beb19e072fd46f0318bd7a1e2c0ac3b8ad1deaf47bdc0cf1ce7ca1e9c6364c5", ref.artifacts.contract_sha256);
    try std.testing.expectEqualStrings("d807f5ad9fecbcd96c870c3965ca8357ca8a6fbfeafe3ecc5eace35f53a0e472", ref.artifacts.runtime_contract_sha256);
    try std.testing.expectEqualStrings("4e9fee278e8e4757f1ee03aec87208dfc7cbb58eb7a009b30e8f525c8eb9cde5", ref.artifacts.shared_helper_sha256);
    try std.testing.expectEqualStrings("4364c3fc1cfacfca4bad2cd48e19a8600af8041339b817e44c111e252bb1a844", ref.artifacts.public_decide_helper_sha256);
    try std.testing.expectEqualStrings("b97d2c69860e3496b3f4d01df055c54a42b6b3b78e95fb33cbc83dbb371e8271", ref.artifacts.requests_sha256);
    try std.testing.expectEqualStrings("450cd404b0ce9bd00f0cb9ccf1a54744f9faeb31e39de020062f1b9bf296126a", ref.artifacts.public_requests_sha256);
    try std.testing.expectEqualStrings("3.12.3", ref.runtime.python);
    try std.testing.expectEqualStrings("15.0.0", ref.runtime.unicode);
    try std.testing.expectEqualStrings("5.17.0", ref.runtime.isolated_packages.transformers);
    try std.testing.expectEqualStrings("0.23.2", ref.runtime.isolated_packages.tokenizers);
    try std.testing.expectEqualStrings("2.9.1", ref.runtime.external_packages.torch);
    try std.testing.expectEqualStrings("2.12.3", ref.runtime.external_packages.pydantic);
    try std.testing.expectEqualStrings("78ec1ce21579b38dfb83950a0658cd119f87212a2fcfdff478096ce9d6c03801", ref.runtime.transformers_distribution.sha256);
    try std.testing.expectEqualStrings("856157a2f3e9594954310df18fdccc31ffddebe9", ref.runtime.transformers_distribution.source_revision);
    try std.testing.expectEqual(@as(usize, 4908), ref.runtime.verified_tree.files);
    try std.testing.expectEqual(@as(usize, 27), ref.runtime.verified_tree.record_files);
    try std.testing.expectEqualStrings("6259dc40ff96d6bb7afaf079345be8e36bc3c3fd77194b9a88a6c635ba0f6969", ref.runtime.verified_tree.tree_sha256);
    try std.testing.expectEqualStrings("cpu", ref.runtime.device);
    try std.testing.expectEqualStrings("float32", ref.runtime.dtype);
    try std.testing.expectEqual(@as(usize, 2), ref.runtime.threads);
    try std.testing.expectEqual(@as(usize, 64), ref.rope.head_dim);
    inline for (.{ "full_attention", "sliding_attention" }) |kind| {
        const rope = @field(ref.rope.layers, kind);
        try std.testing.expectEqual(@as(f64, 160000), rope.rope_theta);
        try std.testing.expectEqualStrings("adb41194debe8e6c185d47754f68cc2dd59960a03cb4ccbc71acd14b860c4604", rope.inv_freq_sha256_f32le);
    }
    try std.testing.expectEqual(@as(usize, 8), ref.requests.len);
    var crossed_local_window = false;
    for (ref.requests) |request| {
        if (std.mem.eql(u8, request.id, "long_context_cutoff")) {
            try std.testing.expectEqual(@as(usize, 198), request.encoded.input_ids.len);
            crossed_local_window = true;
        }
    }
    try std.testing.expect(crossed_local_window);
    try service.verifyFiles(a, directory, files);

    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    try std.testing.expect(config == .modern_bert);
    try std.testing.expectEqual(@as(u32, 1792), config.modern_bert.hidden_size);
    try std.testing.expectEqual(@as(u32, 3840), config.modern_bert.intermediate_size);
    try std.testing.expectEqual(@as(u32, 28), config.modern_bert.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 28), config.modern_bert.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 7999), config.modern_bert.max_position_embeddings);
    try std.testing.expectEqual(@as(f32, 160000), config.modern_bert.global_rope_theta);
    try std.testing.expectEqual(@as(f32, 160000), config.modern_bert.local_rope_theta);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();

    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const tolerance: f64 = if (metal) 5e-3 else 2e-3;
    var max_error: f64 = 0;
    for (ref.requests, 0..) |request, request_index| {
        errdefer std.debug.print("Decide-1B {s}/{s}\n", .{ if (metal) "metal" else "native", request.id });
        const control = @import("../execution_control.zig").InferenceExecutionControl{
            .hard_cancellation = if (watchdog) |owner| owner.boundary() else null,
            .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s,
        };
        try std.testing.expectEqualStrings("classification", request.kind);
        var compiled = try schema_mod.compile(a, request.native_schema_json, .{});
        defer compiled.deinit();
        var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = request.text, .schema = &compiled }}, .{ .max_sequence_tokens = 7999, .max_batch_tokens = 7999 });
        defer prepared.deinit();
        try std.testing.expectEqualSlices(i64, request.encoded.input_ids, prepared.input_ids);
        try std.testing.expectEqualSlices(i64, request.encoded.attention_mask, prepared.attention_mask);
        try std.testing.expectEqualSlices(i64, request.native_classification.input_ids, prepared.input_ids);
        const counts = try a.alloc(usize, compiled.schema.classifications.len);
        defer a.free(counts);
        for (compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;
        const rows = rows: {
            var managed = try factory.getManagedComputeBackend(session, a, null, control);
            defer managed.deinit();
            break :rows try executor.classificationLogits(&managed.backend, a, .{ .modern_bert = config.modern_bert }, prepared.samples[0], counts);
        };
        defer {
            for (rows) |row| a.free(row);
            a.free(rows);
        }
        try std.testing.expectEqual(request.native_classification.tasks.len, rows.len);
        for (request.native_classification.tasks, rows, compiled.schema.classifications) |task, got, classification| {
            try std.testing.expectEqualStrings(task.name, classification.task.name);
            try std.testing.expectEqual(task.labels.len, got.len);
            try std.testing.expectEqual(task.raw_logits.len, got.len);
            for (task.labels, task.raw_logits, got, classification.task.labels) |label, want, actual, compiled_label| {
                try std.testing.expectEqualStrings(label, compiled_label);
                max_error = @max(max_error, @abs(@as(f64, want) - actual));
                try std.testing.expectApproxEqAbs(@as(f64, want), actual, tolerance);
            }
        }
        const const_rows = try a.alloc([]const f64, rows.len);
        defer a.free(const_rows);
        for (rows, const_rows) |row, *out| out.* = row;
        var presented = try pipeline.presentClassifications(a, &compiled, const_rows, 1, .{});
        defer presented.deinit();
        try pipeline.expectSample(request.native_expected, .{ .classifications = presented.classifications }, 5e-4);

        if (request_index == 0) {
            const mask = try a.alloc(i64, prepared.classification_width);
            defer a.free(mask);
            @memset(mask, 1);
            var inputs = [_]Tensor{
                try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(prepared.sequence_length) }, prepared.input_ids),
                try Tensor.initInt64(a, "attention_mask", &.{ 1, @intCast(prepared.sequence_length) }, prepared.attention_mask),
                try Tensor.initInt64(a, "decision_marker_positions", &.{ 1, @intCast(mask.len) }, prepared.cls_marker_indices),
                try Tensor.initInt64(a, "decision_marker_mask", &.{ 1, @intCast(mask.len) }, mask),
            };
            defer for (&inputs) |*input| input.deinit();
            const outputs = try session.runWithControl(&inputs, a, control);
            defer {
                for (outputs) |*output| output.deinit();
                a.free(outputs);
            }
            try std.testing.expectEqual(@as(usize, 1), outputs.len);
            var offset: usize = 0;
            for (rows) |row| for (row) |value| {
                try std.testing.expectApproxEqAbs(value, @as(f64, outputs[0].asFloat32()[offset]), tolerance);
                offset += 1;
            };
            try std.testing.expectEqual(offset, outputs[0].asFloat32().len);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), ref.public_decide_requests.len);
    for (ref.public_decide_requests) |reference| {
        errdefer std.debug.print("Decide-1B {s}/public/{s}\n", .{ if (metal) "metal" else "native", reference.id });
        const control = @import("../execution_control.zig").InferenceExecutionControl{
            .hard_cancellation = if (watchdog) |owner| owner.boundary() else null,
            .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s,
        };
        var managed = try factory.getManagedComputeBackend(session, a, null, control);
        defer managed.deinit();
        var request_arena = std.heap.ArenaAllocator.init(a);
        defer request_arena.deinit();
        const request_allocator = request_arena.allocator();
        const wire_request = try decide.parse(request_allocator, reference.decide_request_json);
        try std.testing.expectEqualStrings(ref.model.repo, wire_request.model);
        try std.testing.expectEqualStrings(reference.text, wire_request.state);
        const extraction = try decide.extractionInput(request_allocator, wire_request, .span_marker);
        const extraction_value = try std.json.parseFromSlice(std.json.Value, request_allocator, extraction.json, .{});
        const wire_schema = extraction_value.value.object.get("schema") orelse return error.InvalidFamilyReference;
        const wire_schema_json = try std.json.Stringify.valueAlloc(request_allocator, wire_schema, .{});
        try std.testing.expectEqualStrings(reference.native_schema_json, wire_schema_json);

        var compiled = try schema_mod.compile(a, wire_schema_json, .{});
        defer compiled.deinit();
        var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = wire_request.state, .schema = &compiled }}, .{ .max_sequence_tokens = 7999, .max_batch_tokens = 7999 });
        defer prepared.deinit();
        try std.testing.expectEqualSlices(i64, reference.encoded.input_ids, prepared.input_ids);
        try std.testing.expectEqualSlices(i64, reference.encoded.attention_mask, prepared.attention_mask);
        try std.testing.expectEqualSlices(i64, reference.native_classification.input_ids, prepared.input_ids);
        const counts = try a.alloc(usize, compiled.schema.classifications.len);
        defer a.free(counts);
        for (compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;
        const rows = try executor.classificationLogits(&managed.backend, a, .{ .modern_bert = config.modern_bert }, prepared.samples[0], counts);
        defer {
            for (rows) |row| a.free(row);
            a.free(rows);
        }
        try std.testing.expectEqual(reference.native_classification.tasks.len, rows.len);
        for (reference.native_classification.tasks, rows, compiled.schema.classifications) |task, got, classification| {
            try std.testing.expectEqualStrings(task.name, classification.task.name);
            try std.testing.expectEqual(task.labels.len, got.len);
            for (task.labels, task.raw_logits, got, classification.task.labels) |label, want, actual, compiled_label| {
                try std.testing.expectEqualStrings(label, compiled_label);
                max_error = @max(max_error, @abs(@as(f64, want) - actual));
                try std.testing.expectApproxEqAbs(@as(f64, want), actual, tolerance);
            }
        }
        const const_rows = try a.alloc([]const f64, rows.len);
        defer a.free(const_rows);
        for (rows, const_rows) |row, *out| out.* = row;
        var presented = try pipeline.presentClassifications(a, &compiled, const_rows, 1, .{});
        defer presented.deinit();
        try std.testing.expectEqual(reference.native_classification.tasks.len, presented.classifications.len);
        for (presented.classifications) |classification| {
            const expected_task = reference.probabilities.object.get(classification.name) orelse return error.InvalidFamilyReference;
            try std.testing.expectEqual(expected_task.object.count(), classification.labels.len);
            for (classification.labels) |label| {
                const expected_probability = expected_task.object.get(label.label) orelse return error.InvalidFamilyReference;
                try std.testing.expectApproxEqAbs(expected_probability.float, @as(f64, label.confidence), 5e-4);
            }
        }

        const extraction_json = try extractionResponseJson(request_allocator, presented.classifications, prepared.input_ids.len);
        const response_json = try decide.responseJson(request_allocator, wire_request, extraction_json, .span_marker);
        const response = try std.json.parseFromSlice(std.json.Value, request_allocator, response_json, .{});
        const expected_model = reference.decide_expected.object.get("model") orelse return error.InvalidFamilyReference;
        const expected_answers = reference.decide_expected.object.get("answers") orelse return error.InvalidFamilyReference;
        try expectJsonApprox(expected_model, response.value.object.get("model") orelse return error.InvalidFamilyReference, 5e-4);
        try expectJsonApprox(expected_answers, response.value.object.get("answers") orelse return error.InvalidFamilyReference, 5e-4);
    }
    std.debug.print("GLiNER2.5-Decide-1B {s}: {d} regular + {d} public cases, max raw-logit error {e}\n", .{ if (metal) "metal" else "native", ref.requests.len, ref.public_decide_requests.len, max_error });
}

test "GLiNER2.5 Decide 1B pinned full session native classifier parity" {
    try parity(false);
}
test "GLiNER2.5 Decide 1B pinned full session Metal classifier parity" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    try parity(true);
}

const LongContextReference = struct {
    format_version: u32,
    model_sha256: []const u8,
    requests: []const struct {
        id: []const u8,
        text: []const u8,
        native_schema_json: []const u8,
        encoded: struct { input_ids: []const i64, attention_mask: []const i64 },
        native_classification: struct {
            input_ids: []const i64,
            tasks: []const struct { name: []const u8, labels: []const []const u8, raw_logits: []const f64 },
        },
        probabilities: std.json.Value,
        selected: std.json.Value,
        semantic_expected: std.json.Value,
    },
};

fn jsonNumber(value: std.json.Value) !f64 {
    return switch (value) {
        .float => |number| number,
        .integer => |number| @floatFromInt(number),
        else => error.InvalidFamilyReference,
    };
}

fn expectLongContextProbabilities(compiled: *const schema_mod.CompiledSchema, rows: []const []const f64, expected: std.json.Value) !void {
    if (expected != .object or expected.object.count() != rows.len) return error.InvalidFamilyReference;
    const structured = schema_mod.usesStructuredClassification(compiled.schema.classifications, compiled.schema.classification_constraints.roots.len > 0);
    for (compiled.schema.classifications, rows) |classification, row| {
        const task = expected.object.get(classification.task.name) orelse return error.InvalidFamilyReference;
        if (task != .object or task.object.count() != row.len) return error.InvalidFamilyReference;
        const temperature: f64 = classification.task.temperature;
        const exclusive = classification.task.min_labels == 1 and classification.task.maximum() == 1;
        const sigmoid = switch (classification.activation) {
            .sigmoid => true,
            .softmax => false,
            .auto => if (structured) !exclusive else classification.mode == .multi,
        };
        var maximum = -std.math.inf(f64);
        if (!sigmoid) {
            for (row) |logit| maximum = @max(maximum, logit / temperature);
        }
        var denominator: f64 = 0;
        if (!sigmoid) {
            for (row) |logit| denominator += @exp(logit / temperature - maximum);
        }
        for (classification.task.labels, row) |label, logit| {
            const want = try jsonNumber(task.object.get(label) orelse return error.InvalidFamilyReference);
            const got = if (sigmoid) 1.0 / (1.0 + @exp(-(logit / temperature))) else @exp(logit / temperature - maximum) / denominator;
            try std.testing.expectApproxEqAbs(want, got, 5e-4);
        }
    }
}

fn expectLongContextPresentation(classifications: []const pipeline.Classification, probabilities: std.json.Value, selected: std.json.Value) !void {
    if (probabilities != .object or probabilities.object.count() != classifications.len or selected != .object or selected.object.count() != classifications.len)
        return error.InvalidFamilyReference;
    for (classifications) |classification| {
        const task_probabilities = probabilities.object.get(classification.name) orelse return error.InvalidFamilyReference;
        const task_selected = selected.object.get(classification.name) orelse return error.InvalidFamilyReference;
        if (task_probabilities != .object or task_probabilities.object.count() != classification.labels.len or task_selected != .array or task_selected.array.items.len != 1)
            return error.InvalidFamilyReference;
        if (task_selected.array.items[0] != .string or classification.labels.len == 0) return error.InvalidFamilyReference;
        var best = classification.labels[0];
        for (classification.labels) |label| {
            const want = try jsonNumber(task_probabilities.object.get(label.label) orelse return error.InvalidFamilyReference);
            try std.testing.expectApproxEqAbs(want, @as(f64, label.confidence), 5e-4);
            if (label.confidence > best.confidence) best = label;
        }
        try std.testing.expectEqualStrings(task_selected.array.items[0].string, best.label);
    }
}

fn semanticTargetMatch(classifications: []const pipeline.Classification, expected: std.json.Value) !bool {
    if (expected != .object) return error.InvalidFamilyReference;
    var matched = true;
    var it = expected.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .string) return error.InvalidFamilyReference;
        const task = for (classifications) |classification| {
            if (std.mem.eql(u8, classification.name, entry.key_ptr.*)) break classification;
        } else {
            matched = false;
            continue;
        };
        if (task.labels.len == 0) {
            matched = false;
            continue;
        }
        var best = task.labels[0];
        for (task.labels[1..]) |label| if (label.confidence > best.confidence) {
            best = label;
        };
        matched = matched and std.mem.eql(u8, best.label, entry.value_ptr.string);
    }
    return matched;
}

fn longContextParity(metal: bool) !void {
    const directory = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR") orelse return error.SkipZigTest;
    const reference_path = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_LONG_CONTEXT_REFERENCE") orelse return error.SkipZigTest;
    if (metal and !@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = try @import("../util/c_file.zig").readFileMax(a, reference_path, 32 * 1024 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(LongContextReference, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const ref = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), ref.format_version);
    try std.testing.expectEqualStrings(files.@"model.safetensors".sha256, ref.model_sha256);
    try service.verifyFiles(a, directory, files);

    const session = if (metal) try factory.createMetalSession(a, directory) else try factory.createNativeSession(a, directory);
    defer session.close();
    const config = try factory.getGlinerSpanConfig(session);
    if (config != .modern_bert or config.modern_bert.max_position_embeddings != 7999) return error.InvalidFamilyReference;
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tok.tokenizer().deinitTokenizer();
    const watchdog = if (metal) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);

    const case_filter = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_LONG_CONTEXT_CASE");
    const max_tokens = platform.env.getenvUsize("ANTFLY_GLINER25_DECIDE_1B_LONG_CONTEXT_MAX_TOKENS");
    const tolerance: f64 = if (metal) 5e-3 else 2e-3;
    var matching_case = case_filter == null;
    var ran: usize = 0;
    for (ref.requests) |request| {
        if (case_filter) |filter| {
            if (!std.mem.eql(u8, request.id, filter)) continue;
            matching_case = true;
        }
        if (max_tokens) |limit| if (request.encoded.input_ids.len > limit) continue;
        if (request.encoded.input_ids.len <= 198 or request.encoded.input_ids.len > 7999) return error.InvalidFamilyReference;
        const qualified_backend: qualification.Backend = if (metal) .metal else .native;
        try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, qualification.requireRequest(.decide_1b, qualified_backend, 1, 1, request.encoded.input_ids.len));
        errdefer std.debug.print("Decide-1B long-context {s}/{s}\n", .{ if (metal) "metal" else "native", request.id });
        const control = @import("../execution_control.zig").InferenceExecutionControl{
            .hard_cancellation = if (watchdog) |owner| owner.boundary() else null,
            .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s,
        };
        var compiled = try schema_mod.compile(a, request.native_schema_json, .{});
        defer compiled.deinit();
        var prepared = try processor.prepare(a, tok.tokenizer(), &.{.{ .text = request.text, .schema = &compiled }}, .{
            .max_text_words = 7999,
            .max_total_words = 7999,
            .max_sequence_tokens = 7999,
            .max_batch_tokens = 7999,
            .control = control,
        });
        defer prepared.deinit();
        try std.testing.expectEqualSlices(i64, request.encoded.input_ids, prepared.input_ids);
        try std.testing.expectEqualSlices(i64, request.encoded.attention_mask, prepared.attention_mask);
        try std.testing.expectEqualSlices(i64, request.native_classification.input_ids, prepared.input_ids);
        const counts = try a.alloc(usize, compiled.schema.classifications.len);
        defer a.free(counts);
        for (compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;
        const began = platform.time.monotonicNs();
        const rows = rows: {
            var managed = try factory.getManagedComputeBackend(session, a, null, control);
            defer managed.deinit();
            break :rows try executor.classificationLogits(&managed.backend, a, .{ .modern_bert = config.modern_bert }, prepared.samples[0], counts);
        };
        defer {
            for (rows) |row| a.free(row);
            a.free(rows);
        }
        var max_error: f64 = 0;
        try std.testing.expectEqual(request.native_classification.tasks.len, rows.len);
        for (request.native_classification.tasks, rows, compiled.schema.classifications) |task, got, classification| {
            try std.testing.expectEqualStrings(task.name, classification.task.name);
            try std.testing.expectEqual(task.labels.len, got.len);
            try std.testing.expectEqual(task.raw_logits.len, got.len);
            for (task.labels, task.raw_logits, got, classification.task.labels) |label, want, actual, compiled_label| {
                try std.testing.expectEqualStrings(label, compiled_label);
                max_error = @max(max_error, @abs(want - actual));
                try std.testing.expectApproxEqAbs(want, actual, tolerance);
            }
        }
        try expectLongContextProbabilities(&compiled, rows, request.probabilities);
        const const_rows = try a.alloc([]const f64, rows.len);
        defer a.free(const_rows);
        for (rows, const_rows) |row, *out| out.* = row;
        var presented = try pipeline.presentClassifications(a, &compiled, const_rows, 1, .{});
        defer presented.deinit();
        try expectLongContextPresentation(presented.classifications, request.probabilities, request.selected);
        const semantic_match = try semanticTargetMatch(presented.classifications, request.semantic_expected);
        const elapsed_ms = @as(f64, @floatFromInt(platform.time.monotonicNs() - began)) / 1e6;
        std.debug.print("GLiNER2.5-Decide-1B long-context {s}/{s}: tokens={d} elapsed_ms={d:.3} max_raw_logit_error={e} semantic_target_match={}\n", .{ if (metal) "metal" else "native", request.id, prepared.input_ids.len, elapsed_ms, max_error, semantic_match });
        ran += 1;
    }
    if (!matching_case) return error.InvalidFamilyReference;
    if (ran == 0) return error.SkipZigTest;
}

test "GLiNER2.5 Decide 1B long context native oracle parity" {
    try longContextParity(false);
}
test "GLiNER2.5 Decide 1B long context Metal oracle parity" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    try longContextParity(true);
}
