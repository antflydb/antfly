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

//! Real-session proof for the bounded window composition, including mixed
//! descriptions, relations, raw-logit aggregation and source-offset rebasing.
const std = @import("std");
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const wire = @import("extraction_v2.zig");
const span = @import("gliner_span_v2_executor.zig");
const long = @import("gliner_boundary_long_executor.zig");
const Capture = struct {
    variant: []const u8,
    cases: []const struct {
        id: []const u8,
        text: []const u8,
        native_schema_json: []const u8,
        window_words: usize,
        classification_logits: []const f64,
        windows: []const struct { text: []const u8, input_ids: []const i64 },
        expected: pipeline.ExpectedSample,
    },
};

fn parity(comptime backend: enum { native, metal, cuda }) !void {
    const accelerated = backend != .native;
    const directory = platform.env.getenv("ANTFLY_GLINER_WINDOW_MODEL_DIR") orelse if (backend == .cuda and platform.env.getenvBool("ANTFLY_GLINER25_REQUIRE_PRODUCTION_MODELS"))
        platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.MissingProductionQualificationModel
    else
        return error.SkipZigTest;
    const capture_path = platform.env.getenv("ANTFLY_GLINER_WINDOW_CAPTURE");
    const variant = platform.env.getenv("ANTFLY_GLINER_WINDOW_VARIANT") orelse "base";
    const fixtures = @import("gliner_graph_qualification_fixtures.zig");
    const pinned = if (std.mem.eql(u8, variant, "base")) fixtures.base_windows else if (std.mem.eql(u8, variant, "multi_decide")) fixtures.multi_decide_windows else if (std.mem.eql(u8, variant, "decide_1b")) fixtures.decide_1b_windows else return error.InvalidFixtureVariant;
    const a = std.testing.allocator;
    const bytes = if (capture_path) |path| try @import("../util/c_file.zig").readFileMax(a, path, 16 * 1024 * 1024) else try a.dupe(u8, pinned);
    defer a.free(bytes);
    const capture = try std.json.parseFromSlice(Capture, a, bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    const modern = std.mem.eql(u8, capture.value.variant, "decide_1b");
    const session = switch (backend) {
        .native => try factory.createNativeSession(a, directory),
        .metal => try factory.createMetalSession(a, directory),
        .cuda => try factory.createCudaSession(a, directory),
    };
    defer session.close();
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFileMax(a, tokenizer_path, 32 * 1024 * 1024);
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    if (modern) {
        const qualification = @import("../models/gliner_decide_qualification.zig");
        try factory.sealGlinerDecisionTokenizerDigest(session, qualification.Digest.of(tokenizer_bytes));
        _ = try qualification.require(try factory.getGlinerDecisionIdentity(session));
    }

    const watchdog: ?*@import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog = if (accelerated) try @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(std.testing.io);
    const control = @import("../execution_control.zig").InferenceExecutionControl{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s };
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    for (capture.value.cases) |case| {
        errdefer std.debug.print("window parity {s} {s}/{s}\n", .{ @tagName(backend), capture.value.variant, case.id });
        const schema = try std.json.parseFromSlice(std.json.Value, a, case.native_schema_json, .{});
        defer schema.deinit();
        const body = try std.json.Stringify.valueAlloc(a, .{ .schema_version = @as(u32, 2), .model = "window", .schema = schema.value, .options = .{ .offset_unit = "unicode_codepoints", .long_document = .{ .mode = "window", .window_words = case.window_words, .overlap_words = @as(usize, 16) } }, .inputs = .{.{ .content = case.text }} }, .{});
        defer a.free(body);
        var request = try wire.parseJson(a, body, .{});
        defer request.deinit();
        const item = &request.items[0];
        for (case.windows) |window| {
            var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = window.text, .schema = &item.compiled }}, .{ .max_sequence_tokens = 1024, .max_batch_tokens = 1024 });
            defer prepared.deinit();
            try std.testing.expectEqualSlices(i64, window.input_ids, prepared.input_ids);
        }
        if (modern) {
            const config = (try factory.getGlinerSpanConfig(session)).modern_bert;
            var prepared = try span.prepareItem(a, tokenizer.tokenizer(), item, .{ .max_sequence_tokens = 1024, .max_batch_tokens = 1024 }, .{ .modern_span_extraction = true, .control = control });
            defer prepared.deinit(a);
            try std.testing.expectEqual(case.windows.len, prepared.batches.len);
            for (case.windows, prepared.batches) |window, batch| try std.testing.expectEqualSlices(i64, window.input_ids, batch.input_ids);
            var logits = try span.preparedItemLogits(&managed.backend, a, .{ .modern_bert = config }, item, &prepared, null);
            defer logits.deinit(a);
            for (case.classification_logits, logits.rows[0]) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-3);
            var result = try @import("gliner_span_long_executor.zig").extract(&managed.backend, a, config, item, &prepared, item.options.native(.{ .control = control }));
            defer result.deinit();
            try pipeline.expectSample(case.expected, result.samples[0], pipeline.fp32_confidence_tolerance);
        } else {
            const config = try factory.getGlinerBoundaryConfig(session);
            const group_sizes: []const usize = if (backend == .cuda) &.{ 1, 2, 4 } else &.{1};
            for (group_sizes) |group_size| {
                const options = long.Options{ .identity = try factory.getGlinerBoundaryIdentity(session), .control = control, .pipeline = item.options.native(.{ .control = control }), .limits = .{ .window_batch_size = group_size } };
                var gate = try @import("gliner_boundary_qualification.zig").Gate.init(options.identity, switch (backend) {
                    .native => .native,
                    .metal => .metal,
                    .cuda => .cuda,
                }, &request, control);
                var result = try long.executeQualified(&managed.backend, a, &config, tokenizer.tokenizer(), item, options, &gate);
                defer result.deinit();
                try std.testing.expectEqual(case.windows.len, result.window_count);
                try pipeline.expectSample(case.expected, result.sample, pipeline.fp32_confidence_tolerance);
            }
        }
    }
    if (backend == .cuda) {
        // Grepstein uses plain entity/relation labels, without descriptions or
        // classifications. It must still fit complete prompts to the qualified
        // token context rather than treating source words as model tokens.
        const base_case = capture.value.cases[capture.value.cases.len - 1];
        const text = try std.fmt.allocPrint(a, "{s} {s} {s} {s} {s} {s}", .{ base_case.text, base_case.text, base_case.text, base_case.text, base_case.text, base_case.text });
        defer a.free(text);
        const body = try std.json.Stringify.valueAlloc(a, .{
            .schema_version = @as(u32, 2),
            .model = "window",
            .schema = .{ .entities = .{ "person", "organization" }, .relations = .{.{ .type = "works_for" }} },
            .options = .{ .long_document = .{ .mode = "window", .window_words = @as(usize, 1024), .overlap_words = @as(usize, 16) } },
            .inputs = .{.{ .content = text }},
        }, .{});
        defer a.free(body);
        var request = try wire.parseJson(a, body, .{});
        defer request.deinit();
        const item = &request.items[0];
        const options = long.Options{ .identity = try factory.getGlinerBoundaryIdentity(session), .control = control, .pipeline = item.options.native(.{ .control = control }), .limits = .{ .window_batch_size = 1 } };
        var gate = try @import("gliner_boundary_qualification.zig").Gate.init(options.identity, .cuda, &request, control);
        const config = try factory.getGlinerBoundaryConfig(session);
        var serial = try long.executeQualified(&managed.backend, a, &config, tokenizer.tokenizer(), item, options, &gate);
        defer serial.deinit();
        try std.testing.expect(serial.window_count > 4);
        try std.testing.expect(serial.prompt_tokens <= 1024 * serial.window_count);
        const expected_json = try std.json.Stringify.valueAlloc(a, serial.sample, .{});
        defer a.free(expected_json);
        const expected = try std.json.parseFromSlice(pipeline.ExpectedSample, a, expected_json, .{ .ignore_unknown_fields = true });
        defer expected.deinit();
        for ([_]usize{ 2, 4 }) |group_size| {
            var grouped_options = options;
            grouped_options.limits.window_batch_size = group_size;
            var grouped_gate = try @import("gliner_boundary_qualification.zig").Gate.init(options.identity, .cuda, &request, control);
            var grouped = try long.executeQualified(&managed.backend, a, &config, tokenizer.tokenizer(), item, grouped_options, &grouped_gate);
            defer grouped.deinit();
            try std.testing.expectEqual(serial.window_count, grouped.window_count);
            try pipeline.expectSample(expected.value, grouped.sample, pipeline.fp32_confidence_tolerance);
        }
        // Inject one backing allocation failure inside the real CUDA encode
        // phase. Successful completion must retry a smaller group and preserve
        // the complete serial result; model/backend execution is not mocked.
        var failure = EncodeAllocationFailure{ .allocator = std.testing.FailingAllocator.init(a, .{}) };
        var retry_options = options;
        retry_options.limits.window_batch_size = 4;
        retry_options.observer = .{ .context = &failure, .emit_fn = EncodeAllocationFailure.observe };
        var retry_gate = try @import("gliner_boundary_qualification.zig").Gate.init(options.identity, .cuda, &request, control);
        var recovered = try long.executeQualified(&managed.backend, failure.allocator.allocator(), &config, tokenizer.tokenizer(), item, retry_options, &retry_gate);
        defer recovered.deinit();
        try std.testing.expect(failure.allocator.has_induced_failure);
        try std.testing.expect(failure.attempts > (serial.window_count + 3) / 4);
        try std.testing.expectEqual(serial.window_count, recovered.window_count);
        try pipeline.expectSample(expected.value, recovered.sample, pipeline.fp32_confidence_tolerance);
    }
}

test "GLiNER bounded window composition pinned native" {
    try parity(.native);
}
test "GLiNER bounded window composition pinned Metal" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    try parity(.metal);
}

test "GLiNER bounded window composition pinned CUDA" {
    if (!@import("build_options").enable_cuda) return error.SkipZigTest;
    try parity(.cuda);
}

const EncodeAllocationFailure = struct {
    allocator: std.testing.FailingAllocator,
    armed: bool = false,
    attempts: usize = 0,
    fn observe(raw: ?*anyopaque, event: @import("extraction_observer.zig").Event) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (event != .phase) return;
        if (event.phase == .tokenizing and self.allocator.has_induced_failure)
            self.allocator.fail_index = std.math.maxInt(usize);
        if (event.phase == .execution) {
            self.attempts += 1;
            if (!self.armed) {
                // The schema pointer array precedes encodeGroup's allocations.
                self.allocator.fail_index = self.allocator.alloc_index + 1;
                self.armed = true;
            }
        }
    }
};
