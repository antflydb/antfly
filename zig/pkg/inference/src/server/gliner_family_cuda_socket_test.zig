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

//! Family CUDA serving correctness over real loopback sockets. Diagnostic
//! tests can exercise unpublished profiles; production tests retain artifact,
//! backend, precision and geometry qualification without test-only bypasses.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const Node = @import("server.zig").Node;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const Io = std.Io;
const factory = @import("../architectures/session_factory.zig");

pub fn queuedCancellation(node: *Node, transport: *Loopback, directory: []const u8, path: []const u8, body: []const u8, expected: []const u8) !void {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const loaded = handle.get();
    const boundary = loaded.manifest.gliner_architecture == .boundary or loaded.manifest.gliner_specialty_profile != null;
    const queue = if (boundary) &loaded.gliner_boundary_admission_lock else loaded.targetInferenceExecutionMutex() orelse return error.MissingCudaExecutionMutex;
    try std.testing.expect(queue.tryLock());
    var held = true;
    defer if (held) queue.unlock();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    const expected_handles = loaded.active_handles + 1;
    const before = factory.getCudaRuntimeStats(loaded.session).?;
    var peer = try transport.connect();
    var open = true;
    defer if (open) peer.close();
    try peer.setSendTimeout(5_000);
    var buffer: [256]u8 = undefined;
    const header = try std.fmt.bufPrint(&buffer, "POST {s} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{ path, body.len });
    try peer.sendAll(header);
    try peer.sendAll(body);
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    const control = @import("../execution_control.zig").InferenceExecutionControl{ .io = std.testing.io, .deadline_ns = deadline };
    while (true) {
        var handles: usize = undefined;
        {
            try control.lock(&node.model_manager.load_lock);
            defer node.model_manager.load_lock.unlock();
            handles = loaded.active_handles;
        }
        if (handles == expected_handles) break;
        if (platform.time.monotonicNs() >= deadline) return error.QueueAdmissionTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const waiting = domain.admission.snapshot();
    if (boundary) {
        try std.testing.expectEqual(retained.backend_scratch_bytes, waiting.backend_scratch_bytes);
    } else {
        // Span requests may reserve a bounded workspace before waiting for the
        // execution mutex; no allocation or learned work may run while held.
        try std.testing.expect(waiting.backend_scratch_bytes >= retained.backend_scratch_bytes);
    }
    try std.testing.expect(waiting.host_scratch_bytes > retained.host_scratch_bytes);
    try std.testing.expectEqual(before.kernel_launches, factory.getCudaRuntimeStats(loaded.session).?.kernel_launches);
    // A hard disconnect must drain the queued request while its model lane
    // remains held. FIN alone would be a legal HTTP half-close.
    var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
    try std.posix.setsockopt(peer.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
    peer.close();
    open = false;
    try transport.idle();
    try std.testing.expectEqual(expected_handles - 1, loaded.active_handles);
    try std.testing.expectEqual(retained, domain.admission.snapshot());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(before.kernel_launches, factory.getCudaRuntimeStats(loaded.session).?.kernel_launches);
    queue.unlock();
    held = false;
    const retry = try responseAt(transport, path, body);
    defer std.testing.allocator.free(retry);
    try std.testing.expectEqualStrings(expected, retry);
    try transport.idle();
    try std.testing.expectEqual(retained, domain.admission.snapshot());
    std.debug.print("family_cuda_http: queued_disconnect_drained=true leases_restored=true no_queued_gpu_work=true retry_equal=true\n", .{});
}

fn response(transport: *Loopback, bytes: []const u8) ![]u8 {
    return responseAt(transport, "/ai/v1/decisions", bytes);
}

fn responseAt(transport: *Loopback, path: []const u8, bytes: []const u8) ![]u8 {
    var result = try transport.request(.POST, path, bytes);
    defer result.deinit();
    errdefer std.debug.print("family CUDA HTTP: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), result.status.code);
    return std.testing.allocator.dupe(u8, result.body orelse return error.MissingResponseBody);
}

const Call = struct {
    transport: *Loopback,
    request: []const u8,
    expected: []const u8,
    path: []const u8 = "/ai/v1/decisions",

    fn run(self: Call) anyerror!void {
        const actual = try responseAt(self.transport, self.path, self.request);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings(self.expected, actual);
    }
};

test "GLiNER family CUDA typed decisions through concurrent HTTP sockets" {
    try typedDecisions(false, "ANTFLY_GLINER25_FAMILY_MODEL_DIR");
}

test "GLiNER production CUDA typed decisions through concurrent HTTP sockets" {
    try typedDecisions(true, "ANTFLY_GLINER25_FAMILY_MODEL_DIR");
}

test "GLiNER production CUDA typed decisions through concurrent HTTP sockets Q8" {
    try typedDecisions(true, "ANTFLY_GLINER_DECIDE_Q8_MODEL");
}

fn typedDecisions(comptime production: bool, environment: [*:0]const u8) !void {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const source = platform.env.getenv(environment) orelse return error.SkipZigTest;
    const precision = std.meta.stringToEnum(factory.GlinerCudaPrecision, platform.env.getenv("ANTFLY_GLINER25_FAMILY_CUDA_PRECISION") orelse "fp32") orelse return error.InvalidPrecision;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const prepared_directory: ?[]u8 = if (production) try @import("../models/gliner_decide_qualification_fixture.zig").prepare(a, &temporary, source) else null;
    defer if (prepared_directory) |value| a.free(value);
    const directory = prepared_directory orelse source;
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .allow_unknown_models = true,
        .max_loaded_models = 1,
        .max_concurrent_requests = 16,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            // Sixteen request heaps reserve 512 MiB each, in addition to
            // resident model storage and the one active CUDA workspace.
            .host_limit_bytes = 14 * GiB,
            .backend_limit_bytes = 16 * GiB,
            .combined_limit_bytes = 30 * GiB,
            .scratch_limit_bytes = 12 * GiB,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.cuda};
    node.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.preferred_backends = &.{.cuda};
    node.model_manager.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.test_allow_unqualified_gliner_cuda_precision = !production;
    node.test_allow_unqualified_gliner_boundary = !production;
    try node.attachIo(std.testing.io);
    // This is a loaded-model serving check. The debug test build can spend
    // minutes validating/uploading gigabytes of weights; keep that cold load
    // outside the socket deadline and verify the selected backend explicitly.
    {
        var handle = try node.model_manager.acquireFromDirWithGlinerCudaPrecision(directory, precision);
        defer handle.release();
        try std.testing.expectEqual(.cuda, handle.get().session.backend());
        if (!production and handle.get().manifest.gliner_classification_head == .label_marker_mlp and handle.get().manifest.gliner_span_encoder_family == .modern_bert)
            try @import("model_manager.zig").qualifyGlinerEttinForTest(&node.model_manager, handle.get(), directory, precision);
    }
    std.debug.print("family_cuda_http: precision={s}\n", .{@tagName(precision)});
    const transport = try Loopback.initWithCapacity(a, &node, 16);
    defer transport.deinit();
    try transport.start();

    var requests: [16][]u8 = undefined;
    var expected: [16][]u8 = undefined;
    var prepared: usize = 0;
    defer for (0..prepared) |index| {
        a.free(requests[index]);
        a.free(expected[index]);
    };
    for (0..16) |index| {
        const state = try std.fmt.allocPrint(a, "Ticket {d}: {s}", .{ index, if (index % 2 == 0) "Please refund the duplicate payment today." else "I cannot log in and need help resetting my password." });
        defer a.free(state);
        const body = try std.json.Stringify.valueAlloc(a, .{
            .model = std.fs.path.basename(directory),
            .input = state,
            .questions = .{
                .{ .name = "intent", .type = "choice", .instructions = "What does the customer need?", .choices = .{ .{ .value = "refund", .description = "Refund a payment" }, .{ .value = "support", .description = "Technical support" } } },
                .{ .name = "urgency", .type = "score", .instructions = "How urgent is the request?", .levels = .{ .{ .label = "routine", .description = "Routine" }, .{ .label = "urgent", .description = "Urgent" } } },
                .{ .name = "act", .type = "predicate", .instructions = "Does the customer need assistance?" },
            },
        }, .{});
        errdefer a.free(body);
        if (production and index == 0) {
            const direct = node.decideDirectJsonWithControl(a, body, null) catch |err| {
                std.debug.print("production CUDA typed decision failed: {s}\n", .{@errorName(err)});
                return err;
            };
            defer a.free(direct);
            const http = try response(transport, body);
            defer a.free(http);
            try std.testing.expectEqualStrings(direct, http);
        }
        const baseline = try response(transport, body);
        requests[index] = body;
        expected[index] = baseline;
        prepared += 1;
    }
    try transport.idle();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    var driver = Io.Threaded.init(a, .{ .concurrent_limit = .limited(16) });
    defer driver.deinit();
    const io = driver.io();
    for ([_]usize{ 1, 4, 16 }) |concurrency| {
        var futures: [16]Io.Future(anyerror!void) = undefined;
        var started: usize = 0;
        // Join every client on a spawn/error path before destroying its body,
        // expected output, transport, or model owner.
        defer for (futures[0..started]) |*future| {
            future.await(io) catch {};
        };
        for (0..concurrency) |index| {
            futures[index] = try io.concurrent(Call.run, .{Call{ .transport = transport, .request = requests[index], .expected = expected[index] }});
            started += 1;
        }
        var first_error: ?anyerror = null;
        for (futures[0..started]) |*future| future.await(io) catch |err| {
            if (first_error == null) first_error = err;
        };
        started = 0;
        if (first_error) |err| return err;
        try transport.idle();
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        try std.testing.expectEqual(retained, domain.admission.snapshot());
        std.debug.print("family_cuda_http: concurrency={d} responses_equal=true leases_released=true\n", .{concurrency});
    }
    var entries = node.model_manager.loaded.valueIterator();
    const loaded = (entries.next() orelse return error.MissingLoadedModel).*;
    try std.testing.expectEqual(.cuda, loaded.session.backend());
    try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
    try queuedCancellation(&node, transport, directory, "/ai/v1/decisions", requests[0], expected[0]);
    try transport.finish();
}

fn extractionSockets(directory: []const u8, schema_json: []const u8, text: []const u8, expected_sample: @import("../pipelines/gliner_boundary_pipeline.zig").ExpectedSample) !void {
    const a = std.testing.allocator;
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .allow_unknown_models = true,
        .max_loaded_models = 1,
        .max_concurrent_requests = 16,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 14 * GiB,
            .backend_limit_bytes = 16 * GiB,
            .combined_limit_bytes = 30 * GiB,
            .scratch_limit_bytes = 12 * GiB,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.cuda};
    node.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.preferred_backends = &.{.cuda};
    node.model_manager.session_manager.required_backend = .cuda;
    try node.attachIo(std.testing.io);
    {
        var handle = try node.model_manager.acquireFromDir(directory);
        defer handle.release();
        try std.testing.expectEqual(.cuda, handle.get().session.backend());
    }
    var schema = try std.json.parseFromSlice(std.json.Value, a, schema_json, .{});
    defer schema.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = std.fs.path.basename(directory),
        .schema = schema.value,
        .inputs = .{.{ .id = "probe", .content = text }},
        .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
    }, .{});
    defer a.free(body);
    const transport = try Loopback.initWithCapacity(a, &node, 16);
    defer transport.deinit();
    try transport.start();
    const expected = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(expected);
    try @import("gliner_family_service_test.zig").expectEntities(a, expected, expected_sample);
    try transport.idle();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    var driver = Io.Threaded.init(a, .{ .concurrent_limit = .limited(16) });
    defer driver.deinit();
    const io = driver.io();
    for ([_]usize{ 1, 4, 16 }) |concurrency| {
        var futures: [16]Io.Future(anyerror!void) = undefined;
        var started: usize = 0;
        defer for (futures[0..started]) |*future| {
            future.await(io) catch {};
        };
        for (0..concurrency) |_| {
            futures[started] = try io.concurrent(Call.run, .{Call{ .transport = transport, .request = body, .expected = expected, .path = "/ai/v1/extract" }});
            started += 1;
        }
        var failure: ?anyerror = null;
        for (futures[0..started]) |*future| future.await(io) catch |err| {
            if (failure == null) failure = err;
        };
        started = 0;
        if (failure) |err| return err;
        try transport.idle();
        try std.testing.expectEqual(retained, domain.admission.snapshot());
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        std.debug.print("production_cuda_extract model={s} concurrency={d} responses_equal=true leases_released=true\n", .{ std.fs.path.basename(directory), concurrency });
    }
    {
        var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
        defer handle.release();
        const stats = factory.getCudaRuntimeStats(handle.get().session) orelse return error.MissingCudaRuntimeStats;
        try std.testing.expect(stats.kernel_launches > 0);
        try std.testing.expectEqual(@as(usize, 0), stats.to_float32_calls);
    }
    try queuedCancellation(&node, transport, directory, "/ai/v1/extract", body, expected);
    try transport.finish();
}

test "GLiNER production CUDA extraction through concurrent HTTP sockets base" {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const base_dir = platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
    const base_bytes = try fixtures.fixtureBytes(a, "pipeline_cases_base.json");
    defer a.free(base_bytes);
    var base = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, base_bytes, .{});
    defer base.deinit();
    const base_case = base.value.cases[0];
    const base_schema = try std.json.Stringify.valueAlloc(a, base_case.schema, .{});
    defer a.free(base_schema);
    try extractionSockets(base_dir, base_schema, base_case.text, base_case.expected);
}

test "GLiNER production CUDA extraction through concurrent HTTP sockets multi" {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const multi_dir = platform.env.getenv("ANTFLY_GLINER25_MULTI_V1_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
    const multi_bytes = try fixtures.fixtureBytes(a, "family/multi_v1_capture.json");
    defer a.free(multi_bytes);
    var multi = try std.json.parseFromSlice(pipeline.FamilyReference, a, multi_bytes, .{ .ignore_unknown_fields = true });
    defer multi.deinit();
    const multi_case = multi.value.requests[1];
    try std.testing.expectEqualStrings("spanish_entities", multi_case.id);
    try extractionSockets(multi_dir, multi_case.native_schema_json, multi_case.text, multi_case.native_expected);
}

pub const BatchCase = struct { text: []const u8, schema: std.json.Value };
pub const additional_multi_batch_fixtures = [_][]const u8{
    "family/multi_v1_general_classification_capture.json",
    "family/multi_v1_endpoint_capture.json",
    "family/multi_v1_clean_entity_short_capture.json",
    "family/multi_v1_classification_floor_capture.json",
    "family/multi_v1_general_entity_capture.json",
    "family/multi_v1_source_word_floor_capture.json",
};
const BatchInput = struct { id: []const u8, content: []const u8, schema: std.json.Value };

pub fn batchBody(a: std.mem.Allocator, model: []const u8, cases: []const BatchCase, n: usize, reverse: bool) ![]u8 {
    const inputs = try a.alloc(BatchInput, n);
    defer a.free(inputs);
    var built: usize = 0;
    defer for (inputs[0..built]) |input| a.free(input.id);
    for (inputs, 0..) |*input, i| {
        const index = (if (reverse) n - 1 - i else i) % cases.len;
        input.* = .{ .id = try std.fmt.allocPrint(a, "row-{d}", .{i}), .content = cases[index].text, .schema = cases[index].schema };
        built += 1;
    }
    return std.json.Stringify.valueAlloc(a, .{ .schema_version = 2, .model = model, .schema = cases[0].schema, .inputs = inputs, .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" } }, .{});
}

pub fn equalBatchValue(want: std.json.Value, got: std.json.Value, tolerance: f64) anyerror!void {
    try std.testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        .object => |object| {
            try std.testing.expectEqual(object.count(), got.object.count());
            var it = object.iterator();
            while (it.next()) |entry| {
                // Row IDs are separately checked against the batch's order.
                if (std.mem.eql(u8, entry.key_ptr.*, "id")) continue;
                try equalBatchValue(entry.value_ptr.*, got.object.get(entry.key_ptr.*) orelse return error.MissingBatchField, tolerance);
            }
        },
        .array => |array| {
            try std.testing.expectEqual(array.items.len, got.array.items.len);
            for (array.items, got.array.items) |left, right| try equalBatchValue(left, right, tolerance);
        },
        .float => |v| try std.testing.expectApproxEqAbs(v, got.float, tolerance),
        .integer => |v| try std.testing.expectEqual(v, got.integer),
        .string, .number_string => |v| try std.testing.expectEqualStrings(v, if (got == .string) got.string else got.number_string),
        .bool => |v| try std.testing.expectEqual(v, got.bool),
        .null => {},
    }
}

fn batchPool(node: *Node, transport: *Loopback, directory: []const u8, cases: []const BatchCase, decide: bool) !void {
    const a = std.testing.allocator;
    const baselines = try a.alloc([]u8, cases.len);
    defer a.free(baselines);
    const singleton_launches = try a.alloc(usize, cases.len);
    defer a.free(singleton_launches);
    var built: usize = 0;
    defer for (baselines[0..built]) |bytes| a.free(bytes);
    for (cases, baselines) |case, *baseline| {
        const body = try batchBody(a, std.fs.path.basename(directory), &.{case}, 1, false);
        defer a.free(body);
        const before = if (node.model_manager.acquireLoadedModel(directory)) |owned| stats: {
            var temporary_handle = owned;
            defer temporary_handle.release();
            break :stats factory.getCudaRuntimeStats(temporary_handle.get().session).?.kernel_launches;
        } else 0;
        baseline.* = try responseAt(transport, "/ai/v1/extract", body);
        var loaded = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
        singleton_launches[built] = factory.getCudaRuntimeStats(loaded.get().session).?.kernel_launches - before;
        loaded.release();
        built += 1;
    }
    try transport.idle();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    for ([_]usize{ 2, 4, 8 }) |n| {
        for ([_]bool{ false, true }) |reverse| {
            const body = try batchBody(a, std.fs.path.basename(directory), cases, n, reverse);
            defer a.free(body);
            const before = factory.getCudaRuntimeStats(handle.get().session).?;
            const actual = try responseAt(transport, "/ai/v1/extract", body);
            defer a.free(actual);
            const after = factory.getCudaRuntimeStats(handle.get().session).?;
            try std.testing.expect(after.kernel_launches > before.kernel_launches);
            var serial_launches: usize = 0;
            for (0..n) |i| serial_launches += singleton_launches[(if (reverse) n - 1 - i else i) % cases.len];
            try std.testing.expect(after.kernel_launches - before.kernel_launches < serial_launches);
            // Decide has exactly one final logits readback for the whole
            // physical batch, not one readback/encoder forward per input.
            if (decide) try std.testing.expectEqual(before.to_float32_calls + 1, after.to_float32_calls) else try std.testing.expectEqual(before.to_float32_calls, after.to_float32_calls);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, actual, .{});
            defer parsed.deinit();
            const rows = parsed.value.object.get("data").?.array.items;
            try std.testing.expectEqual(n, rows.len);
            for (rows, 0..) |row, i| {
                const index = (if (reverse) n - 1 - i else i) % cases.len;
                var expected = try std.json.parseFromSlice(std.json.Value, a, baselines[index], .{});
                defer expected.deinit();
                var id: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "row-{d}", .{i}), row.object.get("id").?.string);
                try equalBatchValue(expected.value.object.get("data").?.array.items[0], row, if (decide) 5e-3 else 5e-4);
            }
            try transport.idle();
            try std.testing.expectEqual(retained, domain.admission.snapshot());
            try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
            std.debug.print("production_cuda_batch model={s} items={d} reverse={} launches={d} readbacks={d} parity=true leases_restored=true\n", .{ std.fs.path.basename(directory), n, reverse, after.kernel_launches - before.kernel_launches, after.to_float32_calls - before.to_float32_calls });
        }
    }
    // Explicit over-limit batches must fail before CUDA work and preserve leases.
    const oversized = try batchBody(a, std.fs.path.basename(directory), cases, 9, false);
    defer a.free(oversized);
    const before = factory.getCudaRuntimeStats(handle.get().session).?;
    var rejected = try transport.request(.POST, "/ai/v1/extract", oversized);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 413), rejected.status.code);
    try std.testing.expectEqual(before.kernel_launches, factory.getCudaRuntimeStats(handle.get().session).?.kernel_launches);
    try transport.idle();
    try std.testing.expectEqual(retained, domain.admission.snapshot());
}

fn additionalMultiBatchProfiles(node: *Node, transport: *Loopback, directory: []const u8, intents: []const BatchCase) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    for (additional_multi_batch_fixtures, 0..) |file, file_index| {
        std.debug.print("production_cuda_batch_profile fixture={s}\n", .{file});
        const bytes = try fixtures.fixtureBytes(a, file);
        defer a.free(bytes);
        var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer fixture.deinit();
        const values = fixture.value.object.get("requests").?.array.items;
        var cases: [16]BatchCase = undefined;
        var schemas: [16]std.json.Parsed(std.json.Value) = undefined;
        var built: usize = 0;
        defer for (schemas[0..built]) |*schema| schema.deinit();
        for (values, 0..) |value, i| {
            schemas[i] = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            built += 1;
            cases[i] = .{ .text = value.object.get("text").?.string, .schema = schemas[i].value };
        }
        switch (file_index) {
            0 => {
                var bare: [5]BatchCase = undefined;
                @memcpy(bare[0..4], intents);
                bare[4] = cases[0];
                try batchPool(node, transport, directory, &bare, false);
                for ([_]usize{ 1, 3, 5 }) |i| try batchPool(node, transport, directory, cases[i..][0..2], false);
            },
            1 => try batchPool(node, transport, directory, cases[0..values.len], false),
            4 => {
                for ([_]usize{ 0, 2 }) |i| {
                    // The one-byte "a" capture is a numerical diagnostic, not
                    // a serving row. Preserve that rejection; mix the measured
                    // clean two-byte floor with the realistic long capture.
                    const invalid = try batchBody(a, std.fs.path.basename(directory), cases[i..][0..1], 2, false);
                    defer a.free(invalid);
                    var rejected = try transport.request(.POST, "/ai/v1/extract", invalid);
                    defer rejected.deinit();
                    try std.testing.expectEqual(@as(u16, 413), rejected.status.code);
                    var cohort = [_]BatchCase{ cases[i], cases[i + 1] };
                    cohort[0].text = "a.";
                    try batchPool(node, transport, directory, &cohort, false);
                }
            },
            else => for (0..values.len) |i| try batchPool(node, transport, directory, cases[i..][0..1], false),
        }
    }
}

fn distinctInputBatches(environment: [*:0]const u8, comptime role: enum { base, multi, decide }) !void {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    const source = platform.env.getenv(environment) orelse return error.SkipZigTest;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const published: ?[]u8 = if (role == .decide) try @import("../models/gliner_decide_qualification_fixture.zig").prepare(a, &temporary, source) else null;
    defer if (published) |path| a.free(path);
    const directory = published orelse source;
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 16,
        .process_termination_available = true,
        .process_memory_limit_bytes = 40 * GiB,
        .process_memory_limit_provenance = .explicit,
        .generation_budget_overrides = .{ .host_limit_bytes = 20 * GiB, .backend_limit_bytes = 24 * GiB, .combined_limit_bytes = 32 * GiB, .scratch_limit_bytes = 16 * GiB },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.cuda};
    node.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.preferred_backends = &.{.cuda};
    node.model_manager.session_manager.required_backend = .cuda;
    try node.attachIo(std.testing.io);
    const transport = try Loopback.initWithCapacity(a, &node, 16);
    defer transport.deinit();
    try transport.start();
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    const bytes = try fixtures.fixtureBytes(a, if (role == .base) "pipeline_cases_base.json" else if (role == .multi) "family/multi_v1_capture.json" else "decide/cases.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer fixture.deinit();
    const values = fixture.value.object.get(if (role == .base) "cases" else if (role == .multi) "requests" else "classification").?.array.items;
    var cases: [16]BatchCase = undefined;
    var schemas: [16]std.json.Parsed(std.json.Value) = undefined;
    var schemas_built: usize = 0;
    defer for (schemas[0..schemas_built]) |*schema| schema.deinit();
    for (values, 0..) |value, i| {
        const schema = if (role == .multi) parsed: {
            schemas[i] = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            schemas_built += 1;
            break :parsed schemas[i].value;
        } else value.object.get(if (role == .base) "schema" else "v2_schema").?;
        cases[i] = .{ .text = value.object.get("text").?.string, .schema = schema };
    }
    if (role == .multi) {
        // Distinct measured profiles stay separate, with mixed languages and
        // per-item label schemas inside the entity and intent cohorts.
        try batchPool(&node, transport, directory, cases[1..4], false);
        try batchPool(&node, transport, directory, cases[4..8], false);
        for ([_]usize{ 0, 8, 9, 10 }) |i| try batchPool(&node, transport, directory, cases[i..][0..1], false);
        try additionalMultiBatchProfiles(&node, transport, directory, cases[4..8]);
    } else if (role == .base) {
        // The source-default JointIE beam profile remains independently gated;
        // the existing base public rows cover the other nine frozen cases.
        try batchPool(&node, transport, directory, cases[0..8], false);
        try batchPool(&node, transport, directory, cases[7..9], false);
    } else {
        try batchPool(&node, transport, directory, cases[0..values.len], true);
    }
    // A queued eight-item batch must cancel without launches and recover.
    const selected = if (role == .multi) cases[4..8] else if (role == .base) cases[0..8] else cases[0..values.len];
    const body = try batchBody(a, std.fs.path.basename(directory), selected, 8, false);
    defer a.free(body);
    const expected = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(expected);
    try queuedCancellation(&node, transport, directory, "/ai/v1/extract", body, expected);
    try transport.finish();
}

test "GLiNER production CUDA distinct input batches base" {
    try distinctInputBatches("ANTFLY_GLINER25_BASE_MODEL_DIR", .base);
}
test "GLiNER production CUDA distinct input batches multi" {
    try distinctInputBatches("ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", .multi);
}
test "GLiNER production CUDA distinct input batches Decide FP32" {
    try distinctInputBatches("ANTFLY_GLINER_DECIDE_MODEL", .decide);
}
test "GLiNER production CUDA distinct input batches Decide Q8" {
    try distinctInputBatches("ANTFLY_GLINER_DECIDE_Q8_MODEL", .decide);
}
