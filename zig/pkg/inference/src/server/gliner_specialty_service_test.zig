// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in original FP32 specialty checkpoints through real public HTTP routes.
const std = @import("std");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const factory = @import("../architectures/session_factory.zig");
const qualification = @import("../models/gliner_specialty_qualification.zig");
const Reference = @import("../extractors/gliner_specialty_parity_test.zig").Reference;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const wire = @import("../extractors/extraction_v2.zig");
const executor = @import("../extractors/gliner_span_v2_executor.zig");
const Backend = @import("../backends/backends.zig").BackendType;
const a = std.testing.allocator;

fn body(model: []const u8, case: anytype, count: usize) ![]u8 {
    const Input = struct { id: []const u8, content: []const u8 };
    const inputs = try a.alloc(Input, count);
    defer a.free(inputs);
    for (inputs) |*input| input.* = .{ .id = case.id, .content = case.text };
    const schema = try std.json.parseFromSlice(std.json.Value, a, case.native_schema_json, .{});
    defer schema.deinit();
    return std.json.Stringify.valueAlloc(a, .{
        .model = model,
        .schema_version = 2,
        .schema = schema.value,
        .inputs = inputs,
        .options = .{ .threshold = case.threshold, .offset_unit = "unicode_codepoints", .include_spans = true, .include_confidence = true },
    }, .{});
}

fn number(value: std.json.Value) !f64 {
    return switch (value) {
        .float => |x| x,
        .integer => |x| @floatFromInt(x),
        else => error.InvalidNumber,
    };
}

fn raggedBody(model: []const u8, cases: anytype) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    const Input = struct { id: []const u8, content: []const u8, schema: std.json.Value, options: struct {
        threshold: f32,
        offset_unit: []const u8 = "unicode_codepoints",
        include_spans: bool = true,
        include_confidence: bool = true,
    } };
    const inputs = try temporary.alloc(Input, cases.len);
    for (cases, inputs) |case, *input| input.* = .{
        .id = case.id,
        .content = case.text,
        .schema = try std.json.parseFromSliceLeaky(std.json.Value, temporary, case.native_schema_json, .{}),
        .options = .{ .threshold = case.threshold },
    };
    return std.json.Stringify.valueAlloc(a, .{
        .model = model,
        .schema_version = 2,
        .inputs = inputs,
        .schema = inputs[0].schema,
        .options = .{ .offset_unit = "unicode_codepoints", .include_spans = true, .include_confidence = true },
    }, .{});
}

fn checkSample(profile: qualification.Profile, case: anytype, sample: std.json.Value) !void {
    try std.testing.expectEqualStrings(case.id, sample.object.get("id").?.string);
    try std.testing.expectEqualStrings("unicode_codepoints", sample.object.get("offset_unit").?.string);
    const rows = sample.object.get(if (profile == .pii) "entities" else "classifications") orelse return error.MissingResults;
    if (profile == .pii) {
        try std.testing.expectEqual(case.expected_entities.len, rows.array.items.len);
        for (case.expected_entities, rows.array.items) |expected, actual| {
            try std.testing.expectEqualStrings(expected.label, actual.object.get("label").?.string);
            try std.testing.expectEqualStrings(expected.text, actual.object.get("text").?.string);
            try std.testing.expectEqual(@as(i64, @intCast(expected.start)), actual.object.get("start").?.integer);
            try std.testing.expectEqual(@as(i64, @intCast(expected.end)), actual.object.get("end").?.integer);
            try std.testing.expectApproxEqAbs(@as(f64, expected.score), try number(actual.object.get("score").?), pipeline.fp32_confidence_tolerance);
        }
    } else {
        try std.testing.expectEqual(case.expected_classifications.len, rows.array.items.len);
        for (case.expected_classifications, rows.array.items) |expected, actual| {
            try std.testing.expectEqualStrings(expected.name, actual.object.get("name").?.string);
            try std.testing.expectEqualStrings(expected.label, actual.object.get("label").?.string);
            try std.testing.expectApproxEqAbs(@as(f64, expected.score), try number(actual.object.get("score").?), pipeline.fp32_confidence_tolerance);
        }
    }
}

fn timedResponse(transport: *Loopback, bytes: []const u8, profile: qualification.Profile, case: anytype, count: usize) !u64 {
    const start = platform.time.monotonicNs();
    var response = try transport.post(bytes);
    const elapsed = platform.time.monotonicNs() - start;
    defer response.deinit();
    errdefer std.debug.print("specialty HTTP {s}: status={d} body={s}\n", .{ case.id, response.status.code, response.body orelse "<missing>" });
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, response.body orelse return error.MissingResponse, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("schema_version").?.integer);
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(count, data.len);
    for (data) |sample| try checkSample(profile, case, sample);
    return elapsed;
}

fn checkResponse(transport: *Loopback, bytes: []const u8, profile: qualification.Profile, case: anytype, count: usize) !void {
    _ = try timedResponse(transport, bytes, profile, case, count);
}

fn expectRejected(transport: *Loopback, bytes: []const u8, status: u16) !void {
    var response = try transport.post(bytes);
    defer response.deinit();
    errdefer std.debug.print("specialty rejection status={d} body={s}\n", .{ response.status.code, response.body orelse "<missing>" });
    try std.testing.expectEqual(status, response.status.code);
}

fn reservation(loaded: *server.model_manager.LoadedModel, bytes: []const u8, profile: qualification.Profile) !usize {
    var request = try wire.parseJson(a, bytes, .{});
    defer request.deinit();
    const options = executor.Options{ .specialty_profile = profile, .classification_batch_items = if (profile == .gliguard) 8 else 1 };
    var planned = try executor.plan(a, loaded.getTokenizer(), &request, options);
    defer planned.deinit(a);
    const config = (try factory.getGlinerSpanConfig(loaded.session)).deberta;
    return executor.plannedDeviceScratchUpperBound(.{ .deberta = config }, &request, &planned, options);
}

fn qualify(comptime profile: qualification.Profile, comptime backend: Backend) !void {
    if (backend == .cuda and !@import("build_options").enable_cuda) return error.SkipZigTest;
    if (backend == .metal and !@import("build_options").enable_metal) return error.SkipZigTest;
    const directory = platform.env.getenv(if (profile == .pii) "ANTFLY_GLINER_PII_MODEL_DIR" else "ANTFLY_GLIGUARD_MODEL_DIR") orelse return error.SkipZigTest;
    const capture = platform.env.getenv(if (profile == .pii) "ANTFLY_GLINER_PII_CAPTURE" else "ANTFLY_GLIGUARD_CAPTURE") orelse return error.SkipZigTest;
    const bytes = try @import("../util/c_file.zig").readFileMax(a, capture, 64 * 1024 * 1024);
    defer a.free(bytes);
    try @import("../extractors/gliner_specialty_parity_test.zig").verifyCapture(profile, bytes);
    const reference = try std.json.parseFromSlice(Reference, a, bytes, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expect(reference.value.cases.len > 0);
    const GiB = 1024 * 1024 * 1024;
    var node = try server.Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 8,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 12 * GiB,
            .backend_limit_bytes = 12 * GiB,
            .combined_limit_bytes = 24 * GiB,
            .scratch_limit_bytes = 8 * GiB,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{backend};
    node.session_manager.required_backend = backend;
    node.model_manager.session_manager.preferred_backends = &.{backend};
    node.model_manager.session_manager.required_backend = backend;
    try node.attachIo(std.testing.io);
    var handle = try node.model_manager.acquireFromDir(directory);
    var handle_active = true;
    defer if (handle_active) handle.release();
    const loaded = handle.get();
    try std.testing.expectEqual(backend, loaded.session.backend());
    try std.testing.expectEqual(profile, try qualification.require(try factory.getGlinerDecisionIdentity(loaded.session)));
    const initial_stats = factory.getCudaRuntimeStats(loaded.session);
    const initial_memory = factory.getCudaAllocationStats(loaded.session);
    var max_reservation: usize = 0;
    // Eight concurrent metadata reads include the original 16MB tokenizer.
    // The 128x512-token request also exceeds the older fixture's 64KiB body.
    const transport = try Loopback.initWithLimits(a, &node, 8, .{
        .transport_memory_limit_bytes = 256 * 1024 * 1024,
        .max_request_body_bytes = 1024 * 1024,
    });
    defer transport.deinit();
    try transport.start();
    const model = std.fs.path.basename(directory);
    var elapsed_ns: u64 = 0;
    const CaseTiming = struct { id: []const u8, prepared_tokens: usize, samples_ns: [5]u64, h2d_bytes: usize, d2h_bytes: usize, allocations: usize };
    const timings = try a.alloc(CaseTiming, reference.value.cases.len);
    defer a.free(timings);
    for (reference.value.cases, timings) |case, *timing| {
        const request = try body(model, case, 1);
        defer a.free(request);
        max_reservation = @max(max_reservation, try reservation(loaded, request, profile));
        try checkResponse(transport, request, profile, case, 1);
        const before = factory.getCudaRuntimeStats(loaded.session);
        timing.* = .{ .id = case.id, .prepared_tokens = case.input_ids.len, .samples_ns = undefined, .h2d_bytes = 0, .d2h_bytes = 0, .allocations = 0 };
        for (&timing.samples_ns) |*sample_ns| {
            sample_ns.* = try timedResponse(transport, request, profile, case, 1);
            elapsed_ns += sample_ns.*;
        }
        if (comptime backend == .cuda) {
            const delta = factory.cudaStatsDelta(factory.getCudaRuntimeStats(loaded.session).?, before.?);
            timing.h2d_bytes = delta.h2d_bytes;
            timing.d2h_bytes = delta.d2h_bytes;
            timing.allocations = delta.device_alloc_calls;
        }
    }
    try transport.idle();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const idle = domain.admission.snapshot();
    try std.testing.expectEqual(@as(usize, 0), idle.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), idle.host_scratch_bytes);
    // Two physical forwards for GLiGuard and serial PII, preserving every row.
    const first = reference.value.cases[0];
    const batched = try body(model, first, 9);
    defer a.free(batched);
    max_reservation = @max(max_reservation, try reservation(loaded, batched, profile));
    const before_batch = factory.getCudaRuntimeStats(loaded.session);
    try checkResponse(transport, batched, profile, first, 9);
    if (comptime backend == .cuda and profile == .gliguard)
        try std.testing.expectEqual(before_batch.?.download_events + 2, factory.getCudaRuntimeStats(loaded.session).?.download_events);
    const ragged = try raggedBody(model, reference.value.cases);
    defer a.free(ragged);
    max_reservation = @max(max_reservation, try reservation(loaded, ragged, profile));
    {
        var response = try transport.post(ragged);
        defer response.deinit();
        errdefer std.debug.print("specialty ragged HTTP {d}: {s}\n", .{ response.status.code, response.body orelse "<missing>" });
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, response.body.?, .{});
        defer parsed.deinit();
        const data = parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(reference.value.cases.len, data.len);
        for (reference.value.cases, data) |case, sample| try checkSample(profile, case, sample);
    }
    if (comptime backend == .cuda) {
        const maximum = try body(model, first, server.max_serial_family_batch_items);
        defer a.free(maximum);
        max_reservation = @max(max_reservation, try reservation(loaded, maximum, profile));
        try checkResponse(transport, maximum, profile, first, server.max_serial_family_batch_items);
    }
    try transport.idle();
    try std.testing.expectEqual(idle, domain.admission.snapshot());
    // The unsupported task, long-document and token-limit failures must leave
    // the admitted model usable without unloading it.
    const invalid = try std.fmt.allocPrint(a, "{{\"schema_version\":2,\"model\":{f},\"inputs\":[{{\"content\":\"hello\"}}],\"schema\":{s}}}", .{ std.json.fmt(model, .{}), if (profile == .pii) "{\"classifications\":[{\"name\":\"x\",\"labels\":[\"a\",\"b\"]}]}" else "{\"entities\":[\"person\"]}" });
    defer a.free(invalid);
    try expectRejected(transport, invalid, 400);
    const long = try std.fmt.allocPrint(a, "{{\"schema_version\":2,\"model\":{f},\"inputs\":[{{\"content\":\"hello\"}}],\"schema\":{s},\"options\":{{\"long_document\":{{\"mode\":\"window\"}}}}}}", .{ std.json.fmt(model, .{}), first.native_schema_json });
    defer a.free(long);
    try expectRejected(transport, long, 400);
    const oversized_text = try a.alloc(u8, 4096);
    defer a.free(oversized_text);
    for (oversized_text, 0..) |*char, i| char.* = if (i % 2 == 0) 'a' else ' ';
    var oversized_case = first;
    oversized_case.text = oversized_text;
    const oversized = try body(model, oversized_case, 1);
    defer a.free(oversized);
    try expectRejected(transport, oversized, 413);
    const singleton = try body(model, first, 1);
    defer a.free(singleton);
    try checkResponse(transport, singleton, profile, first, 1);
    // Exercise the actual encoder/head at the inclusive prepared-token limit.
    // Larger public GLiGuard requests still use physical groups of eight.
    if (comptime backend == .cuda) {
        var request = try wire.parseJson(a, singleton, .{});
        defer request.deinit();
        const boundary_text = try @import("../extractors/gliner_specialty_parity_test.zig").textAtPreparedTokenLimit(a, loaded.getTokenizer(), &request.items[0].compiled, 512);
        defer a.free(boundary_text);
        var boundary_case = first;
        boundary_case.text = boundary_text;
        boundary_case.threshold = 1.0;
        const count: usize = if (profile == .gliguard) 8 else 1;
        const boundary_request = try body(model, boundary_case, count);
        defer a.free(boundary_request);
        max_reservation = @max(max_reservation, try reservation(loaded, boundary_request, profile));
        var response = try transport.post(boundary_request);
        defer response.deinit();
        errdefer std.debug.print("specialty512 HTTP {d}: {s}\n", .{ response.status.code, response.body orelse "<missing>" });
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, response.body.?, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(512 * count)), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
        const boundary_rows = parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(count, boundary_rows.len);
        const expected_boundary = try std.json.Stringify.valueAlloc(a, boundary_rows[0], .{});
        defer a.free(expected_boundary);
        // The largest admitted request reaches both limits together: 128
        // public items, each with all 512 prepared tokens including its schema.
        const maximum_geometry = try body(model, boundary_case, server.max_serial_family_batch_items);
        defer a.free(maximum_geometry);
        max_reservation = @max(max_reservation, try reservation(loaded, maximum_geometry, profile));
        const before_maximum = factory.getCudaRuntimeStats(loaded.session).?;
        var maximum_response = try transport.post(maximum_geometry);
        defer maximum_response.deinit();
        errdefer std.debug.print("specialty maximum geometry HTTP {d}: {s}\n", .{ maximum_response.status.code, maximum_response.body orelse "<missing>" });
        try std.testing.expectEqual(@as(u16, 200), maximum_response.status.code);
        const maximum_parsed = try std.json.parseFromSlice(std.json.Value, a, maximum_response.body.?, .{});
        defer maximum_parsed.deinit();
        try std.testing.expectEqual(@as(i64, 512 * server.max_serial_family_batch_items), maximum_parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
        const maximum_rows = maximum_parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(server.max_serial_family_batch_items, maximum_rows.len);
        for (maximum_rows) |row| {
            const actual_boundary = try std.json.Stringify.valueAlloc(a, row, .{});
            defer a.free(actual_boundary);
            try std.testing.expectEqualStrings(expected_boundary, actual_boundary);
        }
        if (comptime profile == .gliguard)
            try std.testing.expectEqual(before_maximum.download_events + server.max_serial_family_batch_items / 8, factory.getCudaRuntimeStats(loaded.session).?.download_events);
        const over_text = try @import("../extractors/gliner_specialty_parity_test.zig").textAtPreparedTokenLimit(a, loaded.getTokenizer(), &request.items[0].compiled, 513);
        defer a.free(over_text);
        boundary_case.text = over_text;
        const over_request = try body(model, boundary_case, 1);
        defer a.free(over_request);
        try expectRejected(transport, over_request, 413);
    }
    {
        var parsed = try std.json.parseFromSlice(std.json.Value, a, singleton, .{});
        defer parsed.deinit();
        _ = parsed.value.object.swapRemove("schema_version");
        const unversioned = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(unversioned);
        try checkResponse(transport, unversioned, profile, first, 1);
    }
    try transport.idle();
    try std.testing.expectEqual(idle, domain.admission.snapshot());
    if (comptime backend == .cuda) {
        const launches_before_denial = factory.getCudaRuntimeStats(loaded.session).?.kernel_launches;
        const original_scratch_limit = node.config.generation_budget_overrides.scratch_limit_bytes;
        {
            defer node.config.generation_budget_overrides.scratch_limit_bytes = original_scratch_limit;
            node.config.generation_budget_overrides.scratch_limit_bytes = 1;
            try expectRejected(transport, singleton, 507);
        }
        try transport.idle();
        try std.testing.expectEqual(launches_before_denial, factory.getCudaRuntimeStats(loaded.session).?.kernel_launches);
        try std.testing.expectEqual(idle, domain.admission.snapshot());
        var response = try transport.post(singleton);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        try transport.idle();
        try @import("gliner_family_cuda_socket_test.zig").queuedCancellation(&node, transport, directory, "/ai/v1/extract", singleton, response.body.?);
        // Concurrent requests share the model's admission lane and release all
        // request leases before the model can be unloaded.
        const Call = struct {
            fn run(t: *Loopback, request: []const u8, case: @TypeOf(first)) anyerror!void {
                try checkResponse(t, request, profile, case, 1);
            }
        };
        var driver = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(8) });
        defer driver.deinit();
        var futures: [8]std.Io.Future(anyerror!void) = undefined;
        var started: usize = 0;
        defer for (futures[0..started]) |*future| {
            future.await(driver.io()) catch {};
        };
        for (&futures) |*future| {
            future.* = try driver.io().concurrent(Call.run, .{ transport, singleton, first });
            started += 1;
        }
        for (futures[0..started]) |*future| try future.await(driver.io());
        started = 0;
        try transport.idle();
        try std.testing.expectEqual(idle, domain.admission.snapshot());
    }
    if (comptime backend == .cuda) {
        const stats = factory.cudaStatsDelta(factory.getCudaRuntimeStats(loaded.session).?, initial_stats.?);
        const memory = factory.getCudaAllocationStats(loaded.session).?;
        const reserved_peak = @max(initial_memory.?.peak, initial_memory.?.live + max_reservation);
        try std.testing.expect(memory.peak <= reserved_peak);
        try std.testing.expect(stats.kernel_launches > 0);
        if (profile == .pii) try std.testing.expect(stats.gliner_label_gru_no_residual_calls > 0) else try std.testing.expectEqual(@as(usize, 0), stats.gliner_label_gru_calls);
        const receipt = try std.json.Stringify.valueAlloc(a, .{
            .profile = @tagName(profile),
            .backend = @tagName(backend),
            .cases = reference.value.cases.len,
            .timings = timings,
            .elapsed_ns = elapsed_ns,
            .h2d_bytes = stats.h2d_bytes,
            .d2h_bytes = stats.d2h_bytes,
            .kernel_launches = stats.kernel_launches,
            .gru_calls = stats.gliner_label_gru_calls,
            .gru_no_residual_calls = stats.gliner_label_gru_no_residual_calls,
            .device_alloc_calls = stats.device_alloc_calls,
            .initial_device_memory = initial_memory.?,
            .final_device_memory = memory,
            .max_reserved_scratch_bytes = max_reservation,
            .reserved_physical_peak_bytes = reserved_peak,
            .idle_admission = idle,
        }, .{});
        defer a.free(receipt);
        std.debug.print("specialty_http_receipt: {s}\n", .{receipt});
    }
    const runtime_id = loaded.runtime_id;
    try std.testing.expectEqual(server.model_manager.UnloadRuntimeResult{ .in_use = 1 }, node.model_manager.unloadRuntime(runtime_id));
    try checkResponse(transport, singleton, profile, first, 1);
    try transport.idle();
    handle.release();
    handle_active = false;
    try std.testing.expectEqual(.unloaded, node.model_manager.unloadRuntime(runtime_id));
    try std.testing.expectEqual(.already_unloaded, node.model_manager.unloadRuntime(runtime_id));
    try checkResponse(transport, singleton, profile, first, 1);
    try transport.idle();
    var reloaded = try node.model_manager.acquireFromDir(directory);
    defer reloaded.release();
    try std.testing.expect(reloaded.get().runtime_id != runtime_id);
    try std.testing.expectEqual(backend, reloaded.get().session.backend());
    try std.testing.expectEqual(profile, try qualification.require(try factory.getGlinerDecisionIdentity(reloaded.get().session)));
    try std.testing.expectEqual(@as(usize, 0), domain.admission.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), domain.admission.snapshot().host_scratch_bytes);
    try transport.finish();
}

test "GLiNER specialty PII public HTTP native" {
    try qualify(.pii, .native);
}
test "GLiNER specialty PII public HTTP CUDA" {
    try qualify(.pii, .cuda);
}
test "GLiNER specialty PII public HTTP Metal" {
    try qualify(.pii, .metal);
}
test "GLiNER specialty GLiGuard public HTTP native" {
    try qualify(.gliguard, .native);
}
test "GLiNER specialty GLiGuard public HTTP CUDA" {
    try qualify(.gliguard, .cuda);
}
test "GLiNER specialty GLiGuard public HTTP Metal" {
    try qualify(.gliguard, .metal);
}
