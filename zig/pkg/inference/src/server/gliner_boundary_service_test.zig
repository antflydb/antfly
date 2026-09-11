// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in, real-model HTTP handler qualification. This calls extractJSON with
//! real request/response ownership, managed loading, admission and monitoring;
//! it does not open a listener or qualify socket delivery or public availability.
const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const factory = @import("../architectures/session_factory.zig");
const memory = @import("../runtime/tier/memory.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Value = std.json.Value;
const Allocator = std.mem.Allocator;
pub const Input = struct { id: ?[]const u8 = null, content: []const u8 };

fn pinBytes(pin: pipeline.PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(pin.size_bytes, bytes.len);
    const actual = bundle.Digest.of(bytes);
    try std.testing.expectEqualStrings(pin.sha256, &actual.sha256);
}

pub fn verifyFiles(a: Allocator, directory: []const u8, pins: pipeline.PublishedModelFiles) !void {
    inline for (std.meta.fields(pipeline.PublishedModelFiles)) |field| {
        const pin = @field(pins, field.name);
        const path = try std.fs.path.join(a, &.{ directory, field.name });
        defer a.free(path);
        if (comptime std.mem.eql(u8, field.name, "model.safetensors")) {
            var reader = try @import("../models/safetensors.zig").MMapReader.openFileAbsoluteLimited(a, path, pin.size_bytes, 1024 * 1024);
            defer reader.deinit();
            try pinBytes(pin, reader.file_bytes);
        } else {
            const bytes = try @import("../util/c_file.zig").readFileMax(a, path, pin.size_bytes);
            defer a.free(bytes);
            try pinBytes(pin, bytes);
        }
    }
}

pub fn requestBytes(a: Allocator, name: []const u8, schema: Value, inputs: []const Input) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = name,
        .schema = schema,
        .inputs = inputs,
        .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
    }, .{ .emit_null_optional_fields = false });
}

fn dispatch(a: Allocator, node: *Node, raw: []const u8) !httpx.Response {
    var request = try httpx.Request.init(a, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body = raw;
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    ctx.max_request_body_size = 64 * 1024;
    ctx.application_deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s;
    // ResponseBuilder.build owns the returned bytes independently of Context
    // and of the executor's JSON, both of which are destroyed before return.
    return node.extractJSON(&ctx);
}

pub fn errorResponse(a: Allocator, response: *const httpx.Response, status: u16, code: []const u8, stage: []const u8, index: ?i64) !void {
    errdefer std.debug.print("HTTP qualification error response: status={d} body={s}\n", .{ response.status.code, response.body orelse "<absent>" });
    try std.testing.expectEqual(status, response.status.code);
    var parsed = try std.json.parseFromSlice(Value, a, response.body orelse return error.MissingResponseBody, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    try std.testing.expectEqualStrings(code, object.get("error").?.string);
    try std.testing.expectEqualStrings(stage, object.get("stage").?.string);
    try std.testing.expectEqual(@as(i64, 2), object.get("schema_version").?.integer);
    if (index) |expected| {
        try std.testing.expectEqual(expected, object.get("input_index").?.integer);
    } else if (object.get("input_index")) |actual| try std.testing.expect(actual == .null);
    // Earlier decoded rows and usage must never escape an atomic failure.
    try std.testing.expect(!object.contains("data"));
    try std.testing.expect(!object.contains("usage"));
    try std.testing.expect(!object.contains("entities"));
}

pub fn successfulResponse(a: Allocator, response: *const httpx.Response, name: []const u8, id: ?[]const u8, source: []const u8, expected: pipeline.ExpectedSample) !i64 {
    errdefer std.debug.print("HTTP qualification success response: status={d} body={s}\n", .{ response.status.code, response.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try std.testing.expectEqualStrings("application/json", response.header("content-type").?);
    var parsed = try std.json.parseFromSlice(Value, a, response.body orelse return error.MissingResponseBody, .{});
    defer parsed.deinit();
    const envelope = parsed.value.object;
    try std.testing.expectEqualStrings("extraction", envelope.get("object").?.string);
    try std.testing.expectEqualStrings(name, envelope.get("model").?.string);
    try std.testing.expectEqual(@as(i64, 2), envelope.get("schema_version").?.integer);
    try std.testing.expectEqual(@as(usize, 1), envelope.get("data").?.array.items.len);
    const output = envelope.get("data").?.array.items[0].object;
    if (id) |expected_id| {
        try std.testing.expectEqualStrings(expected_id, output.get("id").?.string);
    } else try std.testing.expect(!output.contains("id"));
    try std.testing.expectEqualStrings("unicode_codepoints", output.get("offset_unit").?.string);
    const entities = output.get("entities").?.array.items;
    var entity_count: usize = 0;
    for (expected.entities) |group| entity_count += group.values.len;
    try std.testing.expectEqual(@as(usize, 4), entity_count);
    try std.testing.expectEqual(entity_count, entities.len);
    var position: usize = 0;
    for (expected.entities) |group| for (group.values) |value| {
        const actual = entities[position].object;
        position += 1;
        try std.testing.expectEqualStrings(group.name, actual.get("label").?.string);
        try std.testing.expectEqualStrings(value.text, actual.get("text").?.string);
        try std.testing.expectApproxEqAbs(@as(f64, value.confidence), actual.get("score").?.float, 5e-4);
        const offsets = value.source.?;
        try std.testing.expectEqual(@as(i64, @intCast(offsets.start)), actual.get("start").?.integer);
        try std.testing.expectEqual(@as(i64, @intCast(offsets.end)), actual.get("end").?.integer);
        // This pinned case is ASCII, so its codepoint and byte slices agree.
        try std.testing.expectEqualStrings(value.text, source[offsets.start..offsets.end]);
    };
    const classifications = output.get("classifications").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), classifications.len);
    try std.testing.expectEqual(@as(usize, 1), expected.classifications.len);
    const wanted = expected.classifications[0];
    try std.testing.expectEqual(@as(usize, 1), wanted.labels.len);
    const actual = classifications[0].object;
    try std.testing.expectEqualStrings(wanted.name, actual.get("name").?.string);
    try std.testing.expectEqualStrings(wanted.labels[0].label, actual.get("label").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, wanted.labels[0].confidence), actual.get("score").?.float, 5e-4);
    try std.testing.expectEqual(@as(usize, 0), expected.relations.len);
    try std.testing.expectEqual(@as(usize, 0), expected.structures.len);
    try std.testing.expect(!output.contains("relations"));
    try std.testing.expect(!output.contains("structures"));
    try std.testing.expect(!output.contains("long_document"));
    const usage = envelope.get("usage").?.object;
    const tokens = usage.get("prompt_tokens").?.integer;
    try std.testing.expect(tokens > 0);
    try std.testing.expectEqual(@as(i64, 0), usage.get("completion_tokens").?.integer);
    try std.testing.expectEqual(tokens, usage.get("total_tokens").?.integer);
    return tokens;
}

pub fn idle(node: *Node) !memory.AdmissionAmounts {
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(i64, 0), node.metrics.requests_active.impl.value);
    try std.testing.expectEqual(@as(i64, 0), node.metrics.extraction_v2.active.impl.value);
    const amounts = node.model_manager.resource_domain.?.admission.snapshot();
    try std.testing.expectEqual(@as(usize, 0), amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.backend_kv_bytes);
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    defer node.model_manager.load_lock.unlock();
    var iterator = node.model_manager.loaded.valueIterator();
    while (iterator.next()) |entry| try std.testing.expectEqual(@as(usize, 0), entry.*.active_handles);
    if (node.hard_cancellation_watchdog) |watchdog| {
        while (!watchdog.mutex.tryLock()) std.atomic.spinLoopHint();
        defer watchdog.mutex.unlock();
        try std.testing.expect(watchdog.io != null);
        try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    }
    return amounts;
}

pub fn cachedModel(node: *Node, directory: []const u8, pins: pipeline.PublishedModelFiles) !usize {
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
    try std.testing.expectEqual(.native, loaded.session.backend());
    try std.testing.expectEqual(model.Backbone.small, (try factory.getGlinerBoundaryConfig(loaded.session)).backbone);
    const identity = try factory.getGlinerBoundaryIdentity(loaded.session);
    try std.testing.expectEqual(.fp32, identity.precision);
    try std.testing.expectEqualStrings(pins.@"model.safetensors".sha256, &identity.weight.sha256);
    try std.testing.expectEqual(@as(u64, pins.@"model.safetensors".size_bytes), identity.weight.size_bytes);
    inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" }, 0..) |name, index| {
        const pin = @field(pins, name);
        try std.testing.expectEqualStrings(pin.sha256, &identity.sidecars[index].sha256);
        try std.testing.expectEqual(@as(u64, pin.size_bytes), identity.sidecars[index].size_bytes);
    }
    return @intFromPtr(loaded);
}

/// Owner-local backend policy shared by the handler and loopback fixtures.
/// The caller attaches its Io only after the Node has its final address.
pub fn useNativeBackend(node: *Node) void {
    node.session_manager.preferred_backends = &.{.native};
    node.session_manager.required_backend = .native;
    node.session_manager.required_backend_invalid = false;
    node.model_manager.session_manager.preferred_backends = &.{.native};
    node.model_manager.session_manager.required_backend = .native;
    node.model_manager.session_manager.required_backend_invalid = false;
}

test "gliner boundary v2 lightweight model preflight avoids vocabulary and recovers model budget denial" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const config_bytes = try fixtures.fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    try temporary.dir.createDirPath(io, "preflight/encoder_config");
    try temporary.dir.writeFile(io, .{ .sub_path = "preflight/config.json", .data = config_bytes });
    try temporary.dir.writeFile(io, .{ .sub_path = "preflight/encoder_config/config.json", .data = encoder_bytes });
    // Gate qualification needs the architecture, not tokenization or weights.
    // Even the raw vocabulary is larger than the entire request heap. Its
    // generic JSON tree must never be constructed by this preflight.
    try temporary.dir.writeFile(io, .{ .sub_path = "preflight/tokenizer.json", .data = "{\"model\":{\"type\":\"Unigram\",\"vocab\":[" ++ ("[\"unused\",0]," ** 16384) ++ "[\"last\",0]]}}" });
    try temporary.dir.writeFile(io, .{ .sub_path = "preflight/model.safetensors", .data = "gate-only fixture; never loaded" });
    var schema = try std.json.parseFromSlice(Value, a, "{\"entities\":[\"person\"]}", .{});
    defer schema.deinit();
    const raw = try requestBytes(a, "preflight", schema.value, &.{.{ .content = "Ada" }});
    defer a.free(raw);
    var node = try Node.init(a, .{
        .models_dir = parent,
        .max_concurrent_requests = 1,
        .generation_budget_overrides = .{ .host_limit_bytes = 32 * 1024 * 1024, .scratch_limit_bytes = 128 * 1024 },
    });
    defer node.deinit();
    {
        var response = try dispatch(a, &node, raw);
        defer response.deinit();
        try errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
        try std.testing.expectEqual(@as(usize, 0), (try idle(&node)).hostTotalBytes());
    }
    {
        // A valid but oversized architecture file must still consume the same
        // capped owner. This fails in model preflight, beyond JSON/schema parse.
        const file = try temporary.dir.createFile(io, "preflight/config.json", .{});
        defer file.close(io);
        try file.writeStreamingAll(io, config_bytes);
        try file.writeStreamingAll(io, " " ** (256 * 1024));
    }
    {
        var response = try dispatch(a, &node, raw);
        defer response.deinit();
        try errorResponse(a, &response, 507, "MEMORY_BUDGET_EXCEEDED", "model", null);
        try std.testing.expectEqual(@as(usize, 0), (try idle(&node)).hostTotalBytes());
    }
    try temporary.dir.writeFile(io, .{ .sub_path = "preflight/config.json", .data = config_bytes });
    {
        var response = try dispatch(a, &node, raw);
        defer response.deinit();
        try errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
        try std.testing.expectEqual(@as(usize, 0), (try idle(&node)).hostTotalBytes());
    }
    try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
    try std.testing.expectEqual(@as(u64, 3), node.metrics.extraction_v2.requests.get(.http));
    try std.testing.expectEqual(@as(u64, 2), node.metrics.extraction_v2.outcomes.get(.unsupported));
    try std.testing.expectEqual(@as(u64, 1), node.metrics.extraction_v2.outcomes.get(.memory_budget));
    try std.testing.expectEqual(@as(u64, 3), node.metrics.extraction_v2.failure_stages.get(.model));
    try std.testing.expectEqual(@as(u64, 0), node.metrics.extraction_v2.decoded_items.impl.count);
    try std.testing.expect(!model.runtime_available);
}

test "gliner boundary v2 pinned small HTTP handler qualification and atomic recovery" {
    const requested_directory = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = if (std.fs.path.isAbsolute(requested_directory))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested_directory, &path_buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested_directory, &path_buffer);
    const directory = path_buffer[0..path_len];
    const models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath;
    const name = std.fs.path.basename(directory);
    const fixture_bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(fixture_bytes);
    var fixture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, fixture_bytes, .{});
    defer fixture.deinit();
    const pins = fixture.value.model_files;
    try verifyFiles(a, directory, pins);
    const case = fixture.value.cases[0];
    try std.testing.expectEqualStrings("mixed_tasks", case.id);
    try std.testing.expect(!model.runtime_available);
    const raw = try requestBytes(a, name, case.schema, &.{.{ .id = case.id, .content = case.text }});
    defer a.free(raw);
    {
        var node = try Node.init(a, .{
            .models_dir = models_dir,
            .max_loaded_models = 1,
            .max_concurrent_requests = 1,
            .keep_alive_ms = 30 * 60 * 1000,
            .process_termination_available = true,
            .generation_budget_overrides = .{ .host_limit_bytes = 1024 * 1024 * 1024, .scratch_limit_bytes = 128 * 1024 * 1024 },
        });
        defer node.deinit();
        // Backend selection is local to this owner; no environment policy or
        // global capability is changed by the qualification test.
        useNativeBackend(&node);
        try node.attachIo(std.testing.io);
        try std.testing.expect(!node.test_allow_unqualified_gliner_boundary);
        {
            var response = try dispatch(a, &node, raw);
            defer response.deinit();
            try errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
            try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
            try std.testing.expectEqual(@as(usize, 0), (try idle(&node)).hostTotalBytes());
        }
        node.test_allow_unqualified_gliner_boundary = true;
        // An independent Node retains the production default even while this
        // owner is allowed through the test-only gate.
        {
            var other = try Node.init(a, .{ .models_dir = models_dir, .generation_budget_overrides = .{ .scratch_limit_bytes = 16 * 1024 * 1024 } });
            defer other.deinit();
            try std.testing.expect(!other.test_allow_unqualified_gliner_boundary);
            var response = try dispatch(a, &other, raw);
            defer response.deinit();
            try errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
            try std.testing.expectEqual(@as(usize, 0), other.model_manager.loaded.count());
            _ = try idle(&other);
        }
        // Verify that the real Node-owned monitor is live and releases its
        // request lease. Native CPU execution itself remains cooperative.
        {
            const control = Control{ .io = std.testing.io, .hard_cancellation = node.hard_cancellation_watchdog.?.boundary(), .deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s };
            var guard = try control.enterUninterruptible(.process_required);
            guard.deinit();
            _ = try idle(&node);
        }
        var prompt_tokens: i64 = 0;
        {
            var response = try dispatch(a, &node, raw);
            defer response.deinit();
            prompt_tokens = try successfulResponse(a, &response, name, case.id, case.text, case.expected);
        }
        const cached = try cachedModel(&node, directory, pins);
        const resident = try idle(&node);
        try std.testing.expect(resident.host_weight_bytes >= pins.@"model.safetensors".size_bytes);
        try std.testing.expectEqual(@as(usize, 0), resident.backend_weight_bytes);
        {
            // The second row passes envelope/schema/options preflight, then
            // rejects before encoding. The first row has already decoded.
            const too_long = "x " ** 4097;
            const batch = try requestBytes(a, name, case.schema, &.{ .{ .id = "decoded-first", .content = case.text }, .{ .id = "rejected-second", .content = too_long } });
            defer a.free(batch);
            var response = try dispatch(a, &node, batch);
            defer response.deinit();
            try errorResponse(a, &response, 413, "EXTRACTION_LIMIT_EXCEEDED", "tokenizing", 1);
            try std.testing.expectEqual(@as(u64, 2), node.metrics.extraction_v2.decoded_items.impl.count);
            try std.testing.expectEqual(@as(u64, 1), node.metrics.extraction_v2.returned_items.impl.count);
        }
        try std.testing.expectEqual(resident, try idle(&node));
        try std.testing.expectEqual(cached, try cachedModel(&node, directory, pins));
        {
            const retry = try requestBytes(a, name, case.schema, &.{.{ .content = case.text }});
            defer a.free(retry);
            var response = try dispatch(a, &node, retry);
            defer response.deinit();
            try std.testing.expectEqual(prompt_tokens, try successfulResponse(a, &response, name, null, case.text, case.expected));
        }
        try std.testing.expectEqual(resident, try idle(&node));
        try std.testing.expectEqual(cached, try cachedModel(&node, directory, pins));
        const metrics = &node.metrics.extraction_v2;
        try std.testing.expectEqual(@as(u64, 4), metrics.requests.get(.http));
        try std.testing.expectEqual(@as(u64, 0), metrics.requests.get(.direct));
        try std.testing.expectEqual(@as(u64, 2), metrics.outcomes.get(.success));
        try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(.unsupported));
        try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(.resource_limit));
        try std.testing.expectEqual(@as(u64, 1), metrics.failure_stages.get(.model));
        try std.testing.expectEqual(@as(u64, 1), metrics.failure_stages.get(.tokenizing));
        try std.testing.expectEqual(@as(u64, 5), metrics.parsed_items.impl.count);
        try std.testing.expectEqual(@as(u64, 3), metrics.decoded_items.impl.count);
        try std.testing.expectEqual(@as(u64, 2), metrics.returned_items.impl.count);
        try std.testing.expectEqual(@as(u64, @intCast(3 * prompt_tokens)), metrics.decoded_prompt_tokens.impl.count);
        try std.testing.expectEqual(@as(u64, 15), metrics.decoded_output_values.impl.count);
        try std.testing.expectEqual(@as(u64, 4), node.metrics.extract_requests.impl.count);
        try std.testing.expectEqual(@as(u64, 2), node.metrics.errors_total.impl.count);
        try std.testing.expect(metrics.phase_visits.get(.teardown) >= 2);
        try std.testing.expect(!model.runtime_available);
    }
    // Re-hash every consumed artifact after the managed session is destroyed;
    // no successful test may qualify substituted or modified source bytes.
    try verifyFiles(a, directory, pins);
}
