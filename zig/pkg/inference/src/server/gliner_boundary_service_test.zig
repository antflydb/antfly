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
const extracting_api = @import("antfly_extracting");
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
    // This fabricated small-backbone directory is never a reviewed
    // production identity, so every dispatch above stayed at the coarse
    // UNSUPPORTED_EXTRACTION_FEATURE rejection regardless of whether the
    // family-wide runtime is published; the 400 responses already prove it.
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
    // The small backbone has no reviewed production row (only base does),
    // so the first dispatch below is rejected on the production default and
    // the rest of this test relies on the explicit test_allow_unqualified_
    // gliner_boundary override, never on the family-wide runtime flag.
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
    }
    // Re-hash every consumed artifact after the managed session is destroyed;
    // no successful test may qualify substituted or modified source bytes.
    try verifyFiles(a, directory, pins);
}

// The documented plain extraction request omits "schema_version"; a boundary
// model can only execute through the schema_version:2 path, so extractJSON
// must upgrade a plain request naming a boundary model onto that path
// (Node.boundaryUpgradeRequestJsonIfNeeded) instead of routing it into the
// pre-boundary legacy dispatcher, which cannot run this architecture.
test "gliner boundary v2 upgrades a plain extraction request without schema_version for the pinned base checkpoint" {
    const requested_directory = platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = if (std.fs.path.isAbsolute(requested_directory))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested_directory, &path_buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested_directory, &path_buffer);
    const directory = path_buffer[0..path_len];
    const models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath;
    const name = std.fs.path.basename(directory);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    try node.attachIo(std.testing.io);

    const plain_body = try std.fmt.allocPrint(a,
        \\{{"model":"{s}","inputs":[{{"id":"1","content":"The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection."}}],"schema":{{"entities":["component","subsystem","test"],"relations":[{{"type":"depends_on"}},{{"type":"tested_by"}}]}},"options":{{"include_confidence":true,"include_spans":true}}}}
    , .{name});
    defer a.free(plain_body);
    var response = try dispatch(a, &node, plain_body);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const body = response.body orelse return error.MissingResponseBody;
    try std.testing.expect(std.mem.indexOf(u8, body, "\"entities\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"relations\":[") != null);

    // An already-versioned request for the same model is passed through
    // unchanged by the upgrade helper and must still succeed identically.
    const versioned_body = try std.fmt.allocPrint(a,
        \\{{"schema_version":2,"model":"{s}","inputs":[{{"id":"1","content":"The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection."}}],"schema":{{"entities":["component","subsystem","test"],"relations":[{{"type":"depends_on"}},{{"type":"tested_by"}}]}},"options":{{"include_confidence":true,"include_spans":true}}}}
    , .{name});
    defer a.free(versioned_body);
    var versioned_response = try dispatch(a, &node, versioned_body);
    defer versioned_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), versioned_response.status.code);
}

// The in-process worker's provider "extract" operation
// (host.linkedInferenceInvokeProvider in antfly/src/standalone/inference_host.zig)
// calls Node.extractDirectWithControl directly, never through extractJSON.
// It sends a typed extracting_api.Request built from the enrichment runtime's
// producer_json config (examples/dogfood/index_config.go's
// knowledgeGraphIndexJSON: {"provider":"antfly","model":...,"schema":
// {"entities":[...],"relations":[{"type":...}]},"options":{...}}, rendered
// by zig/lib/extracting without ever setting "schema_version"), so
// request.schema_version is null. Before the extractJSON-level upgrade was
// moved into extractWithAdmission (the entry both extractJSON's "structures"
// operation and extractDirect share), this fell into the pre-boundary
// legacy dispatch and failed with error.BoundaryExtractionRequiresSchema.
// This checks the real production model directory directly (not the
// ANTFLY_GLINER25_BASE_MODEL_DIR override above), matching how an operator
// would actually have it pulled.
test "gliner boundary provider extractDirect upgrades a plain request for the qualified base checkpoint" {
    const home = platform.env.getenv("HOME") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const directory = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino", "gliner2.5-base-v1" });
    defer a.free(directory);
    std.Io.Dir.cwd().access(std.testing.io, directory, .{}) catch return error.SkipZigTest;
    const models_dir = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino" });
    defer a.free(models_dir);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    try node.attachIo(std.testing.io);

    const content_json = try std.json.Stringify.valueAlloc(a, "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection.", .{});
    defer a.free(content_json);
    const request = extracting_api.Request{
        .inputs = &.{.{ .id = "1", .content_json = content_json }},
        .schema_json =
        \\{"entities":["component","subsystem","test"],"relations":[{"type":"depends_on"},{"type":"tested_by"}]}
        ,
        .options_json =
        \\{"include_confidence":true,"include_spans":true}
        ,
    };
    try std.testing.expect(request.schema_version == null);
    var response = try node.extractDirect(a, "gliner2.5-base-v1", request);
    defer response.deinit();
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"entities\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.json, "\"relations\":[") != null);
}

// Follow-up: closing the gap between the in-process provider rate (measured
// at ~1.8 sections/s during a real examples/dogfood ingest, which also runs
// the Qwen3 embedder concurrently on the same Metal device -- a confound) and
// the standalone-server HTTP rate (~4.5 sections/s, GLINER25.md's long-document
// throughput section). This isolates the provider entry itself
// (Node.extractDirectWithControl, exactly as the in-process worker's
// "extract" provider operation calls it -- see the upgrade test above and
// GLINER25.md section 8) from that confound: same real corpus sections, same
// schema, same backend budgets, same window defaults, no concurrent
// embedding work, sequential (one request in flight at a time, matching
// GLINER25.md's "direct" baseline). Compare its printed sections/s against a
// live `antfly inference run --port 8098` server fed the identical corpus
// file with the identical schema (see GLINER25.md's throughput section for
// the exact HTTP-side command and numbers). Skipped unless both env vars are
// set; never asserts a specific throughput number itself (machine-dependent),
// only that every section is served successfully, so this stays a stable
// regression guard while GLINER25.md records the actual measured numbers.
test "gliner boundary provider extractDirect throughput on a real corpus matches a live HTTP baseline" {
    const home = platform.env.getenv("HOME") orelse return error.SkipZigTest;
    const corpus_path = platform.env.getenv("ANTFLY_GLINER25_THROUGHPUT_CORPUS") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const directory = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino", "gliner2.5-base-v1" });
    defer a.free(directory);
    std.Io.Dir.cwd().access(std.testing.io, directory, .{}) catch return error.SkipZigTest;
    const models_dir = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino" });
    defer a.free(models_dir);

    const Section = struct { id: []const u8, text: []const u8 };
    const corpus_bytes = try @import("../util/c_file.zig").readFileMax(a, corpus_path, 8 * 1024 * 1024);
    defer a.free(corpus_bytes);
    var parsed_corpus = try std.json.parseFromSlice([]const Section, a, corpus_bytes, .{});
    defer parsed_corpus.deinit();
    const sections = parsed_corpus.value;

    // Same 11-entity/6-relation dogfood schema and window.mode as
    // GLINER25.md's throughput section and examples/dogfood's
    // knowledgeGraphIndexJSON, so this measures the identical request shape.
    const schema_json =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
    ;
    const options_json =
        \\{"include_confidence":true,"include_spans":true,"long_document":{"mode":"window"}}
    ;

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 16 * 1024 * 1024 * 1024, .combined_limit_bytes = 32 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    try node.attachIo(std.testing.io);
    const control = Control{ .hard_cancellation = node.hard_cancellation_watchdog.?.boundary(), .deadline_ns = platform.time.monotonicNs() + 900 * std.time.ns_per_s };

    // Warm-up call: load weights and prepare the Metal session once before
    // timing, matching a long-lived server that has already served traffic.
    {
        const warm_content = try std.json.Stringify.valueAlloc(a, "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection.", .{});
        defer a.free(warm_content);
        var response = try node.extractDirectWithControl(a, "gliner2.5-base-v1", .{
            .inputs = &.{.{ .id = "warm", .content_json = warm_content }},
            .schema_version = 2,
            .schema_json = schema_json,
            .options_json = options_json,
        }, control);
        response.deinit();
    }

    var errors: usize = 0;
    const wall_started = platform.time.monotonicNs();
    for (sections, 0..) |section, index| {
        const content_json = try std.json.Stringify.valueAlloc(a, section.text, .{});
        defer a.free(content_json);
        const started = platform.time.monotonicNs();
        const outcome = node.extractDirectWithControl(a, "gliner2.5-base-v1", .{
            .inputs = &.{.{ .id = section.id, .content_json = content_json }},
            .schema_version = 2,
            .schema_json = schema_json,
            .options_json = options_json,
        }, control);
        const elapsed_ms = (platform.time.monotonicNs() - started) / std.time.ns_per_ms;
        if (outcome) |*response| {
            var mutable_response = response.*;
            defer mutable_response.deinit();
            std.debug.print("provider throughput [{d}/{d}] id={s} bytes={d} latency_ms={d}\n", .{ index + 1, sections.len, section.id, section.text.len, elapsed_ms });
        } else |err| {
            errors += 1;
            std.debug.print("provider throughput [{d}/{d}] id={s} bytes={d} latency_ms={d} ERROR={s}\n", .{ index + 1, sections.len, section.id, section.text.len, elapsed_ms, @errorName(err) });
        }
    }
    const wall_ns = platform.time.monotonicNs() - wall_started;
    const wall_s = @as(f64, @floatFromInt(wall_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
    std.debug.print("provider throughput TOTAL sections={d} errors={d} wall_s={d:.3} sections_per_s={d:.3}\n", .{ sections.len, errors, wall_s, @as(f64, @floatFromInt(sections.len)) / wall_s });
    try std.testing.expectEqual(@as(usize, 0), errors);
}

// Slices out the section starting at `heading` and running to the next
// Markdown heading line or end of file. Mirrors the section boundaries
// examples/dogfood's docsaf.MarkdownProcessor produces (a new section at
// every heading), duplicated locally (rather than imported from
// extractors/gliner_boundary_qualification.zig) to avoid a cross-package
// test-only dependency from server/ into extractors/.
fn extractHeadingSection(full: []const u8, heading: []const u8) ![]const u8 {
    const start = std.mem.indexOf(u8, full, heading) orelse return error.MissingFixtureSection;
    var end = full.len;
    var cursor = start + heading.len;
    while (cursor + 1 < full.len) : (cursor += 1) {
        if (full[cursor] == '\n' and full[cursor + 1] == '#') {
            end = cursor;
            break;
        }
    }
    return full[start..end];
}

// zig/EXTRACT.md's canonical schema_version 2 envelope (entities with
// text/label/start/end/score; relations with type, source.entity_index,
// target.entity_index, score) is produced by extraction_v2.zig's writeSample
// for EVERY pipeline.Sample regardless of which executor produced it, but
// the long-document executor builds its Sample through an independent merge
// path (gliner_boundary_long_executor.zig's mergeAll / long_relations.merge)
// that could in principle diverge -- e.g. by leaving raw head/tail edges, or
// by losing entity_index resolution when relation-merge and entity-merge
// independently deduplicate overlapping window candidates. This exercises
// that merge path end to end, through the real HTTP handler, on a real
// design-doc section large enough to require more than one window (the
// zig/GRAPH.md-documented extraction_relation source parser consumes exactly
// this shape, via examples/dogfood's knowledgeGraphIndexJSON).
test "gliner boundary long executor HTTP canonical schema_version 2 shape for a real multi-window document with relations native" {
    try longExecutorHttpCanonicalShape(false);
}
test "gliner boundary long executor HTTP canonical schema_version 2 shape for a real multi-window document with relations Metal" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    try longExecutorHttpCanonicalShape(true);
}
fn longExecutorHttpCanonicalShape(metal: bool) !void {
    const requested_directory = platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = if (std.fs.path.isAbsolute(requested_directory))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested_directory, &path_buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested_directory, &path_buffer);
    const directory = path_buffer[0..path_len];
    const models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath;
    const name = std.fs.path.basename(directory);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        // Metal is a process-required backend (backends.zig's
        // requiresProcessIsolation); ModelManager refuses to close/reopen
        // such a session unless the harness opts in here (mirrors
        // gliner_boundary_metal_socket_test.zig's Node.init config).
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    if (!metal) useNativeBackend(&node) else {
        node.session_manager.preferred_backends = &.{.metal};
        node.session_manager.required_backend = .metal;
        node.model_manager.session_manager.preferred_backends = &.{.metal};
        node.model_manager.session_manager.required_backend = .metal;
    }
    try node.attachIo(std.testing.io);

    // Path relative to the inference-test binary's working directory
    // (zig/pkg/inference, per zig/TESTING.md's build steps).
    const full = try @import("../util/c_file.zig").readFile(a, "../../VOPR.md");
    defer a.free(full);
    const section = try extractHeadingSection(full, "### Completion-Claim Audit");

    const schema_source =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
    ;
    var parsed_schema = try std.json.parseFromSlice(Value, a, schema_source, .{});
    defer parsed_schema.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = name,
        .schema = parsed_schema.value,
        .options = .{ .include_confidence = true, .include_spans = true, .long_document = .{ .mode = "window" } },
        .inputs = &.{.{ .id = "1", .content = section }},
    }, .{});
    defer a.free(body);

    var request = try httpx.Request.init(a, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body = body;
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    // The 37KB real section plus schema/JSON overhead exceeds the 64KB cap
    // other tests in this file use for short bodies.
    ctx.max_request_body_size = 128 * 1024;
    ctx.application_deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s;
    var response = try node.extractJSON(&ctx);
    defer response.deinit();
    errdefer std.debug.print("long-document HTTP response: status={d} body={s}\n", .{ response.status.code, response.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    var parsed = try std.json.parseFromSlice(Value, a, response.body orelse return error.MissingResponseBody, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("data").?.array.items[0].object;

    // Confirms the request actually took the long-document windowed path
    // (rather than silently fitting in one window), so this is real evidence
    // for the merge codepath, not just the single-window shape already
    // covered elsewhere.
    const window_count = output.get("long_document").?.object.get("window_count").?.integer;
    try std.testing.expect(window_count >= 2);

    const entities = output.get("entities").?.array.items;
    try std.testing.expect(entities.len > 0);
    for (entities) |raw_entity| {
        const entity = raw_entity.object;
        try std.testing.expect(entity.contains("label"));
        try std.testing.expect(entity.contains("text"));
        try std.testing.expect(entity.contains("start"));
        try std.testing.expect(entity.contains("end"));
        try std.testing.expect(entity.contains("score"));
        try std.testing.expect(!entity.contains("head"));
        try std.testing.expect(!entity.contains("tail"));
    }
    if (output.get("relations")) |raw_relations| {
        for (raw_relations.array.items) |raw_relation| {
            const relation = raw_relation.object;
            try std.testing.expect(relation.contains("type"));
            try std.testing.expect(relation.contains("source"));
            try std.testing.expect(relation.contains("target"));
            try std.testing.expect(!relation.contains("head"));
            try std.testing.expect(!relation.contains("tail"));
            inline for (.{ "source", "target" }) |key| {
                const endpoint = relation.get(key).?.object;
                try std.testing.expect(endpoint.contains("text"));
                if (endpoint.get("entity_index")) |index| {
                    try std.testing.expect(index.integer >= 0 and index.integer < @as(i64, @intCast(entities.len)));
                    try std.testing.expect(endpoint.contains("label"));
                }
            }
        }
    }
}

// Deterministic companion to the multi-window HTTP test above, through the
// provider entry point instead (Node.extractDirect, as the in-process worker
// calls it): the short repro text is already known (GLINER25.md's "End to
// end" evidence) to reliably produce a "tested_by" relation from this exact
// checkpoint, so this can assert the canonical shape strictly rather than
// only when a relation happens to be present.
test "gliner boundary long executor provider extractDirect canonical schema_version 2 relations shape for a windowed request native" {
    try longExecutorProviderCanonicalShape(false);
}
test "gliner boundary long executor provider extractDirect canonical schema_version 2 relations shape for a windowed request Metal" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    try longExecutorProviderCanonicalShape(true);
}
fn longExecutorProviderCanonicalShape(metal: bool) !void {
    const home = platform.env.getenv("HOME") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const directory = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino", "gliner2.5-base-v1" });
    defer a.free(directory);
    std.Io.Dir.cwd().access(std.testing.io, directory, .{}) catch return error.SkipZigTest;
    const models_dir = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino" });
    defer a.free(models_dir);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    if (!metal) useNativeBackend(&node) else {
        node.session_manager.preferred_backends = &.{.metal};
        node.session_manager.required_backend = .metal;
        node.model_manager.session_manager.preferred_backends = &.{.metal};
        node.model_manager.session_manager.required_backend = .metal;
    }
    try node.attachIo(std.testing.io);

    // Uses examples/dogfood's real 11-entity/6-relation production schema
    // (not the narrower 3-entity repro schema from GLINER25.md's earlier
    // single-window evidence): the qualified long-document row's measured
    // padded_sequence_tokens floor (106) was measured against this wider
    // schema, which encodes more schema prefix tokens per document than the
    // narrower one -- a request in the narrower schema's shape would
    // correctly fail closed here as ungeasured geometry for this row.
    const content_json = try std.json.Stringify.valueAlloc(a, "The metadata server coordinates Raft groups. VOPR exercises the DataServer under fault injection.", .{});
    defer a.free(content_json);
    const request = extracting_api.Request{
        .schema_version = 2,
        .inputs = &.{.{ .id = "1", .content_json = content_json }},
        .schema_json =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
        ,
        .options_json =
        \\{"include_confidence":true,"include_spans":true,"long_document":{"mode":"window"}}
        ,
    };
    var response = try node.extractDirect(a, "gliner2.5-base-v1", request);
    defer response.deinit();
    errdefer std.debug.print("long-document provider response: {s}\n", .{response.json});
    var parsed = try std.json.parseFromSlice(Value, a, response.json, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("data").?.array.items[0].object;
    try std.testing.expect(output.get("long_document") != null);
    const entities = output.get("entities").?.array.items;
    try std.testing.expect(entities.len > 0);
    for (entities) |raw_entity| {
        const entity = raw_entity.object;
        try std.testing.expect(entity.contains("label"));
        try std.testing.expect(entity.contains("text"));
        try std.testing.expect(entity.contains("start"));
        try std.testing.expect(entity.contains("end"));
        try std.testing.expect(entity.contains("score"));
    }
    if (output.get("relations")) |raw_relations| {
        for (raw_relations.array.items) |raw_relation| {
            const relation = raw_relation.object;
            try std.testing.expect(relation.contains("type"));
            try std.testing.expect(!relation.contains("head"));
            try std.testing.expect(!relation.contains("tail"));
            inline for (.{ "source", "target" }) |key| {
                const endpoint = relation.get(key).?.object;
                try std.testing.expect(endpoint.contains("text"));
                if (endpoint.get("entity_index")) |index| {
                    try std.testing.expect(index.integer >= 0 and index.integer < @as(i64, @intCast(entities.len)));
                }
            }
        }
    }
}

// Corpus-maximum-scale companion to the two windowed tests above: zig/PDF.md's
// "Review findings and required fixes" is the largest single section
// examples/dogfood's real docsaf.MarkdownProcessor splitting currently
// produces across the whole ingest corpus (measured directly in
// extractors/gliner_boundary_qualification.zig's long-document geometry
// test: ~92KB body, 15 windows at the wire's 1024-word default), so this
// exercises the merge path at real production scale through both entries a
// caller can reach it from -- the HTTP handler and the in-process provider
// entry (Node.extractDirect, as examples/dogfood's embedded worker calls
// it) -- rather than only the ~37KB section the tests above already cover.
test "gliner boundary long executor HTTP canonical schema_version 2 shape for the corpus-maximum real section" {
    const a = std.testing.allocator;
    const requested_directory = platform.env.getenv("ANTFLY_GLINER25_BASE_MODEL_DIR") orelse return error.SkipZigTest;
    var path_buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const path_len = if (std.fs.path.isAbsolute(requested_directory))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested_directory, &path_buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested_directory, &path_buffer);
    const directory = path_buffer[0..path_len];
    const models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath;
    const name = std.fs.path.basename(directory);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    useNativeBackend(&node);
    try node.attachIo(std.testing.io);

    // Path relative to the inference-test binary's working directory
    // (zig/pkg/inference, per zig/TESTING.md's build steps).
    const full = try @import("../util/c_file.zig").readFile(a, "../../PDF.md");
    defer a.free(full);
    const section = try extractHeadingSection(full, "## Review findings and required fixes");
    try std.testing.expect(section.len > 90000);

    const schema_source =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
    ;
    var parsed_schema = try std.json.parseFromSlice(Value, a, schema_source, .{});
    defer parsed_schema.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = name,
        .schema = parsed_schema.value,
        .options = .{ .include_confidence = true, .include_spans = true, .long_document = .{ .mode = "window" } },
        .inputs = &.{.{ .id = "1", .content = section }},
    }, .{});
    defer a.free(body);

    var request = try httpx.Request.init(a, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body = body;
    var ctx = httpx.Context.init(a, std.testing.io, &request);
    defer ctx.deinit();
    ctx.max_request_body_size = 128 * 1024;
    ctx.application_deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s;
    var response = try node.extractJSON(&ctx);
    defer response.deinit();
    errdefer std.debug.print("corpus-maximum HTTP response: status={d} body={s}\n", .{ response.status.code, response.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    var parsed = try std.json.parseFromSlice(Value, a, response.body orelse return error.MissingResponseBody, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("data").?.array.items[0].object;

    // Confirms this really took the multi-window merge path at close to the
    // qualified window_count ceiling, not a coincidentally-small window count.
    const window_count = output.get("long_document").?.object.get("window_count").?.integer;
    try std.testing.expect(window_count >= 10);

    const entities = output.get("entities").?.array.items;
    try std.testing.expect(entities.len > 0);
    for (entities) |raw_entity| {
        const entity = raw_entity.object;
        try std.testing.expect(entity.contains("label"));
        try std.testing.expect(entity.contains("text"));
        try std.testing.expect(entity.contains("start"));
        try std.testing.expect(entity.contains("end"));
        try std.testing.expect(entity.contains("score"));
    }
    const relations = output.get("relations").?.array.items;
    try std.testing.expect(relations.len > 0);
    for (relations) |raw_relation| {
        const relation = raw_relation.object;
        try std.testing.expect(relation.contains("type"));
        try std.testing.expect(relation.contains("source"));
        try std.testing.expect(relation.contains("target"));
        try std.testing.expect(!relation.contains("head"));
        try std.testing.expect(!relation.contains("tail"));
    }
}

test "gliner boundary long executor provider extractDirect canonical schema_version 2 shape for the corpus-maximum real section" {
    const a = std.testing.allocator;
    const home = platform.env.getenv("HOME") orelse return error.SkipZigTest;
    const directory = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino", "gliner2.5-base-v1" });
    defer a.free(directory);
    std.Io.Dir.cwd().access(std.testing.io, directory, .{}) catch return error.SkipZigTest;
    const models_dir = try std.fs.path.join(a, &.{ home, ".antfly", "inference", "models", "fastino" });
    defer a.free(models_dir);

    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .generation_budget_overrides = .{ .host_limit_bytes = 16 * 1024 * 1024 * 1024, .scratch_limit_bytes = 4 * 1024 * 1024 * 1024, .combined_limit_bytes = 16 * 1024 * 1024 * 1024, .backend_limit_bytes = 16 * 1024 * 1024 * 1024, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    useNativeBackend(&node);
    try node.attachIo(std.testing.io);

    const full = try @import("../util/c_file.zig").readFile(a, "../../PDF.md");
    defer a.free(full);
    const section = try extractHeadingSection(full, "## Review findings and required fixes");
    try std.testing.expect(section.len > 90000);
    const content_json = try std.json.Stringify.valueAlloc(a, section, .{});
    defer a.free(content_json);

    const request = extracting_api.Request{
        .schema_version = 2,
        .inputs = &.{.{ .id = "1", .content_json = content_json }},
        .schema_json =
        \\{"entities":["component","subsystem","file","test","invariant","decision","person","model","backend","format","protocol"],"relations":[{"type":"depends_on"},{"type":"owns"},{"type":"implements"},{"type":"supersedes"},{"type":"tested_by"},{"type":"documented_in"}]}
        ,
        .options_json =
        \\{"include_confidence":true,"include_spans":true,"long_document":{"mode":"window"}}
        ,
    };
    var response = try node.extractDirect(a, "gliner2.5-base-v1", request);
    defer response.deinit();
    errdefer std.debug.print("corpus-maximum provider response: {s}\n", .{response.json});
    var parsed = try std.json.parseFromSlice(Value, a, response.json, .{});
    defer parsed.deinit();
    const output = parsed.value.object.get("data").?.array.items[0].object;
    const window_count = output.get("long_document").?.object.get("window_count").?.integer;
    try std.testing.expect(window_count >= 10);
    const entities = output.get("entities").?.array.items;
    try std.testing.expect(entities.len > 0);
    const relations = output.get("relations").?.array.items;
    try std.testing.expect(relations.len > 0);
}
