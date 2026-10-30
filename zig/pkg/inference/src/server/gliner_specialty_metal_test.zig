// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact-artifact native/Metal qualification for the legacy GLiNER2 specialty
//! profiles. This suite is deliberately opt-in: it hard-links the original
//! FP32 artifacts into a managed-model fixture, exercises the public HTTP
//! route, and compares native and Metal presentation against frozen upstream
//! captures. It never downloads or rewrites an upstream artifact.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const native_backend = @import("gliner_boundary_service_test.zig");
const family_batch = @import("gliner_family_cuda_socket_test.zig");
const factory = @import("../architectures/session_factory.zig");
const span_executor = @import("../extractors/gliner_span_v2_executor.zig");
const span_core = @import("../extractors/gliner_span_extraction.zig");
const extraction_v2 = @import("../extractors/extraction_v2.zig");
const gliner_head = @import("../architectures/gliner_head.zig");
const span_processor = @import("../pipelines/gliner_boundary_processor.zig");
const specialty = @import("../models/gliner_specialty_qualification.zig");
const registry = @import("../registry/registry.zig");
const c_file = @import("../util/c_file.zig");
const metal = @import("../backends/metal_tensor.zig");
const metal_compute = @import("../ops/metal_compute.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Value = std.json.Value;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const run_environment = "ANTFLY_RUN_GLINER_SPECIALTY_METAL";
const require_environment = "ANTFLY_REQUIRE_GLINER_SPECIALTY_METAL";
const model_root_environment = "ANTFLY_GLINER_SPECIALTY_MODELS_DIR";
const fixture_root_environment = "ANTFLY_GLINER_SPECIALTY_FIXTURES_DIR";
const default_model_root = ".benchmark-results/gliner-specialty/models";
const default_fixture_root = "zig/pkg/inference/testdata/gliner_specialty";
const pii_directory = "gliner2-privacy-filter-PII-multi";
const guard_directory = "gliguard-LLMGuardrails-300M";
const GiB = 1024 * 1024 * 1024;

const Backend = enum { native, metal };

const NumericContext = struct {
    backend: Backend,
    profile: specialty.Profile,
    case_id: []const u8,
};

fn printStructDiff(stage: []const u8, before: anytype, after: @TypeOf(before)) void {
    inline for (@typeInfo(@TypeOf(before)).@"struct".field_names) |field_name| {
        const before_value = @field(before, field_name);
        const after_value = @field(after, field_name);
        if (before_value != after_value) {
            std.debug.print("GLiNER specialty lifecycle stage={s} field={s} before={any} after={any}\n", .{
                stage,
                field_name,
                before_value,
                after_value,
            });
        }
    }
}

fn expectAdmissionRestored(stage: []const u8, before: anytype, after: @TypeOf(before)) !void {
    std.testing.expectEqual(before, after) catch |err| {
        printStructDiff(stage, before, after);
        return err;
    };
}

fn expectNoMetalDeviceWork(stage: []const u8, before: metal.MemoryStats, after: metal.MemoryStats) !void {
    // Baselines are taken only after prior HTTP workers are idle. Exact
    // equality therefore proves the denied/queued request neither dispatches
    // learned work nor allocates, releases, or downloads Metal storage.
    std.testing.expectEqual(before, after) catch |err| {
        printStructDiff(stage, before, after);
        return err;
    };
}

fn expectClose(
    context: NumericContext,
    stage: []const u8,
    task: []const u8,
    label: []const u8,
    index: ?usize,
    expected: f64,
    actual: f64,
    tolerance: f64,
) !void {
    std.testing.expectApproxEqAbs(expected, actual, tolerance) catch |err| {
        std.debug.print(
            "GLiNER specialty numerical mismatch backend={s} profile={s} case={s} stage={s} task={s} label={s} index={?d} expected={d} actual={d} delta={d} tolerance={d}\n",
            .{ @tagName(context.backend), @tagName(context.profile), context.case_id, stage, task, label, index, expected, actual, @abs(expected - actual), tolerance },
        );
        return err;
    };
}

const Captures = struct {
    pii: std.json.Parsed(Value),
    guard: std.json.Parsed(Value),

    fn deinit(self: *Captures) void {
        self.pii.deinit();
        self.guard.deinit();
    }
};

const Outputs = struct {
    pii: [][]u8,
    guard: [][]u8,
    pii_boundary: ?[]u8 = null,
    guard_boundary: ?[]u8 = null,

    fn deinit(self: *Outputs, a: Allocator) void {
        for (self.pii) |bytes| a.free(bytes);
        for (self.guard) |bytes| a.free(bytes);
        if (self.pii_boundary) |bytes| a.free(bytes);
        if (self.guard_boundary) |bytes| a.free(bytes);
        a.free(self.pii);
        a.free(self.guard);
        self.* = undefined;
    }
};

fn suiteEnabled() !bool {
    const run = platform.env.getenvBool(run_environment);
    if (!run and platform.env.getenvBool(require_environment))
        return error.RequiredGlinerSpecialtyQualificationWasNotEnabled;
    return run;
}

fn requireMetalAvailable() !void {
    if (comptime !build_options.enable_metal or builtin.os.tag != .macos)
        return error.RequiredGlinerSpecialtyMetalUnavailable;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable())
        return error.RequiredGlinerSpecialtyMetalUnavailable;
}

fn readFixture(a: Allocator, name: []const u8) ![]u8 {
    if (platform.env.getenv(fixture_root_environment)) |root| {
        const path = try std.fs.path.join(a, &.{ root, name });
        defer a.free(path);
        return c_file.readFileMax(a, path, 64 * 1024 * 1024);
    }
    for ([_][]const u8{ default_fixture_root, "pkg/inference/testdata/gliner_specialty", "testdata/gliner_specialty" }) |root| {
        const path = try std.fs.path.join(a, &.{ root, name });
        defer a.free(path);
        return c_file.readFileMax(a, path, 64 * 1024 * 1024) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
    }
    return error.RequiredGlinerSpecialtyFixtureMissing;
}

fn loadCaptures(a: Allocator) !Captures {
    const pii_bytes = try readFixture(a, "pii_multi_oracle.json");
    defer a.free(pii_bytes);
    const guard_bytes = try readFixture(a, "gliguard_oracle.json");
    defer a.free(guard_bytes);
    var pii = try std.json.parseFromSlice(Value, a, pii_bytes, .{});
    errdefer pii.deinit();
    const guard = try std.json.parseFromSlice(Value, a, guard_bytes, .{});
    return .{ .pii = pii, .guard = guard };
}

fn hardLink(a: Allocator, source: []const u8, destination: []const u8) !void {
    const source_z = try a.dupeSentinel(u8, source, 0);
    defer a.free(source_z);
    const destination_z = try a.dupeSentinel(u8, destination, 0);
    defer a.free(destination_z);
    if (c_file.c.link(source_z.ptr, destination_z.ptr) != 0)
        return error.GlinerSpecialtyFixtureLinkFailed;
}

fn sourceModelRoot() []const u8 {
    return platform.env.getenv(model_root_environment) orelse default_model_root;
}

/// Publish the exact original files through the same synthesized managed
/// manifest path used after a registry pull. Hard links keep this qualification
/// fixture cheap without weakening live-byte identity checks.
fn prepareModel(a: Allocator, temporary: *std.testing.TmpDir, source_root: []const u8, name: []const u8) ![]u8 {
    const io = std.testing.io;
    const encoder_relative = try std.fs.path.join(a, &.{ name, "encoder_config" });
    defer a.free(encoder_relative);
    try temporary.dir.createDirPath(io, encoder_relative);
    const root = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    inline for (.{ "config.json", "encoder_config/config.json", "model.safetensors", "tokenizer.json", "tokenizer_config.json" }) |file| {
        const from = try std.fs.path.join(a, &.{ source_root, name, file });
        defer a.free(from);
        const to = try std.fs.path.join(a, &.{ root, name, file });
        defer a.free(to);
        try hardLink(a, from, to);
    }
    const directory = try std.fs.path.join(a, &.{ root, name });
    errdefer a.free(directory);
    const manifest_json = try registry.synthesizePulledModelManifestJson(a, directory, null, null);
    defer a.free(manifest_json);
    const manifest_relative = try std.fs.path.join(a, &.{ name, "model_manifest.json" });
    defer a.free(manifest_relative);
    try temporary.dir.writeFile(io, .{ .sub_path = manifest_relative, .data = manifest_json });
    return directory;
}

fn responseAt(transport: *Loopback, path: []const u8, body: []const u8) ![]u8 {
    var result = try transport.request(.POST, path, body);
    defer result.deinit();
    errdefer std.debug.print("GLiNER specialty HTTP: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), result.status.code);
    return std.testing.allocator.dupe(u8, result.body orelse return error.MissingResponseBody);
}

fn expectRejected(transport: *Loopback, path: []const u8, body: []const u8, status: u16, code: []const u8) !void {
    var result = try transport.request(.POST, path, body);
    defer result.deinit();
    errdefer std.debug.print("GLiNER specialty expected rejection: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(status, result.status.code);
    var parsed = try std.json.parseFromSlice(Value, std.testing.allocator, result.body orelse return error.MissingResponseBody, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value == .object);
    try std.testing.expectEqualStrings(code, parsed.value.object.get("error").?.string);
    try std.testing.expect(!parsed.value.object.contains("data"));
}

fn requestBody(a: Allocator, model: []const u8, id: []const u8, text: []const u8, schema: Value) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = model,
        .schema = schema,
        .inputs = .{.{ .id = id, .content = text }},
        .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
    }, .{});
}

fn windowedRequestBody(a: Allocator, model: []const u8, id: []const u8, text: []const u8, schema: Value) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = model,
        .schema = schema,
        .inputs = .{.{ .id = id, .content = text, .options = .{ .long_document = .{ .mode = "window" } } }},
        .options = .{ .include_confidence = true, .include_spans = true },
    }, .{});
}

fn piiSchema(a: Allocator, labels: Value) !std.json.Parsed(Value) {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .entities = labels }, .{});
    defer a.free(bytes);
    return std.json.parseFromSlice(Value, a, bytes, .{});
}

fn piiCaseSchema(a: Allocator, case: Value) !std.json.Parsed(Value) {
    const labels = case.object.get("labels").?.array.items;
    const thresholds = if (case.object.get("entity_thresholds")) |value| value.object else null;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    try bytes.appendSlice(a, "{\"entities\":[");
    for (labels, 0..) |label, i| {
        if (i != 0) try bytes.append(a, ',');
        try appendJson(&bytes, a, label);
    }
    try bytes.append(a, ']');
    if (thresholds) |values| if (values.count() != 0) {
        try bytes.appendSlice(a, ",\"entity_definitions\":{");
        var first = true;
        for (labels) |label| if (values.get(label.string)) |threshold| {
            if (!first) try bytes.append(a, ',');
            first = false;
            try appendJson(&bytes, a, label);
            try bytes.appendSlice(a, ":{\"threshold\":");
            try appendJson(&bytes, a, threshold);
            try bytes.append(a, '}');
        };
        try bytes.append(a, '}');
    };
    try bytes.append(a, '}');
    return std.json.parseFromSlice(Value, a, bytes.items, .{});
}

fn assertPiiWireCase(a: Allocator, body: []const u8, case: Value) !void {
    var parsed = try extraction_v2.parseJson(a, body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.items.len);
    const entities = parsed.items[0].compiled.schema.entities;
    const labels = case.object.get("labels").?.array.items;
    try std.testing.expectEqual(labels.len, entities.len);
    const thresholds = if (case.object.get("entity_thresholds")) |value| value.object else null;
    for (labels, entities) |label, entity| {
        try std.testing.expectEqualStrings(label.string, entity.name);
        const expected = if (thresholds) |values| values.get(label.string) else null;
        if (expected) |threshold| {
            try std.testing.expect(entity.threshold != null);
            try std.testing.expectApproxEqAbs(try number(threshold), entity.threshold.?, 1e-12);
        } else try std.testing.expect(entity.threshold == null);
    }
}

test "GLiNER specialty per-label thresholds use public entity definitions" {
    const a = std.testing.allocator;
    var case = try std.json.parseFromSlice(Value, a,
        \\{"id":"thresholds","text":"Ada works at Antfly","labels":["person","organization"],"entity_thresholds":{"person":0.75}}
    , .{});
    defer case.deinit();
    var schema = try piiCaseSchema(a, case.value);
    defer schema.deinit();
    const body = try piiCaseRequestBody(a, case.value, schema.value);
    defer a.free(body);
    try assertPiiWireCase(a, body, case.value);
}

fn piiCaseRequestBody(a: Allocator, case: Value, schema: Value) ![]u8 {
    var options = std.json.ObjectMap{};
    defer options.deinit(a);
    try options.put(a, "include_confidence", .{ .bool = true });
    try options.put(a, "include_spans", .{ .bool = true });
    try options.put(a, "offset_unit", .{ .string = "unicode_codepoints" });
    if (case.object.get("request_options")) |configured| {
        var iterator = configured.object.iterator();
        while (iterator.next()) |entry| try options.put(a, entry.key_ptr.*, entry.value_ptr.*);
    }
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = pii_directory,
        .schema = schema,
        .inputs = .{.{ .id = case.object.get("id").?.string, .content = case.object.get("text").?.string }},
        .options = Value{ .object = options },
    }, .{});
}

fn piiDefaultByteRequestBody(a: Allocator, case: Value, schema: Value) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = pii_directory,
        .schema = schema,
        .inputs = .{.{ .id = case.object.get("id").?.string, .content = case.object.get("text").?.string }},
        .options = .{ .include_confidence = true, .include_spans = true },
    }, .{});
}

fn codepointByteOffset(text_value: []const u8, target: usize) !usize {
    var view = try std.unicode.Utf8View.init(text_value);
    var iterator = view.iterator();
    var count: usize = 0;
    while (iterator.nextCodepointSlice()) |slice| {
        if (count == target) return @intFromPtr(slice.ptr) - @intFromPtr(text_value.ptr);
        count += 1;
    }
    if (count == target) return text_value.len;
    return error.InvalidGlinerSpecialtyFixture;
}

fn qualifyDefaultByteOffsets(a: Allocator, transport: *Loopback, case: Value, tolerance: f64) !void {
    var schema = try piiCaseSchema(a, case);
    defer schema.deinit();
    const body = try piiDefaultByteRequestBody(a, case, schema.value);
    defer a.free(body);
    const response = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(response);
    var parsed = try std.json.parseFromSlice(Value, a, response, .{});
    defer parsed.deinit();
    const actual = parsed.value.object.get("data").?.array.items[0].object.get("entities").?.array.items;
    const expected = case.object.get("presented_output").?.object.get("entities").?.object;
    const text_value = case.object.get("text").?.string;
    var index: usize = 0;
    for (case.object.get("labels").?.array.items) |label| for (expected.get(label.string).?.array.items) |want| {
        const got = actual[index].object;
        try std.testing.expectEqualStrings(label.string, got.get("label").?.string);
        try std.testing.expectEqual(try codepointByteOffset(text_value, @intCast(want.object.get("start").?.integer)), @as(usize, @intCast(got.get("start").?.integer)));
        try std.testing.expectEqual(try codepointByteOffset(text_value, @intCast(want.object.get("end").?.integer)), @as(usize, @intCast(got.get("end").?.integer)));
        try std.testing.expectApproxEqAbs(try number(want.object.get("confidence").?), try number(got.get("score").?), tolerance);
        index += 1;
    };
    try std.testing.expectEqual(index, actual.len);
}

fn appendJson(bytes: *std.ArrayList(u8), a: Allocator, value: anytype) !void {
    const serialized = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(serialized);
    try bytes.appendSlice(a, serialized);
}

fn classificationSchema(a: Allocator, case: Value) !std.json.Parsed(Value) {
    const heads = case.object.get("heads") orelse return error.InvalidGlinerSpecialtyFixture;
    if (heads != .array) return error.InvalidGlinerSpecialtyFixture;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    try bytes.appendSlice(a, "{\"classifications\":[");
    for (heads.array.items, 0..) |head, i| {
        if (i != 0) try bytes.append(a, ',');
        const config = head.object;
        try bytes.appendSlice(a, "{\"name\":");
        try appendJson(&bytes, a, config.get("task").?);
        const labels = config.get("labels") orelse return error.InvalidGlinerSpecialtyFixture;
        try bytes.appendSlice(a, ",\"labels\":");
        try appendJson(&bytes, a, labels);
        const multi = config.get("multi_label").?.bool;
        if (multi) {
            try bytes.appendSlice(a, ",\"multi_label\":true,\"top_k\":");
            try appendJson(&bytes, a, labels.array.items.len);
        }
        try bytes.appendSlice(a, ",\"threshold\":");
        try appendJson(&bytes, a, config.get("threshold").?);
        try bytes.appendSlice(a, ",\"activation\":");
        try appendJson(&bytes, a, config.get("activation").?);
        try bytes.append(a, '}');
    }
    try bytes.appendSlice(a, "]}");
    return std.json.parseFromSlice(Value, a, bytes.items, .{});
}

fn number(value: Value) !f64 {
    return switch (value) {
        .float => |v| v,
        .integer => |v| @floatFromInt(v),
        else => error.InvalidGlinerSpecialtyFixture,
    };
}

fn expectPiiOracle(actual_json: []const u8, case: Value, context: NumericContext, tolerance: f64) !void {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Value, a, actual_json, .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    const actual = rows[0].object.get("entities") orelse Value{ .array = .init(a) };
    const expected = case.object.get("presented_output").?.object.get("entities").?.object;
    var expected_count: usize = 0;
    var max_error: f64 = 0;
    for (case.object.get("labels").?.array.items) |label| for ((expected.get(label.string) orelse return error.MissingGlinerSpecialtyEntityGroup).array.items) |want| {
        const left = want.object;
        if (expected_count >= actual.array.items.len) return error.MissingGlinerSpecialtyEntity;
        const right = actual.array.items[expected_count].object;
        try std.testing.expectEqualStrings(label.string, right.get("label").?.string);
        try std.testing.expectEqualStrings(left.get("text").?.string, right.get("text").?.string);
        try std.testing.expectEqual(left.get("start").?.integer, right.get("start").?.integer);
        try std.testing.expectEqual(left.get("end").?.integer, right.get("end").?.integer);
        expected_count += 1;
        const expected_score = try number(left.get("confidence").?);
        const actual_score = try number(right.get("score").?);
        max_error = @max(max_error, @abs(expected_score - actual_score));
        try expectClose(context, "http_presented_entity_probability", label.string, left.get("text").?.string, null, expected_score, actual_score, tolerance);
    };
    try std.testing.expectEqual(expected_count, actual.array.items.len);
    std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=http_presented_entity_probability max_abs_error={d} tolerance={d}\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id, max_error, tolerance });
}

fn expectGuardOracle(actual_json: []const u8, case: Value, context: NumericContext, tolerance: f64) !void {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(Value, a, actual_json, .{});
    defer parsed.deinit();
    const row = parsed.value.object.get("data").?.array.items[0].object;
    const actual = row.get("classifications").?.array.items;
    const upstream = case.object.get("presented_output").?.object;
    var expected_count: usize = 0;
    var max_error: f64 = 0;
    for (case.object.get("heads").?.array.items) |head| {
        const object = head.object;
        const labels = object.get("labels").?.array.items;
        const probabilities = object.get("probabilities").?.object.get("values").?.array.items;
        const multi = object.get("multi_label").?.bool;
        const threshold = try number(object.get("threshold").?);
        var best: usize = 0;
        var above_threshold = false;
        for (probabilities, 0..) |probability, i| if (try number(probability) > try number(probabilities[best])) {
            best = i;
        };
        if (multi) for (probabilities) |probability| if (try number(probability) >= threshold) {
            above_threshold = true;
            break;
        };
        for (labels, probabilities, 0..) |label, probability, i| {
            const selected = if (multi)
                (if (above_threshold) try number(probability) >= threshold else i == best)
            else
                i == best;
            if (!selected) continue;
            const presented = upstream.get(object.get("task").?.string) orelse return error.MissingGlinerSpecialtyClassification;
            if (multi) {
                var captured = false;
                for (presented.array.items) |value| if (std.mem.eql(u8, value.string, label.string)) {
                    captured = true;
                    break;
                };
                try std.testing.expect(captured);
            } else try std.testing.expectEqualStrings(presented.string, label.string);
            if (expected_count >= actual.len) return error.MissingGlinerSpecialtyClassification;
            const found = actual[expected_count];
            try std.testing.expectEqualStrings(object.get("task").?.string, found.object.get("name").?.string);
            try std.testing.expectEqualStrings(label.string, found.object.get("label").?.string);
            const expected_score = try number(probability);
            const actual_score = try number(found.object.get("score").?);
            max_error = @max(max_error, @abs(expected_score - actual_score));
            try expectClose(context, "http_presented_classification_probability", object.get("task").?.string, label.string, i, expected_score, actual_score, tolerance);
            expected_count += 1;
        }
    }
    var upstream_count: usize = 0;
    var upstream_tasks = upstream.iterator();
    while (upstream_tasks.next()) |task| upstream_count += if (task.value_ptr.* == .array) task.value_ptr.array.items.len else 1;
    try std.testing.expectEqual(upstream_count, expected_count);
    try std.testing.expectEqual(expected_count, actual.len);
    std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=http_presented_classification_probability max_abs_error={d} tolerance={d}\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id, max_error, tolerance });
}

fn configureBackend(node: *Node, backend: Backend) void {
    switch (backend) {
        .native => native_backend.useNativeBackend(node),
        .metal => {
            node.session_manager.preferred_backends = &.{.metal};
            node.session_manager.required_backend = .metal;
            node.session_manager.required_backend_invalid = false;
            node.model_manager.session_manager.preferred_backends = &.{.metal};
            node.model_manager.session_manager.required_backend = .metal;
            node.model_manager.session_manager.required_backend_invalid = false;
        },
    }
}

fn verifyIdentity(node: *Node, directory: []const u8, backend: Backend, profile: specialty.Profile) !void {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    try std.testing.expectEqual(@as(@import("../backends/backends.zig").BackendType, switch (backend) {
        .native => .native,
        .metal => .metal,
    }), handle.get().session.backend());
    try std.testing.expectEqual(profile, handle.get().manifest.gliner_specialty_profile.?);
    const identity = try factory.getGlinerSpecialtyIdentity(handle.get().session);
    try std.testing.expectEqual(profile, try specialty.require(identity));
    const span_config = try factory.getGlinerSpanConfig(handle.get().session);
    switch (span_config) {
        .deberta => |config| try std.testing.expectEqual(
            profile == .pii_multi,
            config.metal_require_f32_weight_mirrors,
        ),
        .modern_bert => return error.SpecialtyExpectedDebertaRuntime,
    }
}

fn compareEnvelope(expected: []const u8, actual: []const u8, tolerance: f64) !void {
    const a = std.testing.allocator;
    var left = try std.json.parseFromSlice(Value, a, expected, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, a, actual, .{});
    defer right.deinit();
    try family_batch.equalBatchValue(left.value, right.value, tolerance);
}

fn preparedTokenCount(a: Allocator, tokenizer: @import("inference_tokenizer").Tokenizer, model: []const u8, schema: Value, text: []const u8, span_profile: bool) !usize {
    const body = try requestBody(a, model, "geometry", text, schema);
    defer a.free(body);
    var request = try extraction_v2.parseJson(a, body, .{});
    defer request.deinit();
    // This tokenizer-only measuring plan is deliberately wider so it can
    // identify the first rejected geometry. The public HTTP request below
    // retains the specialty runtime's immutable 512-token hard limit.
    var options = span_executor.Options{
        .processor = .{ .max_sequence_tokens = 2048, .max_batch_tokens = 2048 },
        .max_prompt_tokens = 2048,
        .max_sequences_per_item = 1,
        .max_request_tokens = 2048,
    };
    if (span_profile) options.span_extraction_profile = .{};
    var plan = try span_executor.plan(a, tokenizer, &request, options);
    defer plan.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), plan.items.len);
    try std.testing.expectEqual(@as(usize, 1), plan.items[0].batches.len);
    return plan.items[0].batches[0].samples[0].input_ids.len;
}

fn repeatedText(a: Allocator, word: []const u8, count: usize) ![]u8 {
    const words = try a.alloc([]const u8, count);
    defer a.free(words);
    @memset(words, word);
    return std.mem.join(a, " ", words);
}

/// Search with the held production tokenizer rather than assuming one token
/// per source word. The selected text is re-planned and asserted by the caller.
fn textForPreparedTokens(a: Allocator, tokenizer: @import("inference_tokenizer").Tokenizer, model: []const u8, schema: Value, target: usize, span_profile: bool) ![]u8 {
    for ([_][]const u8{ "a", "token", "hello" }) |word| {
        var low: usize = 1;
        var high: usize = 2 * target;
        while (low <= high) {
            const count = low + (high - low) / 2;
            const text = try repeatedText(a, word, count);
            const prepared = try preparedTokenCount(a, tokenizer, model, schema, text, span_profile);
            if (prepared == target) return text;
            a.free(text);
            if (prepared < target) {
                low = count + 1;
            } else if (count == 0) {
                break;
            } else {
                high = count - 1;
            }
        }
    }
    return error.ExactPreparedTokenGeometryUnavailable;
}

fn expectFiniteJson(value: Value) !void {
    switch (value) {
        .float => |number_value| try std.testing.expect(std.math.isFinite(number_value)),
        .array => |array| for (array.items) |item| try expectFiniteJson(item),
        .object => |object| {
            var iterator = object.iterator();
            while (iterator.next()) |entry| try expectFiniteJson(entry.value_ptr.*);
        },
        else => {},
    }
}

fn qualifyPreparedBoundary(
    a: Allocator,
    node: *Node,
    transport: *Loopback,
    directory: []const u8,
    model: []const u8,
    schema: Value,
    span_profile: bool,
    backend: Backend,
    native_expected: ?[]const u8,
) ![]u8 {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const tokenizer = handle.get().getTokenizer();
    const accepted_text = try textForPreparedTokens(a, tokenizer, model, schema, 512, span_profile);
    defer a.free(accepted_text);
    const rejected_text = try textForPreparedTokens(a, tokenizer, model, schema, 513, span_profile);
    defer a.free(rejected_text);
    try std.testing.expectEqual(@as(usize, 512), try preparedTokenCount(a, tokenizer, model, schema, accepted_text, span_profile));
    try std.testing.expectEqual(@as(usize, 513), try preparedTokenCount(a, tokenizer, model, schema, rejected_text, span_profile));

    const accepted_body = try requestBody(a, model, "prepared-512", accepted_text, schema);
    defer a.free(accepted_body);
    const accepted = try responseAt(transport, "/ai/v1/extract", accepted_body);
    errdefer a.free(accepted);
    var parsed = try std.json.parseFromSlice(Value, a, accepted, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.object.get("data").?.array.items.len);
    try expectFiniteJson(parsed.value);
    if (native_expected) |expected| try compareEnvelope(expected, accepted, 5e-3);

    const rejected_body = try requestBody(a, model, "prepared-513", rejected_text, schema);
    defer a.free(rejected_body);
    try expectRejected(transport, "/ai/v1/extract", rejected_body, 413, "EXTRACTION_LIMIT_EXCEEDED");
    std.debug.print("GLiNER specialty prepared boundary backend={s} model={s} accepted_tokens=512 rejected_tokens=513 finite=true\n", .{ @tagName(backend), model });
    return accepted;
}

fn expectInputIds(sample: span_processor.Sample, case: Value) !void {
    const expected = case.object.get("preprocessing").?.object.get("active_input_ids").?.array.items;
    try std.testing.expectEqual(expected.len, sample.input_ids.len);
    for (expected, sample.input_ids) |want, got| try std.testing.expectEqual(want.integer, got);
}

fn taskCounts(a: Allocator, item: *const extraction_v2.Item) ![]usize {
    const counts = try a.alloc(usize, item.compiled.schema.classifications.len);
    for (item.compiled.schema.classifications, counts) |classification, *count|
        count.* = classification.task.labels.len;
    return counts;
}

fn expectRawClassificationRows(rows: []const []const f64, case: Value, context: NumericContext, tolerance: f64) !void {
    const heads = case.object.get("heads").?.array.items;
    try std.testing.expectEqual(heads.len, rows.len);
    var max_error: f64 = 0;
    for (heads, rows) |head, row| {
        const expected = head.object.get("raw_logits").?.object.get("values").?.array.items;
        const labels = head.object.get("labels").?.array.items;
        try std.testing.expectEqual(expected.len, row.len);
        for (expected, row, 0..) |want, got, i| {
            const expected_logit = try number(want);
            max_error = @max(max_error, @abs(expected_logit - got));
            try expectClose(context, "direct_classifier_raw_logit", head.object.get("task").?.string, labels[i].string, i, expected_logit, got, tolerance);
        }
    }
    std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=direct_classifier_raw_logit max_abs_error={d} tolerance={d}\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id, max_error, tolerance });
}

fn freeClassificationRows(a: Allocator, rows: [][]f64) void {
    for (rows) |row| a.free(row);
    a.free(rows);
}

fn executorConfig(session: @import("../backends/session.zig").Session) !span_executor.EncoderConfig {
    return switch (try factory.getGlinerSpanConfig(session)) {
        .deberta => |config| .{ .deberta = config },
        .modern_bert => |config| .{ .modern_bert = config },
    };
}

/// Freeze both the tokenizer boundary and classifier head beneath the HTTP
/// presentation layer. The batched call uses all seven ragged schemas in one
/// physical encoder forward; public 8/9-item chunking is checked separately.
fn directGuardParity(a: Allocator, node: *Node, directory: []const u8, cases: []const Value, schemas: []const std.json.Parsed(Value), backend: Backend, tolerance: f64) !void {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const loaded = handle.get();
    const config = try executorConfig(loaded.session);
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    const control = Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 10 * 60 * std.time.ns_per_s, .hard_cancellation = watchdog.boundary() };
    const execution_mutex = loaded.targetInferenceExecutionMutex();
    if (execution_mutex) |mutex| try control.lock(mutex);
    defer if (execution_mutex) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(loaded.session, a, null, control);
    defer managed.deinit();

    // Exercise the public singleton head independently of its batched wrapper.
    {
        const case = cases[0];
        const body = try requestBody(a, guard_directory, case.object.get("id").?.string, case.object.get("text").?.string, schemas[0].value);
        defer a.free(body);
        var request = try extraction_v2.parseJson(a, body, .{});
        defer request.deinit();
        var plan = try span_executor.plan(a, loaded.getTokenizer(), &request, .{ .max_prompt_tokens = 512, .max_sequences_per_item = 1 });
        defer plan.deinit(a);
        const sample = plan.items[0].batches[0].samples[0];
        try expectInputIds(sample, case);
        const counts = try taskCounts(a, &request.items[0]);
        defer a.free(counts);
        const rows = try span_executor.classificationLogits(&managed.backend, a, config, sample, counts);
        defer freeClassificationRows(a, rows);
        try expectRawClassificationRows(rows, case, .{ .backend = backend, .profile = .gliguard, .case_id = case.object.get("id").?.string }, tolerance);
    }

    const requests = try a.alloc(extraction_v2.Request, cases.len);
    const plans = a.alloc(span_executor.Plan, cases.len) catch |err| {
        a.free(requests);
        return err;
    };
    const samples = a.alloc(span_processor.Sample, cases.len) catch |err| {
        a.free(plans);
        a.free(requests);
        return err;
    };
    const counts = a.alloc([]const usize, cases.len) catch |err| {
        a.free(samples);
        a.free(plans);
        a.free(requests);
        return err;
    };
    defer {
        a.free(counts);
        a.free(samples);
        a.free(plans);
        a.free(requests);
    }
    var built: usize = 0;
    defer for (0..built) |i| {
        a.free(counts[i]);
        plans[i].deinit(a);
        requests[i].deinit();
    };
    for (cases, schemas, 0..) |case, schema, i| {
        const body = try requestBody(a, guard_directory, case.object.get("id").?.string, case.object.get("text").?.string, schema.value);
        defer a.free(body);
        requests[i] = try extraction_v2.parseJson(a, body, .{});
        errdefer requests[i].deinit();
        plans[i] = try span_executor.plan(a, loaded.getTokenizer(), &requests[i], .{ .max_prompt_tokens = 512, .max_sequences_per_item = 1 });
        errdefer plans[i].deinit(a);
        samples[i] = plans[i].items[0].batches[0].samples[0];
        try expectInputIds(samples[i], case);
        counts[i] = try taskCounts(a, &requests[i].items[0]);
        built += 1;
    }
    const batched = try span_executor.classificationBatchLogits(&managed.backend, a, config, samples, counts, null);
    defer {
        for (batched) |rows| freeClassificationRows(a, rows);
        a.free(batched);
    }
    try std.testing.expectEqual(cases.len, batched.len);
    for (batched, cases) |rows, case| try expectRawClassificationRows(rows, case, .{ .backend = backend, .profile = .gliguard, .case_id = case.object.get("id").?.string }, tolerance);
}

/// PII's first capture predicts one entity instance, so slot zero is the exact
/// upstream CountLSTM projection used by the published raw span tensor. Compare
/// every count logit and every valid label/word/width score after layout conversion.
fn directPiiParity(a: Allocator, node: *Node, directory: []const u8, cases: []const Value, backend: Backend, tolerance: f64) !void {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const loaded = handle.get();
    const config = try executorConfig(loaded.session);
    const H: usize = config.hiddenSize();
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    const control = Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 10 * 60 * std.time.ns_per_s, .hard_cancellation = watchdog.boundary() };
    const execution_mutex = loaded.targetInferenceExecutionMutex();
    if (execution_mutex) |mutex| try control.lock(mutex);
    defer if (execution_mutex) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(loaded.session, a, null, control);
    defer managed.deinit();

    for (cases) |case| {
        var schema = try piiCaseSchema(a, case);
        defer schema.deinit();
        const body = try piiCaseRequestBody(a, case, schema.value);
        defer a.free(body);
        var request = try extraction_v2.parseJson(a, body, .{});
        defer request.deinit();
        var plan = try span_executor.plan(a, loaded.getTokenizer(), &request, .{ .span_extraction_profile = .{}, .max_sequences_per_item = 1 });
        defer plan.deinit(a);
        const sample = plan.items[0].batches[0].samples[0];
        try expectInputIds(sample, case);

        var head_scratch = std.heap.ArenaAllocator.init(a);
        defer head_scratch.deinit();
        const head_a = head_scratch.allocator();

        const head_capture = case.object.get("heads").?.array.items[0].object;
        const context = NumericContext{ .backend = backend, .profile = .pii_multi, .case_id = case.object.get("id").?.string };
        const predicted_count: usize = @intCast(head_capture.get("predicted_count").?.integer);
        const hidden = try span_executor.encodeSample(&managed.backend, a, config, sample);
        defer managed.backend.free(hidden);
        const group = sample.groups[0];
        const parent = try span_core.gather(&managed.backend, head_a, hidden, &.{group.parent_marker}, H);
        defer managed.backend.free(parent);
        const count_logits = try span_core.mlp(&managed.backend, head_a, parent, H, 20, "count_pred");
        defer managed.backend.free(count_logits);
        const actual_counts = try managed.backend.toFloat32(count_logits, a);
        defer a.free(actual_counts);
        const expected_counts = head_capture.get("count_logits").?.object.get("values").?.array.items;
        try std.testing.expectEqual(expected_counts.len, actual_counts.len);
        var count_max_error: f64 = 0;
        for (expected_counts, actual_counts, 0..) |want, got, count| {
            const expected_logit = try number(want);
            count_max_error = @max(count_max_error, @abs(expected_logit - got));
            try expectClose(context, "direct_count_logit", "entities", "predicted_count", count, expected_logit, got, tolerance);
        }
        var actual_count: usize = 0;
        for (actual_counts, 0..) |logit, count| if (logit > actual_counts[actual_count]) {
            actual_count = count;
        };
        try std.testing.expectEqual(predicted_count, actual_count);
        std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=direct_count_logit max_abs_error={d} tolerance={d}\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id, count_max_error, tolerance });

        if (predicted_count == 0) {
            try std.testing.expect(head_capture.get("raw_span_logits") == null);
            std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=direct_slot0_raw_span_logit zero_gate=true\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id });
            continue;
        }

        const spans = try span_core.spanRepresentations(&managed.backend, head_a, hidden, sample, H);
        defer managed.backend.free(spans);
        const labels = try span_core.gather(&managed.backend, head_a, hidden, group.child_markers, H);
        defer managed.backend.free(labels);
        const columns = group.child_markers.len;
        const projected = try gliner_head.countLstmV1AtStep(&managed.backend, a, labels, columns, H, 0);
        defer managed.backend.free(projected);
        const span_count = sample.words.len * 8;
        const score_tensor = if (managed.backend.kind() == .native)
            try managed.backend.linearNoBias(spans, projected, span_count, H, columns)
        else
            try managed.backend.primDotGeneral(spans, projected, &.{ @intCast(span_count), @intCast(H) }, &.{ @intCast(columns), @intCast(H) }, &.{1}, &.{1}, &.{}, &.{});
        defer managed.backend.free(score_tensor);
        const actual_scores = try managed.backend.toFloat32(score_tensor, a);
        defer a.free(actual_scores);
        const raw = head_capture.get("raw_span_logits").?.object;
        const shape = raw.get("shape").?.array.items;
        try std.testing.expectEqual(@as(usize, 3), shape.len);
        const slots: usize = @intCast(shape[0].integer);
        try std.testing.expect(slots >= 1);
        try std.testing.expectEqual(@as(i64, @intCast(columns)), shape[1].integer);
        var valid_span_count: usize = 0;
        for (0..sample.words.len) |word| valid_span_count += @min(8, sample.words.len - word);
        try std.testing.expectEqual(@as(i64, @intCast(valid_span_count)), shape[2].integer);
        const expected_scores = raw.get("values").?.array.items;
        try std.testing.expectEqual(slots * columns * valid_span_count, expected_scores.len);
        var span_max_error: f64 = 0;
        var packed_index: usize = 0;
        for (0..columns) |label| for (0..sample.words.len) |word| for (0..8) |width| {
            // Upstream gathers word zero for masked tail cells; the shared
            // native head clamps their end. Both decoders discard these
            // placeholders, so only valid candidates have a parity contract.
            if (word + width >= sample.words.len) continue;
            const want = try number(expected_scores[packed_index]);
            packed_index += 1;
            const got: f64 = actual_scores[(word * 8 + width) * columns + label];
            span_max_error = @max(span_max_error, @abs(want - got));
            try expectClose(context, "direct_slot0_raw_span_logit", "entities", case.object.get("labels").?.array.items[label].string, word * 8 + width, want, got, tolerance + 1e-5 * @abs(want));
        };
        try std.testing.expectEqual(columns * valid_span_count, packed_index);
        std.debug.print("GLiNER specialty parity backend={s} profile={s} case={s} stage=direct_slot0_raw_span_logit max_abs_error={d} base_tolerance={d}\n", .{ @tagName(context.backend), @tagName(context.profile), context.case_id, span_max_error, tolerance });
    }
    if (backend == .metal) {
        const provider = managed.backend.debugTimingSnapshot().provider;
        try std.testing.expect(provider.metal_runtime_dense_linear_f32_slots >= 72);
        try std.testing.expectEqual(@as(u64, 0), provider.metal_runtime_dense_linear_bf16_slots);
        try std.testing.expectEqual(@as(u64, 0), provider.metal_runtime_dense_linear_f16_slots);
        try std.testing.expectEqual(@as(u64, 0), provider.metal_runtime_dense_linear_bf16_weight_bytes);
        try std.testing.expectEqual(@as(u64, 0), provider.metal_runtime_dense_linear_f16_weight_bytes);
        if (comptime build_options.enable_metal) {
            // The runtime-wide counters also include the specialty head's
            // dynamically registered linears. Inspect the fixed encoder range
            // itself so this proves every one of its 72 slots retained F32.
            const compute: *const metal_compute.MetalCompute = @ptrCast(@alignCast(managed.backend.ptr));
            const metal_provider = compute.provider_impl;
            const runtime = metal_provider.raw_decode_runtime orelse return error.MetalRuntimeUnavailable;
            var encoder_weight_bytes: u64 = 0;
            for (0..72) |slot| {
                try std.testing.expect(metal_provider.raw_linear_slots_prepared[slot]);
                try std.testing.expectEqual(
                    @as(c_int, @backingInt(metal_runtime.DenseLinearSlotDtype.f32_mps)),
                    metal_runtime.termite_metal_decode_runtime_linear_slot_dtype(runtime, slot),
                );
                const elements = try std.math.mul(
                    u64,
                    @intCast(metal_provider.raw_linear_slot_in_dims[slot]),
                    @intCast(metal_provider.raw_linear_slot_out_dims[slot]),
                );
                encoder_weight_bytes = try std.math.add(u64, encoder_weight_bytes, try std.math.mul(u64, elements, @sizeOf(f32)));
            }
            try std.testing.expectEqual(@as(u64, 339_738_624), encoder_weight_bytes);
            try std.testing.expect(provider.metal_runtime_dense_linear_f32_weight_bytes >= encoder_weight_bytes);
        }
        const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
        const admitted = domain.admission.snapshot();
        const owned = metal.memoryStatsSnapshot();
        try std.testing.expect(provider.metal_runtime_total_bytes + owned.device_owned_live_bytes <= @as(u64, @intCast(admitted.backend_weight_bytes + admitted.backend_scratch_bytes)));
        try std.testing.expect(owned.host_mirror_live_bytes <= @as(u64, @intCast(admitted.host_weight_bytes + admitted.host_scratch_bytes)));
        std.debug.print("GLiNER specialty PII Metal mirrors encoder_f32_slots=72 encoder_f32_weight_bytes=339738624 total_f32_slots={d} bf16_slots=0 f16_slots=0\n", .{provider.metal_runtime_dense_linear_f32_slots});
    }
}

fn rejectionChecks(a: Allocator, transport: *Loopback, pii_name: []const u8, guard_name: []const u8) !void {
    var classification = try std.json.parseFromSlice(Value, a, "{\"classifications\":[{\"name\":\"safety\",\"labels\":[\"safe\",\"unsafe\"]}]}", .{});
    defer classification.deinit();
    const pii_wrong = try requestBody(a, pii_name, "unsupported", "hello", classification.value);
    defer a.free(pii_wrong);
    try expectRejected(transport, "/ai/v1/extract", pii_wrong, 400, "UNSUPPORTED_EXTRACTION_FEATURE");

    var entities = try std.json.parseFromSlice(Value, a, "{\"entities\":[\"person\"]}", .{});
    defer entities.deinit();
    const guard_wrong = try requestBody(a, guard_name, "unsupported", "Ada", entities.value);
    defer a.free(guard_wrong);
    try expectRejected(transport, "/ai/v1/extract", guard_wrong, 400, "UNSUPPORTED_EXTRACTION_FEATURE");

    const decide = try std.json.Stringify.valueAlloc(a, .{
        .model = guard_name,
        .input = "hello",
        .questions = .{.{ .name = "safe", .type = "predicate", .instructions = "Is this safe?" }},
    }, .{});
    defer a.free(decide);
    try expectRejected(transport, "/ai/v1/decisions", decide, 400, "INVALID_MODEL");

    const empty = try requestBody(a, pii_name, "empty", " \t\r\n", entities.value);
    defer a.free(empty);
    try expectRejected(transport, "/ai/v1/extract", empty, 400, "INVALID_EXTRACTION_REQUEST");

    const long_words = try a.alloc([]const u8, 600);
    defer a.free(long_words);
    @memset(long_words, "token");
    const long_text = try std.mem.join(a, " ", long_words);
    defer a.free(long_text);
    const pii_long = try requestBody(a, pii_name, "over-512", long_text, entities.value);
    defer a.free(pii_long);
    try expectRejected(transport, "/ai/v1/extract", pii_long, 413, "EXTRACTION_LIMIT_EXCEEDED");
    const guard_long = try requestBody(a, guard_name, "over-512", long_text, classification.value);
    defer a.free(guard_long);
    try expectRejected(transport, "/ai/v1/extract", guard_long, 413, "EXTRACTION_LIMIT_EXCEEDED");

    const pii_window = try windowedRequestBody(a, pii_name, "window", "Ada", entities.value);
    defer a.free(pii_window);
    const pii_before = metal.memoryStatsSnapshot();
    try expectRejected(transport, "/ai/v1/extract", pii_window, 400, "UNSUPPORTED_EXTRACTION_FEATURE");
    try transport.idle();
    try expectNoMetalDeviceWork("pii_window_rejection", pii_before, metal.memoryStatsSnapshot());
    const guard_window = try windowedRequestBody(a, guard_name, "window", "hello", classification.value);
    defer a.free(guard_window);
    const guard_before = metal.memoryStatsSnapshot();
    try expectRejected(transport, "/ai/v1/extract", guard_window, 400, "UNSUPPORTED_EXTRACTION_FEATURE");
    try transport.idle();
    try expectNoMetalDeviceWork("guard_window_rejection", guard_before, metal.memoryStatsSnapshot());
}

const Call = struct {
    transport: *Loopback,
    body: []const u8,
    expected: []const u8,

    fn run(self: Call) anyerror!void {
        const actual = try responseAt(self.transport, "/ai/v1/extract", self.body);
        defer std.testing.allocator.free(actual);
        try compareEnvelope(self.expected, actual, 5e-4);
    }
};

fn lifecycle(a: Allocator, node: *Node, transport: *Loopback, target_dir: []const u8, pinned_dir: []const u8, body: []const u8, expected: []const u8) !void {
    var stage: []const u8 = "concurrent_requests";
    errdefer std.debug.print("GLiNER specialty lifecycle failed stage={s}\n", .{stage});
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    var driver = Io.Threaded.init(a, .{ .concurrent_limit = .limited(2) });
    defer driver.deinit();
    var futures: [2]Io.Future(anyerror!void) = undefined;
    var started: usize = 0;
    defer for (futures[0..started]) |*future| future.await(driver.io()) catch {};
    for (&futures) |*future| {
        future.* = try driver.io().concurrent(Call.run, .{Call{ .transport = transport, .body = body, .expected = expected }});
        started += 1;
    }
    var first_error: ?anyerror = null;
    for (&futures) |*future| future.await(driver.io()) catch |err| {
        if (first_error == null) first_error = err;
    };
    started = 0;
    if (first_error) |err| return err;
    try transport.idle();
    try expectAdmissionRestored(stage, retained, domain.admission.snapshot());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());

    // Keep both resident specialty models pinned. Scratch acquisition is
    // eviction-aware; without the PII pin a denied Guard request may evict the
    // idle PII model and legitimately move Metal release counters.
    var pinned_handle = node.model_manager.acquireLoadedModel(pinned_dir) orelse return error.MissingLoadedModel;
    defer pinned_handle.release();
    var target_handle = node.model_manager.acquireLoadedModel(target_dir) orelse return error.MissingLoadedModel;
    defer target_handle.release();
    const limits = node.config.generation_budget_overrides.apply(.{});
    {
        stage = "scratch_denial";
        const gpu = domain.admission.snapshotBackend(.gpu);
        try std.testing.expect(gpu.scratchTotalBytes() < limits.scratch_limit_bytes);
        var pressure = try domain.admission.tryAcquire(.gpu, limits, .{
            .backend_scratch_bytes = limits.scratch_limit_bytes - gpu.scratchTotalBytes(),
        }, false);
        defer pressure.release();
        const before = metal.memoryStatsSnapshot();
        var rejected = try transport.request(.POST, "/ai/v1/extract", body);
        defer rejected.deinit();
        try std.testing.expectEqual(@as(u16, 503), rejected.status.code);
        try std.testing.expectEqualStrings("1", rejected.header("Retry-After").?);
        var busy = try std.json.parseFromSlice(Value, a, rejected.body orelse return error.MissingResponseBody, .{});
        defer busy.deinit();
        try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy.value.object.get("error").?.string);
        try std.testing.expect(busy.value.object.get("retryable").?.bool);
        try transport.idle();
        try expectNoMetalDeviceWork(stage, before, metal.memoryStatsSnapshot());
    }
    stage = "scratch_denial_release";
    try transport.idle();
    try expectAdmissionRestored(stage, retained, domain.admission.snapshot());
    stage = "scratch_denial_retry";
    const retry = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(retry);
    try compareEnvelope(expected, retry, 5e-4);
    // responseAt observes a complete HTTP response, but the request worker can
    // still be unwinding its final retained Metal views. Drain it before the
    // queued-cancellation no-device-work baseline.
    try transport.idle();

    stage = "queued_cancellation_setup";
    const mutex = target_handle.get().targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
    const control = Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s };
    try control.lock(mutex);
    var held = true;
    defer if (held) mutex.unlock();
    const before = metal.memoryStatsSnapshot();
    var peer = try transport.connect();
    var open = true;
    defer if (open) peer.close();
    try peer.setSendTimeout(5_000);
    var header_buffer: [256]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buffer, "POST /ai/v1/extract HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});
    try peer.sendAll(header);
    try peer.sendAll(body);
    while (true) {
        try control.check();
        try control.lock(&node.model_manager.load_lock);
        const active = target_handle.get().active_handles;
        node.model_manager.load_lock.unlock();
        if (active == 2) break;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
    try std.posix.setsockopt(peer.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
    peer.close();
    open = false;
    stage = "queued_cancellation_drain";
    try transport.idle();
    try control.lock(&node.model_manager.load_lock);
    const active_after_cancel = target_handle.get().active_handles;
    node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), active_after_cancel);
    try expectAdmissionRestored(stage, retained, domain.admission.snapshot());
    try expectNoMetalDeviceWork(stage, before, metal.memoryStatsSnapshot());
    mutex.unlock();
    held = false;
    stage = "queued_cancellation_retry";
    const after_cancel = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(after_cancel);
    try compareEnvelope(expected, after_cancel, 5e-4);
}

fn qualifyMaxBatch(
    a: Allocator,
    transport: *Loopback,
    model: []const u8,
    cases: []const family_batch.BatchCase,
    singleton_outputs: []const []u8,
    expected_readbacks: u64,
) !void {
    const maximum: usize = 128;
    const body = try family_batch.batchBody(a, model, cases, maximum, false);
    defer a.free(body);
    const before = metal.memoryStatsSnapshot();
    const actual = try responseAt(transport, "/ai/v1/extract", body);
    defer a.free(actual);
    try transport.idle();
    const after = metal.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls + expected_readbacks, after.to_host_device_calls);
    var parsed = try std.json.parseFromSlice(Value, a, actual, .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(maximum, rows.len);
    for (rows, 0..) |row, i| {
        var id_buffer: [32]u8 = undefined;
        const expected_id = try std.fmt.bufPrint(&id_buffer, "row-{d}", .{i});
        try std.testing.expectEqualStrings(expected_id, row.object.get("id").?.string);
        var singleton = try std.json.parseFromSlice(Value, a, singleton_outputs[i % singleton_outputs.len], .{});
        defer singleton.deinit();
        try family_batch.equalBatchValue(singleton.value.object.get("data").?.array.items[0], row, 5e-4);
    }

    const oversized = try family_batch.batchBody(a, model, cases, maximum + 1, false);
    defer a.free(oversized);
    const rejected_before = metal.memoryStatsSnapshot();
    try expectRejected(transport, "/ai/v1/extract", oversized, 413, "EXTRACTION_LIMIT_EXCEEDED");
    try transport.idle();
    try expectNoMetalDeviceWork("batch_129_rejection", rejected_before, metal.memoryStatsSnapshot());
    std.debug.print("GLiNER specialty max batch model={s} items=128 ordered=true readbacks={d} over_limit_rejected=true\n", .{ model, expected_readbacks });
}

fn qualifySoak(
    a: Allocator,
    node: *Node,
    transport: *Loopback,
    pii_body: []const u8,
    pii_expected: []const u8,
    guard_body: []const u8,
    guard_expected: []const u8,
) !void {
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    try transport.idle();
    const admitted = domain.admission.snapshot();
    const baseline = metal.memoryStatsSnapshot();
    for (0..25) |_| {
        const pii = try responseAt(transport, "/ai/v1/extract", pii_body);
        defer a.free(pii);
        try compareEnvelope(pii_expected, pii, 5e-4);
        const guard = try responseAt(transport, "/ai/v1/extract", guard_body);
        defer a.free(guard);
        try compareEnvelope(guard_expected, guard, 5e-4);
        try transport.idle();
        try expectAdmissionRestored("alternating_soak", admitted, domain.admission.snapshot());
        const current = metal.memoryStatsSnapshot();
        try std.testing.expectEqual(baseline.device_owned_live_bytes, current.device_owned_live_bytes);
        try std.testing.expectEqual(baseline.host_mirror_live_bytes, current.host_mirror_live_bytes);
        // Max-batch qualification has already established the process high
        // water mark; repeated smaller requests must not raise it.
        try std.testing.expectEqual(baseline.device_owned_peak_live_bytes, current.device_owned_peak_live_bytes);
    }
    const after = metal.memoryStatsSnapshot();
    try std.testing.expectEqual(baseline.to_host_device_calls + 25 * 4, after.to_host_device_calls);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    std.debug.print("GLiNER specialty alternating soak cycles=25 parity=true admission_stable=true live_bytes_stable=true peak_stable=true\n", .{});
}

fn runBackend(a: Allocator, backend: Backend, root: []const u8, pii_dir: []const u8, guard_dir: []const u8, captures: *const Captures, native: ?*const Outputs) !Outputs {
    const process_baseline = metal.memoryStatsSnapshot();
    var node = try Node.init(a, .{
        .models_dir = root,
        .max_loaded_models = 2,
        .max_concurrent_requests = 2,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 12 * GiB,
            .backend_limit_bytes = 12 * GiB,
            .combined_limit_bytes = 20 * GiB,
            .scratch_limit_bytes = 8 * GiB,
        },
    });
    var node_open = true;
    errdefer if (node_open) node.deinit();
    configureBackend(&node, backend);
    try node.attachIo(std.testing.io);
    // The max-input qualification legitimately exceeds the small 64 KiB
    // default used by generic socket unit tests. Reuse the Node's production
    // request ceiling so transport ingress does not preempt the v2 parser's
    // exact 128-item contract.
    const http_body_limit = node.httpServerConfig("127.0.0.1", 0).max_body_size;
    const transport = try Loopback.initWithCapacityAndBodyLimit(a, &node, 2, http_body_limit);
    var transport_open = true;
    errdefer if (transport_open) transport.deinit();
    try transport.start();

    const pii_cases = captures.pii.value.object.get("cases").?.array.items;
    const guard_cases = captures.guard.value.object.get("cases").?.array.items;
    // Match the established qualified span-family numerical contract: native
    // DeBERTa presentation/raw heads use 2e-3 and Metal uses 5e-3. Entity and
    // label identity, ordering, offsets, cardinality and every non-near-tie
    // selection remain exact; only floating accumulation is approximate.
    const oracle_tolerance: f64 = if (backend == .metal) 5e-3 else 2e-3;
    const pii_outputs = try a.alloc([]u8, pii_cases.len);
    const guard_outputs = a.alloc([]u8, guard_cases.len) catch |err| {
        a.free(pii_outputs);
        return err;
    };
    var outputs = Outputs{ .pii = pii_outputs, .guard = guard_outputs };
    var pii_built: usize = 0;
    var guard_built: usize = 0;
    errdefer {
        for (outputs.pii[0..pii_built]) |bytes| a.free(bytes);
        for (outputs.guard[0..guard_built]) |bytes| a.free(bytes);
        if (outputs.pii_boundary) |bytes| a.free(bytes);
        if (outputs.guard_boundary) |bytes| a.free(bytes);
        a.free(outputs.pii);
        a.free(outputs.guard);
    }

    for (pii_cases, 0..) |case, i| {
        var schema = try piiCaseSchema(a, case);
        defer schema.deinit();
        const body = try piiCaseRequestBody(a, case, schema.value);
        defer a.free(body);
        try assertPiiWireCase(a, body, case);
        const before = if (backend == .metal) metal.memoryStatsSnapshot() else undefined;
        outputs.pii[i] = try responseAt(transport, "/ai/v1/extract", body);
        pii_built += 1;
        const predicted_count: usize = @intCast(case.object.get("heads").?.array.items[0].object.get("predicted_count").?.integer);
        // A closed count gate downloads only the count logits. Open cases add
        // the final span scores; GRU and encoder stay resident in both cases.
        if (backend == .metal)
            try std.testing.expectEqual(before.to_host_device_calls + 1 + @intFromBool(predicted_count != 0), metal.memoryStatsSnapshot().to_host_device_calls);
        const context = NumericContext{ .backend = backend, .profile = .pii_multi, .case_id = case.object.get("id").?.string };
        try expectPiiOracle(outputs.pii[i], case, context, oracle_tolerance);
        if (native) |expected| try compareEnvelope(expected.pii[i], outputs.pii[i], oracle_tolerance);
    }
    for (pii_cases) |case| if (std.mem.eql(u8, case.object.get("id").?.string, "unicode_codepoint_offsets")) {
        try qualifyDefaultByteOffsets(a, transport, case, oracle_tolerance);
        break;
    };
    try verifyIdentity(&node, pii_dir, backend, .pii_multi);
    try directPiiParity(a, &node, pii_dir, pii_cases, backend, oracle_tolerance);
    var pii_boundary_schema = try piiSchema(a, pii_cases[0].object.get("labels").?);
    defer pii_boundary_schema.deinit();
    outputs.pii_boundary = try qualifyPreparedBoundary(a, &node, transport, pii_dir, pii_directory, pii_boundary_schema.value, true, backend, if (native) |expected| expected.pii_boundary.? else null);
    std.debug.print("specialty_parity backend={s} profile=pii_multi cases={d} prepared_tokens=true count_logits=true valid_span_logits=true outputs=true\n", .{ @tagName(backend), pii_cases.len });

    const batch_cases = try a.alloc(family_batch.BatchCase, guard_cases.len);
    defer a.free(batch_cases);
    const schemas = try a.alloc(std.json.Parsed(Value), guard_cases.len);
    defer a.free(schemas);
    var schemas_built: usize = 0;
    defer for (schemas[0..schemas_built]) |*schema| schema.deinit();
    for (guard_cases, batch_cases, 0..) |case, *batch_case, i| {
        schemas[i] = try classificationSchema(a, case);
        schemas_built += 1;
        batch_case.* = .{ .text = case.object.get("text").?.string, .schema = schemas[i].value };
        const body = try family_batch.batchBody(a, guard_directory, batch_cases[i..][0..1], 1, false);
        defer a.free(body);
        const before = if (backend == .metal) metal.memoryStatsSnapshot() else undefined;
        outputs.guard[i] = try responseAt(transport, "/ai/v1/extract", body);
        guard_built += 1;
        if (backend == .metal and i + 1 == guard_cases.len)
            try std.testing.expectEqual(before.to_host_device_calls + 1, metal.memoryStatsSnapshot().to_host_device_calls);
        const context = NumericContext{ .backend = backend, .profile = .gliguard, .case_id = case.object.get("id").?.string };
        try expectGuardOracle(outputs.guard[i], case, context, oracle_tolerance);
        if (native) |expected| try compareEnvelope(expected.guard[i], outputs.guard[i], oracle_tolerance);
    }
    try verifyIdentity(&node, guard_dir, backend, .gliguard);
    try directGuardParity(a, &node, guard_dir, guard_cases, schemas, backend, oracle_tolerance);
    outputs.guard_boundary = try qualifyPreparedBoundary(a, &node, transport, guard_dir, guard_directory, schemas[0].value, false, backend, if (native) |expected| expected.guard_boundary.? else null);
    std.debug.print("specialty_parity backend={s} profile=gliguard cases={d} prepared_tokens=true singleton_and_batch_logits=true outputs=true\n", .{ @tagName(backend), guard_cases.len });

    if (backend == .metal) {
        for ([_]usize{ 8, 9 }) |count| {
            const body = try family_batch.batchBody(a, guard_directory, batch_cases, count, false);
            defer a.free(body);
            const before = metal.memoryStatsSnapshot();
            const actual = try responseAt(transport, "/ai/v1/extract", body);
            defer a.free(actual);
            const after = metal.memoryStatsSnapshot();
            try std.testing.expectEqual(before.to_host_device_calls + (count + 7) / 8, after.to_host_device_calls);
            std.debug.print("specialty_metal_batch items={d} physical_forwards={d} final_logit_readbacks={d}\n", .{ count, (count + 7) / 8, after.to_host_device_calls - before.to_host_device_calls });
            var parsed = try std.json.parseFromSlice(Value, a, actual, .{});
            defer parsed.deinit();
            const rows = parsed.value.object.get("data").?.array.items;
            try std.testing.expectEqual(count, rows.len);
            for (rows, 0..) |row, i| {
                var singleton = try std.json.parseFromSlice(Value, a, outputs.guard[i % outputs.guard.len], .{});
                defer singleton.deinit();
                try family_batch.equalBatchValue(singleton.value.object.get("data").?.array.items[0], row, 5e-4);
            }
            if (count == 9) try lifecycle(a, &node, transport, guard_dir, pii_dir, body, actual);
        }
        // The composed six-task case must remain one prepared sequence and one
        // encoder/head readback, rather than six hidden-state recomputations.
        const composed = guard_cases.len - 1;
        var parsed = try std.json.parseFromSlice(Value, a, outputs.guard[composed], .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.value.object.get("data").?.array.items[0].object.get("classifications").?.array.items.len >= 6);

        const pii_batch_cases = [_]family_batch.BatchCase{.{
            .text = pii_cases[0].object.get("text").?.string,
            .schema = pii_boundary_schema.value,
        }};
        try qualifyMaxBatch(a, transport, pii_directory, &pii_batch_cases, outputs.pii[0..1], 2 * 128);
        try qualifyMaxBatch(a, transport, guard_directory, batch_cases, outputs.guard, 16);

        const pii_lifecycle_body = try requestBody(
            a,
            pii_directory,
            pii_cases[0].object.get("id").?.string,
            pii_cases[0].object.get("text").?.string,
            pii_boundary_schema.value,
        );
        defer a.free(pii_lifecycle_body);
        try lifecycle(a, &node, transport, pii_dir, guard_dir, pii_lifecycle_body, outputs.pii[0]);

        const guard_soak_body = try family_batch.batchBody(a, guard_directory, batch_cases, 9, false);
        defer a.free(guard_soak_body);
        const guard_soak_expected = try responseAt(transport, "/ai/v1/extract", guard_soak_body);
        defer a.free(guard_soak_expected);
        try transport.idle();
        try qualifySoak(a, &node, transport, pii_lifecycle_body, outputs.pii[0], guard_soak_body, guard_soak_expected);
    }
    try rejectionChecks(a, transport, pii_directory, guard_directory);
    try transport.finish();
    transport.deinit();
    transport_open = false;
    node.deinit();
    node_open = false;
    const process_after = metal.memoryStatsSnapshot();
    try std.testing.expectEqual(process_baseline.device_owned_live_bytes, process_after.device_owned_live_bytes);
    try std.testing.expectEqual(process_baseline.host_mirror_live_bytes, process_after.host_mirror_live_bytes);
    try std.testing.expectEqual(
        process_after.device_owned_buffers_created - process_baseline.device_owned_buffers_created,
        process_after.device_owned_buffers_released - process_baseline.device_owned_buffers_released,
    );
    try std.testing.expectEqual(
        process_after.device_owned_bytes_created - process_baseline.device_owned_bytes_created,
        process_after.device_owned_bytes_released - process_baseline.device_owned_bytes_released,
    );
    return outputs;
}

fn expectNotLoaded(node: *Node, directory: []const u8) !void {
    if (node.model_manager.acquireLoadedModel(directory)) |found| {
        var unexpected = found;
        unexpected.release();
        return error.UnexpectedLoadedSpecialtyModel;
    }
}

fn qualifyCapacityEviction(a: Allocator, root: []const u8, pii_dir: []const u8, guard_dir: []const u8, captures: *const Captures, expected: *const Outputs) !void {
    const baseline = metal.memoryStatsSnapshot();
    var node = try Node.init(a, .{
        .models_dir = root,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 12 * GiB, .backend_limit_bytes = 12 * GiB, .combined_limit_bytes = 20 * GiB, .scratch_limit_bytes = 8 * GiB },
    });
    var node_open = true;
    errdefer if (node_open) node.deinit();
    configureBackend(&node, .metal);
    try node.attachIo(std.testing.io);
    const transport = try Loopback.initWithCapacity(a, &node, 1);
    var transport_open = true;
    errdefer if (transport_open) transport.deinit();
    try transport.start();

    const pii_case = captures.pii.value.object.get("cases").?.array.items[0];
    var pii_schema = try piiCaseSchema(a, pii_case);
    defer pii_schema.deinit();
    const pii_body = try piiCaseRequestBody(a, pii_case, pii_schema.value);
    defer a.free(pii_body);
    const first = try responseAt(transport, "/ai/v1/extract", pii_body);
    defer a.free(first);
    try compareEnvelope(expected.pii[0], first, 5e-3);
    try transport.idle();

    var held = node.model_manager.acquireLoadedModel(pii_dir) orelse return error.MissingLoadedModel;
    var held_open = true;
    defer if (held_open) held.release();
    const guard_case = captures.guard.value.object.get("cases").?.array.items[0];
    var guard_schema = try classificationSchema(a, guard_case);
    defer guard_schema.deinit();
    const guard_cases = [_]family_batch.BatchCase{.{ .text = guard_case.object.get("text").?.string, .schema = guard_schema.value }};
    const guard_body = try family_batch.batchBody(a, guard_directory, &guard_cases, 1, false);
    defer a.free(guard_body);
    try expectRejected(transport, "/ai/v1/extract", guard_body, 503, "MODEL_RESOURCE_BUSY");
    try transport.idle();
    try expectNotLoaded(&node, guard_dir);
    held.release();
    held_open = false;

    const guard = try responseAt(transport, "/ai/v1/extract", guard_body);
    defer a.free(guard);
    var actual_guard = try std.json.parseFromSlice(Value, a, guard, .{});
    defer actual_guard.deinit();
    var expected_guard = try std.json.parseFromSlice(Value, a, expected.guard[0], .{});
    defer expected_guard.deinit();
    try family_batch.equalBatchValue(expected_guard.value.object.get("data").?.array.items[0], actual_guard.value.object.get("data").?.array.items[0], 5e-3);
    try transport.idle();
    try expectNotLoaded(&node, pii_dir);

    const reloaded = try responseAt(transport, "/ai/v1/extract", pii_body);
    defer a.free(reloaded);
    try compareEnvelope(expected.pii[0], reloaded, 5e-3);
    try transport.idle();
    try expectNotLoaded(&node, guard_dir);
    try transport.finish();
    transport.deinit();
    transport_open = false;
    node.deinit();
    node_open = false;
    const after = metal.memoryStatsSnapshot();
    try std.testing.expectEqual(baseline.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(baseline.host_mirror_live_bytes, after.host_mirror_live_bytes);
    try std.testing.expectEqual(after.device_owned_buffers_created - baseline.device_owned_buffers_created, after.device_owned_buffers_released - baseline.device_owned_buffers_released);
    try std.testing.expectEqual(after.device_owned_bytes_created - baseline.device_owned_bytes_created, after.device_owned_bytes_released - baseline.device_owned_bytes_released);
    std.debug.print("GLiNER specialty cache max_loaded=1 held_handle_blocks_eviction=true alternating_reload=true outputs_equal=true teardown_balanced=true\n", .{});
}

test "GLiNER specialty exact FP32 native oracle and forced Metal HTTP qualification" {
    if (!try suiteEnabled()) return error.SkipZigTest;
    try requireMetalAvailable();
    const a = std.testing.allocator;
    var captures = try loadCaptures(a);
    defer captures.deinit();
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const source_root = sourceModelRoot();
    const pii_dir = try prepareModel(a, &temporary, source_root, pii_directory);
    defer a.free(pii_dir);
    const guard_dir = try prepareModel(a, &temporary, source_root, guard_directory);
    defer a.free(guard_dir);
    const root = std.fs.path.dirname(pii_dir) orelse return error.InvalidModelPath;
    var native = try runBackend(a, .native, root, pii_dir, guard_dir, &captures, null);
    defer native.deinit(a);
    var gpu = try runBackend(a, .metal, root, pii_dir, guard_dir, &captures, &native);
    defer gpu.deinit(a);
    try qualifyCapacityEviction(a, root, pii_dir, guard_dir, &captures, &native);
}
