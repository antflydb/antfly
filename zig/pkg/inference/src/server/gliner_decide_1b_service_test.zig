// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in public service qualification for the exact GLiNER2.5-Decide-1B
//! artifact after the pinned capture and shared loaded-identity gate passed
//! native and physical Metal parity. Both direct and HTTP presentation paths
//! use an explicit 10 GiB process envelope with a 10 GiB generation budget.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const registry = @import("../registry/registry.zig");
const manifest_mod = @import("../models/manifest.zig");
const decide_parity = @import("../extractors/gliner_decide_1b_parity_test.zig");
const factory = @import("../architectures/session_factory.zig");
const shared = @import("gliner_boundary_service_test.zig");
const c_file = @import("../util/c_file.zig");
const memory = @import("../runtime/tier/memory.zig");
const perf = @import("gliner_family_perf.zig");
const extracting = @import("antfly_extracting");
const decide = @import("antfly_decisions").legacy;
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const classification_pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const span_executor = @import("../extractors/gliner_span_v2_executor.zig");
const BackendType = @import("../backends/backends.zig").BackendType;
const Allocator = std.mem.Allocator;

const pins = decide_parity.files;
const service_process_memory_bytes = 10 * 1024 * 1024 * 1024;
const service_generation_memory_bytes = 10 * 1024 * 1024 * 1024;

const OwnershipSnapshot = struct {
    model: memory.AdmissionAmounts = .{},
    tokenizer_load: memory.AdmissionAmounts = .{},
    tokenizer_cache: memory.AdmissionAmounts = .{},
    weight_cache: memory.AdmissionAmounts = .{},
    workspace: memory.AdmissionAmounts = .{},
};

fn ownershipSnapshot(node: *Node) OwnershipSnapshot {
    var result: OwnershipSnapshot = .{};
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    {
        defer node.model_manager.load_lock.unlock();
        var iterator = node.model_manager.loaded.valueIterator();
        while (iterator.next()) |entry| {
            const loaded = entry.*;
            if (loaded.resource_lease) |lease| result.model = result.model.merge(lease.amounts) catch result.model;
            if (loaded.tokenizer_resource_lease) |lease| result.tokenizer_load = result.tokenizer_load.merge(lease.amounts) catch result.tokenizer_load;
            result.weight_cache = result.weight_cache.merge(factory.sharedCacheAdmissionAmounts(loaded.session)) catch result.weight_cache;
            result.workspace = result.workspace.merge(factory.glinerBoundaryWorkspaceAdmissionAmounts(loaded.session)) catch result.workspace;
        }
    }
    result.tokenizer_cache = node.model_manager.tokenizerCacheAdmissionAmounts() catch .{};
    return result;
}

fn reportWorkerFailure(
    node: *Node,
    comptime backend: BackendType,
    stage: []const u8,
    case_id: []const u8,
    err: anyerror,
) void {
    const process = platform.process_memory.pressureSnapshot();
    const host = memory.currentSystemMemoryInfo();
    const worker = memory.currentSystemMemoryInfoForLimit(service_process_memory_bytes, .explicit);
    const admitted = if (node.model_manager.resource_domain) |domain| domain.admission.snapshot() else memory.AdmissionAmounts{};
    const limits = if (node.model_manager.resource_domain) |domain| domain.admission_limits else memory.Limits{};
    const owned = ownershipSnapshot(node);
    std.debug.print(
        "GLiNER2.5 Decide-1B worker failure={s} backend={s} stage={s} case={s} process_footprint={d} process_rss={d} process_envelope={d} generation_envelope={d} host={any} worker={any} admitted={any} limits={any} model_owned={any} tokenizer_load_owned={any} tokenizer_cache_owned={any} weight_cache_owned={any} workspace_owned={any}\n",
        .{
            @errorName(err),
            @tagName(backend),
            stage,
            case_id,
            process.footprint_bytes,
            process.resident_bytes,
            service_process_memory_bytes,
            service_generation_memory_bytes,
            host,
            worker,
            admitted,
            limits,
            owned.model,
            owned.tokenizer_load,
            owned.tokenizer_cache,
            owned.weight_cache,
            owned.workspace,
        },
    );
}

const CapturedClassification = struct {
    input_ids: []const i64,
    tasks: []const struct {
        name: []const u8,
        labels: []const []const u8,
        raw_logits: []const f32,
    },
};

const Capture = struct {
    requests: []const struct {
        id: []const u8,
        text: []const u8,
        native_schema_json: []const u8,
    },
    public_decide_requests: []const struct {
        id: []const u8,
        text: []const u8,
        encoded: struct { input_ids: []const i64 },
        decide_request_json: []const u8,
        native_schema_json: []const u8,
        native_classification: CapturedClassification,
        decide_expected: std.json.Value,
    },
};

const perf_profile = "decide_1b";
const direct_perf_profile = "decide_1b_direct_kernel";
const perf_model_repo = "fastino/GLiNER2.5-Decide-1B";
const perf_model_revision = "688cd7ba8917a0855ad3ce929cba5a9998932e79";
const perf_caveats = &.{
    "Serial single-request samples; p95 is descriptive and is not a concurrent-load service SLO.",
    "The HTTP path is in-process handler dispatch and excludes socket transport.",
    "Response correctness, destruction, cache identity, and exact idle-ownership checks run outside each timed interval.",
    "One direct and one in-process HTTP validation preflight run before the three paired warmups.",
    "The fixture uses std.testing.allocator; production uses platform.processAllocator with smp_allocator.",
    "File pins are verified before the cold sample, so the cold timing may benefit from filesystem cache state.",
    "External process maximum RSS is recorded by the /usr/bin/time -l campaign wrapper.",
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
            var iterator = values.iterator();
            while (iterator.next()) |entry| {
                const got = actual.object.get(entry.key_ptr.*) orelse return error.TestExpectedEqual;
                try expectJsonApprox(entry.value_ptr.*, got, tolerance);
            }
        },
    }
}

fn realDirectory(a: Allocator, requested: []const u8) ![]u8 {
    var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const length = if (std.fs.path.isAbsolute(requested))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested, &buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested, &buffer);
    return a.dupe(u8, buffer[0..length]);
}

fn hardLink(a: Allocator, source: []const u8, destination: []const u8) !void {
    const source_z = try a.dupeSentinel(u8, source, 0);
    defer a.free(source_z);
    const destination_z = try a.dupeSentinel(u8, destination, 0);
    defer a.free(destination_z);
    if (c_file.c.link(source_z.ptr, destination_z.ptr) != 0) return error.Decide1BServiceFixtureLinkFailed;
}

fn linkedModel(a: Allocator, temporary: *std.testing.TmpDir, source: []const u8, synthesize_manifest: bool) ![:0]u8 {
    const io = std.testing.io;
    try temporary.dir.createDirPath(io, "model/encoder_config");
    const root = try temporary.dir.realPathFileAlloc(io, ".", a);
    errdefer a.free(root);
    inline for (.{ "config.json", "encoder_config/config.json", "model.safetensors", "tokenizer.json", "tokenizer_config.json" }) |name| {
        const from = try std.fs.path.join(a, &.{ source, name });
        defer a.free(from);
        const to = try std.fs.path.join(a, &.{ root, "model", name });
        defer a.free(to);
        try hardLink(a, from, to);
    }
    const model_dir = try std.fs.path.join(a, &.{ root, "model" });
    defer a.free(model_dir);
    if (synthesize_manifest) {
        const manifest_json = try registry.synthesizePulledModelManifestJson(a, model_dir, null, null);
        defer a.free(manifest_json);
        try temporary.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = manifest_json });
    }
    return root;
}

fn requestJson(a: Allocator, captured: []const u8) ![]u8 {
    return @import("gliner_decision_fixture.zig").requestJson(a, captured, "model");
}

fn expectResponse(a: Allocator, bytes: []const u8, expected: std.json.Value) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("model", parsed.value.object.get("model").?.string);
    try expectJsonApprox(
        expected.object.get("answers") orelse return error.InvalidFamilyReference,
        (try @import("gliner_decision_fixture.zig").comparisonValue(parsed.arena.allocator(), parsed.value)).object.get("answers") orelse return error.InvalidFamilyReference,
        5e-4,
    );
}

fn dispatch(a: Allocator, node: *Node, raw: []const u8) !httpx.Response {
    return dispatchWithIo(a, std.testing.io, node, raw);
}

fn dispatchWithIo(a: Allocator, io: std.Io, node: *Node, raw: []const u8) !httpx.Response {
    var request = try httpx.Request.init(a, .POST, "/ai/v1/decisions");
    defer request.deinit();
    request.body = raw;
    var context = httpx.Context.init(a, io, &request);
    defer context.deinit();
    context.max_request_body_size = 64 * 1024;
    context.application_deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s;
    return node.decide(&context);
}

fn dispatchExtraction(a: Allocator, node: *Node, raw: []const u8) !httpx.Response {
    var request = try httpx.Request.init(a, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body = raw;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    context.max_request_body_size = 64 * 1024;
    context.application_deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s;
    return node.extractJSON(&context);
}

fn classificationRequestJson(
    a: Allocator,
    schema_version: u8,
    texts: []const []const u8,
    schema_json: []const u8,
) ![]u8 {
    var schema = try std.json.parseFromSlice(std.json.Value, a, schema_json, .{ .duplicate_field_behavior = .@"error" });
    defer schema.deinit();
    const inputs = try a.alloc(struct { id: []const u8, content: []const u8 }, texts.len);
    var initialized: usize = 0;
    defer {
        for (inputs[0..initialized]) |input| a.free(input.id);
        a.free(inputs);
    }
    for (texts, inputs, 0..) |text, *input, index| {
        input.* = .{ .id = try std.fmt.allocPrint(a, "item-{d}", .{index}), .content = text };
        initialized += 1;
    }
    return std.json.Stringify.valueAlloc(a, .{
        .model = "model",
        .schema_version = schema_version,
        .inputs = inputs,
        .schema = schema.value,
        .options = .{ .include_confidence = true },
    }, .{});
}

fn classificationRequestJsonWithThreshold(
    a: Allocator,
    schema_version: u8,
    texts: []const []const u8,
    schema_json: []const u8,
    threshold: f64,
) ![]u8 {
    const base = try classificationRequestJson(a, schema_version, texts, schema_json);
    defer a.free(base);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, base, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try parsed.value.object.getPtr("options").?.object.put(parsed.arena.allocator(), "threshold", .{ .float = threshold });
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

fn withoutSchemaVersion(a: Allocator, bytes: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.swapRemove("schema_version"));
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

fn expectSameResponseJson(a: Allocator, expected: []const u8, actual: []const u8) !void {
    var want = try std.json.parseFromSlice(std.json.Value, a, expected, .{ .duplicate_field_behavior = .@"error" });
    defer want.deinit();
    var got = try std.json.parseFromSlice(std.json.Value, a, actual, .{ .duplicate_field_behavior = .@"error" });
    defer got.deinit();
    try expectJsonApprox(want.value, got.value, 5e-4);
}

fn expectProviderMatchesV2(
    a: Allocator,
    v2_json: []const u8,
    provider: []const []const Node.DirectClassificationScore,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, v2_json, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const results = parsed.value.object.get("data") orelse return error.InvalidFamilyReference;
    try std.testing.expectEqual(@as(usize, 1), results.array.items.len);
    const classifications = results.array.items[0].object.get("classifications") orelse return error.InvalidFamilyReference;
    try std.testing.expectEqual(@as(usize, 1), provider.len);
    try std.testing.expectEqual(classifications.array.items.len, provider[0].len);
    for (provider[0], 0..) |actual, index| {
        if (index > 0) try std.testing.expect(provider[0][index - 1].score >= actual.score);
        for (provider[0][0..index]) |prior| try std.testing.expect(!std.mem.eql(u8, prior.label, actual.label));
        const expected = for (classifications.array.items) |candidate| {
            const label = candidate.object.get("label") orelse return error.InvalidFamilyReference;
            if (label == .string and std.mem.eql(u8, label.string, actual.label)) break candidate;
        } else return error.InvalidFamilyReference;
        const confidence = expected.object.get("score").?;
        const value: f64 = switch (confidence) {
            .float => |number| number,
            .integer => |number| @floatFromInt(number),
            else => return error.InvalidFamilyReference,
        };
        try std.testing.expectApproxEqAbs(value, @as(f64, actual.score), 5e-4);
    }
}

fn freeProviderResults(a: Allocator, results: []const []const Node.DirectClassificationScore) void {
    for (results) |row| a.free(row);
    a.free(results);
}

fn expectClassificationCount(a: Allocator, bytes: []const u8, count: usize) !?f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const data = parsed.value.object.get("data") orelse return error.InvalidFamilyReference;
    try std.testing.expectEqual(@as(usize, 1), data.array.items.len);
    const classifications = data.array.items[0].object.get("classifications") orelse return error.InvalidFamilyReference;
    try std.testing.expectEqual(count, classifications.array.items.len);
    if (count == 0) return null;
    const score = classifications.array.items[0].object.get("score") orelse return error.InvalidFamilyReference;
    return switch (score) {
        .float => |value| value,
        .integer => |value| @floatFromInt(value),
        else => return error.InvalidFamilyReference,
    };
}

fn requireBackend(node: *Node, comptime backend: BackendType) void {
    node.session_manager.preferred_backends = &.{backend};
    node.session_manager.required_backend = backend;
    node.session_manager.required_backend_invalid = false;
    node.model_manager.session_manager.preferred_backends = &.{backend};
    node.model_manager.session_manager.required_backend = backend;
    node.model_manager.session_manager.required_backend_invalid = false;
}

fn expectCached(node: *Node, directory: []const u8, comptime backend: BackendType) !usize {
    return expectCachedWithActiveHandles(node, directory, backend, 0);
}

fn expectCachedWithActiveHandles(node: *Node, directory: []const u8, comptime backend: BackendType, active_handles: usize) !usize {
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    try std.testing.expectEqual(active_handles, loaded.active_handles);
    try std.testing.expectEqual(backend, loaded.session.backend());
    switch (try factory.getGlinerSpanConfig(loaded.session)) {
        .deberta => return error.ExpectedModernBertSession,
        .modern_bert => |config| {
            try std.testing.expectEqual(@as(u32, 1792), config.hidden_size);
            try std.testing.expectEqual(@as(u32, 28), config.num_hidden_layers);
        },
    }
    return @intFromPtr(loaded);
}

fn expectIdle(node: *Node) !memory.AdmissionAmounts {
    return expectIdleWithActiveHandles(node, 0);
}

fn expectIdleWithActiveHandles(node: *Node, active_handles: usize) !memory.AdmissionAmounts {
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(i64, 0), node.metrics.requests_active.impl.value);
    try std.testing.expectEqual(@as(i64, 0), node.metrics.extraction_v2.active.impl.value);
    var retained: memory.AdmissionAmounts = .{};
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    {
        defer node.model_manager.load_lock.unlock();
        var iterator = node.model_manager.loaded.valueIterator();
        while (iterator.next()) |entry| {
            const loaded = entry.*;
            try std.testing.expectEqual(active_handles, loaded.active_handles);
            if (loaded.resource_lease) |lease| retained = try retained.merge(lease.amounts);
            if (loaded.tokenizer_resource_lease) |lease| retained = try retained.merge(lease.amounts);
            retained = try retained.merge(factory.sharedCacheAdmissionAmounts(loaded.session));
            // Currently zero for ModernBERT span sessions; retaining this
            // architecture-aware reader keeps the ownership assertion valid
            // if a boundary session ever shares this helper.
            retained = try retained.merge(factory.glinerBoundaryWorkspaceAdmissionAmounts(loaded.session));
        }
    }
    retained = try retained.merge(try node.model_manager.tokenizerCacheAdmissionAmounts());
    const amounts = node.model_manager.resource_domain.?.admission.snapshot();
    try std.testing.expectEqual(retained, amounts);
    if (node.hard_cancellation_watchdog) |watchdog| {
        while (!watchdog.mutex.tryLock()) std.atomic.spinLoopHint();
        defer watchdog.mutex.unlock();
        try std.testing.expect(watchdog.io != null);
        try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    }
    return amounts;
}

fn modelOwned(node: *Node) !memory.AdmissionAmounts {
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    return if (loaded.resource_lease) |lease| lease.amounts else error.MissingModelResourceLease;
}

fn expectSameModelWeights(expected: memory.AdmissionAmounts, actual: memory.AdmissionAmounts) !void {
    try std.testing.expectEqual(expected.host_weight_bytes, actual.host_weight_bytes);
    try std.testing.expectEqual(expected.backend_weight_bytes, actual.backend_weight_bytes);
}

fn perfIdleLedger(amounts: memory.AdmissionAmounts) perf.IdleLedger {
    return .{
        .host_weight_bytes = amounts.host_weight_bytes,
        .backend_weight_bytes = amounts.backend_weight_bytes,
        .host_kv_bytes = amounts.host_kv_bytes,
        .backend_kv_bytes = amounts.backend_kv_bytes,
        .host_scratch_bytes = amounts.host_scratch_bytes,
        .backend_scratch_bytes = amounts.backend_scratch_bytes,
    };
}

fn runtimeCpuThreadBudget() !usize {
    const configured = platform.env.getenv("ANTFLY_INFERENCE_CPU_THREADS") orelse return 1;
    const value = try std.fmt.parseInt(usize, configured, 10);
    if (value == 0) return error.InvalidInferenceCpuThreadBudget;
    return value;
}

fn perfMemorySnapshot(node: *Node) !perf.MemorySnapshot {
    return perfMemorySnapshotWithActiveHandles(node, 0);
}

fn perfMemorySnapshotWithActiveHandles(node: *Node, active_handles: usize) !perf.MemorySnapshot {
    const process = platform.process_memory.pressureSnapshot();
    const idle = try expectIdleWithActiveHandles(node, active_handles);
    const owned = ownershipSnapshot(node);
    return .{
        .process_footprint_bytes = process.footprint_bytes,
        .process_rss_bytes = process.resident_bytes,
        .idle_ledger = perfIdleLedger(idle),
        .ownership = .{
            .model = perfIdleLedger(owned.model),
            .tokenizer_load = perfIdleLedger(owned.tokenizer_load),
            .tokenizer_cache = perfIdleLedger(owned.tokenizer_cache),
            .weight_cache = perfIdleLedger(owned.weight_cache),
            .workspace = perfIdleLedger(owned.workspace),
        },
    };
}

fn explicitV1ClassificationRegression(
    a: Allocator,
    node: *Node,
    directory: []const u8,
    capture: Capture,
) !void {
    const source = for (capture.requests) |request| {
        if (std.mem.eql(u8, request.id, "english_intent")) break request;
    } else return error.InvalidFamilyReference;
    try std.testing.expect(source.text.len > 0 and source.text[source.text.len - 1] == '.');
    // Omitting the source period is deliberate. The qualified span processor
    // must synthesize it exactly as upstream before every public adapter runs.
    const text = source.text[0 .. source.text.len - 1];
    const content_json = try std.json.Stringify.valueAlloc(a, text, .{});
    defer a.free(content_json);
    const options_json = "{\"include_confidence\":true}";
    const legacy_schema =
        \\{"classifications":[{"name":"intent","labels":["refund","technical_support","sales"],"top_k":1}]}
    ;

    var v2 = try node.extractDirect(a, "model", .{
        .schema_version = 2,
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = legacy_schema,
        .options_json = options_json,
    });
    defer v2.deinit();

    // An explicitly supplied legacy version used to suppress the automatic
    // V2 upgrade and reach the raw ModernBERT marker head.
    var direct_v1 = try node.extractDirect(a, "model", .{
        .schema_version = 1,
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = legacy_schema,
        .options_json = options_json,
    });
    defer direct_v1.deinit();
    try expectSameResponseJson(a, v2.json, direct_v1.json);
    var direct_implicit = try node.extractDirect(a, "model", .{
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = legacy_schema,
        .options_json = options_json,
    });
    defer direct_implicit.deinit();
    try expectSameResponseJson(a, v2.json, direct_implicit.json);

    const texts = [_][]const u8{text};
    const http_json = try classificationRequestJson(a, 1, &texts, legacy_schema);
    defer a.free(http_json);
    var http_v1 = try dispatchExtraction(a, node, http_json);
    defer http_v1.deinit();
    try std.testing.expectEqual(@as(u16, 200), http_v1.status.code);
    try expectSameResponseJson(a, v2.json, http_v1.body orelse return error.MissingResponseBody);
    const implicit_http_json = try withoutSchemaVersion(a, http_json);
    defer a.free(implicit_http_json);
    var implicit_http = try dispatchExtraction(a, node, implicit_http_json);
    defer implicit_http.deinit();
    try std.testing.expectEqual(@as(u16, 200), implicit_http.status.code);
    try expectSameResponseJson(a, v2.json, implicit_http.body orelse return error.MissingResponseBody);

    // The classifier-provider surface has no task-description argument. Use
    // its exact public contract and compare it with the equivalent qualified
    // V2 task, including the synthetic terminal period.
    const provider_schema =
        \\{"classifications":[{"name":"classification","labels":["refund","technical_support","sales"],"top_k":1}]}
    ;
    var provider_v2 = try node.extractDirect(a, "model", .{
        .schema_version = 2,
        .inputs = &.{.{ .id = "provider", .content_json = content_json }},
        .schema_json = provider_schema,
        .options_json = options_json,
    });
    defer provider_v2.deinit();
    const labels = [_][]const u8{ "refund", "technical_support", "sales" };
    const provider = try node.classifyTextsDirect(a, "model", &texts, &labels, null, false);
    defer freeProviderResults(a, provider);
    try expectProviderMatchesV2(a, provider_v2.json, provider);

    // An empty provider model name resolves the configured default root. Keep
    // the override scoped so every subsequent assertion exercises the normal
    // fixture catalog, while the already cached session proves no reload is
    // needed for this trusted absolute lookup.
    {
        const saved_models_dir = node.config.models_dir;
        node.config.models_dir = directory;
        defer node.config.models_dir = saved_models_dir;
        const default_provider = try node.classifyTextsDirect(a, "", &texts, &labels, null, false);
        defer freeProviderResults(a, default_provider);
        try expectProviderMatchesV2(a, provider_v2.json, default_provider);
    }

    const multi_provider_schema =
        \\{"classifications":[{"name":"classification","labels":["refund","technical_support","sales"],"multi_label":true,"min_labels":3,"max_labels":3}]}
    ;
    var multi_provider_v2 = try node.extractDirect(a, "model", .{
        .schema_version = 2,
        .inputs = &.{.{ .id = "provider", .content_json = content_json }},
        .schema_json = multi_provider_schema,
        .options_json = options_json,
    });
    defer multi_provider_v2.deinit();
    const multi_provider = try node.classifyTextsDirect(a, "model", &texts, &labels, null, true);
    defer freeProviderResults(a, multi_provider);
    try expectProviderMatchesV2(a, multi_provider_v2.json, multi_provider);

    // Explicit V1 keeps its original schema contract. V2-only cardinality and
    // constraint fields must be rejected instead of being silently upgraded.
    try std.testing.expectError(error.AdvancedExtractionSchemaRequiresVersion2, node.extractDirect(a, "model", .{
        .schema_version = 1,
        .inputs = &.{.{ .id = "advanced-v1", .content_json = content_json }},
        .schema_json = source.native_schema_json,
        .options_json = options_json,
    }));
    const advanced_http_json = try classificationRequestJson(a, 1, &texts, source.native_schema_json);
    defer a.free(advanced_http_json);
    var advanced_http = try dispatchExtraction(a, node, advanced_http_json);
    defer advanced_http.deinit();
    try std.testing.expectEqual(@as(u16, 400), advanced_http.status.code);
    try std.testing.expect(std.mem.indexOf(u8, advanced_http.body orelse return error.MissingResponseBody, "UNSUPPORTED_EXTRACTION_FEATURE") != null);

    const invalid_public_threshold =
        \\{"classifications":[{"name":"intent","labels":["refund","technical_support","sales"],"multi_label":true,"threshold":0}]}
    ;
    try std.testing.expectError(error.InvalidClassificationCalibration, node.extractDirect(a, "model", .{
        .schema_version = 2,
        .inputs = &.{.{ .id = "invalid-public-threshold", .content_json = content_json }},
        .schema_json = invalid_public_threshold,
        .options_json = options_json,
    }));
    try std.testing.expectError(error.InvalidClassificationCalibration, node.extractDirect(a, "model", .{
        .inputs = &.{.{ .id = "invalid-implicit-v2-threshold", .content_json = content_json }},
        .schema_json = invalid_public_threshold,
        .options_json = options_json,
    }));
    const invalid_public_http_json = try classificationRequestJson(a, 2, &texts, invalid_public_threshold);
    defer a.free(invalid_public_http_json);
    var invalid_public_http = try dispatchExtraction(a, node, invalid_public_http_json);
    defer invalid_public_http.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_public_http.status.code);
    try std.testing.expect(std.mem.indexOf(u8, invalid_public_http.body orelse return error.MissingResponseBody, "INVALID_EXTRACTION_REQUEST") != null);
    const invalid_implicit_http_json = try withoutSchemaVersion(a, invalid_public_http_json);
    defer a.free(invalid_implicit_http_json);
    var invalid_implicit_http = try dispatchExtraction(a, node, invalid_implicit_http_json);
    defer invalid_implicit_http.deinit();
    try std.testing.expectEqual(@as(u16, 400), invalid_implicit_http.status.code);
    try std.testing.expect(std.mem.indexOf(u8, invalid_implicit_http.body orelse return error.MissingResponseBody, "INVALID_EXTRACTION_REQUEST") != null);

    // Legacy multi-label threshold endpoints are inclusive. At one, this
    // captured row has no exact-one probability and must remain empty; public
    // V2 keeps its documented best-label fallback at a valid interior cutoff.
    const multi_legacy_schema =
        \\{"classifications":[{"name":"intent","labels":["refund","technical_support","sales"],"multi_label":true,"top_k":3}]}
    ;
    var high_v1 = try node.extractDirect(a, "model", .{
        .schema_version = 1,
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = multi_legacy_schema,
        .options_json = "{\"include_confidence\":true,\"threshold\":1}",
    });
    defer high_v1.deinit();
    try std.testing.expect((try expectClassificationCount(a, high_v1.json, 0)) == null);
    var high_implicit = try node.extractDirect(a, "model", .{
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = multi_legacy_schema,
        .options_json = "{\"include_confidence\":true,\"threshold\":1}",
    });
    defer high_implicit.deinit();
    try std.testing.expect((try expectClassificationCount(a, high_implicit.json, 0)) == null);
    const high_http_json = try classificationRequestJsonWithThreshold(a, 1, &texts, multi_legacy_schema, 1);
    defer a.free(high_http_json);
    var high_http = try dispatchExtraction(a, node, high_http_json);
    defer high_http.deinit();
    try std.testing.expectEqual(@as(u16, 200), high_http.status.code);
    try std.testing.expect((try expectClassificationCount(a, high_http.body orelse return error.MissingResponseBody, 0)) == null);
    const high_implicit_http_json = try withoutSchemaVersion(a, high_http_json);
    defer a.free(high_implicit_http_json);
    var high_implicit_http = try dispatchExtraction(a, node, high_implicit_http_json);
    defer high_implicit_http.deinit();
    try std.testing.expectEqual(@as(u16, 200), high_implicit_http.status.code);
    try std.testing.expect((try expectClassificationCount(a, high_implicit_http.body orelse return error.MissingResponseBody, 0)) == null);

    const public_v2_schema =
        \\{"classifications":[{"name":"intent","labels":["refund","technical_support","sales"],"multi_label":true,"threshold":0.999999,"top_k":3}]}
    ;
    var high_v2 = try node.extractDirect(a, "model", .{
        .schema_version = 2,
        .inputs = &.{.{ .id = "item-0", .content_json = content_json }},
        .schema_json = public_v2_schema,
        .options_json = options_json,
    });
    defer high_v2.deinit();
    const fallback_score = (try expectClassificationCount(a, high_v2.json, 1)) orelse return error.InvalidFamilyReference;
    try std.testing.expect(fallback_score < 0.999999);

    // Qualification covers one request item and one prepared sequence through
    // 198 tokens. Legacy adapters must reject before executing outside it.
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, node.extractDirect(a, "model", .{
        .schema_version = 1,
        .inputs = &.{
            .{ .id = "one", .content_json = content_json },
            .{ .id = "two", .content_json = content_json },
        },
        .schema_json = legacy_schema,
        .options_json = options_json,
    }));
    const two_texts = [_][]const u8{ text, text };
    try std.testing.expectError(
        error.UnsupportedGlinerDecisionGeometry,
        node.classifyTextsDirect(a, "model", &two_texts, &labels, null, false),
    );
    const two_http_json = try classificationRequestJson(a, 1, &two_texts, legacy_schema);
    defer a.free(two_http_json);
    var two_http = try dispatchExtraction(a, node, two_http_json);
    defer two_http.deinit();
    try std.testing.expectEqual(@as(u16, 400), two_http.status.code);
    try std.testing.expect(std.mem.indexOf(u8, two_http.body orelse return error.MissingResponseBody, "UnsupportedGlinerDecisionGeometry") != null);

    var long_text = std.ArrayListUnmanaged(u8).empty;
    defer long_text.deinit(a);
    for (0..256) |_| try long_text.appendSlice(a, "token ");
    const long_content_json = try std.json.Stringify.valueAlloc(a, long_text.items, .{});
    defer a.free(long_content_json);
    try std.testing.expectError(error.BoundarySequenceLimitExceeded, node.extractDirect(a, "model", .{
        .schema_version = 1,
        .inputs = &.{.{ .id = "over-qualified-limit", .content_json = long_content_json }},
        .schema_json = legacy_schema,
        .options_json = options_json,
    }));
    const long_texts = [_][]const u8{long_text.items};
    try std.testing.expectError(
        error.BoundarySequenceLimitExceeded,
        node.classifyTextsDirect(a, "model", &long_texts, &labels, null, false),
    );
    const long_http_json = try classificationRequestJson(a, 1, &long_texts, legacy_schema);
    defer a.free(long_http_json);
    var long_http = try dispatchExtraction(a, node, long_http_json);
    defer long_http.deinit();
    try std.testing.expectEqual(@as(u16, 413), long_http.status.code);
    try std.testing.expect(std.mem.indexOf(u8, long_http.body orelse return error.MissingResponseBody, "EXTRACTION_LIMIT_EXCEEDED") != null);
    _ = try expectIdle(node);
}

fn measuredDirect(
    a: Allocator,
    node: *Node,
    comptime backend: BackendType,
    directory: []const u8,
    cached: usize,
    resident_model: memory.AdmissionAmounts,
    case_id: []const u8,
    request: []const u8,
    expected: std.json.Value,
) !u64 {
    const started = perf.nowNs();
    const response = node.decideDirectJsonWithControl(a, request, null) catch |err| {
        reportWorkerFailure(node, backend, "perf-direct", case_id, err);
        return err;
    };
    const elapsed = perf.nowNs() - started;
    defer a.free(response);
    try expectResponse(a, response, expected);
    try std.testing.expectEqual(cached, try expectCached(node, directory, backend));
    _ = try expectIdle(node);
    try expectSameModelWeights(resident_model, try modelOwned(node));
    return elapsed;
}

fn measuredHttp(
    a: Allocator,
    node: *Node,
    comptime backend: BackendType,
    directory: []const u8,
    cached: usize,
    resident_model: memory.AdmissionAmounts,
    case_id: []const u8,
    request: []const u8,
    expected: std.json.Value,
) !u64 {
    const started = perf.nowNs();
    var response = dispatch(a, node, request) catch |err| {
        reportWorkerFailure(node, backend, "perf-http", case_id, err);
        return err;
    };
    const elapsed = perf.nowNs() - started;
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try expectResponse(a, response.body orelse return error.MissingResponseBody, expected);
    try std.testing.expectEqual(cached, try expectCached(node, directory, backend));
    _ = try expectIdle(node);
    try expectSameModelWeights(resident_model, try modelOwned(node));
    return elapsed;
}

fn perfMetadata(
    comptime backend: BackendType,
    case_id: []const u8,
    path: []const u8,
    input_bytes: usize,
    prepared_tokens: usize,
    runtime_cpu_thread_budget: usize,
    source_head: []const u8,
    memory_snapshot: perf.MemorySnapshot,
) perf.Metadata {
    return .{
        .profile = perf_profile,
        .backend = @tagName(backend),
        .case_id = case_id,
        .path = path,
        .model_repo = perf_model_repo,
        .model_revision = perf_model_revision,
        .model_sha256 = pins.@"model.safetensors".sha256,
        .model_size_bytes = pins.@"model.safetensors".size_bytes,
        .sidecars = .{
            .config_json = .{ .sha256 = pins.@"config.json".sha256, .size_bytes = pins.@"config.json".size_bytes },
            .encoder_config_json = .{ .sha256 = pins.@"encoder_config/config.json".sha256, .size_bytes = pins.@"encoder_config/config.json".size_bytes },
            .tokenizer_json = .{ .sha256 = pins.@"tokenizer.json".sha256, .size_bytes = pins.@"tokenizer.json".size_bytes },
            .tokenizer_config_json = .{ .sha256 = pins.@"tokenizer_config.json".sha256, .size_bytes = pins.@"tokenizer_config.json".size_bytes },
        },
        .capture_sha256 = decide_parity.capture_pin.sha256,
        .source_head = source_head,
        .input_bytes = input_bytes,
        .prepared_tokens = prepared_tokens,
        .runtime_cpu_thread_budget = runtime_cpu_thread_budget,
        .sync_pool_parallelism = false,
        .fixture_allocator = "std.testing.allocator",
        .production_allocator = "platform.processAllocator(smp_allocator)",
        .timing_boundary = if (std.mem.eql(u8, path, "direct"))
            "Node.decideDirectJsonWithControl only; response verification, deallocation, identity, and ownership checks excluded"
        else
            "Node.decide in-process HTTP handler dispatch only; response verification, deallocation, identity, and ownership checks excluded",
        .caveats = perf_caveats,
        .memory = memory_snapshot,
    };
}

fn receiptSha256(name: [*:0]const u8) ![]const u8 {
    const value = platform.env.getenv(name) orelse return error.MissingGliner25PerfReceipt;
    if (value.len != 64) return error.InvalidGliner25PerfReceipt;
    for (value) |char| if (!std.ascii.isHex(char)) return error.InvalidGliner25PerfReceipt;
    return value;
}

fn directPerfMetadata(
    case_id: []const u8,
    input_bytes: usize,
    prepared_tokens: usize,
    runtime_cpu_thread_budget: usize,
    source_head: []const u8,
    memory_snapshot: perf.MemorySnapshot,
) !perf.Metadata {
    var metadata = perfMetadata(
        .metal,
        case_id,
        "direct",
        input_bytes,
        prepared_tokens,
        runtime_cpu_thread_budget,
        source_head,
        memory_snapshot,
    );
    metadata.profile = direct_perf_profile;
    metadata.path = "direct_kernel";
    metadata.source_diff_sha256 = try receiptSha256("ANTFLY_GLINER25_PERF_SOURCE_DIFF_SHA256");
    metadata.binary_sha256 = try receiptSha256("ANTFLY_GLINER25_PERF_BINARY_SHA256");
    metadata.measurement_scope = "validated_direct_loaded_session_pipeline_latency";
    metadata.validation_preflights = .{ .direct = 1, .http_handler = 0 };
    metadata.timing_boundary = "classification schema compilation, preprocessing/tokenization, managed loaded-session backend creation, ModernBERT encoder and head, readback, and classification presentation; excludes request parsing, admission and execution-lock acquisition, response serialization, validation, and deallocation";
    metadata.caveats = &.{
        "Direct loaded-session diagnostic fixture; it bypasses the generic extraction request heap and is not serving or HTTP qualification.",
        "Production admission guards are unchanged; this fixture cannot replace the exact direct-plus-HTTP service test.",
        "Each sample acquires the production GPU scratch lease and target execution mutex before its clock, then holds the mutex through preprocessing and model execution; the generic 512 MiB extraction host heap is intentionally absent.",
        "Serial single-request samples; p95 is descriptive and is not a concurrent-load service SLO.",
        "Exact captured token IDs, ordered task/label raw logits at 2e-3, probabilities, and presented outputs at 5e-4 are checked after every call, outside the timed interval.",
        "The clock matches benchmark_family_python.py prepare_decide.execute: schema construction, preprocessing, scoring, and decoding/presentation are included; request parsing and response serialization are excluded.",
        "Three warmups precede twenty measured samples.",
        "The fixture uses std.testing.allocator; production uses platform.processAllocator with smp_allocator.",
    };
    return metadata;
}

fn directExtractionResponseJson(a: Allocator, classifications: []const classification_pipeline.Classification, prompt_tokens: usize) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var rows = std.array_list.Managed(std.json.Value).init(scratch);
    for (classifications) |classification| for (classification.labels) |label| {
        var row: std.json.ObjectMap = .empty;
        try row.put(scratch, "name", .{ .string = classification.name });
        try row.put(scratch, "label", .{ .string = label.label });
        try row.put(scratch, "score", .{ .float = label.confidence });
        try rows.append(.{ .object = row });
    };
    var item: std.json.ObjectMap = .empty;
    try item.put(scratch, "classifications", .{ .array = rows });
    var data = std.array_list.Managed(std.json.Value).init(scratch);
    try data.append(.{ .object = item });
    var usage: std.json.ObjectMap = .empty;
    try usage.put(scratch, "prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
    try usage.put(scratch, "completion_tokens", .{ .integer = 0 });
    var root: std.json.ObjectMap = .empty;
    try root.put(scratch, "data", .{ .array = data });
    try root.put(scratch, "usage", .{ .object = usage });
    return std.json.Stringify.valueAlloc(a, std.json.Value{ .object = root }, .{});
}

fn directKernelSample(
    a: Allocator,
    node: *Node,
    loaded: *@import("model_manager.zig").LoadedModel,
    config: span_executor.EncoderConfig,
    text: []const u8,
    schema_json: []const u8,
    expected_input_ids: []const i64,
    expected_classification: *const CapturedClassification,
    wire_request: decide.Request,
    expected: std.json.Value,
) !u64 {
    return (try directPipelineSample(a, std.testing.io, node, loaded, config, text, schema_json, expected_input_ids, expected_classification, wire_request, expected)).pipeline_ns;
}

fn directPipelineSample(
    a: Allocator,
    io: std.Io,
    node: *Node,
    loaded: *@import("model_manager.zig").LoadedModel,
    config: span_executor.EncoderConfig,
    text: []const u8,
    schema_json: []const u8,
    expected_input_ids: []const i64,
    expected_classification: *const CapturedClassification,
    wire_request: decide.Request,
    expected: std.json.Value,
) !struct { pipeline_ns: u64, encoder_head_ns: u64 } {
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    const control = @import("../execution_control.zig").InferenceExecutionControl{
        .io = io,
        .hard_cancellation = watchdog.boundary(),
        .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s,
        .cancellation_grace_ns = 5 * std.time.ns_per_s,
    };
    const is_gpu = loaded.session.backend() == .metal;
    const resource_class: memory.BackendClass = if (is_gpu) .gpu else .cpu;
    const device_limits = node.config.generation_budget_overrides.apply(
        memory.defaultLimitsForBackendWithProcessLimit(resource_class, node.config.process_memory_limit_bytes),
    );
    const device_bytes = if (is_gpu) try span_executor.deviceScratchUpperBound(config, expected_input_ids.len) else 512 * 1024 * 1024;
    var budget = memory.RunBudget.init(device_limits);
    try budget.reserveEstimate(.{
        .prompt_tokens = 0,
        .retained_tokens = 0,
        .kv_bytes = 0,
        .kv_tier = .host,
        .scratch_bytes = device_bytes,
        .scratch_tier = if (is_gpu) .backend else .host,
    });
    var device_lease = try node.model_manager.acquireRunResourceAmounts(resource_class, device_limits, if (is_gpu) .{ .backend_scratch_bytes = device_bytes } else .{ .host_scratch_bytes = device_bytes });
    defer device_lease.release();
    const execution_mutex = loaded.targetInferenceExecutionMutex();
    if (execution_mutex) |mutex| try control.lock(mutex);
    defer if (execution_mutex) |mutex| mutex.unlock();

    const started = perf.nowNs();
    // Match the Python `prepare_decide.execute` boundary: rebuild the schema,
    // preprocess/tokenize, score the model, and decode/present classifications.
    // JSON request parsing, response serialization, and validation stay outside.
    var compiled = try schema_mod.compile(a, schema_json, .{});
    defer compiled.deinit();
    var prepared = try processor.prepare(a, loaded.getTokenizer(), &.{.{ .text = text, .schema = &compiled }}, .{ .max_sequence_tokens = 198, .max_batch_tokens = 198 });
    defer prepared.deinit();
    const counts = try a.alloc(usize, compiled.schema.classifications.len);
    defer a.free(counts);
    for (compiled.schema.classifications, counts) |classification, *count| count.* = classification.task.labels.len;
    var encoder_elapsed: u64 = 0;
    const rows = rows: {
        var managed = try factory.getManagedComputeBackend(loaded.session, a, &budget, control);
        defer managed.deinit();
        const encoder_started = perf.nowNs();
        const scored = try span_executor.classificationLogits(&managed.backend, a, config, prepared.samples[0], counts);
        encoder_elapsed = perf.nowNs() - encoder_started;
        break :rows scored;
    };
    defer {
        for (rows) |row| a.free(row);
        a.free(rows);
    }
    const const_rows = try a.alloc([]const f64, rows.len);
    defer a.free(const_rows);
    for (rows, const_rows) |row, *out| out.* = row;
    var presented = try classification_pipeline.presentClassifications(a, &compiled, const_rows, 1, .{});
    defer presented.deinit();
    const elapsed = perf.nowNs() - started;

    try std.testing.expectEqualSlices(i64, expected_input_ids, prepared.input_ids);
    try std.testing.expectEqualSlices(i64, expected_classification.input_ids, prepared.input_ids);
    try std.testing.expectEqual(expected_classification.tasks.len, rows.len);
    for (expected_classification.tasks, rows, compiled.schema.classifications) |task, row, classification| {
        try std.testing.expectEqualStrings(task.name, classification.task.name);
        try std.testing.expectEqual(task.labels.len, row.len);
        try std.testing.expectEqual(task.raw_logits.len, row.len);
        for (task.labels, task.raw_logits, row, classification.task.labels) |label, want, got, compiled_label| {
            try std.testing.expectEqualStrings(label, compiled_label);
            try std.testing.expectApproxEqAbs(@as(f64, want), got, 2e-3);
        }
    }
    // Decide's response builder borrows request-arena storage for its parsed
    // extraction tree and distribution maps. Keep validation-only allocations
    // in the same bounded lifetime rather than leaking them on the test owner.
    var validation_arena = std.heap.ArenaAllocator.init(a);
    defer validation_arena.deinit();
    const validation = validation_arena.allocator();
    const extraction_json = try directExtractionResponseJson(validation, presented.classifications, prepared.input_ids.len);
    const response_json = try decide.responseJson(validation, wire_request, extraction_json, .span_marker);
    try expectResponse(validation, response_json, expected);
    return .{ .pipeline_ns = elapsed, .encoder_head_ns = encoder_elapsed };
}

fn directKernelPerformance() !void {
    if (comptime !build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    if (!platform.env.getenvBool("ANTFLY_GLINER25_DECIDE_1B_DIRECT_PERF")) return error.SkipZigTest;
    const requested = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var perf_config = (try perf.Config.fromEnv(a)) orelse return error.MissingGliner25PerfOutputDir;
    defer perf_config.deinit();
    if (!perf_config.profileEnabled(direct_perf_profile)) return error.SkipZigTest;
    const runtime_cpu_thread_budget = try runtimeCpuThreadBudget();
    const perf_source_head = try perf.sourceHead();
    _ = try receiptSha256("ANTFLY_GLINER25_PERF_SOURCE_DIFF_SHA256");
    _ = try receiptSha256("ANTFLY_GLINER25_PERF_BINARY_SHA256");

    const source = try realDirectory(a, requested);
    defer a.free(source);
    try shared.verifyFiles(a, source, pins);
    const fixture_bytes = try decide_parity.referenceBytes(a);
    defer a.free(fixture_bytes);
    var capture = try std.json.parseFromSlice(Capture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), capture.value.public_decide_requests.len);

    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const models_dir = try linkedModel(a, &temporary, source, true);
    defer a.free(models_dir);
    const directory = try std.fs.path.join(a, &.{ models_dir, "model" });
    defer a.free(directory);
    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .process_memory_limit_bytes = service_process_memory_bytes,
        .process_memory_limit_provenance = .explicit,
        .generation_budget_overrides = .{ .host_limit_bytes = service_generation_memory_bytes, .backend_limit_bytes = service_generation_memory_bytes, .scratch_limit_bytes = service_generation_memory_bytes, .combined_limit_bytes = service_generation_memory_bytes, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    requireBackend(&node, .metal);
    try node.attachIo(std.testing.io);
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    const control = @import("../execution_control.zig").InferenceExecutionControl{
        .io = std.testing.io,
        .hard_cancellation = watchdog.boundary(),
        .deadline_ns = platform.time.monotonicNs() + 600 * std.time.ns_per_s,
        .cancellation_grace_ns = 5 * std.time.ns_per_s,
    };
    var handle = try node.model_manager.acquireFromDirWithControl(directory, control);
    var handle_live = true;
    defer if (handle_live) handle.release();
    const loaded = handle.get();
    const session_config = try factory.getGlinerSpanConfig(loaded.session);
    if (session_config != .modern_bert) return error.InvalidFamilyReference;
    const config = span_executor.EncoderConfig{ .modern_bert = session_config.modern_bert };
    const cached = try expectCachedWithActiveHandles(&node, directory, .metal, 1);
    const resident_model = try modelOwned(&node);
    try std.testing.expect(resident_model.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);

    for (capture.value.public_decide_requests) |case| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var wire_request = try decide.parse(arena.allocator(), case.decide_request_json);
        wire_request.model = "model";

        // One exact untimed preflight establishes numeric and ownership parity.
        try std.testing.expectEqualSlices(i64, case.encoded.input_ids, case.native_classification.input_ids);
        _ = try directKernelSample(a, &node, loaded, config, wire_request.state, case.native_schema_json, case.encoded.input_ids, &case.native_classification, wire_request, case.decide_expected);
        _ = try expectIdleWithActiveHandles(&node, 1);
        try std.testing.expectEqual(cached, try expectCachedWithActiveHandles(&node, directory, .metal, 1));
        try expectSameModelWeights(resident_model, try modelOwned(&node));
        var samples = perf.SampleSet.init();
        for (0..perf.warmup_count) |_| {
            _ = try directKernelSample(a, &node, loaded, config, wire_request.state, case.native_schema_json, case.encoded.input_ids, &case.native_classification, wire_request, case.decide_expected);
            _ = try expectIdleWithActiveHandles(&node, 1);
            try expectSameModelWeights(resident_model, try modelOwned(&node));
        }
        for (0..perf.measured_count) |_| {
            try samples.appendMeasured(try directKernelSample(a, &node, loaded, config, wire_request.state, case.native_schema_json, case.encoded.input_ids, &case.native_classification, wire_request, case.decide_expected));
            _ = try expectIdleWithActiveHandles(&node, 1);
            try expectSameModelWeights(resident_model, try modelOwned(&node));
        }
        try samples.write(a, perf_config, try directPerfMetadata(
            case.id,
            case.text.len,
            case.encoded.input_ids.len,
            runtime_cpu_thread_budget,
            perf_source_head,
            try perfMemorySnapshotWithActiveHandles(&node, 1),
        ));
    }
    handle.release();
    handle_live = false;
    _ = try expectIdle(&node);
    try std.testing.expectEqual(cached, try expectCached(&node, directory, .metal));
    try expectSameModelWeights(resident_model, try modelOwned(&node));
    try shared.verifyFiles(a, directory, pins);
}

fn serviceParity(comptime backend: BackendType) !void {
    if (backend == .metal) {
        if (comptime !build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
        if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    }
    const requested = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var perf_config = try perf.Config.fromEnv(a);
    defer if (perf_config) |*config| config.deinit();
    const run_perf = if (perf_config) |config| config.profileEnabled(perf_profile) else false;
    const runtime_cpu_thread_budget = if (run_perf) try runtimeCpuThreadBudget() else 1;
    const perf_source_head = if (run_perf) try perf.sourceHead() else "";
    const source = try realDirectory(a, requested);
    defer a.free(source);
    try shared.verifyFiles(a, source, pins);
    const fixture_bytes = try decide_parity.referenceBytes(a);
    defer a.free(fixture_bytes);
    var capture = try std.json.parseFromSlice(Capture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), capture.value.public_decide_requests.len);

    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    // Exercise the public Node routes against the exact upstream directory,
    // without Antfly pull-time task/capability synthesis. Architecture-aware
    // listing may select the qualified route, while the live session must
    // still prove the pinned weight and sidecar identity before execution.
    const models_dir = try linkedModel(a, &temporary, source, false);
    defer a.free(models_dir);
    const directory = try std.fs.path.join(a, &.{ models_dir, "model" });
    defer a.free(directory);
    try shared.verifyFiles(a, directory, pins);
    const manifest_path = try std.fs.path.join(a, &.{ directory, "model_manifest.json" });
    defer a.free(manifest_path);
    try std.testing.expect(!c_file.fileExists(a, manifest_path));
    var raw_listing = try manifest_mod.loadListingFromDir(a, directory);
    defer raw_listing.deinit();
    try std.testing.expectEqual(@import("../models/gliner_boundary.zig").Architecture.span, raw_listing.gliner_architecture);
    try std.testing.expect(raw_listing.gliner_span_declared);
    try std.testing.expectEqual(manifest_mod.GlinerSpanEncoderFamily.modern_bert, raw_listing.gliner_span_encoder_family);
    try std.testing.expectEqual(@as(usize, 0), raw_listing.tasks.len);
    try std.testing.expectEqual(@as(usize, 0), raw_listing.capabilities.len);
    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        // The measured cold Metal load plus partial lazy F32 host-cache growth
        // already exceeds 7 GiB. The complete pinned mapping, bounded request
        // scratch, tokenizer ownership, default 512 MiB request heap and the
        // mandatory emergency reserve have a 9.46 GiB conservative upper
        // bound. The explicit 10 GiB worker envelope constrains the available
        // memory signal and keeps that ownership contract plus the mandatory
        // 512 MiB emergency reserve within the same process limit.
        .process_memory_limit_bytes = service_process_memory_bytes,
        .process_memory_limit_provenance = .explicit,
        .generation_budget_overrides = .{ .host_limit_bytes = service_generation_memory_bytes, .backend_limit_bytes = service_generation_memory_bytes, .scratch_limit_bytes = service_generation_memory_bytes, .combined_limit_bytes = service_generation_memory_bytes, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    requireBackend(&node, backend);
    try node.attachIo(std.testing.io);

    var stage: []const u8 = "ready";
    var case_id: []const u8 = "<none>";
    var cached: ?usize = null;
    var resident_model: ?memory.AdmissionAmounts = null;
    var cold_recorded = false;
    for (capture.value.public_decide_requests) |case| {
        case_id = case.id;
        stage = "prepare-request";
        const request = try requestJson(a, case.decide_request_json);
        defer a.free(request);
        stage = "direct";
        const cold_started = if (run_perf and !cold_recorded) perf.nowNs() else 0;
        const direct = node.decideDirectJsonWithControl(a, request, null) catch |err| {
            reportWorkerFailure(&node, backend, stage, case_id, err);
            return err;
        };
        const cold_elapsed = if (run_perf and !cold_recorded) perf.nowNs() - cold_started else null;
        defer a.free(direct);
        stage = "direct-response";
        try expectResponse(a, direct, case.decide_expected);
        const direct_idle = expectIdle(&node) catch |err| {
            reportWorkerFailure(&node, backend, stage, case_id, err);
            return err;
        };
        const direct_model = try modelOwned(&node);
        if (cached == null) {
            cached = try expectCached(&node, directory, backend);
            resident_model = direct_model;
            if (backend == .metal)
                try std.testing.expect(resident_model.?.backend_weight_bytes >= pins.@"model.safetensors".size_bytes)
            else
                try std.testing.expect(resident_model.?.host_weight_bytes >= pins.@"model.safetensors".size_bytes);
        } else {
            try std.testing.expectEqual(cached.?, try expectCached(&node, directory, backend));
            try expectSameModelWeights(resident_model.?, direct_model);
        }
        stage = "http";
        var response = dispatch(a, &node, request) catch |err| {
            reportWorkerFailure(&node, backend, stage, case_id, err);
            return err;
        };
        defer response.deinit();
        stage = "http-response";
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        try expectResponse(a, response.body orelse return error.MissingResponseBody, case.decide_expected);
        try std.testing.expectEqual(cached.?, try expectCached(&node, directory, backend));
        const http_idle = expectIdle(&node) catch |err| {
            reportWorkerFailure(&node, backend, stage, case_id, err);
            return err;
        };
        try expectSameModelWeights(resident_model.?, try modelOwned(&node));
        // The HTTP replay has the same geometry as the direct request. Any
        // tokenizer or workspace cache growth must already be reflected in
        // the direct request's owned idle baseline for this case.
        std.testing.expectEqual(direct_idle, http_idle) catch |err| {
            reportWorkerFailure(&node, backend, stage, case_id, err);
            return err;
        };

        if (run_perf) {
            var direct_samples = perf.SampleSet.init();
            var http_samples = perf.SampleSet.init();
            if (cold_elapsed) |elapsed| {
                try direct_samples.recordCold(elapsed);
                cold_recorded = true;
            }
            const cached_session = cached.?;
            const model_weights = resident_model.?;

            for (0..perf.warmup_count) |iteration| {
                if (iteration % 2 == 0) {
                    _ = try measuredDirect(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected);
                    _ = try measuredHttp(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected);
                } else {
                    _ = try measuredHttp(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected);
                    _ = try measuredDirect(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected);
                }
            }
            for (0..perf.measured_count) |iteration| {
                if (iteration % 2 == 0) {
                    try direct_samples.appendMeasured(try measuredDirect(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected));
                    try http_samples.appendMeasured(try measuredHttp(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected));
                } else {
                    try http_samples.appendMeasured(try measuredHttp(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected));
                    try direct_samples.appendMeasured(try measuredDirect(a, &node, backend, directory, cached_session, model_weights, case.id, request, case.decide_expected));
                }
            }
            const memory_snapshot = try perfMemorySnapshot(&node);
            try direct_samples.write(a, perf_config.?, perfMetadata(
                backend,
                case.id,
                "direct",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                memory_snapshot,
            ));
            try http_samples.write(a, perf_config.?, perfMetadata(
                backend,
                case.id,
                "http_handler",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                memory_snapshot,
            ));
        }
    }
    // Performance receipts intentionally retain their original call count and
    // timing scope. These compatibility regressions run only in qualification.
    if (!run_perf) {
        try explicitV1ClassificationRegression(a, &node, directory, capture.value);
        try std.testing.expectEqual(cached.?, try expectCached(&node, directory, backend));
        try expectSameModelWeights(resident_model.?, try modelOwned(&node));
    }
    try shared.verifyFiles(a, directory, pins);
}

test "GLiNER2.5 Decide-1B exact native direct and HTTP service parity" {
    try serviceParity(.native);
}

test "GLiNER2.5 Decide-1B exact Metal direct and HTTP service parity" {
    try serviceParity(.metal);
}

test "GLiNER2.5 Decide-1B Metal direct-only kernel performance" {
    try directKernelPerformance();
}

/// Standalone production-allocation benchmark. Reuses qualification assertions,
/// never the test allocator or test IO. The supervisor supplies a models root
/// containing the pinned checkpoint as `model` and records process provenance.
pub fn runProductionBenchmark(a: Allocator, io: std.Io, args: []const []const u8) !void {
    if (args.len != 2) return error.ExpectedBackendAndModelsDirectory;
    if (std.mem.eql(u8, args[0], "metal")) return productionBenchmark(a, io, .metal, args[1]);
    if (std.mem.eql(u8, args[0], "native")) return productionBenchmark(a, io, .native, args[1]);
    return error.UnsupportedBenchmarkBackend;
}

fn productionBenchmark(a: Allocator, io: std.Io, comptime backend: BackendType, models_dir: []const u8) !void {
    if (backend == .metal and !@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.MetalUnavailable;
    var config = (try perf.Config.fromEnv(a)) orelse return error.MissingBenchmarkOutputDirectory;
    defer config.deinit();
    const head = try perf.sourceHead();
    const diff = try receiptSha256("ANTFLY_GLINER25_PERF_SOURCE_DIFF_SHA256");
    const binary = try receiptSha256("ANTFLY_GLINER25_PERF_BINARY_SHA256");
    const threads = try runtimeCpuThreadBudget();
    if (threads != 2) return error.ExpectedTwoCpuThreads;
    const directory = try std.fs.path.join(a, &.{ models_dir, "model" });
    defer a.free(directory);
    try shared.verifyFiles(a, directory, pins);
    const bytes = try decide_parity.referenceBytes(a);
    defer a.free(bytes);
    var capture = try std.json.parseFromSlice(Capture, a, bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    const holdout_path = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_HOLDOUT") orelse return error.MissingShortHoldout;
    const holdout_bytes = try c_file.readFileMax(a, holdout_path, 2 * 1024 * 1024);
    defer a.free(holdout_bytes);
    var holdout_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(holdout_bytes, &holdout_digest, .{});
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(holdout_digest, .lower), "e429219899d3e3d470113f06756c9e807a286a6ba65cc210fd90b5c628628cff")) return error.InvalidShortHoldout;
    var holdout = try std.json.parseFromSlice(Capture, a, holdout_bytes, .{ .ignore_unknown_fields = true });
    defer holdout.deinit();
    const cases = try std.mem.concat(a, @typeInfo(@TypeOf(capture.value.public_decide_requests)).pointer.child, &.{ capture.value.public_decide_requests, holdout.value.public_decide_requests });
    defer a.free(cases);
    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .process_memory_limit_bytes = service_process_memory_bytes,
        .process_memory_limit_provenance = .explicit,
        .generation_budget_overrides = .{ .host_limit_bytes = service_generation_memory_bytes, .backend_limit_bytes = service_generation_memory_bytes, .scratch_limit_bytes = service_generation_memory_bytes, .combined_limit_bytes = service_generation_memory_bytes, .kv_limit_bytes = 4 * 1024 * 1024 * 1024 },
    });
    defer node.deinit();
    requireBackend(&node, backend);
    try node.attachIo(io);
    for (cases) |case| {
        const request = try requestJson(a, case.decide_request_json);
        defer a.free(request);
        var request_arena = std.heap.ArenaAllocator.init(a);
        defer request_arena.deinit();
        const typed_request = (try @import("antfly_decisions").parse(request_arena.allocator(), request)).inner;
        var samples = [4]perf.SampleSet{ .init(), .init(), .init(), .init() };
        const Stage = @import("extraction_metrics.zig").Stage;
        var stage_start: [std.enums.values(Stage).len]u64 = undefined;
        var cached: ?usize = null;
        var resident: ?memory.AdmissionAmounts = null;
        for (0..1 + perf.warmup_count + perf.measured_count) |iteration| {
            if (iteration == 1 + perf.warmup_count) {
                for (std.enums.values(Stage), &stage_start) |stage, *value| value.* = node.metrics.extraction_v2.phase_ns.get(stage);
            }
            for (0..2) |offset| {
                const path_index = (iteration + offset) % 2;
                const started = perf.nowNs();
                var response: ?httpx.Response = null;
                const result = if (path_index == 0)
                    try node.decideDirectJsonWithControl(a, request, null)
                else blk: {
                    response = try dispatchWithIo(a, io, &node, request);
                    if (response.?.status.code != 200) {
                        std.debug.print("HTTP benchmark failure: {s}\n", .{response.?.body orelse "empty response"});
                        response.?.deinit();
                        return error.UnsuccessfulBenchmarkResponse;
                    }
                    break :blk response.?.body orelse return error.MissingResponseBody;
                };
                const elapsed = perf.nowNs() - started;
                defer if (response) |*value| value.deinit() else a.free(result);
                try expectResponse(a, result, case.decide_expected);
                var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
                defer parsed.deinit();
                const tokens = parsed.value.object.get("usage").?.object.get("input_tokens").?.integer;
                try std.testing.expectEqual(@as(i64, @intCast(case.encoded.input_ids.len)), tokens);
                const identity = try expectCached(&node, directory, backend);
                const weights = try modelOwned(&node);
                if (cached) |prior| try std.testing.expectEqual(prior, identity);
                if (resident) |prior| try expectSameModelWeights(prior, weights);
                cached = identity;
                resident = weights;
                _ = try expectIdle(&node);
                if (iteration > perf.warmup_count) try samples[path_index].appendMeasured(elapsed);
            }
            var handle = try node.model_manager.acquireFromDir(directory);
            const loaded = handle.get();
            const encoder = switch (try factory.getGlinerSpanConfig(loaded.session)) {
                .modern_bert => |value| span_executor.EncoderConfig{ .modern_bert = value },
                else => return error.ExpectedModernBertSession,
            };
            const timing = directPipelineSample(a, io, &node, loaded, encoder, case.text, case.native_schema_json, case.encoded.input_ids, &case.native_classification, typed_request, case.decide_expected) catch |err| {
                handle.release();
                return err;
            };
            handle.release();
            _ = try expectIdle(&node);
            if (iteration > perf.warmup_count) {
                try samples[2].appendMeasured(timing.pipeline_ns);
                try samples[3].appendMeasured(timing.encoder_head_ns);
            }
        }
        for (std.enums.values(Stage), stage_start) |stage, before| {
            const total_ns = node.metrics.extraction_v2.phase_ns.get(stage) - before;
            std.debug.print("decide_service_stage case={s} stage={s} mean_ns={d} samples=40\n", .{ case.id, @tagName(stage), total_ns / 40 });
        }
        const snapshot = try perfMemorySnapshot(&node);
        for ([_][]const u8{ "direct", "http_handler", "loaded_pipeline", "encoder_head" }, 0..) |path, index| {
            var metadata = perfMetadata(backend, case.id, path, case.text.len, case.encoded.input_ids.len, threads, head, snapshot);
            if (!std.mem.eql(u8, case.id, "described_prompt_choice") and !std.mem.eql(u8, case.id, "choice_score_noul"))
                metadata.capture_sha256 = "e429219899d3e3d470113f06756c9e807a286a6ba65cc210fd90b5c628628cff";
            metadata.source_diff_sha256 = diff;
            metadata.binary_sha256 = binary;
            metadata.fixture_allocator = "platform.processAllocator(smp_allocator)";
            metadata.measurement_scope = if (index < 2) "validated_production_allocator_service_latency" else "validated_production_allocator_pipeline_latency";
            if (index >= 2) metadata.timing_boundary = if (index == 2)
                "schema compilation, preprocessing, managed backend setup, encoder, head, readback and classification presentation; excludes admission, execution-lock acquisition, Decide JSON serialization and validation"
            else
                "prepared encoder, classification head and completed readback; excludes schema, preprocessing, admission, managed backend setup and presentation";
            metadata.caveats = &.{
                "Standalone executable using production allocator and IO; no test runner or testing allocator.",
                "Three warmups and twenty samples follow a validation preflight on both paths.",
                "HTTP means in-process handler dispatch, excluding socket transport; all request admission and serialization are timed.",
                "Response, token count, cached identity and idle ownership validation run outside each timed interval.",
                "Serial p95 is descriptive, not a concurrent-load service SLO.",
            };
            try samples[index].writeWithIo(a, io, config, metadata);
        }
    }
    try shared.verifyFiles(a, directory, pins);
}
