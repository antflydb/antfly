// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in real-artifact service qualification for the released multilingual
//! GLiNER2.5 boundary checkpoints. Production policy imports this test only
//! after exact identity, feature and geometry rows have been reviewed.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const model = @import("../models/gliner_boundary.zig");
const factory = @import("../architectures/session_factory.zig");
const memory = @import("../runtime/tier/memory.zig");
const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
const shared = @import("gliner_boundary_service_test.zig");
const registry = @import("../registry/registry.zig");
const extracting = @import("antfly_extracting");
const c_file = @import("../util/c_file.zig");
const perf = @import("gliner_family_perf.zig");
const BackendType = @import("../backends/backends.zig").BackendType;
const Allocator = std.mem.Allocator;
const Value = std.json.Value;

// These opt-in service fixtures model a dedicated 7 GiB worker. Admission
// retains 2.515 GB of model weights while the Metal request holds 2.953 GB of
// transient workspace, so the generation domain needs 6 GiB. The process
// envelope also covers the latched load/request epoch and mandatory 512 MiB
// emergency reserve while continuing to use real physical telemetry.
const service_worker_memory_bytes = 7 * 1024 * 1024 * 1024;
const service_generation_memory_bytes = 6 * 1024 * 1024 * 1024;

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

fn reportWorkerAdmissionFailure(node: *Node, err: anyerror) void {
    const process = platform.process_memory.pressureSnapshot();
    const host = memory.currentSystemMemoryInfo();
    const worker = memory.currentSystemMemoryInfoForLimit(service_worker_memory_bytes, .explicit);
    const admitted = if (node.model_manager.resource_domain) |domain| domain.admission.snapshot() else memory.AdmissionAmounts{};
    const owned = ownershipSnapshot(node);
    std.debug.print(
        "GLiNER family worker failure={s} process_footprint={d} process_rss={d} host={any} worker={any} admitted={any} model_owned={any} tokenizer_load_owned={any} tokenizer_cache_owned={any} weight_cache_owned={any} workspace_owned={any}\n",
        .{ @errorName(err), process.footprint_bytes, process.resident_bytes, host, worker, admitted, owned.model, owned.tokenizer_load, owned.tokenizer_cache, owned.weight_cache, owned.workspace },
    );
}

const Profile = struct {
    name: []const u8,
    repo: []const u8,
    revision: []const u8,
    environment: [*:0]const u8,
    capture_fixture: []const u8,
    capture: pipeline.PublishedModelPin,
    pins: pipeline.PublishedModelFiles,
};

const profiles = [_]Profile{
    .{
        .name = "multi_v1",
        .repo = "fastino/gliner2.5-multi-v1",
        .revision = "2ca71aafb3446d9014e1c55c7ff51c9bc7209c47",
        .environment = "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR",
        .capture_fixture = "family/multi_v1_capture.json",
        .capture = .{ .sha256 = "4c46106eaa56b5899ca607cb4deca877d197ecb0f3800ac5198c44a17fbddcd2", .size_bytes = 74084 },
        .pins = .{
            .@"config.json" = .{ .sha256 = "8b59a0f426a65859c89cd1ea850c3529c09aa3be3a6fafd8eddfdd17b1bf0146", .size_bytes = 3151 },
            .@"encoder_config/config.json" = .{ .sha256 = "fa4f9ef2903b5369ab172333aae4574e6a476511d7465845cf59f8360ee18716", .size_bytes = 857 },
            .@"model.safetensors" = .{ .sha256 = "c1ff4ec0bc00031c15530b8f3c33d3677f27949e6a0cb52e1247a6224b6c5395", .size_bytes = 1149461028 },
            .@"tokenizer.json" = .{ .sha256 = "c62446df87ae18ec98b133f8f84fc449a07cc89bbf8ef192a4cb5f9c53777a7a", .size_bytes = 16035853 },
            .@"tokenizer_config.json" = .{ .sha256 = "0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1", .size_bytes = 645 },
        },
    },
    .{
        .name = "multi_decide",
        .repo = "fastino/GLiNER2.5-multi-Decide",
        .revision = "a35a0cd3b7a0f00f2effc576f454cd48fa98aa5f",
        .environment = "ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR",
        .capture_fixture = "family/multi_decide_capture.json",
        .capture = .{ .sha256 = "06163fe217ff9769df3a045a30c9583cd6c352e72a23077c11e7aaa6ce59074d", .size_bytes = 68545 },
        .pins = .{
            .@"config.json" = .{ .sha256 = "be5123080c0f3f01b938bc46a5dd0d7a2e515a34f6df798dfd70ed04c277c8bf", .size_bytes = 3152 },
            .@"encoder_config/config.json" = .{ .sha256 = "d0ebbcb8b458e285a39e12cc315cbaf3d1c6f631e7281e6b22dd5b4071183f83", .size_bytes = 858 },
            .@"model.safetensors" = .{ .sha256 = "9efe0f88c99f2aa794452e9559dc60e98d60d9fa2bf1b60cf2710411b6da5b4e", .size_bytes = 1149461028 },
            .@"tokenizer.json" = .{ .sha256 = "c62446df87ae18ec98b133f8f84fc449a07cc89bbf8ef192a4cb5f9c53777a7a", .size_bytes = 16035853 },
            .@"tokenizer_config.json" = .{ .sha256 = "fd4a31dc2f1f17e31638c5f0e783b81cdb2fbe6bddd116a8d9e5d50d78148cf1", .size_bytes = 646 },
        },
    },
};

const Capture = struct {
    requests: []const struct {
        id: []const u8,
        text: []const u8,
        native_schema_json: []const u8,
        native_expected: pipeline.ExpectedSample,
        encoded: struct { input_ids: []const i64 },
    },
};

const DecideCapture = struct {
    requests: []const struct {
        id: []const u8,
        text: []const u8,
        decide_request_json: []const u8,
        encoded: struct { input_ids: []const i64 },
    },
};

const decide_capture_pin = pipeline.PublishedModelPin{ .sha256 = "04d9bbf219858166a1b0d7199b12673dd81eac7fae37454757d213debdd6e732", .size_bytes = 23332 };

fn pinBytes(pin: pipeline.PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(pin.size_bytes, bytes.len);
    try std.testing.expectEqualStrings(pin.sha256, &bundle.Digest.of(bytes).sha256);
}

fn realDirectory(a: Allocator, requested: []const u8) ![]u8 {
    var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const length = if (std.fs.path.isAbsolute(requested))
        try std.Io.Dir.realPathFileAbsolute(std.testing.io, requested, &buffer)
    else
        try std.Io.Dir.cwd().realPathFile(std.testing.io, requested, &buffer);
    return a.dupe(u8, buffer[0..length]);
}

fn fixtureCase(capture: Capture, id: []const u8) !@TypeOf(capture.requests[0]) {
    for (capture.requests) |request| if (std.mem.eql(u8, request.id, id)) return request;
    return error.MissingFamilyReferenceCase;
}

fn requestJson(a: Allocator, name: []const u8, case: anytype) ![]u8 {
    var schema = try std.json.parseFromSlice(Value, a, case.native_schema_json, .{ .duplicate_field_behavior = .@"error" });
    defer schema.deinit();
    return std.json.Stringify.valueAlloc(a, .{
        .schema_version = @as(u32, 2),
        .model = name,
        .schema = schema.value,
        .inputs = &.{.{ .id = case.id, .content = case.text }},
        .options = .{ .include_confidence = true, .include_spans = true, .offset_unit = "unicode_codepoints" },
    }, .{});
}

fn dispatchExtraction(a: Allocator, node: *Node, raw: []const u8) !httpx.Response {
    var request = try httpx.Request.init(a, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body = raw;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    context.max_request_body_size = 64 * 1024;
    context.application_deadline_ns = platform.time.monotonicNs() + 240 * std.time.ns_per_s;
    return node.extractJSON(&context);
}

fn dispatchDecide(a: Allocator, node: *Node, raw: []const u8) !httpx.Response {
    var request = try httpx.Request.init(a, .POST, "/ai/v1/decide");
    defer request.deinit();
    request.body = raw;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    context.max_request_body_size = 64 * 1024;
    context.application_deadline_ns = platform.time.monotonicNs() + 240 * std.time.ns_per_s;
    return node.decide(&context);
}

fn expectEntities(a: Allocator, json: []const u8, expected: pipeline.ExpectedSample) !void {
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), data.len);
    const actual = data[0].object.get("entities").?.array.items;
    var count: usize = 0;
    for (expected.entities) |group| count += group.values.len;
    try std.testing.expectEqual(count, actual.len);
    var index: usize = 0;
    for (expected.entities) |group| for (group.values) |value| {
        const entity = actual[index].object;
        index += 1;
        try std.testing.expectEqualStrings(group.name, entity.get("label").?.string);
        try std.testing.expectEqualStrings(value.text, entity.get("text").?.string);
        try std.testing.expectApproxEqAbs(@as(f64, value.confidence), entity.get("score").?.float, pipeline.fp32_confidence_tolerance);
        try std.testing.expectEqual(@as(i64, @intCast(value.source.?.start)), entity.get("start").?.integer);
        try std.testing.expectEqual(@as(i64, @intCast(value.source.?.end)), entity.get("end").?.integer);
    };
}

fn expectCached(node: *Node, directory: []const u8, pins: pipeline.PublishedModelFiles, backend: BackendType) !usize {
    while (!node.model_manager.load_lock.tryLock()) std.atomic.spinLoopHint();
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
    try std.testing.expectEqual(backend, loaded.session.backend());
    try std.testing.expectEqual(model.Backbone.multi, (try factory.getGlinerBoundaryConfig(loaded.session)).backbone);
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

fn expectIdle(node: *Node) !memory.AdmissionAmounts {
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
            try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
            if (loaded.resource_lease) |lease| retained = try retained.merge(lease.amounts);
            if (loaded.tokenizer_resource_lease) |lease| retained = try retained.merge(lease.amounts);
            retained = try retained.merge(factory.sharedCacheAdmissionAmounts(loaded.session));
            retained = try retained.merge(factory.glinerBoundaryWorkspaceAdmissionAmounts(loaded.session));
        }
    }
    retained = try retained.merge(try node.model_manager.tokenizerCacheAdmissionAmounts());
    const amounts = node.model_manager.resource_domain.?.admission.snapshot();
    // Metal retains its planned workspace with the cached model, and BPE
    // caches retain separately owned admission quanta. At idle the domain
    // must equal those explicit owners exactly; request leases must be gone.
    try std.testing.expectEqual(retained, amounts);
    if (node.hard_cancellation_watchdog) |watchdog| {
        while (!watchdog.mutex.tryLock()) std.atomic.spinLoopHint();
        defer watchdog.mutex.unlock();
        try std.testing.expect(watchdog.io != null);
        try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    }
    return amounts;
}

fn expectSameRetainedWeights(expected: memory.AdmissionAmounts, actual: memory.AdmissionAmounts) !void {
    try std.testing.expectEqual(expected.host_weight_bytes, actual.host_weight_bytes);
    try std.testing.expectEqual(expected.backend_weight_bytes, actual.backend_weight_bytes);
}

fn requireBackend(node: *Node, comptime backend: BackendType) void {
    node.session_manager.preferred_backends = &.{backend};
    node.session_manager.required_backend = backend;
    node.session_manager.required_backend_invalid = false;
    node.model_manager.session_manager.preferred_backends = &.{backend};
    node.model_manager.session_manager.required_backend = backend;
    node.model_manager.session_manager.required_backend_invalid = false;
}

fn requireAvailable(comptime backend: BackendType) !void {
    if (backend != .metal) return;
    if (comptime !build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
}

fn hardLink(a: Allocator, source: []const u8, destination: []const u8) !void {
    const source_z = try a.dupeSentinel(u8, source, 0);
    defer a.free(source_z);
    const destination_z = try a.dupeSentinel(u8, destination, 0);
    defer a.free(destination_z);
    if (c_file.c.link(source_z.ptr, destination_z.ptr) != 0) return error.FamilyServiceFixtureLinkFailed;
}

fn linkedDecisionModel(a: Allocator, temporary: *std.testing.TmpDir, source: []const u8) ![:0]u8 {
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
    // Exercise the same exact-byte publication gate as a real registry pull;
    // a handwritten test manifest could bypass or drift from that contract.
    const model_dir = try std.fs.path.join(a, &.{ root, "model" });
    defer a.free(model_dir);
    const manifest_json = try registry.synthesizePulledModelManifestJson(a, model_dir, null, null);
    defer a.free(manifest_json);
    try temporary.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = manifest_json });
    return root;
}

fn decisionRequest(a: Allocator, captured: []const u8, model_name: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(Value, a, captured, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    try parsed.value.object.put(parsed.arena.allocator(), "model", .{ .string = model_name });
    return std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

fn probability(map: std.json.ObjectMap, name: []const u8, expected: f64) !void {
    try std.testing.expectApproxEqAbs(expected, map.get(name).?.float, 5e-4);
}

fn expectDecision(a: Allocator, json: []const u8, model_name: []const u8, id: []const u8) !void {
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings(model_name, root.get("model").?.string);
    const answers = root.get("answers").?.object;
    if (std.mem.eql(u8, id, "described_prompt_choice")) {
        const action = answers.get("action").?.object;
        try std.testing.expectEqualStrings("choice", action.get("type").?.string);
        try std.testing.expectEqualStrings("replace_card", action.get("choice").?.string);
        const values = action.get("probabilities").?.object;
        try probability(values, "send_pin", 0.4240283701104467);
        try probability(values, "replace_card", 0.4789577168900583);
        try probability(values, "balance_help", 0.097013912999495);
        return;
    }
    try std.testing.expectEqualStrings("choice_score_noul", id);
    const response = answers.get("response").?.object;
    try std.testing.expectEqualStrings("rollback", response.get("choice").?.string);
    try probability(response.get("probabilities").?.object, "rollback", 0.9035260779404779);
    const severity = answers.get("severity").?.object;
    try std.testing.expectApproxEqAbs(@as(f64, 2.2798491190792314), severity.get("score").?.float, 5e-4);
    const levels = severity.get("probabilities").?.object;
    try probability(levels, "0", 0.07849329957840633);
    try probability(levels, "1", 0.1146990468336053);
    try probability(levels, "2", 0.25527288851833924);
    try probability(levels, "3", 0.5515347650696492);
    try std.testing.expectApproxEqAbs(@as(f64, 0.6232684367094173), answers.get("page_on_call").?.object.get("noul").?.float, 5e-4);
}

const perf_caveats = &[_][]const u8{
    "Serial descriptive samples; p95 is not a concurrent-load or production-SLO claim.",
    "HTTP samples invoke the in-process handler and do not traverse a bound socket or TCP stack.",
    "Correctness validation, response destruction, identity checks, and ownership checks are outside the timed interval.",
    "The cold first-direct sample includes load, preprocessing, model execution, and presentation; files were preverified and OS filesystem cache state is uncontrolled.",
    "No pure model-load decomposition is reported.",
    "source_head identifies the checked-out commit; retain the working-tree diff when sampling locally modified sources.",
    "The three warmups for each path follow one exact-output and ownership validation preflight for direct and in-process HTTP handler paths.",
    "These are validated test-fixture latencies using std.testing.allocator; production uses processAllocator(smp_allocator).",
};

fn performanceConfig(a: Allocator) !?perf.Config {
    var result = try perf.Config.fromEnv(a);
    if (result) |*config| if (config.profile_filter) |filter| {
        if (!std.mem.eql(u8, filter, "multi_v1") and !std.mem.eql(u8, filter, "multi_decide")) {
            config.deinit();
            return error.InvalidGliner25PerfProfile;
        }
    };
    return result;
}

fn runtimeCpuThreadBudget() !usize {
    const configured = platform.env.getenv("ANTFLY_INFERENCE_CPU_THREADS") orelse return 1;
    const value = try std.fmt.parseUnsigned(usize, configured, 10);
    if (value == 0) return error.InvalidInferenceCpuThreadBudget;
    return value;
}

fn performanceLedger(amounts: memory.AdmissionAmounts) perf.IdleLedger {
    return .{
        .host_weight_bytes = amounts.host_weight_bytes,
        .backend_weight_bytes = amounts.backend_weight_bytes,
        .host_kv_bytes = amounts.host_kv_bytes,
        .backend_kv_bytes = amounts.backend_kv_bytes,
        .host_scratch_bytes = amounts.host_scratch_bytes,
        .backend_scratch_bytes = amounts.backend_scratch_bytes,
    };
}

fn performanceMemory(node: *Node, idle: memory.AdmissionAmounts) perf.MemorySnapshot {
    const process = platform.process_memory.pressureSnapshot();
    const owned = ownershipSnapshot(node);
    return .{
        .process_footprint_bytes = @intCast(process.footprint_bytes),
        .process_rss_bytes = @intCast(process.resident_bytes),
        .idle_ledger = performanceLedger(idle),
        .ownership = .{
            .model = performanceLedger(owned.model),
            .tokenizer_load = performanceLedger(owned.tokenizer_load),
            .tokenizer_cache = performanceLedger(owned.tokenizer_cache),
            .weight_cache = performanceLedger(owned.weight_cache),
            .workspace = performanceLedger(owned.workspace),
        },
    };
}

fn performanceMetadata(
    profile: Profile,
    backend: BackendType,
    capture_sha256: []const u8,
    case_id: []const u8,
    path: []const u8,
    input_bytes: usize,
    prepared_tokens: usize,
    runtime_cpu_thread_budget: usize,
    source_head: []const u8,
    timing_boundary: []const u8,
    memory_snapshot: perf.MemorySnapshot,
) perf.Metadata {
    return .{
        .profile = profile.name,
        .backend = @tagName(backend),
        .case_id = case_id,
        .path = path,
        .model_repo = profile.repo,
        .model_revision = profile.revision,
        .model_sha256 = profile.pins.@"model.safetensors".sha256,
        .model_size_bytes = profile.pins.@"model.safetensors".size_bytes,
        .sidecars = .{
            .config_json = .{ .sha256 = profile.pins.@"config.json".sha256, .size_bytes = profile.pins.@"config.json".size_bytes },
            .encoder_config_json = .{ .sha256 = profile.pins.@"encoder_config/config.json".sha256, .size_bytes = profile.pins.@"encoder_config/config.json".size_bytes },
            .tokenizer_json = .{ .sha256 = profile.pins.@"tokenizer.json".sha256, .size_bytes = profile.pins.@"tokenizer.json".size_bytes },
            .tokenizer_config_json = .{ .sha256 = profile.pins.@"tokenizer_config.json".sha256, .size_bytes = profile.pins.@"tokenizer_config.json".size_bytes },
        },
        .capture_sha256 = capture_sha256,
        .source_head = source_head,
        .input_bytes = input_bytes,
        .prepared_tokens = prepared_tokens,
        .runtime_cpu_thread_budget = runtime_cpu_thread_budget,
        .sync_pool_parallelism = false,
        .fixture_allocator = "std.testing.allocator",
        .production_allocator = "processAllocator(smp_allocator)",
        .timing_boundary = timing_boundary,
        .caveats = perf_caveats,
        .memory = memory_snapshot,
    };
}

fn sampleExtractionDirect(
    a: Allocator,
    node: *Node,
    name: []const u8,
    directory: []const u8,
    profile: Profile,
    backend: BackendType,
    case: anytype,
    content_json: []const u8,
    cached: usize,
    resident: memory.AdmissionAmounts,
) !u64 {
    const start = perf.nowNs();
    var result = node.extractDirect(a, name, extracting.Request{
        .schema_version = 2,
        .inputs = &.{.{ .id = case.id, .content_json = content_json }},
        .schema_json = case.native_schema_json,
        .options_json = "{\"include_confidence\":true,\"include_spans\":true,\"offset_unit\":\"unicode_codepoints\"}",
    }) catch |err| {
        reportWorkerAdmissionFailure(node, err);
        return err;
    };
    const elapsed = perf.nowNs() - start;
    defer result.deinit();
    try expectEntities(a, result.json, case.native_expected);
    try std.testing.expectEqual(cached, try expectCached(node, directory, profile.pins, backend));
    try std.testing.expectEqual(resident, try expectIdle(node));
    return elapsed;
}

fn sampleExtractionHttp(
    a: Allocator,
    node: *Node,
    directory: []const u8,
    profile: Profile,
    backend: BackendType,
    case: anytype,
    raw: []const u8,
    cached: usize,
    resident: memory.AdmissionAmounts,
) !u64 {
    const start = perf.nowNs();
    var response = try dispatchExtraction(a, node, raw);
    const elapsed = perf.nowNs() - start;
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try expectEntities(a, response.body orelse return error.MissingResponseBody, case.native_expected);
    try std.testing.expectEqual(cached, try expectCached(node, directory, profile.pins, backend));
    try std.testing.expectEqual(resident, try expectIdle(node));
    return elapsed;
}

fn sampleDecisionDirect(a: Allocator, node: *Node, directory: []const u8, backend: BackendType, request: []const u8, case_id: []const u8, cached: usize, resident: memory.AdmissionAmounts) !u64 {
    const start = perf.nowNs();
    const result = node.decideDirectJsonWithControl(a, request, null) catch |err| {
        reportWorkerAdmissionFailure(node, err);
        return err;
    };
    const elapsed = perf.nowNs() - start;
    defer a.free(result);
    try expectDecision(a, result, "model", case_id);
    try std.testing.expectEqual(cached, try expectCached(node, directory, profiles[1].pins, backend));
    try std.testing.expectEqual(resident, try expectIdle(node));
    return elapsed;
}

fn sampleDecisionHttp(a: Allocator, node: *Node, directory: []const u8, backend: BackendType, request: []const u8, case_id: []const u8, cached: usize, resident: memory.AdmissionAmounts) !u64 {
    const start = perf.nowNs();
    var response = try dispatchDecide(a, node, request);
    const elapsed = perf.nowNs() - start;
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    try expectDecision(a, response.body orelse return error.MissingResponseBody, "model", case_id);
    try std.testing.expectEqual(cached, try expectCached(node, directory, profiles[1].pins, backend));
    try std.testing.expectEqual(resident, try expectIdle(node));
    return elapsed;
}

fn familyExtractionServiceParity(comptime backend: BackendType) !void {
    try requireAvailable(backend);
    const a = std.testing.allocator;
    var perf_config = try performanceConfig(a);
    defer if (perf_config) |*config| config.deinit();
    const runtime_cpu_thread_budget = if (perf_config != null) try runtimeCpuThreadBudget() else 1;
    const perf_source_head = if (perf_config != null) try perf.sourceHead() else "";
    for (profiles) |profile| {
        if (perf_config) |config| if (!config.profileEnabled(profile.name)) continue;
        const requested = platform.env.getenv(profile.environment) orelse return error.SkipZigTest;
        const directory = try realDirectory(a, requested);
        defer a.free(directory);
        const models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath;
        const name = std.fs.path.basename(directory);
        try shared.verifyFiles(a, directory, profile.pins);
        const capture_bytes = try fixtures.fixtureBytes(a, profile.capture_fixture);
        defer a.free(capture_bytes);
        try pinBytes(profile.capture, capture_bytes);
        var capture = try std.json.parseFromSlice(Capture, a, capture_bytes, .{ .ignore_unknown_fields = true });
        defer capture.deinit();
        const case = try fixtureCase(capture.value, "spanish_entities");

        var node = try Node.init(a, .{
            .models_dir = models_dir,
            .max_loaded_models = 1,
            .max_concurrent_requests = 1,
            .keep_alive_ms = 30 * 60 * 1000,
            .process_termination_available = true,
            .process_memory_limit_bytes = service_worker_memory_bytes,
            .process_memory_limit_provenance = .explicit,
            .generation_budget_overrides = .{ .host_limit_bytes = service_generation_memory_bytes, .backend_limit_bytes = service_generation_memory_bytes, .scratch_limit_bytes = service_generation_memory_bytes, .combined_limit_bytes = service_generation_memory_bytes, .kv_limit_bytes = service_generation_memory_bytes },
        });
        defer node.deinit();
        requireBackend(&node, backend);
        try node.attachIo(std.testing.io);

        const content_json = try std.json.Stringify.valueAlloc(a, case.text, .{});
        defer a.free(content_json);
        var cold_elapsed: u64 = undefined;
        {
            const cold_start = perf.nowNs();
            var direct = node.extractDirect(a, name, extracting.Request{
                .schema_version = 2,
                .inputs = &.{.{ .id = case.id, .content_json = content_json }},
                .schema_json = case.native_schema_json,
                .options_json = "{\"include_confidence\":true,\"include_spans\":true,\"offset_unit\":\"unicode_codepoints\"}",
            }) catch |err| {
                reportWorkerAdmissionFailure(&node, err);
                return err;
            };
            cold_elapsed = perf.nowNs() - cold_start;
            defer direct.deinit();
            try expectEntities(a, direct.json, case.native_expected);
        }
        const cached = try expectCached(&node, directory, profile.pins, backend);
        const resident = try expectIdle(&node);
        if (backend == .metal)
            try std.testing.expect(resident.backend_weight_bytes >= profile.pins.@"model.safetensors".size_bytes)
        else
            try std.testing.expect(resident.host_weight_bytes >= profile.pins.@"model.safetensors".size_bytes);

        const raw = try requestJson(a, name, case);
        defer a.free(raw);
        if (perf_config) |config| {
            var direct_samples = perf.SampleSet.init();
            var http_samples = perf.SampleSet.init();
            try direct_samples.recordCold(cold_elapsed);
            // The cold direct call above and this handler call are validation
            // preflights. Neither is part of the three explicit warmups.
            _ = try sampleExtractionHttp(a, &node, directory, profile, backend, case, raw, cached, resident);
            for (0..perf.warmup_count + perf.measured_count) |index| {
                var direct_ns: u64 = undefined;
                var http_ns: u64 = undefined;
                if (index % 2 == 0) {
                    direct_ns = try sampleExtractionDirect(a, &node, name, directory, profile, backend, case, content_json, cached, resident);
                    http_ns = try sampleExtractionHttp(a, &node, directory, profile, backend, case, raw, cached, resident);
                } else {
                    http_ns = try sampleExtractionHttp(a, &node, directory, profile, backend, case, raw, cached, resident);
                    direct_ns = try sampleExtractionDirect(a, &node, name, directory, profile, backend, case, content_json, cached, resident);
                }
                if (index >= perf.warmup_count) {
                    try direct_samples.appendMeasured(direct_ns);
                    try http_samples.appendMeasured(http_ns);
                }
            }
            const final_idle = try expectIdle(&node);
            const snapshot = performanceMemory(&node, final_idle);
            try direct_samples.write(a, config, performanceMetadata(
                profile,
                backend,
                profile.capture.sha256,
                case.id,
                "direct",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                "node.extractDirect call: preprocessing, model execution, presentation, and JSON serialization",
                snapshot,
            ));
            try http_samples.write(a, config, performanceMetadata(
                profile,
                backend,
                profile.capture.sha256,
                case.id,
                "http_handler",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                "in-process HTTP handler dispatch: request/body parse, preprocessing, model execution, presentation, and JSON serialization",
                snapshot,
            ));
        } else {
            var response = try dispatchExtraction(a, &node, raw);
            defer response.deinit();
            try std.testing.expectEqual(@as(u16, 200), response.status.code);
            try expectEntities(a, response.body orelse return error.MissingResponseBody, case.native_expected);
            try std.testing.expectEqual(resident, try expectIdle(&node));
            try std.testing.expectEqual(cached, try expectCached(&node, directory, profile.pins, backend));
        }
        try shared.verifyFiles(a, directory, profile.pins);
    }
}

test "GLiNER2.5 multilingual family pinned native extraction direct and HTTP service parity" {
    try familyExtractionServiceParity(.native);
}

test "GLiNER2.5 multilingual family pinned Metal extraction direct and HTTP service parity" {
    try familyExtractionServiceParity(.metal);
}

fn familyDecisionServiceParity(comptime backend: BackendType) !void {
    try requireAvailable(backend);
    const a = std.testing.allocator;
    var perf_config = try performanceConfig(a);
    defer if (perf_config) |*config| config.deinit();
    const runtime_cpu_thread_budget = if (perf_config != null) try runtimeCpuThreadBudget() else 1;
    const perf_source_head = if (perf_config != null) try perf.sourceHead() else "";
    if (perf_config) |config| if (!config.profileEnabled("multi_decide")) return;
    const requested = platform.env.getenv("ANTFLY_GLINER25_MULTI_DECIDE_MODEL_DIR") orelse return error.SkipZigTest;
    const source = try realDirectory(a, requested);
    defer a.free(source);
    try shared.verifyFiles(a, source, profiles[1].pins);
    const fixture_bytes = try fixtures.fixtureBytes(a, "family/multi_decide_decide_capture.json");
    defer a.free(fixture_bytes);
    try pinBytes(decide_capture_pin, fixture_bytes);
    var capture = try std.json.parseFromSlice(DecideCapture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer capture.deinit();
    try std.testing.expectEqual(@as(usize, 2), capture.value.requests.len);

    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const models_dir = try linkedDecisionModel(a, &temporary, source);
    defer a.free(models_dir);
    const directory = try std.fs.path.join(a, &.{ models_dir, "model" });
    defer a.free(directory);
    try shared.verifyFiles(a, directory, profiles[1].pins);
    var node = try Node.init(a, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .process_memory_limit_bytes = service_worker_memory_bytes,
        .process_memory_limit_provenance = .explicit,
        .generation_budget_overrides = .{ .host_limit_bytes = service_generation_memory_bytes, .backend_limit_bytes = service_generation_memory_bytes, .scratch_limit_bytes = service_generation_memory_bytes, .combined_limit_bytes = service_generation_memory_bytes, .kv_limit_bytes = service_generation_memory_bytes },
    });
    defer node.deinit();
    requireBackend(&node, backend);
    try node.attachIo(std.testing.io);

    var cached: ?usize = null;
    var resident_weights: ?memory.AdmissionAmounts = null;
    for (capture.value.requests) |case| {
        const request = try decisionRequest(a, case.decide_request_json, "model");
        defer a.free(request);
        const first_direct = cached == null;
        var cold_elapsed: u64 = undefined;
        {
            const cold_start = perf.nowNs();
            const direct = node.decideDirectJsonWithControl(a, request, null) catch |err| {
                reportWorkerAdmissionFailure(&node, err);
                return err;
            };
            cold_elapsed = perf.nowNs() - cold_start;
            defer a.free(direct);
            try expectDecision(a, direct, "model", case.id);
        }
        const direct_idle = try expectIdle(&node);
        if (cached == null) {
            cached = try expectCached(&node, directory, profiles[1].pins, backend);
            resident_weights = direct_idle;
            if (backend == .metal)
                try std.testing.expect(resident_weights.?.backend_weight_bytes >= profiles[1].pins.@"model.safetensors".size_bytes)
            else
                try std.testing.expect(resident_weights.?.host_weight_bytes >= profiles[1].pins.@"model.safetensors".size_bytes);
        } else {
            try std.testing.expectEqual(cached.?, try expectCached(&node, directory, profiles[1].pins, backend));
            try expectSameRetainedWeights(resident_weights.?, direct_idle);
        }
        if (perf_config) |config| {
            var direct_samples = perf.SampleSet.init();
            var http_samples = perf.SampleSet.init();
            if (first_direct) try direct_samples.recordCold(cold_elapsed);
            // The direct call above and this handler call validate both arms
            // before their three explicit paired warmups.
            _ = try sampleDecisionHttp(a, &node, directory, backend, request, case.id, cached.?, direct_idle);
            for (0..perf.warmup_count + perf.measured_count) |index| {
                var direct_ns: u64 = undefined;
                var http_ns: u64 = undefined;
                if (index % 2 == 0) {
                    direct_ns = try sampleDecisionDirect(a, &node, directory, backend, request, case.id, cached.?, direct_idle);
                    http_ns = try sampleDecisionHttp(a, &node, directory, backend, request, case.id, cached.?, direct_idle);
                } else {
                    http_ns = try sampleDecisionHttp(a, &node, directory, backend, request, case.id, cached.?, direct_idle);
                    direct_ns = try sampleDecisionDirect(a, &node, directory, backend, request, case.id, cached.?, direct_idle);
                }
                if (index >= perf.warmup_count) {
                    try direct_samples.appendMeasured(direct_ns);
                    try http_samples.appendMeasured(http_ns);
                }
            }
            const final_idle = try expectIdle(&node);
            const snapshot = performanceMemory(&node, final_idle);
            try direct_samples.write(a, config, performanceMetadata(
                profiles[1],
                backend,
                decide_capture_pin.sha256,
                case.id,
                "direct",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                "node.decideDirectJsonWithControl call: request translation, preprocessing, model execution, decision presentation, and JSON serialization",
                snapshot,
            ));
            try http_samples.write(a, config, performanceMetadata(
                profiles[1],
                backend,
                decide_capture_pin.sha256,
                case.id,
                "http_handler",
                case.text.len,
                case.encoded.input_ids.len,
                runtime_cpu_thread_budget,
                perf_source_head,
                "in-process HTTP handler dispatch: request/body parse, translation, preprocessing, model execution, decision presentation, and JSON serialization",
                snapshot,
            ));
        } else {
            var response = try dispatchDecide(a, &node, request);
            defer response.deinit();
            try std.testing.expectEqual(@as(u16, 200), response.status.code);
            try expectDecision(a, response.body orelse return error.MissingResponseBody, "model", case.id);
            try std.testing.expectEqual(cached.?, try expectCached(&node, directory, profiles[1].pins, backend));
            const http_idle = try expectIdle(&node);
            try expectSameRetainedWeights(resident_weights.?, http_idle);
            // The HTTP replay has the same geometry as the direct request, so it
            // must reuse the now-sized owned workspace rather than growing it.
            try std.testing.expectEqual(direct_idle, http_idle);
        }
    }
    try shared.verifyFiles(a, directory, profiles[1].pins);
}

test "GLiNER2.5 multilingual Decide pinned native direct and HTTP distributions" {
    try familyDecisionServiceParity(.native);
}

test "GLiNER2.5 multilingual Decide pinned Metal direct and HTTP distributions" {
    try familyDecisionServiceParity(.metal);
}
