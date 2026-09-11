// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual managed-Metal cache lifecycle through generated loopback HTTP.
//! The model-manager test bridge passes its private production maintenance
//! implementation an exact timestamp. No clock, counter, admission lease,
//! backend selector or capability is changed process-wide.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const manager = @import("model_manager.zig");
const shared = @import("gliner_boundary_service_test.zig");
const sockets = @import("gliner_boundary_socket_test.zig");
const factory = @import("../architectures/session_factory.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const boundary = @import("../models/gliner_boundary.zig");
const memory = @import("../runtime/tier/memory.zig");
const metal = @import("../backends/metal_tensor.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Node = server.Node;
const Io = std.Io;
const MiB = 1024 * 1024;
const GiB = 1024 * MiB;
const keep_alive_ms = 30 * 60 * 1000;
const Maintenance = *const fn (*manager.ModelManager, ?u64) void;

fn config(directory: []const u8) !server.NodeConfig {
    return .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = keep_alive_ms,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = GiB,
            .backend_limit_bytes = 4 * GiB,
            .combined_limit_bytes = 5 * GiB,
            .scratch_limit_bytes = 3 * GiB,
        },
    };
}

fn requireMetal(node: *Node) void {
    node.session_manager.preferred_backends = &.{.metal};
    node.session_manager.required_backend = .metal;
    node.session_manager.required_backend_invalid = false;
    node.model_manager.session_manager.preferred_backends = &.{.metal};
    node.model_manager.session_manager.required_backend = .metal;
    node.model_manager.session_manager.required_backend_invalid = false;
}

fn control(node: *Node) !Control {
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    return .{
        .io = std.testing.io,
        .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s,
        .hard_cancellation = watchdog.boundary(),
    };
}

const Entry = struct {
    model_address: usize,
    session_address: usize,
    last_used_ns: u64,
    aliases: usize,
    lease: memory.AdmissionAmounts,
};
const Snapshot = struct {
    domain_address: usize,
    amounts: memory.AdmissionAmounts,
    entry: ?Entry,
};

fn inspect(node: *Node, directory: []const u8, pins: pipeline.PublishedModelFiles, count: usize, handles: usize) !Snapshot {
    const admission = node.inference_admission.stats();
    try std.testing.expectEqual(@as(usize, 0), admission.in_flight_requests);
    try std.testing.expectEqual(@as(usize, 0), admission.in_flight_units);
    try std.testing.expectEqual(@as(i64, 0), @atomicLoad(i64, &node.metrics.requests_active.impl.value, .monotonic));
    try std.testing.expectEqual(@as(i64, 0), @atomicLoad(i64, &node.metrics.extraction_v2.active.impl.value, .monotonic));
    const active = try control(node);
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    var entry: ?Entry = null;
    {
        try active.lock(&node.model_manager.load_lock);
        defer node.model_manager.load_lock.unlock();
        try std.testing.expectEqual(count, node.model_manager.loaded.count());
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_loads.count());
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.composite_assets.count());
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_composite_assets.count());
        if (count == 0) {
            try std.testing.expectEqual(@as(usize, 0), handles);
            try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded_aliases.count());
        } else {
            try std.testing.expectEqual(@as(usize, 1), count);
            var iterator = node.model_manager.loaded.valueIterator();
            const loaded = iterator.next().?.*;
            try std.testing.expect(loaded.session.close_protection != null);
            try std.testing.expectEqualStrings(directory, loaded.model_dir);
            try std.testing.expectEqual(handles, loaded.active_handles);
            try std.testing.expect(!loaded.pinned and !loaded.retired);
            try std.testing.expectEqual(.metal, loaded.session.backend());
            try std.testing.expectEqual(.process_required, loaded.session.interruption());
            const gate = loaded.targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
            try active.lock(gate);
            defer gate.unlock();
            const identity = try factory.getGlinerBoundaryIdentity(loaded.session);
            try std.testing.expectEqual(boundary.Backbone.small, (try factory.getGlinerBoundaryConfig(loaded.session)).backbone);
            try std.testing.expectEqual(.fp32, identity.precision);
            try std.testing.expectEqualStrings(pins.@"model.safetensors".sha256, &identity.weight.sha256);
            try std.testing.expectEqual(@as(u64, pins.@"model.safetensors".size_bytes), identity.weight.size_bytes);
            inline for (.{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" }, 0..) |name, index| {
                const pin = @field(pins, name);
                try std.testing.expectEqualStrings(pin.sha256, &identity.sidecars[index].sha256);
                try std.testing.expectEqual(@as(u64, pin.size_bytes), identity.sidecars[index].size_bytes);
            }
            const lease = loaded.resource_lease orelse return error.MissingModelResourceLease;
            try std.testing.expect(lease.controller == &domain.admission);
            try std.testing.expect(lease.amounts.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);
            const tokenizer_lease = loaded.tokenizer_resource_lease orelse return error.MissingTokenizerResourceLease;
            try std.testing.expect(tokenizer_lease.controller == &domain.admission);
            try std.testing.expect(tokenizer_lease.amounts.host_weight_bytes > 0);
            const run_admission = loaded.session.run_admission orelse return error.MissingSessionRunAdmission;
            try std.testing.expect(run_admission.controller == &domain.admission);
            try std.testing.expectEqual(.gpu, run_admission.backend_class);
            try std.testing.expect(run_admission.check_live_memory);
            // HTTP's default alias is borrowed from the canonical cache owner.
            // Its removal is part of eviction, not a later request side effect.
            try std.testing.expect(node.model_manager.loaded_aliases.get(directory) == loaded);
            entry = .{
                .model_address = @intFromPtr(loaded),
                .session_address = @intFromPtr(loaded.session.ptr),
                .last_used_ns = loaded.last_used_ns,
                .aliases = node.model_manager.loaded_aliases.count(),
                .lease = lease.amounts,
            };
        }
    }
    const amounts = domain.admission.snapshot();
    try std.testing.expectEqual(@as(usize, 0), amounts.host_scratch_bytes);
    try std.testing.expectEqual(if (entry) |loaded| loaded.lease.backend_scratch_bytes else 0, amounts.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.backend_kv_bytes);
    const watchdog = node.hard_cancellation_watchdog.?;
    {
        try active.lock(&watchdog.mutex);
        defer watchdog.mutex.unlock();
        try std.testing.expect(watchdog.io != null);
        try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    }
    if (node.model_manager.teardown_domain) |teardown| {
        try active.lock(&teardown.watchdog.mutex);
        defer teardown.watchdog.mutex.unlock();
        try std.testing.expect(teardown.watchdog.io != null);
        try std.testing.expectEqual(count, teardown.watchdog.entries.items.len);
    } else try std.testing.expectEqual(@as(usize, 0), count);
    return .{ .domain_address = @intFromPtr(domain), .amounts = amounts, .entry = entry };
}

fn expire(node: *Node, maintenance: Maintenance, now_ns: u64) !void {
    const active = try control(node);
    // Production ModelManager owns each session's dormant teardown ticket.
    // Do not lend this test request's watchdog to cache maintenance.
    const domain = node.model_manager.teardown_domain orelse return error.MissingTeardownDomain;
    try std.testing.expect(domain.watchdog != node.hard_cancellation_watchdog.?);
    try std.testing.expect(domain.watchdog.io != null);
    maintenance(&node.model_manager, now_ns);
    try active.check();
}

fn expiry(entry: Entry) !u64 {
    return std.math.add(u64, entry.last_used_ns, keep_alive_ms * std.time.ns_per_ms);
}

fn samePhysical(expected: metal.MemoryStats) !void {
    const actual = metal.memoryStatsSnapshot();
    try std.testing.expectEqual(expected.device_owned_live_bytes, actual.device_owned_live_bytes);
    try std.testing.expectEqual(expected.host_mirror_live_bytes, actual.host_mirror_live_bytes);
    try std.testing.expectEqual(actual.device_owned_bytes_created - expected.device_owned_bytes_created, actual.device_owned_bytes_released - expected.device_owned_bytes_released);
}

fn success(a: std.mem.Allocator, transport: *sockets.Loopback, raw: []const u8, name: []const u8, case: anytype) !i64 {
    var response = try transport.post(raw);
    defer response.deinit();
    const tokens = try shared.successfulResponse(a, &response, name, case.id, case.text, case.expected);
    try transport.idle();
    return tokens;
}

/// Called only by the test declared inside model_manager.zig, where the
/// maintenance implementation remains private. No test import in server.zig
/// or production clock override is required.
pub fn exercise(maintenance: Maintenance) !void {
    if (comptime !build_options.enable_metal or builtin.os.tag != .macos) return error.SkipZigTest;
    const requested = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path: [Io.Dir.max_path_bytes]u8 = undefined;
    const length = if (std.fs.path.isAbsolute(requested))
        try Io.Dir.realPathFileAbsolute(std.testing.io, requested, &path)
    else
        try Io.Dir.cwd().realPathFile(std.testing.io, requested, &path);
    const directory = path[0..length];
    const name = std.fs.path.basename(directory);
    const raw_fixture = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(raw_fixture);
    var fixture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, raw_fixture, .{});
    defer fixture.deinit();
    const case = fixture.value.cases[0];
    const pins = fixture.value.model_files;
    try std.testing.expectEqualStrings("mixed_tasks", case.id);
    try shared.verifyFiles(a, directory, pins);
    const raw = try shared.requestBytes(a, name, case.schema, &.{.{ .id = case.id, .content = case.text }});
    defer a.free(raw);
    const initial_physical = metal.memoryStatsSnapshot();
    {
        var node = try Node.init(a, try config(directory));
        defer node.deinit();
        requireMetal(&node);
        try node.attachIo(std.testing.io);
        const transport = try sockets.Loopback.init(a, &node);
        defer transport.deinit();
        try transport.start();
        try std.testing.expect(!boundary.runtime_available and !node.test_allow_unqualified_gliner_boundary);
        {
            var response = try transport.post(raw);
            defer response.deinit();
            try shared.errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
        }
        try transport.idle();
        const empty = try inspect(&node, directory, pins, 0, 0);
        try std.testing.expectEqual(memory.AdmissionAmounts{}, empty.amounts);
        node.test_allow_unqualified_gliner_boundary = true;
        const tokens = try success(a, transport, raw, name, case);
        const warm = try inspect(&node, directory, pins, 1, 0);
        try std.testing.expect(warm.amounts.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);
        try std.testing.expectEqual(empty.domain_address, warm.domain_address);
        const warm_physical = metal.memoryStatsSnapshot();
        try std.testing.expect(warm_physical.device_owned_live_bytes > initial_physical.device_owned_live_bytes);
        var held = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingCachedModel;
        // Idempotent release always precedes listener and Node destruction on
        // failure. There is no borrowed pointer used after final eviction.
        defer held.release();
        const retained = try inspect(&node, directory, pins, 1, 1);
        const retained_physical = metal.memoryStatsSnapshot();
        try expire(&node, maintenance, try expiry(retained.entry.?));
        try std.testing.expectEqual(retained, try inspect(&node, directory, pins, 1, 1));
        try samePhysical(retained_physical);
        try std.testing.expectEqual(tokens, try success(a, transport, raw, name, case));
        const reused = try inspect(&node, directory, pins, 1, 1);
        try std.testing.expectEqual(retained.entry.?.model_address, reused.entry.?.model_address);
        try std.testing.expectEqual(retained.entry.?.session_address, reused.entry.?.session_address);
        try std.testing.expectEqual(retained.amounts, reused.amounts);
        held.release();
        const released = try inspect(&node, directory, pins, 1, 0);
        try std.testing.expect(released.entry.?.last_used_ns >= reused.entry.?.last_used_ns);
        const at = try expiry(released.entry.?);
        try expire(&node, maintenance, at - 1);
        try std.testing.expectEqual(released, try inspect(&node, directory, pins, 1, 0));
        try expire(&node, maintenance, at);
        try std.testing.expectEqual(empty, try inspect(&node, directory, pins, 0, 0));
        try samePhysical(initial_physical);
        if (node.model_manager.acquireLoadedModel(directory)) |found| {
            var unexpected = found;
            unexpected.release();
            return error.EvictedAliasStillReachable;
        }
        // Repeated maintenance stays empty, and source bytes remain unchanged.
        try expire(&node, maintenance, at);
        try std.testing.expectEqual(empty, try inspect(&node, directory, pins, 0, 0));
        try shared.verifyFiles(a, directory, pins);
        const before_reload = metal.memoryStatsSnapshot();
        try std.testing.expectEqual(tokens, try success(a, transport, raw, name, case));
        const reloaded = try inspect(&node, directory, pins, 1, 0);
        try std.testing.expectEqual(empty.domain_address, reloaded.domain_address);
        try std.testing.expectEqual(warm.amounts, reloaded.amounts);
        const reload_physical = metal.memoryStatsSnapshot();
        try std.testing.expectEqual(warm_physical.device_owned_live_bytes, reload_physical.device_owned_live_bytes);
        try std.testing.expectEqual(warm_physical.host_mirror_live_bytes, reload_physical.host_mirror_live_bytes);
        try std.testing.expect(reload_physical.device_owned_bytes_created - before_reload.device_owned_bytes_created >= warm_physical.device_owned_live_bytes - initial_physical.device_owned_live_bytes);
        // Allocation may reuse addresses after eviction. The empty-cache and
        // physical release/reallocation transitions prove reload, not pointer
        // inequality between objects from different lifetimes.
        try std.testing.expectEqual(tokens, try success(a, transport, raw, name, case));
        const retried = try inspect(&node, directory, pins, 1, 0);
        try std.testing.expectEqual(reloaded.entry.?.model_address, retried.entry.?.model_address);
        try std.testing.expectEqual(reloaded.entry.?.session_address, retried.entry.?.session_address);
        try std.testing.expectEqual(reloaded.amounts, retried.amounts);
        {
            var response = try transport.request(.GET, "/ml/v1/metrics", null);
            defer response.deinit();
            try sockets.metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 5);
            try sockets.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"success\"}", 4);
            try sockets.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"unsupported\"}", 1);
            try sockets.metric(&response, "antfly_inference_extract_v2_parsed_items_total", 5);
            try sockets.metric(&response, "antfly_inference_extract_v2_decoded_items_total", 4);
            try sockets.metric(&response, "antfly_inference_extract_v2_returned_items_total", 4);
            try sockets.metric(&response, "antfly_inference_extract_v2_active", 0);
        }
        try transport.idle();
        try expire(&node, maintenance, try expiry(retried.entry.?));
        try std.testing.expectEqual(empty, try inspect(&node, directory, pins, 0, 0));
        try samePhysical(initial_physical);
        try transport.finish();
        try std.testing.expect(!boundary.runtime_available);
    }
    try samePhysical(initial_physical);
    try shared.verifyFiles(a, directory, pins);
}
