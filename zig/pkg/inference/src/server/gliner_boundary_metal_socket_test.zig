// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Guarded, opt-in qualification of the generated HTTP route with an actual
//! managed Metal session. This profile is deliberately distinct from the CPU
//! socket fixture: the unchanged executor admits 2 GiB encoder + 256 MiB head
//! device scratch in addition to its 512 MiB request heap. Neither backend
//! selection nor the physical-memory guard is changed process-wide.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const shared = @import("gliner_boundary_service_test.zig");
const socket = @import("gliner_boundary_socket_test.zig");
const factory = @import("../architectures/session_factory.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const model = @import("../models/gliner_boundary.zig");
const executor = @import("../extractors/gliner_boundary_executor.zig");
const memory = @import("../runtime/tier/memory.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const metal_tensor = @import("../backends/metal_tensor.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Io = std.Io;
const MiB = 1024 * 1024;
const GiB = 1024 * MiB;
const clock = platform.time.monotonicNs;
const metadata_bytes = 8 * MiB;
const request_heap_bytes = 512 * MiB;

fn limits(scratch_bytes: usize) memory.Limits {
    return .{
        .host_limit_bytes = GiB,
        .backend_limit_bytes = 4 * GiB,
        .combined_limit_bytes = 5 * GiB,
        .scratch_limit_bytes = scratch_bytes,
    };
}

fn config(directory: []const u8, scratch_bytes: usize) !server.NodeConfig {
    const bound = limits(scratch_bytes);
    return .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = bound.host_limit_bytes,
            .backend_limit_bytes = bound.backend_limit_bytes,
            .combined_limit_bytes = bound.combined_limit_bytes,
            .scratch_limit_bytes = bound.scratch_limit_bytes,
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

fn inspectionControl(node: *Node) !Control {
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    return .{
        .io = std.testing.io,
        .deadline_ns = clock() + 5 * std.time.ns_per_s,
        .hard_cancellation = watchdog.boundary(),
    };
}

/// No execution or model handle may remain after the transport has become
/// idle. Model and workspace leases retain their own scratch, reconciled
/// separately from request-local storage.
fn idle(node: *Node) !memory.AdmissionAmounts {
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(i64, 0), node.metrics.requests_active.impl.value);
    try std.testing.expectEqual(@as(i64, 0), node.metrics.extraction_v2.active.impl.value);
    const control = try inspectionControl(node);
    var retained: memory.AdmissionAmounts = .{};
    {
        try control.lock(&node.model_manager.load_lock);
        defer node.model_manager.load_lock.unlock();
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_loads.count());
        var iterator = node.model_manager.loaded.valueIterator();
        while (iterator.next()) |entry| {
            const loaded = entry.*;
            try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
            try std.testing.expectEqual(.metal, loaded.session.backend());
            const lease = loaded.resource_lease orelse return error.MissingModelResourceLease;
            try std.testing.expect(lease.controller != null);
            // No active handle exists and this cache lock excludes new ones.
            // The workspace reader copies scalars, without taking a GPU lease.
            retained = try retained.merge(lease.amounts);
            retained = try retained.merge(factory.glinerBoundaryWorkspaceAdmissionAmounts(loaded.session));
            const mutex = loaded.targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
            try control.lock(mutex);
            mutex.unlock();
        }
    }
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const amounts = domain.admission.snapshot();
    try std.testing.expectEqual(retained.host_scratch_bytes, amounts.host_scratch_bytes);
    try std.testing.expectEqual(retained.backend_scratch_bytes, amounts.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.backend_kv_bytes);
    const watchdog = node.hard_cancellation_watchdog.?;
    try control.lock(&watchdog.mutex);
    defer watchdog.mutex.unlock();
    try std.testing.expect(watchdog.io != null);
    try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    return amounts;
}

const CachedIdentity = struct {
    model: usize,
    generation: u64,
    workspace_generation: u64,
    model_bytes: usize,
    workspace_bytes: usize,
};

fn cachedModel(node: *Node, directory: []const u8, pins: pipeline.PublishedModelFiles) !CachedIdentity {
    const control = try inspectionControl(node);
    try control.lock(&node.model_manager.load_lock);
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
    try std.testing.expectEqual(.metal, loaded.session.backend());
    try std.testing.expectEqual(.process_required, loaded.session.interruption());
    try std.testing.expect(loaded.targetInferenceExecutionMutex() != null);
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
    const lease = loaded.resource_lease orelse return error.MissingModelResourceLease;
    try std.testing.expect(lease.amounts.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);
    const resident = try factory.glinerBoundaryResidentStats(loaded.session);
    try std.testing.expect(resident.ready);
    try std.testing.expect(resident.generation != 0 and resident.workspace_generation != 0);
    try std.testing.expect(resident.model_live_bytes > 0 and resident.workspace_live_bytes > 0);
    try std.testing.expectEqual(resident.workspace_capacity_bytes, resident.workspace_live_bytes);
    return .{
        .model = @intFromPtr(loaded),
        .generation = resident.generation,
        .workspace_generation = resident.workspace_generation,
        .model_bytes = resident.model_live_bytes,
        .workspace_bytes = resident.workspace_live_bytes,
    };
}

/// Acquire only the already-loaded session, with its ordinary model mutex,
/// resource lease and watchdog. This metadata inspection issues no tensor
/// upload, inference, download, or counter reset. It cannot qualify a CPU
/// fallback merely from a requested backend name or admission byte estimate.
fn inspectBackend(node: *Node, directory: []const u8) !void {
    var snapshot = try node.model_manager.acquireLoadedModelSnapshot(std.testing.allocator);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.handles.len);
    const loaded = snapshot.handles[0].get();
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    const control = try inspectionControl(node);
    var metadata_lease = try node.model_manager.acquireRunResourceAmounts(.gpu, limits(3 * GiB), .{ .host_scratch_bytes = metadata_bytes });
    defer metadata_lease.release();
    var budget = BoundedAllocator{ .backing = std.testing.allocator, .limit = metadata_bytes };
    defer std.debug.assert(budget.live == 0);
    const mutex = loaded.targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
    try control.lock(mutex);
    defer mutex.unlock();
    var managed = try factory.getManagedComputeBackend(loaded.session, budget.allocator(), null, control);
    defer managed.deinit();
    try std.testing.expectEqual(.metal, managed.backend.kind());
    try std.testing.expect(managed.backend.vtable.glinerBoundaryDevice != null);
    try std.testing.expect(managed.backend.vtable.glinerBoundaryDownload != null);
    try std.testing.expect(managed.backend.decoderRuntimeReady());
    try std.testing.expect(!managed.backend.decoderRuntimeHasActiveFrame());
    const stats = managed.backend.debugTimingSnapshot();
    try std.testing.expect(!stats.native_quant_null);
    // These are physical MetalTensor counters populated by this provider,
    // not reserved/admitted bytes. Every boundary operation has a strict
    // resident-input contract; a host fallback is an error in this executor.
    try std.testing.expect(stats.provider.metal_tensor_device_owned_buffers_created > 0);
}

fn expectDeviceWork(before: metal_tensor.MemoryStats, after: metal_tensor.MemoryStats) !void {
    // The standard runner is serial. Read only after HTTP task drain and never
    // reset process counters: other tests retain their own cumulative evidence.
    try std.testing.expect(after.device_owned_buffers_created > before.device_owned_buffers_created);
    try std.testing.expect(after.device_owned_bytes_created > before.device_owned_bytes_created);
    try std.testing.expect(after.device_owned_buffers_released > before.device_owned_buffers_released);
    try std.testing.expect(after.device_owned_bytes_released > before.device_owned_bytes_released);
}

test "gliner boundary socket Metal profile accounts for unchanged device and host scratch" {
    const device_bytes = try executor.deviceScratchUpperBound(.{});
    try std.testing.expectEqual(@as(usize, 2304 * MiB), device_bytes);
    var small = memory.RunBudget.init(limits(128 * MiB));
    try small.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 128 * MiB, .scratch_tier = .host });
    try std.testing.expectError(error.MemoryBudgetExceeded, small.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = device_bytes, .scratch_tier = .backend }));
    try std.testing.expectEqual(@as(usize, 128 * MiB), small.scratchTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), small.backendTotalBytes());
    var admitted = memory.RunBudget.init(limits(3 * GiB));
    try admitted.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = request_heap_bytes, .scratch_tier = .host });
    try admitted.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = device_bytes, .scratch_tier = .backend });
    try std.testing.expectEqual(@as(usize, 2816 * MiB), admitted.scratchTotalBytes());
}

test "gliner boundary socket pinned small Metal managed HTTP success atomic recovery and residency" {
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
    const bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer fixture.deinit();
    const pins = fixture.value.model_files;
    try shared.verifyFiles(a, directory, pins);
    const case = fixture.value.cases[0];
    try std.testing.expectEqualStrings("mixed_tasks", case.id);
    const raw = try shared.requestBytes(a, name, case.schema, &.{.{ .id = case.id, .content = case.text }});
    defer a.free(raw);
    const original_physical = metal_tensor.memoryStatsSnapshot();
    {
        var node = try Node.init(a, try config(directory, 3 * GiB));
        defer node.deinit();
        requireMetal(&node);
        try node.attachIo(std.testing.io);
        const transport = try socket.Loopback.init(a, &node);
        defer transport.deinit();
        // Loopback preserves Node.serve's resource-owner startup. A cold or
        // default-rejected request still has an authoritative empty domain,
        // with no model or synthetic admission reservation.
        const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
        try std.testing.expectEqual(memory.AdmissionAmounts{}, domain.admission.snapshot());
        try transport.start();
        try std.testing.expect(!node.test_allow_unqualified_gliner_boundary);
        {
            var response = try transport.post(raw);
            defer response.deinit();
            try shared.errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
        }
        try transport.idle();
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
        try std.testing.expectEqual(memory.AdmissionAmounts{}, try idle(&node));
        node.test_allow_unqualified_gliner_boundary = true;
        var prompt_tokens: i64 = 0;
        const before_request = metal_tensor.memoryStatsSnapshot();
        {
            var response = try transport.post(raw);
            defer response.deinit();
            prompt_tokens = try shared.successfulResponse(a, &response, name, case.id, case.text, case.expected);
        }
        try transport.idle();
        try expectDeviceWork(before_request, metal_tensor.memoryStatsSnapshot());
        const cached = try cachedModel(&node, directory, pins);
        const resident = try idle(&node);
        try std.testing.expect(resident.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);
        try inspectBackend(&node, directory);
        try std.testing.expectEqual(resident, try idle(&node));
        {
            const batch = try shared.requestBytes(a, name, case.schema, &.{
                .{ .id = "valid-first", .content = case.text },
                .{ .id = "rejected-second", .content = "x " ** 4097 },
            });
            defer a.free(batch);
            const before_failure = metal_tensor.memoryStatsSnapshot();
            var response = try transport.post(batch);
            defer response.deinit();
            try shared.errorResponse(a, &response, 413, "EXTRACTION_LIMIT_EXCEEDED", "tokenizing", 1);
            try transport.idle();
            // Whole-request workspace geometry rejects the second item before
            // either row reaches a CB. No new allocation, view, transfer or
            // learned result may appear on this atomic failure path.
            try std.testing.expectEqual(before_failure, metal_tensor.memoryStatsSnapshot());
        }
        try std.testing.expectEqual(resident, try idle(&node));
        try std.testing.expectEqual(cached, try cachedModel(&node, directory, pins));
        {
            const retry = try shared.requestBytes(a, name, case.schema, &.{.{ .content = case.text }});
            defer a.free(retry);
            const before_retry = metal_tensor.memoryStatsSnapshot();
            var response = try transport.post(retry);
            defer response.deinit();
            try std.testing.expectEqual(prompt_tokens, try shared.successfulResponse(a, &response, name, null, case.text, case.expected));
            try transport.idle();
            try expectDeviceWork(before_retry, metal_tensor.memoryStatsSnapshot());
        }
        try std.testing.expectEqual(resident, try idle(&node));
        try std.testing.expectEqual(cached, try cachedModel(&node, directory, pins));
        {
            var response = try transport.request(.GET, "/ml/v1/metrics", null);
            defer response.deinit();
            try socket.metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 4);
            try socket.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"success\"}", 2);
            try socket.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"unsupported\"}", 1);
            try socket.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"resource_limit\"}", 1);
            try socket.metric(&response, "antfly_inference_extract_v2_failures_total{stage=\"model\"}", 1);
            try socket.metric(&response, "antfly_inference_extract_v2_failures_total{stage=\"tokenizing\"}", 1);
            try socket.metric(&response, "antfly_inference_extract_v2_parsed_items_total", 5);
            try socket.metric(&response, "antfly_inference_extract_v2_decoded_items_total", 2);
            try socket.metric(&response, "antfly_inference_extract_v2_returned_items_total", 2);
            try socket.metric(&response, "antfly_inference_extract_v2_decoded_prompt_tokens_total", @intCast(2 * prompt_tokens));
            try socket.metric(&response, "antfly_inference_extract_v2_active", 0);
        }
        try transport.idle();
        try std.testing.expectEqual(resident, try idle(&node));
        try transport.finish();
        _ = try idle(&node);
        try std.testing.expect(!model.runtime_available);
    }
    const final_physical = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(original_physical.device_owned_live_bytes, final_physical.device_owned_live_bytes);
    try std.testing.expectEqual(original_physical.host_mirror_live_bytes, final_physical.host_mirror_live_bytes);
    try std.testing.expectEqual(
        final_physical.device_owned_bytes_created - original_physical.device_owned_bytes_created,
        final_physical.device_owned_bytes_released - original_physical.device_owned_bytes_released,
    );
    try shared.verifyFiles(a, directory, pins);
}
