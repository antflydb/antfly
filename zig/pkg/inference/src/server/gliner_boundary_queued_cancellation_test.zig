// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A real managed-Metal HTTP request owns admitted host scratch while waiting
//! for the existing model mutex to snapshot its resident workspace. Observe
//! completed preprocessing, reset its owned TCP socket, and require cancellation
//! to drain that request while the test still holds the mutex. The immutable
//! model and workspace retain their own leases. This proves queued cancellation,
//! not interruption inside an encoder/kernel. No production hooks or global
//! backend policy changes are used. Kept separate from ledger-pinned tests.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server.zig");
const Node = server.Node;
const manager = @import("model_manager.zig");
const shared = @import("gliner_boundary_service_test.zig");
const sockets = @import("gliner_boundary_socket_test.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const factory = @import("../architectures/session_factory.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const boundary = @import("../models/gliner_boundary.zig");
const executor = @import("../extractors/gliner_boundary_executor.zig");
const memory = @import("../runtime/tier/memory.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Io = std.Io;
const clock = platform.time.monotonicNs;
const MiB = 1024 * 1024;
const GiB = 1024 * MiB;
const wait_ms = 5_000;
const request_heap_bytes = 512 * MiB;

fn config(directory: []const u8) !server.NodeConfig {
    return .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .keep_alive_ms = 30 * 60 * 1000,
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

fn controlUntil(node: *Node, deadline_ns: u64) !Control {
    const watchdog = node.hard_cancellation_watchdog orelse return error.MissingHardCancellationWatchdog;
    return .{ .io = std.testing.io, .deadline_ns = deadline_ns, .hard_cancellation = watchdog.boundary() };
}

fn shortControl(node: *Node) !Control {
    return controlUntil(node, clock() + wait_ms * std.time.ns_per_ms);
}

const CacheIdentity = struct {
    model: usize,
    session: usize,
    model_generation: u64,
    workspace_generation: u64,
    model_bytes: usize,
    workspace_bytes: usize,
};

fn cached(node: *Node, directory: []const u8, pins: pipeline.PublishedModelFiles, handles: usize) !CacheIdentity {
    const control = try shortControl(node);
    try control.lock(&node.model_manager.load_lock);
    defer node.model_manager.load_lock.unlock();
    try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_loads.count());
    var iterator = node.model_manager.loaded.valueIterator();
    const loaded = iterator.next().?.*;
    try std.testing.expectEqualStrings(directory, loaded.model_dir);
    try std.testing.expectEqual(handles, loaded.active_handles);
    try std.testing.expectEqual(.metal, loaded.session.backend());
    try std.testing.expectEqual(.process_required, loaded.session.interruption());
    try std.testing.expect(loaded.targetInferenceExecutionMutex() != null);
    try std.testing.expectEqual(boundary.Backbone.small, (try factory.getGlinerBoundaryConfig(loaded.session)).backbone);
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
    try std.testing.expect(lease.controller != null);
    try std.testing.expect(lease.amounts.backend_weight_bytes >= pins.@"model.safetensors".size_bytes);
    // No request is active here. A nonzero expected handle count is the test's
    // HeldRequest, which already owns the model execution mutex.
    const resident = try factory.glinerBoundaryResidentStats(loaded.session);
    try std.testing.expect(resident.ready);
    try std.testing.expect(resident.generation != 0 and resident.workspace_generation != 0);
    try std.testing.expect(resident.model_live_bytes > 0 and resident.workspace_live_bytes > 0);
    try std.testing.expectEqual(resident.workspace_capacity_bytes, resident.workspace_live_bytes);
    return .{
        .model = @intFromPtr(loaded),
        .session = @intFromPtr(loaded.session.ptr),
        .model_generation = resident.generation,
        .workspace_generation = resident.workspace_generation,
        .model_bytes = resident.model_live_bytes,
        .workspace_bytes = resident.workspace_live_bytes,
    };
}

/// Retained residency is accounted separately from the transient request.
/// This deliberately does not acquire the execution mutex: the cancellation
/// assertions must run while the test still owns that mutex and one handle.
fn idle(node: *Node, handles: usize) !memory.AdmissionAmounts {
    const admission = node.inference_admission.stats();
    try std.testing.expectEqual(@as(usize, 0), admission.in_flight_requests);
    try std.testing.expectEqual(@as(usize, 0), admission.in_flight_units);
    try std.testing.expectEqual(@as(i64, 0), @atomicLoad(i64, &node.metrics.requests_active.impl.value, .monotonic));
    try std.testing.expectEqual(@as(i64, 0), @atomicLoad(i64, &node.metrics.extraction_v2.active.impl.value, .monotonic));
    const control = try shortControl(node);
    var retained_scratch: memory.AdmissionAmounts = .{};
    {
        try control.lock(&node.model_manager.load_lock);
        defer node.model_manager.load_lock.unlock();
        try std.testing.expectEqual(@as(usize, 1), node.model_manager.loaded.count());
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_loads.count());
        var iterator = node.model_manager.loaded.valueIterator();
        const loaded = iterator.next().?.*;
        try std.testing.expectEqual(handles, loaded.active_handles);
        const lease = loaded.resource_lease orelse return error.MissingModelResourceLease;
        // With zero handles the cache lock excludes new owners. When handles
        // is one, the test already owns the execution mutex via HeldRequest.
        const workspace = factory.glinerBoundaryWorkspaceAdmissionAmounts(loaded.session);
        try std.testing.expect(workspace.host_scratch_bytes > 0 and workspace.backend_scratch_bytes > 0);
        retained_scratch = try lease.amounts.merge(workspace);
    }
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const amounts = domain.admission.snapshot();
    try std.testing.expectEqual(retained_scratch.host_scratch_bytes, amounts.host_scratch_bytes);
    try std.testing.expectEqual(retained_scratch.backend_scratch_bytes, amounts.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), amounts.backend_kv_bytes);
    const watchdog = node.hard_cancellation_watchdog.?;
    try control.lock(&watchdog.mutex);
    defer watchdog.mutex.unlock();
    try std.testing.expect(watchdog.io != null);
    try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
    return amounts;
}

/// Reuse the qualified transport's four-lane executor. Connecting owns only
/// two structured tasks; both finish before the raw socket is returned. A
/// native timed connect is not used because Zig 0.16 leaves it unimplemented.
fn connect(transport: *sockets.Loopback) !httpx.Socket {
    const Outcome = union(enum) { connected: anyerror!httpx.Socket, deadline: Io.Cancelable!void };
    const Tasks = struct {
        fn expire(io: Io) Io.Cancelable!void {
            try io.sleep(.fromMilliseconds(wait_ms), .awake);
        }
        fn discard(outcome: Outcome) void {
            switch (outcome) {
                .connected => |result| if (result) |value| {
                    var peer = value;
                    peer.close();
                } else |_| {},
                .deadline => {},
            }
        }
    };
    const io = transport.client_io.io();
    var storage: [2]Outcome = undefined;
    var select = Io.Select(Outcome).init(io, &storage);
    try select.concurrent(.connected, httpx.Socket.connect, .{ transport.address orelse return error.MissingLoopbackAddress, io });
    errdefer while (select.cancel()) |late| Tasks.discard(late);
    try select.concurrent(.deadline, Tasks.expire, .{io});
    switch (try select.await()) {
        .connected => |result| {
            select.cancelDiscard();
            return try result;
        },
        .deadline => |result| {
            while (select.cancel()) |late| Tasks.discard(late);
            try result;
            return error.Timeout;
        },
    }
}

fn sendBefore(peer: *httpx.Socket, bytes: []const u8, deadline_ns: u64) !void {
    var sent: usize = 0;
    while (sent < bytes.len) {
        const now = clock();
        if (now >= deadline_ns) return error.Timeout;
        const remaining_ms = (deadline_ns - now + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
        try peer.setSendTimeout(remaining_ms);
        const count = try peer.send(bytes[sent..]);
        if (count == 0) return error.WriteZero;
        sent += count;
    }
}

fn rawPost(transport: *sockets.Loopback, body: []const u8) !httpx.Socket {
    if (body.len > 64 * 1024) return error.RequestBodyTooLarge;
    var peer = try connect(transport);
    errdefer peer.close();
    try peer.setRecvTimeout(wait_ms);
    var header: [256]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&header, "POST /ai/v1/extract HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});
    const deadline_ns = clock() + wait_ms * std.time.ns_per_ms;
    try sendBefore(&peer, bytes, deadline_ns);
    try sendBefore(&peer, body, deadline_ns);
    return peer;
}

const HeldRequest = struct {
    node: *Node,
    handle: manager.ModelHandle,
    mutex: *std.atomic.Mutex,
    mutex_held: bool = true,
    peer: ?httpx.Socket = null,

    fn init(node: *Node, directory: []const u8) !HeldRequest {
        var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingCachedModel;
        errdefer handle.release();
        const mutex = handle.get().targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
        try (try shortControl(node)).lock(mutex);
        return .{ .node = node, .handle = handle, .mutex = mutex };
    }

    fn post(self: *HeldRequest, transport: *sockets.Loopback, body: []const u8) !void {
        std.debug.assert(self.mutex_held and self.peer == null);
        self.peer = try rawPost(transport, body);
    }

    fn waitAdmitted(self: *HeldRequest, transport: *sockets.Loopback, resource_admission: *memory.AdmissionController, retained: memory.AdmissionAmounts, preflights: u64, tokenizations: u64) !void {
        const expected = try retained.merge(.{
            .host_scratch_bytes = request_heap_bytes,
        });
        const deadline_ns = clock() + wait_ms * std.time.ns_per_ms;
        const control = try controlUntil(self.node, deadline_ns);
        errdefer std.debug.print("queued Metal admission failed: units={d} host_scratch={d} backend_scratch={d} expected_host={d} expected_backend={d} tokenizations={d} expected_tokenizations={d}\n", .{
            self.node.inference_admission.inFlightUnits(),
            resource_admission.snapshot().host_scratch_bytes,
            resource_admission.snapshot().backend_scratch_bytes,
            expected.host_scratch_bytes,
            expected.backend_scratch_bytes,
            self.node.metrics.extraction_v2.phase_visits.get(.tokenizing),
            tokenizations + 1,
        });
        while (true) {
            if (transport.listener.runtimeFailure()) |err| return err;
            try control.check();
            const admission = self.node.inference_admission.stats();
            const amounts = resource_admission.snapshot();
            // Real preprocessing has completed before the first workspace
            // snapshot lock. Only host request scratch is newly admitted;
            // persistent GPU workspace remains owned by the cached model.
            // No reservation or timing-based queue assumption is made here.
            if (admission.in_flight_requests == 1 and admission.in_flight_units == 1 and
                std.meta.eql(expected, amounts) and
                self.node.metrics.extraction_v2.phase_visits.get(.preflight) > preflights and
                self.node.metrics.extraction_v2.phase_visits.get(.tokenizing) > tokenizations)
            {
                try control.lock(&self.node.model_manager.load_lock);
                defer self.node.model_manager.load_lock.unlock();
                try std.testing.expectEqual(@as(usize, 0), self.node.model_manager.in_flight_loads.count());
                try std.testing.expectEqual(@as(usize, 2), self.handle.get().active_handles);
                try std.testing.expect(self.mutex_held);
                return;
            }
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    fn resetPeer(self: *HeldRequest) !void {
        const peer = if (self.peer) |*value| value else return error.MissingQueuedSocket;
        // A real reset is required. FIN alone is a legal HTTP half-close and
        // cannot prove cancellation. This descriptor is still owned here.
        var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
        try std.posix.setsockopt(peer.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
        peer.close();
        self.peer = null;
    }

    fn release(self: *HeldRequest) void {
        if (self.peer != null) self.resetPeer() catch {
            self.peer.?.close();
            self.peer = null;
        };
        if (self.mutex_held) {
            self.mutex.unlock();
            self.mutex_held = false;
        }
        self.handle.release();
    }

    fn deinit(self: *HeldRequest) void {
        // Registered after Loopback's defer: unlock and release the model
        // before any listener/client joins, including failed assertions or
        // failed socket reset. Joining under this mutex would deadlock.
        self.release();
    }
};

fn expectDeviceWork(before: metal_tensor.MemoryStats, after: metal_tensor.MemoryStats) !void {
    try std.testing.expect(after.device_owned_buffers_created > before.device_owned_buffers_created);
    try std.testing.expect(after.device_owned_bytes_created > before.device_owned_bytes_created);
    try std.testing.expect(after.device_owned_buffers_released > before.device_owned_buffers_released);
    try std.testing.expect(after.device_owned_bytes_released > before.device_owned_bytes_released);
}

test "gliner boundary socket pinned small Metal queued cancellation releases admission before unlock and retries" {
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
    try std.testing.expectEqual(@as(usize, 2304 * MiB), try executor.deviceScratchUpperBound(.{}));
    const original_physical = metal_tensor.memoryStatsSnapshot();
    {
        var node = try Node.init(a, try config(directory));
        defer node.deinit();
        requireMetal(&node);
        try node.attachIo(std.testing.io);
        const transport = try sockets.Loopback.init(a, &node);
        defer transport.deinit();
        // The shared helper establishes Node.serve's authoritative resource
        // domain before publishing the listener. Keep that pointer stable.
        const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
        const resource_admission = &domain.admission;
        try std.testing.expectEqual(memory.AdmissionAmounts{}, resource_admission.snapshot());
        try transport.start();
        try std.testing.expect(!node.test_allow_unqualified_gliner_boundary);
        node.test_allow_unqualified_gliner_boundary = true;
        var prompt_tokens: i64 = 0;
        const before_warm = metal_tensor.memoryStatsSnapshot();
        {
            var response = try transport.post(raw);
            defer response.deinit();
            prompt_tokens = try shared.successfulResponse(a, &response, name, case.id, case.text, case.expected);
        }
        try transport.idle();
        try expectDeviceWork(before_warm, metal_tensor.memoryStatsSnapshot());
        const cache = try cached(&node, directory, pins, 0);
        const retained = try idle(&node, 0);
        const before_queued = metal_tensor.memoryStatsSnapshot();
        const preflights = node.metrics.extraction_v2.phase_visits.get(.preflight);
        const tokenizations = node.metrics.extraction_v2.phase_visits.get(.tokenizing);
        const model_phases = node.metrics.extraction_v2.phase_visits.get(.model);
        {
            var held = try HeldRequest.init(&node, directory);
            defer held.deinit();
            try std.testing.expectEqual(cache, try cached(&node, directory, pins, 1));
            try held.post(transport, raw);
            try held.waitAdmitted(transport, resource_admission, retained, preflights, tokenizations);
            try std.testing.expectEqual(@as(u64, 2), node.metrics.extraction_v2.requests.get(.http));
            try std.testing.expectEqual(@as(u64, 2), @atomicLoad(u64, &node.metrics.extraction_v2.parsed_items.impl.count, .monotonic));
            try std.testing.expectEqual(@as(u64, 1), @atomicLoad(u64, &node.metrics.extraction_v2.decoded_items.impl.count, .monotonic));
            // Phase counters count completions, not entry into the phase.
            try std.testing.expectEqual(model_phases + 1, node.metrics.extraction_v2.phase_visits.get(.model));
            try std.testing.expectEqual(tokenizations + 1, node.metrics.extraction_v2.phase_visits.get(.tokenizing));
            try std.testing.expectEqual(before_queued, metal_tensor.memoryStatsSnapshot());
            try held.resetPeer();
            // This bounded drain must succeed BEFORE releasing the mutex.
            // On failure the HeldRequest defer unlocks before Loopback joins.
            try transport.idle();
            try std.testing.expect(held.mutex_held);
            try std.testing.expect(held.peer == null);
            try std.testing.expectEqual(cache, try cached(&node, directory, pins, 1));
            try std.testing.expectEqual(retained, try idle(&node, 1));
            try std.testing.expectEqual(@as(u64, 1), transport.server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
            try std.testing.expectEqual(@as(u64, 1), node.metrics.extraction_v2.outcomes.get(.cancelled));
            try std.testing.expectEqual(@as(u64, 1), node.metrics.extraction_v2.failure_stages.get(.model));
            try std.testing.expectEqual(model_phases + 2, node.metrics.extraction_v2.phase_visits.get(.model));
            try std.testing.expectEqual(@as(u64, 1), @atomicLoad(u64, &node.metrics.extraction_v2.decoded_items.impl.count, .monotonic));
            try std.testing.expectEqual(@as(u64, 1), @atomicLoad(u64, &node.metrics.extraction_v2.returned_items.impl.count, .monotonic));
            // The queue never created a managed compute backend or executed
            // a learned operation. Tests run serially; no counters are reset.
            try std.testing.expectEqual(before_queued, metal_tensor.memoryStatsSnapshot());
            held.release();
        }
        try std.testing.expectEqual(cache, try cached(&node, directory, pins, 0));
        try std.testing.expectEqual(retained, try idle(&node, 0));
        const before_retry = metal_tensor.memoryStatsSnapshot();
        {
            const retry = try shared.requestBytes(a, name, case.schema, &.{.{ .content = case.text }});
            defer a.free(retry);
            var response = try transport.post(retry);
            defer response.deinit();
            try std.testing.expectEqual(prompt_tokens, try shared.successfulResponse(a, &response, name, null, case.text, case.expected));
        }
        try transport.idle();
        try expectDeviceWork(before_retry, metal_tensor.memoryStatsSnapshot());
        try std.testing.expectEqual(cache, try cached(&node, directory, pins, 0));
        try std.testing.expectEqual(retained, try idle(&node, 0));
        {
            var response = try transport.request(.GET, "/ml/v1/metrics", null);
            defer response.deinit();
            try sockets.metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 3);
            try sockets.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"success\"}", 2);
            try sockets.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"cancelled\"}", 1);
            try sockets.metric(&response, "antfly_inference_extract_v2_failures_total{stage=\"model\"}", 1);
            try sockets.metric(&response, "antfly_inference_extract_v2_parsed_items_total", 3);
            try sockets.metric(&response, "antfly_inference_extract_v2_decoded_items_total", 2);
            try sockets.metric(&response, "antfly_inference_extract_v2_returned_items_total", 2);
            try sockets.metric(&response, "antfly_inference_extract_v2_decoded_prompt_tokens_total", @intCast(2 * prompt_tokens));
            try sockets.metric(&response, "antfly_inference_extract_v2_active", 0);
            try sockets.metric(&response, "antfly_admission_inference_in_flight_units", 0);
        }
        try transport.idle();
        try std.testing.expectEqual(retained, try idle(&node, 0));
        try transport.finish();
        _ = try idle(&node, 0);
        {
            // The transport has stopped. Retire via the real manager owner so
            // its independent close watchdog protects immutable/workspace
            // teardown before checking the still-live admission controller.
            var final_handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingCachedModel;
            defer final_handle.release();
            final_handle.retire();
        }
        try std.testing.expectEqual(memory.AdmissionAmounts{}, resource_admission.snapshot());
        {
            const control = try shortControl(&node);
            try control.lock(&node.model_manager.load_lock);
            defer node.model_manager.load_lock.unlock();
            try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
            try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded_aliases.count());
            try std.testing.expectEqual(@as(usize, 0), node.model_manager.in_flight_loads.count());
        }
        try std.testing.expect(!boundary.runtime_available);
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
