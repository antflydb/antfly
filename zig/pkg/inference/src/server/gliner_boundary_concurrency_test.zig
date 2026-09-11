// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Deterministic concurrent HTTP request-count admission, against a real cold
//! pinned-small model. The caller owns the existing model-registry lock only
//! until capacity rejection is observed. No counters or runtime hooks change.
//! The shared Loopback helper retains the already-qualified transport owner,
//! while a second client and executor provide independent competing requests.
const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const Node = @import("server.zig").Node;
const shared = @import("gliner_boundary_service_test.zig");
const socket_tests = @import("gliner_boundary_socket_test.zig");
const Loopback = socket_tests.Loopback;
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const model = @import("../models/gliner_boundary.zig");
const memory = @import("../runtime/tier/memory.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Io = std.Io;
const clock = platform.time.monotonicNs;
const wait_ms = 5_000;
const request_ms = 180_000;
const scratch_bytes = 128 * 1024 * 1024;

/// Own the lock and first asynchronous client as one cleanup boundary. In
/// particular, cancellation cannot deadlock by joining a request while its
/// model-registry lock is still owned by this test. The future captures only
/// this stable stack address; the owner is never moved after start().
const HeldColdRequest = struct {
    node: *Node,
    resource_admission: *memory.AdmissionController,
    transport: *Loopback,
    driver_io: Io,
    url: []const u8,
    body: []const u8,
    lock_held: bool = false,
    cancelled: std.atomic.Value(bool) = .init(false),
    completed: std.atomic.Value(bool) = .init(false),
    future: ?Io.Future(anyerror!httpx.Response) = null,

    fn start(self: *HeldColdRequest) !void {
        std.debug.assert(!self.lock_held and self.future == null);
        const until = clock() + wait_ms * std.time.ns_per_ms;
        while (!self.node.model_manager.load_lock.tryLock()) {
            if (clock() >= until) return error.ColdLoadLockTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        self.lock_held = true;
        errdefer self.release();
        try std.testing.expectEqual(@as(usize, 0), self.node.model_manager.loaded.count());
        try std.testing.expectEqual(@as(usize, 0), self.node.model_manager.in_flight_loads.count());
        self.future = try self.driver_io.concurrent(run, .{self});
    }

    fn run(self: *HeldColdRequest) anyerror!httpx.Response {
        defer self.completed.store(true, .release);
        return self.transport.client.post(self.url, .{
            .borrowed_body = self.body,
            .headers = &.{.{ "Content-Type", "application/json" }},
            .timeout_ms = request_ms,
            .cancellation = .fromAtomic(&self.cancelled),
        });
    }

    fn waitAdmitted(self: *HeldColdRequest) !void {
        const until = clock() + wait_ms * std.time.ns_per_ms;
        while (true) {
            if (self.completed.load(.acquire)) {
                var response = try self.join();
                defer response.deinit();
                std.debug.print("cold request completed before admission barrier: {d} {s}\n", .{ response.status.code, response.body orelse "<absent>" });
                return error.ColdRequestNotHeld;
            }
            const admission = self.node.inference_admission.stats();
            // Captured before listener publication, after the same resource
            // startup precondition used by Node.serve. Never race a lazy
            // resource_domain pointer publication from the HTTP worker.
            const amounts = self.resource_admission.snapshot();
            if (admission.in_flight_requests == 1 and admission.in_flight_units == 1 and
                amounts.host_scratch_bytes == scratch_bytes and
                self.node.metrics.extraction_v2.phase_visits.get(.preflight) > 0) break;
            if (clock() >= until) return error.ColdRequestAdmissionTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // Registry reads are protected by the lock owned here. The actual
        // managed load cannot create a flight or model until release().
        try std.testing.expect(self.lock_held);
        try std.testing.expectEqual(@as(usize, 0), self.node.model_manager.loaded.count());
        try std.testing.expectEqual(@as(usize, 0), self.node.model_manager.in_flight_loads.count());
        try std.testing.expectEqual(@as(u64, 1), self.node.metrics.extraction_v2.requests.get(.http));
    }

    fn release(self: *HeldColdRequest) void {
        if (self.lock_held) {
            self.node.model_manager.load_lock.unlock();
            self.lock_held = false;
        }
    }

    fn join(self: *HeldColdRequest) !httpx.Response {
        self.release();
        const future = if (self.future) |*value| value else return error.NoColdRequest;
        defer self.future = null;
        return future.await(self.driver_io);
    }

    fn deinit(self: *HeldColdRequest) void {
        self.cancelled.store(true, .release);
        self.release();
        if (self.future) |*future| {
            // The HTTP client's cancellation race interrupts I/O and joins
            // its own request/timer tasks. Dispose any racing successful body.
            if (future.await(self.driver_io)) |value| {
                var response = value;
                response.deinit();
            } else |_| {}
            self.future = null;
        }
    }
};

fn capacityResponse(a: std.mem.Allocator, response: *const httpx.Response) !void {
    errdefer std.debug.print("concurrent extraction rejection: {d} {s}\n", .{ response.status.code, response.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.header("Retry-After").?);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, response.body orelse return error.MissingCapacityBody, .{});
    defer parsed.deinit();
    const value = parsed.value.object;
    try std.testing.expectEqualStrings("SERVICE_UNAVAILABLE", value.get("error").?.string);
    try std.testing.expectEqualStrings("inference_admission", value.get("reason").?.string);
    try std.testing.expect(value.get("retryable").?.bool);
    try std.testing.expectEqual(@as(i64, 1000), value.get("retry_after_ms").?.integer);
    // Rejection precedes version parsing; do not invent V2 provenance here.
    for ([_][]const u8{ "schema_version", "input_index", "data", "usage" }) |key|
        try std.testing.expect(!value.contains(key));
}

test "gliner boundary socket pinned small concurrent admission rejection release and retry" {
    if (builtin.os.tag != .macos and builtin.os.tag != .linux) return error.SkipZigTest;
    const requested = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path: [Io.Dir.max_path_bytes]u8 = undefined;
    const length = if (std.fs.path.isAbsolute(requested))
        try Io.Dir.realPathFileAbsolute(std.testing.io, requested, &path)
    else
        try Io.Dir.cwd().realPathFile(std.testing.io, requested, &path);
    const directory = path[0..length];
    const name = std.fs.path.basename(directory);
    const fixture_bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(fixture_bytes);
    var fixture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, fixture_bytes, .{});
    defer fixture.deinit();
    const pins = fixture.value.model_files;
    try shared.verifyFiles(a, directory, pins);
    const case = fixture.value.cases[0];
    try std.testing.expectEqualStrings("mixed_tasks", case.id);
    const raw = try shared.requestBytes(a, name, case.schema, &.{.{ .id = "held-cold", .content = case.text }});
    defer a.free(raw);
    {
        var node = try Node.init(a, .{
            .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
            .max_loaded_models = 1,
            .max_concurrent_requests = 1,
            .keep_alive_ms = 30 * 60 * 1000,
            .process_termination_available = true,
            .generation_budget_overrides = .{ .host_limit_bytes = 1024 * 1024 * 1024, .scratch_limit_bytes = scratch_bytes },
        });
        defer node.deinit();
        shared.useNativeBackend(&node);
        try node.attachIo(std.testing.io);
        try std.testing.expect(!node.test_allow_unqualified_gliner_boundary);
        node.test_allow_unqualified_gliner_boundary = true;
        const transport = try Loopback.init(a, &node);
        defer transport.deinit();
        // Loopback initialized the actual serving resource owner, without a
        // model, request lease, or fake usage. Capture it before concurrency.
        const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
        const resource_admission = &domain.admission;
        try std.testing.expectEqual(memory.AdmissionAmounts{}, resource_admission.snapshot());
        try transport.start();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/ai/v1/extract", .{transport.address.?.ip4.port});
        defer a.free(url);

        // The competing HTTP client has independent connection state, timers
        // and execution lanes. Neither requests a pool permit from the first.
        var second_budget = BoundedAllocator{ .backing = a, .limit = 4 * 1024 * 1024 };
        defer std.debug.assert(second_budget.live == 0);
        // A cold bounded httpx call owns four client lanes simultaneously:
        // request + request watchdog + connect + connect watchdog. The test
        // driver must therefore run outside that four-lane client executor.
        // One separate driver lane is sufficient; it and the competing
        // client's metadata share this existing 4 MiB bounded owner.
        var driver = Io.Threaded.init(second_budget.allocator(), .{ .concurrent_limit = .limited(1) });
        defer driver.deinit();
        var second_io = Io.Threaded.init(second_budget.allocator(), .{ .concurrent_limit = .limited(4) });
        defer second_io.deinit();
        var second = httpx.Client.initWithConfig(second_budget.allocator(), second_io.io(), .{
            .timeouts = .{ .connect_ms = wait_ms, .read_ms = request_ms, .write_ms = wait_ms, .request_ms = request_ms },
            .retry_policy = .noRetry(),
            .redirect_policy = .{ .follow_redirects = false, .max_redirects = 0 },
            .max_response_size = 1024 * 1024,
            .max_response_headers = 64,
            .keep_alive = false,
            .pool_max_connections = 1,
            .pool_max_per_host = 1,
            .cookies_enabled = false,
            .cancel_in_flight_on_shutdown = true,
        });
        defer second.deinit();
        var first = HeldColdRequest{ .node = &node, .resource_admission = resource_admission, .transport = transport, .driver_io = driver.io(), .url = url, .body = raw };
        defer first.deinit();
        try first.start();
        try first.waitAdmitted();
        const before = node.inference_admission.stats();
        try std.testing.expectEqual(@as(usize, 1), before.in_flight_requests);
        try std.testing.expectEqual(@as(usize, 1), before.in_flight_units);
        try std.testing.expectEqual(@as(usize, 0), before.available_units);
        {
            const start = clock();
            var rejected = try second.post(url, .{
                .borrowed_body = raw,
                .headers = &.{.{ "Content-Type", "application/json" }},
                .timeout_ms = wait_ms,
            });
            defer rejected.deinit();
            try std.testing.expect(clock() - start < wait_ms * std.time.ns_per_ms);
            try capacityResponse(a, &rejected);
        }
        const rejected = node.inference_admission.stats();
        try std.testing.expectEqual(@as(usize, 1), rejected.in_flight_requests);
        try std.testing.expectEqual(@as(usize, 1), rejected.in_flight_units);
        try std.testing.expectEqual(@as(usize, 1), rejected.peak_in_flight_requests);
        try std.testing.expectEqual(@as(u64, 1), rejected.rejected_requests_total);
        try std.testing.expect(!first.completed.load(.acquire));
        try std.testing.expectEqual(@as(u64, 1), node.metrics.extraction_v2.requests.get(.http));
        // Transport itself admitted both handlers; 503 is specifically Node
        // capacity, not a hidden HTTP executor or connection-lane rejection.
        const http_stats = transport.server.runtimeStats();
        try std.testing.expectEqual(@as(usize, 2), http_stats.peak_active_requests);
        try std.testing.expectEqual(@as(u64, 0), http_stats.request_dispatch_rejections_total);
        try std.testing.expectEqual(@as(u64, 0), http_stats.connection_dispatch_rejections_total);

        var prompt_tokens: i64 = 0;
        {
            // join releases the registry lock before waiting for cold loading.
            var response = try first.join();
            defer response.deinit();
            prompt_tokens = try shared.successfulResponse(a, &response, name, "held-cold", case.text, case.expected);
        }
        try transport.idle();
        const cached = try shared.cachedModel(&node, directory, pins);
        const resident = try shared.idle(&node);
        try std.testing.expect(resident.host_weight_bytes >= pins.@"model.safetensors".size_bytes);
        {
            const retry = try shared.requestBytes(a, name, case.schema, &.{.{ .content = case.text }});
            defer a.free(retry);
            var response = try second.post(url, .{
                .borrowed_body = retry,
                .headers = &.{.{ "Content-Type", "application/json" }},
                .timeout_ms = request_ms,
            });
            defer response.deinit();
            try std.testing.expectEqual(prompt_tokens, try shared.successfulResponse(a, &response, name, null, case.text, case.expected));
        }
        try transport.idle();
        try std.testing.expectEqual(cached, try shared.cachedModel(&node, directory, pins));
        try std.testing.expectEqual(resident, try shared.idle(&node));
        {
            var response = try transport.request(.GET, "/ml/v1/metrics", null);
            defer response.deinit();
            try socket_tests.metric(&response, "antfly_admission_inference_rejected_requests_total", 1);
            try socket_tests.metric(&response, "antfly_admission_inference_rejected_units_total", 1);
            try socket_tests.metric(&response, "antfly_admission_inference_in_flight_requests", 0);
            try socket_tests.metric(&response, "antfly_admission_inference_in_flight_units", 0);
            try socket_tests.metric(&response, "antfly_admission_inference_available_units", 1);
            try socket_tests.metric(&response, "antfly_inference_errors_total", 1);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 2);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"success\"}", 2);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"admission\"}", 0);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_parsed_items_total", 2);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_decoded_items_total", 2);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_returned_items_total", 2);
            try socket_tests.metric(&response, "antfly_inference_extract_v2_decoded_prompt_tokens_total", @intCast(2 * prompt_tokens));
        }
        try transport.idle();
        try std.testing.expectEqual(resident, try shared.idle(&node));
        try transport.finish();
        const final = node.inference_admission.stats();
        try std.testing.expectEqual(@as(usize, 0), final.in_flight_requests);
        try std.testing.expectEqual(@as(usize, 0), final.in_flight_units);
        try std.testing.expectEqual(@as(usize, 1), final.peak_in_flight_requests);
        try std.testing.expectEqual(@as(u64, 1), final.rejected_requests_total);
        try std.testing.expect(!model.runtime_available);
    }
    try shared.verifyFiles(a, directory, pins);
}
