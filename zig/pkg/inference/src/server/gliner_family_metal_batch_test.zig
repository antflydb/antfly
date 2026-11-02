// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exact-artifact Metal batch serving: distinct schemas, ragged prompts,
//! output order, physical GPU work and request resource lifetimes.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const server = @import("server.zig");
const Node = server.Node;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const batch = @import("gliner_family_cuda_socket_test.zig");
const factory = @import("../architectures/session_factory.zig");
const metal = @import("../backends/metal_tensor.zig");
const decide_qualification = @import("../models/gliner_decide_qualification.zig");
const memory = @import("../runtime/tier/memory.zig");
const manager = @import("model_manager.zig");
const httpx = @import("httpx");
const Io = std.Io;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Role = enum { base, multi, decide };
const batch_sizes = [_]usize{ 2, 4, 8 };

fn response(transport: *Loopback, body: []const u8) ![]u8 {
    var result = try transport.request(.POST, "/ai/v1/extract", body);
    defer result.deinit();
    errdefer std.debug.print("Metal batch HTTP: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), result.status.code);
    return std.testing.allocator.dupe(u8, result.body orelse return error.MissingResponseBody);
}

fn rejectBeforeDeviceWork(node: *Node, transport: *Loopback, body: []const u8) !void {
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    const before = metal.memoryStatsSnapshot();
    var rejected = try transport.request(.POST, "/ai/v1/extract", body);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 413), rejected.status.code);
    try transport.idle();
    try std.testing.expectEqual(before.device_owned_buffers_created, metal.memoryStatsSnapshot().device_owned_buffers_created);
    try std.testing.expectEqual(retained, domain.admission.snapshot());
}

fn batchPool(node: *Node, transport: *Loopback, directory: []const u8, comptime role: Role, cases: []const batch.BatchCase, sizes: []const usize) !void {
    const a = std.testing.allocator;
    const name = std.fs.path.basename(directory);
    const baselines = try a.alloc([]u8, cases.len);
    defer a.free(baselines);
    const allocations = try a.alloc(u64, cases.len);
    defer a.free(allocations);
    var built: usize = 0;
    defer for (baselines[0..built]) |body| a.free(body);
    for (cases, baselines, allocations) |case, *baseline, *work| {
        const body = try batch.batchBody(a, name, &.{case}, 1, false);
        defer a.free(body);
        const warmup = try response(transport, body);
        a.free(warmup);
        try transport.idle();
        const before = metal.memoryStatsSnapshot();
        baseline.* = try response(transport, body);
        try transport.idle();
        work.* = metal.memoryStatsSnapshot().device_owned_buffers_created - before.device_owned_buffers_created;
        built += 1;
    }
    try transport.idle();
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    try std.testing.expectEqual(.metal, handle.get().session.backend());
    if (role != .decide) try std.testing.expect(factory.isGlinerBoundaryResidentReady(handle.get().session));
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    // The first pass may grow the resident workspace; the second must reuse it.
    for (0..2) |pass| for (sizes) |n| for ([_]bool{ false, true }) |reverse| {
        const body = try batch.batchBody(a, name, cases, n, reverse);
        defer a.free(body);
        const retained = domain.admission.snapshot();
        const before = metal.memoryStatsSnapshot();
        const actual = try response(transport, body);
        defer a.free(actual);
        const after = metal.memoryStatsSnapshot();
        const work = after.device_owned_buffers_created - before.device_owned_buffers_created;
        try std.testing.expect(work > 0);
        var serial_work: u64 = 0;
        for (0..n) |i| serial_work += allocations[(if (reverse) n - 1 - i else i) % cases.len];
        try std.testing.expect(work < serial_work);
        if (role == .decide) {
            // Large public requests still use <=8 rows and one readback per
            // physical forward, including their final partial group.
            const forwards: u64 = @intCast((n - 1) / decide_qualification.metal_batch_items + 1);
            try std.testing.expectEqual(before.to_host_device_calls + forwards, after.to_host_device_calls);
        }
        var parsed = try std.json.parseFromSlice(std.json.Value, a, actual, .{});
        defer parsed.deinit();
        const rows = parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(n, rows.len);
        for (rows, 0..) |row, i| {
            const index = (if (reverse) n - 1 - i else i) % cases.len;
            var expected = try std.json.parseFromSlice(std.json.Value, a, baselines[index], .{});
            defer expected.deinit();
            var id: [32]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "row-{d}", .{i}), row.object.get("id").?.string);
            try batch.equalBatchValue(expected.value.object.get("data").?.array.items[0], row, if (role == .decide) 5e-3 else 5e-4);
        }
        try transport.idle();
        if (pass != 0 or role == .decide) try std.testing.expectEqual(retained, domain.admission.snapshot());
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        std.debug.print("production_metal_batch role={s} items={d} reverse={} pass={d} buffers={d} serial_buffers={d} parity=true leases_released=true\n", .{ @tagName(role), n, reverse, pass, work, serial_work });
    };
    const limit = if (role == .decide) server.max_serial_family_batch_items else 8;
    const oversized = try batch.batchBody(a, name, cases, limit + 1, false);
    defer a.free(oversized);
    try rejectBeforeDeviceWork(node, transport, oversized);
}

fn additionalMultilingualProfiles(node: *Node, transport: *Loopback, directory: []const u8, intents: []const batch.BatchCase) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    for (batch.additional_multi_batch_fixtures, 0..) |file, file_index| {
        std.debug.print("production_metal_batch_profile fixture={s}\n", .{file});
        const bytes = try fixtures.fixtureBytes(a, file);
        defer a.free(bytes);
        var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer fixture.deinit();
        const values = fixture.value.object.get("requests").?.array.items;
        const cases = try a.alloc(batch.BatchCase, values.len);
        defer a.free(cases);
        const schemas = try a.alloc(std.json.Parsed(std.json.Value), values.len);
        defer a.free(schemas);
        var built: usize = 0;
        defer for (schemas[0..built]) |*schema| schema.deinit();
        for (values, cases, schemas) |value, *out, *schema| {
            schema.* = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            built += 1;
            out.* = .{ .text = value.object.get("text").?.string, .schema = schema.value };
        }
        switch (file_index) {
            0 => {
                var bare: [5]batch.BatchCase = undefined;
                @memcpy(bare[0..4], intents);
                bare[4] = cases[0];
                try batchPool(node, transport, directory, .multi, &bare, &batch_sizes);
                for ([_]usize{ 1, 3, 5 }) |i| try batchPool(node, transport, directory, .multi, cases[i..][0..2], &batch_sizes);
            },
            1 => try batchPool(node, transport, directory, .multi, cases, &batch_sizes),
            4 => {
                for ([_]usize{ 0, 2 }) |i| {
                    // The one-byte diagnostic remains outside the serving
                    // profile. Exercise its rejection and the clean floor.
                    const invalid = try batch.batchBody(a, std.fs.path.basename(directory), cases[i..][0..1], 2, false);
                    defer a.free(invalid);
                    try rejectBeforeDeviceWork(node, transport, invalid);
                    var cohort = [_]batch.BatchCase{ cases[i], cases[i + 1] };
                    cohort[0].text = "a.";
                    try batchPool(node, transport, directory, .multi, &cohort, &batch_sizes);
                }
            },
            else => for (cases) |case| try batchPool(node, transport, directory, .multi, &.{case}, &batch_sizes),
        }
    }
}

const Call = struct {
    transport: *Loopback,
    body: []const u8,

    fn run(self: Call) anyerror![]u8 {
        return response(self.transport, self.body);
    }
};

/// Release the model lane before joining any HTTP clients, including assertion
/// failures. A queued request must never be joined under the mutex it needs.
const ExecutionBarrier = struct {
    handle: manager.ModelHandle,
    mutex: *std.atomic.Mutex,
    held: bool = true,

    fn init(node: *Node, directory: []const u8) !ExecutionBarrier {
        var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
        errdefer handle.release();
        try std.testing.expectEqual(.metal, handle.get().session.backend());
        const mutex = handle.get().targetInferenceExecutionMutex() orelse return error.MissingMetalExecutionMutex;
        try (Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s }).lock(mutex);
        return .{ .handle = handle, .mutex = mutex };
    }

    fn unlock(self: *ExecutionBarrier) void {
        if (self.held) self.mutex.unlock();
        self.held = false;
    }

    fn deinit(self: *ExecutionBarrier) void {
        self.unlock();
        self.handle.release();
    }

    fn waitQueued(self: *ExecutionBarrier, node: *Node, transport: *Loopback, count: usize, tokenizations: u64) !void {
        const control = Control{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s };
        while (true) {
            if (transport.listener.runtimeFailure()) |err| return err;
            try control.check();
            const admitted = node.inference_admission.stats();
            if (admitted.in_flight_requests == count and admitted.in_flight_units == count and
                node.metrics.extraction_v2.phase_visits.get(.tokenizing) >= tokenizations + count)
            {
                try control.lock(&node.model_manager.load_lock);
                defer node.model_manager.load_lock.unlock();
                try std.testing.expectEqual(count + 1, self.handle.get().active_handles);
                try std.testing.expect(self.held);
                return;
            }
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    }
};

fn equalResponse(comptime role: Role, expected: []const u8, actual: []const u8) !void {
    var want = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, expected, .{});
    defer want.deinit();
    var got = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, actual, .{});
    defer got.deinit();
    try batch.equalBatchValue(want.value, got.value, if (role == .decide) 5e-3 else 5e-4);
    for (got.value.object.get("data").?.array.items, 0..) |row, i| {
        var id: [32]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "row-{d}", .{i}), row.object.get("id").?.string);
    }
}

fn restored(node: *Node, transport: *Loopback, retained: memory.AdmissionAmounts) !void {
    try transport.idle();
    try std.testing.expectEqual(retained, node.model_manager.resource_domain.?.admission.snapshot());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.stats().in_flight_requests);
}

fn lifecycle(node: *Node, transport: *Loopback, directory: []const u8, comptime role: Role, cases: []const batch.BatchCase) !void {
    const a = std.testing.allocator;
    // Freeze singleton parity and grow the maximum physical workspace before
    // comparing resource ownership across queued, cancelled and rejected work.
    try batchPool(node, transport, directory, role, cases, &.{ 2, 8 });
    const large_count: usize = if (role == .decide) 17 else 8;
    const small = try batch.batchBody(a, std.fs.path.basename(directory), cases, 2, false);
    defer a.free(small);
    const large = try batch.batchBody(a, std.fs.path.basename(directory), cases, large_count, true);
    defer a.free(large);
    const requests = [_][]const u8{ small, large };
    var expected: [2][]u8 = undefined;
    var built: usize = 0;
    defer for (expected[0..built]) |bytes| a.free(bytes);
    for (requests, &expected) |body, *out| {
        out.* = try response(transport, body);
        built += 1;
        try transport.idle();
    }
    const domain = node.model_manager.resource_domain.?;
    const retained = domain.admission.snapshot();
    var driver = Io.Threaded.init(a, .{ .concurrent_limit = .limited(2) });
    defer driver.deinit();
    for (0..2) |pass| {
        var futures: [2]?Io.Future(anyerror![]u8) = .{ null, null };
        defer for (&futures) |*slot| if (slot.*) |*future| {
            if (future.await(driver.io())) |bytes| a.free(bytes) else |_| {}
        };
        var barrier = try ExecutionBarrier.init(node, directory);
        defer barrier.deinit();
        const before = metal.memoryStatsSnapshot();
        const tokenizations = node.metrics.extraction_v2.phase_visits.get(.tokenizing);
        for (&futures, 0..) |*slot, i| slot.* = try driver.io().concurrent(Call.run, .{Call{ .transport = transport, .body = requests[(i + pass) % 2] }});
        try barrier.waitQueued(node, transport, 2, tokenizations);
        try std.testing.expectEqual(before, metal.memoryStatsSnapshot());
        const waiting = domain.admission.snapshot();
        try std.testing.expect(waiting.host_scratch_bytes > retained.host_scratch_bytes);
        // Boundary waiters cannot admit their workspace until the snapshot
        // lock is available. Decide may admit the front batch's workspace.
        if (role != .decide) try std.testing.expectEqual(retained.backend_scratch_bytes, waiting.backend_scratch_bytes);
        std.debug.print("production_metal_batch_queue role={s} admitted_requests=2 tokenized=true no_gpu_work=true\n", .{@tagName(role)});
        barrier.unlock();
        for (&futures, 0..) |*slot, i| {
            var future = slot.*.?;
            slot.* = null;
            const actual = try future.await(driver.io());
            defer a.free(actual);
            try equalResponse(role, expected[(i + pass) % 2], actual);
        }
        const after = metal.memoryStatsSnapshot();
        try std.testing.expect(after.device_owned_buffers_created > before.device_owned_buffers_created);
        if (role == .decide) try std.testing.expectEqual(before.to_host_device_calls + 4, after.to_host_device_calls);
        try restored(node, transport, retained);
        std.debug.print("production_metal_batch_lifecycle role={s} concurrent_items=2,{d} pass={d} simultaneous_admissions=2 parity=true leases_restored=true\n", .{ @tagName(role), large_count, pass });
    }
    {
        var barrier = try ExecutionBarrier.init(node, directory);
        defer barrier.deinit();
        const before = metal.memoryStatsSnapshot();
        const tokenizations = node.metrics.extraction_v2.phase_visits.get(.tokenizing);
        const cancelled = node.metrics.extraction_v2.outcomes.get(.cancelled);
        const disconnects = transport.server.httpRuntimeStats().h1_hard_disconnect_cancellations_total;
        var peer = try transport.connect();
        var open = true;
        defer if (open) peer.close();
        try peer.setSendTimeout(5_000);
        var buffer: [256]u8 = undefined;
        const header = try std.fmt.bufPrint(&buffer, "POST /ai/v1/extract HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{large.len});
        try peer.sendAll(header);
        try peer.sendAll(large);
        try barrier.waitQueued(node, transport, 1, tokenizations);
        try std.testing.expectEqual(before, metal.memoryStatsSnapshot());
        var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
        try std.posix.setsockopt(peer.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
        peer.close();
        open = false;
        try restored(node, transport, retained);
        try std.testing.expect(barrier.held);
        try std.testing.expectEqual(cancelled + 1, node.metrics.extraction_v2.outcomes.get(.cancelled));
        try std.testing.expectEqual(disconnects + 1, transport.server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
        try std.testing.expectEqual(before, metal.memoryStatsSnapshot());
    }
    const after_cancel = try response(transport, large);
    defer a.free(after_cancel);
    try equalResponse(role, expected[1], after_cancel);
    try restored(node, transport, retained);
    std.debug.print("production_metal_batch_lifecycle role={s} cancelled_items={d} drained_before_unlock=true no_queued_gpu_work=true retry_equal=true\n", .{ @tagName(role), large_count });
    // Occupy the real scratch ledger with another owner's reservation. No
    // physical allocation is made for this controlled budget-pressure probe;
    // live-memory checks for the HTTP requests remain unchanged. Keep the model
    // pinned so recovery must reuse its existing session and workspace.
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const session = handle.get().session.ptr;
    const limits = node.config.generation_budget_overrides.apply(.{});
    for (0..2) |pass| {
        {
            const gpu = domain.admission.snapshotBackend(.gpu);
            var pressure = try domain.admission.tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = limits.scratch_limit_bytes - gpu.scratchTotalBytes() }, false);
            defer pressure.release();
            const occupied = domain.admission.snapshot();
            const before = metal.memoryStatsSnapshot();
            var rejected = try transport.post(large);
            defer rejected.deinit();
            errdefer std.debug.print("Metal pressure HTTP: {d} {s}\n", .{ rejected.status.code, rejected.body orelse "<absent>" });
            try std.testing.expectEqual(@as(u16, 503), rejected.status.code);
            try std.testing.expectEqualStrings("1", rejected.header("Retry-After").?);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, rejected.body orelse return error.MissingResponseBody, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", parsed.value.object.get("error").?.string);
            try std.testing.expect(parsed.value.object.get("retryable").?.bool);
            try restored(node, transport, occupied);
            try std.testing.expectEqual(before, metal.memoryStatsSnapshot());
        }
        try restored(node, transport, retained);
        const actual = try response(transport, large);
        defer a.free(actual);
        try equalResponse(role, expected[1], actual);
        try restored(node, transport, retained);
        try std.testing.expectEqual(session, handle.get().session.ptr);
        std.debug.print("production_metal_batch_lifecycle role={s} pressure_pass={d} budget_denial=503 retryable=true no_rejected_gpu_work=true same_session_retry=true leases_restored=true\n", .{ @tagName(role), pass });
    }
    std.debug.print("production_metal_batch_lifecycle role={s} successful_http_requests={d} cancelled_http_requests={d}\n", .{ @tagName(role), node.metrics.extraction_v2.outcomes.get(.success), node.metrics.extraction_v2.outcomes.get(.cancelled) });
}

fn parity(comptime role: Role, environment: [:0]const u8, comptime lifecycle_only: bool) !void {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const source = platform.env.getenv(environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const directory = try @import("../models/gliner_decide_qualification_fixture.zig").prepare(a, &temporary, source);
    defer a.free(directory);
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 2,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 6 * GiB, .backend_limit_bytes = 8 * GiB, .combined_limit_bytes = 12 * GiB, .scratch_limit_bytes = 4 * GiB },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.metal};
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &.{.metal};
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(std.testing.io);
    const transport = try Loopback.initWithCapacity(a, &node, 2);
    defer transport.deinit();
    try transport.start();
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, switch (role) {
        .base => "pipeline_cases_base.json",
        .multi => "family/multi_v1_capture.json",
        .decide => "decide/cases.json",
    });
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer fixture.deinit();
    const values = fixture.value.object.get(switch (role) {
        .base => "cases",
        .multi => "requests",
        .decide => "classification",
    }).?.array.items;
    // The source-default JointIE beam fixture remains outside the public base
    // profile. Cover its other nine frozen mixed-task cases in two cohorts.
    const count = if (role == .base) 9 else values.len;
    const cases = try a.alloc(batch.BatchCase, count);
    defer a.free(cases);
    const schemas = try a.alloc(std.json.Parsed(std.json.Value), if (role == .multi) count else 0);
    defer a.free(schemas);
    var schemas_built: usize = 0;
    defer for (schemas[0..schemas_built]) |*schema| schema.deinit();
    for (cases, values[0..count], 0..) |*out, value, i| {
        const schema = if (role == .multi) parsed: {
            schemas[i] = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            schemas_built += 1;
            break :parsed schemas[i].value;
        } else value.object.get(if (role == .base) "schema" else "v2_schema").?;
        out.* = .{ .text = value.object.get("text").?.string, .schema = schema };
    }
    const Cohort = struct { start: usize, end: usize };
    const cohorts: []const Cohort = switch (role) {
        .base => &.{ .{ .start = 0, .end = 8 }, .{ .start = 7, .end = 9 } },
        // Each multilingual profile remains independent; a union of their
        // features must never acquire a new qualification row through batching.
        .multi => &.{ .{ .start = 1, .end = 4 }, .{ .start = 4, .end = 8 }, .{ .start = 0, .end = 1 }, .{ .start = 8, .end = 9 }, .{ .start = 9, .end = 10 }, .{ .start = 10, .end = 11 } },
        .decide => &.{.{ .start = 0, .end = count }},
    };
    if (lifecycle_only) {
        const range = cohorts[0];
        try lifecycle(&node, transport, directory, role, cases[range.start..range.end][0..2]);
        try transport.finish();
        return;
    }
    for (cohorts) |range| {
        const sizes: []const usize = if (role == .decide) &.{ 2, 4, 8, 9, 17, server.max_serial_family_batch_items } else &batch_sizes;
        try batchPool(&node, transport, directory, role, cases[range.start..range.end], sizes);
    }
    if (role == .multi) try additionalMultilingualProfiles(&node, transport, directory, cases[4..8]);
    if (role == .decide) {
        // Preferred-only Metal selection must preserve the same public envelope
        // as an explicitly required Metal backend.
        node.session_manager.required_backend = null;
        node.model_manager.session_manager.required_backend = null;
        std.debug.print("production_metal_batch_preferred_backend required=false\n", .{});
        try batchPool(&node, transport, directory, .decide, cases[0..2], &.{9});
    }
    try transport.finish();
}

test "GLiNER production Metal distinct input batches base FP32" {
    try parity(.base, "ANTFLY_GLINER25_BASE_MODEL_DIR", false);
}

test "GLiNER production Metal distinct input batches Decide FP32" {
    try parity(.decide, "ANTFLY_GLINER_DECIDE_MODEL", false);
}

test "GLiNER production Metal distinct input batches multilingual FP32" {
    try parity(.multi, "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", false);
}

test "GLiNER production Metal batch lifecycle base FP32" {
    try parity(.base, "ANTFLY_GLINER25_BASE_MODEL_DIR", true);
}

test "GLiNER production Metal batch lifecycle multilingual FP32" {
    try parity(.multi, "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR", true);
}

test "GLiNER production Metal batch lifecycle Decide FP32" {
    try parity(.decide, "ANTFLY_GLINER_DECIDE_MODEL", true);
}
