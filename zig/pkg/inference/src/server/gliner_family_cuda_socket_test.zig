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

//! Opt-in family CUDA serving correctness over real loopback sockets. The
//! existing test-only boundary gate permits measuring unpublished profiles;
//! no production qualification or mixed-precision policy is changed here.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const Node = @import("server.zig").Node;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const Io = std.Io;
const factory = @import("../architectures/session_factory.zig");

fn queuedCancellation(node: *Node, transport: *Loopback, directory: []const u8, body: []const u8, expected: []const u8) !void {
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    const loaded = handle.get();
    if (loaded.manifest.gliner_architecture != .boundary) return;
    const queue = &loaded.gliner_boundary_admission_lock;
    try std.testing.expect(queue.tryLock());
    var held = true;
    defer if (held) queue.unlock();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    const before = factory.getCudaRuntimeStats(loaded.session).?;
    var peer = try transport.connect();
    var open = true;
    defer if (open) peer.close();
    try peer.setSendTimeout(5_000);
    var buffer: [256]u8 = undefined;
    const header = try std.fmt.bufPrint(&buffer, "POST /ai/v1/decide HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});
    try peer.sendAll(header);
    try peer.sendAll(body);
    const deadline = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
    const control = @import("../execution_control.zig").InferenceExecutionControl{ .io = std.testing.io, .deadline_ns = deadline };
    while (true) {
        var handles: usize = undefined;
        {
            try control.lock(&node.model_manager.load_lock);
            defer node.model_manager.load_lock.unlock();
            handles = loaded.active_handles;
        }
        if (handles == 2) break;
        if (platform.time.monotonicNs() >= deadline) return error.QueueAdmissionTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const waiting = domain.admission.snapshot();
    try std.testing.expectEqual(retained.backend_scratch_bytes, waiting.backend_scratch_bytes);
    try std.testing.expect(waiting.host_scratch_bytes > retained.host_scratch_bytes);
    try std.testing.expectEqual(before.kernel_launches, factory.getCudaRuntimeStats(loaded.session).?.kernel_launches);
    // A hard disconnect must drain the queued request while its model lane
    // remains held. FIN alone would be a legal HTTP half-close.
    var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
    try std.posix.setsockopt(peer.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
    peer.close();
    open = false;
    try transport.idle();
    try std.testing.expectEqual(@as(usize, 1), loaded.active_handles);
    try std.testing.expectEqual(retained, domain.admission.snapshot());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(before.kernel_launches, factory.getCudaRuntimeStats(loaded.session).?.kernel_launches);
    queue.unlock();
    held = false;
    const retry = try response(transport, body);
    defer std.testing.allocator.free(retry);
    try std.testing.expectEqualStrings(expected, retry);
    try transport.idle();
    try std.testing.expectEqual(retained, domain.admission.snapshot());
    std.debug.print("family_cuda_http: queued_disconnect_drained=true device_admission_unchanged=true retry_equal=true\n", .{});
}

fn response(transport: *Loopback, bytes: []const u8) ![]u8 {
    var result = try transport.request(.POST, "/ai/v1/decide", bytes);
    defer result.deinit();
    errdefer std.debug.print("family CUDA HTTP: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), result.status.code);
    return std.testing.allocator.dupe(u8, result.body orelse return error.MissingResponseBody);
}

const Call = struct {
    transport: *Loopback,
    request: []const u8,
    expected: []const u8,

    fn run(self: Call) anyerror!void {
        const actual = try response(self.transport, self.request);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings(self.expected, actual);
    }
};

test "GLiNER family CUDA typed decisions through concurrent HTTP sockets" {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const directory = platform.env.getenv("ANTFLY_GLINER25_FAMILY_MODEL_DIR") orelse return error.SkipZigTest;
    const precision = std.meta.stringToEnum(factory.GlinerCudaPrecision, platform.env.getenv("ANTFLY_GLINER25_FAMILY_CUDA_PRECISION") orelse "fp32") orelse return error.InvalidPrecision;
    const a = std.testing.allocator;
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .allow_unknown_models = true,
        .max_loaded_models = 1,
        .max_concurrent_requests = 16,
        .keep_alive_ms = 30 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            // Sixteen request heaps reserve 512 MiB each, in addition to
            // resident model storage and the one active CUDA workspace.
            .host_limit_bytes = 14 * GiB,
            .backend_limit_bytes = 16 * GiB,
            .combined_limit_bytes = 30 * GiB,
            .scratch_limit_bytes = 12 * GiB,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.cuda};
    node.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.preferred_backends = &.{.cuda};
    node.model_manager.session_manager.required_backend = .cuda;
    node.model_manager.session_manager.test_allow_unqualified_gliner_cuda_precision = true;
    node.test_allow_unqualified_gliner_boundary = true;
    try node.attachIo(std.testing.io);
    // This is a loaded-model serving check. The debug test build can spend
    // minutes validating/uploading gigabytes of weights; keep that cold load
    // outside the socket deadline and verify the selected backend explicitly.
    {
        var handle = try node.model_manager.acquireFromDirWithGlinerCudaPrecision(directory, precision);
        defer handle.release();
        try std.testing.expectEqual(.cuda, handle.get().session.backend());
        if (handle.get().manifest.gliner_classification_head == .label_marker_mlp)
            try @import("model_manager.zig").qualifyGlinerEttinForTest(&node.model_manager, handle.get(), directory, precision);
    }
    std.debug.print("family_cuda_http: precision={s}\n", .{@tagName(precision)});
    const transport = try Loopback.initWithCapacity(a, &node, 16);
    defer transport.deinit();
    try transport.start();

    var requests: [16][]u8 = undefined;
    var expected: [16][]u8 = undefined;
    var prepared: usize = 0;
    defer for (0..prepared) |index| {
        a.free(requests[index]);
        a.free(expected[index]);
    };
    for (0..16) |index| {
        const state = try std.fmt.allocPrint(a, "Ticket {d}: {s}", .{ index, if (index % 2 == 0) "Please refund the duplicate payment today." else "I cannot log in and need help resetting my password." });
        defer a.free(state);
        const body = try std.json.Stringify.valueAlloc(a, .{
            .model = std.fs.path.basename(directory),
            .state = state,
            .questions = .{
                .intent = .{ .type = "choice", .instructions = "What does the customer need?", .criteria = .{ .refund = "Refund a payment", .support = "Technical support" } },
                .urgency = .{ .type = "score", .instructions = "How urgent is the request?", .criteria = [_][]const u8{ "Routine", "Urgent" } },
                .act = .{ .type = "noul", .instructions = "Does the customer need assistance?" },
            },
        }, .{});
        errdefer a.free(body);
        const baseline = try response(transport, body);
        requests[index] = body;
        expected[index] = baseline;
        prepared += 1;
    }
    try transport.idle();
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    var driver = Io.Threaded.init(a, .{ .concurrent_limit = .limited(16) });
    defer driver.deinit();
    const io = driver.io();
    for ([_]usize{ 1, 4, 16 }) |concurrency| {
        var futures: [16]Io.Future(anyerror!void) = undefined;
        var started: usize = 0;
        // Join every client on a spawn/error path before destroying its body,
        // expected output, transport, or model owner.
        defer for (futures[0..started]) |*future| {
            future.await(io) catch {};
        };
        for (0..concurrency) |index| {
            futures[index] = try io.concurrent(Call.run, .{Call{ .transport = transport, .request = requests[index], .expected = expected[index] }});
            started += 1;
        }
        var first_error: ?anyerror = null;
        for (futures[0..started]) |*future| future.await(io) catch |err| {
            if (first_error == null) first_error = err;
        };
        started = 0;
        if (first_error) |err| return err;
        try transport.idle();
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        try std.testing.expectEqual(retained, domain.admission.snapshot());
        std.debug.print("family_cuda_http: concurrency={d} responses_equal=true leases_released=true\n", .{concurrency});
    }
    var entries = node.model_manager.loaded.valueIterator();
    const loaded = (entries.next() orelse return error.MissingLoadedModel).*;
    try std.testing.expectEqual(.cuda, loaded.session.backend());
    try std.testing.expectEqual(@as(usize, 0), loaded.active_handles);
    try queuedCancellation(&node, transport, directory, requests[0], expected[0]);
    try transport.finish();
}
