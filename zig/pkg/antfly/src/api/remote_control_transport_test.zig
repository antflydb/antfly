// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Real TCP proof of the pre-helper control gate; no durable retirement claim.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = local.common_http_http_common;
const clock = @import("antfly_platform").time;
const Gate = @import("remote_control_admission.zig").Gate;
const Client = @import("remote_attempt_client.zig").Client;
const attempts = @import("../common/remote_attempts.zig");
const replies = @import("../common/remote_attempt_reply.zig");
const capabilities = @import("../common/remote_read_capabilities.zig");
const wire = @import("../common/remote_attempt_wire.zig");
const auth = @import("internal_service_auth.zig");

fn pause(deadline: u64) !void {
    if (clock.monotonicNs() >= deadline) return error.TestControlProgressDeadline;
    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
}

test "remote control admission TCP discovery progresses behind unavailable status under foreground pressure" {
    const App = struct {
        signer: replies.Signer,
        calls: std.atomic.Value(usize) = .init(0),
        entered: std.atomic.Value(bool) = .init(false),
        unblock: std.Io.Event = .unset,
        second_entered: std.atomic.Value(bool) = .init(false),
        second_is_status: std.atomic.Value(bool) = .init(false),
        unblock_second: std.Io.Event = .unset,
        fn execute(raw: *anyopaque, allocator: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ordinal = self.calls.fetchAdd(1, .acq_rel);
            if (ordinal == 0) {
                self.entered.store(true, .release);
                try self.unblock.wait(std.testing.io);
            }
            if (ordinal == 1) {
                self.second_is_status.store(std.mem.endsWith(u8, request.uri, "/status"), .release);
                self.second_entered.store(true, .release);
                try self.unblock_second.wait(std.testing.io);
            }
            if (std.mem.endsWith(u8, request.uri, "/capabilities")) {
                const token = request.header(auth.header_name) orelse return error.TestMissingControlCredential;
                var fields = std.mem.splitScalar(u8, token, '.');
                _ = fields.next() orelse return error.TestMalformedControlCredential;
                const payload = fields.next() orelse return error.TestMalformedControlCredential;
                const decoder = std.base64.url_safe_no_pad.Decoder;
                const decoded = try allocator.alloc(u8, try decoder.calcSizeForSlice(payload));
                defer allocator.free(decoded);
                try decoder.decode(decoded, payload);
                const Claims = struct { metadata: struct { remote_attempt: []const u8 } };
                var parsed = try std.json.parseFromSlice(Claims, allocator, decoded, .{ .ignore_unknown_fields = true });
                defer parsed.deinit();
                const context = try wire.Context.decode(parsed.value.metadata.remote_attempt);
                const reply = try capabilities.Reply.sign(&self.signer, context, .{ .retained_read = true });
                return .{ .status = 200, .body = try allocator.dupe(u8, &reply.encode()) };
            }
            return .{ .status = 503 };
        }
    };
    const Counted = struct {
        underlying: http.RequestExecutor,
        calls: std.atomic.Value(usize) = .init(0),
        fn execute(raw: *anyopaque, allocator: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .acq_rel);
            return self.underlying.execute(allocator, request);
        }
    };
    const Task = struct {
        gate: *Gate,
        client: Client,
        id: attempts.Id,
        deadline: u64,
        discover: bool,
        outcome: std.atomic.Value(u8) = .init(0),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.work() catch |err| {
                self.failure = err;
                self.outcome.store(2, .release);
                return;
            };
            self.outcome.store(1, .release);
        }
        fn work(self: *@This()) !void {
            var permit = try self.gate.acquireIdle(.{ .deadline_ns = self.deadline });
            defer permit.release();
            var client = self.client;
            client.control_permit = permit.borrow();
            if (self.discover) {
                const observation = try client.probeCapabilities(0, self.deadline, null);
                const families = try observation.forAuthority(client.targets[0].authority, clock.monotonicNs());
                try std.testing.expect(families.retained_read);
            } else {
                try std.testing.expectError(error.RemoteControlUnavailable, client.status(0, self.id, self.deadline));
            }
        }
    };
    var scheduler = try local.common_request_admission.runnable_scheduler.Scheduler.init(.{ .slots = 2, .read_isolation = false, .max_operations = 8, .control_bytes = 1 << 20 });
    defer scheduler.close();
    const gate = try Gate.create(std.testing.allocator, std.testing.io, &scheduler);
    defer gate.tryDestroy() catch unreachable;
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    var app: App = .{ .signer = try replies.Signer.init(authority.local, authority.public_key, @splat(7)) };
    const Listener = local.common_http_std_http_listener.StdHttpListener;
    var listener = Listener.init(std.testing.allocator, .{ .serve_in_connection_threads = true }, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
    defer listener.deinit();
    try listener.start();
    const uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(uri);
    var executor: @import("../common/http/std_http_executor.zig").StdHttpExecutor = undefined;
    try executor.initInPlaceFallible(std.testing.allocator, .{ .io_concurrent_limit = 8, .pool_max_connections = 1, .pool_max_per_host = 1, .pool_max_waiters = 0, .pool_max_wait_bytes = 0, .pool_max_wait_ms = 0 });
    defer executor.deinit();
    var counted: Counted = .{ .underlying = executor.executor() };
    const transport: http.RequestExecutor = .{ .ptr = &counted, .clock_io = counted.underlying.clock_io, .vtable = &.{ .execute = Counted.execute } };
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 64 }};
    var ledger = try attempts.Coordinator.init(7, 1, &slots, &destinations, 64);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    const targets = [_]@import("remote_attempt_client.zig").Target{.{ .base_uri = uri, .authority = authority }};
    const client: Client = .{ .ledger = &ledger, .store = undefined, .targets = &targets, .signing = .{ .secret = "control-admission-rpc-signing-secret-32", .issuer = "control-admission-test" }, .read_allocator = std.testing.allocator, .control_allocator = std.testing.allocator, .read_executor = transport, .control_executor = transport, .control_admission = gate };
    const deadline = clock.monotonicNs() + 5 * std.time.ns_per_s;
    const id: attempts.Id = .{ .coordinator = 7, .generation = 1, .operation = 9, .sequence = 1 };
    var tasks: [3]Task = undefined;
    for (&tasks, 0..) |*task, i| task.* = .{ .gate = gate, .client = client, .id = id, .deadline = deadline, .discover = i == 2 };
    var group: std.Io.Group = .init;
    defer group.cancel(std.testing.io);
    // Unblock the handler before any error-path join.
    defer app.unblock.set(std.testing.io);
    defer app.unblock_second.set(std.testing.io);
    try group.concurrent(std.testing.io, Task.run, .{&tasks[0]});
    while (!app.entered.load(.acquire)) try pause(deadline);
    for (1..3) |i| {
        try group.concurrent(std.testing.io, Task.run, .{&tasks[i]});
        while (gate.admission.stats().queued != i) try pause(deadline);
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), executor.resolved_client.outbound.snapshot().active);
    try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().waiting);
    var empty: [0]u8 = .{};
    var denied_allocator = std.heap.FixedBufferAllocator.init(&empty);
    var foreground = client;
    foreground.control_allocator = denied_allocator.allocator();
    for (0..64) |_| try std.testing.expectError(error.OutboundCapacityUnavailable, foreground.status(0, id, deadline));
    try std.testing.expectEqual(@as(usize, 1), counted.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), app.calls.load(.acquire));
    app.unblock.set(std.testing.io);
    while (!app.second_entered.load(.acquire)) try pause(deadline);
    try std.testing.expect(app.second_is_status.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), gate.admission.stats().queued);
    for (0..64) |_| try std.testing.expectError(error.OutboundCapacityUnavailable, foreground.status(0, id, deadline));
    try std.testing.expectEqual(@as(usize, 2), counted.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), app.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), executor.resolved_client.outbound.snapshot().active);
    try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().waiting);
    app.unblock_second.set(std.testing.io);
    // Keep applying foreground pressure while both queued consumers progress.
    // Once both are granted, a new foreground request may proceed as well.
    while (tasks[1].outcome.load(.acquire) == 0 or tasks[2].outcome.load(.acquire) == 0) {
        _ = client.status(0, id, deadline) catch |err| switch (err) {
            error.OutboundCapacityUnavailable, error.RemoteControlUnavailable => {},
            else => return err,
        };
        try pause(deadline);
    }
    try group.await(std.testing.io);
    for (&tasks) |*task| if (task.outcome.load(.acquire) != 1) return task.failure orelse error.TestControlTaskFailed;
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().queued);
    try std.testing.expectEqual(@as(usize, 0), gate.admission.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().active);
    try std.testing.expectEqual(@as(usize, 0), executor.resolved_client.outbound.snapshot().waiting);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
    try std.testing.expectEqual(@as(u64, 1), ledger.next_sequence);
    const fresh = try client.probeCapabilities(0, deadline, null);
    try std.testing.expect((try fresh.forAuthority(authority, clock.monotonicNs())).retained_read);
}
