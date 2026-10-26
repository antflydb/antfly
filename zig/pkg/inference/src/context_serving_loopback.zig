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

const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const Node = @import("server/server.zig").Node;
const BoundedAllocator = @import("runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const clock = platform.time.monotonicNs;
const short_timeout_ms = 5_000;
const request_timeout_ms = 180_000;

/// Stable addresses for Server, its ListenerTask, executors and allocator.
/// Node and its watchdog borrow std.testing.io, whose lifetime extends beyond
/// this transport owner. Per-request controls borrow server_io only until the
/// listener joins. Model weights and request scratch retain Node admission.
pub const Loopback = struct {
    allocator: Allocator,
    budget: BoundedAllocator,
    server_io: Io.Threaded,
    client_io: Io.Threaded,
    server: httpx.Server,
    client: httpx.Client,
    listener: httpx.Server.ListenerTask,
    address: ?Io.net.IpAddress = null,

    pub fn init(a: Allocator, node: *Node) !*Loopback {
        // Caller-owned listeners need the same exclusive startup precondition
        // as Node.serve. Publish the resource owner before request workers can
        // race its lazy creation; this neither loads a model nor reserves work.
        try node.model_manager.ensureResourceOwnerReady();
        const self = try a.create(Loopback);
        errdefer a.destroy(self);
        self.allocator = a;
        self.budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
        const transport_allocator = self.budget.allocator();
        self.server_io = Io.Threaded.init(transport_allocator, .{ .concurrent_limit = .limited(4) });
        errdefer self.server_io.deinit();
        self.client_io = Io.Threaded.init(transport_allocator, .{ .concurrent_limit = .limited(4) });
        errdefer self.client_io.deinit();
        self.server = httpx.Server.initWithConfig(transport_allocator, self.server_io.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .max_body_size = 64 * 1024,
            .max_headers = 32,
            .max_connections = 2,
            .max_request_tasks = 2,
            .max_h1_inflight_bodies = 1,
            .header_read_timeout_ms = short_timeout_ms,
            .body_read_timeout_ms = short_timeout_ms,
            .response_write_timeout_ms = short_timeout_ms,
            .keep_alive_timeout_ms = short_timeout_ms,
            .keep_alive = false,
            .max_requests_per_connection = 1,
            .request_body_buffer_budget_bytes = 128 * 1024,
        });
        errdefer self.server.deinit();
        // The ingress deadline is fixed by this test owner, never by request
        // contents. Socket timeouts separately bound connect and delivery.
        try self.server.preRoute(deadline);
        try node.registerHttpRoutes(&self.server);
        self.client = httpx.Client.initWithConfig(transport_allocator, self.client_io.io(), .{
            .timeouts = .{
                .connect_ms = short_timeout_ms,
                .read_ms = request_timeout_ms,
                .write_ms = short_timeout_ms,
                .request_ms = request_timeout_ms,
            },
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
        self.listener = .init(&self.server);
        self.address = null;
        return self;
    }

    fn deadline(ctx: *httpx.Context) !void {
        ctx.application_deadline_ns = clock() + request_timeout_ms * std.time.ns_per_ms;
    }

    pub fn start(self: *Loopback) !void {
        // Read the bound address before publishing any listener task. There is
        // no free-port probe/rebind race and no concurrent listener-field read.
        try self.server.bind();
        self.address = self.server.boundAddress() orelse return error.MissingLoopbackAddress;
        const address = self.address.?;
        try std.testing.expect(address == .ip4);
        try std.testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &address.ip4.bytes);
        try std.testing.expect(address.ip4.port != 0);
        try self.listener.start();
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (!self.server.listen_started.load(.acquire)) {
            if (self.listener.runtimeFailure()) |err| return err;
            if (clock() >= until) return error.LoopbackStartTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    /// Caller-owned URL used only by a separately spawned production CLI
    /// client. The listener remains owned by this persistent Driver.
    pub fn baseUrlAlloc(self: *const Loopback, allocator: Allocator) ![]u8 {
        const address = self.address orelse return error.MissingLoopbackAddress;
        if (address != .ip4 or !std.mem.eql(u8, &address.ip4.bytes, &.{ 127, 0, 0, 1 }))
            return error.InvalidLoopbackAddress;
        return std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{address.ip4.port});
    }

    pub fn request(self: *Loopback, method: httpx.types.Method, path: []const u8, bytes: ?[]const u8) !httpx.Response {
        const a = self.budget.allocator();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}{s}", .{ self.address.?.ip4.port, path });
        defer a.free(url);
        return self.client.request(method, url, .{
            .borrowed_body = bytes,
            .headers = &.{.{ "Content-Type", "application/json" }},
            .timeout_ms = if (method == .GET) short_timeout_ms else request_timeout_ms,
        });
    }

    pub fn idle(self: *Loopback) !void {
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (true) {
            const stats = self.server.runtimeStats();
            if (stats.active_connections == 0 and stats.active_requests == 0 and
                self.server.httpRuntimeStats().active_h1_cancellation_observers == 0) break;
            if (self.listener.runtimeFailure()) |err| return err;
            if (clock() >= until) return error.LoopbackDrainTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, 0), self.server.runtimeStats().body_buffer_in_use_bytes);
    }

    pub fn finish(self: *Loopback) !void {
        const started = clock();
        self.listener.shutdown(25);
        try self.listener.join();
        try std.testing.expect(clock() - started < short_timeout_ms * std.time.ns_per_ms);
        try std.testing.expectEqual(httpx.Server.ListenerTask.RuntimeState.stopped, self.listener.runtimeState());
        try self.idle();
        const stats = self.server.httpRuntimeStats();
        try std.testing.expectEqual(@as(usize, 0), stats.active_listener_leases);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_connection_capacity);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_request_capacity);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_h1_request_capacity);
    }

    pub fn deinit(self: *Loopback) void {
        // Also runs after a failed assertion or response parse. join consumes
        // its Future on errors; never destroy routes/Node under live handlers.
        self.listener.requestStop();
        self.listener.join() catch {};
        self.client.deinit();
        self.server.deinit();
        self.client_io.deinit();
        self.server_io.deinit();
        std.debug.assert(self.budget.live == 0);
        self.allocator.destroy(self);
    }
};
