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

//! Actual signed HTTP original-terminal recovery against the live native Raft owner.
const std = @import("std");
const runtime = @import("runtime_root.zig");
const httpx = @import("httpx");
const metadata = @import("../metadata/api.zig");
const wire = @import("source_legacy_terminal_http_wire.zig");
const Client = @import("../api/http_client.zig").ApiHttpClient;
pub noinline fn submit(a: std.mem.Allocator, node: anytype, fence: metadata.CatalogRouteFence, request: wire.Request) !wire.Reply {
    const Worker = struct {
        alloc: std.mem.Allocator,
        node: @TypeOf(node),
        fence: metadata.CatalogRouteFence,
        request: wire.Request,
        result: ?wire.Reply = null,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.result = submitOnWorker(self.alloc, self.node, self.fence, self.request) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    // Native API/IO construction has large frames. Keep this additional
    // loopback owner off the already large parent Raft fixture's stack.
    var worker: Worker = .{ .alloc = a, .node = node, .fence = fence, .request = request };
    const thread = try std.Thread.spawn(.{ .stack_size = 32 * 1024 * 1024 }, Worker.run, .{&worker});
    thread.join();
    if (worker.failure) |err| return err;
    return worker.result orelse error.TestExpectedProducerReply;
}
fn submitOnWorker(a: std.mem.Allocator, node: anytype, fence: metadata.CatalogRouteFence, request: wire.Request) !wire.Reply {
    const secret = "legacy-terminal-rpc-native-secret-long-enough";
    const Status = struct {
        fn read(_: *anyopaque) !metadata.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn listen(listener: *httpx.Server) void {
            listener.listen() catch |err| std.debug.panic("legacy terminal listener failed: {}", .{err});
        }
    };
    const api = try a.create(runtime.public_api.kernel_bridge.ApiHttpServer);
    defer a.destroy(api);
    api.* = try runtime.public_api.kernel_bridge.ApiHttpServer.initWithConfig(a, .{
        .backend_runtime = node.backend_runtime,
        .internal_service_secret = secret,
        .internal_service_issuer = "legacy-terminal-native",
        .internal_service_accept_legacy_unauthenticated = true,
        .native_legacy_terminal = node.legacyTerminalPort(),
    }, .{ .ptr = node, .vtable = &.{ .status = Status.read } }, null, null);
    defer api.deinit();
    var handler = try runtime.public_api.kernel_bridge.createHandler(api);
    defer runtime.public_api.kernel_bridge.deinitHandler(&handler);
    try handler.initRuntime(a);
    var network_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer network_io.deinit();
    var listener = httpx.Server.initWithConfig(a, network_io.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer listener.deinit();
    try handler.registerRoutes(&listener);
    try listener.bind();
    var task = try std.testing.io.concurrent(Status.listen, .{&listener});
    defer {
        listener.stop();
        task.await(std.testing.io);
    }
    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{listener.boundAddress().?.ip4.port});
    defer a.free(base);
    var executor = @import("../raft/transport/std_http_executor.zig").StdHttpExecutor.init(a, .{});
    defer executor.deinit();
    var client = Client.init(a, executor.executor());
    try std.testing.expectError(error.Unauthorized, client.fetchGroupLegacyTerminal(base, request.owner.group_id, "docs", request, fence, .{}));
    _ = client.withInternalServiceAuth(secret, "legacy-terminal-native");
    const result = try client.fetchGroupLegacyTerminal(base, request.owner.group_id, "docs", request, fence, .{});
    try result.validate(a, request, try request.digest(a));
    return result;
}
