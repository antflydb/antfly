// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const raft_engine = @import("raft_engine");
const http_common = @import("http_common.zig");
const http_driver = @import("http_driver.zig");
const http_server = @import("http_server.zig");
const http_snapshot = @import("http_snapshot.zig");

pub const HttpTransportStackConfig = struct {
    driver: http_driver.HttpDriverConfig = .{},
    snapshot: http_snapshot.HttpSnapshotConfig,
    retry_policy: raft_engine.runtime.TransportRetryPolicy = .{},
};

pub const HttpTransportStack = struct {
    alloc: std.mem.Allocator,
    driver: *http_driver.HttpFrameDriver,
    transport_host: raft_engine.runtime.CodecTransportHost,
    snapshot_transport: http_snapshot.HttpSnapshotTransport,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpTransportStackConfig,
        executor: http_common.RequestExecutor,
        snapshot_resolver: ?http_snapshot.SnapshotTargetResolver,
    ) !HttpTransportStack {
        const driver = try alloc.create(http_driver.HttpFrameDriver);
        errdefer alloc.destroy(driver);
        driver.* = http_driver.HttpFrameDriver.init(alloc, cfg.driver, executor);
        return .{
            .alloc = alloc,
            .driver = driver,
            .transport_host = raft_engine.runtime.CodecTransportHost.init(
                alloc,
                raft_engine.runtime.BinaryCodec.codec(),
                driver.frameDriver(),
                cfg.retry_policy,
            ),
            .snapshot_transport = http_snapshot.HttpSnapshotTransport.init(
                alloc,
                cfg.snapshot,
                executor,
                snapshot_resolver,
            ),
        };
    }

    pub fn deinit(self: *HttpTransportStack) void {
        self.transport_host.deinit();
        self.alloc.destroy(self.driver);
        self.* = undefined;
    }

    pub fn runtimeHooks(self: *HttpTransportStack) raft_engine.runtime.multi_raft.RuntimeHooks {
        return .{
            .transport = self.transport_host.transport(),
            .snapshot_transport = self.snapshot_transport.transport(),
        };
    }

    pub fn makeServer(
        self: *HttpTransportStack,
        batch_handler: http_server.BatchHandler,
        snapshot_store: ?http_server.SnapshotStore,
    ) http_server.HttpServer {
        return http_server.HttpServer.init(
            self.alloc,
            .{},
            raft_engine.runtime.BinaryCodec.codec(),
            batch_handler,
            snapshot_store,
        );
    }
};

test "http transport stack compiles" {
    const Executor = struct {
        fn iface(_: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            _ = req;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var executor = Executor{};
    var stack = try HttpTransportStack.init(std.testing.allocator, .{
        .snapshot = .{ .root_dir = "/tmp" },
    }, executor.iface(), null);
    defer stack.deinit();

    const hooks = stack.runtimeHooks();
    try std.testing.expect(hooks.transport != null);
    try std.testing.expect(hooks.snapshot_transport != null);
}
