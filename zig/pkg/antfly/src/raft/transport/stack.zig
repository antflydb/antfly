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
const snapshot_transfer = @import("snapshot_transfer.zig");
const threaded_io_limits = @import("../../common/threaded_io_limits.zig");

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
    owned_io_impl: ?*std.Io.Threaded = null,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpTransportStackConfig,
        executor: http_common.RequestExecutor,
        io: ?std.Io,
        snapshot_resolver: ?http_snapshot.SnapshotTargetResolver,
    ) !HttpTransportStack {
        if (cfg.snapshot.chunk_size < snapshot_transfer.min_chunk_bytes or
            cfg.snapshot.chunk_size > snapshot_transfer.max_chunk_bytes or
            cfg.snapshot.max_snapshot_bytes == 0 or
            cfg.snapshot.legacy_fallback_max_request_bytes == 0)
            return error.InvalidSnapshotTransferLimits;
        var owned_io_impl: ?*std.Io.Threaded = null;
        errdefer if (owned_io_impl) |io_impl| {
            io_impl.deinit();
            alloc.destroy(io_impl);
        };
        const driver_io = io orelse blk: {
            const io_impl = try alloc.create(std.Io.Threaded);
            errdefer alloc.destroy(io_impl);
            // Embedded hosts may omit a shared backend runtime. Keep that
            // process-lifetime fallback finite as well.
            io_impl.* = threaded_io_limits.initService(alloc);
            owned_io_impl = io_impl;
            break :blk io_impl.io();
        };

        const driver = try alloc.create(http_driver.HttpFrameDriver);
        errdefer alloc.destroy(driver);
        if (io != null) {
            try driver.initAsyncInPlace(alloc, cfg.driver, executor, driver_io);
        } else {
            driver.* = http_driver.HttpFrameDriver.init(alloc, cfg.driver, executor, driver_io);
        }
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
            .owned_io_impl = owned_io_impl,
        };
    }

    pub fn deinit(self: *HttpTransportStack) void {
        self.transport_host.deinit();
        self.driver.deinit();
        self.alloc.destroy(self.driver);
        if (self.owned_io_impl) |io_impl| {
            io_impl.deinit();
            self.alloc.destroy(io_impl);
        }
        self.* = undefined;
    }

    pub fn runtimeHooks(self: *HttpTransportStack) raft_engine.runtime.multi_raft.RuntimeHooks {
        return .{
            .transport = self.transport_host.transport(),
            .snapshot_transport = self.snapshot_transport.transport(),
        };
    }

    pub fn asyncSendMetricsSnapshot(self: *HttpTransportStack) http_driver.AsyncSendMetricsSnapshot {
        return self.driver.metricsSnapshot();
    }

    pub fn makeServer(
        self: *HttpTransportStack,
        batch_handler: http_server.BatchHandler,
        snapshot_store: ?http_server.SnapshotStore,
        snapshot_upload_handler: ?http_server.SnapshotUploadHandler,
    ) http_server.HttpServer {
        return http_server.HttpServer.init(
            self.alloc,
            .{},
            raft_engine.runtime.BinaryCodec.codec(),
            batch_handler,
            snapshot_store,
            snapshot_upload_handler,
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
    }, executor.iface(), null, null);
    defer stack.deinit();

    const hooks = stack.runtimeHooks();
    try std.testing.expect(hooks.transport != null);
    try std.testing.expect(hooks.snapshot_transport != null);
    try std.testing.expectEqual(
        std.Io.Limit.limited(threaded_io_limits.service),
        stack.owned_io_impl.?.concurrent_limit,
    );
}
