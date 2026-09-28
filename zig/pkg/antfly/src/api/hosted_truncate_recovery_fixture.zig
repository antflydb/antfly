// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Cold-owner restart and bounded phase rendezvous for mounted TRUNCATE proof.
const std = @import("std");
const platform = @import("antfly_platform");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const server_mod = @import("http_server.zig");

pub const DataRestart = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    server: *data_runtime.DataServer,
    server_live: *bool,
    raft_driver: *raft.ManagedProgressDriver,
    raft_live: *bool,
    control_driver: *raft.ManagedProgressDriver,
    control_live: *bool,
    config: data_runtime.DataServerConfig,
    metadata_uri: []const u8,

    fn runRaft(ptr: *anyopaque) !void {
        const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
        try server.runRaftRoundOnly();
    }
    fn runControl(ptr: *anyopaque) !void {
        const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
        try server.runControlRoundOnly();
    }
    pub fn restart(self: DataRestart) !void {
        self.control_driver.deinit();
        self.control_live.* = false;
        self.raft_driver.deinit();
        self.raft_live.* = false;
        self.server.deinit();
        self.server_live.* = false;
        self.server.* = try data_runtime.DataServer.initFromMetadataApiUrl(self.alloc, self.config, self.metadata_uri);
        self.server_live.* = true;
        try self.server.start();
        const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
        while (true) {
            self.server.registerNodeIfConfigured() catch |err| switch (err) {
                error.StoreRegistrationNotVisible => {
                    if (platform.time.monotonicNs() >= deadline) return err;
                    try self.io.sleep(.fromMilliseconds(10), .awake);
                    continue;
                },
                else => return err,
            };
            break;
        }
        self.raft_driver.* = raft.ManagedProgressDriver.init(self.io, .{ .ptr = self.server, .run_once = runRaft }, std.time.ns_per_ms);
        self.raft_live.* = true;
        try self.raft_driver.start();
        self.control_driver.* = raft.ManagedProgressDriver.init(self.io, .{ .ptr = self.server, .run_once = runControl }, std.time.ns_per_ms);
        self.control_live.* = true;
        try self.control_driver.start();
    }
};

pub fn awaitBoundary(io: std.Io, server: *server_mod.ApiHttpServer, drivers: []const *raft.ManagedProgressDriver) !void {
    const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        for (drivers) |driver| try driver.checkFailure();
        if (server_mod.ApiHttpServer.TruncateTestDriver.seen(server)) return;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.TruncateFaultBoundaryTimeout;
}
