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
const common = @import("http_common.zig");
const routes = @import("routes.zig");

pub const HttpDriverConfig = struct {
    request_timeout_ms: u32 = 5_000,
    max_batch_bytes: usize = 1 << 20,
};

pub const SendBatch = struct {
    source_id: ?u64 = null,
    peer_id: u64,
    base_uri: []const u8,
    body: []const u8,
    content_type: []const u8,
};

pub const HttpFrameDriver = struct {
    alloc: std.mem.Allocator,
    cfg: HttpDriverConfig,
    executor: common.RequestExecutor,

    pub fn init(alloc: std.mem.Allocator, cfg: HttpDriverConfig, executor: common.RequestExecutor) HttpFrameDriver {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .executor = executor,
        };
    }

    pub fn frameDriver(self: *HttpFrameDriver) raft_engine.runtime.FrameDriver {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_frame = sendFrame,
            },
        };
    }

    pub fn sendBatch(self: *HttpFrameDriver, batch: SendBatch) !void {
        if (batch.body.len > self.cfg.max_batch_bytes) return error.BatchTooLarge;
        var uri_stack_buf: [256]u8 = undefined;
        const uri, const uri_owned = blk: {
            const joined = routes.Routes.joinInto(&uri_stack_buf, batch.base_uri, routes.Routes.raft_batch) catch |err| switch (err) {
                error.NoSpace => {
                    const owned = try routes.Routes.join(self.alloc, batch.base_uri, routes.Routes.raft_batch);
                    break :blk .{ owned, true };
                },
            };
            break :blk .{ joined, false };
        };
        defer if (uri_owned) self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .source_node_id = batch.source_id,
            .content_type = batch.content_type,
            .body = batch.body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
    }

    fn sendFrame(ptr: *anyopaque, req: raft_engine.runtime.frame_driver_iface.SendFrameRequest) !void {
        const self: *HttpFrameDriver = @ptrCast(@alignCast(ptr));
        try self.sendBatch(.{
            .source_id = req.source_id,
            .peer_id = req.peer_id,
            .base_uri = req.endpoint.address,
            .body = req.frame.bytes,
            .content_type = req.frame.media_type,
        });
    }
};

test "http driver module compiles" {
    _ = HttpDriverConfig;
    _ = SendBatch;
    _ = HttpFrameDriver;
}

test "http frame driver posts batch frames to raft batch route" {
    const RecordingExecutor = struct {
        alloc: std.mem.Allocator,
        last_req: ?common.HttpRequest = null,

        fn deinit(self: *@This()) void {
            if (self.last_req) |req| {
                self.alloc.free(req.uri);
                if (req.content_type) |content_type| self.alloc.free(content_type);
                if (req.body.len > 0) self.alloc.free(req.body);
            }
            self.* = undefined;
        }

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_req) |prev| {
                self.alloc.free(prev.uri);
                if (prev.content_type) |content_type| self.alloc.free(content_type);
                if (prev.body.len > 0) self.alloc.free(prev.body);
            }
            self.last_req = .{
                .method = req.method,
                .uri = try self.alloc.dupe(u8, req.uri),
                .source_node_id = req.source_node_id,
                .content_type = if (req.content_type) |content_type| try self.alloc.dupe(u8, content_type) else null,
                .body = try self.alloc.dupe(u8, req.body),
            };
            return .{
                .status = 202,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var executor = RecordingExecutor{ .alloc = std.testing.allocator };
    defer executor.deinit();
    var driver = HttpFrameDriver.init(std.testing.allocator, .{}, executor.iface());
    try driver.sendBatch(.{
        .source_id = 1,
        .peer_id = 2,
        .base_uri = "http://n2:8080",
        .body = "frame-bytes",
        .content_type = "application/x-antflydb-raft-binary-v1",
    });
    try std.testing.expectEqual(common.Method.POST, executor.last_req.?.method);
    try std.testing.expectEqual(@as(?u64, 1), executor.last_req.?.source_node_id);
    try std.testing.expectEqualStrings("http://n2:8080/raft/v1/batch", executor.last_req.?.uri);
    try std.testing.expectEqualStrings("frame-bytes", executor.last_req.?.body);
}
