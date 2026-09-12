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
const common_http = @import("../../common/http/mod.zig");
const platform_time = @import("antfly_platform").time;
const routes = @import("routes.zig");

pub const HttpDriverConfig = struct {
    /// Exclusive sender capacity borrowed from the enclosing runtime owner.
    sender_io: ?std.Io = null,
    request_timeout_ms: u32 = 5_000,
    max_batch_bytes: usize = common_http.default_max_request_bytes,
    async_send_queue_max: usize = 4096,
    async_send_queue_max_per_peer: usize = 256,
    /// Four maximum-size requests plus bounded routing metadata.
    async_send_retained_bytes_max: usize = 4 * (common_http.default_max_request_bytes + 64 * 1024),
    async_send_retained_bytes_max_per_peer: usize = common_http.default_max_request_bytes + 64 * 1024,
    async_send_worker_count: u32 = 4,
    isolated_worker_executors: bool = false,
    isolated_worker_executor_config: common_http.StdHttpExecutorConfig = .{},
};

pub const AsyncSendMetricsSnapshot = struct {
    enqueued: u64 = 0,
    failed: u64 = 0,
    retried: u64 = 0,
    dropped: u64 = 0,
    queue_full: u64 = 0,
    peer_queue_full: u64 = 0,
    pending: usize = 0,
    retained_bytes: usize = 0,
    retained_frames: usize = 0,
};

const AsyncSendMetrics = struct {
    enqueued: std.atomic.Value(u64) = .init(0),
    failed: std.atomic.Value(u64) = .init(0),
    retried: std.atomic.Value(u64) = .init(0),
    dropped: std.atomic.Value(u64) = .init(0),
    queue_full: std.atomic.Value(u64) = .init(0),
    peer_queue_full: std.atomic.Value(u64) = .init(0),
};

pub const SendBatch = struct {
    source_id: ?u64 = null,
    peer_id: u64,
    base_uri: []const u8,
    body: []const u8,
    content_type: []const u8,
};

pub const HttpFrameDriver = struct {
    const Retention = struct { bytes: usize = 0, frames: usize = 0 };
    const QueuedFrame = struct {
        source_id: ?u64 = null,
        peer_id: u64,
        base_uri: []u8,
        body: []u8,
        content_type: []u8,
        attempt: u32 = 1,
        group_ids: []u64,

        fn deinit(self: *QueuedFrame, alloc: std.mem.Allocator) void {
            alloc.free(self.base_uri);
            alloc.free(self.body);
            alloc.free(self.content_type);
            alloc.free(self.group_ids);
            self.* = undefined;
        }
    };

    alloc: std.mem.Allocator,
    cfg: HttpDriverConfig,
    executor: common.RequestExecutor,
    io: std.Io,
    // Dedicated capacity keeps Raft senders independent of request/artifact fan-out.
    sender_io: ?std.Io.Threaded = null,
    workers: []std.Io.Future(void) = &.{},
    isolated_executors: []common_http.StdHttpExecutor = &.{},
    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    closing: bool = false,
    queue: std.ArrayListUnmanaged(QueuedFrame) = .empty,
    queue_head: usize = 0,
    failed: std.ArrayListUnmanaged(QueuedFrame) = .empty,
    failed_head: usize = 0,
    retained: Retention = .{},
    peer_retention: std.AutoHashMapUnmanaged(u64, Retention) = .empty,
    in_flight_peers: std.AutoHashMapUnmanaged(u64, void) = .empty,
    metrics: AsyncSendMetrics = .{},

    pub fn init(alloc: std.mem.Allocator, cfg: HttpDriverConfig, executor: common.RequestExecutor, io: std.Io) HttpFrameDriver {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .executor = executor,
            .io = io,
        };
    }

    pub fn initAsyncInPlace(self: *HttpFrameDriver, alloc: std.mem.Allocator, cfg: HttpDriverConfig, executor: common.RequestExecutor, io: std.Io) !void {
        self.* = HttpFrameDriver.init(alloc, cfg, executor, io);
        errdefer self.deinit();
        try self.startAsyncSender();
    }

    pub fn deinit(self: *HttpFrameDriver) void {
        self.stopAsyncSender();
        self.mutex.lockUncancelable(self.io);
        self.clearQueueLocked();
        self.queue.deinit(self.alloc);
        self.failed.deinit(self.alloc);
        self.peer_retention.deinit(self.alloc);
        self.in_flight_peers.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    /// Publish async-sender shutdown without awaiting or destroying workers.
    /// Shared deterministic schedulers use this before driving their task set
    /// to quiescence; `deinit` remains the sole join and destruction owner.
    pub fn beginShutdown(self: *HttpFrameDriver) void {
        if (self.workers.len == 0) return;
        self.mutex.lockUncancelable(self.io);
        self.closing = true;
        self.cond.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    pub fn frameDriver(self: *HttpFrameDriver) raft_engine.runtime.FrameDriver {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_frame = sendFrame,
                .poll_failed_frame = pollFailedFrame,
                .invalidate_route = invalidateRoute,
            },
        };
    }

    pub fn metricsSnapshot(self: *HttpFrameDriver) AsyncSendMetricsSnapshot {
        self.mutex.lockUncancelable(self.io);
        const pending = self.pendingQueueCountLocked();
        const retained = self.retained;
        self.mutex.unlock(self.io);
        return .{
            .enqueued = self.metrics.enqueued.load(.monotonic),
            .failed = self.metrics.failed.load(.monotonic),
            .retried = self.metrics.retried.load(.monotonic),
            .dropped = self.metrics.dropped.load(.monotonic),
            .queue_full = self.metrics.queue_full.load(.monotonic),
            .peer_queue_full = self.metrics.peer_queue_full.load(.monotonic),
            .pending = pending,
            .retained_bytes = retained.bytes,
            .retained_frames = retained.frames,
        };
    }

    pub fn sendBatch(self: *HttpFrameDriver, batch: SendBatch) !void {
        return self.sendBatchWithExecutor(batch, self.executor);
    }

    fn sendBatchWithExecutor(self: *HttpFrameDriver, batch: SendBatch, executor: common.RequestExecutor) !void {
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

        var resp = try executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .source_node_id = batch.source_id,
            .content_type = batch.content_type,
            .timeout_ms = self.cfg.request_timeout_ms,
            .body = batch.body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
    }

    fn senderIo(self: *@This()) std.Io {
        return self.cfg.sender_io orelse self.sender_io.?.io();
    }

    fn startAsyncSender(self: *HttpFrameDriver) !void {
        if (self.workers.len != 0) return;
        // A zero-sized pool is the explicit synchronous mode. It is useful to
        // deterministic runtimes that drive Raft in bounded rounds: delivery
        // must finish (and may itself yield through borrowed Io) before the
        // next modeled round begins.
        if (self.cfg.async_send_worker_count == 0) return;
        try self.in_flight_peers.ensureTotalCapacity(self.alloc, self.cfg.async_send_worker_count);
        if (self.cfg.isolated_worker_executors) {
            self.isolated_executors = try self.alloc.alloc(common_http.StdHttpExecutor, self.cfg.async_send_worker_count);
            for (self.isolated_executors) |*executor| {
                executor.initInPlace(self.alloc, self.cfg.isolated_worker_executor_config);
            }
        }
        errdefer self.deinitIsolatedExecutors();
        self.workers = try self.alloc.alloc(std.Io.Future(void), self.cfg.async_send_worker_count);
        if (self.cfg.sender_io == null) self.sender_io = std.Io.Threaded.init(self.alloc, .{
            .async_limit = .nothing,
            .concurrent_limit = .limited(self.cfg.async_send_worker_count),
        });
        errdefer {
            if (self.sender_io) |*owned| owned.deinit();
            self.sender_io = null;
        }
        var started: usize = 0;
        errdefer {
            self.mutex.lockUncancelable(self.io);
            self.closing = true;
            self.cond.broadcast(self.io);
            self.mutex.unlock(self.io);
            for (self.workers[0..started]) |*future| future.await(self.senderIo());
            self.alloc.free(self.workers);
            self.workers = &.{};
        }
        while (started < self.workers.len) : (started += 1) {
            self.workers[started] = try self.senderIo().concurrent(asyncSenderMain, .{ self, started });
        }
    }

    fn stopAsyncSender(self: *HttpFrameDriver) void {
        if (self.workers.len == 0) return;
        self.beginShutdown();
        for (self.workers) |*worker| _ = worker.await(self.senderIo());
        self.alloc.free(self.workers);
        self.workers = &.{};
        if (self.sender_io) |*owned| owned.deinit();
        self.sender_io = null;
        self.deinitIsolatedExecutors();
    }

    fn deinitIsolatedExecutors(self: *HttpFrameDriver) void {
        if (self.isolated_executors.len == 0) return;
        for (self.isolated_executors) |*executor| executor.deinit();
        self.alloc.free(self.isolated_executors);
        self.isolated_executors = &.{};
    }

    fn asyncSenderMain(self: *HttpFrameDriver, worker_index: usize) void {
        const executor = if (self.isolated_executors.len == 0)
            self.executor
        else
            self.isolated_executors[worker_index].executor();
        while (true) {
            const frame = self.popQueuedFrame() orelse break;
            var owned = frame;
            self.sendBatchWithExecutor(.{
                .source_id = owned.source_id,
                .peer_id = owned.peer_id,
                .base_uri = owned.base_uri,
                .body = owned.body,
                .content_type = owned.content_type,
            }, executor) catch |err| {
                _ = self.metrics.failed.fetchAdd(1, .monotonic);
                self.mutex.lockUncancelable(self.io);
                std.debug.assert(self.in_flight_peers.remove(frame.peer_id));
                if (err == error.BatchTooLarge or self.closing) {
                    _ = self.metrics.dropped.fetchAdd(1, .monotonic);
                    self.releaseRetentionLocked(owned);
                    owned.deinit(self.alloc);
                } else self.publishFailureLocked(owned);
                self.cond.broadcast(self.io);
                self.mutex.unlock(self.io);
                continue;
            };
            self.mutex.lockUncancelable(self.io);
            std.debug.assert(self.in_flight_peers.remove(frame.peer_id));
            self.releaseRetentionLocked(owned);
            self.cond.broadcast(self.io);
            self.mutex.unlock(self.io);
            owned.deinit(self.alloc);
        }
    }

    fn releaseRetentionLocked(self: *HttpFrameDriver, frame: QueuedFrame) void {
        const size = frame.body.len + frame.base_uri.len + frame.content_type.len + frame.group_ids.len * @sizeOf(u64);
        self.retained.bytes -= size;
        self.retained.frames -= 1;
        const peer = self.peer_retention.getPtr(frame.peer_id).?;
        peer.bytes -= size;
        peer.frames -= 1;
        if (peer.frames == 0) _ = self.peer_retention.remove(frame.peer_id);
    }

    fn publishFailureLocked(self: *HttpFrameDriver, frame: QueuedFrame) void {
        if (self.failed_head > 0 and self.failed_head * 2 >= self.failed.items.len) {
            const remaining = self.failed.items.len - self.failed_head;
            std.mem.copyForwards(QueuedFrame, self.failed.items[0..remaining], self.failed.items[self.failed_head..]);
            self.failed.items.len = remaining;
            self.failed_head = 0;
        }
        self.failed.append(self.alloc, frame) catch {
            var owned = frame;
            self.releaseRetentionLocked(owned);
            owned.deinit(self.alloc);
            _ = self.metrics.dropped.fetchAdd(1, .monotonic);
        };
    }

    fn pollFailedFrame(ptr: *anyopaque) ?raft_engine.runtime.frame_driver_iface.FailedFrame {
        const self: *HttpFrameDriver = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.failed_head == self.failed.items.len) return null;
        const frame = self.failed.items[self.failed_head];
        self.failed_head += 1;
        self.releaseRetentionLocked(frame);
        self.alloc.free(frame.base_uri);
        self.alloc.free(frame.group_ids);
        return .{ .alloc = self.alloc, .source_id = frame.source_id, .peer_id = frame.peer_id, .frame = .{ .bytes = frame.body, .media_type = frame.content_type }, .attempt = frame.attempt };
    }

    fn invalidateRoute(ptr: *anyopaque, group_id: u64, peer_id: u64) void {
        const self: *HttpFrameDriver = @ptrCast(@alignCast(ptr));
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var i = self.queue_head;
        while (i < self.queue.items.len) {
            const frame = self.queue.items[i];
            if (frame.peer_id != peer_id or std.mem.indexOfScalar(u64, frame.group_ids, group_id) == null) {
                i += 1;
                continue;
            }
            _ = self.queue.orderedRemove(i);
            self.publishFailureLocked(frame);
        }
        // In-flight requests were admitted under the previous route. Their
        // eventual failure returns here through the normal completion path.
        self.cond.broadcast(self.io);
    }

    fn enqueueFrame(self: *HttpFrameDriver, req: raft_engine.runtime.frame_driver_iface.SendFrameRequest) !void {
        if (self.workers.len == 0) {
            return try self.sendBatch(.{
                .source_id = req.source_id,
                .peer_id = req.peer_id,
                .base_uri = req.endpoint.address,
                .body = req.frame.bytes,
                .content_type = req.frame.media_type,
            });
        }

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closing) return error.AsyncSenderClosed;
        if (req.frame.bytes.len > self.cfg.max_batch_bytes) return error.BatchTooLarge;
        const size = std.math.add(usize, req.frame.bytes.len, req.endpoint.address.len) catch return error.BatchTooLarge;
        const metadata_size = std.math.add(usize, req.frame.media_type.len, std.math.mul(usize, req.group_ids.len, @sizeOf(u64)) catch return error.BatchTooLarge) catch return error.BatchTooLarge;
        const bytes = std.math.add(usize, size, metadata_size) catch return error.BatchTooLarge;
        const peer = self.peer_retention.get(req.peer_id) orelse Retention{};
        if (self.retained.frames >= self.cfg.async_send_queue_max or bytes > self.cfg.async_send_retained_bytes_max -| self.retained.bytes) {
            _ = self.metrics.queue_full.fetchAdd(1, .monotonic);
            return error.AsyncSendQueueFull;
        }
        if (peer.frames >= self.cfg.async_send_queue_max_per_peer or bytes > self.cfg.async_send_retained_bytes_max_per_peer -| peer.bytes) {
            _ = self.metrics.peer_queue_full.fetchAdd(1, .monotonic);
            return error.AsyncSendQueueFull;
        }
        // Reserve before copying bytes. Queued, in-flight and failed completions
        // all retain the same reservation until delivery or ownership transfer.
        const entry = try self.peer_retention.getOrPut(self.alloc, req.peer_id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        entry.value_ptr.bytes += bytes;
        entry.value_ptr.frames += 1;
        self.retained.bytes += bytes;
        self.retained.frames += 1;
        errdefer {
            self.retained.bytes -= bytes;
            self.retained.frames -= 1;
            entry.value_ptr.bytes -= bytes;
            entry.value_ptr.frames -= 1;
            if (entry.value_ptr.frames == 0) _ = self.peer_retention.remove(req.peer_id);
        }
        const address = try self.alloc.dupe(u8, req.endpoint.address);
        errdefer self.alloc.free(address);
        const body = try self.alloc.dupe(u8, req.frame.bytes);
        errdefer self.alloc.free(body);
        const content_type = try self.alloc.dupe(u8, req.frame.media_type);
        errdefer self.alloc.free(content_type);
        const group_ids = try self.alloc.dupe(u64, req.group_ids);
        errdefer self.alloc.free(group_ids);
        try self.queue.append(self.alloc, .{ .source_id = req.source_id, .peer_id = req.peer_id, .base_uri = address, .body = body, .content_type = content_type, .group_ids = group_ids, .attempt = req.attempt });
        _ = self.metrics.enqueued.fetchAdd(1, .monotonic);
        if (req.attempt > 1) _ = self.metrics.retried.fetchAdd(1, .monotonic);
        self.cond.signal(self.io);
    }

    fn popQueuedFrame(self: *HttpFrameDriver) ?QueuedFrame {
        while (true) {
            self.mutex.lockUncancelable(self.io);
            if (self.closing) {
                self.mutex.unlock(self.io);
                return null;
            }
            if (self.popReadyFrameLocked()) |frame| {
                self.mutex.unlock(self.io);
                return frame;
            }
            const pending = self.pendingQueueCountLocked();
            self.mutex.unlock(self.io);

            if (pending == 0) {
                self.mutex.lockUncancelable(self.io);
                if (!self.closing and self.pendingQueueCountLocked() == 0) {
                    self.cond.waitUncancelable(self.io, &self.mutex);
                }
                self.mutex.unlock(self.io);
            } else {
                self.io.sleep(std.Io.Duration.fromMilliseconds(1), .awake) catch {};
            }
        }
    }

    fn popReadyFrameLocked(self: *HttpFrameDriver) ?QueuedFrame {
        self.compactQueueIfNeededLocked();
        for (self.queue.items, 0..) |frame, index| {
            if (self.in_flight_peers.contains(frame.peer_id)) continue;
            const out = frame;
            if (index + 1 < self.queue.items.len) {
                std.mem.copyForwards(
                    QueuedFrame,
                    self.queue.items[index .. self.queue.items.len - 1],
                    self.queue.items[index + 1 ..],
                );
            }
            self.queue.items.len -= 1;
            self.in_flight_peers.putAssumeCapacity(frame.peer_id, {});
            return out;
        }
        return null;
    }

    fn pendingQueueCountLocked(self: *const HttpFrameDriver) usize {
        return self.queue.items.len - self.queue_head;
    }

    fn pendingQueueCountForPeerLocked(self: *const HttpFrameDriver, peer_id: u64) usize {
        var count: usize = 0;
        for (self.queue.items[self.queue_head..]) |frame| {
            if (frame.peer_id == peer_id) count += 1;
        }
        return count;
    }

    fn compactQueueIfNeededLocked(self: *HttpFrameDriver) void {
        if (self.queue_head == 0) return;
        if (self.queue_head < 64 and self.queue_head * 2 < self.queue.items.len) return;
        const remaining = self.queue.items.len - self.queue_head;
        std.mem.copyForwards(QueuedFrame, self.queue.items[0..remaining], self.queue.items[self.queue_head..]);
        self.queue.items.len = remaining;
        self.queue_head = 0;
    }

    fn clearQueueLocked(self: *HttpFrameDriver) void {
        for (self.queue.items[self.queue_head..]) |*frame| {
            self.releaseRetentionLocked(frame.*);
            frame.deinit(self.alloc);
        }
        for (self.failed.items[self.failed_head..]) |*frame| {
            self.releaseRetentionLocked(frame.*);
            frame.deinit(self.alloc);
        }
        self.failed.clearRetainingCapacity();
        self.failed_head = 0;
        self.queue.clearRetainingCapacity();
        self.queue_head = 0;
    }

    fn sendFrame(ptr: *anyopaque, req: raft_engine.runtime.frame_driver_iface.SendFrameRequest) !void {
        const self: *HttpFrameDriver = @ptrCast(@alignCast(ptr));
        try self.enqueueFrame(req);
    }
};

fn nowMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

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
                .timeout_ms = req.timeout_ms,
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
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var driver = HttpFrameDriver.init(std.testing.allocator, .{}, executor.iface(), io_impl.io());
    try driver.sendBatch(.{
        .source_id = 1,
        .peer_id = 2,
        .base_uri = "http://n2:8080",
        .body = "frame-bytes",
        .content_type = "application/x-antflydb-raft-binary-v1",
    });
    try std.testing.expectEqual(common.Method.POST, executor.last_req.?.method);
    try std.testing.expectEqual(@as(?u64, 1), executor.last_req.?.source_node_id);
    try std.testing.expectEqual(@as(?u32, 5_000), executor.last_req.?.timeout_ms);
    try std.testing.expectEqualStrings("http://n2:8080/raft/v1/batch", executor.last_req.?.uri);
    try std.testing.expectEqualStrings("frame-bytes", executor.last_req.?.body);
}

test "http frame driver isolates blocked peers without reordering a peer lane" {
    const BlockingExecutor = struct {
        io: std.Io,
        mutex: std.Io.Mutex = .init,
        cond: std.Io.Condition = .init,
        allow: bool = false,
        calls: usize = 0,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn release(self: *@This()) void {
            self.mutex.lockUncancelable(self.io);
            self.allow = true;
            self.cond.broadcast(self.io);
            self.mutex.unlock(self.io);
        }

        fn callCount(self: *@This()) usize {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            return self.calls;
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutex.lockUncancelable(self.io);
            self.calls += 1;
            while (!self.allow) {
                self.cond.waitUncancelable(self.io, &self.mutex);
            }
            self.mutex.unlock(self.io);
            try std.testing.expectEqual(@as(?u32, 5_000), req.timeout_ms);
            return .{
                .status = 202,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer io_impl.deinit();
    const io = io_impl.io();

    var executor = BlockingExecutor{ .io = io };
    var driver: HttpFrameDriver = undefined;
    try driver.initAsyncInPlace(std.testing.allocator, .{}, executor.iface(), io);
    defer driver.deinit();
    defer executor.release();

    const frame_driver = driver.frameDriver();
    const frame_bytes = try std.testing.allocator.dupe(u8, "frame-bytes");
    defer std.testing.allocator.free(frame_bytes);
    try frame_driver.sendFrame(.{
        .source_id = 1,
        .peer_id = 2,
        .endpoint = .{ .protocol = .http1, .address = "http://n2:8080" },
        .frame = .{
            .bytes = frame_bytes,
            .media_type = "application/x-antflydb-raft-binary-v1",
        },
    });
    try frame_driver.sendFrame(.{
        .source_id = 1,
        .peer_id = 2,
        .endpoint = .{ .protocol = .http1, .address = "http://n2:8080" },
        .frame = .{
            .bytes = frame_bytes,
            .media_type = "application/x-antflydb-raft-binary-v1",
        },
    });
    try frame_driver.sendFrame(.{
        .source_id = 1,
        .peer_id = 3,
        .endpoint = .{ .protocol = .http1, .address = "http://n3:8080" },
        .frame = .{
            .bytes = frame_bytes,
            .media_type = "application/x-antflydb-raft-binary-v1",
        },
    });

    const deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (executor.callCount() < 2 and platform_time.monotonicNs() < deadline_ns) std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
    // One worker may block per peer. The second peer-2 frame stays queued while
    // peer 3 progresses independently.
    try std.testing.expectEqual(@as(usize, 2), executor.callCount());
    executor.release();
}

test "http frame driver propagates isolated worker executor configuration" {
    const UnusedExecutor = struct {
        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedRequest;
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var unused = UnusedExecutor{};
    const executor_config: common_http.StdHttpExecutorConfig = .{
        .read_buffer_size = 12_345,
        .write_buffer_size = 2_345,
        .max_response_bytes = 65_535,
        .io_concurrent_limit = 23,
        .keep_alive = true,
        .max_requests_per_connection = 17,
    };
    var driver: HttpFrameDriver = undefined;
    try driver.initAsyncInPlace(std.testing.allocator, .{
        .async_send_worker_count = 1,
        .isolated_worker_executors = true,
        .isolated_worker_executor_config = executor_config,
    }, unused.iface(), io_impl.io());
    defer driver.deinit();

    try std.testing.expectEqual(@as(usize, 1), driver.isolated_executors.len);
    const actual = driver.isolated_executors[0].cfg;
    try std.testing.expectEqual(executor_config.read_buffer_size, actual.read_buffer_size);
    try std.testing.expectEqual(executor_config.write_buffer_size, actual.write_buffer_size);
    try std.testing.expectEqual(executor_config.max_response_bytes, actual.max_response_bytes);
    try std.testing.expectEqual(executor_config.io_concurrent_limit, actual.io_concurrent_limit);
    try std.testing.expectEqual(executor_config.keep_alive, actual.keep_alive);
    try std.testing.expectEqual(executor_config.max_requests_per_connection, actual.max_requests_per_connection);
}

test "http frame driver split shutdown wakes idle senders before deinit" {
    const UnusedExecutor = struct {
        fn iface(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedRequest;
        }
    };

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var unused = UnusedExecutor{};
    var driver: HttpFrameDriver = undefined;
    try driver.initAsyncInPlace(std.testing.allocator, .{
        .async_send_worker_count = 1,
    }, unused.iface(), io_impl.io());
    driver.beginShutdown();
    driver.beginShutdown();
    try std.testing.expect(driver.closing);
    driver.deinit();
}

test "http frame sender drains partial startup and releases private capacity" {
    const UnusedExecutor = struct {
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedRequest;
        }
    };
    const executor: common.RequestExecutor = .{
        .ptr = undefined,
        .vtable = &.{ .execute = UnusedExecutor.execute },
    };
    var shared = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer shared.deinit();
    var concurrency_failures: usize = 0;
    for (0..32) |fail_index| {
        // Idle senders do not allocate: future allocation/destruction and
        // startup rollback all happen on this test's owning task.
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var driver = HttpFrameDriver.init(failing.allocator(), .{ .async_send_worker_count = 2 }, executor, shared.io());
        defer driver.deinit();
        driver.startAsyncSender() catch |err| {
            switch (err) {
                error.OutOfMemory => {},
                error.ConcurrencyUnavailable => concurrency_failures += 1,
            }
            try std.testing.expectEqual(@as(usize, 0), driver.workers.len);
            try std.testing.expect(driver.sender_io == null);
            continue;
        };
        driver.stopAsyncSender();
        try std.testing.expectEqual(@as(usize, 0), driver.workers.len);
        try std.testing.expect(driver.sender_io == null);
        // Exercise failure on both first and later future allocations.
        try std.testing.expect(concurrency_failures >= 2);
        return;
    }
    return error.TestUnexpectedResult;
}

test "http frame sender borrows capacity and drains a refused partial startup" {
    var lane = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .limited(1) });
    defer lane.deinit();
    const Unused = struct {
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedRequest;
        }
        fn done() void {}
    };
    var driver = HttpFrameDriver.init(std.testing.allocator, .{ .sender_io = lane.io(), .async_send_worker_count = 2 }, .{
        .ptr = undefined,
        .vtable = &.{ .execute = Unused.execute },
    }, std.testing.io);
    defer driver.deinit();
    try std.testing.expectError(error.ConcurrencyUnavailable, driver.startAsyncSender());
    try std.testing.expectEqual(@as(usize, 0), driver.workers.len);
    try std.testing.expect(driver.sender_io == null);
    var probe = try lane.io().concurrent(Unused.done, .{});
    probe.await(lane.io());
}

test "http frame driver budgets in flight and failed frames and invalidates queued routes" {
    const BlockingFailure = struct {
        io: std.Io,
        mutex: std.Io.Mutex = .init,
        cond: std.Io.Condition = .init,
        allow: bool = false,
        calls: std.atomic.Value(usize) = .init(0),
        fn release(self: *@This()) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            self.allow = true;
            self.cond.broadcast(self.io);
        }
        fn execute(ptr: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            _ = self.calls.fetchAdd(1, .release);
            while (!self.allow) self.cond.waitUncancelable(self.io, &self.mutex);
            return error.ConnectionRefused;
        }
    };
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var executor = BlockingFailure{ .io = io };
    var driver: HttpFrameDriver = undefined;
    try driver.initAsyncInPlace(alloc, .{ .async_send_worker_count = 1, .async_send_retained_bytes_max = 256, .async_send_retained_bytes_max_per_peer = 100 }, .{ .ptr = &executor, .vtable = &.{ .execute = BlockingFailure.execute } }, io);
    defer driver.deinit();
    defer executor.release();
    var bytes = "payload".*;
    const req: raft_engine.runtime.frame_driver_iface.SendFrameRequest = .{ .peer_id = 2, .source_id = 1, .endpoint = .{ .protocol = .http1, .address = "http://old" }, .frame = .{ .bytes = &bytes, .media_type = "raft" }, .group_ids = &.{41} };
    const size = bytes.len + req.endpoint.address.len + req.frame.media_type.len + 8;
    try driver.frameDriver().sendFrame(req);
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (executor.calls.load(.acquire) == 0 and platform_time.monotonicNs() < deadline) try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.acquire));
    try driver.frameDriver().sendFrame(req);
    try driver.frameDriver().sendFrame(req);
    try std.testing.expectError(error.AsyncSendQueueFull, driver.frameDriver().sendFrame(req));
    driver.frameDriver().invalidateRoute(41, 2);
    try std.testing.expectEqual(@as(usize, 0), driver.metricsSnapshot().pending);
    try std.testing.expectEqual(3 * size, driver.metricsSnapshot().retained_bytes);
    // Invalidation transfers unsent frames back without releasing their budget.
    try std.testing.expectError(error.AsyncSendQueueFull, driver.frameDriver().sendFrame(req));
    for (0..2) |_| {
        var failed = driver.frameDriver().pollFailedFrame().?;
        defer failed.deinit();
        try std.testing.expectEqualStrings("payload", failed.frame.bytes);
        try std.testing.expectEqual(@as(?u64, 1), failed.source_id);
        try std.testing.expectEqual(@as(u32, 1), failed.attempt);
    }
    try std.testing.expectEqual(size, driver.metricsSnapshot().retained_bytes);
    executor.release();
    var completion: ?raft_engine.runtime.frame_driver_iface.FailedFrame = null;
    while (completion == null and platform_time.monotonicNs() < deadline) {
        completion = driver.frameDriver().pollFailedFrame();
        if (completion == null) try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(completion != null);
    completion.?.deinit();
    try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), driver.metricsSnapshot().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), driver.metricsSnapshot().retained_frames);
    driver.cfg.async_send_retained_bytes_max = size - 1;
    try std.testing.expectError(error.AsyncSendQueueFull, driver.frameDriver().sendFrame(req));
    try std.testing.expectEqual(@as(u64, 1), driver.metricsSnapshot().queue_full);
}
