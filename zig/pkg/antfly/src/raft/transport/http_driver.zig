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
const platform_time = @import("../../platform/time.zig");
const routes = @import("routes.zig");

pub const HttpDriverConfig = struct {
    request_timeout_ms: u32 = 5_000,
    max_batch_bytes: usize = common_http.default_max_request_bytes,
    async_send_queue_max: usize = 4096,
    async_send_queue_max_per_peer: usize = 256,
    async_send_immediate_retry_attempts: u32 = 1,
    async_send_max_attempts: u32 = 32,
    async_send_retry_base_ms: u64 = 50,
    async_send_retry_max_ms: u64 = 1_000,
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
    const QueuedFrame = struct {
        source_id: ?u64 = null,
        peer_id: u64,
        base_uri: []u8,
        body: []u8,
        content_type: []u8,
        attempts: u32 = 0,
        not_before_ms: u64 = 0,

        fn deinit(self: *QueuedFrame, alloc: std.mem.Allocator) void {
            alloc.free(self.base_uri);
            alloc.free(self.body);
            alloc.free(self.content_type);
            self.* = undefined;
        }
    };

    alloc: std.mem.Allocator,
    cfg: HttpDriverConfig,
    executor: common.RequestExecutor,
    io: std.Io,
    threads: []std.Thread = &.{},
    isolated_executors: []common_http.StdHttpExecutor = &.{},
    mutex: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    closing: bool = false,
    queue: std.ArrayListUnmanaged(QueuedFrame) = .empty,
    queue_head: usize = 0,
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
        self.in_flight_peers.deinit(self.alloc);
        self.mutex.unlock(self.io);
        self.* = undefined;
    }

    pub fn frameDriver(self: *HttpFrameDriver) raft_engine.runtime.FrameDriver {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_frame = sendFrame,
            },
        };
    }

    pub fn metricsSnapshot(self: *HttpFrameDriver) AsyncSendMetricsSnapshot {
        self.mutex.lockUncancelable(self.io);
        const pending = self.pendingQueueCountLocked();
        self.mutex.unlock(self.io);
        return .{
            .enqueued = self.metrics.enqueued.load(.monotonic),
            .failed = self.metrics.failed.load(.monotonic),
            .retried = self.metrics.retried.load(.monotonic),
            .dropped = self.metrics.dropped.load(.monotonic),
            .queue_full = self.metrics.queue_full.load(.monotonic),
            .peer_queue_full = self.metrics.peer_queue_full.load(.monotonic),
            .pending = pending,
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

    fn startAsyncSender(self: *HttpFrameDriver) !void {
        if (self.threads.len != 0) return;
        if (self.cfg.async_send_worker_count == 0) return error.InvalidAsyncSendWorkerCount;
        try self.in_flight_peers.ensureTotalCapacity(self.alloc, self.cfg.async_send_worker_count);
        if (self.cfg.isolated_worker_executors) {
            self.isolated_executors = try self.alloc.alloc(common_http.StdHttpExecutor, self.cfg.async_send_worker_count);
            for (self.isolated_executors) |*executor| {
                executor.initInPlace(self.alloc, self.cfg.isolated_worker_executor_config);
            }
        }
        errdefer self.deinitIsolatedExecutors();
        self.threads = try self.alloc.alloc(std.Thread, self.cfg.async_send_worker_count);
        var started: usize = 0;
        errdefer {
            self.mutex.lockUncancelable(self.io);
            self.closing = true;
            self.cond.broadcast(self.io);
            self.mutex.unlock(self.io);
            for (self.threads[0..started]) |thread| thread.join();
            self.alloc.free(self.threads);
            self.threads = &.{};
        }
        while (started < self.threads.len) : (started += 1) {
            self.threads[started] = try std.Thread.spawn(.{}, asyncSenderMain, .{ self, started });
        }
    }

    fn stopAsyncSender(self: *HttpFrameDriver) void {
        if (self.threads.len == 0) return;
        self.mutex.lockUncancelable(self.io);
        self.closing = true;
        self.cond.broadcast(self.io);
        self.mutex.unlock(self.io);
        for (self.threads) |thread| thread.join();
        self.alloc.free(self.threads);
        self.threads = &.{};
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
                const retrying = self.retryQueuedFrame(&owned, err);
                self.finishInFlightPeer(frame.peer_id);
                if (retrying) continue;
                owned.deinit(self.alloc);
                continue;
            };
            self.finishInFlightPeer(frame.peer_id);
            owned.deinit(self.alloc);
        }
    }

    fn finishInFlightPeer(self: *HttpFrameDriver, peer_id: u64) void {
        self.mutex.lockUncancelable(self.io);
        std.debug.assert(self.in_flight_peers.remove(peer_id));
        self.cond.broadcast(self.io);
        self.mutex.unlock(self.io);
    }

    fn retryQueuedFrame(self: *HttpFrameDriver, frame: *QueuedFrame, err: anyerror) bool {
        _ = self.metrics.failed.fetchAdd(1, .monotonic);
        if (err == error.BatchTooLarge) {
            _ = self.metrics.dropped.fetchAdd(1, .monotonic);
            std.log.warn("raft http async send dropping oversized frame peer_id={d} bytes={d} max_bytes={d}", .{
                frame.peer_id,
                frame.body.len,
                self.cfg.max_batch_bytes,
            });
            return false;
        }
        frame.attempts +|= 1;
        if (frame.attempts >= self.cfg.async_send_max_attempts) {
            _ = self.metrics.dropped.fetchAdd(1, .monotonic);
            std.log.warn("raft http async send dropping frame peer_id={d} attempts={d} err={}", .{
                frame.peer_id,
                frame.attempts,
                err,
            });
            return false;
        }
        const delay_ms = self.retryDelayMs(frame.*);
        frame.not_before_ms = nowMs() + delay_ms;
        if (frame.attempts == 1 or frame.attempts % 16 == 0) {
            std.log.debug("raft http async send failed peer_id={d} attempts={d} retry_delay_ms={d} err={}", .{
                frame.peer_id,
                frame.attempts,
                delay_ms,
                err,
            });
        }

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closing) return false;
        if (self.pendingQueueCountLocked() >= self.cfg.async_send_queue_max) {
            _ = self.metrics.queue_full.fetchAdd(1, .monotonic);
            _ = self.metrics.dropped.fetchAdd(1, .monotonic);
            return false;
        }
        if (self.pendingQueueCountForPeerLocked(frame.peer_id) >= self.cfg.async_send_queue_max_per_peer) {
            _ = self.metrics.peer_queue_full.fetchAdd(1, .monotonic);
            _ = self.metrics.dropped.fetchAdd(1, .monotonic);
            return false;
        }
        self.queue.append(self.alloc, frame.*) catch return false;
        _ = self.metrics.retried.fetchAdd(1, .monotonic);
        frame.* = undefined;
        self.cond.signal(self.io);
        return true;
    }

    fn enqueueFrame(self: *HttpFrameDriver, req: raft_engine.runtime.frame_driver_iface.SendFrameRequest) !void {
        if (self.threads.len == 0) {
            return try self.sendBatch(.{
                .source_id = req.source_id,
                .peer_id = req.peer_id,
                .base_uri = req.endpoint.address,
                .body = req.frame.bytes,
                .content_type = req.frame.media_type,
            });
        }

        var frame: QueuedFrame = .{
            .source_id = req.source_id,
            .peer_id = req.peer_id,
            .base_uri = try self.alloc.dupe(u8, req.endpoint.address),
            .body = try self.alloc.dupe(u8, req.frame.bytes),
            .content_type = try self.alloc.dupe(u8, req.frame.media_type),
        };
        errdefer frame.deinit(self.alloc);

        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.closing) return error.AsyncSenderClosed;
        if (self.pendingQueueCountLocked() >= self.cfg.async_send_queue_max) {
            _ = self.metrics.queue_full.fetchAdd(1, .monotonic);
            return error.AsyncSendQueueFull;
        }
        if (self.pendingQueueCountForPeerLocked(frame.peer_id) >= self.cfg.async_send_queue_max_per_peer) {
            _ = self.metrics.peer_queue_full.fetchAdd(1, .monotonic);
            return error.AsyncSendQueueFull;
        }
        try self.queue.append(self.alloc, frame);
        _ = self.metrics.enqueued.fetchAdd(1, .monotonic);
        self.cond.signal(self.io);
    }

    fn popQueuedFrame(self: *HttpFrameDriver) ?QueuedFrame {
        while (true) {
            self.mutex.lockUncancelable(self.io);
            if (self.closing) {
                self.mutex.unlock(self.io);
                return null;
            }
            if (self.popReadyFrameLocked(nowMs())) |frame| {
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
                self.io.sleep(std.Io.Duration.fromMilliseconds(@intCast(self.nextSleepMs())), .awake) catch {};
            }
        }
    }

    fn popReadyFrameLocked(self: *HttpFrameDriver, now_ms: u64) ?QueuedFrame {
        self.compactQueueIfNeededLocked();
        for (self.queue.items, 0..) |frame, index| {
            if (frame.not_before_ms > now_ms) continue;
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

    fn nextSleepMs(self: *HttpFrameDriver) u64 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const now_ms = nowMs();
        var next_ready_ms: ?u64 = null;
        for (self.queue.items[self.queue_head..]) |frame| {
            if (frame.not_before_ms <= now_ms) return 1;
            if (next_ready_ms == null or frame.not_before_ms < next_ready_ms.?) {
                next_ready_ms = frame.not_before_ms;
            }
        }
        const delay = if (next_ready_ms) |ready_ms| ready_ms -| now_ms else 25;
        return @max(@as(u64, 1), @min(@as(u64, 25), delay));
    }

    fn retryDelayMs(self: *const HttpFrameDriver, frame: QueuedFrame) u64 {
        if (frame.attempts <= self.cfg.async_send_immediate_retry_attempts) return 0;
        const retry_index = frame.attempts - self.cfg.async_send_immediate_retry_attempts;
        const shift: u6 = @intCast(@min(retry_index - 1, 6));
        const capped = @min(self.cfg.async_send_retry_max_ms, self.cfg.async_send_retry_base_ms << shift);
        if (capped <= 1) return capped;
        const low = capped - capped / 4;
        const span = capped - low + 1;
        return low + pseudoJitter(frame.peer_id, frame.attempts, nowMs()) % span;
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
        for (self.queue.items[self.queue_head..]) |*frame| frame.deinit(self.alloc);
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

fn pseudoJitter(peer_id: u64, attempts: u32, now_ms: u64) u64 {
    var x = peer_id ^ (@as(u64, attempts) << 32) ^ now_ms;
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return x;
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

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
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
    while (executor.callCount() < 2 and platform_time.monotonicNs() < deadline_ns) std.Thread.yield() catch {};
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
    try std.testing.expectEqual(executor_config.keep_alive, actual.keep_alive);
    try std.testing.expectEqual(executor_config.max_requests_per_connection, actual.max_requests_per_connection);
}
