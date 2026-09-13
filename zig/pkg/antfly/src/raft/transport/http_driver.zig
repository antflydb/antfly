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
    const PeerState = struct {
        bytes: usize = 0,
        frames: usize = 0,
        queue: std.ArrayListUnmanaged(QueuedFrame) = .empty,
        head: usize = 0,
        in_flight: bool = false,
        ready: bool = false,
        previous: ?u64 = null,
        next: ?u64 = null,
    };
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
    ready_head: ?u64 = null,
    ready_tail: ?u64 = null,
    pending: usize = 0,
    failed: std.ArrayListUnmanaged(QueuedFrame) = .empty,
    failed_head: usize = 0,
    retained: Retention = .{},
    peers: std.AutoHashMapUnmanaged(u64, PeerState) = .empty,
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
        self.failed.deinit(self.alloc);
        self.peers.deinit(self.alloc);
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
                self.completePeerLocked(frame.peer_id);
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
            self.completePeerLocked(frame.peer_id);
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
        const peer = self.peers.getPtr(frame.peer_id).?;
        peer.bytes -= size;
        peer.frames -= 1;
        if (peer.frames == 0) {
            std.debug.assert(!peer.ready and !peer.in_flight and peer.head == peer.queue.items.len);
            peer.queue.deinit(self.alloc);
            _ = self.peers.remove(frame.peer_id);
        }
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
        const peer = self.peers.getPtr(peer_id) orelse return;
        var kept: usize = 0;
        self.removeReadyLocked(peer_id);
        // Pin this map entry while OOM in failure publication may release its
        // last real frame. No allocation is required to invalidate a route.
        peer.frames += 1;
        self.retained.frames += 1;
        for (peer.queue.items[peer.head..]) |frame| {
            if (std.mem.indexOfScalar(u64, frame.group_ids, group_id) != null) {
                self.pending -= 1;
                self.publishFailureLocked(frame);
            } else {
                peer.queue.items[kept] = frame;
                kept += 1;
            }
        }
        peer.queue.items.len = kept;
        peer.head = 0;
        peer.frames -= 1;
        self.retained.frames -= 1;
        if (peer.frames == 0) {
            peer.queue.deinit(self.alloc);
            _ = self.peers.remove(peer_id);
        } else self.makeReadyLocked(peer_id);
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
        const peer = self.peers.get(req.peer_id) orelse PeerState{};
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
        const entry = try self.peers.getOrPut(self.alloc, req.peer_id);
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
            if (entry.value_ptr.frames == 0) {
                entry.value_ptr.queue.deinit(self.alloc);
                _ = self.peers.remove(req.peer_id);
            }
        }
        const address = try self.alloc.dupe(u8, req.endpoint.address);
        errdefer self.alloc.free(address);
        const body = try self.alloc.dupe(u8, req.frame.bytes);
        errdefer self.alloc.free(body);
        const content_type = try self.alloc.dupe(u8, req.frame.media_type);
        errdefer self.alloc.free(content_type);
        const group_ids = try self.alloc.dupe(u64, req.group_ids);
        errdefer self.alloc.free(group_ids);
        const queue = &entry.value_ptr.queue;
        if (entry.value_ptr.head > 0 and entry.value_ptr.head * 2 >= queue.items.len) {
            const remaining = queue.items.len - entry.value_ptr.head;
            std.mem.copyForwards(QueuedFrame, queue.items[0..remaining], queue.items[entry.value_ptr.head..]);
            queue.items.len = remaining;
            entry.value_ptr.head = 0;
        }
        try queue.append(self.alloc, .{ .source_id = req.source_id, .peer_id = req.peer_id, .base_uri = address, .body = body, .content_type = content_type, .group_ids = group_ids, .attempt = req.attempt });
        self.pending += 1;
        self.makeReadyLocked(req.peer_id);
        _ = self.metrics.enqueued.fetchAdd(1, .monotonic);
        if (req.attempt > 1) _ = self.metrics.retried.fetchAdd(1, .monotonic);
        self.cond.signal(self.io);
    }

    fn popQueuedFrame(self: *HttpFrameDriver) ?QueuedFrame {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        while (!self.closing) {
            if (self.popReadyFrameLocked()) |frame| return frame;
            self.cond.waitUncancelable(self.io, &self.mutex);
        }
        return null;
    }

    fn makeReadyLocked(self: *HttpFrameDriver, id: u64) void {
        const peer = self.peers.getPtr(id).?;
        if (peer.ready or peer.in_flight or peer.head == peer.queue.items.len) return;
        peer.ready = true;
        peer.previous = self.ready_tail;
        peer.next = null;
        if (self.ready_tail) |tail| self.peers.getPtr(tail).?.next = id else self.ready_head = id;
        self.ready_tail = id;
    }

    fn removeReadyLocked(self: *HttpFrameDriver, id: u64) void {
        const peer = self.peers.getPtr(id).?;
        if (!peer.ready) return;
        if (peer.previous) |previous| self.peers.getPtr(previous).?.next = peer.next else self.ready_head = peer.next;
        if (peer.next) |next| self.peers.getPtr(next).?.previous = peer.previous else self.ready_tail = peer.previous;
        peer.ready = false;
        peer.previous = null;
        peer.next = null;
    }

    fn completePeerLocked(self: *HttpFrameDriver, id: u64) void {
        const peer = self.peers.getPtr(id).?;
        std.debug.assert(peer.in_flight);
        peer.in_flight = false;
        self.makeReadyLocked(id);
    }

    fn popReadyFrameLocked(self: *HttpFrameDriver) ?QueuedFrame {
        const id = self.ready_head orelse return null;
        self.removeReadyLocked(id);
        const peer = self.peers.getPtr(id).?;
        const frame = peer.queue.items[peer.head];
        peer.head += 1;
        peer.in_flight = true;
        self.pending -= 1;
        return frame;
    }

    fn pendingQueueCountLocked(self: *const HttpFrameDriver) usize {
        return self.pending;
    }

    fn pendingQueueCountForPeerLocked(self: *const HttpFrameDriver, peer_id: u64) usize {
        const peer = self.peers.get(peer_id) orelse return 0;
        return peer.queue.items.len - peer.head;
    }

    fn clearQueueLocked(self: *HttpFrameDriver) void {
        // Workers have joined. No map mutation while walking peer queues.
        var peers = self.peers.valueIterator();
        while (peers.next()) |peer| {
            for (peer.queue.items[peer.head..]) |*frame| frame.deinit(self.alloc);
            peer.queue.deinit(self.alloc);
        }
        for (self.failed.items[self.failed_head..]) |*frame| {
            frame.deinit(self.alloc);
        }
        self.failed.clearRetainingCapacity();
        self.failed_head = 0;
        self.peers.clearRetainingCapacity();
        self.retained = .{};
        self.pending = 0;
        self.ready_head = null;
        self.ready_tail = null;
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
    const AdmissionLane = @import("test_admission_lane.zig");
    const Unused = struct {
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedRequest;
        }
        fn done() void {}
    };
    // Await guarantees task completion, not immediate reuse of a Threaded
    // concurrency slot: its busy count is retired after waking the awaiter.
    // Inject admission refusal instead of racing that executor bookkeeping.
    // Check every rollback prefix, including refusal before the first worker.
    for (0..4) |refuse_after| {
        var lane = AdmissionLane.init(std.testing.allocator, refuse_after);
        defer lane.deinit();
        const io = lane.io();
        {
            var driver = HttpFrameDriver.init(std.testing.allocator, .{ .sender_io = io, .async_send_worker_count = 4 }, .{
                .ptr = undefined,
                .vtable = &.{ .execute = Unused.execute },
            }, std.testing.io);
            defer driver.deinit();
            try std.testing.expectError(error.ConcurrencyUnavailable, driver.startAsyncSender());
            try std.testing.expectEqual(refuse_after, lane.admitted);
            try std.testing.expectEqual(lane.admitted, lane.awaited);
            try std.testing.expectEqual(@as(usize, 0), driver.workers.len);
            try std.testing.expect(driver.sender_io == null);
            try std.testing.expect(driver.closing);
        }
        // Destroying the borrower must leave the owner's executor usable.
        // Admission now succeeds independently of worker-retirement timing.
        lane.refuse_after = null;
        var probe = try io.concurrent(Unused.done, .{});
        probe.await(io);
        try std.testing.expectEqual(refuse_after + 1, lane.admitted);
        try std.testing.expectEqual(lane.admitted, lane.awaited);
    }
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

test "http frame driver ready peers drain fairly and retain FIFO across backlog" {
    const Fake = struct {
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedSend;
        }
    };
    var context: u8 = 0;
    var driver = HttpFrameDriver.init(std.testing.allocator, .{}, .{ .ptr = &context, .vtable = &.{ .execute = Fake.execute } }, std.testing.io);
    defer driver.deinit();
    var workers: [1]std.Io.Future(void) = undefined;
    driver.workers = &workers; // Exercise queue ownership without starting I/O.
    defer driver.workers = &.{};
    for (0..3) |sequence| for (1..5) |peer| {
        var body = [_]u8{@intCast(sequence)};
        try driver.enqueueFrame(.{ .peer_id = peer, .endpoint = .{ .protocol = .http1, .address = "http://peer" }, .frame = .{ .bytes = &body, .media_type = "raft" } });
    };
    for (0..3) |sequence| for (1..5) |peer| {
        var frame = driver.popReadyFrameLocked().?;
        try std.testing.expectEqual(peer, frame.peer_id);
        try std.testing.expectEqual(@as(u8, @intCast(sequence)), frame.body[0]);
        driver.completePeerLocked(peer);
        driver.releaseRetentionLocked(frame);
        frame.deinit(std.testing.allocator);
    };
    try std.testing.expect(driver.popReadyFrameLocked() == null);
    try std.testing.expectEqual(@as(usize, 0), driver.retained.frames);
}

test "http frame driver scheduler workload benchmark" {
    if (std.c.getenv("ANTFLY_HTTP_SCHEDULER_BENCH") == null) return;
    const Fake = struct {
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return error.UnexpectedSend;
        }
    };
    const alloc = std.heap.c_allocator;
    for ([_]usize{ 256, 1024, 4096 }) |count| {
        for ([_]bool{ false, true }) |ready_peers| {
            var samples: [9]u64 = undefined;
            for (&samples) |*sample| {
                var context: u8 = 0;
                var driver = HttpFrameDriver.init(alloc, .{}, .{ .ptr = &context, .vtable = &.{ .execute = Fake.execute } }, std.testing.io);
                defer driver.deinit();
                var workers: [1]std.Io.Future(void) = undefined;
                driver.workers = &workers;
                defer driver.workers = &.{};
                for (0..count) |i| {
                    var body = [_]u8{@truncate(i)};
                    try driver.enqueueFrame(.{ .peer_id = i % 16, .endpoint = .{ .protocol = .http1, .address = "http://peer" }, .frame = .{ .bytes = &body, .media_type = "raft" } });
                }
                // Old global FIFO reproduced with the same owned frames. No
                // network, allocator setup or payload copies are timed.
                var old = std.ArrayListUnmanaged(HttpFrameDriver.QueuedFrame).empty;
                defer old.deinit(alloc);
                if (!ready_peers) {
                    while (driver.popReadyFrameLocked()) |frame| {
                        try old.append(alloc, frame);
                        driver.completePeerLocked(frame.peer_id);
                    }
                }
                const start = platform_time.monotonicNs();
                var drained: usize = 0;
                while (drained < count) : (drained += 1) {
                    var frame = if (ready_peers) driver.popReadyFrameLocked().? else old.orderedRemove(0);
                    if (ready_peers) driver.completePeerLocked(frame.peer_id);
                    driver.releaseRetentionLocked(frame);
                    frame.deinit(alloc);
                }
                sample.* = platform_time.monotonicNs() - start;
                try std.testing.expectEqual(@as(usize, 0), driver.retained.frames);
            }
            std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
            std.debug.print("HTTP_SCHEDULER_BENCH frames={d} ready_peers={} p50_ms={d:.6}\n", .{ count, ready_peers, @as(f64, @floatFromInt(samples[4])) / 1e6 });
        }
    }
}
