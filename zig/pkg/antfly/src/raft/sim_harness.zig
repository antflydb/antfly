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
const catalog = @import("catalog.zig");
const host = @import("host.zig");
const managed_host = @import("managed_host.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_apply = @import("metadata_apply.zig");
const peer_resolver = @import("peer_resolver.zig");
const read_gate = @import("read_gate.zig");
const runtime_loop = @import("runtime_loop.zig");
const service = @import("service.zig");
const db_enrichment_executor = @import("db_enrichment_executor.zig");
const db_enrichment_runtime_factory = @import("db_enrichment_runtime_factory.zig");
const enrichment_runtime = @import("enrichment_runtime.zig");
const db_types = @import("../storage/db/types.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const feature_reads = @import("feature_reads.zig");
const transport = @import("transport/mod.zig");
const transition_checker = @import("transition_checker.zig");
const transition_runtime_mod = @import("transition_runtime.zig");
const raft_state_machine = @import("state_machine/mod.zig");
const raft_engine = @import("raft_engine");
const data_mod = @import("../data/mod.zig");

pub const ManagedHostSimulationConfig = struct {
    host: managed_host.ManagedHostConfig,
    service: service.ManagedServiceConfig = .{},
    runtime: runtime_loop.RuntimeLoopConfig = .{},
};

pub const ManagedHostSimulationDeps = struct {
    host: managed_host.ManagedHostDeps = .{},
    service: service.ManagedServiceDeps = .{},
};

pub const ManagedHttpHostSimulationConfig = struct {
    host: managed_host.ManagedHttpHostConfig,
    service: service.ManagedServiceConfig = .{},
    runtime: runtime_loop.RuntimeLoopConfig = .{},
};

pub const ManagedHttpHostSimulationDeps = struct {
    host: managed_host.ManagedHttpHostDeps = .{},
    service: service.ManagedServiceDeps = .{},
};

pub const DelayingRequestExecutor = struct {
    const DelayMode = enum {
        virtual,
        wall_clock,
    };

    alloc: std.mem.Allocator,
    delay_ns: u64,
    virtual_elapsed_ns: u64 = 0,
    request_count: u64 = 0,
    mode: DelayMode = .virtual,
    inner: transport.StdHttpExecutor,

    pub fn init(alloc: std.mem.Allocator, delay_ns: u64) DelayingRequestExecutor {
        return initWithMode(alloc, delay_ns, .virtual);
    }

    pub fn initWallClock(alloc: std.mem.Allocator, delay_ns: u64) DelayingRequestExecutor {
        return initWithMode(alloc, delay_ns, .wall_clock);
    }

    fn initWithMode(alloc: std.mem.Allocator, delay_ns: u64, mode: DelayMode) DelayingRequestExecutor {
        var inner: transport.StdHttpExecutor = undefined;
        inner.initInPlace(alloc, .{});
        return .{
            .alloc = alloc,
            .delay_ns = delay_ns,
            .mode = mode,
            .inner = inner,
        };
    }

    pub fn deinit(self: *DelayingRequestExecutor) void {
        self.inner.deinit();
        self.* = undefined;
    }

    pub fn executor(self: *DelayingRequestExecutor) transport.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: transport.HttpRequest) !transport.HttpResponse {
        const self: *DelayingRequestExecutor = @ptrCast(@alignCast(ptr));
        self.request_count +|= 1;
        self.virtual_elapsed_ns +|= self.delay_ns;
        if (self.mode == .wall_clock) {
            try sleepForNanos(self.delay_ns);
        }
        return try self.inner.executor().execute(alloc, req);
    }
};

pub const VirtualHttpNetwork = struct {
    pub const DeliveryMode = enum {
        immediate,
        queued,
    };

    pub const ReleasePolicy = enum {
        fifo,
        random_by_seed,
    };

    pub const RandomDropConfig = struct {
        seed: u64,
        numerator: u32,
        denominator: u32,
    };

    pub const Link = struct {
        source_id: u64,
        target_id: u64,
    };

    const QueuedRequest = struct {
        due_tick: u64,
        sequence: u64,
        base_uri: []u8,
        request: transport.HttpRequest,

        fn deinit(self: *QueuedRequest, alloc: std.mem.Allocator) void {
            alloc.free(self.base_uri);
            alloc.free(@constCast(self.request.uri));
            for (self.request.headers) |header| {
                alloc.free(@constCast(header.name));
                alloc.free(@constCast(header.value));
            }
            if (self.request.headers.len > 0) alloc.free(@constCast(self.request.headers));
            if (self.request.authorization) |authorization| alloc.free(@constCast(authorization));
            if (self.request.content_type) |content_type| alloc.free(@constCast(content_type));
            if (self.request.body.len > 0) alloc.free(@constCast(self.request.body));
            self.* = undefined;
        }
    };

    alloc: std.mem.Allocator,
    queue_alloc: std.mem.Allocator = std.heap.page_allocator,
    routes: std.StringHashMapUnmanaged(transport.RequestExecutor) = .empty,
    partitioned_nodes: std.AutoHashMapUnmanaged(u64, void) = .empty,
    partitioned_links: std.AutoHashMapUnmanaged(Link, void) = .empty,
    queued_requests: std.ArrayListUnmanaged(QueuedRequest) = .empty,
    random_drop_prng: std.Random.DefaultPrng = std.Random.DefaultPrng.init(0),
    release_prng: std.Random.DefaultPrng = std.Random.DefaultPrng.init(0),
    release_policy: ReleasePolicy = .fifo,
    random_drop_numerator: u32 = 0,
    random_drop_denominator: u32 = 0,
    drop_next_count: u32 = 0,
    duplicate_next_count: u32 = 0,
    delay_next_ticks_value: u64 = 0,
    delivery_mode: DeliveryMode = .immediate,
    virtual_tick: u64 = 0,
    next_sequence: u64 = 0,
    request_count: u64 = 0,
    delivered_count: u64 = 0,
    dropped_count: u64 = 0,
    duplicated_count: u64 = 0,
    delayed_count: u64 = 0,

    pub fn init(alloc: std.mem.Allocator) VirtualHttpNetwork {
        return .{
            .alloc = alloc,
            // This queue is synthetic transport state. Keep the external request
            // allocator stable for request execution / response teardown, but use
            // page allocation for queued request buffers to avoid debug allocator
            // stack-trace capture on every enqueue.
            .queue_alloc = std.heap.page_allocator,
        };
    }

    pub fn deinit(self: *VirtualHttpNetwork) void {
        for (self.queued_requests.items) |*queued| queued.deinit(self.queue_alloc);
        self.queued_requests.deinit(self.queue_alloc);
        var key_it = self.routes.keyIterator();
        while (key_it.next()) |key| self.alloc.free(@constCast(key.*));
        self.routes.deinit(self.alloc);
        self.partitioned_nodes.deinit(self.alloc);
        self.partitioned_links.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn executor(self: *VirtualHttpNetwork) transport.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn baseUri(alloc: std.mem.Allocator, node_id: u64) ![]u8 {
        return try std.fmt.allocPrint(alloc, "sim://raft-node/{d}", .{node_id});
    }

    pub fn registerNode(self: *VirtualHttpNetwork, node_id: u64, executor_: transport.RequestExecutor) !void {
        const uri = try baseUri(self.alloc, node_id);
        errdefer self.alloc.free(uri);
        const gop = try self.routes.getOrPut(self.alloc, uri);
        if (gop.found_existing) {
            self.alloc.free(uri);
        } else {
            gop.key_ptr.* = uri;
        }
        gop.value_ptr.* = executor_;
    }

    pub fn useQueuedDelivery(self: *VirtualHttpNetwork) void {
        self.delivery_mode = .queued;
    }

    pub fn useImmediateDelivery(self: *VirtualHttpNetwork) void {
        self.delivery_mode = .immediate;
    }

    pub fn queuedCount(self: *const VirtualHttpNetwork) usize {
        return self.queued_requests.items.len;
    }

    pub fn useFifoRelease(self: *VirtualHttpNetwork) void {
        self.release_policy = .fifo;
    }

    pub fn useRandomRelease(self: *VirtualHttpNetwork, seed: u64) void {
        self.release_prng = std.Random.DefaultPrng.init(seed);
        self.release_policy = .random_by_seed;
    }

    pub fn partitionNode(self: *VirtualHttpNetwork, node_id: u64) !void {
        try self.partitioned_nodes.put(self.alloc, node_id, {});
    }

    pub fn healNode(self: *VirtualHttpNetwork, node_id: u64) void {
        _ = self.partitioned_nodes.remove(node_id);
    }

    pub fn healAll(self: *VirtualHttpNetwork) void {
        self.partitioned_nodes.clearRetainingCapacity();
        self.partitioned_links.clearRetainingCapacity();
    }

    pub fn isPartitioned(self: *const VirtualHttpNetwork, node_id: u64) bool {
        return self.partitioned_nodes.contains(node_id);
    }

    pub fn partitionLink(self: *VirtualHttpNetwork, link: Link) !void {
        try self.partitioned_links.put(self.alloc, link, {});
    }

    pub fn healLink(self: *VirtualHttpNetwork, link: Link) void {
        _ = self.partitioned_links.remove(link);
    }

    pub fn isLinkPartitioned(self: *const VirtualHttpNetwork, link: Link) bool {
        return self.partitioned_links.contains(link);
    }

    pub fn dropNext(self: *VirtualHttpNetwork) void {
        self.drop_next_count +|= 1;
    }

    pub fn duplicateNext(self: *VirtualHttpNetwork) void {
        self.duplicate_next_count +|= 1;
    }

    pub fn delayNextTicks(self: *VirtualHttpNetwork, ticks: u64) void {
        self.delay_next_ticks_value = ticks;
    }

    pub fn configureRandomDrop(self: *VirtualHttpNetwork, cfg: RandomDropConfig) !void {
        if (cfg.denominator == 0 or cfg.numerator > cfg.denominator) return error.InvalidVirtualRandomDropConfig;
        self.random_drop_prng = std.Random.DefaultPrng.init(cfg.seed);
        self.random_drop_numerator = cfg.numerator;
        self.random_drop_denominator = cfg.denominator;
    }

    pub fn clearRandomDrop(self: *VirtualHttpNetwork) void {
        self.random_drop_numerator = 0;
        self.random_drop_denominator = 0;
    }

    pub fn clearOneShotFaults(self: *VirtualHttpNetwork) void {
        self.drop_next_count = 0;
        self.duplicate_next_count = 0;
        self.delay_next_ticks_value = 0;
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: transport.HttpRequest) !transport.HttpResponse {
        const self: *VirtualHttpNetwork = @ptrCast(@alignCast(ptr));
        self.request_count +|= 1;
        const route = splitVirtualUri(req.uri) orelse {
            self.dropped_count +|= 1;
            return error.UnknownVirtualRoute;
        };
        if (self.partitioned_nodes.contains(route.node_id)) {
            self.dropped_count +|= 1;
            return error.VirtualNetworkPartitioned;
        }
        if (req.source_node_id) |source_id| {
            if (self.partitioned_links.contains(.{ .source_id = source_id, .target_id = route.node_id })) {
                self.dropped_count +|= 1;
                return error.VirtualNetworkPartitioned;
            }
        }
        const target = self.routes.get(route.base_uri) orelse {
            self.dropped_count +|= 1;
            return error.UnknownVirtualRoute;
        };
        if (self.random_drop_denominator > 0 and self.random_drop_numerator > 0) {
            const draw = self.random_drop_prng.random().intRangeLessThan(u32, 0, self.random_drop_denominator);
            if (draw < self.random_drop_numerator) {
                self.dropped_count +|= 1;
                return error.VirtualNetworkDropped;
            }
        }
        if (self.drop_next_count > 0) {
            self.drop_next_count -= 1;
            self.dropped_count +|= 1;
            return error.VirtualNetworkDropped;
        }
        var delay_ticks: u64 = 0;
        if (self.delay_next_ticks_value > 0) {
            delay_ticks = self.delay_next_ticks_value;
            self.delay_next_ticks_value = 0;
            self.delayed_count +|= 1;
        }
        var forwarded = req;
        forwarded.uri = route.path;
        const should_queue = self.delivery_mode == .queued and req.method != .GET;
        if (should_queue) {
            try self.enqueue(route.base_uri, forwarded, delay_ticks);
            if (self.duplicate_next_count > 0) {
                self.duplicate_next_count -= 1;
                self.duplicated_count +|= 1;
                try self.enqueue(route.base_uri, forwarded, delay_ticks);
            }
            return .{ .status = 202 };
        }
        self.virtual_tick +|= delay_ticks;
        var response = try target.execute(alloc, forwarded);
        errdefer response.deinit(alloc);
        self.delivered_count +|= 1;
        if (self.duplicate_next_count > 0) {
            self.duplicate_next_count -= 1;
            self.duplicated_count +|= 1;
            var duplicate_response = try target.execute(alloc, forwarded);
            duplicate_response.deinit(alloc);
            self.delivered_count +|= 1;
        }
        return response;
    }

    pub fn advanceTicks(self: *VirtualHttpNetwork, ticks: u64) !usize {
        self.virtual_tick +|= ticks;
        return try self.drainDue(null);
    }

    pub fn drainDue(self: *VirtualHttpNetwork, max_events: ?usize) !usize {
        var delivered: usize = 0;
        while (self.nextDueIndex()) |index| {
            if (max_events) |limit| {
                if (delivered >= limit) break;
            }
            var queued = self.queued_requests.orderedRemove(index);
            defer queued.deinit(self.queue_alloc);
            try self.deliverQueued(&queued);
            delivered += 1;
        }
        return delivered;
    }

    pub fn runUntilIdle(self: *VirtualHttpNetwork) !usize {
        var delivered: usize = 0;
        while (self.queued_requests.items.len > 0) {
            const next_index = self.nextQueuedIndex() orelse break;
            const due_tick = self.queued_requests.items[next_index].due_tick;
            if (due_tick > self.virtual_tick) self.virtual_tick = due_tick;
            delivered += try self.drainDue(null);
        }
        return delivered;
    }

    fn enqueue(self: *VirtualHttpNetwork, base_uri: []const u8, request: transport.HttpRequest, delay_ticks: u64) !void {
        const owned_base_uri = try self.queue_alloc.dupe(u8, base_uri);
        errdefer self.queue_alloc.free(owned_base_uri);
        const owned_uri = try self.queue_alloc.dupe(u8, request.uri);
        errdefer self.queue_alloc.free(owned_uri);
        const owned_headers = try self.queue_alloc.alloc(transport.RequestHeader, request.headers.len);
        var owned_header_count: usize = 0;
        errdefer {
            for (owned_headers[0..owned_header_count]) |header| {
                self.queue_alloc.free(header.name);
                self.queue_alloc.free(header.value);
            }
            if (owned_headers.len > 0) self.queue_alloc.free(owned_headers);
        }
        for (owned_headers, request.headers) |*out, header| {
            const name = try self.queue_alloc.dupe(u8, header.name);
            const value = self.queue_alloc.dupe(u8, header.value) catch |err| {
                self.queue_alloc.free(name);
                return err;
            };
            out.* = .{ .name = name, .value = value };
            owned_header_count += 1;
        }
        const owned_authorization = if (request.authorization) |authorization| try self.queue_alloc.dupe(u8, authorization) else null;
        errdefer if (owned_authorization) |authorization| self.queue_alloc.free(authorization);
        const owned_content_type = if (request.content_type) |content_type| try self.queue_alloc.dupe(u8, content_type) else null;
        errdefer if (owned_content_type) |content_type| self.queue_alloc.free(content_type);
        const owned_body = try self.queue_alloc.dupe(u8, request.body);
        errdefer if (owned_body.len > 0) self.queue_alloc.free(owned_body);

        try self.queued_requests.append(self.queue_alloc, .{
            .due_tick = self.virtual_tick +| delay_ticks,
            .sequence = self.next_sequence,
            .base_uri = owned_base_uri,
            .request = .{
                .method = request.method,
                .uri = owned_uri,
                .headers = owned_headers,
                .source_node_id = request.source_node_id,
                .authorization = owned_authorization,
                .content_type = owned_content_type,
                .body = owned_body,
            },
        });
        self.next_sequence +|= 1;
    }

    fn deliverQueued(self: *VirtualHttpNetwork, queued: *const QueuedRequest) !void {
        const split = splitVirtualUri(queued.base_uri) orelse {
            self.dropped_count +|= 1;
            return;
        };
        if (self.partitioned_nodes.contains(split.node_id)) {
            self.dropped_count +|= 1;
            return;
        }
        if (queued.request.source_node_id) |source_id| {
            if (self.partitioned_links.contains(.{ .source_id = source_id, .target_id = split.node_id })) {
                self.dropped_count +|= 1;
                return;
            }
        }
        const target = self.routes.get(queued.base_uri) orelse {
            self.dropped_count +|= 1;
            return;
        };
        var response = try target.execute(self.alloc, queued.request);
        response.deinit(self.alloc);
        self.delivered_count +|= 1;
    }

    fn nextDueIndex(self: *VirtualHttpNetwork) ?usize {
        if (self.release_policy == .random_by_seed) return self.nextRandomDueIndex();
        return self.nextFifoDueIndex();
    }

    fn nextFifoDueIndex(self: *const VirtualHttpNetwork) ?usize {
        var best_index: ?usize = null;
        for (self.queued_requests.items, 0..) |queued, index| {
            if (queued.due_tick > self.virtual_tick) continue;
            const best = best_index orelse {
                best_index = index;
                continue;
            };
            const best_queued = self.queued_requests.items[best];
            if (queued.due_tick < best_queued.due_tick or
                (queued.due_tick == best_queued.due_tick and queued.sequence < best_queued.sequence))
            {
                best_index = index;
            }
        }
        return best_index;
    }

    fn nextRandomDueIndex(self: *VirtualHttpNetwork) ?usize {
        var due_count: u32 = 0;
        for (self.queued_requests.items) |queued| {
            if (queued.due_tick <= self.virtual_tick) due_count += 1;
        }
        if (due_count == 0) return null;

        const chosen = self.release_prng.random().intRangeLessThan(u32, 0, due_count);
        var seen: u32 = 0;
        for (self.queued_requests.items, 0..) |queued, index| {
            if (queued.due_tick > self.virtual_tick) continue;
            if (seen == chosen) return index;
            seen += 1;
        }
        unreachable;
    }

    fn nextQueuedIndex(self: *const VirtualHttpNetwork) ?usize {
        var best_index: ?usize = null;
        for (self.queued_requests.items, 0..) |queued, index| {
            const best = best_index orelse {
                best_index = index;
                continue;
            };
            const best_queued = self.queued_requests.items[best];
            if (queued.due_tick < best_queued.due_tick or
                (queued.due_tick == best_queued.due_tick and queued.sequence < best_queued.sequence))
            {
                best_index = index;
            }
        }
        return best_index;
    }

    const SplitUri = struct {
        node_id: u64,
        base_uri: []const u8,
        path: []const u8,
    };

    fn splitVirtualUri(uri: []const u8) ?SplitUri {
        const scheme = "sim://";
        if (!std.mem.startsWith(u8, uri, scheme)) return null;
        const host_start = scheme.len;
        const node_prefix = "raft-node/";
        if (!std.mem.startsWith(u8, uri[host_start..], node_prefix)) return null;
        const node_start = host_start + node_prefix.len;
        const path_sep = std.mem.indexOfScalarPos(u8, uri, node_start, '/') orelse uri.len;
        if (path_sep == node_start) return null;
        const node_id = std.fmt.parseInt(u64, uri[node_start..path_sep], 10) catch return null;
        return .{
            .node_id = node_id,
            .base_uri = uri[0..path_sep],
            .path = if (path_sep < uri.len) uri[path_sep..] else "/",
        };
    }
};

test "virtual http network can partition and heal target nodes" {
    const RecordingExecutor = struct {
        count: u64 = 0,
        last_uri: []const u8 = "",

        fn executor(self: *@This()) transport.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: transport.HttpRequest) !transport.HttpResponse {
            _ = alloc;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count +|= 1;
            self.last_uri = req.uri;
            return .{ .status = 200 };
        }
    };

    var network = VirtualHttpNetwork.init(std.testing.allocator);
    defer network.deinit();
    var target = RecordingExecutor{};
    try network.registerNode(7, target.executor());

    const executor = network.executor();
    var response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 1), target.count);
    try std.testing.expectEqualStrings("/raft/v1/frame", target.last_uri);

    try network.partitionNode(7);
    try std.testing.expect(network.isPartitioned(7));
    try std.testing.expectError(error.VirtualNetworkPartitioned, executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    }));
    try std.testing.expectEqual(@as(u64, 1), network.dropped_count);

    network.healNode(7);
    try std.testing.expect(!network.isPartitioned(7));
    var healed_response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7",
    });
    defer healed_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 2), target.count);
    try std.testing.expectEqualStrings("/", target.last_uri);

    try network.partitionLink(.{ .source_id = 1, .target_id = 7 });
    try std.testing.expect(network.isLinkPartitioned(.{ .source_id = 1, .target_id = 7 }));
    try std.testing.expectError(error.VirtualNetworkPartitioned, executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
        .source_node_id = 1,
    }));
    network.healLink(.{ .source_id = 1, .target_id = 7 });
    try std.testing.expect(!network.isLinkPartitioned(.{ .source_id = 1, .target_id = 7 }));

    network.dropNext();
    try std.testing.expectError(error.VirtualNetworkDropped, executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    }));
    try std.testing.expectEqual(@as(u64, 2), target.count);
    try std.testing.expectEqual(@as(u64, 3), network.dropped_count);

    network.delayNextTicks(9);
    var delayed_response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    });
    defer delayed_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 9), network.virtual_tick);
    try std.testing.expectEqual(@as(u64, 1), network.delayed_count);
    try std.testing.expectEqual(@as(u64, 3), target.count);

    network.duplicateNext();
    var duplicated_response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    });
    defer duplicated_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 1), network.duplicated_count);
    try std.testing.expectEqual(@as(u64, 5), target.count);

    try network.configureRandomDrop(.{ .seed = 0xA17F_7001, .numerator = 1, .denominator = 1 });
    try std.testing.expectError(error.VirtualNetworkDropped, executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    }));
    try std.testing.expectEqual(@as(u64, 4), network.dropped_count);
    try std.testing.expectEqual(@as(u64, 5), target.count);
    network.clearRandomDrop();
    var after_random_drop = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    });
    defer after_random_drop.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 6), target.count);

    network.useQueuedDelivery();
    network.delayNextTicks(3);
    var queued_response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/frame",
    });
    defer queued_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), queued_response.status);
    try std.testing.expectEqual(@as(usize, 1), network.queuedCount());
    try std.testing.expectEqual(@as(u64, 6), target.count);
    try std.testing.expectEqual(@as(usize, 0), try network.advanceTicks(2));
    try std.testing.expectEqual(@as(u64, 6), target.count);
    try std.testing.expectEqual(@as(usize, 1), try network.advanceTicks(1));
    try std.testing.expectEqual(@as(usize, 0), network.queuedCount());
    try std.testing.expectEqual(@as(u64, 7), target.count);

    network.useRandomRelease(0xA17F_7002);
    var queued_a = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/a",
    });
    defer queued_a.deinit(std.testing.allocator);
    var queued_b = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/b",
    });
    defer queued_b.deinit(std.testing.allocator);
    var queued_c = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/c",
    });
    defer queued_c.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), network.queuedCount());
    try std.testing.expectEqual(@as(usize, 3), try network.runUntilIdle());
    try std.testing.expectEqual(@as(u64, 10), target.count);
    network.useFifoRelease();
}

test "virtual http network delivers queued GET requests synchronously" {
    const RecordingExecutor = struct {
        count: u64 = 0,

        fn executor(self: *@This()) transport.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: transport.HttpRequest) !transport.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count +|= 1;
            return .{
                .status = 200,
                .body = try alloc.dupe(u8, req.uri),
            };
        }
    };

    var network = VirtualHttpNetwork.init(std.testing.allocator);
    defer network.deinit();
    network.useQueuedDelivery();

    var target = RecordingExecutor{};
    try network.registerNode(7, target.executor());

    const executor = network.executor();
    var get_response = try executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = "sim://raft-node/7/raft/v1/snapshot/fetch/snap-1",
    });
    defer get_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), get_response.status);
    try std.testing.expectEqualStrings("/raft/v1/snapshot/fetch/snap-1", get_response.body);
    try std.testing.expectEqual(@as(u64, 1), target.count);
    try std.testing.expectEqual(@as(usize, 0), network.queuedCount());

    var post_response = try executor.execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "sim://raft-node/7/raft/v1/batch",
    });
    defer post_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), post_response.status);
    try std.testing.expectEqual(@as(u64, 1), target.count);
    try std.testing.expectEqual(@as(usize, 1), network.queuedCount());
}

fn sleepForNanos(delay_ns: u64) !void {
    var req = std.posix.timespec{
        .sec = @intCast(delay_ns / std.time.ns_per_s),
        .nsec = @intCast(delay_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

pub const ManagedHostSimulation = struct {
    alloc: std.mem.Allocator,
    updates: *runtime_loop.MemoryUpdateSource,
    applier: metadata_apply.MetadataApplier,
    runtime: runtime_loop.ManagedHostRuntime,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: ManagedHostSimulationConfig,
        deps: ManagedHostSimulationDeps,
    ) !ManagedHostSimulation {
        const updates = try alloc.create(runtime_loop.MemoryUpdateSource);
        errdefer alloc.destroy(updates);
        updates.* = runtime_loop.MemoryUpdateSource.init(alloc);
        errdefer updates.deinit();

        return .{
            .alloc = alloc,
            .updates = updates,
            .applier = metadata_apply.MetadataApplier.init(updates.sink()),
            .runtime = try runtime_loop.ManagedHostRuntime.init(
                alloc,
                cfg.host,
                deps.host,
                cfg.service,
                deps.service,
                updates.source(),
                cfg.runtime,
            ),
        };
    }

    pub fn deinit(self: *ManagedHostSimulation) void {
        self.runtime.deinit();
        self.updates.deinit();
        self.alloc.destroy(self.updates);
        self.* = undefined;
    }

    pub fn apply(self: *ManagedHostSimulation, change: metadata_apply.AppliedMetadataChange) !void {
        try self.applier.apply(change);
    }

    pub fn applyBatch(self: *ManagedHostSimulation, changes: []const metadata_apply.AppliedMetadataChange) !void {
        try self.applier.applyBatch(changes);
    }

    pub fn stepOnce(self: *ManagedHostSimulation) !runtime_loop.RuntimeStepResult {
        return try self.runtime.stepOnce();
    }

    pub fn status(self: *ManagedHostSimulation, group_id: u64) host.HostedReplicaStatus {
        return self.runtime.svc.host.host.status(group_id);
    }

    pub fn metricsSnapshot(self: *ManagedHostSimulation) host.HostMetrics {
        return self.runtime.svc.host.host.metricsSnapshot();
    }

    pub fn listGroupIds(self: *ManagedHostSimulation, alloc: std.mem.Allocator) ![]u64 {
        return try self.runtime.svc.host.host.listGroupIds(alloc);
    }

    pub fn serviceMetrics(self: *ManagedHostSimulation) service.ManagedServiceMetrics {
        return self.runtime.svc.metrics;
    }

    pub fn pendingUpdates(self: *ManagedHostSimulation) usize {
        return self.runtime.svc.pending_updates.items.len;
    }

    pub fn campaignGroup(self: *ManagedHostSimulation, group_id: u64) !void {
        try self.runtime.svc.host.host.campaignGroup(group_id);
    }

    pub fn propose(self: *ManagedHostSimulation, group_id: u64, data: []const u8) !void {
        try self.runtime.svc.host.host.propose(group_id, data);
    }

    pub fn transferLeader(self: *ManagedHostSimulation, group_id: u64, transferee: u64) !void {
        try self.runtime.svc.host.host.transferLeader(group_id, transferee);
    }

    pub fn requestReadableLease(self: *ManagedHostSimulation, group_id: u64, request_ctx: []const u8) !void {
        try self.runtime.svc.requestReadableLease(group_id, request_ctx);
    }

    pub fn prepareEnrichmentRead(self: *ManagedHostSimulation, group_id: u64, kind: read_gate.EnrichmentReadKind) !void {
        try self.runtime.svc.prepareEnrichmentRead(group_id, kind);
    }

    pub fn prepareSearchRead(self: *ManagedHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareSearchRead(group_id);
    }

    pub fn prepareSearchRequest(self: *ManagedHostSimulation, group_id: u64, req: db_types.SearchRequest) !void {
        try self.runtime.svc.prepareSearchRequest(group_id, req);
    }

    pub fn featureReads(self: *ManagedHostSimulation) feature_reads.FeatureReads {
        return self.runtime.svc.featureReads();
    }

    pub fn prepareLookupRead(self: *ManagedHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareLookupRead(group_id);
    }

    pub fn prepareLookupRequest(self: *ManagedHostSimulation, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.runtime.svc.prepareLookupRequest(group_id, key, opts);
    }

    pub fn prepareScanRead(self: *ManagedHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareScanRead(group_id);
    }

    pub fn prepareScanRequest(
        self: *ManagedHostSimulation,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.runtime.svc.prepareScanRequest(group_id, from_key, to_key, opts);
    }

    pub fn readIndex(self: *ManagedHostSimulation, group_id: u64, request_ctx: []const u8) !void {
        try self.requestReadableLease(group_id, request_ctx);
    }

    pub fn proposeConfChangeV2(self: *ManagedHostSimulation, group_id: u64, conf_change: raft_engine.core.ConfChangeV2) !void {
        try self.runtime.svc.host.host.proposeConfChangeV2(group_id, conf_change);
    }

    pub fn raftStatus(self: *ManagedHostSimulation, group_id: u64) ?@import("raft_engine").core.Status {
        return self.runtime.svc.host.host.raftStatus(group_id);
    }

    pub fn leaderId(self: *ManagedHostSimulation, group_id: u64) ?u64 {
        return self.runtime.svc.host.host.leaderId(group_id);
    }

    pub fn isLocalLeader(self: *ManagedHostSimulation, group_id: u64) bool {
        return self.runtime.svc.host.host.isLocalLeader(group_id);
    }
};

pub const ManagedHttpHostSimulation = struct {
    alloc: std.mem.Allocator,
    updates: *runtime_loop.MemoryUpdateSource,
    applier: metadata_apply.MetadataApplier,
    runtime: runtime_loop.ManagedHttpHostRuntime,
    virtual_base_uri: ?[]u8 = null,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: ManagedHttpHostSimulationConfig,
        deps: ManagedHttpHostSimulationDeps,
    ) !ManagedHttpHostSimulation {
        const updates = try alloc.create(runtime_loop.MemoryUpdateSource);
        errdefer alloc.destroy(updates);
        updates.* = runtime_loop.MemoryUpdateSource.init(alloc);
        errdefer updates.deinit();

        return .{
            .alloc = alloc,
            .updates = updates,
            .applier = metadata_apply.MetadataApplier.init(updates.sink()),
            .runtime = try runtime_loop.ManagedHttpHostRuntime.init(
                alloc,
                cfg.host,
                deps.host,
                cfg.service,
                deps.service,
                updates.source(),
                cfg.runtime,
            ),
        };
    }

    pub fn deinit(self: *ManagedHttpHostSimulation) void {
        if (self.virtual_base_uri) |uri| self.alloc.free(uri);
        self.runtime.deinit();
        self.updates.deinit();
        self.alloc.destroy(self.updates);
        self.* = undefined;
    }

    pub fn start(self: *ManagedHttpHostSimulation) !void {
        if (self.virtual_base_uri != null) return;
        try self.runtime.start();
    }

    pub fn stop(self: *ManagedHttpHostSimulation) void {
        if (self.virtual_base_uri != null) return;
        self.runtime.stop();
    }

    pub fn baseUri(self: *ManagedHttpHostSimulation, alloc: std.mem.Allocator) ![]u8 {
        if (self.virtual_base_uri) |uri| return try alloc.dupe(u8, uri);
        return try self.runtime.baseUri(alloc);
    }

    fn useVirtualBaseUri(self: *ManagedHttpHostSimulation, node_id: u64) !void {
        if (self.virtual_base_uri) |uri| {
            self.alloc.free(uri);
            self.virtual_base_uri = null;
        }
        self.virtual_base_uri = try VirtualHttpNetwork.baseUri(self.alloc, node_id);
    }

    fn serverExecutor(self: *ManagedHttpHostSimulation) transport.RequestExecutor {
        return self.runtime.svc.host.http_host.server.executor();
    }

    pub fn upsertPeerRoute(
        self: *ManagedHttpHostSimulation,
        group_id: u64,
        node_id: u64,
        endpoints: []const peer_resolver.PeerEndpoint,
    ) !usize {
        return try self.runtime.svc.host.http_host.upsertResolvedPeerEndpoints(group_id, node_id, endpoints);
    }

    pub fn removePeerRoute(self: *ManagedHttpHostSimulation, group_id: u64, node_id: u64) !bool {
        return try self.runtime.svc.host.http_host.removePeerRoute(group_id, node_id);
    }

    pub fn apply(self: *ManagedHttpHostSimulation, change: metadata_apply.AppliedMetadataChange) !void {
        try self.applier.apply(change);
    }

    pub fn applyBatch(self: *ManagedHttpHostSimulation, changes: []const metadata_apply.AppliedMetadataChange) !void {
        try self.applier.applyBatch(changes);
    }

    pub fn stepOnce(self: *ManagedHttpHostSimulation) !runtime_loop.RuntimeStepResult {
        return try self.runtime.stepOnce();
    }

    pub fn status(self: *ManagedHttpHostSimulation, group_id: u64) host.HostedReplicaStatus {
        return self.runtime.svc.host.http_host.host.status(group_id);
    }

    pub fn metricsSnapshot(self: *ManagedHttpHostSimulation) host.HostMetrics {
        return self.runtime.svc.host.http_host.host.metricsSnapshot();
    }

    pub fn listGroupIds(self: *ManagedHttpHostSimulation, alloc: std.mem.Allocator) ![]u64 {
        return try self.runtime.svc.host.http_host.host.listGroupIds(alloc);
    }

    pub fn serviceMetrics(self: *ManagedHttpHostSimulation) service.ManagedServiceMetrics {
        return self.runtime.svc.metrics;
    }

    pub fn observeSplitTransition(self: *ManagedHttpHostSimulation, transition_id: u64) !?metadata_mod.SplitObservation {
        return try self.runtime.svc.observeSplitTransition(transition_id);
    }

    pub fn describeSplitTransition(self: *ManagedHttpHostSimulation, transition_id: u64) !?metadata_mod.SplitExecutionState {
        return try self.runtime.svc.describeSplitTransition(transition_id);
    }

    pub fn observeMergeTransition(self: *ManagedHttpHostSimulation, transition_id: u64) !?metadata_mod.MergeObservation {
        return try self.runtime.svc.observeMergeTransition(transition_id);
    }

    pub fn describeMergeTransition(self: *ManagedHttpHostSimulation, transition_id: u64) !?metadata_mod.MergeExecutionState {
        return try self.runtime.svc.describeMergeTransition(transition_id);
    }

    pub fn campaignGroup(self: *ManagedHttpHostSimulation, group_id: u64) !void {
        try self.runtime.svc.host.http_host.campaignGroup(group_id);
    }

    pub fn propose(self: *ManagedHttpHostSimulation, group_id: u64, data: []const u8) !void {
        try self.runtime.svc.host.http_host.propose(group_id, data);
    }

    pub fn transferLeader(self: *ManagedHttpHostSimulation, group_id: u64, transferee: u64) !void {
        try self.runtime.svc.host.http_host.transferLeader(group_id, transferee);
    }

    pub fn requestReadableLease(self: *ManagedHttpHostSimulation, group_id: u64, request_ctx: []const u8) !void {
        try self.runtime.svc.requestReadableLease(group_id, request_ctx);
    }

    pub fn prepareEnrichmentRead(self: *ManagedHttpHostSimulation, group_id: u64, kind: read_gate.EnrichmentReadKind) !void {
        try self.runtime.svc.prepareEnrichmentRead(group_id, kind);
    }

    pub fn prepareSearchRead(self: *ManagedHttpHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareSearchRead(group_id);
    }

    pub fn prepareSearchRequest(self: *ManagedHttpHostSimulation, group_id: u64, req: db_types.SearchRequest) !void {
        try self.runtime.svc.prepareSearchRequest(group_id, req);
    }

    pub fn featureReads(self: *ManagedHttpHostSimulation) feature_reads.FeatureReads {
        return self.runtime.svc.featureReads();
    }

    pub fn prepareLookupRead(self: *ManagedHttpHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareLookupRead(group_id);
    }

    pub fn prepareLookupRequest(self: *ManagedHttpHostSimulation, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.runtime.svc.prepareLookupRequest(group_id, key, opts);
    }

    pub fn prepareScanRead(self: *ManagedHttpHostSimulation, group_id: u64) !void {
        try self.runtime.svc.prepareScanRead(group_id);
    }

    pub fn prepareScanRequest(
        self: *ManagedHttpHostSimulation,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.runtime.svc.prepareScanRequest(group_id, from_key, to_key, opts);
    }

    pub fn readIndex(self: *ManagedHttpHostSimulation, group_id: u64, request_ctx: []const u8) !void {
        try self.requestReadableLease(group_id, request_ctx);
    }

    pub fn proposeConfChangeV2(self: *ManagedHttpHostSimulation, group_id: u64, conf_change: raft_engine.core.ConfChangeV2) !void {
        try self.runtime.svc.host.http_host.proposeConfChangeV2(group_id, conf_change);
    }

    pub fn raftStatus(self: *ManagedHttpHostSimulation, group_id: u64) ?@import("raft_engine").core.Status {
        return self.runtime.svc.host.http_host.raftStatus(group_id);
    }

    pub fn leaderId(self: *ManagedHttpHostSimulation, group_id: u64) ?u64 {
        return self.runtime.svc.host.http_host.leaderId(group_id);
    }

    pub fn isLocalLeader(self: *ManagedHttpHostSimulation, group_id: u64) bool {
        return self.runtime.svc.host.http_host.isLocalLeader(group_id);
    }
};

pub const ManagedHttpClusterSimulation = struct {
    alloc: std.mem.Allocator,
    network: *VirtualHttpNetwork,
    configs: []ManagedHttpHostSimulationConfig,
    deps: []ManagedHttpHostSimulationDeps,
    nodes: []ManagedHttpHostSimulation,
    started: bool = false,

    pub const Fault = union(enum) {
        partition_node: u64,
        partition_link: VirtualHttpNetwork.Link,
        drop_next,
        duplicate_next,
        delay_next_ticks: u64,
        random_drop: VirtualHttpNetwork.RandomDropConfig,
        clear_random_drop,
        release_fifo,
        release_random: u64,
    };

    pub const ProgressPredicate = *const fn (*ManagedHttpClusterSimulation, *anyopaque) anyerror!bool;

    pub fn init(
        alloc: std.mem.Allocator,
        configs: []const ManagedHttpHostSimulationConfig,
        deps: []const ManagedHttpHostSimulationDeps,
    ) !ManagedHttpClusterSimulation {
        if (configs.len != deps.len) return error.MismatchedClusterNodeCount;
        const network = try alloc.create(VirtualHttpNetwork);
        errdefer alloc.destroy(network);
        network.* = VirtualHttpNetwork.init(alloc);
        network.useQueuedDelivery();
        errdefer network.deinit();

        const owned_configs = try alloc.dupe(ManagedHttpHostSimulationConfig, configs);
        errdefer alloc.free(owned_configs);
        const owned_deps = try alloc.dupe(ManagedHttpHostSimulationDeps, deps);
        errdefer alloc.free(owned_deps);
        for (owned_deps) |*dep| dep.host.http.request_executor = network.executor();

        const nodes = try alloc.alloc(ManagedHttpHostSimulation, configs.len);
        errdefer alloc.free(nodes);

        var initialized: usize = 0;
        errdefer {
            var i: usize = initialized;
            while (i > 0) {
                i -= 1;
                nodes[i].deinit();
            }
        }

        for (owned_configs, owned_deps, 0..) |cfg, dep, i| {
            nodes[i] = try ManagedHttpHostSimulation.init(alloc, cfg, dep);
            initialized += 1;
            const node_id = cfg.host.http.host.local_node_id;
            try nodes[i].useVirtualBaseUri(node_id);
            try network.registerNode(node_id, nodes[i].serverExecutor());
        }

        return .{
            .alloc = alloc,
            .network = network,
            .configs = owned_configs,
            .deps = owned_deps,
            .nodes = nodes,
        };
    }

    pub fn deinit(self: *ManagedHttpClusterSimulation) void {
        for (self.nodes) |*sim| sim.deinit();
        self.alloc.free(self.nodes);
        self.alloc.free(self.configs);
        self.alloc.free(self.deps);
        self.network.deinit();
        self.alloc.destroy(self.network);
        self.* = undefined;
    }

    pub fn startAll(self: *ManagedHttpClusterSimulation) !void {
        var started: usize = 0;
        errdefer {
            var i: usize = started;
            while (i > 0) {
                i -= 1;
                self.nodes[i].stop();
            }
        }

        for (self.nodes) |*sim| {
            try sim.start();
            started += 1;
        }
        self.started = true;
    }

    pub fn stopAll(self: *ManagedHttpClusterSimulation) void {
        for (self.nodes) |*sim| sim.stop();
        self.started = false;
    }

    pub fn node(self: *ManagedHttpClusterSimulation, index: usize) *ManagedHttpHostSimulation {
        return &self.nodes[index];
    }

    pub fn stepAll(self: *ManagedHttpClusterSimulation) !void {
        _ = try self.network.drainDue(null);
        for (self.nodes) |*sim| {
            _ = try sim.stepOnce();
            _ = try self.network.drainDue(null);
        }
        _ = try self.network.advanceTicks(1);
    }

    pub fn step(self: *ManagedHttpClusterSimulation) !void {
        try self.stepAll();
    }

    pub fn runUntil(
        self: *ManagedHttpClusterSimulation,
        max_rounds: usize,
        context: *anyopaque,
        predicate: ProgressPredicate,
    ) !bool {
        var i: usize = 0;
        while (i < max_rounds) : (i += 1) {
            if (try predicate(self, context)) return true;
            try self.step();
        }
        return try predicate(self, context);
    }

    pub fn assertProgress(
        self: *ManagedHttpClusterSimulation,
        label: []const u8,
        max_rounds: usize,
        context: *anyopaque,
        predicate: ProgressPredicate,
    ) !void {
        if (try self.runUntil(max_rounds, context, predicate)) return;
        std.debug.print("metadata cluster sim progress timeout label={s} rounds={d}\n", .{ label, max_rounds });
        return error.SimulationProgressTimeout;
    }

    pub fn inject(self: *ManagedHttpClusterSimulation, fault: Fault) !void {
        switch (fault) {
            .partition_node => |node_id| try self.network.partitionNode(node_id),
            .partition_link => |link| try self.network.partitionLink(link),
            .drop_next => self.network.dropNext(),
            .duplicate_next => self.network.duplicateNext(),
            .delay_next_ticks => |ticks| self.network.delayNextTicks(ticks),
            .random_drop => |cfg| try self.network.configureRandomDrop(cfg),
            .clear_random_drop => self.network.clearRandomDrop(),
            .release_fifo => self.network.useFifoRelease(),
            .release_random => |seed| self.network.useRandomRelease(seed),
        }
    }

    pub fn heal(self: *ManagedHttpClusterSimulation, fault: Fault) void {
        switch (fault) {
            .partition_node => |node_id| self.network.healNode(node_id),
            .partition_link => |link| self.network.healLink(link),
            .drop_next, .duplicate_next, .delay_next_ticks => self.network.clearOneShotFaults(),
            .random_drop, .clear_random_drop => self.network.clearRandomDrop(),
            .release_fifo, .release_random => self.network.useFifoRelease(),
        }
    }

    pub fn healAll(self: *ManagedHttpClusterSimulation) void {
        self.network.clearOneShotFaults();
        self.network.clearRandomDrop();
        self.network.useFifoRelease();
        self.network.healAll();
    }

    pub fn restartNode(self: *ManagedHttpClusterSimulation, index: usize) !void {
        if (self.started) self.nodes[index].stop();
        self.nodes[index].deinit();
        self.nodes[index] = try ManagedHttpHostSimulation.init(self.alloc, self.configs[index], self.deps[index]);
        const node_id = self.configs[index].host.http.host.local_node_id;
        try self.nodes[index].useVirtualBaseUri(node_id);
        try self.network.registerNode(node_id, self.nodes[index].serverExecutor());
        if (self.started) try self.nodes[index].start();
    }

    pub fn waitForLeader(self: *ManagedHttpClusterSimulation, group_id: u64, max_rounds: usize) !?u64 {
        const Context = struct {
            group_id: u64,
            leader_id: ?u64 = null,

            fn done(cluster: *ManagedHttpClusterSimulation, ptr: *anyopaque) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                for (cluster.nodes) |*sim| {
                    if (sim.raftStatus(ctx.group_id)) |status| {
                        if (status.soft.role == .leader) {
                            ctx.leader_id = status.id;
                            return true;
                        }
                    }
                }
                return false;
            }
        };

        var ctx = Context{ .group_id = group_id };
        if (try self.runUntil(max_rounds, &ctx, Context.done)) return ctx.leader_id;
        return null;
    }

    pub fn waitForLeaderId(
        self: *ManagedHttpClusterSimulation,
        group_id: u64,
        expected_leader_id: u64,
        max_rounds: usize,
    ) !bool {
        const Context = struct {
            group_id: u64,
            expected_leader_id: u64,

            fn done(cluster: *ManagedHttpClusterSimulation, ptr: *anyopaque) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                for (cluster.nodes) |*sim| {
                    if (sim.raftStatus(ctx.group_id)) |status| {
                        if (status.soft.role == .leader and status.id == ctx.expected_leader_id) return true;
                    }
                }
                return false;
            }
        };

        var ctx = Context{ .group_id = group_id, .expected_leader_id = expected_leader_id };
        return try self.runUntil(max_rounds, &ctx, Context.done);
    }

    pub fn waitForLastIndex(
        self: *ManagedHttpClusterSimulation,
        store: *raft_engine.core.MemoryStorage,
        target_index: u64,
        max_rounds: usize,
    ) !bool {
        const Context = struct {
            store: *raft_engine.core.MemoryStorage,
            target_index: u64,

            fn done(_: *ManagedHttpClusterSimulation, ptr: *anyopaque) !bool {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                return (try ctx.store.storage().lastIndex()) >= ctx.target_index;
            }
        };

        var ctx = Context{ .store = store, .target_index = target_index };
        return try self.runUntil(max_rounds, &ctx, Context.done);
    }

    pub fn validateMirroredMergePair(
        self: *ManagedHttpClusterSimulation,
        donor: data_mod.MergeTransitionStatus,
        receiver: data_mod.MergeTransitionStatus,
    ) transition_checker.MergeTransitionCheckError!void {
        _ = self;
        try transition_checker.validateMirroredMergePair(donor, receiver);
    }

    pub fn validateSplitTransitionEnrichment(
        self: *ManagedHttpClusterSimulation,
        status: data_mod.SplitTransitionStatus,
        destination_owns_transition_range: bool,
    ) transition_checker.TransitionEnrichmentCheckError!void {
        _ = self;
        try transition_checker.validateSplitEnrichment(status, destination_owns_transition_range);
    }

    pub fn validateMergeTransitionEnrichment(
        self: *ManagedHttpClusterSimulation,
        donor: data_mod.MergeTransitionStatus,
        receiver: data_mod.MergeTransitionStatus,
        donor_owns_merged_range: bool,
        receiver_owns_merged_range: bool,
    ) transition_checker.TransitionEnrichmentCheckError!void {
        _ = self;
        try transition_checker.validateMergeEnrichment(
            donor,
            receiver,
            donor_owns_merged_range,
            receiver_owns_merged_range,
        );
    }

    pub fn driveSplitTransition(
        self: *ManagedHttpClusterSimulation,
        runtime: transition_runtime_mod.TransitionRuntime,
        record: *metadata_mod.SplitTransitionRecord,
        max_rounds: usize,
    ) !void {
        var rounds: usize = 0;
        var runtime_mut = runtime;
        const metadata_runtime = runtime_mut.metadataRuntime();
        while (rounds < max_rounds and record.phase != .finalized and record.phase != .rolled_back) : (rounds += 1) {
            _ = try metadata_mod.TransitionDriver.stepSplit(metadata_runtime, record);
            try self.stepAll();
        }
    }

    pub fn driveMergeTransition(
        self: *ManagedHttpClusterSimulation,
        runtime: transition_runtime_mod.TransitionRuntime,
        record: *metadata_mod.MergeTransitionRecord,
        max_rounds: usize,
    ) !void {
        var rounds: usize = 0;
        var runtime_mut = runtime;
        const metadata_runtime = runtime_mut.metadataRuntime();
        while (rounds < max_rounds and record.phase != .finalized and record.phase != .rolled_back) : (rounds += 1) {
            _ = try metadata_mod.TransitionDriver.stepMerge(metadata_runtime, record);
            try self.stepAll();
        }
    }
};

fn splitTransitionInactive(observation: ?metadata_mod.SplitObservation) bool {
    const observed = observation orelse return true;
    return switch (observed.status.phase) {
        .finalized, .rolled_back => true,
        else => false,
    };
}

fn mergeTransitionInactive(observation: ?metadata_mod.MergeObservation) bool {
    const observed = observation orelse return true;
    return switch (observed.receiver.phase) {
        .finalized, .rolled_back => true,
        else => false,
    };
}

fn expectSplitTransitionInactive(cluster: *ManagedHttpClusterSimulation, node_index: usize, transition_id: u64) !void {
    try std.testing.expect(splitTransitionInactive(try cluster.node(node_index).observeSplitTransition(transition_id)));
}

fn expectMergeTransitionInactive(cluster: *ManagedHttpClusterSimulation, node_index: usize, transition_id: u64) !void {
    try std.testing.expect(mergeTransitionInactive(try cluster.node(node_index).observeMergeTransition(transition_id)));
}

const StorageRecorder = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(u64, *raft_engine.core.MemoryStorage) = .empty,

    fn deinit(self: *StorageRecorder) void {
        self.stores.deinit(self.alloc);
        self.* = undefined;
    }

    fn registerStore(self: *StorageRecorder, group_id: u64, store: *raft_engine.core.MemoryStorage) !void {
        try self.stores.put(self.alloc, group_id, store);
    }

    fn iface(self: *StorageRecorder) raft_engine.runtime.storage_iface.GroupStorage {
        return .{
            .ptr = self,
            .vtable = &.{
                .persist_ready = persistReady,
            },
        };
    }

    fn persistReady(ptr: *anyopaque, group_id: u64, ready: raft_engine.core.Ready) !void {
        const self: *StorageRecorder = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(group_id) orelse return error.UnknownGroup;
        if (ready.snapshot) |snapshot| try store.applySnapshot(snapshot);
        if (ready.hard_state) |hard_state| store.setHardState(hard_state);
        if (ready.entries.len > 0) try store.append(ready.entries);
    }
};

const SimulationDescriptorFactory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    peers: []const raft_engine.core.types.NodeId,
    fetch_from: ?u64 = null,
    fetch_snapshot_id: ?[]const u8 = null,
    fetch_uri: ?[]const u8 = null,

    fn iface(self: *@This()) host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, self.peers);

        const desc = raft_engine.runtime.ReplicaDescriptor{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers[0..],
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = false,
                    .check_quorum = true,
                },
                .storage = self.store.storage(),
            },
            .bootstrap = switch (record.bootstrap_mode) {
                .empty => .empty,
                .persisted => .persisted,
                .fetch_snapshot => blk: {
                    const from = self.fetch_from orelse return error.MissingSnapshotBootstrap;
                    const snapshot_id = self.fetch_snapshot_id orelse return error.MissingSnapshotBootstrap;
                    const uri = self.fetch_uri orelse return error.MissingSnapshotBootstrap;
                    break :blk .{
                        .fetch_snapshot = .{
                            .from = from,
                            .locator = .{
                                .snapshot_id = try self.alloc.dupe(u8, snapshot_id),
                                .uri = try self.alloc.dupe(u8, uri),
                            },
                            .fetch_immediately = true,
                        },
                    };
                },
            },
        };
        return desc;
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = alloc;
        self.alloc.free(desc.group.raft_config.peers);
        switch (desc.bootstrap) {
            .empty, .persisted => {},
            .fetch_snapshot => |*snapshot| {
                if (snapshot.locator.snapshot_id.len > 0) self.alloc.free(snapshot.locator.snapshot_id);
                if (snapshot.locator.uri.len > 0) self.alloc.free(snapshot.locator.uri);
            },
        }
    }
};

fn stepHttpPair(a: *ManagedHttpHostSimulation, b: *ManagedHttpHostSimulation) !void {
    _ = try a.stepOnce();
    _ = try b.stepOnce();
}

fn waitForLeader(
    a: *ManagedHttpHostSimulation,
    b: *ManagedHttpHostSimulation,
    group_id: u64,
    max_rounds: usize,
) !?u64 {
    var i: usize = 0;
    while (i < max_rounds) : (i += 1) {
        try stepHttpPair(a, b);
        if (a.raftStatus(group_id)) |status| {
            if (status.soft.role == .leader) return status.id;
        }
        if (b.raftStatus(group_id)) |status| {
            if (status.soft.role == .leader) return status.id;
        }
    }
    return null;
}

fn waitForLastIndex(
    a: *ManagedHttpHostSimulation,
    b: *ManagedHttpHostSimulation,
    store: *raft_engine.core.MemoryStorage,
    target_index: u64,
    max_rounds: usize,
) !bool {
    var i: usize = 0;
    while (i < max_rounds) : (i += 1) {
        if (try store.storage().lastIndex() >= target_index) return true;
        try stepHttpPair(a, b);
    }
    return (try store.storage().lastIndex()) >= target_index;
}

fn waitForLastIndexInCluster(
    cluster: *ManagedHttpClusterSimulation,
    store: *raft_engine.core.MemoryStorage,
    target_index: u64,
    max_rounds: usize,
) !bool {
    var i: usize = 0;
    while (i < max_rounds) : (i += 1) {
        if (try store.storage().lastIndex() >= target_index) return true;
        try cluster.stepAll();
    }
    return (try store.storage().lastIndex()) >= target_index;
}

fn waitForCommitIndex(
    sim: *ManagedHostSimulation,
    group_id: u64,
    target_index: u64,
    max_rounds: usize,
) !bool {
    var i: usize = 0;
    while (i < max_rounds) : (i += 1) {
        if (sim.raftStatus(group_id)) |status| {
            if (status.hard.commit_index >= target_index) return true;
        }
        _ = try sim.stepOnce();
    }
    if (sim.raftStatus(group_id)) |status| return status.hard.commit_index >= target_index;
    return false;
}

fn waitForCommitIndexInCluster(
    cluster: *ManagedHttpClusterSimulation,
    node_index: usize,
    group_id: u64,
    target_index: u64,
    max_rounds: usize,
) !bool {
    var i: usize = 0;
    while (i < max_rounds) : (i += 1) {
        if (cluster.node(node_index).raftStatus(group_id)) |status| {
            if (status.hard.commit_index >= target_index) return true;
        }
        try cluster.stepAll();
    }
    if (cluster.node(node_index).raftStatus(group_id)) |status| return status.hard.commit_index >= target_index;
    return false;
}

test "managed host simulation drives add and peer refresh through deterministic steps" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{ record.local_node_id, 2 });
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
        .host = .{ .host = .{ .local_node_id = 1 } },
    }, .{
        .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
    });
    defer sim.deinit();

    try sim.applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 1201,
                    .replica_id = 11,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{2},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 1201,
                .node_id = 2,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = "http://n2:9010",
                        .metadata = "",
                    },
                },
            },
        },
    });

    try std.testing.expectEqual(@as(usize, 0), sim.pendingUpdates());
    const result = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 2), result.drained_updates);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.refreshed_peers);
    try std.testing.expectEqual(.active, sim.status(1201));
    try std.testing.expectEqual(@as(usize, 2), sim.serviceMetrics().applied_updates);

    const metrics = sim.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), metrics.ensure_replica_calls);
    try std.testing.expectEqual(@as(usize, 1), metrics.endpoint_refreshes);

    const groups = try sim.listGroupIds(std.testing.allocator);
    defer std.testing.allocator.free(groups);
    try std.testing.expectEqualSlices(u64, &.{1201}, groups);
}

test "managed host simulation removes routes and replicas across deterministic steps" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{ record.local_node_id, 2 });
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
        .host = .{ .host = .{ .local_node_id = 1 } },
    }, .{
        .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
    });
    defer sim.deinit();

    try sim.applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 1301,
                    .replica_id = 13,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{2},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 1301,
                .node_id = 2,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = "http://n2:9020",
                        .metadata = "",
                    },
                },
            },
        },
    });
    _ = try sim.stepOnce();

    try sim.applyBatch(&.{
        .{
            .remove_peer_route = .{
                .group_id = 1301,
                .node_id = 2,
            },
        },
        .{
            .remove_replica_intent = .{
                .group_id = 1301,
            },
        },
    });

    const result = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 2), result.drained_updates);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.removed);
    try std.testing.expectEqual(.absent, sim.status(1301));

    const metrics = sim.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), metrics.endpoint_removals);
    try std.testing.expectEqual(@as(usize, 1), metrics.remove_replica_calls);
}

test "managed host simulation restores through both raft state backends" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const BackingCase = struct {
        backend: host.ReplicaStateBackend,
        label: []const u8,
    };
    const cases = [_]BackingCase{
        .{ .backend = .file_image, .label = "file-image" },
        .{ .backend = .wal, .label = "wal" },
    };

    for (cases, 0..) |case_cfg, case_index| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/sim-{s}", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_root);
        const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/sim-{s}/catalog.txt", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_catalog_path);

        var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer dummy_store.deinit();
        var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

        const group_id: u64 = 1500 + case_index + 1;

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try sim.apply(.{
                .upsert_replica_intent = .{
                    .record = .{
                        .group_id = group_id,
                        .replica_id = 1,
                        .local_node_id = 1,
                        .bootstrap_mode = .persisted,
                    },
                    .peer_node_ids = &.{},
                },
            });
            _ = try sim.stepOnce();
            try sim.campaignGroup(group_id);
            _ = try sim.stepOnce();
            try sim.propose(group_id, "before-restart");
            try std.testing.expect(try waitForCommitIndex(&sim, group_id, 2, 32));
        }

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try std.testing.expectEqual(.active, sim.status(group_id));
            const baseline_commit = if (sim.raftStatus(group_id)) |status|
                status.hard.commit_index
            else
                return error.MissingRaftStatus;
            try std.testing.expect(baseline_commit >= 1);

            try sim.campaignGroup(group_id);
            _ = try sim.stepOnce();
            try sim.propose(group_id, "after-restart");
            try std.testing.expect(try waitForCommitIndex(&sim, group_id, baseline_commit + 1, 32));
        }
    }
}

test "managed host simulation keeps WAL replay debt bounded across repeated proposals" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/sim-wal-bounded-replay-debt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/sim-wal-bounded-replay-debt/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

    const group_id: u64 = 1550;
    const checkpoint_threshold: usize = 4;

    var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .host = .{
                .local_node_id = 1,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
                .replica_state_backend = .wal,
            },
            .wal_replica_state = .{
                .checkpoint_replay_records_threshold = checkpoint_threshold,
                .checkpoint_replay_bytes_threshold = 0,
            },
        },
    }, .{
        .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = group_id,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .peer_node_ids = &.{},
        },
    });
    _ = try sim.stepOnce();
    try sim.campaignGroup(group_id);
    _ = try sim.stepOnce();

    var proposal_index: usize = 0;
    while (proposal_index < 12) : (proposal_index += 1) {
        const payload = try std.fmt.allocPrint(std.testing.allocator, "proposal-{d}", .{proposal_index});
        defer std.testing.allocator.free(payload);

        const before = sim.raftStatus(group_id) orelse return error.MissingRaftStatus;
        try sim.propose(group_id, payload);
        try std.testing.expect(try waitForCommitIndex(&sim, group_id, before.hard.commit_index + 1, 32));
    }

    const provider = sim.runtime.svc.host.owned_wal_replica_provider orelse return error.TestExpectedEqual;
    const state = provider.stateForGroup(group_id) orelse return error.TestExpectedEqual;
    const stats = state.statsSnapshot();
    try std.testing.expect(stats.replay_debt_records < checkpoint_threshold);

    const wal_entries = try state.wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (wal_entries) |entry| std.testing.allocator.free(@constCast(entry.data));
        std.testing.allocator.free(wal_entries);
    }
    try std.testing.expect(wal_entries.len <= checkpoint_threshold);
}

test "managed host simulation persists replica removal across restart for both raft state backends" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const BackingCase = struct {
        backend: host.ReplicaStateBackend,
        label: []const u8,
    };
    const cases = [_]BackingCase{
        .{ .backend = .file_image, .label = "file-image-remove" },
        .{ .backend = .wal, .label = "wal-remove" },
    };

    for (cases) |case_cfg| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_root);
        const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}/catalog.txt", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_catalog_path);

        var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer dummy_store.deinit();
        var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try sim.apply(.{
                .upsert_replica_intent = .{
                    .record = .{
                        .group_id = 1601,
                        .replica_id = 1,
                        .local_node_id = 1,
                    },
                    .peer_node_ids = &.{},
                },
            });
            _ = try sim.stepOnce();
            try std.testing.expectEqual(.active, sim.status(1601));

            try sim.apply(.{ .remove_replica_intent = .{ .group_id = 1601 } });
            _ = try sim.stepOnce();
            try std.testing.expectEqual(.absent, sim.status(1601));
        }

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try std.testing.expectEqual(.absent, sim.status(1601));
            const groups = try sim.listGroupIds(std.testing.allocator);
            defer std.testing.allocator.free(groups);
            try std.testing.expectEqual(@as(usize, 0), groups.len);
        }
    }
}

test "managed host simulation drops queued metadata updates across restart for both raft state backends" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const BackingCase = struct {
        backend: host.ReplicaStateBackend,
        label: []const u8,
    };
    const cases = [_]BackingCase{
        .{ .backend = .file_image, .label = "file-image-queued-intent" },
        .{ .backend = .wal, .label = "wal-queued-intent" },
    };

    for (cases) |case_cfg| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_root);
        const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}/catalog.txt", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_catalog_path);

        var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer dummy_store.deinit();
        var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try sim.apply(.{
                .upsert_replica_intent = .{
                    .record = .{
                        .group_id = 1701,
                        .replica_id = 1,
                        .local_node_id = 1,
                    },
                    .peer_node_ids = &.{},
                },
            });
            try std.testing.expectEqual(.absent, sim.status(1701));
            const groups = try sim.listGroupIds(std.testing.allocator);
            defer std.testing.allocator.free(groups);
            try std.testing.expectEqual(@as(usize, 0), groups.len);
        }

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try std.testing.expectEqual(.absent, sim.status(1701));
            try std.testing.expectEqual(@as(usize, 0), sim.pendingUpdates());
            const groups = try sim.listGroupIds(std.testing.allocator);
            defer std.testing.allocator.free(groups);
            try std.testing.expectEqual(@as(usize, 0), groups.len);
        }
    }
}

test "managed host simulation does not persist proposals before a runtime round across both raft state backends" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const BackingCase = struct {
        backend: host.ReplicaStateBackend,
        label: []const u8,
    };
    const cases = [_]BackingCase{
        .{ .backend = .file_image, .label = "file-image-unpersisted-proposal" },
        .{ .backend = .wal, .label = "wal-unpersisted-proposal" },
    };

    for (cases) |case_cfg| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_root);
        const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}/catalog.txt", .{ tmp.sub_path, case_cfg.label });
        defer std.testing.allocator.free(replica_catalog_path);

        var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer dummy_store.deinit();
        var factory = Factory{ .alloc = std.testing.allocator, .store = &dummy_store };

        const group_id: u64 = 1801;
        var baseline_commit: u64 = 0;

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try sim.apply(.{
                .upsert_replica_intent = .{
                    .record = .{
                        .group_id = group_id,
                        .replica_id = 1,
                        .local_node_id = 1,
                    },
                    .peer_node_ids = &.{},
                },
            });
            _ = try sim.stepOnce();
            try sim.campaignGroup(group_id);
            _ = try sim.stepOnce();

            baseline_commit = if (sim.raftStatus(group_id)) |status|
                status.hard.commit_index
            else
                return error.MissingRaftStatus;
            try std.testing.expect(baseline_commit >= 1);

            try sim.propose(group_id, "not-yet-persisted");
        }

        {
            var sim = try ManagedHostSimulation.init(std.testing.allocator, .{
                .host = .{ .host = .{
                    .local_node_id = 1,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                    .replica_state_backend = case_cfg.backend,
                } },
            }, .{
                .host = .{ .host = .{ .descriptor_factory = factory.iface() } },
            });
            defer sim.deinit();

            try std.testing.expectEqual(.active, sim.status(group_id));
            const restored_commit = if (sim.raftStatus(group_id)) |status|
                status.hard.commit_index
            else
                return error.MissingRaftStatus;
            try std.testing.expectEqual(baseline_commit, restored_commit);

            try sim.campaignGroup(group_id);
            _ = try sim.stepOnce();
            try sim.propose(group_id, "persisted-after-restart");
            try std.testing.expect(try waitForCommitIndex(&sim, group_id, baseline_commit + 1, 32));
        }
    }
}

test "managed http host simulation starts listener and applies deterministic metadata updates" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{ record.local_node_id, 2 });
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-sim-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = root_dir },
                },
            },
        },
    }, .{
        .host = .{ .http = .{ .host = .{ .descriptor_factory = factory.iface() } } },
    });
    defer sim.deinit();

    try sim.start();
    defer sim.stop();

    const base_uri = try sim.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    try std.testing.expect(std.mem.startsWith(u8, base_uri, "http://127.0.0.1:"));

    try sim.applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 1401,
                    .replica_id = 14,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{2},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 1401,
                .node_id = 2,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = base_uri,
                        .metadata = "",
                    },
                },
            },
        },
    });

    const result = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 2), result.drained_updates);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.refreshed_peers);
    try std.testing.expectEqual(.active, sim.status(1401));
}

test "managed http host simulations elect and replicate over real HTTP" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    try persist_a.registerStore(2001, &store_a);
    try persist_b.registerStore(2001, &store_b);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2 },
    };

    var sim_a = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = root_a },
                },
            },
        },
    }, .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory_a.iface(),
                    .runtime_hooks = .{
                        .group_storage = persist_a.iface(),
                    },
                },
            },
        },
    });
    defer sim_a.deinit();

    var sim_b = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 2 },
                .transport = .{
                    .snapshot = .{ .root_dir = root_b },
                },
            },
        },
    }, .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory_b.iface(),
                    .runtime_hooks = .{
                        .group_storage = persist_b.iface(),
                    },
                },
            },
        },
    });
    defer sim_b.deinit();

    try sim_a.start();
    defer sim_a.stop();
    try sim_b.start();
    defer sim_b.stop();

    const base_a = try sim_a.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try sim_b.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);

    try sim_a.applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 2001,
                    .replica_id = 1,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{2},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 2001,
                .node_id = 2,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = base_b,
                        .metadata = "",
                    },
                },
            },
        },
    });
    try sim_b.applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 2001,
                    .replica_id = 2,
                    .local_node_id = 2,
                },
                .peer_node_ids = &.{1},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 2001,
                .node_id = 1,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = base_a,
                        .metadata = "",
                    },
                },
            },
        },
    });

    _ = try sim_a.stepOnce();
    _ = try sim_b.stepOnce();

    try sim_a.campaignGroup(2001);
    const leader = try waitForLeader(&sim_a, &sim_b, 2001, 64);
    try std.testing.expectEqual(@as(?u64, 1), leader);

    try sim_a.propose(2001, "hello-http");
    try std.testing.expect(try waitForLastIndex(&sim_a, &sim_b, &store_b, 2, 64));

    const entries = try store_b.storage().entries(std.testing.allocator, 1, 3, 0);
    defer raft_engine.core.types.freeEntries(std.testing.allocator, entries);
    var found_payload = false;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.data, "hello-http")) {
            found_payload = true;
            break;
        }
    }
    try std.testing.expect(found_payload);
}

test "managed http host simulation can remove and rejoin from HTTP snapshot fetch" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-rejoin-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-rejoin-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    try persist_a.registerStore(2002, &store_a);
    try persist_b.registerStore(2002, &store_b);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2 },
    };

    var sim_a = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = root_a },
                },
            },
        },
    }, .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory_a.iface(),
                    .runtime_hooks = .{
                        .group_storage = persist_a.iface(),
                    },
                },
            },
        },
    });
    defer sim_a.deinit();

    var sim_b = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 2 },
                .transport = .{
                    .snapshot = .{ .root_dir = root_b },
                },
            },
        },
    }, .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory_b.iface(),
                    .runtime_hooks = .{
                        .group_storage = persist_b.iface(),
                    },
                },
            },
        },
    });
    defer sim_b.deinit();

    try sim_a.start();
    defer sim_a.stop();
    try sim_b.start();
    defer sim_b.stop();

    const base_a = try sim_a.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);

    const snapshot_id = "rejoin-2002";
    const upload_path = try transport.Routes.snapshotUploadPath(std.testing.allocator, snapshot_id);
    defer std.testing.allocator.free(upload_path);
    const fetch_path = try transport.Routes.snapshotFetchPath(std.testing.allocator, snapshot_id);
    defer std.testing.allocator.free(fetch_path);
    const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, upload_path });
    defer std.testing.allocator.free(upload_uri);
    const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, fetch_path });
    defer std.testing.allocator.free(fetch_uri);

    const snapshot_voters = try std.testing.allocator.dupe(u64, &.{ 1, 2 });
    defer std.testing.allocator.free(snapshot_voters);
    const snapshot_data = try std.testing.allocator.dupe(u8, "snapshot-state");
    defer std.testing.allocator.free(snapshot_data);

    try sim_a.runtime.svc.host.http_host.transport_stack.snapshot_transport.transport().sendSnapshot(.{
        .group_id = 2002,
        .to = 1,
        .snapshot = .{
            .metadata = .{
                .index = 7,
                .term = 3,
                .conf_state = .{
                    .voters = snapshot_voters,
                },
            },
            .data = snapshot_data,
        },
        .locator = .{
            .snapshot_id = snapshot_id,
            .uri = upload_uri,
        },
    });

    try sim_b.applyBatch(&.{
        .{
            .upsert_peer_route = .{
                .group_id = 2002,
                .node_id = 1,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = base_a,
                        .metadata = "",
                    },
                },
            },
        },
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 2002,
                    .replica_id = 2,
                    .local_node_id = 2,
                },
                .peer_node_ids = &.{1},
            },
        },
    });
    _ = try sim_b.stepOnce();
    try std.testing.expectEqual(.active, sim_b.status(2002));

    try sim_b.apply(.{
        .remove_replica_intent = .{
            .group_id = 2002,
        },
    });
    _ = try sim_b.stepOnce();
    try std.testing.expectEqual(.absent, sim_b.status(2002));

    factory_b.fetch_from = 1;
    factory_b.fetch_snapshot_id = snapshot_id;
    factory_b.fetch_uri = fetch_uri;

    try sim_b.applyBatch(&.{
        .{
            .upsert_peer_route = .{
                .group_id = 2002,
                .node_id = 1,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = base_a,
                        .metadata = "",
                    },
                },
            },
        },
        .{
            .upsert_replica_intent = .{
                .record = .{
                    .group_id = 2002,
                    .replica_id = 2,
                    .local_node_id = 2,
                    .bootstrap_mode = .fetch_snapshot,
                },
                .peer_node_ids = &.{1},
            },
        },
    });
    _ = try sim_b.stepOnce();

    const last_index = try store_b.storage().lastIndex();
    try std.testing.expectEqual(@as(u64, 7), last_index);
    const snapshot = try store_b.storage().snapshot(std.testing.allocator);
    defer {
        var owned = snapshot;
        owned.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(u64, 7), snapshot.metadata.index);
    try std.testing.expectEqualStrings("snapshot-state", snapshot.data);
}

test "managed http cluster simulation drives three-node churn and admin workflows" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-cluster-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-cluster-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-cluster-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3014, &store_a);
    try persist_b.registerStore(3014, &store_b);
    try persist_c.registerStore(3014, &store_c);
    try persist_a.registerStore(3014, &store_a);
    try persist_b.registerStore(3014, &store_b);
    try persist_c.registerStore(3014, &store_c);
    try persist_a.registerStore(3001, &store_a);
    try persist_b.registerStore(3001, &store_b);
    try persist_c.registerStore(3001, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 1 },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 3 },
                    .transport = .{ .snapshot = .{ .root_dir = root_c } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory_a.iface(),
                        .runtime_hooks = .{ .group_storage = persist_a.iface() },
                    },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory_b.iface(),
                        .runtime_hooks = .{ .group_storage = persist_b.iface() },
                    },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory_c.iface(),
                        .runtime_hooks = .{ .group_storage = persist_c.iface() },
                    },
                },
            },
        },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{ .group_id = 3001, .replica_id = 1, .local_node_id = 1 },
                .peer_node_ids = &.{ 2, 3 },
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 2,
                .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 3,
                .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }},
            },
        },
    });
    try cluster.node(1).applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{ .group_id = 3001, .replica_id = 2, .local_node_id = 2 },
                .peer_node_ids = &.{ 1, 3 },
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 1,
                .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 3,
                .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }},
            },
        },
    });
    try cluster.node(2).applyBatch(&.{
        .{
            .upsert_replica_intent = .{
                .record = .{ .group_id = 3001, .replica_id = 3, .local_node_id = 3 },
                .peer_node_ids = &.{ 1, 2 },
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 1,
                .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }},
            },
        },
        .{
            .upsert_peer_route = .{
                .group_id = 3001,
                .node_id = 2,
                .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }},
            },
        },
    });

    try cluster.stepAll();
    try std.testing.expectEqual(.active, cluster.node(0).status(3001));
    try std.testing.expectEqual(.active, cluster.node(1).status(3001));
    try std.testing.expectEqual(.active, cluster.node(2).status(3001));

    try cluster.node(0).campaignGroup(3001);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3001, 64));

    try cluster.node(0).propose(3001, "cluster-warmup");
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_b, 2, 64));
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_c, 2, 64));

    try cluster.node(0).transferLeader(3001, 2);
    try std.testing.expect(try cluster.waitForLeaderId(3001, 2, 64));

    const conf_target = try store_b.storage().lastIndex() + 1;
    var changes = [_]raft_engine.core.ConfChangeSingle{
        .{ .change_type = .remove_node, .node_id = 3 },
    };
    try cluster.node(1).proposeConfChangeV2(3001, .{ .changes = changes[0..] });

    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_a, conf_target, 64));
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_b, conf_target, 64));
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_c, conf_target, 64));

    try cluster.node(2).applyBatch(&.{
        .{ .remove_peer_route = .{ .group_id = 3001, .node_id = 1 } },
        .{ .remove_peer_route = .{ .group_id = 3001, .node_id = 2 } },
        .{ .remove_replica_intent = .{ .group_id = 3001 } },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(.absent, cluster.node(2).status(3001));
}

test "managed http cluster simulation restarts a node and catches it back up through metadata replay" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-restart-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3002, &store_a);
    try persist_b.registerStore(3002, &store_b);
    try persist_c.registerStore(3002, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3002, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });
    const initial_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(initial_c);
    try cluster.node(0).apply(.{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = initial_c, .metadata = "" }} } });

    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3002, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = initial_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3002, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3002);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3002, 64));

    try cluster.node(0).propose(3002, "before-restart");
    try std.testing.expect(try cluster.waitForLastIndex(&store_c, 2, 64));

    try cluster.restartNode(2);
    try std.testing.expectEqual(.absent, cluster.node(2).status(3002));

    const restarted_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(restarted_c);
    try cluster.node(0).apply(.{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = restarted_c, .metadata = "" }} } });
    try cluster.node(1).apply(.{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = restarted_c, .metadata = "" }} } });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3002, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3002, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try std.testing.expectEqual(.active, cluster.node(2).status(3002));

    try cluster.node(0).propose(3002, "after-restart");
    try std.testing.expect(try cluster.waitForLastIndex(&store_c, 3, 64));

    const entries = try store_c.storage().entries(std.testing.allocator, 1, 4, 0);
    defer raft_engine.core.types.freeEntries(std.testing.allocator, entries);
    var found = false;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.data, "after-restart")) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "managed http cluster simulation restarts a node with WAL-backed raft state" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);
    const catalog_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-a/catalog.txt", .{tmp_a.sub_path});
    defer std.testing.allocator.free(catalog_a);
    const catalog_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-b/catalog.txt", .{tmp_b.sub_path});
    defer std.testing.allocator.free(catalog_b);
    const catalog_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-wal-c/catalog.txt", .{tmp_c.sub_path});
    defer std.testing.allocator.free(catalog_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{
            .local_node_id = 1,
            .replica_root_dir = root_a,
            .replica_catalog_path = catalog_a,
            .replica_state_backend = .wal,
        }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{
            .local_node_id = 2,
            .replica_root_dir = root_b,
            .replica_catalog_path = catalog_b,
            .replica_state_backend = .wal,
        }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{
            .local_node_id = 3,
            .replica_root_dir = root_c,
            .replica_catalog_path = catalog_c,
            .replica_state_backend = .wal,
        }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface() } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface() } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface() } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3004, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });
    const initial_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(initial_c);
    try cluster.node(0).apply(.{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = initial_c, .metadata = "" }} } });

    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3004, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = initial_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3004, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3004);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3004, 64));

    try cluster.node(0).propose(3004, "before-restart");
    try std.testing.expect(try waitForCommitIndexInCluster(&cluster, 2, 3004, 2, 64));

    try cluster.restartNode(2);
    try std.testing.expectEqual(.active, cluster.node(2).status(3004));
    const baseline_commit = if (cluster.node(2).raftStatus(3004)) |status|
        status.hard.commit_index
    else
        return error.MissingRaftStatus;
    try std.testing.expect(baseline_commit >= 1);
    const leader_baseline_commit = if (cluster.node(0).raftStatus(3004)) |status|
        status.hard.commit_index
    else
        return error.MissingRaftStatus;

    const restarted_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(restarted_c);
    try cluster.node(0).apply(.{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = restarted_c, .metadata = "" }} } });
    try cluster.node(1).apply(.{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = restarted_c, .metadata = "" }} } });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3004, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3004, 64));
    try cluster.node(0).propose(3004, "after-restart");
    try std.testing.expect(try waitForCommitIndexInCluster(&cluster, 0, 3004, leader_baseline_commit + 1, 128));
}

test "managed http cluster simulation can remove and rejoin a node from HTTP snapshot fetch" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3003, &store_a);
    try persist_b.registerStore(3003, &store_b);
    try persist_c.registerStore(3003, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } } } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3003, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3003, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3003, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });
    try cluster.stepAll();

    const snapshot_id = "cluster-rejoin-3003";
    const upload_path = try transport.Routes.snapshotUploadPath(std.testing.allocator, snapshot_id);
    defer std.testing.allocator.free(upload_path);
    const fetch_path = try transport.Routes.snapshotFetchPath(std.testing.allocator, snapshot_id);
    defer std.testing.allocator.free(fetch_path);
    const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, upload_path });
    defer std.testing.allocator.free(upload_uri);
    const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, fetch_path });
    defer std.testing.allocator.free(fetch_uri);

    const snapshot_voters = try std.testing.allocator.dupe(u64, &.{ 1, 2, 3 });
    defer std.testing.allocator.free(snapshot_voters);
    const snapshot_data = try std.testing.allocator.dupe(u8, "cluster-snapshot-state");
    defer std.testing.allocator.free(snapshot_data);

    try cluster.node(0).runtime.svc.host.http_host.transport_stack.snapshot_transport.transport().sendSnapshot(.{
        .group_id = 3003,
        .to = 1,
        .snapshot = .{
            .metadata = .{
                .index = 9,
                .term = 4,
                .conf_state = .{ .voters = snapshot_voters },
            },
            .data = snapshot_data,
        },
        .locator = .{ .snapshot_id = snapshot_id, .uri = upload_uri },
    });

    try cluster.node(2).applyBatch(&.{
        .{ .remove_peer_route = .{ .group_id = 3003, .node_id = 1 } },
        .{ .remove_peer_route = .{ .group_id = 3003, .node_id = 2 } },
        .{ .remove_replica_intent = .{ .group_id = 3003 } },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(.absent, cluster.node(2).status(3003));

    factory_c.fetch_from = 1;
    factory_c.fetch_snapshot_id = snapshot_id;
    factory_c.fetch_uri = fetch_uri;

    try cluster.node(2).applyBatch(&.{
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3003, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3003, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .fetch_snapshot }, .peer_node_ids = &.{ 1, 2 } } },
    });
    try cluster.stepAll();

    try std.testing.expectEqual(.active, cluster.node(2).status(3003));
    try std.testing.expectEqual(@as(u64, 9), try store_c.storage().lastIndex());
    const snapshot = try store_c.storage().snapshot(std.testing.allocator);
    defer {
        var owned = snapshot;
        owned.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(u64, 9), snapshot.metadata.index);
    try std.testing.expectEqualStrings("cluster-snapshot-state", snapshot.data);
}

test "managed http cluster simulation restarts a rejoined node from persisted snapshot state across both raft state backends" {
    const BackingCase = struct {
        backend: host.ReplicaStateBackend,
        label: []const u8,
        snapshot_data: []const u8,
    };
    const cases = [_]BackingCase{
        .{ .backend = .file_image, .label = "file-image", .snapshot_data = "cluster-snapshot-state-file-image" },
        .{ .backend = .wal, .label = "wal", .snapshot_data = "cluster-snapshot-state-wal" },
    };

    for (cases) |case_cfg| {
        var tmp_a = std.testing.tmpDir(.{});
        defer tmp_a.cleanup();
        var tmp_b = std.testing.tmpDir(.{});
        defer tmp_b.cleanup();
        var tmp_c = std.testing.tmpDir(.{});
        defer tmp_c.cleanup();

        const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-a", .{ tmp_a.sub_path, case_cfg.label });
        defer std.testing.allocator.free(root_a);
        const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-b", .{ tmp_b.sub_path, case_cfg.label });
        defer std.testing.allocator.free(root_b);
        const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-c", .{ tmp_c.sub_path, case_cfg.label });
        defer std.testing.allocator.free(root_c);
        const catalog_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-a/catalog.txt", .{ tmp_a.sub_path, case_cfg.label });
        defer std.testing.allocator.free(catalog_a);
        const catalog_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-b/catalog.txt", .{ tmp_b.sub_path, case_cfg.label });
        defer std.testing.allocator.free(catalog_b);
        const catalog_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-http-snap-{s}-c/catalog.txt", .{ tmp_c.sub_path, case_cfg.label });
        defer std.testing.allocator.free(catalog_c);

        var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer store_a.deinit();
        var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer store_b.deinit();
        var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
        defer store_c.deinit();

        var factory_a = SimulationDescriptorFactory{
            .alloc = std.testing.allocator,
            .store = &store_a,
            .peers = &.{ 1, 2, 3 },
        };
        var factory_b = SimulationDescriptorFactory{
            .alloc = std.testing.allocator,
            .store = &store_b,
            .peers = &.{ 1, 2, 3 },
        };
        var factory_c = SimulationDescriptorFactory{
            .alloc = std.testing.allocator,
            .store = &store_c,
            .peers = &.{ 1, 2, 3 },
        };

        const configs = [_]ManagedHttpHostSimulationConfig{
            .{ .host = .{ .http = .{ .host = .{
                .local_node_id = 1,
                .replica_root_dir = root_a,
                .replica_catalog_path = catalog_a,
                .replica_state_backend = case_cfg.backend,
            }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
            .{ .host = .{ .http = .{ .host = .{
                .local_node_id = 2,
                .replica_root_dir = root_b,
                .replica_catalog_path = catalog_b,
                .replica_state_backend = case_cfg.backend,
            }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
            .{ .host = .{ .http = .{ .host = .{
                .local_node_id = 3,
                .replica_root_dir = root_c,
                .replica_catalog_path = catalog_c,
                .replica_state_backend = case_cfg.backend,
            }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
        };
        const deps = [_]ManagedHttpHostSimulationDeps{
            .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface() } } } },
            .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface() } } } },
            .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface() } } } },
        };

        var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
        defer cluster.deinit();
        try cluster.startAll();
        defer cluster.stopAll();

        const base_a = try cluster.node(0).baseUri(std.testing.allocator);
        defer std.testing.allocator.free(base_a);
        const base_b = try cluster.node(1).baseUri(std.testing.allocator);
        defer std.testing.allocator.free(base_b);
        const base_c = try cluster.node(2).baseUri(std.testing.allocator);
        defer std.testing.allocator.free(base_c);

        try cluster.node(0).applyBatch(&.{
            .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3005, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
        });
        try cluster.node(1).applyBatch(&.{
            .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3005, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
        });
        try cluster.node(2).applyBatch(&.{
            .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3005, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        });
        try cluster.stepAll();

        const snapshot_id = "cluster-rejoin-3005";
        const upload_path = try transport.Routes.snapshotUploadPath(std.testing.allocator, snapshot_id);
        defer std.testing.allocator.free(upload_path);
        const fetch_path = try transport.Routes.snapshotFetchPath(std.testing.allocator, snapshot_id);
        defer std.testing.allocator.free(fetch_path);
        const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, upload_path });
        defer std.testing.allocator.free(upload_uri);
        const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_a, fetch_path });
        defer std.testing.allocator.free(fetch_uri);

        const snapshot_voters = try std.testing.allocator.dupe(u64, &.{ 1, 2, 3 });
        defer std.testing.allocator.free(snapshot_voters);
        const snapshot_data = try std.testing.allocator.dupe(u8, case_cfg.snapshot_data);
        defer std.testing.allocator.free(snapshot_data);

        try cluster.node(0).runtime.svc.host.http_host.transport_stack.snapshot_transport.transport().sendSnapshot(.{
            .group_id = 3005,
            .to = 1,
            .snapshot = .{
                .metadata = .{
                    .index = 9,
                    .term = 4,
                    .conf_state = .{ .voters = snapshot_voters },
                },
                .data = snapshot_data,
            },
            .locator = .{ .snapshot_id = snapshot_id, .uri = upload_uri },
        });

        try cluster.node(2).applyBatch(&.{
            .{ .remove_peer_route = .{ .group_id = 3005, .node_id = 1 } },
            .{ .remove_peer_route = .{ .group_id = 3005, .node_id = 2 } },
            .{ .remove_replica_intent = .{ .group_id = 3005 } },
        });
        try cluster.stepAll();
        try std.testing.expectEqual(.absent, cluster.node(2).status(3005));

        factory_c.fetch_from = 1;
        factory_c.fetch_snapshot_id = snapshot_id;
        factory_c.fetch_uri = fetch_uri;

        try cluster.node(2).applyBatch(&.{
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
            .{ .upsert_peer_route = .{ .group_id = 3005, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
            .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3005, .replica_id = 3, .local_node_id = 3, .bootstrap_mode = .fetch_snapshot }, .peer_node_ids = &.{ 1, 2 } } },
        });
        try cluster.stepAll();
        try std.testing.expectEqual(.active, cluster.node(2).status(3005));

        try cluster.restartNode(2);
        try std.testing.expectEqual(.active, cluster.node(2).status(3005));
        if (cluster.node(2).raftStatus(3005)) |status| {
            try std.testing.expect(status.hard.commit_index >= 1);
        } else return error.MissingRaftStatus;
    }
}

test "simulation harness module compiles" {
    _ = ManagedHostSimulationConfig;
    _ = ManagedHostSimulationDeps;
    _ = ManagedHostSimulation;
    _ = ManagedHttpHostSimulationConfig;
    _ = ManagedHttpHostSimulationDeps;
    _ = ManagedHttpHostSimulation;
    _ = ManagedHttpClusterSimulation;
    _ = StorageRecorder;
    _ = SimulationDescriptorFactory;
}

test "cluster simulation validates mirrored merge pair invariants" {
    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, &.{}, &.{});
    defer cluster.deinit();

    const donor = data_mod.storage.range_transition.deriveMergeStatus(
        41,
        42,
        true,
        true,
        7,
        7,
        false,
        false,
        false,
    );
    const receiver = data_mod.storage.range_transition.deriveMergeStatus(
        41,
        42,
        true,
        true,
        7,
        7,
        false,
        false,
        false,
    );

    try cluster.validateMirroredMergePair(donor, receiver);
}

test "cluster simulation validates split transition enrichment invariants" {
    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, &.{}, &.{});
    defer cluster.deinit();

    const replay = data_mod.storage.range_transition.deriveSplitStatus(.splitting, true, 9, 8);
    try std.testing.expectError(
        error.DestinationOwnsTransitionRangeBeforeCutover,
        cluster.validateSplitTransitionEnrichment(replay, true),
    );

    const cutover = data_mod.storage.range_transition.deriveSplitStatus(.splitting, true, 9, 9);
    try cluster.validateSplitTransitionEnrichment(cutover, true);
    try cluster.validateSplitTransitionEnrichment(cutover, false);

    const rolled_back = data_mod.storage.range_transition.deriveSplitStatus(.rolling_back, true, 9, 9);
    try std.testing.expectError(
        error.DestinationOwnsTransitionRangeBeforeCutover,
        cluster.validateSplitTransitionEnrichment(rolled_back, true),
    );
}

test "cluster simulation validates merge transition enrichment invariants" {
    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, &.{}, &.{});
    defer cluster.deinit();

    const replay_donor = data_mod.storage.range_transition.deriveMergeStatus(
        51,
        52,
        true,
        true,
        7,
        6,
        false,
        false,
        false,
    );
    const replay_receiver = data_mod.storage.range_transition.deriveMergeStatus(
        51,
        52,
        true,
        true,
        7,
        6,
        false,
        false,
        false,
    );
    try std.testing.expectError(
        error.ReceiverOwnsMergedRangeBeforeCutover,
        cluster.validateMergeTransitionEnrichment(replay_donor, replay_receiver, false, true),
    );

    const finalized_donor = data_mod.storage.range_transition.deriveMergeStatus(
        51,
        52,
        true,
        true,
        7,
        7,
        false,
        true,
        false,
    );
    const finalized_receiver = data_mod.storage.range_transition.deriveMergeStatus(
        51,
        52,
        true,
        true,
        7,
        7,
        false,
        true,
        false,
    );
    try std.testing.expectError(
        error.DonorOwnsMergedRangeAfterFinalize,
        cluster.validateMergeTransitionEnrichment(finalized_donor, finalized_receiver, true, true),
    );
    try cluster.validateMergeTransitionEnrichment(finalized_donor, finalized_receiver, false, true);
}

test "cluster simulation drives split transition actions deterministically" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status = .{
                .phase = .bootstrap_peer,
                .source_split_phase = .splitting,
                .bootstrapped = false,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 1,
                .dest_delta_sequence = 0,
            };
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status = .{
                .phase = .replay_deltas,
                .source_split_phase = .splitting,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 1,
            };
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status = .{
                .phase = .cutover_ready,
                .source_split_phase = .finalizing,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .destination_ready_for_reads = true,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 2,
            };
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status = .{
                .phase = .finalized,
                .source_split_phase = .none,
                .bootstrapped = true,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = true,
                .destination_ready_for_reads = true,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 2,
            };
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status = .{
                .phase = .rolled_back,
                .source_split_phase = .none,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 0,
                .dest_delta_sequence = 0,
            };
            return true;
        }
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, &.{}, &.{});
    defer cluster.deinit();

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    const runtime = transition_runtime_mod.TransitionRuntime{ .split = split.iface() };
    var record = metadata_mod.SplitTransitionRecord{
        .transition_id = 9001,
        .source_group_id = 101,
        .destination_group_id = 102,
    };

    try cluster.driveSplitTransition(runtime, &record, 8);

    try std.testing.expectEqual(metadata_mod.TransitionPhase.finalized, record.phase);
    try std.testing.expectEqual(@as(usize, 4), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[1]);
    try std.testing.expectEqualStrings("catchup", split.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[3]);
}

test "http host simulation drives queued split transitions through the service lane" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 2;
            self.status.dest_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 2;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .split = split.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9101,
            .source_group_id = 101,
            .destination_group_id = 102,
        },
    });

    _ = try sim.stepOnce();
    const observed = (try sim.observeSplitTransition(9101)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.bootstrap_peer, observed.status.phase);
    const described = (try sim.describeSplitTransition(9101)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, described.tag);
    try std.testing.expect(described.actionable());
    try std.testing.expect(described.action == .bootstrap_split_destination);
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();

    const metrics = sim.serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.awaiting_split_source_start);
    try std.testing.expectEqual(@as(usize, 1), metrics.bootstrapping_split_destination);
    try std.testing.expectEqual(@as(usize, 1), metrics.split_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), metrics.split_ready_to_finalize);
    try std.testing.expectEqual(@as(usize, 4), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[1]);
    try std.testing.expectEqualStrings("catchup", split.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[3]);
}

test "http host simulation rolls back and retries queued split transitions through the service lane" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn reset(self: *@This()) void {
            self.status = .{
                .phase = .prepare,
                .source_split_phase = .prepare,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 0,
                .dest_delta_sequence = 0,
            };
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status.phase = .cutover_ready;
            self.status.source_split_phase = .finalizing;
            self.status.bootstrapped = true;
            self.status.replay_required = true;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.source_delta_sequence = 1;
            self.status.dest_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status.phase = .rolled_back;
            self.status.source_split_phase = .none;
            self.status.bootstrapped = false;
            self.status.replay_required = false;
            self.status.replay_caught_up = false;
            self.status.cutover_ready = false;
            self.status.destination_ready_for_reads = false;
            self.status.source_delta_sequence = 0;
            self.status.dest_delta_sequence = 0;
            return true;
        }
    };

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-split-rollback", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .split = split.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9151,
            .source_group_id = 151,
            .destination_group_id = 152,
            .rollback_reason = "operator abort",
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), split.calls.items.len);
    try std.testing.expectEqualStrings("rollback", split.calls.items[0]);

    split.reset();
    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9151,
            .source_group_id = 151,
            .destination_group_id = 152,
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();

    const metrics = sim.serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 2), metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 4), split.calls.items.len);
    try std.testing.expectEqualStrings("rollback", split.calls.items[0]);
    try std.testing.expectEqualStrings("start", split.calls.items[1]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[3]);
}

test "http host simulation removes queued split transition mid-flight" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            return true;
        }

        fn catchUpDestination(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-split-remove", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .split = split.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9152,
            .source_group_id = 161,
            .destination_group_id = 162,
        },
    });
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);

    try sim.apply(.{ .remove_split_transition = .{ .transition_id = 9152 } });
    _ = try sim.stepOnce();

    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), split.calls.items.len);
}

test "http host simulation updates split transition to rollback mid-flight" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn catchUpDestination(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status.phase = .rolled_back;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }
    };

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-split-rollback-update", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .split = split.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9153,
            .source_group_id = 171,
            .destination_group_id = 172,
        },
    });
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().queued_split_transitions);

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9153,
            .source_group_id = 171,
            .destination_group_id = 172,
            .rollback_reason = "operator abort",
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();

    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 2), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("rollback", split.calls.items[1]);
}

test "http host simulation drives queued split transitions through the service lane with real split coordinator" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 101,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 101,
        .dest_group_id = 102,
    });
    defer split.deinit();

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .split = split.runtime() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_split_transition = .{
            .transition_id = 9102,
            .source_group_id = 101,
            .destination_group_id = 102,
        },
    });

    const first = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), first.drained_updates);
    const after_start = (try sim.describeSplitTransition(9102)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, after_start.tag);

    _ = try sim.stepOnce();
    const after_bootstrap = (try sim.describeSplitTransition(9102)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.ready_to_finalize, after_bootstrap.tag);

    var rounds: usize = 0;
    while (rounds < 8 and sim.serviceMetrics().completed_split_transitions == 0) : (rounds += 1) {
        _ = try sim.stepOnce();
    }

    const metrics = sim.serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.awaiting_split_source_start);
    try std.testing.expectEqual(@as(usize, 1), metrics.bootstrapping_split_destination);
    try std.testing.expectEqual(@as(usize, 0), metrics.split_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), metrics.split_ready_to_finalize);

    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    const range = dest.getRange();
    try std.testing.expectEqualStrings("doc:m", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const right = (try dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", right);
}

test "cluster simulation drives merge transition actions deterministically" {
    const StatefulMerge = struct {
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 201,
            .receiver_group_id = 202,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .record_doc_identity_reassignment = recordDocIdentityReassignment,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn recordDocIdentityReassignment(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "record_identity");
            self.status.allow_doc_identity_reassignment = true;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "accept");
            self.status = .{
                .phase = .bootstrap_peer,
                .donor_group_id = 201,
                .receiver_group_id = 202,
                .receiver_accepts_donor_range = true,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status = .{
                .phase = .cutover_ready,
                .donor_group_id = 201,
                .receiver_group_id = 202,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .receiver_ready_for_reads = true,
                .donor_delta_sequence = 3,
                .receiver_delta_sequence = 3,
            };
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status = .{
                .phase = .finalized,
                .donor_group_id = 201,
                .receiver_group_id = 202,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = true,
                .receiver_ready_for_reads = true,
                .donor_delta_sequence = 3,
                .receiver_delta_sequence = 3,
            };
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status = .{
                .phase = .rolled_back,
                .donor_group_id = 201,
                .receiver_group_id = 202,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
            return true;
        }
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, &.{}, &.{});
    defer cluster.deinit();

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    const runtime = transition_runtime_mod.TransitionRuntime{ .merge = merge.iface() };
    var record = metadata_mod.MergeTransitionRecord{
        .transition_id = 9002,
        .donor_group_id = 201,
        .receiver_group_id = 202,
    };

    try cluster.driveMergeTransition(runtime, &record, 8);

    try std.testing.expectEqual(metadata_mod.TransitionPhase.finalized, record.phase);
    try std.testing.expectEqual(@as(usize, 3), merge.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge.calls.items[0]);
    try std.testing.expectEqualStrings("catchup", merge.calls.items[1]);
    try std.testing.expectEqualStrings("finalize", merge.calls.items[2]);
}

test "cluster simulation drives queued split transitions through service-owned metadata updates" {
    const StatefulSplit = struct {
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "prepare");
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 2;
            self.status.dest_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 2;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();

    const snapshot_root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(snapshot_root_a);
    const snapshot_root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(snapshot_root_b);

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 1 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .service = .{ .transition_runtime = .{ .split = split.iface() } } },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9201,
            .source_group_id = 401,
            .destination_group_id = 402,
        },
    });

    var rounds: usize = 0;
    while (rounds < 8 and cluster.node(0).serviceMetrics().completed_split_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 4), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[1]);
    try std.testing.expectEqualStrings("catchup", split.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[3]);
}

test "cluster simulation drives queued split transitions through service-owned metadata updates with real split coordinator" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();

    const snapshot_root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-real-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(snapshot_root_a);
    const snapshot_root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-real-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(snapshot_root_b);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-real-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-real-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 401,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 401,
        .dest_group_id = 402,
    });
    defer split.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 1 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .service = .{ .transition_runtime = .{ .split = split.runtime() } } },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9202,
            .source_group_id = 401,
            .destination_group_id = 402,
        },
    });

    var rounds: usize = 0;
    while (rounds < 8 and cluster.node(0).serviceMetrics().completed_split_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.awaiting_split_source_start);
    try std.testing.expectEqual(@as(usize, 1), metrics.bootstrapping_split_destination);
    try std.testing.expectEqual(@as(usize, 0), metrics.split_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), metrics.split_ready_to_finalize);

    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    const range = dest.getRange();
    try std.testing.expectEqualStrings("doc:m", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const right = (try dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", right);
}

test "cluster simulation resumes queued split transitions after node restart with real split coordinator" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-restart-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-restart-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1701,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9401,
                .source_group_id = 1701,
                .destination_group_id = 1702,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 1701,
        .dest_group_id = 1702,
    });
    defer split.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .split = split.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const metadata_store_before = cluster.node(0).runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
    const projected_before = try metadata_store_before.listSplitTransitions(std.testing.allocator, 1300);
    defer metadata_store_before.freeSplitTransitions(std.testing.allocator, projected_before);
    try std.testing.expectEqual(@as(usize, 1), projected_before.len);
    try std.testing.expectEqual(@as(u64, 9401), projected_before[0].transition_id);

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9401)) |state| {
            if (state.tag == .bootstrapping_destination) break;
        }
    }
    const before_restart = (try cluster.node(0).describeSplitTransition(9401)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, before_restart.tag);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().queued_split_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_split_transitions);
    const resumed = (try cluster.node(0).describeSplitTransition(9401)) orelse return error.TestExpectedEqual;
    try std.testing.expect(resumed.tag == .bootstrapping_destination or resumed.tag == .ready_to_finalize);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().completed_split_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);

    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    const range = dest.getRange();
    try std.testing.expectEqualStrings("doc:m", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const right = (try dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", right);
}

test "cluster simulation removes queued split transition mid-flight across node restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-remove-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-remove-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-remove-restart-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-remove-restart-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1901,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9501,
                .source_group_id = 1901,
                .destination_group_id = 1902,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 1901,
        .dest_group_id = 1902,
    });
    defer split.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .split = split.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9501)) |state| {
            if (state.tag == .bootstrapping_destination) break;
        }
    }
    const before_remove = (try cluster.node(0).describeSplitTransition(9501)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, before_remove.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .remove_split_transition = .{ .transition_id = 9501 },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{ .remove_split_transition = .{ .transition_id = 9501 } });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().completed_split_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().completed_split_transitions);
    try std.testing.expectEqual(@as(?metadata_mod.SplitObservation, null), try cluster.node(0).observeSplitTransition(9501));
    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    try std.testing.expectEqual(@as(?[]u8, null), try dest.get(std.testing.allocator, "doc:t"));

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9502,
                .source_group_id = 1901,
                .destination_group_id = 1902,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9502,
            .source_group_id = 1901,
            .destination_group_id = 1902,
        },
    });

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9502)) |state| {
            try std.testing.expect(state.tag == .bootstrapping_destination or state.tag == .ready_to_finalize);
            break;
        }
    }
    const retried_split = (try cluster.node(0).describeSplitTransition(9502)) orelse return error.TestExpectedEqual;
    try std.testing.expect(retried_split.tag == .bootstrapping_destination or retried_split.tag == .ready_to_finalize);
}

test "cluster simulation rolls back queued split transition mid-flight across node restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-rollback-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-rollback-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-rollback-restart-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-rollback-restart-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1911,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9511,
                .source_group_id = 1911,
                .destination_group_id = 1912,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 1911,
        .dest_group_id = 1912,
    });
    defer split.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .split = split.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9511)) |state| {
            if (state.tag == .bootstrapping_destination) break;
        }
    }
    const before_rollback = (try cluster.node(0).describeSplitTransition(9511)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, before_rollback.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9511,
                .source_group_id = 1911,
                .destination_group_id = 1912,
                .phase = .prepare,
                .rollback_reason = "operator abort",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9511,
            .source_group_id = 1911,
            .destination_group_id = 1912,
            .rollback_reason = "operator abort",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_split_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().completed_split_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9511);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
        const source_range = try source.currentRange(std.testing.allocator, 1911);
        defer {
            std.testing.allocator.free(source_range.start);
            std.testing.allocator.free(source_range.end);
        }
        try std.testing.expectEqualStrings("doc:a", source_range.start);
        try std.testing.expectEqualStrings("doc:z", source_range.end);
        const source_split = try source.currentSplitState(std.testing.allocator, 1911);
        defer if (source_split) |state| {
            std.testing.allocator.free(@constCast(state.split_key));
            std.testing.allocator.free(@constCast(state.original_range_end));
        };
        try std.testing.expect(source_split == null);
    }

    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    try std.testing.expectEqual(@as(?[]u8, null), try dest.get(std.testing.allocator, "doc:t"));
    const range = dest.getRange();
    try std.testing.expectEqualStrings("", range.start);
    try std.testing.expectEqualStrings("", range.end);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9511,
                .source_group_id = 1911,
                .destination_group_id = 1912,
                .phase = .prepare,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9511,
            .source_group_id = 1911,
            .destination_group_id = 1912,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
        },
    });

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9511)) |state| {
            try std.testing.expect(state.tag == .awaiting_source_start or state.tag == .bootstrapping_destination or state.tag == .ready_to_finalize);
            if (state.tag != .awaiting_source_start) break;
        }
    }
    const retried_split = (try cluster.node(0).describeSplitTransition(9511)) orelse return error.TestExpectedEqual;
    try std.testing.expect(retried_split.tag == .bootstrapping_destination or retried_split.tag == .ready_to_finalize);
}

test "cluster simulation survives repeated same-id split overwrites across restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-overwrite-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-overwrite-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-overwrite-restart-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-split-overwrite-restart-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1921,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9521,
                .source_group_id = 1921,
                .destination_group_id = 1922,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 1921,
        .dest_group_id = 1922,
    });
    defer split.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .split = split.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeSplitTransition(9521)) |state| {
            if (state.tag == .bootstrapping_destination) break;
        }
    }
    const before_rollback = (try cluster.node(0).describeSplitTransition(9521)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.SplitExecutionStateTag.bootstrapping_destination, before_rollback.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9521,
                .source_group_id = 1921,
                .destination_group_id = 1922,
                .phase = .prepare,
                .rollback_reason = "operator abort 1",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9521,
            .source_group_id = 1921,
            .destination_group_id = 1922,
            .rollback_reason = "operator abort 1",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_split_transitions);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9521,
                .source_group_id = 1921,
                .destination_group_id = 1922,
                .phase = .prepare,
                .rollback_reason = "operator abort 2",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9521,
            .source_group_id = 1921,
            .destination_group_id = 1922,
            .rollback_reason = "operator abort 2",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().completed_split_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9521);

    try cluster.restartNode(0);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9521,
                .source_group_id = 1921,
                .destination_group_id = 1922,
                .phase = .prepare,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 4,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9521,
            .source_group_id = 1921,
            .destination_group_id = 1922,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
        },
    });

    rounds = 0;
    while (rounds < 24 and cluster.node(0).serviceMetrics().completed_split_transitions < 2) : (rounds += 1) {
        try cluster.stepAll();
    }
    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_split_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9521);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
        const source_range = try source.currentRange(std.testing.allocator, 1921);
        defer {
            std.testing.allocator.free(source_range.start);
            std.testing.allocator.free(source_range.end);
        }
        try std.testing.expectEqualStrings("doc:a", source_range.start);
        try std.testing.expectEqualStrings("doc:m", source_range.end);
        const source_state = try source.groupState(std.testing.allocator, 1921);
        defer {
            for (source_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(source_state);
        }
        try std.testing.expectEqual(@as(usize, 1), source_state.len);
        try std.testing.expectEqualStrings("doc:b", source_state[0].key);
        try std.testing.expectEqualStrings("{\"v\":\"left-0\"}", source_state[0].value);
    }

    var dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
    defer dest.deinit();
    const dest_range = dest.getRange();
    try std.testing.expectEqualStrings("doc:m", dest_range.start);
    try std.testing.expectEqualStrings("doc:z", dest_range.end);
    const right = (try dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", right);
}

test "cluster simulation drives queued merge transitions through service-owned metadata updates" {
    const StatefulMerge = struct {
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 501,
            .receiver_group_id = 502,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .record_doc_identity_reassignment = recordDocIdentityReassignment,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn recordDocIdentityReassignment(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "record_identity");
            self.status.allow_doc_identity_reassignment = true;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "accept");
            self.status.phase = .bootstrap_peer;
            self.status.receiver_accepts_donor_range = true;
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status.phase = .cutover_ready;
            self.status.bootstrapped = true;
            self.status.replay_required = true;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.receiver_ready_for_reads = true;
            self.status.donor_delta_sequence = 3;
            self.status.receiver_delta_sequence = 3;
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status.phase = .finalized;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();

    const snapshot_root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(snapshot_root_a);
    const snapshot_root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(snapshot_root_b);

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 1 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .service = .{ .transition_runtime = .{ .merge = merge.iface() } } },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9202,
            .donor_group_id = 501,
            .receiver_group_id = 502,
            .allow_doc_identity_reassignment = true,
        },
    });

    var rounds: usize = 0;
    while (rounds < 8 and cluster.node(0).serviceMetrics().completed_merge_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 6), merge.calls.items.len);
    try std.testing.expectEqualStrings("record_identity", merge.calls.items[0]);
    try std.testing.expectEqualStrings("accept", merge.calls.items[1]);
    try std.testing.expectEqualStrings("record_identity", merge.calls.items[2]);
    try std.testing.expectEqualStrings("catchup", merge.calls.items[3]);
    try std.testing.expectEqualStrings("record_identity", merge.calls.items[4]);
    try std.testing.expectEqualStrings("finalize", merge.calls.items[5]);
    try std.testing.expect(merge.status.allow_doc_identity_reassignment);
}

test "http host simulation drives queued merge transitions through the service lane with real merge coordinator" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-merge-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-merge-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-real-merge-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);
    const old_receiver_namespace = doc_identity.Namespace{ .table_id = 9, .shard_id = 202, .range_id = 9202 };
    const target_receiver_namespace = doc_identity.Namespace{ .table_id = 9, .shard_id = 202, .range_id = 9302 };

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 201,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = .{ .identity_namespace = old_receiver_namespace },
        });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 201,
        .receiver_group_id = 202,
        .receiver = .{
            .root_dir = "",
            .db = .{
                .identity_namespace = old_receiver_namespace,
                .prefer_existing_identity_namespace = true,
            },
        },
        .receiver_identity_reassignment_namespace = target_receiver_namespace,
    });
    defer merge.deinit();

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .merge = merge.runtime() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9302,
            .donor_group_id = 201,
            .receiver_group_id = 202,
            .allow_doc_identity_reassignment = true,
        },
    });

    const first = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), first.drained_updates);
    const after_accept = (try sim.describeMergeTransition(9302)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.MergeExecutionStateTag.bootstrapping_receiver, after_accept.tag);

    var rounds: usize = 0;
    while (rounds < 8 and sim.serviceMetrics().completed_merge_transitions == 0) : (rounds += 1) {
        _ = try sim.stepOnce();
    }

    const metrics = sim.serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.awaiting_merge_receiver_acceptance);
    try std.testing.expectEqual(@as(usize, 1), metrics.bootstrapping_merge_receiver);
    try std.testing.expectEqual(@as(usize, 0), metrics.merge_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), metrics.merge_ready_to_finalize);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{
        .root_dir = receiver_root,
        .db = .{
            .identity_namespace = target_receiver_namespace,
            .prefer_existing_identity_namespace = true,
        },
    });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(donor_doc);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
    const stats = try receiver.db.diagnosticStats(std.testing.allocator);
    defer db_types.freeDBStats(std.testing.allocator, stats);
    try std.testing.expectEqual(target_receiver_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(target_receiver_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(target_receiver_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u64, 2), stats.doc_identity.live_ordinals);
}

test "http host simulation rolls back and retries queued merge transitions through the service lane" {
    const StatefulMerge = struct {
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 251,
            .receiver_group_id = 252,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn reset(self: *@This()) void {
            self.status = .{
                .phase = .prepare,
                .donor_group_id = 251,
                .receiver_group_id = 252,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
        }

        fn iface(self: *@This()) transition_runtime_mod.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "accept");
            self.status.phase = .bootstrap_peer;
            self.status.receiver_accepts_donor_range = true;
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status.phase = .cutover_ready;
            self.status.bootstrapped = true;
            self.status.replay_required = true;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.receiver_ready_for_reads = true;
            self.status.donor_delta_sequence = 1;
            self.status.receiver_delta_sequence = 1;
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status.phase = .finalized;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status.phase = .rolled_back;
            self.status.receiver_accepts_donor_range = false;
            self.status.bootstrapped = false;
            self.status.replay_required = false;
            self.status.replay_caught_up = false;
            self.status.cutover_ready = false;
            self.status.receiver_ready_for_reads = false;
            self.status.donor_delta_sequence = 0;
            self.status.receiver_delta_sequence = 0;
            return true;
        }
    };

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-merge-rollback", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .merge = merge.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9251,
            .donor_group_id = 251,
            .receiver_group_id = 252,
            .rollback_reason = "operator abort",
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), merge.calls.items.len);
    try std.testing.expectEqualStrings("rollback", merge.calls.items[0]);

    merge.reset();
    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9251,
            .donor_group_id = 251,
            .receiver_group_id = 252,
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();

    const metrics = sim.serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 2), metrics.completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 4), merge.calls.items.len);
    try std.testing.expectEqualStrings("rollback", merge.calls.items[0]);
    try std.testing.expectEqualStrings("accept", merge.calls.items[1]);
    try std.testing.expectEqualStrings("catchup", merge.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", merge.calls.items[3]);
}

test "http host simulation removes queued merge transition mid-flight" {
    const StatefulMerge = struct {
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 261,
            .receiver_group_id = 262,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "accept");
            self.status.phase = .bootstrap_peer;
            self.status.receiver_accepts_donor_range = true;
        }

        fn catchUpReceiver(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-merge-remove", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .merge = merge.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9252,
            .donor_group_id = 261,
            .receiver_group_id = 262,
        },
    });
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), merge.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge.calls.items[0]);

    try sim.apply(.{ .remove_merge_transition = .{ .transition_id = 9252 } });
    _ = try sim.stepOnce();

    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), merge.calls.items.len);
}

test "http host simulation updates merge transition to rollback mid-flight" {
    const StatefulMerge = struct {
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 271,
            .receiver_group_id = 272,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime_mod.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data_mod.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "accept");
            self.status.phase = .bootstrap_peer;
            self.status.receiver_accepts_donor_range = true;
        }

        fn catchUpReceiver(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status.phase = .rolled_back;
            self.status.receiver_accepts_donor_range = false;
            return true;
        }
    };

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/service-lane-merge-rollback-update", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var sim = try ManagedHttpHostSimulation.init(std.testing.allocator, .{
        .host = .{
            .http = .{
                .host = .{ .local_node_id = 1 },
                .transport = .{ .snapshot = .{ .root_dir = snapshot_root } },
            },
        },
    }, .{
        .service = .{
            .transition_runtime = .{ .merge = merge.iface() },
        },
    });
    defer sim.deinit();

    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9253,
            .donor_group_id = 271,
            .receiver_group_id = 272,
        },
    });
    _ = try sim.stepOnce();
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().queued_merge_transitions);

    try sim.apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9253,
            .donor_group_id = 271,
            .receiver_group_id = 272,
            .rollback_reason = "operator abort",
        },
    });
    _ = try sim.stepOnce();
    _ = try sim.stepOnce();

    try std.testing.expectEqual(@as(usize, 0), sim.serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), sim.serviceMetrics().completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 2), merge.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge.calls.items[0]);
    try std.testing.expectEqualStrings("rollback", merge.calls.items[1]);
}

test "cluster simulation drives queued merge transitions through service-owned metadata updates with real merge coordinator" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const snapshot_root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-real-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(snapshot_root_a);
    const snapshot_root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-real-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(snapshot_root_b);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-real-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-real-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 501,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 501,
        .receiver_group_id = 502,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 1 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = snapshot_root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .service = .{ .transition_runtime = .{ .merge = merge.runtime() } } },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9303,
            .donor_group_id = 501,
            .receiver_group_id = 502,
        },
    });

    var rounds: usize = 0;
    while (rounds < 8 and cluster.node(0).serviceMetrics().completed_merge_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.awaiting_merge_receiver_acceptance);
    try std.testing.expectEqual(@as(usize, 1), metrics.bootstrapping_merge_receiver);
    try std.testing.expectEqual(@as(usize, 0), metrics.merge_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), metrics.merge_ready_to_finalize);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(donor_doc);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
}

test "cluster simulation resumes queued merge transitions after node restart with real merge coordinator" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-restart-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-restart-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1801,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9402,
                .donor_group_id = 1801,
                .receiver_group_id = 1802,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 1801,
        .receiver_group_id = 1802,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .merge = merge.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const metadata_store_before = cluster.node(0).runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
    const projected_before = try metadata_store_before.listMergeTransitions(std.testing.allocator, 1300);
    defer metadata_store_before.freeMergeTransitions(std.testing.allocator, projected_before);
    try std.testing.expectEqual(@as(usize, 1), projected_before.len);
    try std.testing.expectEqual(@as(u64, 9402), projected_before[0].transition_id);

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9402)) |state| {
            if (state.tag == .bootstrapping_receiver) break;
        }
    }
    const before_restart = (try cluster.node(0).describeMergeTransition(9402)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.MergeExecutionStateTag.bootstrapping_receiver, before_restart.tag);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().queued_merge_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_merge_transitions);
    const resumed = (try cluster.node(0).describeMergeTransition(9402)) orelse return error.TestExpectedEqual;
    try std.testing.expect(resumed.tag == .bootstrapping_receiver or resumed.tag == .ready_to_finalize);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().completed_merge_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(donor_doc);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
}

test "cluster simulation rolls back queued merge transition mid-flight across node restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-rollback-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-rollback-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-rollback-restart-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-rollback-restart-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1951,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9601,
                .donor_group_id = 1951,
                .receiver_group_id = 1952,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 1951,
        .receiver_group_id = 1952,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .merge = merge.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9601)) |state| {
            if (state.tag == .bootstrapping_receiver) break;
        }
    }
    const before_rollback = (try cluster.node(0).describeMergeTransition(9601)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.MergeExecutionStateTag.bootstrapping_receiver, before_rollback.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9601,
                .donor_group_id = 1951,
                .receiver_group_id = 1952,
                .phase = .prepare,
                .rollback_reason = "operator abort",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9601,
            .donor_group_id = 1951,
            .receiver_group_id = 1952,
            .rollback_reason = "operator abort",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_merge_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 16 and cluster.node(0).serviceMetrics().completed_merge_transitions == 0) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);
    try expectMergeTransitionInactive(&cluster, 0, 9601);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:m", range.end);
    try std.testing.expectEqual(@as(?[]u8, null), try receiver.get(std.testing.allocator, "doc:t"));

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9601,
                .donor_group_id = 1951,
                .receiver_group_id = 1952,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9601,
            .donor_group_id = 1951,
            .receiver_group_id = 1952,
        },
    });

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9601)) |state| {
            try std.testing.expect(state.tag == .awaiting_receiver_acceptance or state.tag == .bootstrapping_receiver or state.tag == .ready_to_finalize);
            if (state.tag != .awaiting_receiver_acceptance) break;
        }
    }
    const retried_merge = (try cluster.node(0).describeMergeTransition(9601)) orelse return error.TestExpectedEqual;
    try std.testing.expect(retried_merge.tag == .bootstrapping_receiver or retried_merge.tag == .ready_to_finalize);
}

test "cluster simulation survives repeated same-id merge overwrites across restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-overwrite-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-overwrite-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-overwrite-restart-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-overwrite-restart-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1971,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9621,
                .donor_group_id = 1971,
                .receiver_group_id = 1972,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 1971,
        .receiver_group_id = 1972,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .merge = merge.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9621)) |state| {
            if (state.tag == .bootstrapping_receiver) break;
        }
    }
    const before_rollback = (try cluster.node(0).describeMergeTransition(9621)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.MergeExecutionStateTag.bootstrapping_receiver, before_rollback.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9621,
                .donor_group_id = 1971,
                .receiver_group_id = 1972,
                .phase = .prepare,
                .rollback_reason = "operator abort 1",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9621,
            .donor_group_id = 1971,
            .receiver_group_id = 1972,
            .rollback_reason = "operator abort 1",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().queued_merge_transitions);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9621,
                .donor_group_id = 1971,
                .receiver_group_id = 1972,
                .phase = .prepare,
                .rollback_reason = "operator abort 2",
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9621,
            .donor_group_id = 1971,
            .receiver_group_id = 1972,
            .rollback_reason = "operator abort 2",
        },
    });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().completed_merge_transitions);
    try expectMergeTransitionInactive(&cluster, 0, 9621);

    try cluster.restartNode(0);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9621,
                .donor_group_id = 1971,
                .receiver_group_id = 1972,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 4,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9621,
            .donor_group_id = 1971,
            .receiver_group_id = 1972,
        },
    });

    rounds = 0;
    while (rounds < 24 and cluster.node(0).serviceMetrics().completed_merge_transitions < 2) : (rounds += 1) {
        try cluster.stepAll();
    }
    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), metrics.completed_merge_transitions);
    try expectMergeTransitionInactive(&cluster, 0, 9621);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const right = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", right);
}

test "cluster simulation isolates concurrent split removal and merge retry across restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);

    const split_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-split-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(split_src_root);
    const split_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-split-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(split_dst_root);
    const merge_donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-merge-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_donor_root);
    const merge_receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-merge-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_receiver_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1981,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = merge_donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1983,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:c", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();

        const split_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9701,
                .source_group_id = 1981,
                .destination_group_id = 1982,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_cmd);
        const merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9702,
                .donor_group_id = 1983,
                .receiver_group_id = 1984,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = split_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_src_root,
        .dest_root_dir = split_dst_root,
        .source_group_id = 1981,
        .dest_group_id = 1982,
    });
    defer split.deinit();

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = merge_donor_root,
        .receiver_root_dir = merge_receiver_root,
        .donor_group_id = 1983,
        .receiver_group_id = 1984,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{
                .split = split.runtime(),
                .merge = merge.runtime(),
            } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 24) : (rounds += 1) {
        try cluster.stepAll();
        const split_state = try cluster.node(0).describeSplitTransition(9701);
        const merge_state = try cluster.node(0).describeMergeTransition(9702);
        if (split_state != null and merge_state != null) {
            if (split_state.?.tag != .awaiting_source_start and merge_state.?.tag != .awaiting_receiver_acceptance) break;
        }
    }
    const split_before = (try cluster.node(0).describeSplitTransition(9701)) orelse return error.TestExpectedEqual;
    const merge_before = (try cluster.node(0).describeMergeTransition(9702)) orelse return error.TestExpectedEqual;
    try std.testing.expect(split_before.tag == .bootstrapping_destination or split_before.tag == .ready_to_finalize);
    try std.testing.expect(merge_before.tag == .bootstrapping_receiver or merge_before.tag == .ready_to_finalize);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();

        const remove_split_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .remove_split_transition = .{ .transition_id = 9701 },
        });
        defer std.testing.allocator.free(remove_split_cmd);
        const rollback_merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9702,
                .donor_group_id = 1983,
                .receiver_group_id = 1984,
                .phase = .prepare,
                .rollback_reason = "operator abort concurrent",
            },
        });
        defer std.testing.allocator.free(rollback_merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = remove_split_cmd },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = rollback_merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 4,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{ .remove_split_transition = .{ .transition_id = 9701 } });
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9702,
            .donor_group_id = 1983,
            .receiver_group_id = 1984,
            .rollback_reason = "operator abort concurrent",
        },
    });

    rounds = 0;
    while (rounds < 24) : (rounds += 1) {
        try cluster.stepAll();
        const metrics = cluster.node(0).serviceMetrics();
        if (splitTransitionInactive(try cluster.node(0).observeSplitTransition(9701)) and
            mergeTransitionInactive(try cluster.node(0).observeMergeTransition(9702)) and
            metrics.queued_split_transitions == 0 and
            metrics.queued_merge_transitions == 0)
        {
            break;
        }
    }
    try expectSplitTransitionInactive(&cluster, 0, 9701);
    try expectMergeTransitionInactive(&cluster, 0, 9702);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_merge_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    try expectSplitTransitionInactive(&cluster, 0, 9701);
    try expectMergeTransitionInactive(&cluster, 0, 9702);
    const merge_completions_before_retry = cluster.node(0).serviceMetrics().completed_merge_transitions;

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9702,
                .donor_group_id = 1983,
                .receiver_group_id = 1984,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 5,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9702,
            .donor_group_id = 1983,
            .receiver_group_id = 1984,
        },
    });

    rounds = 0;
    while (rounds < 24 and cluster.node(0).serviceMetrics().completed_merge_transitions == merge_completions_before_retry) : (rounds += 1) {
        try cluster.stepAll();
    }
    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.completed_split_transitions);
    try std.testing.expectEqual(merge_completions_before_retry + 1, metrics.completed_merge_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9701);
    try expectMergeTransitionInactive(&cluster, 0, 9702);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_src_root });
        defer source.deinit();
        const source_range = try source.currentRange(std.testing.allocator, 1981);
        defer {
            std.testing.allocator.free(source_range.start);
            std.testing.allocator.free(source_range.end);
        }
        try std.testing.expectEqualStrings("doc:a", source_range.start);
        try std.testing.expectEqualStrings("doc:m", source_range.end);
        const source_state = try source.groupState(std.testing.allocator, 1981);
        defer {
            for (source_state) |entry| {
                std.testing.allocator.free(entry.key);
                std.testing.allocator.free(entry.value);
            }
            std.testing.allocator.free(source_state);
        }
        try std.testing.expectEqual(@as(usize, 2), source_state.len);
        try std.testing.expectEqualStrings("doc:b", source_state[0].key);
        try std.testing.expectEqualStrings("{\"v\":\"left-0\"}", source_state[0].value);
        try std.testing.expectEqualStrings("doc:t", source_state[1].key);
        try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", source_state[1].value);
    }

    var split_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_dst_root });
    defer split_dest.deinit();
    try std.testing.expectEqual(@as(?[]u8, null), try split_dest.get(std.testing.allocator, "doc:t"));

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const merged = (try receiver.get(std.testing.allocator, "doc:u")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(merged);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", merged);
}

test "cluster simulation isolates concurrent merge removal and split retry across restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split = std.testing.tmpDir(.{});
    defer tmp_split.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);

    const split_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-split-src", .{tmp_split.sub_path});
    defer std.testing.allocator.free(split_src_root);
    const split_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-split-dst", .{tmp_split.sub_path});
    defer std.testing.allocator.free(split_dst_root);
    const merge_donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-merge-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_donor_root);
    const merge_receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-concurrent-reverse-merge-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_receiver_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-1\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-1\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1991,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = merge_donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"donor-1\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1993,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:d", .value = "{\"v\":\"receiver-1\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();

        const split_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9711,
                .source_group_id = 1991,
                .destination_group_id = 1992,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_cmd);
        const merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9712,
                .donor_group_id = 1993,
                .receiver_group_id = 1994,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = split_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_src_root,
        .dest_root_dir = split_dst_root,
        .source_group_id = 1991,
        .dest_group_id = 1992,
    });
    defer split.deinit();

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = merge_donor_root,
        .receiver_root_dir = merge_receiver_root,
        .donor_group_id = 1993,
        .receiver_group_id = 1994,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{
                .split = split.runtime(),
                .merge = merge.runtime(),
            } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 24) : (rounds += 1) {
        try cluster.stepAll();
        const split_state = try cluster.node(0).describeSplitTransition(9711);
        const merge_state = try cluster.node(0).describeMergeTransition(9712);
        if (split_state != null and merge_state != null) {
            if (split_state.?.tag != .awaiting_source_start and merge_state.?.tag != .awaiting_receiver_acceptance) break;
        }
    }
    const split_before = (try cluster.node(0).describeSplitTransition(9711)) orelse return error.TestExpectedEqual;
    const merge_before = (try cluster.node(0).describeMergeTransition(9712)) orelse return error.TestExpectedEqual;
    try std.testing.expect(split_before.tag == .bootstrapping_destination or split_before.tag == .ready_to_finalize);
    try std.testing.expect(merge_before.tag == .bootstrapping_receiver or merge_before.tag == .ready_to_finalize);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();

        const rollback_split_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9711,
                .source_group_id = 1991,
                .destination_group_id = 1992,
                .phase = .prepare,
                .rollback_reason = "operator abort concurrent reverse",
            },
        });
        defer std.testing.allocator.free(rollback_split_cmd);
        const remove_merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .remove_merge_transition = .{ .transition_id = 9712 },
        });
        defer std.testing.allocator.free(remove_merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = rollback_split_cmd },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = remove_merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 4,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9711,
            .source_group_id = 1991,
            .destination_group_id = 1992,
            .rollback_reason = "operator abort concurrent reverse",
        },
    });
    try cluster.node(0).apply(.{ .remove_merge_transition = .{ .transition_id = 9712 } });

    rounds = 0;
    while (rounds < 24) : (rounds += 1) {
        try cluster.stepAll();
        const metrics = cluster.node(0).serviceMetrics();
        if (splitTransitionInactive(try cluster.node(0).observeSplitTransition(9711)) and
            mergeTransitionInactive(try cluster.node(0).observeMergeTransition(9712)) and
            metrics.queued_split_transitions == 0 and
            metrics.queued_merge_transitions == 0)
        {
            break;
        }
    }
    try expectSplitTransitionInactive(&cluster, 0, 9711);
    try expectMergeTransitionInactive(&cluster, 0, 9712);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_merge_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    try expectSplitTransitionInactive(&cluster, 0, 9711);
    try expectMergeTransitionInactive(&cluster, 0, 9712);
    const split_completions_before_retry = cluster.node(0).serviceMetrics().completed_split_transitions;

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const split_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9711,
                .source_group_id = 1991,
                .destination_group_id = 1992,
                .phase = .prepare,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
            },
        });
        defer std.testing.allocator.free(split_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = split_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 5,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9711,
            .source_group_id = 1991,
            .destination_group_id = 1992,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
        },
    });

    rounds = 0;
    while (rounds < 24 and cluster.node(0).serviceMetrics().completed_split_transitions == split_completions_before_retry) : (rounds += 1) {
        try cluster.stepAll();
    }
    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(split_completions_before_retry + 1, metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.completed_merge_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9711);
    try expectMergeTransitionInactive(&cluster, 0, 9712);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_src_root });
        defer source.deinit();
        const source_range = try source.currentRange(std.testing.allocator, 1991);
        defer {
            std.testing.allocator.free(source_range.start);
            std.testing.allocator.free(source_range.end);
        }
        try std.testing.expectEqualStrings("doc:a", source_range.start);
        try std.testing.expectEqualStrings("doc:m", source_range.end);
    }

    var split_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_dst_root });
    defer split_dest.deinit();
    const split_range = split_dest.getRange();
    try std.testing.expectEqualStrings("doc:m", split_range.start);
    try std.testing.expectEqualStrings("doc:z", split_range.end);
    const right = (try split_dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(right);
    try std.testing.expectEqualStrings("{\"v\":\"right-1\"}", right);

    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:m", range.end);
    try std.testing.expectEqual(@as(?[]u8, null), try receiver.get(std.testing.allocator, "doc:u"));
}

test "cluster simulation drives multiple concurrent real transition ids through multiplexed runtime" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split_a = std.testing.tmpDir(.{});
    defer tmp_split_a.cleanup();
    var tmp_split_b = std.testing.tmpDir(.{});
    defer tmp_split_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-concurrent-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-concurrent-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);

    const split_a_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-split-a-src", .{tmp_split_a.sub_path});
    defer std.testing.allocator.free(split_a_src_root);
    const split_a_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-split-a-dst", .{tmp_split_a.sub_path});
    defer std.testing.allocator.free(split_a_dst_root);
    const split_b_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-split-b-src", .{tmp_split_b.sub_path});
    defer std.testing.allocator.free(split_b_src_root);
    const split_b_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-split-b-dst", .{tmp_split_b.sub_path});
    defer std.testing.allocator.free(split_b_dst_root);
    const merge_donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-merge-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_donor_root);
    const merge_receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-multi-merge-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_receiver_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_a_src_root });
        defer source.deinit();
        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-a\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-a\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2001,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_b_src_root });
        defer source.deinit();
        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:c={\"v\":\"left-b\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"right-b\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:p") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2003,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = merge_donor_root });
        defer donor.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:y={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 2005,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:e", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const split_a_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9801,
                .source_group_id = 2001,
                .destination_group_id = 2002,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_a_cmd);
        const split_b_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9802,
                .source_group_id = 2003,
                .destination_group_id = 2004,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_b_cmd);
        const merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9803,
                .donor_group_id = 2005,
                .receiver_group_id = 2006,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = split_a_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_b_cmd },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split_a = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_a_src_root,
        .dest_root_dir = split_a_dst_root,
        .source_group_id = 2001,
        .dest_group_id = 2002,
    });
    defer split_a.deinit();
    var split_b = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_b_src_root,
        .dest_root_dir = split_b_dst_root,
        .source_group_id = 2003,
        .dest_group_id = 2004,
    });
    defer split_b.deinit();
    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = merge_donor_root,
        .receiver_root_dir = merge_receiver_root,
        .donor_group_id = 2005,
        .receiver_group_id = 2006,
    });
    defer merge.deinit();

    var multiplex = transition_runtime_mod.MultiplexedTransitionRuntime.init(std.testing.allocator);
    defer multiplex.deinit();
    try multiplex.addSplit(2001, 2002, split_a.runtime());
    try multiplex.addSplit(2003, 2004, split_b.runtime());
    try multiplex.addMerge(2005, 2006, merge.runtime());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = multiplex.runtime() },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 32) : (rounds += 1) {
        try cluster.stepAll();
        const split_a_state = try cluster.node(0).describeSplitTransition(9801);
        const split_b_state = try cluster.node(0).describeSplitTransition(9802);
        const merge_state = try cluster.node(0).describeMergeTransition(9803);
        if (split_a_state != null and split_b_state != null and merge_state != null) {
            if (split_a_state.?.tag != .awaiting_source_start and split_b_state.?.tag != .awaiting_source_start and merge_state.?.tag != .awaiting_receiver_acceptance) break;
        }
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const remove_split_a_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .remove_split_transition = .{ .transition_id = 9801 },
        });
        defer std.testing.allocator.free(remove_split_a_cmd);
        const rollback_split_b_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9802,
                .source_group_id = 2003,
                .destination_group_id = 2004,
                .phase = .prepare,
                .rollback_reason = "operator abort multi",
            },
        });
        defer std.testing.allocator.free(rollback_split_b_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = remove_split_a_cmd },
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = rollback_split_b_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 5,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{ .remove_split_transition = .{ .transition_id = 9801 } });
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9802,
            .source_group_id = 2003,
            .destination_group_id = 2004,
            .rollback_reason = "operator abort multi",
        },
    });

    rounds = 0;
    while (rounds < 32) : (rounds += 1) {
        try cluster.stepAll();
        const split_a_obs = try cluster.node(0).observeSplitTransition(9801);
        const split_b_obs = try cluster.node(0).observeSplitTransition(9802);
        if (splitTransitionInactive(split_a_obs) and splitTransitionInactive(split_b_obs)) break;
    }
    try expectSplitTransitionInactive(&cluster, 0, 9801);
    try expectSplitTransitionInactive(&cluster, 0, 9802);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    try expectSplitTransitionInactive(&cluster, 0, 9801);
    try expectSplitTransitionInactive(&cluster, 0, 9802);
    const split_completions_before_retry = cluster.node(0).serviceMetrics().completed_split_transitions;
    const merge_completions_before_retry = cluster.node(0).serviceMetrics().completed_merge_transitions;

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const split_b_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9802,
                .source_group_id = 2003,
                .destination_group_id = 2004,
                .phase = .prepare,
                .split_key = "doc:p",
                .source_range_end = "doc:z",
            },
        });
        defer std.testing.allocator.free(split_b_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 6, .entry_type = .normal, .data = split_b_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 6,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9802,
            .source_group_id = 2003,
            .destination_group_id = 2004,
            .split_key = "doc:p",
            .source_range_end = "doc:z",
        },
    });

    rounds = 0;
    while (rounds < 48 and
        (cluster.node(0).serviceMetrics().completed_split_transitions == split_completions_before_retry or
            cluster.node(0).serviceMetrics().completed_merge_transitions == merge_completions_before_retry)) : (rounds += 1)
    {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(split_completions_before_retry + 1, metrics.completed_split_transitions);
    try std.testing.expectEqual(merge_completions_before_retry, metrics.completed_merge_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9801);
    try expectSplitTransitionInactive(&cluster, 0, 9802);
    try expectMergeTransitionInactive(&cluster, 0, 9803);

    var split_a_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_a_dst_root });
    defer split_a_dest.deinit();
    try std.testing.expectEqual(@as(?[]u8, null), try split_a_dest.get(std.testing.allocator, "doc:t"));

    var split_b_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_b_dst_root });
    defer split_b_dest.deinit();
    const split_b_range = split_b_dest.getRange();
    try std.testing.expectEqualStrings("doc:p", split_b_range.start);
    try std.testing.expectEqualStrings("doc:z", split_b_range.end);
    const split_b_doc = (try split_b_dest.get(std.testing.allocator, "doc:u")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(split_b_doc);
    try std.testing.expectEqualStrings("{\"v\":\"right-b\"}", split_b_doc);

    var merge_receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
    defer merge_receiver.deinit();
    const merge_range = merge_receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", merge_range.start);
    try std.testing.expectEqualStrings("doc:z", merge_range.end);
    const merged = (try merge_receiver.get(std.testing.allocator, "doc:y")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(merged);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", merged);
}

test "cluster simulation isolates overlapping same-id split overwrites while other transitions complete" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_split_a = std.testing.tmpDir(.{});
    defer tmp_split_a.cleanup();
    var tmp_split_b = std.testing.tmpDir(.{});
    defer tmp_split_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);

    const split_a_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-split-a-src", .{tmp_split_a.sub_path});
    defer std.testing.allocator.free(split_a_src_root);
    const split_a_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-split-a-dst", .{tmp_split_a.sub_path});
    defer std.testing.allocator.free(split_a_dst_root);
    const split_b_src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-split-b-src", .{tmp_split_b.sub_path});
    defer std.testing.allocator.free(split_b_src_root);
    const split_b_dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-split-b-dst", .{tmp_split_b.sub_path});
    defer std.testing.allocator.free(split_b_dst_root);
    const merge_donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-merge-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_donor_root);
    const merge_receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-overlap-merge-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(merge_receiver_root);

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_a_src_root });
        defer source.deinit();
        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-a2\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-a2\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2011,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var source = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = split_b_src_root });
        defer source.deinit();
        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:c={\"v\":\"left-b2\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:u={\"v\":\"right-b2\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:p") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2013,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = merge_donor_root });
        defer donor.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:y={\"v\":\"donor-2\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 2015,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:f", .value = "{\"v\":\"receiver-2\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const split_a_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9811,
                .source_group_id = 2011,
                .destination_group_id = 2012,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_a_cmd);
        const split_b_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9812,
                .source_group_id = 2013,
                .destination_group_id = 2014,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(split_b_cmd);
        const merge_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9813,
                .donor_group_id = 2015,
                .receiver_group_id = 2016,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(merge_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = split_a_cmd },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_b_cmd },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var split_a = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_a_src_root,
        .dest_root_dir = split_a_dst_root,
        .source_group_id = 2011,
        .dest_group_id = 2012,
    });
    defer split_a.deinit();
    var split_b = try transition_runtime_mod.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = split_b_src_root,
        .dest_root_dir = split_b_dst_root,
        .source_group_id = 2013,
        .dest_group_id = 2014,
    });
    defer split_b.deinit();
    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = merge_donor_root,
        .receiver_root_dir = merge_receiver_root,
        .donor_group_id = 2015,
        .receiver_group_id = 2016,
    });
    defer merge.deinit();

    var multiplex = transition_runtime_mod.MultiplexedTransitionRuntime.init(std.testing.allocator);
    defer multiplex.deinit();
    try multiplex.addSplit(2011, 2012, split_a.runtime());
    try multiplex.addSplit(2013, 2014, split_b.runtime());
    try multiplex.addMerge(2015, 2016, merge.runtime());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = multiplex.runtime() },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 32) : (rounds += 1) {
        try cluster.stepAll();
        const split_a_state = try cluster.node(0).describeSplitTransition(9811);
        const split_b_state = try cluster.node(0).describeSplitTransition(9812);
        const merge_state = try cluster.node(0).describeMergeTransition(9813);
        if (split_a_state != null and split_b_state != null and merge_state != null) {
            if (split_a_state.?.tag != .awaiting_source_start and split_b_state.?.tag != .awaiting_source_start and merge_state.?.tag != .awaiting_receiver_acceptance) break;
        }
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const rollback_split_b_cmd_1 = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9812,
                .source_group_id = 2013,
                .destination_group_id = 2014,
                .phase = .prepare,
                .rollback_reason = "operator abort overlap 1",
            },
        });
        defer std.testing.allocator.free(rollback_split_b_cmd_1);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = rollback_split_b_cmd_1 },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 4,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9812,
            .source_group_id = 2013,
            .destination_group_id = 2014,
            .rollback_reason = "operator abort overlap 1",
        },
    });
    try cluster.stepAll();

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const rollback_split_b_cmd_2 = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9812,
                .source_group_id = 2013,
                .destination_group_id = 2014,
                .phase = .prepare,
                .rollback_reason = "operator abort overlap 2",
            },
        });
        defer std.testing.allocator.free(rollback_split_b_cmd_2);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 5, .entry_type = .normal, .data = rollback_split_b_cmd_2 },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 5,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9812,
            .source_group_id = 2013,
            .destination_group_id = 2014,
            .rollback_reason = "operator abort overlap 2",
        },
    });

    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const split_a_done = splitTransitionInactive(try cluster.node(0).observeSplitTransition(9811));
        const split_b_done = splitTransitionInactive(try cluster.node(0).observeSplitTransition(9812));
        const merge_done = mergeTransitionInactive(try cluster.node(0).observeMergeTransition(9813));
        if (split_a_done and split_b_done and merge_done) break;
    }
    try expectSplitTransitionInactive(&cluster, 0, 9811);
    try expectSplitTransitionInactive(&cluster, 0, 9812);
    try expectMergeTransitionInactive(&cluster, 0, 9813);

    try cluster.restartNode(0);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const split_b_cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 9812,
                .source_group_id = 2013,
                .destination_group_id = 2014,
                .phase = .prepare,
                .split_key = "doc:p",
                .source_range_end = "doc:z",
            },
        });
        defer std.testing.allocator.free(split_b_cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 6, .entry_type = .normal, .data = split_b_cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 6,
            .entries_bytes = entries,
        });
    }

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    const split_completions_before_retry = cluster.node(0).serviceMetrics().completed_split_transitions;
    const merge_completions_before_retry = cluster.node(0).serviceMetrics().completed_merge_transitions;

    try cluster.node(0).apply(.{
        .upsert_split_transition = .{
            .transition_id = 9812,
            .source_group_id = 2013,
            .destination_group_id = 2014,
            .split_key = "doc:p",
            .source_range_end = "doc:z",
        },
    });

    rounds = 0;
    while (rounds < 48 and cluster.node(0).serviceMetrics().completed_split_transitions == split_completions_before_retry) : (rounds += 1) {
        try cluster.stepAll();
    }

    const metrics = cluster.node(0).serviceMetrics();
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), metrics.queued_merge_transitions);
    try std.testing.expectEqual(split_completions_before_retry + 1, metrics.completed_split_transitions);
    try std.testing.expectEqual(merge_completions_before_retry, metrics.completed_merge_transitions);
    try expectSplitTransitionInactive(&cluster, 0, 9811);
    try expectSplitTransitionInactive(&cluster, 0, 9812);
    try expectMergeTransitionInactive(&cluster, 0, 9813);

    var split_a_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_a_dst_root });
    defer split_a_dest.deinit();
    const split_a_range = split_a_dest.getRange();
    try std.testing.expectEqualStrings("doc:m", split_a_range.start);
    try std.testing.expectEqualStrings("doc:z", split_a_range.end);
    const split_a_doc = (try split_a_dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(split_a_doc);
    try std.testing.expectEqualStrings("{\"v\":\"right-a2\"}", split_a_doc);

    var split_b_dest = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = split_b_dst_root });
    defer split_b_dest.deinit();
    const split_b_range = split_b_dest.getRange();
    try std.testing.expectEqualStrings("doc:p", split_b_range.start);
    try std.testing.expectEqualStrings("doc:z", split_b_range.end);
    const split_b_doc = (try split_b_dest.get(std.testing.allocator, "doc:u")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(split_b_doc);
    try std.testing.expectEqualStrings("{\"v\":\"right-b2\"}", split_b_doc);

    var merge_receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = merge_receiver_root });
    defer merge_receiver.deinit();
    const merge_range = merge_receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", merge_range.start);
    try std.testing.expectEqualStrings("doc:z", merge_range.end);
    const merged = (try merge_receiver.get(std.testing.allocator, "doc:y")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(merged);
    try std.testing.expectEqualStrings("{\"v\":\"donor-2\"}", merged);
}

test "cluster simulation removes queued merge transition mid-flight across node restart" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_merge = std.testing.tmpDir(.{});
    defer tmp_merge.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-remove-restart-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-remove-restart-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const replica_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/replicas", .{root_a});
    defer std.testing.allocator.free(replica_root_a);
    const replica_catalog_path_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/catalog.txt", .{root_a});
    defer std.testing.allocator.free(replica_catalog_path_a);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-remove-restart-donor", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/queued-merge-remove-restart-receiver", .{tmp_merge.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1961,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9611,
                .donor_group_id = 1961,
                .receiver_group_id = 1962,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 1,
            .entries_bytes = entries,
        });
    }

    var metadata_descriptor_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer metadata_descriptor_store.deinit();
    var metadata_factory = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &metadata_descriptor_store,
        .peers = &.{1},
    };

    var merge = try transition_runtime_mod.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 1961,
        .receiver_group_id = 1962,
    });
    defer merge.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{
            .host = .{
                .http = .{
                    .host = .{
                        .local_node_id = 1,
                        .metadata_group_id = 1300,
                        .replica_root_dir = replica_root_a,
                        .replica_catalog_path = replica_catalog_path_a,
                    },
                    .transport = .{ .snapshot = .{ .root_dir = root_a } },
                },
            },
        },
        .{
            .host = .{
                .http = .{
                    .host = .{ .local_node_id = 2 },
                    .transport = .{ .snapshot = .{ .root_dir = root_b } },
                },
            },
        },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{
            .host = .{
                .http = .{ .host = .{ .descriptor_factory = metadata_factory.iface() } },
            },
            .service = .{ .transition_runtime = .{ .merge = merge.runtime() } },
        },
        .{},
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();

    var rounds: usize = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9611)) |state| {
            if (state.tag == .bootstrapping_receiver) break;
        }
    }
    const before_remove = (try cluster.node(0).describeMergeTransition(9611)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata_mod.MergeExecutionStateTag.bootstrapping_receiver, before_remove.tag);

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .remove_merge_transition = .{ .transition_id = 9611 },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 2,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{ .remove_merge_transition = .{ .transition_id = 9611 } });
    try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().completed_merge_transitions);

    try cluster.restartNode(0);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 0), cluster.node(0).serviceMetrics().completed_merge_transitions);
    try expectMergeTransitionInactive(&cluster, 0, 9611);
    var receiver = try data_mod.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:m", range.end);
    try std.testing.expectEqual(@as(?[]u8, null), try receiver.get(std.testing.allocator, "doc:t"));

    {
        var metadata_store = try metadata_mod.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = replica_root_a });
        defer metadata_store.deinit();
        const cmd = try metadata_mod.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 9612,
                .donor_group_id = 1961,
                .receiver_group_id = 1962,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        const entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);
        try metadata_store.snapshotBuilder().applyBatch(.{
            .group_id = 1300,
            .commit_index = 3,
            .entries_bytes = entries,
        });
    }
    try cluster.node(0).apply(.{
        .upsert_merge_transition = .{
            .transition_id = 9612,
            .donor_group_id = 1961,
            .receiver_group_id = 1962,
        },
    });

    rounds = 0;
    while (rounds < 16) : (rounds += 1) {
        try cluster.stepAll();
        if (try cluster.node(0).describeMergeTransition(9612)) |state| {
            try std.testing.expect(state.tag == .bootstrapping_receiver or state.tag == .ready_to_finalize);
            break;
        }
    }
    const retried_merge = (try cluster.node(0).describeMergeTransition(9612)) orelse return error.TestExpectedEqual;
    try std.testing.expect(retried_merge.tag == .bootstrapping_receiver or retried_merge.tag == .ready_to_finalize);
}

test "managed http cluster simulation emits leadership gain and loss events across transfer" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3010, &store_a);
    try persist_b.registerStore(3010, &store_b);
    try persist_c.registerStore(3010, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var exec_a = enrichment_runtime.SimulatedExecutor.init(std.testing.allocator);
    defer exec_a.deinit();
    var exec_b = enrichment_runtime.SimulatedExecutor.init(std.testing.allocator);
    defer exec_b.deinit();
    var leader_runtime_a = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_a.executor());
    var leader_runtime_b = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_b.executor());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } }, .leader_observer = leader_runtime_a.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } }, .leader_observer = leader_runtime_b.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3010, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3010, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3010, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3010, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3010);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3010, 64));

    try cluster.node(0).propose(3010, "leader-transfer-warmup");
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_b, 2, 64));

    try cluster.node(0).transferLeader(3010, 2);
    try std.testing.expect(try cluster.waitForLeaderId(3010, 2, 64));

    var settle_rounds: usize = 0;
    while (settle_rounds < 64 and (!leader_runtime_b.isActive(3010) or leader_runtime_a.isActive(3010))) : (settle_rounds += 1) {
        try cluster.stepAll();
    }

    try std.testing.expect(!leader_runtime_a.isActive(3010));
    try std.testing.expect(leader_runtime_b.isActive(3010));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.gained_events);
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.lost_events);
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_b.metrics.gained_events);
    try std.testing.expectEqual(@as(u64, 0), leader_runtime_b.metrics.lost_events);
}

test "managed http cluster simulation gates enrichment on explicit readable lease" {
    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var exec_a = enrichment_runtime.SimulatedExecutor.init(std.testing.allocator);
    defer exec_a.deinit();
    var exec_b = enrichment_runtime.SimulatedExecutor.init(std.testing.allocator);
    defer exec_b.deinit();
    try persist_a.registerStore(3014, &store_a);
    try persist_b.registerStore(3014, &store_b);
    try persist_c.registerStore(3014, &store_c);
    var lease_runtime_a = enrichment_runtime.LeaseGatedLeaderEnrichmentRuntime.init(std.testing.allocator, exec_a.executor());
    defer lease_runtime_a.deinit();
    var lease_runtime_b = enrichment_runtime.LeaseGatedLeaderEnrichmentRuntime.init(std.testing.allocator, exec_b.executor());
    defer lease_runtime_b.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{
            .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } },
            .leader_observer = lease_runtime_a.observer(),
            .read_state_observer = lease_runtime_a.readStateObserver(),
        } },
        .{ .host = .{
            .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } },
            .leader_observer = lease_runtime_b.observer(),
            .read_state_observer = lease_runtime_b.readStateObserver(),
        } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3014, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3014, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3014, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3014, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3014);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3014, 64));
    var leader_settle: usize = 0;
    while (leader_settle < 64 and lease_runtime_a.state(3014) == .follower) : (leader_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_a.state(3014));
    try std.testing.expect(!lease_runtime_a.isActive(3014));

    try cluster.node(0).featureReads().prepareSearch(3014, .{});
    var readable_settle: usize = 0;
    while (readable_settle < 64 and !lease_runtime_a.isActive(3014)) : (readable_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_a.isActive(3014));
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().read_lease_requests);

    try cluster.node(0).propose(3014, "lease-gated-transfer");
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_b, 2, 64));
    try cluster.node(0).transferLeader(3014, 2);
    try std.testing.expect(try cluster.waitForLeaderId(3014, 2, 64));

    var settle_rounds: usize = 0;
    while (settle_rounds < 64 and (lease_runtime_a.isActive(3014) or lease_runtime_b.state(3014) != .awaiting_readable)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }

    try std.testing.expect(!lease_runtime_a.isActive(3014));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.follower, lease_runtime_a.state(3014));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3014));
    try std.testing.expect(!lease_runtime_b.isActive(3014));

    try cluster.node(1).featureReads().prepareSearch(3014, .{});
    readable_settle = 0;
    while (readable_settle < 64 and !lease_runtime_b.isActive(3014)) : (readable_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3014));
    try std.testing.expectEqual(@as(usize, 1), cluster.node(1).serviceMetrics().read_lease_requests);

    try std.testing.expect(try lease_runtime_b.revokeReadable(3014));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3014));
    try std.testing.expect(!lease_runtime_b.isActive(3014));

    try cluster.node(1).featureReads().prepareLookup(3014, "doc:a", .{});
    readable_settle = 0;
    while (readable_settle < 64 and !lease_runtime_b.isActive(3014)) : (readable_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3014));
    try std.testing.expectEqual(@as(usize, 2), cluster.node(1).serviceMetrics().read_lease_requests);

    try std.testing.expect(try lease_runtime_b.revokeReadable(3014));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3014));
    try std.testing.expect(!lease_runtime_b.isActive(3014));

    try cluster.node(1).featureReads().prepareScan(3014, "doc:a", "doc:z", .{});
    readable_settle = 0;
    while (readable_settle < 64 and !lease_runtime_b.isActive(3014)) : (readable_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3014));
    try std.testing.expectEqual(@as(usize, 3), cluster.node(1).serviceMetrics().read_lease_requests);
}

test "managed http cluster simulation gates real db enrichment runtimes on read index" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) db_enrichment_runtime_factory.GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-db-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-db-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/lease-gated-db-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);
    const enrich_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_a});
    defer std.testing.allocator.free(enrich_root_a);
    const enrich_root_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_b});
    defer std.testing.allocator.free(enrich_root_b);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3015, &store_a);
    try persist_b.registerStore(3015, &store_b);
    try persist_c.registerStore(3015, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var deterministic_a = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_b = embedder_mod.DeterministicDenseEmbedder{};
    var resolver_a = Resolver{ .root = enrich_root_a };
    var resolver_b = Resolver{ .root = enrich_root_b };
    var db_factory_a = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-1",
                .dense_embedder = deterministic_a.interface(),
            },
        },
        .owner_id = "node-1",
    }, resolver_a.iface());
    var db_factory_b = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-2",
                .dense_embedder = deterministic_b.interface(),
            },
        },
        .owner_id = "node-2",
    }, resolver_b.iface());

    var exec_a = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_a.iface(),
    );
    defer exec_a.deinit();
    var exec_b = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_b.iface(),
    );
    defer exec_b.deinit();
    var lease_runtime_a = enrichment_runtime.LeaseGatedLeaderEnrichmentRuntime.init(std.testing.allocator, exec_a.executor());
    defer lease_runtime_a.deinit();
    var lease_runtime_b = enrichment_runtime.LeaseGatedLeaderEnrichmentRuntime.init(std.testing.allocator, exec_b.executor());
    defer lease_runtime_b.deinit();

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{
            .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } },
            .leader_observer = lease_runtime_a.observer(),
            .read_state_observer = lease_runtime_a.readStateObserver(),
        } },
        .{ .host = .{
            .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } },
            .leader_observer = lease_runtime_b.observer(),
            .read_state_observer = lease_runtime_b.readStateObserver(),
        } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3015, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3015, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3015, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3015, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3015);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3015, 64));

    var settle_rounds: usize = 0;
    while (settle_rounds < 64 and lease_runtime_a.state(3015) == .follower) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_a.state(3015));
    try std.testing.expect(!lease_runtime_a.isActive(3015));
    try std.testing.expect(!lease_runtime_b.isActive(3015));

    try cluster.node(0).featureReads().prepareSearch(3015, .{});
    settle_rounds = 0;
    while (settle_rounds < 64 and !lease_runtime_a.isActive(3015)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_a.isActive(3015));
    try std.testing.expectEqual(@as(usize, 1), cluster.node(0).serviceMetrics().read_lease_requests);

    try cluster.node(0).transferLeader(3015, 2);
    try std.testing.expect(try cluster.waitForLeaderId(3015, 2, 64));

    settle_rounds = 0;
    while (settle_rounds < 64 and (lease_runtime_a.isActive(3015) or lease_runtime_b.state(3015) != .awaiting_readable)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(!lease_runtime_a.isActive(3015));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.follower, lease_runtime_a.state(3015));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3015));
    try std.testing.expect(!lease_runtime_b.isActive(3015));

    try cluster.node(1).featureReads().prepareSearch(3015, .{});
    settle_rounds = 0;
    while (settle_rounds < 64 and !lease_runtime_b.isActive(3015)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3015));
    try std.testing.expectEqual(@as(usize, 1), cluster.node(1).serviceMetrics().read_lease_requests);

    try std.testing.expect(try lease_runtime_b.revokeReadable(3015));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3015));
    try std.testing.expect(!lease_runtime_b.isActive(3015));

    try cluster.node(1).featureReads().prepareLookup(3015, "doc:a", .{});
    settle_rounds = 0;
    while (settle_rounds < 64 and !lease_runtime_b.isActive(3015)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3015));
    try std.testing.expectEqual(@as(usize, 2), cluster.node(1).serviceMetrics().read_lease_requests);

    try std.testing.expect(try lease_runtime_b.revokeReadable(3015));
    try std.testing.expectEqual(enrichment_runtime.LeaseReadState.awaiting_readable, lease_runtime_b.state(3015));
    try std.testing.expect(!lease_runtime_b.isActive(3015));

    try cluster.node(1).featureReads().prepareScan(3015, "doc:a", "doc:z", .{});
    settle_rounds = 0;
    while (settle_rounds < 64 and !lease_runtime_b.isActive(3015)) : (settle_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(lease_runtime_b.isActive(3015));
    try std.testing.expectEqual(@as(usize, 3), cluster.node(1).serviceMetrics().read_lease_requests);
}

test "managed http cluster simulation starts real db enrichment runtimes across leader transfer" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) db_enrichment_runtime_factory.GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-db-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-db-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-transfer-db-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);
    const enrich_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_a});
    defer std.testing.allocator.free(enrich_root_a);
    const enrich_root_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_b});
    defer std.testing.allocator.free(enrich_root_b);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3011, &store_a);
    try persist_b.registerStore(3011, &store_b);
    try persist_c.registerStore(3011, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var deterministic_a = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_b = embedder_mod.DeterministicDenseEmbedder{};
    var resolver_a = Resolver{ .root = enrich_root_a };
    var resolver_b = Resolver{ .root = enrich_root_b };
    var db_factory_a = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-1",
                .dense_embedder = deterministic_a.interface(),
            },
        },
        .owner_id = "node-1",
    }, resolver_a.iface());
    var db_factory_b = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-2",
                .dense_embedder = deterministic_b.interface(),
            },
        },
        .owner_id = "node-2",
    }, resolver_b.iface());

    var exec_a = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_a.iface(),
    );
    defer exec_a.deinit();
    var exec_b = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_b.iface(),
    );
    defer exec_b.deinit();
    var leader_runtime_a = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_a.executor());
    var leader_runtime_b = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_b.executor());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } }, .leader_observer = leader_runtime_a.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } }, .leader_observer = leader_runtime_b.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } } } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3011, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3011, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3011, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3011);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3011, 64));

    try cluster.node(0).propose(3011, "leader-transfer-db-runtime");
    try std.testing.expect(try waitForLastIndexInCluster(&cluster, &store_b, 2, 64));

    try cluster.node(0).transferLeader(3011, 2);
    try std.testing.expect(try cluster.waitForLeaderId(3011, 2, 64));

    var settle_rounds: usize = 0;
    while (settle_rounds < 64 and (!leader_runtime_b.isActive(3011) or leader_runtime_a.isActive(3011))) : (settle_rounds += 1) {
        try cluster.stepAll();
    }

    try std.testing.expect(!leader_runtime_a.isActive(3011));
    try std.testing.expect(leader_runtime_b.isActive(3011));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.gained_events);
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.lost_events);
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_b.metrics.gained_events);

    try cluster.restartNode(1);

    const restarted_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(restarted_b);

    try cluster.node(0).apply(.{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = restarted_b, .metadata = "" }} } });
    try cluster.node(2).apply(.{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = restarted_b, .metadata = "" }} } });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3011, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });

    var restart_rounds: usize = 0;
    while (restart_rounds < 96 and leader_runtime_b.isActive(3011)) : (restart_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(!leader_runtime_b.isActive(3011));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_b.metrics.lost_events);

    const leader_after_restart = try cluster.waitForLeader(3011, 96);
    if (leader_after_restart == 1) {
        var settle_a: usize = 0;
        while (settle_a < 64 and !leader_runtime_a.isActive(3011)) : (settle_a += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_a.isActive(3011));
        try std.testing.expectEqual(@as(u64, 2), leader_runtime_a.metrics.gained_events);
    } else if (leader_after_restart == 2) {
        var settle_b: usize = 0;
        while (settle_b < 64 and !leader_runtime_b.isActive(3011)) : (settle_b += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_b.isActive(3011));
        try std.testing.expectEqual(@as(u64, 2), leader_runtime_b.metrics.gained_events);
    } else {
        try std.testing.expectEqual(@as(?u64, 3), leader_after_restart);
        try std.testing.expect(!leader_runtime_a.isActive(3011));
        try std.testing.expect(!leader_runtime_b.isActive(3011));
    }
}

test "managed http cluster simulation fences real db enrichment runtimes across leader restart" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) db_enrichment_runtime_factory.GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-restart-db-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-restart-db-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-restart-db-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);
    const enrich_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_a});
    defer std.testing.allocator.free(enrich_root_a);
    const enrich_root_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_b});
    defer std.testing.allocator.free(enrich_root_b);
    const enrich_root_c = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_c});
    defer std.testing.allocator.free(enrich_root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3012, &store_a);
    try persist_b.registerStore(3012, &store_b);
    try persist_c.registerStore(3012, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var deterministic_a = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_b = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_c = embedder_mod.DeterministicDenseEmbedder{};
    var resolver_a = Resolver{ .root = enrich_root_a };
    var resolver_b = Resolver{ .root = enrich_root_b };
    var resolver_c = Resolver{ .root = enrich_root_c };
    var db_factory_a = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-1",
                .dense_embedder = deterministic_a.interface(),
            },
        },
        .owner_id = "node-1",
    }, resolver_a.iface());
    var db_factory_b = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-2",
                .dense_embedder = deterministic_b.interface(),
            },
        },
        .owner_id = "node-2",
    }, resolver_b.iface());
    var db_factory_c = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-3",
                .dense_embedder = deterministic_c.interface(),
            },
        },
        .owner_id = "node-3",
    }, resolver_c.iface());

    var exec_a = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_a.iface(),
    );
    defer exec_a.deinit();
    var exec_b = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_b.iface(),
    );
    defer exec_b.deinit();
    var exec_c = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_c.iface(),
    );
    defer exec_c.deinit();
    var leader_runtime_a = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_a.executor());
    var leader_runtime_b = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_b.executor());
    var leader_runtime_c = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_c.executor());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } }, .leader_observer = leader_runtime_a.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } }, .leader_observer = leader_runtime_b.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } }, .leader_observer = leader_runtime_c.observer() } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3012, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3012, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3012, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3012);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3012, 64));

    var leader_settle: usize = 0;
    while (leader_settle < 64 and !leader_runtime_a.isActive(3012)) : (leader_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(leader_runtime_a.isActive(3012));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.gained_events);

    try cluster.restartNode(0);

    const restarted_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(restarted_a);
    try cluster.node(1).apply(.{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = restarted_a, .metadata = "" }} } });
    try cluster.node(2).apply(.{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = restarted_a, .metadata = "" }} } });
    try cluster.node(0).applyBatch(&.{
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3012, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });

    var fence_rounds: usize = 0;
    while (fence_rounds < 96 and leader_runtime_a.isActive(3012)) : (fence_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(!leader_runtime_a.isActive(3012));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.lost_events);

    const leader_after_restart = try cluster.waitForLeader(3012, 96);
    try std.testing.expect(leader_after_restart != null);

    if (leader_after_restart == 1) {
        var settle_a: usize = 0;
        while (settle_a < 64 and !leader_runtime_a.isActive(3012)) : (settle_a += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_a.isActive(3012));
        try std.testing.expectEqual(@as(u64, 2), leader_runtime_a.metrics.gained_events);
        try std.testing.expect(!leader_runtime_b.isActive(3012));
        try std.testing.expect(!leader_runtime_c.isActive(3012));
    } else if (leader_after_restart == 2) {
        var settle_b: usize = 0;
        while (settle_b < 64 and !leader_runtime_b.isActive(3012)) : (settle_b += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_b.isActive(3012));
        try std.testing.expectEqual(@as(u64, 1), leader_runtime_b.metrics.gained_events);
        try std.testing.expect(!leader_runtime_a.isActive(3012));
        try std.testing.expect(!leader_runtime_c.isActive(3012));
    } else {
        try std.testing.expectEqual(@as(?u64, 3), leader_after_restart);
        var settle_c: usize = 0;
        while (settle_c < 64 and !leader_runtime_c.isActive(3012)) : (settle_c += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_c.isActive(3012));
        try std.testing.expectEqual(@as(u64, 1), leader_runtime_c.metrics.gained_events);
        try std.testing.expect(!leader_runtime_a.isActive(3012));
        try std.testing.expect(!leader_runtime_b.isActive(3012));
    }
}

test "managed http cluster simulation fences real db enrichment runtimes across ordinary leader loss" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) db_enrichment_runtime_factory.GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp_a = std.testing.tmpDir(.{});
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{});
    defer tmp_b.cleanup();
    var tmp_c = std.testing.tmpDir(.{});
    defer tmp_c.cleanup();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-loss-db-a", .{tmp_a.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-loss-db-b", .{tmp_b.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/leader-loss-db-c", .{tmp_c.sub_path});
    defer std.testing.allocator.free(root_c);
    const enrich_root_a = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_a});
    defer std.testing.allocator.free(enrich_root_a);
    const enrich_root_b = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_b});
    defer std.testing.allocator.free(enrich_root_b);
    const enrich_root_c = try std.fmt.allocPrint(std.testing.allocator, "{s}/enrich", .{root_c});
    defer std.testing.allocator.free(enrich_root_c);

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var persist_a = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_a.deinit();
    var persist_b = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_b.deinit();
    var persist_c = StorageRecorder{ .alloc = std.testing.allocator };
    defer persist_c.deinit();
    try persist_a.registerStore(3013, &store_a);
    try persist_b.registerStore(3013, &store_b);
    try persist_c.registerStore(3013, &store_c);

    var factory_a = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_a,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_b = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_b,
        .peers = &.{ 1, 2, 3 },
    };
    var factory_c = SimulationDescriptorFactory{
        .alloc = std.testing.allocator,
        .store = &store_c,
        .peers = &.{ 1, 2, 3 },
    };

    var deterministic_a = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_b = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_c = embedder_mod.DeterministicDenseEmbedder{};
    var resolver_a = Resolver{ .root = enrich_root_a };
    var resolver_b = Resolver{ .root = enrich_root_b };
    var resolver_c = Resolver{ .root = enrich_root_c };
    var db_factory_a = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-1",
                .dense_embedder = deterministic_a.interface(),
            },
        },
        .owner_id = "node-1",
    }, resolver_a.iface());
    var db_factory_b = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-2",
                .dense_embedder = deterministic_b.interface(),
            },
        },
        .owner_id = "node-2",
    }, resolver_b.iface());
    var db_factory_c = db_enrichment_runtime_factory.OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-3",
                .dense_embedder = deterministic_c.interface(),
            },
        },
        .owner_id = "node-3",
    }, resolver_c.iface());

    var exec_a = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_a.iface(),
    );
    defer exec_a.deinit();
    var exec_b = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_b.iface(),
    );
    defer exec_b.deinit();
    var exec_c = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        db_factory_c.iface(),
    );
    defer exec_c.deinit();
    var leader_runtime_a = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_a.executor());
    var leader_runtime_b = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_b.executor());
    var leader_runtime_c = enrichment_runtime.LeaderEnrichmentRuntime.init(exec_c.executor());

    const configs = [_]ManagedHttpHostSimulationConfig{
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 1 }, .transport = .{ .snapshot = .{ .root_dir = root_a } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 2 }, .transport = .{ .snapshot = .{ .root_dir = root_b } } } } },
        .{ .host = .{ .http = .{ .host = .{ .local_node_id = 3 }, .transport = .{ .snapshot = .{ .root_dir = root_c } } } } },
    };
    const deps = [_]ManagedHttpHostSimulationDeps{
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_a.iface(), .runtime_hooks = .{ .group_storage = persist_a.iface() } } }, .leader_observer = leader_runtime_a.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_b.iface(), .runtime_hooks = .{ .group_storage = persist_b.iface() } } }, .leader_observer = leader_runtime_b.observer() } },
        .{ .host = .{ .http = .{ .host = .{ .descriptor_factory = factory_c.iface(), .runtime_hooks = .{ .group_storage = persist_c.iface() } } }, .leader_observer = leader_runtime_c.observer() } },
    };

    var cluster = try ManagedHttpClusterSimulation.init(std.testing.allocator, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();

    const base_a = try cluster.node(0).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_a);
    const base_b = try cluster.node(1).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_b);
    const base_c = try cluster.node(2).baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_c);

    try cluster.node(0).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3013, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 2, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(1).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3013, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 3 } } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 3, .endpoints = &.{.{ .protocol = .http, .address = base_c, .metadata = "" }} } },
    });
    try cluster.node(2).applyBatch(&.{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = 3013, .replica_id = 3, .local_node_id = 3 }, .peer_node_ids = &.{ 1, 2 } } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 1, .endpoints = &.{.{ .protocol = .http, .address = base_a, .metadata = "" }} } },
        .{ .upsert_peer_route = .{ .group_id = 3013, .node_id = 2, .endpoints = &.{.{ .protocol = .http, .address = base_b, .metadata = "" }} } },
    });

    try cluster.stepAll();
    try cluster.node(0).campaignGroup(3013);
    try std.testing.expectEqual(@as(?u64, 1), try cluster.waitForLeader(3013, 64));

    var leader_settle: usize = 0;
    while (leader_settle < 64 and !leader_runtime_a.isActive(3013)) : (leader_settle += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(leader_runtime_a.isActive(3013));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.gained_events);

    const blackhole = [_]peer_resolver.PeerEndpoint{
        .{ .protocol = .http, .address = "http://127.0.0.1:1", .metadata = "" },
    };
    try std.testing.expectEqual(@as(usize, 1), try cluster.node(0).upsertPeerRoute(3013, 2, blackhole[0..]));
    try std.testing.expectEqual(@as(usize, 1), try cluster.node(0).upsertPeerRoute(3013, 3, blackhole[0..]));

    var loss_rounds: usize = 0;
    while (loss_rounds < 128 and leader_runtime_a.isActive(3013)) : (loss_rounds += 1) {
        try cluster.stepAll();
    }
    try std.testing.expect(!leader_runtime_a.isActive(3013));
    try std.testing.expectEqual(@as(u64, 1), leader_runtime_a.metrics.lost_events);

    const leader_after_loss = try cluster.waitForLeader(3013, 128);
    try std.testing.expect(leader_after_loss == 2 or leader_after_loss == 3);

    if (leader_after_loss == 2) {
        var settle_b: usize = 0;
        while (settle_b < 64 and !leader_runtime_b.isActive(3013)) : (settle_b += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_b.isActive(3013));
        try std.testing.expectEqual(@as(u64, 1), leader_runtime_b.metrics.gained_events);
        try std.testing.expect(!leader_runtime_c.isActive(3013));
    } else {
        var settle_c: usize = 0;
        while (settle_c < 64 and !leader_runtime_c.isActive(3013)) : (settle_c += 1) {
            try cluster.stepAll();
        }
        try std.testing.expect(leader_runtime_c.isActive(3013));
        try std.testing.expectEqual(@as(u64, 1), leader_runtime_c.metrics.gained_events);
        try std.testing.expect(!leader_runtime_b.isActive(3013));
    }
}
