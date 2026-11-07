// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional cold graph topology observations. These are declared static kernel
//! nodes, not completed GPU kernels, device-dynamic launches or opaque totals.
//! Bindings: https://docs.nvidia.com/cuda/cuda-driver-api/cuda_driver_api/group__CUDA__GRAPH.html
const std = @import("std");
const driver = @import("driver.zig");
const platform = @import("antfly_platform");
const Counters = @import("../../execution/runtime_telemetry.zig").CudaCounters;
pub const GetNodes = *const fn (driver.CUgraph, ?[*]driver.CUgraphNode, *usize) callconv(.c) driver.CUresult;
pub const GetType = *const fn (driver.CUgraphNode, *c_uint) callconv(.c) driver.CUresult;
pub const GetChild = *const fn (driver.CUgraphNode, *driver.CUgraph) callconv(.c) driver.CUresult;
pub const Api = struct { get_nodes: ?GetNodes = null, get_type: ?GetType = null, get_child: ?GetChild = null };
pub const Reason = enum { unavailable, invalid_handle, driver_error, empty_graph, conditional_graph, host_callback, unknown_node, depth_budget, node_budget, graph_cycle, graph_changed, allocation_failure };
pub const Known = struct { static_kernel_nodes: u64 = 0, static_graph_nodes: u64 = 0 };
pub const Topology = union(enum) { known: Known, unavailable: Reason };
pub const Limits = struct { max_nodes: usize = 65536, max_depth: usize = 16 };
const WalkError = std.mem.Allocator.Error || error{ Unavailable, InvalidHandle, DriverError, EmptyGraph, Conditional, HostCallback, UnknownNode, DepthBudget, NodeBudget, Cycle, GraphChanged, Overflow };

/// Caller owns exclusive graph access and keeps source graphs alive. No GPU
/// synchronization is inserted. OOM is returned so optional callers can record
/// unavailable coverage without failing their otherwise successful inference.
pub fn inspect(allocator: std.mem.Allocator, api: Api, graph: driver.CUgraph, limits: Limits) std.mem.Allocator.Error!Topology {
    var remaining = limits.max_nodes;
    var ancestry: [64]driver.CUgraph = @splat(null);
    return .{ .known = walk(allocator, api, graph, limits, &remaining, &ancestry, 0) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{ .unavailable = switch (err) {
            error.Unavailable => .unavailable,
            error.InvalidHandle => .invalid_handle,
            error.DriverError => .driver_error,
            error.EmptyGraph => .empty_graph,
            error.Conditional => .conditional_graph,
            error.HostCallback => .host_callback,
            error.UnknownNode => .unknown_node,
            error.DepthBudget => .depth_budget,
            error.NodeBudget, error.Overflow => .node_budget,
            error.Cycle => .graph_cycle,
            error.GraphChanged => .graph_changed,
            error.OutOfMemory => unreachable,
        } };
    } };
}
fn walk(allocator: std.mem.Allocator, api: Api, graph: driver.CUgraph, limits: Limits, remaining: *usize, ancestry: *[64]driver.CUgraph, depth: usize) WalkError!Known {
    if (graph == null) return error.InvalidHandle;
    if (depth >= @min(limits.max_depth, ancestry.len)) return error.DepthBudget;
    for (ancestry[0..depth]) |parent| if (parent == graph) return error.Cycle;
    ancestry[depth] = graph;
    const get_nodes = api.get_nodes orelse return error.Unavailable;
    const get_type = api.get_type orelse return error.Unavailable;
    var count: usize = 0;
    if (get_nodes(graph, null, &count) != driver.CUDA_SUCCESS) return error.DriverError;
    if (count == 0) return error.EmptyGraph;
    if (count > remaining.*) return error.NodeBudget;
    remaining.* -= count;
    const nodes = try allocator.alloc(driver.CUgraphNode, count);
    defer allocator.free(nodes);
    var filled = count;
    if (get_nodes(graph, nodes.ptr, &filled) != driver.CUDA_SUCCESS) return error.DriverError;
    if (filled != count) return error.GraphChanged;
    var result = Known{ .static_graph_nodes = count };
    for (nodes) |node| {
        if (node == null) return error.InvalidHandle;
        var kind: c_uint = undefined;
        if (get_type(node, &kind) != driver.CUDA_SUCCESS) return error.DriverError;
        switch (kind) {
            0 => result.static_kernel_nodes = try std.math.add(u64, result.static_kernel_nodes, 1),
            4 => {
                const get_child = api.get_child orelse return error.Unavailable;
                var child: driver.CUgraph = null;
                if (get_child(node, &child) != driver.CUDA_SUCCESS) return error.DriverError;
                const nested = try walk(allocator, api, child, limits, remaining, ancestry, depth + 1);
                result.static_kernel_nodes = try std.math.add(u64, result.static_kernel_nodes, nested.static_kernel_nodes);
                result.static_graph_nodes = try std.math.add(u64, result.static_graph_nodes, nested.static_graph_nodes);
            },
            3 => return error.HostCallback,
            13 => return error.Conditional,
            1, 2, 5...12 => {},
            else => return error.UnknownNode,
        }
    }
    return result;
}

const Mock = struct {
    fn handle(value: usize) ?*anyopaque {
        return @ptrFromInt(value);
    }
    fn nodes(graph: driver.CUgraph, output: ?[*]driver.CUgraphNode, count: *usize) callconv(.c) driver.CUresult {
        const ids: []const usize = switch (@intFromPtr(graph)) {
            1 => &.{ 11, 12, 13 },
            2 => &.{ 21, 22 },
            3 => &.{31},
            4 => &.{41},
            5 => &.{51},
            6 => &.{},
            else => return 1,
        };
        if (output) |out| for (ids[0..@min(count.*, ids.len)], 0..) |id, i| {
            out[i] = handle(id);
        };
        count.* = ids.len;
        return driver.CUDA_SUCCESS;
    }
    fn kind(node: driver.CUgraphNode, output: *c_uint) callconv(.c) driver.CUresult {
        output.* = switch (@intFromPtr(node)) {
            11, 21, 22 => 0,
            12, 41 => 4,
            13 => 1,
            31 => 13,
            51 => 99,
            else => return 1,
        };
        return driver.CUDA_SUCCESS;
    }
    fn child(node: driver.CUgraphNode, output: *driver.CUgraph) callconv(.c) driver.CUresult {
        output.* = switch (@intFromPtr(node)) {
            12 => handle(2),
            41 => handle(4),
            else => return 1,
        };
        return driver.CUDA_SUCCESS;
    }
    const api = Api{ .get_nodes = nodes, .get_type = kind, .get_child = child };
};

test "cold CUDA graph topology counts nested static nodes and preserves opaque scopes" {
    const a = std.testing.allocator;
    const result = try inspect(a, Mock.api, Mock.handle(1), .{});
    try std.testing.expectEqual(@as(u64, 3), result.known.static_kernel_nodes);
    try std.testing.expectEqual(@as(u64, 5), result.known.static_graph_nodes);
    try std.testing.expectEqual(Reason.conditional_graph, (try inspect(a, Mock.api, Mock.handle(3), .{})).unavailable);
    try std.testing.expectEqual(Reason.graph_cycle, (try inspect(a, Mock.api, Mock.handle(4), .{})).unavailable);
    try std.testing.expectEqual(Reason.unknown_node, (try inspect(a, Mock.api, Mock.handle(5), .{})).unavailable);
    try std.testing.expectEqual(Reason.empty_graph, (try inspect(a, Mock.api, Mock.handle(6), .{})).unavailable);
    try std.testing.expectEqual(Reason.node_budget, (try inspect(a, Mock.api, Mock.handle(1), .{ .max_nodes = 4 })).unavailable);
    try std.testing.expectEqual(Reason.depth_budget, (try inspect(a, Mock.api, Mock.handle(1), .{ .max_depth = 1 })).unavailable);
    try std.testing.expectEqual(Reason.unavailable, (try inspect(a, .{}, Mock.handle(1), .{})).unavailable);
}

test "cold CUDA graph topology introspection unwinds every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const result = try inspect(a, Mock.api, Mock.handle(1), .{});
            try std.testing.expectEqual(@as(u64, 3), result.known.static_kernel_nodes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

/// Optional per-context metadata. Construct only when observations are enabled;
/// graph registration/updates are cold and launch uses a cached address lookup.
const AtomicCounters = blk: {
    const fields = @typeInfo(Counters).@"struct";
    break :blk @Struct(.auto, null, fields.field_names, &@as([fields.field_names.len]type, @splat(std.atomic.Value(u64))), &@splat(.{}));
};
fn initAtomicCounters() AtomicCounters {
    var result: AtomicCounters = undefined;
    inline for (@typeInfo(Counters).@"struct".field_names) |name| @field(result, name) = .init(0);
    return result;
}

pub const State = struct {
    allocator: std.mem.Allocator,
    graphs: std.AutoHashMapUnmanaged(usize, Topology) = .empty,
    atomic_counters: AtomicCounters = initAtomicCounters(),
    graph_mutex: std.atomic.Mutex = .unlocked,

    fn increment(self: *State, comptime name: []const u8, amount: u64) void {
        const counter = &@field(self.atomic_counters, name);
        var previous = counter.load(.monotonic);
        while (true) {
            if (previous == std.math.maxInt(u64)) return;
            previous = counter.cmpxchgWeak(previous, previous +| amount, .monotonic, .monotonic) orelse return;
        }
    }
    pub fn counterValue(self: *const State, comptime name: []const u8) u64 {
        return @field(self.atomic_counters, name).load(.monotonic);
    }
    fn countersSnapshot(self: *const State) Counters {
        var value = Counters{};
        inline for (@typeInfo(Counters).@"struct".field_names) |name| @field(value, name) = self.counterValue(name);
        return value;
    }
    pub fn foreignKernelCall(self: *State) void {
        self.increment("foreign_stream_kernel_calls", 1);
    }
    pub fn hostSynchronization(self: *State, event: bool, result: driver.CUresult) void {
        if (result != driver.CUDA_SUCCESS) self.increment("failed_host_synchronizations", 1) else if (event) self.increment("host_event_synchronizations", 1) else self.increment("host_stream_synchronizations", 1);
    }
    pub fn streamDependency(self: *State, owned_stream: bool, recording: bool, result: driver.CUresult) void {
        if (result != driver.CUDA_SUCCESS) self.increment("failed_gpu_stream_dependencies", 1) else if (!owned_stream) self.increment("foreign_stream_wait_event_calls", 1) else if (recording) self.increment("capture_recording_stream_wait_events", 1) else self.increment("gpu_stream_dependencies", 1);
    }

    pub fn create(allocator: std.mem.Allocator) !*State {
        const self = try allocator.create(State);
        self.* = .{ .allocator = allocator };
        return self;
    }
    pub fn deinit(self: *State) void {
        const allocator = self.allocator;
        self.graphs.deinit(allocator);
        allocator.destroy(self);
    }
    pub fn register(self: *State, api: Api, graph: driver.CUgraph, executable: driver.CUgraphExec) void {
        const topology = inspect(self.allocator, api, graph, .{}) catch blk: {
            self.increment("metadata_failures", 1);
            break :blk Topology{ .unavailable = .allocation_failure };
        };
        platform.sync.lockYielding(&self.graph_mutex);
        defer self.graph_mutex.unlock();
        self.graphs.put(self.allocator, @intFromPtr(executable), topology) catch {
            _ = self.graphs.remove(@intFromPtr(executable));
            self.increment("metadata_failures", 1);
        };
    }
    pub fn retire(self: *State, executable: driver.CUgraphExec) void {
        platform.sync.lockYielding(&self.graph_mutex);
        defer self.graph_mutex.unlock();
        _ = self.graphs.remove(@intFromPtr(executable));
    }
    pub fn kernelCall(self: *State, captured: bool) void {
        if (captured) self.increment("capture_recording_kernel_calls", 1) else self.increment("direct_kernel_submissions", 1);
    }
    pub fn graphCall(self: *State, executable: driver.CUgraphExec, captured: bool) void {
        if (captured) {
            self.increment("capture_recording_graph_calls", 1);
            return;
        }
        platform.sync.lockYielding(&self.graph_mutex);
        const cached = self.graphs.get(@intFromPtr(executable));
        self.graph_mutex.unlock();
        if (cached) |topology| if (topology == .known) {
            self.increment("known_graph_submissions", 1);
            self.increment("declared_graph_kernel_nodes_submitted", topology.known.static_kernel_nodes);
            self.increment("declared_graph_nodes_submitted", topology.known.static_graph_nodes);
            return;
        };
        self.increment("opaque_graph_submissions", 1);
    }
    pub fn snapshot(self: *State) @import("../../execution/runtime_telemetry.zig").Cuda {
        platform.sync.lockYielding(&self.graph_mutex);
        const count = self.graphs.count();
        self.graph_mutex.unlock();
        return .{ .owner_identity = @intFromPtr(self), .counters = self.countersSnapshot(), .registered_graphs = count };
    }
};

test "CUDA submission observations separate recording direct static and opaque graph calls" {
    const state = try State.create(std.testing.allocator);
    defer state.deinit();
    const executable = Mock.handle(100);
    state.register(Mock.api, Mock.handle(1), executable);
    state.kernelCall(true);
    state.kernelCall(false);
    state.graphCall(executable, true);
    state.graphCall(executable, false);
    state.graphCall(Mock.handle(200), false);
    state.retire(executable);
    state.graphCall(executable, false);
    const counters = state.snapshot().counters.?;
    try std.testing.expectEqual(@as(u64, 1), counters.direct_kernel_submissions);
    try std.testing.expectEqual(@as(u64, 1), counters.capture_recording_kernel_calls);
    try std.testing.expectEqual(@as(u64, 1), counters.capture_recording_graph_calls);
    try std.testing.expectEqual(@as(u64, 1), counters.known_graph_submissions);
    try std.testing.expectEqual(@as(u64, 3), counters.declared_graph_kernel_nodes_submitted);
    try std.testing.expectEqual(@as(u64, 2), counters.opaque_graph_submissions);
}

test "CUDA graph metadata failure replaces stale known topology with opaque coverage" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const state = try State.create(failing.allocator());
    defer state.deinit();
    const executable = Mock.handle(100);
    state.register(Mock.api, Mock.handle(1), executable);
    try std.testing.expect(state.graphs.get(@intFromPtr(executable)).? == .known);
    failing.fail_index = failing.alloc_index;
    // The numerical graph update has already succeeded. Observation failure
    // must neither fail inference nor keep the old topology's known count.
    state.register(Mock.api, Mock.handle(3), executable);
    state.graphCall(executable, false);
    const measured = state.snapshot().counters.?;
    try std.testing.expect(measured.metadata_failures > 0);
    try std.testing.expectEqual(@as(u64, 0), measured.known_graph_submissions);
    try std.testing.expectEqual(@as(u64, 1), measured.opaque_graph_submissions);
}

test "CUDA graph metadata map allocation failure stays optional and opaque" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const state = try State.create(failing.allocator());
    defer state.deinit();
    failing.fail_index = failing.alloc_index;
    const executable = Mock.handle(100);
    // An unavailable API needs no temporary node array; the denied allocation
    // is exactly the optional executable topology map's first capacity.
    state.register(.{}, Mock.handle(1), executable);
    state.graphCall(executable, false);
    try std.testing.expectEqual(@as(usize, 0), state.graphs.count());
    const measured = state.snapshot().counters.?;
    try std.testing.expectEqual(@as(u64, 1), measured.metadata_failures);
    try std.testing.expectEqual(@as(u64, 1), measured.opaque_graph_submissions);
}

test "optional CUDA counters and graph metadata support concurrent context workers" {
    const Worker = struct {
        fn run(state: *State) void {
            for (0..1000) |_| {
                state.kernelCall(false);
                state.hostSynchronization(false, driver.CUDA_SUCCESS);
                state.streamDependency(true, false, driver.CUDA_SUCCESS);
                state.register(Mock.api, Mock.handle(1), Mock.handle(100));
                state.graphCall(Mock.handle(100), false);
                state.retire(Mock.handle(100));
            }
        }
    };
    const state = try State.create(std.testing.allocator);
    defer state.deinit();
    var future = try std.testing.io.concurrent(Worker.run, .{state});
    defer future.await(std.testing.io);
    Worker.run(state);
    future.await(std.testing.io);
    const counters = state.snapshot().counters.?;
    try std.testing.expectEqual(@as(u64, 2000), counters.direct_kernel_submissions);
    try std.testing.expectEqual(@as(u64, 2000), counters.host_stream_synchronizations);
    try std.testing.expectEqual(@as(u64, 2000), counters.gpu_stream_dependencies);
    try std.testing.expectEqual(@as(u64, 2000), counters.known_graph_submissions + counters.opaque_graph_submissions);
    try std.testing.expectEqual(@as(u64, 0), counters.metadata_failures);
}

test "optional CUDA observation counters saturate instead of wrapping" {
    const state = try State.create(std.testing.allocator);
    defer state.deinit();
    state.atomic_counters.direct_kernel_submissions.store(std.math.maxInt(u64) - 1, .monotonic);
    state.kernelCall(false);
    state.kernelCall(false);
    try std.testing.expectEqual(std.math.maxInt(u64), state.snapshot().counters.?.direct_kernel_submissions);
}
