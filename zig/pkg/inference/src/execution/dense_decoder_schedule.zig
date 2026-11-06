// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One request's authored dense decoder schedule. The graph owns liveness only:
//! no optimizer or second numerical forward changes the architecture's calls.
//! Persistent Native headers are borrowed by the eager semantic dispatcher;
//! the single owned Frame stays entered until decoder teardown.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const Node = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const route = @import("kernel_route.zig");
const physical = @import("physical_buffers.zig");
const Cleanup = @import("cleanup_context.zig").Context;

/// Cold metadata may be physically freed under the actual decoder Model gate.
/// Accept only reclaiming allocators with no admission/reclamation callbacks;
/// never unwrap a caller quota, bounded owner or arena to bypass its contract.
/// The standalone serving path supplies the process C/SMP allocator. Unknown
/// embedded callers keep the complete original compatibility route.
pub fn preparationBackingSupported(a: std.mem.Allocator) bool {
    if (@import("program.zig").processMetadataAllocator(a)) return true;
    if (comptime @import("builtin").is_test) {
        const testing = std.testing.allocator;
        if (a.vtable == testing.vtable and a.ptr == testing.ptr) return true;
        // Exact std test instrumentation only; arbitrary callback wrappers are
        // still refused. Preserve real allocation-failure enumeration.
        var probe = std.testing.FailingAllocator.init(testing, .{});
        if (a.vtable == probe.allocator().vtable) {
            const owner: *const std.testing.FailingAllocator = @ptrCast(@alignCast(a.ptr));
            return preparationBackingSupported(owner.internal_allocator);
        }
    }
    return false;
}

pub const Builder = struct {
    graph: ml.graph.Graph,
    descriptors: std.ArrayList(?ops.kernel_descriptor.Descriptor) = .empty,
    values: std.ArrayList(?CT) = .empty,
    bindings: std.ArrayList(route.InputBinding) = .empty,
    immutable_resident: std.ArrayList(route.InputBinding) = .empty,
    steps: std.ArrayList(Node) = .empty,
    planes: std.ArrayList(*@import("storage_ownership.zig").Plane) = .empty,
    backend: ops.ComputeBackend,

    pub fn init(a: std.mem.Allocator, cb: *const ops.ComputeBackend) Builder {
        return .{ .graph = .init(a), .backend = cb.withoutSynchronousBorrows() };
    }
    pub fn deinit(self: *Builder) void {
        const a = self.graph.allocator;
        self.descriptors.deinit(a);
        self.values.deinit(a);
        self.bindings.deinit(a);
        self.immutable_resident.deinit(a);
        self.steps.deinit(a);
        for (self.planes.items) |plane| a.destroy(plane);
        self.planes.deinit(a);
        self.graph.deinit();
    }
    pub fn parameter(self: *Builder, shape: Shape, value: CT) !Node {
        return self.parameterWithPlane(shape, value, try residentPlane(&self.backend, value));
    }
    fn parameterWithPlane(self: *Builder, shape: Shape, value: CT, observed: @import("storage_ownership.zig").Plane) !Node {
        var b = ml.graph.Builder.init(&self.graph);
        var name: [40]u8 = undefined;
        const id = try b.parameter(try std.fmt.bufPrint(&name, "dense.input.{d}", .{self.graph.nodeCount()}), shape);
        try self.descriptors.append(self.graph.allocator, null);
        try self.values.append(self.graph.allocator, value);
        const binding: route.Binding = block: {
            const plane = try self.graph.allocator.create(@import("storage_ownership.zig").Plane);
            errdefer self.graph.allocator.destroy(plane);
            plane.* = observed;
            try self.planes.append(self.graph.allocator, plane);
            break :block .{ .resident = .{ .logical = shape, .planes = @as([*]const @import("storage_ownership.zig").Plane, @ptrCast(plane))[0..1], .implementation = .{ .context = self.backend.ptr, .table = self.backend.vtable } } };
        };
        try self.bindings.append(self.graph.allocator, .{ .node_id = id, .value = binding });
        return id;
    }
    /// An actual typed held-source proof, not a partial generic storage report.
    /// The architecture retains the immutable witness and Model/store owner;
    /// the existing F32 Direct route receives only this borrowed primary.
    pub fn residentWeight(self: *Builder, value: CT, plane: @import("storage_ownership.zig").Plane) !Node {
        const node = try self.parameterWithPlane(plane.descriptor.shape, value, plane);
        const binding = self.bindings.pop().?;
        std.debug.assert(binding.node_id == node and binding.value == .resident);
        try self.immutable_resident.append(self.graph.allocator, binding);
        return node;
    }
    pub fn hostParameter(self: *Builder, shape: Shape) !Node {
        var b = ml.graph.Builder.init(&self.graph);
        var name: [40]u8 = undefined;
        const id = try b.parameter(try std.fmt.bufPrint(&name, "dense.host.{d}", .{self.graph.nodeCount()}), shape);
        try self.descriptors.append(self.graph.allocator, null);
        try self.values.append(self.graph.allocator, null);
        try self.bindings.append(self.graph.allocator, .{ .node_id = id, .value = .{ .imported_host = .{ .logical = shape, .target = .native, .owned_ingress = true, .borrowable_host = false } } });
        return id;
    }
    pub fn gather(self: *Builder, weight: Node, indices: Node, width: usize) !Node {
        var b = ml.graph.Builder.init(&self.graph);
        const id = try b.gather(weight, indices, Shape.init(.f32, &.{ 1, @intCast(width) }));
        try self.descriptors.append(self.graph.allocator, ops.kernel_descriptor.lower(&self.graph, id) orelse return error.UnsupportedDenseSchedule);
        try self.values.append(self.graph.allocator, null);
        try self.steps.append(self.graph.allocator, id);
        return id;
    }
    pub fn operation(self: *Builder, op: ml.graph.OpCode, inputs: []const Node, descriptor: ops.kernel_descriptor.Descriptor) !Node {
        var node = ml.graph.Node{ .op = op, .output_shape = descriptor.output_shape, .num_inputs = @intCast(inputs.len) };
        @memcpy(node.inputs[0..inputs.len], inputs);
        const id = try self.graph.addNode(node);
        try self.descriptors.append(self.graph.allocator, descriptor);
        try self.values.append(self.graph.allocator, null);
        try self.steps.append(self.graph.allocator, id);
        return id;
    }
    pub fn output(self: *Builder, id: Node) !void {
        try self.graph.markOutput(id);
    }
    pub fn logicalShape(self: *const Builder, id: Node) Shape {
        return self.graph.node(id).output_shape;
    }
    pub fn add(self: *Builder, a: Node, b: Node) !Node {
        const s = self.logicalShape(a);
        return self.operation(.add, &.{ a, b }, .{ .operation = .{ .binary = .add }, .output_shape = s, .input_shapes = .{ s, self.logicalShape(b), undefined }, .input_count = 2 });
    }
    pub fn norm(self: *Builder, x: Node, w: Node, b: Node, width: usize) !Node {
        const dim = std.math.cast(u32, width) orelse return error.DimensionOverflow;
        return self.operation(.{ .fused_layer_norm = .{ .dim = dim, .eps = 1e-5 } }, &.{ x, w, b }, .{ .operation = .{ .norm = .{ .kind = .layer, .dim = dim, .eps = 1e-5 } }, .output_shape = self.logicalShape(x), .input_shapes = .{ self.logicalShape(x), self.logicalShape(w), self.logicalShape(b) }, .input_count = 3 });
    }
    pub fn linear(self: *Builder, x: Node, w: Node, b: ?Node, width: usize, out: usize) !Node {
        return self.linearRows(x, w, b, 1, width, out);
    }
    pub fn linearRows(self: *Builder, x: Node, w: Node, b: ?Node, rows: usize, width: usize, out: usize) !Node {
        const geometry = try ops.kernel_descriptor.Linear.init(rows, width, out, b != null);
        const op: ml.graph.OpCode = if (b != null) .{ .fused_linear = .{ .rows = geometry.rows, .in_dim = geometry.in_dim, .out_dim = geometry.out_dim } } else .{ .fused_linear_no_bias = .{ .rows = geometry.rows, .in_dim = geometry.in_dim, .out_dim = geometry.out_dim } };
        var inputs = [_]Node{ x, w, b orelse ml.graph.null_node };
        return self.operation(op, inputs[0..if (b != null) @as(usize, 3) else 2], .{ .operation = .{ .linear = geometry }, .output_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(out) }), .input_shapes = .{ self.logicalShape(x), self.logicalShape(w), if (b) |id| self.logicalShape(id) else undefined }, .input_count = if (b != null) 3 else 2 });
    }
    pub fn prefix(self: *Builder, q: Node, k: Node, v: Node, heads: usize, dim: usize, capacity: usize) !Node {
        const geometry = ops.kernel_descriptor.DenseAttentionPrefix{ .num_heads = std.math.cast(u32, heads) orelse return error.DimensionOverflow, .head_dim = std.math.cast(u32, dim) orelse return error.DimensionOverflow, .max_key_rows = std.math.cast(u32, capacity) orelse return error.DimensionOverflow };
        return self.operation(.{ .fused_cross_attention = .{ .batch = 1, .dec_seq = 1, .enc_seq = geometry.max_key_rows, .num_heads = geometry.num_heads, .head_dim = geometry.head_dim } }, &.{ q, k, v }, .{ .operation = .{ .dense_attention_prefix = geometry }, .input_count = 3, .output_shape = self.logicalShape(q), .input_shapes = .{ self.logicalShape(q), self.logicalShape(k), self.logicalShape(v) } });
    }
    pub fn gelu(self: *Builder, x: Node) !Node {
        return self.operation(.{ .fused_gelu = {} }, &.{x}, .{ .operation = .{ .unary = .gelu }, .output_shape = self.logicalShape(x), .input_shapes = .{ self.logicalShape(x), undefined, undefined }, .input_count = 1 });
    }
};

// These facts come from the actual accepted backend, not fabricated planned
// predecessor planes. The retained state/input owner outlives this schedule.
pub fn residentPlane(cb: *const ops.ComputeBackend, value: CT) !@import("storage_ownership.zig").Plane {
    const storage = @import("storage_ownership.zig");
    const State = struct {
        plane: ?storage.Plane = null,
        fn emit(raw: *anyopaque, report: storage.Report) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (report == .plane and report.plane.role == .primary) self.plane = report.plane;
        }
    };
    const kernels = cb.vtable.kernels orelse return error.UnsupportedDenseSchedule;
    if (!kernels.accepts(cb.vtable)) return error.UnsupportedDenseSchedule;
    var state: State = .{};
    const coverage = try (kernels.storageOwnership orelse return error.UnsupportedDenseSchedule)(cb.ptr, value, .{ .context = &state, .visit = State.emit });
    if (coverage != .complete) return error.UnsupportedDenseSchedule;
    const plane = state.plane orelse return error.UnsupportedDenseSchedule;
    return plane;
}

pub fn projectionDescriptor(cb: *const ops.ComputeBackend, expected_data: *const anyopaque, input: CT, weight: CT, bias: ?CT, destination: CT, rows: usize, width: usize, out: usize) !ops.kernel_descriptor.Descriptor {
    return .{ .operation = .{ .linear = try ops.kernel_descriptor.Linear.init(rows, width, out, bias != null) }, .input_count = if (bias != null) 3 else 2, .output_shape = (try residentPlane(cb, destination)).descriptor.shape, .input_shapes = .{ (try residentPlane(cb, input)).descriptor.shape, (try @import("../ops/native_compute.zig").NativeCompute.heldF32WeightPrimary(cb, expected_data, weight)).descriptor.shape, if (bias) |b| (try @import("../ops/native_compute.zig").NativeCompute.heldF32WeightPrimary(cb, expected_data, b)).descriptor.shape else undefined } };
}

pub const Schedule = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    graph: ml.graph.Graph,
    descriptors: []?ops.kernel_descriptor.Descriptor,
    values: []?CT,
    steps: []Node,
    root: *physical.Pool,
    selected: *@import("prepared_routes.zig").PreparedRoutes,
    frame: ?*physical.Frame = null,
    numerical_epoch: ?@import("retirement_sink.zig").Epoch = null,
    borrowed: std.AutoHashMapUnmanaged(CT, void) = .empty,
    cursor: usize = 0,
    analysis: @import("../graph/interpreter.zig").CachedAnalysis,
    bindings: []route.InputBinding,
    planes: []*@import("storage_ownership.zig").Plane,

    /// Creates only metadata. All actual physical banks are funded separately
    /// before enter, without using the architecture's compatibility allowance.
    pub fn create(builder: *Builder) !*Schedule {
        const a = builder.graph.allocator;
        const self = try a.create(Schedule);
        errdefer a.destroy(self);
        self.allocator = a;
        self.backend = builder.backend;
        self.graph = builder.graph;
        builder.graph = ml.graph.Graph.init(a);
        errdefer self.graph.deinit();
        self.descriptors = try builder.descriptors.toOwnedSlice(a);
        errdefer a.free(self.descriptors);
        self.values = try builder.values.toOwnedSlice(a);
        errdefer a.free(self.values);
        self.steps = try builder.steps.toOwnedSlice(a);
        errdefer a.free(self.steps);
        self.planes = try builder.planes.toOwnedSlice(a);
        errdefer {
            for (self.planes) |plane| a.destroy(plane);
            a.free(self.planes);
        }
        self.bindings = try builder.bindings.toOwnedSlice(a);
        errdefer a.free(self.bindings);
        self.analysis = try @import("../graph/interpreter.zig").CachedAnalysis.compute(a, &self.graph);
        errdefer self.analysis.deinit(a);
        var layout = try @import("../graph/runtime.zig").PhysicalLayout.create(a, &self.graph, &.{&self.backend}, .{}, false);
        defer layout.deinit();
        self.root = (try layout.createPool(a, &self.graph, &.{&self.backend}, self.descriptors)) orelse return error.UnsupportedDenseSchedule;
        errdefer self.root.deinit();
        self.root.coverage_graph = &self.graph;
        self.root.coverage_reachable = self.analysis.reachable;
        self.root.coverage_backend = self.backend;
        self.root.native_frame_dispatch = true;
        try self.root.configureRoutes(builder.immutable_resident.items);
        self.selected = (try self.root.prepareRoutesWithPolicy(self.bindings, .compatibility_regions)) orelse return error.UnsupportedDenseSchedule;
        errdefer self.selected.release();
        if (self.selected.pool.payload_coverage != .complete or self.selected.pool.regions.len != 0) return error.UnsupportedDenseSchedule;
        self.frame = null;
        self.numerical_epoch = null;
        self.borrowed = .empty;
        self.cursor = 0;
        return self;
    }
    pub fn setHostWorkspaceAllocator(self: *Schedule, backing: std.mem.Allocator) !void {
        try self.root.setDeclaredHostWorkspaceAllocator(backing);
        try self.selected.pool.setDeclaredHostWorkspaceAllocator(backing);
    }
    pub fn enter(self: *Schedule, capability: ?@import("storage_admission.zig").Capability, current: Cleanup, host_inputs: []const @import("host_ingress.zig").Input) !void {
        if (self.frame != null) return error.DenseScheduleAlreadyEntered;
        const frame = (try self.selected.pool.acquirePrepared(capability, null)) orelse return error.PreparedStorageUnavailable;
        self.frame = frame;
        errdefer {
            _ = frame.finishWithCleanup(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.frame = null;
        }
        try frame.beginWithCleanup(current);
        try self.bindHostInputs(host_inputs);
        try frame.prepareNativeProgramViews(&self.backend, &self.graph, self.values, current);
        try self.borrowed.ensureTotalCapacity(self.allocator, self.graph.nodeCount());
        for (frame.native_program_views) |maybe| if (maybe) |value| self.borrowed.putAssumeCapacity(value, {});
        try frame.sealNativeProgramStorage();
        try frame.finishNativeProgramPreparation();
    }
    pub fn beginExecution(self: *Schedule, current: Cleanup) !void {
        const frame = self.frame orelse return error.DenseScheduleNotEntered;
        if (frame.scope_backends != 0 or self.numerical_epoch != null) return error.DenseScheduleAlreadyEntered;
        if (current.operation != null or current.synchronous_outside_raw != null) return error.UnsupportedDenseScheduleCleanup;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        var held = try epoch.retain();
        errdefer held.release();
        if (try held.isSealed()) return error.RetirementEpochSealed;
        self.numerical_epoch = held;
        frame.beginWithCleanup(.{ .boundary = &self.numerical_epoch.? }) catch |err| {
            self.numerical_epoch = null;
            return err;
        };
    }
    pub fn bindHostInputs(self: *Schedule, inputs: []const @import("host_ingress.zig").Input) !void {
        const frame = self.frame orelse return error.DenseScheduleNotEntered;
        const bound = try frame.bindHostInputs(inputs);
        for (bound) |input| self.values[input.node_id] = input.value;
    }
    pub fn requirements(self: *Schedule, builder: *@import("prepared_storage_admission.zig").Builder, identity: @import("storage_admission.zig").Identity) !void {
        try self.selected.pool.collectAdmissionRequirements(builder, null, identity);
    }
    pub fn releaseTemporary(self: *const Schedule, cb: *const ops.ComputeBackend, value: CT) void {
        if (!self.borrowed.contains(value)) cb.free(value);
    }
    pub fn execute(self: *Schedule, inputs: []const CT, attention: ?*const ops.AttentionContext) !CT {
        if (self.cursor >= self.steps.len) return error.DenseScheduleOrderChanged;
        const id = self.steps[self.cursor];
        const node = self.graph.node(id);
        if (inputs.len != node.num_inputs) return error.DenseScheduleOrderChanged;
        for (node.getInputs(), inputs) |source, value| self.values[source] = value;
        const frame = self.frame orelse return error.DenseScheduleNotEntered;
        const result = try frame.executeNativeProgram(&self.backend, self.values, id, null, attention orelse &.{ .total_sequence_len = 0, .query_sequence_len = 0, .kv_sequence_len = 0 });
        self.cursor += 1;
        return result;
    }
    pub fn deinit(self: *Schedule, current: Cleanup) void {
        if (self.frame) |frame| _ = frame.finishWithCleanup(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.selected.releaseWithCleanup(current);
        self.root.deinitWithCleanup(current);
        if (self.numerical_epoch) |*epoch| epoch.release();
        self.numerical_epoch = null;
        self.borrowed.deinit(self.allocator);
        self.analysis.deinit(self.allocator);
        self.allocator.free(self.bindings);
        for (self.planes) |plane| self.allocator.destroy(plane);
        self.allocator.free(self.planes);
        self.allocator.free(self.steps);
        self.allocator.free(self.values);
        self.allocator.free(self.descriptors);
        self.graph.deinit();
        self.allocator.destroy(self);
    }
};
