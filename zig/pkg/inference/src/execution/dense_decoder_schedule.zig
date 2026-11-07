// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared immutable dense decoder Definitions and request-private schedules.
//! The authored graph owns liveness only:
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
const Plane = @import("storage_ownership.zig").Plane;

const input_facts = @import("prepared_routes.zig");
const PendingBinding = input_facts.IndexedInputBinding;
const BindingSchema = struct {
    values: usize,
    runtime_bindings: usize,
    immutable_bindings: usize,
    planes: usize,
};

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

/// Shared final destruction may follow Model.close at a real B boundary.
/// Only allocator contexts that outlive BOTH boundaries can back this cache.
pub fn sharedBackingSupported(a: std.mem.Allocator) bool {
    if (@import("program.zig").processMetadataAllocator(a)) return true;
    if (comptime @import("builtin").is_test) {
        const testing = std.testing.allocator;
        return a.ptr == testing.ptr and a.vtable == testing.vtable;
    }
    return false;
}

pub const Definition = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    key: [32]u8,
    retired_next: ?*Definition = null,
    shared: ?*@import("cold_metadata.zig").Shared,
    graph: ml.graph.Graph,
    descriptors: []?ops.kernel_descriptor.Descriptor,
    steps: []Node,
    analysis: @import("../graph/interpreter.zig").CachedAnalysis,
    layout: @import("../graph/runtime.zig").PhysicalLayout,
    binding_schema: BindingSchema,

    fn keyOf(self: *Definition) [32]u8 {
        return self.key;
    }
    fn equal(a: [32]u8, b: [32]u8) bool {
        return std.mem.eql(u8, &a, &b);
    }
    pub fn retain(self: *Definition) *Definition {
        const old = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(old != 0 and old != std.math.maxInt(usize));
        return self;
    }
    fn pinned(self: *Definition) bool {
        return self.references.load(.acquire) > 1;
    }
    fn destroy(raw: *anyopaque) void {
        const self: *Definition = @ptrCast(@alignCast(raw));
        self.layout.deinit();
        self.analysis.deinit(self.allocator);
        self.allocator.free(self.steps);
        self.allocator.free(self.descriptors);
        self.graph.deinit();
        self.allocator.destroy(self);
    }
    pub fn release(self: *Definition, current: Cleanup) void {
        const old = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(old != 0);
        if (old != 1) return;
        if (self.shared) |owner| owner.releaseWithCleanup(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker() else destroy(self);
    }
    fn create(builder: *Builder) !*Definition {
        const a = builder.graph.allocator;
        const self = try a.create(Definition);
        errdefer a.destroy(self);
        var graph = builder.graph;
        builder.graph = ml.graph.Graph.init(a);
        errdefer graph.deinit();
        const descriptors = try builder.descriptors.toOwnedSlice(a);
        errdefer a.free(descriptors);
        const steps = try builder.steps.toOwnedSlice(a);
        errdefer a.free(steps);
        var analysis = try @import("../graph/interpreter.zig").CachedAnalysis.compute(a, &graph);
        errdefer analysis.deinit(a);
        var layout = try @import("../graph/runtime.zig").PhysicalLayout.create(a, &graph, &.{&builder.backend}, .{}, true);
        errdefer layout.deinit();
        self.* = .{ .allocator = a, .key = if (builder.preparation) |preparation| preparation.key else @splat(0), .shared = if (builder.preparation) |preparation| preparation.shared else null, .graph = graph, .descriptors = descriptors, .steps = steps, .analysis = analysis, .layout = layout, .binding_schema = builder.schema() };
        // No fallible work follows transfer of the finalizer. Publication is
        // sealed separately by the caller after successful private preparation.
        if (self.shared) |owner| {
            try owner.setFinalizer(self, destroy);
            builder.preparation.?.shared = null;
        }
        // All immutable lists have moved. The emptied builder no longer
        // borrows an allocator context that Definition error cleanup can retire.
        builder.graph = ml.graph.Graph.init(builder.allocator);
        return self;
    }
};
pub const DefinitionSlot = @import("specialization_cache.zig").PinnedSlot(*Definition, [32]u8, Definition.keyOf, Definition.equal, Definition.retain, Definition.pinned);
pub const Preparation = struct {
    key: [32]u8,
    cached: ?*Definition = null,
    shared: ?*@import("cold_metadata.zig").Shared = null,
    pub fn deinit(self: *Preparation, current: Cleanup) void {
        if (self.cached) |value| value.release(current);
        if (self.shared) |owner| owner.releaseWithCleanup(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.* = undefined;
    }
};

pub const Builder = struct {
    allocator: std.mem.Allocator,
    preparation: ?*Preparation = null,
    replay_cursor: usize = 0,
    cached_capacity_reserved: bool = false,
    graph: ml.graph.Graph,
    descriptors: std.ArrayList(?ops.kernel_descriptor.Descriptor) = .empty,
    values: std.ArrayList(?CT) = .empty,
    bindings: std.ArrayList(PendingBinding) = .empty,
    immutable_resident: std.ArrayList(PendingBinding) = .empty,
    steps: std.ArrayList(Node) = .empty,
    planes: std.ArrayList(Plane) = .empty,
    backend: ops.ComputeBackend,

    pub fn init(a: std.mem.Allocator, cb: *const ops.ComputeBackend) Builder {
        return initPrepared(a, cb, null);
    }
    pub fn initPrepared(a: std.mem.Allocator, cb: *const ops.ComputeBackend, preparation: ?*Preparation) Builder {
        const graph_allocator = if (preparation) |value| if (value.shared) |owner| owner.allocator() else a else a;
        return .{ .allocator = a, .preparation = preparation, .graph = if (preparation) |value| if (value.cached) |definition| definition.graph else .init(graph_allocator) else .init(graph_allocator), .backend = cb.withoutSynchronousBorrows() };
    }
    fn cached(self: *const Builder) ?*Definition {
        return if (self.preparation) |value| value.cached else null;
    }
    fn schema(self: *const Builder) BindingSchema {
        return .{ .values = self.values.items.len, .runtime_bindings = self.bindings.items.len, .immutable_bindings = self.immutable_resident.items.len, .planes = self.planes.items.len };
    }
    fn reserveCachedCapacity(self: *Builder, definition: *const Definition) !void {
        if (self.cached_capacity_reserved) return;
        const counts = definition.binding_schema;
        try self.values.ensureTotalCapacityPrecise(self.allocator, counts.values);
        try self.bindings.ensureTotalCapacityPrecise(self.allocator, counts.runtime_bindings);
        try self.immutable_resident.ensureTotalCapacityPrecise(self.allocator, counts.immutable_bindings);
        try self.planes.ensureTotalCapacityPrecise(self.allocator, counts.planes);
        self.cached_capacity_reserved = true;
    }
    fn replay(self: *Builder, shape: Shape, inputs: []const Node, descriptor: ?ops.kernel_descriptor.Descriptor) !?Node {
        const definition = self.cached() orelse return null;
        try self.reserveCachedCapacity(definition);
        if (self.replay_cursor >= definition.graph.nodeCount()) return error.DenseDefinitionChanged;
        const id: Node = @intCast(self.replay_cursor);
        const node = definition.graph.node(id);
        if (!node.output_shape.eq(shape) or !std.mem.eql(Node, node.getInputs(), inputs)) return error.DenseDefinitionChanged;
        const stored = definition.descriptors[id];
        if (descriptor) |expected| {
            const actual = stored orelse return error.DenseDefinitionChanged;
            if (actual.input_count != expected.input_count or !actual.output_shape.eq(expected.output_shape) or !std.meta.eql(actual.operation, expected.operation)) return error.DenseDefinitionChanged;
            for (actual.input_shapes[0..actual.input_count], expected.input_shapes[0..expected.input_count]) |a, b| if (!a.eq(b)) return error.DenseDefinitionChanged;
        } else if (stored != null or node.op != .parameter) return error.DenseDefinitionChanged;
        self.replay_cursor += 1;
        return id;
    }
    pub fn deinit(self: *Builder) void {
        const a = self.allocator;
        self.descriptors.deinit(self.graph.allocator);
        self.values.deinit(a);
        self.bindings.deinit(a);
        self.immutable_resident.deinit(a);
        self.steps.deinit(self.graph.allocator);
        self.planes.deinit(a);
        if (self.cached() == null) self.graph.deinit();
    }
    pub fn parameter(self: *Builder, shape: Shape, value: CT) !Node {
        return self.parameterWithPlane(shape, value, try residentPlane(&self.backend, value), false);
    }
    fn parameterWithPlane(self: *Builder, shape: Shape, value: CT, observed: Plane, comptime immutable: bool) !Node {
        const reused = try self.replay(shape, &.{}, null);
        const id = reused orelse fresh: {
            var b = ml.graph.Builder.init(&self.graph);
            var name: [40]u8 = undefined;
            break :fresh try b.parameter(try std.fmt.bufPrint(&name, "dense.input.{d}", .{self.graph.nodeCount()}), shape);
        };
        if (reused == null) try self.descriptors.append(self.graph.allocator, null);
        try self.values.append(self.allocator, value);
        const index = self.planes.items.len;
        try self.planes.append(self.allocator, observed);
        const binding = PendingBinding{ .node_id = id, .source = .{ .resident = .{ .logical = shape, .plane = index } } };
        if (immutable) try self.immutable_resident.append(self.allocator, binding) else try self.bindings.append(self.allocator, binding);
        return id;
    }
    /// An actual typed held-source proof, not a partial generic storage report.
    /// The architecture retains the immutable witness and Model/store owner;
    /// the existing F32 Direct route receives only this borrowed primary.
    pub fn residentWeight(self: *Builder, value: CT, plane: @import("storage_ownership.zig").Plane) !Node {
        return self.parameterWithPlane(plane.descriptor.shape, value, plane, true);
    }
    pub fn hostParameter(self: *Builder, shape: Shape) !Node {
        const reused = try self.replay(shape, &.{}, null);
        const id = reused orelse fresh: {
            var b = ml.graph.Builder.init(&self.graph);
            var name: [40]u8 = undefined;
            break :fresh try b.parameter(try std.fmt.bufPrint(&name, "dense.host.{d}", .{self.graph.nodeCount()}), shape);
        };
        if (reused == null) try self.descriptors.append(self.graph.allocator, null);
        try self.values.append(self.allocator, null);
        try self.bindings.append(self.allocator, .{ .node_id = id, .source = .{ .host = .{ .logical = shape, .target = .native, .owned_ingress = true, .borrowable_host = false } } });
        return id;
    }
    pub fn gather(self: *Builder, weight: Node, indices: Node, width: usize) !Node {
        const rows = self.logicalShape(indices).numElements() orelse return error.InvalidShape;
        const shape = Shape.init(.f32, &.{ rows, @intCast(width) });
        const id = if (self.cached()) |definition| reuse: {
            if (self.replay_cursor >= definition.graph.nodeCount()) return error.DenseDefinitionChanged;
            const id: Node = @intCast(self.replay_cursor);
            const descriptor = definition.descriptors[id] orelse return error.DenseDefinitionChanged;
            if (descriptor.operation != .gather) return error.DenseDefinitionChanged;
            break :reuse (try self.replay(shape, &.{ weight, indices }, descriptor)).?;
        } else fresh: {
            var b = ml.graph.Builder.init(&self.graph);
            const id = try b.gather(weight, indices, shape);
            try self.descriptors.append(self.graph.allocator, ops.kernel_descriptor.lower(&self.graph, id) orelse return error.UnsupportedDenseSchedule);
            break :fresh id;
        };
        try self.values.append(self.allocator, null);
        if (self.cached() == null) try self.steps.append(self.graph.allocator, id);
        return id;
    }
    pub fn operation(self: *Builder, op: ml.graph.OpCode, inputs: []const Node, descriptor: ops.kernel_descriptor.Descriptor) !Node {
        const reused = try self.replay(descriptor.output_shape, inputs, descriptor);
        var node = ml.graph.Node{ .op = op, .output_shape = descriptor.output_shape, .num_inputs = @intCast(inputs.len) };
        @memcpy(node.inputs[0..inputs.len], inputs);
        const id = reused orelse try self.graph.addNode(node);
        if (reused == null) try self.descriptors.append(self.graph.allocator, descriptor);
        try self.values.append(self.allocator, null);
        if (self.cached() == null) try self.steps.append(self.graph.allocator, id);
        return id;
    }
    pub fn output(self: *Builder, id: Node) !void {
        if (self.cached() != null) {
            for (self.graph.outputs.items) |root| if (root == id) return;
            return error.DenseDefinitionChanged;
        }
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
    pub fn attentionRows(self: *Builder, kind: ops.kernel_descriptor.DenseAttentionRows.Kind, q: Node, k: Node, v: Node, heads: usize, dim: usize, query_rows: usize, key_rows: usize) !Node {
        const geometry = ops.kernel_descriptor.DenseAttentionRows{ .kind = kind, .num_heads = std.math.cast(u32, heads) orelse return error.DimensionOverflow, .head_dim = std.math.cast(u32, dim) orelse return error.DimensionOverflow, .query_rows = std.math.cast(u32, query_rows) orelse return error.DimensionOverflow, .key_rows = std.math.cast(u32, key_rows) orelse return error.DimensionOverflow };
        const op: ml.graph.OpCode = .{ .fused_cross_attention = .{ .batch = 1, .dec_seq = geometry.query_rows, .enc_seq = geometry.key_rows, .num_heads = geometry.num_heads, .head_dim = geometry.head_dim } };
        return self.operation(op, &.{ q, k, v }, .{ .operation = .{ .dense_attention_rows = geometry }, .input_count = 3, .output_shape = self.logicalShape(q), .input_shapes = .{ self.logicalShape(q), self.logicalShape(k), self.logicalShape(v) } });
    }
    pub fn lastRow(self: *Builder, input: Node, rows: usize, width: usize) !Node {
        var slice = ops.kernel_descriptor.Slice{};
        slice.starts[0] = std.math.cast(u32, rows - 1) orelse return error.DimensionOverflow;
        var attrs = @TypeOf(@as(ml.graph.Node, undefined).op.slice){ .num_axes = 2 };
        attrs.starts[0] = @intCast(rows - 1);
        attrs.limits[0] = @intCast(rows);
        attrs.limits[1] = @intCast(width);
        return self.operation(.{ .slice = attrs }, &.{input}, .{ .operation = .{ .slice = slice }, .input_count = 1, .output_shape = Shape.init(.f32, &.{ 1, @intCast(width) }), .input_shapes = .{ self.logicalShape(input), undefined, undefined } });
    }
    pub fn gelu(self: *Builder, x: Node) !Node {
        return self.operation(.{ .fused_gelu = {} }, &.{x}, .{ .operation = .{ .unary = .gelu }, .output_shape = self.logicalShape(x), .input_shapes = .{ self.logicalShape(x), undefined, undefined }, .input_count = 1 });
    }
    pub fn geluExact(self: *Builder, x: Node) !Node {
        return self.operation(.{ .fused_gelu_exact = {} }, &.{x}, .{ .operation = .{ .unary = .gelu_exact }, .output_shape = self.logicalShape(x), .input_shapes = .{ self.logicalShape(x), undefined, undefined }, .input_count = 1 });
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
    definition: *Definition,
    graph: ml.graph.Graph,
    descriptors: []?ops.kernel_descriptor.Descriptor,
    values: []?CT,
    steps: []Node,
    selected: *@import("prepared_routes.zig").PreparedRoutes,
    frame: ?*physical.Frame = null,
    numerical_epoch: ?@import("retirement_sink.zig").Epoch = null,
    borrowed: std.AutoHashMapUnmanaged(CT, void) = .empty,
    cursor: usize = 0,
    analysis: @import("../graph/interpreter.zig").CachedAnalysis,

    /// Creates only metadata. All actual physical banks are funded separately
    /// before enter, without using the architecture's compatibility allowance.
    pub fn create(builder: *Builder) !*Schedule {
        const a = builder.allocator;
        const self = try a.create(Schedule);
        errdefer a.destroy(self);
        self.allocator = a;
        self.backend = builder.backend;
        const definition = if (builder.cached()) |value| block: {
            if (builder.replay_cursor != value.graph.nodeCount() or !std.meta.eql(builder.schema(), value.binding_schema)) return error.DenseDefinitionChanged;
            break :block value.retain();
        } else try Definition.create(builder);
        errdefer definition.release(.{ .outside_execution = true });
        self.definition = definition;
        self.graph = definition.graph;
        self.descriptors = definition.descriptors;
        self.steps = definition.steps;
        self.analysis = definition.analysis;
        self.values = try builder.values.toOwnedSlice(a);
        errdefer a.free(self.values);
        // Both receiving owners copy these actual indexed facts directly;
        // no intermediate InputBinding arrays or producer pointers survive.
        const implementation = route.Implementation{ .context = self.backend.ptr, .table = self.backend.vtable };
        const bindings = input_facts.InputSource{ .indexed = .{ .entries = builder.bindings.items, .planes = builder.planes.items, .implementation = implementation } };
        const immutable = input_facts.InputSource{ .indexed = .{ .entries = builder.immutable_resident.items, .planes = builder.planes.items, .implementation = implementation } };
        var private_pool: ?*physical.Pool = (definition.layout.createPool(a, &self.graph, &.{&self.backend}, self.descriptors) catch |err| switch (err) {
            error.PhysicalOutputLayoutChanged => fallback: {
                var fresh = try @import("../graph/runtime.zig").PhysicalLayout.create(a, &self.graph, &.{&self.backend}, .{}, false);
                defer fresh.deinit();
                break :fallback try fresh.createPool(a, &self.graph, &.{&self.backend}, self.descriptors);
            },
            else => return err,
        }) orelse return error.UnsupportedDenseSchedule;
        errdefer if (private_pool) |pool| pool.deinit();
        private_pool.?.coverage_graph = &self.graph;
        private_pool.?.coverage_reachable = self.analysis.reachable;
        private_pool.?.coverage_backend = self.backend;
        private_pool.?.native_frame_dispatch = true;
        self.selected = (try physical.Pool.preparePrivateNativeSourcesConsuming(&private_pool, immutable, bindings)) orelse return error.UnsupportedDenseSchedule;
        errdefer self.selected.release();
        if (self.selected.pool.payload_coverage != .complete or self.selected.pool.regions.len != 0) return error.UnsupportedDenseSchedule;
        // A ready authored plan owns its complete cold resource recipe before
        // admission, metadata sealing, or the first numerical Model scope.
        _ = try self.selected.storageRequirement();
        self.frame = null;
        self.numerical_epoch = null;
        self.borrowed = .empty;
        self.cursor = 0;
        return self;
    }
    pub fn setHostWorkspaceAllocator(self: *Schedule, backing: std.mem.Allocator) !void {
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
        if (self.numerical_epoch) |*epoch| epoch.release();
        self.numerical_epoch = null;
        self.borrowed.deinit(self.allocator);
        self.allocator.free(self.values);
        const definition = self.definition;
        self.allocator.destroy(self);
        definition.release(current);
    }
};

fn packedColdBindingCase(a: std.mem.Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const first_input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
    defer cb.free(first_input);
    const second_input = try cb.fromFloat32Shape(&.{ -1, -2, -3, -4 }, &.{ 1, 4 });
    defer cb.free(second_input);
    const Author = struct {
        fn create(allocator: std.mem.Allocator, backend: *const ops.ComputeBackend, value: CT, preparation: ?*Preparation) !*Schedule {
            var builder = Builder.initPrepared(allocator, backend, preparation);
            defer builder.deinit();
            const shape = Shape.init(.f32, &.{ 1, 4 });
            var sum = try builder.parameter(shape, value);
            // Every distinct parameter participates; this crosses cold list
            // growth boundaries without granting authority to dead metadata.
            for (0..16) |_| sum = try builder.add(sum, try builder.parameter(shape, value));
            try builder.output(sum);
            return Schedule.create(&builder);
        }
    };
    const first = try Author.create(a, &cb, first_input, null);
    var first_live = true;
    defer if (first_live) first.deinit(.{ .outside_execution = true });
    var preparation = Preparation{ .key = @splat(0), .cached = first.definition.retain() };
    defer preparation.deinit(.{ .outside_execution = true });
    const second = try Author.create(a, &cb, second_input, &preparation);
    defer second.deinit(.{ .outside_execution = true });
    try std.testing.expect(first.definition == second.definition);
    const first_inputs = first.selected.pool.selection.?.inputs.entries;
    const second_inputs = second.selected.pool.selection.?.inputs.entries;
    try std.testing.expect(first_inputs.ptr != second_inputs.ptr);
    const first_plane = try residentPlane(&cb, first_input);
    const second_plane = try residentPlane(&cb, second_input);
    try std.testing.expect(!std.meta.eql(first_plane.backing, second_plane.backing));
    for (first_inputs, second_inputs) |first_binding, second_binding| {
        const one = first_binding.value.resident.planes;
        const two = second_binding.value.resident.planes;
        try std.testing.expect(one.ptr != two.ptr);
        try std.testing.expectEqual(@as(usize, 1), one.len);
        try std.testing.expectEqual(@as(usize, 1), two.len);
        try std.testing.expectEqual(first_plane.backing, one[0].backing);
        try std.testing.expectEqual(second_plane.backing, two[0].backing);
    }
    first.deinit(.{ .outside_execution = true });
    first_live = false;
    // The cached Definition contains counts and semantic metadata only. The
    // surviving request still validates its independently owned actual planes.
    try second.selected.validate(second_inputs);
    for (second_inputs) |binding| try std.testing.expectEqual(second_plane.backing, binding.value.resident.planes[0].backing);
}

test "dense decoder packed cold bindings keep request planes independent across Definition reuse" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try packedColdBindingCase(std.testing.allocator);
}

test "dense decoder packed cold binding publication unwinds every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, packedColdBindingCase, .{});
}
