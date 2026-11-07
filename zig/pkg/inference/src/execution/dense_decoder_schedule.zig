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
pub const RoleKey = struct {
    kind: enum { embedding, positions, token_ids, position_ids, self_q, self_k, self_v, self_o, self_norm, cross_q, cross_k, cross_v, cross_o, cross_norm, ffn_norm, fc1, fc2, final_norm, lm_head, self_slab_k, self_slab_v, cross_slab_k, cross_slab_v, embed_norm },
    layer: ?u32 = null,
    part: enum { value, weight, bias } = .value,
};
pub const ParameterSpec = struct {
    role: RoleKey,
    shape: Shape,
    source_kind: enum { host, held_weight, external },
    source_name: []const u8 = "",
};
pub const BoundParameter = struct {
    role: RoleKey,
    value: ?CT = null,
    source_data: ?*const anyopaque = null,
};
pub const ProgramSpec = struct {
    family: enum { whisper, florence },
    phase: enum { decode, initial },
    geometry: struct { layers: usize, width: usize, heads: usize, ffn_dim: usize, vocab_size: usize, encoder_rows: usize, capacity: usize, query_rows: usize },
    semantics: struct { epsilon: f32 = 1e-5, activation: enum { gelu, gelu_exact }, norm_order: enum { pre, post }, position_offset: usize, global_final_norm: bool },
    parameters: []const ParameterSpec,

    pub fn hasRole(self: *const ProgramSpec, role: RoleKey) bool {
        for (self.parameters) |parameter| if (std.meta.eql(parameter.role, role)) return true;
        return false;
    }
    fn validate(self: *const ProgramSpec) !void {
        const g = self.geometry;
        if (g.width == 0 or g.heads == 0 or g.width % g.heads != 0 or (g.layers != 0 and g.ffn_dim == 0) or g.vocab_size == 0 or g.encoder_rows == 0 or g.capacity == 0 or g.query_rows == 0 or g.query_rows > g.capacity or (self.phase == .decode and g.query_rows != 1) or !std.math.isFinite(self.semantics.epsilon) or self.semantics.epsilon <= 0) return error.DenseDefinitionChanged;
        for (self.parameters) |parameter| try self.validateParameter(parameter);
    }
    fn validateParameter(self: *const ProgramSpec, parameter: ParameterSpec) !void {
        if (parameter.shape.rank() > parameter.shape.dims.len or (parameter.shape.numElements() orelse return error.DenseDefinitionChanged) <= 0) return error.DenseDefinitionChanged;
        if (parameter.role.layer) |layer| if (layer >= self.geometry.layers) return error.DenseDefinitionChanged;
        if ((parameter.source_kind == .held_weight) != (parameter.source_name.len != 0)) return error.DenseDefinitionChanged;
    }
};

const SliceSpecSource = struct {
    entries: []const ParameterSpec,
    pub fn len(self: @This()) usize {
        return self.entries.len;
    }
    pub fn at(self: @This(), index: usize, _: *[256]u8) !ParameterSpec {
        return self.entries[index];
    }
};
fn hashSpecHeader(hash: *std.crypto.hash.sha2.Sha256, spec: *const ProgramSpec, count: usize) void {
    hash.update("dense.semantic.binding.recipe.v1");
    const header = [_]u64{ @backingInt(spec.family), @backingInt(spec.phase), spec.geometry.layers, spec.geometry.width, spec.geometry.heads, spec.geometry.ffn_dim, spec.geometry.vocab_size, spec.geometry.encoder_rows, spec.geometry.capacity, spec.geometry.query_rows, @as(u32, @bitCast(spec.semantics.epsilon)), @backingInt(spec.semantics.activation), @backingInt(spec.semantics.norm_order), spec.semantics.position_offset, @intFromBool(spec.semantics.global_final_norm), count };
    hash.update(std.mem.sliceAsBytes(&header));
}
fn hashParameter(hash: *std.crypto.hash.sha2.Sha256, parameter: ParameterSpec) void {
    const fields = [_]u64{ @backingInt(parameter.role.kind), if (parameter.role.layer) |layer| layer else std.math.maxInt(u64), @backingInt(parameter.role.part), @backingInt(parameter.source_kind), @backingInt(parameter.shape.dtype), parameter.shape.rank(), parameter.source_name.len };
    hash.update(std.mem.sliceAsBytes(&fields));
    hash.update(std.mem.sliceAsBytes(parameter.shape.dims[0..parameter.shape.rank()]));
    hash.update(parameter.source_name);
}
/// Digest is lookup only. Full source equality precedes retained cache floors.
/// Source methods are compile-time bound and never retained by any owner.
pub fn specKeySource(header: *const ProgramSpec, source: anytype) ![32]u8 {
    var pure = header.*;
    pure.parameters = &.{};
    try pure.validate();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hashSpecHeader(&hash, &pure, source.len());
    var scratch: [256]u8 = undefined;
    for (0..source.len()) |index| {
        const parameter = try source.at(index, &scratch);
        try pure.validateParameter(parameter);
        hashParameter(&hash, parameter);
    }
    return hash.finalResult();
}
pub fn specKey(spec: *const ProgramSpec) ![32]u8 {
    return specKeySource(spec, SliceSpecSource{ .entries = spec.parameters });
}
fn equalHeader(a: *const ProgramSpec, b: *const ProgramSpec) bool {
    return a.family == b.family and a.phase == b.phase and std.meta.eql(a.geometry, b.geometry) and std.meta.eql(a.semantics, b.semantics);
}
fn equalParameter(a: ParameterSpec, b: ParameterSpec) bool {
    return std.meta.eql(a.role, b.role) and a.shape.eq(b.shape) and a.source_kind == b.source_kind and std.mem.eql(u8, a.source_name, b.source_name);
}
fn equalSpec(a: *const ProgramSpec, b: *const ProgramSpec) bool {
    if (!equalHeader(a, b) or a.parameters.len != b.parameters.len) return false;
    for (a.parameters, b.parameters) |one, two| if (!equalParameter(one, two)) return false;
    return true;
}
const OwnedSpec = struct {
    value: ProgramSpec,
    names: []u8,
    fn create(a: std.mem.Allocator, source: *const ProgramSpec) !OwnedSpec {
        return fromSource(a, source, SliceSpecSource{ .entries = source.parameters });
    }
    fn fromSource(a: std.mem.Allocator, header: *const ProgramSpec, source: anytype) !OwnedSpec {
        var value = header.*;
        value.parameters = &.{};
        try value.validate();
        var scratch: [256]u8 = undefined;
        var bytes: usize = 0;
        for (0..source.len()) |index| {
            const parameter = try source.at(index, &scratch);
            try value.validateParameter(parameter);
            bytes = try std.math.add(usize, bytes, parameter.source_name.len);
        }
        const names = try a.alloc(u8, bytes);
        errdefer a.free(names);
        const parameters = try a.alloc(ParameterSpec, source.len());
        errdefer a.free(parameters);
        var offset: usize = 0;
        for (parameters, 0..) |*copy, index| {
            const parameter = try source.at(index, &scratch);
            try value.validateParameter(parameter);
            if (parameter.source_name.len > names.len - offset) return error.DenseDefinitionChanged;
            copy.* = parameter;
            copy.source_name = names[offset..][0..parameter.source_name.len];
            @memcpy(names[offset..][0..parameter.source_name.len], parameter.source_name);
            offset += parameter.source_name.len;
        }
        if (offset != names.len) return error.DenseDefinitionChanged;
        value.parameters = parameters;
        return .{ .value = value, .names = names };
    }
    fn deinit(self: *OwnedSpec, a: std.mem.Allocator) void {
        a.free(self.value.parameters);
        a.free(self.names);
    }
};
/// Constructor-owned pure metadata. Exact allocator-matched misses may move it
/// into Definition; the caller's later deinit is safe after that transfer.
pub const ProgramSpecStorage = struct {
    allocator: std.mem.Allocator,
    value: ProgramSpec,
    names: []u8,
    pub fn fromSource(a: std.mem.Allocator, header: *const ProgramSpec, source: anytype) !ProgramSpecStorage {
        const owned = try OwnedSpec.fromSource(a, header, source);
        return .{ .allocator = a, .value = owned.value, .names = owned.names };
    }
    pub fn deinit(self: *ProgramSpecStorage) void {
        var owned = self.take();
        owned.deinit(self.allocator);
    }
    fn take(self: *ProgramSpecStorage) OwnedSpec {
        const result = OwnedSpec{ .value = self.value, .names = self.names };
        self.value.parameters = &.{};
        self.names = &.{};
        return result;
    }
};
const ParameterRecipe = struct { parameter: usize, node: Node, plane: ?usize };
fn checkBound(spec: ParameterSpec, bound: BoundParameter) !void {
    if (!std.meta.eql(spec.role, bound.role)) return error.DenseDefinitionChanged;
    switch (spec.source_kind) {
        .host => if (bound.value != null or bound.source_data != null) return error.DenseDefinitionChanged,
        .held_weight => if (bound.value == null or bound.source_data == null) return error.DenseDefinitionChanged,
        .external => if (bound.value == null or bound.source_data != null) return error.DenseDefinitionChanged,
    }
}
fn currentPlane(cb: *const ops.ComputeBackend, spec: ParameterSpec, bound: BoundParameter) !Plane {
    try checkBound(spec, bound);
    const value = bound.value orelse return error.DenseDefinitionChanged;
    const actual = switch (spec.source_kind) {
        .held_weight => try @import("../ops/native_compute.zig").NativeCompute.heldF32WeightPrimaryNamed(cb, bound.source_data.?, value, spec.source_name),
        .external => try residentPlane(cb, value),
        .host => return error.DenseDefinitionChanged,
    };
    if (!actual.descriptor.shape.eq(spec.shape)) return error.DenseDefinitionChanged;
    return actual;
}

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
    physical_layout: @import("../graph/runtime.zig").FrozenPhysicalLayout,
    program_spec: ?OwnedSpec = null,
    parameter_recipe: []ParameterRecipe = &.{},
    runtime_recipe: []PendingBinding = &.{},
    immutable_recipe: []PendingBinding = &.{},
    binding_schema: BindingSchema,

    pub fn programSpec(self: *const Definition) !*const ProgramSpec {
        if (self.program_spec) |*owned| return &owned.value;
        return error.DenseDefinitionChanged;
    }
    pub fn matchesSpecSource(self: *const Definition, header: *const ProgramSpec, source: anytype) !bool {
        const stored = self.programSpec() catch return false;
        var pure = header.*;
        pure.parameters = &.{};
        try pure.validate();
        if (!equalHeader(stored, &pure) or stored.parameters.len != source.len()) return false;
        var scratch: [256]u8 = undefined;
        for (stored.parameters, 0..) |expected, index| {
            const actual = try source.at(index, &scratch);
            try pure.validateParameter(actual);
            if (!equalParameter(expected, actual)) return false;
        }
        return true;
    }
    pub fn matchesSpec(self: *const Definition, spec: *const ProgramSpec) bool {
        spec.validate() catch return false;
        const stored = self.program_spec orelse return false;
        return equalSpec(&stored.value, spec);
    }
    pub fn parameterNode(self: *const Definition, role: RoleKey) !Node {
        const spec = &(self.program_spec orelse return error.DenseDefinitionChanged).value;
        var result: ?Node = null;
        for (self.parameter_recipe) |entry| if (std.meta.eql(spec.parameters[entry.parameter].role, role)) {
            if (result != null) return error.DenseDefinitionChanged;
            result = entry.node;
        };
        return result orelse error.DenseDefinitionChanged;
    }
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
        self.allocator.free(self.parameter_recipe);
        self.allocator.free(self.runtime_recipe);
        self.allocator.free(self.immutable_recipe);
        if (self.program_spec) |*spec| spec.deinit(self.allocator);
        self.analysis.deinit(self.allocator);
        self.allocator.free(self.steps);
        self.allocator.free(self.descriptors);
        self.physical_layout.deinit();
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
        const transferable = if (builder.spec_storage) |storage| (if (storage.allocator.ptr == a.ptr and storage.allocator.vtable == a.vtable) storage else null) else null;
        var owned_spec: ?OwnedSpec = if (builder.program_spec) |spec| (if (transferable == null) try OwnedSpec.create(a, spec) else null) else null;
        errdefer if (owned_spec) |*spec| spec.deinit(a);
        const key: [32]u8 = if (builder.program_spec) |spec| try specKey(spec) else if (builder.preparation) |preparation| preparation.key else @splat(0);
        if (builder.program_spec != null) {
            try builder.validateRecipe();
            if (builder.preparation) |preparation| if (!std.mem.eql(u8, &key, &preparation.key)) return error.DenseDefinitionChanged;
        }
        const recipe = try builder.parameter_recipe.toOwnedSlice(a);
        errdefer a.free(recipe);
        const runtime_recipe = if (builder.program_spec != null) try a.dupe(PendingBinding, builder.bindings.items) else try a.alloc(PendingBinding, 0);
        errdefer a.free(runtime_recipe);
        const immutable_recipe = if (builder.program_spec != null) try a.dupe(PendingBinding, builder.immutable_resident.items) else try a.alloc(PendingBinding, 0);
        errdefer a.free(immutable_recipe);
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
        var frozen = try @import("../graph/runtime.zig").FrozenPhysicalLayout.initConsuming(&graph, &layout, &.{&builder.backend});
        errdefer frozen.deinit();
        self.* = .{ .allocator = a, .key = key, .shared = if (builder.preparation) |preparation| preparation.shared else null, .graph = frozen.graph().*, .descriptors = descriptors, .steps = steps, .analysis = analysis, .physical_layout = frozen, .binding_schema = builder.schema(), .program_spec = owned_spec, .parameter_recipe = recipe, .runtime_recipe = runtime_recipe, .immutable_recipe = immutable_recipe };
        // No fallible work follows transfer of the finalizer. Publication is
        // sealed separately by the caller after successful private preparation.
        if (self.shared) |owner| {
            try owner.setFinalizer(self, destroy);
            builder.preparation.?.shared = null;
        }
        if (transferable) |storage| self.program_spec = storage.take();
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
    program_spec: ?*const ProgramSpec = null,
    spec_storage: ?*ProgramSpecStorage = null,
    bound_parameters: []const BoundParameter = &.{},
    parameter_recipe: std.ArrayList(ParameterRecipe) = .empty,
    role_indices: std.AutoHashMapUnmanaged(RoleKey, struct { index: usize, used: bool = false }) = .empty,
    spec_validated: bool = false,
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
    pub fn initSpec(a: std.mem.Allocator, cb: *const ops.ComputeBackend, preparation: ?*Preparation, spec: *const ProgramSpec, bound: []const BoundParameter) Builder {
        var self = initPrepared(a, cb, preparation);
        self.program_spec = spec;
        self.bound_parameters = bound;
        return self;
    }
    pub fn initSpecStorage(a: std.mem.Allocator, cb: *const ops.ComputeBackend, preparation: ?*Preparation, storage: *ProgramSpecStorage, bound: []const BoundParameter) Builder {
        var self = initSpec(a, cb, preparation, &storage.value, bound);
        self.spec_storage = storage;
        return self;
    }
    fn prepareSpec(self: *Builder) !void {
        if (self.backend.vtable != &@import("../ops/native_compute.zig").vtable_impl) return error.UnsupportedDenseSchedule;
        if (self.spec_validated) return;
        const spec = self.program_spec orelse return error.DenseDefinitionChanged;
        if (self.cached() != null or self.bound_parameters.len != spec.parameters.len) return error.DenseDefinitionChanged;
        try spec.validate();
        try self.role_indices.ensureTotalCapacity(self.allocator, std.math.cast(u32, spec.parameters.len) orelse return error.DenseDefinitionChanged);
        for (spec.parameters, self.bound_parameters, 0..) |declared, bound, index| {
            try checkBound(declared, bound);
            const entry = self.role_indices.getOrPutAssumeCapacity(declared.role);
            if (entry.found_existing) return error.DenseDefinitionChanged;
            entry.value_ptr.* = .{ .index = index };
        }
        self.spec_validated = true;
    }
    pub fn parameterRole(self: *Builder, role: RoleKey) !Node {
        try self.prepareSpec();
        const spec = self.program_spec.?;
        const role_entry = self.role_indices.getPtr(role) orelse return error.DenseDefinitionChanged;
        const index = role_entry.index;
        const declared = spec.parameters[index];
        const bound = self.bound_parameters[index];
        const plane: ?usize = if (declared.source_kind == .host) null else self.planes.items.len;
        const node = switch (declared.source_kind) {
            .host => try self.hostParameter(declared.shape),
            .held_weight => try self.parameterWithPlane(declared.shape, bound.value.?, try currentPlane(&self.backend, declared, bound), true),
            .external => try self.parameterWithPlane(declared.shape, bound.value.?, try currentPlane(&self.backend, declared, bound), false),
        };
        try self.parameter_recipe.append(self.graph.allocator, .{ .parameter = index, .node = node, .plane = plane });
        return node;
    }
    fn validateRecipe(self: *Builder) !void {
        if (self.program_spec == null) return;
        try self.prepareSpec();
        const spec = self.program_spec.?;
        if (self.values.items.len != self.graph.nodeCount()) return error.DenseDefinitionChanged;
        var roles = self.role_indices.valueIterator();
        while (roles.next()) |entry| entry.used = false;
        var used_roles: usize = 0;
        var recipe_index: usize = 0;
        var runtime_index: usize = 0;
        var immutable_index: usize = 0;
        var plane_index: usize = 0;
        // All three lists are appended in graph-node creation order. Walking
        // their exact ordered correspondence proves uniqueness and complete
        // coverage without a second set or a scan per parameter.
        for (self.graph.nodes.items, 0..) |node, node_index| {
            if (node.op != .parameter) continue;
            if (recipe_index == self.parameter_recipe.items.len) return error.DenseDefinitionChanged;
            const entry = self.parameter_recipe.items[recipe_index];
            recipe_index += 1;
            if (entry.node != node_index or entry.parameter >= spec.parameters.len) return error.DenseDefinitionChanged;
            const declared = spec.parameters[entry.parameter];
            const role_entry = self.role_indices.getPtr(declared.role) orelse return error.DenseDefinitionChanged;
            if (role_entry.index != entry.parameter) return error.DenseDefinitionChanged;
            if (!role_entry.used) {
                role_entry.used = true;
                used_roles += 1;
            }
            if (!node.output_shape.eq(declared.shape)) return error.DenseDefinitionChanged;
            const pending = if (declared.source_kind == .held_weight) self.immutable_resident.items else self.bindings.items;
            const cursor = if (declared.source_kind == .held_weight) &immutable_index else &runtime_index;
            if (cursor.* == pending.len) return error.DenseDefinitionChanged;
            const binding = pending[cursor.*];
            cursor.* += 1;
            if (binding.node_id != entry.node) return error.DenseDefinitionChanged;
            switch (binding.source) {
                .host => if (declared.source_kind != .host or entry.plane != null) return error.DenseDefinitionChanged,
                .resident => |source| {
                    if (declared.source_kind == .host or entry.plane != source.plane or source.plane != plane_index or plane_index >= self.planes.items.len) return error.DenseDefinitionChanged;
                    plane_index += 1;
                },
            }
        }
        if (used_roles != spec.parameters.len or recipe_index != self.parameter_recipe.items.len or runtime_index != self.bindings.items.len or immutable_index != self.immutable_resident.items.len or plane_index != self.planes.items.len) return error.DenseDefinitionChanged;
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
        self.role_indices.deinit(a);
        self.parameter_recipe.deinit(self.graph.allocator);
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
        const definition = if (builder.cached()) |value| block: {
            if (builder.program_spec != null or builder.replay_cursor != value.graph.nodeCount() or !std.meta.eql(builder.schema(), value.binding_schema)) return error.DenseDefinitionChanged;
            break :block value.retain();
        } else try Definition.create(builder);
        defer definition.release(.{ .outside_execution = true });
        const values = try builder.values.toOwnedSlice(a);
        errdefer a.free(values);
        const implementation = route.Implementation{ .context = builder.backend.ptr, .table = builder.backend.vtable };
        const bindings = input_facts.InputSource{ .indexed = .{ .entries = builder.bindings.items, .planes = builder.planes.items, .implementation = implementation } };
        const immutable = input_facts.InputSource{ .indexed = .{ .entries = builder.immutable_resident.items, .planes = builder.planes.items, .implementation = implementation } };
        return createPrepared(a, builder.backend, definition, values, immutable, bindings);
    }
    /// Values and planes belong to this request. Only the frozen pure recipe
    /// enters through the retained Definition; no graph operation is reauthored.
    pub fn instantiate(a: std.mem.Allocator, cb: *const ops.ComputeBackend, definition: *Definition, spec: *const ProgramSpec, bound: []const BoundParameter) !*Schedule {
        if (cb.vtable != &@import("../ops/native_compute.zig").vtable_impl) return error.UnsupportedDenseSchedule;
        if (!definition.matchesSpec(spec) or bound.len != spec.parameters.len) return error.DenseDefinitionChanged;
        for (spec.parameters, bound) |parameter, current| try checkBound(parameter, current);
        const values = try a.alloc(?CT, definition.graph.nodeCount());
        errdefer a.free(values);
        @memset(values, null);
        const planes = try a.alloc(Plane, definition.binding_schema.planes);
        defer a.free(planes);
        for (definition.parameter_recipe) |entry| {
            const parameter = spec.parameters[entry.parameter];
            const current = bound[entry.parameter];
            values[entry.node] = current.value;
            if (entry.plane) |index| planes[index] = try currentPlane(cb, parameter, current);
        }
        const implementation = route.Implementation{ .context = cb.ptr, .table = cb.vtable };
        const immutable = input_facts.InputSource{ .indexed = .{ .entries = definition.immutable_recipe, .planes = planes, .implementation = implementation } };
        const runtime = input_facts.InputSource{ .indexed = .{ .entries = definition.runtime_recipe, .planes = planes, .implementation = implementation } };
        return createPrepared(a, cb.*, definition, values, immutable, runtime);
    }
    // On error the caller still owns values. Success transfers it exactly once.
    fn createPrepared(a: std.mem.Allocator, backend: ops.ComputeBackend, definition: *Definition, values: []?CT, immutable: input_facts.InputSource, bindings: input_facts.InputSource) !*Schedule {
        const self = try a.create(Schedule);
        errdefer a.destroy(self);
        self.allocator = a;
        self.backend = backend.withoutSynchronousBorrows();
        self.definition = definition.retain();
        errdefer self.definition.release(.{ .outside_execution = true });
        self.graph = definition.graph;
        self.descriptors = definition.descriptors;
        self.steps = definition.steps;
        self.analysis = definition.analysis;
        self.values = values;
        var private_pool: ?*physical.Pool = (definition.physical_layout.createPool(a, &.{&self.backend}, self.descriptors) catch |err| switch (err) {
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

fn semanticRecipeCase(a: std.mem.Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    const sink = try @import("retirement_sink.zig").Ref.create();
    defer {
        sink.close() catch @panic("sink close failed");
        var held = sink;
        held.release();
    }
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    const cleanup = Cleanup{ .boundary = &epoch, .outside_execution = true };
    defer {
        guard.finish();
        _ = @import("resource_retirement.zig").poll();
    }
    const data = [_]f32{ 1, 2, 3, 4 };
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer {
        native.deinitPrefetchQueue(&weights);
        weights.resident_weights.deinit(a);
        weights.lazy_weights.deinit(a);
    }
    // Distinct checkpoint roles deliberately share physical bytes. A pointer
    // match cannot substitute for the named held-source proof.
    for ([_][]const u8{ "query", "key" }) |name| try weights.resident_weights.put(a, name, .{ .tensor = .{
        .data = @constCast(std.mem.sliceAsBytes(&data)),
        .dtype = .f32,
        .shape = &.{ 1, 4 },
        .name = name,
        .allocator = a,
        .owns_data = false,
        .owns_shape = false,
    } });
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const query = try cb.getWeight("query");
    defer cb.free(query);
    const key = try cb.getWeight("key");
    defer cb.free(key);
    try native.NativeCompute.stampHeldF32WeightSources(&cb, &weights, &.{ query, key });
    const first_state = try cb.fromFloat32Shape(&data, &.{ 1, 4 });
    defer cb.free(first_state);
    const second_state = try cb.fromFloat32Shape(&.{ -1, -2, -3, -4 }, &.{ 1, 4 });
    defer cb.free(second_state);
    const shape = Shape.init(.f32, &.{ 1, 4 });
    const q_role = RoleKey{ .kind = .self_q, .layer = 0, .part = .weight };
    const k_role = RoleKey{ .kind = .self_k, .layer = 0, .part = .weight };
    const state_role = RoleKey{ .kind = .self_slab_k, .layer = 0 };
    var query_name = [_]u8{ 'q', 'u', 'e', 'r', 'y' };
    const parameters = [_]ParameterSpec{
        .{ .role = q_role, .shape = shape, .source_kind = .held_weight, .source_name = &query_name },
        .{ .role = k_role, .shape = shape, .source_kind = .held_weight, .source_name = "key" },
        .{ .role = state_role, .shape = shape, .source_kind = .external },
    };
    const spec = ProgramSpec{
        .family = .whisper,
        .phase = .decode,
        .geometry = .{ .layers = 1, .width = 4, .heads = 1, .ffn_dim = 4, .vocab_size = 4, .encoder_rows = 1, .capacity = 4, .query_rows = 1 },
        .semantics = .{ .activation = .gelu, .norm_order = .pre, .position_offset = 0, .global_final_norm = true },
        .parameters = &parameters,
    };
    var bound = [_]BoundParameter{
        .{ .role = q_role, .value = query, .source_data = &weights },
        .{ .role = k_role, .value = key, .source_data = &weights },
        .{ .role = state_role, .value = first_state },
    };
    // Independent original Builder authoring is the structural and numerical
    // reference; it does not read ProgramSpec or its cached recipe.
    var original_builder = Builder.init(a, &cb);
    defer original_builder.deinit();
    const original_q = try original_builder.residentWeight(query, try native.NativeCompute.heldF32WeightPrimary(&cb, &weights, query));
    const original_k = try original_builder.residentWeight(key, try native.NativeCompute.heldF32WeightPrimary(&cb, &weights, key));
    const original_state = try original_builder.parameter(shape, first_state);
    const original_sum = try original_builder.add(original_q, original_k);
    try original_builder.output(try original_builder.add(original_sum, original_state));
    const original = try Schedule.create(&original_builder);
    defer original.deinit(cleanup);
    var storage = try ProgramSpecStorage.fromSource(a, &spec, SliceSpecSource{ .entries = &parameters });
    defer storage.deinit();
    var builder = Builder.initSpecStorage(a, &cb, null, &storage, &bound);
    defer builder.deinit();
    const q_node = try builder.parameterRole(q_role);
    const k_node = try builder.parameterRole(k_role);
    const state_node = try builder.parameterRole(state_role);
    try builder.output(try builder.add(try builder.add(q_node, k_node), state_node));
    const first = try Schedule.create(&builder);
    var first_open = true;
    defer if (first_open) first.deinit(cleanup);
    try std.testing.expectEqual(@as(usize, 0), storage.value.parameters.len);
    try std.testing.expectEqual(@as(usize, 0), storage.names.len);
    const definition = first.definition.retain();
    defer definition.release(cleanup);
    try std.testing.expectEqual(original.graph.nodeCount(), first.graph.nodeCount());
    for (original.graph.nodes.items, first.graph.nodes.items) |one, two| {
        try std.testing.expectEqual(std.meta.activeTag(one.op), std.meta.activeTag(two.op));
        try std.testing.expect(one.output_shape.eq(two.output_shape));
        try std.testing.expectEqualSlices(Node, one.getInputs(), two.getInputs());
    }
    try std.testing.expectEqualSlices(Node, original.steps, first.steps);
    try std.testing.expectEqual(q_node, try definition.parameterNode(q_role));
    const owned = try definition.programSpec();
    try std.testing.expect(owned.hasRole(q_role));
    try std.testing.expect(try definition.matchesSpecSource(&spec, SliceSpecSource{ .entries = &parameters }));
    // Names and exact semantics are owned independently of constructor storage.
    query_name[0] = 'X';
    try std.testing.expectEqualStrings("query", owned.parameters[0].source_name);
    try std.testing.expect(!try definition.matchesSpecSource(&spec, SliceSpecSource{ .entries = &parameters }));
    query_name[0] = 'q';
    var changed = spec;
    changed.geometry.capacity += 1;
    try std.testing.expect(!definition.matchesSpec(&changed));
    changed = spec;
    changed.semantics.position_offset += 1;
    try std.testing.expect(!definition.matchesSpec(&changed));
    var changed_parameters = parameters;
    changed = spec;
    changed.parameters = &changed_parameters;
    changed_parameters[0].role = k_role;
    try std.testing.expect(!definition.matchesSpec(&changed));
    changed_parameters = parameters;
    changed_parameters[0].shape = Shape.init(.f32, &.{ 2, 2 });
    try std.testing.expect(!definition.matchesSpec(&changed));
    changed_parameters = parameters;
    changed_parameters[0].source_kind = .external;
    changed_parameters[0].source_name = "";
    try std.testing.expect(!definition.matchesSpec(&changed));
    changed = spec;
    changed.semantics.activation = .gelu_exact;
    try std.testing.expect(!definition.matchesSpec(&changed));
    changed = spec;
    changed.phase = .initial;
    try std.testing.expect(!definition.matchesSpec(&changed));
    // A pure Spec cannot legitimize an omitted role or an extra raw parameter.
    // These constructor failures must propagate real OOM during the sweep.
    {
        var omitted = Builder.initSpec(a, &cb, null, &spec, &bound);
        defer omitted.deinit();
        const q = try omitted.parameterRole(q_role);
        const k = try omitted.parameterRole(k_role);
        try omitted.output(try omitted.add(q, k));
        if (Schedule.create(&omitted)) |unexpected| {
            unexpected.deinit(cleanup);
            return error.ExpectedIncompleteRecipeDenial;
        } else |err| switch (err) {
            error.DenseDefinitionChanged => {},
            else => return err,
        }
    }
    {
        var extra = Builder.initSpec(a, &cb, null, &spec, &bound);
        defer extra.deinit();
        const q = try extra.parameterRole(q_role);
        const k = try extra.parameterRole(k_role);
        const state = try extra.parameterRole(state_role);
        const raw = try extra.parameter(shape, first_state);
        try extra.output(try extra.add(try extra.add(try extra.add(q, k), state), raw));
        if (Schedule.create(&extra)) |unexpected| {
            unexpected.deinit(cleanup);
            return error.ExpectedUnrepresentedParameterDenial;
        } else |err| switch (err) {
            error.DenseDefinitionChanged => {},
            else => return err,
        }
    }
    // Deliberately hold a digest candidate despite different complete semantics.
    // The mismatch is rejected before it can contribute a cached floor.
    var slot = DefinitionSlot{};
    defer if (slot.take()) |entry| entry.release(cleanup);
    try std.testing.expect(slot.publish(definition) == null);
    const candidate = slot.acquireUncounted(definition.key).?;
    const accepted = candidate.matchesSpec(&changed);
    slot.recordLookup(accepted);
    candidate.release(cleanup);
    try std.testing.expect(!accepted);
    try std.testing.expectEqual(@as(u64, 0), slot.trySnapshot().?.hits);
    try std.testing.expectEqual(@as(u64, 1), slot.trySnapshot().?.misses);
    bound[2].value = second_state;
    const second = try Schedule.instantiate(a, &cb, definition, owned, &bound);
    var second_open = true;
    defer if (second_open) second.deinit(cleanup);
    try std.testing.expect(first.definition == second.definition);
    try std.testing.expect(first.values.ptr != second.values.ptr);
    const first_inputs = first.selected.pool.selection.?.inputs.entries;
    const second_inputs = second.selected.pool.selection.?.inputs.entries;
    try std.testing.expect(first_inputs.ptr != second_inputs.ptr);
    try std.testing.expect(!std.meta.eql(first_inputs[0].value.resident.planes[0].backing, second_inputs[0].value.resident.planes[0].backing));
    // Same physical weight bytes and shape, different checkpoint role: deny.
    bound[0].value = key;
    if (Schedule.instantiate(a, &cb, definition, owned, &bound)) |unexpected| {
        unexpected.deinit(cleanup);
        return error.ExpectedNamedRoleDenial;
    } else |err| switch (err) {
        error.NativeWeightRoleChanged => {},
        else => return err,
    }
    bound[0].value = query;
    const wrong_shape = try cb.fromFloat32Shape(&data, &.{ 2, 2 });
    defer cb.free(wrong_shape);
    bound[2].value = wrong_shape;
    if (Schedule.instantiate(a, &cb, definition, owned, &bound)) |unexpected| {
        unexpected.deinit(cleanup);
        return error.ExpectedCurrentShapeDenial;
    } else |err| switch (err) {
        error.DenseDefinitionChanged => {},
        else => return err,
    }
    bound[2].value = second_state;
    bound[0].source_data = @ptrFromInt(16);
    if (Schedule.instantiate(a, &cb, definition, owned, &bound)) |unexpected| {
        unexpected.deinit(cleanup);
        return error.ExpectedCurrentModelDenial;
    } else |err| switch (err) {
        error.ForeignNativeWeightModel => {},
        else => return err,
    }
    bound[0].source_data = &weights;
    for ([_]*Schedule{ original, first, second }) |schedule| {
        try schedule.enter(null, cleanup, &.{});
        try schedule.beginExecution(cleanup);
        const sum = try schedule.execute(&.{ query, key }, null);
        const output = try schedule.execute(&.{ sum, if (schedule == second) second_state else first_state }, null);
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, if (schedule == second) &.{ 1, 2, 3, 4 } else &.{ 3, 6, 9, 12 }, actual);
    }
    first.deinit(cleanup);
    first_open = false;
    // Request one's metadata and snapshots have died; request two retains only
    // the shared pure Definition and its own current facts/physical authority.
    try second.selected.validate(second_inputs);
    const second_output = second.values[second.steps[second.steps.len - 1]].?;
    const held = try cb.primReshape(second_output, &.{ 1, 4 });
    defer cb.free(held);
    second.deinit(cleanup);
    second_open = false;
    const held_values = try cb.toFloat32(held, a);
    defer a.free(held_values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, held_values);
}

test "dense decoder semantic recipes match original graphs and rebind named current request sources" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try semanticRecipeCase(std.testing.allocator);
}

test "dense decoder semantic recipe creation and cached instantiation unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, semanticRecipeCase, .{});
}
