// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold route facts and exact metadata validation. This owner stores no frame
//! addresses. A selected predecessor grants readable logical data only; padded
//! output capacity is issued separately by its retained allocation Lease.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const route = @import("kernel_route.zig");
const ownership = @import("storage_ownership.zig");
const facts = @import("prepared_routes.zig");
const physical = @import("physical_buffers.zig");
const contracts = @import("../graph/backend_contracts.zig");
const NodeId = ml.graph.NodeId;
const max_planes = ops.kernel_descriptor.max_auxiliaries + 1;
var epochs: std.atomic.Value(u64) = .init(1);
fn nextEpoch() !u64 {
    var old = epochs.load(.monotonic);
    while (true) {
        if (old == std.math.maxInt(u64)) return error.PreparedRouteEpochExhausted;
        if (epochs.cmpxchgWeak(old, old + 1, .monotonic, .monotonic)) |actual| old = actual else return old;
    }
}

pub const Selection = struct {
    allocator: std.mem.Allocator,
    epoch: u64,
    inputs: *facts.ImmutableBindings,
    routes: []?route.Owned,
    planes: [][max_planes]ownership.Plane,
    plane_counts: []u8,
    literal_enabled: []bool,
    alias_enabled: []bool = &.{},
    invocation_nodes: []NodeId = &.{},
    planes_witnessed: []bool = &.{},
    literal_claim_selection: ?*physical.SharedLiteralStore.LiteralSelection = null,

    pub fn create(pool: *physical.Pool, runtime: []const route.InputBinding) !Selection {
        return createWithAuthority(pool, runtime, null);
    }
    pub fn createWithAuthority(pool: *physical.Pool, runtime: []const route.InputBinding, authority: ?@import("backend_operation.zig").Borrowed) !Selection {
        return createSourceWithAuthority(pool, .{ .slice = runtime }, authority);
    }
    pub fn createSourceWithAuthority(pool: *physical.Pool, runtime: facts.InputSource, authority: ?@import("backend_operation.zig").Borrowed) !Selection {
        const allocator = pool.allocator;
        for (0..runtime.len()) |index| {
            const input = try runtime.at(index);
            const slot = pool.plan.slotForNode(input.node_id) orelse return error.InvalidKernelInputBinding;
            if (pool.immutable_bindings) |immutable| if (immutable.find(input.node_id) != null) return error.InvalidRuntimeKernelFact;
            if (!slot.roles.runtime_input and slot.kind != .runtime_input) return error.InvalidKernelInputBinding;
            switch (input.value) {
                .tensor, .planned => return error.InvalidRuntimeKernelFact,
                .resident, .imported_host, .unknown => {},
            }
        }
        const inputs = try facts.ImmutableBindings.createSource(allocator, runtime);
        errdefer inputs.release();
        const selected = try allocator.alloc(?route.Owned, pool.kernels.len);
        errdefer allocator.free(selected);
        @memset(selected, null);
        const planes = try allocator.alloc([max_planes]ownership.Plane, selected.len);
        errdefer allocator.free(planes);
        const counts = try allocator.alloc(u8, selected.len);
        errdefer allocator.free(counts);
        @memset(counts, 0);
        const enabled = try allocator.alloc(bool, if (pool.literal_store) |store| store.literals.len else 0);
        errdefer allocator.free(enabled);
        @memset(enabled, true);
        if (pool.plan.execution_reachable) |reachable| {
            if (reachable.len != pool.plan.node_slots.len) return error.InvalidReachabilityTable;
            if (pool.literal_store) |store| for (store.literals, enabled) |literal, *active| {
                active.* = reachable[literal.node];
            };
        }
        const aliases = try allocator.alloc(bool, pool.metadata_aliases.len);
        errdefer allocator.free(aliases);
        @memset(aliases, true);
        const witnessed = try allocator.alloc(bool, selected.len);
        errdefer allocator.free(witnessed);
        @memset(witnessed, false);
        const epoch = try nextEpoch();
        var self = Selection{ .allocator = allocator, .epoch = epoch, .inputs = inputs, .routes = selected, .planes = planes, .plane_counts = counts, .literal_enabled = enabled, .alias_enabled = aliases, .planes_witnessed = witnessed };
        errdefer self.clearRoutes();
        errdefer allocator.free(self.invocation_nodes);
        try self.refreshWithAuthority(pool, authority);
        return self;
    }

    /// Rebuild after cold selection/refresh only. Publish atomically on success.
    pub fn finalizeValidationWorklist(self: *Selection) !void {
        var count: usize = 0;
        for (self.routes) |maybe| if (maybe) |owned| {
            if (owned.validation_scope == .invocation) count += 1;
        };
        const nodes = try self.allocator.alloc(NodeId, count);
        var next: usize = 0;
        for (self.routes, 0..) |maybe, node| if (maybe) |owned| {
            if (owned.validation_scope == .invocation) {
                nodes[next] = @intCast(node);
                next += 1;
            }
        };
        self.allocator.free(self.invocation_nodes);
        self.invocation_nodes = nodes;
    }

    /// Cold only, before a Frame/admission claim exists. Region recoloring can
    /// change output allocation IDs without changing semantic input layouts.
    pub fn refresh(self: *Selection, pool: *physical.Pool) !void {
        return self.refreshWithAuthority(pool, null);
    }
    pub fn refreshWithAuthority(self: *Selection, pool: *physical.Pool, authority: ?@import("backend_operation.zig").Borrowed) !void {
        self.clearRoutes();
        if (self.literal_claim_selection) |claim| claim.release();
        self.literal_claim_selection = null;
        while (true) {
            try self.select(pool, authority);
            var changed = false;
            if (pool.literal_store) |store| for (store.literals, 0..) |literal, index| {
                if (!self.literal_enabled[index] or store.backends[literal.backend_index].kind() == .native) continue;
                for (literal.consumers) |consumer| if (consumer >= self.routes.len or self.routes[consumer] == null) {
                    self.literal_enabled[index] = false;
                    changed = true;
                    break;
                };
            };
            // Legacy lazy consumers may materialize their input wrapper. Never
            // freeze its affine planes for another exact consumer in that case.
            if (pool.coverage_graph) |graph| for (pool.metadata_aliases, 0..) |alias, alias_index| {
                if (!self.alias_enabled[alias_index] or self.plane_counts[alias.node] == 0) continue;
                for (graph.nodes.items, 0..) |consumer, consumer_id| {
                    if (pool.plan.slotForNode(@intCast(consumer_id)) == null) continue;
                    var consumes = false;
                    // The alias output AND its source wrapper must remain
                    // representation-stable. A legacy source consumer can
                    // materialize an existing resident view before this alias.
                    for (consumer.getInputs()) |input| consumes = consumes or input == alias.node or input == alias.source;
                    if (!consumes or self.routes[consumer_id] != null) continue;
                    var exact_alias = false;
                    for (pool.metadata_aliases, 0..) |next, next_index| if (next.node == consumer_id and self.alias_enabled[next_index] and self.plane_counts[consumer_id] == 1) {
                        exact_alias = true;
                        break;
                    };
                    if (!exact_alias) {
                        self.alias_enabled[alias_index] = false;
                        changed = true;
                        break;
                    }
                }
            };
            if (!changed) break;
            self.clearRoutes();
        }
        try self.finalizeValidationWorklist();
        if (pool.literal_store) |store| self.literal_claim_selection = try physical.SharedLiteralStore.LiteralSelection.create(self.allocator, store, self.literal_enabled);
    }

    fn clearRoutes(self: *Selection) void {
        for (self.routes) |*selected| if (selected.*) |*owned| {
            owned.deinit();
            selected.* = null;
        };
        @memset(self.plane_counts, 0);
        @memset(self.planes_witnessed, false);
    }

    pub fn deinit(self: *Selection) void {
        self.deinitWithCleanup(.{});
    }
    pub fn deinitWithCleanup(self: *Selection, cleanup: @import("cleanup_context.zig").Context) void {
        self.clearRoutes();
        if (self.literal_claim_selection) |selected| selected.releaseWithCleanup(cleanup);
        self.inputs.releaseWithCleanup(cleanup);
        self.allocator.free(self.routes);
        self.allocator.free(self.planes);
        self.allocator.free(self.plane_counts);
        self.allocator.free(self.literal_enabled);
        self.allocator.free(self.alias_enabled);
        self.allocator.free(self.invocation_nodes);
        self.allocator.free(self.planes_witnessed);
        self.* = undefined;
    }

    pub fn output(self: *const Selection, pool: *const physical.Pool, node: NodeId, bytes: usize) route.OutputSlot {
        const kernel = pool.kernels[node].?;
        return .{ .allocation = kernel.output_allocation, .node = node, .epoch = self.epoch, .logical = kernel.descriptor.output_shape, .writable_bytes = bytes };
    }

    pub fn binding(self: *const Selection, pool: *const physical.Pool, node: NodeId, runtime: []const route.InputBinding) route.Binding {
        for (runtime) |entry| if (entry.node_id == node) return entry.value;
        if (pool.immutable_bindings) |immutable| if (immutable.find(node)) |value| return value;
        if (node < self.plane_counts.len and self.plane_counts[node] != 0) return .{ .planned = .{ .logical = pool.plan.slotForNode(node).?.desc.shape, .planes = self.planes[node][0..self.plane_counts[node]] } };
        // Alias layout/offset must be supplied by an authoritative provider;
        // following only allocation IDs would invent dense representation.
        return .unknown;
    }

    // Only the retained snapshot reaches this path. Its existing exact index
    // supplies both facts from one lookup; arbitrary public slices keep their
    // original duplicate/equality validation and linear binding adapter.
    const RetainedInput = struct { value: route.Binding, witnessed: bool };
    fn retainedInput(self: *const Selection, pool: *const physical.Pool, node: NodeId) RetainedInput {
        if (self.inputs.find(node)) |value| return .{ .value = value, .witnessed = switch (value) {
            .resident, .imported_host => true,
            else => false,
        } };
        if (pool.immutable_bindings) |immutable| if (immutable.find(node)) |value| return .{ .value = value, .witnessed = false };
        if (node < self.plane_counts.len and self.plane_counts[node] != 0) return .{
            .value = .{ .planned = .{ .logical = pool.plan.slotForNode(node).?.desc.shape, .planes = self.planes[node][0..self.plane_counts[node]] } },
            .witnessed = node < self.planes_witnessed.len and self.planes_witnessed[node],
        };
        return .{ .value = .unknown, .witnessed = false };
    }

    fn readOnlyHostInputs(pool: *const physical.Pool, current: NodeId) [3]bool {
        var result: [3]bool = @splat(false);
        const graph = pool.coverage_graph orelse return result;
        const reachable = pool.coverage_reachable orelse return result;
        if (reachable.len != graph.nodeCount()) return result;
        const kernel = pool.kernels[current].?;
        // The current reachable consumer itself contradicts the original ALL
        // CUDA-gather consumers predicate. Canonical Native identity is a pure
        // kind proof; do not invoke an extra foreign backendKind callback.
        // Require exact graph/kernel correspondence. Rewritten, unreachable,
        // foreign gather or malformed backend cases retain the complete scan.
        const known_current = current < graph.nodeCount() and reachable[current] and
            std.mem.eql(NodeId, graph.nodes.items[current].getInputs(), kernel.inputs[0..kernel.descriptor.input_count]);
        if (known_current and (kernel.descriptor.operation != .gather or
            (kernel.backend_index < pool.backends.len and pool.backends[kernel.backend_index].vtable == &@import("../ops/native_compute.zig").vtable_impl))) return result;
        for (kernel.inputs[0..kernel.descriptor.input_count], 0..) |source, position| {
            const slot = pool.plan.slotForNode(source) orelse continue;
            if (slot.kind != .runtime_input or slot.roles.graph_output) continue;
            if (known_current and position != 1) continue;
            var seen = false;
            var valid = true;
            for (graph.nodes.items, 0..) |node, id| {
                if (!reachable[id]) continue;
                for (node.getInputs(), 0..) |input, input_index| if (input == source) {
                    seen = true;
                    const consumer = pool.kernels[id] orelse {
                        valid = false;
                        continue;
                    };
                    if (consumer.descriptor.operation != .gather or input_index != 1 or consumer.inputs[1] != source or consumer.backend_index >= pool.backends.len or pool.backends[consumer.backend_index].kind() != .cuda) valid = false;
                };
            }
            result[position] = seen and valid;
        }
        return result;
    }

    fn densePlane(shape: ml.graph.Shape, backend: ops.ComputeBackend, role: @FieldType(ownership.Plane, "role"), epoch: u64) !ownership.Plane {
        const alignment = try @import("../ops/physical_storage.zig").minimumAlignment(backend);
        const kind = backend.kind();
        const elements = shape.numElements() orelse return error.InvalidPreparedKernelShape;
        if (elements <= 0) return error.InvalidPreparedKernelShape;
        const bytes = try std.math.mul(usize, @intCast(elements), shape.dtype.byteSize());
        var descriptor = contracts.TensorDesc.init(shape, switch (kind) {
            .native => if (shape.dtype == .f32) .host_f32 else .host_dense,
            .cuda => .cuda_buffer,
            .metal => .metal_buffer,
            else => return error.UnsupportedPreparedKernelBackend,
        });
        descriptor.current_backend = kind;
        descriptor.resident_backend = kind;
        return .{ .descriptor = descriptor, .role = role, .storage_epoch = epoch, .byte_offset = 0, .base_alignment = alignment, .logical_bytes = bytes, .readable_bytes = bytes };
    }

    fn select(self: *Selection, pool: *physical.Pool, authority: ?@import("backend_operation.zig").Borrowed) !void {
        if (pool.literal_store) |store| for (store.literals, 0..) |literal, index| {
            if (!self.literal_enabled[index]) continue;
            self.planes[literal.node][0] = try densePlane(literal.shape, store.backends[literal.backend_index], .primary, self.epoch);
            self.plane_counts[literal.node] = 1;
            self.planes_witnessed[literal.node] = true;
        };
        // The physical plan records actual submission order. Graph IDs are not
        // necessarily topological after independent branches are partitioned.
        const order = try self.allocator.alloc(NodeId, pool.kernels.len);
        defer self.allocator.free(order);
        for (order, 0..) |*node, index| node.* = @intCast(index);
        std.mem.sort(NodeId, order, @as(*const @import("../graph/buffer_plan.zig").BufferPlan, &pool.plan), struct {
            fn less(plan: *const @import("../graph/buffer_plan.zig").BufferPlan, a: NodeId, b: NodeId) bool {
                return plan.positionForNode(a) < plan.positionForNode(b);
            }
        }.less);
        for (order) |node| {
            if (try self.selectAlias(pool, node)) continue;
            const kernel = if (pool.kernels[node]) |*value| value else continue;
            const backend = pool.backends[kernel.backend_index];
            const functions = backend.vtable.kernels orelse continue;
            if (!functions.accepts(backend.vtable)) continue;
            const prepare = functions.prepareKernelRoute orelse continue;
            var incoming: [3]route.Binding = undefined;
            var witnesses: [3]bool = @splat(false);
            var output_witnessed = true;
            for (kernel.inputs[0..kernel.descriptor.input_count], 0..) |input, index| {
                const retained = self.retainedInput(pool, input);
                incoming[index] = retained.value;
                witnesses[index] = retained.witnessed;
                output_witnessed = output_witnessed and witnesses[index];
            }
            const owned = (try prepare(backend.ptr, .{ .metadata_authority = authority, .allocator = self.allocator, .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .descriptor = &kernel.descriptor, .host_readonly_inputs = readOnlyHostInputs(pool, node), .inputs = incoming[0..kernel.descriptor.input_count], .inputs_witnessed = witnesses, .output = self.output(pool, node, 0) })) orelse continue;
            self.routes[node] = owned;
            const required = owned.requirements;
            if (required.auxiliaries.count > ops.kernel_descriptor.max_auxiliaries) return error.InvalidPreparedKernelRequirements;
            const logical = kernel.descriptor.output_shape;
            const primary = try densePlane(logical, backend, .primary, self.epoch);
            if ((logical.dtype != .f32 and logical.dtype != .i32 and logical.dtype != .i64) or required.primary_bytes < primary.logical_bytes or required.primary_alignment == 0 or !std.math.isPowerOfTwo(required.primary_alignment)) return error.InvalidPreparedKernelRequirements;
            if (owned.implementation.context != backend.ptr or owned.implementation.table != @as(*const anyopaque, backend.vtable)) return error.InvalidPreparedKernelImplementation;
            self.planes[node][0] = primary;
            var count: u8 = 1;
            for (required.auxiliaries.items()) |auxiliary| {
                if (count == max_planes) return error.InvalidPreparedKernelRequirements;
                self.planes[node][count] = try densePlane(auxiliary.shape, backend, switch (auxiliary.role) {
                    .bf16_mirror => .bf16_mirror,
                    .mean => .mean,
                    .inverse_std => .inverse_std,
                }, self.epoch);
                count += 1;
            }
            self.plane_counts[node] = count;
            // Publish representation authority only after this exact producer
            // validates its live inputs/profile and assigned output geometry.
            // A materializing kernel can guarantee its output layout while its
            // source weight remains mutable and invocation-validated.
            try owned.validate(.{ .inputs = incoming[0..kernel.descriptor.input_count], .output = self.output(pool, node, required.primary_bytes) });
            self.planes_witnessed[node] = output_witnessed or owned.output_plane_metadata == .assigned_dense;
        }
    }

    fn selectAlias(self: *Selection, pool: *const physical.Pool, node: NodeId) !bool {
        if (pool.plan.slotForNode(node) == null) return false;
        for (pool.metadata_aliases, 0..) |alias, alias_index| {
            if (alias.node != node or !self.alias_enabled[alias_index]) continue;
            const slot = pool.plan.slotForNode(node).?;
            const source = self.retainedInput(pool, alias.source);
            const incoming = source.value;
            if (slot.backend != .native and (incoming != .planned or incoming.planned.planes.len != 1 or @import("metadata_alias.zig").activationRoot(&pool.plan, alias.source, slot.backend) == null)) {
                // Preserve the existing ordinary GPU reshape facts/dispatch.
                // No exact retained Frame hook is claimed for external inputs.
                self.alias_enabled[alias_index] = false;
                break;
            }
            for (pool.backends) |backend| {
                if (backend.kind() != slot.backend) continue;
                const functions = backend.vtable.kernels orelse continue;
                if (!functions.accepts(backend.vtable) or functions.createMetadataAlias == null) continue;
                // Public host facts describe import semantics, not a retained
                // aliasable CT. Its ordinary ingress/legacy owner stays opaque.
                const primary = try @import("dense_alias.zig").primary(incoming, backend, alias.input);
                const plane = alias.apply(primary orelse continue) orelse continue;
                self.planes[node][0] = plane;
                self.plane_counts[node] = 1;
                self.planes_witnessed[node] = source.witnessed;
                return true;
            }
            // A declined Native affine alias must not regain stale facts.
            // Ordinary GPU input/legacy reshape behavior remains unchanged.
            if (slot.backend == .native) return false;
            self.alias_enabled[alias_index] = false;
            break;
        }
        if (pool.plan.slotForNode(node)) |slot| if (slot.backend == .native) return false;
        if (node >= pool.reshape_sources.len) return false;
        const source = pool.reshape_sources[node];
        if (source == ml.graph.null_node) return false;
        const source_slot = pool.plan.slotForNode(source) orelse return false;
        const output_slot = pool.plan.slotForNode(node) orelse return false;
        const retained = self.retainedInput(pool, source);
        const binding_value = retained.value;
        for (pool.backends) |backend| {
            if (backend.kind() != output_slot.backend) continue;
            const functions = backend.vtable.kernels orelse continue;
            if (!functions.accepts(backend.vtable) or !functions.physicalReplayReshapeViews) continue;
            const candidate = if (binding_value == .imported_host) imported: {
                const host = binding_value.imported_host;
                if (host.target != backend.kind() or !host.logical.eq(source_slot.desc.shape)) break :imported null;
                break :imported try densePlane(host.logical, backend, .primary, self.epoch);
            } else try @import("dense_alias.zig").primary(binding_value, backend, source_slot.desc.shape);
            const primary = candidate orelse continue;
            const alias = @import("dense_alias.zig").reshapePlane(primary, source_slot.desc.shape, output_slot.desc.shape, backend.kind(), source) orelse continue;
            self.planes[node][0] = alias;
            self.plane_counts[node] = 1;
            self.planes_witnessed[node] = retained.witnessed;
            return true;
        }
        return false;
    }

    fn equalPlanes(a: []const ownership.Plane, b: []const ownership.Plane) bool {
        if (a.len != b.len) return false;
        for (a, b) |left, right| if (!std.meta.eql(left, right)) return false;
        return true;
    }
    fn equalBinding(a: route.Binding, b: route.Binding) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .unknown => true,
            .imported_host => |value| std.meta.eql(value, b.imported_host),
            .tensor => |value| value.value == b.tensor.value and std.meta.eql(value.implementation, b.tensor.implementation),
            .planned => |value| std.meta.eql(value.logical, b.planned.logical) and equalPlanes(value.planes, b.planned.planes),
            .resident => |value| std.meta.eql(value.logical, b.resident.logical) and std.meta.eql(value.implementation, b.resident.implementation) and equalPlanes(value.planes, b.resident.planes),
        };
    }
    fn matchesInputs(self: *const Selection, runtime: []const route.InputBinding) bool {
        if (runtime.len != self.inputs.entries.len) return false;
        for (self.inputs.entries) |saved| {
            var found = false;
            for (runtime) |input| if (input.node_id == saved.node_id) {
                if (found or !equalBinding(saved.value, input.value)) return false;
                found = true;
            };
            if (!found) return false;
        }
        return true;
    }
    pub fn matches(self: *const Selection, pool: *const physical.Pool, runtime: []const route.InputBinding) bool {
        if (!self.matchesInputs(runtime)) return false;
        for (self.invocation_nodes) |node| {
            const owned = self.routes[node].?;
            const kernel = pool.kernels[node].?;
            var incoming: [3]route.Binding = undefined;
            for (kernel.inputs[0..kernel.descriptor.input_count], 0..) |input, index| incoming[index] = self.binding(pool, input, runtime);
            if (!owned.vtable.matches(owned.context, .{ .inputs = incoming[0..kernel.descriptor.input_count], .output = self.output(pool, node, owned.requirements.primary_bytes) })) return false;
        }
        return true;
    }
    pub fn validate(self: *const Selection, pool: *const physical.Pool, runtime: []const route.InputBinding) !void {
        if (!self.matchesInputs(runtime)) return error.PreparedKernelBindingChanged;
        return self.validatePolicies(pool, runtime);
    }
    pub fn validateRetainedPolicies(self: *const Selection, pool: *const physical.Pool) !void {
        return self.validatePolicies(pool, null);
    }
    fn validatePolicies(self: *const Selection, pool: *const physical.Pool, runtime: ?[]const route.InputBinding) !void {
        for (self.invocation_nodes) |node| {
            const owned = self.routes[node].?;
            const kernel = pool.kernels[node].?;
            var incoming: [3]route.Binding = undefined;
            for (kernel.inputs[0..kernel.descriptor.input_count], 0..) |input, index| incoming[index] = if (runtime) |public| self.binding(pool, input, public) else self.retainedInput(pool, input).value;
            try owned.validate(.{ .inputs = incoming[0..kernel.descriptor.input_count], .output = self.output(pool, node, owned.requirements.primary_bytes) });
        }
    }
};

test "invocation validation worklist excludes immutable programs and refresh is failure atomic" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const Methods = struct {
                fn matches(_: *anyopaque, _: route.Validate) bool {
                    return true;
                }
                fn validate(_: *anyopaque, _: route.Validate) !void {}
                fn deinit(_: *anyopaque) void {}
                const table = route.Owned.VTable{ .matches = matches, .validate = validate, .deinit = deinit };
            };
            const value = route.Owned{ .context = @ptrFromInt(8), .implementation = .{ .context = @ptrFromInt(8), .table = @ptrFromInt(8) }, .key = 1, .requirements = .{ .primary_bytes = 4, .profile = 1 }, .vtable = &Methods.table };
            var entries = [_]?route.Owned{ value, value, null };
            entries[0].?.validation_scope = .prepared_program;
            var selection = Selection{ .allocator = a, .epoch = 1, .inputs = undefined, .routes = &entries, .planes = &.{}, .plane_counts = &.{}, .literal_enabled = &.{} };
            defer a.free(selection.invocation_nodes);
            try selection.finalizeValidationWorklist();
            try std.testing.expectEqualSlices(NodeId, &.{1}, selection.invocation_nodes);
            entries[0].?.validation_scope = .invocation;
            try selection.finalizeValidationWorklist();
            try std.testing.expectEqualSlices(NodeId, &.{ 0, 1 }, selection.invocation_nodes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "invocation worklist validates dynamic owners once and always rejects changed public facts" {
    const a = std.testing.allocator;
    const Probe = struct {
        matches_count: usize = 0,
        validation_count: usize = 0,
        allowed: bool = true,
        fn matches(raw: *anyopaque, _: route.Validate) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.matches_count += 1;
            return self.allowed;
        }
        fn validate(raw: *anyopaque, _: route.Validate) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.validation_count += 1;
            if (!self.allowed) return error.PreparedKernelBindingChanged;
        }
        fn deinit(_: *anyopaque) void {}
        const table = route.Owned.VTable{ .matches = matches, .validate = validate, .deinit = deinit };
    };
    var dynamic = Probe{};
    var fixed = Probe{};
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .negate }, .input_count = 1, .input_shapes = .{ shape, .{}, .{} }, .output_shape = shape };
    var kernels = [_]?physical.KernelPlan{ null, .{ .descriptor = descriptor, .inputs = .{ 0, 0, 0 }, .backend_index = 0, .output_allocation = 0, .workspace_bytes = 0 }, .{ .descriptor = descriptor, .inputs = .{ 0, 0, 0 }, .backend_index = 0, .output_allocation = 1, .workspace_bytes = 0 } };
    const bound_inputs = [_]route.InputBinding{.{ .node_id = 0, .value = .{ .imported_host = .{ .logical = shape, .target = .native } } }};
    const inputs = try @import("prepared_routes.zig").ImmutableBindings.create(a, &bound_inputs);
    defer inputs.release();
    var entries = [_]?route.Owned{ null, .{ .context = &dynamic, .implementation = .{ .context = &dynamic, .table = &Probe.table }, .key = 1, .requirements = .{ .primary_bytes = 16, .profile = 1 }, .vtable = &Probe.table }, .{ .context = &fixed, .implementation = .{ .context = &fixed, .table = &Probe.table }, .key = 2, .requirements = .{ .primary_bytes = 16, .profile = 1 }, .validation_scope = .prepared_program, .vtable = &Probe.table } };
    var selection = Selection{ .allocator = a, .epoch = 1, .inputs = inputs, .routes = &entries, .planes = &.{}, .plane_counts = &.{}, .literal_enabled = &.{} };
    defer a.free(selection.invocation_nodes);
    const pool = physical.Pool{ .allocator = a, .plan = undefined, .kernels = &kernels, .transfers = &.{}, .backends = &.{}, .workspace_allocations = &.{} };
    try selection.finalizeValidationWorklist();
    try std.testing.expect(selection.matches(&pool, &bound_inputs));
    try selection.validate(&pool, &bound_inputs);
    try std.testing.expectEqual(@as(usize, 1), dynamic.matches_count);
    try std.testing.expectEqual(@as(usize, 1), dynamic.validation_count);
    try std.testing.expectEqual(@as(usize, 0), fixed.matches_count);
    try std.testing.expectEqual(@as(usize, 0), fixed.validation_count);
    var changed = bound_inputs;
    changed[0].value.imported_host.logical = ml.graph.Shape.init(.f32, &.{8});
    try std.testing.expect(!selection.matches(&pool, &changed));
    try std.testing.expectError(error.PreparedKernelBindingChanged, selection.validate(&pool, &changed));
    try std.testing.expectEqual(@as(usize, 1), dynamic.matches_count);
    try std.testing.expectEqual(@as(usize, 1), dynamic.validation_count);
    dynamic.allowed = false;
    try std.testing.expect(!selection.matches(&pool, &bound_inputs));
    try std.testing.expectError(error.PreparedKernelBindingChanged, selection.validate(&pool, &bound_inputs));
}

test "failed invocation worklist rebuild retains previous published pointer and nodes" {
    const Methods = struct {
        fn matches(_: *anyopaque, _: route.Validate) bool {
            return true;
        }
        fn validate(_: *anyopaque, _: route.Validate) !void {}
        fn deinit(_: *anyopaque) void {}
        const table = route.Owned.VTable{ .matches = matches, .validate = validate, .deinit = deinit };
    };
    const value = route.Owned{ .context = @ptrFromInt(8), .implementation = .{ .context = @ptrFromInt(8), .table = @ptrFromInt(8) }, .key = 1, .requirements = .{ .primary_bytes = 4, .profile = 1 }, .vtable = &Methods.table };
    var entries = [_]?route.Owned{ value, value };
    entries[0].?.validation_scope = .prepared_program;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var selection = Selection{ .allocator = failing.allocator(), .epoch = 1, .inputs = undefined, .routes = &entries, .planes = &.{}, .plane_counts = &.{}, .literal_enabled = &.{} };
    defer selection.allocator.free(selection.invocation_nodes);
    try selection.finalizeValidationWorklist();
    const previous = selection.invocation_nodes.ptr;
    entries[0].?.validation_scope = .invocation;
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, selection.finalizeValidationWorklist());
    try std.testing.expect(selection.invocation_nodes.ptr == previous);
    try std.testing.expectEqualSlices(NodeId, &.{1}, selection.invocation_nodes);
}

test "prepared program witness propagates public aliases but taints model tensor aliases and descendants" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const backend = context.computeBackend();
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var builder = ml.graph.Builder.init(&graph);
            const input_shape = ml.graph.Shape.init(.f32, &.{4});
            const shaped = ml.graph.Shape.init(.f32, &.{ 2, 2 });
            const input = try builder.parameter("x", input_shape);
            const model = try builder.parameter("w", input_shape);
            const public_producer = try builder.neg(input);
            const public_alias = try builder.reshape(public_producer, shaped);
            const model_alias = try builder.reshape(model, shaped);
            const public_neg = try builder.neg(public_alias);
            const model_neg = try builder.neg(model_alias);
            const combined = try builder.add(public_neg, model_neg);
            try graph.markOutput(combined);
            const weight = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
            defer backend.free(weight);
            const immutable = [_]route.InputBinding{.{ .node_id = model, .value = .{ .tensor = .{ .value = weight, .implementation = .{ .context = backend.ptr, .table = backend.vtable } } } }};
            var analysis = try @import("../graph/interpreter.zig").CachedAnalysis.compute(a, &graph);
            defer analysis.deinit(a);
            var executable = try @import("executable.zig").prepareAnalyzedGraph(a, &graph, analysis, &backend, .partitioned, .{ .immutable_input_bindings = &immutable });
            defer executable.deinit();
            const pool = executable.runtime.physical_pool orelse return error.MissingPhysicalPool;
            const prepared = (try pool.prepareRoutes(&.{.{ .node_id = input, .value = .{ .imported_host = .{ .logical = input_shape, .target = .native } } }})) orelse return error.MissingPreparedKernelRoute;
            defer prepared.release();
            const selection = prepared.pool.selection orelse return error.MissingPreparedKernelRoute;
            try std.testing.expect(selection.planes_witnessed[public_alias]);
            try std.testing.expect(!selection.planes_witnessed[model_alias]);
            try std.testing.expect(selection.planes_witnessed[public_neg]);
            try std.testing.expect(!selection.planes_witnessed[model_neg]);
            try std.testing.expect(!selection.planes_witnessed[combined]);
            try std.testing.expectEqual(route.ValidationScope.prepared_program, selection.routes[public_neg].?.validation_scope);
            try std.testing.expectEqual(route.ValidationScope.invocation, selection.routes[model_neg].?.validation_scope);
            try std.testing.expectEqual(route.ValidationScope.invocation, selection.routes[combined].?.validation_scope);
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

fn retainedRouteInputCase(a: std.mem.Allocator) !void {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    const tensor = try backend.fromFloat32Shape(&.{ 1, 2 }, &.{2});
    defer backend.free(tensor);
    const shape = ml.graph.Shape.init(.f32, &.{2});
    const plane = try @import("dense_decoder_schedule.zig").residentPlane(&backend, tensor);
    const runtime = [_]route.InputBinding{
        .{ .node_id = 4, .value = .{ .resident = .{ .logical = shape, .planes = &.{plane}, .implementation = .{ .context = backend.ptr, .table = backend.vtable } } } },
        .{ .node_id = 0, .value = .{ .imported_host = .{ .logical = ml.graph.Shape.init(.i64, &.{1}), .target = .native } } },
        .{ .node_id = 8, .value = .unknown },
    };
    const immutable = [_]route.InputBinding{.{ .node_id = 12, .value = .{ .planned = .{ .logical = shape, .planes = &.{plane} } } }};
    const owned_runtime = try facts.ImmutableBindings.create(a, &runtime);
    defer owned_runtime.release();
    const owned_immutable = try facts.ImmutableBindings.create(a, &immutable);
    defer owned_immutable.release();
    const pool = physical.Pool{ .allocator = a, .plan = undefined, .kernels = &.{}, .transfers = &.{}, .backends = &.{}, .workspace_allocations = &.{}, .immutable_bindings = owned_immutable };
    const selection = Selection{ .allocator = a, .epoch = 1, .inputs = owned_runtime, .routes = &.{}, .planes = &.{}, .plane_counts = &.{}, .literal_enabled = &.{} };
    const reordered = [_]route.InputBinding{ runtime[2], runtime[0], runtime[1] };
    try std.testing.expect(selection.matchesInputs(&reordered));
    for ([_]NodeId{ 0, 4, 8, 12, 99 }) |node| {
        const retained = selection.retainedInput(&pool, node);
        try std.testing.expect(Selection.equalBinding(selection.binding(&pool, node, &reordered), retained.value));
        try std.testing.expectEqual(node == 0 or node == 4, retained.witnessed);
    }
    try std.testing.expect(selection.retainedInput(&pool, 99).value == .unknown);
    var duplicate = runtime;
    duplicate[1] = runtime[0];
    try std.testing.expect(!selection.matchesInputs(&duplicate));
    try std.testing.expectError(error.PreparedKernelBindingChanged, selection.validate(&pool, &duplicate));
    // Public slices retain their first-match adapter and duplicate refusal;
    // the retained owner itself never admits a duplicate into its index.
    try std.testing.expect(Selection.equalBinding(selection.binding(&pool, 4, &duplicate), runtime[0].value));
    if (facts.ImmutableBindings.create(a, &duplicate)) |unexpected| {
        unexpected.release();
        return error.ExpectedDuplicateKernelInputDenial;
    } else |err| switch (err) {
        error.DuplicateKernelInputBinding => {},
        else => return err,
    }
}

fn currentConsumerReadOnlyCase(a: std.mem.Allocator) !void {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const native = context.computeBackend();
    const Metadata = struct {
        fn cudaKind(_: *anyopaque) ops.BackendKind {
            return .cuda;
        }
        fn nativeKind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        const cuda_table: ops.ComputeBackend.VTable = value: {
            var table = @import("../ops/native_compute.zig").vtable_impl;
            table.backendKind = cudaKind;
            break :value table;
        };
        const foreign_native_table: ops.ComputeBackend.VTable = value: {
            var table = @import("../ops/native_compute.zig").vtable_impl;
            table.backendKind = nativeKind;
            // Distinct literal content prevents immutable table/function
            // folding from disguising this same-kind foreign metadata case.
            table.kernels = null;
            break :value table;
        };
    };
    // Only these context-free kind callbacks are queried. No GPU provider,
    // kernel execution, source ownership, or capability is fabricated here.
    try std.testing.expect(&Metadata.foreign_native_table != &@import("../ops/native_compute.zig").vtable_impl);
    const backends = [_]ops.ComputeBackend{ native, .{ .ptr = @ptrFromInt(16), .vtable = &Metadata.cuda_table }, .{ .ptr = @ptrFromInt(16), .vtable = &Metadata.foreign_native_table } };
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const table_shape = ml.graph.Shape.init(.f32, &.{ 4, 2 });
    const output_shape = ml.graph.Shape.init(.f32, &.{ 1, 2 });
    const table = try builder.parameter("table", table_shape);
    const indices = try builder.parameter("indices", ml.graph.Shape.init(.i64, &.{1}));
    const first = try builder.gather(table, indices, output_shape);
    const second = try builder.gather(table, indices, output_shape);
    try graph.markOutput(first);
    try graph.markOutput(second);
    var layout = try @import("../graph/runtime.zig").PhysicalLayout.create(a, &graph, &.{&native}, .{}, false);
    defer layout.deinit();
    const reachable = try a.alloc(bool, graph.nodeCount());
    defer a.free(reachable);
    @memset(reachable, true);
    const kernels = try a.alloc(?physical.KernelPlan, graph.nodeCount());
    defer a.free(kernels);
    @memset(kernels, null);
    for ([_]NodeId{ first, second }) |node| kernels[node] = .{
        .descriptor = ops.kernel_descriptor.lower(&graph, node).?,
        .inputs = .{ table, indices, 0 },
        .backend_index = 1,
        .output_allocation = 0,
        .workspace_bytes = 0,
    };
    const pool = physical.Pool{ .allocator = a, .plan = layout.buffers, .kernels = kernels, .transfers = &.{}, .backends = &backends, .workspace_allocations = &.{}, .coverage_graph = &graph, .coverage_reachable = reachable };
    // Both live consumers are CUDA gathers of this exact source as indices.
    try std.testing.expectEqual([_]bool{ false, true, false }, Selection.readOnlyHostInputs(&pool, first));
    kernels[second].?.backend_index = 0;
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, first));
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, second));
    // An unreachable current Native consumer is excluded by the ORIGINAL
    // predicate, so the remaining reachable CUDA consumer still permits it.
    reachable[second] = false;
    try std.testing.expectEqual([_]bool{ false, true, false }, Selection.readOnlyHostInputs(&pool, second));
    reachable[second] = true;
    // A current rewritten input slice cannot prove the Native contradiction.
    graph.nodes.items[second].inputs[1] = table;
    try std.testing.expectEqual([_]bool{ false, true, false }, Selection.readOnlyHostInputs(&pool, second));
    graph.nodes.items[second].inputs[1] = indices;
    kernels[second].?.backend_index = 2;
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, second));
    kernels[second].?.backend_index = backends.len;
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, first));
    const saved = kernels[second].?;
    kernels[second] = null;
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, first));
    kernels[second] = saved;
    kernels[second].?.backend_index = 1;
    kernels[second].?.descriptor.operation = .{ .unary = .negate };
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, second));
    try std.testing.expectEqual([_]bool{ false, false, false }, Selection.readOnlyHostInputs(&pool, first));
}

test "retained route input index matches public slices and preserves witnessed duplicate and absent semantics" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try retainedRouteInputCase(std.testing.allocator);
    try currentConsumerReadOnlyCase(std.testing.allocator);
}

test "retained route input and exact mixed consumer preparation unwind every allocation failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, retainedRouteInputCase, .{});
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, currentConsumerReadOnlyCase, .{});
}
