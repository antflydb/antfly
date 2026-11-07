// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Complete program-owned tensor/workspace payload, not a process RSS bound.
//! Opaque model/vendor envelopes and request IO retain independent protection.
const std = @import("std");
const physical = @import("physical_buffers.zig");
const buffers = @import("../graph/buffer_plan.zig");
const route = @import("kernel_route.zig");
const ownership = @import("storage_ownership.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const ml = @import("ml");
pub const Reason = enum { no_selected_routes, execution_path, backend_region, transfer, input_representation, literal, alias, output_publication, operation, workspace };
pub const Complete = struct { accounting_class: enum { host, device } = .host, epoch: u64, operations: usize = 0, aliases: usize = 0, literals: usize = 0 };
pub const Coverage = union(enum) { compatibility: Reason, complete: Complete };

fn canonical(backend: ops.ComputeBackend) bool {
    return backend.vtable == &native.vtable_impl;
}
fn logicalBytes(shape: ml.graph.Shape) ?usize {
    if (shape.dtype != .f32 and shape.dtype != .i32 and shape.dtype != .i64) return null;
    const n = shape.numElements() orelse return null;
    if (n <= 0) return null;
    return std.math.mul(usize, @intCast(n), shape.dtype.byteSize()) catch null;
}
fn allocation(pool: *const physical.Pool, id: buffers.AllocationId, bytes: usize, kind: buffers.AllocationKind) bool {
    if (id == buffers.invalid_allocation or id >= pool.plan.allocations.len) return false;
    const item = (pool.plan.allocationById(id) orelse return false).*;
    return item.id == id and item.backend == .native and item.kind == kind and item.byte_size >= bytes;
}
/// Compressed primary payload is accepted only from the actual canonical
/// Native tensor report (exact_dtype=false), never a declared resident plane.
/// The separately pinned Model owns raw + every prepared KBlock/panel; this is
/// representation validation, not new Frame storage or model-byte credit.
fn packedPrimaryValid(plane: ownership.Plane, expected: ml.graph.Shape) bool {
    const desc = plane.descriptor;
    const actual = desc.shape;
    if (desc.storage != .host_packed_quant or desc.quant_format == null or desc.resident_backend != .native or desc.view_source != null or desc.attention_storage != null or desc.attention_kv_format != null or desc.device_id != 0 or plane.writable != null) return false;
    if (actual.dtype != .f32 or expected.dtype != .f32 or !actual.eq(expected) or actual.rank() != 2) return false;
    const format = desc.quant_format.?;
    switch (format) {
        .q4_k, .q5_k, .q6_k, .q8_0 => {},
        else => return false,
    }
    const geometry = @import("../graph/quant_matmul.zig").packedFormatDescriptor(format);
    if (geometry.values_per_block == 0 or geometry.bytes_per_block == 0 or actual.dim(0) <= 0 or actual.dim(1) <= 0) return false;
    const rows = std.math.cast(usize, actual.dim(0)) orelse return false;
    const width = std.math.cast(usize, actual.dim(1)) orelse return false;
    if (width % geometry.values_per_block != 0) return false;
    const row_bytes = std.math.mul(usize, width / geometry.values_per_block, geometry.bytes_per_block) catch return false;
    const bytes = std.math.mul(usize, rows, row_bytes) catch return false;
    const dense = @import("../graph/backend_contracts.zig").TensorStrides.dense(actual);
    if (!std.meta.eql(desc.strides, dense) or plane.logical_bytes != bytes or plane.readable_bytes != bytes or plane.layout_key != null) return false;
    const backing = plane.backing orelse return false;
    if (backing.kind != .host or backing.context != null) return false;
    const address = std.math.add(usize, backing.base, plane.byte_offset) catch return false;
    _ = std.math.add(usize, address, bytes) catch return false;
    return true;
}
fn primaryValid(plane: ownership.Plane, expected: ml.graph.Shape, exact_dtype: bool) bool {
    if (plane.role != .primary or plane.backing == null or plane.descriptor.current_backend != .native or plane.logical_bytes == 0 or plane.readable_bytes < plane.logical_bytes) return false;
    if (plane.descriptor.quant_format != null or plane.descriptor.storage == .host_packed_quant) {
        if (exact_dtype) return false; // Pure declared resident planes gain no packed authority.
        return packedPrimaryValid(plane, expected);
    }
    const actual = plane.descriptor.shape;
    const count = actual.numElements() orelse return false;
    if (count <= 0) return false;
    const bytes = std.math.mul(usize, @intCast(count), actual.dtype.byteSize()) catch return false;
    if (plane.logical_bytes != bytes) return false;
    if (!exact_dtype and actual.dtype != .f32 and actual.dtype != .f16 and actual.dtype != .bf16) return false;
    return (!exact_dtype or actual.dtype == expected.dtype) and actual.rank() == expected.rank() and std.mem.eql(i64, actual.dims[0..actual.rank()], expected.dims[0..expected.rank()]);
}
fn inputKnown(pool: *const physical.Pool, node: ml.graph.NodeId, binding: route.Binding, expected: ml.graph.Shape, target: ops.ComputeBackend) bool {
    return switch (binding) {
        .imported_host => |host| check: {
            if (!host.owned_ingress or host.target != .native or !host.logical.eq(expected)) break :check false;
            for (pool.host_input_plans) |plan| if (plan.node == node) {
                break :check nativeInputPlan(pool, plan, target);
            };
            break :check false;
        },
        .resident => |value| check: {
            if (value.implementation.context != target.ptr or value.implementation.table != @as(*const anyopaque, @ptrCast(&native.vtable_impl)) or !value.logical.eq(expected)) break :check false;
            var count: usize = 0;
            for (value.planes) |plane| if (plane.role == .primary) {
                if (!primaryValid(plane, expected, true)) break :check false;
                count += 1;
            };
            break :check count == 1;
        },
        .tensor => |value| check: {
            if (value.implementation.context != target.ptr or value.implementation.table != @as(*const anyopaque, @ptrCast(&native.vtable_impl))) break :check false;
            const Collector = struct {
                expected: ml.graph.Shape,
                count: usize = 0,
                valid: bool = true,
                fn emit(raw: *anyopaque, report: ownership.Report) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (report == .plane and report.plane.role == .primary) {
                        self.count += 1;
                        self.valid = self.valid and primaryValid(report.plane, self.expected, false);
                    }
                }
            };
            var observed = Collector{ .expected = expected };
            const query = native.vtable_impl.kernels.?.storageOwnership orelse break :check false;
            const coverage = query(value.implementation.context, value.value, .{ .context = &observed, .visit = Collector.emit }) catch break :check false;
            break :check coverage == .complete and observed.valid and observed.count == 1;
        },
        .unknown, .planned => false,
    };
}
fn literalIndex(pool: *const physical.Pool, slot: buffers.LogicalSlot) ?usize {
    const store = pool.literal_store orelse return null;
    const selected = if (pool.selection) |*value| value else return null;
    if (slot.node_id >= store.node_indices.len) return null;
    const index = store.node_indices[slot.node_id];
    if (index == ml.graph.null_node or index >= store.literals.len or index >= selected.literal_enabled.len or !selected.literal_enabled[index]) return null;
    const item = store.literals[index];
    const bytes = logicalBytes(item.shape) orelse return null;
    if (item.node != slot.node_id or !item.shape.eq(slot.desc.shape) or item.data.len != bytes / 4 or item.backend_index >= pool.backends.len or item.backend_index >= store.backends.len) return null;
    const backend = pool.backends[item.backend_index];
    const owner = store.backends[item.backend_index];
    if (!canonical(backend) or owner.ptr != backend.ptr or owner.vtable != backend.vtable or !allocation(pool, item.allocation, bytes, .constant_binding)) return null;
    return index;
}
fn publication(pool: *const physical.Pool, slot: buffers.LogicalSlot, index: usize) bool {
    const store = pool.literal_store orelse return false;
    if (slot.node_id >= store.publications.len) return false;
    const copy = store.publications[slot.node_id] orelse return false;
    const item = store.literals[index];
    const bytes = logicalBytes(item.shape) orelse return false;
    return copy.node == slot.node_id and copy.literal == index and copy.shape.eq(slot.desc.shape) and copy.allocation != item.allocation and allocation(pool, copy.allocation, bytes, .tensor) and !pool.plan.allocations[copy.allocation].reusable;
}
fn resources(pool: *const physical.Pool, kernel: physical.KernelPlan, owned: route.Owned) bool {
    const needed = owned.requirements;
    if (!allocation(pool, kernel.output_allocation, needed.primary_bytes, .tensor)) return false;
    const scratch = @max(needed.workspace.byte_size, needed.probe_workspace.byte_size);
    const host = @max(needed.workspace.host_byte_size, needed.probe_workspace.host_byte_size);
    if (kernel.workspace_bytes != scratch or kernel.host_workspace_bytes != host) return false;
    if (scratch != 0 and (kernel.backend_index >= pool.workspace_allocations.len or !allocation(pool, pool.workspace_allocations[kernel.backend_index], scratch, .workspace))) return false;
    if (host != 0 and (!allocation(pool, pool.host_workspace_allocation, host, .workspace) or pool.host_alignment.toByteUnits() < @max(needed.host_alignment, needed.probe_host_alignment))) return false;
    var count: usize = 0;
    for (kernel.auxiliaries) |maybe| if (maybe) |aux| {
        if (count >= needed.auxiliaries.count) return false;
        const declaration = needed.auxiliaries.items()[count];
        const bytes = logicalBytes(aux.requirement.shape) orelse return false;
        if (aux.requirement.role != declaration.role or !aux.requirement.shape.eq(declaration.shape) or !allocation(pool, aux.allocation, bytes, .auxiliary)) return false;
        count += 1;
    };
    return count == needed.auxiliaries.count;
}

fn nativeInputPlan(pool: *const physical.Pool, plan: anytype, target: ops.ComputeBackend) bool {
    const selected = if (pool.selection) |*value| value else return false;
    if (plan.backend_index >= pool.backends.len or plan.epoch != selected.epoch or plan.requirements.host_bytes != 0 or plan.requirements.host_backing_bytes != 0) return false;
    const backend = pool.backends[plan.backend_index];
    if (!canonical(backend) or backend.ptr != target.ptr) return false;
    const slot = pool.plan.slotForNode(plan.node) orelse return false;
    if (slot.kind != .runtime_input or !slot.desc.shape.eq(plan.shape)) return false;
    const bytes = logicalBytes(plan.shape) orelse return false;
    if (plan.requirements.bytes != bytes or !allocation(pool, plan.allocation, bytes, .input_binding)) return false;
    if (plan.borrowed and (plan.shape.dtype != .f32 or selected.inputs.find(plan.node) == null or selected.inputs.find(plan.node).? != .imported_host or !selected.inputs.find(plan.node).?.imported_host.borrowable_host)) return false;
    const known = selected.inputs.find(plan.node) orelse return false;
    if (known != .imported_host or !known.imported_host.owned_ingress or known.imported_host.target != .native or !known.imported_host.logical.eq(plan.shape)) return false;
    const query = backend.vtable.kernels.?.hostInputRequirements orelse return false;
    const current = (query(backend.ptr, plan.shape) catch return false) orelse return false;
    return current.eql(plan.requirements) and backend.vtable.kernels.?.createHostInput != null;
}

pub fn assess(pool: *const physical.Pool) Coverage {
    const selected = if (pool.selection) |*value| value else return .{ .compatibility = .no_selected_routes };
    const target_backend = pool.coverage_backend orelse return .{ .compatibility = .execution_path };
    if (@import("cuda_owned_coverage.zig").services(target_backend) != null) return @import("cuda_owned_coverage.zig").assess(pool, target_backend, null);
    if (!canonical(target_backend)) return .{ .compatibility = .execution_path };
    const graph = pool.coverage_graph orelse return .{ .compatibility = .execution_path };
    const reachable = pool.coverage_reachable orelse return .{ .compatibility = .execution_path };
    if (!pool.native_frame_dispatch or reachable.len != graph.nodeCount() or pool.plan.node_slots.len != graph.nodeCount() or graph.outputs.items.len == 0) return .{ .compatibility = .execution_path };
    if (pool.plan.transfers.len != 0) return .{ .compatibility = .transfer };
    for (pool.host_input_plans) |plan| if (!nativeInputPlan(pool, plan, target_backend)) return .{ .compatibility = .input_representation };
    var borrowed = false;
    for (pool.host_input_plans) |input| borrowed = borrowed or input.borrowed;
    if (borrowed) {
        if (pool.regions.len != 0) return .{ .compatibility = .backend_region };
        for (pool.host_input_plans) |input| if (!input.borrowed) return .{ .compatibility = .input_representation };
    }
    var result = Complete{ .epoch = selected.epoch };
    for (reachable, 0..) |executed, id| {
        if (!executed) continue;
        const slot = (pool.plan.slotForNode(@intCast(id)) orelse return .{ .compatibility = .operation }).*;
        if (slot.node_id != id or slot.id >= pool.plan.slots.len or slot.id != pool.plan.node_slots[id] or slot.backend != .native) return .{ .compatibility = .backend_region };
        const bytes = logicalBytes(slot.desc.shape) orelse return .{ .compatibility = .input_representation };
        const node = graph.node(@intCast(id));
        if (!slot.desc.shape.eq(node.output_shape)) return .{ .compatibility = .operation };
        switch (slot.kind) {
            .runtime_input => {
                if (node.op != .parameter) return .{ .compatibility = .input_representation };
                if (slot.roles.graph_output) return .{ .compatibility = .output_publication };
                var binding: route.Binding = .unknown;
                if (selected.inputs.find(@intCast(id))) |known| binding = known else if (pool.immutable_bindings) |immutable| if (immutable.find(@intCast(id))) |known| {
                    binding = known;
                };
                if (!inputKnown(pool, @intCast(id), binding, slot.desc.shape, target_backend)) return .{ .compatibility = .input_representation };
            },
            .constant => {
                if (node.op != .constant or literalIndex(pool, slot) == null) return .{ .compatibility = .literal };
                if (slot.roles.graph_output and !publication(pool, slot, literalIndex(pool, slot).?)) return .{ .compatibility = .output_publication };
                result.literals += 1;
            },
            .view => {
                if (borrowed) return .{ .compatibility = .alias };
                if (node.num_inputs != 1 or id >= selected.plane_counts.len or selected.plane_counts[id] != 1) return .{ .compatibility = .alias };
                var exact_alias = false;
                for (pool.metadata_aliases, 0..) |alias, alias_index| if (alias.node == id and alias.source == node.inputs[0] and selected.alias_enabled[alias_index]) {
                    exact_alias = true;
                    break;
                };
                if (!exact_alias) return .{ .compatibility = .alias };
                const source = slot.source_slot orelse return .{ .compatibility = .alias };
                if (source >= pool.plan.slots.len or pool.plan.slots[source].node_id != node.inputs[0]) return .{ .compatibility = .alias };
                const plane = selected.planes[id][0];
                const readable = @import("metadata_alias.zig").readableBytes(slot.desc.shape, plane.descriptor.strides) orelse return .{ .compatibility = .alias };
                if (plane.role != .primary or plane.descriptor.current_backend != .native or plane.descriptor.storage != .metadata_view or plane.descriptor.quant_format != null or plane.descriptor.attention_storage != null or plane.descriptor.attention_kv_format != null or !plane.descriptor.shape.eq(slot.desc.shape) or plane.descriptor.view_source != node.inputs[0] or plane.logical_bytes != bytes or plane.readable_bytes != readable) return .{ .compatibility = .alias };
                if (slot.roles.graph_output) {
                    var root = slot;
                    var depth: usize = 0;
                    while (root.kind == .view) : (depth += 1) {
                        const parent = root.source_slot orelse return .{ .compatibility = .alias };
                        if (parent >= pool.plan.slots.len or depth >= pool.plan.slots.len) return .{ .compatibility = .alias };
                        root = pool.plan.slots[parent];
                    }
                    if (root.kind == .runtime_input) return .{ .compatibility = .output_publication };
                    if (root.kind == .constant and !publication(pool, slot, literalIndex(pool, root) orelse return .{ .compatibility = .literal })) return .{ .compatibility = .output_publication };
                }
                result.aliases += 1;
            },
            .allocation => {
                if (id >= selected.routes.len or id >= pool.kernels.len) return .{ .compatibility = .operation };
                const owned = selected.routes[id] orelse return .{ .compatibility = .operation };
                const kernel = pool.kernels[id] orelse return .{ .compatibility = .operation };
                if (borrowed and owned.validation_scope != .prepared_program) return .{ .compatibility = .operation };
                if (kernel.backend_index >= pool.backends.len) return .{ .compatibility = .backend_region };
                const backend = pool.backends[kernel.backend_index];
                if (!canonical(backend) or owned.implementation.context != backend.ptr or owned.implementation.table != @as(*const anyopaque, @ptrCast(backend.vtable)) or kernel.output_allocation != slot.allocation or !kernel.descriptor.output_shape.eq(slot.desc.shape) or owned.requirements.primary_bytes < bytes or kernel.descriptor.input_count != node.num_inputs) return .{ .compatibility = .operation };
                for (kernel.inputs[0..kernel.descriptor.input_count], node.getInputs(), 0..) |actual, expected, index| {
                    if (actual != expected or expected >= reachable.len or !reachable[expected] or !kernel.descriptor.input_shapes[index].eq(graph.node(expected).output_shape)) return .{ .compatibility = .operation };
                }
                if (!resources(pool, kernel, owned)) return .{ .compatibility = .workspace };
                result.operations += 1;
            },
        }
    }
    for (graph.outputs.items) |id| {
        if (id >= reachable.len or !reachable[id]) return .{ .compatibility = .output_publication };
        const slot = pool.plan.slotForNode(id) orelse return .{ .compatibility = .output_publication };
        if (!slot.roles.graph_output) return .{ .compatibility = .output_publication };
    }
    return .{ .complete = result };
}

test "Native owned coverage actual packed primary requires exact format storage shape and compressed row bytes" {
    const a = std.testing.allocator;
    _ = a;
    const contracts = @import("../graph/backend_contracts.zig");
    const shape = ml.graph.Shape.init(.f32, &.{ 4, 512 });
    inline for (.{ @import("../graph/quant_matmul.zig").Format.q4_k, .q5_k, .q6_k, .q8_0 }) |format| {
        const geometry = @import("../graph/quant_matmul.zig").packedFormatDescriptor(format);
        const bytes = 4 * (512 / geometry.values_per_block) * geometry.bytes_per_block;
        var descriptor = contracts.TensorDesc.packedQuant(shape, format);
        descriptor.current_backend = .native;
        descriptor.resident_backend = .native;
        var plane = ownership.Plane{ .descriptor = descriptor, .role = .primary, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 0, .logical_bytes = bytes, .readable_bytes = bytes };
        try std.testing.expect(primaryValid(plane, shape, false));
        try std.testing.expect(!primaryValid(plane, shape, true));
        plane.logical_bytes -= 1;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.logical_bytes = bytes + 1;
        plane.readable_bytes = bytes + 1;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.logical_bytes = 4 * 512 * 4; // Never pad compressed metadata to an invented F32 extent.
        plane.readable_bytes = plane.logical_bytes;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.logical_bytes = bytes;
        plane.readable_bytes = bytes;
        plane.descriptor.storage = .host_f32;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.descriptor.storage = .host_packed_quant;
        plane.descriptor.quant_format = if (format == .q4_k) .q5_k else .q4_k;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.descriptor.quant_format = .q8_1;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.descriptor.quant_format = format;
        plane.descriptor.shape = ml.graph.Shape.init(.f32, &.{ 4, 511 });
        try std.testing.expect(!primaryValid(plane, plane.descriptor.shape, false));
        plane.descriptor.shape = shape;
        plane.descriptor.view_source = 3;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.descriptor.view_source = null;
        plane.descriptor.strides.values[0] += 1;
        try std.testing.expect(!primaryValid(plane, shape, false));
        plane.descriptor.strides = contracts.TensorStrides.dense(shape);
        plane.descriptor.resident_backend = .metal;
        try std.testing.expect(!primaryValid(plane, shape, false));
    }
}
test "Native owned coverage dense primary retains original exact extent dtype and shape checks" {
    const contracts = @import("../graph/backend_contracts.zig");
    const shape = ml.graph.Shape.init(.f32, &.{ 4, 512 });
    var descriptor = contracts.TensorDesc.init(shape, .host_f32);
    descriptor.current_backend = .native;
    var plane = ownership.Plane{ .descriptor = descriptor, .role = .primary, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 0, .logical_bytes = 4 * 512 * 4, .readable_bytes = 4 * 512 * 4 };
    try std.testing.expect(primaryValid(plane, shape, true));
    plane.logical_bytes -= 4;
    try std.testing.expect(!primaryValid(plane, shape, true));
    plane.logical_bytes += 4;
    plane.descriptor.shape.dtype = .bf16;
    plane.logical_bytes /= 2;
    plane.readable_bytes /= 2;
    try std.testing.expect(!primaryValid(plane, shape, true));
    try std.testing.expect(primaryValid(plane, shape, false));
    plane.descriptor.shape = ml.graph.Shape.init(.bf16, &.{ 8, 256 });
    try std.testing.expect(!primaryValid(plane, shape, false));
}
