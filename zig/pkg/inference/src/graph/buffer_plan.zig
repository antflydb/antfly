// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const partition_mod = @import("partition.zig");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const BackendKind = contracts.BackendKind;
const TensorDesc = contracts.TensorDesc;
const TensorStorageClass = contracts.TensorStorageClass;
const PartitionPlan = partition_mod.PartitionPlan;

pub fn transferKey(node: NodeId, partition: u32) u64 {
    return (@as(u64, node) << 32) | partition;
}

pub const SlotId = u32;
pub const AllocationId = u32;
pub const invalid_slot: SlotId = std.math.maxInt(SlotId);
pub const invalid_allocation: AllocationId = std.math.maxInt(AllocationId);

pub const SlotKind = enum {
    allocation,
    view,
    runtime_input,
    constant,
};

pub const SlotRoles = packed struct {
    runtime_input: bool = false,
    constant: bool = false,
    partition_input: bool = false,
    partition_output: bool = false,
    graph_output: bool = false,
    transfer_source: bool = false,
    transfer_target: bool = false,
    view: bool = false,
};

pub const LogicalSlot = struct {
    id: SlotId,
    node_id: NodeId,
    partition_index: u32,
    backend: BackendKind,
    kind: SlotKind,
    storage: TensorStorageClass,
    desc: TensorDesc,
    first_use: u32,
    last_use: u32,
    source_slot: ?SlotId = null,
    allocation: AllocationId = invalid_allocation,
    reusable: bool = false,
    roles: SlotRoles = .{},
};

pub const PartitionSlotRoles = packed struct {
    local: bool = false,
    input: bool = false,
    output: bool = false,
    graph_output: bool = false,
    transfer_source: bool = false,
    transfer_target: bool = false,
};

pub const PartitionSlotView = struct {
    slot: LogicalSlot,
    roles: PartitionSlotRoles,
};

pub const AllocationKind = enum {
    input_binding,
    constant_binding,
    tensor,
    transfer,
    workspace,
    auxiliary,
};

pub const PhysicalAllocation = struct {
    id: AllocationId,
    kind: AllocationKind,
    backend: BackendKind,
    storage: TensorStorageClass,
    byte_size: u64,
    first_use: u32,
    last_use: u32,
    reusable: bool,
    slot_count: u32 = 0,
};

pub const TransferEdge = struct {
    source_node: NodeId,
    source_slot: SlotId,
    allocation: AllocationId,
    source_partition: u32,
    target_partition: u32,
    source_backend: BackendKind,
    target_backend: BackendKind,
    source_storage: TensorStorageClass,
    target_storage: TensorStorageClass,
    byte_size: u64,
};

pub const PartitionBufferView = struct {
    partition_index: u32,
    backend: BackendKind,
    slots: []const PartitionSlotView,
    transfers_in: []const TransferEdge,
    transfers_out: []const TransferEdge,

    pub fn deinit(self: *PartitionBufferView, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.transfers_in);
        allocator.free(self.transfers_out);
    }
};

pub const BuildOptions = struct {
    tensor_descs: ?[]const ?TensorDesc = null,
    output_nodes: ?[]const NodeId = null,
};

pub const BufferPlan = struct {
    slots: []LogicalSlot,
    node_slots: []SlotId,
    /// Submission order can differ from graph IDs when partitions group
    /// independent branches. All physical lifetimes use these positions.
    node_positions: []u32,
    allocations: []PhysicalAllocation,
    transfers: []TransferEdge,
    transfer_index: std.AutoHashMapUnmanaged(u64, usize),
    allocator: std.mem.Allocator,

    pub fn deinit(self: *BufferPlan) void {
        self.allocator.free(self.slots);
        self.allocator.free(self.node_slots);
        self.allocator.free(self.node_positions);
        self.allocator.free(self.allocations);
        self.allocator.free(self.transfers);
        self.transfer_index.deinit(self.allocator);
    }

    /// One scratch region per backend/frame is sufficient for sequential
    /// command submission. Its lifetime spans the executable's entire graph.
    pub fn addWorkspace(self: *BufferPlan, backend: BackendKind, bytes: usize, alignment: u32) !AllocationId {
        if (bytes == 0 or bytes % @sizeOf(f32) != 0) return error.InvalidWorkspaceSize;
        if (alignment == 0 or !std.math.isPowerOfTwo(alignment) or alignment > @alignOf(f32)) return error.UnsupportedPhysicalWorkspaceAlignment;
        if (self.allocations.len >= invalid_allocation) return error.TooManyPhysicalAllocations;
        const storage: TensorStorageClass = switch (backend) {
            .native => .host_f32,
            .cuda => .cuda_buffer,
            else => return error.UnsupportedWorkspaceBackend,
        };
        const id: AllocationId = @intCast(self.allocations.len);
        self.allocations = try self.allocator.realloc(self.allocations, self.allocations.len + 1);
        self.allocations[id] = .{ .id = id, .kind = .workspace, .backend = backend, .storage = storage, .byte_size = bytes, .first_use = 0, .last_use = @intCast(self.node_slots.len), .reusable = true };
        return id;
    }

    /// A stable mutable input bank for captured submission. External inputs
    /// remain external logical slots; this allocation records the explicit
    /// device copy which binds their current values to the captured addresses.
    pub fn addInputBinding(self: *BufferPlan, backend: BackendKind, bytes: usize) !AllocationId {
        const id = try self.addWorkspace(backend, bytes, @alignOf(f32));
        self.allocations[id].kind = .input_binding;
        self.allocations[id].reusable = false;
        return id;
    }

    pub fn addConstantBinding(self: *BufferPlan, backend: BackendKind, bytes: usize) !AllocationId {
        const id = try self.addInputBinding(backend, bytes);
        self.allocations[id].kind = .constant_binding;
        return id;
    }

    /// One representation plane per primary allocation and role. Since the
    /// primary's aliases/outputs already determine reuse, the plane follows
    /// that same lifetime and can be reused across all nodes sharing it.
    pub fn addAuxiliaryPlane(self: *BufferPlan, primary_id: AllocationId, bytes: usize) !AllocationId {
        if (primary_id >= self.allocations.len or bytes == 0) return error.InvalidBufferPlan;
        const primary = self.allocations[primary_id];
        if (primary.kind != .tensor) return error.InvalidBufferPlan;
        const rounded = try std.math.add(usize, bytes, @sizeOf(f32) - 1);
        if (self.allocations.len >= invalid_allocation) return error.TooManyPhysicalAllocations;
        const id: AllocationId = @intCast(self.allocations.len);
        self.allocations = try self.allocator.realloc(self.allocations, self.allocations.len + 1);
        self.allocations[id] = primary;
        self.allocations[id].id = id;
        self.allocations[id].kind = .auxiliary;
        self.allocations[id].byte_size = rounded / @sizeOf(f32) * @sizeOf(f32);
        return id;
    }

    fn viewAllocation(self: *const BufferPlan, slot: LogicalSlot) !AllocationId {
        const source = self.slots[slot.source_slot orelse return error.InvalidBufferPlan];
        if (source.backend == slot.backend) return source.allocation;
        const index = self.transfer_index.get(transferKey(source.node_id, slot.partition_index)) orelse return error.MissingViewTransfer;
        return self.transfers[index].allocation;
    }

    pub fn positionForNode(self: *const BufferPlan, node: NodeId) u32 {
        return self.node_positions[node];
    }

    pub fn slotForNode(self: *const BufferPlan, node_id: NodeId) ?*const LogicalSlot {
        const index: usize = @intCast(node_id);
        if (index >= self.node_slots.len) return null;
        const slot_id = self.node_slots[index];
        if (slot_id == invalid_slot) return null;
        return &self.slots[@intCast(slot_id)];
    }

    pub fn allocationForSlot(self: *const BufferPlan, slot_id: SlotId) ?*const PhysicalAllocation {
        const slot_index: usize = @intCast(slot_id);
        if (slot_index >= self.slots.len) return null;
        const allocation_id = self.slots[slot_index].allocation;
        if (allocation_id == invalid_allocation) return null;
        return &self.allocations[@intCast(allocation_id)];
    }

    pub fn totalAllocationBytes(self: *const BufferPlan, backend: ?BackendKind, storage: ?TensorStorageClass) u64 {
        var total: u64 = 0;
        for (self.allocations) |allocation| {
            if (backend) |b| {
                if (allocation.backend != b) continue;
            }
            if (storage) |s| {
                if (allocation.storage != s) continue;
            }
            total = checkedAdd(total, allocation.byte_size);
        }
        return total;
    }

    pub fn partitionView(
        self: *const BufferPlan,
        allocator: std.mem.Allocator,
        partition_plan: *const PartitionPlan,
        partition_index: u32,
    ) !PartitionBufferView {
        const part_index: usize = @intCast(partition_index);
        if (part_index >= partition_plan.partitions.len) return error.InvalidPartitionPlan;
        const part = partition_plan.partitions[part_index];

        if (partition_plan.partitions.len == 1 and partition_index == 0 and self.transfers.len == 0) {
            const slots = try allocator.alloc(PartitionSlotView, self.slots.len);
            errdefer allocator.free(slots);
            for (self.slots, slots) |slot, *slot_view| {
                const graph_output = slot.roles.graph_output;
                slot_view.* = .{
                    .slot = slot,
                    .roles = .{
                        .local = true,
                        .output = graph_output,
                        .graph_output = graph_output,
                    },
                };
            }
            return .{
                .partition_index = partition_index,
                .backend = part.backend,
                .slots = slots,
                .transfers_in = &.{},
                .transfers_out = &.{},
            };
        }

        var slots = std.ArrayListUnmanaged(PartitionSlotView).empty;
        errdefer slots.deinit(allocator);
        try slots.ensureTotalCapacity(allocator, self.slots.len);
        const slot_roles = try allocator.alloc(PartitionSlotRoles, self.slots.len);
        defer allocator.free(slot_roles);
        for (self.slots, slot_roles) |slot, *roles| {
            const graph_output = slot.roles.graph_output and slot.partition_index == partition_index;
            roles.* = .{
                .local = slot.partition_index == partition_index,
                .output = graph_output,
                .graph_output = graph_output,
            };
        }
        for (self.transfers) |transfer| {
            const source_slot_index: usize = @intCast(transfer.source_slot);
            if (source_slot_index >= slot_roles.len) return error.InvalidBufferPlan;
            const roles = &slot_roles[source_slot_index];
            if (transfer.source_partition == partition_index) {
                roles.output = true;
                roles.transfer_source = true;
            }
            if (transfer.target_partition == partition_index) {
                roles.input = true;
                roles.transfer_target = true;
            }
        }
        for (self.slots, slot_roles) |slot, roles| {
            if (roles.local or roles.input or roles.output or roles.graph_output)
                try slots.append(allocator, .{ .slot = slot, .roles = roles });
        }

        var transfers_in = std.ArrayListUnmanaged(TransferEdge).empty;
        errdefer transfers_in.deinit(allocator);
        var transfers_out = std.ArrayListUnmanaged(TransferEdge).empty;
        errdefer transfers_out.deinit(allocator);
        try transfers_in.ensureTotalCapacity(allocator, self.transfers.len);
        try transfers_out.ensureTotalCapacity(allocator, self.transfers.len);
        for (self.transfers) |transfer| {
            if (transfer.target_partition == partition_index) try transfers_in.append(allocator, transfer);
            if (transfer.source_partition == partition_index) try transfers_out.append(allocator, transfer);
        }

        return .{
            .partition_index = partition_index,
            .backend = part.backend,
            .slots = try slots.toOwnedSlice(allocator),
            .transfers_in = try transfers_in.toOwnedSlice(allocator),
            .transfers_out = try transfers_out.toOwnedSlice(allocator),
        };
    }

    pub fn validate(self: *const BufferPlan, graph: *const Graph, partition_plan: *const PartitionPlan) !void {
        const node_count: usize = @intCast(graph.nodeCount());
        if (self.node_slots.len != node_count) return error.InvalidBufferPlan;
        if (self.slots.len != node_count) return error.InvalidBufferPlan;

        for (self.node_slots, 0..) |slot_id, node_index| {
            if (slot_id == invalid_slot) return error.InvalidBufferPlan;
            const slot_index: usize = @intCast(slot_id);
            if (slot_index >= self.slots.len) return error.InvalidBufferPlan;
            const slot = self.slots[slot_index];
            if (slot.node_id != @as(NodeId, @intCast(node_index))) return error.InvalidBufferPlan;
            if (slot.partition_index >= partition_plan.partitions.len) return error.InvalidPartitionPlan;
            if (!slot.desc.shape.eq(graph.node(slot.node_id).output_shape)) return error.TensorDescriptorShapeMismatch;

            if (slot.kind == .view) {
                const source_slot_id = slot.source_slot orelse return error.InvalidBufferPlan;
                const source_index: usize = @intCast(source_slot_id);
                if (source_index >= self.slots.len) return error.InvalidBufferPlan;
                if (slot.allocation != try self.viewAllocation(slot)) return error.InvalidBufferPlan;
            } else if (slot.kind == .allocation) {
                const allocation = self.allocationForSlot(slot.id) orelse return error.InvalidBufferPlan;
                if (allocation.kind != .tensor) return error.InvalidBufferPlan;
                if (allocation.backend != slot.backend or allocation.storage != slot.storage) return error.InvalidBufferPlan;
                if (allocation.byte_size < tensorByteSize(slot.desc.shape)) return error.InvalidBufferPlan;
                if (!allocation.reusable and allocation.slot_count != 1) return error.InvalidBufferPlan;
            } else if (slot.allocation != invalid_allocation) {
                return error.InvalidBufferPlan;
            }

            if (slot.roles.graph_output and slot.last_use < node_count) return error.InvalidBufferPlan;
        }

        for (self.allocations, 0..) |allocation, i| {
            if (allocation.id != @as(AllocationId, @intCast(i))) return error.InvalidBufferPlan;
            if (allocation.byte_size == 0) return error.InvalidBufferPlan;
            if (allocation.first_use > allocation.last_use) return error.InvalidBufferPlan;
        }

        for (self.transfers) |transfer| {
            if (transfer.source_partition >= partition_plan.partitions.len or
                transfer.target_partition >= partition_plan.partitions.len) return error.InvalidPartitionPlan;
            if (transfer.source_slot >= self.slots.len) return error.InvalidBufferPlan;
            if (transfer.allocation >= self.allocations.len) return error.InvalidBufferPlan;
            const source_slot = self.slots[@intCast(transfer.source_slot)];
            if (source_slot.node_id != transfer.source_node) return error.InvalidBufferPlan;
            if (source_slot.partition_index != transfer.source_partition) return error.InvalidBufferPlan;
            const allocation = self.allocations[@intCast(transfer.allocation)];
            if (allocation.kind != .transfer) return error.InvalidBufferPlan;
            if (allocation.backend != transfer.target_backend or allocation.storage != transfer.target_storage) return error.InvalidBufferPlan;
            if (allocation.byte_size < transfer.byte_size) return error.InvalidBufferPlan;
            if (partition_plan.partitions[transfer.source_partition].backend != transfer.source_backend) return error.InvalidBufferPlan;
            if (partition_plan.partitions[transfer.target_partition].backend != transfer.target_backend) return error.InvalidBufferPlan;
        }
    }
};

pub fn build(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    partition_plan: *const PartitionPlan,
    options: BuildOptions,
) !BufferPlan {
    const node_count: usize = @intCast(graph.nodeCount());
    if (partition_plan.node_assignment.len != node_count) return error.InvalidPartitionPlan;
    if (options.tensor_descs) |descs| {
        if (descs.len != node_count) return error.InvalidTensorDescriptorTable;
    }

    const owned_descs = if (options.tensor_descs == null)
        try partition_mod.buildTensorDescriptors(allocator, graph, null)
    else
        null;
    defer if (owned_descs) |descs| allocator.free(descs);
    const tensor_descs = options.tensor_descs orelse owned_descs.?;
    const output_nodes = options.output_nodes orelse graph.outputs.items;

    const positions = try allocator.alloc(u32, node_count);
    errdefer allocator.free(positions);
    @memset(positions, std.math.maxInt(u32));
    var ordinal: u32 = 0;
    for (partition_plan.partitions, 0..) |part, partition_index| {
        for (part.node_ids) |id| {
            if (id >= node_count or positions[id] != std.math.maxInt(u32) or partition_plan.node_assignment[id] != partition_index) return error.InvalidPartitionPlan;
            positions[id] = ordinal;
            ordinal += 1;
        }
    }
    if (ordinal != node_count) return error.InvalidPartitionPlan;
    const last_use = try computeLastUse(allocator, graph, output_nodes, positions);
    defer allocator.free(last_use);

    const is_output = try markOutputs(allocator, node_count, output_nodes);
    defer allocator.free(is_output);

    var slots = try allocator.alloc(LogicalSlot, node_count);
    errdefer allocator.free(slots);
    var node_slots = try allocator.alloc(SlotId, node_count);
    errdefer allocator.free(node_slots);
    @memset(node_slots, invalid_slot);

    for (0..node_count) |i| {
        const node_id: NodeId = @intCast(i);
        const partition_index = partition_plan.node_assignment[i];
        if (partition_index >= partition_plan.partitions.len) return error.InvalidPartitionPlan;
        const part = partition_plan.partitions[partition_index];
        var desc = tensor_descs[i] orelse TensorDesc.init(
            graph.node(node_id).output_shape,
            partition_mod.tensorStorageForBackend(part.backend),
        );
        if (!desc.shape.eq(graph.node(node_id).output_shape)) return error.TensorDescriptorShapeMismatch;
        // These CUDA kernels materialize dense buffers. Resolve this before
        // assigning allocation IDs so downstream views retain the right source.
        if (part.backend == .cuda) {
            const node = graph.node(node_id);
            switch (node.op) {
                .transpose, .broadcast_in_dim => {
                    desc = TensorDesc.init(node.output_shape, .cuda_buffer);
                    desc.resident_backend = .cuda;
                },
                .reshape => desc = TensorDesc.view(node.output_shape, node.inputs[0], contracts.TensorStrides.dense(node.output_shape), .cuda, 0),
                else => {},
            }
        }

        const slot_id: SlotId = @intCast(i);
        node_slots[i] = slot_id;
        const source_slot = sourceSlotForDesc(desc, node_slots);
        const kind = slotKindForNode(graph, node_id, desc);
        slots[i] = .{
            .id = slot_id,
            .node_id = node_id,
            .partition_index = partition_index,
            .backend = part.backend,
            .kind = kind,
            .storage = materializedStorage(desc, part.backend),
            .desc = desc,
            .first_use = positions[i],
            .last_use = last_use[i],
            .source_slot = source_slot,
            .reusable = kind == .allocation and !is_output[i] and desc.storage != .runtime_input and desc.storage != .constant,
            .roles = slotRolesForNode(graph, node_id, kind, is_output[i]),
        };
    }

    // A metadata view keeps its source allocation live through every view
    // consumer and published output. Propagate before assigning/reusing IDs;
    // retaining a tensor wrapper alone cannot prevent an overlapping write.
    var view_index = slots.len;
    while (view_index != 0) {
        view_index -= 1;
        const view = slots[view_index];
        if (view.kind != .view) continue;
        const source = view.source_slot orelse continue;
        if (source >= view_index) return error.InvalidViewSource;
        slots[source].last_use = @max(slots[source].last_use, view.last_use);
    }

    var allocations = std.ArrayListUnmanaged(PhysicalAllocation).empty;
    errdefer allocations.deinit(allocator);
    for (partition_plan.partitions) |part| {
        for (part.node_ids) |node| try assignSlotAllocation(allocator, &allocations, &slots[node], slots);
    }

    var transfers = std.ArrayListUnmanaged(TransferEdge).empty;
    errdefer transfers.deinit(allocator);
    var transfer_index = std.AutoHashMapUnmanaged(u64, usize).empty;
    errdefer transfer_index.deinit(allocator);

    for (partition_plan.partitions, 0..) |part, target_partition| {
        for (part.external_inputs) |ext| {
            const source_index: usize = @intCast(ext.node_id);
            if (source_index >= node_count) return error.InvalidPartitionPlan;
            if (ext.source_partition >= partition_plan.partitions.len) return error.InvalidPartitionPlan;
            const source_part = partition_plan.partitions[ext.source_partition];
            const source_slot = node_slots[source_index];
            if (source_slot == invalid_slot) return error.InvalidPartitionPlan;
            slots[@intCast(source_slot)].roles.partition_output = true;
            slots[@intCast(source_slot)].roles.transfer_source = true;
            const target_storage = partition_mod.tensorStorageForBackend(part.backend);
            const byte_size = tensorByteSize(slots[@intCast(source_slot)].desc.shape);
            const transfer_allocation = try appendAllocation(allocator, &allocations, .{
                .kind = .transfer,
                .backend = part.backend,
                .storage = target_storage,
                .byte_size = byte_size,
                .first_use = if (part.node_ids.len != 0) positions[part.node_ids[0]] else 0,
                .last_use = if (part.node_ids.len != 0) positions[part.node_ids[part.node_ids.len - 1]] else 0,
                .reusable = false,
            });
            try transfer_index.put(allocator, transferKey(ext.node_id, @intCast(target_partition)), transfers.items.len);
            try transfers.append(allocator, .{
                .source_node = ext.node_id,
                .source_slot = source_slot,
                .allocation = transfer_allocation,
                .source_partition = ext.source_partition,
                .target_partition = @intCast(target_partition),
                .source_backend = source_part.backend,
                .target_backend = part.backend,
                .source_storage = slots[@intCast(source_slot)].storage,
                .target_storage = target_storage,
                .byte_size = byte_size,
            });
        }
    }

    // A view across a backend boundary aliases the transferred allocation,
    // not the source device address. Resolve chains after transfer IDs exist.
    for (slots) |*slot| {
        if (slot.kind != .view) continue;
        const source = slots[slot.source_slot orelse return error.InvalidViewSource];
        slot.allocation = if (source.backend == slot.backend) source.allocation else blk: {
            const index = transfer_index.get(transferKey(source.node_id, slot.partition_index)) orelse return error.MissingViewTransfer;
            break :blk transfers.items[index].allocation;
        };
        if (slot.allocation != invalid_allocation) allocations.items[slot.allocation].last_use = @max(allocations.items[slot.allocation].last_use, slot.last_use);
    }

    const owned_allocations = try allocations.toOwnedSlice(allocator);
    errdefer allocator.free(owned_allocations);
    const owned_transfers = try transfers.toOwnedSlice(allocator);
    errdefer allocator.free(owned_transfers);
    return .{
        .slots = slots,
        .node_slots = node_slots,
        .node_positions = positions,
        .allocations = owned_allocations,
        .transfers = owned_transfers,
        .transfer_index = transfer_index,
        .allocator = allocator,
    };
}

fn assignSlotAllocation(
    allocator: std.mem.Allocator,
    allocations: *std.ArrayListUnmanaged(PhysicalAllocation),
    slot: *LogicalSlot,
    slots: []const LogicalSlot,
) !void {
    if (slot.kind == .view) {
        if (slot.source_slot) |source_slot_id| {
            const source_index: usize = @intCast(source_slot_id);
            if (source_index < slots.len) slot.allocation = slots[source_index].allocation;
        }
        return;
    }
    if (slot.kind != .allocation) return;

    const byte_size = tensorByteSize(slot.desc.shape);
    if (slot.reusable) {
        for (allocations.items) |*allocation| {
            if (!allocation.reusable) continue;
            if (allocation.kind != .tensor) continue;
            if (allocation.backend != slot.backend or allocation.storage != slot.storage) continue;
            if (allocation.byte_size < byte_size) continue;
            if (lifetimesOverlap(allocation.first_use, allocation.last_use, slot.first_use, slot.last_use)) continue;
            allocation.first_use = @min(allocation.first_use, slot.first_use);
            allocation.last_use = @max(allocation.last_use, slot.last_use);
            allocation.slot_count += 1;
            slot.allocation = allocation.id;
            return;
        }
    }

    slot.allocation = try appendAllocation(allocator, allocations, .{
        .kind = .tensor,
        .backend = slot.backend,
        .storage = slot.storage,
        .byte_size = byte_size,
        .first_use = slot.first_use,
        .last_use = slot.last_use,
        .reusable = slot.reusable,
    });
}

const AllocationInit = struct {
    kind: AllocationKind,
    backend: BackendKind,
    storage: TensorStorageClass,
    byte_size: u64,
    first_use: u32,
    last_use: u32,
    reusable: bool,
};

fn appendAllocation(
    allocator: std.mem.Allocator,
    allocations: *std.ArrayListUnmanaged(PhysicalAllocation),
    init: AllocationInit,
) !AllocationId {
    const id: AllocationId = @intCast(allocations.items.len);
    try allocations.append(allocator, .{
        .id = id,
        .kind = init.kind,
        .backend = init.backend,
        .storage = init.storage,
        .byte_size = init.byte_size,
        .first_use = init.first_use,
        .last_use = init.last_use,
        .reusable = init.reusable,
        .slot_count = 1,
    });
    return id;
}

fn lifetimesOverlap(a_first: u32, a_last: u32, b_first: u32, b_last: u32) bool {
    return a_first <= b_last and b_first <= a_last;
}

fn computeLastUse(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    output_nodes: []const NodeId,
    positions: []const u32,
) ![]u32 {
    const node_count: usize = @intCast(graph.nodeCount());
    const last_use = try allocator.alloc(u32, node_count);
    errdefer allocator.free(last_use);
    @memcpy(last_use, positions);

    for (0..node_count) |i| {
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        for (n.getInputs()) |input_id| {
            if (input_id == null_node) continue;
            const input_index: usize = @intCast(input_id);
            if (input_index >= node_count) return error.InvalidGraphInput;
            if (positions[input_index] >= positions[i]) return error.InvalidPartitionSchedule;
            last_use[input_index] = @max(last_use[input_index], positions[i]);
        }
    }

    const output_last_use: u32 = @intCast(node_count);
    for (output_nodes) |node_id| {
        const index: usize = @intCast(node_id);
        if (index >= node_count) return error.InvalidGraphOutput;
        last_use[index] = @max(last_use[index], output_last_use);
    }
    return last_use;
}

fn markOutputs(
    allocator: std.mem.Allocator,
    node_count: usize,
    output_nodes: []const NodeId,
) ![]bool {
    const is_output = try allocator.alloc(bool, node_count);
    errdefer allocator.free(is_output);
    @memset(is_output, false);
    for (output_nodes) |node_id| {
        const index: usize = @intCast(node_id);
        if (index >= node_count) return error.InvalidGraphOutput;
        is_output[index] = true;
    }
    return is_output;
}

fn sourceSlotForDesc(desc: TensorDesc, node_slots: []const SlotId) ?SlotId {
    const source = desc.view_source orelse return null;
    const index: usize = @intCast(source);
    if (index >= node_slots.len) return null;
    const slot_id = node_slots[index];
    return if (slot_id == invalid_slot) null else slot_id;
}

fn slotKindForNode(graph: *const Graph, node_id: NodeId, desc: TensorDesc) SlotKind {
    if (desc.isView()) return .view;
    return switch (graph.node(node_id).op) {
        .parameter => .runtime_input,
        .constant => .constant,
        else => .allocation,
    };
}

fn slotRolesForNode(graph: *const Graph, node_id: NodeId, kind: SlotKind, graph_output: bool) SlotRoles {
    var roles = SlotRoles{ .graph_output = graph_output };
    switch (kind) {
        .runtime_input => {
            roles.runtime_input = true;
            roles.partition_input = true;
        },
        .constant => {
            roles.constant = true;
            roles.partition_input = true;
        },
        .view => roles.view = true,
        .allocation => {},
    }
    if (graph_output) roles.partition_output = true;
    _ = graph;
    _ = node_id;
    return roles;
}

fn materializedStorage(desc: TensorDesc, backend: BackendKind) TensorStorageClass {
    if (desc.storage != .unknown) return desc.storage;
    return partition_mod.tensorStorageForBackend(backend);
}

fn tensorByteSize(shape: ml.graph.Shape) u64 {
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return @intCast(shape.dtype.byteSize());
    return checkedMul(@intCast(elems), @intCast(shape.dtype.byteSize()));
}

fn checkedMul(a: u64, b: u64) u64 {
    return std.math.mul(u64, a, b) catch std.math.maxInt(u64);
}

fn checkedAdd(a: u64, b: u64) u64 {
    return std.math.add(u64, a, b) catch std.math.maxInt(u64);
}

test "buffer plan records liveness slots and transfer edges" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const activated = try b.gelu(x);
    const out = try b.neg(activated);
    try g.markOutput(out);

    const onlyGelu = struct {
        fn f(op: ml.graph.OpCode) bool {
            return op == .fused_gelu;
        }
    }.f;
    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &onlyGelu },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();

    var plan = try build(allocator, &g, &partition_plan, .{});
    defer plan.deinit();
    try plan.validate(&g, &partition_plan);

    try std.testing.expectEqual(@as(usize, @intCast(g.nodeCount())), plan.slots.len);
    try std.testing.expectEqual(@as(SlotKind, .runtime_input), plan.slotForNode(x).?.kind);
    try std.testing.expectEqual(@as(u32, @intCast(g.nodeCount())), plan.slotForNode(out).?.last_use);
    try std.testing.expect(plan.allocationForSlot(plan.slotForNode(out).?.id) != null);
    try std.testing.expect(plan.transfers.len >= 1);

    var saw_gelu_to_native = false;
    for (plan.transfers) |transfer| {
        if (transfer.source_node == activated and transfer.target_backend == .native) {
            saw_gelu_to_native = true;
            try std.testing.expectEqual(BackendKind.metal, transfer.source_backend);
            try std.testing.expectEqual(TensorStorageClass.host_dense, transfer.target_storage);
            try std.testing.expect(transfer.allocation != invalid_allocation);
            try std.testing.expectEqual(AllocationKind.transfer, plan.allocations[@intCast(transfer.allocation)].kind);
            try std.testing.expectEqual(@as(u64, 16), transfer.byte_size);
        }
    }
    try std.testing.expect(saw_gelu_to_native);
}

test "buffer plan exposes partition-local slot and transfer views" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const activated = try b.gelu(x);
    const out = try b.neg(activated);
    try g.markOutput(out);

    const onlyGelu = struct {
        fn f(op: ml.graph.OpCode) bool {
            return op == .fused_gelu;
        }
    }.f;
    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &onlyGelu },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();

    var plan = try build(allocator, &g, &partition_plan, .{});
    defer plan.deinit();
    try plan.validate(&g, &partition_plan);

    const target_partition = partition_plan.node_assignment[@intCast(out)];
    var view = try plan.partitionView(allocator, &partition_plan, target_partition);
    defer view.deinit(allocator);

    try std.testing.expectEqual(partition_plan.partitions[target_partition].backend, view.backend);
    try std.testing.expect(view.slots.len > 0);
    try std.testing.expect(view.transfers_in.len > 0);
    var saw_input = false;
    for (view.slots) |slot| {
        if (slot.slot.node_id == activated and slot.roles.input and slot.roles.transfer_target) saw_input = true;
    }
    try std.testing.expect(saw_input);
}

test "buffer plan represents views as metadata slots over source slots" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const out = try b.reshape(x, ml.graph.Shape.init(.f32, &.{ 3, 2 }));
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();

    const descs = try partition_mod.buildTensorDescriptors(allocator, &g, null);
    defer allocator.free(descs);
    var plan = try build(allocator, &g, &partition_plan, .{ .tensor_descs = descs });
    defer plan.deinit();
    try plan.validate(&g, &partition_plan);

    const source = plan.slotForNode(x).?;
    const view = plan.slotForNode(out).?;
    try std.testing.expectEqual(SlotKind.view, view.kind);
    try std.testing.expectEqual(source.id, view.source_slot.?);
    try std.testing.expectEqual(TensorStorageClass.metadata_view, view.storage);
    try std.testing.expectEqual(source.allocation, view.allocation);
}

test "buffer plan reuses non-overlapping tensor allocations" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const a = try b.gelu(x);
    const b_out = try b.neg(a);
    const c = try b.relu(x);
    const d_out = try b.neg(c);
    try g.markOutput(b_out);
    try g.markOutput(d_out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();

    var plan = try build(allocator, &g, &partition_plan, .{});
    defer plan.deinit();
    try plan.validate(&g, &partition_plan);

    const a_slot = plan.slotForNode(a).?;
    const c_slot = plan.slotForNode(c).?;
    try std.testing.expect(a_slot.reusable);
    try std.testing.expect(c_slot.reusable);
    try std.testing.expectEqual(a_slot.allocation, c_slot.allocation);
    const reused = plan.allocations[@intCast(a_slot.allocation)];
    try std.testing.expect(reused.slot_count >= 2);
    try std.testing.expectEqual(@as(u64, 16), reused.byte_size);
}

test "buffer plan reports total allocation bytes by backend and storage" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();

    var plan = try build(allocator, &g, &partition_plan, .{});
    defer plan.deinit();
    try plan.validate(&g, &partition_plan);

    try std.testing.expect(plan.totalAllocationBytes(.native, .host_dense) >= 16);
    try std.testing.expectEqual(plan.totalAllocationBytes(null, null), plan.totalAllocationBytes(.native, null));
}

test "buffer plan CUDA materialized layouts own storage and reshapes retain that allocation" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const transpose = try b.transpose(input, &.{ 1, 0 });
    const reshape = try b.reshape(transpose, ml.graph.Shape.init(.f32, &.{6}));
    const output_shape = ml.graph.Shape.init(.f32, &.{ 2, 6 });
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = output_shape, .num_axes = 1 };
    attrs.broadcast_axes[0] = 1;
    const broadcast = try graph.addNode(.{ .op = .{ .broadcast_in_dim = attrs }, .output_shape = output_shape, .inputs = .{ reshape, null_node, null_node, null_node }, .num_inputs = 1 });
    try graph.markOutput(reshape);
    try graph.markOutput(broadcast);
    const cuda = @import("cuda_capabilities.zig");
    const caps = [_]partition_mod.Capability{
        .{ .backend = .cuda, .priority = 10, .supports = &cuda.supports, .decide = &cuda.decide },
        .{ .backend = .native, .priority = 0, .supports = &partition_mod.supportsAll },
    };
    var placement = try partition_mod.partition(a, &graph, &caps);
    defer placement.deinit();
    var plan = try build(a, &graph, &placement, .{});
    defer plan.deinit();
    try plan.validate(&graph, &placement);
    const transposed = plan.slotForNode(transpose).?;
    const reshaped = plan.slotForNode(reshape).?;
    const broadcasted = plan.slotForNode(broadcast).?;
    try std.testing.expectEqual(BackendKind.cuda, transposed.backend);
    try std.testing.expectEqual(BackendKind.cuda, broadcasted.backend);
    try std.testing.expectEqual(SlotKind.allocation, transposed.kind);
    try std.testing.expectEqual(SlotKind.allocation, broadcasted.kind);
    try std.testing.expectEqual(SlotKind.view, reshaped.kind);
    try std.testing.expectEqual(transposed.id, reshaped.source_slot.?);
    try std.testing.expectEqual(transposed.allocation, reshaped.allocation);
    try std.testing.expect(transposed.allocation != broadcasted.allocation);
    try std.testing.expectEqual(@as(u32, graph.nodeCount()), transposed.last_use);
    try std.testing.expectEqual(@as(u64, 24), plan.allocations[transposed.allocation].byte_size);
    try std.testing.expectEqual(@as(u64, 48), plan.allocations[broadcasted.allocation].byte_size);
}

test "buffer plan reuse follows partition submission order across independent branches" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const left = try b.neg(x);
    const left_output = try b.neg(left);
    const right = try graph.addNode(.{ .op = .{ .exp = {} }, .output_shape = graph.node(x).output_shape, .inputs = .{ x, null_node, null_node, null_node }, .num_inputs = 1 });
    const right_output = try b.neg(right);
    try graph.markOutput(left_output);
    try graph.markOutput(right_output);
    var parts = [_]partition_mod.Partition{
        .{ .backend = .native, .node_ids = &.{ x, left }, .external_inputs = &.{} },
        .{ .backend = .native, .node_ids = &.{ right, right_output }, .external_inputs = &.{.{ .node_id = x, .source_partition = 0 }} },
        .{ .backend = .native, .node_ids = &.{left_output}, .external_inputs = &.{.{ .node_id = left, .source_partition = 0 }} },
    };
    const operator_plans: [5]?@import("operator_plan.zig").OperatorPlan = @splat(null);
    const placement = PartitionPlan{ .partitions = &parts, .node_assignment = &.{ 0, 0, 2, 1, 1 }, .node_operator_plans = &operator_plans, .allocator = a };
    var plan = try build(a, &graph, &placement, .{});
    defer plan.deinit();
    try plan.validate(&graph, &placement);
    const lhs = plan.slotForNode(left).?;
    const rhs = plan.slotForNode(right).?;
    try std.testing.expectEqual(@as(u32, 4), lhs.last_use);
    try std.testing.expectEqual(@as(u32, 2), rhs.first_use);
    try std.testing.expect(lhs.allocation != rhs.allocation);
    try std.testing.expectEqual(@as(u32, 4), plan.positionForNode(left_output));
}

test "buffer plan cross-backend reshape aliases its transfer allocation through publication" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const dense = try b.neg(x);
    const view = try b.reshape(dense, ml.graph.Shape.init(.f32, &.{6}));
    try graph.markOutput(view);
    var parts = [_]partition_mod.Partition{
        .{ .backend = .native, .node_ids = &.{ x, dense }, .external_inputs = &.{} },
        .{ .backend = .cuda, .node_ids = &.{view}, .external_inputs = &.{.{ .node_id = dense, .source_partition = 0 }} },
    };
    const operator_plans: [3]?@import("operator_plan.zig").OperatorPlan = @splat(null);
    const placement = PartitionPlan{ .partitions = &parts, .node_assignment = &.{ 0, 0, 1 }, .node_operator_plans = &operator_plans, .allocator = a };
    var plan = try build(a, &graph, &placement, .{});
    defer plan.deinit();
    try plan.validate(&graph, &placement);
    const source_slot = plan.slotForNode(dense).?;
    const view_slot = plan.slotForNode(view).?;
    try std.testing.expectEqual(SlotKind.view, view_slot.kind);
    const transfer = plan.transfers[plan.transfer_index.get(transferKey(dense, 1)).?];
    try std.testing.expectEqual(transfer.allocation, view_slot.allocation);
    try std.testing.expect(source_slot.allocation != view_slot.allocation);
    try std.testing.expectEqual(@as(u32, graph.nodeCount()), plan.allocations[view_slot.allocation].last_use);
}
