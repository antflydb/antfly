// Copyright 2026 Antfly, Inc.
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
    tensor,
    transfer,
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
    allocations: []PhysicalAllocation,
    transfers: []TransferEdge,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *BufferPlan) void {
        self.allocator.free(self.slots);
        self.allocator.free(self.node_slots);
        self.allocator.free(self.allocations);
        self.allocator.free(self.transfers);
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

        var slots = std.ArrayListUnmanaged(PartitionSlotView).empty;
        errdefer slots.deinit(allocator);
        for (self.slots) |slot| {
            const roles = partitionSlotRoles(self, slot, partition_index);
            if (roles.local or roles.input or roles.output or roles.graph_output)
                try slots.append(allocator, .{ .slot = slot, .roles = roles });
        }

        var transfers_in = std.ArrayListUnmanaged(TransferEdge).empty;
        errdefer transfers_in.deinit(allocator);
        var transfers_out = std.ArrayListUnmanaged(TransferEdge).empty;
        errdefer transfers_out.deinit(allocator);
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
                if (slot.allocation != self.slots[source_index].allocation) return error.InvalidBufferPlan;
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

    const last_use = try computeLastUse(allocator, graph, output_nodes);
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
        const desc = tensor_descs[i] orelse TensorDesc.init(
            graph.node(node_id).output_shape,
            partition_mod.tensorStorageForBackend(part.backend),
        );
        if (!desc.shape.eq(graph.node(node_id).output_shape)) return error.TensorDescriptorShapeMismatch;

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
            .first_use = @intCast(i),
            .last_use = last_use[i],
            .source_slot = source_slot,
            .reusable = kind == .allocation and !is_output[i] and desc.storage != .runtime_input and desc.storage != .constant,
            .roles = slotRolesForNode(graph, node_id, kind, is_output[i]),
        };
    }

    var allocations = std.ArrayListUnmanaged(PhysicalAllocation).empty;
    errdefer allocations.deinit(allocator);
    for (slots) |*slot| {
        try assignSlotAllocation(allocator, &allocations, slot, slots);
    }

    var transfers = std.ArrayListUnmanaged(TransferEdge).empty;
    errdefer transfers.deinit(allocator);
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
                .first_use = partitionFirstUse(part),
                .last_use = partitionLastUse(part),
                .reusable = false,
            });
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

    return .{
        .slots = slots,
        .node_slots = node_slots,
        .allocations = try allocations.toOwnedSlice(allocator),
        .transfers = try transfers.toOwnedSlice(allocator),
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

fn slotReferencedByPartition(
    plan: *const BufferPlan,
    slot_id: SlotId,
    partition_index: u32,
) bool {
    for (plan.transfers) |transfer| {
        if (transfer.source_slot == slot_id and
            (transfer.source_partition == partition_index or transfer.target_partition == partition_index))
        {
            return true;
        }
    }
    return false;
}

fn partitionSlotRoles(
    plan: *const BufferPlan,
    slot: LogicalSlot,
    partition_index: u32,
) PartitionSlotRoles {
    var roles = PartitionSlotRoles{
        .local = slot.partition_index == partition_index,
        .graph_output = slot.roles.graph_output and slot.partition_index == partition_index,
    };
    if (roles.graph_output) roles.output = true;
    for (plan.transfers) |transfer| {
        if (transfer.source_slot != slot.id) continue;
        if (transfer.source_partition == partition_index) {
            roles.output = true;
            roles.transfer_source = true;
        }
        if (transfer.target_partition == partition_index) {
            roles.input = true;
            roles.transfer_target = true;
        }
    }
    return roles;
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

fn partitionFirstUse(part: partition_mod.Partition) u32 {
    if (part.node_ids.len == 0) return 0;
    return @intCast(part.node_ids[0]);
}

fn partitionLastUse(part: partition_mod.Partition) u32 {
    if (part.node_ids.len == 0) return 0;
    return @intCast(part.node_ids[part.node_ids.len - 1]);
}

fn computeLastUse(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    output_nodes: []const NodeId,
) ![]u32 {
    const node_count: usize = @intCast(graph.nodeCount());
    const last_use = try allocator.alloc(u32, node_count);
    errdefer allocator.free(last_use);
    for (last_use, 0..) |*value, i| value.* = @intCast(i);

    for (0..node_count) |i| {
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        for (n.getInputs()) |input_id| {
            if (input_id == null_node) continue;
            const input_index: usize = @intCast(input_id);
            if (input_index >= node_count) return error.InvalidGraphInput;
            last_use[input_index] = @max(last_use[input_index], @as(u32, @intCast(i)));
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
