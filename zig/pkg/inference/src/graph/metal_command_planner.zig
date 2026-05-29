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
const quant_matmul = @import("quant_matmul.zig");
const operator_plan = @import("operator_plan.zig");

pub const QuantMatmulDispatchKind = quant_matmul.DispatchKind;
pub const QuantMatmulFormat = quant_matmul.Format;
pub const QuantMatmulPlan = operator_plan.QuantMatmulPlan;
pub const Operator = operator_plan.Operator;
pub const QuantRowOpPlan = operator_plan.QuantRowOpPlan;
pub const QuantCopyOpPlan = operator_plan.QuantCopyOpPlan;
pub const AttentionOpPlan = operator_plan.AttentionOpPlan;
pub const AttentionKvFormat = quant_matmul.AttentionKvFormat;
pub const OperatorPlan = operator_plan.OperatorPlan;
pub const OperatorPlanStats = operator_plan.Stats;

pub const Access = enum {
    read,
    write,
    read_write,

    fn writes(self: Access) bool {
        return self == .write or self == .read_write;
    }

    fn reads(self: Access) bool {
        return self == .read or self == .read_write;
    }
};

pub const ResourceKind = enum {
    buffer,
    scratch_slot,
    quant_slot,
    norm_slot,
    kv_cache,
};

pub const ResourceRange = struct {
    kind: ResourceKind = .buffer,
    id: usize,
    offset: usize = 0,
    length: usize = std.math.maxInt(usize),

    pub fn whole(kind: ResourceKind, id: usize) ResourceRange {
        return .{ .kind = kind, .id = id };
    }

    pub fn bytes(kind: ResourceKind, id: usize, offset: usize, length: usize) ResourceRange {
        return .{ .kind = kind, .id = id, .offset = offset, .length = length };
    }

    pub fn overlaps(self: ResourceRange, other: ResourceRange) bool {
        if (self.kind != other.kind or self.id != other.id) return false;
        if (self.length == 0 or other.length == 0) return false;
        const self_end = saturatingEnd(self.offset, self.length);
        const other_end = saturatingEnd(other.offset, other.length);
        return self.offset < other_end and other.offset < self_end;
    }

    fn saturatingEnd(offset: usize, length: usize) usize {
        return std.math.add(usize, offset, length) catch std.math.maxInt(usize);
    }
};

pub const ResourceUse = struct {
    range: ResourceRange,
    access: Access,
};

pub const OpKind = enum(u16) {
    unknown = 0,
    attention_pre_norm,
    qkv_linear,
    q_head_norm_rope,
    k_head_norm_rope,
    v_norm,
    kv_seed,
    attention,
    attention_output_linear,
    attention_post_norm_residual,
    ffn_pre_norm_scale,
    ffn_gate_up_activation,
    ffn_down_linear,
    ffn_post_norm_residual,
    ple_gate_activation,
    ple_projection,
    ple_post_norm_residual,
    tail_final_norm,
    tail_lm_head,
    tail_argmax,
    quant_get_rows,
    quant_set_rows,
    quant_copy_q_to_f32,
    quant_copy_f32_to_q,
    attention_flash,
    attention_paged,
    attention_quantized_kv,
};

pub const Op = struct {
    kind: OpKind = .unknown,
    source: usize = 0,
    region: usize = 0,
    resources: []const ResourceUse = &.{},
    quant_matmul: ?QuantMatmulPlan = null,
    operator_plan: ?OperatorPlan = null,
};

pub const ActivationDType = enum(u8) {
    f32 = 0,
    bf16 = 1,
    f16 = 2,

    pub fn byteSize(self: ActivationDType) usize {
        return switch (self) {
            .f32 => @sizeOf(f32),
            .bf16, .f16 => @sizeOf(u16),
        };
    }
};

pub const KvLayout = enum(u8) {
    f32_row_major = 0,
};

pub const FrameMode = enum(u8) {
    prefill,
    decode,
    embedding,
    classification,
};

pub const FrameDescriptor = struct {
    mode: FrameMode,
    rows: usize,
    hidden_size: usize = 0,
    vocab_size: usize = 0,
    sequence_length: usize = 0,
    query_length: usize = 0,
    activation_dtype: ActivationDType = .f32,
    kv_layout: KvLayout = .f32_row_major,
    attention_storage: quant_matmul.AttentionStorage = .paged,

    pub fn prefill(options: struct {
        rows: usize,
        hidden_size: usize,
        vocab_size: usize = 0,
        sequence_length: usize = 0,
        activation_dtype: ActivationDType = .f32,
        kv_layout: KvLayout = .f32_row_major,
        attention_storage: quant_matmul.AttentionStorage = .paged,
    }) FrameDescriptor {
        return .{
            .mode = .prefill,
            .rows = options.rows,
            .hidden_size = options.hidden_size,
            .vocab_size = options.vocab_size,
            .sequence_length = if (options.sequence_length == 0) options.rows else options.sequence_length,
            .query_length = options.rows,
            .activation_dtype = options.activation_dtype,
            .kv_layout = options.kv_layout,
            .attention_storage = options.attention_storage,
        };
    }

    pub fn decode(options: struct {
        hidden_size: usize,
        vocab_size: usize = 0,
        sequence_length: usize,
        activation_dtype: ActivationDType = .f32,
        kv_layout: KvLayout = .f32_row_major,
        attention_storage: quant_matmul.AttentionStorage = .paged,
    }) FrameDescriptor {
        return .{
            .mode = .decode,
            .rows = 1,
            .hidden_size = options.hidden_size,
            .vocab_size = options.vocab_size,
            .sequence_length = options.sequence_length,
            .query_length = 1,
            .activation_dtype = options.activation_dtype,
            .kv_layout = options.kv_layout,
            .attention_storage = options.attention_storage,
        };
    }
};

pub const LayerQuantFormats = struct {
    q: quant_matmul.Format = .q8_0,
    k: quant_matmul.Format = .q8_0,
    v: quant_matmul.Format = .q8_0,
    attention_output: quant_matmul.Format = .q8_0,
    gate: quant_matmul.Format = .q8_0,
    up: quant_matmul.Format = .q8_0,
    down: quant_matmul.Format = .q8_0,
    ple_gate: quant_matmul.Format = .q8_0,
    ple_projection: quant_matmul.Format = .q8_0,
};

fn quantPlan(format: quant_matmul.Format, rows: usize, in_dim: usize, out_dim: usize) QuantMatmulPlan {
    return quant_matmul.plan(.{
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .format = format,
    });
}

fn quantOp(format: quant_matmul.Format, rows: usize, in_dim: usize, out_dim: usize) OpQuant {
    const q = quantPlan(format, rows, in_dim, out_dim);
    return .{
        .quant_matmul = q,
        .operator_plan = .{ .quant_matmul = q },
    };
}

fn resolveFfnActivationDType(
    requested: ActivationDType,
    formats: LayerQuantFormats,
    gate_plan: QuantMatmulPlan,
    down_plan: QuantMatmulPlan,
) ActivationDType {
    if (requested != .f16) return .f32;
    if (formats.gate != .q8_0 or formats.up != .q8_0 or formats.down != .q8_0) return .f32;
    if (gate_plan.dispatch != .mm or down_plan.dispatch != .mm) return .f32;
    return .f16;
}

const OpQuant = struct {
    quant_matmul: QuantMatmulPlan,
    operator_plan: OperatorPlan,
};

pub fn quantRowOp(
    comptime kind: quant_matmul.RowOpKind,
    format: quant_matmul.Format,
    rows: usize,
    dim: usize,
    source: usize,
    region: usize,
    resources: []const ResourceUse,
) Op {
    const row_plan = quant_matmul.rowOpPlan(format, kind, rows, dim);
    return .{
        .kind = switch (kind) {
            .get_rows => .quant_get_rows,
            .set_rows => .quant_set_rows,
        },
        .source = source,
        .region = region,
        .resources = resources,
        .operator_plan = .{ .quant_row = row_plan },
    };
}

pub fn quantCopyOp(
    comptime kind: quant_matmul.CopyOpKind,
    format: quant_matmul.Format,
    rows: usize,
    dim: usize,
    source: usize,
    region: usize,
    resources: []const ResourceUse,
) Op {
    const copy_plan = quant_matmul.copyOpPlan(format, kind, rows, dim);
    return .{
        .kind = switch (kind) {
            .q_to_f32 => .quant_copy_q_to_f32,
            .f32_to_q => .quant_copy_f32_to_q,
        },
        .source = source,
        .region = region,
        .resources = resources,
        .operator_plan = .{ .quant_copy = copy_plan },
    };
}

pub fn attentionOp(
    q_len: usize,
    kv_len: usize,
    head_dim: usize,
    kv_format: quant_matmul.AttentionKvFormat,
    source: usize,
    region: usize,
    resources: []const ResourceUse,
) Op {
    const attention_plan = quant_matmul.attentionPlan(q_len, kv_len, head_dim, kv_format);
    return .{
        .kind = switch (attention_plan.operator) {
            .attention_flash => .attention_flash,
            .attention_paged => .attention_paged,
            .attention_quantized_kv => .attention_quantized_kv,
            else => .attention,
        },
        .source = source,
        .region = region,
        .resources = resources,
        .operator_plan = .{ .attention = attention_plan },
    };
}

pub fn pagedAttentionOp(
    q_len: usize,
    kv_len: usize,
    head_dim: usize,
    kv_format: quant_matmul.AttentionKvFormat,
    source: usize,
    region: usize,
    resources: []const ResourceUse,
) Op {
    const attention_plan = quant_matmul.attentionPlanWithStorage(q_len, kv_len, head_dim, kv_format, .paged);
    return .{
        .kind = switch (attention_plan.operator) {
            .attention_paged => .attention_paged,
            .attention_quantized_kv => .attention_quantized_kv,
            .attention_flash => .attention_flash,
            else => .attention,
        },
        .source = source,
        .region = region,
        .resources = resources,
        .operator_plan = .{ .attention = attention_plan },
    };
}

pub const Options = struct {
    /// Keep scopes small enough for predictable encoder labels/debugging while
    /// still letting dependent dispatches share one encoder with barriers.
    max_ops_per_scope: usize = 64,
};

pub const PlannedOp = struct {
    kind: OpKind,
    op_index: usize,
    scope_index: usize,
    barrier_before: bool,
};

pub const EncoderScope = struct {
    first_op: usize,
    op_count: usize,
    source: usize,
    region: usize,
    barrier_count: usize = 0,
};

pub const Plan = struct {
    planned_ops: []PlannedOp,
    scopes: []EncoderScope,
    barrier_count: usize,

    pub fn deinit(self: *Plan, allocator: std.mem.Allocator) void {
        allocator.free(self.planned_ops);
        allocator.free(self.scopes);
        self.* = undefined;
    }
};

pub const PlanView = struct {
    planned_ops: []const PlannedOp,
    scopes: []const EncoderScope,
    barrier_count: usize,
};

pub const GraphCommandOp = struct {
    kind: OpKind,
    source: usize,
    region: usize,
    planned_op_index: usize,
    scope_index: usize,
    barrier_before: bool,
    resource_start: usize,
    resource_count: usize,
    quant_matmul: ?QuantMatmulPlan = null,
    operator_plan: ?OperatorPlan = null,
    input_dtype: ActivationDType = .f32,
    output_dtype: ActivationDType = .f32,
};

pub const ScratchSlotSize = struct {
    slot: usize,
    bytes: usize,
    dtype: ActivationDType = .f32,
};

pub const ScratchSlotLifetime = struct {
    slot: usize,
    bytes: usize,
    dtype: ActivationDType = .f32,
    first_op: usize,
    last_op: usize,
};

pub const GraphCommandPlanView = struct {
    ops: []const GraphCommandOp,
    resources: []const ResourceUse,
    planned_ops: []const PlannedOp,
    scopes: []const EncoderScope,
    scratch_slots: []const ScratchSlotLifetime,
    barrier_count: usize,

    pub fn planView(self: GraphCommandPlanView) PlanView {
        return .{
            .planned_ops = self.planned_ops,
            .scopes = self.scopes,
            .barrier_count = self.barrier_count,
        };
    }

    pub fn operatorStats(self: GraphCommandPlanView) OperatorPlanStats {
        var stats = OperatorPlanStats{};
        for (self.ops) |op| {
            if (op.operator_plan) |plan| {
                stats.add(plan.operator());
            }
        }
        return stats;
    }
};

pub const GraphCommandPlan = struct {
    allocator: std.mem.Allocator,
    ops: std.ArrayList(GraphCommandOp),
    resources: std.ArrayList(ResourceUse),
    planned_ops: std.ArrayList(PlannedOp),
    scopes: std.ArrayList(EncoderScope),
    scratch_slots: std.ArrayList(ScratchSlotLifetime),
    barrier_count: usize = 0,

    pub fn init(allocator: std.mem.Allocator) GraphCommandPlan {
        return .{
            .allocator = allocator,
            .ops = .empty,
            .resources = .empty,
            .planned_ops = .empty,
            .scopes = .empty,
            .scratch_slots = .empty,
        };
    }

    pub fn deinit(self: *GraphCommandPlan) void {
        self.ops.deinit(self.allocator);
        self.resources.deinit(self.allocator);
        self.planned_ops.deinit(self.allocator);
        self.scopes.deinit(self.allocator);
        self.scratch_slots.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn clearRetainingCapacity(self: *GraphCommandPlan) void {
        self.ops.clearRetainingCapacity();
        self.resources.clearRetainingCapacity();
        self.planned_ops.clearRetainingCapacity();
        self.scopes.clearRetainingCapacity();
        self.scratch_slots.clearRetainingCapacity();
        self.barrier_count = 0;
    }

    pub fn append(self: *GraphCommandPlan, plan: GraphCommandPlanView) !void {
        const op_base = self.ops.items.len;
        const resource_base = self.resources.items.len;
        const planned_base = self.planned_ops.items.len;
        const scope_base = self.scopes.items.len;

        try self.resources.appendSlice(self.allocator, plan.resources);
        try self.ops.ensureUnusedCapacity(self.allocator, plan.ops.len);
        for (plan.ops) |op| {
            self.ops.appendAssumeCapacity(.{
                .kind = op.kind,
                .source = op.source,
                .region = op.region,
                .planned_op_index = planned_base + op.planned_op_index,
                .scope_index = scope_base + op.scope_index,
                .barrier_before = op.barrier_before,
                .resource_start = resource_base + op.resource_start,
                .resource_count = op.resource_count,
                .quant_matmul = op.quant_matmul,
                .operator_plan = op.operator_plan,
                .input_dtype = op.input_dtype,
                .output_dtype = op.output_dtype,
            });
        }

        try self.planned_ops.ensureUnusedCapacity(self.allocator, plan.planned_ops.len);
        for (plan.planned_ops) |planned| {
            self.planned_ops.appendAssumeCapacity(.{
                .kind = planned.kind,
                .op_index = op_base + planned.op_index,
                .scope_index = scope_base + planned.scope_index,
                .barrier_before = planned.barrier_before,
            });
        }

        try self.scopes.ensureUnusedCapacity(self.allocator, plan.scopes.len);
        for (plan.scopes) |scope| {
            self.scopes.appendAssumeCapacity(.{
                .first_op = op_base + scope.first_op,
                .op_count = scope.op_count,
                .source = scope.source,
                .region = scope.region,
                .barrier_count = scope.barrier_count,
            });
        }

        for (plan.scratch_slots) |scratch| {
            try self.mergeScratchSlot(.{
                .slot = scratch.slot,
                .bytes = scratch.bytes,
                .dtype = scratch.dtype,
                .first_op = op_base + scratch.first_op,
                .last_op = op_base + scratch.last_op,
            });
        }
        self.barrier_count += plan.barrier_count;
    }

    pub fn view(self: *const GraphCommandPlan) GraphCommandPlanView {
        return .{
            .ops = self.ops.items,
            .resources = self.resources.items,
            .planned_ops = self.planned_ops.items,
            .scopes = self.scopes.items,
            .scratch_slots = self.scratch_slots.items,
            .barrier_count = self.barrier_count,
        };
    }

    fn mergeScratchSlot(self: *GraphCommandPlan, incoming: ScratchSlotLifetime) !void {
        for (self.scratch_slots.items) |*scratch| {
            if (scratch.slot != incoming.slot) continue;
            scratch.bytes = @max(scratch.bytes, incoming.bytes);
            if (scratch.dtype != incoming.dtype) scratch.dtype = .f32;
            scratch.first_op = @min(scratch.first_op, incoming.first_op);
            scratch.last_op = @max(scratch.last_op, incoming.last_op);
            return;
        }
        try self.scratch_slots.append(self.allocator, incoming);
    }
};

pub const GatedFrameLayerWindow = struct {
    layer_start: usize,
    setup_start: usize,
    block_start: usize,
    layer_end: usize,
};

pub const GatedFrameTailWindow = struct {
    start: usize,
    logits_end: usize,
};

pub const GatedFramePlanCursor = struct {
    plan: GraphCommandPlanView,
    next_index: usize = 0,

    pub fn init(plan: GraphCommandPlanView) GatedFramePlanCursor {
        return .{ .plan = plan };
    }

    pub fn nextLayer(
        self: *GatedFramePlanCursor,
        options: struct {
            shares_kv: bool,
            value_norm: bool,
            kv_seed: bool = true,
            include_ple: bool = true,
        },
    ) ?GatedFrameLayerWindow {
        const layer_start = self.next_index;
        if (!self.expectAt(layer_start, .attention_pre_norm)) return null;

        var index = layer_start + 1;
        const setup_start = index;
        if (!self.consume(&index, .qkv_linear)) return null;
        if (!self.consume(&index, .q_head_norm_rope)) return null;
        if (!options.shares_kv) {
            if (!self.consume(&index, .k_head_norm_rope)) return null;
            if (options.value_norm and !self.consume(&index, .v_norm)) return null;
            if (options.kv_seed and !self.consume(&index, .kv_seed)) return null;
        }
        const block_start = index;

        if (!self.consume(&index, .attention)) return null;
        if (!self.consume(&index, .attention_output_linear)) return null;
        if (!self.consume(&index, .attention_post_norm_residual)) return null;
        if (!self.consume(&index, .ffn_pre_norm_scale)) return null;
        if (!self.consume(&index, .ffn_gate_up_activation)) return null;
        if (!self.consume(&index, .ffn_down_linear)) return null;
        if (!self.consume(&index, .ffn_post_norm_residual)) return null;
        if (options.include_ple) {
            if (!self.consume(&index, .ple_gate_activation)) return null;
            if (!self.consume(&index, .ple_projection)) return null;
            if (!self.consume(&index, .ple_post_norm_residual)) return null;
        }

        self.next_index = index;
        return .{
            .layer_start = layer_start,
            .setup_start = setup_start,
            .block_start = block_start,
            .layer_end = index,
        };
    }

    pub fn nextTailLogits(self: *GatedFramePlanCursor) ?GatedFrameTailWindow {
        const start = self.next_index;
        var index = start;
        if (!self.consume(&index, .tail_final_norm)) return null;
        if (!self.consume(&index, .tail_lm_head)) return null;
        const logits_end = index;
        if (self.expectAt(index, .tail_argmax)) index += 1;
        self.next_index = index;
        return .{ .start = start, .logits_end = logits_end };
    }

    pub fn complete(self: *const GatedFramePlanCursor) bool {
        return self.next_index == self.plan.ops.len and self.next_index == self.plan.planned_ops.len;
    }

    fn expectAt(self: *const GatedFramePlanCursor, index: usize, kind: OpKind) bool {
        if (index >= self.plan.ops.len or index >= self.plan.planned_ops.len) return false;
        return self.plan.ops[index].kind == kind and self.plan.planned_ops[index].kind == kind;
    }

    fn consume(self: *const GatedFramePlanCursor, index: *usize, kind: OpKind) bool {
        if (!self.expectAt(index.*, kind)) return false;
        index.* += 1;
        return true;
    }
};

pub fn BoundedPlan(comptime max_ops: usize, comptime max_scopes: usize, comptime max_resource_uses: usize) type {
    return struct {
        const Self = @This();

        planned_ops: [max_ops]PlannedOp = undefined,
        scopes: [max_scopes]EncoderScope = undefined,
        resource_uses: [max_resource_uses]ResourceUse = undefined,
        view_state: PlanView = .{
            .planned_ops = &.{},
            .scopes = &.{},
            .barrier_count = 0,
        },

        pub fn build(self: *Self, ops: []const Op, options: Options) !PlanView {
            self.view_state = try buildInto(
                ops,
                options,
                &self.planned_ops,
                &self.scopes,
                &self.resource_uses,
            );
            return self.view_state;
        }

        pub fn view(self: *const Self) PlanView {
            return self.view_state;
        }
    };
}

pub fn BoundedGraphCommandPlan(
    comptime max_ops: usize,
    comptime max_scopes: usize,
    comptime max_resource_uses: usize,
    comptime max_scratch_slots: usize,
    comptime max_scope_resource_uses: usize,
) type {
    return struct {
        const Self = @This();

        ops: [max_ops]GraphCommandOp = undefined,
        resources: [max_resource_uses]ResourceUse = undefined,
        planned_ops: [max_ops]PlannedOp = undefined,
        scopes: [max_scopes]EncoderScope = undefined,
        scratch_slots: [max_scratch_slots]ScratchSlotLifetime = undefined,
        scope_resource_uses: [max_scope_resource_uses]ResourceUse = undefined,
        view_state: GraphCommandPlanView = .{
            .ops = &.{},
            .resources = &.{},
            .planned_ops = &.{},
            .scopes = &.{},
            .scratch_slots = &.{},
            .barrier_count = 0,
        },

        pub fn build(self: *Self, plan_ops: []const Op, options: Options, scratch_sizes: []const ScratchSlotSize) !GraphCommandPlanView {
            self.view_state = try buildGraphCommandPlanInto(
                plan_ops,
                options,
                scratch_sizes,
                &self.ops,
                &self.resources,
                &self.planned_ops,
                &self.scopes,
                &self.scratch_slots,
                &self.scope_resource_uses,
            );
            return self.view_state;
        }

        pub fn view(self: *const Self) GraphCommandPlanView {
            return self.view_state;
        }
    };
}

pub const AttentionSetupCommandLowerer = struct {
    const Resource = enum(usize) {
        attention_input,
        projection_input,
        q,
        k,
        v,
        q_ready,
        k_ready,
        v_ready,
    };

    pre_norm_resources: [3]ResourceUse = undefined,
    linear_resources: [7]ResourceUse = undefined,
    q_rope_resources: [3]ResourceUse = undefined,
    k_rope_resources: [4]ResourceUse = undefined,
    v_norm_resources: [3]ResourceUse = undefined,
    kv_seed_resources: [4]ResourceUse = undefined,
    ops: [6]Op = undefined,
    op_count: usize = 0,
    linear_resource_count: usize = 0,
    storage: BoundedPlan(5, 5, 16) = .{},
    command_storage: BoundedGraphCommandPlan(6, 1, 28, 8, 20) = .{},
    scratch_sizes: [8]ScratchSlotSize = undefined,
    scratch_size_count: usize = 0,
    plan_view: PlanView = .{
        .planned_ops = &.{},
        .scopes = &.{},
        .barrier_count = 0,
    },
    command_view: GraphCommandPlanView = .{
        .ops = &.{},
        .resources = &.{},
        .planned_ops = &.{},
        .scopes = &.{},
        .scratch_slots = &.{},
        .barrier_count = 0,
    },

    pub const SetupOptions = struct {
        shares_kv: bool,
        has_attention_pre_norm: bool,
        attention_pre_norm_slot: usize,
        q_linear_slot: usize,
        k_linear_slot: usize,
        v_linear_slot: usize,
        q_head_norm_slot: usize,
        k_head_norm_slot: usize,
        attention_layer_index: usize,
        value_norm: bool,
        kv_seed: bool = false,
        quant_formats: LayerQuantFormats = .{},
        source: usize,
        region: usize,
        rows: usize = 0,
        hidden_size: usize = 0,
        attention_input_size: usize = 0,
        kv_dim: usize = 0,
    };

    pub fn build(self: *AttentionSetupCommandLowerer, options: SetupOptions) !void {
        self.op_count = 0;
        self.linear_resource_count = 0;
        self.scratch_size_count = 0;
        const setup_input_resource: ResourceRange = if (options.has_attention_pre_norm)
            .whole(.scratch_slot, @intFromEnum(Resource.projection_input))
        else
            .whole(.scratch_slot, @intFromEnum(Resource.attention_input));

        self.pre_norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_input)), .access = .read },
            .{ .range = .whole(.norm_slot, options.attention_pre_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.projection_input)), .access = .write },
        };

        self.linear_resources[self.linear_resource_count] = .{ .range = setup_input_resource, .access = .read };
        self.linear_resource_count += 1;
        self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.q_linear_slot), .access = .read };
        self.linear_resource_count += 1;
        if (!options.shares_kv) {
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.k_linear_slot), .access = .read };
            self.linear_resource_count += 1;
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.v_linear_slot), .access = .read };
            self.linear_resource_count += 1;
        }
        self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q)), .access = .write };
        self.linear_resource_count += 1;
        if (!options.shares_kv) {
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k)), .access = .write };
            self.linear_resource_count += 1;
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .write };
            self.linear_resource_count += 1;
        }

        self.q_rope_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q)), .access = .read },
            .{ .range = .whole(.norm_slot, options.q_head_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q_ready)), .access = .write },
        };
        self.k_rope_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k)), .access = .read },
            .{ .range = .whole(.norm_slot, options.k_head_norm_slot), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2), .access = .write },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k_ready)), .access = .write },
        };
        self.v_norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2 + 1), .access = .write },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v_ready)), .access = .write },
        };
        self.kv_seed_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k_ready)), .access = .read },
            .{ .range = if (options.value_norm) .whole(.scratch_slot, @intFromEnum(Resource.v_ready)) else .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2), .access = .write },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2 + 1), .access = .write },
        };

        if (options.has_attention_pre_norm) {
            self.ops[self.op_count] = .{ .kind = .attention_pre_norm, .source = options.source, .region = options.region, .resources = &self.pre_norm_resources };
            self.op_count += 1;
        }
        const q_linear_op = quantOp(options.quant_formats.q, options.rows, options.hidden_size, options.attention_input_size);
        self.ops[self.op_count] = .{
            .kind = .qkv_linear,
            .source = options.source,
            .region = options.region,
            .resources = self.linear_resources[0..self.linear_resource_count],
            .quant_matmul = q_linear_op.quant_matmul,
            .operator_plan = q_linear_op.operator_plan,
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{ .kind = .q_head_norm_rope, .source = options.source, .region = options.region, .resources = &self.q_rope_resources };
        self.op_count += 1;
        if (!options.shares_kv) {
            self.ops[self.op_count] = .{ .kind = .k_head_norm_rope, .source = options.source, .region = options.region, .resources = &self.k_rope_resources };
            self.op_count += 1;
            if (options.value_norm) {
                self.ops[self.op_count] = .{ .kind = .v_norm, .source = options.source, .region = options.region, .resources = &self.v_norm_resources };
                self.op_count += 1;
            }
            if (options.kv_seed) {
                self.ops[self.op_count] = .{ .kind = .kv_seed, .source = options.source, .region = options.region, .resources = &self.kv_seed_resources };
                self.op_count += 1;
            }
        }

        const rows = options.rows;
        self.addScratchSize(@intFromEnum(Resource.attention_input), rows * options.hidden_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.projection_input), rows * options.hidden_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.q), rows * options.attention_input_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.k), rows * options.kv_dim * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.v), rows * options.kv_dim * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.q_ready), rows * options.attention_input_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.k_ready), rows * options.kv_dim * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.v_ready), rows * options.kv_dim * @sizeOf(f32));
        self.command_view = try self.command_storage.build(self.ops[0..self.op_count], .{}, self.scratch_sizes[0..self.scratch_size_count]);
        self.plan_view = self.command_view.planView();
    }

    pub fn view(self: *const AttentionSetupCommandLowerer) PlanView {
        return self.plan_view;
    }

    pub fn commandView(self: *const AttentionSetupCommandLowerer) GraphCommandPlanView {
        return self.command_view;
    }

    fn addScratchSize(self: *AttentionSetupCommandLowerer, slot: usize, bytes: usize) void {
        if (bytes == 0 or self.scratch_size_count >= self.scratch_sizes.len) return;
        self.scratch_sizes[self.scratch_size_count] = .{ .slot = slot, .bytes = bytes };
        self.scratch_size_count += 1;
    }
};

pub const AttentionProjectCommandLowerer = struct {
    const Resource = enum(usize) {
        q,
        k,
        v,
        attention_output,
        projected,
        residual,
        output,
    };

    attention_resources: [4]ResourceUse = undefined,
    kv_seed_resources: [4]ResourceUse = undefined,
    linear_resources: [3]ResourceUse = undefined,
    post_norm_resources: [4]ResourceUse = undefined,
    ops: [3]Op = undefined,
    storage: BoundedPlan(3, 3, 11) = .{},
    plan_view: PlanView = .{
        .planned_ops = &.{},
        .scopes = &.{},
        .barrier_count = 0,
    },

    pub const BuildOptions = struct {
        output_linear_slot: usize,
        post_norm_slot: usize,
        source: usize,
        region: usize,
    };

    pub fn build(self: *AttentionProjectCommandLowerer, options: BuildOptions) !void {
        self.attention_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_output)), .access = .write },
        };
        self.linear_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_output)), .access = .read },
            .{ .range = .whole(.quant_slot, options.output_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.projected)), .access = .write },
        };
        self.post_norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.post_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.residual)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.output)), .access = .write },
        };
        self.ops = .{
            .{ .kind = .attention, .source = options.source, .region = options.region, .resources = &self.attention_resources },
            .{ .kind = .attention_output_linear, .source = options.source, .region = options.region, .resources = &self.linear_resources },
            .{ .kind = .attention_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.post_norm_resources },
        };
        self.plan_view = try self.storage.build(&self.ops, .{});
    }

    pub fn view(self: *const AttentionProjectCommandLowerer) PlanView {
        return self.plan_view;
    }
};

pub const FfnPleCommandLowerer = struct {
    const Resource = enum(usize) {
        input,
        inv_scale,
        ffn_gated,
        ffn_projected,
        residual,
        ffn_output,
        ple_input,
        ple_gated,
        ple_projected,
        output,
    };

    pre_gate_resources: [3]ResourceUse = undefined,
    pair_resources: [6]ResourceUse = undefined,
    down_resources: [3]ResourceUse = undefined,
    post_down_resources: [4]ResourceUse = undefined,
    ple_gate_resources: [4]ResourceUse = undefined,
    ple_proj_resources: [3]ResourceUse = undefined,
    ple_post_resources: [4]ResourceUse = undefined,
    ops: [7]Op = undefined,
    storage: BoundedPlan(7, 7, 27) = .{},
    plan_view: PlanView = .{
        .planned_ops = &.{},
        .scopes = &.{},
        .barrier_count = 0,
    },

    pub const BuildOptions = struct {
        pre_gate_norm_slot: usize,
        gate_linear_slot: usize,
        up_linear_slot: usize,
        down_linear_slot: usize,
        post_down_norm_slot: usize,
        ple_gate_linear_slot: usize,
        ple_proj_linear_slot: usize,
        ple_post_norm_slot: usize,
        source: usize,
        region: usize,
    };

    pub fn build(self: *FfnPleCommandLowerer, options: BuildOptions) !void {
        self.pre_gate_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.input)), .access = .read },
            .{ .range = .whole(.norm_slot, options.pre_gate_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.inv_scale)), .access = .write },
        };
        self.pair_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.input)), .access = .read },
            .{ .range = .whole(.norm_slot, options.pre_gate_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.inv_scale)), .access = .read },
            .{ .range = .whole(.quant_slot, options.gate_linear_slot), .access = .read },
            .{ .range = .whole(.quant_slot, options.up_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_gated)), .access = .write },
        };
        self.down_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_gated)), .access = .read },
            .{ .range = .whole(.quant_slot, options.down_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_projected)), .access = .write },
        };
        self.post_down_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.post_down_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.residual)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .write },
        };
        self.ple_gate_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_input)), .access = .read },
            .{ .range = .whole(.quant_slot, options.ple_gate_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_gated)), .access = .write },
        };
        self.ple_proj_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_gated)), .access = .read },
            .{ .range = .whole(.quant_slot, options.ple_proj_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_projected)), .access = .write },
        };
        self.ple_post_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.ple_post_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.output)), .access = .write },
        };
        self.ops = .{
            .{ .kind = .ffn_pre_norm_scale, .source = options.source, .region = options.region, .resources = &self.pre_gate_resources },
            .{ .kind = .ffn_gate_up_activation, .source = options.source, .region = options.region, .resources = &self.pair_resources },
            .{ .kind = .ffn_down_linear, .source = options.source, .region = options.region, .resources = &self.down_resources },
            .{ .kind = .ffn_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.post_down_resources },
            .{ .kind = .ple_gate_activation, .source = options.source, .region = options.region, .resources = &self.ple_gate_resources },
            .{ .kind = .ple_projection, .source = options.source, .region = options.region, .resources = &self.ple_proj_resources },
            .{ .kind = .ple_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.ple_post_resources },
        };
        self.plan_view = try self.storage.build(&self.ops, .{});
    }

    pub fn view(self: *const FfnPleCommandLowerer) PlanView {
        return self.plan_view;
    }
};

pub const GatedLayerCommandLowerer = struct {
    pub const Resource = enum(usize) {
        attention_input,
        projection_input,
        q,
        k,
        v,
        q_ready,
        k_ready,
        v_ready,
        attention_output,
        attention_projected,
        residual,
        attn_added,
        inv_scale,
        ffn_gated,
        ffn_projected,
        ffn_output,
        ple_input,
        ple_gated,
        ple_projected,
        output,
    };

    pre_norm_resources: [3]ResourceUse = undefined,
    linear_resources: [7]ResourceUse = undefined,
    q_rope_resources: [3]ResourceUse = undefined,
    k_rope_resources: [4]ResourceUse = undefined,
    v_norm_resources: [3]ResourceUse = undefined,
    attention_resources: [4]ResourceUse = undefined,
    kv_seed_resources: [4]ResourceUse = undefined,
    attention_linear_resources: [3]ResourceUse = undefined,
    attention_post_resources: [4]ResourceUse = undefined,
    ffn_pre_resources: [3]ResourceUse = undefined,
    ffn_pair_resources: [6]ResourceUse = undefined,
    ffn_down_resources: [3]ResourceUse = undefined,
    ffn_post_resources: [4]ResourceUse = undefined,
    ple_gate_resources: [4]ResourceUse = undefined,
    ple_proj_resources: [3]ResourceUse = undefined,
    ple_post_resources: [4]ResourceUse = undefined,
    ops: [16]Op = undefined,
    op_count: usize = 0,
    linear_resource_count: usize = 0,
    storage: BoundedPlan(16, 1, 20) = .{},
    command_storage: BoundedGraphCommandPlan(16, 1, 68, 20, 20) = .{},
    scratch_sizes: [20]ScratchSlotSize = undefined,
    scratch_size_count: usize = 0,
    plan_view: PlanView = .{
        .planned_ops = &.{},
        .scopes = &.{},
        .barrier_count = 0,
    },
    command_view: GraphCommandPlanView = .{
        .ops = &.{},
        .resources = &.{},
        .planned_ops = &.{},
        .scopes = &.{},
        .scratch_slots = &.{},
        .barrier_count = 0,
    },

    pub const BuildOptions = struct {
        shares_kv: bool,
        has_attention_pre_norm: bool,
        attention_pre_norm_slot: usize,
        q_linear_slot: usize,
        k_linear_slot: usize,
        v_linear_slot: usize,
        q_head_norm_slot: usize,
        k_head_norm_slot: usize,
        attention_layer_index: usize,
        value_norm: bool,
        quant_formats: LayerQuantFormats = .{},
        activation_dtype: ActivationDType = .f32,
        attention_linear_slot: usize,
        attention_post_norm_slot: usize,
        ffn_pre_norm_slot: usize,
        gate_linear_slot: usize,
        up_linear_slot: usize,
        down_linear_slot: usize,
        ffn_post_norm_slot: usize,
        include_ple: bool = true,
        ple_gate_linear_slot: usize = 0,
        ple_proj_linear_slot: usize = 0,
        ple_post_norm_slot: usize = 0,
        source: usize,
        region: usize,
        rows: usize = 1,
        kv_len: usize = 0,
        hidden_size: usize = 0,
        attention_input_size: usize = 0,
        kv_dim: usize = 0,
        head_dim: usize = 0,
        attention_kv_format: quant_matmul.AttentionKvFormat = .f32,
        attention_storage: quant_matmul.AttentionStorage = .dense,
        kv_seed: bool = false,
        intermediate_size: usize = 0,
        ple_hidden_size: usize = 0,
    };

    pub fn build(self: *GatedLayerCommandLowerer, options: BuildOptions) !void {
        self.op_count = 0;
        self.linear_resource_count = 0;
        self.scratch_size_count = 0;
        const setup_input_resource: ResourceRange = if (options.has_attention_pre_norm)
            .whole(.scratch_slot, @intFromEnum(Resource.projection_input))
        else
            .whole(.scratch_slot, @intFromEnum(Resource.attention_input));

        self.pre_norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_input)), .access = .read },
            .{ .range = .whole(.norm_slot, options.attention_pre_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.projection_input)), .access = .write },
        };

        self.linear_resources[self.linear_resource_count] = .{ .range = setup_input_resource, .access = .read };
        self.linear_resource_count += 1;
        self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.q_linear_slot), .access = .read };
        self.linear_resource_count += 1;
        if (!options.shares_kv) {
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.k_linear_slot), .access = .read };
            self.linear_resource_count += 1;
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.quant_slot, options.v_linear_slot), .access = .read };
            self.linear_resource_count += 1;
        }
        self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q)), .access = .write };
        self.linear_resource_count += 1;
        if (!options.shares_kv) {
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k)), .access = .write };
            self.linear_resource_count += 1;
            self.linear_resources[self.linear_resource_count] = .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .write };
            self.linear_resource_count += 1;
        }

        self.q_rope_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q)), .access = .read },
            .{ .range = .whole(.norm_slot, options.q_head_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q_ready)), .access = .write },
        };
        self.k_rope_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k)), .access = .read },
            .{ .range = .whole(.norm_slot, options.k_head_norm_slot), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2), .access = .write },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k_ready)), .access = .write },
        };
        self.v_norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2 + 1), .access = .write },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.v_ready)), .access = .write },
        };
        self.attention_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.q_ready)), .access = .read },
            .{ .range = if (options.shares_kv) .whole(.kv_cache, options.attention_layer_index * 2) else .whole(.scratch_slot, @intFromEnum(Resource.k_ready)), .access = .read },
            .{ .range = if (options.shares_kv) .whole(.kv_cache, options.attention_layer_index * 2 + 1) else .whole(.scratch_slot, @intFromEnum(Resource.v_ready)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_output)), .access = .write },
        };
        self.kv_seed_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.k_ready)), .access = .read },
            .{ .range = if (options.value_norm) .whole(.scratch_slot, @intFromEnum(Resource.v_ready)) else .whole(.scratch_slot, @intFromEnum(Resource.v)), .access = .read },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2), .access = .write },
            .{ .range = .whole(.kv_cache, options.attention_layer_index * 2 + 1), .access = .write },
        };
        self.attention_linear_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_output)), .access = .read },
            .{ .range = .whole(.quant_slot, options.attention_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_projected)), .access = .write },
        };
        self.attention_post_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attention_projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.attention_post_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.residual)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attn_added)), .access = .write },
        };
        self.ffn_pre_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attn_added)), .access = .read },
            .{ .range = .whole(.norm_slot, options.ffn_pre_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.inv_scale)), .access = .write },
        };
        self.ffn_pair_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attn_added)), .access = .read },
            .{ .range = .whole(.norm_slot, options.ffn_pre_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.inv_scale)), .access = .read },
            .{ .range = .whole(.quant_slot, options.gate_linear_slot), .access = .read },
            .{ .range = .whole(.quant_slot, options.up_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_gated)), .access = .write },
        };
        self.ffn_down_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_gated)), .access = .read },
            .{ .range = .whole(.quant_slot, options.down_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_projected)), .access = .write },
        };
        self.ffn_post_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.ffn_post_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.attn_added)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .write },
        };
        self.ple_gate_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_input)), .access = .read },
            .{ .range = .whole(.quant_slot, options.ple_gate_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_gated)), .access = .write },
        };
        self.ple_proj_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_gated)), .access = .read },
            .{ .range = .whole(.quant_slot, options.ple_proj_linear_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_projected)), .access = .write },
        };
        self.ple_post_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ple_projected)), .access = .read },
            .{ .range = .whole(.norm_slot, options.ple_post_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.ffn_output)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.output)), .access = .write },
        };

        const rows = @max(options.rows, 1);
        if (options.has_attention_pre_norm) {
            self.ops[self.op_count] = .{ .kind = .attention_pre_norm, .source = options.source, .region = options.region, .resources = &self.pre_norm_resources };
            self.op_count += 1;
        }
        const q_linear_op = quantOp(options.quant_formats.q, rows, options.hidden_size, options.attention_input_size);
        const attn_output_op = quantOp(options.quant_formats.attention_output, rows, options.attention_input_size, options.hidden_size);
        const ffn_gate_op = quantOp(options.quant_formats.gate, rows, options.hidden_size, options.intermediate_size);
        const ffn_down_op = quantOp(options.quant_formats.down, rows, options.intermediate_size, options.hidden_size);
        const ple_gate_op = if (options.include_ple) quantOp(options.quant_formats.ple_gate, rows, options.hidden_size, options.ple_hidden_size) else quantOp(.unknown, rows, options.hidden_size, options.ple_hidden_size);
        const ple_projection_op = if (options.include_ple) quantOp(options.quant_formats.ple_projection, rows, options.ple_hidden_size, options.hidden_size) else quantOp(.unknown, rows, options.ple_hidden_size, options.hidden_size);
        const attention_kv_len = if (options.kv_len != 0) options.kv_len else rows;
        const attention_plan = quant_matmul.attentionPlanWithStorage(rows, attention_kv_len, options.head_dim, options.attention_kv_format, options.attention_storage);
        self.ops[self.op_count] = .{
            .kind = .qkv_linear,
            .source = options.source,
            .region = options.region,
            .resources = self.linear_resources[0..self.linear_resource_count],
            .quant_matmul = q_linear_op.quant_matmul,
            .operator_plan = q_linear_op.operator_plan,
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{ .kind = .q_head_norm_rope, .source = options.source, .region = options.region, .resources = &self.q_rope_resources };
        self.op_count += 1;
        if (!options.shares_kv) {
            self.ops[self.op_count] = .{ .kind = .k_head_norm_rope, .source = options.source, .region = options.region, .resources = &self.k_rope_resources };
            self.op_count += 1;
            if (options.value_norm) {
                self.ops[self.op_count] = .{ .kind = .v_norm, .source = options.source, .region = options.region, .resources = &self.v_norm_resources };
                self.op_count += 1;
            }
            if (options.kv_seed) {
                self.ops[self.op_count] = .{ .kind = .kv_seed, .source = options.source, .region = options.region, .resources = &self.kv_seed_resources };
                self.op_count += 1;
            }
        }
        self.ops[self.op_count] = .{
            .kind = .attention,
            .source = options.source,
            .region = options.region,
            .resources = &self.attention_resources,
            .operator_plan = .{ .attention = attention_plan },
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{
            .kind = .attention_output_linear,
            .source = options.source,
            .region = options.region,
            .resources = &self.attention_linear_resources,
            .quant_matmul = attn_output_op.quant_matmul,
            .operator_plan = attn_output_op.operator_plan,
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{ .kind = .attention_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.attention_post_resources };
        self.op_count += 1;
        self.ops[self.op_count] = .{ .kind = .ffn_pre_norm_scale, .source = options.source, .region = options.region, .resources = &self.ffn_pre_resources };
        self.op_count += 1;
        self.ops[self.op_count] = .{
            .kind = .ffn_gate_up_activation,
            .source = options.source,
            .region = options.region,
            .resources = &self.ffn_pair_resources,
            .quant_matmul = ffn_gate_op.quant_matmul,
            .operator_plan = ffn_gate_op.operator_plan,
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{
            .kind = .ffn_down_linear,
            .source = options.source,
            .region = options.region,
            .resources = &self.ffn_down_resources,
            .quant_matmul = ffn_down_op.quant_matmul,
            .operator_plan = ffn_down_op.operator_plan,
        };
        self.op_count += 1;
        self.ops[self.op_count] = .{ .kind = .ffn_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.ffn_post_resources };
        self.op_count += 1;
        if (options.include_ple) {
            self.ops[self.op_count] = .{
                .kind = .ple_gate_activation,
                .source = options.source,
                .region = options.region,
                .resources = &self.ple_gate_resources,
                .quant_matmul = ple_gate_op.quant_matmul,
                .operator_plan = ple_gate_op.operator_plan,
            };
            self.op_count += 1;
            self.ops[self.op_count] = .{
                .kind = .ple_projection,
                .source = options.source,
                .region = options.region,
                .resources = &self.ple_proj_resources,
                .quant_matmul = ple_projection_op.quant_matmul,
                .operator_plan = ple_projection_op.operator_plan,
            };
            self.op_count += 1;
            self.ops[self.op_count] = .{ .kind = .ple_post_norm_residual, .source = options.source, .region = options.region, .resources = &self.ple_post_resources };
            self.op_count += 1;
        }

        const ffn_gated_dtype = resolveFfnActivationDType(
            options.activation_dtype,
            options.quant_formats,
            ffn_gate_op.quant_matmul,
            ffn_down_op.quant_matmul,
        );
        self.addScratchSize(@intFromEnum(Resource.attention_input), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.projection_input), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.q), rows * options.attention_input_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.k), rows * options.kv_dim * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.v), rows * options.kv_dim * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.q_ready), rows * options.attention_input_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.k_ready), rows * options.kv_dim * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.v_ready), rows * options.kv_dim * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.attention_output), rows * options.attention_input_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.attention_projected), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.residual), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.attn_added), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.inv_scale), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.ffn_gated), rows * options.intermediate_size * ffn_gated_dtype.byteSize(), ffn_gated_dtype);
        self.addScratchSize(@intFromEnum(Resource.ffn_projected), rows * options.hidden_size * @sizeOf(f32), .f32);
        self.addScratchSize(@intFromEnum(Resource.ffn_output), rows * options.hidden_size * @sizeOf(f32), .f32);
        if (options.include_ple) {
            self.addScratchSize(@intFromEnum(Resource.ple_input), rows * options.ple_hidden_size * @sizeOf(f32), .f32);
            self.addScratchSize(@intFromEnum(Resource.ple_gated), rows * options.ple_hidden_size * @sizeOf(f32), .f32);
            self.addScratchSize(@intFromEnum(Resource.ple_projected), rows * options.hidden_size * @sizeOf(f32), .f32);
        }

        self.command_view = try self.command_storage.build(self.ops[0..self.op_count], .{}, self.scratch_sizes[0..self.scratch_size_count]);
        self.plan_view = self.command_view.planView();
    }

    pub fn view(self: *const GatedLayerCommandLowerer) PlanView {
        return self.plan_view;
    }

    pub fn commandView(self: *const GatedLayerCommandLowerer) GraphCommandPlanView {
        return self.command_view;
    }

    fn addScratchSize(self: *GatedLayerCommandLowerer, slot: usize, bytes: usize, dtype: ActivationDType) void {
        if (bytes == 0 or self.scratch_size_count >= self.scratch_sizes.len) return;
        self.scratch_sizes[self.scratch_size_count] = .{ .slot = slot, .bytes = bytes, .dtype = dtype };
        self.scratch_size_count += 1;
    }
};

pub const PrefillGatedLayerCommandLowerer = struct {
    inner: GatedLayerCommandLowerer = .{},

    pub const Resource = GatedLayerCommandLowerer.Resource;
    pub const BuildOptions = GatedLayerCommandLowerer.BuildOptions;

    pub fn build(self: *PrefillGatedLayerCommandLowerer, options: BuildOptions) !void {
        if (options.rows <= 1) return error.InvalidPrefillRows;
        try self.inner.build(options);
    }

    pub fn view(self: *const PrefillGatedLayerCommandLowerer) PlanView {
        return self.inner.view();
    }

    pub fn commandView(self: *const PrefillGatedLayerCommandLowerer) GraphCommandPlanView {
        return self.inner.commandView();
    }
};

pub const TailCommandLowerer = struct {
    const Resource = enum(usize) {
        input,
        normalized,
        logits,
        token,
    };

    norm_resources: [3]ResourceUse = undefined,
    linear_resources: [3]ResourceUse = undefined,
    argmax_resources: [2]ResourceUse = undefined,
    ops: [3]Op = undefined,
    storage: BoundedPlan(3, 3, 8) = .{},
    command_storage: BoundedGraphCommandPlan(3, 1, 8, 4, 8) = .{},
    scratch_sizes: [4]ScratchSlotSize = undefined,
    scratch_size_count: usize = 0,
    plan_view: PlanView = .{
        .planned_ops = &.{},
        .scopes = &.{},
        .barrier_count = 0,
    },
    command_view: GraphCommandPlanView = .{
        .ops = &.{},
        .resources = &.{},
        .planned_ops = &.{},
        .scopes = &.{},
        .scratch_slots = &.{},
        .barrier_count = 0,
    },

    pub const BuildOptions = struct {
        final_norm_slot: usize,
        lm_head_slot: usize,
        source: usize,
        region: usize,
        hidden_size: usize = 0,
        vocab_size: usize = 0,
        quant_format: quant_matmul.Format = .q8_0,
    };

    pub fn build(self: *TailCommandLowerer, options: BuildOptions) !void {
        self.scratch_size_count = 0;
        self.norm_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.input)), .access = .read },
            .{ .range = .whole(.norm_slot, options.final_norm_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.normalized)), .access = .write },
        };
        self.linear_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.normalized)), .access = .read },
            .{ .range = .whole(.quant_slot, options.lm_head_slot), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.logits)), .access = .write },
        };
        self.argmax_resources = .{
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.logits)), .access = .read },
            .{ .range = .whole(.scratch_slot, @intFromEnum(Resource.token)), .access = .write },
        };
        const lm_head_op = quantOp(options.quant_format, 1, options.hidden_size, options.vocab_size);
        self.ops = .{
            .{ .kind = .tail_final_norm, .source = options.source, .region = options.region, .resources = &self.norm_resources },
            .{
                .kind = .tail_lm_head,
                .source = options.source,
                .region = options.region,
                .resources = &self.linear_resources,
                .quant_matmul = lm_head_op.quant_matmul,
                .operator_plan = lm_head_op.operator_plan,
            },
            .{ .kind = .tail_argmax, .source = options.source, .region = options.region, .resources = &self.argmax_resources },
        };
        self.addScratchSize(@intFromEnum(Resource.input), options.hidden_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.normalized), options.hidden_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.logits), options.vocab_size * @sizeOf(f32));
        self.addScratchSize(@intFromEnum(Resource.token), @sizeOf(u32));
        self.command_view = try self.command_storage.build(&self.ops, .{}, self.scratch_sizes[0..self.scratch_size_count]);
        self.plan_view = self.command_view.planView();
    }

    pub fn view(self: *const TailCommandLowerer) PlanView {
        return self.plan_view;
    }

    pub fn commandView(self: *const TailCommandLowerer) GraphCommandPlanView {
        return self.command_view;
    }

    fn addScratchSize(self: *TailCommandLowerer, slot: usize, bytes: usize) void {
        if (bytes == 0 or self.scratch_size_count >= self.scratch_sizes.len) return;
        self.scratch_sizes[self.scratch_size_count] = .{ .slot = slot, .bytes = bytes };
        self.scratch_size_count += 1;
    }
};

pub const GatedFrameCommandLowerer = struct {
    allocator: std.mem.Allocator,
    frame: GraphCommandPlan,
    has_tail: bool = false,
    final_norm_slot: usize = 0,
    lm_head_slot: usize = 0,
    hidden_size: usize = 0,
    vocab_size: usize = 0,
    tail_quant_format: QuantMatmulFormat = .unknown,

    pub const Layer = struct {
        shares_kv: bool,
        kv_layer_index: usize,
        kv_heads: usize,
        head_dim: usize,
        intermediate_size: usize,
        attn_pre_norm_slot: usize,
        attn_post_norm_slot: usize,
        ffn_pre_norm_slot: usize,
        ffn_post_norm_slot: usize,
        q_head_norm_slot: ?usize,
        k_head_norm_slot: ?usize,
        q_linear_slot: usize,
        k_linear_slot: usize,
        v_linear_slot: usize,
        attention_linear_slot: usize,
        gate_linear_slot: usize,
        up_linear_slot: usize,
        down_linear_slot: usize,
        ple_gate_linear_slot: ?usize,
        ple_proj_linear_slot: ?usize,
        ple_post_norm_slot: ?usize,
        quant_formats: LayerQuantFormats = .{},
    };

    pub const BuildOptions = struct {
        rows: usize,
        hidden_size: usize,
        vocab_size: usize,
        num_attention_heads: usize,
        global_head_dim: usize,
        ple_hidden_size: usize,
        final_norm_slot: usize,
        lm_head_slot: usize,
        tail_quant_format: quant_matmul.Format = .q8_0,
        activation_dtype: ActivationDType = .f32,
        kv_layout: KvLayout = .f32_row_major,
        attention_storage: quant_matmul.AttentionStorage = .paged,
        layers: []const Layer,
        include_tail: bool = true,
        source: usize,
        layer_region: usize,
        tail_source: usize,
        tail_region: usize,
    };

    pub fn init(allocator: std.mem.Allocator) GatedFrameCommandLowerer {
        return .{
            .allocator = allocator,
            .frame = GraphCommandPlan.init(allocator),
        };
    }

    pub fn deinit(self: *GatedFrameCommandLowerer) void {
        self.frame.deinit();
        self.* = undefined;
    }

    pub fn build(self: *GatedFrameCommandLowerer, options: BuildOptions) !void {
        const frame_descriptor = FrameDescriptor.prefill(.{
            .rows = options.rows,
            .hidden_size = options.hidden_size,
            .vocab_size = options.vocab_size,
            .activation_dtype = options.activation_dtype,
            .kv_layout = options.kv_layout,
            .attention_storage = options.attention_storage,
        });
        if (frame_descriptor.rows <= 1) return error.InvalidPrefillRows;
        if (options.layers.len == 0) return error.InvalidPrefillFrame;
        if ((frame_descriptor.activation_dtype != .f32 and frame_descriptor.activation_dtype != .f16) or
            frame_descriptor.kv_layout != .f32_row_major) return error.UnsupportedPrefillFrame;
        self.frame.clearRetainingCapacity();
        self.has_tail = false;
        self.final_norm_slot = options.final_norm_slot;
        self.lm_head_slot = options.lm_head_slot;
        self.hidden_size = options.hidden_size;
        self.vocab_size = options.vocab_size;
        self.tail_quant_format = options.tail_quant_format;

        for (options.layers) |layer| {
            const attention_input_size = std.math.mul(usize, options.num_attention_heads, layer.head_dim) catch return error.InvalidPrefillFrame;
            const kv_dim = std.math.mul(usize, layer.kv_heads, layer.head_dim) catch return error.InvalidPrefillFrame;
            const include_ple = options.ple_hidden_size != 0;
            const ple_gate_slot = if (include_ple) layer.ple_gate_linear_slot orelse return error.UnsupportedPrefillFrame else 0;
            const ple_proj_slot = if (include_ple) layer.ple_proj_linear_slot orelse return error.UnsupportedPrefillFrame else 0;
            const ple_post_slot = if (include_ple) layer.ple_post_norm_slot orelse return error.UnsupportedPrefillFrame else 0;
            var layer_plan = PrefillGatedLayerCommandLowerer{};
            try layer_plan.build(.{
                .shares_kv = layer.shares_kv,
                .has_attention_pre_norm = true,
                .attention_pre_norm_slot = layer.attn_pre_norm_slot,
                .q_linear_slot = layer.q_linear_slot,
                .k_linear_slot = layer.k_linear_slot,
                .v_linear_slot = layer.v_linear_slot,
                .q_head_norm_slot = layer.q_head_norm_slot orelse 0,
                .k_head_norm_slot = layer.k_head_norm_slot orelse 0,
                .attention_layer_index = layer.kv_layer_index,
                .value_norm = options.global_head_dim != 0 and !layer.shares_kv,
                .kv_seed = options.attention_storage == .paged and !layer.shares_kv,
                .quant_formats = layer.quant_formats,
                .activation_dtype = frame_descriptor.activation_dtype,
                .attention_linear_slot = layer.attention_linear_slot,
                .attention_post_norm_slot = layer.attn_post_norm_slot,
                .ffn_pre_norm_slot = layer.ffn_pre_norm_slot,
                .gate_linear_slot = layer.gate_linear_slot,
                .up_linear_slot = layer.up_linear_slot,
                .down_linear_slot = layer.down_linear_slot,
                .ffn_post_norm_slot = layer.ffn_post_norm_slot,
                .include_ple = include_ple,
                .ple_gate_linear_slot = ple_gate_slot,
                .ple_proj_linear_slot = ple_proj_slot,
                .ple_post_norm_slot = ple_post_slot,
                .source = options.source,
                .region = options.layer_region,
                .rows = frame_descriptor.rows,
                .kv_len = frame_descriptor.sequence_length,
                .hidden_size = frame_descriptor.hidden_size,
                .attention_input_size = attention_input_size,
                .kv_dim = kv_dim,
                .head_dim = layer.head_dim,
                .attention_kv_format = .f32,
                .attention_storage = frame_descriptor.attention_storage,
                .intermediate_size = layer.intermediate_size,
                .ple_hidden_size = options.ple_hidden_size,
            });
            try self.frame.append(layer_plan.commandView());
        }

        if (options.include_tail) {
            var tail_plan = TailCommandLowerer{};
            try tail_plan.build(.{
                .final_norm_slot = options.final_norm_slot,
                .lm_head_slot = options.lm_head_slot,
                .source = options.tail_source,
                .region = options.tail_region,
                .hidden_size = options.hidden_size,
                .vocab_size = options.vocab_size,
                .quant_format = options.tail_quant_format,
            });
            self.has_tail = true;
            try self.frame.append(tail_plan.commandView());
        }
    }

    pub fn view(self: *const GatedFrameCommandLowerer) GraphCommandPlanView {
        return self.frame.view();
    }

    pub fn matchesTail(self: *const GatedFrameCommandLowerer, norm_slot: usize, linear_slot: usize, hidden: usize, vocab: usize) bool {
        return self.has_tail and
            self.final_norm_slot == norm_slot and
            self.lm_head_slot == linear_slot and
            self.hidden_size == hidden and
            self.vocab_size == vocab;
    }
};

pub fn build(allocator: std.mem.Allocator, ops: []const Op, options: Options) !Plan {
    var planned_ops = try std.ArrayList(PlannedOp).initCapacity(allocator, ops.len);
    errdefer planned_ops.deinit(allocator);
    var scopes = try std.ArrayList(EncoderScope).initCapacity(allocator, 0);
    errdefer scopes.deinit(allocator);

    var scope_uses = try std.ArrayList(ResourceUse).initCapacity(allocator, 0);
    defer scope_uses.deinit(allocator);

    var current_scope: ?EncoderScope = null;
    var barrier_count: usize = 0;
    const max_ops_per_scope = @max(options.max_ops_per_scope, 1);

    for (ops, 0..) |op, op_index| {
        if (current_scope == null or !scopeCompatible(current_scope.?, op, max_ops_per_scope)) {
            if (current_scope) |scope| try scopes.append(allocator, scope);
            current_scope = .{
                .first_op = op_index,
                .op_count = 0,
                .source = op.source,
                .region = op.region,
            };
            scope_uses.clearRetainingCapacity();
        }

        const barrier_before = needsBarrier(scope_uses.items, op.resources);
        if (barrier_before) {
            current_scope.?.barrier_count += 1;
            barrier_count += 1;
            scope_uses.clearRetainingCapacity();
        }

        try planned_ops.append(allocator, .{
            .kind = op.kind,
            .op_index = op_index,
            .scope_index = scopes.items.len,
            .barrier_before = barrier_before,
        });
        current_scope.?.op_count += 1;
        try scope_uses.appendSlice(allocator, op.resources);
    }

    if (current_scope) |scope| try scopes.append(allocator, scope);

    return .{
        .planned_ops = try planned_ops.toOwnedSlice(allocator),
        .scopes = try scopes.toOwnedSlice(allocator),
        .barrier_count = barrier_count,
    };
}

pub fn buildInto(
    ops: []const Op,
    options: Options,
    planned_ops_buffer: []PlannedOp,
    scopes_buffer: []EncoderScope,
    scope_uses_buffer: []ResourceUse,
) !PlanView {
    var planned_count: usize = 0;
    var scope_count: usize = 0;
    var scope_use_count: usize = 0;
    var current_scope: ?EncoderScope = null;
    var barrier_count: usize = 0;
    const max_ops_per_scope = @max(options.max_ops_per_scope, 1);

    for (ops, 0..) |op, op_index| {
        if (current_scope == null or !scopeCompatible(current_scope.?, op, max_ops_per_scope)) {
            if (current_scope) |scope| {
                if (scope_count >= scopes_buffer.len) return error.OutOfMemory;
                scopes_buffer[scope_count] = scope;
                scope_count += 1;
            }
            current_scope = .{
                .first_op = op_index,
                .op_count = 0,
                .source = op.source,
                .region = op.region,
            };
            scope_use_count = 0;
        }

        const barrier_before = needsBarrier(scope_uses_buffer[0..scope_use_count], op.resources);
        if (barrier_before) {
            current_scope.?.barrier_count += 1;
            barrier_count += 1;
            scope_use_count = 0;
        }

        if (planned_count >= planned_ops_buffer.len) return error.OutOfMemory;
        planned_ops_buffer[planned_count] = .{
            .kind = op.kind,
            .op_index = op_index,
            .scope_index = scope_count,
            .barrier_before = barrier_before,
        };
        planned_count += 1;
        current_scope.?.op_count += 1;

        if (scope_use_count + op.resources.len > scope_uses_buffer.len) return error.OutOfMemory;
        @memcpy(scope_uses_buffer[scope_use_count..][0..op.resources.len], op.resources);
        scope_use_count += op.resources.len;
    }

    if (current_scope) |scope| {
        if (scope_count >= scopes_buffer.len) return error.OutOfMemory;
        scopes_buffer[scope_count] = scope;
        scope_count += 1;
    }

    return .{
        .planned_ops = planned_ops_buffer[0..planned_count],
        .scopes = scopes_buffer[0..scope_count],
        .barrier_count = barrier_count,
    };
}

pub fn buildGraphCommandPlanInto(
    plan_ops: []const Op,
    options: Options,
    scratch_sizes: []const ScratchSlotSize,
    runtime_ops_buffer: []GraphCommandOp,
    resources_buffer: []ResourceUse,
    planned_ops_buffer: []PlannedOp,
    scopes_buffer: []EncoderScope,
    scratch_slots_buffer: []ScratchSlotLifetime,
    scope_uses_buffer: []ResourceUse,
) !GraphCommandPlanView {
    const planned_view = try buildInto(
        plan_ops,
        options,
        planned_ops_buffer,
        scopes_buffer,
        scope_uses_buffer,
    );
    if (plan_ops.len > runtime_ops_buffer.len) return error.OutOfMemory;

    var resource_count: usize = 0;
    var scratch_count: usize = 0;
    for (planned_view.planned_ops, 0..) |planned, planned_index| {
        const op = plan_ops[planned.op_index];
        const resource_start = resource_count;
        if (resource_count + op.resources.len > resources_buffer.len) return error.OutOfMemory;
        @memcpy(resources_buffer[resource_count..][0..op.resources.len], op.resources);
        resource_count += op.resources.len;
        const dtypes = activationDTypesForOp(op.resources, scratch_sizes);
        runtime_ops_buffer[planned_index] = .{
            .kind = op.kind,
            .source = op.source,
            .region = op.region,
            .planned_op_index = planned_index,
            .scope_index = planned.scope_index,
            .barrier_before = planned.barrier_before,
            .resource_start = resource_start,
            .resource_count = op.resources.len,
            .quant_matmul = op.quant_matmul,
            .operator_plan = op.operator_plan,
            .input_dtype = dtypes.input,
            .output_dtype = dtypes.output,
        };

        for (op.resources) |use| {
            if (use.range.kind != .scratch_slot) continue;
            const bytes = scratchBytesForSlot(scratch_sizes, use.range.id);
            if (bytes == 0) continue;
            var existing: ?usize = null;
            for (scratch_slots_buffer[0..scratch_count], 0..) |scratch, index| {
                if (scratch.slot == use.range.id) {
                    existing = index;
                    break;
                }
            }
            if (existing) |index| {
                if (bytes > scratch_slots_buffer[index].bytes) scratch_slots_buffer[index].bytes = bytes;
                const dtype = scratchDTypeForSlot(scratch_sizes, use.range.id);
                if (dtype != scratch_slots_buffer[index].dtype) scratch_slots_buffer[index].dtype = .f32;
                if (planned_index < scratch_slots_buffer[index].first_op) scratch_slots_buffer[index].first_op = planned_index;
                if (planned_index > scratch_slots_buffer[index].last_op) scratch_slots_buffer[index].last_op = planned_index;
            } else {
                if (scratch_count >= scratch_slots_buffer.len) return error.OutOfMemory;
                scratch_slots_buffer[scratch_count] = .{
                    .slot = use.range.id,
                    .bytes = bytes,
                    .dtype = scratchDTypeForSlot(scratch_sizes, use.range.id),
                    .first_op = planned_index,
                    .last_op = planned_index,
                };
                scratch_count += 1;
            }
        }
    }

    return .{
        .ops = runtime_ops_buffer[0..planned_view.planned_ops.len],
        .resources = resources_buffer[0..resource_count],
        .planned_ops = planned_view.planned_ops,
        .scopes = planned_view.scopes,
        .scratch_slots = scratch_slots_buffer[0..scratch_count],
        .barrier_count = planned_view.barrier_count,
    };
}

fn scratchBytesForSlot(scratch_sizes: []const ScratchSlotSize, slot: usize) usize {
    for (scratch_sizes) |scratch| {
        if (scratch.slot == slot) return scratch.bytes;
    }
    return 0;
}

fn scratchDTypeForSlot(scratch_sizes: []const ScratchSlotSize, slot: usize) ActivationDType {
    for (scratch_sizes) |scratch| {
        if (scratch.slot == slot) return scratch.dtype;
    }
    return .f32;
}

const ActivationDTypes = struct {
    input: ActivationDType = .f32,
    output: ActivationDType = .f32,
};

fn activationDTypesForOp(resources: []const ResourceUse, scratch_sizes: []const ScratchSlotSize) ActivationDTypes {
    var result = ActivationDTypes{};
    var have_input = false;
    var have_output = false;
    for (resources) |use| {
        if (use.range.kind != .scratch_slot) continue;
        const dtype = scratchDTypeForSlot(scratch_sizes, use.range.id);
        if (use.access.reads()) {
            result.input = mergeActivationDType(result.input, dtype, have_input);
            have_input = true;
        }
        if (use.access.writes()) {
            result.output = mergeActivationDType(result.output, dtype, have_output);
            have_output = true;
        }
    }
    return result;
}

fn mergeActivationDType(current: ActivationDType, incoming: ActivationDType, have_current: bool) ActivationDType {
    if (!have_current) return incoming;
    if (current == incoming) return current;
    return .f32;
}

fn scopeCompatible(scope: EncoderScope, op: Op, max_ops_per_scope: usize) bool {
    return scope.op_count < max_ops_per_scope and
        scope.source == op.source and
        scope.region == op.region;
}

fn needsBarrier(prior: []const ResourceUse, current: []const ResourceUse) bool {
    for (prior) |prev_use| {
        if (!prev_use.access.writes()) continue;
        for (current) |use| {
            if (!use.access.reads()) continue;
            if (!prev_use.range.overlaps(use.range)) continue;
            return true;
        }
    }
    return false;
}

test "metal command planner frame descriptor models execution frames generically" {
    const prefill = FrameDescriptor.prefill(.{
        .rows = 8,
        .hidden_size = 2048,
        .vocab_size = 262144,
        .activation_dtype = .bf16,
        .attention_storage = .dense,
    });
    try std.testing.expectEqual(FrameMode.prefill, prefill.mode);
    try std.testing.expectEqual(@as(usize, 8), prefill.rows);
    try std.testing.expectEqual(@as(usize, 8), prefill.query_length);
    try std.testing.expectEqual(@as(usize, 8), prefill.sequence_length);
    try std.testing.expectEqual(@as(usize, 2048), prefill.hidden_size);
    try std.testing.expectEqual(@as(usize, 262144), prefill.vocab_size);
    try std.testing.expectEqual(ActivationDType.bf16, prefill.activation_dtype);
    try std.testing.expectEqual(quant_matmul.AttentionStorage.dense, prefill.attention_storage);

    const decode = FrameDescriptor.decode(.{
        .hidden_size = 2048,
        .vocab_size = 262144,
        .sequence_length = 129,
    });
    try std.testing.expectEqual(FrameMode.decode, decode.mode);
    try std.testing.expectEqual(@as(usize, 1), decode.rows);
    try std.testing.expectEqual(@as(usize, 1), decode.query_length);
    try std.testing.expectEqual(@as(usize, 129), decode.sequence_length);
    try std.testing.expectEqual(ActivationDType.f32, decode.activation_dtype);
    try std.testing.expectEqual(KvLayout.f32_row_major, decode.kv_layout);
}

test "metal command planner groups compatible independent ops" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 1, 0, 64), .access = .read },
            .{ .range = .bytes(.buffer, 2, 0, 64), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 3, 0, 64), .access = .read },
            .{ .range = .bytes(.buffer, 4, 0, 64), .access = .write },
        } },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 2), plan.scopes[0].op_count);
    try std.testing.expectEqual(@as(usize, 0), plan.barrier_count);
}

test "metal command planner inserts dependency barrier inside a compatible scope" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 7, 0, 128), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 7, 64, 32), .access = .read },
        } },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 1), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
}

test "metal command planner does not barrier for ordered overwrite hazards" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .read },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .read },
        } },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 1), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(!plan.planned_ops[1].barrier_before);
    try std.testing.expect(!plan.planned_ops[2].barrier_before);
    try std.testing.expect(plan.planned_ops[3].barrier_before);
}

test "metal command planner barrier resets prior hazards for later sibling consumers" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .write },
            .{ .range = .whole(.buffer, 11), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .read },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 11), .access = .read },
        } },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 1), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(!plan.planned_ops[2].barrier_before);
}

test "metal command planner builds into caller buffers" {
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .read },
        } },
    };
    var planned_ops: [2]PlannedOp = undefined;
    var scopes: [2]EncoderScope = undefined;
    var uses: [2]ResourceUse = undefined;

    const plan = try buildInto(&ops, .{}, &planned_ops, &scopes, &uses);

    try std.testing.expectEqual(@as(usize, 2), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 1), plan.barrier_count);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
}

test "metal command planner bounded plan owns hot-path storage" {
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .whole(.buffer, 10), .access = .read },
        } },
    };
    var bounded = BoundedPlan(2, 2, 2){};

    const plan = try bounded.build(&ops, .{});

    try std.testing.expectEqual(@as(usize, 2), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 1), plan.barrier_count);
    try std.testing.expectEqual(plan.planned_ops.ptr, bounded.view().planned_ops.ptr);
}

test "metal command planner handles row-1 attention setup dependency shape" {
    var setup_plan = AttentionSetupCommandLowerer{};

    try setup_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = true,
        .attention_pre_norm_slot = 1,
        .q_linear_slot = 10,
        .k_linear_slot = 11,
        .v_linear_slot = 12,
        .q_head_norm_slot = 2,
        .k_head_norm_slot = 3,
        .attention_layer_index = 0,
        .value_norm = true,
        .source = 1,
        .region = 2,
        .rows = 10,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
    });

    const plan = setup_plan.view();
    const command = setup_plan.commandView();

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 2), plan.barrier_count);
    try std.testing.expectEqual(plan.planned_ops.len, command.ops.len);
    try std.testing.expectEqual(plan.barrier_count, command.barrier_count);
    try std.testing.expectEqual(@as(usize, 5), command.ops.len);
    try std.testing.expect(command.scratch_slots.len > 0);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(plan.planned_ops[2].barrier_before);
    try std.testing.expect(!plan.planned_ops[3].barrier_before);
    try std.testing.expect(!plan.planned_ops[4].barrier_before);
    var found_q = false;
    for (command.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(AttentionSetupCommandLowerer.Resource.q)) {
            found_q = true;
            try std.testing.expectEqual(@as(usize, 10 * 2048 * @sizeOf(f32)), scratch.bytes);
            try std.testing.expectEqual(@as(usize, 1), scratch.first_op);
            try std.testing.expectEqual(@as(usize, 2), scratch.last_op);
        }
    }
    try std.testing.expect(found_q);
}

test "metal command planner builds shared-kv row-1 attention setup" {
    var setup_plan = AttentionSetupCommandLowerer{};

    try setup_plan.build(.{
        .shares_kv = true,
        .has_attention_pre_norm = true,
        .attention_pre_norm_slot = 1,
        .q_linear_slot = 10,
        .k_linear_slot = 0,
        .v_linear_slot = 0,
        .q_head_norm_slot = 2,
        .k_head_norm_slot = 0,
        .attention_layer_index = 0,
        .value_norm = false,
        .source = 1,
        .region = 2,
    });

    const plan = setup_plan.view();

    try std.testing.expectEqual(@as(usize, 3), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 2), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(plan.planned_ops[2].barrier_before);
}

test "metal command planner handles row-1 attention projection dependency shape" {
    var project_plan = AttentionProjectCommandLowerer{};

    try project_plan.build(.{
        .output_linear_slot = 20,
        .post_norm_slot = 4,
        .source = 3,
        .region = 1,
    });

    const plan = project_plan.view();

    try std.testing.expectEqual(@as(usize, 3), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 2), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(plan.planned_ops[2].barrier_before);
}

test "metal command planner handles row-1 ffn ple dependency shape" {
    var ffn_plan = FfnPleCommandLowerer{};

    try ffn_plan.build(.{
        .pre_gate_norm_slot = 5,
        .gate_linear_slot = 30,
        .up_linear_slot = 31,
        .down_linear_slot = 32,
        .post_down_norm_slot = 6,
        .ple_gate_linear_slot = 33,
        .ple_proj_linear_slot = 34,
        .ple_post_norm_slot = 7,
        .source = 6,
        .region = 3,
    });

    const plan = ffn_plan.view();

    try std.testing.expectEqual(@as(usize, 7), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 6), plan.barrier_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(plan.planned_ops[2].barrier_before);
    try std.testing.expect(plan.planned_ops[3].barrier_before);
    try std.testing.expect(plan.planned_ops[4].barrier_before);
    try std.testing.expect(plan.planned_ops[5].barrier_before);
    try std.testing.expect(plan.planned_ops[6].barrier_before);
}

test "metal command planner handles full quantized row-1 layer dependency shape" {
    var layer_plan = GatedLayerCommandLowerer{};

    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = true,
        .attention_pre_norm_slot = 11,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .kv_len = 17,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .head_dim = 128,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    });

    const plan = layer_plan.view();
    const command = layer_plan.commandView();

    try std.testing.expectEqual(@as(usize, 15), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 12), plan.barrier_count);
    try std.testing.expectEqual(plan.planned_ops.len, command.ops.len);
    try std.testing.expectEqual(plan.planned_ops.len, command.planned_ops.len);
    try std.testing.expectEqual(plan.scopes.len, command.scopes.len);
    try std.testing.expectEqual(plan.barrier_count, command.barrier_count);
    var saw_attention_plan = false;
    for (command.ops) |op| {
        if (op.kind == .attention) {
            const attention = op.operator_plan.?.attention;
            try std.testing.expectEqual(@as(usize, 1), attention.q_len);
            try std.testing.expectEqual(@as(usize, 17), attention.kv_len);
            try std.testing.expectEqual(quant_matmul.AttentionKvFormat.f32, attention.kv_format);
            try std.testing.expectEqual(quant_matmul.AttentionStorage.dense, attention.storage);
            saw_attention_plan = true;
        }
    }
    try std.testing.expect(saw_attention_plan);
    try std.testing.expect(command.resources.len > plan.planned_ops.len);
    try std.testing.expect(command.scratch_slots.len > 0);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expectEqual(OpKind.attention_pre_norm, plan.planned_ops[0].kind);
    try std.testing.expectEqual(OpKind.attention_pre_norm, command.ops[0].kind);
    try std.testing.expectEqual(@as(usize, 0), command.ops[0].planned_op_index);
    try std.testing.expectEqual(@as(usize, 0), command.ops[0].scope_index);
    try std.testing.expectEqual(@as(usize, 0), command.ops[0].resource_start);
    try std.testing.expectEqual(@as(usize, 3), command.ops[0].resource_count);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expectEqual(OpKind.qkv_linear, plan.planned_ops[1].kind);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mmv, command.ops[1].quant_matmul.?.dispatch);
    try std.testing.expectEqual(Operator.mul_mv, command.ops[1].operator_plan.?.operator());
    try std.testing.expectEqual(OpKind.attention, command.ops[5].kind);
    try std.testing.expectEqual(Operator.attention_flash, command.ops[5].operator_plan.?.operator());
    try std.testing.expect(plan.planned_ops[2].barrier_before);
    try std.testing.expectEqual(OpKind.q_head_norm_rope, plan.planned_ops[2].kind);
    try std.testing.expect(!plan.planned_ops[3].barrier_before);
    try std.testing.expectEqual(OpKind.k_head_norm_rope, plan.planned_ops[3].kind);
    try std.testing.expect(!plan.planned_ops[4].barrier_before);
    try std.testing.expectEqual(OpKind.v_norm, plan.planned_ops[4].kind);
    try std.testing.expect(plan.planned_ops[5].barrier_before);
    try std.testing.expectEqual(OpKind.attention, plan.planned_ops[5].kind);
    const expected_tail_kinds = [_]OpKind{
        .attention_output_linear,
        .attention_post_norm_residual,
        .ffn_pre_norm_scale,
        .ffn_gate_up_activation,
        .ffn_down_linear,
        .ffn_post_norm_residual,
        .ple_gate_activation,
        .ple_projection,
        .ple_post_norm_residual,
    };
    for (expected_tail_kinds, 0..) |expected, offset| {
        try std.testing.expectEqual(expected, plan.planned_ops[6 + offset].kind);
    }
    for (plan.planned_ops[6..]) |planned| {
        try std.testing.expect(planned.barrier_before);
    }
    var found_ffn_scratch = false;
    for (command.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(GatedLayerCommandLowerer.Resource.ffn_gated)) {
            found_ffn_scratch = true;
            try std.testing.expectEqual(@as(usize, 8192 * @sizeOf(f32)), scratch.bytes);
            try std.testing.expectEqual(@as(usize, 9), scratch.first_op);
            try std.testing.expectEqual(@as(usize, 10), scratch.last_op);
        }
    }
    try std.testing.expect(found_ffn_scratch);
}

test "metal command planner keeps paged f32 attention distinct from dense flash" {
    var layer_plan = GatedLayerCommandLowerer{};

    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = true,
        .attention_pre_norm_slot = 11,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .kv_len = 17,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .head_dim = 128,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
        .attention_storage = .paged,
    });

    const command = layer_plan.commandView();
    try std.testing.expectEqual(OpKind.attention, command.ops[5].kind);
    const attention = command.ops[5].operator_plan.?.attention;
    try std.testing.expectEqual(Operator.attention_paged, attention.operator);
    try std.testing.expectEqual(quant_matmul.AttentionKvFormat.f32, attention.kv_format);
    try std.testing.expectEqual(quant_matmul.AttentionStorage.paged, attention.storage);
}

test "metal command planner sizes quantized prefill layer scratch for row batches" {
    var layer_plan = PrefillGatedLayerCommandLowerer{};

    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .rows = 10,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    });

    const command = layer_plan.commandView();
    try std.testing.expectEqual(@as(usize, 14), command.ops.len);
    try std.testing.expectEqual(@as(usize, 1), command.scopes.len);
    try std.testing.expectEqual(@as(usize, 11), command.barrier_count);
    try std.testing.expectEqual(OpKind.qkv_linear, command.ops[0].kind);
    try std.testing.expectEqual(OpKind.ple_post_norm_residual, command.ops[13].kind);
    var found_q = false;
    var found_ffn = false;
    var found_ple = false;
    for (command.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.q_ready)) {
            found_q = true;
            try std.testing.expectEqual(@as(usize, 10 * 2048 * @sizeOf(f32)), scratch.bytes);
        }
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.ffn_gated)) {
            found_ffn = true;
            try std.testing.expectEqual(@as(usize, 10 * 8192 * @sizeOf(f32)), scratch.bytes);
        }
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.ple_gated)) {
            found_ple = true;
            try std.testing.expectEqual(@as(usize, 10 * 1024 * @sizeOf(f32)), scratch.bytes);
        }
    }
    try std.testing.expect(found_q);
    try std.testing.expect(found_ffn);
    try std.testing.expect(found_ple);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mm, command.ops[0].quant_matmul.?.dispatch);
    try std.testing.expectEqual(Operator.mul_mm, command.ops[0].operator_plan.?.operator());
}

test "metal command planner uses small-batch quant dispatch for short prefill rows" {
    inline for ([_]usize{ 2, 8 }) |rows| {
        var layer_plan = PrefillGatedLayerCommandLowerer{};

        try layer_plan.build(.{
            .shares_kv = false,
            .has_attention_pre_norm = false,
            .attention_pre_norm_slot = 0,
            .q_linear_slot = 21,
            .k_linear_slot = 22,
            .v_linear_slot = 23,
            .q_head_norm_slot = 31,
            .k_head_norm_slot = 32,
            .attention_layer_index = 4,
            .value_norm = true,
            .attention_linear_slot = 24,
            .attention_post_norm_slot = 12,
            .ffn_pre_norm_slot = 13,
            .gate_linear_slot = 25,
            .up_linear_slot = 26,
            .down_linear_slot = 27,
            .ffn_post_norm_slot = 14,
            .ple_gate_linear_slot = 28,
            .ple_proj_linear_slot = 29,
            .ple_post_norm_slot = 15,
            .source = 11,
            .region = 7,
            .rows = rows,
            .hidden_size = 2048,
            .attention_input_size = 2048,
            .kv_dim = 512,
            .intermediate_size = 8192,
            .ple_hidden_size = 1024,
        });

        const command = layer_plan.commandView();
        var quant_ops: usize = 0;
        for (command.ops) |op| {
            if (op.quant_matmul) |quant| {
                quant_ops += 1;
                try std.testing.expectEqual(QuantMatmulDispatchKind.small_batch, quant.dispatch);
                try std.testing.expectEqual(quant_matmul.Format.q8_0, quant.format);
                try std.testing.expectEqual(Operator.mul_mv_ext, op.operator_plan.?.operator());
            }
        }
        try std.testing.expectEqual(@as(usize, 6), quant_ops);
    }
}

test "metal command planner represents dense f32 prefill linears without quant fallback" {
    var layer_plan = PrefillGatedLayerCommandLowerer{};

    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .quant_formats = .{
            .q = .f32,
            .k = .f32,
            .v = .f32,
            .attention_output = .f32,
            .gate = .f32,
            .up = .f32,
            .down = .f32,
            .ple_gate = .f32,
            .ple_projection = .f32,
        },
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .rows = 10,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    });

    const command = layer_plan.commandView();
    try std.testing.expectEqual(QuantMatmulFormat.f32, command.ops[0].quant_matmul.?.format);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mm, command.ops[0].quant_matmul.?.dispatch);
    try std.testing.expectEqual(Operator.mul_mm, command.ops[0].operator_plan.?.operator());
    try std.testing.expectEqual(QuantMatmulFormat.f32, command.ops[8].quant_matmul.?.format);
    try std.testing.expectEqual(QuantMatmulFormat.f32, command.ops[12].quant_matmul.?.format);
}

test "metal command planner can size FFN intermediates as f16 resident scratch" {
    var layer_plan = PrefillGatedLayerCommandLowerer{};

    try layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .activation_dtype = .f16,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .rows = 10,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    });

    const command = layer_plan.commandView();
    var found_ffn_gated = false;
    var found_ffn_projected = false;
    var found_attention = false;
    var found_gate_up_command = false;
    var found_down_command = false;
    for (command.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.ffn_gated)) {
            found_ffn_gated = true;
            try std.testing.expectEqual(ActivationDType.f16, scratch.dtype);
            try std.testing.expectEqual(@as(usize, 10 * 8192 * @sizeOf(u16)), scratch.bytes);
        }
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.ffn_projected)) {
            found_ffn_projected = true;
            try std.testing.expectEqual(ActivationDType.f32, scratch.dtype);
            try std.testing.expectEqual(@as(usize, 10 * 2048 * @sizeOf(f32)), scratch.bytes);
        }
        if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.attention_output)) {
            found_attention = true;
            try std.testing.expectEqual(ActivationDType.f32, scratch.dtype);
            try std.testing.expectEqual(@as(usize, 10 * 2048 * @sizeOf(f32)), scratch.bytes);
        }
    }
    for (command.ops) |op| {
        if (op.kind == .ffn_gate_up_activation) {
            found_gate_up_command = true;
            try std.testing.expectEqual(ActivationDType.f32, op.input_dtype);
            try std.testing.expectEqual(ActivationDType.f16, op.output_dtype);
        }
        if (op.kind == .ffn_down_linear) {
            found_down_command = true;
            try std.testing.expectEqual(ActivationDType.f16, op.input_dtype);
            try std.testing.expectEqual(ActivationDType.f32, op.output_dtype);
        }
    }
    try std.testing.expect(found_ffn_gated);
    try std.testing.expect(found_ffn_projected);
    try std.testing.expect(found_attention);
    try std.testing.expect(found_gate_up_command);
    try std.testing.expect(found_down_command);
}

test "metal command planner preserves f16 activation dtypes when appending layers into a frame" {
    var frame_plan = GatedFrameCommandLowerer.init(std.testing.allocator);
    defer frame_plan.deinit();

    const layers = [_]GatedFrameCommandLowerer.Layer{.{
        .shares_kv = false,
        .kv_layer_index = 0,
        .kv_heads = 4,
        .head_dim = 128,
        .intermediate_size = 8192,
        .attn_pre_norm_slot = 11,
        .attn_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .ffn_post_norm_slot = 14,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .attention_linear_slot = 24,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
    }};

    try frame_plan.build(.{
        .rows = 10,
        .hidden_size = 2048,
        .vocab_size = 32000,
        .num_attention_heads = 16,
        .global_head_dim = 128,
        .ple_hidden_size = 1024,
        .final_norm_slot = 40,
        .lm_head_slot = 41,
        .include_tail = false,
        .activation_dtype = .f16,
        .layers = &layers,
        .source = 11,
        .layer_region = 7,
        .tail_source = 12,
        .tail_region = 8,
    });

    const command = frame_plan.view();
    var found_gate_up_command = false;
    var found_down_command = false;
    for (command.ops) |op| {
        if (op.kind == .ffn_gate_up_activation) {
            found_gate_up_command = true;
            try std.testing.expectEqual(ActivationDType.f32, op.input_dtype);
            try std.testing.expectEqual(ActivationDType.f16, op.output_dtype);
        }
        if (op.kind == .ffn_down_linear) {
            found_down_command = true;
            try std.testing.expectEqual(ActivationDType.f16, op.input_dtype);
            try std.testing.expectEqual(ActivationDType.f32, op.output_dtype);
        }
    }
    try std.testing.expect(found_gate_up_command);
    try std.testing.expect(found_down_command);
}

test "metal command planner keeps unsupported f16 FFN descriptors in f32 scratch" {
    inline for ([_]struct {
        name: []const u8,
        rows: usize,
        formats: LayerQuantFormats,
    }{
        .{
            .name = "short-row small-batch",
            .rows = 8,
            .formats = .{},
        },
        .{
            .name = "non-q8 gate",
            .rows = 10,
            .formats = .{ .gate = .q4_0 },
        },
    }) |case| {
        _ = case.name;
        var layer_plan = PrefillGatedLayerCommandLowerer{};

        try layer_plan.build(.{
            .shares_kv = false,
            .has_attention_pre_norm = false,
            .attention_pre_norm_slot = 0,
            .q_linear_slot = 21,
            .k_linear_slot = 22,
            .v_linear_slot = 23,
            .q_head_norm_slot = 31,
            .k_head_norm_slot = 32,
            .attention_layer_index = 4,
            .value_norm = true,
            .activation_dtype = .f16,
            .attention_linear_slot = 24,
            .attention_post_norm_slot = 12,
            .ffn_pre_norm_slot = 13,
            .gate_linear_slot = 25,
            .up_linear_slot = 26,
            .down_linear_slot = 27,
            .ffn_post_norm_slot = 14,
            .ple_gate_linear_slot = 28,
            .ple_proj_linear_slot = 29,
            .ple_post_norm_slot = 15,
            .source = 11,
            .region = 7,
            .rows = case.rows,
            .hidden_size = 2048,
            .attention_input_size = 2048,
            .kv_dim = 512,
            .intermediate_size = 8192,
            .ple_hidden_size = 1024,
            .quant_formats = case.formats,
        });

        const command = layer_plan.commandView();
        var found_ffn_gated = false;
        var found_gate_up_command = false;
        var found_down_command = false;
        for (command.scratch_slots) |scratch| {
            if (scratch.slot == @intFromEnum(PrefillGatedLayerCommandLowerer.Resource.ffn_gated)) {
                found_ffn_gated = true;
                try std.testing.expectEqual(ActivationDType.f32, scratch.dtype);
                try std.testing.expectEqual(case.rows * 8192 * @sizeOf(f32), scratch.bytes);
            }
        }
        for (command.ops) |op| {
            if (op.kind == .ffn_gate_up_activation) {
                found_gate_up_command = true;
                try std.testing.expectEqual(ActivationDType.f32, op.input_dtype);
                try std.testing.expectEqual(ActivationDType.f32, op.output_dtype);
            }
            if (op.kind == .ffn_down_linear) {
                found_down_command = true;
                try std.testing.expectEqual(ActivationDType.f32, op.input_dtype);
                try std.testing.expectEqual(ActivationDType.f32, op.output_dtype);
            }
        }
        try std.testing.expect(found_ffn_gated);
        try std.testing.expect(found_gate_up_command);
        try std.testing.expect(found_down_command);
    }
}

test "metal command planner rejects single-row prefill layer lowerer" {
    var layer_plan = PrefillGatedLayerCommandLowerer{};

    try std.testing.expectError(error.InvalidPrefillRows, layer_plan.build(.{
        .shares_kv = false,
        .has_attention_pre_norm = false,
        .attention_pre_norm_slot = 0,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .rows = 1,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    }));
}

test "metal command planner handles row-1 tail dependency shape" {
    var tail_plan = TailCommandLowerer{};

    try tail_plan.build(.{
        .final_norm_slot = 8,
        .lm_head_slot = 40,
        .source = 8,
        .region = 5,
        .hidden_size = 2048,
        .vocab_size = 262144,
    });

    const plan = tail_plan.view();
    const command = tail_plan.commandView();

    try std.testing.expectEqual(@as(usize, 3), plan.planned_ops.len);
    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 2), plan.barrier_count);
    try std.testing.expectEqual(plan.planned_ops.len, command.ops.len);
    try std.testing.expectEqual(plan.barrier_count, command.barrier_count);
    try std.testing.expectEqual(@as(usize, 8), command.resources.len);
    try std.testing.expectEqual(OpKind.tail_final_norm, command.ops[0].kind);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mmv, command.ops[1].quant_matmul.?.dispatch);
    try std.testing.expectEqual(Operator.mul_mv, command.ops[1].operator_plan.?.operator());
    try std.testing.expectEqual(@as(usize, 3), command.ops[0].resource_count);
    try std.testing.expect(!plan.planned_ops[0].barrier_before);
    try std.testing.expect(plan.planned_ops[1].barrier_before);
    try std.testing.expect(plan.planned_ops[2].barrier_before);
    var found_logits = false;
    for (command.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(TailCommandLowerer.Resource.logits)) {
            found_logits = true;
            try std.testing.expectEqual(@as(usize, 262144 * @sizeOf(f32)), scratch.bytes);
            try std.testing.expectEqual(@as(usize, 1), scratch.first_op);
            try std.testing.expectEqual(@as(usize, 2), scratch.last_op);
        }
    }
    try std.testing.expect(found_logits);
}

test "metal command planner appends graph command plans into a frame plan" {
    const allocator = std.testing.allocator;
    var first = GatedLayerCommandLowerer{};
    var second = GatedLayerCommandLowerer{};

    const options = GatedLayerCommandLowerer.BuildOptions{
        .shares_kv = false,
        .has_attention_pre_norm = true,
        .attention_pre_norm_slot = 11,
        .q_linear_slot = 21,
        .k_linear_slot = 22,
        .v_linear_slot = 23,
        .q_head_norm_slot = 31,
        .k_head_norm_slot = 32,
        .attention_layer_index = 4,
        .value_norm = true,
        .attention_linear_slot = 24,
        .attention_post_norm_slot = 12,
        .ffn_pre_norm_slot = 13,
        .gate_linear_slot = 25,
        .up_linear_slot = 26,
        .down_linear_slot = 27,
        .ffn_post_norm_slot = 14,
        .ple_gate_linear_slot = 28,
        .ple_proj_linear_slot = 29,
        .ple_post_norm_slot = 15,
        .source = 11,
        .region = 7,
        .hidden_size = 2048,
        .attention_input_size = 2048,
        .kv_dim = 512,
        .head_dim = 128,
        .intermediate_size = 8192,
        .ple_hidden_size = 1024,
    };
    try first.build(options);
    try second.build(options);

    var frame = GraphCommandPlan.init(allocator);
    defer frame.deinit();
    try frame.append(first.commandView());
    try frame.append(second.commandView());

    const view = frame.view();
    try std.testing.expectEqual(@as(usize, 30), view.ops.len);
    try std.testing.expectEqual(@as(usize, 2), view.scopes.len);
    try std.testing.expectEqual(@as(usize, 24), view.barrier_count);
    try std.testing.expectEqual(@as(usize, 15), view.scopes[1].first_op);
    try std.testing.expectEqual(@as(usize, 15), view.ops[15].planned_op_index);
    try std.testing.expectEqual(@as(usize, 1), view.ops[15].scope_index);
    try std.testing.expect(view.ops[15].resource_start >= first.commandView().resources.len);
    var found_ffn_scratch = false;
    for (view.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(GatedLayerCommandLowerer.Resource.ffn_gated)) {
            found_ffn_scratch = true;
            try std.testing.expectEqual(@as(usize, 8192 * @sizeOf(f32)), scratch.bytes);
            try std.testing.expectEqual(@as(usize, 9), scratch.first_op);
            try std.testing.expectEqual(@as(usize, 25), scratch.last_op);
        }
    }
    try std.testing.expect(found_ffn_scratch);
}

test "metal command planner carries generic operator records and fallback stats" {
    const get_rows_resources = [_]ResourceUse{
        .{ .range = .whole(.quant_slot, 1), .access = .read },
        .{ .range = .whole(.buffer, 10), .access = .write },
    };
    const set_rows_resources = [_]ResourceUse{
        .{ .range = .whole(.buffer, 10), .access = .read },
        .{ .range = .whole(.quant_slot, 1), .access = .write },
    };
    const fallback_set_rows_resources = [_]ResourceUse{
        .{ .range = .whole(.buffer, 11), .access = .read },
        .{ .range = .whole(.quant_slot, 6), .access = .write },
    };
    const copy_resources = [_]ResourceUse{
        .{ .range = .whole(.quant_slot, 2), .access = .read },
        .{ .range = .whole(.scratch_slot, 3), .access = .write },
    };
    const quantize_resources = [_]ResourceUse{
        .{ .range = .whole(.scratch_slot, 4), .access = .read },
        .{ .range = .whole(.quant_slot, 2), .access = .write },
    };
    const flash_attention_resources = [_]ResourceUse{
        .{ .range = .whole(.buffer, 20), .access = .read },
        .{ .range = .whole(.kv_cache, 4), .access = .read },
        .{ .range = .whole(.buffer, 21), .access = .write },
    };
    const quant_attention_resources = [_]ResourceUse{
        .{ .range = .whole(.buffer, 22), .access = .read },
        .{ .range = .whole(.kv_cache, 5), .access = .read },
        .{ .range = .whole(.buffer, 23), .access = .write },
    };
    const ops = [_]Op{
        quantRowOp(.get_rows, .q8_0, 4, 2048, 1, 0, &get_rows_resources),
        quantRowOp(.set_rows, .q8_0, 4, 2048, 1, 0, &set_rows_resources),
        quantRowOp(.set_rows, .iq4_nl, 4, 2048, 1, 0, &fallback_set_rows_resources),
        quantCopyOp(.q_to_f32, .q8_0, 4, 2048, 1, 0, &copy_resources),
        quantCopyOp(.f32_to_q, .q8_0, 4, 2048, 1, 0, &quantize_resources),
        attentionOp(128, 128, 256, .f32, 2, 0, &flash_attention_resources),
        attentionOp(1, 128, 256, .polar4, 2, 0, &quant_attention_resources),
    };

    var command_plan = BoundedGraphCommandPlan(7, 4, 16, 0, 16){};
    const command = try command_plan.build(&ops, .{}, &.{});
    const stats = command.operatorStats();

    try std.testing.expectEqual(@as(usize, 7), command.ops.len);
    try std.testing.expectEqual(OpKind.quant_get_rows, command.ops[0].kind);
    try std.testing.expectEqual(OpKind.quant_set_rows, command.ops[1].kind);
    try std.testing.expectEqual(OpKind.quant_set_rows, command.ops[2].kind);
    try std.testing.expectEqual(OpKind.quant_copy_q_to_f32, command.ops[3].kind);
    try std.testing.expectEqual(OpKind.quant_copy_f32_to_q, command.ops[4].kind);
    try std.testing.expectEqual(OpKind.attention_flash, command.ops[5].kind);
    try std.testing.expectEqual(OpKind.attention_quantized_kv, command.ops[6].kind);
    try std.testing.expectEqual(Operator.get_rows, command.ops[0].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.set_rows, command.ops[1].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.fallback, command.ops[2].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.cpy_q_to_f32, command.ops[3].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.cpy_f32_to_q, command.ops[4].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.attention_flash, command.ops[5].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.attention_quantized_kv, command.ops[6].operator_plan.?.operator());
    try std.testing.expectEqual(@as(usize, 7), stats.total);
    try std.testing.expectEqual(@as(usize, 1), stats.fallback);
    try std.testing.expectEqual(@as(usize, 1), stats.get_rows);
    try std.testing.expectEqual(@as(usize, 1), stats.set_rows);
    try std.testing.expectEqual(@as(usize, 1), stats.cpy_q_to_f32);
    try std.testing.expectEqual(@as(usize, 1), stats.cpy_f32_to_q);
    try std.testing.expectEqual(@as(usize, 1), stats.attention_flash);
    try std.testing.expectEqual(@as(usize, 1), stats.attention_quantized_kv);
    try std.testing.expect(stats.hasFallback());
}

test "metal command planner builds quantized prefill graph command plan before encoding" {
    const allocator = std.testing.allocator;
    const layers = [_]GatedFrameCommandLowerer.Layer{
        .{
            .shares_kv = false,
            .kv_layer_index = 0,
            .kv_heads = 4,
            .head_dim = 128,
            .intermediate_size = 8192,
            .attn_pre_norm_slot = 10,
            .attn_post_norm_slot = 11,
            .ffn_pre_norm_slot = 12,
            .ffn_post_norm_slot = 13,
            .q_head_norm_slot = 30,
            .k_head_norm_slot = 31,
            .q_linear_slot = 20,
            .k_linear_slot = 21,
            .v_linear_slot = 22,
            .attention_linear_slot = 23,
            .gate_linear_slot = 24,
            .up_linear_slot = 25,
            .down_linear_slot = 26,
            .ple_gate_linear_slot = 27,
            .ple_proj_linear_slot = 28,
            .ple_post_norm_slot = 14,
        },
        .{
            .shares_kv = true,
            .kv_layer_index = 0,
            .kv_heads = 4,
            .head_dim = 128,
            .intermediate_size = 8192,
            .attn_pre_norm_slot = 40,
            .attn_post_norm_slot = 41,
            .ffn_pre_norm_slot = 42,
            .ffn_post_norm_slot = 43,
            .q_head_norm_slot = 60,
            .k_head_norm_slot = null,
            .q_linear_slot = 50,
            .k_linear_slot = 0,
            .v_linear_slot = 0,
            .attention_linear_slot = 53,
            .gate_linear_slot = 54,
            .up_linear_slot = 55,
            .down_linear_slot = 56,
            .ple_gate_linear_slot = 57,
            .ple_proj_linear_slot = 58,
            .ple_post_norm_slot = 44,
            .quant_formats = .{
                .q = .q4_k,
                .k = .q4_k,
                .v = .q4_k,
                .attention_output = .q4_k,
                .gate = .q4_k,
                .up = .q4_k,
                .down = .q4_k,
                .ple_gate = .q4_k,
                .ple_projection = .q4_k,
            },
        },
    };
    var frame_plan = GatedFrameCommandLowerer.init(allocator);
    defer frame_plan.deinit();

    try frame_plan.build(.{
        .rows = 10,
        .hidden_size = 2048,
        .vocab_size = 262144,
        .num_attention_heads = 16,
        .global_head_dim = 128,
        .ple_hidden_size = 1024,
        .final_norm_slot = 99,
        .lm_head_slot = 100,
        .tail_quant_format = .q5_k,
        .layers = &layers,
        .source = 11,
        .layer_region = 7,
        .tail_source = 12,
        .tail_region = 8,
    });

    const view = frame_plan.view();
    try std.testing.expectEqual(@as(usize, 32), view.ops.len);
    try std.testing.expectEqual(@as(usize, 3), view.scopes.len);
    try std.testing.expect(frame_plan.has_tail);
    try std.testing.expectEqual(OpKind.attention_pre_norm, view.ops[0].kind);
    try std.testing.expectEqual(OpKind.kv_seed, view.ops[5].kind);
    try std.testing.expectEqual(OpKind.attention, view.ops[6].kind);
    try std.testing.expectEqual(OpKind.attention_pre_norm, view.ops[16].kind);
    try std.testing.expectEqual(OpKind.tail_final_norm, view.ops[29].kind);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mm, view.ops[1].quant_matmul.?.dispatch);
    try std.testing.expectEqual(QuantMatmulDispatchKind.mmv, view.ops[30].quant_matmul.?.dispatch);
    try std.testing.expectEqual(quant_matmul.Format.q8_0, view.ops[1].quant_matmul.?.format);
    try std.testing.expectEqual(quant_matmul.Format.q4_k, view.ops[17].quant_matmul.?.format);
    try std.testing.expectEqual(quant_matmul.Format.q5_k, view.ops[30].quant_matmul.?.format);
    try std.testing.expectEqual(Operator.mul_mm, view.ops[1].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.mul_mm, view.ops[17].operator_plan.?.operator());
    try std.testing.expectEqual(Operator.mul_mv, view.ops[30].operator_plan.?.operator());
    var cursor = GatedFramePlanCursor.init(view);
    const first_window = cursor.nextLayer(.{
        .shares_kv = layers[0].shares_kv,
        .value_norm = true,
    }) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 0), first_window.layer_start);
    try std.testing.expectEqual(@as(usize, 1), first_window.setup_start);
    try std.testing.expectEqual(@as(usize, 6), first_window.block_start);
    try std.testing.expectEqual(@as(usize, 16), first_window.layer_end);
    try std.testing.expectEqual(OpKind.attention_pre_norm, view.ops[first_window.layer_start].kind);
    try std.testing.expectEqual(OpKind.qkv_linear, view.ops[first_window.setup_start].kind);
    try std.testing.expectEqual(OpKind.attention, view.ops[first_window.block_start].kind);
    const second_window = cursor.nextLayer(.{
        .shares_kv = layers[1].shares_kv,
        .value_norm = false,
    }) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 16), second_window.layer_start);
    try std.testing.expectEqual(@as(usize, 17), second_window.setup_start);
    try std.testing.expectEqual(@as(usize, 19), second_window.block_start);
    try std.testing.expectEqual(@as(usize, 29), second_window.layer_end);
    try std.testing.expectEqual(OpKind.attention_pre_norm, view.ops[second_window.layer_start].kind);
    try std.testing.expectEqual(OpKind.qkv_linear, view.ops[second_window.setup_start].kind);
    try std.testing.expectEqual(OpKind.attention, view.ops[second_window.block_start].kind);
    const shared_attention_op = view.ops[second_window.block_start];
    const shared_attention_resources = view.resources[shared_attention_op.resource_start..][0..shared_attention_op.resource_count];
    try std.testing.expectEqual(ResourceKind.kv_cache, shared_attention_resources[1].range.kind);
    try std.testing.expectEqual(@as(usize, 0), shared_attention_resources[1].range.id);
    try std.testing.expectEqual(ResourceKind.kv_cache, shared_attention_resources[2].range.kind);
    try std.testing.expectEqual(@as(usize, 1), shared_attention_resources[2].range.id);
    const tail_window = cursor.nextTailLogits() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 29), tail_window.start);
    try std.testing.expectEqual(@as(usize, 31), tail_window.logits_end);
    try std.testing.expectEqual(OpKind.tail_final_norm, view.ops[tail_window.start].kind);
    try std.testing.expectEqual(OpKind.tail_lm_head, view.ops[tail_window.logits_end - 1].kind);
    try std.testing.expect(cursor.complete());
    var found_logits = false;
    for (view.scratch_slots) |scratch| {
        if (scratch.slot == @intFromEnum(TailCommandLowerer.Resource.logits)) {
            found_logits = true;
            try std.testing.expectEqual(@as(usize, 262144 * @sizeOf(f32)), scratch.bytes);
        }
    }
    try std.testing.expect(found_logits);
}

test "metal command planner splits incompatible source or region" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2 },
        .{ .source = 1, .region = 3 },
        .{ .source = 4, .region = 3 },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 0), plan.barrier_count);
}

test "metal command planner ignores non-overlapping ranges" {
    const allocator = std.testing.allocator;
    const ops = [_]Op{
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 1, 0, 64), .access = .write },
        } },
        .{ .source = 1, .region = 2, .resources = &.{
            .{ .range = .bytes(.buffer, 1, 128, 64), .access = .read },
        } },
    };

    var plan = try build(allocator, &ops, .{});
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), plan.scopes.len);
    try std.testing.expectEqual(@as(usize, 0), plan.barrier_count);
}
