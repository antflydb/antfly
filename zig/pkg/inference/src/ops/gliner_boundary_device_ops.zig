// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict device-only boundary-head contracts. Missing kernels are errors;
//! implementations must not download an activation or execute host math.
const std = @import("std");
const CT = @import("../graph/backend_contracts.zig").CT;

pub const max_inputs = 10;

/// Full bucket tables are independent of request text and sequence length.
/// Their physical owner binds the model generation, FP32 math policy and
/// encoder configuration; compact request bucket maps are never cached here.
pub const DerivedKey = union(enum) {
    relative_normalized,
    relative_query: u32,
    relative_key: u32,
};

/// Only intermediate FP32 GEMM products use this workspace. Bias outputs,
/// hidden states and head results retain their ordinary request ownership.
/// Two encoder layers contain six projections each: five H-wide products and
/// one I-wide product per layer. This is physical capacity, admitted separately
/// from request allocations; it does not increase their enclosing limits.
pub const EncoderWorkspacePlan = struct {
    capacity_bytes: usize,
    max_product_elements: usize,

    pub fn init(batch: usize, sequence: usize, hidden: usize, intermediate: usize) !EncoderWorkspacePlan {
        for ([_]usize{ batch, sequence, hidden, intermediate }) |dimension|
            if (dimension == 0 or dimension > std.math.maxInt(i32)) return error.InvalidBoundaryDeviceShape;
        if (intermediate < hidden) return error.InvalidBoundaryDeviceShape;
        const rows = try std.math.mul(usize, batch, sequence);
        const max_product_elements = try std.math.mul(usize, rows, intermediate);
        if (max_product_elements > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
        const layer_width = try std.math.add(usize, try std.math.mul(usize, hidden, 5), intermediate);
        const capacity_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, layer_width), 8);
        _ = try ProductWorkspaceCursor.init(capacity_bytes, max_product_elements);
        return .{ .capacity_bytes = capacity_bytes, .max_product_elements = max_product_elements };
    }
};

/// Request-owned monotonic range allocation. An offset cannot be recycled
/// until the caller has completed/cancelled its frame and released its views.
pub const ProductWorkspaceCursor = struct {
    capacity_bytes: usize,
    max_product_elements: usize,
    used_bytes: usize = 0,
    peak_bytes: usize = 0,

    pub fn init(capacity_bytes: usize, max_product_elements: usize) !ProductWorkspaceCursor {
        if (capacity_bytes == 0 or capacity_bytes % 4 != 0 or
            capacity_bytes > ScopeLimits.hard_pending_device_bytes or
            max_product_elements == 0 or max_product_elements > std.math.maxInt(i32) or
            max_product_elements > capacity_bytes / 4)
            return error.InvalidGlinerBoundaryWorkspaceRange;
        return .{ .capacity_bytes = capacity_bytes, .max_product_elements = max_product_elements };
    }

    pub fn eligible(self: ProductWorkspaceCursor, elements: usize) !bool {
        if (elements == 0 or elements > std.math.maxInt(i32)) return error.InvalidBoundaryDeviceShape;
        return elements <= self.max_product_elements;
    }

    pub fn needsDrain(self: ProductWorkspaceCursor, elements: usize) !bool {
        if (!try self.eligible(elements)) return false;
        return elements * 4 > self.capacity_bytes - self.used_bytes;
    }

    /// Oversized products stay on the explicitly counted allocated path.
    /// Capacity denial never changes the cursor or high-water mark.
    pub fn reserve(self: *ProductWorkspaceCursor, elements: usize) !?usize {
        if (!try self.eligible(elements)) return null;
        if (try self.needsDrain(elements)) return error.GlinerBoundaryWorkspaceFenceRequired;
        const offset = self.used_bytes;
        self.used_bytes += elements * 4;
        self.peak_bytes = @max(self.peak_bytes, self.used_bytes);
        return offset;
    }

    pub fn retired(self: *ProductWorkspaceCursor) void {
        self.used_bytes = 0;
    }
};

/// An explicit request-owned scope uses the provider's existing serial queue.
/// These ceilings do not replace the enclosing physical-memory admission.
/// Callers bound encoder regions to at most two layers and drain at readbacks.
pub const ScopeLimits = struct {
    max_pending_device_bytes: usize,
    max_dispatches: usize,

    pub const hard_pending_device_bytes: usize = 4 * 1024 * 1024 * 1024;
    pub const hard_dispatches: usize = 4096;

    pub fn validate(self: ScopeLimits) !void {
        if (self.max_pending_device_bytes == 0 or self.max_pending_device_bytes > hard_pending_device_bytes or
            self.max_dispatches == 0 or self.max_dispatches > hard_dispatches)
            return error.InvalidGlinerBoundaryScopeLimits;
    }

    /// Every dispatch can introduce at most ten input allocations and two
    /// outputs (linear product plus bias). Retained wrappers use a bounded
    /// array; its exact backend byte bound is exposed through ScopeStats.
    pub fn maxBuffers(self: ScopeLimits) !usize {
        try self.validate();
        return std.math.mul(usize, self.max_dispatches, max_inputs + 2);
    }

    /// Conservative metadata reservation, independent of optional Metal
    /// imports. The backend checks that its retained tensor fits this slot.
    pub fn retainedHostUpperBound(self: ScopeLimits) !usize {
        return std.math.mul(usize, try self.maxBuffers(), 256);
    }
};

pub const ScopeRequest = union(enum) {
    begin: ScopeLimits,
    finish: struct { generation: u64 },
    cancel: struct { generation: u64 },
    snapshot,
};

pub const ScopeStats = struct {
    active: bool = false,
    generation: u64 = 0,
    pending_device_bytes: usize = 0,
    peak_pending_device_bytes: usize = 0,
    retained_host_bytes: usize = 0,
    begins: u64 = 0,
    dispatches: u64 = 0,
    submissions: u64 = 0,
    cancellations: u64 = 0,
    readback_drains: u64 = 0,
    wait_nanos: u64 = 0,
    gpu_nanos: u64 = 0,
    actual_weight_upload_bytes: u64 = 0,
    resident_weight_acquires: u64 = 0,
    resident_derived_acquires: u64 = 0,
    resident_model_live_bytes: usize = 0,
    workspace_live_bytes: usize = 0,
    workspace_pending_bytes: usize = 0,
    workspace_peak_pending_bytes: usize = 0,
    workspace_product_bytes: u64 = 0,
    workspace_products: u64 = 0,
    workspace_oversized_products: u64 = 0,
    workspace_drains: u64 = 0,
};

/// Allocation-free admission state. Pending payload includes queued upload
/// staging and buffers whose request CT was already freed. It is released
/// only after the owning frame has completed or has been cancelled.
pub const ScopeAccounting = struct {
    limits: ScopeLimits = .{ .max_pending_device_bytes = 1, .max_dispatches = 1 },
    stats: ScopeStats = .{},
    scope_dispatches: usize = 0,

    pub fn begin(self: *ScopeAccounting, limits: ScopeLimits) !void {
        try limits.validate();
        if (self.stats.active) return error.GlinerBoundaryScopeAlreadyActive;
        const next = std.math.add(u64, self.stats.generation, 1) catch return error.GlinerBoundaryScopeGenerationExhausted;
        self.limits = limits;
        self.stats.active = true;
        self.stats.generation = next;
        self.stats.pending_device_bytes = 0;
        self.stats.begins +|= 1;
        self.scope_dispatches = 0;
    }

    pub fn validateGeneration(self: *const ScopeAccounting, generation: u64) !void {
        if (generation == 0 or generation != self.stats.generation) return error.GlinerBoundaryScopeIdentityMismatch;
    }

    pub fn reserve(self: *ScopeAccounting, bytes: usize, dispatches: usize) !void {
        if (!self.stats.active) return error.GlinerBoundaryScopeNotActive;
        const pending = std.math.add(usize, self.stats.pending_device_bytes, bytes) catch return error.ResourceLimitExceeded;
        const count = std.math.add(usize, self.scope_dispatches, dispatches) catch return error.ResourceLimitExceeded;
        if (pending > self.limits.max_pending_device_bytes or count > self.limits.max_dispatches)
            return error.ResourceLimitExceeded;
        self.stats.pending_device_bytes = pending;
        self.stats.peak_pending_device_bytes = @max(self.stats.peak_pending_device_bytes, pending);
        self.scope_dispatches = count;
    }

    pub fn retired(self: *ScopeAccounting) void {
        self.stats.active = false;
        self.stats.pending_device_bytes = 0;
        self.scope_dispatches = 0;
    }
};

/// Physical encoder weight representation. Activations and every task head
/// remain FP32; this type never authorizes dequantization or a dense mirror.
pub const WeightPrecision = enum {
    f32,
    f16,
    q8_0,
    q4_0,
    q4_k,

    pub fn byteLen(self: WeightPrecision, rows: usize, columns: usize) !usize {
        if (rows == 0 or columns == 0 or rows > std.math.maxInt(i32) or columns > std.math.maxInt(i32))
            return error.InvalidBoundaryDeviceShape;
        const values = try std.math.mul(usize, rows, columns);
        if (values > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
        const block: usize = switch (self) {
            .f32, .f16 => 1,
            .q8_0, .q4_0 => 32,
            .q4_k => 256,
        };
        const size: usize = switch (self) {
            .f32 => 4,
            .f16 => 2,
            .q8_0 => 34,
            .q4_0 => 18,
            .q4_k => 144,
        };
        if (columns % block != 0) return error.InvalidGlinerBoundaryQuantizationShape;
        return std.math.mul(usize, values / block, size);
    }
};

pub const Request = union(enum) {
    upload_f32: struct { values: []const f32, shape: []const i32 },
    upload_i32: struct { values: []const i32, shape: []const i32 },
    /// Only weights may originate on the host. Activations require resident
    /// storage; FP16 activation storage is converted by a device kernel.
    resident_f32: struct { input: CT, allow_host_weight: bool = false, source_precision: enum { f32, f16 } = .f32 },
    /// Requires the session's immutable FP32 physical owner. Missing weights,
    /// stale generations or an unprepared owner are errors, never uploads.
    load_f32_weight: struct { name: []const u8, shape: []const i64 },
    load_f32_derived: struct { key: DerivedKey, shape: []const i64 },
    /// Preparation-only publication retains physical storage, never this CT.
    publish_f32_derived: struct { key: DerivedKey, shape: []const i64, input: CT },
    /// Load and retain native checkpoint bytes. The backend verifies exact
    /// shape and physical encoding before preparing any device allocation.
    load_matrix: struct { name: []const u8, rows: usize, columns: usize, precision: WeightPrecision },
    linear: struct { input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize },
    linear_reduced: struct { input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize, precision: WeightPrecision },
    embedding_reduced: struct { weight: CT, ids: []const i64, vocabulary: usize, width: usize, precision: WeightPrecision },
    kernel: Kernel,
};

/// Numeric values form the private Zig/MSL ABI. Keep the implementation in
/// metal_kernels.m in sync, and never pass an unchecked descriptor to Metal.
pub const Kind = enum(u32) {
    bias = 0,
    norm = 1,
    boundary_side = 2,
    concat = 3,
    add = 4,
    mask_rows = 5,
    banded_attention = 6,
    swiglu = 7,
    marginals = 8,
    inside_prefix = 9,
    content_prefix = 10,
    endpoints = 11,
    length_features = 12,
    range_mean = 13,
    shared_sum = 14,
    film = 15,
    gelu = 16,
    shared_score = 17,
    concat_queries = 18,
    cast_half = 19,
    rotary = 20,
    sigmoid = 21,
    explicit_prior = 22,
    explicit_compat = 23,
    explicit_difference = 24,
    explicit_range_mean = 25,
    explicit_base = 26,
    explicit_content = 27,
    explicit_finish = 28,
    gather_i32 = 29,
    deberta_attention = 30,
    relu = 31,
    interval_means = 32,
    relation_gated_score = 33,
    record_attention = 34,
    record_assign = 35,
    /// Generic training routes normalize negative indices; boundary gather's
    /// existing -1 padding sentinel is intentionally a separate operation.
    gather_i32_exact = 36,
    scatter_add_i32 = 37,
    fill_zero = 38,
    scatter_grouped_i32 = 39,
    norm_chunks = 40,
    norm_merge = 41,
};

pub const Params = extern struct {
    kind: u32,
    dims: [8]u32 = @splat(0),
    scalars: [4]f32 = @splat(0),
};

pub const Kernel = struct {
    kind: Kind,
    inputs: [max_inputs]?CT = @splat(null),
    dims: [8]u32 = @splat(0),
    scalars: [4]f32 = @splat(0),

    pub fn params(self: Kernel) Params {
        return .{ .kind = @intFromEnum(self.kind), .dims = self.dims, .scalars = self.scalars };
    }

    pub fn layout(self: Kernel) !Layout {
        return layoutFor(self.kind, self.dims, self.scalars);
    }
};

pub const Layout = struct {
    input_elements: [max_inputs]usize = @splat(0),
    integer_inputs: u16 = 0,
    output_elements: usize = 0,
    work_items: usize = 0,
    simd_groups: bool = false,
};

fn mul(values: []const u32) !usize {
    var result: usize = 1;
    for (values) |v| {
        if (v == 0) return error.InvalidBoundaryDeviceShape;
        result = try std.math.mul(usize, result, v);
    }
    if (result > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return result;
}

fn shape(values: []const usize) !usize {
    var result: usize = 1;
    for (values) |v| {
        if (v == 0) return error.InvalidBoundaryDeviceShape;
        result = try std.math.mul(usize, result, v);
    }
    if (result > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return result;
}

pub fn layoutFor(kind: Kind, d: [8]u32, scalars: [4]f32) !Layout {
    var r = Layout{};
    for (scalars) |v| if (!std.math.isFinite(v)) return error.InvalidBoundaryDeviceShape;
    switch (kind) {
        .bias, .norm => {
            r.output_elements = try mul(d[0..2]);
            r.input_elements[0] = r.output_elements;
            r.input_elements[1] = d[1];
            if (kind == .norm) {
                if (scalars[0] <= 0 or d[1] > 4096) return error.InvalidBoundaryDeviceShape;
                r.input_elements[2] = d[1];
                r.work_items = try shape(&.{ d[0], 32 });
                r.simd_groups = true;
            }
        },
        .boundary_side => { // B,L,H,side. Lengths count body+prefix words.
            if (d[3] > 1 or d[1] == std.math.maxInt(u32)) return error.InvalidBoundaryDeviceShape;
            r.input_elements[0] = try mul(d[0..3]);
            r.input_elements[1] = d[2];
            r.input_elements[2] = d[2];
            r.input_elements[3] = d[0];
            r.integer_inputs = 1 << 3;
            r.output_elements = try shape(&.{ d[0], @as(usize, d[1]) + 1, d[2] });
        },
        .concat => { // rows,left_dim,right_dim
            r.input_elements[0] = try mul(d[0..2]);
            r.input_elements[1] = try shape(&.{ d[0], d[2] });
            r.output_elements = try shape(&.{ d[0], try std.math.add(usize, d[1], d[2]) });
        },
        .add => {
            r.output_elements = try mul(d[0..1]);
            r.input_elements[0] = r.output_elements;
            r.input_elements[1] = r.output_elements;
        },
        .mask_rows, .swiglu => {
            r.output_elements = try mul(d[0..2]);
            r.input_elements[0] = try shape(&.{ r.output_elements, if (kind == .swiglu) @as(usize, 2) else 1 });
            if (kind == .mask_rows) {
                r.input_elements[1] = d[0];
                r.integer_inputs = 1 << 1;
            }
        },
        .banded_attention => { // B,N,heads,head_dim,window
            if (d[3] > 32 or d[4] > 8192) return error.UnsupportedBoundaryDeviceGeometry;
            r.output_elements = try mul(d[0..4]);
            r.input_elements[0] = try shape(&.{ r.output_elements, 3 });
            r.input_elements[1] = d[0];
            r.integer_inputs = 1 << 1;
            r.work_items = try shape(&.{ d[0], d[1], d[2], 32 });
            r.simd_groups = true;
        },
        .marginals => { // B,N,Q,D,is_boundary
            if (d[4] > 1) return error.InvalidBoundaryDeviceShape;
            r.input_elements[0] = try shape(&.{ d[0], d[1], d[3] });
            r.input_elements[1] = try shape(&.{ d[0], d[2], d[3] });
            r.input_elements[2] = d[0];
            r.input_elements[3] = try shape(&.{ d[0], d[2] });
            r.integer_inputs = (1 << 2) | (1 << 3);
            r.output_elements = try mul(d[0..3]);
        },
        .inside_prefix => { // B,L,Q. Packed [B,Q,L+2]: leading zero, prefix, mean.
            r.input_elements[0] = try mul(d[0..3]);
            r.input_elements[1] = d[0];
            r.input_elements[2] = try shape(&.{ d[0], d[2] });
            r.integer_inputs = (1 << 1) | (1 << 2);
            r.output_elements = try shape(&.{ d[0], @as(usize, d[1]) + 2, d[2] });
            r.work_items = r.input_elements[2];
        },
        .content_prefix => { // B,L,D
            r.input_elements[0] = try mul(d[0..3]);
            r.input_elements[1] = d[0];
            r.integer_inputs = 1 << 1;
            r.output_elements = try shape(&.{ d[0], @as(usize, d[1]) + 1, d[2] });
            r.work_items = try shape(&.{ d[0], d[2] });
        },
        .endpoints, .range_mean => { // B,N,C,D
            r.input_elements[0] = try shape(&.{ d[0], d[1], d[3] });
            r.input_elements[1] = try shape(&.{ d[0], d[2], 2 });
            r.integer_inputs = 1 << 1;
            r.output_elements = try shape(&.{ d[0], d[2], d[3], if (kind == .endpoints) @as(usize, 2) else 1 });
        },
        .length_features => { // B,C
            r.input_elements[0] = try shape(&.{ d[0], d[1], 2 });
            r.input_elements[1] = d[0];
            r.integer_inputs = 3;
            r.output_elements = try shape(&.{ d[0], d[1], 3 });
        },
        .shared_sum => { // B,N,C,D
            r.input_elements[0] = try shape(&.{ d[0], d[1], d[3] });
            r.input_elements[1] = r.input_elements[0];
            r.output_elements = try shape(&.{ d[0], d[2], d[3] });
            r.input_elements[2] = r.output_elements;
            r.input_elements[3] = r.output_elements;
            r.input_elements[4] = try shape(&.{ d[0], d[2], 2 });
            r.integer_inputs = 1 << 4;
        },
        .film => { // B,C,Q,P,query_start,query_count
            if (d[4] > d[2] or d[5] > d[2] - d[4]) return error.InvalidBoundaryDeviceShape;
            r.input_elements[0] = try shape(&.{ d[0], d[1], d[3] });
            r.input_elements[1] = try shape(&.{ d[0], d[2], d[3], 2 });
            r.output_elements = try shape(&.{ d[0], d[1], d[5], d[3] });
        },
        .gelu, .cast_half, .sigmoid => {
            r.output_elements = try mul(d[0..1]);
            r.input_elements[0] = r.output_elements;
        },
        .shared_score => { // B,N,C,Q,P,query_start,query_count,use_inside
            if (d[5] > d[3] or d[6] > d[3] - d[5] or d[7] > 1) return error.InvalidBoundaryDeviceShape;
            r.input_elements[0] = try shape(&.{ d[0], d[2], d[4] });
            r.input_elements[1] = try shape(&.{ d[0], d[3], d[4] });
            r.output_elements = try shape(&.{ d[0], d[6], d[2] });
            r.input_elements[2] = r.output_elements;
            r.input_elements[3] = try shape(&.{ d[0], d[3], d[1] });
            r.input_elements[4] = r.input_elements[3];
            r.input_elements[5] = try shape(&.{ d[0], d[3], @as(usize, d[1]) + 1 });
            r.input_elements[6] = try shape(&.{ d[0], d[2], 2 });
            r.input_elements[7] = try shape(&.{ d[0], d[2] });
            r.input_elements[8] = try shape(&.{ d[0], d[3] });
            r.integer_inputs = (1 << 6) | (1 << 7) | (1 << 8);
        },
        .concat_queries => { // B,leftQ,rightQ,C
            r.input_elements[0] = try shape(&.{ d[0], d[1], d[3] });
            r.input_elements[1] = try shape(&.{ d[0], d[2], d[3] });
            r.output_elements = try shape(&.{ d[0], try std.math.add(usize, d[1], d[2]), d[3] });
        },
        .rotary => { // B,N,D, theta in scalars[0]. Consecutive even/odd pairs.
            if (d[2] % 2 != 0 or scalars[0] <= 0) return error.InvalidBoundaryDeviceShape;
            r.output_elements = try mul(d[0..3]);
            r.input_elements[0] = r.output_elements;
        },
        .gather_i32 => { // source_rows,dim,output_rows. -1 maps to a zero row.
            r.input_elements[0] = try mul(d[0..2]);
            r.input_elements[1] = d[2];
            r.integer_inputs = 1 << 1;
            r.output_elements = try shape(&.{ d[2], d[1] });
        },
        .deberta_attention => { // B,S,heads,head_dim,relative_rows
            if (d[3] > 128) return error.UnsupportedBoundaryDeviceGeometry;
            r.output_elements = try mul(d[0..4]);
            r.input_elements[0] = r.output_elements;
            r.input_elements[1] = r.output_elements;
            r.input_elements[2] = r.output_elements;
            r.input_elements[3] = try shape(&.{ d[4], d[2], d[3] });
            r.input_elements[4] = r.input_elements[3];
            r.input_elements[5] = try std.math.sub(usize, try shape(&.{ d[1], 2 }), 1);
            r.input_elements[6] = try mul(d[0..2]);
            r.integer_inputs = (1 << 5) | (1 << 6);
            r.work_items = try shape(&.{ d[0], d[1], d[2], 32 });
            r.simd_groups = true;
        },
        .relu => {
            r.output_elements = try mul(d[0..1]);
            r.input_elements[0] = r.output_elements;
        },
        .gather_i32_exact, .scatter_add_i32 => { // rows,columns,index_count
            const input_rows = if (kind == .gather_i32_exact) d[0] else d[2];
            const output_rows = if (kind == .gather_i32_exact) d[2] else d[0];
            r.input_elements[0] = try shape(&.{ input_rows, d[1] });
            r.input_elements[1] = try mul(d[2..3]);
            r.integer_inputs = 1 << 1;
            r.output_elements = try shape(&.{ output_rows, d[1] });
        },
        .fill_zero => r.output_elements = try mul(d[0..1]),
        .scatter_grouped_i32 => { // output_rows,columns,value_rows,unique_rows
            r.input_elements[0] = try shape(&.{ d[2], d[1] });
            r.input_elements[1] = try mul(d[3..4]);
            r.input_elements[2] = try std.math.add(usize, d[3], 1);
            r.input_elements[3] = d[2];
            r.integer_inputs = (1 << 1) | (1 << 2) | (1 << 3);
            r.output_elements = try mul(d[0..2]);
            r.work_items = try shape(&.{ d[3], d[1] });
        },
        .norm_chunks => { // element_count,chunk_size
            if (d[1] != 1024) return error.InvalidBoundaryDeviceShape;
            const chunks = std.math.divCeil(u32, d[0], d[1]) catch return error.InvalidBoundaryDeviceShape;
            r.input_elements[0] = try mul(d[0..1]);
            r.output_elements = try shape(&.{ chunks, 3 });
            r.work_items = try shape(&.{ chunks, 32 });
            r.simd_groups = true;
        },
        .norm_merge => { // chunk_count
            r.input_elements[0] = try shape(&.{ d[0], 3 });
            r.output_elements = 3;
            r.work_items = 32;
            r.simd_groups = true;
        },
        .interval_means => { // intervals,hidden; each interval has start/end rows.
            r.output_elements = try mul(d[0..2]);
            r.input_elements[0] = try shape(&.{ r.output_elements, 2 });
            r.input_elements[1] = d[0];
        },
        .relation_gated_score => { // pairs,hidden
            r.output_elements = try mul(d[0..1]);
            r.input_elements[0] = r.output_elements;
            r.input_elements[1] = try mul(d[0..2]);
            r.input_elements[2] = r.input_elements[1];
            r.input_elements[3] = r.input_elements[1];
            r.input_elements[4] = r.output_elements;
            r.input_elements[5] = r.output_elements;
            r.integer_inputs = 1 << 5;
        },
        .record_attention => { // instances,candidates,record_dim,hidden
            if (d[2] > 256 or d[3] > 768) return error.UnsupportedBoundaryDeviceGeometry;
            r.input_elements[0] = try shape(&.{ d[0], d[2] });
            r.input_elements[1] = try shape(&.{ d[1], d[2] });
            r.input_elements[2] = try shape(&.{ d[1], d[3] });
            r.output_elements = try shape(&.{ d[0], d[3] });
            r.input_elements[3] = r.output_elements;
            r.work_items = try shape(&.{ d[0], 32 });
            r.simd_groups = true;
        },
        .record_assign => { // instances,record_dim,fields,total_candidates (may be zero)
            r.input_elements[0] = try mul(d[0..2]);
            r.input_elements[1] = try shape(&.{ d[2], d[1] });
            // With no candidates the existing null vector is a safe dummy.
            r.input_elements[2] = try shape(&.{ @max(d[3], 1), d[1] });
            r.input_elements[3] = d[1];
            r.input_elements[4] = try shape(&.{@as(usize, d[2]) + 1});
            r.integer_inputs = 1 << 4;
            r.output_elements = try shape(&.{ d[0], try std.math.add(usize, d[2], d[3]) });
        },
        .explicit_prior, .explicit_compat, .explicit_difference, .explicit_range_mean, .explicit_base, .explicit_content, .explicit_finish => {
            // B,N,Q,C,width,flat_offset,flat_count,compat_heads.
            const slots = try shape(&.{ d[0], d[2], d[3] });
            if (d[1] == 0 or d[6] == 0 or d[5] > slots or d[6] > slots - d[5]) return error.InvalidBoundaryDeviceShape;
            const endpoints = try shape(&.{ d[0], d[1], if (kind == .explicit_finish) @as(usize, 1) else d[4] });
            const queries = try shape(&.{ d[0], d[2] });
            const spans = try shape(&.{ slots, 2 });
            r.output_elements = d[6];
            switch (kind) {
                .explicit_prior, .explicit_compat => {
                    if (d[4] % 2 != 0) return error.InvalidBoundaryDeviceShape;
                    r.input_elements[0] = endpoints;
                    r.input_elements[1] = endpoints;
                    r.input_elements[2] = try shape(&.{ queries, d[4] / 2 });
                    r.input_elements[3] = spans;
                    r.input_elements[4] = slots;
                    r.integer_inputs = (1 << 3) | (1 << 4);
                    if (kind == .explicit_compat) {
                        if (d[7] == 0 or d[4] % d[7] != 0) return error.InvalidBoundaryDeviceShape;
                        r.output_elements = try shape(&.{ d[6], d[7] });
                    }
                },
                .explicit_difference, .explicit_range_mean => {
                    r.input_elements[0] = endpoints;
                    if (kind == .explicit_difference) {
                        r.input_elements[1] = endpoints;
                        r.input_elements[2] = spans;
                        r.integer_inputs = 1 << 2;
                        r.output_elements = try shape(&.{ d[6], d[4], 2 });
                    } else {
                        r.input_elements[1] = spans;
                        r.integer_inputs = 1 << 1;
                        r.output_elements = try shape(&.{ d[6], d[4] });
                    }
                },
                .explicit_base => {
                    r.input_elements[0] = d[6];
                    r.input_elements[1] = d[6];
                    r.input_elements[2] = d[6];
                    r.input_elements[3] = try shape(&.{ queries, d[1] });
                    r.input_elements[4] = r.input_elements[3];
                    r.input_elements[5] = spans;
                    r.integer_inputs = 1 << 5;
                },
                .explicit_content => {
                    r.input_elements[0] = d[6];
                    r.input_elements[1] = try shape(&.{ d[6], d[4] });
                    r.input_elements[2] = try shape(&.{ queries, d[4] });
                    r.input_elements[3] = d[6];
                },
                .explicit_finish => {
                    r.input_elements[0] = d[6];
                    r.input_elements[1] = try shape(&.{ queries, @as(usize, d[1]) + 1 });
                    r.input_elements[2] = queries;
                    r.input_elements[3] = try shape(&.{ queries, 3 });
                    r.input_elements[4] = spans;
                    r.input_elements[5] = slots;
                    r.input_elements[6] = d[0];
                    r.integer_inputs = (1 << 4) | (1 << 5) | (1 << 6);
                },
                else => unreachable,
            }
        },
    }
    if (r.work_items == 0) r.work_items = r.output_elements;
    return r;
}

test "boundary device layouts reject overflow and bound attention memory" {
    const small = try layoutFor(.banded_attention, .{ 2, 7, 4, 4, 128, 0, 0, 0 }, @splat(0));
    try std.testing.expectEqual(@as(usize, 2 * 7 * 16), small.output_elements);
    const long = try layoutFor(.banded_attention, .{ 1, 8193, 4, 32, 128, 0, 0, 0 }, @splat(0));
    try std.testing.expectEqual(@as(usize, 8193 * 128 * 3), long.input_elements[0]);
    try std.testing.expectEqual(@as(usize, 8193 * 128), long.output_elements);
    try std.testing.expectError(error.ResourceLimitExceeded, layoutFor(.shared_score, .{ 8, 8193, 192, 256, 0xffffffff, 0, 32, 1 }, @splat(0)));
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, layoutFor(.film, .{ 1, 192, 3, 128, 2, 2, 0, 0 }, @splat(0)));
    try std.testing.expectError(error.UnsupportedBoundaryDeviceGeometry, layoutFor(.banded_attention, .{ 1, 7, 4, 64, 128, 0, 0, 0 }, @splat(0)));
    const encoder = try layoutFor(.deberta_attention, .{ 1, 16384, 12, 64, 512, 0, 0, 0 }, @splat(0));
    try std.testing.expectEqual(@as(usize, 16384 * 768), encoder.output_elements);
    try std.testing.expectEqual(@as(usize, 512 * 768), encoder.input_elements[3]);
    try std.testing.expectEqual(@as(usize, 2 * 16384 - 1), encoder.input_elements[5]);
    try std.testing.expectEqual(@as(u16, (1 << 5) | (1 << 6)), encoder.integer_inputs);
    try std.testing.expectError(error.UnsupportedBoundaryDeviceGeometry, layoutFor(.deberta_attention, .{ 1, 7, 4, 129, 512, 0, 0, 0 }, @splat(0)));
    const gather = try layoutFor(.gather_i32, .{ 250112, 768, 4096, 0, 0, 0, 0, 0 }, @splat(0));
    try std.testing.expectEqual(@as(u16, 1 << 1), gather.integer_inputs);
    try std.testing.expectEqual(@as(usize, 4096 * 768), gather.output_elements);
}

test "boundary device precision sizes preserve packed row blocks" {
    try std.testing.expectEqual(@as(usize, 256 * 384 * 4), try WeightPrecision.f32.byteLen(384, 256));
    try std.testing.expectEqual(@as(usize, 256 * 384 * 2), try WeightPrecision.f16.byteLen(384, 256));
    try std.testing.expectEqual(@as(usize, 8 * 384 * 34), try WeightPrecision.q8_0.byteLen(384, 256));
    try std.testing.expectEqual(@as(usize, 8 * 384 * 18), try WeightPrecision.q4_0.byteLen(384, 256));
    try std.testing.expectEqual(@as(usize, 384 * 144), try WeightPrecision.q4_k.byteLen(384, 256));
    try std.testing.expectError(error.InvalidGlinerBoundaryQuantizationShape, WeightPrecision.q4_k.byteLen(384, 384));
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, WeightPrecision.f16.byteLen(0, 256));
    try std.testing.expectError(error.ResourceLimitExceeded, WeightPrecision.f32.byteLen(std.math.maxInt(i32), 2));
}

test "strict GLiNER boundary scope admission is transactional and generation checked" {
    var state = ScopeAccounting{};
    try std.testing.expectError(error.InvalidGlinerBoundaryScopeLimits, state.begin(.{ .max_pending_device_bytes = 0, .max_dispatches = 1 }));
    try std.testing.expectError(error.InvalidGlinerBoundaryScopeLimits, state.begin(.{ .max_pending_device_bytes = ScopeLimits.hard_pending_device_bytes + 1, .max_dispatches = 1 }));
    try std.testing.expectError(error.InvalidGlinerBoundaryScopeLimits, state.begin(.{ .max_pending_device_bytes = 64, .max_dispatches = ScopeLimits.hard_dispatches + 1 }));
    const limits = ScopeLimits{ .max_pending_device_bytes = 64, .max_dispatches = 2 };
    try state.begin(limits);
    const first = state.stats.generation;
    try state.reserve(32, 1);
    const before = state;
    try std.testing.expectError(error.ResourceLimitExceeded, state.reserve(33, 1));
    try std.testing.expectEqualDeep(before, state);
    try std.testing.expectError(error.ResourceLimitExceeded, state.reserve(1, 2));
    try std.testing.expectEqualDeep(before, state);
    try std.testing.expectError(error.ResourceLimitExceeded, state.reserve(std.math.maxInt(usize), 0));
    try std.testing.expectEqualDeep(before, state);
    try std.testing.expectError(error.GlinerBoundaryScopeAlreadyActive, state.begin(limits));
    try std.testing.expectEqualDeep(before, state);
    try state.reserve(32, 1);
    try std.testing.expectEqual(@as(usize, 64), state.stats.pending_device_bytes);
    state.retired();
    try std.testing.expectEqual(@as(usize, 0), state.stats.pending_device_bytes);
    try std.testing.expectEqual(@as(usize, 64), state.stats.peak_pending_device_bytes);
    try state.validateGeneration(first); // finish is idempotent after drain.
    try state.begin(limits);
    try std.testing.expectError(error.GlinerBoundaryScopeIdentityMismatch, state.validateGeneration(first));
    try std.testing.expectEqual(@as(usize, 24 * 256), try limits.retainedHostUpperBound());
    state.retired();
    state.stats.generation = std.math.maxInt(u64);
    try std.testing.expectError(error.GlinerBoundaryScopeGenerationExhausted, state.begin(limits));
    try std.testing.expect(!state.stats.active);
}

test "strict GLiNER boundary scope workspace geometry and fenced ranges are bounded" {
    const plan = try EncoderWorkspacePlan.init(2, 128, 384, 1536);
    try std.testing.expectEqual(@as(usize, 2 * 2 * 128 * (5 * 384 + 1536) * 4), plan.capacity_bytes);
    try std.testing.expectEqual(@as(usize, 2 * 128 * 1536), plan.max_product_elements);
    for ([_][4]usize{
        .{ 0, 128, 384, 1536 },
        .{ 1, 0, 384, 1536 },
        .{ 1, 128, 0, 1536 },
        .{ 1, 128, 384, 383 },
    }) |dims| try std.testing.expectError(error.InvalidBoundaryDeviceShape, EncoderWorkspacePlan.init(dims[0], dims[1], dims[2], dims[3]));
    try std.testing.expectError(error.ResourceLimitExceeded, EncoderWorkspacePlan.init(std.math.maxInt(i32), 2, 1, 1));
    try std.testing.expectError(error.Overflow, EncoderWorkspacePlan.init(std.math.maxInt(i32), std.math.maxInt(i32), std.math.maxInt(i32), std.math.maxInt(i32)));
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceRange, EncoderWorkspacePlan.init(1, 256 * 1024 * 1024, 1, 1));
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceRange, ProductWorkspaceCursor.init(31, 4));
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceRange, ProductWorkspaceCursor.init(16, 5));
    var cursor = try ProductWorkspaceCursor.init(32, 4);
    try std.testing.expectEqual(@as(?usize, 0), try cursor.reserve(4));
    try std.testing.expectEqual(@as(?usize, 16), try cursor.reserve(4));
    const full = cursor;
    try std.testing.expect(try cursor.needsDrain(1));
    try std.testing.expectError(error.GlinerBoundaryWorkspaceFenceRequired, cursor.reserve(1));
    try std.testing.expectEqualDeep(full, cursor);
    try std.testing.expectEqual(@as(?usize, null), try cursor.reserve(5));
    try std.testing.expectEqualDeep(full, cursor);
    try std.testing.expectError(error.InvalidBoundaryDeviceShape, cursor.reserve(0));
    try std.testing.expectEqualDeep(full, cursor);
    cursor.retired();
    try std.testing.expectEqual(@as(?usize, 0), try cursor.reserve(4));
    try std.testing.expectEqual(@as(usize, 32), cursor.peak_bytes);
}
