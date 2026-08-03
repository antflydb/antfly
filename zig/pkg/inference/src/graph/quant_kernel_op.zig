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

//! Backend-independent operation identities shared by the kernel compiler and
//! backend renderers. ABI and schedule details belong to tagged render plans.

const std = @import("std");
const quant_matmul = @import("quant_matmul.zig");

pub const OpKind = enum(u8) {
    small_batch_matmul,
    attention,
    microkernel,
};

pub const MicrokernelKind = enum {
    rms_norm,
};

pub const AttentionKind = enum {
    decode_1x,
    prefill_flash,
};

pub const Epilogue = enum(u8) {
    none,
    bias,
    bias_gelu,
    pair,
    triple,
    relu,
    gelu,
    add,
    argmax,
    /// Fused FFN gate+up: two projections sharing one q8_1-quantized input,
    /// activation multiply, q8_1-requantized output.
    pair_activation,
    /// FFN down projection consuming the q8_1-quantized activated vector
    /// produced by `pair_activation`, with f32 output.
    gated_down,
};

/// The activation representation consumed by a specialization. This is part of
/// the semantic key: Q4_0 x F32 and Q4_0 x Q8_1 are different operations even
/// when their weight format and epilogue agree.
pub const ActivationEncoding = enum(u8) {
    f32 = 0,
    f16 = 1,
    bf16 = 2,
    q8_1 = 3,
    /// Page-table-addressed KV keys whose runtime tag selects F16 or Polar4.
    paged_f16_or_polar4 = 4,
    /// Page-table-addressed values whose runtime tag selects F16 or F32.
    paged_f16_or_f32 = 5,
    /// Page-table-addressed raw F16 rows with an exact runtime format tag.
    paged_f16 = 6,
};

/// Mathematical activation applied by a fused epilogue. This is independent
/// of `ActivationEncoding`, which describes how the input is stored.
pub const ActivationFunction = enum(u8) {
    none = 0,
    silu = 1,
    gelu = 2,
    gelu_new = 3,
    relu = 4,
    relu_squared = 5,
    quick_gelu = 6,
};

pub const OutputEncoding = enum(u8) {
    f32 = 0,
    f16 = 1,
    bf16 = 2,
    q8_1 = 3,
    i32 = 4,
};

pub const MatmulSpecialization = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    dispatch: quant_matmul.DispatchKind,
    epilogue: Epilogue,
    activation: ActivationEncoding = .f32,
    function: ActivationFunction = .none,
    output: OutputEncoding = .f32,
};

pub const AttentionSpecialization = struct {
    kind: AttentionKind,
    query: ActivationEncoding = .f32,
    key: ActivationEncoding = .f32,
    value: ActivationEncoding = .f32,
    output: OutputEncoding = .f32,
};

pub const MicrokernelSpecialization = struct {
    kind: MicrokernelKind,
    input: ActivationEncoding = .f32,
    output: OutputEncoding = .f32,
};

/// Canonical, model-neutral semantic identity for an AOT specialization.
/// Runtime dimensions and backend schedules deliberately live outside this
/// union so the same operation can be compiled for several exact shapes and
/// targets without inventing model-specific operation kinds.
pub const SpecializationSignature = union(OpKind) {
    small_batch_matmul: MatmulSpecialization,
    attention: AttentionSpecialization,
    microkernel: MicrokernelSpecialization,

    pub fn opKind(self: SpecializationSignature) OpKind {
        return std.meta.activeTag(self);
    }

    pub fn validate(self: SpecializationSignature) !void {
        switch (self) {
            .small_batch_matmul => |op| {
                if (op.format == .unknown) return error.InvalidSpecializationFormat;
                const expected_dispatch: quant_matmul.DispatchKind = switch (op.row_bucket) {
                    .rows_0 => .scalar,
                    .rows_1 => .mmv,
                    .rows_2_8 => .small_batch,
                    .rows_9_64, .rows_65_plus => .mm,
                };
                if (op.dispatch != expected_dispatch) return error.InvalidSpecializationDispatch;
                if (op.epilogue == .pair_activation and op.function == .none) {
                    return error.MissingSpecializationActivationFunction;
                }
            },
            .attention => {},
            .microkernel => {},
        }
    }

    pub fn eql(a: SpecializationSignature, b: SpecializationSignature) bool {
        if (a.opKind() != b.opKind()) return false;
        return switch (a) {
            .small_batch_matmul => |lhs| blk: {
                const rhs = b.small_batch_matmul;
                break :blk lhs.format == rhs.format and
                    lhs.row_bucket == rhs.row_bucket and
                    lhs.dispatch == rhs.dispatch and
                    lhs.epilogue == rhs.epilogue and
                    lhs.activation == rhs.activation and
                    lhs.function == rhs.function and
                    lhs.output == rhs.output;
            },
            .attention => |lhs| blk: {
                const rhs = b.attention;
                break :blk lhs.kind == rhs.kind and
                    lhs.query == rhs.query and
                    lhs.key == rhs.key and
                    lhs.value == rhs.value and
                    lhs.output == rhs.output;
            },
            .microkernel => |lhs| blk: {
                const rhs = b.microkernel;
                break :blk lhs.kind == rhs.kind and
                    lhs.input == rhs.input and
                    lhs.output == rhs.output;
            },
        };
    }
};

/// One runtime dimension. `fixed` and `min == max != 0` are canonicalized to
/// the same exact constraint by the fingerprint and overlap helpers.
pub const RuntimeDimensionConstraint = struct {
    min: u32 = 0,
    max: u32 = 0,
    multiple_of: u32 = 1,
    fixed: u32 = 0,

    pub fn exact(value: u32) RuntimeDimensionConstraint {
        std.debug.assert(value != 0);
        return .{ .fixed = value };
    }

    pub fn validate(self: RuntimeDimensionConstraint) !void {
        if (self.multiple_of == 0) return error.InvalidRuntimeDimensionMultiple;
        if (self.max != 0 and self.min > self.max) return error.InvalidRuntimeDimensionBounds;
        if (self.fixed != 0) {
            if (self.fixed < self.min) return error.FixedRuntimeDimensionBelowMinimum;
            if (self.max != 0 and self.fixed > self.max) return error.FixedRuntimeDimensionAboveMaximum;
            if (self.fixed % self.multiple_of != 0) return error.FixedRuntimeDimensionMisaligned;
        } else if (self.min != 0 and self.min == self.max and self.min % self.multiple_of != 0) {
            return error.FixedRuntimeDimensionMisaligned;
        }
    }

    pub fn exactValue(self: RuntimeDimensionConstraint) ?u32 {
        if (self.fixed != 0) return self.fixed;
        if (self.min != 0 and self.min == self.max) return self.min;
        return null;
    }

    pub fn isUnconstrained(self: RuntimeDimensionConstraint) bool {
        return self.min == 0 and self.max == 0 and self.fixed == 0 and self.multiple_of == 1;
    }

    pub fn matches(self: RuntimeDimensionConstraint, value: u32) bool {
        if (self.isUnconstrained()) return true;
        if (value == 0) return false;
        if (self.exactValue()) |exact_value| return value == exact_value;
        if (value < self.min) return false;
        if (self.max != 0 and value > self.max) return false;
        return value % self.multiple_of == 0;
    }

    pub fn overlaps(a: RuntimeDimensionConstraint, b: RuntimeDimensionConstraint) bool {
        if (a.isUnconstrained() or b.isUnconstrained()) return true;
        if (a.exactValue()) |value| return b.matches(value);
        if (b.exactValue()) |value| return a.matches(value);

        const lower = @max(@as(u32, 1), @max(a.min, b.min));
        const a_upper = if (a.max == 0) std.math.maxInt(u32) else a.max;
        const b_upper = if (b.max == 0) std.math.maxInt(u32) else b.max;
        const upper = @min(a_upper, b_upper);
        if (lower > upper) return false;

        const a_multiple: u64 = a.multiple_of;
        const b_multiple: u64 = b.multiple_of;
        const divisor = gcd(a_multiple, b_multiple);
        const common_multiple = (a_multiple / divisor) * b_multiple;
        const lower_u64: u64 = lower;
        const first_common = ((lower_u64 + common_multiple - 1) / common_multiple) * common_multiple;
        return first_common <= upper;
    }
};

pub const RuntimeShape = struct {
    rows: u32 = 0,
    input_dim: u32 = 0,
    output_dim: u32 = 0,
    head_dim: u32 = 0,
    query_heads: u32 = 0,
    kv_heads: u32 = 0,
    sequence_length: u32 = 0,
};

/// Runtime contract for an AOT specialization. Compute dimensions are exact;
/// sequence length may remain bounded because attention kernels commonly share
/// one compiled topology across a length bucket.
pub const RuntimeConstraints = struct {
    rows: RuntimeDimensionConstraint = .{},
    input_dim: RuntimeDimensionConstraint = .{},
    output_dim: RuntimeDimensionConstraint = .{},
    head_dim: RuntimeDimensionConstraint = .{},
    query_heads: RuntimeDimensionConstraint = .{},
    kv_heads: RuntimeDimensionConstraint = .{},
    sequence_length: RuntimeDimensionConstraint = .{},

    pub fn exactMatmul(rows: u32, input_dim: u32, output_dim: u32) RuntimeConstraints {
        return .{
            .rows = .exact(rows),
            .input_dim = .exact(input_dim),
            .output_dim = .exact(output_dim),
        };
    }

    pub fn exactAttention(rows: u32, head_dim: u32, query_heads: u32, kv_heads: u32) RuntimeConstraints {
        return .{
            .rows = .exact(rows),
            .head_dim = .exact(head_dim),
            .query_heads = .exact(query_heads),
            .kv_heads = .exact(kv_heads),
        };
    }

    pub fn exactMicrokernel(rows: u32, input_dim: u32) RuntimeConstraints {
        return .{
            .rows = .exact(rows),
            .input_dim = .exact(input_dim),
        };
    }

    pub fn validate(self: RuntimeConstraints) !void {
        inline for (std.meta.fields(RuntimeConstraints)) |field| {
            try @field(self, field.name).validate();
        }
    }

    /// Enforces the exact dimensions needed to select one model-neutral AOT
    /// specialization without consulting model names or layer indices.
    pub fn validateExactFor(self: RuntimeConstraints, signature: SpecializationSignature) !void {
        try self.validate();
        switch (signature) {
            .small_batch_matmul => {
                if (self.rows.exactValue() == null or
                    self.input_dim.exactValue() == null or
                    self.output_dim.exactValue() == null)
                {
                    return error.InexactMatmulRuntimeConstraint;
                }
            },
            .attention => {
                if (self.rows.exactValue() == null or
                    self.head_dim.exactValue() == null or
                    self.query_heads.exactValue() == null or
                    self.kv_heads.exactValue() == null)
                {
                    return error.InexactAttentionRuntimeConstraint;
                }
            },
            .microkernel => {
                if (self.rows.exactValue() == null or self.input_dim.exactValue() == null) {
                    return error.InexactMicrokernelRuntimeConstraint;
                }
            },
        }
    }

    pub fn matches(self: RuntimeConstraints, shape: RuntimeShape) bool {
        inline for (std.meta.fields(RuntimeConstraints)) |field| {
            if (!@field(self, field.name).matches(@field(shape, field.name))) return false;
        }
        return true;
    }

    pub fn overlaps(a: RuntimeConstraints, b: RuntimeConstraints) bool {
        inline for (std.meta.fields(RuntimeConstraints)) |field| {
            if (!@field(a, field.name).overlaps(@field(b, field.name))) return false;
        }
        return true;
    }
};

const signature_fingerprint_seed: u64 = 0x4154_464c_5953_4947; // "ATFLYSIG"

/// Stable semantic fingerprint. Fields are encoded explicitly rather than via
/// struct memory, so padding and host endianness cannot change artifact IDs.
pub fn specializationSignatureFingerprint(signature: SpecializationSignature) u64 {
    var hasher = std.hash.Wyhash.init(signature_fingerprint_seed);
    hashU8(&hasher, 2); // schema version
    hashU8(&hasher, @intFromEnum(signature.opKind()));
    switch (signature) {
        .small_batch_matmul => |op| {
            hashU16(&hasher, @intFromEnum(op.format));
            hashU8(&hasher, @intFromEnum(op.row_bucket));
            hashU8(&hasher, @intFromEnum(op.dispatch));
            hashU8(&hasher, @intFromEnum(op.epilogue));
            hashU8(&hasher, @intFromEnum(op.activation));
            hashU8(&hasher, @intFromEnum(op.function));
            hashU8(&hasher, @intFromEnum(op.output));
        },
        .attention => |op| {
            hashU8(&hasher, @intFromEnum(op.kind));
            hashU8(&hasher, @intFromEnum(op.query));
            hashU8(&hasher, @intFromEnum(op.key));
            hashU8(&hasher, @intFromEnum(op.value));
            hashU8(&hasher, @intFromEnum(op.output));
        },
        .microkernel => |op| {
            hashU8(&hasher, @intFromEnum(op.kind));
            hashU8(&hasher, @intFromEnum(op.input));
            hashU8(&hasher, @intFromEnum(op.output));
        },
    }
    return hasher.final();
}

pub fn specializationSignatureId(
    allocator: std.mem.Allocator,
    signature: SpecializationSignature,
) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "antfly.kernel.signature.v2/{s}/{x:0>16}",
        .{ @tagName(signature.opKind()), specializationSignatureFingerprint(signature) },
    );
}

pub fn runtimeConstraintsFingerprint(constraints: RuntimeConstraints) u64 {
    var hasher = std.hash.Wyhash.init(signature_fingerprint_seed ^ 0x5255_4e54_494d_4521);
    hashU8(&hasher, 1);
    inline for (std.meta.fields(RuntimeConstraints)) |field| {
        hashDimension(&hasher, @field(constraints, field.name));
    }
    return hasher.final();
}

fn hashDimension(hasher: *std.hash.Wyhash, constraint: RuntimeDimensionConstraint) void {
    if (constraint.exactValue()) |value| {
        hashU8(hasher, 1);
        hashU32(hasher, value);
        return;
    }
    hashU8(hasher, 0);
    hashU32(hasher, constraint.min);
    hashU32(hasher, constraint.max);
    hashU32(hasher, constraint.multiple_of);
}

fn gcd(a_value: u64, b_value: u64) u64 {
    var a = a_value;
    var b = b_value;
    while (b != 0) {
        const remainder = a % b;
        a = b;
        b = remainder;
    }
    return a;
}

fn hashU8(hasher: *std.hash.Wyhash, value: u8) void {
    hasher.update(&.{value});
}

fn hashU16(hasher: *std.hash.Wyhash, value: u16) void {
    var bytes: [2]u8 = undefined;
    std.mem.writeInt(u16, &bytes, value, .little);
    hasher.update(&bytes);
}

fn hashU32(hasher: *std.hash.Wyhash, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hasher.update(&bytes);
}

test "specialization signatures have stable canonical identities" {
    const signature = SpecializationSignature{ .small_batch_matmul = .{
        .format = .q4_0,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = .gated_down,
        .activation = .q8_1,
    } };
    try signature.validate();
    try std.testing.expectEqual(@as(u64, 0x001c_b129_89a8_8694), specializationSignatureFingerprint(signature));
    try std.testing.expectEqual(
        specializationSignatureFingerprint(signature),
        specializationSignatureFingerprint(signature),
    );

    var different = signature;
    different.small_batch_matmul.activation = .f32;
    try std.testing.expect(!signature.eql(different));
    try std.testing.expect(
        specializationSignatureFingerprint(signature) != specializationSignatureFingerprint(different),
    );

    const id = try specializationSignatureId(std.testing.allocator, signature);
    defer std.testing.allocator.free(id);
    try std.testing.expect(std.mem.startsWith(u8, id, "antfly.kernel.signature.v2/small_batch_matmul/"));
}

test "runtime constraints canonicalize exact dimensions and detect overlap" {
    const fixed = RuntimeDimensionConstraint.exact(1536);
    const bounded = RuntimeDimensionConstraint{ .min = 1536, .max = 1536 };
    try std.testing.expectEqual(fixed.exactValue(), bounded.exactValue());

    const a = RuntimeDimensionConstraint{ .min = 64, .max = 256, .multiple_of = 32 };
    const b = RuntimeDimensionConstraint{ .min = 96, .max = 160, .multiple_of = 64 };
    const disjoint = RuntimeDimensionConstraint{ .min = 257, .max = 512, .multiple_of = 32 };
    try std.testing.expect(a.overlaps(b));
    try std.testing.expect(!a.overlaps(disjoint));

    const constraints = RuntimeConstraints.exactMatmul(1, 1536, 6144);
    try std.testing.expect(constraints.matches(.{ .rows = 1, .input_dim = 1536, .output_dim = 6144 }));
    try std.testing.expect(!constraints.matches(.{ .rows = 2, .input_dim = 1536, .output_dim = 6144 }));
    try std.testing.expectEqual(
        runtimeConstraintsFingerprint(.{ .input_dim = fixed }),
        runtimeConstraintsFingerprint(.{ .input_dim = bounded }),
    );
}

test "attention runtime constraints keep topology exact and length bucketed" {
    const signature = SpecializationSignature{ .attention = .{ .kind = .decode_1x } };
    var constraints = RuntimeConstraints.exactAttention(1, 256, 8, 1);
    constraints.sequence_length = .{ .min = 1, .max = 2048 };
    try constraints.validateExactFor(signature);
    try std.testing.expect(constraints.matches(.{
        .rows = 1,
        .head_dim = 256,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = 1024,
    }));
    try std.testing.expect(!constraints.matches(.{
        .rows = 1,
        .head_dim = 256,
        .query_heads = 8,
        .kv_heads = 1,
        .sequence_length = 4096,
    }));
}
