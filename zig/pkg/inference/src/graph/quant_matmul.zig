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

pub const DispatchKind = enum(u8) {
    scalar = 0,
    mmv = 1,
    small_batch = 2,
    mm = 3,
};

pub const Primitive = enum(u8) {
    /// One decode row, shaped like a backend mul_mat_vec / mul_mv.
    mat_vec,
    /// Small prompt rows that still behave like repeated vector products.
    mat_vec_ext,
    /// Larger prompt rows, shaped like a backend mul_mat / mul_mm.
    mat_mat,
    /// Correctness fallback only.
    scalar,
};

pub const Operator = enum(u8) {
    fallback = 0,
    mul_mv = 1,
    mul_mv_ext = 2,
    mul_mm = 3,
    get_rows = 4,
    set_rows = 5,
    cpy_q_to_f32 = 6,
    cpy_f32_to_q = 7,
    attention_flash = 8,
    attention_paged = 9,
    attention_quantized_kv = 10,
};

pub const RowBucket = enum(u8) {
    rows_0 = 0,
    rows_1 = 1,
    rows_2_8 = 2,
    rows_9_64 = 3,
    rows_65_plus = 4,
};

pub const Format = enum(u16) {
    unknown = 0,
    q4_0,
    q4_1,
    q5_0,
    q5_1,
    q8_0,
    q8_1,
    q2_k,
    q3_k,
    q4_k,
    q5_k,
    q6_k,
    q8_k,
    iq1_s,
    iq1_m,
    iq2_xxs,
    iq2_xs,
    iq2_s,
    iq3_xxs,
    iq3_s,
    iq4_nl,
    iq4_xs,
    tq1_0,
    tq2_0,
    i2_s,
    i8_s,
    tl1,
    tl2,
    mxfp4,
    nvfp4,
    q1_0,
    f32 = 254,

    pub fn valuesPerBlock(self: Format) ?usize {
        return switch (self) {
            .unknown => null,
            .f32 => 1,
            .tl1, .tl2 => 1,
            .q4_0, .q4_1, .q5_0, .q5_1, .q8_0, .q8_1, .iq4_nl, .mxfp4 => 32,
            .q1_0, .i2_s => 128,
            .i8_s => 1,
            .q2_k,
            .q3_k,
            .q4_k,
            .q5_k,
            .q6_k,
            .q8_k,
            .iq1_s,
            .iq1_m,
            .iq2_xxs,
            .iq2_xs,
            .iq2_s,
            .iq3_xxs,
            .iq3_s,
            .iq4_xs,
            .tq1_0,
            .tq2_0,
            => 256,
            .nvfp4 => 64,
        };
    }

    pub fn bytesPerBlock(self: Format) ?usize {
        return switch (self) {
            .unknown => null,
            .f32 => 4,
            .q1_0 => 18,
            .i2_s => 32,
            .i8_s => 1,
            .q2_k => 84,
            .q3_k => 110,
            .q4_0 => 18,
            .q4_1 => 20,
            .q4_k => 144,
            .q5_0 => 22,
            .q5_1 => 24,
            .q5_k => 176,
            .q6_k => 210,
            .q8_0 => 34,
            .q8_1 => 36,
            .q8_k => 292,
            .iq4_nl => 18,
            .iq4_xs => 136,
            .mxfp4 => 17,
            .tl1,
            .tl2,
            => 1,
            .iq1_s,
            .iq1_m,
            .iq2_xxs,
            .iq2_s,
            .iq3_xxs,
            .iq3_s,
            .tq1_0,
            .tq2_0,
            => null,
            .iq2_xs => 74,
            .nvfp4 => 36,
        };
    }
};

pub const PackedFormatDescriptor = struct {
    format: Format,
    values_per_block: usize,
    bytes_per_block: usize,
    load_helper: Helper,
    operators: OperatorSupport,

    pub const Helper = enum(u8) {
        q1_0,
        i2_s,
        i8_s,
        q2_k,
        q3_k,
        q4_0,
        q4_1,
        q4_k,
        q5_0,
        q5_1,
        q5_k,
        q6_k,
        q8_0,
        q8_1,
        q8_k,
        iq4_nl,
        iq4_xs,
        mxfp4,
        nvfp4,
        iq2_xs,
        tl1,
        tl2,
        unsupported,
    };

    pub fn supported(self: PackedFormatDescriptor) bool {
        return self.load_helper != .unsupported and self.values_per_block != 0 and self.bytes_per_block != 0;
    }
};

pub const OperatorSupport = struct {
    scalar_linear: bool = false,
    mul_mv: bool = false,
    mul_mv_ext: bool = false,
    mul_mm: bool = false,
    get_rows: bool = false,
    set_rows: bool = false,
    cpy_q_to_f32: bool = false,
    cpy_f32_to_q: bool = false,

    pub fn supports(self: OperatorSupport, operator: Operator) bool {
        return switch (operator) {
            .fallback => true,
            .mul_mv => self.mul_mv,
            .mul_mv_ext => self.mul_mv_ext,
            .mul_mm => self.mul_mm,
            .get_rows => self.get_rows,
            .set_rows => self.set_rows,
            .cpy_q_to_f32 => self.cpy_q_to_f32,
            .cpy_f32_to_q => self.cpy_f32_to_q,
            .attention_flash, .attention_paged, .attention_quantized_kv => false,
        };
    }
};

pub const Shape = struct {
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    format: Format,
};

pub const Plan = struct {
    dispatch: DispatchKind,
    primitive: Primitive,
    operator: Operator,
    row_bucket: RowBucket,
    format: Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

pub fn plan(shape: Shape) Plan {
    return .{
        .dispatch = select(shape),
        .primitive = primitiveForDispatch(select(shape)),
        .operator = operatorForDispatch(shape, select(shape)),
        .row_bucket = rowBucket(shape.rows),
        .format = shape.format,
        .rows = shape.rows,
        .in_dim = shape.in_dim,
        .out_dim = shape.out_dim,
    };
}

pub fn q8_0(rows: usize, in_dim: usize, out_dim: usize) Plan {
    return plan(.{
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .format = .q8_0,
    });
}

pub fn select(shape: Shape) DispatchKind {
    if (!validShape(shape)) return .scalar;
    if (shape.rows >= 2 and shape.rows <= 8) return .small_batch;
    if (shape.rows >= 9) return .mm;
    return .mmv;
}

pub fn primitiveForDispatch(dispatch: DispatchKind) Primitive {
    return switch (dispatch) {
        .scalar => .scalar,
        .mmv => .mat_vec,
        .small_batch => .mat_vec_ext,
        .mm => .mat_mat,
    };
}

pub fn operatorForDispatch(shape: Shape, dispatch: DispatchKind) Operator {
    const descriptor = packedFormatDescriptor(shape.format);
    return switch (dispatch) {
        .scalar => if (descriptor.operators.scalar_linear) .fallback else .fallback,
        .mmv => if (descriptor.operators.mul_mv) .mul_mv else .fallback,
        .small_batch => if (descriptor.operators.mul_mv_ext) .mul_mv_ext else .fallback,
        .mm => if (descriptor.operators.mul_mm) .mul_mm else .fallback,
    };
}

pub fn packedFormatDescriptor(format: Format) PackedFormatDescriptor {
    const values_per_block = format.valuesPerBlock() orelse 0;
    const bytes_per_block = format.bytesPerBlock() orelse 0;
    return .{
        .format = format,
        .values_per_block = values_per_block,
        .bytes_per_block = bytes_per_block,
        .load_helper = switch (format) {
            .f32 => .unsupported,
            .q1_0 => .q1_0,
            .i2_s => .i2_s,
            .i8_s => .i8_s,
            .q2_k => .q2_k,
            .q3_k => .q3_k,
            .q4_0 => .q4_0,
            .q4_1 => .q4_1,
            .q4_k => .q4_k,
            .q5_0 => .q5_0,
            .q5_1 => .q5_1,
            .q5_k => .q5_k,
            .q6_k => .q6_k,
            .q8_0 => .q8_0,
            .q8_1 => .q8_1,
            .q8_k => .q8_k,
            .iq4_nl => .iq4_nl,
            .iq4_xs => .iq4_xs,
            .mxfp4 => .mxfp4,
            .nvfp4 => .nvfp4,
            .iq2_xs => .iq2_xs,
            .tl1 => .tl1,
            .tl2 => .tl2,
            else => .unsupported,
        },
        .operators = operatorSupport(format),
    };
}

pub fn operatorSupport(format: Format) OperatorSupport {
    return switch (format) {
        .f32 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
        },
        .q8_0 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q4_0 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q5_0 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q4_k => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q5_k => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q6_k => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q8_k,
        .iq4_nl,
        .iq4_xs,
        .mxfp4,
        .nvfp4,
        .iq2_xs,
        .tl1,
        .tl2,
        => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
        },
        .q4_1 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q5_1 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q8_1 => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
            .get_rows = true,
            .set_rows = true,
            .cpy_q_to_f32 = true,
            .cpy_f32_to_q = true,
        },
        .q1_0,
        .i2_s,
        .i8_s,
        .q2_k,
        .q3_k,
        => .{
            .scalar_linear = true,
            .mul_mv = true,
            .mul_mv_ext = true,
            .mul_mm = true,
        },
        else => .{},
    };
}

pub const RowOpKind = enum(u8) {
    get_rows,
    set_rows,
};

pub const RowOpPlan = struct {
    operator: Operator,
    kind: RowOpKind,
    format: Format,
    rows: usize,
    dim: usize,
};

pub fn rowOpPlan(format: Format, kind: RowOpKind, rows: usize, dim: usize) RowOpPlan {
    const support = operatorSupport(format);
    const operator: Operator = switch (kind) {
        .get_rows => if (support.get_rows and rows > 0 and dim > 0) .get_rows else .fallback,
        .set_rows => if (support.set_rows and rows > 0 and dim > 0) .set_rows else .fallback,
    };
    return .{
        .operator = operator,
        .kind = kind,
        .format = format,
        .rows = rows,
        .dim = dim,
    };
}

pub const CopyOpKind = enum(u8) {
    q_to_f32,
    f32_to_q,
};

pub const CopyOpPlan = struct {
    operator: Operator,
    kind: CopyOpKind,
    format: Format,
    rows: usize,
    dim: usize,
};

pub fn copyOpPlan(format: Format, kind: CopyOpKind, rows: usize, dim: usize) CopyOpPlan {
    const support = operatorSupport(format);
    const operator: Operator = switch (kind) {
        .q_to_f32 => if (support.cpy_q_to_f32 and rows > 0 and dim > 0) .cpy_q_to_f32 else .fallback,
        .f32_to_q => if (support.cpy_f32_to_q and rows > 0 and dim > 0) .cpy_f32_to_q else .fallback,
    };
    return .{
        .operator = operator,
        .kind = kind,
        .format = format,
        .rows = rows,
        .dim = dim,
    };
}

pub const AttentionKvFormat = enum(u8) {
    f32,
    polar4,
    turbo3,
    quantized,
};

pub const AttentionStorage = enum(u8) {
    dense,
    paged,
};

pub const AttentionPlan = struct {
    operator: Operator,
    q_len: usize,
    kv_len: usize,
    head_dim: usize,
    kv_format: AttentionKvFormat,
    storage: AttentionStorage = .dense,
};

pub fn attentionPlan(q_len: usize, kv_len: usize, head_dim: usize, kv_format: AttentionKvFormat) AttentionPlan {
    return attentionPlanWithStorage(q_len, kv_len, head_dim, kv_format, .dense);
}

pub fn attentionPlanWithStorage(q_len: usize, kv_len: usize, head_dim: usize, kv_format: AttentionKvFormat, storage: AttentionStorage) AttentionPlan {
    const valid = q_len > 0 and kv_len > 0 and head_dim > 0;
    const operator: Operator = if (!valid)
        .fallback
    else switch (storage) {
        .dense => switch (kv_format) {
            .f32 => .attention_flash,
            .polar4, .turbo3, .quantized => .attention_quantized_kv,
        },
        .paged => switch (kv_format) {
            .f32, .quantized => .attention_paged,
            .polar4, .turbo3 => .attention_quantized_kv,
        },
    };
    return .{
        .operator = operator,
        .q_len = q_len,
        .kv_len = kv_len,
        .head_dim = head_dim,
        .kv_format = kv_format,
        .storage = storage,
    };
}

pub fn rowBucket(rows: usize) RowBucket {
    if (rows == 0) return .rows_0;
    if (rows == 1) return .rows_1;
    if (rows <= 8) return .rows_2_8;
    if (rows <= 64) return .rows_9_64;
    return .rows_65_plus;
}

fn validShape(shape: Shape) bool {
    if (shape.rows == 0 or shape.in_dim == 0 or shape.out_dim == 0) return false;
    switch (shape.format) {
        .tl1 => return bitnetTl1ShapeSupported(shape.out_dim, shape.in_dim),
        .tl2 => return bitnetTl2ShapeSupported(shape.out_dim, shape.in_dim),
        else => {},
    }
    const values_per_block = shape.format.valuesPerBlock() orelse return false;
    if (values_per_block == 0) return false;
    return shape.in_dim % values_per_block == 0;
}

fn bitnetTl1ShapeSupported(rows: usize, cols: usize) bool {
    return switch (rows) {
        1024 => cols == 4096,
        1536 => cols == 4096 or cols == 1536,
        3200 => cols == 8640 or cols == 3200,
        4096 => cols == 1536 or cols == 14336 or cols == 4096,
        8640 => cols == 3200,
        14336 => cols == 4096,
        else => false,
    };
}

fn bitnetTl2ShapeSupported(rows: usize, cols: usize) bool {
    return switch (rows) {
        1024 => cols == 4096,
        1536 => cols == 4096 or cols == 1536,
        3200 => cols == 8640 or cols == 3200,
        4096 => cols == 1536 or cols == 14336 or cols == 4096,
        8640 => cols == 3200,
        14336 => cols == 4096,
        else => false,
    };
}

test "quant matmul selector mirrors ggml row buckets for Q8_0" {
    try std.testing.expectEqual(DispatchKind.scalar, select(.{ .rows = 0, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
    try std.testing.expectEqual(DispatchKind.mmv, select(.{ .rows = 1, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
    try std.testing.expectEqual(DispatchKind.small_batch, select(.{ .rows = 2, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
    try std.testing.expectEqual(DispatchKind.small_batch, select(.{ .rows = 8, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
    try std.testing.expectEqual(DispatchKind.mm, select(.{ .rows = 9, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
    try std.testing.expectEqual(DispatchKind.mm, select(.{ .rows = 65, .in_dim = 2048, .out_dim = 2048, .format = .q8_0 }));
}

test "quant matmul plan exposes backend-neutral operator buckets" {
    try std.testing.expectEqual(Primitive.mat_vec, q8_0(1, 2048, 2048).primitive);
    try std.testing.expectEqual(Operator.mul_mv, q8_0(1, 2048, 2048).operator);
    try std.testing.expectEqual(Primitive.mat_vec_ext, q8_0(4, 2048, 2048).primitive);
    try std.testing.expectEqual(Operator.mul_mv_ext, q8_0(4, 2048, 2048).operator);
    try std.testing.expectEqual(Primitive.mat_mat, q8_0(128, 2048, 2048).primitive);
    try std.testing.expectEqual(Operator.mul_mm, q8_0(128, 2048, 2048).operator);
    try std.testing.expectEqual(Primitive.scalar, plan(.{
        .rows = 1,
        .in_dim = 2049,
        .out_dim = 2048,
        .format = .q8_0,
    }).primitive);
    try std.testing.expectEqual(Operator.fallback, plan(.{
        .rows = 1,
        .in_dim = 2049,
        .out_dim = 2048,
        .format = .q8_0,
    }).operator);
}

test "quant matmul packed format descriptors gate kernel helper support" {
    const q8 = packedFormatDescriptor(.q8_0);
    try std.testing.expect(q8.supported());
    try std.testing.expectEqual(@as(usize, 32), q8.values_per_block);
    try std.testing.expectEqual(@as(usize, 34), q8.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.q8_0, q8.load_helper);

    const q4k = packedFormatDescriptor(.q4_k);
    try std.testing.expect(q4k.supported());
    try std.testing.expectEqual(@as(usize, 256), q4k.values_per_block);
    try std.testing.expectEqual(@as(usize, 144), q4k.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.q4_k, q4k.load_helper);

    const q6k = packedFormatDescriptor(.q6_k);
    try std.testing.expect(q6k.supported());
    try std.testing.expectEqual(@as(usize, 256), q6k.values_per_block);
    try std.testing.expectEqual(@as(usize, 210), q6k.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.q6_k, q6k.load_helper);

    const mxfp4 = packedFormatDescriptor(.mxfp4);
    try std.testing.expect(mxfp4.supported());
    try std.testing.expectEqual(@as(usize, 32), mxfp4.values_per_block);
    try std.testing.expectEqual(@as(usize, 17), mxfp4.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.mxfp4, mxfp4.load_helper);

    const nvfp4 = packedFormatDescriptor(.nvfp4);
    try std.testing.expect(nvfp4.supported());
    try std.testing.expectEqual(@as(usize, 64), nvfp4.values_per_block);
    try std.testing.expectEqual(@as(usize, 36), nvfp4.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.nvfp4, nvfp4.load_helper);

    const iq2_xs = packedFormatDescriptor(.iq2_xs);
    try std.testing.expect(iq2_xs.supported());
    try std.testing.expectEqual(@as(usize, 256), iq2_xs.values_per_block);
    try std.testing.expectEqual(@as(usize, 74), iq2_xs.bytes_per_block);
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.iq2_xs, iq2_xs.load_helper);

    const tl1 = packedFormatDescriptor(.tl1);
    try std.testing.expect(tl1.supported());
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.tl1, tl1.load_helper);

    const tl2 = packedFormatDescriptor(.tl2);
    try std.testing.expect(tl2.supported());
    try std.testing.expectEqual(PackedFormatDescriptor.Helper.tl2, tl2.load_helper);
}

test "quant matmul selector uses quant block alignment generically" {
    try std.testing.expectEqual(DispatchKind.scalar, select(.{
        .rows = 1,
        .in_dim = 128,
        .out_dim = 2048,
        .format = .q4_k,
    }));
    try std.testing.expectEqual(DispatchKind.mmv, select(.{
        .rows = 1,
        .in_dim = 256,
        .out_dim = 2048,
        .format = .q4_k,
    }));
    try std.testing.expectEqual(DispatchKind.mm, select(.{
        .rows = 16,
        .in_dim = 256,
        .out_dim = 2048,
        .format = .q4_k,
    }));
    try std.testing.expectEqual(DispatchKind.scalar, select(.{
        .rows = 1,
        .in_dim = 2048,
        .out_dim = 2048,
        .format = .unknown,
    }));
}

test "quant matmul operator support distinguishes real ggml kernels from scalar fallback" {
    try std.testing.expect(operatorSupport(.q8_0).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q8_0).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q8_0).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q8_0).supports(.get_rows));

    try std.testing.expect(operatorSupport(.q4_0).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q4_0).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q4_0).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q4_0).supports(.get_rows));
    try std.testing.expect(operatorSupport(.q5_0).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q5_0).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q5_0).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q5_0).supports(.cpy_q_to_f32));
    try std.testing.expect(operatorSupport(.q4_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q4_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q4_k).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q4_k).supports(.get_rows));
    try std.testing.expect(operatorSupport(.q4_k).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q4_k).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q5_k).supports(.cpy_q_to_f32));
    try std.testing.expect(operatorSupport(.q5_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q5_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q5_k).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q5_k).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q5_k).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q6_k).supports(.cpy_q_to_f32));
    try std.testing.expect(operatorSupport(.q6_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q6_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q6_k).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q6_k).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q6_k).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q4_1).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q4_1).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q4_1).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q4_1).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q4_1).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q5_1).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q5_1).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q5_1).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q5_1).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q5_1).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q8_1).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q8_1).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q8_1).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q8_1).supports(.set_rows));
    try std.testing.expect(operatorSupport(.q8_1).supports(.cpy_f32_to_q));
    try std.testing.expect(operatorSupport(.q8_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q8_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q8_k).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.iq4_nl).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.iq4_nl).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.iq4_nl).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.iq4_xs).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.iq4_xs).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.iq4_xs).supports(.mul_mm));

    try std.testing.expect(operatorSupport(.mxfp4).scalar_linear);
    try std.testing.expect(operatorSupport(.mxfp4).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.mxfp4).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.mxfp4).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.nvfp4).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.nvfp4).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.nvfp4).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.iq2_xs).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.iq2_xs).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.iq2_xs).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.tl1).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.tl1).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.tl1).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.tl2).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.tl2).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.tl2).supports(.mul_mm));

    try std.testing.expect(operatorSupport(.q1_0).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q1_0).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q1_0).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.i2_s).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.i2_s).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.i2_s).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.i8_s).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.i8_s).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.i8_s).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q2_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q2_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q2_k).supports(.mul_mm));
    try std.testing.expect(operatorSupport(.q3_k).supports(.mul_mv));
    try std.testing.expect(operatorSupport(.q3_k).supports(.mul_mv_ext));
    try std.testing.expect(operatorSupport(.q3_k).supports(.mul_mm));
}

test "quant row copy and attention plans expose missing ggml operator surface" {
    const q8_get = rowOpPlan(.q8_0, .get_rows, 4, 2048);
    try std.testing.expectEqual(Operator.get_rows, q8_get.operator);

    const q8_set = rowOpPlan(.q8_0, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q8_set.operator);

    const q4_set = rowOpPlan(.q4_0, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q4_set.operator);

    const q8_copy = copyOpPlan(.q8_0, .q_to_f32, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_q_to_f32, q8_copy.operator);

    const q4_copy = copyOpPlan(.q4_0, .q_to_f32, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_q_to_f32, q4_copy.operator);

    const q5_get = rowOpPlan(.q5_0, .get_rows, 4, 2048);
    try std.testing.expectEqual(Operator.get_rows, q5_get.operator);

    const q5_set = rowOpPlan(.q5_0, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q5_set.operator);

    const q4k_copy = copyOpPlan(.q4_k, .q_to_f32, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_q_to_f32, q4k_copy.operator);

    const q4k_set = rowOpPlan(.q4_k, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q4k_set.operator);

    const q5k_set = rowOpPlan(.q5_k, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q5k_set.operator);

    const q6k_get = rowOpPlan(.q6_k, .get_rows, 4, 2048);
    try std.testing.expectEqual(Operator.get_rows, q6k_get.operator);

    const q6k_set = rowOpPlan(.q6_k, .set_rows, 4, 2048);
    try std.testing.expectEqual(Operator.set_rows, q6k_set.operator);

    const iq_copy = copyOpPlan(.iq4_nl, .q_to_f32, 4, 2048);
    try std.testing.expectEqual(Operator.fallback, iq_copy.operator);

    const q8_quantize = copyOpPlan(.q8_0, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q8_quantize.operator);

    const q4_quantize = copyOpPlan(.q4_0, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q4_quantize.operator);

    const q41_quantize = copyOpPlan(.q4_1, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q41_quantize.operator);

    const q5_quantize = copyOpPlan(.q5_0, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q5_quantize.operator);

    const q51_quantize = copyOpPlan(.q5_1, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q51_quantize.operator);

    const q81_quantize = copyOpPlan(.q8_1, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q81_quantize.operator);

    const q6k_quantize = copyOpPlan(.q6_k, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q6k_quantize.operator);

    const q4k_quantize = copyOpPlan(.q4_k, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q4k_quantize.operator);

    const q5k_quantize = copyOpPlan(.q5_k, .f32_to_q, 4, 2048);
    try std.testing.expectEqual(Operator.cpy_f32_to_q, q5k_quantize.operator);

    const f32_attn = attentionPlan(128, 128, 256, .f32);
    try std.testing.expectEqual(Operator.attention_flash, f32_attn.operator);
    try std.testing.expectEqual(AttentionStorage.dense, f32_attn.storage);

    const f32_paged_attn = attentionPlanWithStorage(128, 128, 256, .f32, .paged);
    try std.testing.expectEqual(Operator.attention_paged, f32_paged_attn.operator);
    try std.testing.expectEqual(AttentionStorage.paged, f32_paged_attn.storage);

    const polar_attn = attentionPlan(1, 128, 256, .polar4);
    try std.testing.expectEqual(Operator.attention_quantized_kv, polar_attn.operator);

    const polar_paged_attn = attentionPlanWithStorage(1, 128, 256, .polar4, .paged);
    try std.testing.expectEqual(Operator.attention_quantized_kv, polar_paged_attn.operator);
    try std.testing.expectEqual(AttentionStorage.paged, polar_paged_attn.storage);
}
