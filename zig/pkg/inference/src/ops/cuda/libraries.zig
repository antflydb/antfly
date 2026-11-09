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
const build_options = @import("build_options");

pub const Policy = enum {
    auto,
    off,
    required,

    pub fn current() Policy {
        if (std.mem.eql(u8, build_options.cuda_libraries, "off")) return .off;
        if (std.mem.eql(u8, build_options.cuda_libraries, "required")) return .required;
        return .auto;
    }
};

pub const CublasStatus = c_int;
pub const CUBLAS_STATUS_SUCCESS: CublasStatus = 0;
pub const CUBLAS_STATUS_NOT_SUPPORTED: CublasStatus = 15;

pub const CublasHandle = ?*anyopaque;
pub const CublasLtHandle = ?*anyopaque;
pub const CublasLtMatmulDesc = ?*anyopaque;
pub const CublasLtMatrixLayout = ?*anyopaque;
pub const CublasLtMatmulPreference = ?*anyopaque;

pub const CUBLAS_OP_N: c_int = 0;
pub const CUBLAS_OP_T: c_int = 1;

pub const CUBLAS_COMPUTE_32F: c_int = 68;

pub const CUDA_R_32F: c_int = 0;
pub const CUDA_R_16F: c_int = 2;
pub const CUDA_R_16BF: c_int = 14;

pub const CUBLASLT_ORDER_COL: c_int = 0;
pub const CUBLASLT_ORDER_ROW: c_int = 1;

pub const CUBLASLT_MATRIX_LAYOUT_ORDER: c_int = 1;

pub const CUBLASLT_MATMUL_DESC_TRANSA: c_int = 3;
pub const CUBLASLT_MATMUL_DESC_TRANSB: c_int = 4;
pub const CUBLASLT_MATMUL_DESC_EPILOGUE: c_int = 7;
pub const CUBLASLT_MATMUL_DESC_BIAS_POINTER: c_int = 8;

pub const CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES: c_int = 1;

pub const CUBLASLT_EPILOGUE_DEFAULT: c_int = 1;
pub const CUBLASLT_EPILOGUE_BIAS: c_int = 4;

pub const CublasLtMatmulAlgo = extern struct {
    data: [8]u64,
};

pub const CublasLtMatmulHeuristicResult = extern struct {
    algo: CublasLtMatmulAlgo,
    workspaceSize: usize,
    state: CublasStatus,
    wavesCount: f32,
    reserved: [4]c_int,
};

/// Arithmetic contract is independent of geometry and model identity. BF16
/// reduced partial accumulation matches eager BF16 vendor GEMM; the default
/// full-F32 contract preserves the existing Lt path and all F32 operations.
pub const MatmulArithmetic = enum {
    full_f32,
    bf16_reduced,

    /// Cold capability selection. Explicit off and optional unavailable
    /// libraries retain the old full-F32 Lt/scalar contract; required fails.
    /// A fallback is never represented as source-compatible BF16 arithmetic.
    pub fn select(policy: Policy, classic_available: bool) !MatmulArithmetic {
        if (policy == .off) return .full_f32;
        if (classic_available) return .bf16_reduced;
        if (policy == .required) return error.CudaLibrariesUnavailable;
        return .full_f32;
    }

    pub fn workspaceBytes(self: MatmulArithmetic, compute_major: i32) !usize {
        if (self == .full_f32) return 0;
        if (compute_major <= 0) return error.InvalidCudaState;
        return if (compute_major == 9 or compute_major == 10 or compute_major == 12) 32 * 1024 * 1024 else 8519680;
    }
    pub const maximum_workspace_bytes = 32 * 1024 * 1024;
};

const CublasTable = struct {
    setWorkspace: ?*const fn (CublasHandle, ?*anyopaque, usize) callconv(.c) CublasStatus = null,
    gemmEx: ?*const fn (CublasHandle, c_int, c_int, c_int, c_int, c_int, *const f32, *const anyopaque, c_int, c_int, *const anyopaque, c_int, c_int, *const f32, *anyopaque, c_int, c_int, c_int, c_int) callconv(.c) CublasStatus = null,
    create: *const fn (*CublasHandle) callconv(.c) CublasStatus,
    destroy: *const fn (CublasHandle) callconv(.c) CublasStatus,
    getVersion: *const fn (CublasHandle, *c_int) callconv(.c) CublasStatus,
    setStream: *const fn (CublasHandle, ?*anyopaque) callconv(.c) CublasStatus,
    setMathMode: *const fn (CublasHandle, c_int) callconv(.c) CublasStatus,
    sgemm: *const fn (CublasHandle, c_int, c_int, c_int, c_int, c_int, *const f32, *const anyopaque, c_int, *const anyopaque, c_int, *const f32, *anyopaque, c_int) callconv(.c) CublasStatus,
    sgemmStridedBatched: *const fn (CublasHandle, c_int, c_int, c_int, c_int, c_int, *const f32, *const anyopaque, c_int, i64, *const anyopaque, c_int, i64, *const f32, *anyopaque, c_int, i64, c_int) callconv(.c) CublasStatus,
};

pub const CublasLtTable = struct {
    create: *const fn (*CublasLtHandle) callconv(.c) CublasStatus,
    destroy: *const fn (CublasLtHandle) callconv(.c) CublasStatus,
    matmul: *const fn (
        CublasLtHandle,
        CublasLtMatmulDesc,
        ?*const anyopaque,
        ?*const anyopaque,
        CublasLtMatrixLayout,
        ?*const anyopaque,
        CublasLtMatrixLayout,
        ?*const anyopaque,
        ?*const anyopaque,
        CublasLtMatrixLayout,
        ?*anyopaque,
        CublasLtMatrixLayout,
        ?*const CublasLtMatmulAlgo,
        ?*anyopaque,
        usize,
        ?*anyopaque,
    ) callconv(.c) CublasStatus,
    matmulDescCreate: *const fn (*CublasLtMatmulDesc, c_int, c_int) callconv(.c) CublasStatus,
    matmulDescDestroy: *const fn (CublasLtMatmulDesc) callconv(.c) CublasStatus,
    matmulDescSetAttribute: *const fn (CublasLtMatmulDesc, c_int, ?*const anyopaque, usize) callconv(.c) CublasStatus,
    matrixLayoutCreate: *const fn (*CublasLtMatrixLayout, c_int, u64, u64, i64) callconv(.c) CublasStatus,
    matrixLayoutDestroy: *const fn (CublasLtMatrixLayout) callconv(.c) CublasStatus,
    matrixLayoutSetAttribute: *const fn (CublasLtMatrixLayout, c_int, ?*const anyopaque, usize) callconv(.c) CublasStatus,
    preferenceCreate: *const fn (*CublasLtMatmulPreference) callconv(.c) CublasStatus,
    preferenceDestroy: *const fn (CublasLtMatmulPreference) callconv(.c) CublasStatus,
    preferenceSetAttribute: *const fn (CublasLtMatmulPreference, c_int, ?*const anyopaque, usize) callconv(.c) CublasStatus,
    matmulAlgoGetHeuristic: *const fn (
        CublasLtHandle,
        CublasLtMatmulDesc,
        CublasLtMatrixLayout,
        CublasLtMatrixLayout,
        CublasLtMatrixLayout,
        CublasLtMatrixLayout,
        CublasLtMatmulPreference,
        c_int,
        [*]CublasLtMatmulHeuristicResult,
        *c_int,
    ) callconv(.c) CublasStatus,
};

/// An explicit SDK path is authoritative; normal system loader lookup remains
/// the default. Never discover libraries through an unrelated model runtime.
pub fn openCudnnLibrary() !std.DynLib {
    const configured = @import("antfly_platform").env.getenv("ANTFLY_INFERENCE_CUDA_CUDNN_LIBRARY");
    return std.DynLib.open(configured orelse "libcudnn.so.9") catch error.CudnnUnavailable;
}

const CublasLibrary = struct {
    lib: std.DynLib,
    fns: CublasTable,

    fn open(configured_path: ?[]const u8) !CublasLibrary {
        // An explicit training runtime is authoritative: never silently fall
        // back to another major version after a path or symbol failure.
        if (configured_path) |path| try validateTrainingLibraryPath(path);
        var lib = if (configured_path) |path| try std.DynLib.open(path) else try openAny(&cublas_names);
        errdefer lib.close();
        return .{
            .lib = lib,
            .fns = .{
                .create = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).create), "cublasCreate_v2"),
                .destroy = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).destroy), "cublasDestroy_v2"),
                .getVersion = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).getVersion), "cublasGetVersion_v2"),
                .setStream = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).setStream), "cublasSetStream_v2"),
                .setMathMode = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).setMathMode), "cublasSetMathMode"),
                .setWorkspace = lib.lookup(@typeInfo(@FieldType(CublasTable, "setWorkspace")).optional.child, "cublasSetWorkspace_v2"),
                .gemmEx = lib.lookup(@typeInfo(@FieldType(CublasTable, "gemmEx")).optional.child, "cublasGemmEx"),
                .sgemm = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).sgemm), "cublasSgemm_v2"),
                .sgemmStridedBatched = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).sgemmStridedBatched), "cublasSgemmStridedBatched"),
            },
        };
    }

    pub fn deinit(self: *CublasLibrary) void {
        self.lib.close();
    }
};

/// Stream-bound classic BLAS. F32 methods retain their original SGEMM path.
/// Explicit BF16 arithmetic uses caller-funded workspace for each submission;
/// the handle retains no borrowed workspace after the call. Handle-private SDK
/// state has the same cold owner/deinit contract as the existing Lt handle.
pub const Bf16GemmProgram = struct {
    input_layout: enum { row_major, column_major } = .row_major,
    addend: enum { zero, output } = .zero,

    pub const Span = struct { offset: usize = 0, bytes: usize = 0 };
    pub const TemporaryLayout = struct {
        input: Span,
        weight: Span,
        output: Span,
        bias: Span,
        workspace: Span,
        bytes: usize,
        fn append(cursor: *usize, bytes: usize) !Span {
            if (bytes == 0) return .{};
            const padded = try std.math.add(usize, cursor.*, 255);
            const offset = std.mem.alignBackward(usize, padded, 256);
            cursor.* = try std.math.add(usize, offset, bytes);
            return .{ .offset = offset, .bytes = bytes };
        }
        pub fn resolveSourceRows(rows: @import("../kernel_geometry.zig").SourceGemmRows, input: usize, output: usize, pack_weight: bool, pack_bias: bool, workspace_bytes: usize) !@This() {
            try rows.validate();
            if (input == 0 or output == 0) return error.InvalidShape;
            return resolve(rows.physical, input, output, pack_weight, pack_bias, workspace_bytes);
        }
        pub fn resolve(rows: usize, input: usize, output: usize, pack_weight: bool, pack_bias: bool, workspace_bytes: usize) !@This() {
            var cursor: usize = 0;
            const activation = try append(&cursor, try std.math.mul(usize, try std.math.mul(usize, rows, input), 2));
            const weight = try append(&cursor, if (pack_weight) try std.math.mul(usize, try std.math.mul(usize, input, output), 2) else 0);
            const result = try append(&cursor, try std.math.mul(usize, try std.math.mul(usize, rows, output), 2));
            const bias = try append(&cursor, if (pack_bias) try std.math.mul(usize, output, 2) else 0);
            const workspace = try append(&cursor, workspace_bytes);
            return .{ .input = activation, .weight = weight, .output = result, .bias = bias, .workspace = workspace, .bytes = cursor };
        }
    };
    /// Bias-free source temporary peak over the existing F32 result owner.
    /// Exact aligned layout includes F32-weight fallback and serial SDK work.
    pub fn maximumSourceTemporaryBytes(rows: @import("../kernel_geometry.zig").SourceGemmRows, input: usize, output: usize) !usize {
        try rows.validate();
        if (input == 0 or output == 0) return error.InvalidShape;
        return maximumTemporaryBytes(rows.physical, input, output);
    }
    pub fn maximumTemporaryBytes(rows: usize, input: usize, output: usize) !usize {
        return (try TemporaryLayout.resolve(rows, input, output, true, false, MatmulArithmetic.maximum_workspace_bytes)).bytes;
    }
};

/// Column-major F32 C = op(A) op(B), alpha=1/beta=0. Leading dimensions,
/// strides and available spans are in F32 elements, not bytes. Read-only input
/// batches may interleave; their bounding spans need not be disjoint.
pub const F32StridedBatchedLayout = struct {
    m: usize,
    n: usize,
    k: usize,
    batches: usize,
    transpose_a: bool = false,
    transpose_b: bool = false,
    lda: usize,
    ldb: usize,
    ldc: usize,
    stride_a: usize,
    stride_b: usize,
    stride_c: usize,
    a_elements: usize,
    b_elements: usize,
    c_elements: usize,

    /// Compatibility view of packed row-major C = op(lhs) op(rhs).
    pub fn packedRowMajor(batches: usize, rows: usize, width: usize, columns: usize, lhs_transposed: bool, rhs_transposed: bool) !F32StridedBatchedLayout {
        for ([_]usize{ batches, rows, width, columns }) |value| try dimension(value);
        const a_stride = try std.math.mul(usize, columns, width);
        const b_stride = try std.math.mul(usize, rows, width);
        const c_stride = try std.math.mul(usize, rows, columns);
        return .{
            .m = columns,
            .n = rows,
            .k = width,
            .batches = batches,
            .transpose_a = rhs_transposed,
            .transpose_b = lhs_transposed,
            .lda = if (rhs_transposed) width else columns,
            .ldb = if (lhs_transposed) rows else width,
            .ldc = columns,
            .stride_a = a_stride,
            .stride_b = b_stride,
            .stride_c = c_stride,
            .a_elements = try std.math.mul(usize, batches, a_stride),
            .b_elements = try std.math.mul(usize, batches, b_stride),
            .c_elements = try std.math.mul(usize, batches, c_stride),
        };
    }

    fn dimension(value: usize) !void {
        const number = std.math.cast(c_int, value) orelse return error.InvalidCublasShape;
        if (number <= 0) return error.InvalidCublasShape;
    }

    fn matrixSpan(rows: usize, columns: usize, leading: usize) !usize {
        if (leading < rows) return error.InvalidCublasShape;
        return std.math.add(usize, try std.math.mul(usize, columns - 1, leading), rows);
    }

    fn checkSpan(batches: usize, stride: usize, matrix_elements: usize, available: usize) !usize {
        // The documented strided SGEMM contract requires nonzero offsets.
        // Do not promote historical zero-stride broadcast behavior to a
        // supported multi-batch contract without separate vendor qualification.
        if ((batches > 1 and stride == 0) or std.math.cast(i64, stride) == null) return error.InvalidCublasShape;
        const end = try std.math.add(usize, try std.math.mul(usize, batches - 1, stride), matrix_elements);
        if (end > available) return error.InvalidCublasShape;
        _ = try std.math.mul(usize, available, @sizeOf(f32));
        return end;
    }

    pub fn validate(self: F32StridedBatchedLayout) !void {
        _ = try self.checkedSpans();
    }

    const UsedSpans = struct { a: usize, b: usize, c: usize };

    fn checkedSpans(self: F32StridedBatchedLayout) !UsedSpans {
        for ([_]usize{ self.m, self.n, self.k, self.batches, self.lda, self.ldb, self.ldc }) |value| try dimension(value);
        const a_span = try matrixSpan(if (self.transpose_a) self.k else self.m, if (self.transpose_a) self.m else self.k, self.lda);
        const b_span = try matrixSpan(if (self.transpose_b) self.n else self.k, if (self.transpose_b) self.k else self.n, self.ldb);
        const c_span = try matrixSpan(self.m, self.n, self.ldc);
        const used: UsedSpans = .{
            .a = try checkSpan(self.batches, self.stride_a, a_span, self.a_elements),
            .b = try checkSpan(self.batches, self.stride_b, b_span, self.b_elements),
            .c = try checkSpan(self.batches, self.stride_c, c_span, self.c_elements),
        };
        if (self.batches > 1 and self.stride_c < c_span) {
            // Also admit row-interleaved outputs when every batch's row slab
            // fits inside one column pitch. Other overlapping bounding spans
            // need a stronger proof and are rejected before SDK publication.
            const row_end = try std.math.add(usize, try std.math.mul(usize, self.batches - 1, self.stride_c), self.m);
            if (self.stride_c < self.m or row_end > self.ldc) return error.InvalidCublasShape;
        }
        return used;
    }

    const AddressSpan = struct {
        start: usize,
        end: usize,

        fn overlaps(self: AddressSpan, other: AddressSpan) bool {
            return self.start < other.end and other.start < self.end;
        }
    };

    const Addresses = struct { a: AddressSpan, b: AddressSpan, c: AddressSpan };
    fn checkedAddresses(self: F32StridedBatchedLayout, output: *anyopaque, a: *const anyopaque, b: *const anyopaque) !Addresses {
        const used = try self.checkedSpans();
        const addresses: Addresses = .{ .a = try checkAddress(a, used.a), .b = try checkAddress(b, used.b), .c = try checkAddress(output, used.c) };
        // SGEMM is not an in-place operation. Bounding intervals conservatively
        // reject output hidden in interleaved padding; A/B may overlap freely.
        if (addresses.c.overlaps(addresses.a) or addresses.c.overlaps(addresses.b)) return error.InvalidCublasShape;
        return addresses;
    }

    fn checkAddress(pointer: *const anyopaque, used_elements: usize) !AddressSpan {
        const address = @intFromPtr(pointer);
        if (address % @alignOf(f32) != 0) return error.InvalidCublasShape;
        return .{ .start = address, .end = try std.math.add(usize, address, try std.math.mul(usize, used_elements, @sizeOf(f32))) };
    }
};

pub const Cublas = struct {
    library: CublasLibrary,
    handle: CublasHandle,
    stream: ?*anyopaque,
    calls: u64 = 0,
    /// Vendor runtime version is part of retained-training checkpoint identity.
    version: u32,

    pub fn init(stream: ?*anyopaque) !Cublas {
        return initWithLibrary(stream, @import("antfly_platform").env.getenv("ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY"));
    }

    pub fn initWithLibrary(stream: ?*anyopaque, configured_path: ?[]const u8) !Cublas {
        if (Policy.current() == .off) return error.CudaLibrariesDisabled;
        var library = try CublasLibrary.open(configured_path);
        errdefer library.deinit();
        var handle: CublasHandle = null;
        try check(library.fns.create(&handle));
        errdefer _ = library.fns.destroy(handle);
        var version: c_int = 0;
        try check(library.fns.getVersion(handle, &version));
        if (version <= 0) return error.InvalidCublasVersion;
        try check(library.fns.setMathMode(handle, 0)); // CUBLAS_DEFAULT_MATH: no TF32.
        try check(library.fns.setStream(handle, stream));
        return .{ .library = library, .handle = handle, .stream = stream, .version = @intCast(version) };
    }

    pub fn deinit(self: *Cublas) void {
        _ = self.library.fns.destroy(self.handle);
        self.library.deinit();
        self.* = undefined;
    }

    pub fn linear(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, input: *const anyopaque, weight: *const anyopaque, rows: usize, width: usize, columns: usize, accumulate: bool) !void {
        return self.matrix(stream, output, input, weight, rows, width, columns, false, true, accumulate);
    }

    /// Row-major C = op(lhs) op(rhs), without copying either transpose.
    pub fn matrix(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, lhs: *const anyopaque, rhs: *const anyopaque, rows: usize, width: usize, columns: usize, lhs_transposed: bool, rhs_transposed: bool, accumulate: bool) !void {
        const m = std.math.cast(c_int, rows) orelse return error.InvalidCublasShape;
        const k = std.math.cast(c_int, width) orelse return error.InvalidCublasShape;
        const n = std.math.cast(c_int, columns) orelse return error.InvalidCublasShape;
        if (m <= 0 or k <= 0 or n <= 0) return error.InvalidCublasShape;
        try self.bindStream(stream);
        const alpha: f32 = 1;
        const beta: f32 = if (accumulate) 1 else 0;
        // Column-major C^T = op(rhs)^T op(lhs)^T.
        try check(self.library.fns.sgemm(self.handle, if (rhs_transposed) CUBLAS_OP_T else CUBLAS_OP_N, if (lhs_transposed) CUBLAS_OP_T else CUBLAS_OP_N, n, m, k, &alpha, rhs, if (rhs_transposed) k else n, lhs, if (lhs_transposed) m else k, &beta, output, n));
        self.calls +|= 1;
    }

    pub fn batched(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, lhs: *const anyopaque, rhs: *const anyopaque, batches: usize, rows: usize, width: usize, columns: usize, lhs_transposed: bool, rhs_transposed: bool) !void {
        // Column-major C^T = op(rhs)^T op(lhs)^T; preserve the exact original
        // SGEMM argument tuple, input addresses, arithmetic and batch order.
        return self.stridedBatchedF32(stream, output, rhs, lhs, try F32StridedBatchedLayout.packedRowMajor(batches, rows, width, columns, lhs_transposed, rhs_transposed));
    }

    /// The caller retains the three device spans through stream retirement.
    /// Validate all layout/address arithmetic before even rebinding the stream.
    pub fn stridedBatchedF32(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, a: *const anyopaque, b: *const anyopaque, layout: F32StridedBatchedLayout) !void {
        _ = try layout.checkedAddresses(output, a, b);
        try self.bindStream(stream);
        try self.submitStridedF32(output, a, b, layout);
    }

    pub const f32_source_workspace_bytes = 32 * 1024 * 1024;
    pub fn requireF32Workspace(self: *const Cublas) !void {
        if (self.library.fns.setWorkspace == null) return error.CublasF32WorkspaceUnavailable;
    }

    /// Same F32 GEMM ABI with explicit caller-funded source workspace. Neither
    /// the handle nor this adapter retains the address after submission; actual
    /// queued/captured use remains covered by the caller's stream/graph owner.
    pub fn stridedBatchedF32WithWorkspace(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, a: *const anyopaque, b: *const anyopaque, layout: F32StridedBatchedLayout, workspace: *anyopaque, workspace_bytes: usize) !void {
        return self.f32WithWorkspace(stream, output, a, b, layout, workspace, workspace_bytes, false);
    }

    /// Actual single-GEMM submission is a distinct SDK schedule from a batch
    /// of one. Both adapters share checked physical spans and workspace custody.
    pub fn singleF32WithWorkspace(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, a: *const anyopaque, b: *const anyopaque, layout: F32StridedBatchedLayout, workspace: *anyopaque, workspace_bytes: usize) !void {
        return self.f32WithWorkspace(stream, output, a, b, layout, workspace, workspace_bytes, true);
    }

    fn f32WithWorkspace(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, a: *const anyopaque, b: *const anyopaque, layout: F32StridedBatchedLayout, workspace: *anyopaque, workspace_bytes: usize, comptime single: bool) !void {
        if (single and layout.batches != 1) return error.InvalidCublasShape;
        const addresses = try layout.checkedAddresses(output, a, b);
        if (@intFromPtr(workspace) % 256 != 0 or workspace_bytes < f32_source_workspace_bytes) return error.InvalidCublasWorkspace;
        const occupied = try F32StridedBatchedLayout.checkAddress(workspace, f32_source_workspace_bytes / 4);
        if (occupied.overlaps(addresses.a) or occupied.overlaps(addresses.b) or occupied.overlaps(addresses.c)) return error.InvalidCublasWorkspace;
        try self.requireF32Workspace();
        try self.bindStream(stream); // setStream resets workspace: bind FIRST.
        const set_workspace = self.library.fns.setWorkspace.?;
        // Even failed SDK publication may have changed handle metadata.
        defer if (set_workspace(self.handle, null, 0) != CUBLAS_STATUS_SUCCESS) @import("antfly_platform").inference_process_supervisor.restartWorker();
        try check(set_workspace(self.handle, workspace, f32_source_workspace_bytes));
        if (single) {
            const alpha: f32 = 1;
            const beta: f32 = 0;
            try check(self.library.fns.sgemm(self.handle, if (layout.transpose_a) CUBLAS_OP_T else CUBLAS_OP_N, if (layout.transpose_b) CUBLAS_OP_T else CUBLAS_OP_N, @intCast(layout.m), @intCast(layout.n), @intCast(layout.k), &alpha, a, @intCast(layout.lda), b, @intCast(layout.ldb), &beta, output, @intCast(layout.ldc)));
            self.calls +|= 1;
        } else try self.submitStridedF32(output, a, b, layout);
    }

    fn submitStridedF32(self: *Cublas, output: *anyopaque, a: *const anyopaque, b: *const anyopaque, layout: F32StridedBatchedLayout) !void {
        const alpha: f32 = 1;
        const beta: f32 = 0;
        try check(self.library.fns.sgemmStridedBatched(
            self.handle,
            if (layout.transpose_a) CUBLAS_OP_T else CUBLAS_OP_N,
            if (layout.transpose_b) CUBLAS_OP_T else CUBLAS_OP_N,
            @intCast(layout.m),
            @intCast(layout.n),
            @intCast(layout.k),
            &alpha,
            a,
            @intCast(layout.lda),
            // Only a single batch may have an unused zero stride. Give the
            // SDK a legal nonzero offset without changing any touched address.
            @intCast(@max(1, layout.stride_a)),
            b,
            @intCast(layout.ldb),
            @intCast(@max(1, layout.stride_b)),
            &beta,
            output,
            @intCast(layout.ldc),
            @intCast(@max(1, layout.stride_c)),
            @intCast(layout.batches),
        ));
        self.calls +|= 1;
    }

    pub fn requireBf16(self: *const Cublas) !void {
        if (self.library.fns.setWorkspace == null or self.library.fns.gemmEx == null) return error.CublasBf16Unavailable;
    }

    /// Row-major output = input * weight^T, BF16 inputs and output. The
    /// external workspace remains caller-owned until actual stream retirement.
    pub fn linearBf16(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, input: *const anyopaque, weight: *const anyopaque, rows: usize, width: usize, columns: usize, arithmetic: MatmulArithmetic, workspace: ?*anyopaque, workspace_bytes: usize, compute_major: i32, program: Bf16GemmProgram) !void {
        const m = std.math.cast(c_int, rows) orelse return error.InvalidCublasShape;
        const k = std.math.cast(c_int, width) orelse return error.InvalidCublasShape;
        const n = std.math.cast(c_int, columns) orelse return error.InvalidCublasShape;
        if (m <= 0 or k <= 0 or n <= 0) return error.InvalidCublasShape;
        if (arithmetic != .bf16_reduced) return error.InvalidCublasArithmetic;
        const required = try arithmetic.workspaceBytes(compute_major);
        if (workspace == null or @intFromPtr(workspace.?) % 256 != 0 or workspace_bytes < required) return error.InvalidCublasWorkspace;
        try self.requireBf16();
        try self.bindStream(stream); // setStream resets workspace, so bind FIRST.
        const set_workspace = self.library.fns.setWorkspace.?;
        // Register cleanup BEFORE attempted SDK publication: even a rejected
        // binding may have changed handle state before returning its error.
        // Clear only handle metadata; queued/captured work retains the actual
        // caller-owned device address until its ordinary stream/graph fence.
        defer if (set_workspace(self.handle, null, 0) != CUBLAS_STATUS_SUCCESS) @import("antfly_platform").inference_process_supervisor.restartWorker();
        try check(set_workspace(self.handle, workspace, required));
        const alpha: f32 = 1;
        const beta: f32 = if (program.addend == .output) 1 else 0;
        try check(self.library.fns.gemmEx.?(self.handle, CUBLAS_OP_T, if (program.input_layout == .column_major) CUBLAS_OP_T else CUBLAS_OP_N, n, m, k, &alpha, weight, CUDA_R_16BF, k, input, CUDA_R_16BF, if (program.input_layout == .column_major) m else k, &beta, output, CUDA_R_16BF, n, CUBLAS_COMPUTE_32F, 99));
        self.calls +|= 1;
    }

    fn bindStream(self: *Cublas, stream: ?*anyopaque) !void {
        if (self.stream != stream) {
            try check(self.library.fns.setStream(self.handle, stream));
            self.stream = stream;
        }
    }

    fn check(status: CublasStatus) !void {
        if (status != CUBLAS_STATUS_SUCCESS) return error.CublasError;
    }
};

const CublasLtLibrary = struct {
    lib: std.DynLib,
    fns: CublasLtTable,
    handle: CublasLtHandle,

    fn open() !CublasLtLibrary {
        var lib = try openAny(&cublaslt_names);
        errdefer lib.close();
        const fns = CublasLtTable{
            .create = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).create), "cublasLtCreate"),
            .destroy = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).destroy), "cublasLtDestroy"),
            .matmul = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matmul), "cublasLtMatmul"),
            .matmulDescCreate = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matmulDescCreate), "cublasLtMatmulDescCreate"),
            .matmulDescDestroy = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matmulDescDestroy), "cublasLtMatmulDescDestroy"),
            .matmulDescSetAttribute = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matmulDescSetAttribute), "cublasLtMatmulDescSetAttribute"),
            .matrixLayoutCreate = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matrixLayoutCreate), "cublasLtMatrixLayoutCreate"),
            .matrixLayoutDestroy = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matrixLayoutDestroy), "cublasLtMatrixLayoutDestroy"),
            .matrixLayoutSetAttribute = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matrixLayoutSetAttribute), "cublasLtMatrixLayoutSetAttribute"),
            .preferenceCreate = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).preferenceCreate), "cublasLtMatmulPreferenceCreate"),
            .preferenceDestroy = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).preferenceDestroy), "cublasLtMatmulPreferenceDestroy"),
            .preferenceSetAttribute = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).preferenceSetAttribute), "cublasLtMatmulPreferenceSetAttribute"),
            .matmulAlgoGetHeuristic = try lookup(&lib, @TypeOf(@as(CublasLtTable, undefined).matmulAlgoGetHeuristic), "cublasLtMatmulAlgoGetHeuristic"),
        };
        var handle: CublasLtHandle = null;
        if (fns.create(&handle) != CUBLAS_STATUS_SUCCESS) return error.CudaLibrariesUnavailable;
        errdefer _ = fns.destroy(handle);
        return .{
            .lib = lib,
            .fns = fns,
            .handle = handle,
        };
    }

    pub fn deinit(self: *CublasLtLibrary) void {
        if (self.handle != null) {
            _ = self.fns.destroy(self.handle);
            self.handle = null;
        }
        self.lib.close();
    }
};

pub const CudaLibraries = struct {
    policy: Policy = .auto,
    cublas: ?CublasLibrary = null,
    cublaslt: ?CublasLtLibrary = null,

    pub fn init() !CudaLibraries {
        const policy = Policy.current();
        if (policy == .off) return .{ .policy = policy };

        var libs = CudaLibraries{
            .policy = policy,
            .cublas = CublasLibrary.open(null) catch null,
            .cublaslt = CublasLtLibrary.open() catch null,
        };
        errdefer libs.deinit();

        if (policy == .required and (!libs.hasCublas() or !libs.hasCublasLt())) {
            return error.CudaLibrariesUnavailable;
        }
        return libs;
    }

    pub fn deinit(self: *CudaLibraries) void {
        if (self.cublaslt) |*lib| lib.deinit();
        self.cublaslt = null;
        if (self.cublas) |*lib| lib.deinit();
        self.cublas = null;
    }

    pub fn hasCublas(self: *const CudaLibraries) bool {
        return self.cublas != null;
    }

    pub fn hasCublasLt(self: *const CudaLibraries) bool {
        return self.cublaslt != null and self.cublaslt.?.handle != null;
    }

    pub fn denseAccelerationAvailable(self: *const CudaLibraries) bool {
        return self.hasCublasLt();
    }

    pub fn cublasLtFns(self: *const CudaLibraries) ?*const CublasLtTable {
        if (self.cublaslt) |*lib| return &lib.fns;
        return null;
    }

    pub fn cublasLtHandle(self: *const CudaLibraries) CublasLtHandle {
        if (self.cublaslt) |lib| return lib.handle;
        return null;
    }
};

fn validateTrainingLibraryPath(path: []const u8) !void {
    if (path.len == 0 or path.len > 4096 or !std.fs.path.isAbsolute(path) or std.mem.indexOfScalar(u8, path, 0) != null)
        return error.InvalidCudaTrainingLibraryPath;
}

test "CUDA training cuBLAS selection requires an explicit absolute library path" {
    try validateTrainingLibraryPath("/opt/cuda/lib64/libcublas.so.12");
    for ([_][]const u8{ "", "libcublas.so.12", "../libcublas.so.12", "/opt/cuda/lib\x00ignored" }) |path|
        try std.testing.expectError(error.InvalidCudaTrainingLibraryPath, validateTrainingLibraryPath(path));
    const too_long = [_]u8{'/'} ++ @as([4096]u8, @splat('a'));
    try std.testing.expectError(error.InvalidCudaTrainingLibraryPath, validateTrainingLibraryPath(&too_long));
}

const cublas_names = [_][]const u8{
    "libcublas.so.13",
    "libcublas.so",
    "libcublas.so.12",
    "libcublas.so.11",
    "/usr/local/cuda-13.2/targets/x86_64-linux/lib/libcublas.so.13",
    "/usr/local/cuda-13.2/targets/x86_64-linux/lib/libcublas.so.13.4.0.1",
};

const cublaslt_names = [_][]const u8{
    "libcublasLt.so.13",
    "libcublasLt.so",
    "/usr/local/cuda-13.2/targets/x86_64-linux/lib/libcublasLt.so.13",
    "/usr/local/cuda-13.2/targets/x86_64-linux/lib/libcublasLt.so.13.4.0.1",
};

fn openAny(names: []const []const u8) !std.DynLib {
    for (names) |name| {
        if (std.DynLib.open(name)) |lib| return lib else |_| {}
    }
    return error.CudaLibrariesUnavailable;
}

fn lookup(lib: *std.DynLib, comptime T: type, name: [:0]const u8) !T {
    return lib.lookup(T, name) orelse error.CudaLibrariesUnavailable;
}

test "classic BF16 arithmetic requires explicit funded workspace and clears its borrowed handle binding" {
    const Probe = struct {
        var sets: usize = 0;
        var calls: usize = 0;
        var bound: ?*anyopaque = null;
        var bad: bool = false;
        var fail: bool = false;
        var fail_bind: bool = false;
        var column_major: bool = false;
        var add_output: bool = false;
        fn workspace(_: CublasHandle, ptr: ?*anyopaque, bytes: usize) callconv(.c) CublasStatus {
            sets += 1;
            if (ptr) |_| {
                if (bytes != 32 * 1024 * 1024) bad = true;
            } else if (bytes != 0) bad = true;
            bound = ptr;
            return if (ptr != null and fail_bind) 13 else 0;
        }
        fn gemm(_: CublasHandle, ta: c_int, tb: c_int, m: c_int, n: c_int, k: c_int, alpha: *const f32, _: *const anyopaque, at: c_int, lda: c_int, _: *const anyopaque, bt: c_int, ldb: c_int, beta: *const f32, _: *anyopaque, ct: c_int, ldc: c_int, compute: c_int, algo: c_int) callconv(.c) CublasStatus {
            calls += 1;
            if (bound == null or ta != CUBLAS_OP_T or tb != (if (column_major) CUBLAS_OP_T else CUBLAS_OP_N) or m != 512 or n != 15 or k != 2048 or alpha.* != 1 or beta.* != (if (add_output) @as(f32, 1) else 0) or at != CUDA_R_16BF or bt != CUDA_R_16BF or ct != CUDA_R_16BF or lda != 2048 or ldb != (if (column_major) @as(c_int, 15) else 2048) or ldc != 512 or compute != CUBLAS_COMPUTE_32F or algo != 99) bad = true;
            return if (fail) 13 else 0;
        }
    };
    Probe.sets = 0;
    Probe.calls = 0;
    Probe.bound = null;
    Probe.bad = false;
    Probe.fail = false;
    Probe.fail_bind = false;
    Probe.column_major = false;
    Probe.add_output = false;
    var blas: Cublas = undefined;
    blas.library.fns.setWorkspace = &Probe.workspace;
    blas.library.fns.gemmEx = &Probe.gemm;
    blas.handle = null;
    blas.stream = null;
    blas.calls = 0;
    const output: *anyopaque = @ptrFromInt(0x1000);
    const input: *const anyopaque = @ptrFromInt(0x2000);
    const weight: *const anyopaque = @ptrFromInt(0x3000);
    const workspace: *anyopaque = @ptrFromInt(0x4000);
    try std.testing.expectError(error.InvalidCublasArithmetic, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .full_f32, workspace, 32 * 1024 * 1024, 12, .{}));
    try std.testing.expectError(error.InvalidCublasWorkspace, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024 - 1, 12, .{}));
    try std.testing.expectEqual(@as(usize, 0), Probe.sets);
    try std.testing.expectEqual(@as(usize, 0), Probe.calls);
    try blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12, .{});
    try std.testing.expectEqual(@as(usize, 2), Probe.sets);
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    Probe.fail = true;
    try std.testing.expectError(error.CublasError, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12, .{}));
    try std.testing.expectEqual(@as(usize, 4), Probe.sets);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    // Simulate SDK partial publication: it records the borrowed address then
    // rejects binding, before any GEMM can be submitted.
    Probe.fail = false;
    Probe.fail_bind = true;
    try std.testing.expectError(error.CublasError, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12, .{}));
    try std.testing.expectEqual(@as(usize, 6), Probe.sets);
    try std.testing.expectEqual(@as(usize, 2), Probe.calls);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    // The caller has physically packed the borrowed input column-major and
    // seeded the output with BF16 bias. The descriptor must change both
    // transposition/leading dimension and beta, without retaining either span.
    Probe.fail_bind = false;
    Probe.column_major = true;
    Probe.add_output = true;
    try blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12, .{
        .input_layout = .column_major,
        .addend = .output,
    });
    try std.testing.expectEqual(@as(usize, 8), Probe.sets);
    try std.testing.expectEqual(@as(usize, 3), Probe.calls);
    try std.testing.expectEqual(@as(u64, 2), blas.calls);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
}

test "matmul arithmetic workspace bounds retain the full F32 default" {
    try std.testing.expectEqual(@as(usize, 0), try MatmulArithmetic.full_f32.workspaceBytes(0));
    for ([_]i32{ 9, 10, 12 }) |major| try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), try MatmulArithmetic.bf16_reduced.workspaceBytes(major));
    for ([_]i32{ 7, 8, 11, 13 }) |major| try std.testing.expectEqual(@as(usize, 8519680), try MatmulArithmetic.bf16_reduced.workspaceBytes(major));
    try std.testing.expectError(error.InvalidCudaState, MatmulArithmetic.bf16_reduced.workspaceBytes(0));
}

test "cold matmul arithmetic distinguishes optional fallback from required BF16 capability" {
    for ([_]bool{ false, true }) |available| {
        try std.testing.expectEqual(MatmulArithmetic.full_f32, try MatmulArithmetic.select(.off, available));
        try std.testing.expectEqual(@as(usize, 0), try (try MatmulArithmetic.select(.off, available)).workspaceBytes(0));
    }
    try std.testing.expectEqual(MatmulArithmetic.full_f32, try MatmulArithmetic.select(.auto, false));
    try std.testing.expectEqual(@as(usize, 0), try (try MatmulArithmetic.select(.auto, false)).workspaceBytes(0));
    for ([_]Policy{ .auto, .required }) |policy| {
        try std.testing.expectEqual(MatmulArithmetic.bf16_reduced, try MatmulArithmetic.select(policy, true));
        try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), try (try MatmulArithmetic.select(policy, true)).workspaceBytes(12));
    }
    try std.testing.expectError(error.CudaLibrariesUnavailable, MatmulArithmetic.select(.required, false));
}

test "source BF16 linear temporary bound includes actual physical layout packs and serial workspace" {
    const Program = Bf16GemmProgram;
    try std.testing.expectEqual(@as(usize, 120 * 1024 * 1024), try Program.maximumTemporaryBytes(8192, 1024, 4096));
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024 + 8 * 1024 * 1024 + 25 * 5120 * 2), try Program.maximumTemporaryBytes(25, 1024, 4096));
    try std.testing.expectError(error.Overflow, Program.maximumTemporaryBytes(std.math.maxInt(usize), 1024, 4096));
}

test "official source FFN bank bound also covers the smaller biased output layout" {
    for ([_]usize{ 1, 25, 128, 8192, 32768 }) |rows| {
        const largest = try Bf16GemmProgram.maximumTemporaryBytes(rows, 1024, 4096);
        const biased_output = try std.math.add(usize, try Bf16GemmProgram.maximumTemporaryBytes(rows, 1024, 1536), 1536 * 2);
        try std.testing.expect(largest >= biased_output);
    }
}

test "source GEMM temporary funding uses full physical input and output rows" {
    const Rows = @import("../kernel_geometry.zig").SourceGemmRows;
    const rows = try Rows.resolve(2340, 2520);
    const layout = try Bf16GemmProgram.TemporaryLayout.resolveSourceRows(rows, 768, 768, true, false, 32 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 2520 * 768 * 2), layout.input.bytes);
    try std.testing.expectEqual(@as(usize, 2520 * 768 * 2), layout.output.bytes);
    try std.testing.expectEqual(try Bf16GemmProgram.maximumTemporaryBytes(2520, 768, 768), try Bf16GemmProgram.maximumSourceTemporaryBytes(rows, 768, 768));
    try std.testing.expect(layout.bytes > try Bf16GemmProgram.maximumTemporaryBytes(2340, 768, 768));
    try std.testing.expectError(error.InvalidShape, Bf16GemmProgram.maximumSourceTemporaryBytes(.{ .logical = 0, .physical = 2520 }, 768, 768));
    try std.testing.expectError(error.InvalidShape, Bf16GemmProgram.maximumSourceTemporaryBytes(rows, 0, 768));
    try std.testing.expectError(error.InvalidShape, Bf16GemmProgram.TemporaryLayout.resolveSourceRows(rows, 768, 0, true, false, 32 * 1024 * 1024));
    try std.testing.expectError(error.Overflow, Bf16GemmProgram.maximumSourceTemporaryBytes(.{ .logical = 1, .physical = std.math.maxInt(usize) }, 768, 768));
}

// CPU-only SDK substitute: interprets the actual column-major ABI. The
// references below independently index row-major or token/head-major inputs.
const StridedGemmProbe = struct {
    const Args = struct {
        ta: c_int,
        tb: c_int,
        m: c_int,
        n: c_int,
        k: c_int,
        lda: c_int,
        ldb: c_int,
        ldc: c_int,
        sa: i64,
        sb: i64,
        sc: i64,
        batches: c_int,
        a: usize,
        b: usize,
        c: usize,
    };
    var last: Args = undefined;
    var streams: usize = 0;
    var calls: usize = 0;
    var fail: bool = false;
    fn stream(_: CublasHandle, _: ?*anyopaque) callconv(.c) CublasStatus {
        streams += 1;
        return 0;
    }
    fn gemm(_: CublasHandle, ta: c_int, tb: c_int, m: c_int, n: c_int, k: c_int, alpha: *const f32, a: *const anyopaque, lda: c_int, sa: i64, b: *const anyopaque, ldb: c_int, sb: i64, beta: *const f32, c: *anyopaque, ldc: c_int, sc: i64, batches: c_int) callconv(.c) CublasStatus {
        calls += 1;
        last = .{ .ta = ta, .tb = tb, .m = m, .n = n, .k = k, .lda = lda, .ldb = ldb, .ldc = ldc, .sa = sa, .sb = sb, .sc = sc, .batches = batches, .a = @intFromPtr(a), .b = @intFromPtr(b), .c = @intFromPtr(c) };
        if (fail or alpha.* != 1 or beta.* != 0) return 13;
        const ap: [*]const f32 = @ptrCast(@alignCast(a));
        const bp: [*]const f32 = @ptrCast(@alignCast(b));
        const cp: [*]f32 = @ptrCast(@alignCast(c));
        for (0..@intCast(batches)) |batch| {
            for (0..@intCast(n)) |col| {
                for (0..@intCast(m)) |row| {
                    var sum: f32 = 0;
                    for (0..@intCast(k)) |inner| {
                        const ai = batch * @as(usize, @intCast(sa)) + if (ta == CUBLAS_OP_N) row + inner * @as(usize, @intCast(lda)) else inner + row * @as(usize, @intCast(lda));
                        const bi = batch * @as(usize, @intCast(sb)) + if (tb == CUBLAS_OP_N) inner + col * @as(usize, @intCast(ldb)) else col + inner * @as(usize, @intCast(ldb));
                        sum += ap[ai] * bp[bi];
                    }
                    cp[batch * @as(usize, @intCast(sc)) + col * @as(usize, @intCast(ldc)) + row] = sum;
                }
            }
        }
        return 0;
    }
    fn owner() Cublas {
        streams = 0;
        calls = 0;
        fail = false;
        var result: Cublas = undefined;
        result.handle = null;
        result.stream = null;
        result.calls = 0;
        result.library.fns.setStream = &stream;
        result.library.fns.sgemmStridedBatched = &gemm;
        return result;
    }
    fn left(batch: usize, row: usize, inner: usize) f32 {
        return @floatFromInt((batch + 2 * row + inner % 7) % 5);
    }
    fn right(batch: usize, col: usize, inner: usize) f32 {
        return @floatFromInt((3 * batch + col + inner % 11) % 7);
    }
};

test "F32 strided packed compatibility preserves every row-major transpose tuple" {
    const Probe = StridedGemmProbe;
    for ([_]bool{ false, true }) |lt| {
        for ([_]bool{ false, true }) |rt| {
            var lhs: [2 * 3 * 5]f32 = undefined;
            var rhs: [2 * 5 * 2]f32 = undefined;
            var output: [2 * 3 * 2]f32 = @splat(std.math.nan(f32));
            for (0..2) |h| {
                for (0..3) |r| for (0..5) |k| {
                    lhs[h * 15 + if (lt) k * 3 + r else r * 5 + k] = Probe.left(h, r, k);
                };
                for (0..2) |c| for (0..5) |k| {
                    rhs[h * 10 + if (rt) c * 5 + k else k * 2 + c] = Probe.right(h, c, k);
                };
            }
            var blas = Probe.owner();
            try blas.batched(@ptrFromInt(1), @ptrCast(&output), @ptrCast(&lhs), @ptrCast(&rhs), 2, 3, 5, 2, lt, rt);
            try std.testing.expectEqualDeep(Probe.Args{ .ta = if (rt) CUBLAS_OP_T else CUBLAS_OP_N, .tb = if (lt) CUBLAS_OP_T else CUBLAS_OP_N, .m = 2, .n = 3, .k = 5, .lda = if (rt) 5 else 2, .ldb = if (lt) 3 else 5, .ldc = 2, .sa = 10, .sb = 15, .sc = 6, .batches = 2, .a = @intFromPtr(&rhs), .b = @intFromPtr(&lhs), .c = @intFromPtr(&output) }, Probe.last);
            for (0..2) |h| for (0..3) |r| for (0..2) |c| {
                var expected: f32 = 0;
                for (0..5) |k| expected += Probe.left(h, r, k) * Probe.right(h, c, k);
                try std.testing.expectEqual(expected, output[h * 6 + r * 2 + c]);
            };
            try std.testing.expectEqual(@as(usize, 1), Probe.streams);
            try std.testing.expectEqual(@as(u64, 1), blas.calls);
        }
    }
}

test "F32 strided interleaved TN matches independent token head addresses" {
    const Probe = StridedGemmProbe;
    var a: [13 * 8 * 128]f32 = undefined;
    var b: [36 * 8 * 128]f32 = undefined;
    var c: [8 * 13 * 36]f32 = @splat(std.math.nan(f32));
    for (0..8) |h| {
        for (0..13) |token| for (0..128) |d| {
            a[token * 1024 + h * 128 + d] = Probe.left(h, token, d);
        };
        for (0..36) |token| for (0..128) |d| {
            b[token * 1024 + h * 128 + d] = Probe.right(h, token, d);
        };
    }
    const layout: F32StridedBatchedLayout = .{ .m = 13, .n = 36, .k = 128, .batches = 8, .transpose_a = true, .lda = 1024, .ldb = 1024, .ldc = 13, .stride_a = 128, .stride_b = 128, .stride_c = 13 * 36, .a_elements = a.len, .b_elements = b.len, .c_elements = c.len };
    var blas = Probe.owner();
    try blas.stridedBatchedF32(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout);
    for (0..8) |h| for (0..36) |col| for (0..13) |row| {
        var expected: f32 = 0;
        for (0..128) |d| expected += Probe.left(h, row, d) * Probe.right(h, col, d);
        try std.testing.expectEqual(expected, c[h * 13 * 36 + col * 13 + row]);
    };
    // The last actual element, not batches * bounding matrix span, determines
    // whether this interleaved allocation is large enough.
    var short = layout;
    short.a_elements -= 1;
    try std.testing.expectError(error.InvalidCublasShape, short.validate());
    short = layout;
    short.b_elements -= 1;
    try std.testing.expectError(error.InvalidCublasShape, short.validate());
}

fn testPackedF32(comptime m: usize, comptime n: usize, comptime k: usize) !void {
    const Probe = StridedGemmProbe;
    var a: [24 * m * k]f32 = undefined;
    var b: [24 * k * n]f32 = undefined;
    var c: [24 * m * n]f32 = @splat(std.math.nan(f32));
    for (0..24) |h| {
        for (0..k) |inner| for (0..m) |row| {
            a[h * m * k + inner * m + row] = Probe.left(h, row, inner);
        };
        for (0..n) |col| for (0..k) |inner| {
            b[h * k * n + col * k + inner] = Probe.right(h, col, inner);
        };
    }
    const layout: F32StridedBatchedLayout = .{ .m = m, .n = n, .k = k, .batches = 24, .lda = m, .ldb = k, .ldc = m, .stride_a = m * k, .stride_b = k * n, .stride_c = m * n, .a_elements = a.len, .b_elements = b.len, .c_elements = c.len };
    var blas = Probe.owner();
    try blas.stridedBatchedF32(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout);
    for (0..24) |h| for (0..n) |col| for (0..m) |row| {
        var expected: f32 = 0;
        for (0..k) |inner| expected += Probe.left(h, row, inner) * Probe.right(h, col, inner);
        try std.testing.expectEqual(expected, c[h * m * n + col * m + row]);
    };
}

test "F32 strided packed NN content and context match independent logical dots" {
    try testPackedF32(24, 12, 128);
    try testPackedF32(128, 12, 24);
}

test "F32 strided output admission proves every interleaved address is disjoint" {
    var layout: F32StridedBatchedLayout = .{ .m = 2, .n = 3, .k = 1, .batches = 4, .lda = 2, .ldb = 1, .ldc = 8, .stride_a = 2, .stride_b = 3, .stride_c = 2, .a_elements = 8, .b_elements = 12, .c_elements = 24 };
    try layout.validate();
    var seen: [24]bool = @splat(false);
    for (0..4) |h| for (0..3) |col| for (0..2) |row| {
        const index = h * 2 + col * 8 + row;
        try std.testing.expect(!seen[index]);
        seen[index] = true;
    };
    try std.testing.expect(std.mem.allEqual(bool, &seen, true));
    layout.stride_c = 3;
    layout.c_elements = 27;
    try std.testing.expectError(error.InvalidCublasShape, layout.validate());
}

test "F32 strided malformed spans and pointer arithmetic fail before driver calls" {
    const Probe = StridedGemmProbe;
    const valid = try F32StridedBatchedLayout.packedRowMajor(2, 3, 5, 2, false, false);
    var variants: [12]F32StridedBatchedLayout = @splat(valid);
    variants[0].m = 0;
    variants[1].batches = @as(usize, std.math.maxInt(c_int)) + 1;
    variants[2].lda = 1;
    variants[3].ldb = 1;
    variants[4].stride_a = 0;
    variants[5].stride_b = 0;
    variants[6].stride_c = 1;
    variants[7].b_elements -= 1;
    variants[8].c_elements -= 1;
    variants[9].ldc = 1;
    variants[10].stride_c = 0;
    variants[11].lda = std.math.maxInt(usize);
    var blas = Probe.owner();
    for (variants) |layout|
        try std.testing.expectError(error.InvalidCublasShape, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout));
    var overflow = valid;
    overflow.a_elements = std.math.maxInt(usize);
    try std.testing.expectError(error.Overflow, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), overflow));
    if (comptime @bitSizeOf(usize) >= 64) {
        overflow = valid;
        overflow.batches = 4;
        overflow.stride_a = @intCast(std.math.maxInt(i64));
        try std.testing.expectError(error.Overflow, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), overflow));
        overflow = valid;
        overflow.stride_a = @as(usize, std.math.maxInt(i64)) + 1;
        try std.testing.expectError(error.InvalidCublasShape, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), overflow));
    }
    try std.testing.expectError(error.Overflow, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(std.math.maxInt(usize) - 3), @ptrFromInt(0x3000), valid));
    try std.testing.expectError(error.InvalidCublasShape, blas.stridedBatchedF32(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2001), @ptrFromInt(0x3000), valid));
    try std.testing.expectEqual(@as(usize, 0), Probe.streams);
    try std.testing.expectEqual(@as(usize, 0), Probe.calls);
    try std.testing.expectEqual(@as(u64, 0), blas.calls);
}

test "F32 strided single batch unused zero offsets normalize without changing addresses" {
    var a = [_]f32{ 2, 3 };
    var b = [_]f32{5};
    var c: [2]f32 = undefined;
    const layout: F32StridedBatchedLayout = .{ .m = 2, .n = 1, .k = 1, .batches = 1, .lda = 2, .ldb = 1, .ldc = 2, .stride_a = 0, .stride_b = 0, .stride_c = 0, .a_elements = a.len, .b_elements = b.len, .c_elements = c.len };
    var blas = StridedGemmProbe.owner();
    try blas.stridedBatchedF32(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout);
    try std.testing.expectEqualSlices(f32, &.{ 10, 15 }, &c);
    try std.testing.expectEqual(@as(i64, 1), StridedGemmProbe.last.sa);
    try std.testing.expectEqual(@as(i64, 1), StridedGemmProbe.last.sb);
    try std.testing.expectEqual(@as(i64, 1), StridedGemmProbe.last.sc);
    StridedGemmProbe.fail = true;
    try std.testing.expectError(error.CublasError, blas.stridedBatchedF32(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout));
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
}

test "F32 strided rejects output input aliases before stream or SDK publication" {
    const Probe = StridedGemmProbe;
    var storage: [32]f32 = @splat(1);
    var blas = Probe.owner();
    // C=A, C=B, and partial overlap in both address directions. Inputs use
    // honest remaining capacities; no invalid access is needed by this probe.
    for ([_][3]usize{ .{ 0, 8, 0 }, .{ 0, 8, 8 }, .{ 0, 8, 1 }, .{ 1, 8, 0 }, .{ 0, 8, 7 } }) |offsets| {
        const layout: F32StridedBatchedLayout = .{ .m = 2, .n = 1, .k = 1, .batches = 1, .lda = 2, .ldb = 1, .ldc = 2, .stride_a = 2, .stride_b = 1, .stride_c = 2, .a_elements = storage.len - offsets[0], .b_elements = storage.len - offsets[1], .c_elements = storage.len - offsets[2] };
        try std.testing.expectError(error.InvalidCublasShape, blas.stridedBatchedF32(@ptrFromInt(1), @ptrCast(&storage[offsets[2]]), @ptrCast(&storage[offsets[0]]), @ptrCast(&storage[offsets[1]]), layout));
    }
    try std.testing.expectEqual(@as(usize, 0), Probe.streams);
    try std.testing.expectEqual(@as(usize, 0), Probe.calls);
    try std.testing.expectEqual(@as(u64, 0), blas.calls);
}

test "F32 strided accepts adjacent used spans and shared read only inputs" {
    const Probe = StridedGemmProbe;
    // Available allocation tails overlap. Only the actual used addresses are
    // relevant: C immediately after or before A is valid without copying.
    for ([_][3]usize{ .{ 0, 8, 2 }, .{ 2, 8, 0 }, .{ 0, 0, 2 } }) |offsets| {
        var storage: [32]f32 = @splat(0);
        storage[offsets[0]] = 2;
        storage[offsets[0] + 1] = 3;
        if (offsets[1] != offsets[0]) storage[offsets[1]] = 5;
        const multiplier = storage[offsets[1]];
        const layout: F32StridedBatchedLayout = .{ .m = 2, .n = 1, .k = 1, .batches = 1, .lda = 2, .ldb = 1, .ldc = 2, .stride_a = 2, .stride_b = 1, .stride_c = 2, .a_elements = storage.len - offsets[0], .b_elements = storage.len - offsets[1], .c_elements = storage.len - offsets[2] };
        var blas = Probe.owner();
        try blas.stridedBatchedF32(@ptrFromInt(1), @ptrCast(&storage[offsets[2]]), @ptrCast(&storage[offsets[0]]), @ptrCast(&storage[offsets[1]]), layout);
        try std.testing.expectEqual(@as(f32, 2) * multiplier, storage[offsets[2]]);
        try std.testing.expectEqual(@as(f32, 3) * multiplier, storage[offsets[2] + 1]);
        try std.testing.expectEqual(@as(usize, 1), Probe.streams);
        try std.testing.expectEqual(@as(usize, 1), Probe.calls);
    }
}

test "F32 strided source workspace validates before SDK publication and clears failed bindings" {
    const Probe = struct {
        var calls: usize = 0;
        var clears: usize = 0;
        var fail_binding = false;
        var active = false;
        fn workspace(_: CublasHandle, pointer: ?*anyopaque, bytes: usize) callconv(.c) CublasStatus {
            calls += 1;
            if (pointer == null) {
                clears += 1;
                active = false;
                return if (bytes == 0) 0 else 13;
            }
            if (StridedGemmProbe.streams != 1 or bytes != Cublas.f32_source_workspace_bytes) return 13;
            active = true;
            return if (fail_binding) 13 else 0;
        }
    };
    var blas = StridedGemmProbe.owner();
    blas.library.fns.setWorkspace = &Probe.workspace;
    Probe.calls = 0;
    Probe.clears = 0;
    Probe.fail_binding = false;
    Probe.active = false;
    const layout = try F32StridedBatchedLayout.packedRowMajor(1, 1, 1, 1, false, false);
    const workspace: *anyopaque = @ptrFromInt(0x10000000);
    try std.testing.expectError(error.InvalidCublasWorkspace, blas.stridedBatchedF32WithWorkspace(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout, workspace, Cublas.f32_source_workspace_bytes - 1));
    try std.testing.expectError(error.InvalidCublasWorkspace, blas.stridedBatchedF32WithWorkspace(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout, @ptrFromInt(0x10000004), Cublas.f32_source_workspace_bytes));
    try std.testing.expectError(error.InvalidCublasWorkspace, blas.stridedBatchedF32WithWorkspace(@ptrFromInt(1), @ptrFromInt(0x10000000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout, workspace, Cublas.f32_source_workspace_bytes));
    try std.testing.expectError(error.InvalidCublasShape, blas.stridedBatchedF32WithWorkspace(@ptrFromInt(1), @ptrFromInt(0x2000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout, workspace, Cublas.f32_source_workspace_bytes));
    try std.testing.expectEqual(@as(usize, 0), Probe.calls);
    try std.testing.expectEqual(@as(usize, 0), StridedGemmProbe.streams);
    try std.testing.expectEqual(@as(usize, 0), StridedGemmProbe.calls);
    Probe.fail_binding = true;
    try std.testing.expectError(error.CublasError, blas.stridedBatchedF32WithWorkspace(@ptrFromInt(1), @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), layout, workspace, Cublas.f32_source_workspace_bytes));
    try std.testing.expectEqual(@as(usize, 2), Probe.calls);
    try std.testing.expectEqual(@as(usize, 1), Probe.clears);
    try std.testing.expect(!Probe.active);
    try std.testing.expectEqual(@as(usize, 0), StridedGemmProbe.calls);
    try std.testing.expectEqual(@as(u64, 0), blas.calls);
}

test "F32 strided source workspace survives submission and clears after SDK failure" {
    const Probe = struct {
        var active = false;
        var clears: usize = 0;
        fn workspace(_: CublasHandle, pointer: ?*anyopaque, bytes: usize) callconv(.c) CublasStatus {
            if (pointer == null) {
                active = false;
                clears += 1;
                return 0;
            }
            if (bytes != Cublas.f32_source_workspace_bytes) return 13;
            active = true;
            return 0;
        }
        fn gemm(handle: CublasHandle, ta: c_int, tb: c_int, m: c_int, n: c_int, k: c_int, alpha: *const f32, a: *const anyopaque, lda: c_int, sa: i64, b: *const anyopaque, ldb: c_int, sb: i64, beta: *const f32, c: *anyopaque, ldc: c_int, sc: i64, batches: c_int) callconv(.c) CublasStatus {
            if (!active) return 13;
            return StridedGemmProbe.gemm(handle, ta, tb, m, n, k, alpha, a, lda, sa, b, ldb, sb, beta, c, ldc, sc, batches);
        }
    };
    var blas = StridedGemmProbe.owner();
    blas.library.fns.setWorkspace = &Probe.workspace;
    blas.library.fns.sgemmStridedBatched = &Probe.gemm;
    Probe.active = false;
    Probe.clears = 0;
    var a = [_]f32{2};
    var b = [_]f32{3};
    var c: [1]f32 = undefined;
    const layout = try F32StridedBatchedLayout.packedRowMajor(1, 1, 1, 1, false, false);
    try blas.stridedBatchedF32WithWorkspace(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout, @ptrFromInt(0x10000000), Cublas.f32_source_workspace_bytes);
    try std.testing.expectEqual(@as(f32, 6), c[0]);
    try std.testing.expect(!Probe.active and Probe.clears == 1);
    StridedGemmProbe.fail = true;
    try std.testing.expectError(error.CublasError, blas.stridedBatchedF32WithWorkspace(null, @ptrCast(&c), @ptrCast(&a), @ptrCast(&b), layout, @ptrFromInt(0x10000000), Cublas.f32_source_workspace_bytes));
    try std.testing.expect(!Probe.active and Probe.clears == 2);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
}

test "F32 single source GEMM preserves captured NT physical SDK tuple" {
    const Probe = struct {
        var calls: usize = 0;
        var active = false;
        fn workspace(_: CublasHandle, pointer: ?*anyopaque, bytes: usize) callconv(.c) CublasStatus {
            active = pointer != null;
            return if (bytes == (if (active) Cublas.f32_source_workspace_bytes else @as(usize, 0))) 0 else 13;
        }
        fn gemm(_: CublasHandle, ta: c_int, tb: c_int, m: c_int, n: c_int, k: c_int, alpha: *const f32, _: *const anyopaque, lda: c_int, _: *const anyopaque, ldb: c_int, beta: *const f32, _: *anyopaque, ldc: c_int) callconv(.c) CublasStatus {
            if (!active or ta != 0 or tb != 1 or m != 768 or n != 280 or k != 2520 or lda != 768 or ldb != 280 or ldc != 768 or alpha.* != 1 or beta.* != 0) return 13;
            calls += 1;
            return 0;
        }
    };
    const layout: F32StridedBatchedLayout = .{ .m = 768, .n = 280, .k = 2520, .batches = 1, .transpose_b = true, .lda = 768, .ldb = 280, .ldc = 768, .stride_a = 2520 * 768, .stride_b = 2520 * 280, .stride_c = 280 * 768, .a_elements = 2520 * 768, .b_elements = 2520 * 280, .c_elements = 280 * 768 };
    var blas = StridedGemmProbe.owner();
    blas.library.fns.setWorkspace = &Probe.workspace;
    blas.library.fns.sgemm = &Probe.gemm;
    Probe.calls = 0;
    Probe.active = false;
    try blas.singleF32WithWorkspace(null, @ptrFromInt(0x3000000), @ptrFromInt(0x1000000), @ptrFromInt(0x2000000), layout, @ptrFromInt(0x10000000), Cublas.f32_source_workspace_bytes);
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    try std.testing.expect(!Probe.active);
    var invalid = layout;
    invalid.batches = 2;
    try std.testing.expectError(error.InvalidCublasShape, blas.singleF32WithWorkspace(null, @ptrFromInt(0x3000000), @ptrFromInt(0x1000000), @ptrFromInt(0x2000000), invalid, @ptrFromInt(0x10000000), Cublas.f32_source_workspace_bytes));
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
}
