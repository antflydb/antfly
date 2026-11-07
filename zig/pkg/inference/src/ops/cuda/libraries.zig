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
        const count = std.math.cast(c_int, batches) orelse return error.InvalidCublasShape;
        const m = std.math.cast(c_int, rows) orelse return error.InvalidCublasShape;
        const k = std.math.cast(c_int, width) orelse return error.InvalidCublasShape;
        const n = std.math.cast(c_int, columns) orelse return error.InvalidCublasShape;
        if (count <= 0 or m <= 0 or k <= 0 or n <= 0) return error.InvalidCublasShape;
        try self.bindStream(stream);
        const alpha: f32 = 1;
        const beta: f32 = 0;
        // View row-major C = A B as column-major C^T = B^T A^T.
        // Positive int32 dimensions make these int64 element strides safe.
        try check(self.library.fns.sgemmStridedBatched(
            self.handle,
            if (rhs_transposed) CUBLAS_OP_T else CUBLAS_OP_N,
            if (lhs_transposed) CUBLAS_OP_T else CUBLAS_OP_N,
            n,
            m,
            k,
            &alpha,
            rhs,
            if (rhs_transposed) k else n,
            @as(i64, n) * k,
            lhs,
            if (lhs_transposed) m else k,
            @as(i64, m) * k,
            &beta,
            output,
            n,
            @as(i64, m) * n,
            count,
        ));
        self.calls +|= 1;
    }

    pub fn requireBf16(self: *const Cublas) !void {
        if (self.library.fns.setWorkspace == null or self.library.fns.gemmEx == null) return error.CublasBf16Unavailable;
    }

    /// Row-major output = input * weight^T, BF16 inputs and output. The
    /// external workspace remains caller-owned until actual stream retirement.
    pub fn linearBf16(self: *Cublas, stream: ?*anyopaque, output: *anyopaque, input: *const anyopaque, weight: *const anyopaque, rows: usize, width: usize, columns: usize, arithmetic: MatmulArithmetic, workspace: ?*anyopaque, workspace_bytes: usize, compute_major: i32) !void {
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
        const beta: f32 = 0;
        try check(self.library.fns.gemmEx.?(self.handle, CUBLAS_OP_T, CUBLAS_OP_N, n, m, k, &alpha, weight, CUDA_R_16BF, k, input, CUDA_R_16BF, k, &beta, output, CUDA_R_16BF, n, CUBLAS_COMPUTE_32F, 99));
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
            if (bound == null or ta != CUBLAS_OP_T or tb != CUBLAS_OP_N or m != 512 or n != 15 or k != 2048 or alpha.* != 1 or beta.* != 0 or at != CUDA_R_16BF or bt != CUDA_R_16BF or ct != CUDA_R_16BF or lda != 2048 or ldb != 2048 or ldc != 512 or compute != CUBLAS_COMPUTE_32F or algo != 99) bad = true;
            return if (fail) 13 else 0;
        }
    };
    Probe.sets = 0;
    Probe.calls = 0;
    Probe.bound = null;
    Probe.bad = false;
    Probe.fail = false;
    Probe.fail_bind = false;
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
    try std.testing.expectError(error.InvalidCublasArithmetic, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .full_f32, workspace, 32 * 1024 * 1024, 12));
    try std.testing.expectError(error.InvalidCublasWorkspace, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024 - 1, 12));
    try std.testing.expectEqual(@as(usize, 0), Probe.sets);
    try std.testing.expectEqual(@as(usize, 0), Probe.calls);
    try blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12);
    try std.testing.expectEqual(@as(usize, 2), Probe.sets);
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    Probe.fail = true;
    try std.testing.expectError(error.CublasError, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12));
    try std.testing.expectEqual(@as(usize, 4), Probe.sets);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
    // Simulate SDK partial publication: it records the borrowed address then
    // rejects binding, before any GEMM can be submitted.
    Probe.fail = false;
    Probe.fail_bind = true;
    try std.testing.expectError(error.CublasError, blas.linearBf16(null, output, input, weight, 15, 2048, 512, .bf16_reduced, workspace, 32 * 1024 * 1024, 12));
    try std.testing.expectEqual(@as(usize, 6), Probe.sets);
    try std.testing.expectEqual(@as(usize, 2), Probe.calls);
    try std.testing.expect(Probe.bound == null and !Probe.bad);
    try std.testing.expectEqual(@as(u64, 1), blas.calls);
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
