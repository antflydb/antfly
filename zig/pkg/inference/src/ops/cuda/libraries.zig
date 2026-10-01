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

const CublasTable = struct {
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
                .sgemm = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).sgemm), "cublasSgemm_v2"),
                .sgemmStridedBatched = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).sgemmStridedBatched), "cublasSgemmStridedBatched"),
            },
        };
    }

    fn deinit(self: *CublasLibrary) void {
        self.lib.close();
    }
};

/// Stream-bound full-precision BLAS for resident training. Keep the standard
/// SGEMM reduction path used by the Python CUDA reference; serving keeps its
/// existing cuBLASLt policies. Handle-private driver memory is not an
/// application-managed allocation, like the existing cuBLASLt handle.
pub const CublasF32 = struct {
    library: CublasLibrary,
    handle: CublasHandle,
    stream: ?*anyopaque,
    calls: u64 = 0,
    /// Vendor runtime version is part of retained-training checkpoint identity.
    version: u32,

    pub fn init(stream: ?*anyopaque) !CublasF32 {
        return initWithLibrary(stream, @import("antfly_platform").env.getenv("ANTFLY_INFERENCE_CUDA_TRAINING_CUBLAS_LIBRARY"));
    }

    pub fn initWithLibrary(stream: ?*anyopaque, configured_path: ?[]const u8) !CublasF32 {
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

    pub fn deinit(self: *CublasF32) void {
        _ = self.library.fns.destroy(self.handle);
        self.library.deinit();
        self.* = undefined;
    }

    pub fn linear(self: *CublasF32, stream: ?*anyopaque, output: *anyopaque, input: *const anyopaque, weight: *const anyopaque, rows: usize, width: usize, columns: usize, accumulate: bool) !void {
        return self.matrix(stream, output, input, weight, rows, width, columns, false, true, accumulate);
    }

    /// Row-major C = op(lhs) op(rhs), without copying either transpose.
    pub fn matrix(self: *CublasF32, stream: ?*anyopaque, output: *anyopaque, lhs: *const anyopaque, rhs: *const anyopaque, rows: usize, width: usize, columns: usize, lhs_transposed: bool, rhs_transposed: bool, accumulate: bool) !void {
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

    pub fn batched(self: *CublasF32, stream: ?*anyopaque, output: *anyopaque, lhs: *const anyopaque, rhs: *const anyopaque, batches: usize, rows: usize, width: usize, columns: usize, lhs_transposed: bool, rhs_transposed: bool) !void {
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

    fn bindStream(self: *CublasF32, stream: ?*anyopaque) !void {
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

    fn deinit(self: *CublasLtLibrary) void {
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
    const too_long = [_]u8{'/'} ++ [_]u8{'a'} ** 4096;
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
