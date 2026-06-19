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

    fn open() !CublasLibrary {
        var lib = try openAny(&cublas_names);
        errdefer lib.close();
        return .{
            .lib = lib,
            .fns = .{
                .create = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).create), "cublasCreate_v2"),
                .destroy = try lookup(&lib, @TypeOf(@as(CublasTable, undefined).destroy), "cublasDestroy_v2"),
            },
        };
    }

    fn deinit(self: *CublasLibrary) void {
        self.lib.close();
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
            .cublas = CublasLibrary.open() catch null,
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

const cublas_names = [_][]const u8{
    "libcublas.so.13",
    "libcublas.so",
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
