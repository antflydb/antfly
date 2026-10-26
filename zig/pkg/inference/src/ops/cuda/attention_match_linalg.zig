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

//! FP64 matrix primitives for native KV fitting. Handles belong to one CUDA
//! context/stream; there are no process-global or thread-local CUDA owners.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const H = ?*anyopaque;
const P = [*]f64;
const C = [*]const f64;
const I = c_int;
const S = c_int;

const Blas = struct {
    create: *const fn (*H) callconv(.c) S,
    destroy: *const fn (H) callconv(.c) S,
    setStream: *const fn (H, H) callconv(.c) S,
    setMathMode: *const fn (H, I) callconv(.c) S,
    setWorkspace: *const fn (H, H, usize) callconv(.c) S,
    gemm: *const fn (H, I, I, I, I, I, *const f64, C, I, C, I, *const f64, P, I) callconv(.c) S,
    gemv: *const fn (H, I, I, I, *const f64, C, I, C, I, *const f64, P, I) callconv(.c) S,
    axpy: *const fn (H, I, *const f64, C, I, P, I) callconv(.c) S,
    trsv: *const fn (H, I, I, I, I, C, I, P, I) callconv(.c) S,
    trsm: *const fn (H, I, I, I, I, I, I, *const f64, C, I, P, I) callconv(.c) S,
};
const Solver = struct {
    create: *const fn (*H) callconv(.c) S,
    destroy: *const fn (H) callconv(.c) S,
    setStream: *const fn (H, H) callconv(.c) S,
    qrSize: *const fn (H, I, I, P, I, *I) callconv(.c) S,
    qr: *const fn (H, I, I, P, I, P, P, I, *I) callconv(.c) S,
    applySize: *const fn (H, I, I, I, I, I, C, I, C, C, I, *I) callconv(.c) S,
    apply: *const fn (H, I, I, I, I, I, C, I, C, P, I, P, I, *I) callconv(.c) S,
};
fn openAny(names: []const []const u8) !std.DynLib {
    for (names) |name| if (std.DynLib.open(name)) |lib| return lib else |_| {};
    return error.CompactionCudaLibraryUnavailable;
}
fn lookup(lib: *std.DynLib, comptime T: type, name: [:0]const u8) !T {
    return lib.lookup(T, name) orelse error.CompactionCudaSymbolMissing;
}
pub fn check(status: S) !void {
    if (status != 0) return error.CompactionCudaLinalgFailure;
}
pub fn ptr(address: usize) P {
    return @ptrFromInt(address);
}
pub fn int(value: usize) I {
    // Every public fitting shape is validated before reaching this helper.
    return @intCast(value);
}
pub const Linalg = struct {
    context: *Context,
    stream: @import("driver.zig").CUstream,
    blas_library: std.DynLib,
    solver_library: std.DynLib,
    blas: Blas,
    solver: Solver,
    bh: H,
    sh: H,
    workspace: @import("buffer.zig").DeviceBuffer,

    pub fn init(context: *Context) !Linalg {
        return initOnStream(context, context.stream);
    }
    pub fn initOnStream(context: *Context, stream: @import("driver.zig").CUstream) !Linalg {
        try context.makeCurrent();
        if (try context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        var blas_library = try openAny(&.{ "libcublas.so.13", "libcublas.so.12", "libcublas.so" });
        errdefer blas_library.close();
        var solver_library = try openAny(&.{ "libcusolver.so.12", "libcusolver.so.11", "libcusolver.so" });
        errdefer solver_library.close();
        var blas: Blas = undefined;
        inline for (.{ .{ "create", "cublasCreate_v2" }, .{ "destroy", "cublasDestroy_v2" }, .{ "setStream", "cublasSetStream_v2" }, .{ "setMathMode", "cublasSetMathMode" }, .{ "setWorkspace", "cublasSetWorkspace_v2" }, .{ "gemm", "cublasDgemm_v2" }, .{ "gemv", "cublasDgemv_v2" }, .{ "axpy", "cublasDaxpy_v2" }, .{ "trsv", "cublasDtrsv_v2" }, .{ "trsm", "cublasDtrsm_v2" } }) |pair| {
            @field(blas, pair[0]) = try lookup(&blas_library, @FieldType(Blas, pair[0]), pair[1]);
        }
        var solver: Solver = undefined;
        inline for (.{ .{ "create", "cusolverDnCreate" }, .{ "destroy", "cusolverDnDestroy" }, .{ "setStream", "cusolverDnSetStream" }, .{ "qrSize", "cusolverDnDgeqrf_bufferSize" }, .{ "qr", "cusolverDnDgeqrf" }, .{ "applySize", "cusolverDnDormqr_bufferSize" }, .{ "apply", "cusolverDnDormqr" } }) |pair| {
            @field(solver, pair[0]) = try lookup(&solver_library, @FieldType(Solver, pair[0]), pair[1]);
        }
        var bh: H = null;
        try check(blas.create(&bh));
        errdefer _ = blas.destroy(bh);
        var sh: H = null;
        try check(solver.create(&sh));
        errdefer _ = solver.destroy(sh);
        try check(blas.setMathMode(bh, 2)); // CUBLAS_PEDANTIC_MATH
        try check(blas.setStream(bh, stream));
        try check(solver.setStream(sh, stream));
        var workspace = try @import("buffer.zig").DeviceBuffer.alloc(context, 4 * 1024 * 1024);
        errdefer workspace.free(context);
        try check(blas.setWorkspace(bh, @ptrFromInt(workspace.ptr), workspace.len));
        return .{ .context = context, .stream = stream, .blas_library = blas_library, .solver_library = solver_library, .blas = blas, .solver = solver, .bh = bh, .sh = sh, .workspace = workspace };
    }
    pub fn deinit(self: *Linalg) void {
        self.context.makeCurrent() catch {};
        _ = self.context.driver.fns.cuStreamSynchronize(self.stream);
        _ = self.solver.destroy(self.sh);
        _ = self.blas.destroy(self.bh);
        self.workspace.free(self.context);
        self.solver_library.close();
        self.blas_library.close();
        self.* = undefined;
    }
    pub fn gemv(self: *Linalg, trans: I, rows: usize, columns: usize, alpha: f64, matrix: usize, lda: usize, vector: usize, beta: f64, out: usize) !void {
        try check(self.blas.gemv(self.bh, trans, int(rows), int(columns), &alpha, ptr(matrix), int(lda), ptr(vector), 1, &beta, ptr(out), 1));
    }
};
