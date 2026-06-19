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
const platform = @import("antfly_platform");

const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const libraries_mod = @import("libraries.zig");
const tensor_mod = @import("../../backends/tensor.zig");

const default_workspace_mb: usize = 32;
const default_min_ops: usize = 1_000_000;

pub const Stats = struct {
    enabled: bool,
    workspace_bytes: usize,
    min_ops: usize,
    plan_cache_entries: usize,
    attempts: usize,
    successes: usize,
    fallbacks: usize,
    heuristic_misses: usize,
};

const PlanKey = struct {
    dtype_tag: u8,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    has_bias: bool,
};

const Plan = struct {
    algo: libraries_mod.CublasLtMatmulAlgo,
    workspace_size: usize,
};

pub const DenseLt = struct {
    allocator: std.mem.Allocator = undefined,
    enabled_: bool = false,
    workspace: buffer_mod.DeviceBuffer = .{},
    workspace_bytes: usize = 0,
    min_ops: usize = default_min_ops,
    plans: std.AutoHashMapUnmanaged(PlanKey, Plan) = .empty,
    attempts: usize = 0,
    successes: usize = 0,
    fallbacks: usize = 0,
    heuristic_misses: usize = 0,

    pub fn init(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, libraries: *const libraries_mod.CudaLibraries) !DenseLt {
        var self = DenseLt{
            .allocator = allocator,
            .enabled_ = false,
            .workspace_bytes = 0,
            .min_ops = platform.env.getenvUsize("TERMITE_CUDA_CUBLASLT_MIN_OPS") orelse default_min_ops,
        };
        if (platform.env.getenvBool("TERMITE_CUDA_DISABLE_CUBLASLT_MATMUL")) return self;
        if (!libraries.hasCublasLt()) return self;

        const workspace_mb = platform.env.getenvUsize("ANTFLY_CUDA_CUBLASLT_WORKSPACE_MB") orelse
            platform.env.getenvUsize("TERMITE_CUDA_CUBLASLT_WORKSPACE_MB") orelse
            default_workspace_mb;
        const workspace_bytes = std.math.mul(usize, workspace_mb, 1024 * 1024) catch |err| {
            if (libraries.policy == .required) return err;
            return self;
        };
        if (workspace_bytes > 0) {
            self.workspace = buffer_mod.DeviceBuffer.alloc(ctx, workspace_bytes) catch |err| {
                if (libraries.policy == .required) return err;
                return self;
            };
            self.workspace_bytes = workspace_bytes;
        }
        self.enabled_ = true;
        return self;
    }

    pub fn deinit(self: *DenseLt, ctx: *context_mod.CudaContext) void {
        self.plans.deinit(self.allocator);
        self.workspace.free(ctx);
        self.* = .{ .allocator = self.allocator };
    }

    pub fn enabled(self: *const DenseLt) bool {
        return self.enabled_;
    }

    pub fn stats(self: *const DenseLt) Stats {
        return .{
            .enabled = self.enabled_,
            .workspace_bytes = self.workspace_bytes,
            .min_ops = self.min_ops,
            .plan_cache_entries = self.plans.count(),
            .attempts = self.attempts,
            .successes = self.successes,
            .fallbacks = self.fallbacks,
            .heuristic_misses = self.heuristic_misses,
        };
    }

    pub fn linear(
        self: *DenseLt,
        ctx: *context_mod.CudaContext,
        libraries: *const libraries_mod.CudaLibraries,
        dst: buffer_mod.DeviceBuffer,
        activation16: buffer_mod.DeviceBuffer,
        weight16: buffer_mod.DeviceBuffer,
        bias: ?buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        dtype: tensor_mod.DType,
    ) !bool {
        if (!self.enabled_) return false;
        if (!eligible(self, rows, in_dim, out_dim, dtype)) return false;

        self.attempts += 1;
        const ran = self.linearInner(ctx, libraries, dst, activation16, weight16, bias, rows, in_dim, out_dim, dtype) catch false;
        if (ran) {
            self.successes += 1;
        } else {
            self.fallbacks += 1;
        }
        return ran;
    }

    fn linearInner(
        self: *DenseLt,
        ctx: *context_mod.CudaContext,
        libraries: *const libraries_mod.CudaLibraries,
        dst: buffer_mod.DeviceBuffer,
        activation16: buffer_mod.DeviceBuffer,
        weight16: buffer_mod.DeviceBuffer,
        bias: ?buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        dtype: tensor_mod.DType,
    ) !bool {
        const fns = libraries.cublasLtFns() orelse return false;
        const handle = libraries.cublasLtHandle();
        if (handle == null) return false;

        const out_count = try checkedMul(rows, out_dim);
        try checkRawBytes(dst, try checkedMul(out_count, @sizeOf(f32)));
        try checkRawBytes(activation16, try checkedMul(try checkedMul(rows, in_dim), @sizeOf(u16)));
        try checkRawBytes(weight16, try checkedMul(try checkedMul(out_dim, in_dim), @sizeOf(u16)));
        if (bias) |b| try checkRawBytes(b, try checkedMul(out_dim, @sizeOf(f32)));
        if (out_count == 0) return true;

        var op_desc: libraries_mod.CublasLtMatmulDesc = null;
        if (fns.matmulDescCreate(&op_desc, libraries_mod.CUBLAS_COMPUTE_32F, libraries_mod.CUDA_R_32F) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        defer _ = fns.matmulDescDestroy(op_desc);

        var op_n = libraries_mod.CUBLAS_OP_N;
        if (fns.matmulDescSetAttribute(op_desc, libraries_mod.CUBLASLT_MATMUL_DESC_TRANSA, &op_n, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        var op_t = libraries_mod.CUBLAS_OP_T;
        if (fns.matmulDescSetAttribute(op_desc, libraries_mod.CUBLASLT_MATMUL_DESC_TRANSB, &op_t, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        if (bias) |b| {
            var epilogue = libraries_mod.CUBLASLT_EPILOGUE_BIAS;
            if (fns.matmulDescSetAttribute(op_desc, libraries_mod.CUBLASLT_MATMUL_DESC_EPILOGUE, &epilogue, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
            var bias_ptr = b.ptr;
            if (fns.matmulDescSetAttribute(op_desc, libraries_mod.CUBLASLT_MATMUL_DESC_BIAS_POINTER, &bias_ptr, @sizeOf(driver_mod.CUdeviceptr)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        }

        var a_desc: libraries_mod.CublasLtMatrixLayout = null;
        if (fns.matrixLayoutCreate(&a_desc, cudaDataType(dtype), @intCast(out_dim), @intCast(in_dim), @intCast(in_dim)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        defer _ = fns.matrixLayoutDestroy(a_desc);
        var b_desc: libraries_mod.CublasLtMatrixLayout = null;
        if (fns.matrixLayoutCreate(&b_desc, cudaDataType(dtype), @intCast(rows), @intCast(in_dim), @intCast(in_dim)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        defer _ = fns.matrixLayoutDestroy(b_desc);
        var c_desc: libraries_mod.CublasLtMatrixLayout = null;
        if (fns.matrixLayoutCreate(&c_desc, libraries_mod.CUDA_R_32F, @intCast(out_dim), @intCast(rows), @intCast(out_dim)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        defer _ = fns.matrixLayoutDestroy(c_desc);
        var d_desc: libraries_mod.CublasLtMatrixLayout = null;
        if (fns.matrixLayoutCreate(&d_desc, libraries_mod.CUDA_R_32F, @intCast(out_dim), @intCast(rows), @intCast(out_dim)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        defer _ = fns.matrixLayoutDestroy(d_desc);

        var row_order = libraries_mod.CUBLASLT_ORDER_ROW;
        if (fns.matrixLayoutSetAttribute(a_desc, libraries_mod.CUBLASLT_MATRIX_LAYOUT_ORDER, &row_order, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        if (fns.matrixLayoutSetAttribute(b_desc, libraries_mod.CUBLASLT_MATRIX_LAYOUT_ORDER, &row_order, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        var col_order = libraries_mod.CUBLASLT_ORDER_COL;
        if (fns.matrixLayoutSetAttribute(c_desc, libraries_mod.CUBLASLT_MATRIX_LAYOUT_ORDER, &col_order, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;
        if (fns.matrixLayoutSetAttribute(d_desc, libraries_mod.CUBLASLT_MATRIX_LAYOUT_ORDER, &col_order, @sizeOf(c_int)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return false;

        const key = PlanKey{
            .dtype_tag = dtypeTag(dtype),
            .rows = try toU32(rows),
            .in_dim = try toU32(in_dim),
            .out_dim = try toU32(out_dim),
            .has_bias = bias != null,
        };
        const plan = try self.planFor(fns, handle, op_desc, a_desc, b_desc, c_desc, d_desc, key);

        var alpha: f32 = 1.0;
        var beta: f32 = 0.0;
        const a_ptr: ?*const anyopaque = @ptrFromInt(weight16.ptr);
        const b_ptr: ?*const anyopaque = @ptrFromInt(activation16.ptr);
        const c_ptr: ?*const anyopaque = @ptrFromInt(dst.ptr);
        const d_ptr: ?*anyopaque = @ptrFromInt(dst.ptr);
        const workspace_ptr: ?*anyopaque = if (self.workspace.ptr != 0 and plan.workspace_size > 0)
            @ptrFromInt(self.workspace.ptr)
        else
            null;

        try ctx.makeCurrent();
        const status = fns.matmul(
            handle,
            op_desc,
            &alpha,
            a_ptr,
            a_desc,
            b_ptr,
            b_desc,
            &beta,
            c_ptr,
            c_desc,
            d_ptr,
            d_desc,
            &plan.algo,
            workspace_ptr,
            plan.workspace_size,
            ctx.stream,
        );
        return status == libraries_mod.CUBLAS_STATUS_SUCCESS;
    }

    fn planFor(
        self: *DenseLt,
        fns: *const libraries_mod.CublasLtTable,
        handle: libraries_mod.CublasLtHandle,
        op_desc: libraries_mod.CublasLtMatmulDesc,
        a_desc: libraries_mod.CublasLtMatrixLayout,
        b_desc: libraries_mod.CublasLtMatrixLayout,
        c_desc: libraries_mod.CublasLtMatrixLayout,
        d_desc: libraries_mod.CublasLtMatrixLayout,
        key: PlanKey,
    ) !Plan {
        if (self.plans.get(key)) |plan| return plan;
        self.heuristic_misses += 1;

        var pref: libraries_mod.CublasLtMatmulPreference = null;
        if (fns.preferenceCreate(&pref) != libraries_mod.CUBLAS_STATUS_SUCCESS) return error.CublasLtFallback;
        defer _ = fns.preferenceDestroy(pref);

        var max_workspace = self.workspace_bytes;
        if (fns.preferenceSetAttribute(pref, libraries_mod.CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES, &max_workspace, @sizeOf(usize)) != libraries_mod.CUBLAS_STATUS_SUCCESS) return error.CublasLtFallback;

        var result: [1]libraries_mod.CublasLtMatmulHeuristicResult = undefined;
        var count: c_int = 0;
        const status = fns.matmulAlgoGetHeuristic(handle, op_desc, a_desc, b_desc, c_desc, d_desc, pref, 1, &result, &count);
        if (status != libraries_mod.CUBLAS_STATUS_SUCCESS or count <= 0) return error.CublasLtFallback;
        if (result[0].state != libraries_mod.CUBLAS_STATUS_SUCCESS) return error.CublasLtFallback;
        if (result[0].workspaceSize > self.workspace_bytes) return error.CublasLtFallback;

        const plan = Plan{
            .algo = result[0].algo,
            .workspace_size = result[0].workspaceSize,
        };
        try self.plans.put(self.allocator, key, plan);
        return plan;
    }

    fn eligible(self: *const DenseLt, rows: usize, in_dim: usize, out_dim: usize, dtype: tensor_mod.DType) bool {
        if (dtype != .f16 and dtype != .bf16) return false;
        if (rows == 0 or in_dim == 0 or out_dim == 0) return false;
        if (in_dim % 8 != 0 or out_dim % 8 != 0) return false;
        if (rows > std.math.maxInt(u32) or in_dim > std.math.maxInt(u32) or out_dim > std.math.maxInt(u32)) return false;
        const ops = std.math.mul(usize, rows, out_dim) catch return false;
        const total_ops = std.math.mul(usize, ops, in_dim) catch return false;
        return total_ops >= self.min_ops;
    }
};

fn cudaDataType(dtype: tensor_mod.DType) c_int {
    return switch (dtype) {
        .f16 => libraries_mod.CUDA_R_16F,
        .bf16 => libraries_mod.CUDA_R_16BF,
        else => libraries_mod.CUDA_R_32F,
    };
}

fn dtypeTag(dtype: tensor_mod.DType) u8 {
    return switch (dtype) {
        .f16 => 1,
        .bf16 => 2,
        else => 0,
    };
}

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidCudaState;
}

fn toU32(value: usize) !u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidCudaState;
    return @intCast(value);
}

fn checkRawBytes(buffer: buffer_mod.DeviceBuffer, bytes: usize) !void {
    if (bytes > buffer.len) return error.InvalidCudaState;
}
