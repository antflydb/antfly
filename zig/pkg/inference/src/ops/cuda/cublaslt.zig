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
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");

const Status = c_int;
const Handle = ?*anyopaque;
const MatmulDesc = ?*anyopaque;
const MatrixLayout = ?*anyopaque;
const MatmulPreference = ?*anyopaque;

const CUBLAS_STATUS_SUCCESS: Status = 0;
const CUBLAS_STATUS_NOT_SUPPORTED: Status = 15;
const CUBLAS_OP_N: c_int = 0;
const CUBLAS_OP_T: c_int = 1;
const CUDA_R_32F: c_int = 0;
const CUDA_R_16F: c_int = 2;
const CUDA_R_16BF: c_int = 14;
const CUBLAS_COMPUTE_32F: c_int = 68;
const CUBLASLT_MATMUL_DESC_TRANSA: c_int = 3;
const CUBLASLT_MATMUL_DESC_TRANSB: c_int = 4;
const CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES: c_int = 1;
const CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_A_BYTES: c_int = 5;
const CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_B_BYTES: c_int = 6;
const CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_C_BYTES: c_int = 7;
const CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_D_BYTES: c_int = 8;
const CUBLASLT_MATRIX_LAYOUT_ORDER: c_int = 1;
const CUBLASLT_ORDER_ROW: c_int = 1;
const CUBLASLT_MATRIX_LAYOUT_BATCH_COUNT: c_int = 5;
const CUBLASLT_MATRIX_LAYOUT_STRIDED_BATCH_OFFSET: c_int = 6;

pub const Error = error{
    CublasLtUnavailable,
    CublasLtSymbolMissing,
    CublasLtError,
    CublasLtUnsupported,
    CublasLtTuningInsufficientCandidates,
};

const MatmulAlgo = extern struct {
    data: [8]u64,
};

const MatmulHeuristicResult = extern struct {
    algo: MatmulAlgo,
    workspace_size: usize,
    state: Status,
    waves_count: f32,
    reserved: [4]c_int,
};

const TensorCorePlanKind = enum(u8) {
    dense,
    /// Input-gradient contraction for a frozen row-major weight:
    /// [rows, out_dim] * [out_dim, in_dim] -> [rows, in_dim].
    dense_input_gradient,
    strided_batched,
};

const maximum_matmul_heuristics: u8 = 16;
const maximum_matmul_tuning_warmups: u8 = 4;
const maximum_matmul_tuning_iterations: u8 = 10;

/// Bounded first-use tuning for tensor-core GEMMs. Candidate counts are
/// normalized to 1...16, warmups to 0...4, and timed iterations to 1...10.
/// The default preserves the
/// existing single-heuristic behavior. Qualified model profiles may opt in
/// during excluded warm requests; the same enabled policy must remain active
/// afterward so the selected plan's complete tuning fingerprint is reused and
/// measured inference performs no timing work.
pub const MatmulTuning = struct {
    enabled: bool = false,
    candidate_count: u8 = 8,
    warmups: u8 = 1,
    iterations: u8 = 3,

    fn normalized(self: MatmulTuning) MatmulTuning {
        if (!self.enabled) return .{};
        return .{
            .enabled = true,
            .candidate_count = @max(@min(self.candidate_count, maximum_matmul_heuristics), 1),
            .warmups = @min(self.warmups, maximum_matmul_tuning_warmups),
            .iterations = @max(@min(self.iterations, maximum_matmul_tuning_iterations), 1),
        };
    }

    fn fingerprint(self: MatmulTuning) u32 {
        if (!self.enabled) return 0;
        const effective = self.normalized();
        return @as(u32, effective.candidate_count) |
            (@as(u32, effective.warmups) << 8) |
            (@as(u32, effective.iterations) << 16);
    }
};

/// How the cached algorithm used by an invocation was selected. Callers use
/// this to attest that an experimental tuning profile actually timed more
/// than one admissible algorithm instead of silently exercising the ordinary
/// heuristic path.
pub const MatmulSelection = enum {
    heuristic,
    tuned,
};

const TensorCorePlanKey = struct {
    kind: TensorCorePlanKind,
    input_type: c_int,
    batch_count: u32,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    workspace_bytes: usize,
    alignment_a_bytes: u32,
    alignment_b_bytes: u32,
    alignment_c_bytes: u32,
    alignment_d_bytes: u32,
    tuning_fingerprint: u32,
};

const TensorCorePlan = struct {
    algo: MatmulAlgo,
    workspace_size: usize,
    op_desc: MatmulDesc,
    a_desc: MatrixLayout,
    b_desc: MatrixLayout,
    c_desc: MatrixLayout,
    d_desc: MatrixLayout,
    selection: MatmulSelection,
};

const TensorCorePlanLease = struct {
    plan: TensorCorePlan,
    owns_descriptors: bool,
    pending_key: ?TensorCorePlanKey = null,
    owns_tuning_mutex: bool = false,

    /// Publish only after cublasLt has accepted one real invocation with this
    /// plan. A failed first submission therefore cannot poison the
    /// process-lifetime cache. Concurrent ordinary misses may race here; the
    /// first canonical entry wins and every other lease destroys its own
    /// descriptors after the already-submitted call completes.
    fn publish(self: *TensorCorePlanLease, cublas: *CublasLt) bool {
        const key = self.pending_key orelse return true;
        const allocator = cublas.allocator orelse return false;
        spinLock(&cublas.tensor_core_plans_mutex);
        if (cublas.tensor_core_plans.contains(key)) {
            cublas.tensor_core_plans_mutex.unlock();
            self.pending_key = null;
            return true;
        }
        if (cublas.tensor_core_plans.count() >= maximum_tensor_core_plans) {
            cublas.tensor_core_plans_mutex.unlock();
            return false;
        }
        cublas.tensor_core_plans.put(allocator, key, self.plan) catch {
            cublas.tensor_core_plans_mutex.unlock();
            return false;
        };
        cublas.tensor_core_plans_mutex.unlock();
        self.pending_key = null;
        self.owns_descriptors = false;
        return true;
    }

    fn deinit(self: *TensorCorePlanLease, cublas: *CublasLt) void {
        if (self.owns_descriptors) cublas.destroyTensorCorePlan(self.plan);
        if (self.owns_tuning_mutex) cublas.tensor_core_tuning_mutex.unlock();
        self.* = undefined;
    }
};

const TensorCoreInvocation = struct {
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    workspace: buffer_mod.DeviceBuffer,
};

const TimedSelection = struct {
    index: usize,
    timed_candidate_count: usize,
    elapsed_us: u64,
};

const CandidateSubmission = enum {
    submitted,
    unsupported,
};

// Shape diversity is small for a loaded model, but untrusted request shapes
// must not be able to grow this process-lifetime cache without bound.
const maximum_tensor_core_plans = 256;

pub const CublasLt = struct {
    allocator: ?std.mem.Allocator = null,
    lib: std.DynLib,
    handle: Handle,
    owner_context: driver_mod.CUcontext,
    owner_device: driver_mod.CUdevice,
    fns: Table,
    tensor_core_plans: std.AutoHashMapUnmanaged(TensorCorePlanKey, TensorCorePlan) = .empty,
    tensor_core_plans_mutex: std.atomic.Mutex = .unlocked,
    // Tuning synchronizes CUDA events and can take much longer than the plan
    // map critical section. Serialize tuned cache misses separately and yield
    // while waiting so duplicate first-use requests cannot distort timings or
    // pin a host core.
    tensor_core_tuning_mutex: std.atomic.Mutex = .unlocked,

    const Table = struct {
        cublasLtCreate: *const fn (*Handle) callconv(.c) Status,
        cublasLtDestroy: *const fn (Handle) callconv(.c) Status,
        cublasLtMatmul: *const fn (
            Handle,
            MatmulDesc,
            ?*const anyopaque,
            ?*const anyopaque,
            MatrixLayout,
            ?*const anyopaque,
            MatrixLayout,
            ?*const anyopaque,
            ?*const anyopaque,
            MatrixLayout,
            ?*anyopaque,
            MatrixLayout,
            ?*const MatmulAlgo,
            ?*anyopaque,
            usize,
            driver_mod.CUstream,
        ) callconv(.c) Status,
        cublasLtMatmulDescCreate: *const fn (*MatmulDesc, c_int, c_int) callconv(.c) Status,
        cublasLtMatmulDescDestroy: *const fn (MatmulDesc) callconv(.c) Status,
        cublasLtMatmulDescSetAttribute: *const fn (MatmulDesc, c_int, ?*const anyopaque, usize) callconv(.c) Status,
        cublasLtMatrixLayoutCreate: *const fn (*MatrixLayout, c_int, u64, u64, i64) callconv(.c) Status,
        cublasLtMatrixLayoutDestroy: *const fn (MatrixLayout) callconv(.c) Status,
        cublasLtMatrixLayoutSetAttribute: *const fn (MatrixLayout, c_int, ?*const anyopaque, usize) callconv(.c) Status,
        cublasLtMatmulPreferenceCreate: *const fn (*MatmulPreference) callconv(.c) Status,
        cublasLtMatmulPreferenceDestroy: *const fn (MatmulPreference) callconv(.c) Status,
        cublasLtMatmulPreferenceSetAttribute: *const fn (MatmulPreference, c_int, ?*const anyopaque, usize) callconv(.c) Status,
        cublasLtMatmulAlgoGetHeuristic: *const fn (
            Handle,
            MatmulDesc,
            MatrixLayout,
            MatrixLayout,
            MatrixLayout,
            MatrixLayout,
            MatmulPreference,
            c_int,
            [*]MatmulHeuristicResult,
            *c_int,
        ) callconv(.c) Status,
        cublasLtMatmulAlgoCheck: *const fn (
            Handle,
            MatmulDesc,
            MatrixLayout,
            MatrixLayout,
            MatrixLayout,
            MatrixLayout,
            *const MatmulAlgo,
            *MatmulHeuristicResult,
        ) callconv(.c) Status,
    };

    pub fn open(ctx: *context_mod.CudaContext) Error!CublasLt {
        return openWithAllocator(null, ctx);
    }

    pub fn openWithAllocator(allocator: ?std.mem.Allocator, ctx: *context_mod.CudaContext) Error!CublasLt {
        ctx.makeCurrent() catch return error.CublasLtUnavailable;
        var lib = openLibrary() catch return error.CublasLtUnavailable;
        errdefer lib.close();
        const fns = Table{
            .cublasLtCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtCreate), "cublasLtCreate") catch return error.CublasLtSymbolMissing,
            .cublasLtDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtDestroy), "cublasLtDestroy") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmul = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmul), "cublasLtMatmul") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulDescCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulDescCreate), "cublasLtMatmulDescCreate") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulDescDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulDescDestroy), "cublasLtMatmulDescDestroy") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulDescSetAttribute = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulDescSetAttribute), "cublasLtMatmulDescSetAttribute") catch return error.CublasLtSymbolMissing,
            .cublasLtMatrixLayoutCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatrixLayoutCreate), "cublasLtMatrixLayoutCreate") catch return error.CublasLtSymbolMissing,
            .cublasLtMatrixLayoutDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatrixLayoutDestroy), "cublasLtMatrixLayoutDestroy") catch return error.CublasLtSymbolMissing,
            .cublasLtMatrixLayoutSetAttribute = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatrixLayoutSetAttribute), "cublasLtMatrixLayoutSetAttribute") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulPreferenceCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulPreferenceCreate), "cublasLtMatmulPreferenceCreate") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulPreferenceDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulPreferenceDestroy), "cublasLtMatmulPreferenceDestroy") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulPreferenceSetAttribute = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulPreferenceSetAttribute), "cublasLtMatmulPreferenceSetAttribute") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulAlgoGetHeuristic = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulAlgoGetHeuristic), "cublasLtMatmulAlgoGetHeuristic") catch return error.CublasLtSymbolMissing,
            .cublasLtMatmulAlgoCheck = lookup(&lib, @TypeOf(@as(Table, undefined).cublasLtMatmulAlgoCheck), "cublasLtMatmulAlgoCheck") catch return error.CublasLtSymbolMissing,
        };
        var handle: Handle = null;
        if (fns.cublasLtCreate(&handle) != CUBLAS_STATUS_SUCCESS) return error.CublasLtUnavailable;
        return .{
            .allocator = allocator,
            .lib = lib,
            .handle = handle,
            .owner_context = ctx.ctx,
            .owner_device = ctx.device,
            .fns = fns,
        };
    }

    pub fn deinit(self: *CublasLt) void {
        // Like other allocator/driver objects in this backend, deinit requires
        // exclusive ownership. CudaCompute destroys it only after model
        // execution has quiesced; concurrent deinit and matmul is unsupported.
        if (self.allocator) |allocator| {
            var plans = self.tensor_core_plans.valueIterator();
            while (plans.next()) |plan| self.destroyTensorCorePlan(plan.*);
            self.tensor_core_plans.deinit(allocator);
        }
        if (self.handle != null) {
            _ = self.fns.cublasLtDestroy(self.handle);
            self.handle = null;
        }
        self.lib.close();
    }

    pub fn matmulBf16WeightF32Out(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_bf16: buffer_mod.DeviceBuffer,
        weight_bf16: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) Error!void {
        _ = try self.matmulBf16WeightF32OutWithTuning(ctx, dst, input_bf16, weight_bf16, workspace, rows, in_dim, out_dim, .{});
    }

    pub fn matmulBf16WeightF32OutWithTuning(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_bf16: buffer_mod.DeviceBuffer,
        weight_bf16: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        tuning: MatmulTuning,
    ) Error!MatmulSelection {
        return self.matmul16WeightF32Out(ctx, dst, input_bf16, weight_bf16, workspace, rows, in_dim, out_dim, CUDA_R_16BF, tuning, .dense);
    }

    /// Tensor-core input-gradient contraction for a frozen row-major BF16
    /// weight. `grad_output_bf16` is [rows, out_dim], `weight_bf16` is
    /// [out_dim, in_dim], and the F32 result is [rows, in_dim].
    pub fn matmulBf16WeightInputGradientF32OutWithTuning(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        grad_output_bf16: buffer_mod.DeviceBuffer,
        weight_bf16: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        tuning: MatmulTuning,
    ) Error!MatmulSelection {
        return self.matmul16WeightF32Out(
            ctx,
            dst,
            grad_output_bf16,
            weight_bf16,
            workspace,
            rows,
            in_dim,
            out_dim,
            CUDA_R_16BF,
            tuning,
            .dense_input_gradient,
        );
    }

    /// Tensor-core matmul for FP16 GGUF weights. Activations are staged from
    /// the F32 graph representation, while accumulation and the output stay
    /// in F32 so this is numerically compatible with the existing encoder
    /// execution contract.
    pub fn matmulF16WeightF32Out(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_f16: buffer_mod.DeviceBuffer,
        weight_f16: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) Error!void {
        _ = try self.matmul16WeightF32Out(ctx, dst, input_f16, weight_f16, workspace, rows, in_dim, out_dim, CUDA_R_16F, .{}, .dense);
    }

    fn matmul16WeightF32Out(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        input_type: c_int,
        tuning: MatmulTuning,
        kind: TensorCorePlanKind,
    ) Error!MatmulSelection {
        try self.validateContext(ctx);
        if (rows == 0 or in_dim == 0 or out_dim == 0) return .heuristic;
        if (rows > std.math.maxInt(u32) or in_dim > std.math.maxInt(u32) or out_dim > std.math.maxInt(u32)) return error.CublasLtUnsupported;
        if (kind == .strided_batched) return error.CublasLtUnsupported;
        const input_cols = if (kind == .dense_input_gradient) out_dim else in_dim;
        const output_cols = if (kind == .dense_input_gradient) in_dim else out_dim;
        const input_bytes = try checkedMatrixBytes(rows, input_cols, @sizeOf(u16));
        const weight_bytes = try checkedMatrixBytes(out_dim, in_dim, @sizeOf(u16));
        const dst_bytes = try checkedMatrixBytes(rows, output_cols, @sizeOf(f32));
        try checkRawBytes(input, input_bytes);
        try checkRawBytes(weight, weight_bytes);
        try checkRawBytes(dst, dst_bytes);
        try requireDisjoint(dst, dst_bytes, input, input_bytes);
        try requireDisjoint(dst, dst_bytes, weight, weight_bytes);
        try requireDisjoint(workspace, workspace.len, input, input_bytes);
        try requireDisjoint(workspace, workspace.len, weight, weight_bytes);
        try requireDisjoint(workspace, workspace.len, dst, dst_bytes);

        const invocation = TensorCoreInvocation{
            .dst = dst,
            .input = input,
            .weight = weight,
            .workspace = workspace,
        };
        try validateWorkspaceAlignment(workspace);
        var plan_lease = try self.tensorCorePlanFor(kind, input_type, 1, rows, in_dim, out_dim, workspace.len, tuning, ctx, invocation);
        defer plan_lease.deinit(self);
        const plan = plan_lease.plan;
        try checkRawBytes(workspace, plan.workspace_size);

        var alpha: f32 = 1.0;
        var beta: f32 = 0.0;
        const workspace_ptr: ?*anyopaque = if (workspace.ptr != 0 and plan.workspace_size > 0)
            @ptrFromInt(workspace.ptr)
        else
            null;
        ctx.makeCurrent() catch return error.CublasLtError;
        const submit_status = self.fns.cublasLtMatmul(
            self.handle,
            plan.op_desc,
            &alpha,
            @ptrFromInt(weight.ptr),
            plan.a_desc,
            @ptrFromInt(input.ptr),
            plan.b_desc,
            &beta,
            @ptrFromInt(dst.ptr),
            plan.c_desc,
            @ptrFromInt(dst.ptr),
            plan.d_desc,
            &plan.algo,
            workspace_ptr,
            plan.workspace_size,
            ctx.stream,
        );
        self.check(submit_status) catch |err| {
            ctx.synchronize() catch {};
            return err;
        };
        if (!plan_lease.publish(self) and tuning.normalized().enabled) {
            // A tuned request promises reusable measured inference. If cache
            // publication fails, drain the accepted launch before returning
            // an error so caller-owned buffers are no longer in flight.
            ctx.synchronize() catch {};
            return error.CublasLtUnsupported;
        }
        return plan.selection;
    }

    fn tensorCorePlanFor(
        self: *CublasLt,
        kind: TensorCorePlanKind,
        input_type: c_int,
        batch_count: usize,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        workspace_bytes: usize,
        tuning: MatmulTuning,
        ctx: *context_mod.CudaContext,
        invocation: TensorCoreInvocation,
    ) Error!TensorCorePlanLease {
        if (batch_count == 0 or batch_count > std.math.maxInt(u32)) return error.CublasLtUnsupported;
        if (rows == 0 or rows > std.math.maxInt(u32) or in_dim == 0 or in_dim > std.math.maxInt(u32) or out_dim == 0 or out_dim > std.math.maxInt(u32)) return error.CublasLtUnsupported;
        const effective_tuning = tuning.normalized();
        const key = TensorCorePlanKey{
            .kind = kind,
            .input_type = input_type,
            .batch_count = @intCast(batch_count),
            .rows = @intCast(rows),
            .in_dim = @intCast(in_dim),
            .out_dim = @intCast(out_dim),
            .workspace_bytes = workspace_bytes,
            // cublasLt's default heuristic preference assumes 256-byte
            // matrix alignment. Subviews are not guaranteed to satisfy that
            // contract, so make the actual A/B/C/D pointer alignments part of
            // both the preference and the cache identity.
            .alignment_a_bytes = cappedPointerAlignment(invocation.weight.ptr),
            .alignment_b_bytes = cappedPointerAlignment(invocation.input.ptr),
            .alignment_c_bytes = cappedPointerAlignment(invocation.dst.ptr),
            .alignment_d_bytes = cappedPointerAlignment(invocation.dst.ptr),
            .tuning_fingerprint = effective_tuning.fingerprint(),
        };

        // Keep the steady-state path to one short plan-map lookup. The much
        // longer tuning mutex is taken only after a cache miss, then the map
        // is checked again so a concurrent first user can publish the plan.
        spinLock(&self.tensor_core_plans_mutex);
        const cached_plan = self.tensor_core_plans.get(key);
        self.tensor_core_plans_mutex.unlock();
        if (cached_plan) |plan| return .{ .plan = plan, .owns_descriptors = false };

        // Descriptor creation, heuristic discovery, algo checks, and event
        // timing are all first-use host work. A cached plan is graph-safe, but
        // every cache miss must fail closed while the actual stream is being
        // captured, including ordinary (non-tuned) misses.
        if (ctx.streamCaptureActive() catch return error.CublasLtUnsupported) {
            return error.CublasLtUnsupported;
        }

        // An enabled tuning request is useful only when its result can be
        // cached. Otherwise every invocation would repeat synchronous timing.
        if (effective_tuning.enabled and self.allocator == null) return error.CublasLtUnsupported;
        var owns_tuning_mutex = false;
        if (effective_tuning.enabled) {
            lockYielding(&self.tensor_core_tuning_mutex);
            owns_tuning_mutex = true;
        }
        errdefer if (owns_tuning_mutex) self.tensor_core_tuning_mutex.unlock();

        spinLock(&self.tensor_core_plans_mutex);
        const cached_after_tuning_wait = self.tensor_core_plans.get(key);
        const cache_has_capacity = self.tensor_core_plans.count() < maximum_tensor_core_plans;
        self.tensor_core_plans_mutex.unlock();
        if (cached_after_tuning_wait) |plan| {
            if (owns_tuning_mutex) self.tensor_core_tuning_mutex.unlock();
            return .{ .plan = plan, .owns_descriptors = false };
        }
        if (effective_tuning.enabled and !cache_has_capacity) return error.CublasLtUnsupported;

        const candidate = try self.createTensorCorePlan(key, effective_tuning, ctx, invocation);
        return .{
            .plan = candidate,
            .owns_descriptors = true,
            .pending_key = if (self.allocator != null) key else null,
            .owns_tuning_mutex = owns_tuning_mutex,
        };
    }

    fn createTensorCorePlan(
        self: *CublasLt,
        key: TensorCorePlanKey,
        tuning: MatmulTuning,
        ctx: *context_mod.CudaContext,
        invocation: TensorCoreInvocation,
    ) Error!TensorCorePlan {
        var plan = TensorCorePlan{
            .algo = undefined,
            .workspace_size = 0,
            .op_desc = null,
            .a_desc = null,
            .b_desc = null,
            .c_desc = null,
            .d_desc = null,
            .selection = .heuristic,
        };
        errdefer self.destroyTensorCorePlan(plan);

        try self.check(self.fns.cublasLtMatmulDescCreate(&plan.op_desc, CUBLAS_COMPUTE_32F, CUDA_R_32F));
        var transa: c_int = undefined;
        var transb: c_int = undefined;
        switch (key.kind) {
            .dense => {
                transa = CUBLAS_OP_T;
                transb = CUBLAS_OP_N;
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.a_desc, key.input_type, key.in_dim, key.out_dim, key.in_dim));
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.b_desc, key.input_type, key.in_dim, key.rows, key.in_dim));
            },
            .dense_input_gradient => {
                // Row-major W[out, in] is column-major [in, out] in-place;
                // row-major dY[rows, out] is column-major [out, rows]. Their
                // ordinary column-major product is the row-major dX result.
                transa = CUBLAS_OP_N;
                transb = CUBLAS_OP_N;
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.a_desc, key.input_type, key.in_dim, key.out_dim, key.in_dim));
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.b_desc, key.input_type, key.out_dim, key.rows, key.out_dim));
            },
            .strided_batched => {
                if (key.batch_count > std.math.maxInt(i32)) return error.CublasLtUnsupported;
                transa = CUBLAS_OP_N;
                transb = CUBLAS_OP_T;
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.a_desc, key.input_type, key.out_dim, key.in_dim, key.in_dim));
                try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.b_desc, key.input_type, key.rows, key.in_dim, key.in_dim));
            },
        }
        try self.check(self.fns.cublasLtMatmulDescSetAttribute(plan.op_desc, CUBLASLT_MATMUL_DESC_TRANSA, &transa, @sizeOf(c_int)));
        try self.check(self.fns.cublasLtMatmulDescSetAttribute(plan.op_desc, CUBLASLT_MATMUL_DESC_TRANSB, &transb, @sizeOf(c_int)));
        const output_dim = if (key.kind == .dense_input_gradient) key.in_dim else key.out_dim;
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.c_desc, CUDA_R_32F, output_dim, key.rows, output_dim));
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&plan.d_desc, CUDA_R_32F, output_dim, key.rows, output_dim));

        if (key.kind == .strided_batched) {
            var row_order = CUBLASLT_ORDER_ROW;
            try self.check(self.fns.cublasLtMatrixLayoutSetAttribute(plan.a_desc, CUBLASLT_MATRIX_LAYOUT_ORDER, &row_order, @sizeOf(c_int)));
            try self.check(self.fns.cublasLtMatrixLayoutSetAttribute(plan.b_desc, CUBLASLT_MATRIX_LAYOUT_ORDER, &row_order, @sizeOf(c_int)));
            const batch_i32: i32 = @intCast(key.batch_count);
            const weight_stride = try checkedI64Product(key.out_dim, key.in_dim);
            const input_stride = try checkedI64Product(key.rows, key.in_dim);
            const output_stride = try checkedI64Product(key.rows, key.out_dim);
            try self.configureStridedBatch(plan.a_desc, batch_i32, weight_stride);
            try self.configureStridedBatch(plan.b_desc, batch_i32, input_stride);
            try self.configureStridedBatch(plan.c_desc, batch_i32, output_stride);
            try self.configureStridedBatch(plan.d_desc, batch_i32, output_stride);
        }

        var pref: MatmulPreference = null;
        try self.check(self.fns.cublasLtMatmulPreferenceCreate(&pref));
        defer _ = self.fns.cublasLtMatmulPreferenceDestroy(pref);
        var max_workspace = key.workspace_bytes;
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES, &max_workspace, @sizeOf(usize)));
        var alignment_a = key.alignment_a_bytes;
        var alignment_b = key.alignment_b_bytes;
        var alignment_c = key.alignment_c_bytes;
        var alignment_d = key.alignment_d_bytes;
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_A_BYTES, &alignment_a, @sizeOf(u32)));
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_B_BYTES, &alignment_b, @sizeOf(u32)));
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_C_BYTES, &alignment_c, @sizeOf(u32)));
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MIN_ALIGNMENT_D_BYTES, &alignment_d, @sizeOf(u32)));

        var heuristics: [maximum_matmul_heuristics]MatmulHeuristicResult = undefined;
        var returned: c_int = 0;
        const requested: c_int = if (tuning.enabled)
            @intCast(tuning.candidate_count)
        else
            1;
        try self.check(self.fns.cublasLtMatmulAlgoGetHeuristic(self.handle, plan.op_desc, plan.a_desc, plan.b_desc, plan.c_desc, plan.d_desc, pref, requested, &heuristics, &returned));
        const result_count = try checkedHeuristicResultCount(returned, requested, heuristics.len);
        var checked_heuristics: [maximum_matmul_heuristics]MatmulHeuristicResult = undefined;
        var checked_count: usize = 0;
        for (heuristics[0..result_count]) |heuristic| {
            if (heuristic.state != CUBLAS_STATUS_SUCCESS or
                heuristic.workspace_size > invocation.workspace.len)
            {
                continue;
            }
            // Heuristic results are recommendations, not an execution
            // guarantee. Revalidate every configured algorithm against the
            // exact operation/layout descriptors and current device before it
            // can become either the fallback or a timed candidate.
            var checked = heuristic;
            if (self.fns.cublasLtMatmulAlgoCheck(
                self.handle,
                plan.op_desc,
                plan.a_desc,
                plan.b_desc,
                plan.c_desc,
                plan.d_desc,
                &heuristic.algo,
                &checked,
            ) != CUBLAS_STATUS_SUCCESS) continue;
            if (checked.state != CUBLAS_STATUS_SUCCESS or
                checked.workspace_size > invocation.workspace.len)
            {
                continue;
            }
            // The API documents that AlgoCheck does not update the algo field.
            checked.algo = heuristic.algo;
            checked_heuristics[checked_count] = checked;
            checked_count += 1;
        }
        const candidates = checked_heuristics[0..checked_count];
        const fallback = firstUsableHeuristicIndex(candidates, invocation.workspace.len) orelse
            return error.CublasLtUnsupported;

        var selected = fallback;
        var timed_selection = false;
        var timed_candidate_count: usize = 0;
        var selected_elapsed_us: u64 = 0;
        if (tuning.enabled and checked_count > 1) {
            const fastest = try self.selectFastestTensorCoreAlgorithm(
                ctx,
                plan,
                candidates,
                invocation,
                tuning,
            );
            selected = fastest.index;
            timed_candidate_count = fastest.timed_candidate_count;
            selected_elapsed_us = fastest.elapsed_us;
            timed_selection = true;
        }
        plan.algo = candidates[selected].algo;
        plan.workspace_size = candidates[selected].workspace_size;
        plan.selection = if (timed_selection) .tuned else .heuristic;
        if (tuning.enabled) {
            std.log.info(
                "cuda_cublaslt_bf16_tuning: status={s} rows={d} in_dim={d} out_dim={d} candidates={d} timed_candidates={d} selected_index={d} selected_elapsed_us={d} workspace_bytes={d} warmups={d} iterations={d}",
                .{
                    if (timed_selection) "selected" else "heuristic-fallback",
                    key.rows,
                    key.in_dim,
                    key.out_dim,
                    checked_count,
                    timed_candidate_count,
                    selected,
                    selected_elapsed_us,
                    plan.workspace_size,
                    tuning.warmups,
                    tuning.iterations,
                },
            );
        }
        return plan;
    }

    fn selectFastestTensorCoreAlgorithm(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        plan: TensorCorePlan,
        heuristics: []const MatmulHeuristicResult,
        invocation: TensorCoreInvocation,
        tuning: MatmulTuning,
    ) Error!TimedSelection {
        const iterations: usize = tuning.iterations;
        var fastest_index: ?usize = null;
        var fastest_us: u64 = std.math.maxInt(u64);
        var timed_candidate_count: usize = 0;

        for (heuristics, 0..) |heuristic, index| {
            if (heuristic.state != CUBLAS_STATUS_SUCCESS) continue;
            if (heuristic.workspace_size > invocation.workspace.len) continue;

            var warmup_succeeded = true;
            var warmup: usize = 0;
            while (warmup < tuning.warmups) : (warmup += 1) {
                const submission = self.submitTensorCoreCandidate(
                    ctx,
                    plan,
                    heuristic.algo,
                    heuristic.workspace_size,
                    invocation,
                ) catch |err| {
                    // Earlier warmups may already be queued. Drain them before
                    // propagating a fatal status so an Error return transfers
                    // buffer ownership back to the caller unambiguously.
                    ctx.synchronize() catch {};
                    return err;
                };
                if (submission == .unsupported) {
                    warmup_succeeded = false;
                    break;
                }
            }
            // Drain every submitted warmup before creating timing events. In
            // addition to isolating the measured loop, this gives the public
            // error path a strict ownership guarantee: once this function
            // returns, no untracked warmup may still reference caller-owned
            // buffers. A synchronization failure can represent an
            // asynchronous device error and must abort plan publication.
            ctx.synchronize() catch return error.CublasLtError;
            if (!warmup_succeeded) continue;

            // Event allocation/recording failed before any timed launch. It
            // is safe to try another warm request through the caller's
            // baseline fallback, but there is no trustworthy selection to
            // cache in this request.
            const event_pair = ctx.beginProfileEventPair() catch
                return error.CublasLtTuningInsufficientCandidates;
            var launched: usize = 0;
            while (launched < iterations) : (launched += 1) {
                const submission = self.submitTensorCoreCandidate(
                    ctx,
                    plan,
                    heuristic.algo,
                    heuristic.workspace_size,
                    invocation,
                ) catch |err| {
                    // Finish/destroy the event pair exactly once and drain any
                    // earlier timed submissions before returning the fatal
                    // candidate error.
                    _ = ctx.endProfileEventPairUs(event_pair) catch {};
                    ctx.synchronize() catch {};
                    return err;
                };
                if (submission == .unsupported) break;
            }
            // endProfileEventPairUs owns and destroys both events on success
            // and error. Call it exactly once after every successful begin.
            // A failure here may report an asynchronous device error from one
            // of the candidate launches. Do not continue on a potentially
            // poisoned stream or publish any plan selected by this run.
            const elapsed_us = ctx.endProfileEventPairUs(event_pair) catch {
                // endProfileEventPairUs normally synchronizes the end event;
                // make one best-effort stream drain before returning so its
                // buffer-lifetime contract also holds on a driver error.
                ctx.synchronize() catch {};
                return error.CublasLtError;
            };
            if (launched != iterations) continue;
            timed_candidate_count += 1;
            if (elapsed_us < fastest_us) {
                fastest_us = elapsed_us;
                fastest_index = index;
            }
        }
        if (timed_candidate_count < 2) return error.CublasLtTuningInsufficientCandidates;
        return .{
            .index = fastest_index orelse return error.CublasLtTuningInsufficientCandidates,
            .timed_candidate_count = timed_candidate_count,
            .elapsed_us = fastest_us,
        };
    }

    fn submitTensorCoreCandidate(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        plan: TensorCorePlan,
        algo: MatmulAlgo,
        workspace_size: usize,
        invocation: TensorCoreInvocation,
    ) Error!CandidateSubmission {
        try checkRawBytes(invocation.workspace, workspace_size);
        var alpha: f32 = 1.0;
        var beta: f32 = 0.0;
        const workspace_ptr: ?*anyopaque = if (invocation.workspace.ptr != 0 and workspace_size > 0)
            @ptrFromInt(invocation.workspace.ptr)
        else
            null;
        ctx.makeCurrent() catch return error.CublasLtError;
        const status = self.fns.cublasLtMatmul(
            self.handle,
            plan.op_desc,
            &alpha,
            @ptrFromInt(invocation.weight.ptr),
            plan.a_desc,
            @ptrFromInt(invocation.input.ptr),
            plan.b_desc,
            &beta,
            @ptrFromInt(invocation.dst.ptr),
            plan.c_desc,
            @ptrFromInt(invocation.dst.ptr),
            plan.d_desc,
            &algo,
            workspace_ptr,
            workspace_size,
            ctx.stream,
        );
        if (status == CUBLAS_STATUS_SUCCESS) return .submitted;
        if (status == CUBLAS_STATUS_NOT_SUPPORTED) return .unsupported;
        // AlgoCheck already accepted this exact descriptor/algo combination.
        // INVALID_VALUE/ARCH_MISMATCH are invariant violations; execution,
        // mapping, allocation, and internal failures may poison the stream or
        // reflect global resource pressure. None are safe to skip as though
        // they were an ordinary unsupported candidate.
        return error.CublasLtError;
    }

    fn destroyTensorCorePlan(self: *CublasLt, plan: TensorCorePlan) void {
        if (plan.a_desc != null) _ = self.fns.cublasLtMatrixLayoutDestroy(plan.a_desc);
        if (plan.b_desc != null) _ = self.fns.cublasLtMatrixLayoutDestroy(plan.b_desc);
        if (plan.c_desc != null) _ = self.fns.cublasLtMatrixLayoutDestroy(plan.c_desc);
        if (plan.d_desc != null) _ = self.fns.cublasLtMatrixLayoutDestroy(plan.d_desc);
        if (plan.op_desc != null) _ = self.fns.cublasLtMatmulDescDestroy(plan.op_desc);
    }

    /// Tensor-core strided-batched row-major GEMM. The logical operation is
    /// `dst[batch, rows, out_dim] = input[batch, rows, in_dim] *
    /// weight[batch, out_dim, in_dim]^T`; F16 inputs accumulate and store F32.
    pub fn matmulF16StridedBatchedF32Out(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_f16: buffer_mod.DeviceBuffer,
        weight_f16: buffer_mod.DeviceBuffer,
        workspace: buffer_mod.DeviceBuffer,
        batch_count: usize,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) Error!void {
        try self.validateContext(ctx);
        if (batch_count == 0 or batch_count > std.math.maxInt(i32) or rows == 0 or in_dim == 0 or out_dim == 0) return error.CublasLtUnsupported;
        if (rows > std.math.maxInt(u32) or in_dim > std.math.maxInt(u32) or out_dim > std.math.maxInt(u32)) return error.CublasLtUnsupported;
        const input_stride = std.math.mul(usize, rows, in_dim) catch return error.CublasLtUnsupported;
        const weight_stride = std.math.mul(usize, out_dim, in_dim) catch return error.CublasLtUnsupported;
        const output_stride = std.math.mul(usize, rows, out_dim) catch return error.CublasLtUnsupported;
        const input_count = std.math.mul(usize, batch_count, input_stride) catch return error.CublasLtUnsupported;
        const weight_count = std.math.mul(usize, batch_count, weight_stride) catch return error.CublasLtUnsupported;
        const output_count = std.math.mul(usize, batch_count, output_stride) catch return error.CublasLtUnsupported;
        try checkRawBytes(input_f16, std.math.mul(usize, input_count, @sizeOf(u16)) catch return error.CublasLtUnsupported);
        try checkRawBytes(weight_f16, std.math.mul(usize, weight_count, @sizeOf(u16)) catch return error.CublasLtUnsupported);
        try checkRawBytes(dst, std.math.mul(usize, output_count, @sizeOf(f32)) catch return error.CublasLtUnsupported);
        const input_bytes = std.math.mul(usize, input_count, @sizeOf(u16)) catch return error.CublasLtUnsupported;
        const weight_bytes = std.math.mul(usize, weight_count, @sizeOf(u16)) catch return error.CublasLtUnsupported;
        const dst_bytes = std.math.mul(usize, output_count, @sizeOf(f32)) catch return error.CublasLtUnsupported;
        try requireDisjoint(dst, dst_bytes, input_f16, input_bytes);
        try requireDisjoint(dst, dst_bytes, weight_f16, weight_bytes);
        try requireDisjoint(workspace, workspace.len, input_f16, input_bytes);
        try requireDisjoint(workspace, workspace.len, weight_f16, weight_bytes);
        try requireDisjoint(workspace, workspace.len, dst, dst_bytes);
        try validateWorkspaceAlignment(workspace);

        const invocation = TensorCoreInvocation{
            .dst = dst,
            .input = input_f16,
            .weight = weight_f16,
            .workspace = workspace,
        };
        var plan_lease = try self.tensorCorePlanFor(.strided_batched, CUDA_R_16F, batch_count, rows, in_dim, out_dim, workspace.len, .{}, ctx, invocation);
        defer plan_lease.deinit(self);
        const plan = plan_lease.plan;
        try checkRawBytes(workspace, plan.workspace_size);
        var alpha: f32 = 1.0;
        var beta: f32 = 0.0;
        const workspace_ptr: ?*anyopaque = if (workspace.ptr != 0 and plan.workspace_size > 0) @ptrFromInt(workspace.ptr) else null;
        ctx.makeCurrent() catch return error.CublasLtError;
        const submit_status = self.fns.cublasLtMatmul(
            self.handle,
            plan.op_desc,
            &alpha,
            @ptrFromInt(weight_f16.ptr),
            plan.a_desc,
            @ptrFromInt(input_f16.ptr),
            plan.b_desc,
            &beta,
            @ptrFromInt(dst.ptr),
            plan.c_desc,
            @ptrFromInt(dst.ptr),
            plan.d_desc,
            &plan.algo,
            workspace_ptr,
            plan.workspace_size,
            ctx.stream,
        );
        self.check(submit_status) catch |err| {
            ctx.synchronize() catch {};
            return err;
        };
        _ = plan_lease.publish(self);
    }

    fn configureStridedBatch(self: *CublasLt, layout: MatrixLayout, batch_count: i32, stride: i64) Error!void {
        try self.check(self.fns.cublasLtMatrixLayoutSetAttribute(layout, CUBLASLT_MATRIX_LAYOUT_BATCH_COUNT, &batch_count, @sizeOf(i32)));
        try self.check(self.fns.cublasLtMatrixLayoutSetAttribute(layout, CUBLASLT_MATRIX_LAYOUT_STRIDED_BATCH_OFFSET, &stride, @sizeOf(i64)));
    }

    pub fn matmulF32WeightF32Out(
        self: *CublasLt,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_f32: buffer_mod.DeviceBuffer,
        weight_f32: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) Error!void {
        try self.validateContext(ctx);
        if (rows == 0 or in_dim == 0 or out_dim == 0) return;
        if (rows > std.math.maxInt(u32) or in_dim > std.math.maxInt(u32) or out_dim > std.math.maxInt(u32)) return error.CublasLtUnsupported;
        try checkRawBytes(input_f32, try checkedMatrixBytes(rows, in_dim, @sizeOf(f32)));
        try checkRawBytes(weight_f32, try checkedMatrixBytes(out_dim, in_dim, @sizeOf(f32)));
        try checkRawBytes(dst, try checkedMatrixBytes(rows, out_dim, @sizeOf(f32)));

        var op_desc: MatmulDesc = null;
        try self.check(self.fns.cublasLtMatmulDescCreate(&op_desc, CUBLAS_COMPUTE_32F, CUDA_R_32F));
        defer _ = self.fns.cublasLtMatmulDescDestroy(op_desc);

        var transa = CUBLAS_OP_T;
        var transb = CUBLAS_OP_N;
        try self.check(self.fns.cublasLtMatmulDescSetAttribute(op_desc, CUBLASLT_MATMUL_DESC_TRANSA, &transa, @sizeOf(c_int)));
        try self.check(self.fns.cublasLtMatmulDescSetAttribute(op_desc, CUBLASLT_MATMUL_DESC_TRANSB, &transb, @sizeOf(c_int)));

        var a_desc: MatrixLayout = null;
        var b_desc: MatrixLayout = null;
        var c_desc: MatrixLayout = null;
        var d_desc: MatrixLayout = null;
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&a_desc, CUDA_R_32F, in_dim, out_dim, @intCast(in_dim)));
        defer _ = self.fns.cublasLtMatrixLayoutDestroy(a_desc);
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&b_desc, CUDA_R_32F, in_dim, rows, @intCast(in_dim)));
        defer _ = self.fns.cublasLtMatrixLayoutDestroy(b_desc);
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&c_desc, CUDA_R_32F, out_dim, rows, @intCast(out_dim)));
        defer _ = self.fns.cublasLtMatrixLayoutDestroy(c_desc);
        try self.check(self.fns.cublasLtMatrixLayoutCreate(&d_desc, CUDA_R_32F, out_dim, rows, @intCast(out_dim)));
        defer _ = self.fns.cublasLtMatrixLayoutDestroy(d_desc);

        var pref: MatmulPreference = null;
        try self.check(self.fns.cublasLtMatmulPreferenceCreate(&pref));
        defer _ = self.fns.cublasLtMatmulPreferenceDestroy(pref);
        var max_workspace: usize = 0;
        try self.check(self.fns.cublasLtMatmulPreferenceSetAttribute(pref, CUBLASLT_MATMUL_PREF_MAX_WORKSPACE_BYTES, &max_workspace, @sizeOf(usize)));

        var heuristic: [1]MatmulHeuristicResult = undefined;
        var returned: c_int = 0;
        try self.check(self.fns.cublasLtMatmulAlgoGetHeuristic(self.handle, op_desc, a_desc, b_desc, c_desc, d_desc, pref, 1, &heuristic, &returned));
        if (returned <= 0 or heuristic[0].state != CUBLAS_STATUS_SUCCESS) return error.CublasLtUnsupported;

        var alpha: f32 = 1.0;
        var beta: f32 = 0.0;
        ctx.makeCurrent() catch return error.CublasLtError;
        try self.check(self.fns.cublasLtMatmul(
            self.handle,
            op_desc,
            &alpha,
            @ptrFromInt(weight_f32.ptr),
            a_desc,
            @ptrFromInt(input_f32.ptr),
            b_desc,
            &beta,
            @ptrFromInt(dst.ptr),
            c_desc,
            @ptrFromInt(dst.ptr),
            d_desc,
            &heuristic[0].algo,
            null,
            0,
            ctx.stream,
        ));
    }

    fn check(self: *const CublasLt, status: Status) Error!void {
        _ = self;
        if (status != CUBLAS_STATUS_SUCCESS) return error.CublasLtError;
    }

    fn validateContext(self: *const CublasLt, ctx: *context_mod.CudaContext) Error!void {
        if (ctx.ctx != self.owner_context or ctx.device != self.owner_device) {
            return error.CublasLtUnsupported;
        }
        ctx.makeCurrent() catch return error.CublasLtError;
    }
};

fn openLibrary() !std.DynLib {
    return std.DynLib.open("libcublasLt.so") catch
        std.DynLib.open("libcublasLt.so.11") catch
        std.DynLib.open("libcublasLt.so.12");
}

fn lookup(lib: *std.DynLib, comptime T: type, name: [:0]const u8) Error!T {
    return lib.lookup(T, name) orelse error.CublasLtSymbolMissing;
}

fn checkRawBytes(buffer: buffer_mod.DeviceBuffer, bytes: usize) Error!void {
    if (bytes > buffer.len or (bytes != 0 and buffer.ptr == 0)) return error.CublasLtUnsupported;
}

fn validateWorkspaceAlignment(workspace: buffer_mod.DeviceBuffer) Error!void {
    if (workspace.len == 0) return;
    // cuBLASLt documents workspace as 256-byte aligned. Device allocations
    // satisfy this, but borrowed/subview buffers must be rejected rather than
    // passed to an algorithm selected under a stronger alignment assumption.
    if (workspace.ptr == 0 or (workspace.ptr & 255) != 0) return error.CublasLtUnsupported;
}

fn requireDisjoint(
    a: buffer_mod.DeviceBuffer,
    a_bytes: usize,
    b: buffer_mod.DeviceBuffer,
    b_bytes: usize,
) Error!void {
    if (a_bytes == 0 or b_bytes == 0) return;
    if (a.ptr == 0 or b.ptr == 0) return error.CublasLtUnsupported;
    const a_len: u64 = std.math.cast(u64, a_bytes) orelse return error.CublasLtUnsupported;
    const b_len: u64 = std.math.cast(u64, b_bytes) orelse return error.CublasLtUnsupported;
    const a_end = std.math.add(u64, a.ptr, a_len) catch return error.CublasLtUnsupported;
    const b_end = std.math.add(u64, b.ptr, b_len) catch return error.CublasLtUnsupported;
    if (a.ptr < b_end and b.ptr < a_end) return error.CublasLtUnsupported;
}

fn cappedPointerAlignment(ptr: driver_mod.CUdeviceptr) u32 {
    if (ptr == 0) return 1;
    var alignment: u32 = 1;
    while (alignment < 256 and ptr % (@as(u64, alignment) * 2) == 0) {
        alignment *= 2;
    }
    return alignment;
}

fn checkedMatrixBytes(rows: usize, cols: usize, element_bytes: usize) Error!usize {
    const elements = std.math.mul(usize, rows, cols) catch return error.CublasLtUnsupported;
    return std.math.mul(usize, elements, element_bytes) catch return error.CublasLtUnsupported;
}

fn checkedI64Product(a: u32, b: u32) Error!i64 {
    const product = std.math.mul(u64, a, b) catch return error.CublasLtUnsupported;
    if (product > std.math.maxInt(i64)) return error.CublasLtUnsupported;
    return @intCast(product);
}

fn checkedHeuristicResultCount(returned: c_int, requested: c_int, capacity: usize) Error!usize {
    if (returned <= 0 or requested <= 0 or returned > requested) return error.CublasLtUnsupported;
    const count: usize = @intCast(returned);
    if (count > capacity) return error.CublasLtUnsupported;
    return count;
}

fn firstUsableHeuristicIndex(heuristics: []const MatmulHeuristicResult, workspace_bytes: usize) ?usize {
    for (heuristics, 0..) |heuristic, index| {
        if (heuristic.state == CUBLAS_STATUS_SUCCESS and heuristic.workspace_size <= workspace_bytes) {
            return index;
        }
    }
    return null;
}

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn lockYielding(mutex: *std.atomic.Mutex) void {
    var spins: u8 = 0;
    while (!mutex.tryLock()) {
        if (spins < 64) {
            std.atomic.spinLoopHint();
            spins += 1;
        } else {
            std.Thread.yield() catch std.atomic.spinLoopHint();
        }
    }
}

test "tensor core plan keys distinguish incompatible layouts" {
    const common = TensorCorePlanKey{
        .kind = .dense,
        .input_type = CUDA_R_16F,
        .batch_count = 1,
        .rows = 256,
        .in_dim = 768,
        .out_dim = 768,
        .workspace_bytes = 4 * 1024 * 1024,
        .alignment_a_bytes = 256,
        .alignment_b_bytes = 256,
        .alignment_c_bytes = 256,
        .alignment_d_bytes = 256,
        .tuning_fingerprint = 0,
    };
    var batched = common;
    batched.kind = .strided_batched;
    try std.testing.expect(!std.meta.eql(common, batched));
    var input_gradient = common;
    input_gradient.kind = .dense_input_gradient;
    try std.testing.expect(!std.meta.eql(common, input_gradient));
    try std.testing.expect(!std.meta.eql(batched, input_gradient));
}

test "matmul tuning fingerprints only normalized tuning behavior" {
    try std.testing.expectEqual(@as(u32, 0), (MatmulTuning{}).fingerprint());
    try std.testing.expectEqual(@as(u32, 0), (MatmulTuning{
        .candidate_count = 16,
        .warmups = 7,
        .iterations = 9,
    }).fingerprint());

    const tuned = MatmulTuning{
        .enabled = true,
        .candidate_count = 8,
        .warmups = 2,
        .iterations = 3,
    };
    try std.testing.expectEqual(@as(u32, 0x0003_0208), tuned.fingerprint());
    try std.testing.expectEqual(
        (MatmulTuning{ .enabled = true, .candidate_count = maximum_matmul_heuristics }).fingerprint(),
        (MatmulTuning{ .enabled = true, .candidate_count = std.math.maxInt(u8) }).fingerprint(),
    );
    try std.testing.expectEqual(
        (MatmulTuning{ .enabled = true, .candidate_count = 1 }).fingerprint(),
        (MatmulTuning{ .enabled = true, .candidate_count = 0 }).fingerprint(),
    );
    try std.testing.expectEqual(
        (MatmulTuning{ .enabled = true, .iterations = 1 }).fingerprint(),
        (MatmulTuning{ .enabled = true, .iterations = 0 }).fingerprint(),
    );
    try std.testing.expectEqual(
        (MatmulTuning{ .enabled = true, .warmups = maximum_matmul_tuning_warmups }).fingerprint(),
        (MatmulTuning{ .enabled = true, .warmups = std.math.maxInt(u8) }).fingerprint(),
    );
    try std.testing.expectEqual(
        (MatmulTuning{ .enabled = true, .iterations = maximum_matmul_tuning_iterations }).fingerprint(),
        (MatmulTuning{ .enabled = true, .iterations = std.math.maxInt(u8) }).fingerprint(),
    );
}

test "heuristic result counts reject invalid library output" {
    try std.testing.expectEqual(@as(usize, 2), try checkedHeuristicResultCount(2, 4, 16));
    try std.testing.expectError(error.CublasLtUnsupported, checkedHeuristicResultCount(0, 4, 16));
    try std.testing.expectError(error.CublasLtUnsupported, checkedHeuristicResultCount(5, 4, 16));
    try std.testing.expectError(error.CublasLtUnsupported, checkedHeuristicResultCount(2, 4, 1));
}

test "heuristic fallback skips failed and oversized algorithms" {
    const heuristics = [_]MatmulHeuristicResult{
        .{
            .algo = .{ .data = .{0} ** 8 },
            .workspace_size = 0,
            .state = 7,
            .waves_count = 0,
            .reserved = .{0} ** 4,
        },
        .{
            .algo = .{ .data = .{0} ** 8 },
            .workspace_size = 4096,
            .state = CUBLAS_STATUS_SUCCESS,
            .waves_count = 0,
            .reserved = .{0} ** 4,
        },
        .{
            .algo = .{ .data = .{0} ** 8 },
            .workspace_size = 1024,
            .state = CUBLAS_STATUS_SUCCESS,
            .waves_count = 0,
            .reserved = .{0} ** 4,
        },
    };
    try std.testing.expectEqual(@as(?usize, 2), firstUsableHeuristicIndex(&heuristics, 2048));
    try std.testing.expectEqual(@as(?usize, null), firstUsableHeuristicIndex(&heuristics, 512));
}

test "pointer alignment is capped at the cuBLASLt preference contract" {
    try std.testing.expectEqual(@as(u32, 1), cappedPointerAlignment(0));
    try std.testing.expectEqual(@as(u32, 1), cappedPointerAlignment(0x1001));
    try std.testing.expectEqual(@as(u32, 16), cappedPointerAlignment(0x1010));
    try std.testing.expectEqual(@as(u32, 256), cappedPointerAlignment(0x2000));
    try std.testing.expectEqual(@as(u32, 256), cappedPointerAlignment(0x1_0000));
}

test "workspace alignment rejects non-device-allocation subviews" {
    try validateWorkspaceAlignment(.{});
    try validateWorkspaceAlignment(.{ .ptr = 0x2000, .len = 4096 });
    try std.testing.expectError(
        error.CublasLtUnsupported,
        validateWorkspaceAlignment(.{ .ptr = 0, .len = 4096 }),
    );
    try std.testing.expectError(
        error.CublasLtUnsupported,
        validateWorkspaceAlignment(.{ .ptr = 0x2080, .len = 4096 }),
    );
}

test "matmul buffers reject overlap and address overflow" {
    try requireDisjoint(
        .{ .ptr = 0x1000, .len = 0x100 },
        0x100,
        .{ .ptr = 0x1100, .len = 0x100 },
        0x100,
    );
    try std.testing.expectError(
        error.CublasLtUnsupported,
        requireDisjoint(
            .{ .ptr = 0x1000, .len = 0x200 },
            0x200,
            .{ .ptr = 0x1100, .len = 0x100 },
            0x100,
        ),
    );
    try std.testing.expectError(
        error.CublasLtUnsupported,
        requireDisjoint(
            .{ .ptr = std.math.maxInt(u64) - 15, .len = 32 },
            32,
            .{ .ptr = 0x2000, .len = 32 },
            32,
        ),
    );
}

test "matrix byte checks reject overflow" {
    try std.testing.expectError(error.CublasLtUnsupported, checkedMatrixBytes(std.math.maxInt(usize), 2, 2));
}
