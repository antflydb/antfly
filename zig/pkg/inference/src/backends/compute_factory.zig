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

//! Backend construction from resolved storage and execution policy. Model
//! adapters translate architecture metadata before entering this boundary.
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const memory = @import("../runtime/tier/memory.zig");
const metal = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const metal_runtime = if (build_options.enable_metal) @import("metal_runtime.zig") else struct {};
const MetalWeights = if (build_options.enable_metal) @import("../ops/gpu_hosted_store.zig").WeightStore else void;
pub const MetalJitOptions = if (build_options.enable_metal) metal_runtime.MetalJitOptions else void;

pub const NativePolicy = struct {
    quantized_activations: native.QuantizedActivationPolicy = .automatic,
    borrow_bf16_linear_weights: bool = false,
    cache_bf16_weight_mirrors: bool = false,
};

pub fn createNative(allocator: std.mem.Allocator, weights: *native.WeightStore, budget: ?*memory.RunBudget, io: ?std.Io, policy: NativePolicy) !ops.ComputeBackend {
    const compute = try allocator.create(native.NativeCompute);
    compute.* = if (io) |handle| native.NativeCompute.initWithIo(allocator, weights, budget, handle) else native.NativeCompute.init(allocator, weights, budget);
    compute.quantized_activation_policy = policy.quantized_activations;
    compute.borrow_bf16_linear_weights = policy.borrow_bf16_linear_weights;
    compute.cache_embeddinggemma2_bf16_weights = policy.cache_bf16_weight_mirrors;
    return compute.computeBackend();
}

/// Static execution policies keep the existing tuned schedules and precision.
/// Historical backend field/native-symbol names are implementation adapters.
pub const MetalPolicy = struct {
    require_f32_linears: bool = false,
    encoder_rms_norm: bool = false,
    deferred_attention: bool = false,
    projection_fusions: bool = false,
};

pub fn createMetal(allocator: std.mem.Allocator, weights: *MetalWeights, budget: ?*memory.RunBudget, io: ?std.Io, jit: MetalJitOptions, policy: MetalPolicy) !ops.ComputeBackend {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const compute = try allocator.create(metal.MetalCompute);
    errdefer allocator.destroy(compute);
    try compute.initInPlaceWithKernelJitOptions(allocator, weights, budget, io, jit);
    compute.require_f32_linears = policy.require_f32_linears;
    compute.qwen35_ocr_deferred_attention = policy.deferred_attention;
    compute.lightonocr3_projections = policy.projection_fusions;
    // Initialization holds the model's shared-provider execution lease.
    if (compute.provider_impl.raw_decode_runtime) |raw| {
        metal_runtime.termite_metal_decode_runtime_set_embeddinggemma2_rms_norm(raw, @intFromBool(policy.encoder_rms_norm));
        metal_runtime.termite_metal_decode_runtime_set_qwen35_ocr_vector_loads(raw, @intFromBool(policy.deferred_attention));
        metal_runtime.termite_metal_decode_runtime_set_lightonocr3_projections(raw, @intFromBool(policy.projection_fusions));
    }
    return compute.ownedComputeBackend();
}

/// The caller already owns the shared provider's execution lease. This owner
/// must neither acquire it again nor release it when the borrowed backend dies.
pub fn borrowMetalProvider(allocator: std.mem.Allocator, weights: *MetalWeights, io: ?std.Io, jit: MetalJitOptions, require_f32_linears: bool) !ops.ComputeBackend {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const compute = try allocator.create(metal.MetalCompute);
    errdefer allocator.destroy(compute);
    compute.* = try metal.MetalCompute.initBorrowingSharedProvider(allocator, weights, io, jit);
    compute.require_f32_linears = require_f32_linears;
    var backend = compute.ownedComputeBackend();
    errdefer backend.deinit();
    try backend.beginRequest();
    return backend;
}

test "backend factory native construction and outputs unwind every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
            defer weights.deinitOwned();
            var backend = try createNative(a, &weights, null, null, .{ .quantized_activations = .strict_f32 });
            defer backend.deinit();
            const x = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
            defer backend.free(x);
            const y = try backend.add(x, x);
            defer backend.free(y);
            const values = try backend.toFloat32(y, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, values);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

pub fn acquireCuda(compute: *if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute else void, budget: ?*memory.RunBudget) !ops.ComputeBackend {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    // Shared compute never retains a pointer into an expired request budget.
    return if (budget) |active| compute.computeBackendWithScopedRunBudget(active) else compute.computeBackend();
}

pub fn beginRequest(session: @import("session.zig").Session, backend: *const ops.ComputeBackend) !void {
    if (comptime build_options.enable_cuda) {
        if (backend.kind() == .cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(backend.ptr));
            try compute.configureEmbeddingGemma2Admission(if (session.run_admission) |admission| .{
                .controller = admission.controller,
                .limits = admission.limits,
                .check_live_memory = admission.check_live_memory,
            } else null);
        }
    }
    // Invalidate request-local capture/budget state even when an early cache
    // hit will bypass normal KV-hook provisioning.
    try backend.beginRequest();
}
