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
    /// Cold construction policy, independent of submitting-thread registries.
    /// Requested owned spans are observed from creation; allocator padding and
    /// model storage retain their separate coverage and lifetime contracts.
    observe_allocations: bool = false,
    quantized_activations: native.QuantizedActivationPolicy = .automatic,
    borrow_bf16_linear_weights: bool = false,
    cache_bf16_weight_mirrors: bool = false,
};

pub fn createNative(allocator: std.mem.Allocator, weights: *native.WeightStore, budget: ?*memory.RunBudget, io: ?std.Io, policy: NativePolicy) !ops.ComputeBackend {
    // The default path keeps its exact allocator. Optional instrumentation
    // starts before constructing the temporary compute and retains only State
    // metadata through the enclosing synchronous dispatch observation.
    const registry = @import("../execution/allocation_registry.zig");
    const observers = @import("../execution/allocation_observer.zig");
    var effective_allocator = allocator;
    var owned_observer: ?*observers.State = null;
    defer if (owned_observer) |owner| owner.release();
    const register = registry.enabled();
    if (register) {
        if (observers.State.forAllocator(weights.allocator)) |owner| registry.bind(.model, owner) else registry.missing(.model);
    }
    var observation_failed = false;
    if (policy.observe_allocations or register) {
        if (observers.State.forAllocator(allocator)) |owner| {
            if (register) registry.bind(.compute, owner);
        } else if (observers.State.create(allocator, .from_owner_creation) catch null) |owner| {
            // A Source's admitted wrapper allocates its actual blocks from
            // this parent too. Link only the attested live backing, so Registry
            // excludes the child's overlapping payload from ancestor sums.
            if (@import("admitted_allocator.zig").AdmittedAllocator.backingAllocator(allocator)) |backing| {
                if (observers.State.forAllocator(backing)) |parent| owner.parent_owner = parent.retain();
            }
            owned_observer = owner;
            effective_allocator = owner.allocator();
            if (register) registry.bind(.compute, owner);
        } else {
            observation_failed = true;
            if (register) registry.missing(.compute);
        }
    }
    const compute = try effective_allocator.create(native.NativeCompute);
    compute.* = if (io) |handle| native.NativeCompute.initWithIo(effective_allocator, weights, budget, handle) else native.NativeCompute.init(effective_allocator, weights, budget);
    compute.allocation_observer_enable_failed = observation_failed;
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

/// Exact caller allocator request for the ORIGINAL receiving-header
/// constructor. No provider/SDK/weight access occurs in this cold declaration.
pub const HostConstructionLayout = struct { bytes: usize, alignment: std.mem.Alignment };
pub fn metalConstructionLayout() !HostConstructionLayout {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    return .{ .bytes = @sizeOf(metal.MetalCompute), .alignment = .of(metal.MetalCompute) };
}

/// Same explicit-capacity/stdlib allocation recipe used by the actual
/// constructor; only the caller's allocator payload/alignment are returned.
pub fn metalRmsMetadataLayout(entries: u32) !HostConstructionLayout {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const layout = try metal.MetalCompute.rmsMetadata(entries);
    return .{ .bytes = layout.bytes, .alignment = layout.alignment };
}

pub fn metalProjectionMetadataHostBytes(linear_entries: u32, zero_widths: []const usize) !usize {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    return metal.MetalCompute.projectionMetadataHostBytes(.{ .linear_entries = linear_entries, .zero_widths = zero_widths });
}

/// Exact generic metadata requests for the model-authored text inventory.
pub fn metalWeightMetadataHostBytes(request: anytype) !usize {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    return metal.MetalCompute.weightMetadataHostBytes(.{
        .entries = request.entries,
        .name_bytes = request.name_bytes,
        .shape_axes = request.shape_axes,
        .vector_entries = request.vector_entries,
        .vector_elements = request.vector_elements,
        .slice_views = request.slice_views,
    });
}

pub fn createMetal(allocator: std.mem.Allocator, weights: *MetalWeights, budget: ?*memory.RunBudget, io: ?std.Io, jit: MetalJitOptions, policy: MetalPolicy) !ops.ComputeBackend {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const compute = try allocator.create(metal.MetalCompute);
    errdefer allocator.destroy(compute);
    try compute.initInPlaceWithKernelJitOptions(allocator, weights, budget, io, jit);
    errdefer compute.deinit();
    compute.require_f32_linears = policy.require_f32_linears;
    compute.qwen35_ocr_deferred_attention = policy.deferred_attention;
    compute.lightonocr3_projections = policy.projection_fusions;
    // Engine callers carry this explicit provider-operation capability through
    // outer frames and cached-runtime calls, independently of model lifetime.
    try compute.beginEngineOperation();
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
    if (compute.cacheRetirementPending()) return error.QueueFull;
    // Shared compute never retains a pointer into an expired request budget.
    return if (budget) |active| compute.computeBackendWithScopedRunBudget(active) else compute.computeBackend();
}

pub fn beginRequest(session: @import("session.zig").Session, backend: *const ops.ComputeBackend) !void {
    return beginRequestWithCleanup(session, backend, .{});
}
pub fn beginRequestWithCleanup(session: @import("session.zig").Session, backend: *const ops.ComputeBackend, current: @import("../execution/cleanup_context.zig").Context) !void {
    if (comptime build_options.enable_cuda) {
        if (backend.kind() == .cuda) {
            try @import("../ops/cuda/cuda_compute.zig").CudaCompute.checkRequestCacheRetirement(backend);
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(backend.ptr));
            try compute.configureEmbeddingGemma2AdmissionWithCleanup(if (session.run_admission) |admission| .{
                .controller = admission.controller,
                .limits = admission.limits,
                .check_live_memory = admission.check_live_memory,
            } else null, current);
            try @import("../ops/cuda/cuda_compute.zig").CudaCompute.beginRequestWithCleanup(backend, current);
            return;
        }
    }
    if (comptime build_options.enable_native) if (backend.vtable == &native.vtable_impl) {
        try @import("../execution/architecture_session.zig").validateNativePanelPreparationReceiver(session);
        try native.NativeCompute.configurePanelPreparationPolicy(backend, session.run_admission != null);
    };
    // Invalidate request-local capture/budget state even when an early cache
    // hit will bypass normal KV-hook provisioning.
    try backend.beginRequest();
}

pub fn closeMetalWeights(allocator: std.mem.Allocator, weights: *MetalWeights) void {
    closeMetalWeightsWithCleanup(allocator, weights, .{ .outside_execution = true });
}
pub fn closeMetalWeightsWithCleanup(allocator: std.mem.Allocator, weights: *MetalWeights, current: @import("../execution/cleanup_context.zig").Context) void {
    if (comptime !build_options.enable_metal) return;
    metal.validateSharedNativeProviderWorkspaceRetirement(weights, current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    metal.stopPrefetchWorker(weights);
    if (weights.boundary_resident) |owner| {
        owner.destroy();
        weights.boundary_resident = null;
    }
    metal.deinitSharedNativeProviderWithCleanup(weights, current);
    metal.deinitPrefetchQueue(weights);
    var it = weights.lazy_weights.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
        if (entry.value_ptr.host_loaded) |*host_loaded| host_loaded.deinit();
        entry.value_ptr.tensor_ref.deinit(allocator);
        allocator.free(entry.key_ptr.*);
    }
    weights.lazy_weights.deinit(allocator);
    metal.deinitPackedExpertViews(weights, allocator);
    if (weights.residency) |*residency| residency.deinit();
    if (weights.jina_lora_adapter) |adapter| adapter.destroy();
    if (weights.tensor_store) |store| store.deinit();
    if (weights.tier_cache) |*tier_cache|
        tier_cache.deinitAdmission();
}

pub fn prepareMetalProvider(allocator: std.mem.Allocator, weights: *MetalWeights, jit: MetalJitOptions) !void {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    if (!jit.config.mode.compiles()) return;
    var prepared = try metal.MetalCompute.initWithKernelJitScopeAndLoadContext(allocator, weights, null, jit.config, jit.scope, jit.load_context);
    prepared.deinit();
}

pub fn initializeMetalWeightPrefetch(allocator: std.mem.Allocator, weights: *MetalWeights, start_worker: bool) !void {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    metal.initPrefetchQueue(weights, allocator);
    if (weights.lazy_weights.count() > 0 and start_worker) try metal.startPrefetchWorker(weights);
}

/// Model-owned CUDA compute stays at one address from construction through
/// upload, request reuse and capture retirement. Request handles only borrow it.
pub fn createCuda(allocator: std.mem.Allocator, config: @import("../graph/kernel_jit.zig").Config, profile: if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CapabilityProfile else void, scope: if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").KernelJitRouteScope else void, load_context: @import("../graph/kernel_jit.zig").LoadContext) !*if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute else void {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    const compute = try allocator.create(@import("../ops/cuda/cuda_compute.zig").CudaCompute);
    errdefer allocator.destroy(compute);
    try compute.initInPlaceWithKernelJitForScopeAndLoadContext(allocator, config, profile, scope, load_context);
    return compute;
}

pub fn closeCuda(allocator: std.mem.Allocator, compute: *if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute else void) void {
    if (comptime !build_options.enable_cuda) return;
    compute.deinit();
    allocator.destroy(compute);
}

pub fn closeCudaWithBoundary(allocator: std.mem.Allocator, compute: *if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute else void, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
    if (comptime !build_options.enable_cuda) return;
    closeCudaWithCleanup(allocator, compute, .{ .boundary = boundary });
}
pub fn closeCudaWithCleanup(allocator: std.mem.Allocator, compute: *if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig").CudaCompute else void, current: @import("../execution/cleanup_context.zig").Context) void {
    if (comptime !build_options.enable_cuda) return;
    compute.deinitWithCleanup(current);
    allocator.destroy(compute);
}

test "backend factory CUDA request handles borrow one stable model owner" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    const compute = createCuda(a, .{}, .bert_encoder, .{}, .dynamic) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer closeCuda(a, compute);
    const owner_address = @intFromPtr(compute);
    var first = try acquireCuda(compute, null);
    const input = try first.fromFloat32Shape(&.{ -1, 2, -3, 4 }, &.{ 2, 2 });
    defer first.free(input);
    const held = try first.add(input, input);
    defer first.free(held);
    first.deinit();
    var second = try acquireCuda(compute, null);
    defer second.deinit();
    try std.testing.expectEqual(owner_address, @intFromPtr(second.ptr));
    const output = try second.multiply(input, input);
    defer second.free(output);
    const values = try second.toFloat32(output, a);
    defer a.free(values);
    const retained = try second.toFloat32(held, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, &.{ 1, 4, 9, 16 }, values);
    try std.testing.expectEqualSlices(f32, &.{ -2, 4, -6, 8 }, retained);
}

/// Optional PJRT plugin ownership belongs to backend construction. The native
/// fallback remains explicit and is selected before any model execution.
pub fn createPjrtClient(allocator: std.mem.Allocator) if (build_options.enable_pjrt) ?@import("pjrt").pjrt.Client else void {
    if (comptime !build_options.enable_pjrt) return;
    return @import("pjrt").pjrt.Client.initFromEnv(allocator) catch |err| {
        std.log.warn("PJRT plugin not found ({s}); PJRT session will use native CPU fallback", .{@errorName(err)});
        return null;
    };
}

/// Install guards only after the inventory reaches its final stable address.
/// The session owner closes the partially initialized inventory on failure.
pub fn initializeNativeWeightPrefetch(weights: *native.WeightStore, allocator: std.mem.Allocator) !void {
    native.initPrefetchQueue(weights, allocator);
    var it = weights.lazy_weights.iterator();
    while (it.next()) |entry| entry.value_ptr.guard = weights.prefetch.lockHandle();
    if (weights.lazy_weights.count() > 0) try native.startPrefetchWorker(weights);
}

test "observed native factory retires its caller arena before immutable interval sampling" {
    const a = std.testing.allocator;
    const registry_mod = @import("../execution/allocation_registry.zig");
    var registry: registry_mod.Registry = undefined;
    registry.begin(a);
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    // The model-owned prefetch queue keeps its independently valid allocator.
    native.initPrefetchQueue(&weights, a);
    var arena = std.heap.ArenaAllocator.init(a);
    var backend = try createNative(arena.allocator(), &weights, null, null, .{ .quantized_activations = .strict_f32 });
    const x = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
    const y = try backend.add(x, x);
    const actual = try backend.toFloat32(y, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, actual);
    backend.free(y);
    backend.free(x);
    backend.deinit();
    arena.deinit();
    const measured = registry.finish();
    try std.testing.expect(measured.allocations.? > 0);
    try std.testing.expectEqual(measured.allocations.?, measured.frees.?);
    try std.testing.expectEqual(@as(u64, 0), measured.requested_live_bytes.?);
    try std.testing.expect(measured.maximum_individual_lifetime_peak_bytes.? > 0);
    try std.testing.expectEqual(@as(usize, 1), measured.missing_model_registrations);
    try std.testing.expectEqual(@as(usize, 0), measured.missing_compute_registrations);
}

test "explicit cold Native factory observation preserves default allocator and escaped storage ownership" {
    const a = std.testing.allocator;
    const observers = @import("../execution/allocation_observer.zig");
    const registry = @import("../execution/allocation_registry.zig");
    try std.testing.expect(!registry.enabled());
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    var ordinary = try createNative(a, &weights, null, null, .{});
    defer ordinary.deinit();
    const raw: *native.NativeCompute = @ptrCast(@alignCast(ordinary.ptr));
    try std.testing.expect(raw.allocator.ptr == a.ptr and raw.allocator.vtable == a.vtable);
    try std.testing.expect(observers.State.forAllocator(raw.allocator) == null);
    var measured = try createNative(a, &weights, null, null, .{ .observe_allocations = true });
    var live = true;
    defer if (live) measured.deinit();
    const compute: *native.NativeCompute = @ptrCast(@alignCast(measured.ptr));
    const owner = (observers.State.forAllocator(compute.allocator) orelse return error.MissingColdFactoryObserver).retain();
    defer owner.release();
    try std.testing.expect(owner.coverage == .from_owner_creation);
    const tensor = try measured.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
    defer ordinary.free(tensor);
    measured.deinit();
    live = false;
    try std.testing.expect(owner.snapshot().requested_live_bytes != 0);
    const values = try ordinary.toFloat32(tensor, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
    try std.testing.expect(!registry.enabled());
}

test "explicit cold Native factory construction and tensor retirement unwind every required allocation fault" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
            defer weights.deinitOwned();
            var cb = try createNative(a, &weights, null, null, .{ .observe_allocations = true });
            defer cb.deinit();
            const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
            defer cb.free(tensor);
            const values = try cb.toFloat32(tensor, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
        }
    };
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(backing.allocator(), Case.run, .{});
}

/// Cold allocation before reader/provider/model execution lanes. The retained
/// target moves only when a concrete prepared bank is returned. Unsupported
/// private/match modes retain original admission and numerical preparation.
pub fn prepareKvBanks(target: *@import("cold_compute_target.zig").Retained, dtype: @import("../runtime/kv/pool.zig").KvDType, window: @import("../runtime/kv/device_capacity.zig").Window, mode: @import("../execution/prepared_kv_banks.zig").Mode, geometry: []const @import("../runtime/kv/device_capacity.zig").LayerGeometry, admission: @import("../runtime/kv/device_capacity.zig").Admission, control: @import("../execution_control.zig").InferenceExecutionControl, cleanup: *const @import("../execution/retirement_sink.zig").Epoch) !?@import("../execution/prepared_kv_banks.zig").Owned {
    const Banks = @import("../execution/prepared_kv_banks.zig").Owned;
    switch (target.target) {
        .cuda => {
            if (comptime !build_options.enable_cuda) return null;
            const Leaf = @import("../ops/cuda/cuda_compute.zig").ColdKvBank;
            const leaf = (try Leaf.prepare(target, dtype, window, mode, geometry, admission, control, cleanup)) orelse return null;
            return Banks.fromLeaf(Leaf, leaf);
        },
        .metal => {
            if (comptime !build_options.enable_metal) return null;
            const Leaf = @import("metal_kv_storage.zig").MetalKvStorage.ColdKvBank;
            const leaf = (try Leaf.prepare(target, dtype, window, mode, geometry, admission, control, cleanup)) orelse return null;
            return Banks.fromLeaf(Leaf, leaf);
        },
    }
}

/// Metadata only; the caller's actual retained model keeps Target valid. The
/// returned Descriptor owns rows inline and must be placed before coverage().
pub fn describeKvCoverage(target: @import("cold_compute_target.zig").Target, dtype: @import("../runtime/kv/pool.zig").KvDType, window: @import("../runtime/kv/device_capacity.zig").Window, mode: @import("../execution/prepared_kv_banks.zig").Mode, geometry: []const @import("../runtime/kv/device_capacity.zig").LayerGeometry) !?@import("../execution/prepared_kv_banks.zig").Descriptor {
    return switch (target) {
        .cuda => if (comptime build_options.enable_cuda) try @import("../ops/cuda/cuda_compute.zig").ColdKvBank.describeCoverage(target, dtype, window, mode, geometry) else null,
        .metal => if (comptime build_options.enable_metal) try @import("metal_kv_storage.zig").MetalKvStorage.ColdKvBank.describeCoverage(target, dtype, window, mode, geometry) else null,
    };
}

test "Metal receiving constructor host declaration uses actual type layout without provider or SDK preparation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const layout = try metalConstructionLayout();
    try std.testing.expectEqual(@as(usize, @sizeOf(metal.MetalCompute)), layout.bytes);
    try std.testing.expectEqual(std.mem.Alignment.of(metal.MetalCompute), layout.alignment);
    const Allocator = @import("admitted_allocator.zig").AdmittedAllocator;
    const bytes = try Allocator.constructionHostBytes(layout.bytes, layout.alignment);
    try std.testing.expectEqual(layout.bytes + Allocator.allocationOverhead(layout.alignment) + Allocator.reservation_overhead, bytes);
}

test "Metal Gemma text Source declares RMS metadata payload and one allocator header without another reservation node" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const layout = try metalRmsMetadataLayout(try @import("../architectures/embedding_gemma2.zig").textRmsMetadataEntries(24));
    const exact = try metal.MetalCompute.rmsMetadata(try @import("../architectures/embedding_gemma2.zig").textRmsMetadataEntries(24));
    try std.testing.expectEqual(exact.bytes, layout.bytes);
    try std.testing.expectEqual(exact.alignment, layout.alignment);
    const Allocator = @import("admitted_allocator.zig").AdmittedAllocator;
    const actual = try std.math.add(usize, layout.bytes, Allocator.allocationOverhead(layout.alignment));
    const declaration_with_extra_node = try Allocator.constructionHostBytes(layout.bytes, layout.alignment);
    try std.testing.expectEqual(actual + Allocator.reservation_overhead, declaration_with_extra_node);
}

test "Metal Gemma RMS typed host declaration exactcap and onebelow retain original device floor" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const sessions = @import("session.zig");
    const Allocator = @import("admitted_allocator.zig").AdmittedAllocator;
    const metadata = try metalRmsMetadataLayout(try @import("../architectures/embedding_gemma2.zig").textRmsMetadataEntries(24));
    const constructor = try metalConstructionLayout();
    const host = try std.math.add(usize, 64 + try Allocator.constructionHostBytes(constructor.bytes, constructor.alignment), metadata.bytes + Allocator.allocationOverhead(metadata.alignment));
    const Info = struct {
        fn outputs(_: *anyopaque) []const @import("tensor.zig").TensorInfo {
            return &.{};
        }
    };
    var table: sessions.Session.VTable = undefined;
    table.outputInfo = Info.outputs;
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    for ([_]usize{ host - 1, host }) |limit| {
        const session: sessions.Session = .{ .ptr = undefined, .vtable = &table, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = limit, .backend_limit_bytes = 4096 }, .static_workspace_bytes = 0, .check_live_memory = false } };
        var owner: Allocator = .{ .backing = std.testing.allocator, .session = session };
        defer owner.deinit();
        var scope: Allocator.RunScope = undefined;
        const request: sessions.RunRequest = .{ .host_preprocess_bytes = host, .workspace_bytes = 4096, .output_bytes = 0 };
        if (limit < host) {
            try std.testing.expectError(error.ResourceLimitExceeded, owner.admitRunInto(&scope, request));
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        } else {
            try owner.admitRunInto(&scope, request);
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = host, .backend_scratch_bytes = 4096 }, scope.permit.lease.?.amounts);
            scope.deinit();
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        }
    }
}

test "Native admitted observer ancestry is actual backing on independent concurrent workers" {
    const observers = @import("../execution/allocation_observer.zig");
    const registry = @import("../execution/allocation_registry.zig");
    const Admitted = @import("admitted_allocator.zig").AdmittedAllocator;
    const Worker = struct {
        root: *observers.State,
        controller: *memory.AdmissionController,
        result: registry.Summary = .{},
        failure: ?anyerror = null,
        fn admit(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = bytes }, false);
        }
        fn execute(self: *@This()) void {
            self.run() catch |err| {
                self.failure = err;
            };
        }
        fn run(self: *@This()) !void {
            // No parent/submitting-thread registry is inherited by a worker.
            try std.testing.expect(!registry.enabled());
            var local: registry.Registry = undefined;
            local.begin(std.testing.allocator);
            var active = true;
            defer if (active) {
                _ = local.finish();
            };
            var admitted = Admitted{ .backing = self.root.allocator(), .admission_context = self, .admit = admit };
            defer admitted.deinit();
            var weights = native.WeightStore{ .allocator = self.root.allocator(), .resident_weights = .empty, .lazy_weights = .empty };
            native.initPrefetchQueue(&weights, weights.allocator);
            defer weights.deinitOwned();
            var cb = try createNative(admitted.allocator(), &weights, null, null, .{});
            var cb_live = true;
            defer if (cb_live) cb.deinit();
            const compute: *native.NativeCompute = @ptrCast(@alignCast(cb.ptr));
            const child = observers.State.forAllocator(compute.allocator) orelse return error.MissingWorkerObserver;
            try std.testing.expect(child.parent().? == self.root);
            const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
            const values = cb.toFloat32(tensor, std.testing.allocator) catch |err| {
                cb.free(tensor);
                return err;
            };
            defer std.testing.allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
            cb.free(tensor);
            cb.deinit();
            cb_live = false;
            self.result = local.finish();
            active = false;
            try std.testing.expectEqual(@as(usize, 2), self.result.owners);
            try std.testing.expectEqual(@as(usize, 1), self.result.observed_roots);
            try std.testing.expectEqual(@as(usize, 0), self.result.missing_model_registrations);
            try std.testing.expectEqual(@as(usize, 0), self.result.missing_compute_registrations);
            var child_seen = false;
            for (self.result.reports[0..self.result.reports_count]) |value| if (value.compute) {
                child_seen = true;
                try std.testing.expectEqual(@intFromPtr(self.root), value.parent_identity.?);
                try std.testing.expect(value.covered_by_registered_ancestor);
            };
            try std.testing.expect(child_seen);
        }
    };
    const root = try observers.State.create(std.testing.allocator, .from_owner_creation);
    var root_live = true;
    defer if (root_live) root.release();
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var parent: registry.Registry = undefined;
    parent.begin(std.testing.allocator);
    var parent_live = true;
    defer if (parent_live) {
        _ = parent.finish();
    };
    registry.bind(.model, root);
    var workers: [3]Worker = undefined;
    var threads: [3]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&workers, &threads) |*worker, *thread| {
        worker.* = .{ .root = root, .controller = &controller };
        thread.* = try std.Thread.spawn(.{}, Worker.execute, .{worker});
        started += 1;
    }
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    for (workers) |worker| if (worker.failure) |err| return err;
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    const identity = @intFromPtr(root);
    root.release();
    root_live = false;
    const summary = parent.finish();
    parent_live = false;
    // Main-thread registration was not mutated by worker-local registries.
    // The actual shared root interval includes its real concurrent activity.
    try std.testing.expectEqual(@as(usize, 1), summary.owners);
    try std.testing.expect(summary.allocations.? > 0);
    try std.testing.expectEqual(summary.allocations.?, summary.frees.?);
    try std.testing.expectEqual(@as(u64, 0), summary.requested_live_bytes.?);
    for (workers) |worker| for (worker.result.reports[0..worker.result.reports_count]) |report| if (report.compute) {
        try std.testing.expectEqual(identity, report.parent_identity.?); // copied after actual owners retired
    };
}

test "Native admitted observer ancestry refuses copied allocator tables before opaque pointer access" {
    const Admitted = @import("admitted_allocator.zig").AdmittedAllocator;
    var admitted = Admitted{ .backing = std.testing.allocator };
    const actual = admitted.allocator();
    try std.testing.expect(Admitted.backingAllocator(actual).?.ptr == std.testing.allocator.ptr);
    const copied = actual.vtable.*;
    const unknown = std.mem.Allocator{ .ptr = @ptrFromInt(1), .vtable = &copied };
    try std.testing.expect(Admitted.backingAllocator(unknown) == null);
}

test "Native late allocator observation preserves admitted actual parent without claiming creation coverage" {
    const observers = @import("../execution/allocation_observer.zig");
    const registry = @import("../execution/allocation_registry.zig");
    const Admitted = @import("admitted_allocator.zig").AdmittedAllocator;
    const root = try observers.State.create(std.testing.allocator, .from_owner_creation);
    defer root.release();
    const Funding = struct {
        controller: memory.AdmissionController = .{},
        fn admit(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = bytes }, false);
        }
    };
    var funding = Funding{};
    defer funding.controller.deinit();
    var admitted = Admitted{ .backing = root.allocator(), .admission_context = &funding, .admit = Funding.admit };
    defer admitted.deinit();
    var weights = native.WeightStore{ .allocator = root.allocator(), .resident_weights = .empty, .lazy_weights = .empty };
    native.initPrefetchQueue(&weights, weights.allocator);
    defer weights.deinitOwned();
    try std.testing.expect(!registry.enabled());
    var cb = try createNative(admitted.allocator(), &weights, null, null, .{});
    defer cb.deinit();
    const compute: *native.NativeCompute = @ptrCast(@alignCast(cb.ptr));
    try std.testing.expect(observers.State.forAllocator(compute.allocator) == null);
    cb.enableRuntimeObservation();
    const child = observers.State.forAllocator(compute.allocator) orelse return error.MissingLateObserver;
    try std.testing.expect(child.parent().? == root);
    try std.testing.expectEqual(observers.Coverage.since_enable, child.coverage);
    const tensor = try cb.fromFloat32Shape(&.{ 2, 3 }, &.{2});
    defer cb.free(tensor);
    try std.testing.expect(child.snapshot().allocations > 0);
}

test "Native admitted observer parent allocation failure retires child before actual source owner" {
    const Case = struct {
        controller: memory.AdmissionController = .{},
        fn admit(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = bytes }, false);
        }
        fn run(a: std.mem.Allocator) !void {
            const observers = @import("../execution/allocation_observer.zig");
            const parent = try observers.State.createWithMetadata(a, a, .from_owner_creation);
            defer parent.release();
            var funding = @This(){};
            defer funding.controller.deinit();
            var admitted = @import("admitted_allocator.zig").AdmittedAllocator{ .backing = parent.allocator(), .admission_context = &funding, .admit = admit };
            defer admitted.deinit();
            var weights = native.WeightStore{ .allocator = parent.allocator(), .resident_weights = .empty, .lazy_weights = .empty };
            native.initPrefetchQueue(&weights, weights.allocator);
            defer weights.deinitOwned();
            var cb = try createNative(admitted.allocator(), &weights, null, null, .{ .observe_allocations = true });
            defer cb.deinit();
            const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
            defer cb.free(tensor);
            const values = try cb.toFloat32(tensor, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
