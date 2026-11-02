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

//! Device-hosted artifact preparation. Resolves weight mappings, immutable
//! storage policy and admitted residency before handing a typed inventory to
//! the final session. No serving or session implementation dependency.
const backend_factory = @import("../backends/compute_factory.zig");
const std = @import("std");
const backend_compatibility = @import("backend_compatibility.zig");
const host_weights = @import("host_weights.zig");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const BackendType = @import("../backends/backends.zig").BackendType;
const bert = @import("bert.zig");
const gpt_mod = @import("gpt.zig");
const safetensors_mod = @import("safetensors.zig");
const boundary_bundle = @import("gliner_boundary_bundle.zig");
const embedding_gemma2_arch = @import("../architectures/embedding_gemma2.zig");
const deepseek_v4_arch = @import("../architectures/deepseek_v4.zig");
const gliner_decide_qualification = @import("gliner_decide_qualification.zig");
const kernel_jit = @import("../graph/kernel_jit.zig");
const manifest_mod = @import("manifest.zig");
const weight_source_mod = @import("weight_source.zig");
const tensor_store_mod = @import("tensor_store.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = @import("../util/c_file.zig");
const runtime = @import("../runtime/root.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");
const GpuHostedQuantExecutionMode = @import("../ops/gpu_hosted_store.zig").QuantExecutionMode;
const metal_runtime = if (build_options.enable_metal) @import("../backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }

    fn validateMetalJitLoadContext(
        config: kernel_jit.Config,
        load_context: kernel_jit.LoadContext,
    ) !void {
        if (!load_context.allowsQualification() and
            (config.mode.failClosed() or config.qualified_profile_path != null or config.profile_capture_only))
        {
            return error.KernelJitRequiredDynamicLoad;
        }
    }
};
const MetalJitRouteScope = if (build_options.enable_metal) metal_runtime.MetalJitRouteScope else void;
const directQuantEnabled = host_weights.directQuantEnabled;

pub fn gpuHostedQuantExecutionMode(direct_quant_enabled: bool) GpuHostedQuantExecutionMode {
    if (!direct_quant_enabled) return .prefer_backend_dense;

    const slice = platform.env.getenv("TERMITE_METAL_QUANT_MODE") orelse return .device_native;
    if (std.ascii.eqlIgnoreCase(slice, "dense") or std.ascii.eqlIgnoreCase(slice, "prefer_backend_dense")) {
        return .prefer_backend_dense;
    }
    if (std.ascii.eqlIgnoreCase(slice, "wrapper") or std.ascii.eqlIgnoreCase(slice, "wrapper_direct_quant")) {
        return .wrapper_direct_quant;
    }
    if (std.ascii.eqlIgnoreCase(slice, "device") or std.ascii.eqlIgnoreCase(slice, "device_native")) {
        return .device_native;
    }
    return .device_native;
}

fn gpuHostedEagerDenseMaxBytes() u64 {
    const mb = platform.env.getenvUsize("TERMITE_METAL_EAGER_DENSE_MAX_MB") orelse return 1024 * 1024 * 1024;
    return mb * 1024 * 1024;
}

fn shouldUseMetalHostedEagerDenseLoad(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    _ = allocator;
    _ = mf;
    _ = arch_config;
    return false;
}

fn shouldUseGpuHostedEagerDenseLoad(
    backend_type: BackendType,
    allocator: std.mem.Allocator,
    mf: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) bool {
    return switch (backend_type) {
        .metal => shouldUseMetalHostedEagerDenseLoad(allocator, mf, arch_config),
        else => unreachable,
    };
}

fn shouldPreferGpuHostedF32DenseTensors(arch_config: ArchConfig) bool {
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .gemma,
        .layoutlmv3, .embedding_gemma2 => true,
        else => false,
    };
}

pub fn estimateNativeWeightBytes(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest) !u64 {
    return switch (mf.nativeWeightArtifactKind() orelse return 0) {
        .gguf => blk: {
            var total = try c_file.fileSize(allocator, mf.gguf_path.?);
            if (mf.gliner_head_gguf_path) |head_path| {
                total = std.math.add(u64, total, try c_file.fileSize(allocator, head_path)) catch
                    return error.ResourceLimitExceeded;
            }
            if (mf.gliner_head_safetensors_path) |head_path| {
                total = std.math.add(u64, total, try c_file.fileSize(allocator, head_path)) catch
                    return error.ResourceLimitExceeded;
            }
            break :blk total;
        },
        .safetensors => c_file.fileSize(allocator, mf.safetensors_path.?),
        .sharded_safetensors => shardedSafetensorsTotalBytes(
            allocator,
            mf.safetensors_index_path.?,
        ),
    };
}

const glinerBaseWeightKey = host_weights.glinerBaseWeightKey;
const validateNativeBoundaryWeights = host_weights.validateNativeBoundaryWeights;
const captureBoundaryIdentity = host_weights.captureBoundaryIdentity;
const captureGlinerDecisionIdentity = host_weights.captureGlinerDecisionIdentity;

fn shardedSafetensorsTotalBytes(allocator: std.mem.Allocator, index_path: []const u8) !u64 {
    const index_bytes = try c_file.readFile(allocator, index_path);
    defer allocator.free(index_bytes);

    var index = try safetensors_mod.ShardedIndex.load(allocator, index_bytes);
    defer index.deinit();

    const model_dir = std.fs.path.dirname(index_path) orelse return error.InvalidPath;
    var seen = std.StringHashMapUnmanaged(void){};
    defer {
        var it = seen.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        seen.deinit(allocator);
    }

    var total: u64 = 0;
    var it = index.weight_map.iterator();
    while (it.next()) |entry| {
        const shard_name = entry.value_ptr.*;
        if (seen.contains(shard_name)) continue;
        try seen.put(allocator, try allocator.dupe(u8, shard_name), {});
        const shard_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, shard_name });
        defer allocator.free(shard_path);
        total += try c_file.fileSize(allocator, shard_path);
    }
    return total;
}

const model_definition = @import("../architectures/model_definition.zig");
const ArchType = model_definition.ArchType;
const ArchConfig = model_definition.ArchConfig;
const SessionTask = @import("../backends/session.zig").ModelContract.Task;

pub const TaskOverride = SessionTask;

pub fn sessionTaskForModelType(model_type: manifest_mod.ModelType, override: ?TaskOverride) SessionTask {
    if (override) |value| return value;
    return switch (model_type) {
        .classifier, .reranker => .classifier,
        .extractor => .extractor,
        else => .generic,
    };
}

pub fn sessionEnablesDebertaRerankerWeightMirrors(
    model_type: manifest_mod.ModelType,
    arch_type: ArchType,
    task: SessionTask,
) bool {
    return model_type == .reranker and arch_type == .deberta and task == .classifier;
}

pub fn sessionEnablesImmutableF32WeightBorrow(
    backend_type: BackendType,
    arch_type: ArchType,
    task: SessionTask,
    gliner_span_encoder_family: manifest_mod.GlinerSpanEncoderFamily,
) bool {
    // These model-scoped lazy weights are read-only for the session lifetime.
    // Finetuning's explicit generic sessions and caller-created WeightStores
    // retain the copying contract; they must never inherit this capability
    // merely because a parameter name resembles an inference weight.
    if (backend_type != .metal or task != .extractor) return false;
    return arch_type == .gliner or
        (arch_type == .modern_bert and gliner_span_encoder_family == .modern_bert);
}

pub fn sessionPrefersImmutableGlinerF32Mps(
    backend_type: BackendType,
    arch_type: ArchType,
    task: SessionTask,
) bool {
    return backend_type == .metal and arch_type == .gliner and task == .extractor;
}

pub const GgufInspectionReport = host_weights.GgufInspectionReport;

pub fn qualifiedA4bArtifact(report: GgufInspectionReport) bool {
    const config = report.gpt_config orelse return false;
    if (!gemma4_runtime.isQualifiedA4bArchitecture(config)) return false;
    return report.a4b_packed_expert_layout_qualified and
        report.packed_moe_expert_tensor_count == report.packed_moe_q4_0_tensor_count and
        report.missing_required_tensors.len == 0;
}

const requiresModernBertSpanClassifier = host_weights.requiresModernBertSpanClassifier;
const layaQuantizesWeights = host_weights.layaQuantizesWeights;
const buildGgufInspectionReport = host_weights.buildGgufInspectionReport;
const ensureGgufInspectionCompatible = host_weights.ensureGgufInspectionCompatible;

fn ensureMetalGgufInspectionCompatible(report: GgufInspectionReport, gguf_path: []const u8) !void {
    if (comptime !build_options.enable_metal) return;
    if (metalGgufIncompatibleTensorType(report)) |entry| {
        std.log.err(
            "GGUF {s} uses {d} {s} tensors that are not release-safe on Metal; rejecting Metal backend",
            .{ gguf_path, entry.count, entry.tensor_type.name() },
        );
        return error.UnsupportedQuantFormatForMetalOnly;
    }
}

const metalGgufIncompatibleTensorType = backend_compatibility.metalGgufIncompatibleTensorType;
const normalizeWeightKey = host_weights.normalizeWeightKey;
const maybeInferGptAttentionLayoutFromStore = host_weights.maybeInferGptAttentionLayoutFromStore;
const findTensorNameByPriority = host_weights.findTensorNameByPriority;
const appendPackedMoeLazyWeights = host_weights.appendPackedMoeLazyWeights;

fn refineArchConfigFromStore(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    all_names: [][]const u8,
    arch_config: *ArchConfig,
) !void {
    switch (arch_config.*) {
        .gpt => |*cfg| try refineGptConfigFromStore(allocator, store, all_names, cfg),
        else => {},
    }
}

pub fn refineGptConfigFromStore(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    all_names: [][]const u8,
    config: *gpt_mod.Config,
) !void {
    if (store.kind() == .gguf) {
        deepseek_v4_arch.inferSchedulesFromGgufNames(config, all_names);
    }

    const embed_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.embed_tokens.weight",
        "language_model.model.embed_tokens.weight",
        "model.embed_tokens.weight",
        "embed_tokens.weight",
    });
    const lm_head_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.lm_head.weight",
        "lm_head.weight",
        "language_model.lm_head.weight",
        "output.weight",
    });

    if (embed_name) |name| {
        var ref = try store.describeTensor(allocator, name);
        defer ref.deinit(allocator);
        var loaded = try store.loadTensorRef(&ref);
        defer loaded.deinit();
        if (loaded.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(loaded.tensor.shape[0]);
            config.hidden_size = @intCast(loaded.tensor.shape[1]);
            if (lm_head_name == null) config.weight_tying = true;
            return;
        }
    }

    if (lm_head_name) |name| {
        var ref = try store.describeTensor(allocator, name);
        defer ref.deinit(allocator);
        var loaded = try store.loadTensorRef(&ref);
        defer loaded.deinit();
        if (loaded.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(loaded.tensor.shape[0]);
            config.hidden_size = @intCast(loaded.tensor.shape[1]);
        }
    }
}

fn shouldKeepGpuHostedLazyWeightDense(backend_type: BackendType, arch_config: ArchConfig, key: []const u8) bool {
    _ = backend_type;
    if (arch_config == .gliner_boundary) {
        const policy = @import("gliner_boundary_artifact.zig");
        for (policy.specs(arch_config.gliner_boundary.backbone)) |spec| {
            if (std.mem.eql(u8, glinerBaseWeightKey(spec.name), key)) return policy.role(spec) != .encoder_matrix;
        }
        // The inventory admission rejects unknown tensors before this point.
        // Preserve dense bytes if a future caller reaches an unknown key.
        return true;
    }
    return false;
}

fn isMetalJitTiedEmbeddingWeightKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "embed_tokens.weight") or
        std.mem.endsWith(u8, key, ".embed_tokens.weight") or
        std.mem.eql(u8, key, "wte.weight") or
        std.mem.endsWith(u8, key, ".wte.weight");
}

pub fn isMetalJitLinearWeightKey(key: []const u8, projection_mask: u8, tied_lm_head: bool) bool {
    if (projection_mask != 0) return true;
    if (!std.mem.endsWith(u8, key, ".weight")) return false;
    // A tied GPT head reaches the token table through ordinary linearNoBias,
    // even though the same storage is also used by embedding lookup.
    if (tied_lm_head and isMetalJitTiedEmbeddingWeightKey(key)) return true;
    // These two-dimensional tensors are lookup tables, not matmul routes.
    const non_linear_tables = [_][]const u8{
        "embed_tokens",
        "embedding",
        "embeddings",
        "position_embeddings",
        "token_type_embeddings",
        "word_embeddings",
        "rope_freqs",
        "wpe.weight",
        "wte.weight",
    };
    for (non_linear_tables) |needle| {
        if (std.mem.indexOf(u8, key, needle) != null) return false;
    }
    return true;
}

pub fn metalJitUsesExactProfileScope(config: kernel_jit.Config) bool {
    return config.profile_capture_only or config.qualified_profile_path != null;
}

fn metalJitTensorTypeInExactProfileScope(tensor_type: gguf_mod.tensor_types.KnownTensorType) bool {
    // Keep this boundary aligned with the workload tuner's first exact slice.
    // Other formats remain part of the general JIT scope, but cannot affect a
    // Q4_0/Q4_K/Q6_K capture or the later activation of that contract.
    return tensor_type == .Q4_0 or tensor_type == .Q4_K or tensor_type == .Q6_K;
}

/// Derive Metal JIT slots from quantized matrices that the loaded WeightStore
/// can actually feed to linear dispatch. Global GGUF type presence is too
/// broad: embedding tables and dequant-only tensors must not become required
/// matmul routes.
pub fn includeMetalJitTensorShape(
    scope: *MetalJitRouteScope,
    tensor_type: gguf_mod.tensor_types.KnownTensorType,
    dimensions: []const u64,
    fused_gate_up: bool,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(tensor_type)) return;
    const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(tensor_type) orelse return;
    if (dimensions.len < 2) {
        scope.invalidate(.invalid_shape, format, 0, 0);
        return;
    }
    // GGUF stores matrices as [in_dim, out_dim]; the JIT scope uses logical
    // [out_dim, in_dim] so its qualification fixtures match linear dispatch.
    const in_dim = std.math.cast(usize, dimensions[0]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, 0);
        return;
    };
    var out_dim = std.math.cast(usize, dimensions[1]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, in_dim);
        return;
    };
    // Packed GGUF gate+up sources register two logical lazy projections; each
    // runtime dispatch sees one half of the combined output dimension.
    if (fused_gate_up) {
        if (out_dim == 0 or out_dim % 2 != 0) {
            scope.invalidate(.invalid_shape, format, out_dim, in_dim);
            return;
        }
        out_dim /= 2;
    }
    _ = scope.includeQuantShape(format, out_dim, in_dim);
}

/// Composite stores expose their primary GGUF through `ggufFile`, so a linear
/// weight from a secondary GGUF may not be present in that catalog. Recover
/// the logical shape from the store's existing zero-copy quantized view.
fn includeMetalJitStorageShape(
    scope: *MetalJitRouteScope,
    storage: *const weight_source_mod.QuantizedStorage,
    fused_gate_up: bool,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    const tensor_type = switch (storage.tensor_type) {
        .known => |known| known,
        .bitnet_tl2 => return,
        .unknown => {
            scope.invalidate(.scope_discovery, .unknown, 0, 0);
            return;
        },
    };
    if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(tensor_type)) return;
    const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(tensor_type) orelse return;
    if (storage.shape.len != 2 or storage.shape[0] <= 0 or storage.shape[1] <= 0) {
        scope.invalidate(.invalid_shape, format, 0, 0);
        return;
    }
    var out_dim = std.math.cast(usize, storage.shape[0]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, 0);
        return;
    };
    const in_dim = std.math.cast(usize, storage.shape[1]) orelse {
        scope.invalidate(.dimension_overflow, format, out_dim, 0);
        return;
    };
    if (fused_gate_up) {
        if (out_dim % 2 != 0) {
            scope.invalidate(.invalid_shape, format, out_dim, in_dim);
            return;
        }
        out_dim /= 2;
    }
    _ = scope.includeQuantShape(format, out_dim, in_dim);
}

pub fn includeMetalJitLinearWeightFormats(
    scope: *MetalJitRouteScope,
    lazy_weights: *const std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry),
    tensor_store: ?tensor_store_mod.TensorStore,
    direct_quant_enabled: bool,
    arch_config: ArchConfig,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    if (!direct_quant_enabled) return;
    const store = tensor_store orelse return;
    const file = store.ggufFile() orelse return;
    const catalog = gguf_mod.tensor_catalog.Catalog.init(file);
    const tied_lm_head = switch (arch_config) {
        .gpt => |config| blk: {
            if (config.weight_tying) break :blk true;
            var has_separate_head = false;
            var names = lazy_weights.keyIterator();
            while (names.next()) |key| {
                if (isMetalJitOutputHeadKey(key.*)) {
                    has_separate_head = true;
                    break;
                }
            }
            break :blk !has_separate_head;
        },
        else => false,
    };
    var iterator = lazy_weights.iterator();
    while (iterator.next()) |entry| {
        const lazy = entry.value_ptr;
        if (!lazy.tensor_ref.quantized or lazy.prefer_dense or
            !isMetalJitLinearWeightKey(entry.key_ptr.*, lazy.projection_mask, tied_lm_head)) continue;
        const source_name = lazy.tensor_ref.source_name orelse lazy.tensor_ref.name;
        const tensor = catalog.find(source_name) orelse {
            var storage = (store.loadQuantizedStorageRef(&lazy.tensor_ref) catch |err| {
                std.log.warn(
                    "Metal runtime JIT scope discovery failed key={s} source={s}: {s}",
                    .{ entry.key_ptr.*, source_name, @errorName(err) },
                );
                scope.invalidate(.scope_discovery, .unknown, 0, 0);
                continue;
            }) orelse {
                std.log.warn(
                    "Metal runtime JIT scope discovery found no quantized storage key={s} source={s}",
                    .{ entry.key_ptr.*, source_name },
                );
                scope.invalidate(.scope_discovery, .unknown, 0, 0);
                continue;
            };
            defer storage.deinit();
            includeMetalJitStorageShape(scope, &storage, lazy.tensor_ref.fused_gate_up, exact_profile_scope);
            continue;
        };
        switch (tensor.tensor_type) {
            .known => |known| {
                if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(known)) continue;
                const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(known) orelse continue;
                if (tensor.dimensions.len != 2 and lazy.projection_mask == 0) {
                    scope.invalidate(.invalid_shape, format, 0, 0);
                    continue;
                }
                includeMetalJitTensorShape(scope, known, tensor.dimensions, lazy.tensor_ref.fused_gate_up, exact_profile_scope);
            },
            // BitNet TL2 has a bundled Metal route but no generated runtime-JIT
            // artifact in this catalog, so it is outside the JIT scope.
            .bitnet_tl2 => {},
            .unknown => scope.invalidate(.scope_discovery, .unknown, 0, 0),
        }
    }
}

pub fn isMetalJitOutputHeadKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "output.weight") or
        std.mem.eql(u8, key, "lm_head.weight") or
        std.mem.endsWith(u8, key, ".lm_head.weight");
}

const defaultResidentExpertsPerLayer = host_weights.defaultResidentExpertsPerLayer;

fn resolveA4bInferenceConfig(
    arch_config: ArchConfig,
    request: ?backend_contracts.A4bInferenceRequest,
    artifact_qualified: bool,
) !?backend_contracts.A4bInferenceConfig {
    const gpt_config = switch (arch_config) {
        .gpt => |config| config,
        else => {
            if (request != null) return error.A4bUnsupportedGeometry;
            return null;
        },
    };
    return resolveA4bGptInferenceConfig(gpt_config, request, artifact_qualified);
}

pub fn resolveA4bGptInferenceConfig(
    gpt_config: gpt_mod.Config,
    request: ?backend_contracts.A4bInferenceRequest,
    artifact_qualified: bool,
) !?backend_contracts.A4bInferenceConfig {
    if (!gemma4_runtime.isQualifiedA4bArchitecture(gpt_config)) {
        if (request != null) return error.A4bUnsupportedGeometry;
        return null;
    }
    if (!artifact_qualified) {
        if (request != null) return error.A4bUnsupportedArtifact;
        return null;
    }
    return try backend_contracts.buildA4bInferenceConfig(
        request orelse .{},
        backend_contracts.qualified_a4b_geometries[0],
    );
}

const defaultPlanContextForBackend = host_weights.defaultPlanContextForBackend;

pub fn recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes: u64, quant_mode: GpuHostedQuantExecutionMode) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return .{};

    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    // In device_native mode, quantized weights stay quantized on GPU — no 3× expansion.
    // Only need ~1.5× for quantized weights + activations + KV cache headroom.
    const host_max = if (quant_mode == .device_native) gib(6) else gib(4);
    const host_floor = clampBytes(total_bytes + mib(256), gib(2), host_max);
    const backend_multiplier: usize = if (quant_mode == .device_native) 1 else 3;
    const backend_extra: usize = if (quant_mode == .device_native) mib(512) else gib(1);
    const backend_floor = clampBytes(total_bytes * backend_multiplier + backend_extra, gib(2), gib(14));
    const combined_floor = clampBytes(host_floor + backend_floor + gib(1), gib(4), gib(18));

    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    };
}

fn recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes: u64, quant_mode: GpuHostedQuantExecutionMode) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes, quant_mode);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

pub fn recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(
    model_weight_bytes: u64,
    prefer_f32_dense_tensors: bool,
) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return .{};

    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    const promoted_bytes = if (prefer_f32_dense_tensors)
        std.math.mul(usize, total_bytes, 2) catch std.math.maxInt(usize)
    else
        total_bytes;

    const host_floor = clampBytes(total_bytes / 3 + gib(1), gib(2), gib(4));
    const backend_floor = clampBytes((promoted_bytes * 2) / 3 + gib(1), gib(8), gib(12));
    const combined_floor = clampBytes(host_floor + backend_floor + gib(1), gib(12), gib(18));

    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    };
}

fn recommendedGpuHostedLargeMultimodalGemmaSharedCacheBudget(
    model_weight_bytes: u64,
    prefer_f32_dense_tensors: bool,
) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(model_weight_bytes, prefer_f32_dense_tensors);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

/// Large dense safetensors models have two independently real residency
/// domains in GPU-hosted execution: the lazily faulted source mapping and the
/// prepared backend weights. The generic GPU cache is intentionally too small
/// for a complete 2B BF16 model, so qualified bundle families need an explicit
/// floor sized from their immutable weight artifact rather than failing midway
/// through the decoder after a partial publication.
pub fn recommendedGpuHostedLargeDenseSafetensorsBudgetFloor(
    model_weight_bytes: u64,
) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return .{};
    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    const host_floor = clampBytes(total_bytes +| mib(256), gib(2), gib(6));
    const backend_floor = clampBytes(total_bytes +| mib(512), gib(4), gib(8));
    const combined_floor = clampBytes(host_floor +| backend_floor +| gib(1), gib(8), gib(14));
    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
    };
}

fn recommendedGpuHostedLargeDenseSafetensorsSharedCacheBudget(
    model_weight_bytes: u64,
) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedLargeDenseSafetensorsBudgetFloor(model_weight_bytes);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

pub fn isBgeM3DenseEncoder(manifest: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    if (!std.mem.eql(u8, manifest.config_model_arch, "xlm-roberta")) return false;
    return switch (arch_config) {
        .bert => |cfg| cfg.model_type == .roberta and
            cfg.vocab_size == 250002 and
            cfg.hidden_size == 1024 and
            cfg.num_hidden_layers == 24 and
            cfg.num_attention_heads == 16 and
            cfg.intermediate_size == 4096 and
            cfg.max_position_embeddings == 8194 and
            cfg.type_vocab_size == 1 and
            cfg.hidden_act == .gelu_exact and
            cfg.layer_norm_eps == 1e-5 and
            cfg.pad_token_id == 1 and
            cfg.position_id_mode == .roberta_padding,
        else => false,
    };
}

/// Unquantized GLiNER2 span checkpoints on a DeBERTa-v3-large backbone
/// (e.g. fastino/GLiNER2.5-Decide, ~1.9 GB F32) exceed the generic 1.5 GiB
/// host cache on 16 GiB machines, like dense BGE-M3.
pub fn isLargeGlinerSpanEncoder(manifest: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    if (manifest.usesGgufWeights()) return false;
    return switch (arch_config) {
        .gliner => |cfg| cfg.hidden_size == 1024 and cfg.num_hidden_layers == 24 and cfg.num_attention_heads == 16,
        .modern_bert => manifest.gliner_architecture == .span and manifest.gliner_span_encoder_family == .modern_bert,
        else => false,
    };
}

fn usesDenseF32EncoderBudget(manifest: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    return isBgeM3DenseEncoder(manifest, arch_config) or isLargeGlinerSpanEncoder(manifest, arch_config);
}

pub fn sessionDirectQuantEnabled(
    direct_quant_enabled: bool,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) bool {
    if (arch_config == .gliner_boundary) {
        return if (manifest.gliner_boundary_bundle) |receipt| switch (receipt.value.precision) {
            .q8_0, .q4_0, .q4_k => true,
            .fp32, .fp16_encoder => false,
        } else false;
    }
    return direct_quant_enabled and arch_config != .embedding_gemma2 and !usesDenseF32EncoderBudget(manifest, arch_config);
}

pub fn recommendedGpuHostedBgeM3BudgetFloor(model_weight_bytes: u64) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0) return .{};
    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));

    // The official dense BGE-M3 checkpoint is F32 and mmap-backed. Metal keeps
    // those source views alive while preparing reusable F16 projection slots
    // (or the explicit F32 rollback), so the host cache must be able to account
    // for the complete artifact. The ordinary GPU defaults already cover the
    // prepared projections and the persistent embedding table on supported
    // machines; these are minimums, not an override of an explicit limit.
    const host_floor = clampBytes(total_bytes +| mib(256), gib(2), gib(4));
    const backend_floor = clampBytes((total_bytes *| 3) / 4 +| gib(1), gib(3), gib(6));
    const combined_floor = clampBytes(host_floor +| backend_floor +| mib(512), gib(6), gib(10));
    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    };
}

pub fn recommendedGpuHostedBgeM3SharedCacheBudget(model_weight_bytes: u64) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedBgeM3BudgetFloor(model_weight_bytes);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

fn ensureGpuHostedSessionAvailable(backend_type: BackendType) !void {
    return switch (backend_type) {
        .metal => ensureMetalHostedSessionAvailable(),
        else => unreachable,
    };
}

pub fn ensureMetalHostedSessionAvailable() !void {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    if (!metal_runtime.metalDeviceAvailable()) return error.MetalDeviceUnavailable;
}

pub const GpuHostedBudgetPolicy = struct {
    budget_floor: runtime.tier.memory.Limits,
    shared_cache_floor: runtime.tier.cache.Budget,
    plan_context: runtime.tier.planner.PlanContext,
    prefer_f32_dense_tensors: bool,
};

pub fn a4bGpuHostedBudgetPolicy(
    config: backend_contracts.A4bInferenceConfig,
) GpuHostedBudgetPolicy {
    const budget: usize = @intCast(config.memory_budget_bytes);
    const kv: usize = @intCast(config.kv_budget_bytes);
    const scratch: usize = @intCast(config.safety_reserve_bytes);
    const weights = budget -| kv -| scratch;
    return .{
        .budget_floor = .{
            .backend_limit_bytes = budget,
            .combined_limit_bytes = budget,
            .kv_limit_bytes = kv,
            .scratch_limit_bytes = scratch,
        },
        .shared_cache_floor = .{
            .host_limit_bytes = weights,
            .backend_limit_bytes = weights,
        },
        .plan_context = .{
            .backend = .gpu,
            .host_budget_bytes = weights,
            .backend_budget_bytes = weights,
        },
        .prefer_f32_dense_tensors = false,
    };
}

pub fn gpuHostedBudgetPolicy(
    backend_type: BackendType,
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    return switch (backend_type) {
        .metal => metalHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode),
        .cuda => sharedGpuHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode),
        else => runtimeGpuHostedBudgetPolicyUnsupported(backend_type),
    };
}

fn runtimeGpuHostedBudgetPolicyUnsupported(backend_type: BackendType) GpuHostedBudgetPolicy {
    std.debug.assert(!backend_type.usesGpuHostedSession());
    return .{
        .budget_floor = runtime.tier.memory.Limits{},
        .shared_cache_floor = runtime.tier.cache.Budget{},
        .plan_context = defaultPlanContextForBackend(.cpu),
        .prefer_f32_dense_tensors = false,
    };
}

fn metalHostedBudgetPolicy(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    return sharedGpuHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode);
}

pub fn lightOnOcr4bBudgetPolicy(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) ?GpuHostedBudgetPolicy {
    if (!manifest.isLightOnOcr3() or manifest.usesGgufWeights() or model_weight_bytes == 0) return null;
    const qualified = switch (arch_config) {
        .gpt => |cfg| cfg.hasQwen35VisionArchitectureContract() and cfg.hidden_size == 2560 and cfg.intermediate_size == 9216 and cfg.num_hidden_layers == 32,
        else => false,
    };
    if (!qualified) return null;
    const weights: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    const host = weights +| mib(256);
    const device = weights +| gib(1);
    var context = defaultPlanContextForBackend(.gpu);
    context.host_budget_bytes = @max(context.host_budget_bytes, host);
    context.backend_budget_bytes = @max(context.backend_budget_bytes, device);
    return .{
        .budget_floor = .{ .host_limit_bytes = host, .backend_limit_bytes = device, .combined_limit_bytes = host +| device +| gib(1) },
        .shared_cache_floor = .{ .host_limit_bytes = host, .backend_limit_bytes = device },
        .plan_context = context,
        .prefer_f32_dense_tensors = false,
    };
}

fn sharedGpuHostedBudgetPolicy(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    // The 4B BF16 artifact exceeds generic cache limits. Account for its
    // mapped source and prepared GPU weights before loading any projections.
    // Explicit serving limits are applied after this automatic floor.
    if (lightOnOcr4bBudgetPolicy(model_weight_bytes, manifest, arch_config)) |policy| return policy;
    const prefer_f32_dense_tensors = shouldPreferGpuHostedF32DenseTensors(arch_config);
    const lazy_quant_budget_floor = if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, manifest, quant_mode, false))
        recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes, quant_mode)
    else
        runtime.tier.memory.Limits{};
    const lazy_quant_shared_cache_floor = if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, manifest, quant_mode, false))
        recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes, quant_mode)
    else
        runtime.tier.cache.Budget{};
    const gemma_budget_floor = if (shouldUseLargeGpuHostedMultimodalGemmaBudgets(model_weight_bytes, arch_config))
        recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(model_weight_bytes, prefer_f32_dense_tensors)
    else
        runtime.tier.memory.Limits{};
    const gemma_shared_cache_floor = if (shouldUseLargeGpuHostedMultimodalGemmaBudgets(model_weight_bytes, arch_config))
        recommendedGpuHostedLargeMultimodalGemmaSharedCacheBudget(model_weight_bytes, prefer_f32_dense_tensors)
    else
        runtime.tier.cache.Budget{};
    const dense_safetensors_budget_floor = if (shouldUseLargeDenseSafetensorsBudgets(model_weight_bytes, manifest, arch_config))
        recommendedGpuHostedLargeDenseSafetensorsBudgetFloor(model_weight_bytes)
    else
        runtime.tier.memory.Limits{};
    const dense_safetensors_shared_cache_floor = if (shouldUseLargeDenseSafetensorsBudgets(model_weight_bytes, manifest, arch_config))
        recommendedGpuHostedLargeDenseSafetensorsSharedCacheBudget(model_weight_bytes)
    else
        runtime.tier.cache.Budget{};
    // Qwen3-VL's external projector is mapped only by an image rerank request,
    // not by the decoder session. Reserve a host envelope for that mapped GGUF
    // and bounded preprocessing before request-time admission; otherwise a
    // correctly loaded Q8 reranker can fail every qualified 2 MP image with a
    // generic 2 GiB host-cache limit.
    const qwen3vl_reranker_gguf_budget_floor = if (shouldUseQwen3VlRerankerGgufBudgets(model_weight_bytes, manifest, arch_config))
        recommendedGpuHostedQwen3VlRerankerGgufBudgetFloor(model_weight_bytes)
    else
        runtime.tier.memory.Limits{};
    const bge_m3_budget_floor = if (usesDenseF32EncoderBudget(manifest, arch_config))
        recommendedGpuHostedBgeM3BudgetFloor(model_weight_bytes)
    else
        runtime.tier.memory.Limits{};
    const bge_m3_shared_cache_floor = if (usesDenseF32EncoderBudget(manifest, arch_config))
        recommendedGpuHostedBgeM3SharedCacheBudget(model_weight_bytes)
    else
        runtime.tier.cache.Budget{};
    const budget_floor = widenLimits(
        widenLimits(
            widenLimits(lazy_quant_budget_floor, gemma_budget_floor),
            widenLimits(dense_safetensors_budget_floor, qwen3vl_reranker_gguf_budget_floor),
        ),
        bge_m3_budget_floor,
    );
    const shared_cache_floor = runtime.tier.cache.Budget{
        .host_limit_bytes = @max(
            @max(
                @max(lazy_quant_shared_cache_floor.host_limit_bytes, gemma_shared_cache_floor.host_limit_bytes),
                dense_safetensors_shared_cache_floor.host_limit_bytes,
            ),
            bge_m3_shared_cache_floor.host_limit_bytes,
        ),
        .backend_limit_bytes = @max(
            @max(
                @max(lazy_quant_shared_cache_floor.backend_limit_bytes, gemma_shared_cache_floor.backend_limit_bytes),
                dense_safetensors_shared_cache_floor.backend_limit_bytes,
            ),
            bge_m3_shared_cache_floor.backend_limit_bytes,
        ),
    };
    const plan_context: runtime.tier.planner.PlanContext = blk: {
        var ctx = defaultPlanContextForBackend(.gpu);
        ctx.host_budget_bytes = @max(ctx.host_budget_bytes, budget_floor.host_limit_bytes);
        ctx.backend_budget_bytes = @max(ctx.backend_budget_bytes, budget_floor.backend_limit_bytes);
        break :blk ctx;
    };
    return .{
        .budget_floor = budget_floor,
        .shared_cache_floor = shared_cache_floor,
        .plan_context = plan_context,
        .prefer_f32_dense_tensors = prefer_f32_dense_tensors,
    };
}

fn shouldUseLargeGpuHostedLazyQuantBudgets(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    quant_mode: GpuHostedQuantExecutionMode,
    eager_dense: bool,
) bool {
    return manifest.usesGgufWeights() and
        !eager_dense and
        quant_mode == .device_native and
        model_weight_bytes > gpuHostedEagerDenseMaxBytes();
}

fn shouldUseLargeGpuHostedMultimodalGemmaBudgets(
    model_weight_bytes: u64,
    arch_config: ArchConfig,
) bool {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .gemma and !cfg.usesMoe() and cfg.isMultimodal(),
        else => false,
    };
}

pub fn shouldUseLargeDenseSafetensorsBudgets(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) bool {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return false;
    if (isLargeGlinerSpanEncoder(manifest, arch_config) and arch_config == .modern_bert)
        return manifest.safetensors_path != null or manifest.safetensors_index_path != null;
    if (!manifest.isQwen3VlRerankerSafetensorsBundle()) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .qwen3_vl and !cfg.usesMoe(),
        else => false,
    };
}

pub fn shouldUseQwen3VlRerankerGgufBudgets(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) bool {
    if (model_weight_bytes == 0 or !manifest.isQwen3VlRerankerGgufBundle()) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .qwen3_vl and !cfg.usesMoe(),
        else => false,
    };
}

/// A Qwen3-VL reranker keeps its decoder resident, then maps the external
/// Q8 projector for image requests. The host bucket must fit both artifacts
/// plus bounded image preprocessing. This is a limit floor rather than a
/// permanent reservation; per-run admission still charges the projector and
/// scratch exactly, and an explicit operator limit remains authoritative.
pub fn recommendedGpuHostedQwen3VlRerankerGgufBudgetFloor(
    model_weight_bytes: u64,
) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0) return .{};
    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    return .{
        .host_limit_bytes = clampBytes(total_bytes +| gib(1), gib(3), gib(6)),
    };
}

pub fn widenLimits(base: runtime.tier.memory.Limits, floor: runtime.tier.memory.Limits) runtime.tier.memory.Limits {
    return .{
        .host_limit_bytes = @max(base.host_limit_bytes, floor.host_limit_bytes),
        .backend_limit_bytes = @max(base.backend_limit_bytes, floor.backend_limit_bytes),
        .combined_limit_bytes = @max(base.combined_limit_bytes, floor.combined_limit_bytes),
        .kv_limit_bytes = @max(base.kv_limit_bytes, floor.kv_limit_bytes),
        .scratch_limit_bytes = @max(base.scratch_limit_bytes, floor.scratch_limit_bytes),
    };
}

pub fn mib(value: usize) usize {
    return value * 1024 * 1024;
}

pub fn gib(value: usize) usize {
    return value * 1024 * 1024 * 1024;
}

fn clampBytes(value: usize, min_value: usize, max_value: usize) usize {
    return @min(@max(value, min_value), max_value);
}

const parseMoeExpertCoord = host_weights.parseMoeExpertCoord;
const projectionMaskForWeightKey = host_weights.projectionMaskForWeightKey;
const parseJinaLoraConfig = host_weights.parseJinaLoraConfig;
const jinaRetrievalAdapterPaths = host_weights.jinaRetrievalAdapterPaths;
const native_mod = @import("../ops/native_compute.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const LazyWeightEntry = native_mod.LazyWeightEntry;
const GpuHostedData = if (false or build_options.enable_metal) gpu_hosted_store_mod.WeightStore else void;

pub const PreparedDeviceWeights = struct {
    architecture: ArchConfig,
    task: SessionTask,
    span_family: manifest_mod.GlinerSpanEncoderFamily,
    weights: GpuHostedData,
    boundary_identity: ?boundary_bundle.Identity,
    decision_identity: ?gliner_decide_qualification.Identity,
    deberta_weight_mirrors: bool,
    jit_scope: MetalJitRouteScope,
    budget_floor: runtime.tier.memory.Limits,
    cache_floor: runtime.tier.cache.Budget,
    owns_weights: bool = true,

    pub fn deinit(self: *PreparedDeviceWeights) void {
        if (!self.owns_weights) return;
        self.owns_weights = false;
        if (comptime !build_options.enable_metal) return;
        backend_factory.closeMetalWeights(self.weights.allocator, &self.weights);
    }
    pub fn takeWeights(self: *PreparedDeviceWeights) GpuHostedData {
        std.debug.assert(self.owns_weights);
        self.owns_weights = false;
        return self.weights;
    }
};

pub fn prepare(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    backend_type: BackendType,
    kernel_jit_config: kernel_jit.Config,
    kernel_jit_load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !PreparedDeviceWeights {
    try kernel_jit_config.validate();
    try metal_runtime.validateMetalJitLoadContext(kernel_jit_config, kernel_jit_load_context);
    try ensureGpuHostedSessionAvailable(backend_type);
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const direct_quant_enabled = directQuantEnabled();

    var loaded_model = try model_definition.LoadedModel.open(allocator, model_path);
    defer loaded_model.deinit();
    var mf = loaded_model.manifest;
    var gpu_jina_lora_adapter: ?*gpu_hosted_store_mod.JinaLoraAdapter = null;
    errdefer if (gpu_jina_lora_adapter) |adapter| adapter.destroy();
    const model_weight_bytes = estimateNativeWeightBytes(allocator, mf) catch 0;

    var arch_config = loaded_model.architecture;
    if (arch_config == .embedding_gemma2 and backend_type != .metal) return error.UnsupportedEmbeddingGemma2Backend;
    if (arch_config == .gliner_boundary and backend_type != .metal) return error.UnsupportedGlinerBoundaryBackend;
    var boundary_identity: ?boundary_bundle.Identity = null;
    var gliner_decision_identity: ?gliner_decide_qualification.Identity = null;
    // BGE-M3 publishes an F32 checkpoint and its dense embedding contract is
    // expected to preserve those weights. Treating SafeTensors F32 storage as
    // a generic direct-quant source silently staged every projection to Q8_0,
    // which was fast but measurably changed the normalized embedding. Keep
    // this exception exact-geometry qualified; all other BERT checkpoints
    // retain the existing direct-quant policy.
    const session_direct_quant_enabled = sessionDirectQuantEnabled(direct_quant_enabled, mf, arch_config);
    const quant_mode = gpuHostedQuantExecutionMode(session_direct_quant_enabled);
    var metal_jit_scope: MetalJitRouteScope = if (build_options.enable_metal)
        metal_runtime.MetalJitRouteScope.none()
    else {};
    var a4b_artifact_qualified = false;
    if (mf.usesGgufWeights()) {
        var report_opt = try buildGgufInspectionReport(allocator, arch_config, loaded_model.weights.?, requiresModernBertSpanClassifier(mf));
        defer if (report_opt) |*report| report.deinit();
        if (report_opt) |report| {
            try ensureGgufInspectionCompatible(report, mf.gguf_path.?);
            if (backend_type == .metal) {
                try ensureMetalGgufInspectionCompatible(report, mf.gguf_path.?);
                a4b_artifact_qualified = qualifiedA4bArtifact(report);
            }
        }
    }

    const a4b_inference = try resolveA4bInferenceConfig(
        arch_config,
        a4b_request,
        a4b_artifact_qualified,
    );
    if (a4b_inference) |config| {
        std.log.info(
            "gemma4_a4b_runtime: mode={s} budget_mb={d} kv_mb={d} safety_mb={d} expert_slots={d} model={s}",
            .{
                @tagName(config.residency_mode),
                config.memory_budget_bytes / (1024 * 1024),
                config.kv_budget_bytes / (1024 * 1024),
                config.safety_reserve_bytes / (1024 * 1024),
                config.expert_cache_slots,
                model_path,
            },
        );
    }

    var lazy_weights = std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry){};
    var tensor_store: ?tensor_store_mod.TensorStore = null;
    var backend_resources_transferred = false;
    errdefer {
        if (!backend_resources_transferred) {
            var it = lazy_weights.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
                if (entry.value_ptr.host_loaded) |*host_loaded| host_loaded.deinit();
                entry.value_ptr.tensor_ref.deinit(allocator);
                allocator.free(entry.key_ptr.*);
            }
            lazy_weights.deinit(allocator);
            if (tensor_store) |store| store.deinit();
        }
    }

    var eager_dense = false;
    const resident_weight_bytes_override: usize = 0;
    const budget_policy = if (a4b_inference) |config|
        a4bGpuHostedBudgetPolicy(config)
    else
        gpuHostedBudgetPolicy(backend_type, model_weight_bytes, mf, arch_config, quant_mode);
    // Boundary artifacts have an exact per-tensor precision contract. Keep
    // original F16 matrix bytes available to the strict resident operations.
    const prefer_f32_dense_tensors = if (arch_config == .gliner_boundary) false else budget_policy.prefer_f32_dense_tensors;
    const budget_floor = budget_policy.budget_floor;
    const shared_cache_floor = budget_policy.shared_cache_floor;
    const plan_context = budget_policy.plan_context;

    const resident_prefix: []const u8 = if (mf.safetensors_path != null or mf.safetensors_index_path != null or mf.gguf_path != null) blk: {
        tensor_store = loaded_model.takeWeights();
        if (arch_config == .modern_bert) {
            if (arch_config.modern_bert.laya) |config| try @import("../models/laya.zig").validateWeights(tensor_store.?, config, arch_config.modern_bert);
        }
        if (arch_config == .gliner_boundary) {
            try validateNativeBoundaryWeights(allocator, mf, arch_config.gliner_boundary, tensor_store.?);
            boundary_identity = try captureBoundaryIdentity(&mf, tensor_store.?);
        }
        if (arch_config == .embedding_gemma2) try embedding_gemma2_arch.validateWeights(allocator, tensor_store.?, arch_config.embedding_gemma2);
        gliner_decision_identity = try captureGlinerDecisionIdentity(&mf, tensor_store.?);
        const source = (try tensor_store.?.weightSource()) orelse return error.NoDenseWeightSource;
        const all_names = try source.listNames(allocator);
        defer allocator.free(all_names);
        try maybeInferGptAttentionLayoutFromStore(allocator, tensor_store.?, all_names, &arch_config);
        const is_gliner_span_modern = mf.gliner_architecture == .span and mf.gliner_span_encoder_family == .modern_bert;
        const is_gliner = arch_config == .gliner or arch_config == .gliner_boundary or is_gliner_span_modern;
        const actual_prefix = detected: {
            if (is_gliner) break :detected "encoder";
            switch (arch_config) {
                .gpt => |cfg| if (cfg.weight_prefix.len != 0) break :detected "",
                else => {},
            }
            var detected_prefix: []const u8 = switch (arch_config) {
                .bert => |cfg| cfg.effectivePrefix(),
                .modern_bert => if (is_gliner_span_modern) "encoder" else "",
                .gliner_modern_bert => "",
                .nomic_bert, .embedding_gemma2 => "",
                .deberta => "deberta",
                else => "",
            };
            for (all_names) |name| {
                if (std.mem.startsWith(u8, name, "bert.")) {
                    detected_prefix = "bert";
                    break;
                } else if (std.mem.startsWith(u8, name, "deberta.")) {
                    detected_prefix = "deberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "roberta.")) {
                    detected_prefix = "roberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "distilbert.")) {
                    detected_prefix = "distilbert";
                    break;
                } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "model.language_model.")) {
                    detected_prefix = "model.language_model";
                    break;
                } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "language_model.")) {
                    detected_prefix = "language_model";
                    break;
                }
            }
            break :detected detected_prefix;
        };
        eager_dense = shouldUseGpuHostedEagerDenseLoad(backend_type, allocator, mf, arch_config);
        if (eager_dense) {
            if (comptime !false) {
                std.log.err("metal backend no longer supports eager dense resident loading", .{});
                return error.EagerDenseLoadRequiresGpuHosted;
            }
            std.log.err("metal backend no longer supports eager dense resident loading", .{});
            return error.EagerDenseLoadUnsupported;
        } else {
            const laya_q8 = try layaQuantizesWeights(arch_config);
            for (all_names) |full_name| {
                if (try appendPackedMoeLazyWeights(allocator, &lazy_weights, tensor_store.?, arch_config, full_name, plan_context)) {
                    continue;
                }
                const base_key = if (is_gliner)
                    glinerBaseWeightKey(full_name)
                else if (actual_prefix.len > 0 and std.mem.startsWith(u8, full_name, actual_prefix) and full_name.len > actual_prefix.len and full_name[actual_prefix.len] == '.')
                    full_name[actual_prefix.len + 1 ..]
                else
                    full_name;
                var key_buf: [256]u8 = undefined;
                const key = try normalizeWeightKey(tensor_store.?.kind(), arch_config, base_key, &key_buf);
                if (lazy_weights.contains(key)) {
                    if (arch_config == .gliner_boundary) return error.DuplicateGlinerBoundaryWeight;
                    continue;
                }
                const expert_coord = parseMoeExpertCoord(key);
                const tensor_ref = try tensor_store.?.describeTensor(allocator, full_name);
                errdefer {
                    var owned_ref = tensor_ref;
                    owned_ref.deinit(allocator);
                }
                const owned_key = try allocator.dupe(u8, key);
                errdefer allocator.free(owned_key);
                try lazy_weights.put(allocator, owned_key, .{
                    .tensor_ref = tensor_ref,
                    .expert_coord = expert_coord,
                    .projection_mask = if (expert_coord != null) projectionMaskForWeightKey(key) else 0,
                    .placement = runtime.tier.planner.planForContext(plan_context, key, tensor_ref.byte_len),
                    .prefer_dense = shouldKeepGpuHostedLazyWeightDense(backend_type, arch_config, key),
                    .quantize_q8_0 = laya_q8 and @import("../models/laya.zig").quantizedLinear(full_name),
                });
            }
            // Gemma 4 audio encoder tensors live in the projector GGUF. Keyed
            // by their own names they load lazily on first use and stay
            // resident like any other weight, so a media request no longer
            // reads and dequantizes the encoder from disk.
            if (try tensor_store_mod.projectorAudioTensorNames(tensor_store.?, allocator)) |projector_names| {
                defer allocator.free(projector_names);
                for (projector_names) |name| {
                    if (lazy_weights.contains(name)) continue;
                    const tensor_ref = try tensor_store.?.describeTensor(allocator, name);
                    try lazy_weights.put(allocator, try allocator.dupe(u8, name), .{
                        .tensor_ref = tensor_ref,
                        .placement = runtime.tier.planner.planForContext(plan_context, name, tensor_ref.byte_len),
                    });
                }
            }
            if (tensor_store.?.kind() != .gguf) {
                try refineArchConfigFromStore(allocator, tensor_store.?, all_names, &arch_config);
            }
            if (backend_type == .metal and std.mem.eql(u8, mf.config_model_arch, "jina_embeddings_v5")) {
                if (try jinaRetrievalAdapterPaths(allocator, model_path)) |paths| {
                    defer allocator.free(paths.config);
                    defer allocator.free(paths.weights);
                    const cfg = try parseJinaLoraConfig(allocator, paths.config);
                    gpu_jina_lora_adapter = try gpu_hosted_store_mod.JinaLoraAdapter.create(allocator, paths.weights, cfg.scale());
                }
            }
        }
        break :blk "";
    } else return error.NoSafetensorsFile;

    const moe_num_experts = switch (arch_config) {
        .gpt => |cfg| cfg.num_local_experts,
        else => 0,
    };
    const residency = if (lazy_weights.count() > 0 and moe_num_experts > 0)
        runtime.moe.residency.SharedResidency.init(
            allocator,
            if (a4b_inference) |config| config.expert_cache_slots else defaultResidentExpertsPerLayer(arch_config),
        )
    else
        null;
    const tier_cache = if (lazy_weights.count() > 0) blk: {
        var budget = runtime.tier.cache.defaultBudgetForBackend(.gpu);
        if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, mf, quant_mode, eager_dense)) {
            const floor = recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes, quant_mode);
            budget.host_limit_bytes = @max(budget.host_limit_bytes, floor.host_limit_bytes);
            budget.backend_limit_bytes = @max(budget.backend_limit_bytes, floor.backend_limit_bytes);
        }
        budget.host_limit_bytes = @max(budget.host_limit_bytes, shared_cache_floor.host_limit_bytes);
        budget.backend_limit_bytes = @max(budget.backend_limit_bytes, shared_cache_floor.backend_limit_bytes);
        break :blk runtime.tier.cache.SharedCache.init(budget);
    } else null;
    errdefer {
        if (!backend_resources_transferred) {
            if (residency) |value| {
                var v = value;
                v.deinit();
            }
        }
    }
    if (comptime build_options.enable_metal) {
        // CUDA/native sessions do not own a Metal provider, so avoid a GGUF
        // catalog scan that cannot affect their dispatch.
        if (backend_type == .metal) {
            includeMetalJitLinearWeightFormats(
                &metal_jit_scope,
                &lazy_weights,
                tensor_store,
                session_direct_quant_enabled,
                arch_config,
                metalJitUsesExactProfileScope(kernel_jit_config),
            );
        }
    }

    const task = sessionTaskForModelType(mf.model_type, override);
    const weights: GpuHostedData = .{
        .allocator = allocator,
        .resident_weight_estimate_bytes = resident_weight_bytes_override,
        .prefix = resident_prefix,
        .lazy_weights = lazy_weights,
        .tensor_store = tensor_store,
        .moe_num_experts = @intCast(moe_num_experts),
        .a4b_inference = a4b_inference,
        .residency = residency,
        .tier_cache = tier_cache,
        .allow_direct_quant = session_direct_quant_enabled,
        .quant_execution_mode = quant_mode,
        .prefer_f32_dense_tensors = prefer_f32_dense_tensors,
        .allow_immutable_f32_weight_borrow = sessionEnablesImmutableF32WeightBorrow(backend_type, std.meta.activeTag(arch_config), task, mf.gliner_span_encoder_family),
        .prefer_immutable_gliner_f32_mps = sessionPrefersImmutableGlinerF32Mps(backend_type, std.meta.activeTag(arch_config), task),
        .mirror_kv_to_manager = false,
        .jina_lora_adapter = gpu_jina_lora_adapter,
    };
    backend_resources_transferred = true;
    gpu_jina_lora_adapter = null;
    return .{
        .architecture = arch_config,
        .task = task,
        .span_family = mf.gliner_span_encoder_family,
        .weights = weights,
        .boundary_identity = boundary_identity,
        .decision_identity = gliner_decision_identity,
        .deberta_weight_mirrors = sessionEnablesDebertaRerankerWeightMirrors(mf.model_type, std.meta.activeTag(arch_config), task),
        .jit_scope = metal_jit_scope,
        .budget_floor = budget_floor,
        .cache_floor = shared_cache_floor,
    };
}

/// Only after the inventory occupies its final stable address. Startup JIT
/// authority is borrowed for this call and never stored on a published session.
pub fn finalizeStableStorage(allocator: std.mem.Allocator, weights: *GpuHostedData, identity: ?boundary_bundle.Identity, jit: backend_factory.MetalJitOptions) !void {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    if (identity) |value| if (value.precision == .fp32) {
        weights.boundary_resident = try boundary_resident.Owner.create(allocator, value);
    };
    try backend_factory.prepareMetalProvider(allocator, weights, jit);
    try backend_factory.initializeMetalWeightPrefetch(allocator, weights, !disablePrefetchWorkerDebug());
}

const boundary_resident = @import("../ops/gliner_boundary_resident.zig");

fn disablePrefetchWorkerDebug() bool {
    return platform.env.getenvBool("TERMITE_DISABLE_PREFETCH_WORKER");
}
