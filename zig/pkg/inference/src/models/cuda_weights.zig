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

//! CUDA artifact admission and prepared device inventory. Owns preflight,
//! upload ordering and rollback before a session publishes a stable compute
//! owner. Architecture execution and serving remain outside this boundary.
const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const backend_factory = @import("../backends/compute_factory.zig");
const host_weights = @import("host_weights.zig");
const device_weights = @import("device_weights.zig");
const model_definition = @import("../architectures/model_definition.zig");
const manifest_mod = @import("manifest.zig");
const kernel_jit = @import("../graph/kernel_jit.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const runtime = @import("../runtime/root.zig");
const qwen35_execution = @import("../architectures/qwen35_execution.zig");
const boundary_bundle = @import("gliner_boundary_bundle.zig");
const gliner_decide_qualification = @import("gliner_decide_qualification.zig");
const LoadedWeight = @import("weight_source.zig").LoadedWeight;
const Tensor = @import("../backends/tensor.zig").Tensor;
const gpt_mod = @import("gpt.zig");
const c_file = @import("../util/c_file.zig");
const cuda_load_plan = @import("../ops/cuda/load_plan.zig");
const cuda_compute_mod = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const a4b_prepared_pack_mod = @import("../ops/cuda/a4b_prepared_pack.zig");
const ArchConfig = model_definition.ArchConfig;
const SessionTask = @import("../backends/session.zig").ModelContract.Task;
const TaskOverride = SessionTask;
const GgufInspectionReport = host_weights.GgufInspectionReport;
const requiresModernBertSpanClassifier = host_weights.requiresModernBertSpanClassifier;
const inspectGgufModelForListing = host_weights.inspectGgufModelForListing;
const sessionTaskForModelType = device_weights.sessionTaskForModelType;
const qualifiedA4bArtifact = device_weights.qualifiedA4bArtifact;
const resolveA4bGptInferenceConfig = device_weights.resolveA4bGptInferenceConfig;
const estimateNativeWeightBytes = device_weights.estimateNativeWeightBytes;
const mib = device_weights.mib;
const BackendType = @import("../backends/backends.zig").BackendType;

pub fn ensureCudaGlinerModernBertGgufEligible(
    manifest: manifest_mod.ModelManifest,
    report: GgufInspectionReport,
) !void {
    if (!requiresModernBertSpanClassifier(manifest)) return;
    for (report.all_tensor_types) |entry| {
        if (!entry.tensor_type.isQuantized()) continue;
        switch (entry.tensor_type) {
            .known => |kind| if (kind != .Q8_0) return error.UnsupportedCudaQuantizedModernBertSpan,
            else => return error.UnsupportedCudaQuantizedModernBertSpan,
        }
    }
}

pub fn resolveCudaA4bInferenceConfigForModelListing(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    manifest: manifest_mod.ModelManifest,
    request: ?backend_contracts.A4bInferenceRequest,
) !?backend_contracts.A4bInferenceConfig {
    var report_opt = try inspectGgufModelForListing(allocator, model_path, manifest);
    defer if (report_opt) |*report| report.deinit();
    const report = report_opt orelse {
        if (request != null) return error.A4bUnsupportedArtifact;
        return null;
    };
    const config = report.gpt_config orelse {
        if (request != null) return error.A4bUnsupportedGeometry;
        return null;
    };
    return resolveCudaA4bGptInferenceConfig(config, request, qualifiedA4bArtifact(report));
}

pub fn resolveCudaA4bGptInferenceConfig(
    gpt_config: gpt_mod.Config,
    request: ?backend_contracts.A4bInferenceRequest,
    artifact_qualified: bool,
) !?backend_contracts.A4bInferenceConfig {
    // Apply the CUDA defaults (16 GiB envelope, resident placement) before
    // the geometry pre-pass. Validating the raw request first rejects an
    // explicit `residency_mode: resident` without `memory_budget_mb` against
    // the streamed floor, so the documented default could never apply.
    const effective = backend_contracts.effectiveCudaA4bRequest(request);
    const detected = (try resolveA4bGptInferenceConfig(
        gpt_config,
        if (request != null) effective else null,
        artifact_qualified,
    )) orelse return null;
    return try backend_contracts.buildCudaA4bInferenceConfig(request, detected.geometry);
}

pub fn cudaProfileForArch(
    arch_config: ArchConfig,
    task: SessionTask,
    model_manifest: *const manifest_mod.ModelManifest,
) ?CudaCapabilityProfile {
    return switch (arch_config) {
        .clip, .clap => .clipclap,
        .bert => .bert_encoder,
        .modern_bert => |cfg| blk: {
            if (cfg.num_attention_heads == 0 or cfg.hidden_size % cfg.num_attention_heads != 0) break :blk null;
            const head_dim = cfg.hidden_size / cfg.num_attention_heads;
            if (task == .extractor and model_manifest.gliner_architecture == .span and
                model_manifest.gliner_span_encoder_family == .modern_bert and
                cfg.max_position_embeddings > 0 and cfg.max_position_embeddings <= 8192 and
                (head_dim == 64 or head_dim == 128)) break :blk .gliner25_modern_bert;
            if (head_dim <= 128 and cfg.laya != null and cfg.laya.?.format == .laya and
                !cfg.laya.?.packing.enabled() and cfg.laya.?.max_len <= 512) break :blk .laya;
            break :blk null;
        },
        .deberta => .deberta_reranker,
        .gliner => .gliner2,
        .gliner_boundary => |cfg| if (cfg.encoder.family == .deberta) .gliner25_boundary else null,
        .gliner_modern_bert => |cfg| if (cfg.num_attention_heads > 0 and cfg.max_position_embeddings <= 8192 and
            (cfg.hidden_size / cfg.num_attention_heads == 64 or cfg.hidden_size / cfg.num_attention_heads == 128)) .gliner25_modern_bert else null,
        .florence => .florence2,
        .gpt => |cfg| switch (cfg.family) {
            .gemma => .gemma4,
            .qwen3 => if (task == .generic and model_manifest.isLastTokenDecoderEmbedder()) .qwen3_embedding else null,
            .qwen3_vl => if (task == .generic and
                model_manifest.model_type == .generator and
                model_manifest.isQwen3VlGenerationSafetensorsBundle())
                .qwen3_vl_generation
            else
                null,
            .qwen3_5 => if (task == .generic and model_manifest.isLightOnOcr3() and
                cfg.hasQwen35VisionArchitectureContract() and
                cfg.qwen35_linear_key_head_dim == 128 and cfg.qwen35_linear_value_head_dim == 128 and
                cfg.qwen35_linear_conv_kernel_dim == 4)
                .lightonocr3
            else
                null,
            else => null,
        },
        .embedding_gemma2 => .embedding_gemma2,
        else => null,
    };
}

pub fn cudaSupportsArch(arch_config: ArchConfig, model_manifest: *const manifest_mod.ModelManifest) bool {
    return cudaProfileForArch(arch_config, .generic, model_manifest) != null;
}

pub fn glinerCudaBudgetFloor(arch_config: ArchConfig, encoded_bytes: u64) !runtime.tier.memory.Limits {
    if (encoded_bytes == 0) return .{};
    const cfg = switch (arch_config) {
        .modern_bert, .gliner_modern_bert => |config| config,
        else => return .{},
    };
    const weights = std.math.cast(usize, encoded_bytes) orelse return error.ResourceLimitExceeded;
    const scratch = try layaCudaWorkspace(1, 16384, 16384, cfg.hidden_size, cfg.intermediate_size);
    const host = try std.math.add(usize, weights, mib(256));
    const backend = try std.math.add(usize, try estimateBackendWeightResidencyBytes(.cuda, weights), try std.math.add(usize, scratch, mib(64)));
    return .{
        .host_limit_bytes = host,
        .backend_limit_bytes = backend,
        .combined_limit_bytes = try std.math.add(usize, host, backend),
        .scratch_limit_bytes = scratch,
    };
}

pub fn layaCudaWorkspace(batch: usize, sequence: usize, options: usize, hidden: usize, intermediate: usize) !usize {
    const mul = std.math.mul;
    const add = std.math.add;
    const encoder_width = try add(usize, try mul(usize, hidden, 20), try mul(usize, intermediate, 6));
    const head_width = try mul(usize, hidden, 32);
    const activations = try mul(usize, try mul(usize, batch, sequence), @max(encoder_width, head_width));
    const scorer = try mul(usize, try mul(usize, batch, options), try mul(usize, hidden, 8));
    // Include integer staging, action features, and allocator alignment headroom.
    return add(usize, try mul(usize, try add(usize, activations, scorer), 4), 1024 * 1024);
}

pub fn estimateBackendWeightResidencyBytes(
    backend: BackendType,
    encoded_bytes: usize,
) !usize {
    return switch (backend) {
        .cuda => cuda_load_plan.estimateEncodedArtifactDeviceBytes(encoded_bytes),
        .metal => encoded_bytes,
        .native, .wasm => 0,
        .pjrt => error.UnsupportedBackend,
    };
}

const CudaResidentUpload = struct {
    key: []const u8,
    loaded: *const LoadedWeight,
    mmap_bytes: ?[]const u8,
    mmap_base: ?usize,
    mmap_offset: ?usize,
};

fn loadedWeightMmapSpan(loaded: *const LoadedWeight) ?struct { bytes: []const u8, base: usize, offset: usize } {
    if (loaded.quantized_storage) |storage| {
        if (storage.raw_mmap_backed) {
            if (storage.raw_mmap_source_bytes) |full| {
                if (c_file.mappedSliceOffset(full, storage.raw_bytes)) |offset| {
                    return .{ .bytes = storage.raw_bytes, .base = @intFromPtr(full.ptr), .offset = offset };
                }
            }
        }
    }
    if (loaded.tensor.mmap_source_bytes) |full| {
        if (c_file.mappedSliceOffset(full, loaded.tensor.data)) |offset| {
            return .{ .bytes = loaded.tensor.data, .base = @intFromPtr(full.ptr), .offset = offset };
        }
    }
    return null;
}

fn cudaResidentUploadLessThan(_: void, lhs: CudaResidentUpload, rhs: CudaResidentUpload) bool {
    if (lhs.mmap_offset) |lhs_offset| {
        if (rhs.mmap_offset) |rhs_offset| {
            const lhs_base = lhs.mmap_base.?;
            const rhs_base = rhs.mmap_base.?;
            if (lhs_base != rhs_base) return lhs_base < rhs_base;
            if (lhs_offset != rhs_offset) return lhs_offset < rhs_offset;
        } else return true;
    } else if (rhs.mmap_offset != null) return false;
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

test "CUDA resident uploads follow mmap offsets before heap weights" {
    var mapped: [64]u8 = @splat(0);
    var loaded: LoadedWeight = undefined;
    const base = @intFromPtr(mapped[0..].ptr);
    var uploads = [_]CudaResidentUpload{
        .{ .key = "heap", .loaded = &loaded, .mmap_bytes = null, .mmap_base = null, .mmap_offset = null },
        .{ .key = "later", .loaded = &loaded, .mmap_bytes = mapped[40..48], .mmap_base = base, .mmap_offset = 40 },
        .{ .key = "earlier", .loaded = &loaded, .mmap_bytes = mapped[8..16], .mmap_base = base, .mmap_offset = 8 },
    };
    std.mem.sort(CudaResidentUpload, &uploads, {}, cudaResidentUploadLessThan);
    try std.testing.expectEqualStrings("earlier", uploads[0].key);
    try std.testing.expectEqualStrings("later", uploads[1].key);
    try std.testing.expectEqualStrings("heap", uploads[2].key);
}

/// CUDA preference training builds only the Gemma text graph.  Unified
/// checkpoints also contain modality towers that are unreachable from that
/// graph and must not consume scarce device residency.  Shared-tail K/V
/// projections are omitted for the same reason: those layers consume their
/// donor layer's cache in the graph.
fn gemma4CudaTrainingUsesWeight(arch_config: ArchConfig, name: []const u8) bool {
    const config = switch (arch_config) {
        .gpt => |value| value,
        else => return false,
    };
    if (config.family != .gemma) return false;
    const relative_name = if (config.weight_prefix.len == 0)
        if (std.mem.startsWith(u8, name, "model.")) name["model.".len..] else name
    else blk: {
        if (std.mem.eql(u8, name, "lm_head.weight")) return true;
        if (name.len <= config.weight_prefix.len or
            !std.mem.startsWith(u8, name, config.weight_prefix) or
            name[config.weight_prefix.len] != '.') return false;
        break :blk name[config.weight_prefix.len + 1 ..];
    };

    if (std.mem.startsWith(u8, relative_name, "layers.")) {
        const after_layers = relative_name["layers.".len..];
        if (std.mem.indexOfScalar(u8, after_layers, '.')) |layer_end| {
            const layer = std.fmt.parseInt(usize, after_layers[0..layer_end], 10) catch return false;
            const suffix = after_layers[layer_end + 1 ..];
            if (config.layerSharesKv(layer) and
                (std.mem.eql(u8, suffix, "self_attn.k_proj.weight") or
                    std.mem.eql(u8, suffix, "self_attn.v_proj.weight"))) return false;
            if (config.layerOmitsVProj(layer) and
                std.mem.eql(u8, suffix, "self_attn.v_proj.weight")) return false;
        }
    }

    if (config.weight_prefix.len == 0) return true;
    if (std.mem.eql(u8, name, "lm_head.weight")) return true;
    return name.len > config.weight_prefix.len and
        std.mem.startsWith(u8, name, config.weight_prefix) and
        name[config.weight_prefix.len] == '.';
}

test "gemma4 CUDA text training excludes unreachable unified modality weights" {
    const unified = ArchConfig{ .gpt = .{
        .family = .gemma,
        .weight_prefix = "model.language_model",
    } };
    try std.testing.expect(gemma4CudaTrainingUsesWeight(unified, "model.language_model.embed_tokens.weight"));
    try std.testing.expect(gemma4CudaTrainingUsesWeight(unified, "model.language_model.layers.41.mlp.down_proj.weight"));
    try std.testing.expect(gemma4CudaTrainingUsesWeight(unified, "lm_head.weight"));
    try std.testing.expect(!gemma4CudaTrainingUsesWeight(unified, "model.audio_tower.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(!gemma4CudaTrainingUsesWeight(unified, "model.vision_tower.vision_model.embeddings.patch_embedding.weight"));
    try std.testing.expect(!gemma4CudaTrainingUsesWeight(unified, "model.multi_modal_projector.mm_input_projection_weight"));

    const text_only = ArchConfig{ .gpt = .{ .family = .gemma } };
    try std.testing.expect(gemma4CudaTrainingUsesWeight(text_only, "model.layers.0.self_attn.q_proj.weight"));
}

fn uploadGlinerCudaWeight(compute: anytype, name: []const u8, loaded: *const LoadedWeight, precision: GlinerCudaPrecision) !void {
    // Packed-only weights have no dense tensor shape. Check the requested
    // policy before the rank gate so explicit mixed precision cannot silently
    // execute the default Q8 route or change only attention precision.
    if (precision != .auto and precision != .fp32 and (loaded.quantized or loaded.quantized_storage != null))
        return error.UnsupportedGlinerCudaPrecision;
    // Preserve embeddings, normalization, biases and the decision head in FP32.
    // Only encoder matrix products use lower precision; accumulation and all
    // externally visible logits remain FP32.
    if (precision == .auto or precision == .fp32 or loaded.tensor.shape.len != 2 or
        !std.mem.startsWith(u8, name, "model.layers.") or !std.mem.endsWith(u8, name, ".weight"))
        return compute.insertWeightFromLoaded(try compute.allocator.dupe(u8, name), loaded);
    if (loaded.quantized or loaded.quantized_storage != null or loaded.tensor.dtype != .f32)
        return error.UnsupportedGlinerCudaPrecision;
    if (precision == .bf16) return compute.insertBf16WeightFromF32Tensor(try compute.allocator.dupe(u8, name), &loaded.tensor);
    const values = loaded.tensor.asFloat32();
    const half = try compute.allocator.alloc(f16, values.len);
    defer compute.allocator.free(half);
    for (values, half) |value, *dest| {
        dest.* = @floatCast(value);
        if (!std.math.isFinite(dest.*)) return error.UnsupportedGlinerCudaPrecision;
    }
    var tensor = loaded.tensor;
    tensor.dtype = .f16;
    tensor.data = std.mem.sliceAsBytes(half);
    return compute.insertF16WeightFromTensor(try compute.allocator.dupe(u8, name), &tensor);
}

test "GLiNER Decide CUDA mixed precision rejects packed-only weights before upload" {
    const Compute = struct {
        allocator: std.mem.Allocator,
        fn insertWeightFromLoaded(_: *@This(), _: []const u8, _: *const LoadedWeight) !void {
            return error.UnexpectedUpload;
        }
        fn insertBf16WeightFromF32Tensor(_: *@This(), _: []const u8, _: *const Tensor) !void {
            return error.UnexpectedUpload;
        }
        fn insertF16WeightFromTensor(_: *@This(), _: []const u8, _: *const Tensor) !void {
            return error.UnexpectedUpload;
        }
    };
    var compute = Compute{ .allocator = std.testing.allocator };
    var loaded = LoadedWeight{
        .tensor = .{ .data = &.{}, .dtype = .f32, .shape = &.{}, .name = "", .allocator = std.testing.allocator, .owns_data = false, .owns_shape = false },
        .quantized = true,
    };
    for ([_]GlinerCudaPrecision{ .fp16, .bf16 }) |precision|
        try std.testing.expectError(error.UnsupportedGlinerCudaPrecision, uploadGlinerCudaWeight(&compute, "model.layers.0.attn.Wqkv.weight", &loaded, precision));
}

pub const CudaCapabilityProfile = if (build_options.enable_cuda) cuda_compute_mod.CapabilityProfile else enum {
    clipclap,
    bert_encoder,
    laya,
    deberta_reranker,
    gliner2,
    gliner2_training,
    gliner25_boundary,
    gliner25_modern_bert,
    florence2,
    gemma4,
    gemma4_training,
    qwen3_embedding,
    embedding_gemma2,
    qwen3_vl_generation,
    lightonocr3,
};
pub const GlinerCudaPrecision = enum { auto, fp32, fp16, bf16 };
pub const PreparedCudaWeights = struct {
    allocator: std.mem.Allocator,
    compute: ?*if (build_options.enable_cuda) cuda_compute_mod.CudaCompute else void,
    architecture: ArchConfig,
    recurrent_execution: ?qwen35_execution.Decision,
    task: SessionTask,
    span_encoder_family: @FieldType(manifest_mod.ModelManifest, "gliner_span_encoder_family"),
    decision_identity: ?gliner_decide_qualification.Identity,
    boundary_identity: ?boundary_bundle.Identity,
    budget_floor: runtime.tier.memory.Limits,

    pub fn deinit(self: *PreparedCudaWeights) void {
        const compute = self.compute orelse return;
        self.compute = null;
        backend_factory.closeCuda(self.allocator, compute);
    }

    pub fn takeCompute(self: *PreparedCudaWeights) *if (build_options.enable_cuda) cuda_compute_mod.CudaCompute else void {
        const compute = self.compute orelse unreachable;
        self.compute = null;
        return compute;
    }
};

pub fn prepare(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
    required_profile_override: ?CudaCapabilityProfile,
    gliner_precision: GlinerCudaPrecision,
) !PreparedCudaWeights {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    const load_started_ns = platform.time.monotonicNs();
    defer std.log.info("inference_model_load backend=cuda phase=total elapsed_ms={d}", .{(platform.time.monotonicNs() -| load_started_ns) / std.time.ns_per_ms});
    try config.validate();
    if (config.mode.failClosed() and !load_context.allowsQualification()) {
        return error.KernelJitRequiredDynamicLoad;
    }

    var model_manifest = try manifest_mod.loadFromDir(allocator, model_path);
    var manifest_owned = true;
    defer if (manifest_owned) model_manifest.deinit();
    try model_manifest.requireRecognizedGlinerArchitecture();
    if (model_manifest.embedding_style == .embedding_gemma2)
        try cuda_compute_mod.preflightProfileDevice(.embedding_gemma2);
    if (model_manifest.gliner_boundary_bundle) |receipt| {
        // This CUDA profile consumes canonical FP32 boundary weights. A
        // reduced artifact needs its own qualified storage/compute profile;
        // converting it silently would disagree with the executor identity.
        if (receipt.value.precision != .fp32) return error.UnsupportedGlinerCudaPrecision;
    }
    if (model_manifest.usesGgufWeights() and
        model_manifest.gliner_architecture == .span and
        model_manifest.gliner_span_encoder_family == .modern_bert)
    {
        var report_opt = try inspectGgufModelForListing(allocator, model_path, model_manifest);
        defer if (report_opt) |*report| report.deinit();
        if (report_opt) |report| try ensureCudaGlinerModernBertGgufEligible(model_manifest, report);
    }
    const a4b_inference = if (required_profile_override == null)
        try resolveCudaA4bInferenceConfigForModelListing(
            allocator,
            model_path,
            model_manifest,
            a4b_request,
        )
    else
        null;
    if (a4b_inference) |a4b| {
        if (a4b.residency_mode != .resident)
            return error.A4bCudaStreamingUnsupported;

        // Required, stale, and malformed deployment packs fail before native
        // session construction or any CUDA allocation. The hot loader later
        // checks its exact source inventory against the canonical GGUF catalog.
        if (a4b.prepared_pack != .off) {
            const source_artifact_path = model_manifest.gguf_path orelse
                return error.A4bCudaPackedStoreUnavailable;
            const installed = a4b_prepared_pack_mod.preflightInstalled(
                allocator,
                model_path,
                source_artifact_path,
                .{
                    .moe_layer_count = a4b.geometry.moe_layer_count,
                    .expert_count = a4b.geometry.expert_count,
                    .top_k = a4b.geometry.top_k,
                    .hidden_size = a4b.geometry.hidden_size,
                    .expert_intermediate_size = a4b.geometry.expert_intermediate_size,
                    .encoded_expert_bytes = a4b.geometry.encoded_expert_bytes,
                },
            ) catch |err| switch (a4b.prepared_pack) {
                .auto => blk: {
                    std.log.warn(
                        "cuda_a4b: ignoring unusable optional prepared pack during early preflight error={s}; using canonical GGUF",
                        .{@errorName(err)},
                    );
                    break :blk false;
                },
                .required => return err,
                .off => unreachable,
            };
            if (!installed and a4b.prepared_pack == .required)
                return error.A4bPreparedPackRequired;
        }
    }

    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    if (debug_cuda_session) std.log.info("cuda-session: prepare host weights start path={s}", .{model_path});
    const host_prepare_started_ns = platform.time.monotonicNs();
    manifest_owned = false; // fromOwnedManifest consumes it even on failure.
    var loaded_model = try model_definition.LoadedModel.fromOwnedManifest(allocator, model_path, model_manifest);
    defer loaded_model.deinit();
    var host_weights_owner = try host_weights.prepare(allocator, model_path, &loaded_model, .{ .intent = .device_upload });
    defer host_weights_owner.deinit();
    const source_task = sessionTaskForModelType(loaded_model.manifest.model_type, override);
    std.log.info("inference_model_load backend=cuda phase=host_prepare elapsed_ms={d}", .{(platform.time.monotonicNs() -| host_prepare_started_ns) / std.time.ns_per_ms});
    if (debug_cuda_session) std.log.info("cuda-session: prepare host weights done path={s}", .{model_path});
    if (a4b_inference) |a4b| {
        try cuda_compute_mod.CudaCompute.preflightA4bPreparedPackInventory(
            allocator,
            &host_weights_owner.weights,
            a4b,
            model_path,
            model_manifest.gguf_path orelse return error.A4bCudaPackedStoreUnavailable,
        );
    }
    const architecture_profile = cudaProfileForArch(
        host_weights_owner.architecture,
        source_task,
        &model_manifest,
    ) orelse return error.UnsupportedCudaArchitecture;
    if (required_profile_override == .gemma4_training) {
        const training_config = switch (host_weights_owner.architecture) {
            .gpt => |value| value,
            else => return error.UnsupportedCudaArchitecture,
        };
        if (training_config.family != .gemma) return error.UnsupportedCudaArchitecture;
        if (training_config.usesMoe()) return error.UnsupportedGemmaMoeTraining;
    }
    const cuda_profile = required_profile_override orelse architecture_profile;
    if (gliner_precision != .auto and gliner_precision != .fp32 and cuda_profile != .gliner25_modern_bert)
        return error.UnsupportedGlinerCudaPrecision;
    const jit_scope = cuda_compute_mod.kernelJitRouteScopeForLoadedWeights(
        cuda_profile,
        &host_weights_owner.weights.resident_weights,
    );

    if (debug_cuda_session) std.log.info("cuda-session: init cuda compute start path={s}", .{model_path});
    const device_init_started_ns = platform.time.monotonicNs();
    const cuda_compute = try backend_factory.createCuda(
        allocator,
        config,
        cuda_profile,
        jit_scope,
        load_context,
    );
    errdefer backend_factory.closeCuda(allocator, cuda_compute);
    std.log.info("inference_model_load backend=cuda phase=device_init elapsed_ms={d}", .{(platform.time.monotonicNs() -| device_init_started_ns) / std.time.ns_per_ms});
    if (debug_cuda_session) std.log.info("cuda-session: init cuda compute done path={s}", .{model_path});

    if (debug_cuda_session) std.log.info("cuda-session: require profile {s}", .{@tagName(cuda_profile)});
    try cuda_compute.requireProfile(cuda_profile);
    cuda_compute.strict_f32_weights = cuda_profile == .laya or cuda_profile == .gliner25_boundary or cuda_profile == .gliner25_modern_bert;
    cuda_compute.gliner_encoder_attention = cuda_profile == .gliner25_modern_bert;
    cuda_compute.gliner_boundary_inference = cuda_profile == .gliner25_boundary;
    cuda_compute.gliner_mixed_attention = cuda_compute.gliner_encoder_attention and (gliner_precision == .fp16 or gliner_precision == .bf16);
    cuda_compute.gliner_q8_f16_mirrors = cuda_compute.gliner_encoder_attention and
        host_weights_owner.decision_identity != null and !host_weights_owner.decision_identity.?.inventory.all_f32 and
        platform.env.getenvBool("ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS");
    cuda_compute.gliner_q8_f16_attention = cuda_compute.gliner_q8_f16_mirrors and
        platform.env.getenvBoolDefault("ANTFLY_CUDA_GLINER_1B_Q8_F16_ATTENTION", true);
    cuda_compute.laya_optimizations = (cuda_profile == .laya or cuda_profile == .gliner25_modern_bert) and platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_OPTIMIZATIONS", true);
    cuda_compute.laya_fusion = cuda_compute.laya_optimizations and platform.env.getenvBoolDefault("ANTFLY_CUDA_LAYA_FUSION", true);
    if (a4b_inference != null and !cuda_compute.kernels.hasGemma4A4BResidentQ4_0Primitives()) {
        return error.A4bCudaKernelUnavailable;
    }
    cuda_compute.a4b_inference = a4b_inference;
    var resident_uploads: std.ArrayListUnmanaged(CudaResidentUpload) = .empty;
    defer resident_uploads.deinit(allocator);
    var it = host_weights_owner.weights.resident_weights.iterator();
    while (it.next()) |entry| {
        if (required_profile_override == .gemma4_training and
            !gemma4CudaTrainingUsesWeight(host_weights_owner.architecture, entry.key_ptr.*))
        {
            continue;
        }
        const span = loadedWeightMmapSpan(entry.value_ptr);
        try resident_uploads.append(allocator, .{
            .key = entry.key_ptr.*,
            .loaded = entry.value_ptr,
            .mmap_bytes = if (span) |mapped| mapped.bytes else null,
            .mmap_base = if (span) |mapped| mapped.base else null,
            .mmap_offset = if (span) |mapped| mapped.offset else null,
        });
    }
    std.mem.sort(CudaResidentUpload, resident_uploads.items, {}, cudaResidentUploadLessThan);
    const dense_upload_start_ns = platform.time.monotonicNs();
    var dense_mmap_weight_count: usize = 0;
    if (a4b_inference != null) {
        for (resident_uploads.items) |upload| {
            if (upload.mmap_offset != null) dense_mmap_weight_count += 1;
        }
        std.log.info("cuda_a4b: dense upload start weights={d} mmap_weights={d} access=offset_sorted_sequential", .{
            resident_uploads.items.len,
            dense_mmap_weight_count,
        });
    }
    for (resident_uploads.items) |upload| {
        if (upload.mmap_bytes) |bytes| c_file.MmapRegion.adviseBytesSequential(bytes);
        try uploadGlinerCudaWeight(cuda_compute, upload.key, upload.loaded, gliner_precision);
    }
    std.log.info("inference_model_load backend=cuda phase=weight_upload weights={d} elapsed_ms={d}", .{ resident_uploads.items.len, (platform.time.monotonicNs() -| dense_upload_start_ns) / std.time.ns_per_ms });
    if (a4b_inference != null) {
        cuda_compute.noteA4bDenseUpload(
            platform.time.monotonicNs() -| dense_upload_start_ns,
            resident_uploads.items.len,
            dense_mmap_weight_count,
        );
        std.log.info("cuda_a4b: dense upload complete weights={d} elapsed_ms={d}", .{
            resident_uploads.items.len,
            (platform.time.monotonicNs() -| dense_upload_start_ns) / std.time.ns_per_ms,
        });
    }
    if (a4b_inference != null) {
        try cuda_compute.loadA4bResidentFromHostStore(
            &host_weights_owner.weights,
            model_path,
            model_manifest.gguf_path orelse return error.A4bCudaPackedStoreUnavailable,
        );
        if (!a4b_inference.?.drop_host_cache_after_load) {
            if (host_weights_owner.weights.tensor_store) |store| {
                store.preserveFileCacheOnDeinit();
                std.log.info("cuda_a4b: retaining clean checkpoint pages for shared-cache reloads", .{});
            }
        }
    }
    if (debug_cuda_session) std.log.info("cuda-session: uploaded resident weights count={d}", .{resident_uploads.items.len});
    if (cuda_compute.gliner_q8_f16_mirrors) {
        std.log.info("cuda: attached {d} decoded Q8 FP16 projection mirrors; raw Q8 and F32 heads retained; ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS=0 disables", .{cuda_compute.gliner_boundary_f16_mirrors.count()});
    }
    const upload_stats = cuda_compute.snapshotStats();
    if (upload_stats.bf16_mirror_weight_count > 0) {
        // The default-on prefill mirrors trade device memory for cuBLASLt
        // prefill speed; say so at load time so an OOM investigation can see
        // the cost and the switch without reading source.
        std.log.info(
            "cuda: attached {d} BF16 prefill weight mirrors (+{d} MiB device memory); ANTFLY_INFERENCE_CUDA_BERT_Q4_0_BF16_PREFILL=0 disables",
            .{ upload_stats.bf16_mirror_weight_count, upload_stats.bf16_mirror_weight_bytes / (1024 * 1024) },
        );
    }

    const gliner_budget = if (host_weights_owner.architecture == .gliner_modern_bert or
        (host_weights_owner.architecture == .modern_bert and loaded_model.manifest.gliner_span_encoder_family == .modern_bert))
        try glinerCudaBudgetFloor(host_weights_owner.architecture, try estimateNativeWeightBytes(allocator, model_manifest))
    else
        runtime.tier.memory.Limits{};
    // Artifact preparation has never been tokenizer-sealed. Publication must
    // independently seal the target owner after parsing its tokenizer snapshot.
    var resolved_arch_config = host_weights_owner.architecture;
    const recurrent_execution: ?qwen35_execution.Decision = if (resolved_arch_config == .gpt and
        resolved_arch_config.gpt.isQwen35() and resolved_arch_config.gpt.qwen35_has_linear_attention)
    blk: {
        const decision = qwen35_execution.apply(&resolved_arch_config.gpt, cuda_compute.ctx.info.compute_major, qwen35_execution.cudaCoreEnabled());
        const placement_format = "inference recurrent placement backend=cuda placement={s} reason={s} compute={d}.{d}";
        const placement_args = .{
            @tagName(decision.placement), @tagName(decision.reason), cuda_compute.ctx.info.compute_major, cuda_compute.ctx.info.compute_minor,
        };
        // The native benchmark runner suppresses info logs. Keep its placement
        // evidence explicit instead of inferring execution from a backend name.
        if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
            std.debug.print(placement_format ++ "\n", placement_args)
        else
            std.log.info(placement_format, placement_args);
        break :blk decision;
    } else null;
    return .{
        .allocator = allocator,
        .compute = cuda_compute,
        .architecture = resolved_arch_config,
        .recurrent_execution = recurrent_execution,
        .task = source_task,
        .span_encoder_family = loaded_model.manifest.gliner_span_encoder_family,
        .decision_identity = host_weights_owner.decision_identity,
        .boundary_identity = host_weights_owner.boundary_identity,
        .budget_floor = gliner_budget,
    };
}
