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

//! Artifact-derived load/admission geometry. This layer describes tokenizer,
//! representation, persistent-weight and temporary preparation requirements;
//! serving owns eviction, admission leases, publication and cancellation lanes.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const manifest_mod = @import("manifest.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const session_factory = @import("../architectures/session_factory.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const runtime = @import("../runtime/root.zig");
fn manifestHasNativeAssets(man: manifest_mod.ModelManifest) bool {
    return man.nativeWeightArtifactKind() != null;
}

pub const ModelLoadAdmissionPlan = struct {
    /// Maximum simultaneous bytes while parsing/importing/repacking.
    peak: runtime.tier.memory.AdmissionAmounts,
    /// Bytes retained by the completed backend session.
    resident: runtime.tier.memory.AdmissionAmounts,
    /// Minimum serving envelope justified by the same resolved load plan.
    /// Operator overrides are applied after this floor.
    limit_floor: runtime.tier.memory.Limits = .{},
    /// CUDA A4B serving excludes temporary GGUF staging and reuses the
    /// resident KV/scratch reservation through a bounded session sub-ledger.
    serving_floor: ?runtime.tier.memory.Limits = null,
};

pub fn modelRunWorkspaceAllowance(weight_bytes: usize) usize {
    const min_workspace = 16 * 1024 * 1024;
    const max_workspace = 1024 * 1024 * 1024;
    return std.math.clamp(weight_bytes / 8, min_workspace, max_workspace);
}

pub const TokenizerArtifactKind = enum {
    huggingface,
    sentencepiece,
    wordpiece,
};

pub const tokenizer_fixed_resident_bytes = 16 * 1024 * 1024;

pub const tokenizer_fixed_peak_bytes = 24 * 1024 * 1024;

pub fn checkedScaledBytes(base: usize, multiplier: usize, fixed: usize) !usize {
    return std.math.add(
        usize,
        std.math.mul(usize, base, multiplier) catch
            return error.ResourceLimitExceeded,
        fixed,
    ) catch return error.ResourceLimitExceeded;
}

pub fn tokenizerAdmissionPlan(encoded_bytes: usize, kind: TokenizerArtifactKind) !ModelLoadAdmissionPlan {
    const factors: struct { resident: usize, peak: usize } = switch (kind) {
        .huggingface => .{ .resident = 10, .peak = 13 },
        .wordpiece => .{ .resident = 7, .peak = 9 },
        .sentencepiece => .{ .resident = 5, .peak = 7 },
    };
    return .{
        .peak = .{
            .host_weight_bytes = try checkedScaledBytes(
                encoded_bytes,
                factors.peak,
                tokenizer_fixed_peak_bytes,
            ),
        },
        .resident = .{
            .host_weight_bytes = try checkedScaledBytes(
                encoded_bytes,
                factors.resident,
                tokenizer_fixed_resident_bytes,
            ),
        },
    };
}

pub fn tokenizerFileAdmissionPlan(
    allocator: std.mem.Allocator,
    path: []const u8,
    kind: TokenizerArtifactKind,
) !ModelLoadAdmissionPlan {
    const encoded_u64 = try c_file.fileSize(allocator, path);
    const encoded = std.math.cast(usize, encoded_u64) orelse
        return error.ResourceLimitExceeded;
    return tokenizerAdmissionPlan(encoded, kind);
}

pub fn tokenizerLoadAdmissionPlan(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    tokenizer_type: manifest_mod.TokenizerType,
) !ModelLoadAdmissionPlan {
    // Admission and loading must inspect the same catalog-selected artifacts,
    // including export-local vocabularies and root metadata fallbacks.
    if (tokenizer_type == .huggingface) {
        if (man.tokenizer_json_path) |path| return tokenizerFileAdmissionPlan(allocator, path, .huggingface);
        if (man.vocab_txt_path) |path| {
            var encoded: usize = 0;
            for ([_]?[]const u8{ path, man.tokenizer_config_path, man.special_tokens_map_path }) |candidate| {
                if (candidate) |selected| {
                    const size = std.math.cast(usize, try c_file.fileSize(allocator, selected)) orelse return error.ResourceLimitExceeded;
                    encoded = std.math.add(usize, encoded, size) catch return error.ResourceLimitExceeded;
                }
            }
            return tokenizerAdmissionPlan(encoded, .wordpiece);
        }
    } else {
        var encoded: usize = 0;
        var found_sentencepiece = false;
        const sidecars = [_][]const u8{
            "tokenizer.model",
            "tokenizer.json",
            "added_tokens.json",
        };
        for (sidecars) |name| {
            const path = try std.fs.path.join(allocator, &.{ model_dir, name });
            defer allocator.free(path);
            if (!c_file.fileExists(allocator, path)) continue;
            const size = std.math.cast(
                usize,
                try c_file.fileSize(allocator, path),
            ) orelse return error.ResourceLimitExceeded;
            encoded = std.math.add(usize, encoded, size) catch
                return error.ResourceLimitExceeded;
            if (std.mem.eql(u8, name, "tokenizer.model")) {
                found_sentencepiece = true;
            }
        }
        if (found_sentencepiece) {
            return tokenizerAdmissionPlan(encoded, .sentencepiece);
        }
    }
    if (man.gguf_path) |path| {
        var region = try c_file.MmapRegion.init(allocator, path);
        defer region.deinit();
        const encoded = try gguf_format.encodedMetadataBytesWithPrefix(
            region.data,
            "tokenizer.",
        );
        if (encoded == 0) return error.NoTokenizerFound;
        var plan = try tokenizerAdmissionPlan(
            encoded,
            if (tokenizer_type == .huggingface)
                .huggingface
            else
                .sentencepiece,
        );
        // The tokenizer loader currently parses the complete GGUF metadata and
        // tensor-info header before extracting tokenizer arrays. Account for
        // those temporary copied keys/names/dimensions in the construction peak.
        const header_bytes = try gguf_format.encodedHeaderBytes(region.data);
        if (header_bytes < encoded) return error.InvalidTokenizerMetadata;
        const non_tokenizer_header = header_bytes - encoded;
        plan.peak.host_weight_bytes = std.math.add(
            usize,
            plan.peak.host_weight_bytes,
            std.math.mul(usize, non_tokenizer_header, 3) catch
                return error.ResourceLimitExceeded,
        ) catch return error.ResourceLimitExceeded;
        return plan;
    }
    return error.NoTokenizerFound;
}

pub const ComponentArtifactEstimate = union(enum) {
    disabled,
    onnx: usize,
    native: manifest_mod.ModelManifest,

    pub fn init(allocator: std.mem.Allocator, model_path: []const u8) !ComponentArtifactEstimate {
        if (std.mem.endsWith(u8, model_path, ".onnx")) {
            var artifacts = try backends.imported_onnx_session.inspectArtifactSet(
                allocator,
                model_path,
            );
            defer artifacts.deinit();
            return .{ .onnx = artifacts.encoded_bytes };
        }

        // Native sessions accept directories as well as direct GGUF paths.
        // Loading the manifest is essential here: a directory's stat size only
        // describes its entries and bears no relation to resident model bytes.
        return .{ .native = try manifest_mod.loadFromDir(allocator, model_path) };
    }

    pub fn deinit(self: *ComponentArtifactEstimate) void {
        switch (self.*) {
            .native => |*man| man.deinit(),
            .disabled, .onnx => {},
        }
        self.* = .disabled;
    }

    pub fn encodedBytes(self: *const ComponentArtifactEstimate) !usize {
        return switch (self.*) {
            .disabled => 0,
            .onnx => |bytes| bytes,
            .native => |man| estimateModelArtifactBytes(man),
        };
    }

    pub fn extraBackendResidentAmounts(
        self: *const ComponentArtifactEstimate,
        backend: backends.BackendType,
    ) runtime.tier.memory.AdmissionAmounts {
        if (backend != .metal) return .{};
        return switch (self.*) {
            .native => |man| session_factory.metalDebertaFastPathAdmissionAmounts(man),
            .disabled, .onnx => .{},
        };
    }
};

pub fn addArtifactBytes(total: *usize, allocator: std.mem.Allocator, maybe_path: ?[]const u8) !void {
    const path = maybe_path orelse return;
    const size = if (std.mem.endsWith(u8, path, ".onnx")) blk: {
        var artifacts = try backends.imported_onnx_session.inspectArtifactSet(allocator, path);
        defer artifacts.deinit();
        break :blk artifacts.encoded_bytes;
    } else std.math.cast(usize, try c_file.fileSize(allocator, path)) orelse
        return error.ResourceLimitExceeded;
    total.* = std.math.add(usize, total.*, size) catch return error.ResourceLimitExceeded;
}

pub fn estimateModelArtifactBytes(man: manifest_mod.ModelManifest) !usize {
    var total: usize = 0;
    const uses_native_weights = manifestHasNativeAssets(man);
    if (uses_native_weights) {
        const native_weight_bytes = std.math.cast(
            usize,
            try session_factory.estimateNativeWeightBytes(man.allocator, man),
        ) orelse return error.ResourceLimitExceeded;
        total = std.math.add(usize, total, native_weight_bytes) catch
            return error.ResourceLimitExceeded;
    } else {
        try addArtifactBytes(&total, man.allocator, man.onnx_path);
    }

    if (total == 0) return error.NoModelFileFound;
    return total;
}

pub fn estimateModelLoadAdmission(
    model_path: []const u8,
    man: manifest_mod.ModelManifest,
    backend_runtime: backends.BackendRuntime,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
    laya_packed: bool,
) !ModelLoadAdmissionPlan {
    const weights = try estimateModelArtifactBytes(man);
    if (!manifestHasNativeAssets(man)) return onnxModelLoadAdmission(weights, backend_runtime);
    if (backend_runtime.backend == .metal) {
        if (!laya_packed) {
            if (try session_factory.layaResidentLoadAmounts(man, weights)) |resident| {
                return .{ .peak = resident.peak, .resident = resident.resident };
            }
        }
        if (try session_factory.glinerBoundaryResidentLoadAmounts(man, weights)) |resident| {
            return .{ .peak = resident.peak, .resident = resident.resident };
        }
    }
    if (backend_runtime.backend == .metal or backend_runtime.backend == .cuda) {
        const config = if (backend_runtime.backend == .cuda)
            try session_factory.resolveCudaA4bInferenceConfigForModelListing(
                man.allocator,
                model_path,
                man,
                a4b_request,
            )
        else
            try session_factory.resolveA4bInferenceConfigForModelListing(
                man.allocator,
                model_path,
                man,
                a4b_request,
            );
        if (config) |resolved| {
            return a4bGpuModelLoadAdmission(resolved, weights, backend_runtime.backend);
        }
    }
    const extra_backend_resident = if (backend_runtime.backend == .metal)
        session_factory.metalDebertaFastPathAdmissionAmounts(man)
    else
        runtime.tier.memory.AdmissionAmounts{};
    return nativeModelLoadAdmission(weights, backend_runtime.backend, extra_backend_resident);
}

pub fn a4bGpuModelLoadAdmission(
    config: backend_contracts.A4bInferenceConfig,
    encoded_artifact_bytes: usize,
    backend: backends.BackendType,
) ModelLoadAdmissionPlan {
    const budget: usize = @intCast(config.memory_budget_bytes);
    const kv: usize = @intCast(config.kv_budget_bytes);
    const scratch: usize = @intCast(config.safety_reserve_bytes);
    const weights = budget -| kv -| scratch;
    const resident = runtime.tier.memory.AdmissionAmounts{
        .backend_weight_bytes = weights,
        .backend_kv_bytes = kv,
        .backend_scratch_bytes = scratch,
    };
    var peak = resident;
    // CUDA constructs a temporary native GGUF session while uploading its
    // resident representation. Metal maps the encoded artifact directly and
    // does not retain a second host copy.
    if (backend == .cuda) peak.host_weight_bytes = encoded_artifact_bytes;
    const host_staging = if (backend == .cuda) encoded_artifact_bytes else 0;
    return .{
        .peak = peak,
        .resident = resident,
        .limit_floor = .{
            .host_limit_bytes = host_staging,
            .backend_limit_bytes = budget,
            .combined_limit_bytes = budget +| host_staging,
            .kv_limit_bytes = kv,
            .scratch_limit_bytes = scratch,
        },
        .serving_floor = if (backend == .cuda) .{
            .backend_limit_bytes = budget,
            .combined_limit_bytes = budget,
            .kv_limit_bytes = kv,
            .scratch_limit_bytes = scratch,
        } else null,
    };
}

pub fn onnxModelLoadAdmission(
    weights: usize,
    backend_runtime: backends.BackendRuntime,
) !ModelLoadAdmissionPlan {
    return switch (backend_runtime.backend) {
        .metal, .cuda => .{
            .peak = .{
                // The importer retains encoded host bytes while constructing device
                // parameters and graph metadata.
                .backend_weight_bytes = weights,
                .host_weight_bytes = weights,
            },
            .resident = .{ .backend_weight_bytes = weights },
        },
        .native, .wasm => .{
            .peak = .{
                // The current ONNX path reads the encoded protobuf and then owns the
                // converted parameter storage, so reserve both copies.
                .host_weight_bytes = std.math.mul(usize, weights, 2) catch
                    return error.ResourceLimitExceeded,
            },
            .resident = .{ .host_weight_bytes = weights },
        },
        .pjrt => return error.UnsupportedBackend,
    };
}

pub fn nativeModelLoadAdmission(
    weights: usize,
    backend: backends.BackendType,
    extra_backend_resident: runtime.tier.memory.AdmissionAmounts,
) !ModelLoadAdmissionPlan {
    return switch (backend) {
        .metal => blk: {
            const encoded_resident = try session_factory.estimateBackendWeightResidencyBytes(.metal, weights);
            var resident = extra_backend_resident;
            resident.backend_weight_bytes = std.math.add(
                usize,
                encoded_resident,
                resident.backend_weight_bytes,
            ) catch return error.ResourceLimitExceeded;
            var peak = resident;
            peak.host_weight_bytes = weights / 4;
            break :blk .{
                // Device weights plus persistent architecture caches, bounded
                // graph-plan scratch, and conservative host import staging.
                .peak = peak,
                .resident = resident,
            };
        },
        .cuda => .{
            .peak = .{
                // CUDA construction currently builds a complete native session first,
                // then uploads every resident weight before releasing the native copy.
                .backend_weight_bytes = try session_factory.estimateBackendWeightResidencyBytes(.cuda, weights),
                .host_weight_bytes = weights,
            },
            .resident = .{
                .backend_weight_bytes = try session_factory.estimateBackendWeightResidencyBytes(.cuda, weights),
            },
        },
        .native, .wasm => .{
            .peak = .{
                // Native import may retain a source mapping while materializing or
                // repacking resident weights.
                .host_weight_bytes = std.math.add(usize, weights, weights / 4) catch
                    return error.ResourceLimitExceeded,
            },
            .resident = .{ .host_weight_bytes = weights },
        },
        .pjrt => return error.UnsupportedBackend,
    };
}

pub fn qwenEmbeddingAdmissionConfig(
    backend: backends.BackendType,
    man: ?*const manifest_mod.ModelManifest,
    config: ?@import("../architectures/gpt.zig").Config,
) ?@import("../architectures/gpt.zig").Config {
    if (backend != .metal) return null;
    const manifest = man orelse return null;
    if (manifest.model_type != .embedder or !manifest.isLastTokenDecoderEmbedder()) return null;
    const cfg = config orelse return null;
    if (!@import("../architectures/gpt.zig").denseQwen3PrefillEligible(cfg)) return null;
    return cfg;
}

pub fn sessionRunAdmissionModelProfile(
    backend_runtime: backends.BackendRuntime,
    man: ?*const manifest_mod.ModelManifest,
    config: ?@import("../architectures/gpt.zig").Config,
) @import("../backends/session.zig").RunAdmission.ModelProfile {
    const manifest = man orelse return .{};
    const qwen_cfg = qwenEmbeddingAdmissionConfig(backend_runtime.backend, man, config);
    return .{
        .hidden_size = manifest.hidden_size,
        .intermediate_size = manifest.intermediate_size,
        .attention_heads = manifest.num_attention_heads,
        .embedding_gemma2_cuda = manifest.embedding_style == .embedding_gemma2 and backend_runtime.backend == .cuda,
        .qwen_dense_workspace = qwen_cfg != null,
        .query_width = if (qwen_cfg) |cfg| @as(usize, cfg.num_attention_heads) * cfg.headDim() else 0,
        .kv_width = if (qwen_cfg) |cfg| @as(usize, cfg.effectiveKVHeads()) * cfg.headDim() else 0,
    };
}

pub fn admissionBackendClassForRuntime(
    backend_runtime: backends.BackendRuntime,
) runtime.tier.memory.BackendClass {
    return if (backend_runtime.usesGpuHostedSession()) .gpu else .cpu;
}

pub fn attachSessionRunAdmission(
    allocator: std.mem.Allocator,
    session: *backends.Session,
    controller: *runtime.tier.memory.AdmissionController,
    backend_runtime: backends.BackendRuntime,
    limits: runtime.tier.memory.Limits,
    resident: runtime.tier.memory.AdmissionAmounts,
    man: ?*const manifest_mod.ModelManifest,
) !void {
    const backend_class = admissionBackendClassForRuntime(backend_runtime);
    try session_factory.configureSharedCacheAdmissionForSession(
        session.*,
        allocator,
        controller,
        backend_class,
        limits,
        resident,
    );
    const weight_bytes = std.math.add(
        usize,
        resident.host_weight_bytes,
        resident.backend_weight_bytes,
    ) catch std.math.maxInt(usize);
    const qwen_cfg = if (backend_runtime.backend == .metal and
        metal_runtime.appleSimdgroupFeatureEnabled("TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING", true))
        session_factory.getGptConfig(session.*)
    else
        null;
    session.run_admission = .{
        .controller = controller,
        .backend_class = backend_class,
        .limits = limits,
        .static_workspace_bytes = modelRunWorkspaceAllowance(weight_bytes),
        .model_profile = sessionRunAdmissionModelProfile(backend_runtime, man, qwen_cfg),
    };
}
