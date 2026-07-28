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

// Model manager: lazy-loads models and caches ready-to-use pipelines.
//
// Given a model directory path, loads the manifest, creates a tokenizer
// and backend session, and returns a pipeline ready for inference.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const backends = @import("../backends/backends.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_compatibility = @import("../models/compatibility.zig");
const safetensors_mod = @import("../models/safetensors.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const gguf_writer = @import("../gguf/writer.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const tokenizer_mod = @import("inference_tokenizer");
const embedding_mod = @import("../pipelines/embedding.zig");
const EmbeddingPipeline = embedding_mod.EmbeddingPipeline;
const EmbeddingConfig = embedding_mod.EmbeddingConfig;
const PoolingStrategy = embedding_mod.PoolingStrategy;
const RerankingPipeline = @import("../pipelines/reranking.zig").RerankingPipeline;
const RerankingConfig = @import("../pipelines/reranking.zig").RerankingConfig;
const ScoringMode = @import("../pipelines/reranking.zig").ScoringMode;
const ClassificationPipeline = @import("../pipelines/classification.zig").ClassificationPipeline;
const ClassificationConfig = @import("../pipelines/classification.zig").ClassificationConfig;
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const NerPipeline = @import("../pipelines/ner.zig").NerPipeline;
const NerConfig = @import("../pipelines/ner.zig").NerConfig;
const GlinerPipeline = @import("../pipelines/gliner.zig").GlinerPipeline;
const GlinerConfig = @import("../pipelines/gliner.zig").GlinerConfig;
const generation = @import("../pipelines/generation.zig");
const ChatTemplate = generation.ChatTemplate;
const session_factory = @import("../architectures/session_factory.zig");
const graph_mod = @import("../graph/root.zig");
const runtime = @import("../runtime/root.zig");
const onnx_graph = @import("onnx_graph");
const ml = @import("ml");

fn shouldPreferNativeSession(man: manifest_mod.ModelManifest) bool {
    // GLiNER has a native DeBERTa + span-head path. When native weights are
    // present, prefer the directory-backed session so the model does not get
    // pinned to ONNX just because an export also exists.
    if (!manifestHasNativeAssets(man)) return false;
    if (man.model_type == .embedder and
        man.visual_model_path == null and
        man.audio_model_path == null and
        man.text_projection_path == null and
        man.visual_projection_path == null and
        man.audio_projection_path == null)
    {
        return true;
    }
    if (man.gliner_model_type.len > 0) return true;
    switch (man.model_type) {
        .classifier, .recognizer => return true,
        else => {},
    }
    return switch (man.native_arch_hint) {
        .clip, .whisper, .florence, .layoutlmv3 => true,
        .clap, .none => false,
    };
}

fn nativeBackendsAvailable() bool {
    return build_options.enable_native or build_options.enable_metal or build_options.enable_cuda;
}

fn manifestHasNativeAssets(man: manifest_mod.ModelManifest) bool {
    return man.nativeWeightArtifactKind() != null;
}

const ArtifactCandidateKind = enum {
    gguf,
    safetensors,
    onnx,
    component_bundle,
};

fn artifactCandidateForBackend(
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) ?ArtifactCandidateKind {
    if (backend == .pjrt) return null;
    if (backend == .onnx) return if (man.onnx_path != null) .onnx else null;
    if (man.nativeWeightArtifactKind()) |artifact| {
        return switch (artifact) {
            .gguf => .gguf,
            .safetensors, .sharded_safetensors => .safetensors,
        };
    }
    if (man.onnx_path != null) return .onnx;
    if (man.visual_model_path != null or
        man.audio_model_path != null or
        man.text_projection_path != null or
        man.visual_projection_path != null or
        man.audio_projection_path != null)
    {
        return .component_bundle;
    }
    return null;
}

pub const CompatibilitySummary = struct {
    level: model_compatibility.Level,
    code: model_compatibility.Code,
    message: []const u8,
};

/// Describes the runtime contract represented by a set of independently loaded
/// model components. A component bundle must not inherit an incompatible
/// whole-model classification when a dedicated pipeline owns its runtime.
pub const ComponentContract = enum(u8) {
    manifest,
    multistage_ocr,
};

fn summaryFromAssessment(assessment: model_compatibility.Assessment) CompatibilitySummary {
    return .{
        .level = assessment.level,
        .code = assessment.code,
        .message = assessment.message,
    };
}

const NativeCompanionKind = enum {
    gguf,
    safetensors,
};

const NativeCompanionRole = enum {
    generic,
    projector,
};

const NativeCompanion = struct {
    path: []const u8,
    kind: NativeCompanionKind,
    role: NativeCompanionRole = .generic,
};

const ProjectorDecoderFamily = enum {
    gemma3,
    gemma4,
    unsupported,
};

const ProjectorDecoderContract = struct {
    family: ProjectorDecoderFamily,
    hidden_size: u32 = 0,
    tokens_per_image: u32 = 0,
    requires_image: bool = false,
    requires_audio: bool = false,
};

fn projectorDecoderFamilyForArchitecture(architecture: []const u8) ProjectorDecoderFamily {
    if (std.mem.eql(u8, architecture, "gemma3")) return .gemma3;
    if (std.mem.eql(u8, architecture, "gemma4") or
        std.mem.eql(u8, architecture, "gemma4_unified"))
    {
        return .gemma4;
    }
    return .unsupported;
}

fn projectorMatchesDecoder(
    projector: projector_format_mod.Kind,
    decoder: ProjectorDecoderContract,
) bool {
    return switch (projector) {
        .antfly_gemma3 => decoder.family == .gemma3,
        .clip_gemma4_image,
        .clip_gemma4_audio,
        .clip_gemma4_image_audio,
        => decoder.family == .gemma4,
        .unknown => false,
    };
}

fn validateGgufCompanionForBackend(
    allocator: std.mem.Allocator,
    path: []const u8,
    backend: backends.BackendType,
    role: NativeCompanionRole,
    projector_decoder: ProjectorDecoderContract,
) !?CompatibilitySummary {
    var mapped = c_file.MmapRegion.init(allocator, path) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "a required companion GGUF is missing or unreadable",
        };
    };
    defer mapped.deinit();

    var file = gguf_format.parseStructure(allocator, mapped.data) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "a required companion GGUF has invalid structure",
        };
    };
    defer file.deinit(allocator);
    gguf_format.validateTensorDataRanges(&file, mapped.data.len) catch {
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "a required companion GGUF has invalid or truncated tensor data",
        };
    };
    const parsed_prefix_len = std.math.cast(usize, file.data_region_offset) orelse mapped.data.len;
    mapped.adviseSequentialPrefix(@min(parsed_prefix_len, mapped.data.len));

    if (file.tensors.len == 0) {
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "a required companion GGUF contains no tensors",
        };
    }
    if (role == .projector) {
        if (projector_format_mod.detectFile(&file) == .unknown) {
            return .{
                .level = .incompatible,
                .code = .unsupported_backend,
                .message = "a projector GGUF does not declare a supported Antfly Gemma 3 or Gemma 4 CLIP projector format",
            };
        }
        const contract = projector_format_mod.inspectFileContract(&file) catch {
            return .{
                .level = .incompatible,
                .code = .artifact_unreadable,
                .message = "a projector GGUF is missing required metadata or tensors, or declares an invalid shape contract",
            };
        };
        if (!projectorMatchesDecoder(contract.kind, projector_decoder)) {
            return .{
                .level = .incompatible,
                .code = .unsupported_backend,
                .message = "the projector GGUF format does not match the selected decoder architecture",
            };
        }
        if (projector_decoder.hidden_size > 0 and
            contract.text_hidden_size != projector_decoder.hidden_size)
        {
            return .{
                .level = .incompatible,
                .code = .missing_required_tensor,
                .message = "the projector output width does not match the selected decoder hidden size",
            };
        }
        if (projector_decoder.tokens_per_image > 0 and
            contract.tokens_per_image != null and
            contract.tokens_per_image.? != projector_decoder.tokens_per_image)
        {
            return .{
                .level = .incompatible,
                .code = .missing_required_tensor,
                .message = "the projector image-token count does not match the selected decoder",
            };
        }
        if ((projector_decoder.requires_image and !contract.supports_image) or
            (projector_decoder.requires_audio and !contract.supports_audio))
        {
            return .{
                .level = .incompatible,
                .code = .unsupported_backend,
                .message = "the projector modalities do not satisfy the model input contract",
            };
        }
    }
    for (file.tensors) |tensor| {
        if (!session_factory.ggufTensorTypeSupportsBackend(tensor.tensor_type, backend)) {
            return .{
                .level = .incompatible,
                .code = .unsupported_tensor_type,
                .message = "the selected backend cannot materialize a required companion GGUF",
            };
        }
    }
    return null;
}

fn validateNativeCompanionsForBackend(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
    candidate: ArtifactCandidateKind,
    backend: backends.BackendType,
    projector_decoder: ProjectorDecoderContract,
) !?CompatibilitySummary {
    // An ONNX route never consumes native sidecars. Native primary routes can
    // consume a projector, a split GLiNER head, and lazily loaded multimodal
    // sessions, so all of those are part of the compatibility contract.
    if (candidate == .onnx) return null;

    var companions: [8]NativeCompanion = undefined;
    var companion_count: usize = 0;
    if (man.gguf_projector_path) |path| {
        companions[companion_count] = .{
            .path = path,
            .kind = .gguf,
            .role = .projector,
        };
        companion_count += 1;
    }
    if (candidate == .gguf) {
        if (man.gliner_head_gguf_path) |path| {
            companions[companion_count] = .{ .path = path, .kind = .gguf };
            companion_count += 1;
        } else if (man.gliner_head_safetensors_path) |path| {
            companions[companion_count] = .{ .path = path, .kind = .safetensors };
            companion_count += 1;
        }
    }

    const lazy_component_paths = [_]?[]const u8{
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    };
    for (lazy_component_paths) |maybe_path| {
        const path = maybe_path orelse continue;
        const kind: NativeCompanionKind = if (std.mem.endsWith(u8, path, ".gguf"))
            .gguf
        else if (std.mem.endsWith(u8, path, ".safetensors"))
            .safetensors
        else
            continue;
        companions[companion_count] = .{ .path = path, .kind = kind };
        companion_count += 1;
    }

    for (companions[0..companion_count], 0..) |companion, companion_index| {
        var duplicate = false;
        for (companions[0..companion_index]) |previous| {
            if (previous.kind == companion.kind and std.mem.eql(u8, previous.path, companion.path)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;

        switch (companion.kind) {
            .gguf => if (try validateGgufCompanionForBackend(
                allocator,
                companion.path,
                backend,
                companion.role,
                projector_decoder,
            )) |summary| return summary,
            .safetensors => safetensors_mod.validateArtifactSet(
                allocator,
                companion.path,
                null,
            ) catch |err| {
                if (err == error.OutOfMemory) return err;
                return .{
                    .level = .incompatible,
                    .code = .artifact_unreadable,
                    .message = "a required companion safetensors file is invalid or unreadable",
                };
            },
        }
    }
    return null;
}

/// Assess exactly the artifact route that `backend` would load. Optional
/// artifacts for other backends must not poison this result.
pub fn compatibilitySummaryForBackend(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    backend: backends.BackendType,
) !?CompatibilitySummary {
    const candidate = artifactCandidateForBackend(man.*, backend) orelse return null;

    // GGUF metadata is authoritative only when this route will consume GGUF.
    // ONNX/safetensors routes use the manifest architecture rather than
    // accidentally inheriting an unrelated optional GGUF's classification.
    var candidate_manifest = man.*;
    if (candidate != .gguf) candidate_manifest.gguf_path = null;
    var inspection = try model_compatibility.inspectAlloc(allocator, &candidate_manifest);
    defer inspection.deinit(allocator);
    const assessment = model_compatibility.assessInspection(&candidate_manifest, inspection);
    if (assessment.level == .incompatible) return summaryFromAssessment(assessment);

    var projector_decoder = ProjectorDecoderContract{
        .family = projectorDecoderFamilyForArchitecture(man.config_model_arch),
        .hidden_size = man.hidden_size,
        .requires_image = manifestRequiresInput(man, "image"),
        .requires_audio = manifestRequiresInput(man, "audio"),
    };
    if (candidate == .gguf) {
        var maybe_report = session_factory.inspectGgufModelForListing(
            allocator,
            model_dir,
            candidate_manifest,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            return .{
                .level = .incompatible,
                .code = .artifact_unreadable,
                .message = "GGUF structure or tensor metadata is invalid or unreadable",
            };
        };
        if (maybe_report) |*report| {
            defer report.deinit();
            projector_decoder.family = projectorDecoderFamilyForArchitecture(report.architecture);
            if (report.gpt_config) |config| {
                projector_decoder.hidden_size = config.hidden_size;
                projector_decoder.tokens_per_image = config.mm_tokens_per_image;
                // Gemma 3 needs decoder-side image-token geometry at runtime.
                // Gemma 4 external projectors carry their own media geometry
                // and can validly accompany a decoder GGUF that predates the
                // Antfly multimodal metadata namespace.
                if (!config.isMultimodal() and projector_decoder.family == .gemma3)
                    projector_decoder.family = .unsupported;
            }
            if (report.missing_required_tensors.len > 0) {
                return .{
                    .level = .incompatible,
                    .code = .missing_required_tensor,
                    .message = "GGUF is missing required normalized tensors",
                };
            }
            if (!session_factory.ggufInspectionSupportsBackend(report.*, backend)) {
                return .{
                    .level = .incompatible,
                    .code = .unsupported_tensor_type,
                    .message = "the selected backend cannot materialize this GGUF tensor contract",
                };
            }
        }
    }
    if (candidate == .safetensors) {
        safetensors_mod.validateArtifactSet(
            allocator,
            man.safetensors_path,
            man.safetensors_index_path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            return .{
                .level = .incompatible,
                .code = .artifact_unreadable,
                .message = "safetensors file, index, or referenced shard is invalid or unreadable",
            };
        };
    }
    if (try validateNativeCompanionsForBackend(
        allocator,
        man,
        candidate,
        backend,
        projector_decoder,
    )) |summary| return summary;

    // Validate only ONNX graphs reachable from the selected artifact route.
    // Native GGUF/safetensors routes can still lazily load vision/audio and
    // projection components, but never consume the optional primary ONNX graph.
    const onnx_paths = [_]?[]const u8{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    };
    const first_onnx_path: usize = if (candidate == .onnx) 0 else 1;
    for (onnx_paths[first_onnx_path..], first_onnx_path..) |maybe_path, path_index| {
        const path = maybe_path orelse continue;
        if (!std.mem.endsWith(u8, path, ".onnx")) continue;
        var duplicate = false;
        for (onnx_paths[first_onnx_path..path_index]) |previous| {
            if (previous) |existing| {
                if (std.mem.eql(u8, existing, path)) {
                    duplicate = true;
                    break;
                }
            }
        }
        if (duplicate) continue;

        var artifacts = backends.imported_onnx_session.inspectArtifactSet(
            allocator,
            path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            return .{
                .level = .incompatible,
                .code = .invalid_graph,
                .message = "ONNX graph or external tensor data is invalid or unreadable",
            };
        };
        artifacts.deinit();

        if (backend != .onnx or !build_options.enable_onnx) {
            backends.imported_onnx_session.inspectGraphCompatibility(
                allocator,
                path,
            ) catch |err| {
                if (err == error.OutOfMemory) return err;
                return .{
                    .level = .incompatible,
                    .code = .invalid_graph,
                    .message = "ONNX graph cannot be converted and validated by the selected backend",
                };
            };
        }
    }
    return summaryFromAssessment(assessment);
}

/// Aggregate candidate compatibility as an OR: a model bundle is usable when
/// any configured backend has a compatible artifact. Unknown is returned only
/// when no compatible route exists, and incompatible only when every candidate
/// is known to be unusable.
pub fn compatibilitySummaryForBackends(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    preferred_backends: []const backends.BackendType,
) !CompatibilitySummary {
    var best: ?CompatibilitySummary = null;
    for (preferred_backends) |backend| {
        const summary = try compatibilitySummaryForBackend(
            allocator,
            model_dir,
            man,
            backend,
        ) orelse continue;
        best = selectBetterCompatibility(best, summary);
        if (best.?.level == .compatible) return best.?;
    }
    return best orelse .{
        .level = .incompatible,
        .code = .unsupported_backend,
        .message = "no configured backend accepts an artifact in this model bundle",
    };
}

fn selectBetterCompatibility(
    current: ?CompatibilitySummary,
    candidate: CompatibilitySummary,
) CompatibilitySummary {
    const existing = current orelse return candidate;
    const candidate_rank: u2 = switch (candidate.level) {
        .incompatible => 0,
        .unknown => 1,
        .compatible => 2,
    };
    const existing_rank: u2 = switch (existing.level) {
        .incompatible => 0,
        .unknown => 1,
        .compatible => 2,
    };
    return if (candidate_rank > existing_rank) candidate else existing;
}

fn policyAllowedBackends(
    allocator: std.mem.Allocator,
    scratch: *[7]backends.BackendType,
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    preferred_backends: []const backends.BackendType,
    policy: model_compatibility.Policy,
) ![]const backends.BackendType {
    var count: usize = 0;
    var first_policy_err: ?anyerror = null;
    for (preferred_backends) |backend| {
        if (!backend.supportsDirectSessionLoad()) continue;
        const summary = try compatibilitySummaryForBackend(
            allocator,
            model_dir,
            man,
            backend,
        ) orelse continue;
        const allowed = switch (summary.level) {
            .compatible => true,
            .unknown => policy.allow_unknown,
            .incompatible => false,
        };
        if (allowed) {
            scratch[count] = backend;
            count += 1;
        } else if (first_policy_err == null) {
            first_policy_err = if (summary.level == .unknown)
                error.UnknownModelCompatibility
            else
                error.IncompatibleModel;
        }
    }
    if (count > 0) return scratch[0..count];
    if (first_policy_err) |err| return err;
    var inspection = try model_compatibility.inspectAlloc(allocator, man);
    defer inspection.deinit(allocator);
    const bundle_assessment = model_compatibility.assessInspection(man, inspection);
    if (bundle_assessment.level == .unknown and !policy.allow_unknown)
        return error.UnknownModelCompatibility;
    return error.IncompatibleModel;
}

const ComponentInspection = struct {
    const backend_count = std.meta.fields(backends.BackendType).len;

    allocator: std.mem.Allocator,
    base_summary: CompatibilitySummary,
    invalid_summary: ?CompatibilitySummary = null,
    has_onnx_component: bool = false,
    has_native_component: bool = false,
    imported_graph_compatible: bool = true,
    native_backend_summaries: [backend_count]?CompatibilitySummary =
        [_]?CompatibilitySummary{null} ** backend_count,
    dependencies: std.ArrayListUnmanaged([]u8) = .empty,

    fn deinit(self: *ComponentInspection) void {
        for (self.dependencies.items) |path| self.allocator.free(path);
        self.dependencies.deinit(self.allocator);
        self.* = undefined;
    }

    fn addDependency(self: *ComponentInspection, path: []const u8) !void {
        for (self.dependencies.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return;
        }
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        try self.dependencies.append(self.allocator, owned_path);
    }

    fn hasDependency(self: *const ComponentInspection, path: []const u8) bool {
        for (self.dependencies.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return true;
        }
        return false;
    }

    fn mergeNativeSummary(
        self: *ComponentInspection,
        backend: backends.BackendType,
        summary: CompatibilitySummary,
    ) void {
        const slot = &self.native_backend_summaries[@intFromEnum(backend)];
        slot.* = selectWorseCompatibility(slot.*, summary);
    }

    fn summaryForBackend(
        self: *const ComponentInspection,
        backend: backends.BackendType,
    ) CompatibilitySummary {
        if (self.invalid_summary) |summary| return summary;
        if (backend == .onnx and self.has_native_component) {
            return .{
                .level = .incompatible,
                .code = .unsupported_backend,
                .message = "the ONNX Runtime backend cannot load a directory-backed component",
            };
        }
        if (self.has_onnx_component and
            (backend != .onnx or !build_options.enable_onnx) and
            !self.imported_graph_compatible)
        {
            return .{
                .level = .incompatible,
                .code = .invalid_graph,
                .message = "a component ONNX graph cannot be converted and validated by the selected backend",
            };
        }
        if (self.native_backend_summaries[@intFromEnum(backend)]) |native_summary| {
            return selectWorseCompatibility(self.base_summary, native_summary);
        }
        return self.base_summary;
    }
};

fn selectWorseCompatibility(
    current: ?CompatibilitySummary,
    candidate: CompatibilitySummary,
) CompatibilitySummary {
    const existing = current orelse return candidate;
    const candidate_rank: u2 = switch (candidate.level) {
        .incompatible => 0,
        .unknown => 1,
        .compatible => 2,
    };
    const existing_rank: u2 = switch (existing.level) {
        .incompatible => 0,
        .unknown => 1,
        .compatible => 2,
    };
    return if (candidate_rank < existing_rank) candidate else existing;
}

const ComponentPlanKey = [std.crypto.hash.sha2.Sha256.digest_length]u8;
const component_plan_cache_capacity = 256;

fn updateComponentPlanKeySlice(
    hash: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    const len: u64 = @intCast(value.len);
    hash.update(std.mem.asBytes(&len));
    hash.update(value);
}

const ComponentPlanCacheEntry = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    signature: ComponentPlanKey,
    allowed_backends: [7]backends.BackendType = undefined,
    allowed_backend_count: usize = 0,
    dependencies: [][]u8,

    fn create(
        allocator: std.mem.Allocator,
        signature: ComponentPlanKey,
        allowed: []const backends.BackendType,
        dependencies: []const []const u8,
    ) !*ComponentPlanCacheEntry {
        const self = try allocator.create(ComponentPlanCacheEntry);
        errdefer allocator.destroy(self);
        const owned_dependencies = try allocator.alloc([]u8, dependencies.len);
        errdefer allocator.free(owned_dependencies);
        var initialized: usize = 0;
        errdefer for (owned_dependencies[0..initialized]) |path| allocator.free(path);
        for (dependencies, 0..) |path, index| {
            owned_dependencies[index] = try allocator.dupe(u8, path);
            initialized += 1;
        }
        self.* = .{
            .allocator = allocator,
            .signature = signature,
            .dependencies = owned_dependencies,
        };
        @memcpy(self.allowed_backends[0..allowed.len], allowed);
        self.allowed_backend_count = allowed.len;
        return self;
    }

    fn retain(self: *ComponentPlanCacheEntry) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }

    fn release(self: *ComponentPlanCacheEntry) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.dependencies) |path| self.allocator.free(path);
        self.allocator.free(self.dependencies);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

fn componentPlanKey(
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    preferred_backends: []const backends.BackendType,
    component_paths: []const []const u8,
    policy: model_compatibility.Policy,
    contract: ComponentContract,
) ComponentPlanKey {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    updateComponentPlanKeySlice(&hash, model_dir);
    hash.update(&.{
        @intFromEnum(man.model_type),
        @intFromEnum(man.native_arch_hint),
        @intFromEnum(contract),
        @intFromBool(policy.allow_unknown),
        @intFromBool(manifestHasNativeAssets(man.*)),
        @intFromBool(man.hasIncompleteGlinerBundle()),
        @intFromBool(man.hasIncompleteColqwenBundle()),
        @intFromBool(man.hasIncompleteClipclapGgufBundle()),
        @intFromBool(man.hasIncompleteFlorence2GgufBundle()),
    });
    updateComponentPlanKeySlice(&hash, man.config_model_arch);
    updateComponentPlanKeySlice(&hash, man.gliner_model_type);
    updateComponentPlanKeySlice(&hash, man.inference_bundle_family);
    const backend_count: u64 = @intCast(preferred_backends.len);
    hash.update(std.mem.asBytes(&backend_count));
    for (preferred_backends) |backend| hash.update(&.{@intFromEnum(backend)});
    const component_count: u64 = @intCast(component_paths.len);
    hash.update(std.mem.asBytes(&component_count));
    for (component_paths) |path| {
        updateComponentPlanKeySlice(&hash, path);
    }
    var digest: ComponentPlanKey = undefined;
    hash.final(&digest);
    return digest;
}

/// Build a cheap cache-coherency identity from filesystem metadata. Full
/// artifact validation populates the cache; cache hits only need to detect
/// replacement or mutation. inode/size/mtime/ctime cover those transitions
/// without rereading and hashing model contents on request paths.
fn componentDependencySignature(
    io: std.Io,
    dependencies: []const []const u8,
) !ComponentPlanKey {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (dependencies) |path| {
        updateComponentPlanKeySlice(&hash, path);
        const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch {
            hash.update("missing");
            continue;
        };
        hash.update(std.mem.asBytes(&stat.inode));
        hash.update(std.mem.asBytes(&stat.size));
        const mtime_ns = stat.mtime.toNanoseconds();
        const ctime_ns = stat.ctime.toNanoseconds();
        // std.Io.Timestamp uses i96, whose ABI storage contains padding on
        // common targets. Widen before hashing so fingerprints never include
        // indeterminate padding bytes.
        const canonical_mtime_ns: i128 = mtime_ns;
        const canonical_ctime_ns: i128 = ctime_ns;
        hash.update(std.mem.asBytes(&canonical_mtime_ns));
        hash.update(std.mem.asBytes(&canonical_ctime_ns));
        const final_stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch {
            hash.update("changed-during-read");
            continue;
        };
        if (final_stat.inode != stat.inode or
            final_stat.size != stat.size or
            final_stat.mtime.toNanoseconds() != mtime_ns or
            final_stat.ctime.toNanoseconds() != ctime_ns)
            hash.update("changed-during-read");
    }
    var digest: ComponentPlanKey = undefined;
    hash.final(&digest);
    return digest;
}

fn addNativeComponentDependencies(
    allocator: std.mem.Allocator,
    component_path: []const u8,
    native_manifest: manifest_mod.ModelManifest,
    result: *ComponentInspection,
) !void {
    if (!std.mem.endsWith(u8, component_path, ".gguf")) {
        // Include absent manifest candidates too: creating a config file must
        // invalidate a plan that was cached before it existed.
        const manifest_dependencies = [_][]const u8{
            "config.json",
            "clip_config.json",
            "model_manifest.json",
            "antfly_inference_bundle.json",
            "antfly_inference_variants.json",
            "gliner_config.json",
            "added_tokens.json",
            "1_SpladePooling/config.json",
        };
        for (manifest_dependencies) |relative_path| {
            const dependency = try std.fs.path.join(
                allocator,
                &.{ component_path, relative_path },
            );
            defer allocator.free(dependency);
            try result.addDependency(dependency);
        }
    }

    const native_paths = [_]?[]const u8{
        native_manifest.gguf_path,
        native_manifest.gguf_projector_path,
        native_manifest.safetensors_path,
        native_manifest.safetensors_index_path,
        native_manifest.gliner_head_gguf_path,
        native_manifest.gliner_head_safetensors_path,
    };
    for (native_paths) |maybe_native_path| {
        if (maybe_native_path) |native_path| try result.addDependency(native_path);
    }

    // Native sessions can lazily open these companions after the primary
    // session has been admitted. They therefore participate in the cached
    // compatibility decision just as much as the primary weight artifact.
    // For ONNX companions, retain every referenced external-data file too.
    const lazy_component_paths = [_]?[]const u8{
        native_manifest.visual_model_path,
        native_manifest.audio_model_path,
        native_manifest.text_projection_path,
        native_manifest.visual_projection_path,
        native_manifest.audio_projection_path,
    };
    for (lazy_component_paths) |maybe_path| {
        const path = maybe_path orelse continue;
        const already_collected = result.hasDependency(path);
        try result.addDependency(path);
        if (already_collected or !std.mem.endsWith(u8, path, ".onnx")) continue;

        var artifacts = backends.imported_onnx_session.inspectArtifactSet(
            allocator,
            path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            result.invalid_summary = .{
                .level = .incompatible,
                .code = .invalid_graph,
                .message = "a directory-backed component ONNX companion or its external tensor data is invalid or unreadable",
            };
            return;
        };
        defer artifacts.deinit();
        for (artifacts.external_paths) |external_path|
            try result.addDependency(external_path);
    }

    if (artifactCandidateForBackend(native_manifest, .native) == .safetensors) {
        var safetensors_dependencies = safetensors_mod.inspectArtifactDependencies(
            allocator,
            native_manifest.safetensors_path,
            native_manifest.safetensors_index_path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            result.invalid_summary = .{
                .level = .incompatible,
                .code = .artifact_unreadable,
                .message = "a directory-backed component safetensors index is invalid or unreadable",
            };
            return;
        };
        defer safetensors_dependencies.deinit();
        for (safetensors_dependencies.paths) |dependency|
            try result.addDependency(dependency);
    }
}

fn inspectComponentArtifacts(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
    component_paths: []const []const u8,
    contract: ComponentContract,
) !ComponentInspection {
    var component_manifest = man.*;
    // Component ONNX sessions do not consume an optional GGUF payload. Do not
    // let unrelated GGUF metadata classify the route selected below.
    component_manifest.gguf_path = null;
    var inspection = try model_compatibility.inspectAlloc(allocator, &component_manifest);
    defer inspection.deinit(allocator);
    const assessment = model_compatibility.assessInspection(&component_manifest, inspection);
    const base_summary: CompatibilitySummary = switch (contract) {
        .manifest => summaryFromAssessment(assessment),
        // The multistage reader validates its metadata and every stage artifact
        // before construction. Its parent directory is classified as a reader,
        // but it does not use the single-model Florence reader runtime.
        .multistage_ocr => .{
            .level = .compatible,
            .code = .compatible,
            .message = "validated multistage OCR component pipeline",
        },
    };
    var result = ComponentInspection{
        .allocator = allocator,
        .base_summary = base_summary,
    };
    errdefer result.deinit();
    if (base_summary.level == .incompatible) return result;

    for (component_paths, 0..) |path, path_index| {
        var duplicate = false;
        for (component_paths[0..path_index]) |previous| {
            if (std.mem.eql(u8, previous, path)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        try result.addDependency(path);

        if (!std.mem.endsWith(u8, path, ".onnx")) {
            result.has_native_component = true;
            var native_manifest = manifest_mod.loadFromDir(allocator, path) catch {
                result.invalid_summary = .{
                    .level = .incompatible,
                    .code = .artifact_unreadable,
                    .message = "a directory-backed component manifest is invalid or unreadable",
                };
                continue;
            };
            defer native_manifest.deinit();
            if (!manifestHasNativeAssets(native_manifest)) {
                result.invalid_summary = .{
                    .level = .incompatible,
                    .code = .incomplete_bundle,
                    .message = "a directory-backed component has no native model assets",
                };
                continue;
            }
            try addNativeComponentDependencies(allocator, path, native_manifest, &result);
            continue;
        }
        result.has_onnx_component = true;
        var artifacts = backends.imported_onnx_session.inspectArtifactSet(
            allocator,
            path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            result.invalid_summary = .{
                .level = .incompatible,
                .code = .invalid_graph,
                .message = "a component ONNX graph or its external tensor data is invalid or unreadable",
            };
            continue;
        };
        defer artifacts.deinit();
        for (artifacts.external_paths) |external_path| try result.addDependency(external_path);
    }
    return result;
}

fn validateComponentNativeArtifacts(
    allocator: std.mem.Allocator,
    component_paths: []const []const u8,
    preferred_backends: []const backends.BackendType,
    inspection: *ComponentInspection,
) !void {
    if (!inspection.has_native_component or inspection.invalid_summary != null) return;
    for (component_paths, 0..) |path, path_index| {
        if (std.mem.endsWith(u8, path, ".onnx")) continue;
        var duplicate = false;
        for (component_paths[0..path_index]) |previous| {
            if (std.mem.eql(u8, previous, path)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;

        var native_manifest = manifest_mod.loadFromDir(allocator, path) catch |err| {
            if (err == error.OutOfMemory) return err;
            inspection.invalid_summary = .{
                .level = .incompatible,
                .code = .artifact_unreadable,
                .message = "a directory-backed component manifest changed or became unreadable",
            };
            return;
        };
        defer native_manifest.deinit();
        try addNativeComponentDependencies(
            allocator,
            path,
            native_manifest,
            inspection,
        );
        if (inspection.invalid_summary != null) return;
        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad() or backend == .onnx) continue;
            const summary = try compatibilitySummaryForBackend(
                allocator,
                path,
                &native_manifest,
                backend,
            ) orelse CompatibilitySummary{
                .level = .incompatible,
                .code = .unsupported_backend,
                .message = "a directory-backed component has no artifact route for the selected backend",
            };
            inspection.mergeNativeSummary(backend, summary);
        }
    }
}

fn validateComponentImportedGraphs(
    allocator: std.mem.Allocator,
    component_paths: []const []const u8,
    preferred_backends: []const backends.BackendType,
    inspection: *ComponentInspection,
) !void {
    var required = false;
    for (preferred_backends) |backend| {
        if (backend != .onnx or !build_options.enable_onnx) {
            required = true;
            break;
        }
    }
    if (!required or !inspection.has_onnx_component or inspection.invalid_summary != null) return;

    for (component_paths, 0..) |path, path_index| {
        if (!std.mem.endsWith(u8, path, ".onnx")) continue;
        var duplicate = false;
        for (component_paths[0..path_index]) |previous| {
            if (std.mem.eql(u8, previous, path)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        backends.imported_onnx_session.inspectGraphCompatibility(
            allocator,
            path,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            inspection.imported_graph_compatible = false;
            return;
        };
    }
}

fn inspectComponentBundle(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
    component_paths: []const []const u8,
    contract: ComponentContract,
    preferred_backends: []const backends.BackendType,
) !ComponentInspection {
    var inspection = try inspectComponentArtifacts(
        allocator,
        man,
        component_paths,
        contract,
    );
    errdefer inspection.deinit();
    try validateComponentNativeArtifacts(
        allocator,
        component_paths,
        preferred_backends,
        &inspection,
    );
    try validateComponentImportedGraphs(
        allocator,
        component_paths,
        preferred_backends,
        &inspection,
    );
    return inspection;
}

fn componentCompatibilityForBackend(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
    backend: backends.BackendType,
    component_paths: []const []const u8,
    contract: ComponentContract,
) !CompatibilitySummary {
    var inspection = try inspectComponentBundle(
        allocator,
        man,
        component_paths,
        contract,
        &.{backend},
    );
    defer inspection.deinit();
    return inspection.summaryForBackend(backend);
}

fn policyAllowedComponentBackendsFromInspection(
    scratch: *[7]backends.BackendType,
    preferred_backends: []const backends.BackendType,
    policy: model_compatibility.Policy,
    inspection: *const ComponentInspection,
) ![]const backends.BackendType {
    var count: usize = 0;
    var first_policy_err: ?anyerror = null;
    for (preferred_backends) |backend| {
        if (!backend.supportsDirectSessionLoad()) continue;
        const summary = inspection.summaryForBackend(backend);
        const allowed = switch (summary.level) {
            .compatible => true,
            .unknown => policy.allow_unknown,
            .incompatible => false,
        };
        if (allowed) {
            scratch[count] = backend;
            count += 1;
        } else if (first_policy_err == null) {
            first_policy_err = if (summary.level == .unknown)
                error.UnknownModelCompatibility
            else
                error.IncompatibleModel;
        }
    }
    if (count > 0) return scratch[0..count];
    if (first_policy_err) |err| return err;
    return error.NoBackendAvailable;
}

fn shouldUseMetalWholeModelExecutor(session: backends.Session) bool {
    return session.backend() == .metal;
}

fn spinLock(m: *std.atomic.Mutex) void {
    platform.sync.lockYielding(m);
}

pub fn shouldPreferSentencePieceOverride(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return false;
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub fn shouldEnableGemmaSentencePieceCompat(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub fn loadSentencePieceAddedTokens(model_dir: []const u8, allocator: std.mem.Allocator, sp: *sentencepiece.Processor) !void {
    const added_tokens_path = std.fmt.allocPrint(allocator, "{s}/added_tokens.json", .{model_dir}) catch return;
    defer allocator.free(added_tokens_path);
    const added_tokens_bytes = c_file.readFile(allocator, added_tokens_path) catch return;
    defer allocator.free(added_tokens_bytes);
    try loadSentencePieceAddedTokenMap(allocator, added_tokens_bytes, sp);

    const tokenizer_json_path = std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir}) catch return;
    defer allocator.free(tokenizer_json_path);
    const tokenizer_json_bytes = c_file.readFile(allocator, tokenizer_json_path) catch return;
    defer allocator.free(tokenizer_json_bytes);
    try loadSentencePieceAddedTokenArray(allocator, tokenizer_json_bytes, sp);
}

fn loadSentencePieceAddedTokenMap(
    allocator: std.mem.Allocator,
    json_bytes: []const u8,
    sp: *sentencepiece.Processor,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .integer) continue;
        try sp.addExternalSpecialToken(entry.key_ptr.*, @intCast(entry.value_ptr.integer));
    }
}

fn loadSentencePieceAddedTokenArray(
    allocator: std.mem.Allocator,
    json_bytes: []const u8,
    sp: *sentencepiece.Processor,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const added_tokens = parsed.value.object.get("added_tokens") orelse return;
    if (added_tokens != .array) return;
    for (added_tokens.array.items) |item| {
        if (item != .object) continue;
        const content = item.object.get("content") orelse continue;
        const id = item.object.get("id") orelse continue;
        if (content != .string or id != .integer) continue;
        try sp.addExternalSpecialToken(content.string, @intCast(id.integer));
    }
}

fn manifestLooksLikeGemma(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    _ = man;
    if (std.mem.indexOf(u8, model_dir, "gemma") != null) return true;

    const cfg_path = std.fmt.allocPrint(allocator, "{s}/config.json", .{model_dir}) catch return false;
    defer allocator.free(cfg_path);
    const cfg_bytes = c_file.readFile(allocator, cfg_path) catch return false;
    defer allocator.free(cfg_bytes);

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, cfg_bytes, .{}) catch return false;
    defer parsed.deinit();
    const obj = parsed.value.object;
    const model_type = obj.get("model_type") orelse return false;
    if (model_type != .string) return false;
    return std.mem.startsWith(u8, model_type.string, "gemma");
}

fn appendJsonString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    try buf.append(allocator, '"');
    for (value) |c| switch (c) {
        '"' => try buf.appendSlice(allocator, "\\\""),
        '\\' => try buf.appendSlice(allocator, "\\\\"),
        '\n' => try buf.appendSlice(allocator, "\\n"),
        '\r' => try buf.appendSlice(allocator, "\\r"),
        '\t' => try buf.appendSlice(allocator, "\\t"),
        else => {
            if (c < 0x20) {
                const escaped = try std.fmt.allocPrint(allocator, "\\u{X:0>4}", .{@as(u8, c)});
                defer allocator.free(escaped);
                try buf.appendSlice(allocator, escaped);
            } else {
                try buf.append(allocator, c);
            }
        },
    };
    try buf.append(allocator, '"');
}

const LegacyWordPieceMeta = struct {
    do_lower_case: bool = false,
    unk_token: []const u8 = "[UNK]",
    pad_token: []const u8 = "[PAD]",
    cls_token: []const u8 = "[CLS]",
    sep_token: []const u8 = "[SEP]",
    mask_token: []const u8 = "[MASK]",
    unk_token_owned: ?[]u8 = null,
    pad_token_owned: ?[]u8 = null,
    cls_token_owned: ?[]u8 = null,
    sep_token_owned: ?[]u8 = null,
    mask_token_owned: ?[]u8 = null,

    fn deinit(self: *LegacyWordPieceMeta, allocator: std.mem.Allocator) void {
        if (self.unk_token_owned) |buf| allocator.free(buf);
        if (self.pad_token_owned) |buf| allocator.free(buf);
        if (self.cls_token_owned) |buf| allocator.free(buf);
        if (self.sep_token_owned) |buf| allocator.free(buf);
        if (self.mask_token_owned) |buf| allocator.free(buf);
    }
};

fn replaceLegacyToken(allocator: std.mem.Allocator, slot: *[]const u8, owned_slot: *?[]u8, value: []const u8) !void {
    const duped = try allocator.dupe(u8, value);
    if (owned_slot.*) |buf| allocator.free(buf);
    owned_slot.* = duped;
    slot.* = duped;
}

fn extractLegacyTokenString(val: std.json.Value) ?[]const u8 {
    return switch (val) {
        .string => |s| s,
        .object => |obj| blk: {
            if (obj.get("content")) |content| {
                if (content == .string) break :blk content.string;
            }
            break :blk null;
        },
        else => null,
    };
}

fn applyLegacyTokenizerJson(meta: *LegacyWordPieceMeta, json_bytes: []const u8, allocator: std.mem.Allocator) void {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const obj = parsed.value.object;

    if (obj.get("do_lower_case")) |v| {
        if (v == .bool) meta.do_lower_case = v.bool;
    }
    if (obj.get("unk_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.unk_token, &meta.unk_token_owned, s) catch {};
    }
    if (obj.get("pad_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.pad_token, &meta.pad_token_owned, s) catch {};
    }
    if (obj.get("cls_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.cls_token, &meta.cls_token_owned, s) catch {};
    }
    if (obj.get("sep_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.sep_token, &meta.sep_token_owned, s) catch {};
    }
    if (obj.get("mask_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.mask_token, &meta.mask_token_owned, s) catch {};
    }
}

fn appendAddedToken(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    first: *bool,
    token: []const u8,
    id: i64,
) !void {
    if (!first.*) try buf.append(allocator, ',');
    first.* = false;
    try buf.appendSlice(allocator, "{\"id\":");
    const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{id});
    defer allocator.free(id_bytes);
    try buf.appendSlice(allocator, id_bytes);
    try buf.appendSlice(allocator, ",\"content\":");
    try appendJsonString(buf, allocator, token);
    try buf.appendSlice(allocator, ",\"special\":true}");
}

fn loadLegacyWordPieceTokenizerFromDir(allocator: std.mem.Allocator, model_dir: []const u8) !*hf_tokenizer.HfTokenizer {
    const vocab_path = try std.fmt.allocPrint(allocator, "{s}/vocab.txt", .{model_dir});
    defer allocator.free(vocab_path);
    const vocab_bytes = try c_file.readFile(allocator, vocab_path);
    defer allocator.free(vocab_bytes);

    var meta = LegacyWordPieceMeta{};
    defer meta.deinit(allocator);
    var tokenizer_config_bytes_opt: ?[]u8 = null;
    defer if (tokenizer_config_bytes_opt) |bytes| allocator.free(bytes);
    var special_tokens_map_bytes_opt: ?[]u8 = null;
    defer if (special_tokens_map_bytes_opt) |bytes| allocator.free(bytes);

    const tokenizer_config_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer_config.json", .{model_dir});
    defer allocator.free(tokenizer_config_path);
    if (c_file.readFile(allocator, tokenizer_config_path)) |tokenizer_config_bytes| {
        tokenizer_config_bytes_opt = tokenizer_config_bytes;
        applyLegacyTokenizerJson(&meta, tokenizer_config_bytes, allocator);
    } else |_| {}

    const special_tokens_map_path = try std.fmt.allocPrint(allocator, "{s}/special_tokens_map.json", .{model_dir});
    defer allocator.free(special_tokens_map_path);
    if (c_file.readFile(allocator, special_tokens_map_path)) |special_tokens_map_bytes| {
        special_tokens_map_bytes_opt = special_tokens_map_bytes;
        applyLegacyTokenizerJson(&meta, special_tokens_map_bytes, allocator);
    } else |_| {}

    var vocab_entries = std.ArrayListUnmanaged([]const u8).empty;
    defer vocab_entries.deinit(allocator);

    var line_it = std.mem.tokenizeScalar(u8, vocab_bytes, '\n');
    while (line_it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        try vocab_entries.append(allocator, line);
    }

    var unk_id: i64 = -1;
    var pad_id: i64 = -1;
    var cls_id: i64 = -1;
    var sep_id: i64 = -1;
    var mask_id: i64 = -1;
    for (vocab_entries.items, 0..) |token, idx| {
        const id: i64 = @intCast(idx);
        if (std.mem.eql(u8, token, meta.unk_token)) unk_id = id;
        if (std.mem.eql(u8, token, meta.pad_token)) pad_id = id;
        if (std.mem.eql(u8, token, meta.cls_token)) cls_id = id;
        if (std.mem.eql(u8, token, meta.sep_token)) sep_id = id;
        if (std.mem.eql(u8, token, meta.mask_token)) mask_id = id;
    }

    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":{\"type\":\"WordPiece\",\"unk_token\":");
    try appendJsonString(&buf, allocator, meta.unk_token);
    try buf.appendSlice(allocator, ",\"continuing_subword_prefix\":\"##\",\"max_input_chars_per_word\":100,\"vocab\":{");
    for (vocab_entries.items, 0..) |token, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        try appendJsonString(&buf, allocator, token);
        try buf.append(allocator, ':');
        const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        defer allocator.free(id_bytes);
        try buf.appendSlice(allocator, id_bytes);
    }
    try buf.appendSlice(allocator, "}},\"normalizer\":{\"type\":\"BertNormalizer\",\"lowercase\":");
    try buf.appendSlice(allocator, if (meta.do_lower_case) "true" else "false");
    try buf.appendSlice(allocator, "},\"pre_tokenizer\":{\"type\":\"BertPreTokenizer\"},\"added_tokens\":[");

    var first_added = true;
    if (pad_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.pad_token, pad_id);
    if (unk_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.unk_token, unk_id);
    if (cls_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.cls_token, cls_id);
    if (sep_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.sep_token, sep_id);
    if (mask_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.mask_token, mask_id);
    try buf.appendSlice(allocator, "]");

    if (cls_id >= 0 and sep_id >= 0) {
        try buf.appendSlice(allocator, ",\"post_processor\":{\"type\":\"BertProcessing\",\"cls\":[");
        try appendJsonString(&buf, allocator, meta.cls_token);
        const cls_id_bytes = try std.fmt.allocPrint(allocator, ",{d}],\"sep\":[", .{cls_id});
        defer allocator.free(cls_id_bytes);
        try buf.appendSlice(allocator, cls_id_bytes);
        try appendJsonString(&buf, allocator, meta.sep_token);
        const sep_id_bytes = try std.fmt.allocPrint(allocator, ",{d}]", .{sep_id});
        defer allocator.free(sep_id_bytes);
        try buf.appendSlice(allocator, sep_id_bytes);
        try buf.appendSlice(allocator, "}");
    }

    try buf.append(allocator, '}');
    const tokenizer_json = try buf.toOwnedSlice(allocator);
    defer allocator.free(tokenizer_json);
    return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
}

pub fn loadHuggingFaceTokenizerFromDir(allocator: std.mem.Allocator, model_dir: []const u8) !*hf_tokenizer.HfTokenizer {
    return loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, null);
}

pub fn loadHuggingFaceTokenizerFromDirOrGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_path: ?[]const u8,
) !*hf_tokenizer.HfTokenizer {
    const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir});
    defer allocator.free(tok_path);
    if (c_file.readFile(allocator, tok_path)) |tok_bytes| {
        defer allocator.free(tok_bytes);
        return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
    } else |_| {}

    if (c_file.fileExistsInDir(allocator, model_dir, "vocab.txt")) {
        return loadLegacyWordPieceTokenizerFromDir(allocator, model_dir);
    }

    if (gguf_path) |path| {
        return loadHuggingFaceTokenizerFromGguf(allocator, path);
    }

    return error.NoTokenizerFound;
}

fn loadHuggingFaceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !*hf_tokenizer.HfTokenizer {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;

    const flavor: GgufBpeTokenizerFlavor = if (std.mem.eql(u8, model_name, "gpt2"))
        .byte_level
    else if (std.mem.eql(u8, model_name, "gemma4"))
        .gemma4
    else
        return error.NoTokenizerFound;

    const tokenizer_bytes = try bpeTokenizerJsonFromGguf(allocator, &parsed, flavor);
    defer allocator.free(tokenizer_bytes);

    const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    tok.applySpecialTokenIds(
        metadataTokenId(&parsed, "tokenizer.ggml.bos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.eos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.padding_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.unknown_token_id"),
    );
    return tok;
}

const GgufBpeTokenizerFlavor = enum {
    byte_level,
    gemma4,
};

fn bpeTokenizerJsonFromGguf(
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
    flavor: GgufBpeTokenizerFlavor,
) ![]u8 {
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const merges = try getRequiredMetadataArray(parsed, "tokenizer.ggml.merges", .string);
    const token_types = if (findMetadataEntry(parsed, "tokenizer.ggml.token_type") != null)
        try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null)
    else
        null;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);

    switch (flavor) {
        .byte_level => {
            try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"BPE\",\"byte_fallback\":false,\"vocab\":{");
        },
        .gemma4 => {
            try tokenizer_json.appendSlice(
                allocator,
                "{\"normalizer\":{\"type\":\"Replace\",\"pattern\":{\"String\":\" \"},\"content\":\"▁\"},\"pre_tokenizer\":{\"type\":\"Split\",\"pattern\":{\"String\":\" \"},\"behavior\":\"MergedWithPrevious\",\"invert\":false},\"model\":{\"type\":\"BPE\",\"fuse_unk\":true,\"byte_fallback\":true,\"vocab\":{",
            );
        },
    }
    for (tokens.values, 0..) |token_value, idx| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (idx > 0) try tokenizer_json.append(allocator, ',');
        try appendJsonString(&tokenizer_json, allocator, token);
        try tokenizer_json.append(allocator, ':');
        const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        defer allocator.free(id_bytes);
        try tokenizer_json.appendSlice(allocator, id_bytes);
    }
    try tokenizer_json.appendSlice(allocator, "},\"merges\":[");
    for (merges.values, 0..) |merge_value, idx| {
        const merge = switch (merge_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (idx > 0) try tokenizer_json.append(allocator, ',');
        try appendJsonString(&tokenizer_json, allocator, merge);
    }
    switch (flavor) {
        .byte_level => try tokenizer_json.appendSlice(allocator, "]},\"pre_tokenizer\":{\"type\":\"ByteLevel\"},\"added_tokens\":["),
        .gemma4 => try tokenizer_json.appendSlice(allocator, "]},\"added_tokens\":["),
    }

    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]}");

    return tokenizer_json.toOwnedSlice(allocator);
}

fn appendSpecialTokensFromMetadata(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
    tokens: gguf_format.MetadataArray,
    token_types: ?gguf_format.MetadataArray,
) !void {
    var first_added = true;
    var seen = std.AutoHashMapUnmanaged(i64, void){};
    defer seen.deinit(allocator);

    const special_id_keys = [_][]const u8{
        "tokenizer.ggml.bos_token_id",
        "tokenizer.ggml.eos_token_id",
        "tokenizer.ggml.padding_token_id",
        "tokenizer.ggml.unknown_token_id",
    };
    for (special_id_keys) |key| {
        const token_id = metadataTokenId(parsed, key) orelse continue;
        const token = metadataTokenStringById(tokens, token_id) orelse continue;
        if (seen.contains(token_id)) continue;
        try seen.put(allocator, token_id, {});
        try appendAddedToken(buf, allocator, &first_added, token, token_id);
    }

    if (token_types) |types| {
        for (tokens.values, 0..) |token_value, idx| {
            const token = switch (token_value) {
                .string => |value| value,
                else => return error.InvalidTokenizerMetadata,
            };
            const token_type = try metadataI64At(types, idx);
            if (token_type == 1 or token_type == 6) continue;
            const token_id: i64 = @intCast(idx);
            if (seen.contains(token_id)) continue;
            try seen.put(allocator, token_id, {});
            try appendAddedToken(buf, allocator, &first_added, token, token_id);
        }
    }
}

fn metadataTokenId(parsed: *const gguf_format.File, key: []const u8) ?i32 {
    const view = gguf_metadata.View.init(parsed);
    const raw_id = view.getU64(key) orelse return null;
    return std.math.cast(i32, raw_id);
}

fn metadataTokenStringById(tokens: gguf_format.MetadataArray, token_id: i32) ?[]const u8 {
    if (token_id < 0) return null;
    const token_index: usize = @intCast(token_id);
    if (token_index >= tokens.values.len) return null;
    return switch (tokens.values[token_index]) {
        .string => |value| value,
        else => null,
    };
}

fn metadataI64At(arr: gguf_format.MetadataArray, index: usize) !i64 {
    if (index >= arr.values.len) return error.InvalidTokenizerMetadata;
    return switch (arr.values[index]) {
        .i32 => |value| value,
        .i64 => |value| value,
        .u32 => |value| value,
        .u64 => |value| std.math.cast(i64, value) orelse return error.InvalidTokenizerMetadata,
        else => return error.InvalidTokenizerMetadata,
    };
}

fn findMetadataEntry(parsed: *const gguf_format.File, key: []const u8) ?*const gguf_format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

fn manifestRequiresInput(man: *const manifest_mod.ModelManifest, expected: []const u8) bool {
    for (man.inputs) |input| {
        if (std.mem.eql(u8, input, expected)) return true;
    }
    return false;
}

pub fn loadSentencePieceTokenizerFromDirOrGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_path: ?[]const u8,
) !*sentencepiece.Processor {
    const sp = try allocator.create(sentencepiece.Processor);
    errdefer allocator.destroy(sp);

    if (c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) {
        const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
        defer allocator.free(sp_path);
        sp.* = try sentencepiece.Processor.initFromPath(allocator, sp_path);
        return sp;
    }

    const resolved_gguf_path = gguf_path orelse return error.NoTokenizerFound;
    sp.* = try loadSentencePieceTokenizerFromGguf(allocator, resolved_gguf_path);
    return sp;
}

fn loadSentencePieceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !sentencepiece.Processor {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;
    if (!(std.mem.eql(u8, model_name, "llama") or std.mem.startsWith(u8, model_name, "gemma"))) {
        return error.NoTokenizerFound;
    }

    const tokens = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.tokens", .string);
    const scores = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.scores", null);
    const token_types = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.token_type", null);
    if (tokens.values.len != scores.values.len or tokens.values.len != token_types.values.len) {
        return error.InvalidTokenizerMetadata;
    }

    const unknown_token_index = view.getU64("tokenizer.ggml.unknown_token_id");
    const pieces = try allocator.alloc(sentencepiece.PieceInit, tokens.values.len);
    defer allocator.free(pieces);

    var saw_byte_piece = false;
    var saw_unknown_piece = false;
    for (tokens.values, 0..) |token_value, idx| {
        const token_text = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const score = switch (scores.values[idx]) {
            .f32 => |value| value,
            .f64 => |value| @as(f32, @floatCast(value)),
            else => return error.InvalidTokenizerMetadata,
        };
        const token_type_i64 = switch (token_types.values[idx]) {
            .i32 => |value| value,
            .i64 => |value| value,
            .u32 => |value| @as(i64, value),
            .u64 => |value| std.math.cast(i64, value) orelse return error.InvalidTokenizerMetadata,
            else => return error.InvalidTokenizerMetadata,
        };
        if (token_type_i64 < 0 or token_type_i64 > std.math.maxInt(u8)) {
            return error.InvalidTokenizerMetadata;
        }
        var token_type: u8 = @intCast(token_type_i64);
        if (unknown_token_index) |unknown_id| {
            if (unknown_id == idx) token_type = 2;
        }
        if (token_type == 6) saw_byte_piece = true;
        if (token_type == 2) saw_unknown_piece = true;
        pieces[idx] = .{
            .text = token_text,
            .score = score,
            .piece_type = token_type,
        };
    }
    if (!saw_unknown_piece) return error.InvalidTokenizerMetadata;

    const add_dummy_prefix = view.getBool("tokenizer.ggml.add_space_prefix") orelse true;
    const remove_extra_whitespaces = view.getBool("tokenizer.ggml.remove_extra_whitespaces") orelse true;
    const unk_surface = blk: {
        const unk_id = unknown_token_index orelse break :blk " \xe2\x81\x87 ";
        if (unk_id >= tokens.values.len) break :blk " \xe2\x81\x87 ";
        break :blk switch (tokens.values[@intCast(unk_id)]) {
            .string => |value| value,
            else => " \xe2\x81\x87 ",
        };
    };

    return sentencepiece.Processor.initFromPieces(allocator, pieces, .{
        .byte_fallback = saw_byte_piece,
        .unk_surface = unk_surface,
        .add_dummy_prefix = add_dummy_prefix,
        .remove_extra_whitespaces = remove_extra_whitespaces,
    });
}

fn getRequiredMetadataArray(
    parsed: *const gguf_format.File,
    key: []const u8,
    expected_element_type: ?gguf_format.MetadataValueType,
) !gguf_format.MetadataArray {
    for (parsed.metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, key)) continue;
        const arr = switch (entry.value) {
            .array => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (expected_element_type) |elem_type| {
            if (arr.element_type != elem_type) return error.InvalidTokenizerMetadata;
        }
        return arr;
    }
    return error.InvalidTokenizerMetadata;
}

pub fn isModelDirPotentiallyLoadableInCurrentBuild(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    var man = manifest_mod.loadFromDir(allocator, model_dir) catch return false;
    defer man.deinit();
    return isManifestPotentiallyLoadableInCurrentBuild(man);
}

pub fn isManifestPotentiallyLoadableInCurrentBuild(man: manifest_mod.ModelManifest) bool {
    if (man.hasIncompleteGlinerBundle()) return false;
    if (man.hasIncompleteColqwenBundle()) return false;
    if (man.hasIncompleteClipclapGgufBundle()) return false;
    if (man.hasIncompleteFlorence2GgufBundle()) return false;
    if (man.onnx_path != null or
        man.visual_model_path != null or
        man.audio_model_path != null or
        man.text_projection_path != null or
        man.visual_projection_path != null or
        man.audio_projection_path != null)
    {
        return true;
    }
    if (nativeBackendsAvailable() and manifestHasNativeAssets(man)) {
        return true;
    }
    return false;
}

pub const LoadedModel = struct {
    manifest: manifest_mod.ModelManifest,
    hf_tok: ?*hf_tokenizer.HfTokenizer,
    sp_tok: ?*sentencepiece.Processor,
    session: backends.Session,
    session_manager: *backends.SessionManager,
    model_manager: *ModelManager,
    model_dir: []const u8,
    allocator: std.mem.Allocator,
    chat_tmpl: ?*ChatTemplate = null,
    /// The model shipped a chat template that we could not parse, so chat requests fall
    /// back to raw prompting. Distinct from `chat_tmpl == null` with no template at all.
    chat_template_failed: bool = false,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = null,
    prompt_prefix_cache: runtime.kv.prompt_cache.PromptPrefixCache,
    native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    native_generation_graph_cache: graph_mod.cache.GraphCache,
    // ponytail: per-model native generation lock; replace with Metal-safe batching if throughput matters.
    native_generate_lock: std.atomic.Mutex = .unlocked,
    // Multimodal sessions (CLIP/CLAP/CLIPCLAP)
    embedding_session_lock: std.atomic.Mutex = .unlocked,
    reranking_session_lock: std.atomic.Mutex = .unlocked,
    vision_session: ?backends.Session = null,
    audio_session: ?backends.Session = null,
    text_projection: ?backends.Session = null,
    visual_projection: ?backends.Session = null,
    audio_projection: ?backends.Session = null,
    vision_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    text_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    visual_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    /// Accounts for the tokenizer's resident vocabulary, maps, tries, and
    /// parser-owned state independently of backend weight residency.
    tokenizer_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    resident_projection_stats: embedding_mod.AtomicResidentProjectionStats = .{},
    cleanup_head: ?*cleanup_model_mod.CleanupHead = null,
    cleanup_head_loaded: bool = false,
    resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    /// Protected by ModelManager.load_lock. A model can only be evicted while
    /// it has no active handles and is not pinned by preload configuration.
    active_handles: usize = 0,
    last_used_ns: u64 = 0,
    pinned: bool = false,

    pub fn getTokenizer(self: *LoadedModel) tokenizer_mod.Tokenizer {
        if (self.hf_tok) |ht| return ht.tokenizer();
        if (self.sp_tok) |sp| return sp.tokenizer();
        unreachable;
    }

    pub fn attachIo(self: *LoadedModel, io: std.Io) void {
        session_factory.attachIo(self.session, io);
        if (self.vision_session) |session| session_factory.attachIo(session, io);
        if (self.audio_session) |session| session_factory.attachIo(session, io);
        if (self.text_projection) |session| session_factory.attachIo(session, io);
        if (self.visual_projection) |session| session_factory.attachIo(session, io);
        if (self.audio_projection) |session| session_factory.attachIo(session, io);
    }

    pub fn lockNativeGeneration(self: *LoadedModel) void {
        spinLock(&self.native_generate_lock);
    }

    pub fn unlockNativeGeneration(self: *LoadedModel) void {
        self.native_generate_lock.unlock();
    }

    pub fn wholeModelExecutor(self: *LoadedModel, allocator: std.mem.Allocator, kv_dtype: ?runtime.kv.pool.KvDType) !?graph_mod.model_runtime.ModelExecutor {
        const gpt_config = session_factory.getGptConfig(self.session) orelse return null;
        if (build_options.enable_metal and shouldUseMetalWholeModelExecutor(self.session) and graph_mod.metal_executor.supportsSession(self.session)) {
            return try graph_mod.metal_executor.createModelExecutor(
                allocator,
                self.session,
                gpt_config,
                kv_dtype,
                self.shared_moe_cache,
            );
        }
        if (!graph_mod.live_model_executor.supportsSession(self.session)) return null;
        return try graph_mod.live_model_executor.createModelExecutor(
            allocator,
            self.session,
            gpt_config,
            kv_dtype,
            self.shared_moe_cache,
        );
    }

    fn ensureOptionalSession(
        self: *LoadedModel,
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
        path: ?[]const u8,
    ) !void {
        if (slot.* != null) return;
        const session_path = path orelse return;
        const shared_ctx = backends.imported_onnx_session.sharedBackendContext(self.session);
        const strict_backend = [_]backends.BackendType{if (shared_ctx) |shared|
            shared.backendType()
        else
            self.session.backend()};
        var loaded = try self.model_manager.loadManagedSessionWithAdmission(
            session_path,
            strict_backend[0..],
            shared_ctx,
        );
        slot.* = loaded.session;
        lease_slot.* = loaded.resource_lease;
        loaded.owns_session = false;
        loaded.resource_lease = null;
    }

    pub fn ensureVisionSession(self: *LoadedModel) !void {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();
        try self.ensureOptionalSession(
            &self.vision_session,
            &self.vision_resource_lease,
            self.manifest.visual_model_path,
        );
    }

    pub fn ensureEmbeddingAssets(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();

        if (include_text) {
            try self.ensureOptionalSession(
                &self.text_projection,
                &self.text_projection_resource_lease,
                self.manifest.text_projection_path,
            );
        }
        if (include_image) {
            try self.ensureOptionalSession(
                &self.vision_session,
                &self.vision_resource_lease,
                self.manifest.visual_model_path,
            );
            try self.ensureOptionalSession(
                &self.visual_projection,
                &self.visual_projection_resource_lease,
                self.manifest.visual_projection_path,
            );
        }
        if (include_audio) {
            try self.ensureOptionalSession(
                &self.audio_session,
                &self.audio_resource_lease,
                self.manifest.audio_model_path,
            );
            try self.ensureOptionalSession(
                &self.audio_projection,
                &self.audio_projection_resource_lease,
                self.manifest.audio_projection_path,
            );
        }
    }

    pub fn embeddingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        const tok = self.getTokenizer();
        var pipeline = EmbeddingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .normalize = self.manifest.normalize,
            .pooling = switch (self.manifest.pooling) {
                .mean => .mean,
                .cls => .cls,
                .max => .max,
                .last => .last,
            },
            .text_prefix = self.manifest.embedding_text_prefix,
            .trim_padding_to_batch_max = isJinaStyleEmbeddingManifest(&self.manifest),
            .resident_qwen3_embedding = isJinaStyleEmbeddingManifest(&self.manifest),
        });
        if (usesClipImagePreprocessProfile(&self.manifest)) {
            pipeline.config.image_preprocess_profile = .clip;
        }
        if (session_factory.getClipConfig(self.session)) |cfg| {
            pipeline.config.image_size = cfg.image_size;
            if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
        } else if (self.vision_session) |vs| {
            if (session_factory.getClipConfig(vs)) |cfg| {
                pipeline.config.image_size = cfg.image_size;
                if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
            }
        }
        pipeline.vision_session = self.vision_session;
        pipeline.audio_session = self.audio_session;
        pipeline.text_projection = self.text_projection;
        pipeline.visual_projection = self.visual_projection;
        pipeline.audio_projection = self.audio_projection;
        pipeline.resident_projection_stats = &self.resident_projection_stats;
        return pipeline;
    }

    pub fn rerankingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) RerankingPipeline {
        const tok = self.getTokenizer();
        return RerankingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .mode = if (self.manifest.hasCapability("late_interaction") or
                self.manifest.hasCapability("colbert") or
                self.manifest.hasCapability("colqwen") or
                self.manifest.hasCapability("multimodal_late_interaction"))
                ScoringMode.late_interaction
            else
                ScoringMode.cross_encoder,
            .single_text_encoding = if (self.manifest.prefersGenerationEncodingForLateInteraction()) .generation else .encoder,
            .add_bos_token = self.manifest.add_bos_token,
            .distributed = runtime.distributed.configFromEnv(),
        });
    }

    pub fn lockRerankingSession(self: *LoadedModel) void {
        spinLock(&self.reranking_session_lock);
    }

    pub fn unlockRerankingSession(self: *LoadedModel) void {
        self.reranking_session_lock.unlock();
    }

    pub fn classificationPipeline(self: *LoadedModel, allocator: std.mem.Allocator, config: ClassificationConfig) ClassificationPipeline {
        const tok = self.getTokenizer();
        var effective = config;
        effective.distributed = runtime.distributed.configFromEnv();
        return ClassificationPipeline.init(allocator, self.session, tok, effective);
    }

    pub fn nerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) NerPipeline {
        const tok = self.getTokenizer();
        // Cast id2label from ?[][]const u8 to ?[]const []const u8
        const id2label: ?[]const []const u8 = if (self.manifest.id2label) |labels| labels else null;
        return NerPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .id2label = id2label,
            .distributed = runtime.distributed.configFromEnv(),
        });
    }

    pub fn isGlinerModel(self: *LoadedModel) bool {
        return self.manifest.gliner_model_type.len > 0;
    }

    pub fn supportsClassification(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "classification",
        );
    }

    pub fn supportsExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "extraction",
        );
    }

    pub fn supportsRelationExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "relations",
        );
    }

    pub fn glinerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) GlinerPipeline {
        const tok = self.getTokenizer();
        return .{
            .allocator = allocator,
            .session = self.session,
            .tok = tok,
            .config = .{
                .max_width = self.manifest.gliner_max_width,
                .max_length = self.manifest.max_position_embeddings,
                .threshold = self.manifest.gliner_threshold,
                .flat_ner = self.manifest.gliner_flat_ner,
                .default_labels = self.manifest.gliner_default_labels,
                .relation_labels = self.manifest.gliner_relation_labels,
                .relation_threshold = self.manifest.gliner_relation_threshold,
                .model_type = self.manifest.gliner_model_type,
                .capabilities = self.manifest.capabilities,
                .token_p = self.manifest.gliner_token_p,
                .token_c = self.manifest.gliner_token_c,
                .token_e = self.manifest.gliner_token_e,
                .token_r = self.manifest.gliner_token_r,
                .token_sep_text = self.manifest.gliner_token_sep_text,
                .distributed = runtime.distributed.configFromEnv(),
            },
        };
    }

    pub fn getCleanupHead(self: *LoadedModel) !?*const cleanup_model_mod.CleanupHead {
        if (self.cleanup_head_loaded) return self.cleanup_head;

        const loaded = (try cleanup_model_mod.loadHeadIfPresent(self.allocator, self.model_dir)) orelse {
            self.cleanup_head_loaded = true;
            return null;
        };
        const head = try self.allocator.create(cleanup_model_mod.CleanupHead);
        head.* = loaded;
        self.cleanup_head = head;
        self.cleanup_head_loaded = true;
        return head;
    }

    pub fn deinit(self: *LoadedModel) void {
        self.native_generation_graph_cache.deinit();
        self.prompt_prefix_cache.deinit();
        self.session.close();
        if (self.vision_session) |vs| vs.close();
        if (self.audio_session) |as_| as_.close();
        if (self.text_projection) |tp| tp.close();
        if (self.visual_projection) |vp| vp.close();
        if (self.audio_projection) |ap| ap.close();
        if (self.vision_resource_lease) |*lease| lease.release();
        if (self.audio_resource_lease) |*lease| lease.release();
        if (self.text_projection_resource_lease) |*lease| lease.release();
        if (self.visual_projection_resource_lease) |*lease| lease.release();
        if (self.audio_projection_resource_lease) |*lease| lease.release();
        if (self.hf_tok) |ht| ht.deinitSelf();
        if (self.sp_tok) |sp| {
            sp.deinit();
            self.allocator.destroy(sp);
        }
        if (self.tokenizer_resource_lease) |*lease| lease.release();
        if (self.chat_tmpl) |ct| {
            var ct_mut = @constCast(ct);
            ct_mut.deinit();
            self.allocator.destroy(ct_mut);
        }
        if (self.shared_moe_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        }
        if (self.shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        }
        if (self.native_generate_coordinator) |coordinator| {
            coordinator.deinit();
            self.allocator.destroy(coordinator);
        }
        if (self.cleanup_head) |head| {
            head.deinit();
            self.allocator.destroy(head);
        }
        if (self.resource_lease) |*lease| lease.release();
        self.manifest.deinit();
        self.allocator.free(self.model_dir);
    }
};

fn isJinaStyleEmbeddingManifest(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "jina_embeddings_v5") or
        (manifest.pooling == .last and std.mem.eql(u8, manifest.embedding_text_prefix, "Document: "));
}

fn usesClipImagePreprocessProfile(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "clip") or
        std.mem.eql(u8, manifest.config_model_arch, "clipclap") or
        manifest.isClipclapGgufBundle();
}

const LoadFlight = struct {
    completed: std.Io.Event = .unset,
    io: std.Io,
    model: ?*LoadedModel = null,
    err: ?anyerror = null,
    /// Protected by ModelManager.load_lock. The owner starts with one reference;
    /// every waiter takes one before dropping the manager lock.
    refs: usize = 1,
};

fn admissionBackendClassForRuntime(
    backend_runtime: backends.BackendRuntime,
) runtime.tier.memory.BackendClass {
    return if (backend_runtime.usesGpuHostedSession()) .gpu else .cpu;
}

fn admittedSessionCudaLimit(
    backend_runtime: backends.BackendRuntime,
    resident: runtime.tier.memory.AdmissionAmounts,
) !?usize {
    if (backend_runtime.backend != .onnx or
        backend_runtime.onnx_execution_provider != .cuda)
    {
        return null;
    }
    return try std.math.add(
        usize,
        resident.backend_weight_bytes,
        resident.backend_scratch_bytes,
    );
}

fn attachSessionRunAdmission(
    session: *backends.Session,
    manager: *ModelManager,
    backend_runtime: backends.BackendRuntime,
    limits: runtime.tier.memory.Limits,
    resident: runtime.tier.memory.AdmissionAmounts,
    man: ?*const manifest_mod.ModelManifest,
) void {
    const weight_bytes = std.math.add(
        usize,
        resident.host_weight_bytes,
        resident.backend_weight_bytes,
    ) catch std.math.maxInt(usize);
    session.run_admission = .{
        .context = manager,
        .acquire_fn = acquireSessionRunResources,
        .backend_class = admissionBackendClassForRuntime(backend_runtime),
        .limits = limits,
        .static_workspace_bytes = modelRunWorkspaceAllowance(weight_bytes),
        .backend_workspace_reserved = backend_runtime.backend == .onnx and
            backend_runtime.onnx_execution_provider == .cuda,
        .model_profile = if (man) |manifest| .{
            .hidden_size = manifest.hidden_size,
            .intermediate_size = manifest.intermediate_size,
            .attention_heads = manifest.num_attention_heads,
            .quadratic_attention = backend_runtime.backend == .onnx and
                backend_runtime.onnx_execution_provider != .cuda,
        } else .{},
    };
}

fn acquireSessionRunResources(
    context: *anyopaque,
    backend_class: runtime.tier.memory.BackendClass,
    limits: runtime.tier.memory.Limits,
    amounts: runtime.tier.memory.AdmissionAmounts,
) !runtime.tier.memory.AdmissionLease {
    const manager: *ModelManager = @ptrCast(@alignCast(context));
    return manager.acquireAmountsWithEviction(backend_class, limits, amounts);
}

pub const ModelHandle = struct {
    manager: *ModelManager,
    model: ?*LoadedModel,

    pub fn get(self: *const ModelHandle) *LoadedModel {
        return self.model orelse unreachable;
    }

    pub fn pin(self: *ModelHandle) void {
        const model = self.model orelse return;
        self.manager.lockLoadedModels();
        model.pinned = true;
        self.manager.unlockLoadedModels();
    }

    pub fn release(self: *ModelHandle) void {
        const model = self.model orelse return;
        self.manager.lockLoadedModels();
        std.debug.assert(model.active_handles > 0);
        model.active_handles -= 1;
        model.last_used_ns = platform.time.monotonicNs();
        self.manager.unlockLoadedModels();
        self.model = null;
    }
};

pub const ModelManager = struct {
    const EvictedModel = struct {
        key: []const u8,
        model: *LoadedModel,
    };

    allocator: std.mem.Allocator,
    session_manager: backends.SessionManager,
    /// Null for diagnostic/offline CLIs. Serving Nodes set this before any model load.
    serving_policy: ?model_compatibility.Policy = null,
    admission: runtime.tier.memory.AdmissionController = .{},
    admission_enabled: bool = false,
    admission_limit_overrides: runtime.tier.memory.Limits = .{},
    loaded: std.StringHashMapUnmanaged(*LoadedModel),
    loaded_aliases: std.StringHashMapUnmanaged(*LoadedModel),
    /// Protects loaded-model maps and the in-flight registry. Expensive tokenizer,
    /// weight, and backend initialization always runs outside this lock.
    load_lock: std.atomic.Mutex = .unlocked,
    /// Serializes selecting and destroying eviction victims. Destruction
    /// releases admission leases outside load_lock and may be expensive.
    eviction_lock: std.atomic.Mutex = .unlocked,
    keep_alive_ms: u64 = 0,
    max_loaded_models: usize = 0,
    eviction_group: std.Io.Group = .init,
    eviction_io: ?std.Io = null,
    eviction_loop_started: bool = false,
    in_flight_loads: std.StringHashMapUnmanaged(*LoadFlight) = .empty,
    component_plan_cache: std.AutoHashMapUnmanaged(
        ComponentPlanKey,
        *ComponentPlanCacheEntry,
    ) = .empty,
    component_plan_cache_lock: std.atomic.Mutex = .unlocked,
    tokenizer_cache_config: hf_tokenizer.HfTokenizer.BpeCacheConfig = .{},
    tokenizer_parallel_bpe_config: hf_tokenizer.HfTokenizer.ParallelBpeConfig = .{},

    pub fn init(allocator: std.mem.Allocator, session_manager: backends.SessionManager) ModelManager {
        return .{
            .allocator = allocator,
            .session_manager = session_manager,
            .loaded = std.StringHashMapUnmanaged(*LoadedModel){},
            .loaded_aliases = std.StringHashMapUnmanaged(*LoadedModel){},
        };
    }

    pub fn configureServingPolicy(self: *ModelManager, policy: model_compatibility.Policy) void {
        std.debug.assert(self.loaded.count() == 0);
        self.serving_policy = policy;
        self.admission_enabled = true;
        self.admission.configureSharedLimits(
            runtime.tier.memory.sharedAdmissionLimitsWithOverrides(
                self.admission_limit_overrides,
            ),
        );
    }

    pub fn configureModelCache(
        self: *ModelManager,
        keep_alive_ms: u64,
        max_loaded_models: usize,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(!self.eviction_loop_started);
        self.keep_alive_ms = keep_alive_ms;
        self.max_loaded_models = max_loaded_models;
    }

    pub fn acquireRunResources(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        estimate: runtime.tier.memory.Estimate,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireAmountsWithEviction(
            backend_class,
            limits,
            .fromEstimate(estimate),
        );
    }

    /// Atomically admits all transient resources for one execution. Speculative
    /// generation must acquire target and draft KV/scratch together so concurrent
    /// requests cannot each pass admission using only a partial estimate.
    pub fn acquireRunResourceEstimates(
        self: *ModelManager,
        requests: []const runtime.tier.memory.AdmissionRequest,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireRequestsWithEviction(requests);
    }

    pub fn configureAdmissionLimits(
        self: *ModelManager,
        overrides: runtime.tier.memory.Limits,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        self.admission_limit_overrides = overrides;
        self.admission.configureSharedLimits(
            runtime.tier.memory.sharedAdmissionLimitsWithOverrides(overrides),
        );
    }

    pub fn configureAdmissionResourceBudget(
        self: *ModelManager,
        resource_budget: ?runtime.tier.memory.AdmissionResourceBudget,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        self.admission.configureResourceBudget(resource_budget);
    }

    fn modelIsInFlightLocked(self: *ModelManager, model: *LoadedModel) bool {
        var it = self.in_flight_loads.valueIterator();
        while (it.next()) |flight_ptr| {
            if (flight_ptr.*.model == model) return true;
        }
        return false;
    }

    fn takeLruModelLocked(
        self: *ModelManager,
        now_ns: u64,
        expired_only: bool,
    ) ?EvictedModel {
        const ttl_ns = std.math.mul(
            u64,
            self.keep_alive_ms,
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
        var victim_key: ?[]const u8 = null;
        var victim: ?*LoadedModel = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.active_handles != 0 or model.pinned or
                self.modelIsInFlightLocked(model))
            {
                continue;
            }
            const age_ns = if (now_ns >= model.last_used_ns)
                now_ns - model.last_used_ns
            else
                0;
            if (expired_only and
                (self.keep_alive_ms == 0 or age_ns < ttl_ns))
            {
                continue;
            }
            if (victim == null or model.last_used_ns < oldest_ns) {
                victim_key = entry.key_ptr.*;
                victim = model;
                oldest_ns = model.last_used_ns;
            }
        }
        const selected = victim orelse return null;
        const removed = self.loaded.fetchRemove(victim_key.?) orelse unreachable;
        std.debug.assert(removed.value == selected);

        // Aliases never own the model, but every alias must disappear in the
        // same map critical section as its canonical backend-variant key.
        while (true) {
            var alias_key: ?[]const u8 = null;
            var alias_it = self.loaded_aliases.iterator();
            while (alias_it.next()) |entry| {
                if (entry.value_ptr.* == selected) {
                    alias_key = entry.key_ptr.*;
                    break;
                }
            }
            const key = alias_key orelse break;
            const alias = self.loaded_aliases.fetchRemove(key) orelse unreachable;
            self.allocator.free(alias.key);
        }
        return .{ .key = removed.key, .model = selected };
    }

    fn destroyEvictedModel(self: *ModelManager, evicted: EvictedModel) void {
        std.log.info("evicting inference model path={s} backend={s}", .{
            evicted.model.model_dir,
            @tagName(evicted.model.session.backend()),
        });
        evicted.model.deinit();
        self.allocator.destroy(evicted.model);
        self.allocator.free(evicted.key);
    }

    fn acquireAmountsWithEviction(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        amounts: runtime.tier.memory.AdmissionAmounts,
    ) !runtime.tier.memory.AdmissionLease {
        return self.admission.tryAcquire(
            backend_class,
            limits,
            amounts,
            true,
        ) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admission.tryAcquire(
                        backend_class,
                        limits,
                        amounts,
                        true,
                    )) |lease| return lease else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => return retry_err,
                    }
                    self.lockLoadedModels();
                    const evicted = self.takeLruModelLocked(
                        platform.time.monotonicNs(),
                        false,
                    );
                    self.unlockLoadedModels();
                    if (evicted == null) return error.ResourceTemporarilyUnavailable;
                    self.destroyEvictedModel(evicted.?);
                }
            },
            else => return first_err,
        };
    }

    fn acquireRequestsWithEviction(
        self: *ModelManager,
        requests: []const runtime.tier.memory.AdmissionRequest,
    ) !runtime.tier.memory.AdmissionLease {
        return self.admission.tryAcquireRequests(requests, true) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admission.tryAcquireRequests(requests, true)) |lease|
                        return lease
                    else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => return retry_err,
                    }
                    self.lockLoadedModels();
                    const evicted = self.takeLruModelLocked(
                        platform.time.monotonicNs(),
                        false,
                    );
                    self.unlockLoadedModels();
                    if (evicted == null) return error.ResourceTemporarilyUnavailable;
                    self.destroyEvictedModel(evicted.?);
                }
            },
            else => return first_err,
        };
    }

    fn evictExpired(self: *ModelManager) void {
        if (self.keep_alive_ms == 0) return;
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        while (true) {
            self.lockLoadedModels();
            const evicted = self.takeLruModelLocked(
                platform.time.monotonicNs(),
                true,
            );
            self.unlockLoadedModels();
            if (evicted == null) return;
            self.destroyEvictedModel(evicted.?);
        }
    }

    fn evictionLoop(self: *ModelManager, io: std.Io) std.Io.Cancelable!void {
        const interval_ms = @max(
            @as(u64, 10),
            @min(@max(self.keep_alive_ms / 4, 1), @as(u64, 30_000)),
        );
        while (true) {
            try io.sleep(
                std.Io.Duration.fromMilliseconds(@intCast(interval_ms)),
                .awake,
            );
            self.evictExpired();
        }
    }

    /// A short-lived, policy-validated handle for loading every graph in a
    /// composite model bundle. Creating the loader evaluates serving policy
    /// once; every session then uses the same allowed backend order, injected
    /// graph runtime, and process-wide admission controller.
    pub const ComponentLoader = struct {
        const max_component_paths = 32;
        const PathDigest = [std.crypto.hash.sha2.Sha256.digest_length]u8;

        manager: *ModelManager,
        allowed_backends: [7]backends.BackendType = undefined,
        allowed_backend_count: usize = 0,
        component_path_digests: [max_component_paths]PathDigest = undefined,
        component_path_count: usize = 0,

        pub fn preferredBackends(self: *const ComponentLoader) []const backends.BackendType {
            return self.allowed_backends[0..self.allowed_backend_count];
        }

        fn addComponentPath(self: *ComponentLoader, path: []const u8) !void {
            var digest: PathDigest = undefined;
            std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
            for (self.component_path_digests[0..self.component_path_count]) |existing| {
                if (std.mem.eql(u8, existing[0..], digest[0..])) return;
            }
            if (self.component_path_count == self.component_path_digests.len)
                return error.TooManyModelComponents;
            self.component_path_digests[self.component_path_count] = digest;
            self.component_path_count += 1;
        }

        fn ensureComponentPath(self: *const ComponentLoader, path: []const u8) !void {
            var digest: PathDigest = undefined;
            std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
            for (self.component_path_digests[0..self.component_path_count]) |expected| {
                if (std.mem.eql(u8, expected[0..], digest[0..])) return;
            }
            return error.UnvalidatedModelComponent;
        }

        pub fn restrictToBackend(
            self: *const ComponentLoader,
            backend: backends.BackendType,
        ) !ComponentLoader {
            for (self.preferredBackends()) |allowed| {
                if (allowed == backend) {
                    var restricted = self.*;
                    restricted.allowed_backends[0] = backend;
                    restricted.allowed_backend_count = 1;
                    return restricted;
                }
            }
            return error.IncompatibleModel;
        }

        pub fn load(
            self: *const ComponentLoader,
            model_path: []const u8,
        ) !ManagedSession {
            try self.ensureComponentPath(model_path);
            return self.manager.loadManagedSessionWithAdmission(
                model_path,
                self.preferredBackends(),
                null,
            );
        }

        pub fn loadWithImportedOnnxContext(
            self: *const ComponentLoader,
            model_path: []const u8,
            shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
        ) !ManagedSession {
            try self.ensureComponentPath(model_path);
            return self.manager.loadManagedSessionWithAdmission(
                model_path,
                self.preferredBackends(),
                shared_backend_ctx,
            );
        }

        /// Load a composite-model tokenizer through ModelManager so it receives
        /// the same admission, cache, and parallel-BPE policy as primary models.
        pub fn loadHfTokenizerFile(
            self: *const ComponentLoader,
            tokenizer_path: []const u8,
        ) !ManagedHfTokenizer {
            return self.manager.loadManagedHfTokenizerFile(tokenizer_path);
        }

        /// Adapter for SessionPool's lazy factory. The pool takes ownership of
        /// a heap copy of this immutable plan, so lazy loads cannot retain a
        /// pointer to a caller's stack-local ComponentLoader.
        pub fn sessionPoolLoader(self: *const ComponentLoader) !backends.SessionPool.Loader {
            const context = try self.manager.allocator.create(PoolLoaderContext);
            context.* = .{
                .allocator = self.manager.allocator,
                .loader = self.*,
            };
            return .{
                .context = context,
                .load_fn = loadForSessionPool,
                .deinit_fn = deinitSessionPoolLoader,
            };
        }

        fn loadForSessionPool(
            context: *anyopaque,
            model_path: []const u8,
        ) !backends.SessionPool.OwnedSession {
            const pool_context: *PoolLoaderContext = @ptrCast(@alignCast(context));
            const managed = try pool_context.loader.load(model_path);
            const boxed = pool_context.allocator.create(PoolManagedSession) catch |err| {
                var cleanup = managed;
                cleanup.deinit();
                return err;
            };
            boxed.* = .{
                .allocator = pool_context.allocator,
                .managed = managed,
            };
            return .{
                .session = managed.session,
                .close_context = boxed,
                .close_fn = closeSessionPoolSession,
            };
        }

        fn deinitSessionPoolLoader(context: *anyopaque) void {
            const pool_context: *PoolLoaderContext = @ptrCast(@alignCast(context));
            const allocator = pool_context.allocator;
            allocator.destroy(pool_context);
        }

        fn closeSessionPoolSession(context: ?*anyopaque, _: backends.Session) void {
            const boxed: *PoolManagedSession = @ptrCast(@alignCast(context.?));
            const allocator = boxed.allocator;
            boxed.managed.deinit();
            allocator.destroy(boxed);
        }

        const PoolManagedSession = struct {
            allocator: std.mem.Allocator,
            managed: ManagedSession,
        };

        const PoolLoaderContext = struct {
            allocator: std.mem.Allocator,
            loader: ComponentLoader,
        };
    };

    fn componentPlanIo(self: *ModelManager) std.Io {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return self.session_manager.io orelse std.Io.Threaded.global_single_threaded.io();
    }

    fn applyCachedComponentPlan(
        self: *ModelManager,
        key: ComponentPlanKey,
        loader: *ComponentLoader,
    ) !bool {
        spinLock(&self.component_plan_cache_lock);
        const entry = self.component_plan_cache.get(key) orelse {
            self.component_plan_cache_lock.unlock();
            return false;
        };
        entry.retain();
        self.component_plan_cache_lock.unlock();
        defer entry.release();

        const signature = try componentDependencySignature(
            self.componentPlanIo(),
            entry.dependencies,
        );
        if (!std.mem.eql(u8, signature[0..], entry.signature[0..])) return false;
        @memcpy(
            loader.allowed_backends[0..entry.allowed_backend_count],
            entry.allowed_backends[0..entry.allowed_backend_count],
        );
        loader.allowed_backend_count = entry.allowed_backend_count;
        return true;
    }

    fn publishComponentPlan(
        self: *ModelManager,
        key: ComponentPlanKey,
        signature: ComponentPlanKey,
        allowed: []const backends.BackendType,
        dependencies: []const []const u8,
    ) !void {
        const entry = try ComponentPlanCacheEntry.create(
            self.allocator,
            signature,
            allowed,
            dependencies,
        );
        var replaced: ?*ComponentPlanCacheEntry = null;
        var evicted: ?*ComponentPlanCacheEntry = null;

        spinLock(&self.component_plan_cache_lock);
        if (!self.component_plan_cache.contains(key) and
            self.component_plan_cache.count() >= component_plan_cache_capacity)
        {
            var it = self.component_plan_cache.iterator();
            if (it.next()) |candidate| {
                const removed = self.component_plan_cache.fetchRemove(candidate.key_ptr.*);
                if (removed) |old| evicted = old.value;
            }
        }
        const previous = self.component_plan_cache.fetchPut(
            self.allocator,
            key,
            entry,
        ) catch |err| {
            self.component_plan_cache_lock.unlock();
            entry.release();
            if (evicted) |old| old.release();
            return err;
        };
        if (previous) |old| replaced = old.value;
        self.component_plan_cache_lock.unlock();
        if (replaced) |old| old.release();
        if (evicted) |old| old.release();
    }

    pub fn componentLoaderForPaths(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        component_paths: []const []const u8,
    ) !ComponentLoader {
        return self.componentLoaderForPathsWithContract(
            model_dir,
            preferred_backends,
            component_paths,
            .manifest,
        );
    }

    pub fn componentLoaderForPathsWithContract(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        component_paths: []const []const u8,
        contract: ComponentContract,
    ) !ComponentLoader {
        var loader = ComponentLoader{ .manager = self };
        for (component_paths) |path| try loader.addComponentPath(path);
        if (loader.component_path_count == 0) return error.IncompleteModelBundle;
        var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
        defer man.deinit();
        const allowed = if (self.serving_policy) |policy| blk: {
            const key = componentPlanKey(
                model_dir,
                &man,
                preferred_backends,
                component_paths,
                policy,
                contract,
            );
            if (try self.applyCachedComponentPlan(key, &loader))
                return loader;

            var inspection = try inspectComponentArtifacts(
                self.allocator,
                &man,
                component_paths,
                contract,
            );
            defer inspection.deinit();
            const signature_before = try componentDependencySignature(
                self.componentPlanIo(),
                inspection.dependencies.items,
            );
            try validateComponentNativeArtifacts(
                self.allocator,
                component_paths,
                preferred_backends,
                &inspection,
            );
            try validateComponentImportedGraphs(
                self.allocator,
                component_paths,
                preferred_backends,
                &inspection,
            );
            const signature_after = try componentDependencySignature(
                self.componentPlanIo(),
                inspection.dependencies.items,
            );
            if (!std.mem.eql(
                u8,
                signature_before[0..],
                signature_after[0..],
            )) return error.ModelArtifactsChanging;
            const validated = try policyAllowedComponentBackendsFromInspection(
                &loader.allowed_backends,
                preferred_backends,
                policy,
                &inspection,
            );
            self.publishComponentPlan(
                key,
                signature_after,
                validated,
                inspection.dependencies.items,
            ) catch {};
            break :blk validated;
        } else preferred_backends;
        for (allowed) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            if (loader.allowed_backend_count == loader.allowed_backends.len) break;
            loader.allowed_backends[loader.allowed_backend_count] = backend;
            loader.allowed_backend_count += 1;
        }
        if (loader.allowed_backend_count == 0) return error.NoBackendAvailable;
        return loader;
    }

    fn admissionLimitsForBackend(
        self: *const ModelManager,
        backend_runtime: backends.BackendRuntime,
    ) runtime.tier.memory.Limits {
        var limits = runtime.tier.memory.defaultLimitsForBackend(
            admissionBackendClassForRuntime(backend_runtime),
        );
        const overrides = self.admission_limit_overrides;
        if (overrides.host_limit_bytes > 0) limits.host_limit_bytes = overrides.host_limit_bytes;
        if (overrides.backend_limit_bytes > 0) limits.backend_limit_bytes = overrides.backend_limit_bytes;
        if (overrides.combined_limit_bytes > 0) limits.combined_limit_bytes = overrides.combined_limit_bytes;
        if (overrides.kv_limit_bytes > 0) limits.kv_limit_bytes = overrides.kv_limit_bytes;
        if (overrides.scratch_limit_bytes > 0) limits.scratch_limit_bytes = overrides.scratch_limit_bytes;
        return limits;
    }

    /// Lazily loaded composite-model components participate in the same
    /// process-wide admission accounting as the primary session. Reserve their
    /// construction peak immediately before import, then retain only completed
    /// residency for the component lifetime.
    fn loadManagedSessionWithAdmission(
        self: *ModelManager,
        model_path: []const u8,
        preferred_backends: []const backends.BackendType,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
    ) !ManagedSession {
        var first_err: ?anyerror = null;
        var artifact_estimate = if (self.admission_enabled)
            try ComponentArtifactEstimate.init(self.allocator, model_path)
        else
            ComponentArtifactEstimate.disabled;
        defer artifact_estimate.deinit();

        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            if (shared_backend_ctx) |shared| {
                if (shared.backendType() != backend) continue;
            }

            var single_backend = [_]backends.BackendType{backend};
            var session_manager = sessionManagerForPreferredBackends(
                self.allocator,
                single_backend[0..],
                &self.session_manager,
            );
            const backend_runtime = session_manager.resolveBackendRuntime(backend) catch |err| {
                if (first_err == null) first_err = err;
                continue;
            };
            session_manager.onnx_execution_provider = backend_runtime.onnx_execution_provider;

            var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
            var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
            var admission_limits = runtime.tier.memory.Limits{};
            if (self.admission_enabled) {
                const artifact_bytes = artifact_estimate.bytesForBackend(backend) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
                const plan = if (artifact_estimate == .onnx)
                    onnxModelLoadAdmission(artifact_bytes, backend_runtime)
                else
                    nativeModelLoadAdmission(artifact_bytes, backend_runtime.backend);
                const admission_plan = plan catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
                resident_amounts = admission_plan.resident;
                admission_limits = self.admissionLimitsForBackend(backend_runtime);
                if (try admittedSessionCudaLimit(
                    backend_runtime,
                    resident_amounts,
                )) |cuda_limit| {
                    session_manager.onnx_cuda_memory_limit_bytes = cuda_limit;
                }
                resource_lease = self.acquireAmountsWithEviction(
                    admissionBackendClassForRuntime(backend_runtime),
                    admission_limits,
                    admission_plan.peak,
                ) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
            }

            if (session_manager.loadModelWithImportedOnnxContext(
                model_path,
                shared_backend_ctx,
            )) |loaded_session| {
                var session = loaded_session;
                if (resource_lease) |*lease| {
                    lease.retain(resident_amounts) catch |err| {
                        session.close();
                        lease.release();
                        return err;
                    };
                }
                if (self.admission_enabled) {
                    attachSessionRunAdmission(
                        &session,
                        self,
                        backend_runtime,
                        admission_limits,
                        resident_amounts,
                        null,
                    );
                }
                return .{
                    .session = session,
                    .resource_lease = resource_lease,
                };
            } else |err| {
                if (resource_lease) |*lease| lease.release();
                if (first_err == null) first_err = err;
            }
        }
        return first_err orelse error.NoBackendAvailable;
    }

    fn loadManagedHfTokenizerFile(
        self: *ModelManager,
        tokenizer_path: []const u8,
    ) !ManagedHfTokenizer {
        const plan = try tokenizerFileAdmissionPlan(
            self.allocator,
            tokenizer_path,
            .huggingface,
        );
        var resource_lease = try self.acquireTokenizerAdmission(plan);
        errdefer if (resource_lease) |*lease| lease.release();

        const tok_bytes = try c_file.readFile(self.allocator, tokenizer_path);
        defer self.allocator.free(tok_bytes);
        const tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(
            self.allocator,
            tok_bytes,
        );
        errdefer tokenizer.deinitSelf();
        try tokenizer.configureBpeCache(self.tokenizer_cache_config);
        try tokenizer.configureParallelBpe(self.tokenizer_parallel_bpe_config);
        if (resource_lease) |*lease| try lease.retain(plan.resident);

        return .{
            .tokenizer = tokenizer,
            .resource_lease = resource_lease,
        };
    }

    fn acquireTokenizerAdmission(
        self: *ModelManager,
        plan: ModelLoadAdmissionPlan,
    ) !?runtime.tier.memory.AdmissionLease {
        if (!self.admission_enabled) return null;
        return try self.acquireAmountsWithEviction(
            .cpu,
            self.admissionLimitsForBackend(.{ .backend = .native }),
            plan.peak,
        );
    }

    pub fn lockLoadedModels(self: *ModelManager) void {
        spinLock(&self.load_lock);
    }

    pub fn unlockLoadedModels(self: *ModelManager) void {
        self.load_lock.unlock();
    }

    pub fn acquireLoadedModel(self: *ModelManager, model_dir: []const u8) ?ModelHandle {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        const model = self.loaded.get(model_dir) orelse
            self.loaded_aliases.get(model_dir) orelse return null;
        model.active_handles += 1;
        return .{ .manager = self, .model = model };
    }

    pub fn loadedChatTemplateFailed(
        self: *ModelManager,
        model_dir: []const u8,
    ) ?bool {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        const model = self.loaded.get(model_dir) orelse
            self.loaded_aliases.get(model_dir) orelse return null;
        return model.chat_template_failed;
    }

    /// Configure tokenizer-cache admission before any model is loaded. The
    /// same process-wide budget may be shared by every loaded tokenizer.
    pub fn configureTokenizerCaches(
        self: *ModelManager,
        config: hf_tokenizer.HfTokenizer.BpeCacheConfig,
    ) !void {
        if (self.loaded.count() != 0)
            return error.TokenizerCacheConfigAfterModelLoad;
        self.tokenizer_cache_config = config;
    }

    /// Configure persistent std.Io-consumer tokenizer state before loading
    /// models. Private tables use the resource budget supplied through
    /// `configureTokenizerCaches`, so this is a capacity request rather than
    /// an unconditional allocation.
    pub fn configureTokenizerParallelBpe(
        self: *ModelManager,
        config: hf_tokenizer.HfTokenizer.ParallelBpeConfig,
    ) !void {
        if (self.loaded.count() != 0)
            return error.TokenizerParallelConfigAfterModelLoad;
        self.tokenizer_parallel_bpe_config = config;
    }

    pub fn deinit(self: *ModelManager) void {
        if (self.eviction_io) |io| self.eviction_group.cancel(io);
        std.debug.assert(self.in_flight_loads.count() == 0);
        self.in_flight_loads.deinit(self.allocator);
        var component_plan_it = self.component_plan_cache.iterator();
        while (component_plan_it.next()) |entry| entry.value_ptr.*.release();
        self.component_plan_cache.deinit(self.allocator);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded.deinit(self.allocator);
        var alias_it = self.loaded_aliases.iterator();
        while (alias_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded_aliases.deinit(self.allocator);
    }

    pub fn attachIo(self: *ModelManager, io: std.Io) void {
        self.lockLoadedModels();
        self.session_manager.io = io;
        var it = self.loaded.iterator();
        while (it.next()) |entry| entry.value_ptr.*.attachIo(io);
        const start_eviction_loop = self.keep_alive_ms > 0 and
            !self.eviction_loop_started;
        if (start_eviction_loop) {
            self.eviction_loop_started = true;
            self.eviction_io = io;
        }
        self.unlockLoadedModels();
        if (start_eviction_loop)
            self.eviction_group.async(io, evictionLoop, .{ self, io });
    }

    /// Counts loaded models participating in the prompt-cache accounting target.
    /// Used to split that target evenly across active model caches.
    /// `include` is always counted even if its cache has not activated yet.
    fn participatingPromptCacheCount(self: *ModelManager, include: *LoadedModel) usize {
        var count: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model == include) continue;
            if (model.prompt_prefix_cache.isParticipating()) count += 1;
        }
        return count + 1;
    }

    /// Apply one node-wide prompt-cache target to the cache being activated and
    /// every cache that is already active. configure() synchronously evicts
    /// entries against their estimated logical cache bytes.
    pub fn rebalancePromptCaches(
        self: *ModelManager,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        include.prompt_prefix_cache.reserveActivation();
        var per_cache = node_config;
        per_cache.max_bytes /= self.participatingPromptCacheCount(include);
        include.prompt_prefix_cache.configure(per_cache);

        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model != include and model.prompt_prefix_cache.isParticipating()) {
                model.prompt_prefix_cache.configure(per_cache);
            }
        }
    }

    /// Remove a failed first-use reservation and restore the full node target
    /// across the remaining caches. If a pool was already created, the cache
    /// remains a participant and no rollback is needed.
    pub fn cancelPromptCacheActivation(
        self: *ModelManager,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        if (!include.prompt_prefix_cache.cancelPendingActivation()) return;

        var count: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.prompt_prefix_cache.isParticipating()) count += 1;
        }
        if (count == 0) return;

        var per_cache = node_config;
        per_cache.max_bytes /= count;
        var rebalance_it = self.loaded.iterator();
        while (rebalance_it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.prompt_prefix_cache.isParticipating()) {
                model.prompt_prefix_cache.configure(per_cache);
            }
        }
    }

    /// Acquire a model for the duration of one operation. The returned handle
    /// prevents eviction until release.
    pub fn acquireFromDir(self: *ModelManager, model_dir: []const u8) !ModelHandle {
        return self.loadFromDirCoordinated(model_dir, self.session_manager.preferred_backends, true);
    }

    pub fn acquireFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !ModelHandle {
        return self.loadFromDirCoordinated(model_dir, preferred_backends, cache_default_alias);
    }

    /// Compatibility API for offline tools that keep a raw model pointer. Such
    /// callers explicitly pin the model because their pointer lifetime cannot
    /// be observed by the cache.
    pub fn loadFromDir(self: *ModelManager, model_dir: []const u8) !*LoadedModel {
        var handle = try self.acquireFromDir(model_dir);
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }

    pub fn loadFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !*LoadedModel {
        var handle = try self.acquireFromDirWithPreferredBackends(
            model_dir,
            preferred_backends,
            cache_default_alias,
        );
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }

    fn lookupLoadedModelLocked(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !?*LoadedModel {
        if (cache_default_alias) {
            if (self.loaded.get(model_dir)) |model| return model;
            if (self.loaded_aliases.get(model_dir)) |model| return model;
        }
        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            const variant_key = try backendVariantCacheKey(self.allocator, model_dir, backend);
            defer self.allocator.free(variant_key);
            if (self.loaded.get(variant_key)) |model| return model;
            if (self.loaded_aliases.get(variant_key)) |model| return model;
        }
        return null;
    }

    fn loadFlightKey(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) ![]u8 {
        const prefix = try std.fmt.allocPrint(
            self.allocator,
            "{d}:{s}:{d}:",
            .{ model_dir.len, model_dir, @intFromBool(cache_default_alias) },
        );
        defer self.allocator.free(prefix);
        const key = try self.allocator.alloc(u8, prefix.len + preferred_backends.len);
        @memcpy(key[0..prefix.len], prefix);
        for (preferred_backends, 0..) |backend, idx| {
            key[prefix.len + idx] = @intCast(@intFromEnum(backend));
        }
        return key;
    }

    fn finishLoadFlight(
        self: *ModelManager,
        flight: *LoadFlight,
        model: ?*LoadedModel,
        err: ?anyerror,
    ) void {
        self.lockLoadedModels();
        flight.model = model;
        flight.err = err;
        self.unlockLoadedModels();
        flight.completed.set(flight.io);
    }

    fn releaseLoadFlight(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
    ) void {
        self.lockLoadedModels();
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.unlockLoadedModels();
            return;
        }
        const removed = self.in_flight_loads.fetchRemove(flight_key) orelse unreachable;
        std.debug.assert(removed.value == flight);
        self.unlockLoadedModels();

        self.allocator.free(removed.key);
        self.allocator.destroy(flight);
    }

    fn waitForLoadFlight(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
    ) !ModelHandle {
        flight.completed.waitUncancelable(flight.io);
        const model = flight.model;
        const maybe_err = flight.err;

        if (model) |loaded| {
            self.lockLoadedModels();
            loaded.active_handles += 1;
            self.unlockLoadedModels();
        }
        self.releaseLoadFlight(flight_key, flight);
        if (maybe_err) |err| return err;
        return .{
            .manager = self,
            .model = model orelse return error.NoBackendAvailable,
        };
    }

    fn loadFromDirCoordinated(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !ModelHandle {
        const flight_key = try self.loadFlightKey(model_dir, preferred_backends, cache_default_alias);
        defer self.allocator.free(flight_key);

        self.lockLoadedModels();
        const cached = self.lookupLoadedModelLocked(
            model_dir,
            preferred_backends,
            cache_default_alias,
        ) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        if (cached) |model| {
            model.active_handles += 1;
            self.unlockLoadedModels();
            return .{ .manager = self, .model = model };
        }
        if (self.in_flight_loads.get(flight_key)) |flight| {
            flight.refs += 1;
            self.unlockLoadedModels();
            return self.waitForLoadFlight(flight_key, flight);
        }

        const flight = self.allocator.create(LoadFlight) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        // Antfly injects BackendRuntime.io through Node.attachIo. Keep the
        // inference package coupled only to the std.Io capability so standalone
        // and embedded owners can provide different runtime implementations.
        // The process-local fallback is only for offline callers that do not
        // attach a runtime.
        const coordination_io = self.session_manager.io orelse
            std.Io.Threaded.global_single_threaded.io();
        flight.* = .{
            .io = coordination_io,
        };
        const owned_flight_key = self.allocator.dupe(u8, flight_key) catch |err| {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return err;
        };
        self.in_flight_loads.put(self.allocator, owned_flight_key, flight) catch |err| {
            self.allocator.free(owned_flight_key);
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return err;
        };
        // Snapshot the injected runtime while holding load_lock. attachIo may
        // update the manager between cold loads, but a load must use one stable
        // runtime for its complete construction.
        var session_manager = sessionManagerForPreferredBackends(
            self.allocator,
            preferred_backends,
            &self.session_manager,
        );
        self.unlockLoadedModels();

        var handle = self.loadFromDirUncached(model_dir, &session_manager, cache_default_alias) catch |err| {
            self.finishLoadFlight(flight, null, err);
            self.releaseLoadFlight(flight_key, flight);
            return err;
        };
        self.finishLoadFlight(flight, handle.get(), null);
        self.releaseLoadFlight(flight_key, flight);
        return handle;
    }

    fn loadFromDirUncached(
        self: *ModelManager,
        model_dir: []const u8,
        sm: *backends.SessionManager,
        cache_default_alias: bool,
    ) !ModelHandle {

        // Load manifest
        var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
        var man_owned = true;
        errdefer if (man_owned) man.deinit();
        var policy_backend_scratch: [7]backends.BackendType = undefined;
        if (self.serving_policy) |policy| {
            sm.preferred_backends = try policyAllowedBackends(
                self.allocator,
                &policy_backend_scratch,
                model_dir,
                &man,
                sm.preferred_backends,
                policy,
            );
        }
        if (man.hasIncompleteGlinerBundle()) return error.IncompleteGlinerBundle;
        if (man.hasIncompleteColqwenBundle()) return error.IncompleteColqwenBundle;
        if (man.hasIncompleteClipclapGgufBundle()) return error.IncompleteClipclapGgufBundle;
        if (man.hasIncompleteFlorence2GgufBundle()) return error.IncompleteFlorence2Bundle;

        // Load tokenizer
        var hf_tok: ?*hf_tokenizer.HfTokenizer = null;
        var sp_tok: ?*sentencepiece.Processor = null;

        const tokenizer_type = blk: {
            if (shouldPreferSentencePieceOverride(man, model_dir, self.allocator)) {
                break :blk manifest_mod.TokenizerType.sentencepiece;
            }
            break :blk man.tokenizer_type orelse return error.NoTokenizerFound;
        };
        const tokenizer_admission_plan: ?ModelLoadAdmissionPlan = if (self.admission_enabled)
            try tokenizerLoadAdmissionPlan(
                self.allocator,
                model_dir,
                man.gguf_path,
                tokenizer_type,
            )
        else
            null;
        var tokenizer_resource_lease = if (tokenizer_admission_plan) |plan|
            try self.acquireTokenizerAdmission(plan)
        else
            null;
        errdefer if (tokenizer_resource_lease) |*lease| lease.release();
        // Register these after the lease cleanup so LIFO unwinding frees the
        // admitted memory before making its capacity visible to another load.
        errdefer if (hf_tok) |ht| ht.deinitSelf();
        errdefer if (sp_tok) |sp| {
            sp.deinit();
            self.allocator.destroy(sp);
        };

        switch (tokenizer_type) {
            .huggingface => {
                hf_tok = try loadHuggingFaceTokenizerFromDirOrGguf(self.allocator, model_dir, man.gguf_path);
                try hf_tok.?.configureBpeCache(self.tokenizer_cache_config);
                try hf_tok.?.configureParallelBpe(
                    self.tokenizer_parallel_bpe_config,
                );
            },
            .sentencepiece => {
                const sp = try loadSentencePieceTokenizerFromDirOrGguf(self.allocator, model_dir, man.gguf_path);
                errdefer {
                    sp.deinit();
                    self.allocator.destroy(sp);
                }
                if (shouldEnableGemmaSentencePieceCompat(man, model_dir, self.allocator)) {
                    sp.setPreserveInlineSpecialsAfterLiteralBos(true);
                }
                try loadSentencePieceAddedTokens(model_dir, self.allocator, sp);
                sp_tok = sp;
            },
        }
        if (tokenizer_resource_lease) |*lease| {
            try lease.retain(tokenizer_admission_plan.?.resident);
        }

        // Plan and reserve resources before the backend begins allocating weights.
        var loaded_session = try loadSessionForPreferredBackends(self, sm.preferred_backends, model_dir, man, sm);
        errdefer if (loaded_session.resource_lease) |*lease| lease.release();
        const session = loaded_session.session;
        var session_owned = true;
        errdefer if (session_owned) session.close();

        // Load chat template if available (for generator models)
        var chat_template_failed = false;
        var chat_tmpl: ?*ChatTemplate = if (man.chat_template) |ct_source| blk2: {
            const ct = self.allocator.create(ChatTemplate) catch {
                chat_template_failed = true;
                break :blk2 null;
            };
            ct.* = ChatTemplate.init(
                self.allocator,
                ct_source,
                man.bos_token,
                man.eos_token,
                man.unk_token,
                man.pad_token,
            ) catch |err| {
                // Chat requests will silently degrade to raw prompting from here on, which
                // looks like a model quality problem rather than a template problem.
                std.log.err("chat template init failed for {s}: {s}; chat requests will use raw prompts", .{ model_dir, @errorName(err) });
                self.allocator.destroy(ct);
                chat_template_failed = true;
                break :blk2 null;
            };
            break :blk2 ct;
        } else null;
        errdefer if (chat_tmpl) |ct| {
            ct.deinit();
            self.allocator.destroy(ct);
        };

        // Create loaded model
        var shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = blk: {
            if (session_factory.getGptConfig(session)) |cfg| {
                if (cfg.usesMoe()) {
                    const cache = try self.allocator.create(runtime.moe.shared.SharedExpertCache);
                    cache.* = runtime.moe.shared.SharedExpertCache.init(self.allocator);
                    break :blk cache;
                }
            }
            break :blk null;
        };
        errdefer if (shared_moe_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        };
        var shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = if (session_factory.getGptConfig(session)) |_| blk: {
            const state = try self.allocator.create(runtime.tier.shared.SharedPrefetchState);
            errdefer self.allocator.destroy(state);
            state.* = runtime.tier.shared.SharedPrefetchState.init(self.allocator);
            errdefer state.deinit();
            try session_factory.attachSharedPrefetchState(session, state);
            break :blk state;
        } else null;
        errdefer if (shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        };
        var native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = if (session_factory.getGptConfig(session)) |_| blk: {
            const coordinator = try self.allocator.create(runtime.scheduler.native_generate.NativeGenerateCoordinator);
            coordinator.* = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(self.allocator);
            break :blk coordinator;
        } else null;
        errdefer if (native_generate_coordinator) |coordinator| self.allocator.destroy(coordinator);
        const owned_model_dir = try self.allocator.dupe(u8, model_dir);
        var owned_model_dir_owned = true;
        errdefer if (owned_model_dir_owned) self.allocator.free(owned_model_dir);
        const model = try self.allocator.create(LoadedModel);
        var model_storage_owned = true;
        errdefer if (model_storage_owned) self.allocator.destroy(model);
        model.* = .{
            .manifest = man,
            .hf_tok = hf_tok,
            .sp_tok = sp_tok,
            .session = session,
            .session_manager = &self.session_manager,
            .model_manager = self,
            .model_dir = owned_model_dir,
            .allocator = self.allocator,
            .chat_tmpl = chat_tmpl,
            .chat_template_failed = chat_template_failed,
            .shared_moe_cache = shared_moe_cache,
            .shared_prefetch = shared_prefetch,
            .prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(self.allocator),
            .native_generate_coordinator = native_generate_coordinator,
            .native_generation_graph_cache = graph_mod.cache.GraphCache.init(self.allocator),
            .vision_session = null,
            .audio_session = null,
            .text_projection = null,
            .visual_projection = null,
            .audio_projection = null,
            .tokenizer_resource_lease = tokenizer_resource_lease,
            .resource_lease = loaded_session.resource_lease,
        };

        // The fully initialized model is now the sole owner. Disarm every
        // construction errdefer before publishLoadedModel takes responsibility
        // for cleanup on either publication failure or duplicate convergence.
        man_owned = false;
        hf_tok = null;
        sp_tok = null;
        tokenizer_resource_lease = null;
        session_owned = false;
        chat_tmpl = null;
        shared_moe_cache = null;
        shared_prefetch = null;
        native_generate_coordinator = null;
        owned_model_dir_owned = false;
        model_storage_owned = false;
        loaded_session.resource_lease = null;

        if (build_options.enable_metal and shouldUseMetalWholeModelExecutor(session)) {
            if (session_factory.getGptConfig(session)) |gpt_config| {
                if (graph_mod.metal_executor.supportsSession(session)) {
                    _ = graph_mod.metal_executor.prewarmSharedDecoderRuntime(self.allocator, session, gpt_config) catch |err| {
                        std.log.warn("metal decoder-runtime prewarm failed for {s}: {s}", .{ model_dir, @errorName(err) });
                    };
                }
            }
        }

        return self.publishLoadedModel(model, cache_default_alias);
    }

    /// Publish a fully constructed model with only a short map critical section.
    /// Distinct request keys can occasionally converge on the same actual backend;
    /// in that case retain the first model and dispose of the duplicate outside
    /// the lock.
    fn publishLoadedModel(
        self: *ModelManager,
        model: *LoadedModel,
        cache_default_alias: bool,
    ) !ModelHandle {
        var model_owned = true;
        errdefer if (model_owned) {
            model.deinit();
            self.allocator.destroy(model);
        };

        const variant_key = try backendVariantCacheKey(
            self.allocator,
            model.model_dir,
            model.session.backend(),
        );
        var variant_key_owned = true;
        defer if (variant_key_owned) self.allocator.free(variant_key);

        const alias_key = if (cache_default_alias)
            try self.allocator.dupe(u8, model.model_dir)
        else
            null;
        var alias_key_owned = alias_key != null;
        defer if (alias_key_owned) self.allocator.free(alias_key.?);

        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        while (true) {
            self.lockLoadedModels();
            if (self.loaded.get(variant_key)) |existing| {
                if (alias_key != null and
                    self.loaded.get(model.model_dir) == null and
                    self.loaded_aliases.get(model.model_dir) == null)
                {
                    self.loaded_aliases.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                        self.unlockLoadedModels();
                        return err;
                    };
                    self.loaded_aliases.putAssumeCapacity(alias_key.?, existing);
                    alias_key_owned = false;
                }
                existing.active_handles += 1;
                self.unlockLoadedModels();

                model.deinit();
                self.allocator.destroy(model);
                model_owned = false;
                return .{ .manager = self, .model = existing };
            }

            if (self.max_loaded_models > 0 and
                self.loaded.count() >= self.max_loaded_models)
            {
                const evicted = self.takeLruModelLocked(
                    platform.time.monotonicNs(),
                    false,
                );
                self.unlockLoadedModels();
                if (evicted == null)
                    return error.ResourceTemporarilyUnavailable;
                self.destroyEvictedModel(evicted.?);
                continue;
            }

            const needs_alias = alias_key != null and
                self.loaded.get(model.model_dir) == null and
                self.loaded_aliases.get(model.model_dir) == null;
            self.loaded.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                self.unlockLoadedModels();
                return err;
            };
            if (needs_alias) {
                self.loaded_aliases.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                    self.unlockLoadedModels();
                    return err;
                };
            }
            model.active_handles = 1;
            model.last_used_ns = platform.time.monotonicNs();
            self.loaded.putAssumeCapacity(variant_key, model);
            variant_key_owned = false;
            if (needs_alias) {
                self.loaded_aliases.putAssumeCapacity(alias_key.?, model);
                alias_key_owned = false;
            }
            self.unlockLoadedModels();

            model_owned = false;
            return .{ .manager = self, .model = model };
        }
    }
};

fn backendVariantCacheKey(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: backends.BackendType,
) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\nbackend={s}", .{ model_dir, @tagName(backend) });
}

test "model cache eviction skips active and pinned models and removes aliases" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var idle: LoadedModel = undefined;
    idle.active_handles = 0;
    idle.last_used_ns = 10;
    idle.pinned = false;
    var active: LoadedModel = undefined;
    active.active_handles = 1;
    active.last_used_ns = 1;
    active.pinned = false;
    var pinned: LoadedModel = undefined;
    pinned.active_handles = 0;
    pinned.last_used_ns = 0;
    pinned.pinned = true;

    try manager.loaded.put(allocator, try allocator.dupe(u8, "idle"), &idle);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "active"), &active);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "pinned"), &pinned);
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "idle-alias"),
        &idle,
    );

    manager.lockLoadedModels();
    const evicted = manager.takeLruModelLocked(100, false);
    manager.unlockLoadedModels();
    try std.testing.expect(evicted != null);
    try std.testing.expect(evicted.?.model == &idle);
    try std.testing.expectEqual(@as(usize, 2), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    manager.lockLoadedModels();
    const after_eviction = try manager.lookupLoadedModelLocked(
        "idle-alias",
        manager.session_manager.preferred_backends,
        true,
    );
    manager.unlockLoadedModels();
    try std.testing.expect(after_eviction == null);
    allocator.free(evicted.?.key);
}

test "model cache idle expiration can be disabled" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var idle: LoadedModel = undefined;
    idle.active_handles = 0;
    idle.last_used_ns = 1;
    idle.pinned = false;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "idle"), &idle);

    manager.keep_alive_ms = 0;
    manager.lockLoadedModels();
    const disabled = manager.takeLruModelLocked(std.time.ns_per_s, true);
    manager.unlockLoadedModels();
    try std.testing.expect(disabled == null);

    manager.keep_alive_ms = 1;
    manager.lockLoadedModels();
    const expired = manager.takeLruModelLocked(std.time.ns_per_s, true);
    manager.unlockLoadedModels();
    try std.testing.expect(expired != null);
    try std.testing.expect(expired.?.model == &idle);
    allocator.free(expired.?.key);
}

fn preferredModelPathForBackend(
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) ?[]const u8 {
    return switch (backend) {
        .onnx => man.onnx_path orelse model_dir,
        .native, .metal, .cuda, .wasm => if (!manifestHasNativeAssets(man) and man.onnx_path != null)
            man.onnx_path.?
        else
            model_dir,
        .pjrt => null,
    };
}

fn effectiveLoadBackends(
    scratch: *[7]backends.BackendType,
    preferred_backends: []const backends.BackendType,
    man: manifest_mod.ModelManifest,
) []const backends.BackendType {
    if (!shouldPreferNativeSession(man)) return preferred_backends;

    var idx: usize = 0;
    for (preferred_backends) |backend| {
        if (backend == .onnx) continue;
        scratch[idx] = backend;
        idx += 1;
    }
    for (preferred_backends) |backend| {
        if (backend == .onnx) {
            scratch[idx] = backend;
            idx += 1;
        }
    }
    return scratch[0..idx];
}

fn sessionManagerForPreferredBackends(
    allocator: std.mem.Allocator,
    preferred_backends: []const backends.BackendType,
    source: *const backends.SessionManager,
) backends.SessionManager {
    return .{
        .allocator = allocator,
        .preferred_backends = preferred_backends,
        .graph_runtime_strategy = source.graph_runtime_strategy,
        .onnx_execution_provider = source.onnx_execution_provider,
        .onnx_cuda_memory_limit_bytes = source.onnx_cuda_memory_limit_bytes,
        .io = source.io,
    };
}

pub const ManagedSession = struct {
    session: backends.Session,
    resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    owns_session: bool = true,

    pub fn deinit(self: *ManagedSession) void {
        if (self.owns_session) self.session.close();
        if (self.resource_lease) |*lease| lease.release();
        self.resource_lease = null;
        self.owns_session = false;
    }

    /// Transfer session ownership while leaving admission accounting with the
    /// caller. Useful for pipelines that already own and close their sessions.
    pub fn disownSession(self: *ManagedSession) backends.Session {
        self.owns_session = false;
        return self.session;
    }
};

pub const ManagedHfTokenizer = struct {
    tokenizer: *hf_tokenizer.HfTokenizer,
    resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    owns_tokenizer: bool = true,

    pub fn deinit(self: *ManagedHfTokenizer) void {
        if (self.owns_tokenizer) self.tokenizer.deinitSelf();
        if (self.resource_lease) |*lease| lease.release();
        self.resource_lease = null;
        self.owns_tokenizer = false;
    }

    pub fn take(self: *ManagedHfTokenizer) ManagedHfTokenizer {
        const owned = self.*;
        self.resource_lease = null;
        self.owns_tokenizer = false;
        return owned;
    }
};

const LoadedSessionPlan = ManagedSession;

const ModelLoadAdmissionPlan = struct {
    /// Maximum simultaneous bytes while parsing/importing/repacking.
    peak: runtime.tier.memory.AdmissionAmounts,
    /// Bytes retained by the completed backend session.
    resident: runtime.tier.memory.AdmissionAmounts,
};

fn modelRunWorkspaceAllowance(weight_bytes: usize) usize {
    const min_workspace = 16 * 1024 * 1024;
    const max_workspace = 1024 * 1024 * 1024;
    return std.math.clamp(weight_bytes / 8, min_workspace, max_workspace);
}

fn onnxCudaArenaAllowance(weight_bytes: usize) usize {
    const min_workspace = 64 * 1024 * 1024;
    const max_workspace = 2 * 1024 * 1024 * 1024;
    return std.math.clamp(weight_bytes / 8, min_workspace, max_workspace);
}

const TokenizerArtifactKind = enum {
    huggingface,
    sentencepiece,
    wordpiece,
};

const TokenizerArtifactCandidate = struct {
    name: []const u8,
    kind: TokenizerArtifactKind,
};

const tokenizer_fixed_resident_bytes = 16 * 1024 * 1024;
const tokenizer_fixed_peak_bytes = 24 * 1024 * 1024;

fn checkedScaledBytes(base: usize, multiplier: usize, fixed: usize) !usize {
    return std.math.add(
        usize,
        std.math.mul(usize, base, multiplier) catch
            return error.ResourceLimitExceeded,
        fixed,
    ) catch return error.ResourceLimitExceeded;
}

/// Tokenizer parsers expand compact JSON/protobuf vocabularies into multiple
/// owned hash maps and tries. Reserve a deliberately conservative construction
/// peak before reading the artifact, then retain the estimated live structures.
fn tokenizerAdmissionPlan(encoded_bytes: usize, kind: TokenizerArtifactKind) !ModelLoadAdmissionPlan {
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

fn tokenizerFileAdmissionPlan(
    allocator: std.mem.Allocator,
    path: []const u8,
    kind: TokenizerArtifactKind,
) !ModelLoadAdmissionPlan {
    const encoded_u64 = try c_file.fileSize(allocator, path);
    const encoded = std.math.cast(usize, encoded_u64) orelse
        return error.ResourceLimitExceeded;
    return tokenizerAdmissionPlan(encoded, kind);
}

fn tokenizerLoadAdmissionPlan(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_path: ?[]const u8,
    tokenizer_type: manifest_mod.TokenizerType,
) !ModelLoadAdmissionPlan {
    if (tokenizer_type == .huggingface) {
        const candidates = [_]TokenizerArtifactCandidate{
            .{ .name = "tokenizer.json", .kind = .huggingface },
            .{ .name = "vocab.txt", .kind = .wordpiece },
        };
        for (candidates) |candidate| {
            const path = try std.fs.path.join(allocator, &.{ model_dir, candidate.name });
            defer allocator.free(path);
            if (!c_file.fileExists(allocator, path)) continue;
            var encoded = std.math.cast(
                usize,
                try c_file.fileSize(allocator, path),
            ) orelse return error.ResourceLimitExceeded;
            // Legacy WordPiece additionally parses these optional JSON maps.
            if (candidate.kind == .wordpiece) {
                const sidecars = [_][]const u8{
                    "tokenizer_config.json",
                    "special_tokens_map.json",
                };
                for (sidecars) |name| {
                    const sidecar = try std.fs.path.join(allocator, &.{ model_dir, name });
                    defer allocator.free(sidecar);
                    if (!c_file.fileExists(allocator, sidecar)) continue;
                    const size = std.math.cast(
                        usize,
                        try c_file.fileSize(allocator, sidecar),
                    ) orelse return error.ResourceLimitExceeded;
                    encoded = std.math.add(usize, encoded, size) catch
                        return error.ResourceLimitExceeded;
                }
            }
            return tokenizerAdmissionPlan(encoded, candidate.kind);
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
    if (gguf_path) |path| {
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

const ComponentArtifactEstimate = union(enum) {
    disabled,
    onnx: usize,
    native: manifest_mod.ModelManifest,

    fn init(allocator: std.mem.Allocator, model_path: []const u8) !ComponentArtifactEstimate {
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

    fn deinit(self: *ComponentArtifactEstimate) void {
        switch (self.*) {
            .native => |*man| man.deinit(),
            .disabled, .onnx => {},
        }
        self.* = .disabled;
    }

    fn bytesForBackend(
        self: *const ComponentArtifactEstimate,
        backend: backends.BackendType,
    ) !usize {
        return switch (self.*) {
            .disabled => 0,
            .onnx => |bytes| bytes,
            .native => |man| estimateModelArtifactBytes(man, backend),
        };
    }
};

test "tokenizer admission reserves parse peak and retains live structures" {
    const encoded: usize = 2 * 1024 * 1024;
    const plan = try tokenizerAdmissionPlan(encoded, .huggingface);
    try std.testing.expectEqual(
        tokenizer_fixed_peak_bytes + 13 * encoded,
        plan.peak.host_weight_bytes,
    );
    try std.testing.expectEqual(
        tokenizer_fixed_resident_bytes + 10 * encoded,
        plan.resident.host_weight_bytes,
    );
    try std.testing.expect(plan.peak.host_weight_bytes > plan.resident.host_weight_bytes);
}

fn addArtifactBytes(total: *usize, allocator: std.mem.Allocator, maybe_path: ?[]const u8) !void {
    const path = maybe_path orelse return;
    const size = if (std.mem.endsWith(u8, path, ".onnx")) blk: {
        var artifacts = try backends.imported_onnx_session.inspectArtifactSet(allocator, path);
        defer artifacts.deinit();
        break :blk artifacts.encoded_bytes;
    } else std.math.cast(usize, try c_file.fileSize(allocator, path)) orelse
        return error.ResourceLimitExceeded;
    total.* = std.math.add(usize, total.*, size) catch return error.ResourceLimitExceeded;
}

fn estimateModelArtifactBytes(
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) !usize {
    var total: usize = 0;
    const uses_native_weights = backend != .onnx and manifestHasNativeAssets(man);
    if (uses_native_weights) {
        const native_weight_bytes = std.math.cast(
            usize,
            try session_factory.estimateNativeWeightBytes(man.allocator, man),
        ) orelse return error.ResourceLimitExceeded;
        total = std.math.add(usize, total, native_weight_bytes) catch
            return error.ResourceLimitExceeded;
        try addArtifactBytes(&total, man.allocator, man.gguf_projector_path);
    } else {
        try addArtifactBytes(&total, man.allocator, man.onnx_path);
    }

    if (total == 0) return error.NoModelFileFound;
    return total;
}

fn estimateModelLoadAdmission(
    man: manifest_mod.ModelManifest,
    backend_runtime: backends.BackendRuntime,
) !ModelLoadAdmissionPlan {
    const weights = try estimateModelArtifactBytes(man, backend_runtime.backend);
    const uses_onnx_artifact = backend_runtime.backend == .onnx or !manifestHasNativeAssets(man);
    if (uses_onnx_artifact) return onnxModelLoadAdmission(weights, backend_runtime);
    return nativeModelLoadAdmission(weights, backend_runtime.backend);
}

fn onnxModelLoadAdmission(
    weights: usize,
    backend_runtime: backends.BackendRuntime,
) !ModelLoadAdmissionPlan {
    return switch (backend_runtime.backend) {
        .onnx => if (backend_runtime.onnx_execution_provider == .cuda) .{
            .peak = .{
                // ORT parses and materializes the protobuf on the host before
                // CUDA owns its device initializers. Charge the same two-copy
                // host construction peak as ORT CPU plus device residency.
                .host_weight_bytes = std.math.mul(usize, weights, 2) catch
                    return error.ResourceLimitExceeded,
                .backend_weight_bytes = weights,
                .backend_scratch_bytes = onnxCudaArenaAllowance(weights),
            },
            .resident = .{
                .backend_weight_bytes = weights,
                // ORT's CUDA arena retains its high-water allocation. Reserve
                // and cap one bounded workspace for the session lifetime.
                .backend_scratch_bytes = onnxCudaArenaAllowance(weights),
            },
        } else .{
            .peak = .{
                .host_weight_bytes = std.math.mul(usize, weights, 2) catch
                    return error.ResourceLimitExceeded,
            },
            .resident = .{ .host_weight_bytes = weights },
        },
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

fn nativeModelLoadAdmission(
    weights: usize,
    backend: backends.BackendType,
) !ModelLoadAdmissionPlan {
    return switch (backend) {
        .metal => .{
            .peak = .{
                // Device weights plus conservative host-side import/repacking staging.
                .backend_weight_bytes = try session_factory.estimateBackendWeightResidencyBytes(.metal, weights),
                .host_weight_bytes = weights / 4,
            },
            .resident = .{
                .backend_weight_bytes = try session_factory.estimateBackendWeightResidencyBytes(.metal, weights),
            },
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
        .native, .onnx, .wasm => .{
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

fn loadSessionForPreferredBackends(
    manager: *ModelManager,
    preferred_backends: []const backends.BackendType,
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    source_session_manager: *const backends.SessionManager,
) !LoadedSessionPlan {
    var effective_scratch: [7]backends.BackendType = undefined;
    const effective_backends = effectiveLoadBackends(&effective_scratch, preferred_backends, man);
    // Keep the first real failure. Reporting a blanket NoModelFileFound hides the
    // actionable cause: a GGUF whose tensors could not be resolved fails with
    // MissingRequiredWeights, and callers were being told the file did not exist.
    var first_err: ?anyerror = null;
    for (effective_backends) |backend| {
        if (!backend.supportsDirectSessionLoad()) continue;
        const candidate_path = preferredModelPathForBackend(model_dir, man, backend) orelse continue;
        var single_backend = [_]backends.BackendType{backend};
        var backend_session_manager = sessionManagerForPreferredBackends(manager.allocator, single_backend[0..], source_session_manager);
        const backend_runtime = backend_session_manager.resolveBackendRuntime(backend) catch |err| {
            if (first_err == null) first_err = err;
            continue;
        };
        backend_session_manager.onnx_execution_provider = backend_runtime.onnx_execution_provider;
        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
        var admission_limits = runtime.tier.memory.Limits{};
        if (manager.admission_enabled) {
            const admission_plan = estimateModelLoadAdmission(man, backend_runtime) catch |err| {
                if (first_err == null) first_err = err;
                continue;
            };
            resident_amounts = admission_plan.resident;
            admission_limits = manager.admissionLimitsForBackend(backend_runtime);
            if (admittedSessionCudaLimit(
                backend_runtime,
                resident_amounts,
            ) catch |err| {
                if (first_err == null) first_err = err;
                continue;
            }) |cuda_limit| {
                backend_session_manager.onnx_cuda_memory_limit_bytes = cuda_limit;
            }
            resource_lease = manager.acquireAmountsWithEviction(
                admissionBackendClassForRuntime(backend_runtime),
                admission_limits,
                admission_plan.peak,
            ) catch |err| {
                if (first_err == null) first_err = err;
                continue;
            };
        }
        if (backend_session_manager.loadModel(candidate_path)) |loaded_session| {
            var session = loaded_session;
            if (resource_lease) |*lease| {
                lease.retain(resident_amounts) catch |err| {
                    session.close();
                    lease.release();
                    return err;
                };
            }
            if (manager.admission_enabled) {
                attachSessionRunAdmission(
                    &session,
                    manager,
                    backend_runtime,
                    admission_limits,
                    resident_amounts,
                    &man,
                );
            }
            return .{ .session = session, .resource_lease = resource_lease };
        } else |err| {
            if (resource_lease) |*lease| lease.release();
            std.log.warn("loadModel({s}) backend {s} failed: {s}", .{ model_dir, @tagName(backend), @errorName(err) });
            if (first_err == null) first_err = err;
        }
    }

    std.log.err("loadModel({s}) failed: no backend accepted model", .{model_dir});
    std.log.err("manifest paths onnx={?s} visual={?s} audio={?s} text_projection={?s} visual_projection={?s} audio_projection={?s}", .{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    });
    // NoModelFileFound only when nothing was even attempted.
    if (first_err) |err| return err;
    return error.NoModelFileFound;
}

test "serving admission applies the node-wide host-memory override" {
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    defer manager.deinit();

    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 100 });
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.admission.shared_limits.host_limit_bytes,
    );
}

test "shouldPreferNativeSession prefers native GLiNER weights" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    try std.testing.expect(!shouldPreferNativeSession(man));

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    try std.testing.expect(!shouldPreferNativeSession(man));

    man.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(man));
}

test "model manager backend clones preserve explicit graph runtime" {
    var source = backends.SessionManager.init(std.testing.allocator);
    source.graph_runtime_strategy = .partitioned;
    const preferred = [_]backends.BackendType{.onnx};

    const cloned = sessionManagerForPreferredBackends(std.testing.allocator, preferred[0..], &source);
    try std.testing.expectEqual(source.graph_runtime_strategy, cloned.graph_runtime_strategy);
    try std.testing.expectEqualSlices(backends.BackendType, preferred[0..], cloned.preferred_backends);
}

test "preferredModelPathForBackend keeps metal/native on model directory when native assets exist" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.onnx_path = try allocator.dupe(u8, "/tmp/model.onnx");
    man.safetensors_path = try allocator.dupe(u8, "/tmp/model.safetensors");

    try std.testing.expectEqualStrings("/tmp/model.onnx", preferredModelPathForBackend("/tmp/model", man, .onnx).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .metal).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .native).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .metal).?);
}

test "preferredModelPathForBackend routes direct compute backends to onnx path for onnx-only bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.onnx_path = try allocator.dupe(u8, "/tmp/text_model.onnx");
    man.visual_model_path = try allocator.dupe(u8, "/tmp/visual_model.onnx");
    man.audio_model_path = try allocator.dupe(u8, "/tmp/audio_model.onnx");

    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .onnx).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .native).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .metal).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .metal).?);
}

test "shouldPreferNativeSession prefers split GLiNER gguf bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    man.gguf_path = try allocator.dupe(u8, "encoder.gguf");
    man.gliner_head_gguf_path = try allocator.dupe(u8, "gliner_head.gguf");
    try std.testing.expect(shouldPreferNativeSession(man));
}

test "isManifestPotentiallyLoadableInCurrentBuild rejects incomplete GLiNER bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    man.gguf_path = try allocator.dupe(u8, "encoder.gguf");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(man));
}

test "shouldPreferNativeSession prefers native CLIP, Whisper, and Florence weights" {
    const allocator = std.testing.allocator;

    var clip = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .clip };
    defer clip.deinit();
    try std.testing.expect(!shouldPreferNativeSession(clip));
    clip.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(clip));

    var whisper = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .whisper };
    defer whisper.deinit();
    try std.testing.expect(!shouldPreferNativeSession(whisper));
    whisper.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(whisper));

    var florence = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .florence };
    defer florence.deinit();
    try std.testing.expect(!shouldPreferNativeSession(florence));
    florence.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(florence));
}

test "shouldPreferNativeSession prefers native classifier and recognizer weights" {
    const allocator = std.testing.allocator;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    try std.testing.expect(!shouldPreferNativeSession(classifier));
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(classifier));

    var recognizer = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .recognizer };
    defer recognizer.deinit();
    try std.testing.expect(!shouldPreferNativeSession(recognizer));
    recognizer.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(recognizer));
}

test "effectiveLoadBackends keeps gpu native backends ahead of cpu native before onnx" {
    const allocator = std.testing.allocator;
    const preferred = [_]backends.BackendType{ .onnx, .metal, .native };
    var scratch: [7]backends.BackendType = undefined;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");

    const effective = effectiveLoadBackends(&scratch, &preferred, classifier);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .metal, .native, .onnx }, effective);
}

test "effectiveLoadBackends preserves explicit onnx-only classifier preference" {
    const allocator = std.testing.allocator;
    const preferred = [_]backends.BackendType{.onnx};
    var scratch: [7]backends.BackendType = undefined;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");

    const effective = effectiveLoadBackends(&scratch, &preferred, classifier);
    try std.testing.expectEqualSlices(backends.BackendType, &preferred, effective);
}

test "isManifestPotentiallyLoadableInCurrentBuild accepts onnx-only models when onnx model support is enabled" {
    const allocator = std.testing.allocator;

    var onnx_only = manifest_mod.ModelManifest{ .allocator = allocator };
    defer onnx_only.deinit();
    onnx_only.onnx_path = try allocator.dupe(u8, "model.onnx");

    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(onnx_only));

    var native_model = manifest_mod.ModelManifest{ .allocator = allocator };
    defer native_model.deinit();
    native_model.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(native_model));
}

test "hybrid artifact candidates are isolated by backend" {
    const allocator = std.testing.allocator;
    var hybrid = manifest_mod.ModelManifest{ .allocator = allocator };
    defer hybrid.deinit();
    hybrid.gguf_path = try allocator.dupe(u8, "optional.gguf");
    hybrid.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    hybrid.onnx_path = try allocator.dupe(u8, "model.onnx");

    try std.testing.expectEqual(
        ArtifactCandidateKind.onnx,
        artifactCandidateForBackend(hybrid, .onnx).?,
    );
    try std.testing.expectEqual(
        ArtifactCandidateKind.safetensors,
        artifactCandidateForBackend(hybrid, .native).?,
    );
    try std.testing.expectEqual(
        ArtifactCandidateKind.safetensors,
        artifactCandidateForBackend(hybrid, .metal).?,
    );
}

test "native compatibility ignores an unrelated primary ONNX graph" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "stale.onnx",
        .data = "not an ONNX model",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .onnx_path = try std.fs.path.join(allocator, &.{ root, "stale.onnx" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);
}

test "unknown opt in rejects a structurally invalid primary GGUF" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = "not a GGUF",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "brand_new_decoder"),
        .gguf_path = try std.fs.path.join(allocator, &.{ root, "model.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);

    var allowed_scratch: [7]backends.BackendType = undefined;
    try std.testing.expectError(
        error.IncompatibleModel,
        policyAllowedBackends(
            allocator,
            &allowed_scratch,
            root,
            &man,
            &.{.native},
            .{ .allow_unknown = true },
        ),
    );
}

test "safetensors compatibility rejects a missing referenced shard" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors.index.json",
        .data =
        \\{"weight_map":{"model.embed_tokens.weight":"missing-00001-of-00001.safetensors"}}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_index_path = try std.fs.path.join(
            allocator,
            &.{ root, "model.safetensors.index.json" },
        ),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects a missing lazy GGUF companion" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .audio_model_path = try std.fs.path.join(allocator, &.{ root, "missing-clap.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects a corrupt GGUF projector" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "mmproj.gguf",
        .data = "not a GGUF",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects an unrecognized GGUF projector format" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyHeadGgufForModelManagerTest(
        dir.dir,
        allocator,
        "mmproj.gguf",
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma3"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_backend, summary.code);
}

test "native compatibility rejects a projector for a different decoder family" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyProjectorGgufForModelManagerTest(
        dir.dir,
        allocator,
        "mmproj.gguf",
        .gemma4_image,
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var mismatched = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma3"),
        .hidden_size = 4,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer mismatched.deinit();

    const mismatched_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &mismatched,
        .native,
    )).?;
    try std.testing.expectEqual(
        model_compatibility.Level.incompatible,
        mismatched_summary.level,
    );
    try std.testing.expectEqual(
        model_compatibility.Code.unsupported_backend,
        mismatched_summary.code,
    );

    var matched = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 4,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer matched.deinit();
    const matched_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &matched,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, matched_summary.level);

    var wrong_width = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 8,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer wrong_width.deinit();
    const wrong_width_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &wrong_width,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, wrong_width_summary.level);
    try std.testing.expectEqual(model_compatibility.Code.missing_required_tensor, wrong_width_summary.code);

    var audio_inputs = [_][]const u8{"audio"};
    var wrong_modality = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 4,
        .inputs = &audio_inputs,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer {
        // The input slice and its literal are borrowed by this fixture.
        wrong_modality.inputs = &.{};
        wrong_modality.deinit();
    }
    const wrong_modality_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &wrong_modality,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, wrong_modality_summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_backend, wrong_modality_summary.code);
}

test "native compatibility rejects unsupported companion GGUF tensor types" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyHeadGgufWithTensorTypeForModelManagerTest(
        dir.dir,
        allocator,
        "clap.gguf",
        .{ .known = .F64 },
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .audio_model_path = try std.fs.path.join(allocator, &.{ root, "clap.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_tensor_type, summary.code);
}

test "native compatibility rejects a malformed split GLiNER safetensors head" {
    const allocator = std.testing.allocator;
    const summary = (try validateNativeCompanionsForBackend(
        allocator,
        &.{
            .allocator = allocator,
            .gliner_head_safetensors_path = "/missing/gliner_head.safetensors",
        },
        .gguf,
        .native,
        .{ .family = .unsupported },
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "compatible artifact candidate wins aggregate compatibility" {
    const incompatible = CompatibilitySummary{
        .level = .incompatible,
        .code = .unsupported_tensor_type,
        .message = "bad optional artifact",
    };
    const unknown = CompatibilitySummary{
        .level = .unknown,
        .code = .unknown_architecture,
        .message = "unknown candidate",
    };
    const compatible = CompatibilitySummary{
        .level = .compatible,
        .code = .compatible,
        .message = "valid selected artifact",
    };

    try std.testing.expectEqual(
        model_compatibility.Level.unknown,
        selectBetterCompatibility(incompatible, unknown).level,
    );
    try std.testing.expectEqual(
        model_compatibility.Level.compatible,
        selectBetterCompatibility(incompatible, compatible).level,
    );
}

test "component compatibility validates explicit split ONNX graphs" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(
        &.{ 0.1, 0.2, 0.3, 0.4 },
        ml.graph.Shape.init(.f32, &.{4}),
    );
    const output = try builder.add(input, bias);
    try graph.markOutput(output);
    const model_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(model_bytes);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "encoder_model.onnx",
        .data = model_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "decoder_model.onnx",
        .data = model_bytes,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);
    const encoder = try std.fs.path.join(allocator, &.{ root, "encoder_model.onnx" });
    defer allocator.free(encoder);
    const decoder = try std.fs.path.join(allocator, &.{ root, "decoder_model.onnx" });
    defer allocator.free(decoder);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .transcriber,
        .native_arch_hint = .whisper,
        .config_model_arch = try allocator.dupe(u8, "whisper"),
    };
    defer man.deinit();
    const summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .manifest,
    );
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);

    man.model_type = .reader;
    man.native_arch_hint = .none;
    const whole_reader_summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .manifest,
    );
    try std.testing.expectEqual(
        model_compatibility.Level.incompatible,
        whole_reader_summary.level,
    );
    const multistage_summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(model_compatibility.Level.compatible, multistage_summary.level);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureServingPolicy(.{});
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "encoder_model.onnx",
        .data = "invalidated",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{ encoder, decoder },
            .multistage_ocr,
        ),
    );
}

test "component compatibility rejects malformed directory-backed native artifacts" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors",
        .data = "not a safetensors artifact",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var parent = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .reader,
    };
    defer parent.deinit();
    const summary = try componentCompatibilityForBackend(
        allocator,
        &parent,
        .native,
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    // Structurally invalid artifacts remain incompatible even when unknown
    // model contracts are explicitly permitted.
    manager.configureServingPolicy(.{ .allow_unknown = true });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component plan invalidates when a referenced safetensors shard changes" {
    const allocator = std.testing.allocator;
    const shard_json =
        \\{"weight":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}
    ;
    var shard_bytes: [8 + shard_json.len + 8]u8 = undefined;
    std.mem.writeInt(u64, shard_bytes[0..8], shard_json.len, .little);
    @memcpy(shard_bytes[8..][0..shard_json.len], shard_json);
    @memset(shard_bytes[8 + shard_json.len ..], 0);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model-00001-of-00001.safetensors",
        .data = &shard_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors.index.json",
        .data =
        \\{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model-00001-of-00001.safetensors",
        .data = "invalidated shard",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component plan invalidates lazy ONNX graphs and their external data" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    const output = try builder.add(input, weight);
    try graph.markOutput(output);
    const values = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const initializer = onnx_graph.ParameterInitializer{
        .name = "weight",
        .shape = ml.graph.Shape.init(.f32, &.{4}),
        .data = .{ .raw_bytes = std.mem.asBytes(&values) },
    };
    var exported = try onnx_graph.exportGraphWithExternalData(
        allocator,
        &graph,
        .{ .parameter_initializers = &.{initializer} },
        "visual_model.data",
    );
    defer exported.deinit(allocator);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.onnx",
        .data = exported.model_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = exported.external_data.?.bytes,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    // Updating a child in place does not change the component directory's
    // identity. The external-data file itself must invalidate the cached plan.
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = "truncated",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = exported.external_data.?.bytes,
    });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );

    // The lazy graph itself is also a dependency, independently of its external
    // tensor storage.
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.onnx",
        .data = "invalidated graph",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component loader rejects paths outside its validated plan" {
    const session_manager = backends.SessionManager.init(std.testing.allocator);
    var manager = ModelManager.init(std.testing.allocator, session_manager);
    defer manager.deinit();

    var loader = ModelManager.ComponentLoader{ .manager = &manager };
    try loader.addComponentPath("/models/encoder_model.onnx");
    loader.allowed_backends[0] = .native;
    loader.allowed_backend_count = 1;
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        loader.load("/models/substituted.onnx"),
    );

    const restricted = try loader.restrictToBackend(.native);
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        restricted.load("/models/substituted.onnx"),
    );
}

test "cuda model admission includes full native staging peak" {
    const weights: usize = 1024 * 1024;
    const cuda = try nativeModelLoadAdmission(weights, .cuda);
    try std.testing.expectEqual(weights, cuda.peak.host_weight_bytes);
    try std.testing.expect(cuda.peak.backend_weight_bytes >= weights);
    try std.testing.expectEqual(@as(usize, 0), cuda.resident.host_weight_bytes);
    try std.testing.expectEqual(
        cuda.peak.backend_weight_bytes,
        cuda.resident.backend_weight_bytes,
    );

    const metal = try nativeModelLoadAdmission(weights, .metal);
    try std.testing.expectEqual(weights / 4, metal.peak.host_weight_bytes);
    try std.testing.expectEqual(weights, metal.peak.backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), metal.resident.host_weight_bytes);
    try std.testing.expectEqual(weights, metal.resident.backend_weight_bytes);
}

test "onnx admission separates encoded staging from completed residency" {
    const weights: usize = 1024 * 1024;
    const cpu = try onnxModelLoadAdmission(weights, .{ .backend = .onnx });
    try std.testing.expectEqual(weights * 2, cpu.peak.host_weight_bytes);
    try std.testing.expectEqual(weights, cpu.resident.host_weight_bytes);

    const gpu = try onnxModelLoadAdmission(weights, .{
        .backend = .onnx,
        .onnx_execution_provider = .cuda,
    });
    try std.testing.expectEqual(weights * 2, gpu.peak.host_weight_bytes);
    try std.testing.expectEqual(weights, gpu.peak.backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), gpu.resident.host_weight_bytes);
    try std.testing.expectEqual(weights, gpu.resident.backend_weight_bytes);
    try std.testing.expectEqual(
        onnxCudaArenaAllowance(weights),
        gpu.peak.backend_scratch_bytes,
    );
    try std.testing.expectEqual(
        onnxCudaArenaAllowance(weights),
        gpu.resident.backend_scratch_bytes,
    );
    try std.testing.expectEqual(
        runtime.tier.memory.BackendClass.gpu,
        admissionBackendClassForRuntime(.{
            .backend = .onnx,
            .onnx_execution_provider = .cuda,
        }),
    );
}

test "directory-backed component admission charges native model artifacts" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const weight_bytes = 8192;
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = &([_]u8{0x5a} ** weight_bytes),
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var estimate = try ComponentArtifactEstimate.init(allocator, root);
    defer estimate.deinit();
    try std.testing.expectEqual(
        @as(usize, weight_bytes),
        try estimate.bytesForBackend(.native),
    );
    const plan = try nativeModelLoadAdmission(
        try estimate.bytesForBackend(.native),
        .native,
    );
    try std.testing.expectEqual(@as(usize, weight_bytes), plan.resident.host_weight_bytes);
}

test "isManifestPotentiallyLoadableInCurrentBuild hides incomplete colqwen bundles" {
    const allocator = std.testing.allocator;
    var colqwen = manifest_mod.ModelManifest{ .allocator = allocator };
    defer colqwen.deinit();
    colqwen.inference_bundle_family = try allocator.dupe(u8, "colqwen2_gguf_bundle/v1");
    colqwen.config_model_arch = try allocator.dupe(u8, "qwen2");
    colqwen.gguf_path = try allocator.dupe(u8, "model.gguf");
    colqwen.config_path = try allocator.dupe(u8, "config.json");
    colqwen.model_manifest_path = try allocator.dupe(u8, "model_manifest.json");
    colqwen.tokenizer_json_path = try allocator.dupe(u8, "tokenizer.json");
    colqwen.tokenizer_config_path = try allocator.dupe(u8, "tokenizer_config.json");
    colqwen.preprocessor_config_path = try allocator.dupe(u8, "preprocessor_config.json");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(colqwen));

    colqwen.processor_config_path = try allocator.dupe(u8, "processor_config.json");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(colqwen));
}

test "ClipClap manifest selects CLIP image preprocessing profile" {
    const allocator = std.testing.allocator;
    var clipclap = manifest_mod.ModelManifest{ .allocator = allocator };
    defer clipclap.deinit();

    clipclap.config_model_arch = try allocator.dupe(u8, "clipclap");
    try std.testing.expect(usesClipImagePreprocessProfile(&clipclap));

    var siglip = manifest_mod.ModelManifest{ .allocator = allocator };
    defer siglip.deinit();
    siglip.config_model_arch = try allocator.dupe(u8, "siglip");
    try std.testing.expect(!usesClipImagePreprocessProfile(&siglip));
}

test "ModelManager loads split gliner bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"recognizer","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.safetensors\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadSafetensorsForModelManagerTest(tmp.dir, allocator, "gliner_head.safetensors");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const ColdLoadWorker = struct {
        manager: *ModelManager,
        path: []const u8,
        model: ?*LoadedModel = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.model = self.manager.loadFromDir(self.path) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers = [_]ColdLoadWorker{
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
    };
    var threads: [workers.len]std.Thread = undefined;
    for (&workers, 0..) |*worker, i| {
        threads[i] = try std.Thread.spawn(.{}, ColdLoadWorker.run, .{worker});
    }
    for (&threads) |*thread| thread.join();
    for (workers) |worker| {
        try std.testing.expect(worker.err == null);
        try std.testing.expect(worker.model == workers[0].model);
    }

    const model = workers[0].model.?;
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "ModelManager serving policy fails closed before loading generator weights" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"brand_new_decoder\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{});

    try std.testing.expectError(error.UnknownModelCompatibility, manager.loadFromDir(dir_path));
}

test "unknown opt in still rejects a generator without a loadable artifact" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"brand_new_decoder\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });

    try std.testing.expectError(error.IncompatibleModel, manager.loadFromDir(dir_path));
}

test "unknown opt in does not enable a known incompatible generator" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"deepseek4\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });

    try std.testing.expectError(error.IncompatibleModel, manager.loadFromDir(dir_path));
}

test "ModelManager loads split gliner gguf-head bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"recognizer","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.gguf\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadGgufForModelManagerTest(tmp.dir, allocator, "gliner_head.gguf");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const model = try manager.loadFromDir(dir_path);
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "shouldPreferSentencePieceOverride still prefers sentencepiece for multimodal gemma" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.model", .data = "fake-spm" });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "{\n  \"<image_soft_token>\": 262144\n}\n",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\n  \"model_type\": \"gemma3\"\n}\n",
    });

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "shouldEnableGemmaSentencePieceCompat applies to gguf-only gemma dirs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();
    man.tokenizer_type = .sentencepiece;

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "gemma-4-e2b-it-gguf" });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldEnableGemmaSentencePieceCompat(man, dir_path, allocator));
    try std.testing.expect(!shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "loadSentencePieceAddedTokens overlays gemma special tokens from tokenizer json" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();

    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);
    try std.testing.expectEqual(@as(?i32, 105), sp.piece_map.get("<start_of_turn>"));
    try std.testing.expectEqual(@as(?i32, 262144), sp.extra_reserved_map.get("<image_soft_token>"));
    try std.testing.expectEqual("<start_of_turn>".len, sp.special_matcher.findPrefixLen("<start_of_turn>"));

    const encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, "<start_of_turn>", 16, false);
    defer {
        var encoded_mut = encoded;
        encoded_mut.deinit();
    }
    var found = false;
    for (encoded.ids[0..encoded.attention_mask.len], 0..) |id, idx| {
        if (encoded.attention_mask[idx] == 0) break;
        if (id == 105) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "gemma sentencepiece prompt parity against hf tokenizer" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const prompt =
        "<bos><start_of_turn>user\n" ++
        "<start_of_image>Describe this image.<end_of_turn>\n" ++
        "<start_of_turn>model\n";

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();
    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);

    const tokenizer_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir});
    defer allocator.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
    defer allocator.free(tokenizer_bytes);
    var hf = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf.deinitSelf();

    var sp_encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer sp_encoded.deinit();
    var hf_encoded = try hf.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer hf_encoded.deinit();

    var sp_count: usize = 0;
    while (sp_count < sp_encoded.attention_mask.len and sp_encoded.attention_mask[sp_count] != 0) : (sp_count += 1) {}
    var hf_count: usize = 0;
    while (hf_count < hf_encoded.attention_mask.len and hf_encoded.attention_mask[hf_count] != 0) : (hf_count += 1) {}
    try std.testing.expectEqual(sp_count, hf_count);
    try std.testing.expectEqualSlices(i32, sp_encoded.ids[0..sp_count], hf_encoded.ids[0..hf_count]);
}

fn writeTinyDebertaEncoderGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "deberta" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        .{ .key = "deberta.vocab_size", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "deberta.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "deberta.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "deberta.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "deberta.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.position_buckets", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.label_count", .value = .{ .u32 = 1 } },
    };
    const dims_vocab = [_]u64{ 4, 16 };
    const dims_hidden = [_]u64{4};
    const dims_rel = [_]u64{ 4, 16 };
    const dims_dense = [_]u64{ 4, 4 };
    const dims_intermediate = [_]u64{ 4, 8 };
    const dims_output = [_]u64{ 8, 4 };
    const dims_intermediate_bias = [_]u64{8};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "embeddings.word_embeddings.weight", .dimensions = &dims_vocab, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.rel_embeddings.weight", .dimensions = &dims_rel, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.weight", .dimensions = &dims_intermediate, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.bias", .dimensions = &dims_intermediate_bias, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.weight", .dimensions = &dims_output, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
    };

    var layout = try gguf_writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);

    var written_offset: u64 = 0;
    for (tensors, layout.offsets) |tensor, offset| {
        if (offset > written_offset) {
            try data.appendNTimes(allocator, 0, @intCast(offset - written_offset));
            written_offset = offset;
        }
        const byte_len = gguf_tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
        try data.appendNTimes(allocator, 0, byte_len);
        written_offset += @intCast(byte_len);
    }

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadSafetensorsForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const json =
        \\{"span_rep.test":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}
    ;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try appendLeModelManagerTest(u64, allocator, &data, json.len);
    try data.appendSlice(allocator, json);
    try data.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 0.0, 0.0 }));
    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    return writeTinyHeadGgufWithTensorTypeForModelManagerTest(
        dir,
        allocator,
        sub_path,
        .{ .known = .F32 },
    );
}

fn writeTinyHeadGgufWithTensorTypeForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    tensor_type: gguf_tensor_types.TensorType,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-gliner-head" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
    };
    return writeTinyGgufForModelManagerTest(
        dir,
        allocator,
        sub_path,
        &metadata,
        tensor_type,
    );
}

const TestProjectorKind = enum {
    gemma3,
    gemma4_image,
};

fn writeTinyProjectorGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    kind: TestProjectorKind,
) !void {
    const metadata: []const gguf_format.MetadataEntry = switch (kind) {
        .gemma3 => &[_]gguf_format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
            .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
            .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        },
        .gemma4_image => &[_]gguf_format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
            .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.block_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
            .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 14 } },
            .{ .key = "clip.vision.projector_scale_factor", .value = .{ .u32 = 1 } },
            .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        },
    };
    if (kind == .gemma4_image) {
        const dims_patch = [_]u64{588};
        const dims_hidden = [_]u64{4};
        const dims_patch_projection = [_]u64{ 4, 588 };
        const dims_position = [_]u64{ 2, 16, 4 };
        const dims_projection = [_]u64{ 4, 4 };
        const tensors = [_]gguf_writer.TensorSpec{
            .{ .name = "v.patch_norm.1.weight", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.1.bias", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_embd.weight", .dimensions = &dims_patch_projection, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_embd.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.2.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.2.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.position_embd.weight", .dimensions = &dims_position, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.3.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.3.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "mm.input_projection.weight", .dimensions = &dims_projection, .tensor_type = .{ .known = .F32 } },
        };
        var layout = try gguf_writer.buildLayout(allocator, metadata, &tensors);
        defer layout.deinit(allocator);
        var data = std.ArrayListUnmanaged(u8).empty;
        defer data.deinit(allocator);
        try data.appendSlice(allocator, layout.header_bytes);
        const data_region_offset = std.mem.alignForward(
            usize,
            layout.header_bytes.len,
            @intCast(layout.alignment),
        );
        try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
        var written_offset: u64 = 0;
        for (tensors, layout.offsets) |tensor, offset| {
            if (offset > written_offset) {
                try data.appendNTimes(allocator, 0, @intCast(offset - written_offset));
                written_offset = offset;
            }
            const byte_len = gguf_tensor_types.byteLen(
                tensor.tensor_type,
                tensor.dimensions,
            ) orelse return error.UnsupportedTensorType;
            try data.appendNTimes(allocator, 0, byte_len);
            written_offset += @intCast(byte_len);
        }
        try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
        return;
    }
    return writeTinyGgufForModelManagerTest(
        dir,
        allocator,
        sub_path,
        metadata,
        .{ .known = .F32 },
    );
}

fn writeTinyGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    metadata: []const gguf_format.MetadataEntry,
    tensor_type: gguf_tensor_types.TensorType,
) !void {
    const dims = [_]u64{2};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "span_rep.test", .dimensions = &dims, .tensor_type = tensor_type },
    };

    var layout = try gguf_writer.buildLayout(allocator, metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
    const tensor_byte_len = gguf_tensor_types.byteLen(tensor_type, &dims) orelse
        return error.UnsupportedTensorType;
    try data.appendNTimes(allocator, 0, tensor_byte_len);

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn appendLeModelManagerTest(
    comptime T: type,
    allocator: std.mem.Allocator,
    data: *std.ArrayListUnmanaged(u8),
    value: T,
) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

test "load huggingface tokenizer from gguf gpt2 metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGpt2Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ggml-model-i2_s.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "ggml-model-i2_s.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 0), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
}

test "load huggingface tokenizer from gguf gemma4 bpe metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGemma4Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma4-q4_0.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "gemma4-q4_0.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello world", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 2), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 4), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 5), encoded.ids[2]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[2]);

    const special_ids = try tok.tokenizer().encode(allocator, "<|turn>hello");
    defer allocator.free(special_ids);
    try std.testing.expectEqualSlices(i32, &.{ 6, 4 }, special_ids);

    const decoded = try tok.tokenizer().decode(allocator, &.{ 4, 5 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hello world", decoded);
}

fn buildTestGgufWithGpt2Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 7);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bitnet-b1.58");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gpt2");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<|begin_of_text|>",
        "hello",
        "<|end_of_text|>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithGemma4Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 10);

    try appendTestMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gemma4");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<pad>",
        "<eos>",
        "<bos>",
        "<unk>",
        "hello",
        "▁world",
        "<|turn>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1, 3 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn appendTestLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    const bytes = std.mem.asBytes(&std.mem.nativeToLittle(T, value));
    try data.appendSlice(allocator, bytes);
}

fn appendTestString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendTestLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendTestMetadataString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}
