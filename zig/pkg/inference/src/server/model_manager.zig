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
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const backends = @import("../backends/backends.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_compatibility = @import("../models/compatibility.zig");
const managed_receipt = @import("../registry/managed_receipt.zig");
const safetensors_mod = @import("../models/safetensors.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const gguf_writer = @import("../gguf/writer.zig");
const clipclap_format_mod = @import("../architectures/clipclap_format.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const tokenizer_mod = @import("inference_tokenizer");
const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
const encoder_decoder = @import("../pipelines/encoder_decoder.zig");
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
const backend_contracts = @import("../graph/backend_contracts.zig");
const graph_mod = @import("../graph/root.zig");
const kernel_jit_profile_output = @import("../kernel_jit_profile_output.zig");
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

    // ClipClap is a paired encoder contract, not a decoder/projector contract.
    // Validate both headers together so two individually valid GGUFs with
    // incompatible shared embedding widths can never be advertised or loaded.
    const clipclap_audio_validated = candidate == .gguf and man.isClipclapGgufBundle();
    if (clipclap_audio_validated) {
        const clip_path = man.gguf_path orelse return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap bundle is missing its CLIP GGUF",
        };
        const clap_path = man.audio_model_path orelse return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap bundle is missing its CLAP GGUF",
        };
        if (try validateClipclapBundleForBackend(allocator, clip_path, clap_path, backend)) |summary|
            return summary;
    }

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
        if (clipclap_audio_validated and man.audio_model_path != null and
            std.mem.eql(u8, path, man.audio_model_path.?))
        {
            continue;
        }
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

fn validateClipclapBundleForBackend(
    allocator: std.mem.Allocator,
    clip_path: []const u8,
    clap_path: []const u8,
    backend: backends.BackendType,
) !?CompatibilitySummary {
    var clip_mapped = c_file.MmapRegion.init(allocator, clip_path) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap CLIP GGUF is missing or unreadable",
        };
    };
    defer clip_mapped.deinit();
    var clap_mapped = c_file.MmapRegion.init(allocator, clap_path) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap CLAP GGUF is missing or unreadable",
        };
    };
    defer clap_mapped.deinit();

    var clip_file = gguf_format.parseStructure(allocator, clip_mapped.data) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap CLIP GGUF has invalid structure",
        };
    };
    defer clip_file.deinit(allocator);
    var clap_file = gguf_format.parseStructure(allocator, clap_mapped.data) catch |err| {
        if (err == error.OutOfMemory) return err;
        return .{
            .level = .incompatible,
            .code = .artifact_unreadable,
            .message = "the ClipClap CLAP GGUF has invalid structure",
        };
    };
    defer clap_file.deinit(allocator);

    gguf_format.validateTensorDataRanges(&clip_file, clip_mapped.data.len) catch return .{
        .level = .incompatible,
        .code = .artifact_unreadable,
        .message = "the ClipClap CLIP GGUF has invalid or truncated tensor data",
    };
    gguf_format.validateTensorDataRanges(&clap_file, clap_mapped.data.len) catch return .{
        .level = .incompatible,
        .code = .artifact_unreadable,
        .message = "the ClipClap CLAP GGUF has invalid or truncated tensor data",
    };
    clip_mapped.adviseSequentialPrefix(@min(
        std.math.cast(usize, clip_file.data_region_offset) orelse clip_mapped.data.len,
        clip_mapped.data.len,
    ));
    clap_mapped.adviseSequentialPrefix(@min(
        std.math.cast(usize, clap_file.data_region_offset) orelse clap_mapped.data.len,
        clap_mapped.data.len,
    ));

    _ = clipclap_format_mod.inspectFilePair(&clip_file, &clap_file) catch |err| return switch (err) {
        error.UnsupportedClipclapArchitecture, error.ClipclapProjectionMismatch => .{
            .level = .incompatible,
            .code = .unsupported_backend,
            .message = "the ClipClap GGUF pair declares incompatible encoder architectures or projection widths",
        },
        else => .{
            .level = .incompatible,
            .code = .missing_required_tensor,
            .message = "the ClipClap GGUF pair is missing required metadata or tensors, or declares an invalid tensor layout",
        },
    };

    for ([_]*const gguf_format.File{ &clip_file, &clap_file }) |file| {
        for (file.tensors) |tensor| {
            if (!session_factory.ggufTensorTypeSupportsBackend(tensor.tensor_type, backend)) {
                return .{
                    .level = .incompatible,
                    .code = .unsupported_tensor_type,
                    .message = "the selected backend cannot materialize the ClipClap GGUF tensor contract",
                };
            }
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
    const backend_count = @typeInfo(backends.BackendType).@"enum".field_names.len;

    allocator: std.mem.Allocator,
    base_summary: CompatibilitySummary,
    invalid_summary: ?CompatibilitySummary = null,
    has_onnx_component: bool = false,
    has_native_component: bool = false,
    imported_graph_compatible: bool = true,
    native_backend_summaries: [backend_count]?CompatibilitySummary =
        @as([backend_count]?CompatibilitySummary, @splat(null)),
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
        const slot = &self.native_backend_summaries[@backingInt(backend)];
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
        if (self.native_backend_summaries[@backingInt(backend)]) |native_summary| {
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
const whisper_assets_cache_capacity = 256;

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
        @backingInt(man.model_type),
        @backingInt(man.model_type_origin),
        @backingInt(man.native_arch_hint),
        @backingInt(contract),
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
    for (preferred_backends) |backend| hash.update(&.{@backingInt(backend)});
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

/// Writer-preferring process-local gate for optional embedding assets.
/// Primary-only requests take shared access and retain full backend concurrency;
/// requests that swap ephemeral audio sessions take exclusive access.
const EmbeddingAssetGate = struct {
    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,

    fn lockShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_gate);
        defer self.reader_gate.unlock();
        spinLock(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        self.reader_count += 1;
        if (self.reader_count == 1) spinLock(&self.resource_mutex);
    }

    fn tryLockShared(self: *EmbeddingAssetGate) bool {
        if (!self.reader_gate.tryLock()) return false;
        defer self.reader_gate.unlock();
        if (!self.reader_mutex.tryLock()) return false;
        defer self.reader_mutex.unlock();

        if (self.reader_count == 0 and !self.resource_mutex.tryLock()) return false;
        self.reader_count += 1;
        return true;
    }

    fn unlockShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        std.debug.assert(self.reader_count > 0);
        self.reader_count -= 1;
        if (self.reader_count == 0) self.resource_mutex.unlock();
    }

    fn lockExclusive(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_gate);
        spinLock(&self.resource_mutex);
    }

    fn tryLockExclusive(self: *EmbeddingAssetGate) bool {
        if (!self.reader_gate.tryLock()) return false;
        if (!self.resource_mutex.tryLock()) {
            self.reader_gate.unlock();
            return false;
        }
        return true;
    }

    fn unlockExclusive(self: *EmbeddingAssetGate) void {
        self.resource_mutex.unlock();
        self.reader_gate.unlock();
    }

    /// Convert an exclusive owner into the first shared reader without opening
    /// a gap in which another writer could mutate optional-session slots.
    fn downgradeExclusiveToShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_mutex);
        std.debug.assert(self.reader_count == 0);
        self.reader_count = 1;
        self.reader_mutex.unlock();

        // Keep resource_mutex held on behalf of the new reader cohort. New
        // readers may now join, while queued writers remain excluded until the
        // final reader releases the resource.
        self.reader_gate.unlock();
    }
};

pub const EmbeddingAssetLease = struct {
    gate: *EmbeddingAssetGate,
    access: enum { shared, exclusive },
    held: bool = true,

    pub fn release(self: *EmbeddingAssetLease) void {
        if (!self.held) return;
        switch (self.access) {
            .shared => self.gate.unlockShared(),
            .exclusive => self.gate.unlockExclusive(),
        }
        self.held = false;
    }

    pub fn downgradeExclusiveToShared(self: *EmbeddingAssetLease) void {
        std.debug.assert(self.held);
        std.debug.assert(self.access == .exclusive);
        self.gate.downgradeExclusiveToShared();
        self.access = .shared;
    }
};

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
    // Tokenizer extraction is metadata-only. A whole-file DONTNEED here would
    // discard a warmed checkpoint immediately before CUDA model admission.
    region.preserveFileCacheOnDeinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;

    const tokenizer_bytes = if (std.mem.eql(u8, model_name, "t5"))
        try unigramTokenizerJsonFromGguf(allocator, &parsed)
    else blk: {
        const flavor: GgufBpeTokenizerFlavor = if (std.mem.eql(u8, model_name, "gpt2"))
            .byte_level
        else if (std.mem.eql(u8, model_name, "gemma4"))
            .gemma4
        else
            return error.NoTokenizerFound;
        break :blk try bpeTokenizerJsonFromGguf(allocator, &parsed, flavor);
    };
    defer allocator.free(tokenizer_bytes);

    const tok = try hf_tokenizer.HfTokenizer.loadFromBytesAllowDuplicateFields(allocator, tokenizer_bytes);
    errdefer tok.deinitSelf();
    const tokens = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.tokens", .string);
    for (tokens.values, 0..) |token_value, index| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const id = std.math.cast(i32, index) orelse return error.InvalidTokenizerMetadata;
        try tok.addTokenIdAliasForDecode(token, id);
    }
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

fn unigramTokenizerJsonFromGguf(
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
) ![]u8 {
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const scores = try getRequiredMetadataArray(parsed, "tokenizer.ggml.scores", null);
    const token_types = try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null);
    if (tokens.values.len != scores.values.len or tokens.values.len != token_types.values.len) {
        return error.InvalidTokenizerMetadata;
    }
    const unknown_id = metadataTokenId(parsed, "tokenizer.ggml.unknown_token_id") orelse
        return error.InvalidTokenizerMetadata;
    const add_space_prefix = gguf_metadata.View.init(parsed).getBool("tokenizer.ggml.add_space_prefix") orelse true;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);
    try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"Unigram\",\"unk_id\":");
    var unknown_id_buf: [32]u8 = undefined;
    try tokenizer_json.appendSlice(allocator, try std.fmt.bufPrint(&unknown_id_buf, "{d}", .{unknown_id}));
    try tokenizer_json.appendSlice(allocator, ",\"vocab\":[");
    for (tokens.values, scores.values, 0..) |token_value, score_value, index| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const score = switch (score_value) {
            .f32 => |value| value,
            .f64 => |value| @as(f32, @floatCast(value)),
            else => return error.InvalidTokenizerMetadata,
        };
        if (index > 0) try tokenizer_json.append(allocator, ',');
        try tokenizer_json.append(allocator, '[');
        try appendJsonString(&tokenizer_json, allocator, token);
        var score_buf: [64]u8 = undefined;
        try tokenizer_json.appendSlice(allocator, try std.fmt.bufPrint(&score_buf, ",{d}]", .{score}));
    }
    try tokenizer_json.appendSlice(
        allocator,
        if (add_space_prefix)
            "]},\"pre_tokenizer\":{\"type\":\"Metaspace\",\"replacement\":\"\\u2581\",\"prepend_scheme\":\"always\",\"split\":true},\"added_tokens\":["
        else
            "]},\"pre_tokenizer\":{\"type\":\"Metaspace\",\"replacement\":\"\\u2581\",\"prepend_scheme\":\"never\",\"split\":true},\"added_tokens\":[",
    );
    // ponytail: precompiled SentencePiece normalization is intentionally left
    // to a future shared normalizer; ordinary normalized UTF-8 needs no copy.
    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]}");
    return tokenizer_json.toOwnedSlice(allocator);
}

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

fn adoptAndConfigureSentencePieceTokenizer(
    owned: *?*sentencepiece.Processor,
    sp: *sentencepiece.Processor,
    man: manifest_mod.ModelManifest,
    model_dir: []const u8,
    allocator: std.mem.Allocator,
) !void {
    std.debug.assert(owned.* == null);
    owned.* = sp;
    if (shouldEnableGemmaSentencePieceCompat(man, model_dir, allocator)) {
        sp.setPreserveInlineSpecialsAfterLiteralBos(true);
    }
    try loadSentencePieceAddedTokens(model_dir, allocator, sp);
}

fn loadSentencePieceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !sentencepiece.Processor {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();
    // Keep already-warm tensor payload pages; only metadata is consumed here.
    region.preserveFileCacheOnDeinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;
    if (!(std.mem.eql(u8, model_name, "llama") or
        std.mem.eql(u8, model_name, "t5") or
        std.mem.startsWith(u8, model_name, "gemma")))
    {
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

const DeclaredOptionalSessionKind = enum {
    vision,
    audio,
    text_projection,
    visual_projection,
    audio_projection,
};

const DeclaredOptionalSession = struct {
    kind: DeclaredOptionalSessionKind,
    path: ?[]const u8,
};

const declared_optional_session_count = @typeInfo(DeclaredOptionalSessionKind).@"enum".field_names.len;

fn declaredOptionalSessions(manifest: *const manifest_mod.ModelManifest) [declared_optional_session_count]DeclaredOptionalSession {
    return .{
        .{ .kind = .vision, .path = manifest.visual_model_path },
        .{ .kind = .audio, .path = manifest.audio_model_path },
        .{ .kind = .text_projection, .path = manifest.text_projection_path },
        .{ .kind = .visual_projection, .path = manifest.visual_projection_path },
        .{ .kind = .audio_projection, .path = manifest.audio_projection_path },
    };
}

fn declaredOptionalSessionsComplete(
    manifest: *const manifest_mod.ModelManifest,
    loaded: [declared_optional_session_count]bool,
) bool {
    for (declaredOptionalSessions(manifest), loaded) |declared, is_loaded| {
        if (declared.path != null and !is_loaded) return false;
    }
    return true;
}

fn bindOptionalSessionProfile(
    session_manager: *backends.SessionManager,
    kind: DeclaredOptionalSessionKind,
    profile_bundle: ?graph_mod.kernel_jit.QualifiedProfileBundle,
) !void {
    if (profile_bundle) |bundle| {
        const component_path = switch (kind) {
            .vision => bundle.vision,
            .audio => bundle.audio,
            .text_projection => bundle.text_projection,
            .visual_projection => bundle.visual_projection,
            .audio_projection => bundle.audio_projection,
        };
        if (component_path) |path| {
            session_manager.kernel_jit.qualified_profile_path = path;
        } else {
            if (session_manager.kernel_jit.mode.failClosed()) {
                return error.MissingKernelJitProfileBundleComponent;
            }
            std.log.warn(
                "kernel JIT profile bundle has no {s} member; using bundled kernels",
                .{@tagName(kind)},
            );
            session_manager.kernel_jit.qualified_profile_path = null;
            session_manager.kernel_jit.mode = .off;
        }
    } else if (session_manager.kernel_jit.qualified_profile_path != null) {
        if (session_manager.kernel_jit.mode.failClosed()) {
            return error.KernelJitQualifiedProfileOptionalSessionUnsupported;
        }
        // One exact profile is bound to one model fingerprint. Never apply
        // the primary model's profile to an optional submodel.
        session_manager.kernel_jit.qualified_profile_path = null;
        session_manager.kernel_jit.mode = .off;
    }
}

fn ownSelectedFirstBackendPreference(
    allocator: std.mem.Allocator,
    preferred: []const backends.BackendType,
    selected: backends.BackendType,
) ![]backends.BackendType {
    var count: usize = 1;
    for (preferred) |backend| {
        if (backend != selected) count += 1;
    }
    const result = try allocator.alloc(backends.BackendType, count);
    result[0] = selected;
    var index: usize = 1;
    for (preferred) |backend| {
        if (backend == selected) continue;
        result[index] = backend;
        index += 1;
    }
    return result;
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
    whisper_prompt_cache: ?whisper_prompt.PromptCache = null,
    /// The model shipped a chat template that we could not parse, so chat requests fall
    /// back to raw prompting. Distinct from `chat_tmpl == null` with no template at all.
    chat_template_failed: bool = false,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = null,
    prompt_prefix_cache: runtime.kv.prompt_cache.PromptPrefixCache,
    native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    native_generation_graph_cache: graph_mod.cache.GraphCache,
    // ponytail: model-wide safety lock; replace with per-request backend state only when continuous batching is proven safe.
    native_generate_lock: std.atomic.Mutex = .unlocked,
    // Multimodal sessions (CLIP/CLAP/CLIPCLAP). The gate protects session
    // lifetime during execution; the mutex protects short slot mutations.
    embedding_asset_gate: EmbeddingAssetGate = .{},
    // Stateful GPU backends retain one model-level execution lane until
    // per-request resident frames and caches are independently owned.
    embedding_session_lock: std.atomic.Mutex = .unlocked,
    /// Stateful GPU pipelines share mutable command-frame and resident-slot
    /// state within a loaded model. Keep one model-local lane while allowing
    /// independent models to overlap.
    target_inference_run_lock: std.atomic.Mutex = .unlocked,
    vision_session: ?backends.Session = null,
    audio_session: ?backends.Session = null,
    text_projection: ?backends.Session = null,
    visual_projection: ?backends.Session = null,
    audio_projection: ?backends.Session = null,
    /// Owns the strings backing the qualified JIT profile paths retained by
    /// the primary and lazily loaded component sessions.
    kernel_jit_profile_bundle: ?kernel_jit_profile_output.LoadedProfileBundle = null,
    vision_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    text_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    visual_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    /// Last complete optional-session JIT snapshot. Metrics use it when a
    /// cold sidecar load owns embedding_session_lock, keeping scrapes bounded
    /// without racing publication of the optional session handles.
    optional_metal_jit_q4_0_hits: std.atomic.Value(u64) = .init(0),
    optional_metal_jit_q4_k_hits: std.atomic.Value(u64) = .init(0),
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
    /// Removed from the lookup maps after a recoverable runtime-integrity
    /// fault. Existing handles may finish unwinding; the last one destroys the
    /// retired model while new requests load a fresh session.
    retired: bool = false,

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

    /// Snapshot all model-component exact-JIT counters while optional-session
    /// publication is stable. A cold optional-session load can take seconds,
    /// so metrics fall back to the last stable optional snapshot rather than
    /// waiting behind disk I/O. The primary session is immutable and can
    /// always be sampled directly.
    pub fn metalExactJitDispatchStats(self: *LoadedModel) session_factory.MetalExactJitDispatchStats {
        var total = session_factory.MetalExactJitDispatchStats{};
        if (session_factory.getMetalExactJitDispatchStats(self.session)) |stats| total.add(stats);

        if (!self.embedding_session_lock.tryLock()) {
            total.add(.{
                .q4_0_hits = self.optional_metal_jit_q4_0_hits.load(.acquire),
                .q4_k_hits = self.optional_metal_jit_q4_k_hits.load(.acquire),
            });
            return total;
        }
        defer self.embedding_session_lock.unlock();

        var optional = session_factory.MetalExactJitDispatchStats{};
        const sessions = [_]?backends.Session{
            self.vision_session,
            self.audio_session,
            self.text_projection,
            self.visual_projection,
            self.audio_projection,
        };
        for (sessions) |maybe_session| {
            const session = maybe_session orelse continue;
            if (session_factory.getMetalExactJitDispatchStats(session)) |stats| optional.add(stats);
        }
        self.optional_metal_jit_q4_0_hits.store(optional.q4_0_hits, .release);
        self.optional_metal_jit_q4_k_hits.store(optional.q4_k_hits, .release);
        total.add(optional);
        return total;
    }

    pub fn lockNativeGeneration(self: *LoadedModel, io: std.Io) void {
        platform.sync.lockYieldingIo(&self.native_generate_lock, io);
    }

    pub fn unlockNativeGeneration(self: *LoadedModel) void {
        self.native_generate_lock.unlock();
    }

    pub fn nativeGenerationMutex(self: *LoadedModel) *std.atomic.Mutex {
        return &self.native_generate_lock;
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
        kind: DeclaredOptionalSessionKind,
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
        path: ?[]const u8,
    ) !bool {
        if (slot.* != null) return false;
        const session_path = path orelse return false;
        const shared_ctx = backends.imported_onnx_session.sharedBackendContext(self.session);
        const strict_backend = [_]backends.BackendType{if (shared_ctx) |shared|
            shared.backendType()
        else
            self.session.backend()};
        var session_manager = self.session_manager.*.withPreferredBackends(
            self.allocator,
            strict_backend[0..],
        );
        const profile_bundle = if (self.kernel_jit_profile_bundle) |*bundle| blk: {
            const mapped = bundle.kernelJitBundleForMode(session_manager.kernel_jit.mode);
            session_manager.kernel_jit.qualified_profile_path = mapped.primary;
            break :blk mapped;
        } else null;
        try bindOptionalSessionProfile(&session_manager, kind, profile_bundle);
        var loaded = try self.model_manager.loadManagedSessionWithAdmissionUsingManager(
            session_path,
            strict_backend[0..],
            shared_ctx,
            &session_manager,
        );
        slot.* = loaded.session;
        lease_slot.* = loaded.resource_lease;
        loaded.owns_session = false;
        loaded.resource_lease = null;
        return true;
    }

    fn releaseOptionalSession(
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
    ) void {
        if (slot.*) |session| session.close();
        slot.* = null;
        if (lease_slot.*) |*lease| lease.release();
        lease_slot.* = null;
    }

    /// Serialize short optional-session slot mutations. Request execution is
    /// protected separately by embedding_asset_gate so primary-only callers
    /// retain shared concurrency.
    pub fn lockEmbeddingAssets(self: *LoadedModel) void {
        spinLock(&self.embedding_session_lock);
    }

    pub fn unlockEmbeddingAssets(self: *LoadedModel) void {
        self.embedding_session_lock.unlock();
    }

    pub fn acquireEmbeddingAssetLease(self: *LoadedModel, include_audio: bool) EmbeddingAssetLease {
        if (include_audio) {
            self.embedding_asset_gate.lockExclusive();
            return .{ .gate = &self.embedding_asset_gate, .access = .exclusive };
        }
        self.embedding_asset_gate.lockShared();
        return .{ .gate = &self.embedding_asset_gate, .access = .shared };
    }

    /// Load every optional model/projection declared by the manifest without
    /// running media inference. Startup preload calls this while the node owns
    /// the exclusive JIT qualification phase.
    pub fn materializeDeclaredOptionalSessions(self: *LoadedModel) !void {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();

        for (declaredOptionalSessions(&self.manifest)) |declared| {
            _ = switch (declared.kind) {
                .vision => try self.ensureOptionalSession(.vision, &self.vision_session, &self.vision_resource_lease, declared.path),
                .audio => try self.ensureOptionalSession(.audio, &self.audio_session, &self.audio_resource_lease, declared.path),
                .text_projection => try self.ensureOptionalSession(.text_projection, &self.text_projection, &self.text_projection_resource_lease, declared.path),
                .visual_projection => try self.ensureOptionalSession(.visual_projection, &self.visual_projection, &self.visual_projection_resource_lease, declared.path),
                .audio_projection => try self.ensureOptionalSession(.audio_projection, &self.audio_projection, &self.audio_projection_resource_lease, declared.path),
            };
        }
        if (!self.declaredOptionalSessionsMaterializedUnlocked())
            return error.OptionalSessionMaterializationIncomplete;
    }

    pub fn declaredOptionalSessionsMaterialized(self: *LoadedModel) bool {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();
        return self.declaredOptionalSessionsMaterializedUnlocked();
    }

    fn declaredOptionalSessionsMaterializedUnlocked(self: *const LoadedModel) bool {
        return declaredOptionalSessionsComplete(&self.manifest, .{
            self.vision_session != null,
            self.audio_session != null,
            self.text_projection != null,
            self.visual_projection != null,
            self.audio_projection != null,
        });
    }

    pub fn ensureVisionSession(self: *LoadedModel) !void {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        _ = try self.ensureOptionalSession(
            .vision,
            &self.vision_session,
            &self.vision_resource_lease,
            self.manifest.visual_model_path,
        );
    }

    pub fn ensureEmbeddingAssets(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        try self.ensureEmbeddingAssetsLocked(include_text, include_image, include_audio);
    }

    pub fn ensureEmbeddingAssetsLocked(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        try self.ensurePrimaryEmbeddingAssetsLocked(include_text, include_image);
        if (include_audio) try self.ensureAudioEmbeddingAssetsLocked();
    }

    const PrimaryEmbeddingAssetAcquisitions = struct {
        text_projection: bool = false,
        vision: bool = false,
        visual_projection: bool = false,
    };

    fn rollbackPrimaryEmbeddingAssetsLocked(self: *LoadedModel, acquired: PrimaryEmbeddingAssetAcquisitions) void {
        if (acquired.visual_projection) releaseOptionalSession(
            &self.visual_projection,
            &self.visual_projection_resource_lease,
        );
        if (acquired.vision) releaseOptionalSession(
            &self.vision_session,
            &self.vision_resource_lease,
        );
        if (acquired.text_projection) releaseOptionalSession(
            &self.text_projection,
            &self.text_projection_resource_lease,
        );
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    /// Admit the reusable text/image sidecars transactionally. A failed
    /// multi-session admission releases only sessions acquired by this call,
    /// preserving already-warm assets for concurrent request efficiency.
    pub fn ensurePrimaryEmbeddingAssetsLocked(self: *LoadedModel, include_text: bool, include_image: bool) !void {
        var acquired = PrimaryEmbeddingAssetAcquisitions{};
        errdefer self.rollbackPrimaryEmbeddingAssetsLocked(acquired);

        if (include_text) {
            acquired.text_projection = try self.ensureOptionalSession(
                .text_projection,
                &self.text_projection,
                &self.text_projection_resource_lease,
                self.manifest.text_projection_path,
            );
        }
        if (include_image) {
            acquired.vision = try self.ensureOptionalSession(
                .vision,
                &self.vision_session,
                &self.vision_resource_lease,
                self.manifest.visual_model_path,
            );
            acquired.visual_projection = try self.ensureOptionalSession(
                .visual_projection,
                &self.visual_projection,
                &self.visual_projection_resource_lease,
                self.manifest.visual_projection_path,
            );
        }
    }

    /// Admit the ephemeral audio sidecars as one phase. Any partial admission
    /// is rolled back immediately so a failed request cannot strand a lease
    /// and prevent subsequent text/image work from making progress.
    pub fn ensureAudioEmbeddingAssetsLocked(self: *LoadedModel) !void {
        errdefer self.releaseAudioEmbeddingAssetsLocked();
        _ = try self.ensureOptionalSession(
            .audio,
            &self.audio_session,
            &self.audio_resource_lease,
            self.manifest.audio_model_path,
        );
        _ = try self.ensureOptionalSession(
            .audio_projection,
            &self.audio_projection,
            &self.audio_projection_resource_lease,
            self.manifest.audio_projection_path,
        );
    }

    /// Release the lazily loaded audio branch after its outputs have been
    /// materialized. The primary CLIP session can then admit text/image work
    /// without counting an idle CLAP session against the same host budget.
    pub fn releaseAudioEmbeddingAssetsLocked(self: *LoadedModel) void {
        releaseOptionalSession(
            &self.audio_projection,
            &self.audio_projection_resource_lease,
        );
        releaseOptionalSession(
            &self.audio_session,
            &self.audio_resource_lease,
        );
        // Audio sidecars are intentionally request-phased rather than cached.
        // Their backend allocations can be large and differently shaped from
        // the persistent vision/text sessions, so complete the physical
        // release at this explicit cold boundary before admitting later work.
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    pub fn embeddingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        return self.embeddingPipelineLocked(allocator);
    }

    pub fn embeddingPipelineLocked(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        const tok = self.getTokenizer();
        const generic_encoder: ?session_factory.GenericEncoderArchConfig = session_factory.getGenericEncoderArchConfig(self.session) catch null;
        const resident_text_encoder = (self.session.backend() == .metal or self.session.backend() == .cuda) and
            if (generic_encoder) |arch| switch (arch) {
                .bert => true,
                .deberta => false,
            } else false;
        var pipeline = EmbeddingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.maxTextSequenceLength(),
            .normalize = self.manifest.normalize,
            .pooling = switch (self.manifest.pooling) {
                .mean => .mean,
                .cls => .cls,
                .max => .max,
                .last => .last,
            },
            .text_prefix = self.manifest.embedding_text_prefix,
            .trim_padding_to_batch_max = isJinaStyleEmbeddingManifest(&self.manifest) or generic_encoder != null,
            .resident_qwen3_embedding = isJinaStyleEmbeddingManifest(&self.manifest),
            .resident_text_encoder = resident_text_encoder,
        });
        if (usesClipImagePreprocessProfile(&self.manifest)) {
            pipeline.config.image_preprocess_profile = .clip;
        }
        if (session_factory.getClipConfig(self.session)) |cfg| {
            pipeline.config.image_size = cfg.image_size;
            if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
        }
        self.bindEmbeddingPipelineAssetsLocked(&pipeline);
        pipeline.resident_projection_stats = &self.resident_projection_stats;
        pipeline.execution_lock = self.embeddingExecutionLock();
        return pipeline;
    }

    /// Refresh borrowed optional-session handles after a phased admission.
    /// Callers hold embedding_session_lock while rebinding and retain an
    /// embedding asset lease for the pipeline's full use.
    pub fn bindEmbeddingPipelineAssetsLocked(self: *LoadedModel, pipeline: *EmbeddingPipeline) void {
        if (session_factory.getClipConfig(self.session) == null) {
            if (self.vision_session) |vs| {
                if (session_factory.getClipConfig(vs)) |cfg| {
                    pipeline.config.image_size = cfg.image_size;
                    if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
                }
            }
        }
        pipeline.vision_session = self.vision_session;
        pipeline.audio_session = self.audio_session;
        pipeline.text_projection = self.text_projection;
        pipeline.visual_projection = self.visual_projection;
        pipeline.audio_projection = self.audio_projection;
    }

    pub fn embeddingExecutionLock(self: *LoadedModel) ?*std.atomic.Mutex {
        return self.targetInferenceExecutionMutex();
    }

    pub fn targetInferenceExecutionMutex(self: *LoadedModel) ?*std.atomic.Mutex {
        return targetInferenceExecutionMutexForBackend(self.session.backend(), &self.target_inference_run_lock);
    }

    pub fn rerankingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) RerankingPipeline {
        const tok = self.getTokenizer();
        var pipeline = RerankingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.maxTextSequenceLength(),
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
        pipeline.execution_lock = self.targetInferenceExecutionMutex();
        return pipeline;
    }

    pub fn classificationPipeline(self: *LoadedModel, allocator: std.mem.Allocator, config: ClassificationConfig) ClassificationPipeline {
        const tok = self.getTokenizer();
        var effective = config;
        effective.max_length = @min(effective.max_length, self.manifest.maxTextSequenceLength());
        effective.distributed = runtime.distributed.configFromEnv();
        return ClassificationPipeline.init(allocator, self.session, tok, effective);
    }

    pub fn nerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) NerPipeline {
        const tok = self.getTokenizer();
        // Cast id2label from ?[][]const u8 to ?[]const []const u8
        const id2label: ?[]const []const u8 = if (self.manifest.id2label) |labels| labels else null;
        return NerPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.maxTextSequenceLength(),
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
            .execution_lock = self.targetInferenceExecutionMutex(),
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
        if (self.kernel_jit_profile_bundle) |*bundle| bundle.deinit();
        if (self.vision_resource_lease) |*lease| lease.release();
        if (self.audio_resource_lease) |*lease| lease.release();
        if (self.text_projection_resource_lease) |*lease| lease.release();
        if (self.visual_projection_resource_lease) |*lease| lease.release();
        if (self.audio_projection_resource_lease) |*lease| lease.release();
        if (self.whisper_prompt_cache) |*cache| cache.deinit();
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

fn targetInferenceExecutionMutexForBackend(
    backend: backends.BackendType,
    model_mutex: *std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return switch (backend) {
        .metal, .cuda => model_mutex,
        else => null,
    };
}

test "embedding asset gate admits concurrent readers and excludes writers" {
    var gate = EmbeddingAssetGate{};

    gate.lockShared();
    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
    try std.testing.expect(!gate.tryLockExclusive());
    gate.unlockShared();

    try std.testing.expect(gate.tryLockExclusive());
    try std.testing.expect(!gate.tryLockShared());
    gate.unlockExclusive();

    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();

    gate.lockShared();
    var lease = EmbeddingAssetLease{ .gate = &gate, .access = .shared };
    lease.release();
    lease.release();
    try std.testing.expect(gate.tryLockExclusive());
    gate.unlockExclusive();
}

test "embedding asset lease downgrades exclusive access without an ownership gap" {
    var gate = EmbeddingAssetGate{};
    gate.lockExclusive();
    var lease = EmbeddingAssetLease{ .gate = &gate, .access = .exclusive };

    lease.downgradeExclusiveToShared();
    try std.testing.expect(lease.held);
    try std.testing.expect(lease.access == .shared);
    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
    try std.testing.expect(!gate.tryLockExclusive());

    lease.release();
    try std.testing.expect(gate.tryLockExclusive());
    gate.unlockExclusive();
}

test "embedding asset gate blocks late readers behind a queued writer" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding)
        return error.SkipZigTest;

    const Writer = struct {
        gate: *EmbeddingAssetGate,
        acquired: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.gate.lockExclusive();
            self.acquired.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
            self.gate.unlockExclusive();
        }
    };

    var gate = EmbeddingAssetGate{};
    gate.lockShared();
    var shared_held = true;
    defer if (shared_held) gate.unlockShared();

    var writer = Writer{ .gate = &gate };
    var writer_thread = try std.Thread.spawn(.{}, Writer.run, .{&writer});
    var writer_joined = false;
    defer if (!writer_joined) {
        if (shared_held) {
            gate.unlockShared();
            shared_held = false;
        }
        writer.release.store(true, .release);
        writer_thread.join();
    };

    var writer_queued = false;
    for (0..200_000) |_| {
        if (!gate.tryLockShared()) {
            writer_queued = true;
            break;
        }
        gate.unlockShared();
        std.Thread.yield() catch {};
    }
    if (!writer_queued) return error.TestTimeout;

    // Once the writer owns the reader gate, a newly arriving reader cannot
    // join the active reader cohort and bypass it.
    try std.testing.expect(!gate.tryLockShared());
    gate.unlockShared();
    shared_held = false;

    var writer_acquired = false;
    for (0..200_000) |_| {
        if (writer.acquired.load(.acquire)) {
            writer_acquired = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    if (!writer_acquired) return error.TestTimeout;
    try std.testing.expect(!gate.tryLockShared());

    writer.release.store(true, .release);
    writer_thread.join();
    writer_joined = true;

    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
}

test "embedding asset rollback closes only newly acquired primary sessions" {
    const CloseProbe = struct {
        close_count: usize = 0,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.close_count += 1;
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var text_probe = CloseProbe{};
    var existing_vision_probe = CloseProbe{};
    var visual_probe = CloseProbe{};
    var model: LoadedModel = undefined;
    model.text_projection = text_probe.session();
    model.vision_session = existing_vision_probe.session();
    model.visual_projection = visual_probe.session();
    model.text_projection_resource_lease = null;
    model.vision_resource_lease = null;
    model.visual_projection_resource_lease = null;

    model.rollbackPrimaryEmbeddingAssetsLocked(.{
        .text_projection = true,
        .vision = false,
        .visual_projection = true,
    });

    try std.testing.expectEqual(@as(usize, 1), text_probe.close_count);
    try std.testing.expectEqual(@as(usize, 0), existing_vision_probe.close_count);
    try std.testing.expectEqual(@as(usize, 1), visual_probe.close_count);
    try std.testing.expect(model.text_projection == null);
    try std.testing.expect(model.vision_session != null);
    try std.testing.expect(model.visual_projection == null);
}

test "audio asset rollback closes every ephemeral session" {
    const CloseProbe = struct {
        close_count: usize = 0,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.close_count += 1;
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var audio_probe = CloseProbe{};
    var projection_probe = CloseProbe{};
    var model: LoadedModel = undefined;
    model.audio_session = audio_probe.session();
    model.audio_projection = projection_probe.session();
    model.audio_resource_lease = null;
    model.audio_projection_resource_lease = null;

    model.releaseAudioEmbeddingAssetsLocked();

    try std.testing.expectEqual(@as(usize, 1), audio_probe.close_count);
    try std.testing.expectEqual(@as(usize, 1), projection_probe.close_count);
    try std.testing.expect(model.audio_session == null);
    try std.testing.expect(model.audio_projection == null);
}

test "target inference gates are GPU-only and model-local" {
    var first: std.atomic.Mutex = .unlocked;
    var second: std.atomic.Mutex = .unlocked;

    try std.testing.expectEqual(&first, targetInferenceExecutionMutexForBackend(.metal, &first).?);
    try std.testing.expectEqual(&second, targetInferenceExecutionMutexForBackend(.metal, &second).?);
    try std.testing.expectEqual(&first, targetInferenceExecutionMutexForBackend(.cuda, &first).?);
    try std.testing.expect(targetInferenceExecutionMutexForBackend(.native, &first) == null);
    try std.testing.expect(targetInferenceExecutionMutexForBackend(.onnx, &first) == null);
    try std.testing.expect(targetInferenceExecutionMutexForBackend(.wasm, &first) == null);
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
    /// The owner already receives a model handle directly from the uncached
    /// load. Keep its flight reference distinguishable so retirement reserves
    /// handles only for waiters that have not adopted theirs yet.
    owner_ref_pending: bool = true,
    /// A retired model detaches its completed flight so a replacement can use
    /// the same key. Detached flights live until existing waiters consume them.
    registered: bool = true,
    /// Retirement reserves one active model handle for every remaining flight
    /// waiter. Waiters adopt those reservations instead of incrementing the
    /// handle count a second time; the owner already has its handle.
    handles_reserved: bool = false,

    fn unadoptedWaiterRefs(self: *const LoadFlight) usize {
        std.debug.assert(self.refs >= @intFromBool(self.owner_ref_pending));
        return self.refs - @intFromBool(self.owner_ref_pending);
    }
};

test "load flight retirement reservations exclude the owner handle" {
    var flight = LoadFlight{ .io = std.testing.io, .refs = 3 };
    try std.testing.expectEqual(@as(usize, 2), flight.unadoptedWaiterRefs());
    flight.owner_ref_pending = false;
    try std.testing.expectEqual(@as(usize, 3), flight.unadoptedWaiterRefs());
}

const WhisperAssetsLoadFlight = struct {
    completed: std.Io.Event = .unset,
    io: std.Io,
    assets: ?*WhisperCompositeAssets = null,
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
    session.run_admission = .{
        .controller = controller,
        .backend_class = backend_class,
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
        var destroy_retired = false;
        self.manager.lockLoadedModels();
        std.debug.assert(model.active_handles > 0);
        model.active_handles -= 1;
        if (model.retired) {
            destroy_retired = model.active_handles == 0;
        } else {
            model.last_used_ns = platform.time.monotonicNs();
        }
        self.manager.unlockLoadedModels();
        self.model = null;
        if (destroy_retired) self.manager.destroyRetiredModel(model);
    }

    /// Stop publishing this cached runtime and release this handle. A fresh
    /// request can reload the same artifact without waiting for process
    /// restart; concurrent users retain the retired object until they unwind.
    pub fn retire(self: *ModelHandle) void {
        const model = self.model orelse return;
        self.manager.retireLoadedModel(model);
        self.release();
    }
};

pub const LoadedModelSnapshot = struct {
    allocator: std.mem.Allocator,
    handles: []ModelHandle,

    pub fn deinit(self: *LoadedModelSnapshot) void {
        for (self.handles) |*handle| handle.release();
        self.allocator.free(self.handles);
        self.handles = &.{};
    }
};

/// Immutable tokenizer and decoder metadata shared by every request using a
/// split encoder/decoder Whisper bundle. The heavyweight sessions continue to
/// use ManagedSession so admission and backend lifetime stay unchanged.
pub const WhisperCompositeAssets = struct {
    managed_tokenizer: ManagedHfTokenizer,
    prompt_cache: whisper_prompt.PromptCache,
    decoder_config: encoder_decoder.DecoderConfig,
    generation: ComponentPlanKey,
    active_handles: usize = 0,
    last_used_ns: u64 = 0,

    pub fn tokenizer(self: *const WhisperCompositeAssets) tokenizer_mod.Tokenizer {
        return self.managed_tokenizer.tokenizer.tokenizer();
    }

    fn reclaimableAdmission(self: *const WhisperCompositeAssets) ?runtime.tier.memory.AdmissionLease {
        return self.managed_tokenizer.reclaimableAdmission();
    }

    fn deinit(self: *WhisperCompositeAssets) void {
        self.prompt_cache.deinit();
        self.managed_tokenizer.deinit();
        self.* = undefined;
    }
};

pub const WhisperAssetsHandle = struct {
    manager: *ModelManager,
    assets: ?*WhisperCompositeAssets,

    pub fn get(self: *const WhisperAssetsHandle) *const WhisperCompositeAssets {
        return self.assets orelse unreachable;
    }

    pub fn release(self: *WhisperAssetsHandle) void {
        const assets = self.assets orelse return;
        self.manager.lockLoadedModels();
        std.debug.assert(assets.active_handles > 0);
        assets.active_handles -= 1;
        assets.last_used_ns = platform.time.monotonicNs();
        self.manager.unlockLoadedModels();
        self.assets = null;
    }
};

/// Declares who owns the process-wide physical memory budget. Direct inference
/// uses its local admission controller. Embedded inference must attach the
/// node ResourceManager bridge before any model can be loaded or served.
pub const ResourceOwnership = enum {
    local,
    external_required,
};

pub const ModelManager = struct {
    const LoadedModelMap = std.StringHashMapUnmanaged(*LoadedModel);
    const tokenizer_cache_budget_shard_count = 16;
    const tokenizer_cache_admission_quantum_bytes = 1024 * 1024;

    const TokenizerCacheBudgetSource = enum {
        none,
        local_manager,
        external_pair,
    };

    const TokenizerCacheBudgetRecord = struct {
        actual_bytes: usize = 0,
        reserved_bytes: usize = 0,
        transitioning: bool = false,
        credits: std.ArrayListUnmanaged(runtime.tier.memory.AdmissionLease) = .empty,
    };

    const TokenizerCacheBudgetShard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        records: std.AutoHashMapUnmanaged(usize, *TokenizerCacheBudgetRecord) = .empty,
    };

    /// Ref-counted resource domain shared by ModelManager and every tokenizer
    /// that adopts its budget capability. Admission state and tokenizer-cache
    /// ledgers live here, rather than inside ModelManager, so physical memory
    /// remains charged until the last escaped tokenizer is destroyed.
    const ResourceDomain = struct {
        allocator: std.mem.Allocator,
        admission: runtime.tier.memory.AdmissionController = .{},
        admission_limits: runtime.tier.memory.Limits = .{},
        tokenizer_cache_budget_source: TokenizerCacheBudgetSource = .none,
        external_tokenizer_cache_budget: ?hf_tokenizer.HfTokenizer.BpeCacheResourceBudget = null,
        tokenizer_cache_budget_shards: [tokenizer_cache_budget_shard_count]TokenizerCacheBudgetShard =
            @as([tokenizer_cache_budget_shard_count]TokenizerCacheBudgetShard, @splat(.{})),
        references: std.atomic.Value(usize) = .init(1),
        closing: std.atomic.Value(bool) = .init(false),
        managed_mutex: std.atomic.Mutex = .unlocked,
        managed_head: ?*ManagedTokenizerLifetime = null,

        fn create(allocator: std.mem.Allocator) !*@This() {
            const lifetime = try allocator.create(@This());
            lifetime.* = .{
                .allocator = allocator,
            };
            return lifetime;
        }

        fn retain(self: *@This()) bool {
            if (self.closing.load(.acquire)) return false;
            var current = self.references.load(.acquire);
            while (current != 0 and current != std.math.maxInt(usize)) {
                if (self.closing.load(.acquire)) return false;
                if (self.references.cmpxchgWeak(
                    current,
                    current + 1,
                    .acq_rel,
                    .acquire,
                )) |observed| {
                    current = observed;
                } else {
                    if (self.closing.load(.acquire)) {
                        self.release();
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        fn release(self: *@This()) void {
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 0);
            if (previous == 1) {
                if (!self.closing.load(.acquire))
                    @panic("inference resource domain lost its final live reference");
                if (self.managed_head != null)
                    @panic("inference resource domain closed with managed tokenizers");
                for (&self.tokenizer_cache_budget_shards) |*shard| {
                    if (shard.records.count() != 0)
                        @panic("inference resource domain closed with tokenizer observers");
                    shard.records.deinit(self.allocator);
                }
                if (!std.meta.eql(
                    self.admission.snapshot(),
                    runtime.tier.memory.AdmissionAmounts{},
                )) @panic("inference resource domain closed with admission leases");
                self.admission.deinit();
                if (self.external_tokenizer_cache_budget) |budget| budget.releaseContext();
                self.allocator.destroy(self);
            }
        }

        fn close(self: *@This()) void {
            const was_closing = self.closing.swap(true, .acq_rel);
            if (was_closing) @panic("inference resource domain closed twice");
        }

        fn registerManagedTokenizer(
            self: *@This(),
            resource_lease: ?runtime.tier.memory.AdmissionLease,
        ) !*ManagedTokenizerLifetime {
            if (self.closing.load(.acquire)) return error.ResourceOwnerShuttingDown;
            const managed = try self.allocator.create(ManagedTokenizerLifetime);
            managed.* = .{
                .lifetime = self,
                .resource_lease = resource_lease,
            };
            if (!self.retain()) {
                self.allocator.destroy(managed);
                return error.ResourceOwnerShuttingDown;
            }

            spinLock(&self.managed_mutex);
            if (self.closing.load(.acquire)) {
                self.managed_mutex.unlock();
                self.allocator.destroy(managed);
                self.release();
                return error.ResourceOwnerShuttingDown;
            }
            managed.next = self.managed_head;
            if (self.managed_head) |head| head.previous = managed;
            self.managed_head = managed;
            self.managed_mutex.unlock();
            return managed;
        }

        fn unregisterManagedTokenizer(
            self: *@This(),
            managed: *ManagedTokenizerLifetime,
        ) void {
            spinLock(&self.managed_mutex);
            if (managed.previous) |previous| {
                previous.next = managed.next;
            } else {
                std.debug.assert(self.managed_head == managed);
                self.managed_head = managed.next;
            }
            if (managed.next) |next| next.previous = managed.previous;
            if (managed.resource_lease) |*lease| lease.release();
            managed.resource_lease = null;
            managed.previous = null;
            managed.next = null;
            self.managed_mutex.unlock();

            self.allocator.destroy(managed);
            self.release();
        }

        fn resourceBudget(
            self: *@This(),
        ) hf_tokenizer.HfTokenizer.BpeCacheResourceBudget {
            return .{
                .context = self,
                .retain_context = retainContext,
                .release_context = releaseContext,
                .observe = observe,
            };
        }

        fn retainContext(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            return self.retain();
        }

        fn releaseContext(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.release();
        }

        fn observe(
            context: *anyopaque,
            observer_id: usize,
            previous: usize,
            next: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.closing.load(.acquire) and next > previous) return false;
            return ModelManager.reconcileTokenizerCacheUsage(self, observer_id, previous, next);
        }
    };

    const ManagedTokenizerLifetime = struct {
        lifetime: *ResourceDomain,
        resource_lease: ?runtime.tier.memory.AdmissionLease = null,
        previous: ?*ManagedTokenizerLifetime = null,
        next: ?*ManagedTokenizerLifetime = null,

        fn release(self: *@This()) void {
            self.lifetime.unregisterManagedTokenizer(self);
        }
    };

    const EvictedModel = struct {
        key: []const u8,
        model: *LoadedModel,
    };

    const EvictedWhisperAssets = struct {
        key: ComponentPlanKey,
        assets: *WhisperCompositeAssets,
    };

    allocator: std.mem.Allocator,
    session_manager: backends.SessionManager,
    /// Null for diagnostic/offline CLIs. Serving Nodes set this before any model load.
    serving_policy: ?model_compatibility.Policy = null,
    admission_enabled: bool = false,
    admission_limit_overrides: runtime.tier.memory.Limits = .{},
    process_memory_limit_bytes: usize = 0,
    process_memory_limit_provenance: runtime.tier.memory.ProcessMemoryLimitProvenance = .automatic,
    forced_run_admission_denials_for_testing: usize = 0,
    resource_ownership: ResourceOwnership = .local,
    tokenizer_cache_budget_source: TokenizerCacheBudgetSource = .none,
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
    whisper_assets: std.AutoHashMapUnmanaged(ComponentPlanKey, *WhisperCompositeAssets) = .empty,
    in_flight_whisper_assets: std.AutoHashMapUnmanaged(ComponentPlanKey, *WhisperAssetsLoadFlight) = .empty,
    component_plan_cache: std.AutoHashMapUnmanaged(
        ComponentPlanKey,
        *ComponentPlanCacheEntry,
    ) = .empty,
    component_plan_cache_lock: std.atomic.Mutex = .unlocked,
    tokenizer_cache_config_mutex: std.atomic.Mutex = .unlocked,
    resource_domain: ?*ResourceDomain = null,
    tokenizer_cache_config: hf_tokenizer.HfTokenizer.BpeCacheConfig = .{},
    tokenizer_parallel_bpe_config: hf_tokenizer.HfTokenizer.ParallelBpeConfig = .{},

    pub fn init(allocator: std.mem.Allocator, session_manager: backends.SessionManager) ModelManager {
        return .{
            .allocator = allocator,
            .session_manager = session_manager,
            .loaded = LoadedModelMap{},
            .loaded_aliases = LoadedModelMap{},
        };
    }

    pub fn configureServingPolicy(self: *ModelManager, policy: model_compatibility.Policy) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.whisper_assets.count() == 0);
        self.serving_policy = policy;
        self.admission_enabled = true;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    pub fn configureResourceOwnership(
        self: *ModelManager,
        ownership: ResourceOwnership,
    ) !void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.whisper_assets.count() == 0);
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (self.tokenizer_cache_budget_source != .none)
            return error.ResourceOwnershipAfterTokenizerBudgetConfiguration;
        self.resource_ownership = ownership;
    }

    pub fn configureProcessMemoryLimit(
        self: *ModelManager,
        limit_bytes: usize,
        provenance: runtime.tier.memory.ProcessMemoryLimitProvenance,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.whisper_assets.count() == 0);
        self.process_memory_limit_bytes = limit_bytes;
        self.process_memory_limit_provenance = provenance;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    pub fn ensureResourceOwnerReady(self: *ModelManager) !void {
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        switch (self.resource_ownership) {
            .local => {
                switch (self.tokenizer_cache_budget_source) {
                    .none => {},
                    .local_manager => return,
                    .external_pair => return error.ExternalTokenizerBudgetInLocalOwnership,
                }
                if (self.tokenizer_cache_config.resource_budget != null)
                    return error.UntrackedTokenizerResourceBudget;
                const lifetime = try self.ensureResourceDomain();
                lifetime.tokenizer_cache_budget_source = .local_manager;
                self.tokenizer_cache_config.resource_budget = lifetime.resourceBudget();
                self.tokenizer_cache_budget_source = .local_manager;
            },
            .external_required => {
                if (self.tokenizer_cache_budget_source != .external_pair)
                    return error.ExternalResourceManagerNotConfigured;
                if (self.tokenizer_cache_config.resource_budget == null)
                    return error.ExternalTokenizerResourceManagerNotConfigured;
            },
        }
    }

    fn ensureResourceDomain(
        self: *ModelManager,
    ) !*ResourceDomain {
        if (self.resource_domain) |lifetime| return lifetime;
        const lifetime = try ResourceDomain.create(self.allocator);
        self.configureAdmissionController(lifetime);
        self.resource_domain = lifetime;
        return lifetime;
    }

    fn configureAdmissionController(
        self: *const ModelManager,
        domain: *ResourceDomain,
    ) void {
        domain.admission.configureSharedLimits(
            runtime.tier.memory.sharedAdmissionLimitsWithOverridesAndProcessLimit(
                self.admission_limit_overrides,
                self.process_memory_limit_bytes,
            ),
        );
        domain.admission.configureProcessMemoryLimit(
            self.process_memory_limit_bytes,
            self.process_memory_limit_provenance,
        );
        domain.admission.configureForcedRunDenialsForTesting(
            self.forced_run_admission_denials_for_testing,
        );
        domain.admission_limits = self.admissionLimitsForBackend(.{ .backend = .native });
    }

    fn admissionController(
        self: *const ModelManager,
    ) *runtime.tier.memory.AdmissionController {
        return &self.resource_domain.?.admission;
    }

    fn tokenizerCacheBudgetShard(
        self: *ResourceDomain,
        observer_id: usize,
    ) *TokenizerCacheBudgetShard {
        return &self.tokenizer_cache_budget_shards[
            observer_id % tokenizer_cache_budget_shard_count
        ];
    }

    fn tokenizerCacheCreditTarget(actual_bytes: usize) usize {
        if (actual_bytes == 0) return 0;
        const remainder = actual_bytes % tokenizer_cache_admission_quantum_bytes;
        if (remainder == 0) return actual_bytes;
        return std.math.add(
            usize,
            actual_bytes,
            tokenizer_cache_admission_quantum_bytes - remainder,
        ) catch actual_bytes;
    }

    fn acquireTokenizerCacheCredit(
        self: *ResourceDomain,
        bytes: usize,
    ) !runtime.tier.memory.AdmissionLease {
        std.debug.assert(bytes > 0);
        const amounts = runtime.tier.memory.AdmissionAmounts{
            .host_weight_bytes = bytes,
        };
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        var lease = try self.admission.tryAcquireWithPressure(
            .cpu,
            self.admission_limits,
            amounts,
            true,
            &pressure,
        );
        errdefer lease.release();
        // Settle the transient live-memory epoch while retaining ordinary lease
        // ownership for the admitted credit. The ModelManager record, not a raw
        // byte total, remains the sole release authority.
        try lease.retain(amounts);
        return lease;
    }

    fn releaseTokenizerCacheCredit(
        record: *TokenizerCacheBudgetRecord,
        released_bytes: usize,
    ) void {
        std.debug.assert(released_bytes <= record.reserved_bytes);
        var remaining = released_bytes;
        while (remaining > 0) {
            const lease = &record.credits.items[record.credits.items.len - 1];
            const owned_bytes = lease.amounts.host_weight_bytes;
            std.debug.assert(owned_bytes > 0);
            if (owned_bytes <= remaining) {
                remaining -= owned_bytes;
                record.reserved_bytes -= owned_bytes;
                lease.release();
                _ = record.credits.pop();
            } else {
                lease.retain(.{
                    .host_weight_bytes = owned_bytes - remaining,
                }) catch unreachable;
                record.reserved_bytes -= remaining;
                remaining = 0;
            }
        }
    }

    fn removeTokenizerCacheBudgetRecord(
        self: *ResourceDomain,
        shard: *TokenizerCacheBudgetShard,
        observer_id: usize,
        record: *TokenizerCacheBudgetRecord,
    ) void {
        const removed = shard.records.fetchRemove(observer_id) orelse return;
        std.debug.assert(removed.value == record);
        std.debug.assert(record.actual_bytes == 0);
        std.debug.assert(record.reserved_bytes == 0);
        std.debug.assert(record.credits.items.len == 0);
        record.credits.deinit(self.allocator);
        self.allocator.destroy(record);
    }

    /// Track exact tokenizer usage while admitting capacity in bounded chunks.
    /// The shard lock protects only identity transitions; potentially blocking
    /// live-memory probes run after marking the record in flight and releasing
    /// that lock. At most one quantum per tokenizer is conservatively unused.
    fn reconcileLocalTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        if (observer_id == 0) return false;
        const shard = tokenizerCacheBudgetShard(self, observer_id);
        spinLock(&shard.mutex);

        var inserted = false;
        const record = shard.records.get(observer_id) orelse blk: {
            if (previous != 0) {
                shard.mutex.unlock();
                return false;
            }
            if (next == 0) {
                shard.mutex.unlock();
                return true;
            }
            const created = self.allocator.create(TokenizerCacheBudgetRecord) catch {
                shard.mutex.unlock();
                return false;
            };
            created.* = .{};
            shard.records.put(self.allocator, observer_id, created) catch {
                self.allocator.destroy(created);
                shard.mutex.unlock();
                return false;
            };
            inserted = true;
            break :blk created;
        };
        if (record.actual_bytes != previous or record.transitioning) {
            shard.mutex.unlock();
            return false;
        }

        const reserved = record.reserved_bytes;
        const rounded_target = tokenizerCacheCreditTarget(next);
        const credit_target = if (next > reserved)
            rounded_target
        else if (next < previous)
            @min(reserved, rounded_target)
        else
            reserved;
        if (credit_target == reserved) {
            record.actual_bytes = next;
            if (next == 0) removeTokenizerCacheBudgetRecord(
                self,
                shard,
                observer_id,
                record,
            );
            shard.mutex.unlock();
            return true;
        }

        if (credit_target > reserved) {
            record.credits.ensureUnusedCapacity(self.allocator, 1) catch {
                if (inserted) removeTokenizerCacheBudgetRecord(
                    self,
                    shard,
                    observer_id,
                    record,
                );
                shard.mutex.unlock();
                return false;
            };
        }
        record.transitioning = true;
        shard.mutex.unlock();

        if (credit_target > reserved) {
            const preferred_growth = credit_target - reserved;
            const required_growth = next -| reserved;
            const credit = acquireTokenizerCacheCredit(self, preferred_growth) catch retry: {
                if (preferred_growth == required_growth) {
                    spinLock(&shard.mutex);
                    record.transitioning = false;
                    if (inserted) removeTokenizerCacheBudgetRecord(
                        self,
                        shard,
                        observer_id,
                        record,
                    );
                    shard.mutex.unlock();
                    return false;
                }
                break :retry acquireTokenizerCacheCredit(self, required_growth) catch {
                    spinLock(&shard.mutex);
                    record.transitioning = false;
                    if (inserted) removeTokenizerCacheBudgetRecord(
                        self,
                        shard,
                        observer_id,
                        record,
                    );
                    shard.mutex.unlock();
                    return false;
                };
            };

            spinLock(&shard.mutex);
            record.credits.appendAssumeCapacity(credit);
            record.reserved_bytes += credit.amounts.host_weight_bytes;
            record.actual_bytes = next;
            record.transitioning = false;
            shard.mutex.unlock();
            return true;
        }

        const released = reserved - credit_target;
        releaseTokenizerCacheCredit(record, released);
        spinLock(&shard.mutex);
        record.actual_bytes = next;
        record.transitioning = false;
        if (next == 0) removeTokenizerCacheBudgetRecord(
            self,
            shard,
            observer_id,
            record,
        );
        shard.mutex.unlock();
        return true;
    }

    fn reconcileExternalTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        if (observer_id == 0) return false;
        const upstream = self.external_tokenizer_cache_budget orelse return false;
        const shard = tokenizerCacheBudgetShard(self, observer_id);
        spinLock(&shard.mutex);

        var inserted = false;
        const record = shard.records.get(observer_id) orelse blk: {
            if (previous != 0) {
                shard.mutex.unlock();
                return false;
            }
            if (next == 0) {
                shard.mutex.unlock();
                return true;
            }
            const created = self.allocator.create(TokenizerCacheBudgetRecord) catch {
                shard.mutex.unlock();
                return false;
            };
            created.* = .{};
            shard.records.put(self.allocator, observer_id, created) catch {
                self.allocator.destroy(created);
                shard.mutex.unlock();
                return false;
            };
            inserted = true;
            break :blk created;
        };
        if (record.actual_bytes != previous or record.transitioning) {
            shard.mutex.unlock();
            return false;
        }
        record.transitioning = true;
        shard.mutex.unlock();

        if (!upstream.observe(upstream.context, observer_id, previous, next)) {
            spinLock(&shard.mutex);
            record.transitioning = false;
            if (inserted) removeTokenizerCacheBudgetRecord(
                self,
                shard,
                observer_id,
                record,
            );
            shard.mutex.unlock();
            return false;
        }

        spinLock(&shard.mutex);
        record.actual_bytes = next;
        record.transitioning = false;
        if (next == 0) removeTokenizerCacheBudgetRecord(
            self,
            shard,
            observer_id,
            record,
        );
        shard.mutex.unlock();
        return true;
    }

    fn reconcileTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        return switch (self.tokenizer_cache_budget_source) {
            .none => false,
            .local_manager => reconcileLocalTokenizerCacheUsage(
                self,
                observer_id,
                previous,
                next,
            ),
            .external_pair => reconcileExternalTokenizerCacheUsage(
                self,
                observer_id,
                previous,
                next,
            ),
        };
    }

    pub fn configureModelCache(
        self: *ModelManager,
        keep_alive_ms: u64,
        max_loaded_models: usize,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.whisper_assets.count() == 0);
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
        return self.acquireRunResourceAmounts(
            backend_class,
            limits,
            .fromEstimate(estimate),
        );
    }

    /// Admit already-classified transient amounts. Callers use this when an
    /// execution owns request-scoped artifacts in addition to KV and scratch.
    pub fn acquireRunResourceAmounts(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        amounts: runtime.tier.memory.AdmissionAmounts,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireAmountsWithEviction(backend_class, limits, amounts);
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
        std.debug.assert(self.whisper_assets.count() == 0);
        self.admission_limit_overrides = overrides;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    /// Attach both halves of one external process owner atomically. Requiring a
    /// paired configuration prevents a model lease and tokenizer cache from
    /// silently charging different managers.
    pub fn configureExternalResourceBudgets(
        self: *ModelManager,
        admission_budget: runtime.tier.memory.AdmissionResourceBudget,
        tokenizer_budget: hf_tokenizer.HfTokenizer.BpeCacheResourceBudget,
    ) !void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.whisper_assets.count() == 0);
        if (self.resource_ownership != .external_required)
            return error.ExternalResourceBudgetInLocalOwnership;
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (self.tokenizer_cache_budget_source != .none)
            return error.ExternalResourceBudgetsAlreadyConfigured;
        if (!admission_budget.hasValidLifetimeHooks() or
            !tokenizer_budget.hasValidLifetimeHooks() or
            admission_budget.retain_context == null or
            tokenizer_budget.retain_context == null)
            return error.ExternalResourceBudgetRequiresLifetimeHooks;
        if (!tokenizer_budget.retainContext())
            return error.ExternalResourceOwnerShuttingDown;
        errdefer tokenizer_budget.releaseContext();
        const lifetime = try self.ensureResourceDomain();
        try lifetime.admission.configureResourceBudget(admission_budget);
        lifetime.external_tokenizer_cache_budget = tokenizer_budget;
        lifetime.tokenizer_cache_budget_source = .external_pair;
        // Ownership pairing changes only provenance. Cache geometry remains
        // the policy selected in NodeConfig/configureTokenizerCaches.
        self.tokenizer_cache_config.resource_budget = lifetime.resourceBudget();
        self.tokenizer_cache_budget_source = .external_pair;
    }

    pub fn configureForcedRunAdmissionDenialsForTesting(
        self: *ModelManager,
        count: usize,
    ) void {
        self.forced_run_admission_denials_for_testing = count;
        if (self.resource_domain) |domain|
            domain.admission.configureForcedRunDenialsForTesting(count);
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
        admission_pressure: ?runtime.tier.memory.AdmissionPressure,
    ) ?EvictedModel {
        const ttl_ns = std.math.mul(
            u64,
            self.keep_alive_ms,
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
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
            if (admission_pressure) |pressure| {
                if (!loadedModelAdmissionReclaimRelevant(model, pressure)) continue;
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
                victim = model;
                oldest_ns = model.last_used_ns;
            }
        }
        return self.takeLoadedModelLocked(victim orelse return null);
    }

    fn takeLoadedModelLocked(
        self: *ModelManager,
        selected: *LoadedModel,
    ) ?EvictedModel {
        var canonical_key: ?[]const u8 = null;
        var loaded_it = self.loaded.iterator();
        while (loaded_it.next()) |entry| {
            if (entry.value_ptr.* == selected) {
                canonical_key = entry.key_ptr.*;
                break;
            }
        }
        const removed = self.loaded.fetchRemove(canonical_key orelse return null) orelse unreachable;
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

    /// Remove a runtime that returned an integrity error from future lookup.
    /// Destruction is deferred to the final active handle, so concurrent
    /// requests never observe freed session state.
    fn retireLoadedModel(self: *ModelManager, model: *LoadedModel) void {
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();

        self.lockLoadedModels();
        if (model.retired) {
            self.unlockLoadedModels();
            return;
        }
        const retired = self.takeLoadedModelLocked(model) orelse {
            self.unlockLoadedModels();
            return;
        };
        std.debug.assert(model.active_handles > 0);
        model.retired = true;
        model.pinned = false;

        // A completed cold-load flight may still have waiters that have not
        // adopted their model handles. Reserve those handles before detaching
        // the old flight, so a replacement load can publish under the same key
        // without freeing state an existing waiter is about to use.
        while (true) {
            var flight_key: ?[]const u8 = null;
            var flight: ?*LoadFlight = null;
            var flight_it = self.in_flight_loads.iterator();
            while (flight_it.next()) |entry| {
                if (entry.value_ptr.*.model == model) {
                    flight_key = entry.key_ptr.*;
                    flight = entry.value_ptr.*;
                    break;
                }
            }
            const pending = flight orelse break;
            std.debug.assert(!pending.handles_reserved);
            const waiter_refs = pending.unadoptedWaiterRefs();
            std.debug.assert(std.math.maxInt(usize) - model.active_handles >= waiter_refs);
            model.active_handles += waiter_refs;
            pending.handles_reserved = true;
            const detached = self.in_flight_loads.fetchRemove(flight_key.?) orelse unreachable;
            std.debug.assert(detached.value == pending);
            pending.registered = false;
            self.allocator.free(detached.key);
        }
        self.unlockLoadedModels();

        std.log.warn("retiring failed inference model path={s} backend={s}", .{
            model.model_dir,
            @tagName(model.session.backend()),
        });
        self.allocator.free(retired.key);
    }

    fn destroyRetiredModel(self: *ModelManager, model: *LoadedModel) void {
        std.debug.assert(model.retired);
        model.deinit();
        self.allocator.destroy(model);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn destroyEvictedModel(self: *ModelManager, evicted: EvictedModel) void {
        std.log.info("evicting inference model path={s} backend={s}", .{
            evicted.model.model_dir,
            @tagName(evicted.model.session.backend()),
        });
        evicted.model.deinit();
        self.allocator.destroy(evicted.model);
        self.allocator.free(evicted.key);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn takeLruWhisperAssetsLocked(
        self: *ModelManager,
        now_ns: u64,
        expired_only: bool,
    ) ?EvictedWhisperAssets {
        const ttl_ns = std.math.mul(
            u64,
            self.keep_alive_ms,
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
        var victim_key: ?ComponentPlanKey = null;
        var victim: ?*WhisperCompositeAssets = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.whisper_assets.iterator();
        while (it.next()) |entry| {
            const assets = entry.value_ptr.*;
            if (assets.active_handles != 0 or self.whisperAssetsIsInFlightLocked(assets)) continue;
            const age_ns = if (now_ns >= assets.last_used_ns)
                now_ns - assets.last_used_ns
            else
                0;
            if (expired_only and
                (self.keep_alive_ms == 0 or age_ns < ttl_ns))
            {
                continue;
            }
            if (victim == null or assets.last_used_ns < oldest_ns) {
                victim_key = entry.key_ptr.*;
                victim = assets;
                oldest_ns = assets.last_used_ns;
            }
        }
        const selected = victim orelse return null;
        const removed = self.whisper_assets.fetchRemove(victim_key.?) orelse unreachable;
        std.debug.assert(removed.value == selected);
        return .{ .key = removed.key, .assets = selected };
    }

    fn whisperAssetsIsInFlightLocked(
        self: *ModelManager,
        assets: *WhisperCompositeAssets,
    ) bool {
        var it = self.in_flight_whisper_assets.valueIterator();
        while (it.next()) |flight_ptr| {
            if (flight_ptr.*.assets == assets) return true;
        }
        return false;
    }

    fn destroyEvictedWhisperAssets(
        self: *ModelManager,
        evicted: EvictedWhisperAssets,
    ) void {
        evicted.assets.deinit();
        self.allocator.destroy(evicted.assets);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn admissionAmountsPresent(amounts: runtime.tier.memory.AdmissionAmounts) bool {
        return amounts.hostTotalBytes() > 0 or amounts.backendTotalBytes() > 0;
    }

    fn admissionReclaimRelevant(
        reclaimable: runtime.tier.memory.AdmissionLease,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        return switch (pressure) {
            .shared_host => reclaimable.amounts.hostTotalBytes() > 0,
            .shared_unified => admissionAmountsPresent(reclaimable.amounts),
            .live_host => reclaimable.amounts.hostTotalBytes() > 0 or
                (builtin.os.tag == .macos and reclaimable.amounts.backendTotalBytes() > 0),
            .domain_host => |backend_class| reclaimable.amounts_by_backend[@backingInt(backend_class)].hostTotalBytes() > 0,
            .domain_backend => |backend_class| reclaimable.amounts_by_backend[@backingInt(backend_class)].backendTotalBytes() > 0,
            .domain_combined => |backend_class| admissionAmountsPresent(
                reclaimable.amounts_by_backend[@backingInt(backend_class)],
            ),
            .domain_kv => |backend_class| reclaimable.amounts_by_backend[@backingInt(backend_class)].kvTotalBytes() > 0,
            .domain_scratch => |backend_class| reclaimable.amounts_by_backend[@backingInt(backend_class)].scratchTotalBytes() > 0,
            // The process-owner budget is intentionally opaque. Any resident
            // admission released from the aggregate can potentially satisfy it.
            .external_budget => admissionAmountsPresent(reclaimable.amounts),
        };
    }

    fn loadedModelAdmissionReclaimRelevant(
        model: *const LoadedModel,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        const lease_slots = [_]*const ?runtime.tier.memory.AdmissionLease{
            &model.resource_lease,
            &model.tokenizer_resource_lease,
            &model.vision_resource_lease,
            &model.audio_resource_lease,
            &model.text_projection_resource_lease,
            &model.visual_projection_resource_lease,
            &model.audio_projection_resource_lease,
        };
        for (lease_slots) |lease_slot| {
            const lease = lease_slot.* orelse continue;
            if (admissionReclaimRelevant(lease, pressure)) return true;
        }
        return false;
    }

    fn takeLruWhisperAssetsForAdmissionLocked(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) ?EvictedWhisperAssets {
        var victim_key: ?ComponentPlanKey = null;
        var victim: ?*WhisperCompositeAssets = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.whisper_assets.iterator();
        while (it.next()) |entry| {
            const assets = entry.value_ptr.*;
            if (assets.active_handles != 0 or self.whisperAssetsIsInFlightLocked(assets)) continue;
            const reclaimable = assets.reclaimableAdmission() orelse continue;
            if (!admissionReclaimRelevant(reclaimable, pressure)) continue;
            if (victim == null or assets.last_used_ns < oldest_ns) {
                victim_key = entry.key_ptr.*;
                victim = assets;
                oldest_ns = assets.last_used_ns;
            }
        }
        const selected = victim orelse return null;
        const removed = self.whisper_assets.fetchRemove(victim_key.?) orelse unreachable;
        std.debug.assert(removed.value == selected);
        return .{ .key = removed.key, .assets = selected };
    }

    /// Called with eviction_lock held. Reclaim matching lightweight tokenizer
    /// state before tearing down a heavyweight model. The admission attempt is
    /// retried after every victim, so a model is still selected immediately when
    /// tokenizer residency cannot relieve the constrained resource class.
    fn evictOneIdleForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        self.lockLoadedModels();
        const now_ns = platform.time.monotonicNs();
        const assets = self.takeLruWhisperAssetsForAdmissionLocked(pressure);
        const model = if (assets == null)
            self.takeLruModelLocked(now_ns, false, pressure)
        else
            null;
        self.unlockLoadedModels();
        if (assets) |evicted| {
            self.destroyEvictedWhisperAssets(evicted);
            return true;
        }
        if (model) |evicted| {
            self.destroyEvictedModel(evicted);
            return true;
        }
        return false;
    }

    fn pressureCanBeRelievedByHostCache(
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        return switch (pressure) {
            .shared_host, .shared_unified, .external_budget, .live_host => true,
            .domain_host, .domain_combined => true,
            .domain_backend, .domain_kv, .domain_scratch => false,
        };
    }

    /// Called with eviction_lock held after no idle model remains. Active
    /// native/PJRT sessions may still own cold, unpinned lazy weights. Reclaim
    /// one entry and let the caller re-probe the authoritative controller;
    /// never infer success from cache counters alone.
    fn reclaimOneActiveCacheForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        if (!pressureCanBeRelievedByHostCache(pressure)) return false;

        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        var candidate: ?*LoadedModel = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.retired or self.modelIsInFlightLocked(model)) continue;
            if (!loadedModelAdmissionReclaimRelevant(model, pressure)) continue;
            if (candidate == null or model.last_used_ns < oldest_ns) {
                candidate = model;
                oldest_ns = model.last_used_ns;
            }
        }
        var model = candidate orelse return false;
        var released_bytes = session_factory.reclaimOneHostCacheEntryForSession(model.session);
        if (released_bytes == 0) {
            // A session may have no unpinned victim at this instant. Do not let
            // one busy model hide reclaimable capacity in another model.
            var fallback_it = self.loaded.iterator();
            while (fallback_it.next()) |entry| {
                const fallback = entry.value_ptr.*;
                if (fallback == model or fallback.retired or
                    self.modelIsInFlightLocked(fallback) or
                    !loadedModelAdmissionReclaimRelevant(fallback, pressure))
                {
                    continue;
                }
                released_bytes = session_factory.reclaimOneHostCacheEntryForSession(fallback.session);
                if (released_bytes != 0) {
                    model = fallback;
                    break;
                }
            }
        }
        if (released_bytes == 0) return false;
        std.log.info(
            "reclaimed inference host cache path={s} backend={s} bytes={d} pressure={s}",
            .{
                model.model_dir,
                @tagName(model.session.backend()),
                released_bytes,
                @tagName(pressure),
            },
        );
        // Cache storage is physically destroyed before its aggregate credit is
        // observed by the next admission probe. On glibc, also return unused
        // arena pages so the cgroup working set reflects that destruction
        // rather than the allocator's historical high-water mark.
        _ = platform.allocator.reclaimUnusedProcessMemory();
        return true;
    }

    fn reclaimOneForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        if (self.evictOneIdleForAdmission(pressure)) return true;
        return self.reclaimOneActiveCacheForAdmission(pressure);
    }

    fn acquireAmountsWithEviction(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        amounts: runtime.tier.memory.AdmissionAmounts,
    ) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        return self.admissionController().tryAcquireWithPressure(
            backend_class,
            limits,
            amounts,
            true,
            &pressure,
        ) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admissionController().tryAcquireWithPressure(
                        backend_class,
                        limits,
                        amounts,
                        true,
                        &pressure,
                    )) |lease| return lease else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => return retry_err,
                    }
                    if (!self.reclaimOneForAdmission(
                        pressure orelse return error.ResourceTemporarilyUnavailable,
                    ))
                        return error.ResourceTemporarilyUnavailable;
                }
            },
            else => return first_err,
        };
    }

    fn acquireRequestsWithEviction(
        self: *ModelManager,
        requests: []const runtime.tier.memory.AdmissionRequest,
    ) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        return self.admissionController().tryAcquireRequestsWithPressure(
            requests,
            true,
            &pressure,
        ) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admissionController().tryAcquireRequestsWithPressure(
                        requests,
                        true,
                        &pressure,
                    )) |lease|
                        return lease
                    else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => return retry_err,
                    }
                    if (!self.reclaimOneForAdmission(
                        pressure orelse return error.ResourceTemporarilyUnavailable,
                    ))
                        return error.ResourceTemporarilyUnavailable;
                }
            },
            else => return first_err,
        };
    }

    fn evictExpired(self: *ModelManager) void {
        if (self.keep_alive_ms == 0) return;
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        const now_ns = platform.time.monotonicNs();
        while (true) {
            self.lockLoadedModels();
            const evicted = self.takeLruModelLocked(now_ns, true, null);
            self.unlockLoadedModels();
            if (evicted == null) break;
            self.destroyEvictedModel(evicted.?);
        }
        while (true) {
            self.lockLoadedModels();
            const evicted = self.takeLruWhisperAssetsLocked(now_ns, true);
            self.unlockLoadedModels();
            if (evicted == null) break;
            self.destroyEvictedWhisperAssets(evicted.?);
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

    fn componentPlanIo(self: *ModelManager) !std.Io {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return self.session_manager.io orelse error.MissingIoRuntime;
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
            try self.componentPlanIo(),
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
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try self.session_manager.requiredBackendCandidates(
            preferred_backends,
            &required_backend_scratch,
        );
        var loader = ComponentLoader{ .manager = self };
        for (component_paths) |path| try loader.addComponentPath(path);
        if (loader.component_path_count == 0) return error.IncompleteModelBundle;
        var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
        defer man.deinit();
        const allowed = if (self.serving_policy) |policy| blk: {
            const key = componentPlanKey(
                model_dir,
                &man,
                effective_backends,
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
                try self.componentPlanIo(),
                inspection.dependencies.items,
            );
            try validateComponentNativeArtifacts(
                self.allocator,
                component_paths,
                effective_backends,
                &inspection,
            );
            try validateComponentImportedGraphs(
                self.allocator,
                component_paths,
                effective_backends,
                &inspection,
            );
            const signature_after = try componentDependencySignature(
                try self.componentPlanIo(),
                inspection.dependencies.items,
            );
            if (!std.mem.eql(
                u8,
                signature_before[0..],
                signature_after[0..],
            )) return error.ModelArtifactsChanging;
            const validated = try policyAllowedComponentBackendsFromInspection(
                &loader.allowed_backends,
                effective_backends,
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
        } else effective_backends;
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
        var limits = runtime.tier.memory.defaultLimitsForBackendWithProcessLimit(
            admissionBackendClassForRuntime(backend_runtime),
            self.process_memory_limit_bytes,
        );
        limits = runtime.tier.memory.applyLimitOverrides(limits, self.admission_limit_overrides);
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
        return self.loadManagedSessionWithAdmissionUsingManager(
            model_path,
            preferred_backends,
            shared_backend_ctx,
            &self.session_manager,
        );
    }

    fn loadManagedSessionWithAdmissionUsingManager(
        self: *ModelManager,
        model_path: []const u8,
        preferred_backends: []const backends.BackendType,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
        source_session_manager: *const backends.SessionManager,
    ) !ManagedSession {
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try source_session_manager.requiredBackendCandidates(
            preferred_backends,
            &required_backend_scratch,
        );
        var first_err: ?anyerror = null;
        var artifact_estimate = if (self.admission_enabled)
            try ComponentArtifactEstimate.init(self.allocator, model_path)
        else
            ComponentArtifactEstimate.disabled;
        defer artifact_estimate.deinit();

        for (effective_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            if (shared_backend_ctx) |shared| {
                if (shared.backendType() != backend) continue;
            }

            var single_backend = [_]backends.BackendType{backend};
            var session_manager = sessionManagerForPreferredBackends(
                self.allocator,
                single_backend[0..],
                source_session_manager,
            );
            const backend_runtime = session_manager.resolveBackendRuntime(backend) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            session_manager.onnx_execution_provider = backend_runtime.onnx_execution_provider;

            var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
            var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
            var admission_limits = runtime.tier.memory.Limits{};
            if (self.admission_enabled) {
                const artifact_bytes = artifact_estimate.bytesForBackend(backend) catch |err| {
                    rememberPreferredLoadError(&first_err, err);
                    continue;
                };
                const plan = if (artifact_estimate == .onnx)
                    onnxModelLoadAdmission(artifact_bytes, backend_runtime)
                else
                    nativeModelLoadAdmission(
                        artifact_bytes,
                        backend_runtime.backend,
                        artifact_estimate.extraBackendResidentAmounts(backend_runtime.backend),
                    );
                const admission_plan = plan catch |err| {
                    rememberPreferredLoadError(&first_err, err);
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
                    rememberPreferredLoadError(&first_err, err);
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
                        self.allocator,
                        &session,
                        self.admissionController(),
                        backend_runtime,
                        admission_limits,
                        resident_amounts,
                        null,
                    ) catch |err| {
                        session.close();
                        if (resource_lease) |*lease| lease.release();
                        return err;
                    };
                }
                return .{
                    .session = session,
                    .resource_lease = resource_lease,
                };
            } else |err| {
                if (resource_lease) |*lease| lease.release();
                rememberPreferredLoadError(&first_err, err);
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

        const managed_lifetime = if (self.resource_domain) |lifetime|
            try lifetime.registerManagedTokenizer(resource_lease)
        else
            null;
        if (managed_lifetime != null) resource_lease = null;

        return .{
            .tokenizer = tokenizer,
            .resource_lease = resource_lease,
            .managed_lifetime = managed_lifetime,
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

    fn canPrewarmBeforeModelPublication(self: *ModelManager) bool {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return modelCacheHasPublicationCapacity(self.loaded.count(), self.max_loaded_models);
    }

    pub fn acquireLoadedModel(self: *ModelManager, model_dir: []const u8) ?ModelHandle {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        const model = self.loaded.get(model_dir) orelse
            self.loaded_aliases.get(model_dir) orelse return null;
        model.active_handles += 1;
        return .{ .manager = self, .model = model };
    }

    /// Pin one handle for every currently published model. Callers may release
    /// load_lock before taking per-model locks without racing model eviction or
    /// retirement destruction.
    pub fn acquireLoadedModelSnapshot(
        self: *ModelManager,
        allocator: std.mem.Allocator,
    ) !LoadedModelSnapshot {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();

        const handles = try allocator.alloc(ModelHandle, self.loaded.count());
        var index: usize = 0;
        var it = self.loaded.valueIterator();
        while (it.next()) |model_ptr| {
            const model = model_ptr.*;
            model.active_handles += 1;
            handles[index] = .{ .manager = self, .model = model };
            index += 1;
        }
        std.debug.assert(index == handles.len);
        return .{ .allocator = allocator, .handles = handles };
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
        if (self.loaded.count() != 0 or self.whisper_assets.count() != 0)
            return error.TokenizerCacheConfigAfterModelLoad;
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (config.resource_budget != null) {
            return switch (self.resource_ownership) {
                .local => error.ExternalTokenizerBudgetInLocalOwnership,
                .external_required => error.ExternalBudgetsMustBeConfiguredTogether,
            };
        }
        var next = config;
        switch (self.tokenizer_cache_budget_source) {
            .none => {},
            .local_manager, .external_pair => next.resource_budget = self.tokenizer_cache_config.resource_budget,
        }
        self.tokenizer_cache_config = next;
    }

    /// Configure persistent std.Io-consumer tokenizer state before loading
    /// models. Private tables use the resource budget supplied through
    /// `configureTokenizerCaches`, so this is a capacity request rather than
    /// an unconditional allocation.
    pub fn configureTokenizerParallelBpe(
        self: *ModelManager,
        config: hf_tokenizer.HfTokenizer.ParallelBpeConfig,
    ) !void {
        if (self.loaded.count() != 0 or self.whisper_assets.count() != 0)
            return error.TokenizerParallelConfigAfterModelLoad;
        self.tokenizer_parallel_bpe_config = config;
    }

    pub fn deinit(self: *ModelManager) void {
        if (self.eviction_io) |io| self.eviction_group.cancel(io);
        std.debug.assert(self.in_flight_loads.count() == 0);
        std.debug.assert(self.in_flight_whisper_assets.count() == 0);
        self.in_flight_loads.deinit(self.allocator);
        self.in_flight_whisper_assets.deinit(self.allocator);
        var component_plan_it = self.component_plan_cache.iterator();
        while (component_plan_it.next()) |entry| entry.value_ptr.*.release();
        self.component_plan_cache.deinit(self.allocator);
        var whisper_assets_it = self.whisper_assets.iterator();
        while (whisper_assets_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.whisper_assets.deinit(self.allocator);
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
        if (self.resource_domain) |lifetime| {
            lifetime.close();
            self.tokenizer_cache_config.resource_budget = null;
            self.resource_domain = null;
            lifetime.release();
        }
        _ = platform.allocator.reclaimUnusedProcessMemory();
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

    pub fn detachPromptCacheResourceUsageObserver(self: *ModelManager) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        var it = self.loaded.valueIterator();
        while (it.next()) |model| model.*.prompt_prefix_cache.detachResourceUsageObserver();
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
    /// schedule that target for every other active cache. The caller owns the
    /// included model's generation lock, so its configure() may evict safely.
    /// Foreign caches must defer eviction until their own serialized request
    /// boundary; their KvManager pools are otherwise concurrently mutable.
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

        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model != include and model.prompt_prefix_cache.isParticipating()) {
                model.prompt_prefix_cache.scheduleConfigure(per_cache);
            }
        }
        // Existing foreign entries are an eventual-eviction overage, not the
        // active model's allocation. scheduleConfigure() immediately prevents
        // those caches from growing past their new shares; charging their old
        // bytes again here can permanently collapse a busy model's share to
        // zero when an idle owner never reaches another request boundary.
        // Prompt-cache max_bytes is an eviction target rather than a hard RSS
        // limit, so let the active owner use its fair share while foreign
        // owners converge safely on their serialized generation lanes.
        include.prompt_prefix_cache.configure(per_cache);
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
                model.prompt_prefix_cache.scheduleConfigure(per_cache);
            }
        }
    }

    fn finishWhisperAssetsLoadFlight(
        self: *ModelManager,
        flight: *WhisperAssetsLoadFlight,
        assets: ?*WhisperCompositeAssets,
        err: ?anyerror,
    ) void {
        self.lockLoadedModels();
        flight.assets = assets;
        flight.err = err;
        self.unlockLoadedModels();
        flight.completed.set(flight.io);
    }

    fn releaseWhisperAssetsLoadFlight(
        self: *ModelManager,
        flight_key: ComponentPlanKey,
        flight: *WhisperAssetsLoadFlight,
    ) void {
        self.lockLoadedModels();
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.unlockLoadedModels();
            return;
        }
        const removed = self.in_flight_whisper_assets.fetchRemove(flight_key) orelse unreachable;
        std.debug.assert(removed.value == flight);
        self.unlockLoadedModels();

        self.allocator.destroy(flight);
    }

    fn waitForWhisperAssetsLoadFlight(
        self: *ModelManager,
        flight_key: ComponentPlanKey,
        flight: *WhisperAssetsLoadFlight,
    ) !WhisperAssetsHandle {
        flight.completed.waitUncancelable(flight.io);
        const assets = flight.assets;
        const maybe_err = flight.err;

        if (assets) |loaded| {
            self.lockLoadedModels();
            loaded.active_handles += 1;
            self.unlockLoadedModels();
        }
        self.releaseWhisperAssetsLoadFlight(flight_key, flight);
        if (maybe_err) |err| return err;
        return .{
            .manager = self,
            .assets = assets orelse return error.TokenizerLoadFailed,
        };
    }

    const ResolvedWhisperSidecars = struct {
        allocator: std.mem.Allocator,
        tokenizer_path: []u8,
        config_path: []u8,
        generation_config_path: ?[]u8,

        fn deinit(self: *ResolvedWhisperSidecars) void {
            self.allocator.free(self.tokenizer_path);
            self.allocator.free(self.config_path);
            if (self.generation_config_path) |path| self.allocator.free(path);
            self.* = undefined;
        }
    };

    fn resolveWhisperSidecars(
        self: *ModelManager,
        model_dir: []const u8,
    ) !ResolvedWhisperSidecars {
        var manifest = try manifest_mod.loadFromDir(self.allocator, model_dir);
        defer manifest.deinit();
        const tokenizer_path = manifest.tokenizer_json_path orelse
            return error.TokenizerNotFound;
        const config_path = manifest.config_path orelse
            return error.InvalidWhisperDecoderConfig;

        const owned_tokenizer_path = try self.allocator.dupe(u8, tokenizer_path);
        errdefer self.allocator.free(owned_tokenizer_path);
        const owned_config_path = try self.allocator.dupe(u8, config_path);
        errdefer self.allocator.free(owned_config_path);

        var receipt = try managed_receipt.loadValidated(
            self.allocator,
            try self.componentPlanIo(),
            model_dir,
        );
        defer if (receipt) |*validated| validated.deinit();
        const generation_config_path: ?[]u8 = if (receipt) |*validated|
            if (validated.find("generation_config.json")) |artifact|
                try self.allocator.dupe(u8, artifact.canonical_path)
            else
                null
        else
            managed_receipt.resolveContainedArtifactPath(
                self.allocator,
                try self.componentPlanIo(),
                model_dir,
                "generation_config.json",
            ) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };

        return .{
            .allocator = self.allocator,
            .tokenizer_path = owned_tokenizer_path,
            .config_path = owned_config_path,
            .generation_config_path = generation_config_path,
        };
    }

    fn whisperAssetGenerationSignature(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !ComponentPlanKey {
        var dependencies = std.ArrayListUnmanaged([]const u8).empty;
        defer dependencies.deinit(self.allocator);
        try dependencies.appendSlice(self.allocator, component_paths);

        const sidecar_names = [_][]const u8{
            "tokenizer.json",
            "config.json",
            "generation_config.json",
            managed_receipt.complete_filename,
            managed_receipt.in_progress_filename,
            managed_receipt.plan_filename,
        };
        var owned_paths = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_paths.items) |path| self.allocator.free(path);
            owned_paths.deinit(self.allocator);
        }
        try owned_paths.ensureTotalCapacity(self.allocator, sidecar_names.len);
        for (sidecar_names) |name| {
            const path = try std.fs.path.join(self.allocator, &.{ model_dir, name });
            owned_paths.appendAssumeCapacity(path);
            try dependencies.append(self.allocator, path);
        }
        return componentDependencySignature(try self.componentPlanIo(), dependencies.items);
    }

    fn loadWhisperCompositeAssetsUncached(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
        asset_generation: ComponentPlanKey,
    ) !*WhisperCompositeAssets {
        var sidecars = try self.resolveWhisperSidecars(model_dir);
        defer sidecars.deinit();

        var managed_tokenizer = try self.loadManagedHfTokenizerFile(sidecars.tokenizer_path);
        errdefer managed_tokenizer.deinit();
        var prompt_cache = try whisper_prompt.PromptCache.initFromPaths(
            self.allocator,
            sidecars.generation_config_path,
            sidecars.config_path,
            managed_tokenizer.tokenizer.tokenizer(),
        );
        errdefer prompt_cache.deinit();
        const decoder_config = try encoder_decoder.loadDecoderConfigFile(
            self.allocator,
            sidecars.config_path,
        );
        const verified_generation = try self.whisperAssetGenerationSignature(
            model_dir,
            component_paths,
        );
        if (!std.mem.eql(u8, asset_generation[0..], verified_generation[0..]))
            return error.ModelArtifactsChanging;

        const assets = try self.allocator.create(WhisperCompositeAssets);
        assets.* = .{
            .managed_tokenizer = managed_tokenizer.take(),
            .prompt_cache = prompt_cache,
            .decoder_config = decoder_config,
            .generation = asset_generation,
        };
        prompt_cache = undefined;
        return assets;
    }

    fn publishWhisperCompositeAssets(
        self: *ModelManager,
        asset_generation: ComponentPlanKey,
        assets: *WhisperCompositeAssets,
    ) !WhisperAssetsHandle {
        var assets_owned = true;
        errdefer if (assets_owned) {
            assets.deinit();
            self.allocator.destroy(assets);
            _ = platform.allocator.reclaimUnusedProcessMemory();
        };
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        while (true) {
            self.lockLoadedModels();
            if (self.whisper_assets.get(asset_generation)) |existing| {
                existing.active_handles += 1;
                self.unlockLoadedModels();
                assets.deinit();
                self.allocator.destroy(assets);
                _ = platform.allocator.reclaimUnusedProcessMemory();
                assets_owned = false;
                return .{ .manager = self, .assets = existing };
            }

            const configured_capacity = if (self.max_loaded_models > 0)
                self.max_loaded_models
            else
                whisper_assets_cache_capacity;
            const capacity = @max(
                @as(usize, 1),
                @min(configured_capacity, whisper_assets_cache_capacity),
            );
            if (self.whisper_assets.count() >= capacity) {
                const evicted = self.takeLruWhisperAssetsLocked(
                    platform.time.monotonicNs(),
                    false,
                );
                self.unlockLoadedModels();
                if (evicted == null) return error.ResourceTemporarilyUnavailable;
                self.destroyEvictedWhisperAssets(evicted.?);
                continue;
            }

            self.whisper_assets.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                self.unlockLoadedModels();
                return err;
            };
            assets.active_handles = 1;
            assets.last_used_ns = platform.time.monotonicNs();
            self.whisper_assets.putAssumeCapacity(asset_generation, assets);
            self.unlockLoadedModels();
            assets_owned = false;
            return .{ .manager = self, .assets = assets };
        }
    }

    /// Acquire immutable tokenizer and decoder metadata for a split Whisper
    /// bundle. Cold construction is single-flight; warm requests perform one
    /// map lookup and keep the entry pinned through the returned handle.
    pub fn acquireWhisperCompositeAssets(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !WhisperAssetsHandle {
        const asset_generation = try self.whisperAssetGenerationSignature(
            model_dir,
            component_paths,
        );
        self.lockLoadedModels();
        if (self.whisper_assets.get(asset_generation)) |assets| {
            assets.active_handles += 1;
            self.unlockLoadedModels();
            return .{ .manager = self, .assets = assets };
        }
        if (self.in_flight_whisper_assets.get(asset_generation)) |flight| {
            flight.refs += 1;
            self.unlockLoadedModels();
            return self.waitForWhisperAssetsLoadFlight(asset_generation, flight);
        }

        const flight = self.allocator.create(WhisperAssetsLoadFlight) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        const coordination_io = self.session_manager.io orelse {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return error.MissingIoRuntime;
        };
        flight.* = .{ .io = coordination_io };
        self.in_flight_whisper_assets.put(
            self.allocator,
            asset_generation,
            flight,
        ) catch |err| {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return err;
        };
        self.unlockLoadedModels();

        const assets = self.loadWhisperCompositeAssetsUncached(
            model_dir,
            component_paths,
            asset_generation,
        ) catch |err| {
            self.finishWhisperAssetsLoadFlight(flight, null, err);
            self.releaseWhisperAssetsLoadFlight(asset_generation, flight);
            return err;
        };
        const handle = self.publishWhisperCompositeAssets(asset_generation, assets) catch |err| {
            self.finishWhisperAssetsLoadFlight(flight, null, err);
            self.releaseWhisperAssetsLoadFlight(asset_generation, flight);
            return err;
        };
        self.finishWhisperAssetsLoadFlight(flight, handle.assets, null);
        self.releaseWhisperAssetsLoadFlight(asset_generation, flight);
        return handle;
    }

    /// Verify that sessions loaded after asset acquisition still describe the
    /// same immutable publication. A model pull may atomically replace the
    /// directory between those operations; fail the request instead of pairing
    /// sessions and token IDs from different generations.
    pub fn validateWhisperAssetsCurrent(
        self: *ModelManager,
        handle: *const WhisperAssetsHandle,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !void {
        const current = try self.whisperAssetGenerationSignature(model_dir, component_paths);
        const assets = handle.get();
        if (!std.mem.eql(u8, assets.generation[0..], current[0..]))
            return error.ModelArtifactsChanging;
    }

    /// Acquire a model for the duration of one operation. The returned handle
    /// prevents eviction until release.
    pub fn acquireFromDir(self: *ModelManager, model_dir: []const u8) !ModelHandle {
        return self.loadFromDirCoordinated(
            model_dir,
            self.session_manager.preferred_backends,
            true,
            inheritedA4bCachePolicy(self.session_manager.a4b_inference_request, false),
        );
    }

    pub fn acquireFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !ModelHandle {
        return self.loadFromDirCoordinated(
            model_dir,
            preferred_backends,
            cache_default_alias,
            inheritedA4bCachePolicy(self.session_manager.a4b_inference_request, true),
        );
    }

    /// Acquire a model using an immutable A4B policy for this load. Explicit
    /// policies do not accept an existing unqualified alias: the qualified
    /// cache key must match before a model can be reused. A newly qualified
    /// preload may still publish the default alias so later policy-free
    /// requests reuse the warmed session; its policy-specific variant key
    /// remains the ownership key.
    pub fn acquireFromDirWithA4bRequest(
        self: *ModelManager,
        model_dir: []const u8,
        a4b_request: backend_contracts.A4bInferenceRequest,
    ) !ModelHandle {
        return self.loadFromDirCoordinated(
            model_dir,
            self.session_manager.preferred_backends,
            true,
            .{ .a4b_request = a4b_request, .accept_default_alias = false },
        );
    }

    pub fn acquireFromDirWithPreferredBackendsAndA4bRequest(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        a4b_request: backend_contracts.A4bInferenceRequest,
    ) !ModelHandle {
        return self.loadFromDirCoordinated(
            model_dir,
            preferred_backends,
            cache_default_alias,
            .{ .a4b_request = a4b_request, .accept_default_alias = false },
        );
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
        _: bool,
        policy: ModelLoadCachePolicy,
    ) !?*LoadedModel {
        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            const variant_key = try backendVariantCacheKey(
                self.allocator,
                model_dir,
                backend,
                policy.a4b_request,
            );
            defer self.allocator.free(variant_key);
            if (self.loaded.get(variant_key)) |model| return model;
            if (self.loaded_aliases.get(variant_key)) |model| return model;
        }
        if (policy.accept_default_alias) {
            if (self.loaded.get(model_dir)) |model|
                if (!policy.require_default_alias_backend_match or
                    loadedModelUsesPreferredBackend(model, preferred_backends)) return model;
            if (self.loaded_aliases.get(model_dir)) |model|
                if (!policy.require_default_alias_backend_match or
                    loadedModelUsesPreferredBackend(model, preferred_backends)) return model;
        }
        return null;
    }

    fn loadFlightKey(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        policy: ModelLoadCachePolicy,
    ) ![]u8 {
        const prefix = if (policy.a4b_request) |request|
            try std.fmt.allocPrint(
                self.allocator,
                "{d}:{s}:{d}:{d}:{d}:a4b={s}:{d}:",
                .{
                    model_dir.len,
                    model_dir,
                    @intFromBool(cache_default_alias),
                    @intFromBool(policy.accept_default_alias),
                    @intFromBool(policy.require_default_alias_backend_match),
                    @tagName(request.residency_mode),
                    request.memory_budget_mb,
                },
            )
        else
            try std.fmt.allocPrint(
                self.allocator,
                "{d}:{s}:{d}:{d}:{d}:a4b=none:",
                .{
                    model_dir.len,
                    model_dir,
                    @intFromBool(cache_default_alias),
                    @intFromBool(policy.accept_default_alias),
                    @intFromBool(policy.require_default_alias_backend_match),
                },
            );
        defer self.allocator.free(prefix);
        const key = try self.allocator.alloc(u8, prefix.len + preferred_backends.len);
        @memcpy(key[0..prefix.len], prefix);
        for (preferred_backends, 0..) |backend, idx| {
            key[prefix.len + idx] = @intCast(@backingInt(backend));
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
        var removed_key: ?[]const u8 = null;
        self.lockLoadedModels();
        std.debug.assert(flight.owner_ref_pending);
        flight.owner_ref_pending = false;
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.unlockLoadedModels();
            return;
        }
        if (flight.registered) {
            const removed = self.in_flight_loads.fetchRemove(flight_key) orelse unreachable;
            std.debug.assert(removed.value == flight);
            flight.registered = false;
            removed_key = removed.key;
        }
        self.unlockLoadedModels();

        if (removed_key) |key| self.allocator.free(key);
        self.allocator.destroy(flight);
    }

    fn waitForLoadFlight(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
    ) !ModelHandle {
        flight.completed.waitUncancelable(flight.io);
        var removed_key: ?[]const u8 = null;
        var destroy_flight = false;
        self.lockLoadedModels();
        const model = flight.model;
        const maybe_err = flight.err;
        if (model) |loaded| {
            if (!flight.handles_reserved) loaded.active_handles += 1;
        }
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs == 0) {
            if (flight.registered) {
                const removed = self.in_flight_loads.fetchRemove(flight_key) orelse unreachable;
                std.debug.assert(removed.value == flight);
                flight.registered = false;
                removed_key = removed.key;
            }
            destroy_flight = true;
        }
        self.unlockLoadedModels();

        if (removed_key) |key| self.allocator.free(key);
        if (destroy_flight) self.allocator.destroy(flight);
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
        policy: ModelLoadCachePolicy,
    ) !ModelHandle {
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try self.session_manager.requiredBackendCandidates(
            preferred_backends,
            &required_backend_scratch,
        );
        const flight_key = try self.loadFlightKey(
            model_dir,
            effective_backends,
            cache_default_alias,
            policy,
        );
        defer self.allocator.free(flight_key);

        self.lockLoadedModels();
        const cached = self.lookupLoadedModelLocked(
            model_dir,
            effective_backends,
            cache_default_alias,
            policy,
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
        const coordination_io = self.session_manager.io orelse {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return error.MissingIoRuntime;
        };
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
            effective_backends,
            &self.session_manager,
        );
        session_manager.a4b_inference_request = policy.a4b_request;
        self.unlockLoadedModels();

        var handle = self.loadFromDirUncached(
            model_dir,
            &session_manager,
            cache_default_alias,
            policy.a4b_request,
        ) catch |err| {
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
        a4b_request: ?backend_contracts.A4bInferenceRequest,
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

        var qualified_profile_bundle: ?kernel_jit_profile_output.LoadedProfileBundle =
            if (sm.kernel_jit.qualified_profile_path) |path|
                try kernel_jit_profile_output.loadQualifiedProfileBundleIfPresent(
                    self.allocator,
                    sm.io orelse std.Options.debug_io,
                    path,
                )
            else
                null;
        errdefer if (qualified_profile_bundle) |*bundle| bundle.deinit();
        if (qualified_profile_bundle) |*bundle| {
            sm.kernel_jit.qualified_profile_path = bundle.kernelJitBundle().primary;
            if (!sm.kernel_jit.mode.failClosed() and !bundle.hasQualifiedKernels(.primary)) {
                std.log.warn("kernel JIT profile bundle primary has no qualified winner; using bundled kernels", .{});
                sm.kernel_jit.qualified_profile_path = null;
                sm.kernel_jit.mode = .off;
            }
        }

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

        var whisper_prompt_cache: ?whisper_prompt.PromptCache = if (session_factory.getWhisperConfig(session) != null)
            try whisper_prompt.PromptCache.init(
                self.allocator,
                model_dir,
                if (hf_tok) |ht| ht.tokenizer() else sp_tok.?.tokenizer(),
            )
        else
            null;
        errdefer if (whisper_prompt_cache) |*cache| cache.deinit();

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
            .whisper_prompt_cache = whisper_prompt_cache,
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
            .kernel_jit_profile_bundle = null,
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
        whisper_prompt_cache = null;
        shared_moe_cache = null;
        shared_prefetch = null;
        native_generate_coordinator = null;
        owned_model_dir_owned = false;
        model_storage_owned = false;
        loaded_session.resource_lease = null;

        // Publication performs max-loaded eviction. When the cache is already
        // full, prewarming first makes the incoming runtime compete with the
        // still-resident eviction victim and emits a predictable resource
        // warning. Skip that speculative work; generation prepares the runtime
        // lazily after publication has reclaimed the victim.
        if (build_options.enable_metal and
            self.canPrewarmBeforeModelPublication() and
            shouldUseMetalWholeModelExecutor(session))
        {
            if (session_factory.getGptConfig(session)) |gpt_config| {
                if (graph_mod.metal_executor.supportsSession(session)) {
                    _ = graph_mod.metal_executor.prewarmSharedDecoderRuntime(self.allocator, session, gpt_config) catch |err| {
                        std.log.warn("metal decoder-runtime prewarm failed for {s}: {s}", .{ model_dir, @errorName(err) });
                    };
                }
            }
        }

        model.kernel_jit_profile_bundle = qualified_profile_bundle;
        qualified_profile_bundle = null;

        return self.publishLoadedModel(model, cache_default_alias, a4b_request);
    }

    /// Publish a fully constructed model with only a short map critical section.
    /// Distinct request keys can occasionally converge on the same actual backend;
    /// in that case retain the first model and dispose of the duplicate outside
    /// the lock.
    fn publishLoadedModel(
        self: *ModelManager,
        model: *LoadedModel,
        cache_default_alias: bool,
        a4b_request: ?backend_contracts.A4bInferenceRequest,
    ) !ModelHandle {
        var model_owned = true;
        errdefer if (model_owned) {
            model.deinit();
            self.allocator.destroy(model);
            _ = platform.allocator.reclaimUnusedProcessMemory();
        };

        const variant_key = try backendVariantCacheKey(
            self.allocator,
            model.model_dir,
            model.session.backend(),
            a4b_request,
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
                _ = platform.allocator.reclaimUnusedProcessMemory();
                model_owned = false;
                return .{ .manager = self, .model = existing };
            }

            if (self.max_loaded_models > 0 and
                self.loaded.count() >= self.max_loaded_models)
            {
                const evicted = self.takeLruModelLocked(
                    platform.time.monotonicNs(),
                    false,
                    null,
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

    fn destroyLoadedModel(self: *ModelManager, model: *LoadedModel) void {
        model.deinit();
        self.allocator.destroy(model);
    }
};

fn modelCacheHasPublicationCapacity(loaded_count: usize, max_loaded_models: usize) bool {
    return max_loaded_models == 0 or loaded_count < max_loaded_models;
}

const ModelLoadCachePolicy = struct {
    a4b_request: ?backend_contracts.A4bInferenceRequest = null,
    accept_default_alias: bool = true,
    /// Policy-free acquisition consumes the model's explicitly published
    /// default alias regardless of current node preference. A caller that
    /// supplied an explicit backend order still requires an exact match.
    require_default_alias_backend_match: bool = true,
};

fn inheritedA4bCachePolicy(
    request: ?backend_contracts.A4bInferenceRequest,
    require_default_alias_backend_match: bool,
) ModelLoadCachePolicy {
    return .{
        .a4b_request = request,
        // Publishing a new default alias and consuming an existing one are
        // separate decisions. Policy-free requests may reuse a matching
        // qualified preload even when their call site does not publish new
        // aliases. Explicit A4B policies still require their exact variant key.
        .accept_default_alias = request == null,
        .require_default_alias_backend_match = require_default_alias_backend_match,
    };
}

fn loadedModelUsesPreferredBackend(
    model: *const LoadedModel,
    preferred_backends: []const backends.BackendType,
) bool {
    const actual = model.session.backend();
    for (preferred_backends) |preferred| {
        if (preferred == actual) return true;
    }
    return false;
}

fn backendVariantCacheKey(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: backends.BackendType,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) ![]u8 {
    if (a4b_request) |request| {
        return std.fmt.allocPrint(
            allocator,
            "{s}\nbackend={s}\na4b={s}:{d}",
            .{
                model_dir,
                @tagName(backend),
                @tagName(request.residency_mode),
                request.memory_budget_mb,
            },
        );
    }
    return std.fmt.allocPrint(allocator, "{s}\nbackend={s}", .{ model_dir, @tagName(backend) });
}

test "model prewarm defers while max-loaded eviction is pending" {
    try std.testing.expect(modelCacheHasPublicationCapacity(10, 0));
    try std.testing.expect(modelCacheHasPublicationCapacity(0, 1));
    try std.testing.expect(!modelCacheHasPublicationCapacity(1, 1));
    try std.testing.expect(!modelCacheHasPublicationCapacity(2, 1));
}

test "A4B model cache keys isolate residency policies" {
    const allocator = std.testing.allocator;
    const default_key = try backendVariantCacheKey(allocator, "model", .metal, null);
    defer allocator.free(default_key);
    const streamed_key = try backendVariantCacheKey(
        allocator,
        "model",
        .metal,
        .{ .residency_mode = .streamed, .memory_budget_mb = 4096 },
    );
    defer allocator.free(streamed_key);
    const resident_key = try backendVariantCacheKey(
        allocator,
        "model",
        .metal,
        .{ .residency_mode = .resident, .memory_budget_mb = 16384 },
    );
    defer allocator.free(resident_key);

    try std.testing.expect(!std.mem.eql(u8, default_key, streamed_key));
    try std.testing.expect(!std.mem.eql(u8, streamed_key, resident_key));
    try std.testing.expect(std.mem.endsWith(u8, streamed_key, "a4b=streamed:4096"));
}

test "inherited A4B policy isolates explicit loads but reuses policy-free aliases" {
    const request = backend_contracts.A4bInferenceRequest{
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    };
    const isolated = inheritedA4bCachePolicy(request, false);
    try std.testing.expectEqual(request, isolated.a4b_request.?);
    try std.testing.expect(!isolated.accept_default_alias);
    try std.testing.expect(inheritedA4bCachePolicy(null, false).accept_default_alias);
}

test "explicit backend lookup reuses only a matching default alias" {
    const BackendProbe = struct {
        backend_type: backends.BackendType,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(ptr: *anyopaque) backends.BackendType {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backend_type;
        }
        fn close(_: *anyopaque) void {}
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.loaded.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var probe = BackendProbe{ .backend_type = .metal };
    var model: LoadedModel = undefined;
    model.session = probe.session();
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "model"),
        &model,
    );

    manager.lockLoadedModels();
    const matching = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        false,
        inheritedA4bCachePolicy(null, true),
    );
    const mismatching = try manager.lookupLoadedModelLocked(
        "model",
        &.{.native},
        false,
        inheritedA4bCachePolicy(null, true),
    );
    const policy_free = try manager.lookupLoadedModelLocked(
        "model",
        &.{.native},
        true,
        inheritedA4bCachePolicy(null, false),
    );
    manager.unlockLoadedModels();

    try std.testing.expect(matching == &model);
    try std.testing.expect(mismatching == null);
    try std.testing.expect(policy_free == &model);
}

test "explicit A4B loads ignore unqualified model aliases" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var loaded = manager.loaded.iterator();
        while (loaded.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var default_model: LoadedModel = undefined;
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "model"),
        &default_model,
    );
    const request = backend_contracts.A4bInferenceRequest{
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    };
    manager.lockLoadedModels();
    const alias_miss = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        true,
        .{ .a4b_request = request, .accept_default_alias = false },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(alias_miss == null);

    var streamed_model: LoadedModel = undefined;
    const streamed_key = try backendVariantCacheKey(allocator, "model", .metal, request);
    try manager.loaded.put(allocator, streamed_key, &streamed_model);
    manager.lockLoadedModels();
    const qualified_hit = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        true,
        .{ .a4b_request = request, .accept_default_alias = false },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(qualified_hit == &streamed_model);
}

fn admissionEvictionTestModel(last_used_ns: u64) LoadedModel {
    var model: LoadedModel = undefined;
    model.active_handles = 0;
    model.last_used_ns = last_used_ns;
    model.pinned = false;
    model.resource_lease = null;
    model.tokenizer_resource_lease = null;
    model.vision_resource_lease = null;
    model.audio_resource_lease = null;
    model.text_projection_resource_lease = null;
    model.visual_projection_resource_lease = null;
    model.audio_projection_resource_lease = null;
    return model;
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
    const evicted = manager.takeLruModelLocked(100, false, null);
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
        .{},
    );
    manager.unlockLoadedModels();
    try std.testing.expect(after_eviction == null);
    allocator.free(evicted.?.key);
}

test "loaded model snapshot pins model lifetimes until release" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var model: LoadedModel = undefined;
    model.active_handles = 0;
    model.last_used_ns = 1;
    model.pinned = false;
    model.retired = false;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "model"), &model);

    var snapshot = try manager.acquireLoadedModelSnapshot(allocator);
    try std.testing.expectEqual(@as(usize, 1), snapshot.handles.len);
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    try std.testing.expect(snapshot.handles[0].get() == &model);

    snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 0), model.active_handles);
}

test "failed loaded model retires from lookup while active handles unwind" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    const SessionProbe = struct {
        fn run(_: *anyopaque, _: []const backends.Tensor, alloc: std.mem.Allocator) ![]backends.Tensor {
            return alloc.alloc(backends.Tensor, 0);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}

        var state: u8 = 0;
        const vtable = backends.Session.VTable{
            .run = run,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var model: LoadedModel = undefined;
    model.model_dir = "owner/model";
    model.session = .{ .ptr = @ptrCast(&SessionProbe.state), .vtable = &SessionProbe.vtable };
    model.active_handles = 2;
    model.last_used_ns = 1;
    model.pinned = true;
    model.retired = false;

    try manager.loaded.put(allocator, try allocator.dupe(u8, "owner/model\nbackend=metal"), &model);
    try manager.loaded_aliases.put(allocator, try allocator.dupe(u8, "owner/model"), &model);
    const flight = try allocator.create(LoadFlight);
    defer allocator.destroy(flight);
    flight.* = .{
        .io = std.testing.io,
        .model = &model,
        .refs = 2,
        .owner_ref_pending = false,
    };
    try manager.in_flight_loads.put(allocator, try allocator.dupe(u8, "flight"), flight);

    manager.retireLoadedModel(&model);

    try std.testing.expect(model.retired);
    try std.testing.expect(!model.pinned);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    try std.testing.expectEqual(@as(usize, 0), manager.in_flight_loads.count());
    try std.testing.expectEqual(@as(usize, 4), model.active_handles);
    try std.testing.expect(!flight.registered);
    try std.testing.expect(flight.handles_reserved);
}

test "admission eviction skips older models outside the rejected domain" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    const cpu_host = runtime.tier.memory.AdmissionAmounts{ .host_weight_bytes = 64 };
    const gpu_backend = runtime.tier.memory.AdmissionAmounts{ .backend_weight_bytes = 64 };
    var older_cpu = admissionEvictionTestModel(1);
    older_cpu.resource_lease = .{
        .controller = null,
        .amounts = cpu_host,
        .amounts_by_backend = .{ cpu_host, .{} },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };
    var newer_gpu = admissionEvictionTestModel(10);
    // Sidecar leases must participate in victim selection just like the primary
    // model lease; multimodal models can hold most of their GPU residency here.
    newer_gpu.vision_resource_lease = .{
        .controller = null,
        .amounts = gpu_backend,
        .amounts_by_backend = .{ .{}, gpu_backend },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };

    try manager.loaded.put(allocator, try allocator.dupe(u8, "older-cpu"), &older_cpu);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "newer-gpu"), &newer_gpu);

    manager.lockLoadedModels();
    const evicted = manager.takeLruModelLocked(
        100,
        false,
        .{ .domain_backend = .gpu },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(evicted != null);
    try std.testing.expect(evicted.?.model == &newer_gpu);
    allocator.free(evicted.?.key);

    manager.lockLoadedModels();
    const no_relevant_victim = manager.takeLruModelLocked(
        100,
        false,
        .{ .domain_backend = .gpu },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(no_relevant_victim == null);
    try std.testing.expectEqual(@as(usize, 1), manager.loaded.count());
    try std.testing.expect(manager.loaded.get("older-cpu") == &older_cpu);
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
    const disabled = manager.takeLruModelLocked(std.time.ns_per_s, true, null);
    manager.unlockLoadedModels();
    try std.testing.expect(disabled == null);

    manager.keep_alive_ms = 1;
    manager.lockLoadedModels();
    const expired = manager.takeLruModelLocked(std.time.ns_per_s, true, null);
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
        .required_backend = source.required_backend,
        .required_backend_invalid = source.required_backend_invalid,
        .graph_runtime_strategy = source.graph_runtime_strategy,
        .kernel_jit = source.kernel_jit,
        .kernel_jit_load_context = source.kernel_jit_load_context,
        .a4b_inference_request = source.a4b_inference_request,
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
    managed_lifetime: ?*ModelManager.ManagedTokenizerLifetime = null,
    owns_tokenizer: bool = true,

    fn reclaimableAdmission(self: *const ManagedHfTokenizer) ?runtime.tier.memory.AdmissionLease {
        if (self.resource_lease) |lease| return lease;
        const managed = self.managed_lifetime orelse return null;
        const lifetime = managed.lifetime;
        spinLock(&lifetime.managed_mutex);
        defer lifetime.managed_mutex.unlock();
        return managed.resource_lease;
    }

    pub fn deinit(self: *ManagedHfTokenizer) void {
        if (self.owns_tokenizer) self.tokenizer.deinitSelf();
        if (self.resource_lease) |*lease| lease.release();
        if (self.managed_lifetime) |managed| managed.release();
        self.resource_lease = null;
        self.managed_lifetime = null;
        self.owns_tokenizer = false;
    }

    pub fn take(self: *ManagedHfTokenizer) ManagedHfTokenizer {
        const owned = self.*;
        self.resource_lease = null;
        self.managed_lifetime = null;
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

    fn extraBackendResidentAmounts(
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

test "admission eviction only selects leases that relieve the rejected constraint" {
    const cpu_host = runtime.tier.memory.AdmissionAmounts{ .host_weight_bytes = 64 };
    const tokenizer_lease = runtime.tier.memory.AdmissionLease{
        .controller = null,
        .amounts = cpu_host,
        .amounts_by_backend = .{ cpu_host, .{} },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };

    try std.testing.expect(ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .shared_host,
    ));
    try std.testing.expect(ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_host = .cpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_host = .gpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_backend = .cpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_scratch = .cpu },
    ));
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
    } else {
        try addArtifactBytes(&total, man.allocator, man.onnx_path);
    }

    if (total == 0) return error.NoModelFileFound;
    return total;
}

/// A multimodal projector is opened, mapped, and consumed by an individual
/// generation request; it is not part of the decoder session's retained
/// residency. Charge its mapped artifact to the request that owns that mapping.
pub fn projectorRunAdmissionAmounts(
    man: manifest_mod.ModelManifest,
) !runtime.tier.memory.AdmissionAmounts {
    const path = man.gguf_projector_path orelse return .{};
    const bytes = std.math.cast(
        usize,
        try c_file.fileSize(man.allocator, path),
    ) orelse return error.ResourceLimitExceeded;
    return .{ .host_weight_bytes = bytes };
}

fn estimateModelLoadAdmission(
    model_path: []const u8,
    man: manifest_mod.ModelManifest,
    backend_runtime: backends.BackendRuntime,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !ModelLoadAdmissionPlan {
    const weights = try estimateModelArtifactBytes(man, backend_runtime.backend);
    const uses_onnx_artifact = backend_runtime.backend == .onnx or !manifestHasNativeAssets(man);
    if (uses_onnx_artifact) return onnxModelLoadAdmission(weights, backend_runtime);
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

fn a4bGpuModelLoadAdmission(
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
    return .{ .peak = peak, .resident = resident };
}

test "A4B GPU admission lease equals the configured memory envelope" {
    const config = try backend_contracts.buildCudaA4bInferenceConfig(
        null,
        backend_contracts.qualified_a4b_geometries[0],
    );
    try std.testing.expectEqual(
        @as(u64, backend_contracts.qualified_cuda_a4b_memory_budget_mb) * 1024 * 1024,
        config.memory_budget_bytes,
    );
    const metal_plan = a4bGpuModelLoadAdmission(config, 1234, .metal);
    try std.testing.expectEqual(@as(usize, @intCast(config.memory_budget_bytes)), metal_plan.resident.backendTotalBytes());
    try std.testing.expectEqual(metal_plan.resident, metal_plan.peak);
    const cuda_plan = a4bGpuModelLoadAdmission(config, 1234, .cuda);
    try std.testing.expectEqual(cuda_plan.resident.backendTotalBytes(), cuda_plan.peak.backendTotalBytes());
    try std.testing.expectEqual(@as(usize, 1234), cuda_plan.peak.host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), cuda_plan.resident.host_weight_bytes);
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
    var required_backend_scratch: [1]backends.BackendType = undefined;
    const policy_backends = try source_session_manager.requiredBackendCandidates(
        preferred_backends,
        &required_backend_scratch,
    );
    var effective_scratch: [7]backends.BackendType = undefined;
    const effective_backends = effectiveLoadBackends(&effective_scratch, policy_backends, man);
    const fail_closed_cuda_a4b = source_session_manager.a4b_inference_request == null and
        backends.backendOrderSelectsCudaBeforeCpu(effective_backends) and
        (try session_factory.resolveCudaA4bInferenceConfigForModelListing(
            manager.allocator,
            model_dir,
            man,
            null,
        )) != null;
    // Keep the first real failure. Reporting a blanket NoModelFileFound hides the
    // actionable cause: a GGUF whose tensors could not be resolved fails with
    // MissingRequiredWeights, and callers were being told the file did not exist.
    var first_err: ?anyerror = null;
    for (effective_backends) |backend| {
        if (fail_closed_cuda_a4b and !backend.supportsA4bSession()) {
            std.log.err(
                "loadModel({s}) qualified CUDA A4B artifact rejected CPU fallback after GPU admission failure",
                .{model_dir},
            );
            return first_err orelse error.A4bCudaAutoFallbackForbidden;
        }
        if (!backend.supportsDirectSessionLoad()) continue;
        if (source_session_manager.a4b_inference_request != null and
            backend != .metal and backend != .cuda)
        {
            rememberPreferredLoadError(&first_err, error.A4bRequiresGpu);
            continue;
        }
        if (source_session_manager.kernel_jit.mode.failClosed() and
            !backend.supportsKernelJitSession()) continue;
        const candidate_path = preferredModelPathForBackend(model_dir, man, backend) orelse continue;
        if (source_session_manager.kernel_jit.mode.failClosed() and
            std.mem.endsWith(u8, candidate_path, ".onnx")) continue;
        var single_backend = [_]backends.BackendType{backend};
        var backend_session_manager = sessionManagerForPreferredBackends(manager.allocator, single_backend[0..], source_session_manager);
        const backend_runtime = backend_session_manager.resolveBackendRuntime(backend) catch |err| {
            rememberPreferredLoadError(&first_err, err);
            continue;
        };
        backend_session_manager.onnx_execution_provider = backend_runtime.onnx_execution_provider;
        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
        var admission_limits = runtime.tier.memory.Limits{};
        if (manager.admission_enabled) {
            const admission_plan = estimateModelLoadAdmission(
                model_dir,
                man,
                backend_runtime,
                source_session_manager.a4b_inference_request,
            ) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resident_amounts = admission_plan.resident;
            admission_limits = manager.admissionLimitsForBackend(backend_runtime);
            if (admittedSessionCudaLimit(
                backend_runtime,
                resident_amounts,
            ) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }) |cuda_limit| {
                backend_session_manager.onnx_cuda_memory_limit_bytes = cuda_limit;
            }
            resource_lease = manager.acquireAmountsWithEviction(
                admissionBackendClassForRuntime(backend_runtime),
                admission_limits,
                admission_plan.peak,
            ) catch |err| {
                std.log.warn(
                    "loadModel({s}) backend {s} admission failed: {s}; peak_host={d} peak_backend={d} host_limit={d} backend_limit={d} combined_limit={d}",
                    .{
                        model_dir,
                        @tagName(backend),
                        @errorName(err),
                        admission_plan.peak.host_weight_bytes +| admission_plan.peak.host_kv_bytes +| admission_plan.peak.host_scratch_bytes,
                        admission_plan.peak.backend_weight_bytes +| admission_plan.peak.backend_kv_bytes +| admission_plan.peak.backend_scratch_bytes,
                        admission_limits.host_limit_bytes,
                        admission_limits.backend_limit_bytes,
                        admission_limits.combined_limit_bytes,
                    },
                );
                rememberPreferredLoadError(&first_err, err);
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
                    manager.allocator,
                    &session,
                    manager.admissionController(),
                    backend_runtime,
                    admission_limits,
                    resident_amounts,
                    &man,
                ) catch |err| {
                    session.close();
                    if (resource_lease) |*lease| lease.release();
                    return err;
                };
            }
            return .{ .session = session, .resource_lease = resource_lease };
        } else |err| {
            if (resource_lease) |*lease| lease.release();
            std.log.warn("loadModel({s}) backend {s} failed: {s}", .{ model_dir, @tagName(backend), @errorName(err) });
            rememberPreferredLoadError(&first_err, err);
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

fn loadErrorPriority(err: anyerror) u2 {
    return switch (err) {
        // Admission pressure is actionable at the HTTP boundary: callers can
        // retry it after the current model request or eviction completes.
        error.ResourceTemporarilyUnavailable => 3,
        // A stable capacity-policy rejection is more useful than a backend
        // probe miss, but must not replace a retryable alternative.
        error.ResourceLimitExceeded => 2,
        // These are expected while walking a preferred-backend list and carry
        // less information than an actual import or model-contract failure.
        error.NoBackendAvailable, error.UnsupportedBackend => 0,
        else => 1,
    };
}

fn rememberPreferredLoadError(selected: *?anyerror, candidate: anyerror) void {
    const current = selected.* orelse {
        selected.* = candidate;
        return;
    };
    if (loadErrorPriority(candidate) > loadErrorPriority(current)) {
        selected.* = candidate;
    }
}

test "external resource ownership fails closed until budget is attached" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();

    try manager.configureResourceOwnership(.external_required);
    try std.testing.expectError(
        error.ExternalResourceManagerNotConfigured,
        manager.ensureResourceOwnerReady(),
    );

    const Bridge = struct {
        fn retainContext(_: *anyopaque) bool {
            return true;
        }
        fn releaseContext(_: *anyopaque) void {}
        fn reserve(_: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    const CacheBridge = struct {
        fn observe(_: *anyopaque, _: usize, _: usize, _: usize) bool {
            return true;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(
        error.ExternalBudgetsMustBeConfiguredTogether,
        manager.configureTokenizerCaches(.{
            .resource_budget = .{
                .context = &context,
                .observe = CacheBridge.observe,
            },
        }),
    );
    try manager.configureExternalResourceBudgets(
        .{
            .context = &context,
            .retain_context = Bridge.retainContext,
            .release_context = Bridge.releaseContext,
            .try_reserve = Bridge.reserve,
            .retain = Bridge.retain,
            .release = Bridge.release,
        },
        .{
            .context = &context,
            .retain_context = Bridge.retainContext,
            .release_context = Bridge.releaseContext,
            .observe = CacheBridge.observe,
        },
    );
    try manager.ensureResourceOwnerReady();
}

test "local resource ownership rejects external tokenizer provenance" {
    const CacheBridge = struct {
        fn observe(_: *anyopaque, _: usize, _: usize, _: usize) bool {
            return true;
        }
    };
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    defer manager.deinit();
    var context: u8 = 0;

    try std.testing.expectError(
        error.ExternalTokenizerBudgetInLocalOwnership,
        manager.configureTokenizerCaches(.{
            .resource_budget = .{
                .context = &context,
                .observe = CacheBridge.observe,
            },
        }),
    );
    try manager.ensureResourceOwnerReady();
    try std.testing.expectError(
        error.ResourceOwnershipAfterTokenizerBudgetConfiguration,
        manager.configureResourceOwnership(.external_required),
    );
}

test "local resource ownership amortizes exact tokenizer cache usage" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();

    manager.configureServingPolicy(.{});
    const quantum = ModelManager.tokenizer_cache_admission_quantum_bytes;
    manager.configureAdmissionLimits(.{
        .host_limit_bytes = quantum + 20,
        .combined_limit_bytes = quantum + 20,
    });
    try manager.ensureResourceOwnerReady();
    const budget = manager.tokenizer_cache_config.resource_budget.?;

    try std.testing.expect(budget.observe(budget.context, 71, 0, 16));
    try std.testing.expectEqual(
        quantum,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    // Growth inside the admitted quantum performs no additional admission or
    // live-memory probe, but the observer retains the exact current value.
    try std.testing.expect(budget.observe(budget.context, 71, 16, quantum - 1));
    try std.testing.expectEqual(
        quantum,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(!budget.observe(budget.context, 71, 0, 0));
    // The preferred second quantum exceeds policy, so admission retries only
    // the required delta instead of denying useful cache capacity.
    try std.testing.expect(budget.observe(
        budget.context,
        71,
        quantum - 1,
        quantum + 16,
    ));
    try std.testing.expectEqual(
        quantum + 16,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(budget.observe(
        budget.context,
        71,
        quantum + 16,
        quantum + 15,
    ));
    try std.testing.expectEqual(
        quantum + 16,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(!budget.observe(
        budget.context,
        71,
        quantum + 15,
        quantum + 21,
    ));
    try std.testing.expect(budget.observe(budget.context, 71, quantum + 15, 7));
    try std.testing.expectEqual(quantum, manager.admissionController().snapshot().host_weight_bytes);
    try std.testing.expect(budget.observe(budget.context, 71, 7, 0));
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        manager.admissionController().snapshot(),
    );
}

test "local tokenizer lifecycle releases its persistent admission credit" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "c": 3, "ab": 4, "abc": 5},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{
        .host_limit_bytes = 4 * ModelManager.tokenizer_cache_admission_quantum_bytes,
        .combined_limit_bytes = 4 * ModelManager.tokenizer_cache_admission_quantum_bytes,
    });
    try manager.ensureResourceOwnerReady();

    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tokenizer.deinitSelf();
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    for (0..4) |_| {
        const ids = try tokenizer.tokenizer().encode(allocator, "abc");
        allocator.free(ids);
    }
    const actual_cache_bytes = tokenizer.bpeCacheStats().used_bytes;
    try std.testing.expectEqual(
        ModelManager.tokenizerCacheCreditTarget(actual_cache_bytes),
        manager.admissionController().snapshot().host_weight_bytes,
    );

    tokenizer.deinitSelf();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        manager.admissionController().snapshot(),
    );
    for (&manager.resource_domain.?.tokenizer_cache_budget_shards) |*shard|
        try std.testing.expectEqual(@as(usize, 0), shard.records.count());
}

test "managed tokenizer safely outlives ModelManager shutdown" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "ab": 3},
        \\    "merges": ["a b"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data = tokenizer_json,
    });
    const tokenizer_path = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "tokenizer.json" },
    );
    defer allocator.free(tokenizer_path);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    manager.configureServingPolicy(.{});
    try manager.ensureResourceOwnerReady();
    var managed = try manager.loadManagedHfTokenizerFile(tokenizer_path);
    try std.testing.expect(managed.managed_lifetime != null);
    try std.testing.expect(manager.admissionController().snapshot().host_weight_bytes > 0);

    const domain = manager.resource_domain.?;
    try std.testing.expect(domain.retain());
    defer domain.release();

    // Manager shutdown closes new growth but keeps physical tokenizer memory
    // charged in the independently owned resource domain.
    manager.deinit();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);
    managed.deinit();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        domain.admission.snapshot(),
    );
}

test "raw tokenizer budget capability pins resource domain through teardown" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {"type": "BPE", "vocab": {"a": 1}, "merges": []},
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    manager.configureServingPolicy(.{});
    try manager.ensureResourceOwnerReady();

    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    const domain = manager.resource_domain.?;
    try std.testing.expect(domain.retain());
    defer domain.release();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);

    manager.deinit();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);
    tokenizer.deinitSelf();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        domain.admission.snapshot(),
    );
}

test "external ownership pairing preserves cache policy and drains observers" {
    const AdmissionBridge = struct {
        fn retainContext(_: *anyopaque) bool {
            return true;
        }
        fn releaseContext(_: *anyopaque) void {}
        fn reserve(_: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    const CacheBridge = struct {
        current: usize = 0,
        transitions: usize = 0,
        alive: bool = true,
        callbacks_after_shutdown: usize = 0,

        fn observe(
            context: *anyopaque,
            _: usize,
            previous: usize,
            next: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (!self.alive) {
                self.callbacks_after_shutdown += 1;
                return false;
            }
            if (self.current != previous) return false;
            self.current = next;
            self.transitions += 1;
            return true;
        }
    };

    var admission_context: u8 = 0;
    var cache_context = CacheBridge{};
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    var manager_closed = false;
    defer if (!manager_closed) manager.deinit();
    try manager.configureResourceOwnership(.external_required);
    try manager.configureTokenizerCaches(.{
        .max_bytes = 7 * 1024 * 1024,
        .bulk_slots_per_shard = 1024,
    });
    try manager.configureExternalResourceBudgets(
        .{
            .context = &admission_context,
            .retain_context = AdmissionBridge.retainContext,
            .release_context = AdmissionBridge.releaseContext,
            .try_reserve = AdmissionBridge.reserve,
            .retain = AdmissionBridge.retain,
            .release = AdmissionBridge.release,
        },
        .{
            .context = &cache_context,
            .retain_context = AdmissionBridge.retainContext,
            .release_context = AdmissionBridge.releaseContext,
            .observe = CacheBridge.observe,
        },
    );
    try manager.ensureResourceOwnerReady();
    try std.testing.expectEqual(
        @as(usize, 7 * 1024 * 1024),
        manager.tokenizer_cache_config.max_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 1024),
        manager.tokenizer_cache_config.bulk_slots_per_shard,
    );

    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "ab": 3},
        \\    "merges": ["a b"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(
        std.testing.allocator,
        tokenizer_json,
    );
    var tokenizer_owned = true;
    errdefer if (tokenizer_owned) tokenizer.deinitSelf();
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    const managed_lifetime = try manager.resource_domain.?
        .registerManagedTokenizer(null);
    var managed = ManagedHfTokenizer{
        .tokenizer = tokenizer,
        .managed_lifetime = managed_lifetime,
    };
    tokenizer_owned = false;
    defer managed.deinit();
    try std.testing.expect(cache_context.current > 0);

    manager.deinit();
    manager_closed = true;
    try std.testing.expect(cache_context.current > 0);

    // The external owner stays retained until physical tokenizer destruction,
    // which performs the exact final observer transition.
    managed.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache_context.current);
    try std.testing.expect(cache_context.transitions >= 2);
    cache_context.alive = false;
    try std.testing.expectEqual(@as(usize, 0), cache_context.callbacks_after_shutdown);
}

test "model loading preserves retryable admission errors across backend probes" {
    var selected: ?anyerror = null;
    rememberPreferredLoadError(&selected, error.NoBackendAvailable);
    rememberPreferredLoadError(&selected, error.MissingWeight);
    rememberPreferredLoadError(&selected, error.ResourceTemporarilyUnavailable);
    rememberPreferredLoadError(&selected, error.ResourceLimitExceeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.ResourceTemporarilyUnavailable),
        selected,
    );
}

test "model loading prefers actionable import errors over backend probe misses" {
    var selected: ?anyerror = error.UnsupportedBackend;
    rememberPreferredLoadError(&selected, error.ShapeMismatch);
    rememberPreferredLoadError(&selected, error.NoBackendAvailable);
    try std.testing.expectEqual(@as(?anyerror, error.ShapeMismatch), selected);
}

test "serving admission applies the node-wide host-memory override" {
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    defer manager.deinit();

    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 100 });
    try manager.ensureResourceOwnerReady();
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.admissionController().shared_limits.host_limit_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.resource_domain.?.admission_limits.host_limit_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.resource_domain.?.admission_limits.combined_limit_bytes,
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

test "optional sessions retain the selected backend before fallbacks" {
    const preferred = [_]backends.BackendType{ .metal, .native };
    const owned = try ownSelectedFirstBackendPreference(
        std.testing.allocator,
        &preferred,
        .native,
    );
    defer std.testing.allocator.free(owned);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .native, .metal }, owned);

    const added = try ownSelectedFirstBackendPreference(
        std.testing.allocator,
        &preferred,
        .cuda,
    );
    defer std.testing.allocator.free(added);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .cuda, .metal, .native }, added);
}

test "required preload completeness includes every declared optional session" {
    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .visual_model_path = "vision.gguf",
        .audio_model_path = "audio.gguf",
        .text_projection_path = "text-projection.onnx",
        .visual_projection_path = "visual-projection.onnx",
        .audio_projection_path = "audio-projection.onnx",
    };
    const declared = declaredOptionalSessions(&manifest);
    try std.testing.expectEqual(@as(usize, 5), declared.len);
    try std.testing.expect(!declaredOptionalSessionsComplete(
        &manifest,
        .{ true, true, true, true, false },
    ));
    try std.testing.expect(declaredOptionalSessionsComplete(
        &manifest,
        .{ true, true, true, true, true },
    ));

    const text_only = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    try std.testing.expect(declaredOptionalSessionsComplete(
        &text_only,
        .{ false, false, false, false, false },
    ));
}

test "optional sessions bind only their component-qualified profile" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    manager.kernel_jit = .{ .mode = .on, .qualified_profile_path = "primary.json" };
    const bundle: graph_mod.kernel_jit.QualifiedProfileBundle = .{
        .primary = "primary.json",
        .vision = "vision.json",
        .audio = "audio.json",
    };

    try bindOptionalSessionProfile(&manager, .audio, bundle);
    try std.testing.expectEqualStrings("audio.json", manager.kernel_jit.qualified_profile_path.?);
    try bindOptionalSessionProfile(&manager, .text_projection, bundle);
    try std.testing.expect(manager.kernel_jit.qualified_profile_path == null);
    try std.testing.expectEqual(graph_mod.kernel_jit.Mode.off, manager.kernel_jit.mode);

    manager.kernel_jit = .{ .mode = .required, .qualified_profile_path = "primary.json" };
    try std.testing.expectError(
        error.MissingKernelJitProfileBundleComponent,
        bindOptionalSessionProfile(&manager, .text_projection, bundle),
    );

    manager.kernel_jit = .{ .mode = .on, .qualified_profile_path = "primary.json" };
    try bindOptionalSessionProfile(&manager, .vision, null);
    try std.testing.expect(manager.kernel_jit.qualified_profile_path == null);
    try std.testing.expectEqual(graph_mod.kernel_jit.Mode.off, manager.kernel_jit.mode);
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

test "required backend is applied before model manager backend planning" {
    const allocator = std.testing.allocator;
    var source = backends.SessionManager.init(allocator);
    source.preferred_backends = &.{ .native, .onnx };
    source.required_backend = .cuda;
    source.required_backend_invalid = false;

    var required_scratch: [1]backends.BackendType = undefined;
    const policy_backends = try source.requiredBackendCandidates(
        source.preferred_backends,
        &required_scratch,
    );

    var load_scratch: [7]backends.BackendType = undefined;
    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");

    const effective = effectiveLoadBackends(&load_scratch, policy_backends, classifier);
    try std.testing.expectEqualSlices(backends.BackendType, &.{.cuda}, effective);
}

test "model manager rejects an unavailable required backend before artifact selection" {
    const allocator = std.testing.allocator;
    var session_manager = backends.SessionManager.init(allocator);
    session_manager.required_backend = .pjrt;
    session_manager.required_backend_invalid = false;
    var manager = ModelManager.init(allocator, session_manager);
    defer manager.deinit();

    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        manager.acquireFromDir("/nonexistent/required-backend-model"),
    );
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        manager.componentLoaderForPaths(
            "/nonexistent/required-backend-model",
            &.{.native},
            &.{"/nonexistent/component"},
        ),
    );
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

test "published Gemma 4 E2B decoder and projector pass native admission when configured" {
    const allocator = std.testing.allocator;
    const decoder_env = std.c.getenv("ANTFLY_TEST_GEMMA4_E2B_DECODER_GGUF") orelse
        return error.SkipZigTest;
    const projector_env = std.c.getenv("ANTFLY_TEST_GEMMA4_E2B_PROJECTOR_GGUF") orelse
        return error.SkipZigTest;
    const decoder_path = std.mem.span(decoder_env);
    const projector_path = std.mem.span(projector_env);
    const model_dir = std.fs.path.dirname(decoder_path) orelse ".";

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .gguf_path = try allocator.dupe(u8, decoder_path),
        .gguf_projector_path = try allocator.dupe(u8, projector_path),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        model_dir,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);
}

test "published ClipClap GGUF pair passes native admission when configured" {
    const allocator = std.testing.allocator;
    const clip_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLIP_GGUF") orelse
        return error.SkipZigTest;
    const clap_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLAP_GGUF") orelse
        return error.SkipZigTest;

    const summary = try validateClipclapBundleForBackend(
        allocator,
        std.mem.span(clip_env),
        std.mem.span(clap_env),
        .native,
    );
    try std.testing.expect(summary == null);
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

    var required_session_manager = backends.SessionManager.init(allocator);
    required_session_manager.required_backend = .cuda;
    required_session_manager.required_backend_invalid = false;
    var required_manager = ModelManager.init(allocator, required_session_manager);
    defer required_manager.deinit();
    const required_loader = try required_manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), required_loader.allowed_backend_count);
    try std.testing.expectEqual(backends.BackendType.cuda, required_loader.allowed_backends[0]);

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

test "split Whisper assets remain model-lifetime cached across request handles" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"decoder_start_token_id":7,"eos_token_id":8,"max_length":99}
        ,
    });
    try dir.dir.createDir(std.testing.io, "other", .default_dir);
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "other/tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "other/config.json",
        .data =
        \\{"decoder_start_token_id":9,"eos_token_id":8,"max_length":88}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);
    const other_root = try std.fs.path.join(allocator, &.{ root, "other" });
    defer allocator.free(other_root);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureModelCache(0, 2);

    var first = try manager.acquireWhisperCompositeAssets(root, &.{});
    defer first.release();
    const cached = first.get();
    try std.testing.expectEqual(@as(usize, 99), cached.decoder_config.max_length);
    try std.testing.expectEqual(@as(i32, 7), cached.decoder_config.decoder_start_token_id);
    try std.testing.expectEqual(@as(usize, 2), cached.prompt_cache.language_tokens.len);

    // A republished directory gets a distinct immutable generation even while
    // requests still hold the previous tokenizer and prompt metadata.
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"},{"id":14,"content":"new-generation"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13,"new-generation":14},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"decoder_start_token_id":17,"eos_token_id":8,"max_length":77}
        ,
    });
    var second = try manager.acquireWhisperCompositeAssets(root, &.{});
    defer second.release();
    try std.testing.expect(cached != second.get());
    try std.testing.expectEqual(@as(i32, 17), second.get().decoder_config.decoder_start_token_id);
    try std.testing.expectEqual(@as(usize, 77), second.get().decoder_config.max_length);
    try std.testing.expectEqual(@as(usize, 2), manager.whisper_assets.count());
    try std.testing.expectError(
        error.ModelArtifactsChanging,
        manager.validateWhisperAssetsCurrent(&first, root, &.{}),
    );
    try manager.validateWhisperAssetsCurrent(&second, root, &.{});

    // Capacity pressure cannot invalidate an entry while either request holds it.
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        manager.acquireWhisperCompositeAssets(other_root, &.{}),
    );
    second.release();
    first.release();

    // Once idle, the oldest generation is reclaimable and all admission-owned
    // tokenizer memory moves to the replacement entry.
    var replacement = try manager.acquireWhisperCompositeAssets(other_root, &.{});
    defer replacement.release();
    try std.testing.expectEqual(@as(usize, 2), manager.whisper_assets.count());
    try std.testing.expectEqual(@as(usize, 88), replacement.get().decoder_config.max_length);
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

test "gpu model admission classifies weights and persistent scratch" {
    const weights: usize = 1024 * 1024;
    const cuda = try nativeModelLoadAdmission(weights, .cuda, .{});
    try std.testing.expectEqual(weights, cuda.peak.host_weight_bytes);
    try std.testing.expect(cuda.peak.backend_weight_bytes >= weights);
    try std.testing.expectEqual(@as(usize, 0), cuda.resident.host_weight_bytes);
    try std.testing.expectEqual(
        cuda.peak.backend_weight_bytes,
        cuda.resident.backend_weight_bytes,
    );

    const extra_backend_resident = runtime.tier.memory.AdmissionAmounts{
        .backend_weight_bytes = 384 * 1024,
        .backend_scratch_bytes = 64 * 1024,
    };
    const metal = try nativeModelLoadAdmission(weights, .metal, extra_backend_resident);
    try std.testing.expectEqual(weights / 4, metal.peak.host_weight_bytes);
    try std.testing.expectEqual(weights + extra_backend_resident.backend_weight_bytes, metal.peak.backend_weight_bytes);
    try std.testing.expectEqual(extra_backend_resident.backend_scratch_bytes, metal.peak.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), metal.resident.host_weight_bytes);
    try std.testing.expectEqual(weights + extra_backend_resident.backend_weight_bytes, metal.resident.backend_weight_bytes);
    try std.testing.expectEqual(extra_backend_resident.backend_scratch_bytes, metal.resident.backend_scratch_bytes);
}

test "projector residency follows its request-scoped lifecycle" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const decoder_bytes: usize = 8192;
    const projector_bytes: usize = 4096;
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = &(@as([decoder_bytes]u8, @splat(0x31))),
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "mmproj.gguf",
        .data = &(@as([projector_bytes]u8, @splat(0x32))),
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .gguf_path = try std.fs.path.join(allocator, &.{ root, "model.gguf" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    try std.testing.expectEqual(
        decoder_bytes,
        try estimateModelArtifactBytes(man, .native),
    );
    const request = try projectorRunAdmissionAmounts(man);
    try std.testing.expectEqual(projector_bytes, request.host_weight_bytes);
    try std.testing.expectEqual(projector_bytes, request.hostTotalBytes());
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
        .data = &(@as([weight_bytes]u8, @splat(0x5a))),
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
        .{},
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

test "sentencepiece tokenizer is owned before added-token failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "]",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);
    const man = manifest_mod.ModelManifest{ .allocator = allocator };

    const Harness = struct {
        fn run(a: std.mem.Allocator, model_dir: []const u8, manifest: manifest_mod.ModelManifest) !void {
            var owned: ?*sentencepiece.Processor = null;
            errdefer if (owned) |sp| {
                sp.deinit();
                a.destroy(sp);
            };

            const sp = try a.create(sentencepiece.Processor);
            errdefer if (owned == null) a.destroy(sp);
            sp.* = try sentencepiece.Processor.initFromPieces(a, &.{
                .{ .text = "<unk>", .score = 0, .piece_type = 2 },
                .{ .text = "token", .score = -1, .piece_type = 1 },
            }, .{});

            try adoptAndConfigureSentencePieceTokenizer(&owned, sp, manifest, model_dir, a);

            // Keep an unexpected success leak-free so expectError reports only
            // the missing post-load failure.
            sp.deinit();
            a.destroy(sp);
            owned = null;
        }
    };

    try std.testing.expectError(error.SyntaxError, Harness.run(allocator, dir_path, man));
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
    try std.testing.expectEqual(@as(i32, 7), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 5), encoded.ids[2]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[2]);

    const special_ids = try tok.tokenizer().encode(allocator, "<|turn>hello");
    defer allocator.free(special_ids);
    try std.testing.expectEqualSlices(i32, &.{ 6, 7 }, special_ids);

    const decoded = try tok.tokenizer().decode(allocator, &.{ 4, 7 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hellohello", decoded);
    try std.testing.expectEqual(@as(usize, 8), tok.tokenizer().vocabSize());
}

test "load huggingface tokenizer from gguf t5 unigram metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithT5Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "bge-m3-q4_k_m.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "bge-m3-q4_k_m.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForModel(allocator, "hello world", 8);
    defer encoded.deinit();
    try std.testing.expectEqualSlices(i32, &.{ 0, 4, 5, 2, 1, 1, 1, 1 }, encoded.ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 1, 1, 0, 0, 0, 0 }, encoded.attention_mask);
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
        "hello",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1, 3, 1 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithT5Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 11);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bert");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "t5");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<s>", "<pad>", "</s>", "<unk>", "\u{2581}hello", "\u{2581}world",
    });
    try appendTestMetadataF32Array(allocator, &data, "tokenizer.ggml.scores", &.{ 0, 0, 0, 0, -1, -1 });
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_eos_token", true);

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
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}

fn appendTestMetadataF32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const f32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.f32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(u32, allocator, data, @bitCast(value));
}
