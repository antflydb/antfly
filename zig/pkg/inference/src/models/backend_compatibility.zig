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

//! Artifact/backend legality and component inspection, independent of serving
//! cache entries, health, eviction, admission leases and model publication.
const host_weights = @import("host_weights.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const BackendType = backends.BackendType;
const GgufInspectionReport = host_weights.GgufInspectionReport;
const UnsupportedTensorTypeCount = host_weights.UnsupportedTensorTypeCount;
const tensorTypeSupported = host_weights.tensorTypeSupported;
const std = @import("std");
const build_options = @import("build_options");
const backends = @import("../backends/backends.zig");
const manifest_mod = @import("manifest.zig");
const model_compatibility = @import("compatibility.zig");
const safetensors_mod = @import("safetensors.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const clipclap_format_mod = @import("../architectures/clipclap_format.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");

pub fn nativeBackendsAvailable() bool {
    return build_options.enable_native or build_options.enable_metal or build_options.enable_cuda;
}

pub fn manifestHasNativeAssets(man: manifest_mod.ModelManifest) bool {
    return man.nativeWeightArtifactKind() != null;
}

pub const ArtifactCandidateKind = enum {
    gguf,
    safetensors,
    onnx,
    component_bundle,
};

pub fn artifactCandidateForBackend(
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) ?ArtifactCandidateKind {
    if (backend == .pjrt) return null;
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

pub fn summaryFromAssessment(assessment: model_compatibility.Assessment) CompatibilitySummary {
    return .{
        .level = assessment.level,
        .code = assessment.code,
        .message = assessment.message,
    };
}

pub const NativeCompanionKind = enum {
    gguf,
    safetensors,
};

pub const NativeCompanionRole = enum {
    generic,
    projector,
};

pub const NativeCompanion = struct {
    path: []const u8,
    kind: NativeCompanionKind,
    role: NativeCompanionRole = .generic,
};

pub const ProjectorDecoderFamily = enum {
    gemma3,
    gemma4,
    qwen3vl,
    unsupported,
};

pub const ProjectorDecoderContract = struct {
    family: ProjectorDecoderFamily,
    hidden_size: u32 = 0,
    tokens_per_image: u32 = 0,
    requires_image: bool = false,
    requires_audio: bool = false,
};

pub fn projectorDecoderFamilyForArchitecture(architecture: []const u8) ProjectorDecoderFamily {
    if (std.mem.eql(u8, architecture, "gemma3")) return .gemma3;
    if (std.mem.eql(u8, architecture, "qwen3_vl") or
        std.mem.eql(u8, architecture, "qwen3vl")) return .qwen3vl;
    if (std.mem.eql(u8, architecture, "gemma4") or
        std.mem.eql(u8, architecture, "gemma4_unified"))
    {
        return .gemma4;
    }
    return .unsupported;
}

pub fn projectorMatchesDecoder(
    projector: projector_format_mod.Kind,
    decoder: ProjectorDecoderContract,
) bool {
    return switch (projector) {
        .antfly_gemma3 => decoder.family == .gemma3,
        .clip_qwen3vl_image => decoder.family == .qwen3vl,
        .clip_gemma4_image,
        .clip_gemma4_audio,
        .clip_gemma4_image_audio,
        => decoder.family == .gemma4,
        .unknown => false,
    };
}

pub fn validateGgufCompanionForBackend(
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
                .message = "a projector GGUF does not declare a supported Antfly Gemma 3, Gemma 4, or Qwen3-VL CLIP projector format",
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
        if (!ggufTensorTypeSupportsBackend(tensor.tensor_type, backend)) {
            return .{
                .level = .incompatible,
                .code = .unsupported_tensor_type,
                .message = "the selected backend cannot materialize a required companion GGUF",
            };
        }
    }
    return null;
}

pub fn validateNativeCompanionsForBackend(
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

pub fn validateClipclapBundleForBackend(
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
            if (!ggufTensorTypeSupportsBackend(tensor.tensor_type, backend)) {
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

    // Split GGUF Qwen3-VL promotions are Metal-only. The official integrated
    // BF16 generation bundle has a separate CUDA-qualified resident route.
    // Reject other routes before inspectAlloc re-hashes multi-gigabyte files.
    const cuda_qwen3vl_generation = backend == .cuda and
        man.isQwen3VlGenerationSafetensorsBundle();
    if (man.isQwen3VlBundle() and backend != .metal and !cuda_qwen3vl_generation) {
        return .{
            .level = .incompatible,
            .code = .unsupported_backend,
            .message = "the split Qwen3-VL decoder/projector route requires Metal; the integrated safetensors generation route also supports CUDA",
        };
    }
    if (man.embedding_style == .qwen3_embedding and
        man.usesGgufWeights() and
        !qwen3EmbeddingSupportsBackend(backend))
    {
        return .{
            .level = .incompatible,
            .code = .unsupported_backend,
            .message = "Qwen3 GGUF embedding requires Metal, native CPU, or CUDA",
        };
    }

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
        var maybe_report = host_weights.inspectGgufModelForListing(
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
            if (!ggufInspectionSupportsBackend(report.*, backend)) {
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
    return summaryFromAssessment(assessment);
}

pub fn qwen3EmbeddingSupportsBackend(backend: backends.BackendType) bool {
    return backend == .metal or backend == .native or backend == .cuda;
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

pub fn selectBetterCompatibility(
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

pub fn policyAllowedBackends(
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

pub const ComponentInspection = struct {
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

    pub fn deinit(self: *ComponentInspection) void {
        for (self.dependencies.items) |path| self.allocator.free(path);
        self.dependencies.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn addDependency(self: *ComponentInspection, path: []const u8) !void {
        for (self.dependencies.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return;
        }
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        try self.dependencies.append(self.allocator, owned_path);
    }

    pub fn hasDependency(self: *const ComponentInspection, path: []const u8) bool {
        for (self.dependencies.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return true;
        }
        return false;
    }

    pub fn mergeNativeSummary(
        self: *ComponentInspection,
        backend: backends.BackendType,
        summary: CompatibilitySummary,
    ) void {
        const slot = &self.native_backend_summaries[@backingInt(backend)];
        slot.* = selectWorseCompatibility(slot.*, summary);
    }

    pub fn summaryForBackend(
        self: *const ComponentInspection,
        backend: backends.BackendType,
    ) CompatibilitySummary {
        if (self.invalid_summary) |summary| return summary;
        if (self.has_onnx_component and !self.imported_graph_compatible) {
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

pub fn selectWorseCompatibility(
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

pub fn addNativeComponentDependencies(
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

pub fn inspectComponentArtifacts(
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

pub fn validateComponentNativeArtifacts(
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
            if (!backend.supportsDirectSessionLoad()) continue;
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

pub fn validateComponentImportedGraphs(
    allocator: std.mem.Allocator,
    component_paths: []const []const u8,
    preferred_backends: []const backends.BackendType,
    inspection: *ComponentInspection,
) !void {
    if (preferred_backends.len == 0 or !inspection.has_onnx_component or inspection.invalid_summary != null) return;

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

pub fn inspectComponentBundle(
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

pub fn componentCompatibilityForBackend(
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

pub fn policyAllowedComponentBackendsFromInspection(
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

pub fn manifestRequiresInput(man: *const manifest_mod.ModelManifest, expected: []const u8) bool {
    for (man.inputs) |input| {
        if (std.mem.eql(u8, input, expected)) return true;
    }
    return false;
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

pub fn ggufTensorTypeSupportsBackend(
    tensor_type: @import("../gguf/tensor_types.zig").TensorType,
    backend: BackendType,
) bool {
    if (!tensorTypeSupported(tensor_type)) return false;
    return switch (backend) {
        .metal => if (comptime build_options.enable_metal)
            metal_runtime.isMetalNativeSupported(tensor_type)
        else
            true,
        .native, .cuda, .wasm => true,
        .pjrt => false,
    };
}

pub fn metalGgufIncompatibleTensorType(report: GgufInspectionReport) ?UnsupportedTensorTypeCount {
    if (comptime !build_options.enable_metal) return null;
    for (report.all_tensor_types) |entry| {
        if (!metal_runtime.isMetalNativeSupported(entry.tensor_type)) return entry;
    }
    return null;
}

pub fn ggufInspectionSupportsBackend(report: GgufInspectionReport, backend: BackendType) bool {
    if (report.unsupported_tensor_types.len > 0 or report.missing_required_tensors.len > 0)
        return false;
    return switch (backend) {
        .metal => metalGgufIncompatibleTensorType(report) == null,
        .native, .cuda, .wasm => true,
        .pjrt => false,
    };
}
