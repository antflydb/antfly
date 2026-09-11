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

//! Artifact compatibility policy for local inference models.
//!
//! This is intentionally not an artifact certification system. Compatibility is derived
//! from the artifact contract and the runtime paths compiled into this build. Unknown
//! contracts are attempted by default. Known unsafe or invalid contracts remain
//! blocked; repository names, published receipts, and qualification lists are not admission policy.

const std = @import("std");
const manifest_mod = @import("manifest.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_writer = @import("../gguf/writer.zig");

pub const Level = enum {
    compatible,
    unknown,
    incompatible,
};

pub const Code = enum {
    compatible,
    artifact_unreadable,
    unknown_architecture,
    unsafe_runtime,
    incomplete_bundle,
    unsupported_tensor_type,
    missing_required_tensor,
    invalid_graph,
    unsupported_backend,
};

pub const Assessment = struct {
    level: Level,
    code: Code,
    message: []const u8,
    architecture: []const u8,

    pub fn allowed(self: Assessment, allow_unknown: bool) bool {
        return switch (self.level) {
            .compatible => true,
            .unknown => allow_unknown,
            .incompatible => false,
        };
    }
};

pub const Policy = struct {
    allow_unknown: bool = true,
};

pub const Inspection = struct {
    architecture: []u8,
    expert_count: u32 = 0,
    qualified_gemma4_a4b: bool = false,
    artifact_inspected: bool = true,

    pub fn deinit(self: *Inspection, allocator: std.mem.Allocator) void {
        allocator.free(self.architecture);
        self.* = undefined;
    }
};

pub fn inspectAlloc(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
) !Inspection {
    var result = Inspection{
        .architecture = try allocator.dupe(
            u8,
            if (man.config_model_arch.len > 0) man.config_model_arch else "unknown",
        ),
    };
    errdefer result.deinit(allocator);

    if (!man.usesGgufWeights()) return result;
    const gguf_path = man.gguf_path.?;
    result.artifact_inspected = false;
    var region = c_file.MmapRegion.init(allocator, gguf_path) catch
        return result;
    defer region.deinit();
    // Compatibility probing is metadata-only; retaining already-warm payload
    // pages is essential for a following full-residency CUDA admission.
    region.preserveFileCacheOnDeinit();
    const metadata = gguf_format.readSupportMetadata(region.data) catch return result;
    result.artifact_inspected = true;
    try applyArtifactMetadata(allocator, &result, metadata);
    return result;
}

test "compatibility inspection ignores an unselected colocated GGUF" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .config_model_arch = try allocator.dupe(u8, "bert"),
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
        .gguf_path = try allocator.dupe(u8, "missing-export.gguf"),
    };
    defer manifest.deinit();

    var inspection = try inspectAlloc(allocator, &manifest);
    defer inspection.deinit(allocator);
    try std.testing.expectEqualStrings("bert", inspection.architecture);
    try std.testing.expect(inspection.artifact_inspected);
}

fn applyArtifactMetadata(
    allocator: std.mem.Allocator,
    result: *Inspection,
    metadata: gguf_format.SupportMetadata,
) !void {
    result.expert_count = metadata.expert_count;
    result.qualified_gemma4_a4b = metadata.isQualifiedGemma4A4b();
    // The artifact is authoritative. A config.json sidecar is useful for discovery,
    // but it must not be able to relabel a GGUF and bypass a family safety block.
    // Canonical composite bundles are still recognized from their manifest contract
    // in assessWithFacts(), even when their primary GGUF has a component architecture.
    const architecture = metadata.architecture orelse "unknown";
    const owned_architecture = try allocator.dupe(u8, architecture);
    allocator.free(result.architecture);
    result.architecture = owned_architecture;
}

pub fn assess(
    man: *const manifest_mod.ModelManifest,
    architecture: []const u8,
) Assessment {
    return assessWithFacts(man, architecture, 0);
}

pub fn assessInspection(
    man: *const manifest_mod.ModelManifest,
    inspection: Inspection,
) Assessment {
    if (!inspection.artifact_inspected) {
        return makeUnknown(
            inspection.architecture,
            .artifact_unreadable,
            "GGUF compatibility metadata could not be inspected",
        );
    }
    return assessWithRuntimeFacts(
        man,
        inspection.architecture,
        inspection.expert_count,
        inspection.qualified_gemma4_a4b,
    );
}

fn assessWithFacts(
    man: *const manifest_mod.ModelManifest,
    architecture: []const u8,
    expert_count: u32,
) Assessment {
    return assessWithRuntimeFacts(man, architecture, expert_count, false);
}

fn assessWithRuntimeFacts(
    man: *const manifest_mod.ModelManifest,
    architecture: []const u8,
    expert_count: u32,
    qualified_gemma4_a4b: bool,
) Assessment {
    if (man.hasIncompleteGlinerBundle() or
        man.hasIncompleteColqwenBundle() or
        man.hasIncompleteClipclapGgufBundle() or
        man.hasIncompleteFlorence2GgufBundle() or
        man.hasIncompleteQwen3VlGgufBundle())
    {
        return makeIncompatible(
            architecture,
            .incomplete_bundle,
            "the model bundle is missing required artifacts or sidecars",
        );
    }

    if (man.isQwen3VlBundle()) {
        // The CUDA generation route uses the official integrated BF16
        // safetensors bundle instead of the split GGUF decoder/projector
        // promotion used by Metal.
        if (man.isQwen3VlGenerationSafetensorsBundle()) {
            if (man.model_type == .generator and stringIn(architecture, &.{ "qwen3_vl", "qwen3vl" })) {
                return makeCompatible(
                    architecture,
                    "declared Qwen3-VL integrated BF16 safetensors generation bundle",
                );
            }
            return makeIncompatible(
                architecture,
                .unsupported_backend,
                "Qwen3-VL BF16 safetensors bundle does not match the declared generation role",
            );
        }
        if (stringIn(architecture, &.{ "qwen3vl", "qwen3_vl" }) and
            ((man.model_type == .generator and std.mem.eql(u8, man.inference_bundle_family, manifest_mod.qwen3_vl_gguf_bundle_family)) or
                (man.model_type == .reranker and man.isQwen3VlRerankerGgufBundle())))
        {
            return makeCompatible(architecture, "Qwen3-VL decoder/projector runtime");
        }
        return makeIncompatible(architecture, .unsupported_backend, "the Qwen3-VL artifact route does not implement the declared serving role");
    }

    if (man.model_type == .generator)
        return assessGenerator(architecture, expert_count, qualified_gemma4_a4b);

    // Canonical Antfly bundles are known runtime contracts, even when their encoder
    // architecture names overlap with blocked standalone exports.
    if (man.isClipclapGgufBundle()) {
        return makeCompatible(architecture, "canonical ClipClap bundle");
    }
    if (std.mem.eql(u8, man.gliner_model_type, "gliner2")) {
        return makeCompatible(architecture, "GLiNER2 extraction runtime is enabled");
    }

    // A standalone GGUF commonly has no config.json and may live under an
    // owner/model directory rather than the optional generators/ taxonomy.
    // In that case ModelManifest retains its neutral default role, but the
    // inspected GGUF architecture is authoritative. Promote only a selected
    // GGUF with no explicit role or bundle metadata; this prevents a colocated
    // decoder-shaped artifact from relabeling an embedder contract.
    if (mayInferStandaloneGgufDecoder(man)) {
        const decoder_assessment = assessGenerator(
            architecture,
            expert_count,
            qualified_gemma4_a4b,
        );
        if (decoder_assessment.level != .unknown) return decoder_assessment;
    }

    switch (man.model_type) {
        .rewriter => return makeIncompatible(
            architecture,
            .unsafe_runtime,
            "the ONNX encoder-decoder rewrite runtime can panic while importing graphs",
        ),
        .reader => {
            if (man.native_arch_hint == .florence) {
                return makeCompatible(architecture, "Florence reader runtime is enabled");
            }
            return makeIncompatible(
                architecture,
                .unsafe_runtime,
                "no safe reader runtime is available for this architecture",
            );
        },
        .embedder => switch (man.native_arch_hint) {
            .clip => return makeIncompatible(
                architecture,
                .unsafe_runtime,
                "standalone CLIP image inference can exhaust process memory; use ClipClap",
            ),
            .clap => return makeIncompatible(
                architecture,
                .unsupported_backend,
                "standalone CLAP graph conversion is not compatible; use ClipClap",
            ),
            else => {
                // Qwen3-Embedding checkpoints resolve to the qwen3 decoder
                // arch (unknown to the encoder list below) but serve through
                // the resident last-token embedding runtime.
                if (std.mem.eql(u8, architecture, "qwen3") and
                    man.embedding_style == .qwen3_embedding and
                    man.isLastTokenDecoderEmbedder())
                {
                    return makeCompatible(architecture, "Qwen3 last-token embedding runtime");
                }
            },
        },
        .classifier => {
            if (man.native_arch_hint == .layoutlmv3) {
                return makeIncompatible(
                    architecture,
                    .unsafe_runtime,
                    "LayoutLMv3 tokenizer assets are accepted by discovery but not by the loader",
                );
            }
        },
        .reranker => {
            if (std.mem.eql(u8, architecture, "qwen3") and man.usesGgufWeights()) {
                return makeCompatible(architecture, "Qwen3 GGUF final-token yes/no reranking runtime");
            }
        },
        .chunker, .recognizer, .transcriber => {},
        .generator => unreachable,
    }

    if (std.mem.eql(u8, architecture, "bart")) {
        return makeIncompatible(
            architecture,
            .unsafe_runtime,
            "the BART encoder-decoder runtime can panic while importing ONNX graphs; REBEL is not release-safe",
        );
    }

    if (knownEncoderArchitecture(architecture)) {
        return makeCompatible(architecture, "recognized local inference runtime");
    }
    return makeUnknown(
        architecture,
        .unknown_architecture,
        "unrecognized model architecture; the selected backend will validate it at load time",
    );
}

fn mayInferStandaloneGgufDecoder(man: *const manifest_mod.ModelManifest) bool {
    return man.model_type == .embedder and
        man.model_type_origin == .default and
        man.usesGgufWeights() and
        man.config_model_arch.len == 0 and
        man.config_path == null and
        man.model_manifest_path == null and
        man.onnx_path == null and
        man.tasks.len == 0 and
        man.capabilities.len == 0 and
        man.inputs.len == 0 and
        man.gliner_model_type.len == 0 and
        man.inference_bundle_family.len == 0;
}

fn assessGenerator(
    architecture: []const u8,
    expert_count: u32,
    qualified_gemma4_a4b: bool,
) Assessment {
    if (std.mem.startsWith(u8, architecture, "gemma4") and expert_count > 0) {
        if (qualified_gemma4_a4b) {
            return makeCompatible(
                architecture,
                "qualified Gemma 4 26B-A4B Q4_0 runtime (Metal and CUDA SM89)",
            );
        }
        return makeIncompatible(
            architecture,
            .unsupported_backend,
            "only the qualified Gemma 4 26B-A4B Q4_0 mixture-of-experts layout is enabled for this release",
        );
    }

    if (stringIn(architecture, &.{
        "llama",
        "qwen3",
        "gemma",
        "gemma2",
        "gemma3",
        "gemma3_text",
        "gemma4",
        "gemma4_text",
        "gemma4_assistant",
        "gemma4_unified_assistant",
        "gemma4-assistant",
    })) {
        return makeCompatible(architecture, "decoder runtime is enabled for this release");
    }

    if (stringIn(architecture, &.{
        "gemma4_unified",
        "gemma4_unified_text",
    })) {
        return makeIncompatible(
            architecture,
            .missing_required_tensor,
            "this Gemma 4 unified layout has unresolved required weights",
        );
    }

    if (stringIn(architecture, &.{
        "qwen2",
        "qwen2_vl",
        "mistral",
        "mixtral",
        "phi",
        "phi3",
        "bitnet",
        "bitnet-b1.58",
        "deepseek4",
        "deepseek_v4",
        "deepseek_v4_text",
        "deepseek_v4_flash",
        "deepseek_v4_flash_base",
        "deepseek_v4_pro",
        "deepseek_v4_pro_base",
        "deepseek-v4",
        "deepseek-v4-flash",
        "deepseek-v4-flash-base",
        "deepseek-v4-pro",
        "deepseek-v4-pro-base",
        "deepseekv4",
        "qwen3_5",
        "qwen3_5_text",
        "qwen3_5_moe",
        "qwen3_next",
        "qwen35",
        "qwen3next",
        "qwen35moe",
        "qwen3_vl",
        "qwen3_vl_text",
        "qwen3_vl_moe",
        "qwen3vl",
        "qwen3vlmoe",
        "gpt2",
        "gpt_neo",
        "gpt_neox",
        "gptj",
        "falcon",
        "opt",
        "bloom",
        "t5",
    })) {
        return makeIncompatible(
            architecture,
            .unsafe_runtime,
            "the current decoder path is known to be missing, unsafe, or to produce unusable output",
        );
    }

    return makeUnknown(
        architecture,
        .unknown_architecture,
        "unrecognized generator architecture; the selected backend will validate it at load time",
    );
}

fn knownEncoderArchitecture(architecture: []const u8) bool {
    return stringIn(architecture, &.{
        "bert",
        "roberta",
        "xlm-roberta",
        "distilbert",
        "deberta",
        "deberta-v2",
        "deberta_v2",
        "modernbert",
        "modern_bert",
        "nomic-bert",
        "nomic_bert",
        "mmbert",
        "gliner",
        "gliner2",
        "whisper",
        "florence",
        "florence2",
        "florence-2",
        "clipclap",
        "jina_embeddings_v5",
    });
}

fn stringIn(value: []const u8, candidates: []const []const u8) bool {
    for (candidates) |candidate| {
        if (std.mem.eql(u8, value, candidate)) return true;
    }
    return false;
}

pub fn makeCompatible(architecture: []const u8, message: []const u8) Assessment {
    return .{ .level = .compatible, .code = .compatible, .message = message, .architecture = architecture };
}

pub fn makeUnknown(architecture: []const u8, code: Code, message: []const u8) Assessment {
    return .{ .level = .unknown, .code = code, .message = message, .architecture = architecture };
}

pub fn makeIncompatible(architecture: []const u8, code: Code, message: []const u8) Assessment {
    return .{ .level = .incompatible, .code = code, .message = message, .architecture = architecture };
}

test "qwen3 embedding accepts the executable contract without a catalog receipt" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .embedder;
    man.pooling = .last;
    man.embedding_style = .qwen3_embedding;
    const result = assess(&man, "qwen3");
    try std.testing.expectEqual(Level.compatible, result.level);

    var promoted = Inspection{
        .architecture = try std.testing.allocator.dupe(u8, "qwen3"),
    };
    defer promoted.deinit(std.testing.allocator);
    try std.testing.expectEqual(Level.compatible, assessInspection(&man, promoted).level);

    // The manifest style is metadata, not authority to bypass architecture
    // safety policy for an unrelated unsafe family. NomicBERT is now a
    // supported encoder and therefore is deliberately not part of this gate.
    const spoofed_result = assess(&man, "bart");
    try std.testing.expectEqual(Level.incompatible, spoofed_result.level);
    try std.testing.expect(!spoofed_result.allowed(true));

    // A bare qwen3 embedder without the resolved style stays unknown.
    var bare = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    bare.model_type = .embedder;
    const bare_result = assess(&bare, "qwen3");
    try std.testing.expectEqual(Level.unknown, bare_result.level);
}

test "unknown architectures are attempted by default with optional strict policy" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    const result = assess(&man, "brand_new_decoder");
    try std.testing.expectEqual(Level.unknown, result.level);
    try std.testing.expect(!result.allowed(false));
    try std.testing.expect(result.allowed((Policy{}).allow_unknown));
}

test "known unsafe generators cannot be enabled by unknown opt in" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    const result = assess(&man, "deepseek4");
    try std.testing.expectEqual(Level.incompatible, result.level);
    try std.testing.expect(!result.allowed(true));
}

test "artifact architecture remains authoritative over a supported sidecar family" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    man.config_model_arch = "llama";

    var inspection = Inspection{
        .architecture = try std.testing.allocator.dupe(u8, man.config_model_arch),
    };
    defer inspection.deinit(std.testing.allocator);
    try applyArtifactMetadata(std.testing.allocator, &inspection, .{
        .architecture = "deepseek4",
    });

    try std.testing.expectEqualStrings("deepseek4", inspection.architecture);
    const assessment = assessInspection(&man, inspection);
    try std.testing.expectEqual(Level.incompatible, assessment.level);
    try std.testing.expect(!assessment.allowed(true));

    try applyArtifactMetadata(std.testing.allocator, &inspection, .{});
    try std.testing.expectEqualStrings("unknown", inspection.architecture);
    try std.testing.expectEqual(Level.unknown, assessInspection(&man, inspection).level);
}

test "qualified Gemma4 A4B architecture is enabled while unified layout is blocked" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    try std.testing.expectEqual(Level.compatible, assess(&man, "gemma4").level);
    try std.testing.expectEqual(Level.incompatible, assess(&man, "gemma4_unified").level);
    try std.testing.expectEqual(
        Level.incompatible,
        assessWithFacts(&man, "gemma4", 128).level,
    );

    var qualified = Inspection{
        .architecture = try std.testing.allocator.dupe(u8, "gemma4"),
        .expert_count = 128,
        .qualified_gemma4_a4b = true,
    };
    defer qualified.deinit(std.testing.allocator);
    try std.testing.expectEqual(Level.compatible, assessInspection(&man, qualified).level);

    qualified.qualified_gemma4_a4b = false;
    try std.testing.expectEqual(Level.incompatible, assessInspection(&man, qualified).level);
}

test "Qwen3 text reranker uses selected GGUF without enabling unqualified VL bundles" {
    var man = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .reranker,
        .model_type_origin = .tasks,
        .config_model_arch = "qwen3",
        .gguf_path = "qwen3-reranker-0.6b-q8_0.gguf",
    };
    try std.testing.expect(man.isQwen3TextReranker());
    try std.testing.expectEqual(Level.compatible, assessWithFacts(&man, "qwen3", 0).level);
    man.config_model_arch = "qwen3_vl";
    man.inference_bundle_family = manifest_mod.qwen3_vl_reranker_gguf_bundle_family;
    try std.testing.expect(!man.isQwen3TextReranker());
    try std.testing.expectEqual(Level.incompatible, assessWithFacts(&man, "qwen3_vl", 0).level);
    man.config_model_arch = "qwen3";
    man.inference_bundle_family = "";
    man.gguf_path = null;
    try std.testing.expect(!man.isQwen3TextReranker());
    try std.testing.expect(assessWithFacts(&man, "qwen3", 0).level != .compatible);
}

test "standalone GGUF decoder architecture does not depend on directory taxonomy" {
    var man = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .gguf_path = "model.gguf",
    };
    try std.testing.expectEqual(Level.compatible, assessWithFacts(&man, "gemma4", 0).level);
    try std.testing.expectEqual(Level.incompatible, assessWithFacts(&man, "gemma4", 128).level);
    try std.testing.expectEqual(Level.incompatible, assessWithFacts(&man, "deepseek4", 0).level);
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "brand_new_decoder", 0).level);

    // Explicit classification remains authoritative even when its final enum
    // value happens to equal ModelManifest's neutral default.
    man.model_type_origin = .config;
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
}

test "standalone decoder inference requires an artifact-only default manifest" {
    var man = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .gguf_path = "model.gguf",
    };

    man.model_type_origin = .path;
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
    man.model_type_origin = .manifest;
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
    man.model_type_origin = .tasks;
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);

    man.model_type_origin = .default;
    man.config_path = "config.json";
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
    man.config_path = null;
    man.model_manifest_path = "model_manifest.json";
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
    man.model_manifest_path = null;
    man.onnx_path = "model.onnx";
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);

    man.onnx_path = null;
    man.gguf_path = null;
    try std.testing.expectEqual(Level.unknown, assessWithFacts(&man, "gemma4", 0).level);
}

test "listing inspection recognizes standalone GGUF decoder outside taxonomy" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeCompatibilityTestGguf(tmp.dir, allocator, "gemma-4-e2b-it-q4_k_xl.gguf", "gemma4");

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    var listing_man = try manifest_mod.loadListingFromDir(allocator, model_dir);
    defer listing_man.deinit();
    // Listing intentionally avoids opening multi-gigabyte GGUFs; compatibility
    // inspection still recognizes the decoder architecture when requested.
    try expectLoadedGgufAssessment(allocator, &listing_man, .embedder, .default, .compatible);

    var full_man = try manifest_mod.loadFromDir(allocator, model_dir);
    defer full_man.deinit();
    try expectLoadedGgufAssessment(allocator, &full_man, .generator, .config, .compatible);
}

test "loader-derived embedder roles cannot be relabeled by GGUF architecture" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const cases = [_]struct {
        dir: []const u8,
        sidecar_name: ?[]const u8,
        sidecar_data: []const u8,
        origin: manifest_mod.ModelTypeOrigin,
    }{
        .{
            .dir = "embedders/acme/model",
            .sidecar_name = null,
            .sidecar_data = "",
            .origin = .path,
        },
        .{
            .dir = "manifest-role",
            .sidecar_name = "model_manifest.json",
            .sidecar_data = "{\"type\":\"embedder\"}",
            .origin = .manifest,
        },
        .{
            .dir = "task-role",
            .sidecar_name = "model_manifest.json",
            .sidecar_data = "{\"tasks\":[\"embed\"]}",
            .origin = .tasks,
        },
        .{
            .dir = "config-role",
            .sidecar_name = "config.json",
            .sidecar_data = "{\"model_type\":\"bert\"}",
            .origin = .config,
        },
    };

    for (cases) |case| {
        try tmp.dir.createDirPath(io, case.dir);
        const gguf_rel = try std.fs.path.join(allocator, &.{ case.dir, "model.gguf" });
        defer allocator.free(gguf_rel);
        try writeCompatibilityTestGguf(tmp.dir, allocator, gguf_rel, "gemma4");
        if (case.sidecar_name) |sidecar_name| {
            const sidecar_rel = try std.fs.path.join(allocator, &.{ case.dir, sidecar_name });
            defer allocator.free(sidecar_rel);
            try tmp.dir.writeFile(io, .{ .sub_path = sidecar_rel, .data = case.sidecar_data });
        }

        const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], case.dir });
        defer allocator.free(model_dir);
        var listing_man = try manifest_mod.loadListingFromDir(allocator, model_dir);
        defer listing_man.deinit();
        try expectLoadedGgufAssessment(allocator, &listing_man, .embedder, case.origin, .unknown);

        var full_man = try manifest_mod.loadFromDir(allocator, model_dir);
        defer full_man.deinit();
        try expectLoadedGgufAssessment(allocator, &full_man, .embedder, case.origin, .unknown);
    }
}

fn expectLoadedGgufAssessment(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
    expected_type: manifest_mod.ModelType,
    expected_origin: manifest_mod.ModelTypeOrigin,
    expected_level: Level,
) !void {
    try std.testing.expectEqual(expected_type, man.model_type);
    try std.testing.expectEqual(expected_origin, man.model_type_origin);
    try std.testing.expect(man.usesGgufWeights());
    var inspection = try inspectAlloc(allocator, man);
    defer inspection.deinit(allocator);
    try std.testing.expectEqualStrings("gemma4", inspection.architecture);
    try std.testing.expectEqual(expected_level, assessInspection(man, inspection).level);
}

fn writeCompatibilityTestGguf(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    architecture: []const u8,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = architecture } },
    };
    var layout = try gguf_writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);
    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = layout.header_bytes });
}

test "release encoder contracts cover DeBERTa reranking and GLiNER2" {
    var reranker = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    reranker.model_type = .reranker;
    try std.testing.expectEqual(Level.compatible, assess(&reranker, "deberta-v2").level);

    var gliner = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    gliner.model_type = .recognizer;
    gliner.gliner_model_type = "gliner2";
    try std.testing.expectEqual(Level.compatible, assess(&gliner, "extractor").level);
}

test "known Qwen hybrid variants and NomicBERT stay classified" {
    var generator = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    generator.model_type = .generator;
    try std.testing.expectEqual(Level.incompatible, assess(&generator, "qwen3_5_moe").level);
    try std.testing.expectEqual(Level.incompatible, assess(&generator, "qwen3_next").level);
    const qwen3_vl = assess(&generator, "qwen3vl");
    try std.testing.expectEqual(Level.incompatible, qwen3_vl.level);
    try std.testing.expect(!qwen3_vl.allowed(true));
    const qwen3_vl_moe = assess(&generator, "qwen3vlmoe");
    try std.testing.expectEqual(Level.incompatible, qwen3_vl_moe.level);
    try std.testing.expect(!qwen3_vl_moe.allowed(true));

    var embedder = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    embedder.model_type = .embedder;
    try std.testing.expectEqual(Level.compatible, assess(&embedder, "nomic-bert").level);
    try std.testing.expectEqual(Level.compatible, assess(&embedder, "nomic_bert").level);
}

test "Qwen3-VL admission checks required artifacts and implemented serving roles" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .model_type_origin = .bundle,
        .inference_bundle_family = try allocator.dupe(u8, manifest_mod.qwen3_vl_gguf_bundle_family),
        .gguf_path = try allocator.dupe(u8, "decoder.gguf"),
        .gguf_projector_path = try allocator.dupe(u8, "mmproj.gguf"),
    };
    defer manifest.deinit();

    var assessment = assess(&manifest, "qwen3vl");
    try std.testing.expectEqual(Code.incomplete_bundle, assessment.code);
    try std.testing.expect(!assessment.allowed(true));

    manifest.config_path = try allocator.dupe(u8, "config.json");
    manifest.tokenizer_json_path = try allocator.dupe(u8, "tokenizer.json");
    manifest.tokenizer_config_path = try allocator.dupe(u8, "tokenizer_config.json");
    manifest.preprocessor_config_path = try allocator.dupe(u8, "preprocessor_config.json");
    assessment = assess(&manifest, "qwen3vl");
    try std.testing.expectEqual(Level.compatible, assessment.level);
    try std.testing.expect(assessment.allowed(true));

    var reranker = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .reranker,
        .model_type_origin = .bundle,
        .inference_bundle_family = try allocator.dupe(u8, manifest_mod.qwen3_vl_reranker_safetensors_bundle_family),
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
        .config_path = try allocator.dupe(u8, "config.json"),
        .tokenizer_json_path = try allocator.dupe(u8, "tokenizer.json"),
        .tokenizer_config_path = try allocator.dupe(u8, "tokenizer_config.json"),
        .preprocessor_config_path = try allocator.dupe(u8, "preprocessor_config.json"),
    };
    defer reranker.deinit();
    assessment = assess(&reranker, "qwen3_vl");
    try std.testing.expectEqual(Code.unsupported_backend, assessment.code);
    try std.testing.expect(!assessment.allowed(true));
}

test "known unsafe local site models stay blocked even with unknown opt in" {
    var clip = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    clip.model_type = .embedder;
    clip.native_arch_hint = .clip;
    const clip_result = assess(&clip, "clip");
    try std.testing.expectEqual(Level.incompatible, clip_result.level);
    try std.testing.expect(!clip_result.allowed(true));

    var clap = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    clap.model_type = .embedder;
    clap.native_arch_hint = .clap;
    const clap_result = assess(&clap, "clap");
    try std.testing.expectEqual(Level.incompatible, clap_result.level);
    try std.testing.expect(!clap_result.allowed(true));

    var rebel = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    rebel.model_type = .recognizer;
    const rebel_result = assess(&rebel, "bart");
    try std.testing.expectEqual(Level.incompatible, rebel_result.level);
    try std.testing.expect(!rebel_result.allowed(true));
}
