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
//! contracts require an explicit server opt-in. Known unsafe or invalid contracts remain
//! blocked even when that opt-in is set.

const std = @import("std");
const manifest_mod = @import("manifest.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");

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
    allow_unknown: bool = false,
};

pub const Inspection = struct {
    architecture: []u8,
    expert_count: u32 = 0,
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

    const gguf_path = man.gguf_path orelse return result;
    result.artifact_inspected = false;
    var region = c_file.MmapRegion.init(allocator, gguf_path) catch
        return result;
    defer region.deinit();
    const metadata = gguf_format.readSupportMetadata(region.data) catch return result;
    result.artifact_inspected = true;
    try applyArtifactMetadata(allocator, &result, metadata);
    return result;
}

fn applyArtifactMetadata(
    allocator: std.mem.Allocator,
    result: *Inspection,
    metadata: gguf_format.SupportMetadata,
) !void {
    result.expert_count = metadata.expert_count;
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
            "GGUF compatibility metadata could not be inspected; start the server with --allow-unknown-models to opt in",
        );
    }
    return assessWithFacts(man, inspection.architecture, inspection.expert_count);
}

fn assessWithFacts(
    man: *const manifest_mod.ModelManifest,
    architecture: []const u8,
    expert_count: u32,
) Assessment {
    if (man.hasIncompleteGlinerBundle() or
        man.hasIncompleteColqwenBundle() or
        man.hasIncompleteClipclapGgufBundle() or
        man.hasIncompleteFlorence2GgufBundle())
    {
        return makeIncompatible(
            architecture,
            .incomplete_bundle,
            "the model bundle is missing required artifacts or sidecars",
        );
    }

    if (man.model_type == .generator) return assessGenerator(architecture, expert_count);

    // Canonical Antfly bundles are known runtime contracts, even when their encoder
    // architecture names overlap with blocked standalone exports.
    if (man.isClipclapGgufBundle()) {
        return makeCompatible(architecture, "canonical ClipClap bundle");
    }
    if (std.mem.eql(u8, man.gliner_model_type, "gliner2")) {
        return makeCompatible(architecture, "GLiNER2 extraction runtime is enabled");
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
            else => {},
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
        .reranker, .chunker, .recognizer, .transcriber => {},
        .generator => unreachable,
    }

    if (std.mem.eql(u8, architecture, "nomic-bert")) {
        return makeIncompatible(
            architecture,
            .unsupported_backend,
            "the published GGUF tokenizer is not supported by the current loader",
        );
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
        "unrecognized model architecture; start the server with --allow-unknown-models to opt in",
    );
}

fn assessGenerator(architecture: []const u8, expert_count: u32) Assessment {
    if (std.mem.startsWith(u8, architecture, "gemma4") and expert_count > 0) {
        return makeIncompatible(
            architecture,
            .unsupported_backend,
            "Gemma 4 mixture-of-experts layouts are not enabled for this release",
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
        "unrecognized generator architecture; start the server with --allow-unknown-models to opt in",
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

test "unknown generators are unknown and require opt in" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    const result = assess(&man, "brand_new_decoder");
    try std.testing.expectEqual(Level.unknown, result.level);
    try std.testing.expect(!result.allowed(false));
    try std.testing.expect(result.allowed(true));
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

test "gemma 4 E4B architecture is enabled while unified layout is blocked" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    try std.testing.expectEqual(Level.compatible, assess(&man, "gemma4").level);
    try std.testing.expectEqual(Level.incompatible, assess(&man, "gemma4_unified").level);
    try std.testing.expectEqual(
        Level.incompatible,
        assessWithFacts(&man, "gemma4", 128).level,
    );
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

test "known Qwen hybrid variants and incompatible Nomic GGUF stay blocked" {
    var generator = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    generator.model_type = .generator;
    try std.testing.expectEqual(Level.incompatible, assess(&generator, "qwen3_5_moe").level);
    try std.testing.expectEqual(Level.incompatible, assess(&generator, "qwen3_next").level);

    var embedder = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    embedder.model_type = .embedder;
    try std.testing.expectEqual(Level.incompatible, assess(&embedder, "nomic-bert").level);
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
