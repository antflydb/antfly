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

//! Release serving policy for local inference models.
//!
//! This is intentionally not an artifact certification system. `supported` means that
//! Antfly enables the architecture/runtime path by default in this release. Unknown
//! architectures are experimental and require an explicit server opt-in. Models with a
//! known unsafe or unusable runtime path remain blocked even when that opt-in is set.

const std = @import("std");
const manifest_mod = @import("manifest.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");

pub const Level = enum {
    supported,
    experimental,
    unsupported,
};

pub const Assessment = struct {
    level: Level,
    reason: []const u8,
    architecture: []const u8,

    pub fn allowed(self: Assessment, allow_experimental: bool) bool {
        return switch (self.level) {
            .supported => true,
            .experimental => allow_experimental,
            .unsupported => false,
        };
    }
};

pub const Policy = struct {
    allow_experimental: bool = false,
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
    result.expert_count = metadata.expert_count;
    if (man.config_model_arch.len == 0) {
        const arch = metadata.architecture orelse return result;
        allocator.free(result.architecture);
        result.architecture = try allocator.dupe(u8, arch);
    }
    return result;
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
        return experimental(
            inspection.architecture,
            "GGUF compatibility metadata could not be inspected; start the server with --allow-experimental-models to opt in",
        );
    }
    return assessWithFacts(man, inspection.architecture, inspection.expert_count);
}

fn assessWithFacts(
    man: *const manifest_mod.ModelManifest,
    architecture: []const u8,
    expert_count: u32,
) Assessment {
    if (man.model_type == .generator) return assessGenerator(architecture, expert_count);

    // Canonical Antfly bundles are known runtime contracts, even when their encoder
    // architecture names overlap with blocked standalone exports.
    if (man.isClipclapGgufBundle()) {
        return supported(architecture, "canonical ClipClap bundle");
    }
    if (std.mem.eql(u8, man.gliner_model_type, "gliner2")) {
        return supported(architecture, "GLiNER2 extraction runtime is enabled");
    }

    switch (man.model_type) {
        .rewriter => return unsupported(
            architecture,
            "the ONNX encoder-decoder rewrite runtime can panic while importing graphs",
        ),
        .reader => {
            if (man.native_arch_hint == .florence) {
                return supported(architecture, "Florence reader runtime is enabled");
            }
            return unsupported(
                architecture,
                "no safe reader runtime is available for this architecture",
            );
        },
        .embedder => switch (man.native_arch_hint) {
            .clip => return unsupported(
                architecture,
                "standalone CLIP image inference can exhaust process memory; use ClipClap",
            ),
            .clap => return unsupported(
                architecture,
                "standalone CLAP graph conversion is not supported; use ClipClap",
            ),
            else => {},
        },
        .classifier => {
            if (man.native_arch_hint == .layoutlmv3) {
                return unsupported(
                    architecture,
                    "LayoutLMv3 tokenizer assets are accepted by discovery but not by the loader",
                );
            }
        },
        .reranker, .chunker, .recognizer, .transcriber => {},
        .generator => unreachable,
    }

    if (std.mem.eql(u8, architecture, "nomic-bert")) {
        return unsupported(
            architecture,
            "the published GGUF tokenizer is not supported by the current loader",
        );
    }

    if (knownEncoderArchitecture(architecture)) {
        return supported(architecture, "recognized local inference runtime");
    }
    return experimental(
        architecture,
        "unrecognized model architecture; start the server with --allow-experimental-models to opt in",
    );
}

fn assessGenerator(architecture: []const u8, expert_count: u32) Assessment {
    if (std.mem.startsWith(u8, architecture, "gemma4") and expert_count > 0) {
        return unsupported(
            architecture,
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
        return supported(architecture, "decoder runtime is enabled for this release");
    }

    if (stringIn(architecture, &.{
        "gemma4_unified",
        "gemma4_unified_text",
    })) {
        return unsupported(
            architecture,
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
        return unsupported(
            architecture,
            "the current decoder path is known to be missing, unsafe, or to produce unusable output",
        );
    }

    return experimental(
        architecture,
        "unrecognized generator architecture; start the server with --allow-experimental-models to opt in",
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

fn supported(architecture: []const u8, reason: []const u8) Assessment {
    return .{ .level = .supported, .reason = reason, .architecture = architecture };
}

fn experimental(architecture: []const u8, reason: []const u8) Assessment {
    return .{ .level = .experimental, .reason = reason, .architecture = architecture };
}

fn unsupported(architecture: []const u8, reason: []const u8) Assessment {
    return .{ .level = .unsupported, .reason = reason, .architecture = architecture };
}

test "unknown generators are experimental and require opt in" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    const result = assess(&man, "brand_new_decoder");
    try std.testing.expectEqual(Level.experimental, result.level);
    try std.testing.expect(!result.allowed(false));
    try std.testing.expect(result.allowed(true));
}

test "known unsafe generators cannot be enabled by experimental opt in" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    const result = assess(&man, "deepseek4");
    try std.testing.expectEqual(Level.unsupported, result.level);
    try std.testing.expect(!result.allowed(true));
}

test "gemma 4 E4B architecture is enabled while unified layout is blocked" {
    var man = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    man.model_type = .generator;
    try std.testing.expectEqual(Level.supported, assess(&man, "gemma4").level);
    try std.testing.expectEqual(Level.unsupported, assess(&man, "gemma4_unified").level);
    try std.testing.expectEqual(
        Level.unsupported,
        assessWithFacts(&man, "gemma4", 128).level,
    );
}

test "release encoder contracts cover DeBERTa reranking and GLiNER2" {
    var reranker = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    reranker.model_type = .reranker;
    try std.testing.expectEqual(Level.supported, assess(&reranker, "deberta-v2").level);

    var gliner = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    gliner.model_type = .recognizer;
    gliner.gliner_model_type = "gliner2";
    try std.testing.expectEqual(Level.supported, assess(&gliner, "extractor").level);
}

test "known Qwen hybrid variants and unsupported Nomic GGUF stay blocked" {
    var generator = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    generator.model_type = .generator;
    try std.testing.expectEqual(Level.unsupported, assess(&generator, "qwen3_5_moe").level);
    try std.testing.expectEqual(Level.unsupported, assess(&generator, "qwen3_next").level);

    var embedder = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    embedder.model_type = .embedder;
    try std.testing.expectEqual(Level.unsupported, assess(&embedder, "nomic-bert").level);
}
