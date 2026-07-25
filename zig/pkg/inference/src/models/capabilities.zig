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

const std = @import("std");
const manifest_mod = @import("manifest.zig");

pub fn hasCapability(capabilities: []const []const u8, capability: []const u8) bool {
    for (capabilities) |cap| {
        if (std.mem.eql(u8, cap, capability)) return true;
    }
    return false;
}

pub fn modelSupportsCapability(
    model_kind: []const u8,
    gliner_model_type: []const u8,
    capabilities: []const []const u8,
    capability: []const u8,
) bool {
    if (hasCapability(capabilities, capability)) return true;
    if (!std.mem.eql(u8, model_kind, "recognizer")) return false;
    if (!std.mem.eql(u8, gliner_model_type, "gliner2")) return false;
    return std.mem.eql(u8, capability, "classification") or
        std.mem.eql(u8, capability, "relations") or
        std.mem.eql(u8, capability, "extraction");
}

pub fn modelAcceptsInput(manifest: *const manifest_mod.ModelManifest, input: []const u8) bool {
    return modelKindAcceptsInput(
        @tagName(manifest.model_type),
        manifest.gliner_model_type,
        manifest.inputs,
        manifest.visual_model_path != null or manifest.visual_projection_path != null,
        manifest.audio_model_path != null or manifest.audio_projection_path != null,
        input,
    );
}

pub fn modelKindAcceptsInput(
    model_kind: []const u8,
    gliner_model_type: []const u8,
    inputs: []const []const u8,
    has_visual: bool,
    has_audio: bool,
    input: []const u8,
) bool {
    if (inputs.len > 0) {
        for (inputs) |candidate| {
            if (std.mem.eql(u8, candidate, input)) return true;
        }
        return false;
    }

    if (std.mem.eql(u8, input, "image")) {
        return std.mem.eql(u8, model_kind, "reader") or
            (std.mem.eql(u8, model_kind, "embedder") and has_visual);
    }
    if (std.mem.eql(u8, input, "audio")) {
        return std.mem.eql(u8, model_kind, "transcriber") or
            (std.mem.eql(u8, model_kind, "embedder") and has_audio);
    }
    if (!std.mem.eql(u8, input, "text")) return false;

    return std.mem.eql(u8, model_kind, "chunker") or
        std.mem.eql(u8, model_kind, "reranker") or
        std.mem.eql(u8, model_kind, "generator") or
        std.mem.eql(u8, model_kind, "recognizer") or
        std.mem.eql(u8, model_kind, "classifier") or
        std.mem.eql(u8, model_kind, "rewriter") or
        std.mem.eql(u8, model_kind, "extractor") or
        std.mem.eql(u8, model_kind, "embedder") or
        std.mem.eql(u8, gliner_model_type, "gliner2");
}

test "modelSupportsCapability infers gliner2 extraction and classification" {
    try std.testing.expect(modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "classification"));
    try std.testing.expect(modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "relations"));
    try std.testing.expect(modelSupportsCapability("recognizer", "gliner2", &.{"labels"}, "extraction"));
    try std.testing.expect(!modelSupportsCapability("recognizer", "", &.{"labels"}, "extraction"));
}

test "modelKindAcceptsInput infers text and image modalities" {
    try std.testing.expect(modelKindAcceptsInput("recognizer", "gliner2", &.{}, false, false, "text"));
    try std.testing.expect(!modelKindAcceptsInput("recognizer", "gliner2", &.{}, false, false, "image"));
    try std.testing.expect(modelKindAcceptsInput("reader", "", &.{}, false, false, "image"));
    try std.testing.expect(!modelKindAcceptsInput("reader", "", &.{}, false, false, "text"));
    try std.testing.expect(modelKindAcceptsInput("embedder", "", &.{}, true, false, "image"));
    try std.testing.expect(modelKindAcceptsInput("transcriber", "", &.{}, false, false, "audio"));
    try std.testing.expect(modelKindAcceptsInput("recognizer", "", &.{"image"}, false, false, "image"));
    try std.testing.expect(!modelKindAcceptsInput("recognizer", "", &.{"image"}, false, false, "text"));
}

/// How far a non-decoder model class has been taken, mirroring `gpt.SupportLevel`.
///
/// Decoder families are tiered by `ModelFamily` in models/gpt.zig. Everything else --
/// embedders, rerankers, readers, rewriters -- is identified by architecture instead, so
/// it needs its own lookup over what the listing manifest already carries.
pub const SupportLevel = enum { supported, experimental, unsupported };

/// Tier for a model class other than generation.
///
/// `unsupported` here is not merely "unverified". Each entry below was measured, and the
/// ones that are blocked crash the process rather than returning an error, so they must
/// not be reachable from a request:
///
///   standalone CLIP  image embedding allocates without bound (~31 GB for one 64x64 PNG)
///                    until the OS kills the process. ClipClap embeds the same image in
///                    seconds, so the multimodal path itself is fine.
///   TrOCR-style      ONNX encoder/decoder readers OOM the same way. Florence-2 reads
///                    correctly and is the supported reader.
///   ONNX seq2seq     the rewrite path panics on a rank assertion while importing the
///                    graph (lib/ml/src/graph/shape.zig `axis < self.rank_`).
pub fn modelClassSupportLevel(man: *const manifest_mod.ModelManifest) SupportLevel {
    // ClipClap is the supported multimodal bundle and shares the clip/clap arch hints,
    // so it has to be recognized before those are rejected.
    if (man.isClipclapGgufBundle()) return .supported;

    switch (man.model_type) {
        // No rewriter model loads: the ONNX encoder/decoder import panics.
        .rewriter => return .unsupported,
        .reader => {
            // Florence-2 is the verified reader. Other encoder/decoder readers OOM.
            if (man.native_arch_hint == .florence) return .supported;
            return .unsupported;
        },
        .embedder => {
            switch (man.native_arch_hint) {
                .clip, .clap => return .unsupported,
                else => {},
            }
            return .supported;
        },
        .classifier => {
            // layoutlmv3 ships vocab.json + merges.txt with no tokenizer.json, which the
            // manifest accepts as a HuggingFace tokenizer but the loader rejects.
            if (man.native_arch_hint == .layoutlmv3) return .unsupported;
            return .supported;
        },
        .reranker, .chunker, .recognizer, .transcriber => return .supported,
        .generator => return .supported,
    }
}

test "clipclap stays supported while standalone clip and clap do not" {
    var bundle = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    bundle.model_type = .embedder;
    bundle.native_arch_hint = .clip;
    bundle.inference_bundle_family = "clipclap_gguf_bundle/v1";
    try std.testing.expectEqual(SupportLevel.supported, modelClassSupportLevel(&bundle));

    var standalone = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    standalone.model_type = .embedder;
    standalone.native_arch_hint = .clip;
    try std.testing.expectEqual(SupportLevel.unsupported, modelClassSupportLevel(&standalone));

    var plain = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    plain.model_type = .embedder;
    try std.testing.expectEqual(SupportLevel.supported, modelClassSupportLevel(&plain));
}

test "florence reads but other encoder-decoder readers are blocked" {
    var florence = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    florence.model_type = .reader;
    florence.native_arch_hint = .florence;
    try std.testing.expectEqual(SupportLevel.supported, modelClassSupportLevel(&florence));

    var trocr = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    trocr.model_type = .reader;
    try std.testing.expectEqual(SupportLevel.unsupported, modelClassSupportLevel(&trocr));
}

test "rewriters are blocked because the onnx seq2seq import panics" {
    var rewriter = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    rewriter.model_type = .rewriter;
    try std.testing.expectEqual(SupportLevel.unsupported, modelClassSupportLevel(&rewriter));
}
