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
