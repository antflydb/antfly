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

const std = @import("std");

/// Independent of the request's activation workspace. Every inserted payload
/// also requires resident-tier admission and lives only with its model owner.
pub const max_bytes: usize = 640 * 1024 * 1024;
pub const max_entries: usize = 1536;

/// Vision linears have their own cap so warming a movie cannot consume the
/// language cache's allowance. Both classes still reserve against the same
/// model/process admission controller; neither cap grants memory by itself.
pub const metal_vision_max_bytes: usize = 640 * 1024 * 1024;
pub const MetalClass = enum { language, vision, audio };
pub const media_max_bytes: usize = 640 * 1024 * 1024;
/// The audio tower has four FFN projections per layer; its F32 prepared
/// weights exceed the vision ceiling. This cap still requires shared admission.
pub const audio_max_bytes: usize = 1536 * 1024 * 1024;
pub fn classLimit(class: MetalClass) usize {
    return switch (class) {
        .language => max_bytes,
        .vision => media_max_bytes,
        .audio => audio_max_bytes,
    };
}
pub fn nativeClass(name: []const u8) ?MetalClass {
    if (eligibleName(name)) return .language;
    if (std.mem.startsWith(u8, name, "vision_tower.") or std.mem.startsWith(u8, name, "embed_vision.")) return .vision;
    if (std.mem.startsWith(u8, name, "audio_tower.") or std.mem.startsWith(u8, name, "embed_audio.")) return .audio;
    return null;
}

pub fn metalClass(name: []const u8) ?MetalClass {
    if (eligibleName(name)) return .language;
    if ((std.mem.startsWith(u8, name, "vision_tower.encoder.layers.") and
        std.mem.endsWith(u8, name, ".linear.weight")) or
        std.mem.eql(u8, name, "vision_tower.patch_embedder.input_proj.weight") or
        std.mem.eql(u8, name, "embed_vision.embedding_projection.weight")) return .vision;
    if ((std.mem.startsWith(u8, name, "audio_tower.layers.") and std.mem.endsWith(u8, name, ".linear.weight")) or
        std.mem.eql(u8, name, "audio_tower.subsample_conv_projection.input_proj_linear.weight") or
        (std.mem.startsWith(u8, name, "audio_tower.layers.") and std.mem.endsWith(u8, name, ".self_attn.relative_k_proj.weight")) or
        std.mem.eql(u8, name, "embed_audio.embedding_projection.weight")) return .audio;
    return null;
}

test "embeddinggemma2 video Metal cache admits only checkpoint vision linears and projector" {
    try std.testing.expectEqual(MetalClass.language, metalClass("language_model.layers.0.self_attn.q_proj.weight").?);
    try std.testing.expectEqual(MetalClass.vision, metalClass("vision_tower.encoder.layers.0.self_attn.q_proj.linear.weight").?);
    try std.testing.expectEqual(MetalClass.vision, metalClass("vision_tower.encoder.layers.0.mlp.down_proj.linear.weight").?);
    try std.testing.expectEqual(MetalClass.vision, metalClass("embed_vision.embedding_projection.weight").?);
    try std.testing.expectEqual(MetalClass.vision, metalClass("vision_tower.patch_embedder.input_proj.weight").?);
    for ([_][]const u8{
        "vision_tower.encoder.layers.0.self_attn.q_norm.weight",
        "vision_tower.encoder.layers.0.self_attn.q_proj.linear.bias",
        "vision_tower.embeddings.position_embedding.weight",
        "vision_tower.embeddings.patch_embedding.weight",
        "audio_tower.encoder.layers.0.linear.weight",
        "language_model.embed_tokens.weight",
        "embed_vision.other.weight",
        "other.vision_tower.encoder.layers.0.linear.weight",
        "",
    }) |name| try std.testing.expectEqual(@as(?MetalClass, null), metalClass(name));
}

pub fn eligibleName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "language_model.") and
        !std.mem.eql(u8, name, "language_model.embed_tokens.weight") and
        (std.mem.endsWith(u8, name, ".weight") or
            std.mem.endsWith(u8, name, ".layer_scalar"));
}

test "embeddinggemma2 immutable weight cache excludes vocabulary media and unnamed tensors" {
    try std.testing.expect(eligibleName("language_model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(eligibleName("language_model.ple.per_layer_model_projection.weight"));
    try std.testing.expect(eligibleName("language_model.layers.0.layer_scalar"));
    for ([_][]const u8{ "", "language_model.embed_tokens.weight", "vision_tower.layer.weight", "audio_tower.layer.weight", "language_model.layers.0.bias", "other.language_model.norm.weight" }) |name|
        try std.testing.expect(!eligibleName(name));
}

test "embeddinggemma2 media cache classes remain independently bounded" {
    try std.testing.expectEqual(MetalClass.vision, nativeClass("vision_tower.encoder.layers.0.self_attn.q_norm.weight").?);
    try std.testing.expectEqual(MetalClass.audio, nativeClass("audio_tower.layers.0.norm_out.weight").?);
    try std.testing.expectEqual(MetalClass.audio, metalClass("audio_tower.layers.0.self_attn.q_proj.linear.weight").?);
    try std.testing.expectEqual(MetalClass.audio, metalClass("embed_audio.embedding_projection.weight").?);
    try std.testing.expectEqual(@as(?MetalClass, null), metalClass("audio_tower.layers.0.self_attn.q_proj.linear.bias"));
    try std.testing.expectEqual(@as(?MetalClass, null), nativeClass("language_model.embed_tokens.weight"));
}
