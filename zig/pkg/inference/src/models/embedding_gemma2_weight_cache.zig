// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

/// Independent of the request's activation workspace. Every inserted payload
/// also requires resident-tier admission and lives only with its model owner.
pub const max_bytes: usize = 640 * 1024 * 1024;
pub const max_entries: usize = 512;

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
