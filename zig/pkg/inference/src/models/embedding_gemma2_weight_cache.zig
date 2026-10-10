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
