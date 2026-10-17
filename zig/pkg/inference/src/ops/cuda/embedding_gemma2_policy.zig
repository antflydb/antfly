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

//! Immutable per-session qualification policy. New arithmetic routes are opt-in
//! until matched vector/ranking and completed-device benchmarks qualify them.
const env = @import("antfly_platform").env;
pub const Policy = struct {
    extended_flash: bool = false,
    fused_vision: bool = false,
    packed_projections: bool = false,
    graph_replay: bool = false,
    image_batch: bool = false,
    pub fn arithmeticBits(self: Policy) u8 {
        return @as(u8, @intFromBool(self.extended_flash)) | (@as(u8, @intFromBool(self.fused_vision)) << 1) | (@as(u8, @intFromBool(self.packed_projections)) << 2) | (@as(u8, @intFromBool(self.graph_replay)) << 3) | (@as(u8, @intFromBool(self.image_batch)) << 4);
    }
};
pub fn configured() Policy {
    return .{
        .extended_flash = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH", false),
        .fused_vision = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_VISION", false),
        .packed_projections = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS", false),
        .image_batch = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_IMAGE_BATCH", false),
        .graph_replay = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY", false),
    };
}

test "embeddinggemma2 CUDA arithmetic profiles are disjoint including graph padding and image cohorts" {
    const std = @import("std");
    const profiles = [_]Policy{ .{}, .{ .extended_flash = true }, .{ .fused_vision = true }, .{ .packed_projections = true }, .{ .graph_replay = true }, .{ .image_batch = true } };
    for (profiles, 0..) |left, i| for (profiles[i + 1 ..]) |right| try std.testing.expect(left.arithmeticBits() != right.arithmeticBits());
}
