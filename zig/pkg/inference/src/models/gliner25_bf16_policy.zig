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

//! The qualified Decide BF16 profile. Base retains the accurate FP32 default.
const std = @import("std");
const decide = @import("gliner_decide_qualification.zig");

pub const qualified_flags: u32 = 11_698_200;
pub const max_confidence_delta: f32 = 0.02;
pub const qualified_cublas_versions = [_]u32{ 130400, 130401 };
pub const workspace_bytes: usize = 64 * 1024 * 1024;

pub fn sourceEligible(identity: ?decide.Identity) bool {
    const source = identity orelse return false;
    return decide.qualifiedVariant(source) == .decide_deberta;
}

pub fn runtimeEligible(name: []const u8, major: i32, minor: i32, driver_version: i32, cublas_version: u32, cublaslt_version: ?usize) bool {
    return std.mem.eql(u8, name, "NVIDIA L4") and major == 8 and minor == 9 and
        driver_version >= 13000 and cublaslt_version == @as(usize, cublas_version) and std.mem.indexOfScalar(u32, &qualified_cublas_versions, cublas_version) != null;
}

test "GLiNER BF16 encoder upload automatic policy pins source hardware and runtime" {
    const identity = decide.pinned_legacy_fp32_identity;
    try std.testing.expect(sourceEligible(identity));
    try std.testing.expect(!sourceEligible(null));
    var changed = identity;
    changed.sidecars.config.sha256[0] ^= 1;
    try std.testing.expect(!sourceEligible(changed));
    changed = identity;
    changed.inventory.all_f32 = false;
    try std.testing.expect(!sourceEligible(changed));
    try std.testing.expect(!sourceEligible(decide.pinned_fp32_identity));
    for (qualified_cublas_versions) |version| {
        try std.testing.expect(runtimeEligible("NVIDIA L4", 8, 9, 13000, version, version));
        try std.testing.expect(!runtimeEligible("NVIDIA A100", 8, 0, 13000, version, version));
        try std.testing.expect(!runtimeEligible("NVIDIA GeForce RTX 4090", 8, 9, 13000, version, version));
        try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 12080, version, version));
    }
    try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 13000, 130399, 130399));
    try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 13000, 130402, 130402));
    try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 13000, 130400, 120800));
    try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 13000, 130400, 130401));
    try std.testing.expect(!runtimeEligible("NVIDIA L4", 8, 9, 13000, 130400, null));
    try std.testing.expect(!sourceEligible(decide.pinned_legacy_q8_identity));
}
