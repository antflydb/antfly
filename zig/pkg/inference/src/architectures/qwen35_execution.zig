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

// Resolve recurrent placement once per CUDA session. Benchmark throughput is
// not a capability gate: every supported CUDA device uses the portable core.
const std = @import("std");
const platform = @import("antfly_platform");
const gpt = @import("../models/gpt.zig");

pub const Decision = struct {
    placement: enum { cuda, host },
    reason: enum { supported_geometry, operator_disabled, unsupported_device, unsupported_geometry },
};

pub fn cudaCoreEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_CUDA_ENABLE_QWEN35_RECURRENT_CORE", true);
}

pub fn supportedGeometry(cfg: gpt.Config) bool {
    const kh = cfg.qwen35_linear_num_key_heads;
    const vh = cfg.qwen35_linear_num_value_heads;
    return cfg.isQwen35() and cfg.qwen35_has_linear_attention and
        cfg.qwen35_linear_key_head_dim == 128 and cfg.qwen35_linear_value_head_dim == 128 and
        cfg.qwen35_linear_conv_kernel_dim == 4 and kh > 0 and kh <= 256 and
        vh > 0 and vh <= 256 and vh % kh == 0 and
        std.math.isFinite(cfg.norm_eps) and cfg.norm_eps > 0;
}

pub fn resolve(cfg: gpt.Config, compute_major: i32, enabled: bool) Decision {
    if (!enabled) return .{ .placement = .host, .reason = .operator_disabled };
    // The surrounding CUDA model profile requires BF16-capable SM80 or newer.
    // No GB10-only fusion or external prefill library is selected here.
    if (compute_major < 8) return .{ .placement = .host, .reason = .unsupported_device };
    if (!supportedGeometry(cfg)) return .{ .placement = .host, .reason = .unsupported_geometry };
    return .{ .placement = .cuda, .reason = .supported_geometry };
}

pub fn apply(cfg: *gpt.Config, compute_major: i32, enabled: bool) Decision {
    const decision = resolve(cfg.*, compute_major, enabled);
    // Admission and state construction must see the same placement as dispatch.
    cfg.qwen35_device_only_state = decision.placement == .cuda;
    return decision;
}

test "LightOnOCR3 CUDA recurrent placement supports A100 L4 and GB10 without throughput flags" {
    for ([_][]const u8{ @embedFile("../models/testdata/lightonocr3/config.json"), @embedFile("../models/testdata/lightonocr3/config-4b.json") }) |json| {
        const original = try gpt.parseConfig(std.testing.allocator, json);
        for ([_]i32{ 8, 9, 12, 13 }) |major| {
            var cfg = original;
            const decision = apply(&cfg, major, true);
            try std.testing.expectEqual(.cuda, decision.placement);
            try std.testing.expectEqual(.supported_geometry, decision.reason);
            try std.testing.expect(cfg.qwen35_device_only_state);
        }
        var cfg = original;
        const disabled = apply(&cfg, 8, false);
        try std.testing.expectEqual(.operator_disabled, disabled.reason);
        try std.testing.expect(!cfg.qwen35_device_only_state);
        try std.testing.expectEqual(.unsupported_device, resolve(original, 7, true).reason);
    }
}

test "LightOnOCR3 CUDA recurrent placement rejects unsupported state geometry before admission" {
    const original = try gpt.parseConfig(std.testing.allocator, @embedFile("../models/testdata/lightonocr3/config-4b.json"));
    var cfg = original;
    cfg.qwen35_linear_key_head_dim = 64;
    try std.testing.expectEqual(.unsupported_geometry, resolve(cfg, 8, true).reason);
    cfg = original;
    cfg.qwen35_linear_num_key_heads = 0;
    try std.testing.expectEqual(.unsupported_geometry, resolve(cfg, 8, true).reason);
    cfg = original;
    cfg.qwen35_linear_num_value_heads = 17;
    try std.testing.expectEqual(.unsupported_geometry, resolve(cfg, 8, true).reason);
    cfg = original;
    cfg.norm_eps = std.math.nan(f32);
    try std.testing.expectEqual(.unsupported_geometry, resolve(cfg, 8, true).reason);
    try std.testing.expectEqual(.unsupported_geometry, resolve(.{}, 8, true).reason);
}
