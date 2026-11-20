// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
test "GLiNER fast FP32 focused root" {
    std.testing.refAllDecls(@import("models/gliner25_fast_f32_policy.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_fast_f32.zig"));
}
