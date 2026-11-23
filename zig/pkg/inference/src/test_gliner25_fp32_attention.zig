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
test "GLiNER FP32 attention focused ownership root" {
    std.testing.refAllDecls(@import("ops/cuda/gliner25_fast_f32.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_fp32_attention.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_bf16_attention.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_bf16_ffn.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_bf16_projections.zig"));
    std.testing.refAllDecls(@import("ops/cuda/gliner25_relative_cache.zig"));
    _ = @import("ops/cuda/gliner25_banded_attention_test.zig");
    _ = @import("ops/cuda/gliner25_norm_mirror_test.zig");
    _ = @import("architectures/gliner/boundary_ops.zig");
    _ = @import("extractors/gliner_boundary_executor.zig");
    _ = @import("architectures/session_factory.zig");
    std.testing.refAllDecls(@import("models/gliner25_bf16_policy.zig"));
    _ = @import("ops/cuda/cuda_compute.zig");
    _ = @import("architectures/gliner/boundary_request_device_test.zig");
}
