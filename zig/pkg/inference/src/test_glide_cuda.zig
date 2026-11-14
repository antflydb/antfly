// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
test "GLiDE CUDA focused test closure" {
    _ = @import("architectures/session_factory.zig");
    _ = @import("ops/cuda/cuda_compute.zig");
    _ = @import("ops/cuda/glide_delta.zig");
}
