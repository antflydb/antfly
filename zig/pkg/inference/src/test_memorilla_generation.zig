// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
test {
    _ = @import("pipelines/memorilla_generation.zig");
    _ = @import("server/memorilla_service.zig");
    if (@import("build_options").enable_cuda) _ = @import("ops/cuda/cuda_compute.zig");
}
