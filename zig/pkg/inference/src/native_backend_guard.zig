// Copyright 2026 Antfly, Inc.
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

const print = std.debug.print;

pub const MetalHostedChoice = enum {
    metal,
    mlx,
};

pub const Failure = enum {
    metal_not_built,
    metal_unavailable,
    mlx_not_built,
    mlx_metal_unavailable,
};

pub const BackendAvailabilityError = error{
    MetalUnavailable,
    MlxUnavailable,
    MlxMetalUnavailable,
};

pub fn checkMetal(metal_built: bool, metal_available: bool) ?Failure {
    if (!metal_built) return .metal_not_built;
    if (!metal_available) return .metal_unavailable;
    return null;
}

pub fn checkMlx(mlx_built: bool, mlx_metal_available: bool) ?Failure {
    if (!mlx_built) return .mlx_not_built;
    if (!mlx_metal_available) return .mlx_metal_unavailable;
    return null;
}

pub fn printFailure(failure: Failure) void {
    switch (failure) {
        .metal_not_built => print("error: Metal backend is not built into this Antfly inference runtime\n", .{}),
        .metal_unavailable => print("error: Metal backend requires a Metal-capable environment; Metal is unavailable here\n", .{}),
        .mlx_not_built => print("error: MLX backend is not built into this Antfly inference runtime\n", .{}),
        .mlx_metal_unavailable => print("error: Metal/MLX backends require a Metal-capable environment; Metal is unavailable here\n", .{}),
    }
}

pub fn raise(failure: Failure) BackendAvailabilityError {
    return switch (failure) {
        .metal_not_built, .metal_unavailable => error.MetalUnavailable,
        .mlx_not_built => error.MlxUnavailable,
        .mlx_metal_unavailable => error.MlxMetalUnavailable,
    };
}

test "metal availability is independent of mlx availability" {
    try std.testing.expectEqual(@as(?Failure, null), checkMetal(true, true));
    try std.testing.expectEqual(Failure.metal_not_built, checkMetal(false, true).?);
    try std.testing.expectEqual(Failure.metal_unavailable, checkMetal(true, false).?);
}

test "mlx availability remains scoped to mlx" {
    try std.testing.expectEqual(@as(?Failure, null), checkMlx(true, true));
    try std.testing.expectEqual(Failure.mlx_not_built, checkMlx(false, true).?);
    try std.testing.expectEqual(Failure.mlx_metal_unavailable, checkMlx(true, false).?);
}
