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
};

pub const Failure = enum {
    metal_not_built,
    metal_unavailable,
};

pub const BackendAvailabilityError = error{
    MetalUnavailable,
};

pub fn checkMetal(metal_built: bool, metal_available: bool) ?Failure {
    if (!metal_built) return .metal_not_built;
    if (!metal_available) return .metal_unavailable;
    return null;
}

pub fn printFailure(failure: Failure) void {
    switch (failure) {
        .metal_not_built => print("error: Metal backend is not built into this termite binary\n", .{}),
        .metal_unavailable => print("error: Metal backend requires a Metal-capable environment; Metal is unavailable here\n", .{}),
    }
}

pub fn raise(failure: Failure) BackendAvailabilityError {
    return switch (failure) {
        .metal_not_built, .metal_unavailable => error.MetalUnavailable,
    };
}

test "metal availability checks build and device state" {
    try std.testing.expectEqual(@as(?Failure, null), checkMetal(true, true));
    try std.testing.expectEqual(Failure.metal_not_built, checkMetal(false, true).?);
    try std.testing.expectEqual(Failure.metal_unavailable, checkMetal(true, false).?);
}
