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

//! Deployment contract for the canonical CUDA 13.2.86 artifact profiles.
//! Driver API versions describe capability, not the installed toolkit or the
//! operating-system driver package number. Native code uses CUDA 13 minor
//! compatibility; PTX 9.2 JIT requires CUDA 13.2 driver support.
const std = @import("std");

pub const Profile = enum { fatbin, portable, sm89 };
pub const LoadPath = enum { native, ptx };
pub const Error = error{ CudaDriverTooOld, CudaArtifactUnsupportedDevice };
pub const native_driver_version = 13000;
pub const ptx_driver_version = 13020;

pub fn loadPath(profile: Profile, major: i32, minor: i32) Error!LoadPath {
    if (profile == .sm89) {
        if (major == 8 and minor >= 9) return .native;
        return error.CudaArtifactUnsupportedDevice;
    }
    if (major < 7 or (major == 7 and minor < 5)) return error.CudaArtifactUnsupportedDevice;
    if (profile == .fatbin and major <= 12) return .native;
    return .ptx;
}

pub fn requiredDriver(path: LoadPath) i32 {
    return switch (path) {
        .native => native_driver_version,
        .ptx => ptx_driver_version,
    };
}

pub fn admit(profile: Profile, version: i32, major: i32, minor: i32) Error!LoadPath {
    const path = try loadPath(profile, major, minor);
    if (version < requiredDriver(path)) return error.CudaDriverTooOld;
    return path;
}

test "CUDA driver contract distinguishes native compatibility from PTX JIT" {
    for ([_][2]i32{ .{ 7, 5 }, .{ 8, 0 }, .{ 8, 9 }, .{ 9, 0 }, .{ 10, 0 }, .{ 11, 0 }, .{ 12, 0 }, .{ 12, 1 } }) |device| {
        try std.testing.expectEqual(LoadPath.native, try admit(.fatbin, 13000, device[0], device[1]));
        try std.testing.expectError(error.CudaDriverTooOld, admit(.fatbin, 12999, device[0], device[1]));
        try std.testing.expectError(error.CudaDriverTooOld, admit(.portable, 13000, device[0], device[1]));
        try std.testing.expectEqual(LoadPath.ptx, try admit(.portable, 13020, device[0], device[1]));
    }
    try std.testing.expectError(error.CudaDriverTooOld, admit(.fatbin, 13000, 13, 0));
    try std.testing.expectEqual(LoadPath.ptx, try admit(.fatbin, 13020, 13, 0));
    try std.testing.expectEqual(LoadPath.native, try admit(.sm89, 13000, 8, 9));
    try std.testing.expectError(error.CudaDriverTooOld, admit(.sm89, 12999, 8, 9));
    for ([_][2]i32{ .{ 8, 0 }, .{ 9, 0 }, .{ 12, 1 } }) |device| {
        try std.testing.expectError(error.CudaArtifactUnsupportedDevice, admit(.sm89, 13020, device[0], device[1]));
    }
    for ([_][2]i32{ .{ 0, 0 }, .{ 6, 1 }, .{ 7, 0 } }) |device| {
        for ([_]Profile{ .fatbin, .portable, .sm89 }) |profile| {
            try std.testing.expectError(error.CudaArtifactUnsupportedDevice, admit(profile, 13020, device[0], device[1]));
        }
    }
}
