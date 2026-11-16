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

//! Instruction/layout requirements of generated programs, not benchmark targets.
const std = @import("std");

pub const CudaExecutionContract = enum {
    portable_simt_sm75,
    wmma_fragment_sm89,

    pub fn supportsDevice(self: CudaExecutionContract, major: i32, minor: i32) bool {
        if (major < 0 or minor < 0) return false;
        return switch (self) {
            .portable_simt_sm75 => major >= 8 or (major == 7 and minor >= 5),
            // The generated Flash implementation indexes opaque WMMA
            // fragment elements. Its layout is currently specific to SM89.
            .wmma_fragment_sm89 => major == 8 and minor == 9,
        };
    }
};

test "CUDA execution contracts distinguish portable instructions from opaque register layouts" {
    const portable = CudaExecutionContract.portable_simt_sm75;
    for ([_][2]i32{ .{ 7, 5 }, .{ 8, 0 }, .{ 8, 6 }, .{ 8, 9 }, .{ 9, 0 }, .{ 12, 1 } }) |cc| {
        try std.testing.expect(portable.supportsDevice(cc[0], cc[1]));
        try std.testing.expectEqual(cc[0] == 8 and cc[1] == 9, CudaExecutionContract.wmma_fragment_sm89.supportsDevice(cc[0], cc[1]));
    }
    try std.testing.expect(!portable.supportsDevice(7, 0));
    try std.testing.expect(!portable.supportsDevice(8, -1));
}
