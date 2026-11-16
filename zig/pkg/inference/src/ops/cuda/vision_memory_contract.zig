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

//! Shared by admission and both cuDNN vision dispatch paths. CUDA's portable
//! fallback streams attention scores in CTA-local memory; it never materializes
//! an S-by-S device tensor. Vendor plans may use this contract only when their
//! workspace fits below the same linear bound, otherwise dispatch falls back
//! before allocating or queuing work.
const std = @import("std");

fn hiddenBytes(rows: usize, hidden: usize) !usize {
    return std.math.mul(usize, try std.math.mul(usize, rows, hidden), @sizeOf(f32));
}

pub fn vendorWorkspaceLimit(rows: usize, hidden: usize) !usize {
    return std.math.add(usize, try std.math.mul(usize, try hiddenBytes(rows, hidden), 16), 4096);
}

pub fn workingBytes(rows: usize, hidden: usize) !usize {
    // DeviceScratch.acquire drains and frees the old arena before growth.
    // Its retained BF16 Q/K/V/O plus vendor arena is <=18H; it can overlap
    // the generic block's two 4H FFN intermediates (26H total), or the BF16
    // block's <=7H arena (25H). QKV/RoPE/projector outputs are separate.
    // Keep two additional H plus alignment/bias/position-pack allowance.
    return std.math.add(usize, try std.math.mul(usize, try hiddenBytes(rows, hidden), 28), 64 * 1024);
}

test "CUDA vision admission is linear and bounds retained vendor and FFN lifetimes" {
    const a = try workingBytes(8192, 1024);
    const b = try workingBytes(16384, 1024);
    try std.testing.expectEqual(2 * (a - 64 * 1024), b - 64 * 1024);
    try std.testing.expect(b < 4 * @as(usize, 1024 * 1024 * 1024));
    for ([_]usize{ 1, 256, 16384, 65536 }) |rows| {
        const h = try hiddenBytes(rows, 1024);
        const retained_arena = try std.math.add(usize, try vendorWorkspaceLimit(rows, 1024), h * 2 + 4096);
        try std.testing.expect(try workingBytes(rows, 1024) >= retained_arena + h * 8);
        try std.testing.expect(try workingBytes(rows, 1024) >= retained_arena + h * 7);
    }
    try std.testing.expectError(error.Overflow, workingBytes(std.math.maxInt(usize), 1024));
}
