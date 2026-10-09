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

//! Roaring Bitmap: compressed bitmap for document ID sets.
//!
//! Compatible with the standard roaring bitmap serialization; used for posting lists.
//! Uses SIMD (@Vector(8, u64)) for bulk bitwise operations on bitmap containers.
//!
//! Roaring bitmaps partition the 32-bit space into 16-bit "chunks" (high 16 bits).
//! Each chunk uses one of two container types:
//!   - Array container: sorted list of u16 values (sparse, < 4096 elements)
//!   - Bitmap container: 1024 u64 words = 65536 bits (dense, >= 4096 elements)
//!
//! Threshold: 4096 elements (same memory footprint for both representations).

const std = @import("std");
const roaring = @import("roaring");
test "native bitmap lower-bound seek benchmark" {
    var bitmap = roaring.RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    try bitmap.addRange(0, 50_000_000);
    try bitmap.prepareRead();
    var sum_seek: u64 = 0;
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..1_000_000) |i| {
        const target: u32 = @intCast(i * 50);
        var iterator = bitmap.iterator();
        sum_seek += iterator.seekTo(target).?;
    }
    const middle = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var sum_rank: u64 = 0;
    for (0..1_000_000) |i| {
        const target: u32 = @intCast(i * 50);
        sum_rank += bitmap.read_rank.?.select(bitmap.rank(target)).?;
    }
    const finished = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    try std.testing.expectEqual(sum_seek, sum_rank);
    std.debug.print("\nfresh iterator: {d} ns; prepared rank/select: {d} ns; checksum: {d}\n", .{ middle - started, finished - middle, sum_rank });
}
