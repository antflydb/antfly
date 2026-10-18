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

//! Fixed, immutable CSR filterbank plus window and FFT twiddles. Shared ABI
//! uses exact F32 integers for offsets; no request content enters this plan.
const std = @import("std");
pub const size = 2048;
pub const window_offset = 512;
pub const spans_offset = 832;
pub const coefficients_offset = 1216;
pub fn fill(out: *[size]f32, tw: []const f32, window: []const f32, filters: []const f32) !void {
    if (tw.len != 512 or window.len != 320 or filters.len != 128 * 257) return error.InvalidTensorShape;
    @memset(out, 0);
    @memcpy(out[0..512], tw);
    @memcpy(out[512..832], window);
    var next: usize = coefficients_offset;
    for (0..128) |mel| {
        const row = filters[mel * 257 ..][0..257];
        var first: usize = 0;
        while (first < 257 and row[first] == 0) : (first += 1) {}
        var end: usize = 257;
        while (end > first and row[end - 1] == 0) : (end -= 1) {}
        if (end - first > size - next) return error.InvalidTensorShape;
        out[spans_offset + mel * 3] = @floatFromInt(first);
        out[spans_offset + mel * 3 + 1] = @floatFromInt(end);
        out[spans_offset + mel * 3 + 2] = @floatFromInt(next);
        @memcpy(out[next..][0 .. end - first], row[first..end]);
        next += end - first;
    }
}
test "embeddinggemma2 fused mel plan retains exact coefficients empty bands and bounds" {
    var out: [size]f32 = undefined;
    const tw: [512]f32 = @splat(1);
    const window: [320]f32 = @splat(0.5);
    var filters: [128 * 257]f32 = @splat(0);
    filters[7] = 0.25;
    filters[8] = 0.75;
    try fill(&out, &tw, &window, &filters);
    try std.testing.expectEqualSlices(f32, &.{ 7, 9, 1216 }, out[832..835]);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.75 }, out[1216..1218]);
    try std.testing.expectEqual(@as(f32, 257), out[835]);
    @memset(&filters, 1);
    try std.testing.expectError(error.InvalidTensorShape, fill(&out, &tw, &window, &filters));
}
