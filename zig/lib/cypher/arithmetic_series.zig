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

//! Exact arithmetic-series totals, including full signed-i64 domains.
const std = @import("std");

pub fn sum(count: u128, first: i128, step: i128) i256 {
    if (count == 0) return 0;
    const n: i256 = @intCast(count);
    const endpoints = 2 * @as(i256, first) + (n - 1) * @as(i256, step);
    // Either n is even, or n - 1 (and therefore endpoints) is even.
    // Halve that factor before multiplying: this is exact for negative sums
    // too, bounds intermediate growth, and needs no wide division libcall.
    return if (count & 1 == 0) (n >> 1) * endpoints else n * (endpoints >> 1);
}

test "arithmetic series halves an exact factor across signs and wide counts" {
    const counts = [_]u128{ 0, 1, 2, 3, 4, 17, (@as(u128, 1) << 63) - 1, @as(u128, 1) << 63, (@as(u128, 1) << 64) - 1, @as(u128, 1) << 64 };
    const values = [_]i128{ std.math.minInt(i64), -3, -1, 0, 1, 3, std.math.maxInt(i64) };
    for (counts) |count| for (values) |first| for (values) |step| {
        const n: i256 = @intCast(count);
        const expected = @divExact(n * (2 * @as(i256, first) + (n - 1) * @as(i256, step)), 2);
        try std.testing.expectEqual(expected, sum(count, first, step));
    };
    const full: u128 = @as(u128, 1) << 64;
    try std.testing.expectEqual(-(@as(i256, 1) << 63), sum(full, std.math.minInt(i64), 1));
    try std.testing.expectEqual(sum(full, std.math.minInt(i64), 1), sum(full, std.math.maxInt(i64), -1));
}
