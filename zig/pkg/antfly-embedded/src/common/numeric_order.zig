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

//! Exact ordering of int64 and finite float64 values, without rounding integers.
const std = @import("std");

pub fn intFloat(integer: i64, number: f64) std.math.Order {
    std.debug.assert(std.math.isFinite(number));
    if (number >= 9223372036854775808.0) return .lt;
    if (number < -9223372036854775808.0) return .gt;
    const truncated: i64 = @intFromFloat(number);
    const order = std.math.order(integer, truncated);
    if (order != .eq) return order;
    return std.math.order(@as(f64, @floatFromInt(truncated)), number);
}
