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

//! Floating comparison domains shared by SQL and encoded readers.
const std = @import("std");
pub const Domain = enum { finite, float32, float64 };
pub fn normalize(value: f64, domain: Domain) !f64 {
    return switch (domain) {
        .finite => if (std.math.isFinite(value)) value else error.SqlTypeMismatch,
        .float64 => value,
        .float32 => blk: {
            const result: f32 = @floatCast(value);
            if (std.math.isFinite(value) and (!std.math.isFinite(result) or (value != 0 and result == 0))) return error.SqlNumericOutOfRange;
            break :blk result;
        },
    };
}
pub fn order(left: f64, right: f64) std.math.Order {
    if (std.math.isNan(left)) return if (std.math.isNan(right)) .eq else .gt;
    if (std.math.isNan(right)) return .lt;
    return std.math.order(left, right);
}
