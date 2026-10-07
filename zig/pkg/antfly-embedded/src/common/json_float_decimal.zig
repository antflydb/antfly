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

//! Exact interpretation of an already-owned JSON floating numeric value.
//! This is not SQL float-to-JSONB casting: that boundary owns its decimal
//! conversion policy. Parsing JSONB text must retain exact decimal tokens.
//! Comparison, hashing and canonical persistence share this allocation-free
//! kernel so persistence cannot round a value after its logical hash is fixed.
const std = @import("std");

pub const Parts = struct {
    negative: bool,
    mantissa: u64,
    binary_exponent: i32,

    pub fn init(number: f64) !Parts {
        if (!std.math.isFinite(number)) return error.InvalidJsonNumber;
        if (number == 0) return .{ .negative = false, .mantissa = 0, .binary_exponent = 0 };
        const bits: u64 = @bitCast(number);
        const raw_exponent = (bits >> 52) & 0x7ff;
        var mantissa: u64 = bits & 0xfffffffffffff;
        if (raw_exponent != 0) mantissa |= 1 << 52;
        var exponent: i32 = if (raw_exponent == 0) -1074 else @as(i32, @intCast(raw_exponent)) - 1023 - 52;
        while (exponent < 0 and mantissa & 1 == 0) {
            mantissa >>= 1;
            exponent += 1;
        }
        return .{ .negative = bits >> 63 != 0, .mantissa = mantissa, .binary_exponent = exponent };
    }

    /// Charge this before expansion when the caller has a work budget.
    pub fn work(self: Parts) usize {
        return @intCast(-@min(self.binary_exponent, 0));
    }

    pub fn decimalExponent(self: Parts) i32 {
        return @min(self.binary_exponent, 0);
    }

    /// At most 767 decimal digits, bounded by IEEE-754 binary64, not input
    /// text. The fixed integer avoids arbitrary-precision heap allocations.
    pub fn coefficient(self: Parts) u4096 {
        var exact: u4096 = self.mantissa;
        if (self.binary_exponent >= 0) exact <<= @intCast(self.binary_exponent) else {
            for (0..self.work()) |_| exact *= 5;
        }
        return exact;
    }
};
