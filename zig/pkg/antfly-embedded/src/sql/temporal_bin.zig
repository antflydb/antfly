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

//! Fixed-stride timestamp binning in PostgreSQL epoch microseconds. Arithmetic
//! is allocation-free; signed bounds and error precedence are part of SQL parity.
const std = @import("std");
const temporal = @import("temporal_value.zig");

pub fn bin(stride: temporal.Interval, source: temporal.Timestamp, origin: temporal.Timestamp) !temporal.Timestamp {
    if (source.infinity() != 0) return source;
    if (origin.infinity() != 0 or stride.infinity() != 0) return error.TemporalOutOfRange;
    if (stride.months != 0) return error.SqlDateBinCalendarStride;
    const days = std.math.mul(i64, stride.days, temporal.micros_per_day) catch return error.TemporalOutOfRange;
    const width = std.math.add(i64, days, stride.micros) catch return error.TemporalOutOfRange;
    if (width <= 0) return error.TemporalOutOfRange;
    const distance = std.math.sub(i64, source.micros, origin.micros) catch return error.TemporalOutOfRange;
    // Positive modulus produces the preceding boundary on both sides of the
    // origin, without floating point or a potentially overflowing quotient.
    const result = std.math.sub(i64, source.micros, @mod(distance, width)) catch return error.TemporalOutOfRange;
    return temporal.Timestamp.init(result);
}
