// Copyright 2026 Antfly, Inc.
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

const atomic = @import("platform_atomic");

export fn checkAwakeClock() u32 {
    const time = @import("platform_time");
    const io = @import("std").Options.debug_io;
    const before = time.awakeNs(io);
    const after = time.awakeNs(io);
    return if (after > before) 0 else 1;
}

export fn checkEntropy() u32 {
    var bytes: [32]u8 = @splat(0);
    @import("platform_entropy").fill(@import("std").Options.debug_io, &bytes) catch |err| {
        return if (err == error.EntropyUnavailable) 2 else 3;
    };
    for (bytes) |byte| if (byte != 0x5a) return 1;
    return 0;
}

export fn checkWideValues() u32 {
    const high: u64 = 0x1_0000_0000;
    var value = atomic.Value(u64).init(high);
    if (value.load(.acquire) != high) return 1;
    if (value.fetchAdd(7, .acq_rel) != high) return 2;
    if (value.fetchSub(2, .acq_rel) != high + 7) return 3;
    if (value.swap(high + 8, .acq_rel) != high + 5) return 4;
    if (value.fetchOr(3, .release) != high + 8) return 5;
    if (value.fetchAnd(~@as(u64, 1), .release) != high + 11) return 6;
    if (value.cmpxchgStrong(high + 8, 0, .acq_rel, .acquire) != high + 10) return 7;
    if (value.cmpxchgWeak(high + 10, high + 20, .acq_rel, .acquire) != null) return 8;
    value.store(high, .release);
    if (value.load(.monotonic) != high) return 9;
    value.store(~@as(u64, 0), .release);
    _ = value.fetchAdd(1, .monotonic);
    if (value.load(.monotonic) != 0) return 10;
    var raw: u64 = high;
    if (atomic.load(u64, &raw, .monotonic) != high) return 11;
    if (atomic.fetchAdd(u64, &raw, 9, .monotonic) != high or raw != high + 9) return 12;
    return 0;
}

export fn checkF16Distances() u32 {
    const vector = @import("vector");
    var query: [35]f32 = undefined;
    var encoded: [35]f16 = undefined;
    var decoded: [35]f32 = undefined;
    for (0..query.len) |i| {
        query[i] = @as(f32, @floatFromInt(i % 11)) * 0.125 - 0.5;
        encoded[i] = @floatCast(@as(f32, @floatFromInt(i % 7)) * 0.25 - 0.75);
        decoded[i] = @as(f32, @floatCast(encoded[i])) * 2.5;
    }
    inline for ([_]vector.DistanceMetric{ .l2_squared, .inner_product, .cosine }) |metric| {
        const measure = switch (metric) {
            .l2_squared => vector.dot(&query, &query),
            .inner_product => 0,
            .cosine => vector.norm(&query),
        };
        const actual = vector.distanceToQueryF16(&query, measure, &encoded, 2.5, metric);
        const expected = vector.distanceToQuery(&query, measure, &decoded, metric);
        if (!@import("std").math.isFinite(actual) or @abs(actual - expected) > 0.0001) return 1;
    }
    return 0;
}
