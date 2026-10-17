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

//! Relative caller budget, interpreted once at inference route dispatch.
const std = @import("std");
pub const header = "X-Antfly-Inference-Timeout-Ms";
pub const max_budget_ms: u32 = 3_600_000;
pub fn establish(existing: ?u64, headers: anytype, now: u64) !?u64 {
    var value: ?[]const u8 = null;
    for (headers) |entry| if (std.ascii.eqlIgnoreCase(entry.name, header)) {
        if (value != null) return error.InvalidInferenceTimeout;
        value = entry.value;
    };
    const text = value orelse return existing;
    if (text.len == 0) return error.InvalidInferenceTimeout;
    for (text) |c| if (c < '0' or c > '9') return error.InvalidInferenceTimeout;
    const ms = std.fmt.parseUnsigned(u32, text, 10) catch return error.InvalidInferenceTimeout;
    if (ms == 0 or ms > max_budget_ms) return error.InvalidInferenceTimeout;
    const deadline = now +| @as(u64, ms) * std.time.ns_per_ms;
    return @min(existing orelse deadline, deadline);
}
const Header = struct { name: []const u8, value: []const u8 };
test "inference caller deadline narrows existing budget and handles clock bounds" {
    const rows = [_]Header{.{ .name = "x-antfly-inference-timeout-ms", .value = "25" }};
    try std.testing.expectEqual(@as(?u64, 25_000_100), try establish(null, &rows, 100));
    try std.testing.expectEqual(@as(?u64, 99), try establish(99, &rows, 100));
    try std.testing.expectEqual(@as(?u64, 25_000_100), try establish(99_000_000, &rows, 100));
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), try establish(null, &rows, std.math.maxInt(u64) - 1));
    try std.testing.expectEqual(@as(?u64, 99), try establish(99, &[_]Header{}, 100));
}
test "inference caller deadline rejects malformed duplicate and unbounded metadata" {
    for ([_][]const u8{ "", "0", "+1", "-1", " 1", "1 ", "1,2", "1.5", "3600001", "4294967296" }) |value| {
        const rows = [_]Header{.{ .name = header, .value = value }};
        try std.testing.expectError(error.InvalidInferenceTimeout, establish(null, &rows, 0));
    }
    const rows = [_]Header{ .{ .name = header, .value = "1" }, .{ .name = "x-antfly-inference-timeout-ms", .value = "1" } };
    try std.testing.expectError(error.InvalidInferenceTimeout, establish(null, &rows, 0));
}
