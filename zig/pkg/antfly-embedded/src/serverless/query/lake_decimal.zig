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

//! Exact decimal128 decoding and fixed-scale formatting. No floating-point
//! conversion occurs between the Parquet payload and the SQL string contract.
const std = @import("std");
const decimal = @import("../../common/decimal128.zig");
fn mapError(err: anyerror) anyerror {
    return switch (err) {
        error.UnsupportedDecimal128 => error.UnsupportedParquetPage,
        error.InvalidDecimal128 => error.InvalidParquetPage,
        else => err,
    };
}
pub fn validate(precision: i32, scale: i32) !void {
    decimal.validate(precision, scale) catch |err| return mapError(err);
}
pub fn validateCoefficient(value: i128, precision: i32, scale: i32) !void {
    decimal.validateCoefficient(value, precision, scale) catch |err| return mapError(err);
}
pub fn formatAlloc(a: std.mem.Allocator, value: i128, precision: i32, scale: i32) ![]u8 {
    return decimal.formatAlloc(a, value, precision, scale) catch |err| return mapError(err);
}
pub fn signedBigEndian(bytes: []const u8) !i128 {
    return decimal.signedBigEndian(bytes) catch |err| return mapError(err);
}
test "external lake exact decimal128 preserves precision scale sign and validates values" {
    const a = std.testing.allocator;
    const cases = [_]struct { n: i128, precision: i32, scale: i32, text: []const u8 }{
        .{ .n = 9007199254740993, .precision = 18, .scale = 0, .text = "9007199254740993" },
        .{ .n = -1, .precision = 38, .scale = 38, .text = "-0.00000000000000000000000000000000000001" },
        .{ .n = 12300, .precision = 5, .scale = 4, .text = "1.2300" },
        .{ .n = 0, .precision = 4, .scale = 4, .text = "0.0000" },
        .{ .n = 99999999999999999999999999999999999999, .precision = 38, .scale = 2, .text = "999999999999999999999999999999999999.99" },
    };
    for (cases) |case| {
        const text = try formatAlloc(a, case.n, case.precision, case.scale);
        defer a.free(text);
        try std.testing.expectEqualStrings(case.text, text);
        var bytes: [16]u8 = undefined;
        std.mem.writeInt(i128, &bytes, case.n, .big);
        try std.testing.expectEqual(case.n, try signedBigEndian(&bytes));
    }
    try std.testing.expectEqual(@as(i128, -2), try signedBigEndian(&.{0xfe}));
    try std.testing.expectError(error.InvalidParquetPage, formatAlloc(a, 100, 2, 0));
    try std.testing.expectError(error.UnsupportedParquetPage, validate(39, 0));
}
