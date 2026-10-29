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

//! Isolated allocation/limit regressions for the production normalizer.
//! zig test --dep canonical -Mroot=tools/test_canonical_json.zig
//!   -Mcanonical=pkg/antfly-embedded/src/common/canonical_json.zig
const std = @import("std");
const canonical = @import("canonical");
fn allocationCase(a: std.mem.Allocator) !void {
    const raw = "{\"n\":[1e-400,1.234567890123456789]}";
    const output = try canonical.normalize(a, raw, 4096, .{});
    defer if (output.ptr != raw.ptr) a.free(output);
    try std.testing.expectEqualStrings("{\"n\":[0,1.2345678901234567]}", output);
}
test "normalization releases every allocation and preserves OOM" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}
test "normalization rejects malformed syntax and independent depth/byte limits" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "", "[", "{\"n\":]", "true true", "\"\xff\"" }) |raw| {
        if (canonical.normalize(a, raw, 4096, .{})) |value| {
            if (value.ptr != raw.ptr) a.free(value);
            return error.AcceptedMalformedJson;
        } else |_| {}
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, canonical.normalize(a, "[[[0]]]", 4096, .{ .depth = 3 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, canonical.normalize(a, "123456", 2, .{}));
}
test "normalization preserves escaped strings while normalizing nested numbers" {
    const a = std.testing.allocator;
    const raw = "{\"n\":1e-400,\"s\":\"\\uD83D\\uDE80\\n\\\"\\\\\"}";
    const output = try canonical.normalize(a, raw, 4096, .{ .preserve_strings = true });
    defer if (output.ptr != raw.ptr) a.free(output);
    try std.testing.expectEqualStrings("{\"n\":0,\"s\":\"\\uD83D\\uDE80\\n\\\"\\\\\"}", output);
}
