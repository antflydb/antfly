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

//! Shared admission bound for document source bytes. The normal mapper borrows
//! the original JSON; stripping special/vector fields serializes with exact
//! numeric lexemes, and removing fields cannot grow that complete encoding.
const std = @import("std");
pub fn maximumStoredBytes(a: std.mem.Allocator, value: []const u8) !usize {
    if (std.mem.indexOf(u8, value, "_edges") == null and std.mem.indexOf(u8, value, "_embeddings") == null and std.mem.indexOfScalar(u8, value, '\\') == null) return value.len;
    var parsed = std.json.parseFromSlice(std.json.Value, a, value, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTxnRecord,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return value.len;
    var buffer: [1024]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(parsed.value, .{}, &counter.writer);
    return @max(value.len, counter.fullCount());
}

test "document admission bound preserves numeric lexemes and escaped names without inflating ordinary source rows" {
    const values = [_][]const u8{ "{\"ts\":1,\"v\":9007199254740993}", "{\"\\u005fembeddings\":{},\"v\":1e200}", "{\"_edges\":{},\"name\":\"\\u0000\\u2028\"}" };
    for (values) |value| {
        const size = try maximumStoredBytes(std.testing.allocator, value);
        try std.testing.expect(size >= value.len);
        try std.testing.expect(size <= 2 * value.len);
    }
}
