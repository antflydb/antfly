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

//! Opaque native user-row keys. Identity is independent of mutable SQL unique
//! constraints. Generate once during preparation and retain across admission;
//! the native expected-absent predicate is still mandatory at commit.
const native_platform = @import("antfly_platform");
const std = @import("std");

pub fn generate(alloc: std.mem.Allocator, io: std.Io) ![]const u8 {
    const output = try alloc.alloc(u8, 32);
    errdefer alloc.free(output);
    var entropy: [16]u8 = undefined;
    try io.randomSecure(&entropy);
    const encoded = std.fmt.bytesToHex(entropy, .lower);
    @memcpy(output, &encoded);
    return output;
}

test "native generated row identities are opaque owned keys" {
    const first = try generate(std.testing.allocator, native_platform.testing.io);
    defer std.testing.allocator.free(first);
    const second = try generate(std.testing.allocator, native_platform.testing.io);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqual(@as(usize, 32), first.len);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    for (first) |byte| try std.testing.expect(std.ascii.isHex(byte));
}
