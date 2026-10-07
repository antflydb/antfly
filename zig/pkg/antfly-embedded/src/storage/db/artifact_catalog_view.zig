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

//! Allocation-free view of the physical index catalog. Callers must consume
//! the iterator to completion so trailing data cannot escape validation.
const std = @import("std");

pub const Entry = struct { name: []const u8, kind: u8, config: []const u8, generation: u64 };
pub const Iterator = struct {
    bytes: []const u8,
    offset: usize = 12,
    remaining: u32,

    pub fn init(bytes: []const u8) !Iterator {
        if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "AIDX") or
            std.mem.readInt(u32, bytes[4..8], .little) != 2) return error.InvalidArtifactCatalogCommand;
        const count = std.mem.readInt(u32, bytes[8..12], .little);
        if (count > (bytes.len - 12) / 17) return error.InvalidArtifactCatalogCommand;
        return .{ .bytes = bytes, .remaining = count };
    }

    fn take(self: *Iterator, size: usize) ![]const u8 {
        if (size > self.bytes.len - self.offset) return error.InvalidArtifactCatalogCommand;
        const result = self.bytes[self.offset..][0..size];
        self.offset += size;
        return result;
    }
    fn string(self: *Iterator) ![]const u8 {
        const len = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
        return self.take(len);
    }
    pub fn next(self: *Iterator) !?Entry {
        if (self.remaining == 0) {
            if (self.offset != self.bytes.len) return error.InvalidArtifactCatalogCommand;
            return null;
        }
        const name = try self.string();
        const kind = (try self.take(1))[0];
        const config = try self.string();
        const generation = std.mem.readInt(u64, (try self.take(8))[0..8], .little);
        self.remaining -= 1;
        return .{ .name = name, .kind = kind, .config = config, .generation = generation };
    }
};

test "physical artifact catalog view rejects truncation and trailing data" {
    const raw = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00" ++
        "\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}\x07\x00\x00\x00\x00\x00\x00\x00";
    var view = try Iterator.init(raw);
    const entry = (try view.next()).?;
    try std.testing.expectEqualStrings("g", entry.name);
    try std.testing.expectEqual(@as(u64, 7), entry.generation);
    try std.testing.expect((try view.next()) == null);
    var extra = try Iterator.init(raw ++ "x");
    _ = try extra.next();
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, extra.next());
    var truncated = try Iterator.init(raw[0 .. raw.len - 1]);
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, truncated.next());
}
