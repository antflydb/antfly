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

//! Binary physical routes use base64 at every private JSON boundary.
const std = @import("std");
pub const Bytes = struct {
    value: []const u8,
    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        std.debug.assert(self.value.len <= 4096);
        var buffer: [5464]u8 = undefined;
        try stream.write(std.base64.standard.Encoder.encode(&buffer, self.value));
    }
    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        const encoded = try std.json.innerParse([]const u8, a, source, options);
        return fromEncoded(a, encoded);
    }
    pub fn jsonParseFromValue(a: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return fromEncoded(a, try std.json.innerParseFromValue([]const u8, a, value, options));
    }
    fn fromEncoded(a: std.mem.Allocator, encoded: []const u8) !@This() {
        if (encoded.len > 5464) return error.InvalidCharacter;
        const n = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidCharacter;
        if (n > 4096) return error.InvalidCharacter;
        const raw = try a.alloc(u8, n);
        errdefer a.free(raw);
        std.base64.standard.Decoder.decode(raw, encoded) catch return error.InvalidCharacter;
        return .{ .value = raw };
    }
};
test "group binary routes round trip non UTF8 range boundaries in private JSON" {
    const a = std.testing.allocator;
    const original = Bytes{ .value = "\x80\xff\x00" };
    const wire = try std.json.Stringify.valueAlloc(a, original, .{});
    defer a.free(wire);
    try std.testing.expect(std.unicode.utf8ValidateSlice(wire));
    var decoded = try std.json.parseFromSlice(Bytes, a, wire, .{});
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, original.value, decoded.value.value);
    var json = try std.json.parseFromSlice(std.json.Value, a, wire, .{});
    defer json.deinit();
    var from_value = try std.json.parseFromValue(Bytes, a, json.value, .{});
    defer from_value.deinit();
    try std.testing.expectEqualSlices(u8, original.value, from_value.value.value);
    try std.testing.expectError(error.InvalidCharacter, std.json.parseFromSlice(Bytes, a, "\"invalid%%%\"", .{}));
}
