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

//! Canonical logical value identities, shared by SQL and native source metadata.
const std = @import("std");
const V = std.json.Value;
const HashWriter = struct {
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var count: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            self.hash.update(bytes);
            count += bytes.len;
        }
        for (0..splat) |_| {
            const bytes = data[data.len - 1];
            self.hash.update(bytes);
            count += bytes.len;
        }
        return count;
    }
};
const domain = "antfly.source.value.v1\x00";
pub fn digestValue(value: V) ![32]u8 {
    var out: HashWriter = .{};
    out.hash.update(domain);
    try std.json.Stringify.value(value, .{}, &out.writer);
    try out.writer.flush();
    var digest: [32]u8 = undefined;
    out.hash.final(&digest);
    return digest;
}
pub fn identity(value: V) ![68]u8 {
    return "sv1-".* ++ std.fmt.bytesToHex(try digestValue(value), .lower);
}
/// Caller has already strictly validated the scalar JSON. Plain UTF-8 strings
/// are their own canonical encoding: hash borrowed bytes without allocating or
/// reserializing. Numbers and escaped strings use the typed reference.
/// Keep this fallback rather than guessing about floating-point normalization.
pub fn validatedScalarDigest(a: std.mem.Allocator, raw: []const u8) !?[32]u8 {
    const value = std.mem.trim(u8, raw, " \t\r\n");
    const plain = value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"' and
        std.mem.indexOfScalar(u8, value, '\\') == null and std.unicode.utf8ValidateSlice(value);
    if (plain or std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false") or std.mem.eql(u8, value, "null")) {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(domain);
        hash.update(value);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
    // Sources are strictly validated before publication. Stream the identical
    // typed canonical encoding, including escaped keys/strings and numbers.
    // No payload-sized DOM is needed. Unsupported depth remains optional.
    var out: HashWriter = .{};
    out.hash.update(domain);
    @import("canonical_json.zig").write(a, value, &out.writer, .{ .depth = 128 }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    };
    try out.writer.flush();
    var digest: [32]u8 = undefined;
    out.hash.final(&digest);
    return digest;
}

test "logical value conditional streaming canonical digests retain the typed ABI" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{\"n\":1e-400,\"items\":[1.234567890123456789,18446744073709551615,null]}", "[\"line\\nquote\\\"\",{\"k\\uD83D\\uDE80\":\"\\uD83D\\uDE80\"}]", "{\"null\":null,\"bool\":true,\"negative\":-0.0}" }) |raw| {
        var parsed = try std.json.parseFromSlice(V, a, raw, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(try digestValue(parsed.value), (try validatedScalarDigest(a, raw)).?);
    }
}
