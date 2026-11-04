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

const std = @import("std");
const A = std.mem.Allocator;

/// Fixed-width metadata encoding. Every caller preflights its exact size.
pub const Writer = struct {
    bytes: []u8,
    offset: usize = 0,
    pub fn put(self: *@This(), bytes: []const u8) void {
        @memcpy(self.bytes[self.offset..][0..bytes.len], bytes);
        self.offset += bytes.len;
    }
    pub fn int(self: *@This(), comptime T: type, value: T) void {
        std.mem.writeInt(T, self.bytes[self.offset..][0..@sizeOf(T)], value, .little);
        self.offset += @sizeOf(T);
    }
    pub fn key(self: *@This(), encoded: []const u8) !void {
        const n = try keyLength(encoded);
        self.int(u32, @intCast(n));
        try std.base64.standard.Decoder.decode(self.bytes[self.offset..][0..n], encoded);
        self.offset += n;
    }
};
pub const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,
    pub fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len - self.offset) return error.InvalidEnrichmentState;
        defer self.offset += n;
        return self.bytes[self.offset..][0..n];
    }
    pub fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    pub fn flag(self: *@This()) !bool {
        return switch (try self.int(u8)) {
            0 => false,
            1 => true,
            else => error.InvalidEnrichmentState,
        };
    }
    pub fn key(self: *@This(), a: A, max: usize) ![]const u8 {
        const n = try self.int(u32);
        if (n > max) return error.InvalidEnrichmentState;
        const raw = try self.take(n);
        if (n == 0) return "";
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(n));
        _ = std.base64.standard.Encoder.encode(encoded, raw);
        return encoded;
    }
    pub fn end(self: @This()) !void {
        if (self.offset != self.bytes.len) return error.InvalidEnrichmentState;
    }
};
pub fn keyLength(encoded: []const u8) !usize {
    return std.base64.standard.Decoder.calcSizeForSlice(encoded) catch error.InvalidEnrichmentState;
}
