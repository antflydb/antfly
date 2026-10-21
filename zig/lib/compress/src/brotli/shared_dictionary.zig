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

//! RFC 9841 shared dictionaries. Metadata is prepared once and borrows the
//! supplied bytes; decoding never expands word/transform combinations in memory.
const std = @import("std");
const A = std.mem.Allocator;
pub const Format = enum { auto, raw, brotli, @"brotli-google" };
pub const Error = error{ InvalidDictionary, InvalidDictionaryReference, DictionaryOutputTooSmall };
pub const Transform = struct {
    prefix: []const u8,
    suffix: []const u8,
    operation: u8,
    parameter: u16 = 0,
    pub fn apply(self: Transform, word: []const u8, output: []u8) Error![]const u8 {
        if (self.operation > 22) return error.InvalidDictionary;
        var begin: usize = 0;
        var end = word.len;
        if (self.operation >= 1 and self.operation <= 9) end -|= self.operation;
        if (self.operation >= 12 and self.operation <= 20) begin = @min(end, self.operation - 11);
        const size = self.prefix.len + (end - begin) + self.suffix.len;
        if (size > output.len) return error.DictionaryOutputTooSmall;
        @memcpy(output[0..self.prefix.len], self.prefix);
        const body = output[self.prefix.len..][0 .. end - begin];
        @memcpy(body, word[begin..end]);
        if (self.operation == 10 or self.operation == 11 or self.operation == 21 or self.operation == 22) {
            var at: usize = 0;
            while (at < body.len) {
                const step = if (self.operation <= 11) ferment(body[at..]) else shift(body[at..], self.parameter);
                if (self.operation == 10 or self.operation == 21) break;
                at += step;
            }
        }
        @memcpy(output[self.prefix.len + body.len ..][0..self.suffix.len], self.suffix);
        return output[0..size];
    }
    fn ferment(bytes: []u8) usize {
        if (bytes[0] < 0xc0) {
            if (bytes[0] >= 'a' and bytes[0] <= 'z') bytes[0] ^= 32;
            return 1;
        }
        if (bytes[0] < 0xe0) {
            if (bytes.len >= 2) bytes[1] ^= 32;
            return 2;
        }
        if (bytes.len >= 3) bytes[2] ^= 5;
        return 3;
    }
    fn shift(bytes: []u8, parameter: u16) usize {
        const first = bytes[0];
        const n: usize = if (first < 0x80) 1 else if (first < 0xc0 or first >= 0xf8) return 1 else if (first < 0xe0) 2 else if (first < 0xf0) 3 else 4;
        if (bytes.len < n) return bytes.len;
        const first_mask: u8 = switch (n) {
            1 => 0x7f,
            2 => 0x1f,
            3 => 0x0f,
            4 => 7,
            else => unreachable,
        };
        var scalar: u32 = first & first_mask;
        for (bytes[1..n]) |byte| scalar = (scalar << 6) | (byte & 63);
        scalar += @as(u32, parameter) + (if (parameter >= 0x8000) @as(u32, 0xff0000) else 0);
        var i = n;
        while (i > 1) {
            i -= 1;
            bytes[i] = (bytes[i] & 0xc0) | @as(u8, @truncate(scalar & 63));
            scalar >>= 6;
        }
        bytes[0] = (first & ~first_mask) | (@as(u8, @truncate(scalar)) & first_mask);
        return n;
    }
};
pub const WordList = struct { data: []const u8, bits: [32]u8, offsets: [32]usize };
pub const TransformList = struct {
    strings: []const u8,
    offsets: []const u16,
    triplets: []const u8,
    parameters: []const u8 = &.{},
    google_order: bool = false,
    pub fn count(self: TransformList) usize {
        return self.triplets.len / 3;
    }
    pub fn get(self: TransformList, index: usize) Error!Transform {
        if (index >= self.count()) return error.InvalidDictionaryReference;
        const t = self.triplets[index * 3 ..][0..3];
        const prefix = self.string(t[0]);
        const suffix = self.string(t[if (self.google_order) @as(usize, 2) else 1]);
        return .{ .prefix = prefix, .suffix = suffix, .operation = t[if (self.google_order) @as(usize, 1) else 2], .parameter = if (self.parameters.len == 0) 0 else std.mem.readInt(u16, self.parameters[index * 2 ..][0..2], .little) };
    }
    fn string(self: TransformList, index: u8) []const u8 {
        const at = self.offsets[index];
        return self.strings[at + 1 ..][0..self.strings[at]];
    }
};
pub const Dictionary = struct {
    arena: std.heap.ArenaAllocator,
    lz77: []const u8,
    words: []const WordList = &.{},
    transforms: []const TransformList = &.{},
    pairs: [64]Pair = @splat(.{}),
    pair_count: usize = 1,
    contextual: bool = false,
    contexts: [64]u8 = @splat(0),
    const Pair = struct { words: u8 = 0, transforms: u8 = 0 };
    pub const Resolved = struct { word: []const u8, transform: Transform };
    /// Auto recognizes the RFC signature. Google Brotli's experimental wire
    /// order is explicit because its valid triplets can be ambiguous with RFC.
    pub fn init(a: A, bytes: []const u8, format: Format) (Error || A.Error)!Dictionary {
        var self = Dictionary{ .arena = .init(a), .lz77 = bytes };
        errdefer self.deinit();
        if (format == .raw or (format == .auto and !std.mem.startsWith(u8, bytes, &.{ 0x91, 0 }))) return self;
        var input = Input{ .bytes = bytes };
        if (!std.mem.eql(u8, try input.take(2), &.{ 0x91, 0 })) return error.InvalidDictionary;
        const start = input.at;
        const lz_size = try input.varint();
        if (format == .@"brotli-google" and (input.at - start > 5 or lz_size > 0xffffffff)) return error.InvalidDictionary;
        self.lz77 = try input.take(lz_size);
        const allocator = self.arena.allocator();
        const nw = try input.byte();
        if (nw > 64) return error.InvalidDictionary;
        const words = try allocator.alloc(WordList, nw);
        self.words = words;
        for (words) |*word| {
            var bits: [32]u8 = @splat(0);
            @memcpy(bits[4..], try input.take(28));
            var offsets: [32]usize = undefined;
            var size: usize = 0;
            for (bits, 0..) |n, length| {
                if (n > 15) return error.InvalidDictionary;
                offsets[length] = size;
                if (n != 0) size += length << @as(u4, @intCast(n));
            }
            word.* = .{ .data = try input.take(size), .bits = bits, .offsets = offsets };
        }
        const nt = try input.byte();
        if (nt > 64) return error.InvalidDictionary;
        const transforms = try allocator.alloc(TransformList, nt);
        self.transforms = transforms;
        for (transforms) |*list| {
            const size = std.mem.readInt(u16, (try input.take(2))[0..2], .little);
            if (size == 0) return error.InvalidDictionary;
            const strings = try input.take(size);
            var offsets: [256]u16 = undefined;
            var count: usize = 0;
            var at: usize = 0;
            while (at < strings.len) {
                if (count == offsets.len) return error.InvalidDictionary;
                offsets[count] = @intCast(at);
                count += 1;
                const length = strings[at];
                at += 1;
                if (length == 0) {
                    if (at != strings.len) return error.InvalidDictionary;
                    break;
                }
                if (length >= strings.len - at) return error.InvalidDictionary;
                at += length;
            }
            if (strings[strings.len - 1] != 0) return error.InvalidDictionary;
            const n = try input.byte();
            const triplets = try input.take(@as(usize, n) * 3);
            var params = false;
            for (0..n) |i| {
                const t = triplets[i * 3 ..][0..3];
                const op = t[if (format == .@"brotli-google") @as(usize, 1) else 2];
                if (t[0] >= count or t[if (format == .@"brotli-google") @as(usize, 2) else 1] >= count or op > 22) return error.InvalidDictionary;
                params = params or op == 21 or op == 22;
            }
            list.* = .{ .strings = strings, .offsets = try allocator.dupe(u16, offsets[0..count]), .triplets = triplets, .google_order = format == .@"brotli-google", .parameters = if (params) try input.take(@as(usize, n) * 2) else &.{} };
            if (params) for (0..n) |i| {
                const transform = try list.get(i);
                if (transform.operation < 21 and transform.parameter != 0) return error.InvalidDictionary;
            };
        }
        if (nw != 0 or nt != 0) {
            self.pair_count = try input.byte();
            if (self.pair_count == 0 or self.pair_count > 64) return error.InvalidDictionary;
            for (self.pairs[0..self.pair_count]) |*pair| {
                pair.* = .{ .words = try input.byte(), .transforms = try input.byte() };
                if (pair.words > nw or pair.transforms > nt) return error.InvalidDictionary;
            }
            const contextual = try input.byte();
            if (contextual > 1) return error.InvalidDictionary;
            self.contextual = contextual == 1;
            if (self.contextual) {
                @memcpy(&self.contexts, try input.take(64));
                for (self.contexts) |index| if (index >= self.pair_count) return error.InvalidDictionary;
            }
        }
        if (input.at != bytes.len) return error.InvalidDictionary;
        return self;
    }
    pub fn deinit(self: *Dictionary) void {
        self.arena.deinit();
    }
    pub fn resolve(self: *const Dictionary, length: usize, id: usize, context: u6, builtin_words: *const WordList, builtin_transforms: *const TransformList) Error!Resolved {
        if (length < 4 or length > 31) return error.InvalidDictionaryReference;
        var remaining = id;
        const front = self.contexts[context];
        for (0..self.pair_count) |rank| {
            const index = if (!self.contextual) rank else if (rank == 0) front else if (rank <= front) rank - 1 else rank;
            const pair = self.pairs[index];
            const words = if (pair.words == self.words.len) builtin_words else &self.words[pair.words];
            const transforms = if (pair.transforms == self.transforms.len) builtin_transforms else &self.transforms[pair.transforms];
            const bits = words.bits[length];
            if (bits == 0) continue;
            const nwords = @as(usize, 1) << @as(u5, @intCast(bits));
            const total = nwords * transforms.count();
            if (remaining >= total) {
                remaining -= total;
                continue;
            }
            const at = words.offsets[length] + (remaining % nwords) * length;
            return .{ .word = words.data[at..][0..length], .transform = try transforms.get(remaining / nwords) };
        }
        return error.InvalidDictionaryReference;
    }
};
const Input = struct {
    bytes: []const u8,
    at: usize = 0,
    fn take(self: *Input, count: usize) Error![]const u8 {
        if (count > self.bytes.len - self.at) return error.InvalidDictionary;
        const result = self.bytes[self.at..][0..count];
        self.at += count;
        return result;
    }
    fn byte(self: *Input) Error!u8 {
        return (try self.take(1))[0];
    }
    fn varint(self: *Input) Error!usize {
        var result: u64 = 0;
        for (0..9) |i| {
            const b = try self.byte();
            result |= @as(u64, b & 127) << @as(u6, @intCast(i * 7));
            if (b & 128 == 0) return std.math.cast(usize, result) orelse error.InvalidDictionary;
        }
        return error.InvalidDictionary;
    }
};

test "serialized dictionary validates bounds and preserves raw zero-copy data" {
    const a = std.testing.allocator;
    var raw = try Dictionary.init(a, "prefix", .auto);
    defer raw.deinit();
    try std.testing.expectEqualStrings("prefix", raw.lz77);
    const bytes = [_]u8{ 0x91, 0, 3, 'a', 'b', 'c', 0, 0 };
    var d = try Dictionary.init(a, &bytes, .auto);
    defer d.deinit();
    try std.testing.expectEqualStrings("abc", d.lz77);
    for (0..bytes.len) |n| try std.testing.expectError(error.InvalidDictionary, Dictionary.init(a, bytes[0..n], .brotli));
    try std.testing.expectError(error.InvalidDictionary, Dictionary.init(a, &.{ 0x91, 0, 0, 65 }, .auto));
}
test "transforms preserve arbitrary scalar high bits and complete prefixes" {
    var out: [541]u8 = undefined;
    const upper = Transform{ .prefix = "<", .suffix = ">", .operation = 11 };
    try std.testing.expectEqualStrings("<ÉABC>", try upper.apply("éabc", &out));
    const plus = Transform{ .prefix = "", .suffix = "", .operation = 22, .parameter = 1 };
    try std.testing.expectEqualStrings("bê", try plus.apply("aé", &out));
    const minus = Transform{ .prefix = "", .suffix = "", .operation = 21, .parameter = 0xffff };
    try std.testing.expectEqualStrings("`é", try minus.apply("aé", &out));
}

test "all transform operations agree with independent reference fixtures" {
    var output: [541]u8 = undefined;
    for (@import("transform_fixtures.zig").cases) |case| {
        const transform = Transform{ .prefix = "<", .suffix = ">", .operation = case.operation, .parameter = case.parameter };
        try std.testing.expectEqualSlices(u8, case.expected, try transform.apply(case.word, &output));
    }
}

test "dictionary maximum transform and all allocation failures" {
    const prefix: [255]u8 = @splat('<');
    const suffix: [255]u8 = @splat('>');
    const word: [31]u8 = @splat('a');
    const transform = Transform{ .prefix = &prefix, .suffix = &suffix, .operation = 11 };
    var output: [541]u8 = undefined;
    const result = try transform.apply(&word, &output);
    try std.testing.expectEqual(@as(usize, 541), result.len);
    try std.testing.expectEqual(@as(u8, 'A'), result[255]);
    try std.testing.expectEqual(@as(u8, '>'), result[540]);
    try std.testing.expectError(error.DictionaryOutputTooSmall, transform.apply(&word, output[0..540]));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}
fn allocationCase(a: A) !void {
    // One length-4 list, two words; one identity transform; one pair.
    const bytes = [_]u8{ 0x91, 0, 0, 1, 1 } ++ @as([27]u8, @splat(0)) ++ "wordtest".* ++ [_]u8{ 1, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0 };
    var dictionary = try Dictionary.init(a, &bytes, .brotli);
    defer dictionary.deinit();
    const resolved = try dictionary.resolve(4, 1, 0, undefined, undefined);
    try std.testing.expectEqualStrings("test", resolved.word);
    for (0..bytes.len) |size| try std.testing.expectError(error.InvalidDictionary, Dictionary.init(std.testing.allocator, bytes[0..size], .brotli));
}
