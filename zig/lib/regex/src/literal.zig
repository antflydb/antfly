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

//! Allocation-free literal matching, with SIMD ASCII candidates and Unicode folding.
const std = @import("std");
const unicode = @import("unicode.zig");
pub const Span = struct { start: usize, end: usize };
pub fn isAscii(bytes: []const u8) bool {
    return asciiPrefix(bytes) == bytes.len;
}
fn asciiPrefix(bytes: []const u8) usize {
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    var at: usize = 0;
    while (bytes.len - at >= width) : (at += width) {
        const chunk: @Vector(width, u8) = bytes[at..][0..width].*;
        const high: @Int(.unsigned, width) = @bitCast(chunk >= @as(@Vector(width, u8), @splat(128)));
        if (high != 0) return at + @ctz(high);
    }
    for (bytes[at..], at..) |byte, i| if (byte >= 128) return i;
    return bytes.len;
}
fn lower(comptime width: usize, bytes: @Vector(width, u8)) @Vector(width, u8) {
    const upper = (bytes >= @as(@Vector(width, u8), @splat('A'))) & (bytes <= @as(@Vector(width, u8), @splat('Z')));
    return bytes | @select(u8, upper, @as(@Vector(width, u8), @splat(32)), @as(@Vector(width, u8), @splat(0)));
}
/// Count a one-byte literal without producing spans. UTF folding supplements
/// ASCII lanes with exact searches for non-ASCII equivalents from our tables.
pub fn countByte(text: []const u8, needle: u8, insensitive: bool, utf8: bool) usize {
    std.debug.assert(!utf8 or needle < 128);
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    const wanted = if (insensitive) std.ascii.toLower(needle) else needle;
    const target: @Vector(width, u8) = @splat(wanted);
    var at: usize = 0;
    var count: usize = 0;
    while (text.len - at >= width) : (at += width) {
        const chunk: @Vector(width, u8) = text[at..][0..width].*;
        const hits: @Int(.unsigned, width) = @bitCast((if (insensitive) lower(width, chunk) else chunk) == target);
        count += @popCount(hits);
    }
    for (text[at..]) |byte| if ((if (insensitive) std.ascii.toLower(byte) else byte) == wanted) {
        count += 1;
    };
    if (insensitive and utf8) {
        var variants: [5]u32 = undefined;
        for (unicode.equivalents(needle, &variants)) |cp| {
            if (cp < 128) continue;
            var buffer: [4]u8 = undefined;
            const len = std.unicode.utf8Encode(@intCast(cp), &buffer) catch unreachable;
            var offset: usize = 0;
            while (findExact(text, buffer[0..len], offset)) |start| {
                count += 1;
                offset = start + len;
            }
        }
    }
    return count;
}
pub fn findAsciiFolded(text: []const u8, needle: []const u8, offset: usize) ?usize {
    return findSimd(text, needle, offset, true);
}
pub fn findExact(text: []const u8, needle: []const u8, offset: usize) ?usize {
    return findSimd(text, needle, offset, false);
}
/// Number of identical leading bytes, bounded by the supplied slice. Loads stay
/// inside the input; the scalar tail also handles short or unaligned records.
pub fn equalPrefix(text: []const u8, byte: u8) usize {
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    const target: @Vector(width, u8) = @splat(byte);
    var at: usize = 0;
    while (text.len - at >= width) : (at += width) {
        const chunk: @Vector(width, u8) = text[at..][0..width].*;
        const different = chunk != target;
        if (@reduce(.Or, different)) return at + @ctz(@as(@Int(.unsigned, width), @bitCast(different)));
    }
    while (at < text.len and text[at] == byte) : (at += 1) {}
    return at;
}
fn folded(comptime insensitive: bool, byte: u8) u8 {
    return if (insensitive) std.ascii.toLower(byte) else byte;
}
fn equal(comptime insensitive: bool, left: []const u8, right: []const u8) bool {
    return if (insensitive) std.ascii.eqlIgnoreCase(left, right) else std.mem.eql(u8, left, right);
}
fn findSimd(text: []const u8, needle: []const u8, offset: usize, comptime insensitive: bool) ?usize {
    if (offset > text.len or needle.len > text.len - offset) return null;
    if (needle.len == 0) return offset;
    if (!insensitive and needle.len == 1) return std.mem.indexOfScalarPos(u8, text, offset, needle[0]);
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    const first: @Vector(width, u8) = @splat(folded(insensitive, needle[0]));
    const last: @Vector(width, u8) = @splat(folded(insensitive, needle[needle.len - 1]));
    const end = text.len - needle.len + 1;
    var at = offset;
    var work: usize = 0;
    const budget = @max(256, (text.len - offset) *| 2);
    // On NEON, reducing a vector mask is more expensive than loading the
    // next vector. Amortize the reduction over four independent chunks while
    // retaining candidate order and the linear fallback's work budget.
    inline for (.{ 4, 1 }) |lanes| {
        while (end - at >= width * lanes) : (at += width * lanes) {
            var candidates: [lanes]@Vector(width, bool) = undefined;
            var any: @Vector(width, bool) = @splat(false);
            inline for (0..lanes) |lane| {
                const begin = at + lane * width;
                const front: @Vector(width, u8) = text[begin..][0..width].*;
                const back: @Vector(width, u8) = text[begin + needle.len - 1 ..][0..width].*;
                candidates[lane] = ((if (insensitive) lower(width, front) else front) == first) & ((if (insensitive) lower(width, back) else back) == last);
                any |= candidates[lane];
            }
            if (!@reduce(.Or, any)) continue;
            inline for (0..lanes) |lane| {
                var mask: @Int(.unsigned, width) = @bitCast(candidates[lane]);
                while (mask != 0) {
                    const candidate = at + lane * width + @ctz(mask);
                    if (equal(insensitive, text[candidate..][0..needle.len], needle)) return candidate;
                    work +|= needle.len;
                    if (work >= budget) return twoWay(text, needle, candidate + 1, insensitive);
                    mask &= mask - 1;
                }
            }
        }
    }
    while (at < end) : (at += 1) if (equal(insensitive, text[at..][0..needle.len], needle)) return at;
    return null;
}
/// A conservative fused literal/byte prefilter. False proves that neither
/// needle nor the sentinel occurs; true may be a first/last-byte false positive.
/// The sentinel check covers the literal's trailing overhang as well.
pub fn mayContainOrByte(text: []const u8, needle: []const u8, other: u8) bool {
    if (needle.len == 0) return true;
    if (needle.len > text.len) return std.mem.indexOfScalar(u8, text, other) != null;
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    const first: @Vector(width, u8) = @splat(needle[0]);
    const last: @Vector(width, u8) = @splat(needle[needle.len - 1]);
    const sentinel: @Vector(width, u8) = @splat(other);
    const end = text.len - needle.len + 1;
    var at: usize = 0;
    inline for (.{ 4, 1 }) |lanes| {
        while (end - at >= width * lanes) : (at += width * lanes) {
            var any: @Vector(width, bool) = @splat(false);
            inline for (0..lanes) |lane| {
                const begin = at + lane * width;
                const front: @Vector(width, u8) = text[begin..][0..width].*;
                const back: @Vector(width, u8) = text[begin + needle.len - 1 ..][0..width].*;
                any |= ((front == first) & (back == last)) | (front == sentinel);
            }
            if (@reduce(.Or, any)) return true;
        }
    }
    while (at < end) : (at += 1) {
        if (text[at] == other or (text[at] == needle[0] and text[at + needle.len - 1] == needle[needle.len - 1])) return true;
    }
    return std.mem.indexOfScalar(u8, text[at..], other) != null;
}

const Factor = struct { cut: usize, period: usize };
fn maximalSuffix(needle: []const u8, comptime insensitive: bool, comptime reverse: bool) Factor {
    var left: usize = 0; // maximal-suffix index plus one, avoiding a -1 sentinel
    var right: usize = 0;
    var matched: usize = 1;
    var period: usize = 1;
    while (right + matched < needle.len) {
        const a = folded(insensitive, needle[right + matched]);
        const b = folded(insensitive, needle[left + matched - 1]);
        if (if (reverse) a > b else a < b) {
            right += matched;
            matched = 1;
            period = right + 1 - left;
        } else if (a == b) {
            if (matched == period) {
                right += period;
                matched = 1;
            } else matched += 1;
        } else {
            left = right + 1;
            right += 1;
            matched = 1;
            period = 1;
        }
    }
    return .{ .cut = left, .period = period };
}
/// Constant-space linear fallback after a bounded amount of SIMD candidate
/// verification. Based on Crochemore/Perrin critical factorization (JACM 1991),
/// https://monge.univ-mlv.fr/~mac/Articles-PDF/CP-1991-jacm.pdf .
fn twoWay(text: []const u8, needle: []const u8, offset: usize, comptime insensitive: bool) ?usize {
    if (offset > text.len or needle.len > text.len - offset) return null;
    if (needle.len == 0) return offset;
    const forward = maximalSuffix(needle, insensitive, false);
    const reverse = maximalSuffix(needle, insensitive, true);
    const factor = if (forward.cut > reverse.cut) forward else reverse;
    const cut = factor.cut;
    const periodic = factor.period <= needle.len - cut and equal(insensitive, needle[0..cut], needle[factor.period..][0..cut]);
    const period = if (periodic) factor.period else @max(cut -| 1, needle.len - cut) + 1;
    var memory: usize = 0;
    var at = offset;
    while (at <= text.len - needle.len) {
        var i = @max(cut, memory);
        while (i < needle.len and folded(insensitive, needle[i]) == folded(insensitive, text[at + i])) i += 1;
        if (i < needle.len) {
            at += i - cut + 1;
            memory = 0;
            continue;
        }
        i = cut;
        while (i > memory and folded(insensitive, needle[i - 1]) == folded(insensitive, text[at + i - 1])) i -= 1;
        if (i <= memory) return at;
        at += period;
        memory = if (periodic) needle.len - period else 0;
    }
    return null;
}
pub fn find(text: []const u8, needle: []const u8, offset: usize, insensitive: bool, utf8: bool, word: bool, whole: bool) ?Span {
    if (offset > text.len or (whole and offset != 0)) return null;
    // Inspect only the prefix through the next candidate. Inspecting the
    // entire haystack for every -o match makes dense ASCII lines quadratic.
    const ascii_needle = insensitive and utf8 and isAscii(needle);
    var candidate: ?usize = null;
    var scanned = false;
    var at = offset;
    while (at <= text.len) {
        if (ascii_needle and (!scanned or (if (candidate) |start| at > start else false))) {
            candidate = findAsciiFolded(text, needle, at);
            scanned = true;
        }
        const checked_end = if (candidate) |start| start + needle.len else text.len;
        const ascii_end = if (ascii_needle) at + asciiPrefix(text[at..checked_end]) else at;
        const ascii_fold = !insensitive or !utf8 or (ascii_needle and ascii_end == checked_end);
        var span: Span = undefined;
        if (!insensitive or ascii_fold) {
            const found = (if (insensitive) (if (utf8) candidate else findAsciiFolded(text, needle, at)) else findExact(text, needle, at)) orelse return null;
            span = .{ .start = found, .end = found + needle.len };
        } else {
            // An earlier Unicode-folded match must overlap the first non-ASCII
            // byte; no ASCII candidate exists in the preceding prefix. Retain
            // the cached candidate while checking that boundary, rather than
            // rescanning a distant ASCII suffix for each Unicode codepoint.
            if (ascii_needle) at = @max(at, ascii_end -| (needle.len -| 1));
            var left: usize = 0;
            var right = at;
            while (left < needle.len and right < text.len) {
                const x = unicode.decode(needle, left, true);
                const y = unicode.decode(text, right, true);
                if (unicode.fold(x.cp) != unicode.fold(y.cp)) break;
                left += x.len;
                right += y.len;
            }
            if (left != needle.len) {
                if (at == text.len) return null;
                at += unicode.decode(text, at, true).len;
                continue;
            }
            span = .{ .start = at, .end = right };
        }
        if (whole) return if (span.start == 0 and span.end == text.len) span else null;
        if (!word or (!isWord(unicode.previous(text, span.start, utf8), utf8) and !isWord(if (span.end < text.len) unicode.decode(text, span.end, utf8).cp else 0x110000, utf8))) return span;
        if (span.start == text.len) return null;
        at = span.start + 1;
    }
    return null;
}
fn isWord(cp: u32, utf8: bool) bool {
    return if (utf8) unicode.isWord(cp) else cp < 128 and (std.ascii.isAlphanumeric(@intCast(cp)) or cp == '_');
}
test "SIMD candidates agree with scalar ASCII search including vector tails" {
    const a = std.testing.allocator;
    for (0..100) |pad| {
        const text = try a.alloc(u8, pad + 40);
        defer a.free(text);
        @memset(text, 'x');
        @memcpy(text[pad..][0..7], "ReQuEsT");
        for (0..text.len + 1) |at| try std.testing.expectEqual(std.ascii.findIgnoreCasePos(text, at, "request"), findAsciiFolded(text, "request", at));
    }
}
test "Unicode literal folding can change encoded lengths" {
    try std.testing.expectEqual(Span{ .start = 0, .end = 3 }, find("K", "k", 0, true, true, false, true).?);
    try std.testing.expectEqual(Span{ .start = 0, .end = 2 }, find("ſ", "S", 0, true, true, false, true).?);
    try std.testing.expect(find("K", "k", 0, true, false, false, false) == null);
    try std.testing.expectEqual(Span{ .start = 3, .end = 6 }, find("kk K", "k", 0, true, true, true, false).?);
}

test "SIMD and critical-factorization fallback agree with scalar search" {
    var state: u64 = 0x976f40b3;
    var text: [160]u8 = undefined;
    var needle: [40]u8 = undefined;
    for (0..12000) |iteration| {
        for (&text) |*byte| {
            state = state *% 6364136223846793005 +% 1;
            byte.* = "abABcC"[(state >> 32) % 6];
        }
        for (&needle) |*byte| {
            state = state *% 6364136223846793005 +% 1;
            byte.* = "abABcC"[(state >> 32) % 6];
        }
        const text_len = iteration % (text.len + 1);
        const needle_len = iteration % (needle.len + 1);
        const offset = (iteration / 5) % (text_len + 1);
        const subject = text[0..text_len];
        const pattern = needle[0..needle_len];
        try std.testing.expectEqual(std.mem.indexOfPos(u8, subject, offset, pattern), findExact(subject, pattern, offset));
        try std.testing.expectEqual(std.mem.indexOfPos(u8, subject, offset, pattern), twoWay(subject, pattern, offset, false));
        try std.testing.expectEqual(std.ascii.findIgnoreCasePos(subject, offset, pattern), findAsciiFolded(subject, pattern, offset));
        try std.testing.expectEqual(std.ascii.findIgnoreCasePos(subject, offset, pattern), twoWay(subject, pattern, offset, true));
    }
    const a = std.testing.allocator;
    const long = try a.alloc(u8, 65536);
    defer a.free(long);
    @memset(long, 'a');
    var repetitive: [1024]u8 = @splat('a');
    repetitive[512] = 'b';
    try std.testing.expect(findExact(long, &repetitive, 0) == null);
    @memcpy(long[60000..][0..repetitive.len], &repetitive);
    try std.testing.expectEqual(@as(?usize, 60000), findExact(long, &repetitive, 0));
}

test "bounded ASCII proofs preserve mixed Unicode folding and word boundaries" {
    const pieces = [_][]const u8{ "a", "A", "k", "K", "K", "s", "S", "ſ", " ", "_", "x", "Σ", "é" };
    var random: u64 = 0x39513a;
    var bytes: [256]u8 = undefined;
    var needle: [8]u8 = undefined;
    for (0..4000) |_| {
        var len: usize = 0;
        for (0..50) |_| {
            random = random *% 6364136223846793005 +% 1;
            const piece = pieces[(random >> 32) % pieces.len];
            @memcpy(bytes[len..][0..piece.len], piece);
            len += piece.len;
        }
        random = random *% 6364136223846793005 +% 1;
        const count = (random >> 32) % needle.len;
        for (needle[0..count]) |*byte| {
            random = random *% 6364136223846793005 +% 1;
            byte.* = "akKsS _x"[(random >> 32) % 8];
        }
        const text = bytes[0..len];
        const n = needle[0..count];
        const offset = (random >> 32) % (len + 1);
        for ([_]bool{ false, true }) |word| {
            var expected: ?Span = null;
            var at = offset;
            while (at <= len) {
                var left: usize = 0;
                var right = at;
                while (left < n.len and right < text.len) {
                    const x = unicode.decode(n, left, true);
                    const y = unicode.decode(text, right, true);
                    if (unicode.fold(x.cp) != unicode.fold(y.cp)) break;
                    left += x.len;
                    right += y.len;
                }
                if (left == n.len and (!word or (!isWord(unicode.previous(text, at, true), true) and !isWord(if (right < text.len) unicode.decode(text, right, true).cp else 0x110000, true)))) {
                    expected = .{ .start = at, .end = right };
                    break;
                }
                if (at == len) break;
                at += if (left == n.len) 1 else unicode.decode(text, at, true).len;
            }
            try std.testing.expectEqualDeep(expected, find(text, n, offset, true, true, word, false));
        }
    }
}

test "SIMD literal counts include Unicode equivalents and unaligned tails" {
    const text = "kk K K sSſé";
    try std.testing.expectEqual(@as(usize, 4), countByte(text, 'k', true, true));
    try std.testing.expectEqual(@as(usize, 3), countByte(text, 'S', true, true));
    try std.testing.expectEqual(@as(usize, 3), countByte(text, 'k', true, false));
    try std.testing.expectEqual(@as(usize, 2), countByte(text, 'k', false, true));
    var bytes: [1024]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = if (i % 3 == 0) 'a' else 'A';
    for (0..65) |offset| {
        try std.testing.expectEqual(bytes.len - offset, countByte(bytes[offset..], 'a', true, true));
        var count: usize = 0;
        for (bytes[offset..]) |byte| if (byte == 'a') {
            count += 1;
        };
        try std.testing.expectEqual(count, countByte(bytes[offset..], 'a', false, true));
    }
}

test "fused literal and binary proofs cover vector and literal tails" {
    var bytes: [160]u8 = undefined;
    const needle: [200]u8 = @splat('a');
    for (0..bytes.len + 1) |len| for ([_]usize{ 0, 1, 7, 40, 200 }) |size| {
        const text = bytes[0..len];
        @memset(text, 'x');
        try std.testing.expectEqual(size == 0, mayContainOrByte(text, needle[0..size], 0));
        for (0..len) |at| {
            text[at] = 0;
            try std.testing.expect(mayContainOrByte(text, needle[0..size], 0));
            text[at] = 'x';
        }
        if (size <= len) {
            @memcpy(text[len - size ..], needle[0..size]);
            try std.testing.expect(mayContainOrByte(text, needle[0..size], 0));
        }
    };
}
