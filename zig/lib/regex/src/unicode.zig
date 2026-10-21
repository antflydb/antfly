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
const data = @import("unicode_data.zig");
pub const Range = data.Range;
pub fn property(raw: []const u8) ?[]const Range {
    var name: [128]u8 = undefined;
    var n: usize = 0;
    var value = raw;
    if (std.mem.indexOfAny(u8, raw, "=:")) |i| {
        var kind: [32]u8 = undefined;
        var len: usize = 0;
        for (raw[0..i]) |ch| {
            if (ch == '_' or ch == '-' or ch == ' ') continue;
            if (len == kind.len) return null;
            kind[len] = std.ascii.toLower(ch);
            len += 1;
        }
        const namespaces = [_]struct { short: []const u8, long: []const u8 }{
            .{ .short = "gc", .long = "generalcategory" },
            .{ .short = "sc", .long = "script" },
            .{ .short = "scx", .long = "scriptextensions" },
            .{ .short = "age", .long = "age" },
            .{ .short = "gcb", .long = "graphemeclusterbreak" },
            .{ .short = "wb", .long = "wordbreak" },
            .{ .short = "sb", .long = "sentencebreak" },
        };
        var valid = false;
        for (namespaces) |namespace| if (std.mem.eql(u8, kind[0..len], namespace.short) or std.mem.eql(u8, kind[0..len], namespace.long)) {
            @memcpy(name[0..namespace.short.len], namespace.short);
            n = namespace.short.len;
            name[n] = ':';
            n += 1;
            valid = true;
            break;
        };
        if (!valid) return null;
        value = raw[i + 1 ..];
    }
    for (value) |c| {
        if (c == '_' or c == '-' or c == ' ') continue;
        if (n == name.len) return null;
        name[n] = std.ascii.toLower(c);
        n += 1;
    }
    var lo: usize = 0;
    var hi: usize = data.properties.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, name[0..n], data.properties[mid].name)) {
            .eq => return data.properties[mid].ranges,
            .lt => hi = mid,
            .gt => lo = mid + 1,
        }
    }
    return null;
}
pub fn contains(ranges: []const Range, cp: u32) bool {
    var lo: usize = 0;
    var hi = ranges.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (cp < ranges[mid].lo) hi = mid else if (cp > ranges[mid].hi) lo = mid + 1 else return true;
    }
    return false;
}
pub fn fold(cp: u32) u32 {
    var lo: usize = 0;
    var hi: usize = data.folds.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (cp < data.folds[mid].from) hi = mid else if (cp > data.folds[mid].from) lo = mid + 1 else return data.folds[mid].to;
    }
    return cp;
}
pub fn equivalents(cp: u32, out: *[5]u32) []const u32 {
    const canonical = fold(cp);
    out[0] = canonical;
    var count: usize = 1;
    var lo: usize = 0;
    var hi: usize = data.reverse_folds.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (data.reverse_folds[mid].from < canonical) lo = mid + 1 else hi = mid;
    }
    while (lo < data.reverse_folds.len and data.reverse_folds[lo].from == canonical) : (lo += 1) {
        std.debug.assert(count < out.len);
        out[count] = data.reverse_folds[lo].to;
        count += 1;
    }
    return out[0..count];
}
pub fn isWord(cp: u32) bool {
    return contains(property("word").?, cp);
}
pub const Decoded = struct { cp: u32, len: usize };
pub fn decode(text: []const u8, pos: usize, utf8: bool) Decoded {
    if (!utf8 or text[pos] < 128) return .{ .cp = text[pos], .len = 1 };
    const len = std.unicode.utf8ByteSequenceLength(text[pos]) catch return .{ .cp = 0x110000, .len = 1 };
    if (len > text.len - pos) return .{ .cp = 0x110000, .len = 1 };
    const cp = std.unicode.utf8Decode(text[pos..][0..len]) catch return .{ .cp = 0x110000, .len = 1 };
    return .{ .cp = cp, .len = len };
}
pub fn previous(text: []const u8, pos: usize, utf8: bool) u32 {
    if (pos == 0) return 0x110000;
    if (!utf8) return text[pos - 1];
    var start = pos - 1;
    while (start > 0 and pos - start < 4 and text[start] & 0xc0 == 0x80) start -= 1;
    const d = decode(text, start, true);
    return if (start + d.len == pos) d.cp else 0x110000;
}

const pcre_word_sets = [_][]const Range{ property("L").?, property("N").?, property("Mn").?, property("Pc").? };
pub fn pcreWord(cp: u32) bool {
    if (cp < 128) return std.ascii.isAlphanumeric(@intCast(cp)) or cp == '_';
    for (pcre_word_sets) |ranges| if (contains(ranges, cp)) return true;
    return false;
}

/// UTS #39 script intersection, including East Asian virtual scripts and one
/// decimal-digit set. Tables are compact interval/palette data shared by workers.
const script_digits = property("Nd").?;
pub fn scriptRun(text: []const u8, utf8: bool) bool {
    if (text.len == 0) return true;
    if (decode(text, 0, utf8).len == text.len) return true;
    const scripts = @import("script_runs_data.zig");
    var mask: [4]u64 = @splat(std.math.maxInt(u64));
    var digit_zero: ?u32 = null;
    var at: usize = 0;
    while (at < text.len) {
        const decoded = decode(text, at, utf8);
        const cp = decoded.cp;
        var index: usize = 1;
        if (cp < 128) {
            if (std.ascii.isAlphabetic(@intCast(cp))) index = scripts.latin;
        } else {
            var lo: usize = 0;
            var hi = scripts.intervals.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const interval = scripts.intervals[mid];
                if (cp < interval.lo) hi = mid else if (cp > interval.hi) lo = mid + 1 else {
                    index = interval.mask;
                    break;
                }
            }
        }
        var any = false;
        for (&mask, scripts.masks[index]) |*word, bits| {
            word.* &= bits;
            any = any or word.* != 0;
        }
        if (!any) return false;
        if (cp < 128) {
            if (std.ascii.isDigit(@intCast(cp))) {
                if (digit_zero) |prior_zero| {
                    if (prior_zero != '0') return false;
                } else digit_zero = '0';
            }
            at += decoded.len;
            continue;
        }
        const decimal = script_digits;
        var lo: usize = 0;
        var hi = decimal.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const range = decimal[mid];
            if (cp < range.lo) hi = mid else if (cp > range.hi) lo = mid + 1 else {
                const zero = cp - (cp - range.lo) % 10;
                if (digit_zero) |prior_zero| {
                    if (zero != prior_zero) return false;
                } else digit_zero = zero;
                break;
            }
        }
        at += decoded.len;
    }
    return true;
}
