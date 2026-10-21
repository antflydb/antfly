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

//! Native UTF and legacy text transcoding. Invalid sequences emit U+FFFD.
const std = @import("std");
const t = @import("tables.zig");
const A = std.mem.Allocator;
pub fn hasBom(bytes: []const u8) bool {
    return std.mem.startsWith(u8, bytes, "\xef\xbb\xbf") or std.mem.startsWith(u8, bytes, "\xff\xfe") or std.mem.startsWith(u8, bytes, "\xfe\xff") or std.mem.startsWith(u8, bytes, "\x00\x00\xfe\xff");
}
fn emit(out: *std.ArrayList(u8), a: A, scalar: u32) !void {
    const cp: u21 = @intCast(if (scalar > 0x10ffff or (scalar >= 0xd800 and scalar <= 0xdfff)) 0xfffd else scalar);
    var buf: [4]u8 = undefined;
    const n = std.unicode.utf8Encode(cp, &buf) catch unreachable;
    try out.appendSlice(a, buf[0..n]);
}
pub fn normalized(label: []const u8, buf: *[64]u8) ![]const u8 {
    var n: usize = 0;
    for (std.mem.trim(u8, label, " \t\r\n")) |ch| {
        if (ch == '-' or ch == '_' or ch == ' ') continue;
        if (n == buf.len) return error.UnknownEncoding;
        buf[n] = std.ascii.toLower(ch);
        n += 1;
    }
    for (t.aliases) |alias| if (std.mem.eql(u8, buf[0..n], alias.name)) return alias.canonical;
    return buf[0..n];
}
fn single(name: []const u8) ?[]const u21 {
    const entries = .{
        .{ "windows874", &t.cp874 },    .{ "tis620", &t.cp874 },        .{ "iso885911", &t.cp874 },
        .{ "windows1250", &t.cp1250 },  .{ "windows1251", &t.cp1251 },  .{ "windows1252", &t.cp1252 },
        .{ "latin1", &t.cp1252 },       .{ "iso88591", &t.cp1252 },     .{ "ascii", &t.cp1252 },
        .{ "usascii", &t.cp1252 },      .{ "windows1253", &t.cp1253 },  .{ "windows1254", &t.cp1254 },
        .{ "iso88599", &t.cp1254 },     .{ "windows1255", &t.cp1255 },  .{ "windows1256", &t.cp1256 },
        .{ "windows1257", &t.cp1257 },  .{ "windows1258", &t.cp1258 },  .{ "ibm866", &t.cp866 },
        .{ "cp866", &t.cp866 },         .{ "koi8r", &t.koi8r },         .{ "koi8u", &t.koi8u },
        .{ "macintosh", &t.macroman },  .{ "macroman", &t.macroman },   .{ "xmaccyrillic", &t.maccyrillic },
        .{ "iso88592", &t.iso88592 },   .{ "iso88593", &t.iso88593 },   .{ "iso88594", &t.iso88594 },
        .{ "iso88595", &t.iso88595 },   .{ "iso88596", &t.iso88596 },   .{ "iso88597", &t.iso88597 },
        .{ "iso88598", &t.iso88598 },   .{ "iso88598i", &t.iso88598 },  .{ "iso885910", &t.iso885910 },
        .{ "iso885913", &t.iso885913 }, .{ "iso885914", &t.iso885914 }, .{ "iso885915", &t.iso885915 },
        .{ "iso885916", &t.iso885916 },
    };
    inline for (entries) |entry| if (std.mem.eql(u8, name, entry[0])) return entry[1];
    if (std.mem.startsWith(u8, name, "cp125")) {
        inline for (0..9) |i| if (std.mem.eql(u8, name, std.fmt.comptimePrint("cp{d}", .{1250 + i}))) return @field(t, std.fmt.comptimePrint("cp{d}", .{1250 + i}))[0..];
    }
    return null;
}
pub fn validate(label: []const u8) !void {
    var buffer: [64]u8 = undefined;
    const name = try normalized(label, &buffer);
    for ([_][]const u8{ "auto", "none", "utf8", "utf16", "utf16le", "utf16be", "utf32le", "utf32be", "shiftjis", "sjis", "windows31j", "cp932", "big5", "big5hkscs", "gbk", "gb18030", "gb2312", "euckr", "cp949", "eucjp", "iso2022jp", "xuserdefined", "replacement" }) |known| if (std.mem.eql(u8, name, known)) return;
    if (single(name) != null) return;
    return error.UnknownEncoding;
}
pub fn decode(a: A, source: []const u8, label: []const u8) ![]u8 {
    return decodeWithBom(a, source, label, true);
}
pub fn decodeWithBom(a: A, source: []const u8, label: []const u8, detect_bom: bool) ![]u8 {
    var buffer: [64]u8 = undefined;
    var name = try normalized(label, &buffer);
    if (std.mem.eql(u8, name, "none")) return a.dupe(u8, source);
    try validate(label);
    var data = source;
    if (detect_bom and std.mem.startsWith(u8, data, "\x00\x00\xfe\xff")) {
        name = "utf32be";
        data = data[4..];
    } else if (detect_bom and std.mem.startsWith(u8, data, "\xff\xfe\x00\x00")) {
        name = "utf32le";
        data = data[4..];
    } else if (detect_bom and std.mem.startsWith(u8, data, "\xff\xfe")) {
        name = "utf16le";
        data = data[2..];
    } else if (detect_bom and std.mem.startsWith(u8, data, "\xfe\xff")) {
        name = "utf16be";
        data = data[2..];
    } else if (detect_bom and std.mem.startsWith(u8, data, "\xef\xbb\xbf")) {
        name = "utf8";
        data = data[3..];
    }
    if (std.mem.eql(u8, name, "auto")) return a.dupe(u8, data);
    if (std.mem.eql(u8, name, "utf8")) {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        var pos: usize = 0;
        while (pos < data.len) {
            if (data[pos] < 128) {
                try out.append(a, data[pos]);
                pos += 1;
                continue;
            }
            const n = std.unicode.utf8ByteSequenceLength(data[pos]) catch {
                try emit(&out, a, 0xfffd);
                pos += 1;
                continue;
            };
            var valid: usize = 1;
            while (valid < n and pos + valid < data.len) {
                const ch = data[pos + valid];
                if (ch < 0x80 or ch > 0xbf) break;
                if (valid == 1 and ((data[pos] == 0xe0 and ch < 0xa0) or (data[pos] == 0xed and ch > 0x9f) or (data[pos] == 0xf0 and ch < 0x90) or (data[pos] == 0xf4 and ch > 0x8f))) break;
                valid += 1;
            }
            if (valid == n) try out.appendSlice(a, data[pos..][0..n]) else try emit(&out, a, 0xfffd);
            pos += valid;
        }
        return out.toOwnedSlice(a);
    }
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var i: usize = 0;
    if (std.mem.startsWith(u8, name, "utf16")) {
        const endian: std.builtin.Endian = if (std.mem.endsWith(u8, name, "be")) .big else .little;
        while (i + 1 < data.len) {
            const first = std.mem.readInt(u16, data[i..][0..2], endian);
            i += 2;
            var cp: u32 = first;
            if (first >= 0xd800 and first <= 0xdbff) {
                cp = 0xfffd;
                if (i + 1 < data.len) {
                    const next = std.mem.readInt(u16, data[i..][0..2], endian);
                    if (next >= 0xdc00 and next <= 0xdfff) {
                        cp = 0x10000 + (@as(u32, first) - 0xd800) * 1024 + next - 0xdc00;
                        i += 2;
                    }
                }
            }
            try emit(&out, a, cp);
        }
        if (i < data.len) try emit(&out, a, 0xfffd);
    } else if (std.mem.startsWith(u8, name, "utf32")) {
        const endian: std.builtin.Endian = if (std.mem.endsWith(u8, name, "be")) .big else .little;
        while (i + 3 < data.len) : (i += 4) try emit(&out, a, std.mem.readInt(u32, data[i..][0..4], endian));
        if (i < data.len) try emit(&out, a, 0xfffd);
    } else if (single(name)) |table| {
        for (data) |ch| try emit(&out, a, if (ch < 128) ch else table[ch - 128]);
    } else if (std.mem.eql(u8, name, "xuserdefined")) {
        for (data) |ch| try emit(&out, a, if (ch < 128) ch else @as(u32, ch) + 0xf700);
    } else if (std.mem.eql(u8, name, "replacement")) {
        if (data.len > 0) try emit(&out, a, 0xfffd);
    } else {
        const sjis = std.mem.eql(u8, name, "shiftjis") or std.mem.eql(u8, name, "sjis") or std.mem.eql(u8, name, "windows31j") or std.mem.eql(u8, name, "cp932");
        const big5 = std.mem.startsWith(u8, name, "big5");
        const gb = std.mem.eql(u8, name, "gbk") or std.mem.eql(u8, name, "gb18030") or std.mem.eql(u8, name, "gb2312");
        const kr = std.mem.eql(u8, name, "euckr") or std.mem.eql(u8, name, "cp949");
        const iso = std.mem.eql(u8, name, "iso2022jp");
        var state: enum { ascii, roman, katakana, jis } = .ascii;
        while (i < data.len) {
            const ch = data[i];
            i += 1;
            if (iso and ch == 27) {
                if (i + 1 < data.len and data[i] == '(' and std.mem.indexOfScalar(u8, "BJI", data[i + 1]) != null) {
                    state = switch (data[i + 1]) {
                        'J' => .roman,
                        'I' => .katakana,
                        else => .ascii,
                    };
                    i += 2;
                    continue;
                }
                if (i + 1 < data.len and data[i] == '$' and (data[i + 1] == '@' or data[i + 1] == 'B')) {
                    state = .jis;
                    i += 2;
                    continue;
                }
                try emit(&out, a, 0xfffd);
                continue;
            }
            if (iso and state == .roman and (ch == 0x5c or ch == 0x7e)) {
                try emit(&out, a, if (ch == 0x5c) 0xa5 else 0x203e);
                continue;
            }
            if (iso and state == .katakana and ch >= 0x21 and ch <= 0x5f) {
                try emit(&out, a, @as(u32, ch) + 0xff40);
                continue;
            }
            if (ch < 128 and !(iso and state == .jis)) {
                try out.append(a, ch);
                continue;
            }
            if (sjis and ch == 0x80) {
                try emit(&out, a, 0x80);
                continue;
            }
            if (sjis and ch >= 0xa1 and ch <= 0xdf) {
                try emit(&out, a, @as(u32, ch) + 0xfec0);
                continue;
            }
            if (gb and ch == 128) {
                try emit(&out, a, 0x20ac);
                continue;
            }
            if (i == data.len) {
                try emit(&out, a, 0xfffd);
                break;
            }
            const next = data[i];
            if (big5 and ch >= 0x81 and ch <= 0xfe and (next >= 0x40 and next <= 0x7e or next >= 0xa1 and next <= 0xfe)) {
                const pointer = (@as(usize, ch) - 0x81) * 157 + next - @as(usize, if (next < 0x7f) 0x40 else 0x62);
                if (pointer == 1133 or pointer == 1135 or pointer == 1164 or pointer == 1166) {
                    try emit(&out, a, if (pointer < 1164) 0xca else 0xea);
                    try emit(&out, a, if (pointer == 1133 or pointer == 1164) 0x304 else 0x30c);
                    i += 1;
                    continue;
                }
            }

            var cp: u32 = 0xfffd;
            if (gb and std.mem.eql(u8, name, "gb18030") and ch >= 0x81 and ch <= 0xfe and next >= 0x30 and next <= 0x39 and i + 2 < data.len and data[i + 1] >= 0x81 and data[i + 1] <= 0xfe and data[i + 2] >= 0x30 and data[i + 2] <= 0x39) {
                const index = (((@as(u32, ch) - 0x81) * 10 + next - 0x30) * 126 + data[i + 1] - 0x81) * 10 + data[i + 2] - 0x30;
                for (t.gb18030_ranges) |r| if (index >= r.first and index <= r.last) {
                    cp = r.cp + index - r.first;
                    break;
                };
                i += 3;
            } else if (!sjis and !big5 and !gb and !kr and !iso and ch == 0x8f and i + 1 < data.len and next >= 0xa1 and next <= 0xfe and data[i + 1] >= 0xa1 and data[i + 1] <= 0xfe) {
                cp = t.jis212[(@as(usize, next) - 0xa1) * 94 + data[i + 1] - 0xa1];
                i += 2;
            } else {
                const index: usize = if (iso) (@as(usize, ch) + 128) * 256 + @as(usize, next) + 128 else @as(usize, ch) * 256 + next;
                if (iso and (ch < 0x21 or ch > 0x7e or next < 0x21 or next > 0x7e)) {
                    try emit(&out, a, 0xfffd);
                    continue;
                }
                cp = if (sjis) t.sjis[index] else if (big5) t.big5[index] else if (gb) t.gbk[index] else if (kr) t.euckr[index] else t.eucjp[index];
                if (cp != 0xfffd or next >= 128) i += 1;
            }
            try emit(&out, a, cp);
        }
    }
    return out.toOwnedSlice(a);
}
