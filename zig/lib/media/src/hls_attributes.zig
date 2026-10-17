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

//! Bounded RFC 8216 attribute lists. Values borrow the manifest bytes.
const std = @import("std");
pub const Attribute = struct { name: []const u8, value: []const u8, quoted: bool };
pub const List = struct {
    items: [64]Attribute = undefined,
    len: usize = 0,
    pub fn get(self: *const List, name: []const u8) ?Attribute {
        for (self.items[0..self.len]) |item| if (std.mem.eql(u8, item.name, name)) return item;
        return null;
    }
    pub fn string(self: *const List, name: []const u8) !?[]const u8 {
        const item = self.get(name) orelse return null;
        if (!item.quoted or item.value.len == 0) return error.MalformedHls;
        return item.value;
    }
    pub fn integer(self: *const List, name: []const u8) !?u64 {
        const item = self.get(name) orelse return null;
        if (item.quoted) return error.MalformedHls;
        return try uint(item.value);
    }
    pub fn yesNo(self: *const List, name: []const u8) !?bool {
        const item = self.get(name) orelse return null;
        if (item.quoted) return error.MalformedHls;
        if (std.mem.eql(u8, item.value, "YES")) return true;
        if (std.mem.eql(u8, item.value, "NO")) return false;
        return error.MalformedHls;
    }
};
pub fn parse(bytes: []const u8) !List {
    return parseImpl(bytes, false);
}
pub fn parseSkip(bytes: []const u8) !List {
    return parseImpl(bytes, true);
}
fn parseImpl(bytes: []const u8, skip: bool) !List {
    var result = List{};
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        if (result.len == result.items.len) return error.ResourceLimitExceeded;
        const equal = std.mem.indexOfScalarPos(u8, bytes, cursor, '=') orelse return error.MalformedHls;
        const name = bytes[cursor..equal];
        if (name.len == 0) return error.MalformedHls;
        for (name) |c| if (!((c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9') or c == '-')) return error.MalformedHls;
        if (result.get(name) != null) return error.MalformedHls;
        cursor = equal + 1;
        if (cursor == bytes.len) return error.MalformedHls;
        const quoted = bytes[cursor] == '"';
        const value = if (quoted) blk: {
            cursor += 1;
            const end = std.mem.indexOfScalarPos(u8, bytes, cursor, '"') orelse return error.MalformedHls;
            const item = bytes[cursor..end];
            for (item) |c| if ((c < 0x20 and !(skip and c == 9 and std.mem.eql(u8, name, "RECENTLY-REMOVED-DATERANGES"))) or c == 0x7f) return error.MalformedHls;
            cursor = end + 1;
            break :blk item;
        } else blk: {
            const end = std.mem.indexOfScalarPos(u8, bytes, cursor, ',') orelse bytes.len;
            const item = bytes[cursor..end];
            cursor = end;
            if (item.len == 0) return error.MalformedHls;
            for (item) |c| if (c <= 0x20 or c == 0x7f or c == '"') return error.MalformedHls;
            break :blk item;
        };
        result.items[result.len] = .{ .name = name, .value = value, .quoted = quoted };
        result.len += 1;
        if (cursor < bytes.len) {
            if (bytes[cursor] != ',') return error.MalformedHls;
            cursor += 1;
            if (cursor == bytes.len) return error.MalformedHls;
        }
    }
    if (result.len == 0) return error.MalformedHls;
    return result;
}
pub fn uint(text: []const u8) !u64 {
    if (text.len == 0) return error.MalformedHls;
    for (text) |c| if (c < '0' or c > '9') return error.MalformedHls;
    return std.fmt.parseInt(u64, text, 10) catch error.MalformedHls;
}
pub fn decimal(text: []const u8) !f64 {
    var dots: usize = 0;
    var digits: usize = 0;
    for (text) |c| {
        if (c == '.') dots += 1 else if (c >= '0' and c <= '9') digits += 1 else return error.MalformedHls;
    }
    if (dots > 1 or digits == 0) return error.MalformedHls;
    const value = std.fmt.parseFloat(f64, text) catch return error.MalformedHls;
    if (!std.math.isFinite(value) or value <= 0) return error.MalformedHls;
    return value;
}
