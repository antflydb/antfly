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

//! A wrapper over a byte-slice, providing useful methods for parsing string floating point values.

const std = @import("std");
const FloatStream = @This();
const common = @import("common.zig");
const control = @import("control.zig");

slice: []const u8,
offset: usize,
underscore_count: usize,
work: ?*control.Work = null,

pub fn init(s: []const u8) FloatStream {
    return initControlled(s, null);
}
pub fn initControlled(s: []const u8, work: ?*control.Work) FloatStream {
    return .{ .slice = s, .offset = 0, .underscore_count = 0, .work = work };
}

// Returns the offset from the start *excluding* any underscores that were found.
pub fn offsetTrue(self: FloatStream) usize {
    return self.offset - self.underscore_count;
}

pub fn reset(self: *FloatStream) void {
    self.offset = 0;
    self.underscore_count = 0;
}

pub fn len(self: FloatStream) usize {
    if (self.offset > self.slice.len) {
        return 0;
    }
    return self.slice.len - self.offset;
}

pub fn hasLen(self: FloatStream, n: usize) bool {
    return !control.failed(self.work) and self.offset + n <= self.slice.len;
}

pub fn firstUnchecked(self: FloatStream) u8 {
    return self.slice[self.offset];
}

pub fn first(self: FloatStream) ?u8 {
    return if (self.hasLen(1))
        return self.firstUnchecked()
    else
        null;
}

pub fn isEmpty(self: FloatStream) bool {
    return !self.hasLen(1);
}

pub fn firstIs(self: FloatStream, comptime cs: []const u8) bool {
    if (self.first()) |ok| {
        inline for (cs) |c| if (ok == c) return true;
    }
    return false;
}

pub fn firstIsLower(self: FloatStream, comptime cs: []const u8) bool {
    if (self.first()) |ok| {
        inline for (cs) |c| if (ok | 0x20 == c) return true;
    }
    return false;
}

pub fn firstIsDigit(self: FloatStream, comptime base: u8) bool {
    comptime std.debug.assert(base == 10 or base == 16);

    if (self.first()) |ok| {
        return common.isDigit(ok, base);
    }
    return false;
}

pub fn advance(self: *FloatStream, n: usize) void {
    self.offset += n;
    if (self.work) |work| work.advance(n);
}

pub fn skipChars(self: *FloatStream, comptime cs: []const u8) void {
    while (self.firstIs(cs)) : (self.advance(1)) {}
}

pub fn readU64Unchecked(self: FloatStream) u64 {
    return std.mem.readInt(u64, self.slice[self.offset..][0..8], .little);
}

pub fn readU64(self: FloatStream) ?u64 {
    if (self.hasLen(8)) {
        return self.readU64Unchecked();
    }
    return null;
}

pub fn atUnchecked(self: *FloatStream, i: usize) u8 {
    return self.slice[self.offset + i];
}

pub fn scanDigit(self: *FloatStream, comptime base: u8) ?u8 {
    comptime std.debug.assert(base == 10 or base == 16);

    retry: while (true) {
        if (self.first()) |ok| {
            if ('0' <= ok and ok <= '9') {
                self.advance(1);
                return ok - '0';
            } else if (base == 16 and 'a' <= ok and ok <= 'f') {
                self.advance(1);
                return ok - 'a' + 10;
            } else if (base == 16 and 'A' <= ok and ok <= 'F') {
                self.advance(1);
                return ok - 'A' + 10;
            } else if (ok == '_') {
                self.advance(1);
                self.underscore_count += 1;
                continue :retry;
            }
        }
        return null;
    }
}
