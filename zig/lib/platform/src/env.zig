// Copyright 2026 Antfly, Inc.
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
const builtin = @import("builtin");

pub fn getenvZ(name: [*:0]const u8) ?[*:0]u8 {
    if (comptime !builtin.link_libc) return null;
    return std.c.getenv(name);
}

pub fn getenv(name: [*:0]const u8) ?[]const u8 {
    const value = getenvZ(name) orelse return null;
    return std.mem.span(value);
}

pub fn getenvSlice(name: [:0]const u8) ?[]const u8 {
    return getenv(name.ptr);
}

pub fn getenvBool(name: [*:0]const u8) bool {
    const value = getenv(name) orelse return false;
    return truthy(value);
}

pub fn getenvBoolDefault(name: [*:0]const u8, default: bool) bool {
    const value = getenv(name) orelse return default;
    if (value.len == 0) return false;
    if (std.mem.eql(u8, value, "0")) return false;
    if (std.ascii.eqlIgnoreCase(value, "false")) return false;
    if (std.ascii.eqlIgnoreCase(value, "no")) return false;
    if (std.ascii.eqlIgnoreCase(value, "off")) return false;
    return true;
}

pub fn getenvUsize(name: [*:0]const u8) ?usize {
    const value = getenv(name) orelse return null;
    if (value.len == 0) return null;
    return std.fmt.parseUnsigned(usize, value, 10) catch null;
}

pub fn truthy(value: []const u8) bool {
    return std.mem.eql(u8, value, "1") or
        std.ascii.eqlIgnoreCase(value, "true") or
        std.ascii.eqlIgnoreCase(value, "yes") or
        std.ascii.eqlIgnoreCase(value, "on");
}
