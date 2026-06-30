// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub fn stringSlicesContains(values: []const []const u8, value: []const u8) bool {
    for (values) |candidate| {
        if (std.mem.eql(u8, candidate, value)) return true;
    }
    return false;
}

pub fn stringSlicesIntersect(a: []const []const u8, b: []const []const u8) bool {
    for (a) |value| {
        if (stringSlicesContains(b, value)) return true;
    }
    return false;
}

pub fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

pub fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

test {
    const alloc = std.testing.allocator;
    const values = try cloneStringSlice(alloc, &.{ "alpha", "beta" });
    defer freeStringSlice(alloc, values);

    try std.testing.expect(stringSlicesContains(values, "alpha"));
    try std.testing.expect(!stringSlicesContains(values, "gamma"));
    try std.testing.expect(stringSlicesIntersect(values, &.{"beta"}));
    try std.testing.expect(!stringSlicesIntersect(values, &.{"delta"}));
}
