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

// Not sure what I want to do about a "registry"
// But I think I want to wait until comptime allocation is available

pub const Opts = struct {
    prefix: []const u8 = "",
    exclude: ?[]const []const u8 = null,

    pub fn shouldExclude(self: Opts, name: []const u8) bool {
        const excludes = self.exclude orelse return false;
        for (excludes) |exclude| {
            if (std.mem.eql(u8, exclude, name)) {
                return true;
            }
        }
        return false;
    }
};

const t = @import("testing.zig");
test "Registry.Opts: shouldExclude" {
    try t.expectEqual(false, (Opts{}).shouldExclude("abc"));
    try t.expectEqual(false, (Opts{ .exclude = &.{ "ABC", "other" } }).shouldExclude("abc"));
    try t.expectEqual(true, (Opts{ .exclude = &.{ "abc", "other" } }).shouldExclude("abc"));
    try t.expectEqual(true, (Opts{ .exclude = &.{ "a", "otaher", "abc" } }).shouldExclude("abc"));
}
