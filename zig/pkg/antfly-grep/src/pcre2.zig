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

//! Native nonregular regex adapter; implemented by the monorepo regex library.
const std = @import("std");
const regex = @import("antfly_regex").search;
const Options = @import("options.zig").Options;
pub const Pattern = struct {
    a: std.mem.Allocator,
    pattern: *regex.BacktrackingPattern,
    runner: regex.BacktrackingRunner,
    names: []const regex.CaptureName,
    owns_pattern: bool = true,
    pub fn init(a: std.mem.Allocator, text: []const u8, o: *const Options, insensitive: bool) !Pattern {
        const pattern = try a.create(regex.BacktrackingPattern);
        errdefer a.destroy(pattern);
        pattern.* = try regex.BacktrackingPattern.init(a, text, .{ .insensitive = insensitive, .unicode = o.unicode and o.pcre2_unicode, .multiline = true, .dotall = o.multiline_dotall, .whole_word = o.word, .whole_line = o.whole_line, .crlf = o.crlf });
        errdefer pattern.deinit();
        var runner = try regex.BacktrackingRunner.init(a, pattern);
        runner.max_steps = o.pcre2_match_limit;
        return .{ .a = a, .pattern = pattern, .runner = runner, .names = pattern.names };
    }
    pub fn deinit(self: *Pattern) void {
        self.runner.deinit();
        if (self.owns_pattern) {
            self.pattern.deinit();
            self.a.destroy(self.pattern);
        }
    }
    pub fn clone(self: *const Pattern, a: std.mem.Allocator) !Pattern {
        var runner = try regex.BacktrackingRunner.init(a, self.pattern);
        runner.max_steps = self.runner.max_steps;
        return .{ .a = a, .pattern = self.pattern, .runner = runner, .names = self.names, .owns_pattern = false };
    }
    pub fn find(self: *Pattern, text: []const u8, offset: usize) !?regex.Match {
        return self.runner.find(text, offset);
    }
};
