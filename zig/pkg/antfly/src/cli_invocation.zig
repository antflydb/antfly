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

//! Shared lexical boundary between global flags and command-local arguments.
//! Global flags are a prefix; a SQL string or document can never activate mode.
const std = @import("std");
pub fn takesValue(flag: []const u8) ?bool {
    for ([_][]const u8{ "--agent", "--no-agent", "--progress" }) |name|
        if (std.mem.eql(u8, flag, name)) return false;
    for ([_][]const u8{ "--fingerprint", "--output", "--display-rows", "--display-bytes", "--display-cell-bytes", "--deadline-ms", "--max-scan-rows", "--memory-bytes", "--spill-bytes" }) |name|
        if (std.mem.eql(u8, flag, name)) return true;
    return null;
}
pub const Invocation = struct {
    command: ?[]const u8 = null,
    arguments: std.ArrayList([*:0]const u8) = .empty,
    has_global_prefix: bool = false,
    pub fn deinit(self: *Invocation, a: std.mem.Allocator) void {
        self.arguments.deinit(a);
    }
};
pub fn parse(a: std.mem.Allocator, args: *std.process.Args.Iterator) !Invocation {
    var result: Invocation = .{};
    errdefer result.deinit(a);
    while (args.next()) |arg| {
        const consumes = takesValue(arg) orelse {
            result.command = arg;
            break;
        };
        result.has_global_prefix = true;
        try result.arguments.append(a, arg.ptr);
        if (consumes) {
            // Preserve malformed prefixes for the configured runtime to report
            // with the same diagnostic contract as every other argument error.
            const value = args.next() orelse return result;
            try result.arguments.append(a, value.ptr);
        }
    }
    while (args.next()) |arg| try result.arguments.append(a, arg.ptr);
    return result;
}

test "CLI invocation preserves local flag-looking values and malformed global prefixes" {
    const a = std.testing.allocator;
    var argv = [_][*:0]const u8{ "--agent", "sql", "--statement", "--no-agent" };
    var args = std.process.Args.Iterator.init(.{ .vector = &argv });
    var invocation = try parse(a, &args);
    defer invocation.deinit(a);
    try std.testing.expectEqualStrings("sql", invocation.command.?);
    try std.testing.expectEqualStrings("--no-agent", std.mem.span(invocation.arguments.items[2]));
    var bad_argv = [_][*:0]const u8{ "--agent", "--display-rows" };
    var bad_args = std.process.Args.Iterator.init(.{ .vector = &bad_argv });
    var bad = try parse(a, &bad_args);
    defer bad.deinit(a);
    try std.testing.expect(bad.command == null);
    try std.testing.expectEqual(@as(usize, 2), bad.arguments.items.len);
}
