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

//! Finish generation and formatting before the build publishes the output.
const std = @import("std");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const args = try init.minimal.args.toSlice(allocator);
    defer allocator.free(args);
    if (args.len != 4) return error.InvalidArguments;
    const format_only = std.mem.eql(u8, args[1], "--format");
    if (!format_only) {
        const result = try std.process.run(allocator, init.io, .{
            .argv = &.{ args[1], args[2], "-zig", "-o", args[3] },
        });
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);
        switch (result.term) {
            .exited => |code| if (code != 0) {
                std.debug.print("Snowball generator failed: {s}\n", .{result.stderr});
                return error.GenerationFailed;
            },
            else => return error.GenerationFailed,
        }
    }
    const directory = std.Io.Dir.cwd();
    const raw = try directory.readFileAlloc(init.io, if (format_only) args[2] else args[3], allocator, .limited(20 * 1024 * 1024));
    defer allocator.free(raw);
    const formatted = try formatSource(allocator, raw);
    defer allocator.free(formatted);
    try directory.writeFile(init.io, .{ .sub_path = args[3], .data = formatted });
}

fn formatSource(allocator: std.mem.Allocator, source: []const u8) ![]u8 {
    const terminated = try allocator.dupeSentinel(u8, source, 0);
    defer allocator.free(terminated);
    var tree = try std.zig.Ast.parse(allocator, terminated, .{});
    defer tree.deinit(allocator);
    if (tree.errors.len != 0) return error.InvalidZigSource;
    return tree.renderAlloc(allocator);
}
