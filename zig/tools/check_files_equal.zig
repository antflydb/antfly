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

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_impl = std.heap.ArenaAllocator.init(gpa);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const argv = try init.minimal.args.toSlice(arena);
    if (argv.len < 3 or (argv.len - 1) % 2 != 0) {
        std.debug.print("usage: check-files-equal <actual> <expected> [<actual> <expected>...]\n", .{});
        std.process.exit(2);
    }

    var i: usize = 1;
    while (i < argv.len) : (i += 2) {
        const actual_path = argv[i];
        const expected_path = argv[i + 1];

        const actual = std.Io.Dir.cwd().readFileAlloc(io, actual_path, gpa, .limited(20 * 1024 * 1024)) catch |err| {
            std.debug.print("check-files-equal: unable to read actual {s}: {}\n", .{ actual_path, err });
            std.process.exit(1);
        };
        defer gpa.free(actual);

        const expected = std.Io.Dir.cwd().readFileAlloc(io, expected_path, gpa, .limited(20 * 1024 * 1024)) catch |err| {
            std.debug.print("check-files-equal: unable to read expected {s}: {}\n", .{ expected_path, err });
            std.process.exit(1);
        };
        defer gpa.free(expected);

        if (!std.mem.eql(u8, actual, expected)) {
            std.debug.print("check-files-equal: {s} differs from {s}\n", .{ expected_path, actual_path });
            std.process.exit(1);
        }
    }
}
