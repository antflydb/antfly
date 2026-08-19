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
const yacc = @import("yacc");

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next();
    const input_path = args.next() orelse {
        std.debug.print("usage: yacc-zig <input.y> <output.zig>\n", .{});
        std.process.exit(2);
    };
    const output_path = args.next() orelse {
        std.debug.print("usage: yacc-zig <input.y> <output.zig> [source-label]\n", .{});
        std.process.exit(2);
    };
    const source_label = args.next() orelse input_path;
    if (args.next() != null) {
        std.debug.print("usage: yacc-zig <input.y> <output.zig> [source-label]\n", .{});
        std.process.exit(2);
    }

    const source = try std.Io.Dir.cwd().readFileAlloc(io, input_path, arena, .limited(4 * 1024 * 1024));
    const generated = yacc.generateZigMetadata(arena, source_label, source) catch |err| {
        if (err == error.ConflictCountMismatch) {
            const report = yacc.conflictReportAlloc(arena, source_label, source, 20) catch null;
            if (report) |text| std.debug.print("{s}", .{text});
        }
        std.debug.print("yacc-zig: invalid grammar {s}: {}\n", .{ input_path, err });
        std.process.exit(1);
    };

    if (std.mem.lastIndexOfScalar(u8, output_path, '/')) |slash| {
        try std.Io.Dir.cwd().createDirPath(io, output_path[0..slash]);
    }
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = output_path,
        .data = generated,
    });
}
