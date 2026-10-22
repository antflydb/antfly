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

//! Maintenance utility: JSON lines with native parse acceptance/time/peak bytes.
const std = @import("std");
const syntax = @import("grammar.zig");
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args.deinit();
    _ = args.next();
    var failed: bool = false;
    while (args.next()) |path| {
        const grammar = if (std.mem.endsWith(u8, path, ".go")) &@import("grammars/go.zig").grammar else if (std.mem.endsWith(u8, path, ".py")) &@import("grammars/python.zig").grammar else if (std.mem.endsWith(u8, path, ".rs")) &@import("grammars/rust.zig").grammar else continue;
        const source = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(16 * 1024 * 1024));
        defer a.free(source);
        const begin = std.Io.Clock.awake.now(init.io);
        var tree = syntax.parse(a, grammar, source) catch |err| {
            std.debug.print("{s}: {s}\n", .{ path, @errorName(err) });
            failed = true;
            continue;
        };
        defer tree.deinit();
        const elapsed = begin.durationTo(std.Io.Clock.awake.now(init.io)).toNanoseconds();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try std.json.Stringify.value(.{ .path = path, .bytes = source.len, .complete = tree.complete, .elapsed_ns = elapsed, .peak_bytes = tree.peak_bytes, .nodes = tree.nodes.len, .work = tree.work, .stacks = tree.stack_count, .error_offset = tree.error_offset, .error_line = std.mem.count(u8, source[0..tree.error_offset], "\n") + 1 }, .{}, &output.writer);
        try output.writer.writeByte('\n');
        try std.Io.File.stdout().writeStreamingAll(init.io, output.written());
        failed = failed or !tree.complete;
    }
    if (failed) std.process.exit(1);
}
