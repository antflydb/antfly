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

//! In-process VM/JIT profiler. Compilation and source reads are outside timing.
const std = @import("std");
const regex = @import("antfly_regex").search;
pub const antfly_regex_profiling = true;
pub fn main(init: std.process.Init) !void {
    const a = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer args.deinit();
    _ = args.next();
    const expression = args.next() orelse return error.MissingPattern;
    const path = args.next() orelse return error.MissingSubject;
    const rounds = try std.fmt.parseInt(usize, args.next() orelse "7", 10);
    const text = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(256 * 1024 * 1024));
    defer a.free(text);
    var pattern = try regex.BacktrackingPattern.init(a, expression, .{ .multiline = true });
    defer pattern.deinit();
    var runner = try regex.BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    var profile: regex.ExecutionProfile = .{};
    runner.profile = &profile;
    runner.jit_enabled = false;
    _ = try scan(&runner, text);
    runner.profile = null;
    for (0..rounds) |round| for (0..2) |lane| {
        runner.jit_enabled = (lane + round) % 2 == 0;
        const start = std.Io.Clock.awake.now(init.io);
        const checksum = try scan(&runner, text);
        const ns = start.durationTo(std.Io.Clock.awake.now(init.io)).toNanoseconds();
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        try std.json.Stringify.value(.{ .pattern = expression, .bytes = text.len, .round = round, .jit = runner.jit_enabled, .jit_available = pattern.jit != null, .ns = ns, .checksum = checksum, .vm_dispatch = profile.states }, .{}, &output.writer);
        try output.writer.writeByte('\n');
        try std.Io.File.stdout().writeStreamingAll(init.io, output.written());
    };
}
fn scan(runner: *regex.BacktrackingRunner, text: []const u8) !u64 {
    var offset: usize = 0;
    var checksum: u64 = 0;
    while (offset <= text.len) {
        const match = (try runner.find(text, offset)) orelse break;
        checksum +%= match.span.start + match.span.end;
        for (match.captures) |capture| if (capture) |span| {
            checksum +%= span.start + span.end;
        };
        offset = if (match.span.end > offset) match.span.end else offset + 1;
    }
    return checksum;
}
