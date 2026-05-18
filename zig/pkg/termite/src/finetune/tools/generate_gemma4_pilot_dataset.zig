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
const compat = @import("termite_internal").io.compat;

const prompts = [_][]const u8{
    "Return the project code name for sample",
    "Answer with the checksum word for row",
    "Write the concise label for training item",
    "Provide the expected response token for example",
    "Emit the deterministic completion for pilot record",
};

const nouns = [_][]const u8{
    "orchid",
    "copper",
    "harbor",
    "signal",
    "cedar",
    "quartz",
    "raven",
    "ember",
    "atlas",
    "delta",
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const out_path = args.next() orelse return usage();
    const count_arg = args.next() orelse "100";
    const split = args.next() orelse "train";
    const count = try std.fmt.parseUnsigned(usize, count_arg, 10);
    if (count == 0) return error.InvalidPilotExampleCount;

    const file = try compat.cwd().createFile(compat.io(), out_path, .{ .truncate = true });
    defer file.close(init.io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(init.io, &buf);
    for (0..count) |idx| {
        const prompt = prompts[idx % prompts.len];
        const noun = nouns[idx % nouns.len];
        const variant = (idx * 17 + 11) % 997;
        const id = try std.fmt.allocPrint(allocator, "pilot-{d:0>4}", .{idx + 1});
        defer allocator.free(id);
        const user_content = try std.fmt.allocPrint(allocator, "{s} {d}.", .{ prompt, idx + 1 });
        defer allocator.free(user_content);
        const assistant_content = try std.fmt.allocPrint(allocator, "{s}-{d:0>3}", .{ noun, variant });
        defer allocator.free(assistant_content);
        try std.json.Stringify.value(.{
            .schema = "gemma_chat/v1",
            .id = id,
            .split = split,
            .messages = &.{
                .{ .role = "system", .content = "Follow the instruction exactly and be concise." },
                .{ .role = "user", .content = user_content },
                .{ .role = "assistant", .content = assistant_content },
            },
        }, .{}, &writer.interface);
        try writer.interface.writeByte('\n');
    }
    try writer.interface.flush();

    std.debug.print("examples_written: {d}\n", .{count});
    std.debug.print("saved_dataset: {s}\n", .{out_path});
}

fn usage() error{InvalidArguments} {
    std.debug.print(
        \\usage: generate-gemma4-pilot-dataset <out_jsonl> [count=100] [split=train]
        \\
        \\Creates a deterministic Gemma chat JSONL dataset for bounded LoRA pilot runs.
        \\
    , .{});
    return error.InvalidArguments;
}
