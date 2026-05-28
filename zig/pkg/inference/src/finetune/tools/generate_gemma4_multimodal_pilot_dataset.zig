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
    "Identify the dominant visual property for sample",
    "Answer with the image QA control label for row",
    "Return the concise visual checksum for example",
    "Describe the provided reference image marker for item",
    "Emit the deterministic multimodal completion for record",
};

const labels = [_][]const u8{
    "red-square",
    "visual-anchor",
    "image-present",
    "single-image",
    "pilot-mm",
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const out_path = args.next() orelse return usage();
    const count_arg = args.next() orelse return usage();
    const image_path = args.next() orelse return usage();
    const split = args.next() orelse "train";
    const count = try std.fmt.parseUnsigned(usize, count_arg, 10);
    if (count == 0) return error.InvalidPilotExampleCount;

    const file = try compat.cwd().createFile(compat.io(), out_path, .{ .truncate = true });
    defer file.close(init.io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(init.io, &buf);
    for (0..count) |idx| {
        const prompt = prompts[idx % prompts.len];
        const label = labels[idx % labels.len];
        const variant = (idx * 31 + 7) % 997;
        const id = try std.fmt.allocPrint(allocator, "mm-pilot-{d:0>4}", .{idx + 1});
        defer allocator.free(id);
        const text = try std.fmt.allocPrint(allocator, "{s} {d}.", .{ prompt, idx + 1 });
        defer allocator.free(text);
        const response = try std.fmt.allocPrint(allocator, "{s}-{d:0>3}", .{ label, variant });
        defer allocator.free(response);

        try writer.interface.writeAll("{\"schema\":\"gemma_chat/v1\",\"id\":");
        try writeJsonString(&writer.interface, id);
        try writer.interface.writeAll(",\"split\":");
        try writeJsonString(&writer.interface, split);
        try writer.interface.writeAll(",\"messages\":[{\"role\":\"system\",\"content\":\"Use the image and answer with the exact compact label.\"},{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":");
        try writeJsonString(&writer.interface, text);
        try writer.interface.writeAll("},{\"type\":\"image\",\"image_path\":");
        try writeJsonString(&writer.interface, image_path);
        try writer.interface.writeAll("}]},{\"role\":\"assistant\",\"content\":");
        try writeJsonString(&writer.interface, response);
        try writer.interface.writeAll("}]}\n");
    }
    try writer.interface.flush();

    std.debug.print("examples_written: {d}\n", .{count});
    std.debug.print("saved_dataset: {s}\n", .{out_path});
    std.debug.print("image_path: {s}\n", .{image_path});
}

fn writeJsonString(writer: anytype, value: []const u8) !void {
    try std.json.Stringify.value(value, .{}, writer);
}

fn usage() error{InvalidArguments} {
    std.debug.print(
        \\usage: generate-gemma4-multimodal-pilot-dataset <out_jsonl> <count> <image_path> [split=train]
        \\
        \\Creates deterministic Gemma chat JSONL rows with one image content part per example.
        \\
    , .{});
    return error.InvalidArguments;
}
