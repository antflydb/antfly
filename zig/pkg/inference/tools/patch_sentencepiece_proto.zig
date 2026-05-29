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
    const arena = init.arena.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next();
    const root_in = args.next() orelse return error.MissingRootInput;
    const sentencepiece_in = args.next() orelse return error.MissingSentencePieceInput;
    const out_dir = args.next() orelse return error.MissingOutputDir;
    if (args.next() != null) return error.UnexpectedArgument;

    const io = init.io;
    try std.Io.Dir.cwd().createDirPath(io, out_dir);

    const root_bytes = try std.Io.Dir.cwd().readFileAlloc(io, root_in, arena, .limited(1 << 20));
    try writeFile(arena, io, out_dir, "root.zig", root_bytes);

    const sentencepiece_bytes = try std.Io.Dir.cwd().readFileAlloc(io, sentencepiece_in, arena, .limited(1 << 20));
    const patched = try patchSentencePiece(arena, sentencepiece_bytes);
    try writeFile(arena, io, out_dir, "sentencepiece.zig", patched);
}

fn patchSentencePiece(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var output = try allocator.dupe(u8, input);
    output = try replaceAll(allocator, output, "TrainerSpec.ModelType = UNIGRAM", "TrainerSpec.ModelType = .UNIGRAM");
    output = try replaceAll(allocator, output, "ModelProto.SentencePiece.Type = NORMAL", "ModelProto.SentencePiece.Type = .NORMAL");
    return output;
}

fn replaceAll(
    allocator: std.mem.Allocator,
    input: []const u8,
    needle: []const u8,
    replacement: []const u8,
) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    defer out.deinit(allocator);

    var start: usize = 0;
    while (std.mem.indexOfPos(u8, input, start, needle)) |idx| {
        try out.appendSlice(allocator, input[start..idx]);
        try out.appendSlice(allocator, replacement);
        start = idx + needle.len;
    }
    try out.appendSlice(allocator, input[start..]);
    return out.toOwnedSlice(allocator);
}

fn writeFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir_path: []const u8,
    name: []const u8,
    data: []const u8,
) !void {
    const full_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, name });
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = full_path,
        .data = data,
    });
}
