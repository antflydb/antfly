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

//! Qualification-only native PCM export. Pair with independent FFmpeg PCM
//! references and CUDA embeddings; no production decoder interface is added.
const std = @import("std");
const audio = @import("audio");

pub fn main(init: std.process.Init) !void {
    const a = std.heap.page_allocator;
    const args = try init.minimal.args.toSlice(a);
    if (args.len != 3) return error.InvalidArguments;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, args[1], a, .limited(16 * 1024 * 1024));
    var decoded = try audio.decodeBounded(a, bytes, .{ .file_name_hint = args[1] }, 256 * 1024 * 1024);
    defer decoded.deinit();
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = args[2], .data = std.mem.sliceAsBytes(decoded.samples) });
    const metadata = try std.json.Stringify.valueAlloc(a, .{ .sample_rate = decoded.sample_rate, .samples = decoded.samples.len }, .{});
    const metadata_path = try std.fmt.allocPrint(a, "{s}.json", .{args[2]});
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = metadata_path, .data = metadata });
    if (try audio.streaming.supported(a, bytes)) {
        const Sink = struct {
            values: std.ArrayList(f32) = .empty,
            fn write(raw: *anyopaque, samples: []const f32, _: u32, _: u8) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (samples.len > 192000 * 30 - self.values.items.len) return error.AudioTooLarge;
                try self.values.appendSlice(std.heap.page_allocator, samples);
            }
        };
        var sink = Sink{};
        try audio.streaming.visit(a, bytes, .{}, .{ .context = &sink, .write = Sink.write });
        const path = try std.fmt.allocPrint(a, "{s}.stream.f32", .{args[2]});
        try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(sink.values.items) });
    }
}
