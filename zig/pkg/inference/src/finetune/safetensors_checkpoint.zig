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

// SafeTensors checkpoint writer for fused chunker training.
//
// Writes tensors to the SafeTensors format:
//   [8 bytes: header_size as u64 little-endian]
//   [header_size bytes: UTF-8 JSON]
//   [tensor data: concatenated f32 bytes, little-endian]
//
// JSON header:
//   {
//     "__metadata__": {"format": "pt"},
//     "tensor_name": {
//       "dtype": "F32",
//       "shape": [dim0, dim1, ...],
//       "data_offsets": [byte_start, byte_end]
//     }
//   }
// where data_offsets are relative to the start of the data section
// (i.e. not counting the 8-byte size prefix or the JSON header itself).

const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../io/compat.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const NamedTensor = struct {
    name: []const u8,
    data: []const f32,
    shape: []const usize,
};

/// Write tensors to a SafeTensors file.
///
/// Tensors are written in the order provided. The resulting file is valid
/// SafeTensors and can be read by any compliant reader (Python safetensors
/// library, the MMapReader in src/models/safetensors.zig, etc.).
pub fn save(
    allocator: std.mem.Allocator,
    path: []const u8,
    tensors: []const NamedTensor,
) !void {
    return saveControlled(allocator, path, tensors, null, false);
}

/// The durable checkpoint owner publishes this file only after it is synced.
/// Exclusive temporary creation prevents concurrent writers from sharing a
/// staging file. Cancellation is checked between bounded payload chunks.
pub fn saveControlled(
    allocator: std.mem.Allocator,
    path: []const u8,
    tensors: []const NamedTensor,
    control: ?Control,
    exclusive: bool,
) !void {
    if (control) |active| try active.check();
    // 1. Compute per-tensor byte offsets within the data section.
    var offsets = try allocator.alloc(u64, tensors.len + 1);
    defer allocator.free(offsets);
    offsets[0] = 0;
    for (tensors, 0..) |t, i| {
        if (control) |active| try active.check();
        offsets[i + 1] = offsets[i] + @as(u64, t.data.len) * 4;
    }

    // 2. Build the JSON header string manually.
    var json_buf: std.Io.Writer.Allocating = .init(allocator);
    defer json_buf.deinit();
    const jw = &json_buf.writer;

    try jw.writeAll("{\"__metadata__\":{\"format\":\"pt\"}");
    for (tensors, 0..) |t, i| {
        try jw.writeAll(",\"");
        // Escape the tensor name (names in this codebase are simple ASCII identifiers,
        // but we do the minimal escaping required by JSON).
        for (t.name) |c| {
            switch (c) {
                '"' => try jw.writeAll("\\\""),
                '\\' => try jw.writeAll("\\\\"),
                else => try jw.writeByte(c),
            }
        }
        try jw.writeAll("\":{\"dtype\":\"F32\",\"shape\":[");
        for (t.shape, 0..) |dim, si| {
            if (si > 0) try jw.writeByte(',');
            try jw.print("{d}", .{dim});
        }
        try jw.print("],\"data_offsets\":[{d},{d}]}}", .{ offsets[i], offsets[i + 1] });
    }
    try jw.writeByte('}');

    const json_bytes = json_buf.written();
    // SafeTensors permits trailing spaces in the JSON header. Align the data
    // section so mmap readers can borrow typed f32 slices without copying.
    const header_padding = (8 - json_bytes.len % 8) % 8;
    const header_size: u64 = json_bytes.len + header_padding;

    // 3. Write the file.
    if (std.fs.path.dirname(path)) |dir| {
        if (dir.len > 0) try compat.cwd().createDirPath(compat.io(), dir);
    }
    const io = compat.io();
    if (control) |active| try active.check();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true, .exclusive = exclusive });
    errdefer if (exclusive) compat.cwd().deleteFile(io, path) catch {};
    defer file.close(io);
    var write_buffer: [64 * 1024]u8 = undefined;
    var file_writer = file.writer(io, &write_buffer);
    defer file_writer.end() catch {};
    const w = &file_writer.interface;

    // 8-byte header size (little-endian u64).
    var size_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &size_buf, header_size, .little);
    try w.writeAll(&size_buf);

    // JSON header.
    try w.writeAll(json_bytes);
    for (0..header_padding) |_| try w.writeByte(' ');

    // Tensor data: each f32 written as 4 little-endian bytes.
    for (tensors) |t| {
        // On little-endian hosts (x86, ARM) the in-memory f32 bytes are
        // already little-endian, so we can write the slice directly.
        // On big-endian hosts we swap each element.
        if (comptime builtin.cpu.arch.endian() == .little) {
            const bytes = std.mem.sliceAsBytes(t.data);
            var offset: usize = 0;
            while (offset < bytes.len) {
                if (control) |active| try active.check();
                const end = offset + @min(bytes.len - offset, 256 * 1024);
                try w.writeAll(bytes[offset..end]);
                offset = end;
            }
        } else {
            for (t.data, 0..) |val, i| {
                if ((i & 65535) == 0) if (control) |active| try active.check();
                var le_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &le_buf, @bitCast(val), .little);
                try w.writeAll(&le_buf);
            }
        }
    }

    try w.flush();
    if (control) |active| try active.check();
    try file.sync(io);
    if (control) |active| try active.check();
}
