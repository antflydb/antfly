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

//! Private scratch format for offline HBC construction. Each partition is
//! consumed sequentially. Consumed parents are removed before descending
//! into their children.
const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;

pub const Spool = struct {
    dir: std.Io.Dir,
    io: std.Io,
    dims: usize,
    next_file: u64 = 0,

    pub const Partition = struct {
        name: [16]u8,
        count: usize = 0,
        metadata_bytes: usize = 0,
    };

    pub fn create(self: *Spool, buffer: []u8) !Output {
        const name = std.fmt.bytesToHex(std.mem.asBytes(&self.next_file), .lower);
        self.next_file += 1;
        const file = try self.dir.createFile(self.io, &name, .{ .exclusive = true, .permissions = .fromMode(0o600) });
        return .{ .partition = .{ .name = name }, .writer = file.writer(self.io, buffer), .dims = self.dims };
    }

    pub fn open(self: Spool, partition: Partition, buffer: []u8) !std.Io.File.Reader {
        const file = try self.dir.openFile(self.io, &partition.name, .{});
        return file.reader(self.io, buffer);
    }

    pub fn remove(self: Spool, partition: Partition) void {
        self.dir.deleteFile(self.io, &partition.name) catch {};
    }
};

pub const Output = struct {
    partition: Spool.Partition,
    writer: std.Io.File.Writer,
    dims: usize,

    pub fn append(self: *Output, id: u64, key: []const u8, vector: []const f32) !void {
        if (vector.len != self.dims) return error.InvalidDimensions;
        for (vector) |value| {
            if (!std.math.isFinite(value)) return error.InvalidBulkSpoolVector;
        }
        const len = std.math.cast(u16, key.len) orelse return error.BulkSpoolKeyTooLong;
        var header: [10]u8 = undefined;
        std.mem.writeInt(u64, header[0..8], id, .little);
        std.mem.writeInt(u16, header[8..10], len, .little);
        var checksum = Crc32.init();
        checksum.update(&header);
        checksum.update(key);
        checksum.update(std.mem.sliceAsBytes(vector));
        try self.writer.interface.writeAll(&header);
        try self.writer.interface.writeAll(key);
        try self.writer.interface.writeAll(std.mem.sliceAsBytes(vector));
        try self.writer.interface.writeInt(u32, checksum.final(), .little);
        self.partition.count += 1;
        self.partition.metadata_bytes += key.len;
    }

    pub fn finish(self: *Output, io: std.Io) !Spool.Partition {
        try self.writer.interface.flush();
        self.writer.file.close(io);
        return self.partition;
    }

    pub fn abort(self: *Output, spool: Spool) void {
        self.writer.file.close(spool.io);
        spool.remove(self.partition);
    }
};

pub const Row = struct {
    id: u64,
    key: []const u8,
};

pub fn read(reader: *std.Io.Reader, key: []u8, vector: []f32) !Row {
    var header: [10]u8 = undefined;
    try reader.readSliceAll(&header);
    const id = std.mem.readInt(u64, header[0..8], .little);
    const len = std.mem.readInt(u16, header[8..10], .little);
    if (len > key.len) return error.BulkSpoolKeyTooLong;
    try reader.readSliceAll(key[0..len]);
    try reader.readSliceAll(std.mem.sliceAsBytes(vector));
    var checksum = Crc32.init();
    checksum.update(&header);
    checksum.update(key[0..len]);
    checksum.update(std.mem.sliceAsBytes(vector));
    if (try reader.takeInt(u32, .little) != checksum.final()) return error.CorruptBulkSpool;
    return .{ .id = id, .key = key[0..len] };
}
