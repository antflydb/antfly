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
const identity = @import("bulk_identity.zig");
const Crc32 = @import("antfly_hash").Crc32;

pub const Spool = struct {
    dir: std.Io.Dir,
    io: std.Io,
    dims: usize,
    next_file: u64 = 0,
    /// Only a bounded subtree is admitted here. Compact descendants point
    /// into these immutable checked bytes, never a corpus-sized random gather.
    canonical: ?[]const u8 = null,
    compact_rows: u64 = 0,
    written_bytes: u64 = 0,
    read_bytes: u64 = 0,

    pub const Partition = struct {
        name: [16]u8,
        count: usize = 0,
        metadata_bytes: usize = 0,
        sample_count: usize = 0,
        indirect: bool = false,
        identities_verified: bool = false,
        identity_read_bytes: u64 = 0,
        identity_written_bytes: u64 = 0,
        identity_merge_inputs: u8 = 0,
    };

    pub fn create(self: *Spool, buffer: []u8) !Output {
        const name = std.fmt.bytesToHex(std.mem.asBytes(&self.next_file), .lower);
        self.next_file += 1;
        const file = try self.dir.createFile(self.io, &name, .{ .exclusive = true, .permissions = .fromMode(0o600) });
        return .{ .dir = self.dir, .partition = .{ .name = name }, .writer = file.writer(self.io, buffer), .dims = self.dims, .owner = self };
    }

    pub fn open(self: Spool, partition: Partition, buffer: []u8) !std.Io.File.Reader {
        const file = try self.dir.openFile(self.io, &partition.name, .{});
        return file.reader(self.io, buffer);
    }

    pub const Input = struct {
        file: ?std.Io.File.Reader = null,
        memory: std.Io.Reader = .fixed(&.{}),
        pub fn reader(self: *Input) *std.Io.Reader {
            return if (self.file) |*file| &file.interface else &self.memory;
        }
        pub fn close(self: *Input, io: std.Io) void {
            if (self.file) |file| file.file.close(io);
        }
    };

    /// The only full partition beneath an active canonical region is that
    /// region itself. Route and sample its retained bytes instead of issuing
    /// another file scan. Descendants still read their compact ordinal files.
    pub fn openInput(self: *Spool, partition: Partition, buffer: []u8) !Input {
        if (!partition.indirect) if (self.canonical) |bytes| return .{ .memory = .fixed(bytes) };
        const bytes = if (partition.indirect)
            std.math.mul(u64, partition.count, 12) catch return error.InvalidBulkSpoolCount
        else
            std.math.add(u64, std.math.mul(u64, partition.count, self.dims * 4 + 14) catch return error.InvalidBulkSpoolCount, partition.metadata_bytes) catch return error.InvalidBulkSpoolCount;
        self.read_bytes = std.math.add(u64, self.read_bytes, bytes) catch return error.InvalidBulkSpoolCount;
        return .{ .file = try self.open(partition, buffer) };
    }

    pub fn readRow(self: Spool, partition: Partition, reader: *std.Io.Reader, key: []u8, vector: []f32, physical_offset: u64) !Row {
        if (!partition.indirect) {
            var row = try read(reader, key, vector);
            row.offset = physical_offset;
            return row;
        }
        var encoded: [8]u8 = undefined;
        try reader.readSliceAll(&encoded);
        if (try reader.takeInt(u32, .little) != Crc32.hash(&encoded)) return error.CorruptBulkSpool;
        const offset = std.mem.readInt(u64, &encoded, .little);
        const canonical = self.canonical orelse return error.CorruptBulkSpool;
        if (offset >= canonical.len) return error.CorruptBulkSpool;
        var source: std.Io.Reader = .fixed(canonical[@intCast(offset)..]);
        var row = try read(&source, key, vector);
        row.offset = offset;
        return row;
    }

    /// Legacy/unverified spools get one sequential census before any index
    /// writes. Normal lake producers collect it while writing the root.
    pub fn verifyIdentities(self: *Spool, a: std.mem.Allocator, partition: Partition, options: identity.Options) !void {
        const census = try identity.Census.create(a, self.dir, self.io, options);
        defer census.deinit();
        const buffer = try a.alloc(u8, 64 * 1024);
        defer a.free(buffer);
        const key = try a.alloc(u8, std.math.maxInt(u16));
        defer a.free(key);
        const vector = try a.alloc(f32, self.dims);
        defer a.free(vector);
        var input = try self.openInput(partition, buffer);
        defer input.close(self.io);
        for (0..partition.count) |i| {
            if (i % 256 == 0) if (options.check) |callback| try callback(options.context.?);
            const row = try self.readRow(partition, input.reader(), key, vector, 0);
            try census.append(row.id);
        }
        try census.finish(partition.count);
    }

    pub fn remove(self: Spool, partition: Partition) void {
        self.dir.deleteFile(self.io, &partition.name) catch {};
        self.dir.deleteFile(self.io, &sampleName(partition)) catch {};
    }

    pub fn openSample(self: Spool, partition: Partition, buffer: []u8) !std.Io.File.Reader {
        const file = try self.dir.openFile(self.io, &sampleName(partition), .{});
        return file.reader(self.io, buffer);
    }
};

fn sampleName(partition: Spool.Partition) [23]u8 {
    return partition.name ++ ".sample".*;
}

const Sample = struct {
    arena: std.heap.ArenaAllocator,
    ids: []u64,
    vectors: []f32,
    random: std.Random.DefaultPrng,
};

pub const Output = struct {
    dir: std.Io.Dir,
    sample: ?Sample = null,
    partition: Spool.Partition,
    writer: std.Io.File.Writer,
    dims: usize,
    owner: *Spool,
    identity_census: ?*identity.Census = null,

    pub fn enableIdentityCensus(self: *Output, a: std.mem.Allocator, options: identity.Options) !void {
        if (self.partition.count != 0 or self.identity_census != null) return error.InvalidBulkBuildConfig;
        self.identity_census = try identity.Census.create(a, self.dir, self.owner.io, options);
    }

    /// At most one bounded reservoir per open child, released on finish.
    pub fn enableSample(self: *Output, allocator: std.mem.Allocator, rows: usize, bytes: usize, seed: u64) !void {
        if (self.partition.count != 0 or self.sample != null) return error.InvalidBulkBuildConfig;
        const count = @min(rows, bytes / (self.dims * @sizeOf(f32)));
        if (count == 0) return;
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const ids = try arena.allocator().alloc(u64, count);
        const vectors = try arena.allocator().alloc(f32, count * self.dims);
        self.sample = .{ .arena = arena, .ids = ids, .vectors = vectors, .random = .init(seed) };
    }

    pub fn append(self: *Output, id: u64, key: []const u8, vector: []const f32) !void {
        return self.appendLocated(id, key, vector, 0);
    }
    pub fn appendLocated(self: *Output, id: u64, key: []const u8, vector: []const f32, offset: u64) !void {
        if (vector.len != self.dims) return error.InvalidDimensions;
        for (vector) |value| {
            if (!std.math.isFinite(value)) return error.InvalidBulkSpoolVector;
        }
        const len = std.math.cast(u16, key.len) orelse return error.BulkSpoolKeyTooLong;
        if (self.partition.indirect) {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, offset, .little);
            try self.writer.interface.writeAll(&encoded);
            try self.writer.interface.writeInt(u32, Crc32.hash(&encoded), .little);
            self.owner.written_bytes += 12;
            self.owner.compact_rows += 1;
        } else {
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
            self.owner.written_bytes += 14 + key.len + vector.len * @sizeOf(f32);
        }
        if (self.sample) |*sample| {
            const row = self.partition.count;
            const slot = if (row < sample.ids.len) row else sample.random.random().uintLessThan(usize, row + 1);
            if (slot < sample.ids.len) {
                sample.ids[slot] = id;
                @memcpy(sample.vectors[slot * self.dims ..][0..self.dims], vector);
            }
        }
        if (self.identity_census) |census| try census.append(id);
        self.partition.count += 1;
        self.partition.metadata_bytes += key.len;
    }

    pub fn finish(self: *Output, io: std.Io) !Spool.Partition {
        try self.writer.interface.flush();
        if (self.identity_census) |census| {
            try census.finish(self.partition.count);
            self.partition.identity_read_bytes = census.read_bytes;
            self.partition.identity_written_bytes = census.written_bytes;
            self.partition.identity_merge_inputs = census.max_merge_inputs;
            census.deinit();
            self.identity_census = null;
            self.partition.identities_verified = true;
        }
        if (self.sample) |*sample| {
            const file = try self.dir.createFile(io, &sampleName(self.partition), .{ .exclusive = true, .permissions = .fromMode(0o600) });
            defer file.close(io);
            errdefer self.dir.deleteFile(io, &sampleName(self.partition)) catch {};
            var buffer: [64 * 1024]u8 = undefined;
            var writer = file.writer(io, &buffer);
            const count = @min(self.partition.count, sample.ids.len);
            for (sample.ids[0..count], 0..) |id, row| {
                var header: [8]u8 = undefined;
                std.mem.writeInt(u64, &header, id, .little);
                const bytes = std.mem.sliceAsBytes(sample.vectors[row * self.dims ..][0..self.dims]);
                var checksum = Crc32.init();
                checksum.update(&header);
                checksum.update(bytes);
                try writer.interface.writeAll(&header);
                try writer.interface.writeAll(bytes);
                try writer.interface.writeInt(u32, checksum.final(), .little);
            }
            try writer.interface.flush();
            self.partition.sample_count = count;
            sample.arena.deinit();
            self.sample = null;
        }
        self.writer.file.close(io);
        return self.partition;
    }

    pub fn abort(self: *Output, spool: Spool) void {
        self.writer.file.close(spool.io);
        if (self.identity_census) |census| census.deinit();
        self.identity_census = null;
        if (self.sample) |*sample| sample.arena.deinit();
        self.sample = null;
        spool.remove(self.partition);
    }
};

pub const Row = struct {
    id: u64,
    key: []const u8,
    offset: u64 = 0,
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

pub fn readSample(reader: *std.Io.Reader, vector: []f32) !u64 {
    var header: [8]u8 = undefined;
    try reader.readSliceAll(&header);
    try reader.readSliceAll(std.mem.sliceAsBytes(vector));
    var checksum = Crc32.init();
    checksum.update(&header);
    checksum.update(std.mem.sliceAsBytes(vector));
    if (try reader.takeInt(u32, .little) != checksum.final()) return error.CorruptBulkSpool;
    return std.mem.readInt(u64, &header, .little);
}
