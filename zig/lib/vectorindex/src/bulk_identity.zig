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

//! Exact external uniqueness census. The vector producer tees only IDs here;
//! sorted runs and a bounded merge replace random probes of the growing index.
const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const A = std.mem.Allocator;
pub const Options = struct {
    rows: usize = 256 * 1024,
    check: ?*const fn (*anyopaque) anyerror!void = null,
    context: ?*anyopaque = null,
};
const Run = struct { name: [37]u8, count: usize };
const fan_in = 32;
const buffer_bytes = 32 * 1024;
pub const Census = struct {
    a: A,
    dir: std.Io.Dir,
    io: std.Io,
    options: Options,
    values: []u64,
    len: usize = 0,
    count: usize = 0,
    duplicate: bool = false,
    read_bytes: u64 = 0,
    written_bytes: u64 = 0,
    max_merge_inputs: u8 = 0,
    nonce: [16]u8,
    next: u64 = 0,
    runs: std.ArrayList(Run) = .empty,
    names: std.ArrayList([37]u8) = .empty,

    pub fn create(a: A, dir: std.Io.Dir, io: std.Io, options: Options) !*Census {
        if (options.rows == 0 or (options.check != null and options.context == null)) return error.InvalidBulkBuildConfig;
        const self = try a.create(Census);
        errdefer a.destroy(self);
        var random: [8]u8 = undefined;
        io.random(&random);
        self.* = .{ .a = a, .dir = dir, .io = io, .options = options, .values = try a.alloc(u64, options.rows), .nonce = std.fmt.bytesToHex(random, .lower) };
        return self;
    }
    pub fn deinit(self: *Census) void {
        const a = self.a;
        for (self.names.items) |name| self.dir.deleteFile(self.io, &name) catch {};
        self.names.deinit(a);
        self.runs.deinit(a);
        a.free(self.values);
        a.destroy(self);
    }
    fn check(self: *Census) !void {
        if (self.options.check) |callback| try callback(self.options.context.?);
    }
    pub fn append(self: *Census, id: u64) !void {
        if (self.len == self.values.len) try self.flush();
        self.values[self.len] = id;
        self.len += 1;
        self.count = try std.math.add(usize, self.count, 1);
    }
    fn newRun(self: *Census, count: usize) !Run {
        var name: [37]u8 = undefined;
        _ = try std.fmt.bufPrint(&name, "ids-{s}-{s}", .{ self.nonce, std.fmt.bytesToHex(std.mem.asBytes(&self.next), .lower) });
        self.next = try std.math.add(u64, self.next, 1);
        return .{ .name = name, .count = count };
    }
    fn createRun(self: *Census, run: Run) !std.Io.File {
        const file = try self.dir.createFile(self.io, &run.name, .{ .exclusive = true, .permissions = .fromMode(0o600) });
        errdefer {
            file.close(self.io);
            self.dir.deleteFile(self.io, &run.name) catch {};
        }
        try self.names.append(self.a, run.name);
        return file;
    }
    fn write(writer: *std.Io.Writer, id: u64) !void {
        var bytes: [12]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], id, .little);
        std.mem.writeInt(u32, bytes[8..12], Crc32.hash(bytes[0..8]), .little);
        try writer.writeAll(&bytes);
    }
    fn read(reader: *std.Io.Reader) !u64 {
        var bytes: [12]u8 = undefined;
        try reader.readSliceAll(&bytes);
        if (std.mem.readInt(u32, bytes[8..12], .little) != Crc32.hash(bytes[0..8])) return error.CorruptBulkSpool;
        return std.mem.readInt(u64, bytes[0..8], .little);
    }
    fn flush(self: *Census) !void {
        if (self.len == 0) return;
        try self.check();
        const values = self.values[0..self.len];
        std.mem.sort(u64, values, {}, std.sort.asc(u64));
        for (values[1..], values[0 .. values.len - 1]) |id, previous| if (id == previous) {
            self.duplicate = true;
        };
        const run = try self.newRun(values.len);
        const file = try self.createRun(run);
        defer file.close(self.io);
        var buffer: [buffer_bytes]u8 = undefined;
        var writer = file.writer(self.io, &buffer);
        for (values, 0..) |id, i| {
            if (i % 4096 == 0) try self.check();
            try write(&writer.interface, id);
        }
        try writer.interface.flush();
        self.written_bytes += @as(u64, @intCast(run.count)) * 12;
        try self.runs.append(self.a, run);
        self.len = 0;
    }
    fn less(values: *[fan_in]u64, left: usize, right: usize) std.math.Order {
        return std.math.order(values[left], values[right]);
    }
    fn merge(self: *Census, inputs: []const Run) !Run {
        self.max_merge_inputs = @max(self.max_merge_inputs, @as(u8, @intCast(inputs.len)));
        var total: usize = 0;
        for (inputs) |input| total = try std.math.add(usize, total, input.count);
        const result = try self.newRun(total);
        const file = try self.createRun(result);
        defer file.close(self.io);
        var output_buffer: [buffer_bytes]u8 = undefined;
        var writer = file.writer(self.io, &output_buffer);
        const buffers = try self.a.alloc(u8, inputs.len * buffer_bytes);
        defer self.a.free(buffers);
        var readers: [fan_in]std.Io.File.Reader = undefined;
        var opened: usize = 0;
        defer for (readers[0..opened]) |reader| reader.file.close(self.io);
        var remaining: [fan_in]usize = undefined;
        var values: [fan_in]u64 = undefined;
        var heap = std.PriorityQueue(usize, *[fan_in]u64, less).initContext(&values);
        defer heap.deinit(self.a);
        try heap.ensureTotalCapacity(self.a, inputs.len);
        for (inputs, 0..) |input, i| {
            if (input.count == 0) return error.CorruptBulkSpool;
            const source = try self.dir.openFile(self.io, &input.name, .{});
            readers[i] = source.reader(self.io, buffers[i * buffer_bytes ..][0..buffer_bytes]);
            opened += 1;
            remaining[i] = input.count - 1;
            values[i] = try read(&readers[i].interface);
            try heap.push(self.a, i);
        }
        var previous: ?u64 = null;
        var written: usize = 0;
        while (heap.pop()) |i| {
            if (written % 4096 == 0) try self.check();
            const id = values[i];
            if (previous != null and previous.? >= id) return if (previous.? == id) error.DuplicateVectorId else error.CorruptBulkSpool;
            try write(&writer.interface, id);
            previous = id;
            written += 1;
            if (remaining[i] != 0) {
                remaining[i] -= 1;
                values[i] = try read(&readers[i].interface);
                try heap.push(self.a, i);
            } else {
                if (readers[i].interface.takeByte()) |_| return error.CorruptBulkSpool else |err| if (err != error.EndOfStream) return err;
            }
        }
        if (written != total) return error.CorruptBulkSpool;
        try writer.interface.flush();
        self.read_bytes += @as(u64, @intCast(total)) * 12;
        self.written_bytes += @as(u64, @intCast(total)) * 12;
        return result;
    }
    pub fn finish(self: *Census, expected: usize) !void {
        try self.flush();
        try self.check();
        if (self.count != expected) return error.InvalidBulkSpoolCount;
        if (self.duplicate) return error.DuplicateVectorId;
        while (self.runs.items.len > 1) {
            var next: std.ArrayList(Run) = .empty;
            errdefer next.deinit(self.a);
            var first: usize = 0;
            while (first < self.runs.items.len) {
                const inputs = self.runs.items[first..@min(first + fan_in, self.runs.items.len)];
                if (inputs.len == 1) {
                    try next.append(self.a, inputs[0]);
                } else {
                    try next.append(self.a, try self.merge(inputs));
                    for (inputs) |input| self.dir.deleteFile(self.io, &input.name) catch {};
                }
                first += inputs.len;
            }
            self.runs.deinit(self.a);
            self.runs = next;
        }
    }
};

test "spooled bulk identity census bounds merge fan-in and rejects cross-level duplicates" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |duplicate| {
        var tmp = std.testing.tmpDir(.{ .iterate = true });
        defer tmp.cleanup();
        {
            const census = try Census.create(a, tmp.dir, io, .{ .rows = 1 });
            defer census.deinit();
            for (0..1030) |i| try census.append(1029 - i);
            try census.append(if (duplicate) 0 else std.math.maxInt(u64));
            if (duplicate) {
                try std.testing.expectError(error.DuplicateVectorId, census.finish(1031));
            } else {
                try census.finish(1031);
                try std.testing.expectEqual(@as(usize, 1), census.runs.items.len);
                try std.testing.expectEqual(@as(usize, 1031), census.runs.items[0].count);
            }
        }
        var iterator = tmp.dir.iterate();
        try std.testing.expect((try iterator.next(io)) == null);
    }
}

test "spooled bulk identity census validates checksums counts cancellation and cleanup" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Check = struct {
        canceled: bool = false,
        fn run(raw: *anyopaque) !void {
            if (@as(*@This(), @ptrCast(@alignCast(raw))).canceled) return error.Cancelled;
        }
    };
    for (0..3) |failure| {
        var tmp = std.testing.tmpDir(.{ .iterate = true });
        defer tmp.cleanup();
        {
            var check: Check = .{};
            const census = try Census.create(a, tmp.dir, io, .{ .rows = 7, .check = Check.run, .context = &check });
            defer census.deinit();
            for (0..25) |i| try census.append(i);
            if (failure == 0) {
                var bytes = try tmp.dir.readFileAlloc(io, &census.runs.items[0].name, a, .limited(1024));
                defer a.free(bytes);
                bytes[8] ^= 128;
                const file = try tmp.dir.openFile(io, &census.runs.items[0].name, .{ .mode = .read_write });
                defer file.close(io);
                try file.writePositionalAll(io, bytes[8..9], 8);
                try std.testing.expectError(error.CorruptBulkSpool, census.finish(25));
            } else if (failure == 1) {
                check.canceled = true;
                try std.testing.expectError(error.Cancelled, census.finish(25));
            } else try std.testing.expectError(error.InvalidBulkSpoolCount, census.finish(26));
        }
        var iterator = tmp.dir.iterate();
        try std.testing.expect((try iterator.next(io)) == null);
    }
}
