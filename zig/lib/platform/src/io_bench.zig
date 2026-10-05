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

//! Cached positional reads and fsync-per-write comparison, with equal work.
const std = @import("std");
const platform = @import("antfly_platform");
const block_size = 4096;
const block_count = 16384;
const read_ops = 16384;
const write_ops = 128;
const Workload = enum { cached_read, durable_write };
const Worker = struct {
    io: std.Io,
    file: std.Io.File,
    workload: Workload,
    index: usize,
    concurrency: usize,
    fn run(self: Worker) anyerror!u64 {
        var buffer: [block_size]u8 = undefined;
        var checksum: u64 = 0;
        const operations: usize = if (self.workload == .cached_read) read_ops else write_ops;
        var op = self.index;
        while (op < operations) : (op += self.concurrency) {
            switch (self.workload) {
                .cached_read => {
                    const block = (op * 8191 + 17) % block_count;
                    const n = try self.file.readPositionalAll(self.io, &buffer, block * block_size);
                    if (n != buffer.len) return error.ShortRead;
                    const expected: u8 = @intCast(block % 251);
                    if (buffer[0] != expected or buffer[1023] != expected or buffer[2047] != expected or buffer[4095] != expected) return error.CorruptRead;
                    checksum += buffer[0];
                },
                .durable_write => {
                    @memset(&buffer, @intCast(op % 251));
                    try self.file.writePositionalAll(self.io, &buffer, op * block_size);
                    try self.file.sync(self.io);
                    checksum += buffer[0];
                },
            }
        }
        return checksum;
    }
};
fn measure(io: std.Io, file: std.Io.File, workload: Workload, concurrency: usize) !u64 {
    var futures: [32]std.Io.Future(anyerror!u64) = undefined;
    var launched: usize = 0;
    var joined: usize = 0;
    defer for (futures[joined..launched]) |*future| {
        _ = future.cancel(io) catch {};
    };
    const start = std.Io.Clock.awake.now(io);
    for (0..concurrency) |index| {
        futures[index] = try io.concurrent(Worker.run, .{Worker{
            .io = io,
            .file = file,
            .workload = workload,
            .index = index,
            .concurrency = concurrency,
        }});
        launched += 1;
    }
    var checksum: u64 = 0;
    while (joined < launched) {
        const result = futures[joined].await(io);
        joined += 1;
        checksum += try result;
    }
    const elapsed: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
    var expected: u64 = 0;
    const operations: usize = if (workload == .cached_read) read_ops else write_ops;
    for (0..operations) |op| expected += switch (workload) {
        .cached_read => ((op * 8191 + 17) % block_count) % 251,
        .durable_write => op % 251,
    };
    if (checksum != expected) return error.ChecksumMismatch;
    return elapsed;
}
fn verifyWrites(io: std.Io, file: std.Io.File) !void {
    var actual: [block_size]u8 = undefined;
    var expected: [block_size]u8 = undefined;
    for (0..write_ops) |op| {
        @memset(&expected, @intCast(op % 251));
        if (try file.readPositionalAll(io, &actual, op * block_size) != block_size or !std.mem.eql(u8, &actual, &expected)) return error.CorruptWrite;
    }
}
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const directory = args.next() orelse return error.ExpectedExistingTemporaryDirectory;
    const samples = if (args.next()) |value| try std.fmt.parseInt(usize, value, 10) else 7;
    if (samples == 0 or args.next() != null) return error.InvalidArguments;
    const threaded = try init.gpa.create(std.Io.Threaded);
    defer init.gpa.destroy(threaded);
    threaded.* = std.Io.Threaded.init(init.gpa, .{ .concurrent_limit = .limited(64), .async_limit = .nothing });
    defer threaded.deinit();
    const io = threaded.io();
    const evented = try init.gpa.create(platform.Evented);
    defer init.gpa.destroy(evented);
    try evented.init(init.gpa, .{});
    defer evented.deinit();
    const dir = try std.Io.Dir.cwd().openDir(io, directory, .{});
    defer dir.close(io);
    const reads = try dir.createFile(io, "io-bench-reads", .{ .read = true, .exclusive = true });
    defer dir.deleteFile(io, "io-bench-reads") catch {};
    defer reads.close(io);
    const writes = try dir.createFile(io, "io-bench-writes", .{ .read = true, .exclusive = true });
    defer dir.deleteFile(io, "io-bench-writes") catch {};
    defer writes.close(io);
    const fixture = try init.gpa.alloc(u8, block_count * block_size);
    defer init.gpa.free(fixture);
    for (0..block_count) |block| @memset(fixture[block * block_size ..][0..block_size], @intCast(block % 251));
    try reads.writePositionalAll(io, fixture, 0);
    try reads.sync(io);
    try writes.setLength(io, write_ops * block_size);
    const ios = [_]std.Io{ io, evented.io() };
    const names = [_][]const u8{ "threaded", "evented" };
    for ([_]Workload{ .cached_read, .durable_write }) |workload| {
        for ([_]usize{ 1, 8, 32 }) |concurrency| {
            for (ios) |backend_io| {
                _ = try measure(backend_io, if (workload == .cached_read) reads else writes, workload, concurrency);
            }
            for (0..samples) |sample| {
                for (0..2) |order| {
                    const backend = (sample + order) % 2;
                    const ns = try measure(ios[backend], if (workload == .cached_read) reads else writes, workload, concurrency);
                    if (workload == .durable_write) try verifyWrites(io, writes);
                    std.debug.print("{{\"backend\":\"{s}\",\"workload\":\"{s}\",\"concurrency\":{d},\"sample\":{d},\"operations\":{d},\"elapsed_ns\":{d}}}\n", .{ names[backend], @tagName(workload), concurrency, sample, @as(usize, if (workload == .cached_read) read_ops else write_ops), ns });
                }
            }
        }
    }
}
