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
const antfly = @import("antfly-zig");
const platform_time = antfly.platform_time;

const lmdb = antfly.lmdb;

const Config = struct {
    cycles: usize = 20,
    keys: usize = 512,
    value_size: usize = 64,
    defer_page_mutation: bool = false,
};

const Stats = struct {
    put_ns: u128 = 0,
    commit_ns: u128 = 0,

    fn avgPut(self: Stats, cycles: usize) u64 {
        return @intCast(self.put_ns / cycles);
    }

    fn avgCommit(self: Stats, cycles: usize) u64 {
        return @intCast(self.commit_ns / cycles);
    }
};

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const cfg = try parseArgs(init.minimal.args);
    const path = try makeTempPath(alloc);
    defer alloc.free(path);

    var env = try lmdb.Environment.open(path.ptr, .{
        .max_dbs = 1,
        .map_size = 256 * 1024 * 1024,
        .no_sync = false,
        .no_meta_sync = false,
        .no_tls = true,
        .defer_page_mutation = cfg.defer_page_mutation,
    });
    defer env.close();

    var stats = Stats{};
    var cycle: usize = 0;
    while (cycle < cfg.cycles) : (cycle += 1) {
        var txn = try env.begin(.{});
        errdefer txn.abort();
        const dbi = try txn.openDb(null, .{ .create = true });

        var key_buf: [32]u8 = undefined;
        const value = try alloc.alloc(u8, cfg.value_size);
        defer alloc.free(value);
        @memset(value, 'v');

        const put_start = nowNs();
        var key_idx: usize = 0;
        while (key_idx < cfg.keys) : (key_idx += 1) {
            const key = try std.fmt.bufPrint(&key_buf, "k-{d:0>4}-{d:0>8}", .{ cycle, key_idx });
            try txn.put(dbi, key, value, .{});
        }
        stats.put_ns += elapsedSince(put_start);

        const commit_start = nowNs();
        try txn.commit();
        stats.commit_ns += elapsedSince(commit_start);
    }

    std.debug.print(
        "lmdb_commit cycles={d} keys={d} value_size={d} avg_put={d:.3}ms avg_commit={d:.3}ms\n",
        .{
            cfg.cycles,
            cfg.keys,
            cfg.value_size,
            @as(f64, @floatFromInt(stats.avgPut(cfg.cycles))) / 1e6,
            @as(f64, @floatFromInt(stats.avgCommit(cfg.cycles))) / 1e6,
        },
    );
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--cycles")) {
            cfg.cycles = try parseNextUsize(&args, "--cycles");
        } else if (std.mem.eql(u8, arg, "--keys")) {
            cfg.keys = try parseNextUsize(&args, "--keys");
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            cfg.value_size = try parseNextUsize(&args, "--value-size");
        } else if (std.mem.eql(u8, arg, "--defer-page-mutation")) {
            cfg.defer_page_mutation = true;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.cycles == 0 or cfg.keys == 0 or cfg.value_size == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn makeTempPath(alloc: std.mem.Allocator) ![:0]u8 {
    const ts = nowNs();
    const owned = try std.fmt.allocPrint(alloc, "/tmp/antfly-lmdb-commit-{d}", .{ts});
    defer alloc.free(owned);
    const owned_z = try alloc.dupeZ(u8, owned);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), owned);
    return owned_z;
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    const end_ns = nowNs();
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}
