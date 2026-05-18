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

const db_mod = antfly.db;

pub fn main() !void {
    const alloc = std.heap.page_allocator;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tempPath(&tmp_buf);
    defer cleanupTempDir(tmp_path);

    var db = try db_mod.DB.open(alloc, std.mem.span(tmp_path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    const cycles: usize = 12;
    const doc_count: usize = 32;

    for (0..cycles) |cycle| {
        var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
        defer {
            for (writes.items) |write| alloc.free(@constCast(write.key));
            for (writes.items) |write| alloc.free(@constCast(write.value));
            writes.deinit(alloc);
        }

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(@constCast(key));
            deletes.deinit(alloc);
        }

        for (0..doc_count) |i| {
            const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
            if ((cycle + i) % 5 == 0 and cycle > 0) {
                try deletes.append(alloc, key);
                continue;
            }

            const value = try std.fmt.allocPrint(
                alloc,
                "{{\"title\":\"doc {d}\",\"body\":\"cycle {d} common token {d}\"}}",
                .{ i, cycle, i % 7 },
            );
            try writes.append(alloc, .{
                .key = key,
                .value = value,
            });
        }

        try db.batch(.{
            .writes = writes.items,
            .deletes = deletes.items,
        });

        const index = db.core.index_manager.textIndex("ft_v1").?;
        const stats = try db.stats(alloc);
        var common = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{
                .field = "body",
                .term = "common",
            } },
            .limit = doc_count,
            .include_stored = false,
        });
        defer common.deinit();

        if (common.total_hits != stats.doc_count) {
            return error.BenchmarkInvariantFailed;
        }

        const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/index/data.mdb", .{std.mem.span(tmp_path)});
        defer alloc.free(index_path);
        const index_bytes = fileSize(index_path);
        const wal_path = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/wal/data.mdb", .{std.mem.span(tmp_path)});
        defer alloc.free(wal_path);
        const wal_bytes = fileSize(wal_path);

        std.debug.print(
            "{{\"cycle\":{d},\"docs\":{d},\"segments\":{d},\"index_bytes\":{d},\"wal_bytes\":{d},\"common_hits\":{d}}}\n",
            .{ cycle, stats.doc_count, index.snapshot().segments.len, index_bytes, wal_bytes, common.total_hits },
        );
    }
}

fn fileSize(path: []const u8) u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const stat = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return 0;
    return stat.size;
}

fn tempPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-merge-cycle-";
    const ns = platform_time.monotonicNs();
    const path = std.fmt.bufPrint(buf, "{s}{d}\x00", .{ base, ns }) catch unreachable;
    return @ptrCast(path.ptr);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
