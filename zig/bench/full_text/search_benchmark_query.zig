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
const common = @import("search_benchmark_common.zig");

pub fn main(init: std.process.Init) !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const alloc = gpa_state.allocator();

    const args = try common.parseArgs(init.minimal.args);
    var db = try common.openDb(alloc, args.db_path);
    defer db.close();

    var stdin_buf: [64 * 1024]u8 = undefined;
    var reader = std.Io.File.stdin().readerStreaming(init.io, &stdin_buf);
    var stdout_buf: [8192]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    defer stdout.interface.flush() catch {};

    while (try reader.interface.takeDelimiter('\n')) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) continue;

        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse {
            try stdout.interface.writeAll("UNSUPPORTED\n");
            try stdout.interface.flush();
            continue;
        };
        const command = common.parseCommand(line[0..tab]);
        const query_text = line[tab + 1 ..];

        switch (command) {
            .unsupported => try stdout.interface.writeAll("UNSUPPORTED\n"),
            .count => {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = try common.parseLuceneQuery(arena_state.allocator(), query_text);
                const count = try common.countQuery(&db, alloc, query);
                try stdout.interface.print("{d}\n", .{count});
            },
            .top => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = try common.parseLuceneQuery(arena_state.allocator(), query_text);
                try common.topQuery(&db, alloc, query, limit);
                try stdout.interface.writeAll("1\n");
            },
            .top_count => |limit| {
                var arena_state = std.heap.ArenaAllocator.init(alloc);
                defer arena_state.deinit();
                const query = try common.parseLuceneQuery(arena_state.allocator(), query_text);
                try common.topQuery(&db, alloc, query, limit);
                const count = try common.countQuery(&db, alloc, query);
                try stdout.interface.print("{d}\n", .{count});
            },
        }
        try stdout.interface.flush();
    }
}
