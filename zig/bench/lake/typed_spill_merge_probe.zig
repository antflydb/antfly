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

//! Full typed spill sort/merge probe; every key and value is checked.
const std = @import("std");
const local = @import("local");
const spill = local.sql_spill;
const D = local.sql_scalar.Datum;
fn check(_: *anyopaque) !void {}
fn now() i96 {
    var stamp: std.c.timespec = undefined;
    if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("CPU clock unavailable");
    return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
}
test "typed spill merge full sort probe" {
    const count = 8192;
    for ([_]bool{ false, true }) |compressed| {
        var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 32 << 20 };
        defer std.debug.assert(budget.live == 0);
        var marker: u8 = 0;
        var manager: spill.Manager = .{ .alloc = budget.allocator(), .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = if (compressed) .snappy else .none, .async_writes = false };
        defer manager.deinit();
        const started = now();
        {
            var sort = spill.Sort.init(budget.allocator(), &manager, &.{.{}}, 64 * 1024);
            defer sort.deinit();
            sort.allow_indirect = false;
            sort.shared_runs = true;
            sort.parallel_runs = false;
            sort.encoded_bytes = true;
            var bodies: [64][5][96]u8 = @splat(@splat(@splat('v')));
            var cells: [64][8]D = undefined;
            var keys: [64][8]u8 = undefined;
            var key_cells: [64][1]D = undefined;
            var rows: [64][]const D = undefined;
            var key_rows: [64][]const D = undefined;
            var ordinals: [64]u64 = undefined;
            for (0..count / 64) |batch| {
                for (0..64) |i| {
                    const ordinal = batch * 64 + i;
                    const rank = i * (count / 64) + batch;
                    cells[i][0] = D.json(.{ .integer = @as(i64, 9007199254740993) + @as(i64, @intCast(ordinal)) });
                    cells[i][1] = D.json(.{ .float = @as(f64, @floatFromInt(ordinal)) * -0.5 });
                    cells[i][2] = D.json(.{ .bool = ordinal % 2 == 0 });
                    for (0..5) |column| {
                        std.mem.writeInt(u64, bodies[i][column][0..8], ordinal * 5 + column, .little);
                        cells[i][column + 3] = if (ordinal % 17 == 0) .{} else D.json(.{ .string = &bodies[i][column] });
                    }
                    std.mem.writeInt(u64, &keys[i], rank, .big);
                    key_cells[i][0] = D.json(.{ .string = &keys[i] });
                    rows[i] = &cells[i];
                    key_rows[i] = &key_cells[i];
                    ordinals[i] = ordinal;
                }
                try sort.addBatch(.{ .rows = &rows }, &key_rows, &ordinals);
            }
            for (0..count) |rank| {
                const lease = (try sort.nextLeased()).?;
                defer lease.release();
                const ordinal = (rank % (count / 64)) * 64 + rank / (count / 64);
                try std.testing.expectEqual(rank, std.mem.readInt(u64, (try lease.keyCell(0)).value.string[0..8], .big));
                try std.testing.expectEqual(ordinal, lease.row.ordinal);
                try std.testing.expectEqual(@as(i64, 9007199254740993) + @as(i64, @intCast(ordinal)), (try lease.cell(0)).value.integer);
                try std.testing.expectEqual(@as(f64, @floatFromInt(ordinal)) * -0.5, (try lease.cell(1)).value.float);
                try std.testing.expectEqual(ordinal % 2 == 0, (try lease.cell(2)).value.bool);
                for (0..5) |column| {
                    const value = try lease.cell(column + 3);
                    try std.testing.expectEqual(ordinal % 17 == 0, value.sql_null);
                    if (!value.sql_null) {
                        try std.testing.expectEqual(ordinal * 5 + column, std.mem.readInt(u64, value.value.string[0..8], .little));
                        for (value.value.string[8..]) |byte| try std.testing.expectEqual(@as(u8, 'v'), byte);
                    }
                }
            }
            try std.testing.expect((try sort.nextLeased()) == null);
            try std.testing.expect(manager.merges > 0);
        }
        std.debug.print("TYPED_MERGE compressed={any} rows={d} cpu_ns={d} reads={d} writes={d} merges={d} peak={d} quota={d}\n", .{ compressed, count, now() - started, manager.read_bytes, manager.written_bytes, manager.merges, budget.peak, budget.limit });
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
}
