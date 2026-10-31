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

//! Experimental bounded external permutation for publication qualification.
//! This is intentionally benchmark-only: the additional sorting and writes must
//! earn their cost before a production planner may select this strategy.
const std = @import("std");
const local = @import("antfly_local_sources");
const spill = local.sql_spill;

/// Consume one native encoded-key reference stream; the caller owns the
/// returned sort and keeps the original alive until this call completes.
pub fn repack(original: *spill.Sort) !spill.Sort {
    try original.finish();
    if (original.order_sort == null or !original.encoded_bytes or original.orders.len != 1) return error.UnsupportedSpillPermutation;
    const manager = original.manager;
    const a = manager.allocator();
    var physical = spill.Sort.init(a, manager, &.{ .{}, .{} }, original.memory_bytes);
    defer physical.deinit();
    physical.allow_indirect = false;
    physical.shared_runs = true;
    physical.parallel_runs = false;
    while (true) {
        const page = try original.order_sort.?.nextPageLeased(64, 128 << 10);
        defer page.release();
        if (page.rows.len == 0) break;
        var values: [64][1]local.sql_scalar.Datum = undefined;
        var rows: [64][]const local.sql_scalar.Datum = undefined;
        var keys: [64][2]local.sql_scalar.Datum = undefined;
        var key_rows: [64][]const local.sql_scalar.Datum = undefined;
        var ordinals: [64]u64 = undefined;
        for (page.rows, 0..) |lease, i| {
            values[i] = .{try lease.keyCell(0)};
            rows[i] = &values[i];
            keys[i] = .{ try lease.cell(0), try lease.cell(1) };
            key_rows[i] = &keys[i];
            ordinals[i] = lease.row.ordinal;
        }
        try physical.addBatch(.{ .rows = rows[0..page.rows.len] }, key_rows[0..page.rows.len], ordinals[0..page.rows.len]);
    }
    var output = spill.Sort.init(a, manager, original.orders, original.memory_bytes);
    errdefer output.deinit();
    output.allow_indirect = false;
    output.shared_runs = true;
    output.encoded_bytes = true;
    output.parallel_runs = false;
    const reader = try original.payloadReader();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    while (true) {
        const page = try physical.nextPageLeased(64, 128 << 10);
        defer page.release();
        if (page.rows.len == 0) break;
        var begin: usize = 0;
        while (begin < page.rows.len) {
            _ = scratch.reset(.free_all);
            const reference = try spill.PayloadReader.Reference.decode(&.{ try page.rows[begin].keyCell(0), try page.rows[begin].keyCell(1) });
            var end = begin + 1;
            while (end < page.rows.len and (try page.rows[end].keyCell(0)).value.integer == @as(i64, @intCast(reference.offset))) : (end += 1) {}
            const payload = try reader.read(reference);
            defer payload.release();
            var indices: [64]usize = undefined;
            var keys: [64][1]local.sql_scalar.Datum = undefined;
            var key_rows: [64][]const local.sql_scalar.Datum = undefined;
            var ordinals: [64]u64 = undefined;
            for (page.rows[begin..end], 0..) |lease, i| {
                indices[i] = @intCast((try lease.keyCell(1)).value.integer);
                keys[i] = .{try lease.cell(0)};
                key_rows[i] = &keys[i];
                ordinals[i] = lease.row.ordinal;
            }
            const source = try payload.block.batch(scratch.allocator(), false);
            try output.addBatch(try source.select(scratch.allocator(), indices[0 .. end - begin]), key_rows[0 .. end - begin], ordinals[0 .. end - begin]);
            begin = end;
        }
    }
    try output.finish();
    return output;
}
