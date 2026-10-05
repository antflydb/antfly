// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Isolates the storage-key preparation used by real document collectors.
const std = @import("std");
const Counter = @import("antfly_local_sources").allocation_bench_support.Counter;
const Scratch = @import("antfly_local_sources").storage_db_lookup_key_scratch.Scratch;
const keys = @import("antfly_local_sources").storage_internal_keys;
const time = @import("antfly_platform").time;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const count = if (args.len > 1) try std.fmt.parseInt(usize, args[1], 10) else 50_000;
    const batch = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 256;
    if (batch == 0) return error.InvalidBatch;
    const setup = std.heap.smp_allocator;
    const names = try setup.alloc([24]u8, count);
    defer setup.free(names);
    const inputs = try setup.alloc([]const u8, count);
    defer setup.free(inputs);
    var expected: usize = 0;
    for (names, inputs, 0..) |*name, *input, i| {
        input.* = try std.fmt.bufPrint(name, "document-{d:0>10}", .{i});
        const encoded = try keys.documentKeyAlloc(setup, input.*);
        defer setup.free(encoded);
        for (encoded) |byte| expected +%= byte;
    }
    const output = try setup.alloc([]const u8, batch);
    defer setup.free(output);
    for ([_]bool{ true, false }) |counting| {
        for (0..6) |sample| {
            for (0..2) |order| {
                const pooled = (order == 0) == (sample % 2 == 0);
                var counter: Counter = .{};
                const alloc = if (counting) counter.allocator() else std.heap.smp_allocator;
                var checksum: usize = 0;
                const started = time.monotonicNs();
                var offset: usize = 0;
                while (offset < count) {
                    var scratch = Scratch.init(alloc, @min(batch, count - offset));
                    defer scratch.deinit();
                    const end = @min(count, offset + batch);
                    var initialized: usize = 0;
                    defer if (!pooled) {
                        for (output[0..initialized]) |key| alloc.free(key);
                    };
                    for (inputs[offset..end], output[0 .. end - offset]) |input, *key| {
                        key.* = if (pooled) try scratch.key(input, false) else try keys.documentKeyAlloc(alloc, input);
                        initialized += 1;
                    }
                    for (output[0..initialized]) |key| for (key) |byte| {
                        checksum +%= byte;
                    };
                    offset = end;
                }
                const elapsed = time.monotonicNs() - started;
                if (checksum != expected or counter.live != 0) return error.InvalidResult;
                if (sample != 0) std.debug.print("{{\"sample\":{d},\"pooled\":{},\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d}}}\n", .{ sample, pooled, if (counting) "counted" else "timing", elapsed, counter.calls, counter.bytes, counter.peak, checksum });
            }
        }
    }
}
