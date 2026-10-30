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

//! Compare shared-recipe CPU lowering against the pre-migration scalar codec.
const std = @import("std");
const recipe = @import("graph/quant_recipe.zig");
const reference = @import("gguf/testing/quant_reference.zig");
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    inline for (recipe.formats) |format| {
        const r = comptime recipe.forFormat(format);
        const blocks = 2048;
        const raw = try allocator.alloc(u8, r.blockBytes() * blocks);
        defer allocator.free(raw);
        for (raw, 0..) |*v, i| v.* = @truncate(i * 73 + 19);
        const output = try allocator.alloc(f32, r.blockValues() * blocks);
        defer allocator.free(output);
        // Headers are powers of two so both schedules have exactly equal results.
        for (0..blocks) |b| {
            const block = raw[b * r.blockBytes() ..][0..r.blockBytes()];
            switch (format) {
                .q8_k => std.mem.writeInt(u32, block[0..4], @bitCast(@as(f32, 0.125)), .little),
                .q2_k => {
                    std.mem.writeInt(u16, block[16..18], 0x3000, .little);
                    std.mem.writeInt(u16, block[18..20], 0x2800, .little);
                },
                .q3_k => std.mem.writeInt(u16, block[108..110], 0x3000, .little),
                .q6_k => std.mem.writeInt(u16, block[208..210], 0x3000, .little),
                else => {
                    std.mem.writeInt(u16, block[0..2], 0x3000, .little);
                    if (format == .q4_1 or format == .q5_1 or format == .q4_k or format == .q5_k) std.mem.writeInt(u16, block[2..4], 0x2800, .little);
                },
            }
        }
        var medians: [2]f64 = undefined;
        var checksum: f64 = 0;
        // Alternate arms to reduce temperature/frequency bias; report median ns/value.
        var samples: [2][9]f64 = undefined;
        for (0..9) |arm| {
            for (0..2) |j| {
                const kind = (j + arm) % 2;
                const start = std.Io.Clock.awake.now(init.io).nanoseconds;
                for (0..40) |_| {
                    if (kind == 0) try reference.decode(format, raw, output) else try recipe.decode(format, raw, output);
                    std.mem.doNotOptimizeAway(output.ptr);
                }
                const elapsed = std.Io.Clock.awake.now(init.io).nanoseconds - start;
                samples[kind][arm] = @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(output.len * 40));
                checksum += output[arm];
            }
        }
        for (&samples, 0..) |*arm, kind| {
            std.mem.sort(f64, arm, {}, std.sort.asc(f64));
            medians[kind] = arm[4];
        }
        std.debug.print("{s} reference_ns_per_value={d:.4} shared_ns_per_value={d:.4} ratio={d:.4} checksum={d}\n", .{ @tagName(format), medians[0], medians[1], medians[1] / medians[0], checksum });
    }
}

test {
    _ = recipe;
}
