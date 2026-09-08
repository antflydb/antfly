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
const builtin = @import("builtin");
const crc32 = @import("crc32.zig");
const crc64 = @import("crc64.zig");
const adler = @import("adler32.zig");

test "checksum throughput microbenchmark" {
    if (builtin.mode != .ReleaseFast) return error.SkipZigTest;
    const bytes = try std.testing.allocator.alloc(u8, 1024 * 1024);
    defer std.testing.allocator.free(bytes);
    var random = std.Random.DefaultPrng.init(0x593);
    random.random().bytes(bytes);
    std.debug.print("checksum kernels crc32={s} crc32c={s} cpu={s}\n", .{
        @tagName(crc32.Crc32.implementation()), @tagName(crc32.Crc32c.implementation()), builtin.cpu.model.name,
    });
    inline for (.{
        .{ "crc32", std.hash.Crc32, crc32.Crc32 },
        .{ "crc32c", std.hash.crc.Crc32Iscsi, crc32.Crc32c },
        .{ "crc64nvme", crc64.Oracle, crc64.Crc64Nvme },
        .{ "adler32", std.hash.Adler32, adler.Adler32 },
    }) |case| {
        for ([_]usize{ 64, 4096, 1024 * 1024 }) |size| {
            for (0..3) |round| {
                var sums: [2]u64 = undefined;
                inline for (.{ case[1], case[2] }, 0..) |Impl, impl_index| {
                    const count = 32 * 1024 * 1024 / size;
                    var sum: u64 = 0;
                    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
                    for (0..count) |iteration| {
                        bytes[0] = @truncate(iteration + round);
                        sum +%= Impl.hash(bytes[0..size]);
                    }
                    const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
                    sums[impl_index] = sum;
                    std.debug.print("checksum={s} impl={s} size={} bytes={} elapsed_ns={} sum={}\n", .{
                        case[0], if (impl_index == 0) "std" else "antfly", size, size * count, elapsed, sum,
                    });
                }
                try std.testing.expectEqual(sums[0], sums[1]);
            }
        }
    }
}
