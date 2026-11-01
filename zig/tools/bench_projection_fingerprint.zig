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

//! Native canonical digest comparison. Build with --dep identity, pointing
//! identity at pkg/antfly-embedded/src/common/value_identity.zig. JSON lines go
//! to stderr; synthetic corpus generation is outside the measured allocator.
//! Isolate canonical fingerprint allocation and short-write buffering.
const std = @import("std");
const Sha = std.crypto.hash.sha2.Sha256;
pub fn main(init: std.process.Init) !void {
    const a = init.arena.allocator();
    for ([_]usize{ 64, 262144 }) |size| {
        const text = try a.alloc(u8, size);
        @memset(text, 'x');
        const fields: [64][]const u8 = @splat("metadata.revision");
        const request = .{ .fields = fields, .params = .{ .known = text } };
        const encoded = try std.json.Stringify.valueAlloc(a, request, .{});
        var expected: [32]u8 = undefined;
        Sha.hash(encoded, &expected, .{});
        for (0..5) |round| for (0..3) |slot| {
            const mode = if (round % 2 == 0) slot else 2 - slot;
            var measured = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const alloc = measured.allocator();
            const started = std.Io.Clock.awake.now(init.io).nanoseconds;
            for (0..128) |_| {
                var result: [32]u8 = undefined;
                if (mode == 0) {
                    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
                    defer alloc.free(bytes);
                    Sha.hash(bytes, &result, .{});
                } else {
                    var buffer: [4096]u8 = undefined;
                    var hasher = std.Io.Writer.Hashing(Sha).init(if (mode == 1) &.{} else &buffer);
                    try std.json.Stringify.value(request, .{}, &hasher.writer);
                    try hasher.writer.flush();
                    result = hasher.hasher.finalResult();
                }
                if (!std.mem.eql(u8, &expected, &result)) return error.DigestMismatch;
                std.mem.doNotOptimizeAway(&result);
            }
            if (measured.allocated_bytes != measured.freed_bytes) return error.LeakedAllocation;
            std.debug.print("size={d} round={d} mode={d} ns={d} allocations={d} bytes={d}\n", .{ size, round, mode, std.Io.Clock.awake.now(init.io).nanoseconds - started, measured.allocations, measured.allocated_bytes });
        };
    }
}
