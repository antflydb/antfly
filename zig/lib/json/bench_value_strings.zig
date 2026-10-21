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

//! Isolated value-string benchmark, with identical scanner state and inputs.
//! Run from zig/: zig run -O ReleaseFast --dep antfly_json
//! -Mroot=lib/json/bench_value_strings.zig -O ReleaseFast
//! -Mantfly_json=lib/json/src/mod.zig -- 10000
const std = @import("std");
const fast = @import("antfly_json").value_strings;
fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}
pub fn main(init: std.process.Init) !void {
    const a = init.arena.allocator();
    const args = try init.minimal.args.toSlice(a);
    const iterations = if (args.len > 1) try std.fmt.parseInt(usize, args[1], 10) else 10000;
    const Case = struct { name: []const u8, bytes: []const u8, borrow: bool };
    const plain = try a.alloc(u8, 4098);
    @memset(plain, 'x');
    plain[0] = '"';
    plain[plain.len - 1] = '"';
    const escaped = try a.alloc(u8, 34);
    escaped[0] = '"';
    escaped[33] = '"';
    for (0..16) |i| @memcpy(escaped[1 + i * 2 ..][0..2], "\\n");
    const large = try a.alloc(u8, 131074);
    large[0] = '"';
    large[large.len - 1] = '"';
    for (0..65536) |i| @memcpy(large[1 + i * 2 ..][0..2], "\\n");
    for ([_]Case{ .{ .name = "plain_selected_4k", .bytes = plain, .borrow = true }, .{ .name = "escaped_unused_16", .bytes = escaped, .borrow = false }, .{ .name = "escaped_unused_128k", .bytes = large, .borrow = false } }) |case| {
        const repeats = if (case.bytes.len > 65536) @max(1, iterations / 16) else iterations;
        for (0..9) |sample| for (0..2) |order| {
            const accelerated = (order == 0) == (sample % 2 == 0);
            var checksum: usize = 0;
            const started = now(init.io);
            for (0..repeats) |_| {
                var scanner = std.json.Scanner.initCompleteInput(a, case.bytes);
                defer scanner.deinit();
                _ = try scanner.peekNextTokenType();
                if (case.borrow) {
                    const bytes = if (accelerated) fast.borrow(&scanner).? else switch (try scanner.nextAllocMax(a, .alloc_if_needed, case.bytes.len)) {
                        .string => |bytes| bytes,
                        else => return error.UnexpectedToken,
                    };
                    checksum +%= bytes.len;
                } else {
                    if (!accelerated or !try fast.skip(&scanner)) try scanner.skipValue();
                    checksum +%= scanner.cursor;
                }
                if (try scanner.next() != .end_of_document) return error.UnexpectedToken;
            }
            const elapsed = now(init.io) - started;
            if (checksum != repeats * (if (case.borrow) case.bytes.len - 2 else case.bytes.len)) return error.InvalidChecksum;
            std.mem.doNotOptimizeAway(checksum);
            std.debug.print("{{\"case\":\"{s}\",\"sample\":{d},\"native\":{},\"iterations\":{d},\"bytes\":{d},\"ns\":{d},\"checksum\":{d}}}\n", .{ case.name, sample, accelerated, repeats, case.bytes.len, elapsed, checksum });
        };
    }
}
