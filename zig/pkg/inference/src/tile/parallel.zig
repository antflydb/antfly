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

//! CPU "grid launch": split `items` across threads in contiguous ranges and
//! call `body(ctx, begin, end)` for each. Small launches run inline.

const std = @import("std");

pub const max_threads = 64;

var configured_threads: usize = 0;

/// 0 = one per CPU.
pub fn setThreads(n: usize) void {
    configured_threads = n;
}

pub fn threadCount() usize {
    if (configured_threads != 0) return @min(configured_threads, max_threads);
    return @min(std.Thread.getCpuCount() catch 1, max_threads);
}

pub fn forRanges(items: usize, min_per_thread: usize, ctx: anytype, comptime body: fn (@TypeOf(ctx), usize, usize) void) void {
    const want = @max(@as(usize, 1), @min(threadCount(), items / @max(min_per_thread, 1)));
    if (want <= 1) return body(ctx, 0, items);
    var threads: [max_threads]?std.Thread = @splat(null);
    const chunk = (items + want - 1) / want;
    var begin: usize = chunk;
    var t: usize = 1;
    while (begin < items) : ({
        begin += chunk;
        t += 1;
    }) {
        threads[t] = std.Thread.spawn(.{}, body, .{ ctx, begin, @min(begin + chunk, items) }) catch blk: {
            body(ctx, begin, @min(begin + chunk, items));
            break :blk null;
        };
    }
    body(ctx, 0, @min(chunk, items));
    for (threads[1..t]) |th| if (th) |h| h.join();
}
