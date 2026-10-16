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

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Emit into the caller's owner without a response-sized scratch buffer.
/// Emitters borrow stable input for this synchronous call and support replay.
/// Small exact bodies remain buffered; larger ones use bounded sizing followed
/// by direct emission into their sole final allocation.
pub fn alloc(output: Allocator, exact: bool, emitter: anytype) ![]u8 {
    const reclaimable = @import("antfly_platform").allocator.reclaimingScratch(output);
    const arena_output = exact or reclaimable.ptr != output.ptr or reclaimable.vtable != output.vtable;
    if (!arena_output) {
        var writer = std.Io.Writer.Allocating.init(output);
        defer writer.deinit();
        emitter.write(&writer.writer) catch |err| switch (err) {
            error.WriteFailed => return error.OutOfMemory,
            else => return err,
        };
        return writer.toOwnedSlice();
    }
    var buffer: [4096]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try emitter.write(&counter.writer);
    if (counter.count == 0) return output.dupe(u8, counter.writer.buffered());
    const size = std.math.cast(usize, counter.fullCount()) orelse return error.OutOfMemory;
    const result = try output.alloc(u8, size);
    errdefer output.free(result);
    var writer = std.Io.Writer.fixed(result);
    emitter.write(&writer) catch |err| switch (err) {
        error.WriteFailed => return error.OutputSizeChanged,
        else => return err,
    };
    if (writer.end != result.len) return error.OutputSizeChanged;
    return result;
}
