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

//! Count the actual escaped wire representation without allocating it.
//! Hosts preflight mutation output before publishing a durable decision.
const std = @import("std");
pub const Checkpoint = struct { ptr: *anyopaque, call: *const fn (*anyopaque) anyerror!void };
const Counter = struct {
    writer: std.Io.Writer,
    limit: usize,
    count: usize = 0,
    checked: usize = 0,
    checkpoint: ?Checkpoint,
    failure: ?anyerror = null,
    fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Counter = @alignCast(@fieldParentPtr("writer", writer));
        var written = std.math.mul(usize, data[data.len - 1].len, splat) catch return self.fail(error.CypherResponseTooLarge);
        for (data[0 .. data.len - 1]) |bytes| written = std.math.add(usize, written, bytes.len) catch return self.fail(error.CypherResponseTooLarge);
        const growth = std.math.add(usize, written, writer.end) catch return self.fail(error.CypherResponseTooLarge);
        if (growth > self.limit -| self.count) return self.fail(error.CypherResponseTooLarge);
        self.count += growth;
        writer.end = 0;
        if (self.count -| self.checked >= 64 * 1024) {
            if (self.checkpoint) |checkpoint| checkpoint.call(checkpoint.ptr) catch |err| return self.fail(err);
            self.checked = self.count;
        }
        return written;
    }
    fn fail(self: *Counter, err: anyerror) std.Io.Writer.Error {
        self.failure = err;
        return error.WriteFailed;
    }
};
pub fn checkSize(value: anytype, limit: usize, checkpoint: ?Checkpoint) !usize {
    var buffer: [1024]u8 = undefined;
    var counter: Counter = .{ .writer = .{ .buffer = &buffer, .vtable = &.{ .drain = Counter.drain } }, .limit = limit, .checkpoint = checkpoint };
    std.json.Stringify.value(value, .{}, &counter.writer) catch return counter.failure orelse error.CypherResponseTooLarge;
    counter.writer.flush() catch return counter.failure orelse error.CypherResponseTooLarge;
    if (checkpoint) |check| try check.call(check.ptr);
    return counter.count;
}
pub fn encode(allocator: std.mem.Allocator, value: anytype, limit: usize, checkpoint: ?Checkpoint) ![]u8 {
    const size = try checkSize(value, limit, checkpoint);
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    var writer = std.Io.Writer.fixed(bytes);
    std.json.Stringify.value(value, .{}, &writer) catch return error.CypherResponseTooLarge;
    if (writer.end != size) return error.CypherResponseTooLarge;
    return bytes;
}

test "bounded graph serialization counts escaped UTF-8 bytes exactly" {
    const value = .{ .text = "😀\n\"\\" };
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, value, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqual(encoded.len, try checkSize(value, encoded.len, null));
    try std.testing.expectError(error.CypherResponseTooLarge, checkSize(value, encoded.len - 1, null));
}
