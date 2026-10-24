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

//! Owned JSON DOMs from a synchronous consumer's admitted allocator. Scanner
//! input is bounded per refill; no work authority or input buffer escapes.
const std = @import("std");
const Work = @import("operator_work.zig");

pub fn parseLeaky(a: std.mem.Allocator, text: []const u8, control: ?Work.Control) !std.json.Value {
    if (control == null) return std.json.parseFromSliceLeaky(std.json.Value, a, text, .{ .parse_numbers = false });
    var input: Input = .{ .text = text, .control = control };
    input.reader.buffer = &input.buffer;
    var tokens = std.json.Reader.init(a, &input.reader);
    defer tokens.deinit();
    try Work.poll(control);
    const value = std.json.parseFromTokenSourceLeaky(std.json.Value, a, &tokens, .{
        .parse_numbers = false,
        .allocate = .alloc_always,
        .max_value_len = text.len,
    }) catch |err| return input.failure orelse err;
    try Work.poll(control);
    return value;
}

const Input = struct {
    text: []const u8,
    control: ?Work.Control,
    offset: usize = 0,
    failure: ?anyerror = null,
    buffer: [4096]u8 = undefined,
    reader: std.Io.Reader = .{
        .vtable = &.{ .stream = std.Io.Reader.failing.vtable.stream, .readVec = refill },
        .buffer = &.{},
        .seek = 0,
        .end = 0,
    },
    fn refill(reader: *std.Io.Reader, _: [][]u8) std.Io.Reader.Error!usize {
        const self: *Input = @fieldParentPtr("reader", reader);
        Work.poll(self.control) catch |err| {
            self.failure = err;
            return error.ReadFailed;
        };
        if (self.offset == self.text.len) return error.EndOfStream;
        const count = @min(self.buffer.len, self.text.len - self.offset);
        @memcpy(self.buffer[0..count], self.text[self.offset..][0..count]);
        self.offset += count;
        reader.seek = 0;
        reader.end = count;
        return 0;
    }
};
