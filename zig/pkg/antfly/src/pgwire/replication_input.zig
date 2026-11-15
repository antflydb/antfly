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

//! Bounded CopyBoth frontend framing. The reader task owns each returned
//! renewal buffer until the serialized coordinator securely releases it.
const std = @import("std");
const pgoutput = @import("pgoutput.zig");
const A = std.mem.Allocator;
pub const Input = union(enum) {
    feedback: pgoutput.Feedback,
    done,
    abort,
    terminate,
    renew: []u8,
    pub fn deinit(self: *Input, a: A) void {
        if (self.* == .renew) {
            std.crypto.secureZero(u8, self.renew);
            a.free(self.renew);
        }
        self.* = undefined;
    }
};
pub fn read(a: A, reader: *std.Io.Reader, renewable: bool) !Input {
    const tag = try reader.takeByte();
    const length = try reader.takeInt(u32, .big);
    if (length < 4) return error.ProtocolViolation;
    const size = length - 4;
    switch (tag) {
        'd' => {
            if (size != 34) return error.ProtocolViolation;
            var bytes: [34]u8 = undefined;
            try reader.readSliceAll(&bytes);
            return .{ .feedback = pgoutput.feedback(&bytes) catch return error.ProtocolViolation };
        },
        'c', 'X' => {
            if (size != 0) return error.ProtocolViolation;
            return if (tag == 'c') .done else .terminate;
        },
        'f' => {
            if (size == 0 or size > 1024) return error.ProtocolViolation;
            var buffer: [1024]u8 = undefined;
            const bytes = buffer[0..size];
            try reader.readSliceAll(bytes);
            if (bytes[bytes.len - 1] != 0 or std.mem.indexOfScalar(u8, bytes[0 .. bytes.len - 1], 0) != null or !std.unicode.utf8ValidateSlice(bytes[0 .. bytes.len - 1])) return error.ProtocolViolation;
            // Client messages are never reflected into errors or logs.
            return .abort;
        },
        'p' => {
            if (!renewable or size < 2 or size > 4097) return error.ProtocolViolation;
            const bytes = try a.alloc(u8, size);
            errdefer {
                std.crypto.secureZero(u8, bytes);
                a.free(bytes);
            }
            try reader.readSliceAll(bytes);
            if (bytes[bytes.len - 1] != 0 or std.mem.indexOfScalar(u8, bytes[0 .. bytes.len - 1], 0) != null) return error.ProtocolViolation;
            return .{ .renew = bytes };
        },
        else => return error.ProtocolViolation,
    }
}
test "replication input bounds feedback and preserves the next query after CopyDone" {
    const a = std.testing.allocator;
    var reader = std.Io.Reader.fixed("c\x00\x00\x00\x04Qremaining");
    var result = try read(a, &reader, false);
    defer result.deinit(a);
    try std.testing.expect(result == .done);
    try std.testing.expectEqual(@as(u8, 'Q'), try reader.takeByte());
    var status: [39]u8 = @splat(0);
    status[0] = 'd';
    status[4] = 38;
    status[5] = 'r';
    status[38] = 1;
    reader = .fixed(&status);
    var feedback = try read(a, &reader, false);
    defer feedback.deinit(a);
    try std.testing.expect(feedback.feedback.request_reply);
    status[38] = 2;
    reader = .fixed(&status);
    try std.testing.expectError(error.ProtocolViolation, read(a, &reader, false));
    for ([_][]const u8{ "d\x00\x00\x00\x03", "d\xff\xff\xff\xff", "d\x00\x00\x00\x05r", "c\x00\x00\x00\x05x", "h\x00\x00\x00\x04", "p\x00\x00\x00\x06x\x00" }) |bytes| {
        reader = .fixed(bytes);
        try std.testing.expectError(error.ProtocolViolation, read(a, &reader, false));
    }
}
fn renewalProbe(a: A) !void {
    var reader = std.Io.Reader.fixed("p\x00\x00\x00\x0atoken\x00");
    var input = try read(a, &reader, true);
    defer input.deinit(a);
    try std.testing.expectEqualStrings("token\x00", input.renew);
}
test "replication input renewal is owned bounded and released through allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, renewalProbe, .{});
    var reader = std.Io.Reader.fixed("p\x00\x00\x00\x06xy");
    try std.testing.expectError(error.ProtocolViolation, read(std.testing.allocator, &reader, true));
    reader = .fixed("p\x00\x00\x00\x08ab");
    try std.testing.expectError(error.EndOfStream, read(std.testing.allocator, &reader, true));
}
