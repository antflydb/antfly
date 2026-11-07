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

//! Bounded statement notices survive result-arena unwinding. Transports own
//! this buffer only after the first notice and release it at the statement cut.
const std = @import("std");
pub const Severity = enum { notice, warning };
pub const Notice = struct {
    code: []const u8 = "00000",
    message: []const u8,
    severity: Severity = .notice,
    /// Public JSON retains its established code/message shape. Native wire
    /// protocols consume the internal severity without an undeclared API field.
    pub fn jsonStringify(self: Notice, writer: anytype) !void {
        try writer.write(.{ .code = self.code, .message = self.message });
    }
};
pub const Sink = struct { ptr: *anyopaque, emit: *const fn (*anyopaque, Notice) anyerror!void, reserve: ?*const fn (*anyopaque, usize, usize) anyerror!void = null };
pub const Buffer = struct {
    pub const max_count = 256;
    pub const max_bytes = 64 * 1024;
    const Entry = struct { code: [5]u8, severity: Severity, start: usize, len: usize };
    storage: [max_bytes]u8 = undefined,
    entries: [max_count]Entry = undefined,
    count: usize = 0,
    bytes: usize = 0,
    sent: usize = 0,
    pub fn reserve(self: *const Buffer, count: usize, bytes: usize) !void {
        if (count > max_count - self.count or bytes > max_bytes - self.bytes) return error.SqlProgramLimitExceeded;
    }
    pub fn append(self: *Buffer, notice: Notice) !void {
        if (notice.code.len != 5 or !std.unicode.utf8ValidateSlice(notice.message) or std.mem.indexOfScalar(u8, notice.message, 0) != null) return error.InvalidSqlBackendResponse;
        if (self.count >= max_count or notice.message.len > max_bytes - self.bytes) return error.SqlProgramLimitExceeded;
        @memcpy(self.storage[self.bytes..][0..notice.message.len], notice.message);
        self.entries[self.count] = .{ .code = notice.code[0..5].*, .start = self.bytes, .len = notice.message.len, .severity = notice.severity };
        self.count += 1;
        self.bytes += notice.message.len;
    }
    pub fn get(self: *const Buffer, ordinal: usize) Notice {
        const entry = &self.entries[ordinal];
        return .{ .code = &entry.code, .message = self.storage[entry.start..][0..entry.len], .severity = entry.severity };
    }
};

test "SQL notice buffer retains wire warning severity without extending public JSON" {
    const a = std.testing.allocator;
    var buffer: Buffer = .{};
    try buffer.append(.{ .code = "25P01", .message = "outside transaction", .severity = .warning });
    const notice = buffer.get(0);
    try std.testing.expectEqual(.warning, notice.severity);
    const json = try std.json.Stringify.valueAlloc(a, notice, .{});
    defer a.free(json);
    try std.testing.expectEqualStrings("{\"code\":\"25P01\",\"message\":\"outside transaction\"}", json);
    try buffer.append(.{ .message = "ordinary notice" });
    try std.testing.expectEqual(.notice, buffer.get(1).severity);
}
