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

//! Synchronous, borrowed NDJSON consumer. The callback owner supplies the
//! dispatcher, including when this sink is nested inside another runtime call.
//! Byte slices stay zero-copy; compilation-local errors never cross archives.
const std = @import("std");
const boundary = @import("runtime_callback_abi.zig");

pub const ScanStreamSink = struct {
    /// Fixed-buffer adapter. Stable storage and failure identity stay with the
    /// synchronous consumer; producers never see its local writer vtable.
    pub const Writer = struct {
        sink: ScanStreamSink = undefined,
        buffer: [4096]u8 = undefined,
        writer: std.Io.Writer = undefined,
        failure: ?anyerror = null,
        pub fn init(self: *@This(), sink: ScanStreamSink) void {
            self.sink = sink;
            self.failure = null;
            self.writer = .{ .vtable = &.{ .drain = drain }, .buffer = &self.buffer };
        }
        fn send(self: *@This(), bytes: []const u8) std.Io.Writer.Error!void {
            self.sink.write(bytes) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
        }
        fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
            try self.send(w.buffered());
            w.end = 0;
            var n: usize = 0;
            for (data[0 .. data.len - 1]) |bytes| {
                try self.send(bytes);
                n += bytes.len;
            }
            const pattern = data[data.len - 1];
            for (0..splat) |_| try self.send(pattern);
            return n + pattern.len * splat;
        }
        pub fn finish(self: *@This()) !void {
            self.writer.flush() catch |err| return self.failure orelse err;
        }
    };
    const VTable = struct {
        start: *const fn (?*anyopaque) anyerror!void,
        write: *const fn (?*anyopaque, []const u8) anyerror!void,
    };
    const Abi = boundary.Boundary(VTable);

    context: ?*anyopaque,
    start_fn: @FieldType(VTable, "start"),
    write_fn: @FieldType(VTable, "write"),
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,

    /// Called once after routing validates the table, even for an empty scan.
    pub fn start(self: ScanStreamSink) !void {
        try Abi.call("start", self.boundary_dispatch, self.start_fn, .{self.context});
    }

    /// Completion supplies backpressure; errors stop iteration immediately.
    /// The consumer must not retain the borrowed bytes after returning.
    pub fn write(self: ScanStreamSink, bytes: []const u8) !void {
        if (bytes.len != 0)
            try Abi.call("write", self.boundary_dispatch, self.write_fn, .{ self.context, bytes });
    }
};
