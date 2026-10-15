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

//! Synchronous borrowed writer. Its receiver stays local to a whole export;
//! compiler-local writer errors cross the native boundary by name.
const std = @import("std");
const named = @import("named_callback.zig");
const Methods = struct {
    drain: *const fn (*std.Io.Writer, []const u8, []const []const u8, usize) anyerror!usize,
    flush: *const fn (*std.Io.Writer) anyerror!void,
};
const Boundary = named.Boundary(Methods);
pub const Borrow = struct {
    source: *std.Io.Writer,
    methods: Methods = .{ .drain = sourceDrain, .flush = sourceFlush },
    dispatch: named.Dispatch = Boundary.local_dispatch,
    pub fn init(source: *std.Io.Writer) Borrow {
        return .{ .source = source };
    }
    pub fn receive(self: Borrow) Receiver {
        return .{ .borrow = self };
    }
};
fn sourceDrain(source: *std.Io.Writer, pending: []const u8, data: []const []const u8, splat: usize) anyerror!usize {
    try source.writeAll(pending);
    return source.writeSplat(data, splat);
}
fn sourceFlush(source: *std.Io.Writer) anyerror!void {
    try source.flush();
}
pub const Receiver = struct {
    borrow: Borrow,
    buffer: [4096]u8 = undefined,
    interface: std.Io.Writer = undefined,
    pub fn writer(self: *Receiver) *std.Io.Writer {
        self.interface = .{ .buffer = &self.buffer, .vtable = &.{ .drain = drain, .flush = flush } };
        return &self.interface;
    }
    /// Deliver our pending bytes without prematurely flushing the caller's
    /// writer. The caller keeps its original buffering and file lifetime.
    pub fn finish(self: *Receiver) std.Io.Writer.Error!void {
        _ = try drain(&self.interface, &.{""}, 0);
    }
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Receiver = @fieldParentPtr("interface", w);
        const count = Boundary.call("drain", self.borrow.dispatch, self.borrow.methods.drain, .{ self.borrow.source, w.buffer[0..w.end], data, splat }) catch return error.WriteFailed;
        w.end = 0;
        return count;
    }
    fn flush(w: *std.Io.Writer) std.Io.Writer.Error!void {
        const self: *Receiver = @fieldParentPtr("interface", w);
        try self.finish();
        Boundary.call("flush", self.borrow.dispatch, self.borrow.methods.flush, .{self.borrow.source}) catch return error.WriteFailed;
    }
};
