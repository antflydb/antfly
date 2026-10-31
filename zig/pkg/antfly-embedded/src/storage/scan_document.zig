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

//! A synchronous borrow valid only during one ScanVisitor callback. The row,
//! schema and decoded pages remain pinned until writeFields returns. Consumers
//! must not retain the producer; errors cross archives through named dispatch.
const std = @import("std");
const named = @import("named_callback.zig");
const native_writer = @import("native_writer.zig");
const Methods = struct {
    write_fields: *const fn (?*anyopaque, native_writer.Borrow) anyerror!void,
    count_projection: *const fn (?*anyopaque, std.mem.Allocator, []const []const u8) anyerror!?std.json.Value,
};
const Boundary = named.Boundary(Methods);
pub const Borrowed = struct {
    pub const WriterBorrow = native_writer.Borrow;
    context: ?*anyopaque,
    write_fields_fn: @FieldType(Methods, "write_fields"),
    count_projection_fn: ?@FieldType(Methods, "count_projection") = null,
    callback_dispatch: named.Dispatch = Boundary.local_dispatch,
    /// Trusted owner COUNT only: selected count-only properties become true or
    /// null after value validation. Other fields retain their actual values.
    /// No returned scalar or container may escape the enclosing scan callback.
    pub fn countProjection(self: Borrowed, a: std.mem.Allocator, presence: []const []const u8) !?std.json.Value {
        const callback = self.count_projection_fn orelse return null;
        return Boundary.call("count_projection", self.callback_dispatch, callback, .{ self.context, a, presence });
    }
    /// Emit comma-prefixed root fields, excluding the transport-owned _id.
    pub fn writeFields(self: Borrowed, writer: *std.Io.Writer) !void {
        try Boundary.call("write_fields", self.callback_dispatch, self.write_fields_fn, .{ self.context, native_writer.Borrow.init(writer) });
    }
};
