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

//! Pull one projected mutation input while retaining normal physical prefetch.
//! Page leases outlive borrowed cells until the next pull; volatile expressions
//! advance only when requested by the statement's ordered producer.
const std = @import("std");
const runtime = @import("runtime.zig");
const catalog = @import("catalog.zig");
const Datum = @import("scalar.zig").Datum;

pub const Input = struct {
    context: runtime.Context,
    names: []const []const u8,
    pages: std.heap.ArenaAllocator,
    cursor: ?catalog.Cursor = null,
    page: ?catalog.Page = null,

    pub fn init(context: runtime.Context, metadata: std.mem.Allocator) !Input {
        const names = try metadata.alloc([]const u8, context.binding.columns.len);
        for (names, 0..) |*name, index| name.* = try std.fmt.allocPrint(metadata, "\x00query_cursor_{d}", .{index});
        return .{ .context = context, .names = names, .pages = .init(context.alloc) };
    }
    pub fn close(self: *Input) void {
        if (self.page) |page| page.deinit();
        if (self.cursor) |cursor| cursor.close(cursor.ptr);
        self.pages.deinit();
    }
    pub fn next(self: *Input, row: std.mem.Allocator) !?[]const Datum {
        if (self.cursor == null) {
            var context = self.context;
            context.typed_output = true;
            context.limits.result_rows = context.limits.scan_rows;
            self.cursor = try @import("relation_runtime.zig").openMutationCursor(context);
        }
        while (true) {
            try self.context.checkpoint();
            if (self.page) |page| page.deinit();
            self.page = null;
            if (!self.pages.reset(.{ .retain_with_limit = 32 * 1024 })) return error.OutOfMemory;
            const a = self.pages.allocator();
            const values = try row.alloc(Datum, self.names.len);
            if (self.cursor.?.next_columns) |pull| {
                const page = try pull(self.cursor.?.ptr, a, 1);
                try page.validate();
                if (page.selection.len == 0) {
                    if (page.after != null) continue;
                    return null;
                }
                if (page.selection.len != 1) return error.InvalidSqlBackendResponse;
                for (values, self.names) |*value, name| value.* = try page.cell(row, 0, name);
            } else {
                self.page = try self.cursor.?.next(self.cursor.?.ptr, a, 1);
                if (self.page.?.rows.len == 0) {
                    if (self.page.?.after != null) continue;
                    return null;
                }
                if (self.page.?.rows.len != 1) return error.InvalidSqlBackendResponse;
                for (values, self.names) |*value, name| value.* = try self.page.?.rows[0].cell(name);
            }
            return values;
        }
    }
};
