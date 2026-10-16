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

//! Producers borrow snapshot-owned rows/pages through a synchronous callback.
const std = @import("std");
const Plan = @import("relational_projection.zig").Plan;
const codec = @import("../algebraic/relational_row_codec.zig");
const document = @import("../document_query.zig");
const native_writer = @import("../../native_writer.zig");
pub const Context = struct {
    scratch: std.mem.Allocator,
    value: union(enum) {
        row: struct { plan: Plan, row: codec.OrdinalRowView },
        sources: struct { plan: Plan, sources: Plan.Sources },
        document: struct { plan: *const document.SpanProjectionPlan, raw: []const u8 },
        json_fields: []const u8,
    },
    pub fn borrow(self: *@This()) @import("../types.zig").BorrowedScanDocument {
        return .{ .context = self, .write_fields_fn = writeFields };
    }
    fn writeFields(ptr: ?*anyopaque, destination: native_writer.Borrow) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr orelse return error.InvalidArgument));
        var receiver = destination.receive();
        const writer = receiver.writer();
        switch (self.value) {
            .row => |v| try v.plan.writeRowFields(self.scratch, v.row, writer),
            .sources => |v| try v.plan.writeSourcesFields(self.scratch, v.sources, writer),
            .document => |v| try document.writeLookupFields(self.scratch, writer, v.raw, v.plan),
            .json_fields => |raw| try document.writeRawScanFields(self.scratch, writer, raw),
        }
        try receiver.finish();
    }
};

test "public graph metric borrowed wire synchronous scan projection streams bounded fields and retries after failure" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 1024 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    @memcpy(text[0..12], "\"\\\n\u{1}é中xyz");
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"_id\":\"stored\",\"title\":");
    try std.json.Stringify.value(text, .{}, &input.writer);
    try input.writer.writeAll(",\"nested\":{\"_id\":\"inner\",\"n\":1e2}}");
    const opts: @import("../types.zig").LookupOptions = .{ .fields = &.{ "_id", "title", "nested" }, .include_all_fields = false };
    var plan = (try document.SpanProjectionPlan.init(a, opts)).?;
    defer plan.deinit();
    var legacy = try document.lookupJson(a, input.written(), opts);
    defer legacy.deinit(a);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, legacy.json, .{});
    defer parsed.deinit();
    _ = parsed.value.object.swapRemove("_id");
    // Preserve projection order while dropping only the root _id.
    const want_title = parsed.value.object.get("title").?;
    const want_nested = parsed.value.object.get("nested").?;
    var expected = std.Io.Writer.Allocating.init(a);
    defer expected.deinit();
    try expected.writer.writeAll(",\"title\":");
    try std.json.Stringify.value(want_title, .{}, &expected.writer);
    try expected.writer.writeAll(",\"nested\":");
    try std.json.Stringify.value(want_nested, .{}, &expected.writer);
    var no_resize: @import("../../lite/test_allocator.zig").NoResizeAllocator = .{ .backing = a };
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = no_resize.allocator(), .limit = 32 * 1024 };
    var context: Context = .{ .scratch = budget.allocator(), .value = .{ .document = .{ .raw = input.written(), .plan = &plan } } };
    var failed = std.Io.Writer.fixed(&.{});
    try std.testing.expectError(error.WriteFailed, context.borrow().writeFields(&failed));
    for (0..2) |_| {
        var output = std.Io.Writer.Allocating.init(a);
        defer output.deinit();
        try context.borrow().writeFields(&output.writer);
        try std.testing.expectEqualStrings(expected.written(), output.written());
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
}
