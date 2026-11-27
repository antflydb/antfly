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

//! Seed planning request: the `{inserts, transforms}` subset of a public batch
//! whose artifact seeds should be planned. Planning runs the exact producer
//! preparation of the write and reports one outcome per seeded output
//! (`accepted | unchanged | conflict | replaced | deleted`) without committing.
//! It is validation-only and is also how writes report per-item outcomes.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("mutation_values.zig");
const TransactionVersionPredicate = @import("types.zig").TransactionVersionPredicate;

pub const max_plan_documents: usize = 1024;

pub const Request = struct {
    arena: std.heap.ArenaAllocator,
    writes: []types.BatchWrite = &.{},
    transforms: []types.DocumentTransform = &.{},
    /// Lowered `expected_version` of each transform that names one.
    predicates: []TransactionVersionPredicate = &.{},

    pub fn deinit(self: *Request) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn opFromText(text: []const u8) ?types.TransformOpType {
    inline for (@typeInfo(types.TransformOpType).@"enum".field_names) |name| {
        const op = @field(types.TransformOpType, name);
        if (std.mem.eql(u8, types.transformOpText(op), text)) return op;
    }
    return null;
}

pub fn parse(alloc: Allocator, json: []const u8) !Request {
    var out: Request = .{ .arena = std.heap.ArenaAllocator.init(alloc) };
    errdefer out.arena.deinit();
    const a = out.arena.allocator();
    const root = std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }) catch return error.InvalidArgument;
    if (root != .object) return error.InvalidArgument;
    var it = root.object.iterator();
    while (it.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "inserts") and !std.mem.eql(u8, entry.key_ptr.*, "transforms")) return error.InvalidArgument;
    }
    if (root.object.get("inserts")) |inserts| if (inserts != .null) {
        if (inserts != .object or inserts.object.count() > max_plan_documents) return error.InvalidArgument;
        const writes = try a.alloc(types.BatchWrite, inserts.object.count());
        var writes_it = inserts.object.iterator();
        var i: usize = 0;
        while (writes_it.next()) |entry| : (i += 1) {
            if (entry.value_ptr.* != .object) return error.InvalidArgument;
            writes[i] = .{ .key = entry.key_ptr.*, .value = try std.json.Stringify.valueAlloc(a, entry.value_ptr.*, .{}) };
        }
        out.writes = writes;
    };
    if (root.object.get("transforms")) |transforms| if (transforms != .null) {
        if (transforms != .array or transforms.array.items.len > max_plan_documents) return error.InvalidArgument;
        const parsed = try a.alloc(types.DocumentTransform, transforms.array.items.len);
        var predicates = std.ArrayListUnmanaged(TransactionVersionPredicate).empty;
        for (transforms.array.items, parsed) |item, *transform| {
            if (item != .object) return error.InvalidArgument;
            if (item.object.get("expected_version")) |raw| if (raw != .null) {
                const version: u64 = switch (raw) {
                    .number_string, .string => |text| std.fmt.parseUnsigned(u64, text, 10) catch return error.InvalidArgument,
                    else => return error.InvalidArgument,
                };
                if (version == 0) return error.InvalidArgument;
                const target = item.object.get("key") orelse return error.InvalidArgument;
                if (target != .string) return error.InvalidArgument;
                try predicates.append(a, .{ .key = target.string, .expected_version = version });
            };
            const key = item.object.get("key") orelse return error.InvalidArgument;
            const operations = item.object.get("operations") orelse return error.InvalidArgument;
            if (key != .string or operations != .array) return error.InvalidArgument;
            const ops = try a.alloc(types.TransformOp, operations.array.items.len);
            for (operations.array.items, ops) |raw, *op| {
                if (raw != .object) return error.InvalidArgument;
                const name = raw.object.get("op") orelse return error.InvalidArgument;
                const path = raw.object.get("path") orelse return error.InvalidArgument;
                if (name != .string or path != .string) return error.InvalidArgument;
                op.* = .{
                    .op = opFromText(name.string) orelse return error.InvalidArgument,
                    .path = path.string,
                    .value_json = if (raw.object.get("value")) |value| try std.json.Stringify.valueAlloc(a, value, .{}) else null,
                };
            }
            const upsert = if (item.object.get("upsert")) |flag| switch (flag) {
                .bool => |value| value,
                .null => false,
                else => return error.InvalidArgument,
            } else false;
            transform.* = .{ .key = key.string, .operations = ops, .upsert = upsert };
        }
        out.transforms = parsed;
        out.predicates = predicates.items;
    };
    return out;
}

test "seed plan requests parse the public insert and transform subset" {
    const alloc = std.testing.allocator;
    var request = try parse(alloc,
        \\{"inserts":{"doc:a":{"t":1,"_artifacts":{"s":{"value":"x"}}}},"transforms":[{"key":"doc:b","operations":[{"op":"$set","path":"_chunks.c","value":[]}],"upsert":false}]}
    );
    defer request.deinit();
    try std.testing.expectEqual(@as(usize, 1), request.writes.len);
    try std.testing.expectEqualStrings("doc:a", request.writes[0].key);
    try std.testing.expect(std.mem.indexOf(u8, request.writes[0].value, "_artifacts") != null);
    try std.testing.expectEqual(types.TransformOpType.set, request.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("[]", request.transforms[0].operations[0].value_json.?);
    for ([_][]const u8{ "{\"deletes\":[]}", "{\"inserts\":{\"a\":1}}", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$nope\",\"path\":\"x\"}]}]}" }) |bad|
        try std.testing.expectError(error.InvalidArgument, parse(alloc, bad));
}
