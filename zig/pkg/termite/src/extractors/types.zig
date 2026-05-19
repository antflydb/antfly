// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub const StructuredField = struct {
    name: []const u8,
    value: StructuredValue,

    pub fn deinit(self: *StructuredField, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.value.deinit(allocator);
    }
};

pub const StructuredValue = union(enum) {
    string: []const u8,
    number: f64,
    boolean: bool,
    object: []StructuredField,
    array: []StructuredValue,
    null,

    pub fn deinit(self: *StructuredValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            .object => |fields| {
                for (fields) |*field| field.deinit(allocator);
                allocator.free(fields);
            },
            .array => |values| {
                for (values) |*value| value.deinit(allocator);
                allocator.free(values);
            },
            .number, .boolean, .null => {},
        }
    }

    pub fn cloneFromJsonValue(allocator: std.mem.Allocator, value: std.json.Value) !StructuredValue {
        return switch (value) {
            .null => .null,
            .bool => |b| .{ .boolean = b },
            .integer => |n| .{ .number = @floatFromInt(n) },
            .float => |n| .{ .number = @floatCast(n) },
            .number_string => |s| blk: {
                const parsed = std.fmt.parseFloat(f64, s) catch 0.0;
                break :blk .{ .number = parsed };
            },
            .string => |s| .{ .string = try allocator.dupe(u8, s) },
            .array => |items| blk: {
                const out = try allocator.alloc(StructuredValue, items.items.len);
                var initialized: usize = 0;
                errdefer {
                    for (out[0..initialized]) |*item| item.deinit(allocator);
                    allocator.free(out);
                }
                for (items.items, 0..) |item, i| {
                    out[i] = try cloneFromJsonValue(allocator, item);
                    initialized += 1;
                }
                break :blk .{ .array = out };
            },
            .object => |obj| blk: {
                const out = try allocator.alloc(StructuredField, obj.count());
                var initialized: usize = 0;
                errdefer {
                    for (out[0..initialized]) |*field| field.deinit(allocator);
                    allocator.free(out);
                }

                var it = obj.iterator();
                while (it.next()) |entry| {
                    out[initialized] = .{
                        .name = try allocator.dupe(u8, entry.key_ptr.*),
                        .value = try cloneFromJsonValue(allocator, entry.value_ptr.*),
                    };
                    initialized += 1;
                }
                break :blk .{ .object = out };
            },
        };
    }
};

test "StructuredValue clones nested json objects" {
    const allocator = std.testing.allocator;

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"description":"receipt","score":0.9,"flags":[true,false],"meta":{"source":"photo"}}
    , .{});
    defer parsed.deinit();

    var value = try StructuredValue.cloneFromJsonValue(allocator, parsed.value);
    defer value.deinit(allocator);

    try std.testing.expect(value == .object);
    try std.testing.expectEqual(@as(usize, 4), value.object.len);
}
