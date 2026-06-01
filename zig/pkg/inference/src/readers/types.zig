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
const extractor_types = @import("../extractors/types.zig");

pub const Field = struct {
    name: []const u8,
    value: []const u8,

    pub fn deinit(self: *Field, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.value);
    }
};

pub const StructuredField = extractor_types.StructuredField;
pub const StructuredValue = extractor_types.StructuredValue;

pub const Region = struct {
    text: []const u8,
    bbox: [4]f64,
    confidence: ?f32 = null,
    label: ?[]const u8 = null,

    pub fn deinit(self: *Region, allocator: std.mem.Allocator) void {
        allocator.free(self.text);
        if (self.label) |label| allocator.free(label);
    }
};

pub const Result = struct {
    text: []const u8,
    fields: []Field = &.{},
    regions: []Region = &.{},
    structured: ?StructuredValue = null,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result) void {
        self.allocator.free(self.text);
        for (self.fields) |*field| field.deinit(self.allocator);
        if (self.fields.len > 0) self.allocator.free(self.fields);
        for (self.regions) |*region| region.deinit(self.allocator);
        if (self.regions.len > 0) self.allocator.free(self.regions);
        if (self.structured) |*value| value.deinit(self.allocator);
    }
};

pub const ReadOptions = struct {
    prompt: ?[]const u8 = null,
    max_tokens: ?usize = null,
    cache_dtype: ?[]const u8 = null,
};

pub fn flattenStructuredToFields(allocator: std.mem.Allocator, value: *const StructuredValue) ![]Field {
    var fields = std.ArrayListUnmanaged(Field).empty;
    errdefer {
        for (fields.items) |*field| field.deinit(allocator);
        fields.deinit(allocator);
    }

    try appendStructuredFields(allocator, &fields, value, "");
    return try fields.toOwnedSlice(allocator);
}

pub fn structuredFromFields(allocator: std.mem.Allocator, fields: []const Field) !?StructuredValue {
    if (fields.len == 0) return null;

    var root_fields = std.ArrayListUnmanaged(StructuredField).empty;
    errdefer {
        for (root_fields.items) |*field| field.deinit(allocator);
        root_fields.deinit(allocator);
    }

    for (fields) |field| {
        var parts = std.ArrayListUnmanaged([]const u8).empty;
        defer parts.deinit(allocator);

        var iter = std.mem.splitScalar(u8, field.name, '.');
        while (iter.next()) |part| {
            const trimmed = std.mem.trim(u8, part, " \t\r\n");
            if (trimmed.len == 0) continue;
            try parts.append(allocator, trimmed);
        }
        if (parts.items.len == 0) continue;
        try insertStructuredFieldPath(allocator, &root_fields, parts.items, field.value);
    }

    return .{ .object = try root_fields.toOwnedSlice(allocator) };
}

fn appendStructuredFields(
    allocator: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(Field),
    value: *const StructuredValue,
    prefix: []const u8,
) !void {
    switch (value.*) {
        .null => {},
        .string => |s| if (prefix.len > 0) try fields.append(allocator, .{
            .name = try allocator.dupe(u8, prefix),
            .value = try allocator.dupe(u8, s),
        }),
        .number => |n| if (prefix.len > 0) try fields.append(allocator, .{
            .name = try allocator.dupe(u8, prefix),
            .value = try std.fmt.allocPrint(allocator, "{d}", .{n}),
        }),
        .boolean => |b| if (prefix.len > 0) try fields.append(allocator, .{
            .name = try allocator.dupe(u8, prefix),
            .value = try allocator.dupe(u8, if (b) "true" else "false"),
        }),
        .object => |entries| {
            for (entries) |entry| {
                const next_prefix = if (prefix.len == 0)
                    try allocator.dupe(u8, entry.name)
                else
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, entry.name });
                defer allocator.free(next_prefix);
                try appendStructuredFields(allocator, fields, &entry.value, next_prefix);
            }
        },
        .array => |items| {
            if (prefix.len == 0) return;
            if (items.len == 0) return;

            var all_scalar = true;
            for (items) |item| {
                switch (item) {
                    .string, .number, .boolean, .null => {},
                    else => {
                        all_scalar = false;
                        break;
                    },
                }
            }

            if (all_scalar) {
                var joined = std.ArrayListUnmanaged(u8).empty;
                defer joined.deinit(allocator);
                for (items, 0..) |item, i| {
                    if (i > 0) try joined.append(allocator, ',');
                    switch (item) {
                        .string => |s| try joined.appendSlice(allocator, s),
                        .number => |n| {
                            const rendered = try std.fmt.allocPrint(allocator, "{d}", .{n});
                            defer allocator.free(rendered);
                            try joined.appendSlice(allocator, rendered);
                        },
                        .boolean => |b| try joined.appendSlice(allocator, if (b) "true" else "false"),
                        .null => try joined.appendSlice(allocator, "null"),
                        else => unreachable,
                    }
                }
                try fields.append(allocator, .{
                    .name = try allocator.dupe(u8, prefix),
                    .value = try joined.toOwnedSlice(allocator),
                });
                return;
            }

            for (items, 0..) |*item, i| {
                const next_prefix = try std.fmt.allocPrint(allocator, "{s}.{d}", .{ prefix, i });
                defer allocator.free(next_prefix);
                try appendStructuredFields(allocator, fields, item, next_prefix);
            }
        },
    }
}

fn insertStructuredFieldPath(
    allocator: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(StructuredField),
    parts: []const []const u8,
    value: []const u8,
) !void {
    const name = parts[0];
    const tail = parts[1..];

    for (fields.items) |*field| {
        if (!std.mem.eql(u8, field.name, name)) continue;

        if (tail.len == 0) {
            try appendStructuredLeafValue(allocator, &field.value, value);
            return;
        }

        switch (field.value) {
            .object => |*children| {
                var nested = std.ArrayListUnmanaged(StructuredField).fromOwnedSlice(children.*);
                defer children.* = nested.toOwnedSlice(allocator) catch children.*;
                try insertStructuredFieldPath(allocator, &nested, tail, value);
                return;
            },
            else => {
                field.value.deinit(allocator);
                field.value = .{ .object = try allocator.alloc(StructuredField, 0) };
                var nested = std.ArrayListUnmanaged(StructuredField).fromOwnedSlice(field.value.object);
                defer field.value.object = nested.toOwnedSlice(allocator) catch field.value.object;
                try insertStructuredFieldPath(allocator, &nested, tail, value);
                return;
            },
        }
    }

    if (tail.len == 0) {
        try fields.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .value = .{ .string = try allocator.dupe(u8, value) },
        });
        return;
    }

    var nested = std.ArrayListUnmanaged(StructuredField).empty;
    errdefer {
        for (nested.items) |*field| field.deinit(allocator);
        nested.deinit(allocator);
    }
    try insertStructuredFieldPath(allocator, &nested, tail, value);
    try fields.append(allocator, .{
        .name = try allocator.dupe(u8, name),
        .value = .{ .object = try nested.toOwnedSlice(allocator) },
    });
}

fn appendStructuredLeafValue(
    allocator: std.mem.Allocator,
    value: *StructuredValue,
    incoming: []const u8,
) !void {
    switch (value.*) {
        .string => |existing| {
            const items = try allocator.alloc(StructuredValue, 2);
            items[0] = .{ .string = existing };
            items[1] = .{ .string = try allocator.dupe(u8, incoming) };
            value.* = .{ .array = items };
        },
        .array => |existing_items| {
            var grown = try allocator.alloc(StructuredValue, existing_items.len + 1);
            for (existing_items, 0..) |item, i| grown[i] = item;
            grown[existing_items.len] = .{ .string = try allocator.dupe(u8, incoming) };
            allocator.free(existing_items);
            value.* = .{ .array = grown };
        },
        else => {
            value.deinit(allocator);
            value.* = .{ .string = try allocator.dupe(u8, incoming) };
        },
    }
}

test "structuredFromFields builds nested objects" {
    const allocator = std.testing.allocator;
    const fields = [_]Field{
        .{ .name = "person.name", .value = "Alice" },
        .{ .name = "person.skills", .value = "Go" },
        .{ .name = "person.skills", .value = "Zig" },
    };

    var structured = (try structuredFromFields(allocator, &fields)).?;
    defer structured.deinit(allocator);

    try std.testing.expect(structured == .object);
    try std.testing.expectEqual(@as(usize, 1), structured.object.len);
}
