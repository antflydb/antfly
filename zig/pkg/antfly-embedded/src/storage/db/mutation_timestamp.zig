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

//! Pure authored TTL timestamp extraction, independent of writer admission.
const std = @import("std");
const Allocator = std.mem.Allocator;
const schema_mod = @import("../schema.zig");

pub fn ttlTimestampNsFromDocumentValue(
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    value_json: []const u8,
) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return null,
    };
    const ttl_value = root.get(schema.ttl_field) orelse return null;
    if (ttl_value == .null) return null;
    return try ttlTimestampNsFromJsonValue(ttl_value);
}

pub fn ttlTimestampNsFromJsonValue(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |integer| blk: {
            if (integer < 0) return error.InvalidArgument;
            break :blk std.math.cast(u64, integer) orelse return error.InvalidArgument;
        },
        .number_string => |text| std.fmt.parseInt(u64, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.InvalidArgument,
        .string => |text| ttlTimestampNsFromString(text),
        else => error.InvalidArgument,
    };
}

pub fn ttlTimestampNsFromString(text: []const u8) !u64 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidArgument;
    if (std.fmt.parseInt(u64, trimmed, 10)) |ts| return ts else |_| {}
    if (try @import("query/graph_exec.zig").parsePatternRfc3339ToNs(trimmed)) |ts| return ts;
    return error.InvalidArgument;
}
