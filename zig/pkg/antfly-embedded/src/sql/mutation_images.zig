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

//! Native normalization bridge. All buffers belong to the bounded request
//! arena. SQL never reconstructs generated/default values independently.
const std = @import("std");
const catalog = @import("catalog.zig");

pub fn writes(comptime Write: type, alloc: std.mem.Allocator, input: []const catalog.Mutation) ![]Write {
    var result: std.ArrayList(Write) = .empty;
    for (input) |mutation| if (mutation.row) |row| {
        try result.append(alloc, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(alloc, row, .{}), .json_null_fields = mutation.json_null_fields });
    };
    return result.toOwnedSlice(alloc);
}

pub fn merge(alloc: std.mem.Allocator, input: []const catalog.Mutation, normalized: anytype) ![]const catalog.Mutation {
    const result = try alloc.dupe(catalog.Mutation, input);
    var index: usize = 0;
    for (result) |*mutation| if (mutation.row != null) {
        if (index >= normalized.len or !std.mem.eql(u8, mutation.key, normalized[index].key)) return error.InvalidSqlBackendResponse;
        const write = normalized[index];
        const value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, write.value, .{ .parse_numbers = false });
        if (value != .object) return error.InvalidSqlBackendResponse;
        mutation.row = value;
        mutation.json_null_fields = write.json_null_fields;
        index += 1;
    };
    if (index != normalized.len) return error.InvalidSqlBackendResponse;
    return result;
}
