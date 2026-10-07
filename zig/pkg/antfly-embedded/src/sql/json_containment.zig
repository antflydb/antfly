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

//! PostgreSQL JSONB containment: object subsets, unordered array membership,
//! and the top-level array/scalar exception. No JSON serialization or allocation.
const std = @import("std");
const order = @import("json_order.zig");
const Json = std.json.Value;

pub fn exists(value: Json, key: []const u8, work: *order.Budget) !bool {
    try work.consume(key.len + 1);
    return switch (value) {
        .object => value.object.contains(key),
        .string => blk: {
            try work.consume(value.string.len);
            break :blk std.mem.eql(u8, value.string, key);
        },
        .array => blk: {
            for (value.array.items) |item| {
                try work.consume(1);
                if (item == .string) {
                    try work.consume(item.string.len);
                    if (std.mem.eql(u8, item.string, key)) break :blk true;
                }
            }
            break :blk false;
        },
        else => false,
    };
}

pub fn contains(value: Json, subset: Json, work: *order.Budget, depth: usize) anyerror!bool {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    try work.consume(1);
    if (subset == .object) {
        if (value != .object) return false;
        var fields = subset.object.iterator();
        while (fields.next()) |field| {
            try work.consume(field.key_ptr.len + 1);
            const candidate = value.object.get(field.key_ptr.*) orelse return false;
            if (!try contains(candidate, field.value_ptr.*, work, depth + 1)) return false;
        }
        return true;
    }
    if (value == .array) {
        if (subset == .array) {
            for (subset.array.items) |wanted| {
                var found = false;
                for (value.array.items) |candidate| {
                    try work.consume(1);
                    // A scalar cannot match through a nested array wrapper.
                    if ((candidate == .array) != (wanted == .array)) continue;
                    if (try contains(candidate, wanted, work, depth + 1)) {
                        found = true;
                        break;
                    }
                }
                if (!found) return false;
            }
            return true;
        }
        if (depth != 0) return false;
        for (value.array.items) |candidate| {
            try work.consume(1);
            if (candidate == .array or candidate == .object) continue;
            if (try order.compare(candidate, subset, work, depth + 1) == .eq) return true;
        }
        return false;
    }
    if (subset == .array or value == .object) return false;
    return try order.compare(value, subset, work, depth + 1) == .eq;
}
