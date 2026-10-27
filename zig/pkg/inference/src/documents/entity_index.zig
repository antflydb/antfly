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

//! One task-local index for relation identity and evidence resolution.
const std = @import("std");
const V = std.json.Value;
pub fn field(value: V, key: []const u8) ?V {
    return if (value == .object) value.object.get(key) else null;
}
pub const Index = struct {
    entities: []V,
    ids: std.StringHashMapUnmanaged(usize) = .empty,
    pub const Reference = union(enum) { entity: usize, inline_value, unresolved };
    pub fn init(a: std.mem.Allocator, root: V) !Index {
        if (root != .object) return error.InvalidDocumentModelResult;
        const entities = field(root, "entities") orelse V{ .array = .init(a) };
        if (entities != .array) return error.InvalidDocumentModelResult;
        var self = Index{ .entities = entities.array.items };
        for (self.entities, 0..) |entity, i| {
            for ([_][]const u8{ "label", "text" }) |key| {
                const value = field(entity, key) orelse return error.InvalidDocumentModelResult;
                if (value != .string) return error.InvalidDocumentModelResult;
            }
            if (field(entity, "id")) |id| {
                if (id != .string) return error.InvalidDocumentModelResult;
                const entry = try self.ids.getOrPut(a, id.string);
                if (entry.found_existing) return error.InvalidDocumentModelResult;
                entry.value_ptr.* = i;
            }
        }
        return self;
    }
    pub fn resolve(self: Index, endpoint: V) !Reference {
        if (endpoint != .object) return error.InvalidDocumentModelResult;
        var selected: ?usize = null;
        if (field(endpoint, "entity_index")) |value| {
            const i = switch (value) {
                .integer => std.math.cast(usize, value.integer) orelse return .unresolved,
                .number_string => std.fmt.parseUnsigned(usize, value.number_string, 10) catch return .unresolved,
                else => return error.InvalidDocumentModelResult,
            };
            if (i >= self.entities.len) return .unresolved;
            selected = i;
        }
        if (field(endpoint, "id")) |id| {
            if (id != .string) return error.InvalidDocumentModelResult;
            if (self.ids.get(id.string)) |i| {
                if (selected != null and selected.? != i) return .unresolved;
                selected = i;
            } else if (selected != null) return .unresolved;
        }
        if (selected) |i| {
            const entity = self.entities[i];
            for ([_][]const u8{ "label", "text" }) |key| if (field(endpoint, key)) |value| {
                if (value != .string) return error.InvalidDocumentModelResult;
                if (!std.mem.eql(u8, value.string, field(entity, key).?.string)) return .unresolved;
            };
            return .{ .entity = i };
        }
        if (field(endpoint, "text")) |text| {
            if (text != .string) return error.InvalidDocumentModelResult;
            return .inline_value;
        }
        return .unresolved;
    }
};
