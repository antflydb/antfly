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

//! Shared pure namespace records; no runtime catalog or physical store dependency.
const std = @import("std");
const resolver = @import("antfly_resolver");
pub const Namespace = struct {
    record_store: bool = false,
    /// One-time adoption cut; later edits use native namespace revision fences.
    adoption_revision: ?u64 = null,
    adoption_standalone: bool = false,
    key_template: []const u8,
    revision: u64 = 0,
    merges: []const resolver.KeyAlias = &.{},
    state: resolver.curation.State = .{},
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!std.mem.eql(u8, field, "record_store") or self.record_store) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
};
pub const Namespaces = struct { version: u32 = 1, namespaces: []const Namespace = &.{} };

pub fn keyInNamespace(template: []const u8, key: []const u8) bool {
    const first = std.mem.indexOf(u8, template, "{{") orelse return std.mem.eql(u8, template, key);
    const last = std.mem.lastIndexOf(u8, template, "}}") orelse return false;
    const prefix = template[0..first];
    const suffix = template[last + 2 ..];
    return key.len >= prefix.len + suffix.len and std.mem.startsWith(u8, key, prefix) and std.mem.endsWith(u8, key, suffix);
}
