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

//! Durable embedded table directory, shared by API ownership and inspection.
const std = @import("std");
pub const key = "\x00antfly/embedded/tables/v1";
pub const Record = struct { name: []const u8, id: u64 };
pub const Catalog = struct { next_id: u64 = 2, tables: []const Record = &.{} };

pub fn namespaceAlloc(alloc: std.mem.Allocator, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "embedded/tables/{d}", .{id});
}

pub fn validate(catalog: Catalog) !void {
    if (catalog.next_id < 2) return error.InvalidEmbeddedTableCatalog;
    for (catalog.tables, 0..) |record, i| {
        if (record.id < 2 or record.id >= catalog.next_id or record.name.len == 0 or std.mem.eql(u8, record.name, "default")) return error.InvalidEmbeddedTableCatalog;
        for (catalog.tables[0..i]) |before| {
            if (before.id == record.id or std.mem.eql(u8, before.name, record.name)) return error.InvalidEmbeddedTableCatalog;
        }
    }
}
