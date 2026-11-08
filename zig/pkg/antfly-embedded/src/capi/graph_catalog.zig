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

//! Local C/Lite adapter for the shared durable graph catalog.
const std = @import("std");
const h = @import("handles.zig");
const core = @import("../storage/embedded_graph_catalog.zig");
const cypher = @import("cypher");
const A = std.mem.Allocator;
pub const Record = core.Record;
pub const Catalog = core.Catalog;
pub const graph = core.graph;
const Context = struct {
    handle: *h.Handle,
    root: *h.db_mod.DB,
    alloc: A,
    writable: bool,
    uncertain: bool,
    pub fn get(self: @This(), _: A, name: []const u8) !*h.db_mod.DB {
        return @import("tables.zig").get(self.handle, name);
    }
};
fn context(handle: *h.Handle) Context {
    return .{ .handle = handle, .root = &handle.db, .alloc = handle.alloc, .writable = h.liteOpenModeCanWrite(handle.open_mode), .uncertain = handle.sql_decision_uncertain };
}
pub fn load(a: A, handle: *h.Handle) !Catalog {
    return core.load(a, context(handle));
}
pub fn get(a: A, handle: *h.Handle, name: []const u8) !Record {
    return core.get(a, context(handle), name);
}
pub fn validateOwners(a: A, handle: *h.Handle, selected: cypher.catalog.Graph) !void {
    return core.validateOwners(a, context(handle), selected);
}
pub fn execute(handle: *h.Handle, json: []const u8, out: *h.capi.Buffer) !void {
    const result = try core.execute(context(handle), std.heap.c_allocator, json);
    out.* = .{ .ptr = result.ptr, .len = result.len };
}
