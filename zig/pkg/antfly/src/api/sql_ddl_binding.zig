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

//! In-memory pgwire portal fences. The zero scan count distinguishes these
//! envelopes from query bindings. No pointer into a catalog snapshot escapes.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;

const prefix = "ddl-bind" ++ "\x00\x00";

pub const Guard = struct {
    bytes: []const u8,

    pub fn decode(bytes: []const u8) !Guard {
        if (!std.mem.startsWith(u8, bytes, prefix) or bytes.len <= prefix.len) return error.CatalogGenerationChanged;
        return .{ .bytes = bytes };
    }

    pub fn isDdl(bytes: []const u8) bool {
        return std.mem.startsWith(u8, bytes, prefix);
    }

    pub const Table = struct { namespace: []const u8, id: u64 };
    pub const Policy = struct { id: u64, generation: u64 };
    pub const ForeignKeyParent = struct { constraint: []const u8, id: u64, schema_version: u32 };
    pub const Objects = struct {
        fk_parents: []const ForeignKeyParent = &.{},
        tables: []const Table = &.{},
        resource_id: ?u64 = null,
        schema_version: ?u32 = null,
        policy: ?Policy = null,
        fk_parent: ?ForeignKeyParent = null,
        tablespace_id: ?u64 = null,

        // Admission checks the plan's exact parent and catalog revisions. Match
        // by constraint so even a swap between two existing parents is fenced.
        pub fn validateCreatePlan(self: Objects, a: std.mem.Allocator, plan: anytype) !void {
            if (self.tablespace_id) |id| if (id != plan.tablespace_id) return error.CatalogGenerationChanged;
            var expected: std.StringHashMapUnmanaged(ForeignKeyParent) = .empty;
            defer expected.deinit(a);
            for (self.fk_parents) |parent| {
                const entry = try expected.getOrPut(a, parent.constraint);
                if (entry.found_existing) return error.InvalidSqlSyntax;
                entry.value_ptr.* = parent;
            }
            for (plan.parents) |parent| for (parent.transitions) |transition| {
                const binding = expected.fetchRemove(transition.constraint_name) orelse return error.CatalogGenerationChanged;
                if (binding.value.id != parent.table.table_id or binding.value.schema_version != try @import("tables.zig").schemaVersion(parent.table.schema_json)) return error.CatalogGenerationChanged;
            };
            if (expected.count() != 0) return error.CatalogGenerationChanged;
        }

        pub fn table(self: Objects, count: usize, position: usize) !Table {
            if (self.tables.len != count or position >= count) return error.CatalogGenerationChanged;
            return self.tables[position];
        }
    };

    pub fn objects(self: Guard, a: std.mem.Allocator) !Objects {
        return std.json.parseFromSliceLeaky(Objects, a, self.bytes[prefix.len..], .{});
    }

    pub fn encode(a: std.mem.Allocator, value: Objects) !Guard {
        var out: std.Io.Writer.Allocating = .init(a);
        errdefer out.deinit();
        try out.writer.writeAll(prefix);
        try std.json.Stringify.value(value, .{ .emit_null_optional_fields = false }, &out.writer);
        return .{ .bytes = try out.toOwnedSlice() };
    }

    pub fn index(alloc: std.mem.Allocator, value: ?domain.RelationMutationGuard, index_birth: ?u32) !Guard {
        var buffer: [256]u8 = undefined;
        var hash: std.Io.Writer.Hashing(std.crypto.hash.Blake3) = .init(&buffer);
        if (value) |relation| {
            std.json.Stringify.value(.{ relation.target, relation.incarnation, relation.owner.table_id }, .{}, &hash.writer) catch unreachable;
            std.json.Stringify.value(.{ relation.owner.kind, index_birth }, .{}, &hash.writer) catch unreachable;
        }
        hash.writer.flush() catch unreachable;
        var digest: [32]u8 = undefined;
        hash.hasher.final(&digest);
        return .{ .bytes = try std.mem.concat(alloc, u8, &.{ prefix, &digest }) };
    }
};
