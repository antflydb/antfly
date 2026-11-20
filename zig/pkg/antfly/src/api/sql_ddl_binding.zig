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
    pub const Objects = struct {
        tables: []const Table = &.{},
        resource_id: ?u64 = null,
        schema_version: ?u32 = null,
        policy: ?Policy = null,
        parent_id: ?u64 = null,
        tablespace_id: ?u64 = null,

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
