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

    pub fn id(self: Guard, count: usize, position: usize) !u64 {
        const payload = self.bytes[prefix.len..];
        if (count > 128 or position >= count or payload.len != count * 8) return error.CatalogGenerationChanged;
        return std.mem.readInt(u64, payload[position * 8 ..][0..8], .little);
    }

    pub fn single(alloc: std.mem.Allocator, value: u64) !Guard {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        return .{ .bytes = try std.mem.concat(alloc, u8, &.{ prefix, &bytes }) };
    }

    pub fn tables(alloc: std.mem.Allocator, resolved: []const ?domain.ResolvedTable) !Guard {
        if (resolved.len == 0 or resolved.len > 128) return error.InvalidSqlBackendResponse;
        const bytes = try alloc.alloc(u8, prefix.len + resolved.len * 8);
        @memcpy(bytes[0..prefix.len], prefix);
        for (resolved, 0..) |table, i| std.mem.writeInt(u64, bytes[prefix.len + i * 8 ..][0..8], if (table) |t| t.table_id else 0, .little);
        return .{ .bytes = bytes };
    }

    pub fn index(alloc: std.mem.Allocator, value: ?domain.RelationMutationGuard, creating: bool) !Guard {
        // DROP INDEX also pins its schema generation: an index can be dropped
        // and recreated on the same table under the same spelling.
        var buffer: [256]u8 = undefined;
        var hash: std.Io.Writer.Hashing(std.crypto.hash.Blake3) = .init(&buffer);
        if (value) |relation| {
            std.json.Stringify.value(.{ relation.target, relation.incarnation, relation.owner.table_id }, .{}, &hash.writer) catch unreachable;
            if (!creating) std.json.Stringify.value(relation.owner, .{}, &hash.writer) catch unreachable;
        }
        hash.writer.flush() catch unreachable;
        var digest: [32]u8 = undefined;
        hash.hasher.final(&digest);
        return .{ .bytes = try std.mem.concat(alloc, u8, &.{ prefix, &digest }) };
    }
};
