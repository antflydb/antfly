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

//! A speculative SQL overlay depends on its FK-connected tables, not the
//! physical file tip. The complete schema directory fences new dependencies;
//! namespace incarnations and semantic revisions fence committed row/claim
//! changes. No read snapshot or borrowed runtime escapes capture.
const std = @import("std");
const native = @import("../storage/lite/native.zig");
const catalog = @import("../storage/lite/embedded_catalog.zig");

fn bytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, value.len, .little);
    hash.update(&length);
    hash.update(value);
}

pub fn capture(a: std.mem.Allocator, backend: anytype, metadata: anytype, tables: anytype, target: []const u8) !?[32]u8 {
    const file = &backend.native_docstore.?.file;
    if (!file.header.indexed_reclamation or file.header.indexed_revision < 9) return null;
    const records = metadata.table;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var by_name: std.StringHashMapUnmanaged(usize) = .empty;
    const order = try scratch.alloc(usize, records.len);
    const neighbors = try scratch.alloc(std.ArrayList(usize), records.len);
    for (records, order, neighbors, 0..) |record, *index, *edges, i| {
        if ((try by_name.getOrPut(scratch, record.name)).found_existing) return error.IntegrityCatalogUnavailable;
        by_name.getPtr(record.name).?.* = i;
        index.* = i;
        edges.* = .empty;
    }
    const Declaration = struct {
        foreign_keys: ?[]const struct { parent_table: []const u8 } = null,
    };
    for (records, 0..) |record, i| {
        if (record.schema_json.len == 0) continue;
        const declaration = try std.json.parseFromSlice(Declaration, scratch, record.schema_json, .{ .ignore_unknown_fields = true });
        if (declaration.value.foreign_keys) |foreign_keys| for (foreign_keys) |foreign| {
            const parent = by_name.get(foreign.parent_table) orelse return error.IntegrityCatalogUnavailable;
            try neighbors[i].append(scratch, parent);
            try neighbors[parent].append(scratch, i);
        };
    }
    const seen = try scratch.alloc(bool, records.len);
    @memset(seen, false);
    const queue = try scratch.alloc(usize, records.len);
    var tail: usize = 0;
    var staged_tables = tables.keyIterator();
    while (staged_tables.next()) |name| {
        const i = by_name.get(name.*) orelse return error.IntegrityCatalogUnavailable;
        if (!seen[i]) {
            seen[i] = true;
            queue[tail] = i;
            tail += 1;
        }
    }
    const current = by_name.get(target) orelse return error.IntegrityCatalogUnavailable;
    if (!seen[current]) {
        seen[current] = true;
        queue[tail] = current;
        tail += 1;
    }
    var head: usize = 0;
    while (head < tail) : (head += 1) for (neighbors[queue[head]].items) |next| {
        if (seen[next]) continue;
        seen[next] = true;
        queue[tail] = next;
        tail += 1;
    };
    std.mem.sort(usize, order, records, struct {
        fn less(values: @TypeOf(records), lhs: usize, rhs: usize) bool {
            return std.mem.lessThan(u8, values[lhs].name, values[rhs].name);
        }
    }.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("SQL dependency witness v9");
    for (order) |i| {
        const record = records[i];
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, record.table_id, .little);
        hash.update(&id);
        bytes(&hash, record.name);
        bytes(&hash, record.schema_json);
        hash.update(&.{@intFromBool(seen[i])});
        if (!seen[i]) continue;
        const namespace = if (std.mem.eql(u8, record.name, "default")) "root" else try catalog.namespaceAlloc(scratch, record.table_id);
        const incarnation = (try backend.existingRootIncarnation(namespace)) orelse return null;
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u128, &encoded, incarnation, .little);
        hash.update(&encoded);
        const prefix = if (std.mem.eql(u8, namespace, "root")) "" else try backend.documentPrefixAlloc(scratch, namespace);
        const key = try std.mem.concat(scratch, u8, &.{ prefix, native.semantic_revision.key });
        // Legacy/upgraded namespaces and schema-less raw roots cannot yet
        // promise revision maintenance. Keep the conservative file-wide cut
        // until a schema-bearing writer has installed their first marker.
        const revision = (try file.getDocumentAlloc(scratch, key)) orelse return null;
        if (revision.len != 16) return error.InvalidNativeSemanticRevision;
        hash.update(revision);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
