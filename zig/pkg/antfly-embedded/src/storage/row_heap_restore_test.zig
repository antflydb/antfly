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

//! The portable representation is logical for both storage modes. Private
//! physical locations must never be restored without their source heap.
const std = @import("std");
const DocStore = @import("docstore.zig").DocStore;
const lsm = @import("lsm_backend.zig");
const keys = @import("internal_keys.zig");
const heap = @import("row_heap.zig");
const portable = @import("portable_backup.zig");
const schema = @import("schema.zig");
const public_schema = @import("../schema/mod.zig");
const relational = @import("db/relational/rows/store.zig");
const catalog = @import("db/table_catalog.zig");

test "row heap portable restore preserves document and typed rows without private locations" {
    const a = std.testing.allocator;
    inline for (.{ schema.StorageMode.document, schema.StorageMode.relational }) |mode| {
        var source_backend = lsm.Backend.init(a, .{});
        defer source_backend.close();
        var source = try DocStore.openRuntime(a, try source_backend.runtimeStore(a, .{}));
        defer source.close();
        const public_json = if (mode == .relational)
            "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"id\",\"count\"],\"additionalProperties\":false}}}}"
        else
            "{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"},\"count\":{\"type\":\"integer\"}},\"required\":[\"id\",\"count\"],\"additionalProperties\":false}}}}";
        var parsed = try public_schema.parseValidatedTableSchema(a, public_json);
        defer parsed.deinit(a);
        const runtime = try public_schema.deriveRuntimeTableSchema(a, parsed);
        defer schema.freeSchema(a, runtime);
        const encoded_schema = try schema.serializeSchema(a, runtime);
        defer a.free(encoded_schema);
        const row_key = if (mode == .relational) try keys.relationalRowKeyAlloc(a, "row:a") else try keys.documentKeyAlloc(a, "row:a");
        defer a.free(row_key);
        const json = "{\"id\":\"a\",\"count\":9007199254740993}";
        const body = if (mode == .relational) try relational.encodeValueForSchemaAlloc(a, json, runtime) else try a.dupe(u8, json);
        defer a.free(body);
        const facts: catalog.Catalog = .{ .mode_initialized = true, .storage_mode = mode, .active_schema_version = 1, .row_count = 1, .reconciled = true };
        const encoded_catalog = try facts.encodeDurable();
        try source.putBatch(&.{
            .{ .key = row_key, .value = body },
            .{ .key = catalog.key, .value = encoded_catalog.slice() },
            .{ .key = "\x00\x00__metadata__:schema", .value = encoded_schema },
            .{ .key = "\x00\x00__metadata__:schema_v1", .value = encoded_schema },
            .{ .key = keys.public_schema_json_key, .value = public_json },
        }, &.{});
        {
            var writer = try source.beginWriteTxn();
            errdefer writer.abort();
            _ = try writer.relocatePhysicalRow(row_key, @splat(9), 0);
            try writer.commit();
        }
        var archive: std.ArrayList(u8) = .empty;
        defer archive.deinit(a);
        try portable.exportPortable(a, &source, &archive);
        try portable.validatePortable(a, archive.items);
        try std.testing.expect(std.mem.indexOf(u8, archive.items, heap.magic) == null);
        var target_backend = lsm.Backend.init(a, .{});
        defer target_backend.close();
        var target = try DocStore.openRuntime(a, try target_backend.runtimeStore(a, .{}));
        defer target.close();
        try portable.importPortableWithOptions(a, &target, archive.items, .{ .unpublished_staging = true, .schema_cache_bytes = 1 });
        var read = try target.beginReadTxn();
        defer read.abort();
        const restored = try read.get(row_key);
        try std.testing.expectEqualSlices(u8, body, restored);
        var raw = try target.runtime_store.beginRead();
        defer raw.abort();
        try std.testing.expect(!heap.isReference(try raw.get(row_key)));
        const location = try heap.location(row_key, body, @splat(9), 0);
        try std.testing.expectError(error.NotFound, raw.get(&location.physicalKey()));
        // Future ordinary writes do not require the source heap, and a pinned
        // restore reader remains on its own logical generation.
        try target.put(row_key, body);
        try std.testing.expectEqualSlices(u8, body, try read.get(row_key));
    }
}
