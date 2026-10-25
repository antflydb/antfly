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

//! Shared row-to-vector semantics for archive and recent native generations.
const std = @import("std");
const local = @import("antfly_local_sources");
const managed = local.inference_managed_embedder;
const A = std.mem.Allocator;
const V = std.json.Value;
const units_api = @import("lake_enrichment_units.zig");
const rows = local.storage_rowsource_types;
pub const Producer = struct {
    allocator: A,
    managed: ?managed.ManagedEmbedder = null,
    memo: ?Memo = null,
    name: []const u8,
    column: []const u8,
    config: V,
    options: ?managed.InitOptions,
    pub fn init(a: A, name: []const u8, column: []const u8, config: V, options: ?managed.InitOptions) !Producer {
        var result: Producer = .{ .allocator = a, .name = name, .column = column, .config = config, .options = options };
        if (@import("lake_enrichment_units.zig").configured(config, "embedder") != null) {
            const opts = options orelse return error.LakeEmbeddingProviderUnavailable;
            var indexes: V = .{ .object = .empty };
            defer indexes.object.deinit(a);
            try indexes.object.put(a, name, config);
            result.managed = try managed.ManagedEmbedder.initFromIndexValueObjectWithOptions(a, indexes, opts);
        }
        return result;
    }
    pub fn deinit(self: *Producer) void {
        if (self.managed) |*owner| owner.deinit();
    }
    /// Prepare the complete row through the same template, media marker and
    /// chunker interfaces used by native document enrichment. Each unit owns
    /// a distinct vector identity; never pool chunks into a document vector.
    pub fn units(self: *Producer, a: A, row: V) ![]const @import("lake_enrichment_units.zig").Unit {
        if (self.memo) |memo| try memo.context.ensureActive();
        return @import("lake_enrichment_units.zig").prepare(a, self.config, self.column, row, self.options);
    }
    pub const Input = struct {
        producer: *Producer,
        batch: rows.ColumnBatch,
        column: ?rows.ColumnVector,
        whole_row: bool,
        pub const Prepared = struct {
            single: [1]units_api.Unit = undefined,
            count: usize = 0,
            many: ?[]const units_api.Unit = null,
            pub fn units(self: *const Prepared) []const units_api.Unit {
                return self.many orelse self.single[0..self.count];
            }
        };
        pub fn units(self: Input, a: A, ordinal: usize) ![]const units_api.Unit {
            const prepared = try self.prepare(a, ordinal);
            if (prepared.many) |many| return many;
            return a.dupe(units_api.Unit, prepared.single[0..prepared.count]);
        }
        pub fn prepare(self: Input, a: A, ordinal: usize) !Prepared {
            if (ordinal >= self.batch.rowCount()) return error.InvalidSqlBackendResponse;
            if (self.producer.memo) |memo| try memo.context.ensureActive();
            if (self.whole_row) {
                const page: local.sql_catalog.ColumnPage = .{ .batch = self.batch, .selection = &.{ordinal} };
                return .{ .many = try self.producer.units(a, try units_api.rowValue(a, page)) };
            }
            const column = self.column orelse return .{};
            if (column.nulls.isNull(ordinal)) return .{};
            if (column.values == .vector_f32 and self.producer.managed == null) {
                return .{ .single = .{.{ .dense_vector = column.values.vector_f32[ordinal] }}, .count = 1 };
            }
            const value = try column.values.jsonAt(a, ordinal);
            if (self.producer.managed == null) return if (value == .null) .{} else .{ .single = .{.{ .materialized = value }}, .count = 1 };
            return .{ .many = try units_api.prepareSelected(a, self.producer.config, value, null, self.producer.options) };
        }
    };
    /// Resolve the input column once per batch. Unrelated columns stay encoded;
    /// native vectors remain borrowed and dictionary strings stay dictionary-backed.
    pub fn bind(self: *Producer, batch: rows.ColumnBatch) Input {
        return .{ .producer = self, .batch = batch, .column = batch.findColumn(self.column), .whole_row = units_api.configured(self.config, "template") != null and self.managed != null };
    }
    pub fn denseUnit(self: *Producer, a: A, unit: @import("lake_enrichment_units.zig").Unit, dims: u32) !?[]const f32 {
        if (unit.dense_vector) |vector| {
            if (vector.len != dims) return error.InvalidVectorDimensions;
            for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidVectorValue;
            return vector;
        }
        if (unit.materialized) |value| return self.dense(a, value, dims);
        if (unit.parts.len == 1 and unit.parts[0] == .text) return self.dense(a, .{ .string = unit.parts[0].text }, dims);
        const owner = if (self.managed) |*value| value else return error.LakeEmbeddingProviderUnavailable;
        const input = try @import("lake_enrichment_units.zig").partsJson(a, unit.parts);
        defer a.free(input);
        const key = if (self.memo) |memo| try memo.key(a, self.name, "dense-parts", dims, .{ .string = input }) else null;
        var probe: Probe = .{};
        if (self.memo) |memo| {
            probe = try memo.read(a, key.?);
            if (probe.vector) |cached| return try materializedDense(a, cached, dims);
        }
        const vector = try owner.denseInterface().embedDenseParts(a, self.name, unit.parts, dims);
        if (vector.len != dims) return error.InvalidVectorDimensions;
        for (vector) |component| if (!std.math.isFinite(component)) return error.InvalidVectorValue;
        if (self.memo) |memo| {
            const value = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, vector, .{}), .{});
            return try materializedDense(a, try memo.write(a, key.?, probe.etag, value), dims);
        }
        return vector;
    }
    pub fn sparseUnit(self: *Producer, a: A, unit: @import("lake_enrichment_units.zig").Unit) !?local.storage_db_enrichment_embedder.SparseEmbedding {
        if (unit.dense_vector != null) return error.InvalidEmbeddingField;
        if (unit.materialized) |value| return self.sparse(a, value);
        if (unit.parts.len != 1 or unit.parts[0] != .text) return error.UnsupportedSparseMediaInput;
        return self.sparse(a, .{ .string = unit.parts[0].text });
    }
    pub fn dense(self: *Producer, a: A, value: V, dims: u32) !?[]const f32 {
        if (value == .null) return null;
        if (self.managed) |*owner| {
            if (value != .string) return error.InvalidEmbeddingInput;
            const key = if (self.memo) |memo| try memo.key(a, self.name, "dense", dims, value) else null;
            var probe: Probe = .{};
            if (self.memo) |memo| {
                probe = try memo.read(a, key.?);
                if (probe.vector) |cached| return try materializedDense(a, cached, dims);
            }
            const vector = try owner.denseInterface().embedDense(a, self.name, value.string, dims);
            if (vector.len != dims) return error.InvalidVectorDimensions;
            for (vector) |component| if (!std.math.isFinite(component)) return error.InvalidVectorValue;
            if (self.memo) |memo| {
                const payload = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, vector, .{}), .{});
                return try materializedDense(a, try memo.write(a, key.?, probe.etag, payload), dims);
            }
            return vector;
        }
        const parsed = if (value == .string) try std.json.parseFromSliceLeaky(V, a, value.string, .{}) else value;
        return (try local.storage_db_document_mapper.extractDenseVectorFromValue(a, parsed, dims)) orelse error.InvalidVectorValue;
    }
    pub fn sparse(self: *Producer, a: A, value: V) !?local.storage_db_enrichment_embedder.SparseEmbedding {
        if (value == .null) return null;
        if (self.managed) |*owner| {
            if (value != .string) return error.InvalidEmbeddingInput;
            const key = if (self.memo) |memo| try memo.key(a, self.name, "sparse", 0, value) else null;
            var probe: Probe = .{};
            if (self.memo) |memo| {
                probe = try memo.read(a, key.?);
                if (probe.vector) |cached| return try materializedSparse(a, cached);
            }
            const vector = try owner.sparseInterface().embedSparse(a, self.name, value.string);
            if (self.memo) |memo| {
                const payload = try std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, vector, .{}), .{});
                return try materializedSparse(a, try memo.write(a, key.?, probe.etag, payload));
            }
            return vector;
        }
        const parsed = if (value == .string) try std.json.parseFromSliceLeaky(V, a, value.string, .{}) else value;
        const vector = try local.storage_db_document_mapper.parseSparseValue(a, parsed);
        return .{ .indices = vector.indices, .values = vector.values };
    }
};

fn materializedDense(a: A, value: V, dims: u32) ![]const f32 {
    return (try local.storage_db_document_mapper.extractDenseVectorFromValue(a, value, dims)) orelse error.InvalidVectorValue;
}
fn materializedSparse(a: A, value: V) !local.storage_db_enrichment_embedder.SparseEmbedding {
    const vector = try local.storage_db_document_mapper.parseSparseValue(a, value);
    return .{ .indices = vector.indices, .values = vector.values };
}
const Probe = struct { vector: ?V = null, etag: ?[]const u8 = null };
const Record = struct { version: u16 = 1, expires_ms: u64, recipe: [32]u8, vector: V };
/// Per-input durable enrichment completion. A retry/restart assembles already
/// computed vectors instead of reissuing the same vendor work from row zero.
/// Archive promotion consumes the same memo so it preserves vector values.
pub const Memo = struct {
    store: *@import("lake_index_store.zig").Store,
    table_id: u64,
    recipe: [32]u8,
    context: local.serverless_query_lake_read_context.Context,
    fn key(self: Memo, a: A, name: []const u8, kind: []const u8, dims: u32, value: V) ![]const u8 {
        const input = try std.json.Stringify.valueAlloc(a, .{ .recipe = self.recipe, .name = name, .kind = kind, .dims = dims, .input = value }, .{});
        defer a.free(input);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(input, &digest, .{});
        return std.fmt.allocPrint(a, "{s}{s}row-enrichment/{d}/{s}.json", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", self.table_id, std.fmt.bytesToHex(digest, .lower) });
    }
    fn read(self: Memo, a: A, path: []const u8) !Probe {
        try self.context.ensureActive();
        var client = self.store.opened.client;
        var saved = client.getObject(self.store.opened.bucket, path, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = if (self.context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return .{},
            else => return err,
        };
        defer saved.deinit(client.allocator);
        const record = try std.json.parseFromSliceLeaky(Record, a, saved.body, .{ .allocate = .alloc_always });
        if (record.version != 1 or !std.mem.eql(u8, &record.recipe, &self.recipe)) return error.InvalidEnrichmentMemo;
        return .{ .vector = if (record.expires_ms > @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) record.vector else null, .etag = if (saved.metadata.etag) |etag| try a.dupe(u8, etag) else return error.MissingObjectEtag };
    }
    fn write(self: Memo, a: A, path: []const u8, etag: ?[]const u8, vector: V) !V {
        try self.context.ensureActive();
        var client = self.store.opened.client;
        const record: Record = .{ .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms +| 7 * 24 * 60 * 60 * 1000, .recipe = self.recipe, .vector = vector };
        const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
        defer a.free(bytes);
        var saved = client.putObject(self.store.opened.bucket, path, bytes, .{ .if_none_match = etag == null, .if_match_etag = etag, .cancellation = if (self.context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => return (try self.read(a, path)).vector orelse error.EnrichmentMemoConflict,
            else => return err,
        };
        saved.deinit(client.allocator);
        return vector;
    }
};

test "external lake vector publication binds typed batches without reading unrelated columns" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const config = try std.json.parseFromSliceLeaky(V, ca, "{}", .{});
    var producer = try Producer.init(a, "dense", "vector", config, null);
    defer producer.deinit();
    const Poison = struct {
        fn read(_: *const anyopaque, _: usize) !V {
            return error.UnexpectedPayloadMaterialization;
        }
    };
    var dummy: u8 = 0;
    const batch: rows.ColumnBatch = .{
        .snapshot = .{ .table_id = "lake", .snapshot_id = "snapshot" },
        .external_span = .{ .first = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = 3 },
        .columns = &.{
            .{ .name = "vector", .values = .{ .vector_f32 = &.{ &.{ 1, 0 }, &.{ 0, 1 }, &.{} } }, .nulls = .{ .bytes = &.{ 0, 0, 1 } } },
            .{ .name = "unrelated", .values = .{ .encoded = .{ .ptr = &dummy, .begin = 0, .kind = .json, .count = 3, .read = Poison.read } } },
        },
    };
    const input = producer.bind(batch);
    const first = (try input.units(ca, 0))[0];
    const vector = (try producer.denseUnit(ca, first, 2)).?;
    try std.testing.expect(vector.ptr == batch.columns[0].values.vector_f32[0].ptr);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0 }, vector);
    try std.testing.expectEqual(@as(usize, 0), (try input.units(ca, 2)).len);
    try std.testing.expectError(error.InvalidVectorDimensions, producer.denseUnit(ca, first, 3));
    try std.testing.expectError(error.InvalidVectorValue, producer.denseUnit(ca, .{ .dense_vector = &.{std.math.nan(f32)} }, 1));
    try std.testing.expectError(error.InvalidEmbeddingField, producer.sparseUnit(ca, first));
    var strings = batch;
    strings.columns = &.{.{ .name = "vector", .values = .{ .dictionary_bytes = .{ .values = &.{"[0,1]"}, .indices = &.{ 0, 0, 0 } } } }};
    const string_unit = (try producer.bind(strings).units(ca, 1))[0];
    try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, (try producer.denseUnit(ca, string_unit, 2)).?);
    strings.columns = &.{.{ .name = "vector", .values = .{ .json = &.{ "{\"indices\":[3],\"values\":[0.5]}", "null", "{}" } } }};
    const sparse = (try producer.sparseUnit(ca, (try producer.bind(strings).units(ca, 0))[0])).?;
    try std.testing.expectEqualSlices(u32, &.{3}, sparse.indices);
    try std.testing.expectEqualSlices(f32, &.{0.5}, sparse.values);
    try std.testing.expectEqual(@as(usize, 0), (try producer.bind(strings).units(ca, 1)).len);
}

test "external lake enrichment memo survives reopening and conditional races preserve the first completed vector" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-vector-memo");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memo: Memo = .{ .store = &store, .table_id = 7, .recipe = @splat(1), .context = .{ .io = std.testing.io } };
    const path = try memo.key(scratch, "vector", "dense", 2, .{ .string = "document" });
    try std.testing.expect((try memo.read(scratch, path)).vector == null);
    const first = try std.json.parseFromSliceLeaky(V, scratch, "[1,0]", .{});
    const second = try std.json.parseFromSliceLeaky(V, scratch, "[0,1]", .{});
    _ = try memo.write(scratch, path, null, first);
    const winner = try memo.write(scratch, path, null, second);
    try std.testing.expectEqual(@as(i64, 1), winner.array.items[0].integer);
    store.deinit();
    var reopened = try @import("lake_index_store.zig").Store.open(a, &config, null, false);
    defer reopened.deinit();
    memo.store = &reopened;
    const restored = (try memo.read(scratch, path)).vector.?;
    try std.testing.expectEqual(@as(i64, 1), restored.array.items[0].integer);
    memo.recipe = @splat(2);
    const changed = try memo.key(scratch, "vector", "dense", 2, .{ .string = "document" });
    try std.testing.expect(!std.mem.eql(u8, path, changed));
    try std.testing.expect((try memo.read(scratch, changed)).vector == null);
}
