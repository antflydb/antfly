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

//! Producers behind a serverless table's named enrichments (#1791).
//!
//! Embedding declarations use the embedder of the embeddings index that
//! consumes them (`embedding_name` equal to the declaration name), as on the
//! other engines. Asset declarations with a producer (generator, extractor,
//! reader, document_extraction, ...) run through the host's asset producer;
//! a runtime without one leaves those outputs pending.
const std = @import("std");
const Allocator = std.mem.Allocator;
const local = @import("antfly_local_sources");
const named_outputs = @import("named_outputs.zig");
const named_enrichments = @import("../catalog/named_enrichments.zig");

const embedder_mod = local.storage_db_enrichment_embedder;
const managed_embedder = local.inference_managed_embedder;
pub const asset_producer = local.storage_db_enrichment_asset_producer;
const EnrichmentConfig = named_enrichments.EnrichmentConfig;

pub const NamedProducers = struct {
    alloc: Allocator,
    dense: ?embedder_mod.DenseEmbedder = null,
    /// Borrowed from the host runtime.
    asset: ?asset_producer.Producer = null,
    /// Dimensions declared by the consuming index, keyed by declaration.
    dims: std.StringArrayHashMapUnmanaged(u32) = .empty,
    sparse: ?embedder_mod.SparseEmbedder = null,
    /// Declarations consumed by sparse indexes.
    sparse_names: std.StringArrayHashMapUnmanaged(void) = .empty,

    /// Builds the embedders of the indexes that consume the table's named
    /// embedding declarations. Other indexes keep their own pipelines.
    pub fn init(alloc: Allocator, indexes_json: []const u8, declarations: named_enrichments.Declarations, options: managed_embedder.InitOptions, asset: ?asset_producer.Producer) !NamedProducers {
        var self = NamedProducers{ .alloc = alloc, .asset = asset };
        errdefer self.deinit();
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return self;
        var consumers = std.json.ObjectMap.empty;
        defer consumers.deinit(alloc);
        var sparse_consumers = std.json.ObjectMap.empty;
        defer sparse_consumers.deinit(alloc);
        for (declarations.configs) |cfg| {
            if (cfg.kind != .embedding) continue;
            const index = parsed.value.object.get(cfg.name) orelse continue;
            if (index != .object or index.object.get("embedder") == null) continue;
            const name = index.object.get("embedding_name") orelse continue;
            if (name != .string or !std.mem.eql(u8, name.string, cfg.name)) continue;
            if (index.object.get("sparse")) |flag| if (flag == .bool and flag.bool) {
                try sparse_consumers.put(alloc, cfg.name, index);
                try self.sparse_names.put(alloc, cfg.name, {});
                continue;
            };
            try consumers.put(alloc, cfg.name, index);
            const dims: u32 = if (cfg.expected_dims != 0) cfg.expected_dims else dims: {
                const value = index.object.get("dimension") orelse break :dims 0;
                break :dims if (value == .integer and value.integer > 0 and value.integer <= std.math.maxInt(u32)) @intCast(value.integer) else 0;
            };
            try self.dims.put(alloc, cfg.name, dims);
        }
        if (consumers.count() != 0)
            self.dense = try managed_embedder.ManagedEmbedder.createDenseEmbedderFromIndexValueWithOptions(alloc, .{ .object = consumers }, options);
        if (sparse_consumers.count() != 0)
            self.sparse = try managed_embedder.ManagedEmbedder.createSparseEmbedderFromIndexValueWithOptions(alloc, .{ .object = sparse_consumers }, options);
        return self;
    }

    pub fn deinit(self: *NamedProducers) void {
        if (self.dense) |dense| dense.deinit(self.alloc);
        if (self.sparse) |sparse| sparse.deinit(self.alloc);
        self.dims.deinit(self.alloc);
        self.sparse_names.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn producers(self: *NamedProducers) named_outputs.Producers {
        return .{
            .ctx = self,
            .embed = if (self.dense != null) embed else null,
            .embed_sparse = if (self.sparse_names.count() != 0) embedSparse else null,
            .asset = if (self.asset != null) produceAsset else null,
        };
    }

    fn embed(ctx: *anyopaque, a: Allocator, cfg: EnrichmentConfig, texts: []const []const u8) anyerror![][]f32 {
        const self: *NamedProducers = @ptrCast(@alignCast(ctx));
        const dense = self.dense orelse return error.EmbedderUnavailable;
        const dims = self.dims.get(cfg.name) orelse return error.EmbedderUnavailable;
        const out = try a.alloc([]f32, texts.len);
        for (out, texts) |*vector, text| vector.* = try dense.embedDense(a, cfg.name, text, dims);
        return out;
    }

    fn embedSparse(ctx: *anyopaque, a: Allocator, cfg: EnrichmentConfig, texts: []const []const u8) anyerror!?[]named_outputs.SparseVector {
        const self: *NamedProducers = @ptrCast(@alignCast(ctx));
        if (!self.sparse_names.contains(cfg.name)) return null;
        const sparse = self.sparse orelse return error.EmbedderUnavailable;
        const out = try a.alloc(named_outputs.SparseVector, texts.len);
        for (out, texts) |*vector, text| {
            const embedded = try sparse.embedSparse(a, cfg.name, text);
            vector.* = .{ .indices = embedded.indices, .values = embedded.values };
        }
        return out;
    }

    fn produceAsset(ctx: *anyopaque, a: Allocator, cfg: EnrichmentConfig, source: []const u8, _: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) anyerror!std.json.Value {
        const self: *NamedProducers = @ptrCast(@alignCast(ctx));
        var producer = self.asset orelse return error.AssetProducerUnavailable;
        var context = producer.invocation_context;
        context.cancellation = cancellation;
        producer = producer.withInvocationContext(context);
        var config = try asset_producer.parseProducerConfig(a, cfg.producer_json);
        defer config.deinit(a);
        const bytes = try producer.produce(a, .{
            .producer_type = config.type,
            .config_json = config.config_json,
            .source_text = source,
            .content_type = cfg.content_type,
        });
        return outputValue(a, cfg.content_type, bytes);
    }
};

pub const outputValue = named_outputs.outputValue;

test "serverless named producers read JSON assets by content type" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const object = try outputValue(a, "application/json; charset=utf-8", "{\"text\":\"x\"}");
    try std.testing.expect(object == .object);
    const text = try outputValue(a, "text/plain", "{\"text\":\"x\"}");
    try std.testing.expect(text == .string);
    try std.testing.expectError(error.InvalidProducerOutput, outputValue(a, "application/vnd.x+json", "not json"));
}

test "serverless named producers bind only consuming embeddings indexes" {
    const alloc = std.testing.allocator;
    const indexes =
        \\{"body_vec":{"type":"embeddings","dimension":4,"embedder":{"provider":"antfly","model":"m"}},
        \\ "passages_vec":{"type":"embeddings","dimension":3,"embedding_name":"passages_vec","embedder":{"provider":"antfly","model":"m"}},
        \\ "enrichments":[
        \\   {"name":"passages","kind":"chunk","field":"body","chunk_size":64},
        \\   {"name":"passages_vec","kind":"embedding","source_artifact_name":"passages","field":"text"}
        \\ ]}
    ;
    var declarations = try named_enrichments.parseAlloc(alloc, indexes);
    defer declarations.deinit(alloc);
    var named = try NamedProducers.init(alloc, indexes, declarations, .{}, null);
    defer named.deinit();
    try std.testing.expectEqual(@as(usize, 1), named.dims.count());
    try std.testing.expectEqual(@as(u32, 3), named.dims.get("passages_vec").?);
    try std.testing.expect(named.producers().embed_sparse == null);
    const hooks = named.producers();
    try std.testing.expect(hooks.asset == null);
}
