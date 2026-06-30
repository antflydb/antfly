// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const catalog_types = @import("../catalog/types.zig");
const manifest_base_source = @import("base_source.zig");
const manifest_types = @import("types.zig");
const search_sources = @import("../search_sources.zig");

pub const wire_magic = "AFSM";
pub const wire_version: u16 = 12;

const header_size_v2 = 4 + 2 + 4 + 8 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 4;
const header_size_v3 = header_size_v2 + 1 + 1;
const header_size_v5 = header_size_v3 + 4;
const header_size = header_size_v2 + 4 + 4;
const header_size_v7 = header_size + 4 + 4 + 4;
const policy_size = 98;
const header_size_v8 = header_size_v7 + policy_size;
const header_size_v10 = header_size_v8 + 8;
const header_size_v11 = header_size_v10 + 1;
const header_size_v12 = header_size_v11 + 4;

fn encodePolicy(buf: []u8, policy: catalog_types.NamespacePolicy) void {
    var pos: usize = 0;
    buf[pos] = @intFromEnum(policy.default_query_view);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], @intCast(policy.keep_latest_versions), .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], policy.max_pending_records, .little);
    pos += 8;
    buf[pos] = @intFromBool(policy.compaction_enabled);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], @intCast(policy.compaction_trigger_version_count), .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(policy.vector_compaction_max_cluster_imbalance), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @bitCast(policy.vector_compaction_max_distance_span), .little);
    pos += 4;
    buf[pos] = @intCast(@intFromEnum(policy.vector_distance_metric));
    pos += 1;
    buf[pos] = @intFromBool(policy.enrichment_enabled);
    pos += 1;
    buf[pos] = @intFromEnum(policy.lexical_sparse_model_preference);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], @intCast(policy.enrichment_batch_size), .little);
    pos += 8;
    buf[pos] = @intFromEnum(policy.enrichment_failure_policy);
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], policy.enrichment_publish_min_pending_records, .little);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], policy.enrichment_pipeline_version, .little);
    pos += 4;
    buf[pos] = @intFromBool(policy.chunk_preview_enabled);
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], policy.chunk_preview_pipeline_version, .little);
    pos += 4;
    std.mem.writeInt(u64, buf[pos..][0..8], policy.chunk_preview_publish_min_pending_records, .little);
    pos += 8;
    buf[pos] = @intFromBool(policy.chunk_embeddings_enabled);
    pos += 1;
    buf[pos] = @intFromEnum(policy.chunk_embeddings_model_preference);
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], policy.chunk_embeddings_pipeline_version, .little);
    pos += 4;
    std.mem.writeInt(u64, buf[pos..][0..8], policy.chunk_embeddings_publish_min_pending_records, .little);
    pos += 8;
    buf[pos] = @intFromBool(policy.rerank_terms_enabled);
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], policy.rerank_terms_pipeline_version, .little);
    pos += 4;
    std.mem.writeInt(u64, buf[pos..][0..8], policy.rerank_terms_publish_min_pending_records, .little);
    pos += 8;
    std.debug.assert(pos == policy_size);
}

fn decodePolicy(data: []const u8, pos_ptr: *usize) !catalog_types.NamespacePolicy {
    var pos = pos_ptr.*;
    if (pos + policy_size > data.len) return error.InvalidManifest;
    const policy: catalog_types.NamespacePolicy = .{
        .default_query_view = switch (data[pos]) {
            0 => .published,
            1 => .latest,
            else => return error.InvalidManifest,
        },
        .keep_latest_versions = @intCast(std.mem.readInt(u64, data[pos + 1 ..][0..8], .little)),
        .max_pending_records = std.mem.readInt(u64, data[pos + 9 ..][0..8], .little),
        .compaction_enabled = data[pos + 17] != 0,
        .compaction_trigger_version_count = @intCast(std.mem.readInt(u64, data[pos + 18 ..][0..8], .little)),
        .vector_compaction_max_cluster_imbalance = @bitCast(std.mem.readInt(u32, data[pos + 26 ..][0..4], .little)),
        .vector_compaction_max_distance_span = @bitCast(std.mem.readInt(u32, data[pos + 30 ..][0..4], .little)),
        .vector_distance_metric = switch (data[pos + 34]) {
            0 => .l2_squared,
            1 => .inner_product,
            2 => .cosine,
            else => return error.InvalidManifest,
        },
        .enrichment_enabled = data[pos + 35] != 0,
        .lexical_sparse_model_preference = switch (data[pos + 36]) {
            1 => .deterministic_only,
            2 => .prefer_model,
            3 => .require_model,
            else => return error.InvalidManifest,
        },
        .enrichment_batch_size = @intCast(std.mem.readInt(u64, data[pos + 37 ..][0..8], .little)),
        .enrichment_failure_policy = switch (data[pos + 45]) {
            1 => .skip_document,
            2 => .fail_stage,
            else => return error.InvalidManifest,
        },
        .enrichment_publish_min_pending_records = std.mem.readInt(u64, data[pos + 46 ..][0..8], .little),
        .enrichment_pipeline_version = std.mem.readInt(u32, data[pos + 54 ..][0..4], .little),
        .chunk_preview_enabled = data[pos + 58] != 0,
        .chunk_preview_pipeline_version = std.mem.readInt(u32, data[pos + 59 ..][0..4], .little),
        .chunk_preview_publish_min_pending_records = std.mem.readInt(u64, data[pos + 63 ..][0..8], .little),
        .chunk_embeddings_enabled = data[pos + 71] != 0,
        .chunk_embeddings_model_preference = switch (data[pos + 72]) {
            1 => .deterministic_only,
            2 => .prefer_model,
            3 => .require_model,
            else => return error.InvalidManifest,
        },
        .chunk_embeddings_pipeline_version = std.mem.readInt(u32, data[pos + 73 ..][0..4], .little),
        .chunk_embeddings_publish_min_pending_records = std.mem.readInt(u64, data[pos + 77 ..][0..8], .little),
        .rerank_terms_enabled = data[pos + 85] != 0,
        .rerank_terms_pipeline_version = std.mem.readInt(u32, data[pos + 86 ..][0..4], .little),
        .rerank_terms_publish_min_pending_records = std.mem.readInt(u64, data[pos + 90 ..][0..8], .little),
    };
    pos += policy_size;
    pos_ptr.* = pos;
    return policy;
}

fn artifactEncodedSize(artifact: manifest_types.ArtifactRef) usize {
    return 1 + 4 + 4 + 8 + 4 + artifact.name.len + artifact.artifact_id.len + artifact.checksum.len;
}

fn publishedSearchSourceEncodedSize(source: search_sources.SearchSourceDescriptor) usize {
    return 1 + 1 + 4 + source.indexName().len;
}

fn publishedSearchSourcesEncodedSize(sources: search_sources.PublishedSearchSources) usize {
    const items: []const search_sources.SearchSourceDescriptor = sources.items orelse &.{};
    var size: usize = 0;
    for (items) |item| size += publishedSearchSourceEncodedSize(item);
    return size;
}

fn derivedOutputEncodedSize(output: ?search_sources.DerivedOutputDescriptor) usize {
    if (output) |value| return 1 + 4 + value.name.len;
    return 0;
}

fn stringEncodedSize(value: []const u8) usize {
    return 4 + value.len;
}

fn optionalStringEncodedSize(value: ?[]const u8) usize {
    return stringEncodedSize(value orelse &.{});
}

fn stringListEncodedSize(values: []const []const u8) usize {
    var size: usize = 4;
    for (values) |value| size += stringEncodedSize(value);
    return size;
}

fn baseSourceEncodedSize(base_source: manifest_types.BaseSourceDescriptor) usize {
    return switch (base_source) {
        .antfly_document_segments, .antfly_lsm_overlay => 1,
        .antfly_row_fragments => |source| 1 +
            stringEncodedSize(source.snapshot_id) +
            stringEncodedSize(source.schema_fingerprint) +
            stringListEncodedSize(source.row_fragment_artifacts) +
            stringListEncodedSize(source.row_fragment_stats_artifacts),
        .external_parquet, .external_iceberg, .external_lance => |source| 1 + 1 +
            stringEncodedSize(source.source_uri) +
            stringEncodedSize(source.snapshot_id) +
            stringEncodedSize(source.schema_fingerprint) +
            optionalStringEncodedSize(source.file_inventory_artifact) +
            optionalStringEncodedSize(source.row_group_metadata_artifact) +
            optionalStringEncodedSize(source.delete_metadata_artifact),
    };
}

pub fn encodeAlloc(alloc: Allocator, manifest: manifest_types.Manifest) ![]u8 {
    const derived_output_items: []const search_sources.DerivedOutputDescriptor = manifest.stats.derived_outputs.items orelse &.{};
    const published_source_items: []const search_sources.SearchSourceDescriptor = manifest.stats.published_search_sources.items orelse &.{};
    const base_source_len: u32 = if (manifest.base_source) |base_source|
        @intCast(baseSourceEncodedSize(base_source))
    else
        0;
    var size: usize = header_size_v12 + manifest.namespace.len +
        manifest.stats.schema_json.len +
        manifest.stats.read_schema_json.len +
        manifest.stats.indexes_json.len +
        publishedSearchSourcesEncodedSize(manifest.stats.published_search_sources) +
        base_source_len;
    for (derived_output_items) |output| size += derivedOutputEncodedSize(output);
    for (manifest.artifacts) |artifact| size += artifactEncodedSize(artifact);

    const buf = try alloc.alloc(u8, size);
    errdefer alloc.free(buf);

    var pos: usize = 0;
    @memcpy(buf[pos..][0..4], wire_magic);
    pos += 4;
    std.mem.writeInt(u16, buf[pos..][0..2], wire_version, .little);
    pos += 2;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(manifest.namespace.len), .little);
    pos += 4;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.version, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.built_at_ns, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.wal_start_lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.wal_end_lsn, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.stats.document_count, .little);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], manifest.stats.document_base_version, .little);
    pos += 8;
    buf[pos] = @intFromEnum(manifest.stats.document_publish_mode);
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], manifest.stats.text_segment_count, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], manifest.stats.vector_segment_count, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], manifest.stats.sparse_segment_count, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], manifest.stats.graph_segment_count, .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(manifest.artifacts.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(published_source_items.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(derived_output_items.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(manifest.stats.schema_json.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(manifest.stats.read_schema_json.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(manifest.stats.indexes_json.len), .little);
    pos += 4;
    encodePolicy(buf[pos .. pos + policy_size], manifest.stats.policy);
    pos += policy_size;
    std.mem.writeInt(u32, buf[pos..][0..4], base_source_len, .little);
    pos += 4;

    @memcpy(buf[pos..][0..manifest.namespace.len], manifest.namespace);
    pos += manifest.namespace.len;
    @memcpy(buf[pos..][0..manifest.stats.schema_json.len], manifest.stats.schema_json);
    pos += manifest.stats.schema_json.len;
    @memcpy(buf[pos..][0..manifest.stats.read_schema_json.len], manifest.stats.read_schema_json);
    pos += manifest.stats.read_schema_json.len;
    @memcpy(buf[pos..][0..manifest.stats.indexes_json.len], manifest.stats.indexes_json);
    pos += manifest.stats.indexes_json.len;

    for (published_source_items) |source| {
        switch (source) {
            .text => |value| {
                buf[pos] = 3;
                pos += 1;
                buf[pos] = 1;
                pos += 1;
                std.mem.writeInt(u32, buf[pos..][0..4], @intCast(value.index_name.len), .little);
                pos += 4;
                @memcpy(buf[pos..][0..value.index_name.len], value.index_name);
                pos += value.index_name.len;
            },
            .vector => |value| {
                buf[pos] = 1;
                pos += 1;
                buf[pos] = switch (value.document_source) {
                    .top_level_embedding => 1,
                    .chunk_embeddings => 2,
                    .chunk_embeddings_or_top_level => 3,
                };
                pos += 1;
                std.mem.writeInt(u32, buf[pos..][0..4], @intCast(value.index_name.len), .little);
                pos += 4;
                @memcpy(buf[pos..][0..value.index_name.len], value.index_name);
                pos += value.index_name.len;
            },
            .sparse => |value| {
                buf[pos] = 2;
                pos += 1;
                buf[pos] = switch (value.document_source) {
                    .sparse_embedding => 1,
                };
                pos += 1;
                std.mem.writeInt(u32, buf[pos..][0..4], @intCast(value.index_name.len), .little);
                pos += 4;
                @memcpy(buf[pos..][0..value.index_name.len], value.index_name);
                pos += value.index_name.len;
            },
        }
    }

    for (derived_output_items) |output| {
        buf[pos] = switch (output.kind) {
            .chunk_preview => 1,
            .chunk_embeddings => 2,
            .rerank_terms => 3,
        };
        pos += 1;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(output.name.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..output.name.len], output.name);
        pos += output.name.len;
    }

    for (manifest.artifacts) |artifact| {
        buf[pos] = @intFromEnum(artifact.kind);
        pos += 1;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(artifact.name.len), .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(artifact.artifact_id.len), .little);
        pos += 4;
        std.mem.writeInt(u64, buf[pos..][0..8], artifact.byte_len, .little);
        pos += 8;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(artifact.checksum.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..artifact.name.len], artifact.name);
        pos += artifact.name.len;
        @memcpy(buf[pos..][0..artifact.artifact_id.len], artifact.artifact_id);
        pos += artifact.artifact_id.len;
        @memcpy(buf[pos..][0..artifact.checksum.len], artifact.checksum);
        pos += artifact.checksum.len;
    }

    if (manifest.base_source) |base_source| {
        encodeBaseSource(buf[pos .. pos + base_source_len], base_source);
        pos += base_source_len;
    }

    std.debug.assert(pos == buf.len);
    return buf;
}

fn encodeString(buf: []u8, pos_ptr: *usize, value: []const u8) void {
    var pos = pos_ptr.*;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(value.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..value.len], value);
    pos += value.len;
    pos_ptr.* = pos;
}

fn encodeOptionalString(buf: []u8, pos_ptr: *usize, value: ?[]const u8) void {
    encodeString(buf, pos_ptr, value orelse &.{});
}

fn encodeStringList(buf: []u8, pos_ptr: *usize, values: []const []const u8) void {
    var pos = pos_ptr.*;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(values.len), .little);
    pos += 4;
    pos_ptr.* = pos;
    for (values) |value| encodeString(buf, pos_ptr, value);
}

fn encodeBaseSource(buf: []u8, descriptor: manifest_types.BaseSourceDescriptor) void {
    var pos: usize = 0;
    buf[pos] = @intFromEnum(std.meta.activeTag(descriptor));
    pos += 1;

    switch (descriptor) {
        .antfly_document_segments, .antfly_lsm_overlay => {},
        .antfly_row_fragments => |source| {
            encodeString(buf, &pos, source.snapshot_id);
            encodeString(buf, &pos, source.schema_fingerprint);
            encodeStringList(buf, &pos, source.row_fragment_artifacts);
            encodeStringList(buf, &pos, source.row_fragment_stats_artifacts);
        },
        .external_parquet, .external_iceberg, .external_lance => |source| {
            buf[pos] = @intFromEnum(source.format);
            pos += 1;
            encodeString(buf, &pos, source.source_uri);
            encodeString(buf, &pos, source.snapshot_id);
            encodeString(buf, &pos, source.schema_fingerprint);
            encodeOptionalString(buf, &pos, source.file_inventory_artifact);
            encodeOptionalString(buf, &pos, source.row_group_metadata_artifact);
            encodeOptionalString(buf, &pos, source.delete_metadata_artifact);
        },
    }

    std.debug.assert(pos == buf.len);
}

pub fn decodeAlloc(alloc: Allocator, data: []const u8) !manifest_types.Manifest {
    if (data.len < header_size_v2) return error.InvalidManifest;

    var pos: usize = 0;
    if (!std.mem.eql(u8, data[pos..][0..4], wire_magic)) return error.InvalidManifest;
    pos += 4;

    const version = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += 2;
    if (version != 2 and version != 3 and version != 4 and version != 5 and version != 6 and version != 7 and version != 8 and version != 10 and version != 11 and version != wire_version) return error.UnsupportedManifestVersion;

    const namespace_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const manifest_version = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const built_at_ns = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const wal_start_lsn = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const wal_end_lsn = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const document_count = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;
    const document_base_version = if (version >= 10) blk: {
        const value = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        break :blk value;
    } else 0;
    const document_publish_mode = if (version >= 11) blk: {
        const value: catalog_types.DocumentPublishMode = switch (data[pos]) {
            1 => .append_mutation_tail,
            2 => .inline_rebase,
            3 => .head_republish,
            else => return error.InvalidManifest,
        };
        pos += 1;
        break :blk value;
    } else if (wal_start_lsn == wal_end_lsn)
        catalog_types.DocumentPublishMode.inline_rebase
    else
        catalog_types.DocumentPublishMode.append_mutation_tail;
    const text_segment_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const vector_segment_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const sparse_segment_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const graph_segment_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const artifact_count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const has_vector_source = if (version >= 3 and version <= 5) blk: {
        const value = data[pos] != 0;
        pos += 1;
        break :blk value;
    } else false;
    const has_sparse_source = if (version >= 3 and version <= 5) blk: {
        const value = data[pos] != 0;
        pos += 1;
        break :blk value;
    } else false;
    const published_search_source_count = if (version >= 6) blk: {
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;
    const has_chunk_preview_output = if (version == 4) blk: {
        const value = data[pos] != 0;
        pos += 1;
        break :blk value;
    } else false;
    const has_rerank_terms_output = if (version == 4) blk: {
        const value = data[pos] != 0;
        pos += 1;
        break :blk value;
    } else false;
    const derived_output_count = if (version >= 5) blk: {
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;
    const schema_len = if (version >= 7) blk: {
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;
    const read_schema_len = if (version >= 7) blk: {
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;
    const indexes_len = if (version >= 7) blk: {
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;
    const policy = if (version >= 8) try decodePolicy(data, &pos) else catalog_types.NamespacePolicy{};
    const base_source_len = if (version >= 12) blk: {
        if (pos + 4 > data.len) return error.InvalidManifest;
        const value = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        break :blk value;
    } else 0;

    if (pos + namespace_len > data.len) return error.InvalidManifest;
    const namespace = try alloc.dupe(u8, data[pos .. pos + namespace_len]);
    pos += namespace_len;
    errdefer alloc.free(namespace);

    var schema_json: []u8 = &.{};
    errdefer if (schema_json.len > 0) alloc.free(schema_json);
    var read_schema_json: []u8 = &.{};
    errdefer if (read_schema_json.len > 0) alloc.free(read_schema_json);
    var indexes_json: []u8 = &.{};
    errdefer if (indexes_json.len > 0) alloc.free(indexes_json);
    if (version >= 7) {
        if (pos + schema_len + read_schema_len + indexes_len > data.len) return error.InvalidManifest;
        if (schema_len > 0) schema_json = try alloc.dupe(u8, data[pos .. pos + schema_len]);
        pos += schema_len;
        if (read_schema_len > 0) read_schema_json = try alloc.dupe(u8, data[pos .. pos + read_schema_len]);
        pos += read_schema_len;
        if (indexes_len > 0) indexes_json = try alloc.dupe(u8, data[pos .. pos + indexes_len]);
        pos += indexes_len;
    }

    var published_search_sources: search_sources.PublishedSearchSources = .{};
    errdefer search_sources.deinitPublishedSearchSources(alloc, &published_search_sources);
    if (version >= 6) {
        if (published_search_source_count > 0) {
            const items = try alloc.alloc(search_sources.SearchSourceDescriptor, published_search_source_count);
            errdefer alloc.free(items);
            var initialized_sources: usize = 0;
            errdefer {
                for (items[0..initialized_sources]) |*item| search_sources.deinitSearchSourceDescriptor(alloc, item);
            }
            for (0..published_search_source_count) |idx| {
                if (pos + 1 + 1 + 4 > data.len) return error.InvalidManifest;
                const source_kind = data[pos];
                pos += 1;
                const kind = data[pos];
                pos += 1;
                const index_name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
                if (pos + index_name_len > data.len) return error.InvalidManifest;
                const index_name = try alloc.dupe(u8, data[pos .. pos + index_name_len]);
                pos += index_name_len;
                items[idx] = switch (source_kind) {
                    1 => .{ .vector = .{
                        .index_name = index_name,
                        .document_source = switch (kind) {
                            1 => .top_level_embedding,
                            2 => .chunk_embeddings,
                            3 => .chunk_embeddings_or_top_level,
                            else => return error.InvalidManifest,
                        },
                    } },
                    2 => .{ .sparse = .{
                        .index_name = index_name,
                        .document_source = switch (kind) {
                            1 => .sparse_embedding,
                            else => return error.InvalidManifest,
                        },
                    } },
                    3 => .{ .text = .{
                        .index_name = index_name,
                    } },
                    else => return error.InvalidManifest,
                };
                initialized_sources += 1;
            }
            published_search_sources = blk: {
                const owned = items;
                var out = search_sources.PublishedSearchSources{ .items = owned };
                for (owned) |item| switch (item) {
                    .text => |value| {
                        if (out.text == null) out.text = value;
                    },
                    .vector => |value| out.vector = value,
                    .sparse => |value| out.sparse = value,
                };
                break :blk out;
            };
        }
    } else {
        var legacy_items = std.ArrayListUnmanaged(search_sources.SearchSourceDescriptor).empty;
        errdefer {
            for (legacy_items.items) |*item| search_sources.deinitSearchSourceDescriptor(alloc, item);
            legacy_items.deinit(alloc);
        }
        if (has_vector_source) {
            const value = blk: {
                if (pos + 1 + 4 > data.len) return error.InvalidManifest;
                const kind = data[pos];
                pos += 1;
                const index_name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
                if (pos + index_name_len > data.len) return error.InvalidManifest;
                const index_name = try alloc.dupe(u8, data[pos .. pos + index_name_len]);
                pos += index_name_len;
                break :blk search_sources.VectorSourceDescriptor{
                    .index_name = index_name,
                    .document_source = switch (kind) {
                        1 => .top_level_embedding,
                        2 => .chunk_embeddings,
                        3 => .chunk_embeddings_or_top_level,
                        else => return error.InvalidManifest,
                    },
                };
            };
            try legacy_items.append(alloc, .{ .vector = value });
        }
        if (has_sparse_source) {
            const value = blk: {
                if (pos + 1 + 4 > data.len) return error.InvalidManifest;
                const kind = data[pos];
                pos += 1;
                const index_name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
                if (pos + index_name_len > data.len) return error.InvalidManifest;
                const index_name = try alloc.dupe(u8, data[pos .. pos + index_name_len]);
                pos += index_name_len;
                break :blk search_sources.SparseSourceDescriptor{
                    .index_name = index_name,
                    .document_source = switch (kind) {
                        1 => .sparse_embedding,
                        else => return error.InvalidManifest,
                    },
                };
            };
            try legacy_items.append(alloc, .{ .sparse = value });
        }
        if (legacy_items.items.len > 0) {
            const owned = try legacy_items.toOwnedSlice(alloc);
            var out = search_sources.PublishedSearchSources{ .items = owned };
            for (owned) |item| switch (item) {
                .text => |value| {
                    if (out.text == null) out.text = value;
                },
                .vector => |value| out.vector = value,
                .sparse => |value| out.sparse = value,
            };
            published_search_sources = out;
        }
    }

    var derived_outputs: search_sources.MaterializedDerivedOutputs = .{};
    errdefer search_sources.deinitMaterializedDerivedOutputs(alloc, &derived_outputs);
    if (version >= 5) {
        if (derived_output_count > 0) {
            const items = try alloc.alloc(search_sources.DerivedOutputDescriptor, derived_output_count);
            errdefer alloc.free(items);
            var initialized_outputs: usize = 0;
            errdefer {
                for (items[0..initialized_outputs]) |*item| search_sources.deinitDerivedOutputDescriptor(alloc, item);
            }
            for (0..derived_output_count) |idx| {
                if (pos + 1 + 4 > data.len) return error.InvalidManifest;
                const kind = data[pos];
                pos += 1;
                const name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
                if (pos + name_len > data.len) return error.InvalidManifest;
                const name = try alloc.dupe(u8, data[pos .. pos + name_len]);
                pos += name_len;
                items[idx] = .{
                    .name = name,
                    .kind = switch (kind) {
                        1 => .chunk_preview,
                        2 => .chunk_embeddings,
                        3 => .rerank_terms,
                        else => return error.InvalidManifest,
                    },
                };
                initialized_outputs += 1;
            }
            derived_outputs = .{ .items = items };
        }
    } else {
        var legacy_outputs = std.ArrayListUnmanaged(search_sources.DerivedOutputDescriptor).empty;
        errdefer {
            for (legacy_outputs.items) |*item| search_sources.deinitDerivedOutputDescriptor(alloc, item);
            legacy_outputs.deinit(alloc);
        }
        if (has_chunk_preview_output) {
            if (pos + 1 + 4 > data.len) return error.InvalidManifest;
            const kind = data[pos];
            pos += 1;
            const name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            if (pos + name_len > data.len) return error.InvalidManifest;
            const name = try alloc.dupe(u8, data[pos .. pos + name_len]);
            pos += name_len;
            try legacy_outputs.append(alloc, .{
                .name = name,
                .kind = switch (kind) {
                    1 => .chunk_preview,
                    2 => .chunk_embeddings,
                    3 => .rerank_terms,
                    else => return error.InvalidManifest,
                },
            });
        }
        if (has_rerank_terms_output) {
            if (pos + 1 + 4 > data.len) return error.InvalidManifest;
            const kind = data[pos];
            pos += 1;
            const name_len = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            if (pos + name_len > data.len) return error.InvalidManifest;
            const name = try alloc.dupe(u8, data[pos .. pos + name_len]);
            pos += name_len;
            try legacy_outputs.append(alloc, .{
                .name = name,
                .kind = switch (kind) {
                    1 => .chunk_preview,
                    2 => .chunk_embeddings,
                    3 => .rerank_terms,
                    else => return error.InvalidManifest,
                },
            });
        }
        if (legacy_outputs.items.len > 0) {
            derived_outputs = .{ .items = try legacy_outputs.toOwnedSlice(alloc) };
        } else {
            legacy_outputs.deinit(alloc);
        }
    }

    const artifacts = try alloc.alloc(manifest_types.ArtifactRef, artifact_count);
    errdefer alloc.free(artifacts);

    var initialized: usize = 0;
    errdefer {
        for (artifacts[0..initialized]) |artifact| {
            if (artifact.name.len > 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
    }

    for (0..artifact_count) |idx| {
        const min_artifact_header_len: usize = if (version >= 9) 1 + 4 + 4 + 8 + 4 else 1 + 4 + 8 + 4;
        if (pos + min_artifact_header_len > data.len) return error.InvalidManifest;
        const kind: manifest_types.ArtifactKind = @enumFromInt(data[pos]);
        pos += 1;
        const name_len = if (version >= 9) blk: {
            const value = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            break :blk value;
        } else 0;
        const artifact_id_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;
        const byte_len = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        const checksum_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        if (pos + name_len + artifact_id_len + checksum_len > data.len) return error.InvalidManifest;
        const name = if (name_len > 0) try alloc.dupe(u8, data[pos .. pos + name_len]) else &.{};
        pos += name_len;
        errdefer if (name.len > 0) alloc.free(name);
        const artifact_id = try alloc.dupe(u8, data[pos .. pos + artifact_id_len]);
        pos += artifact_id_len;
        errdefer alloc.free(artifact_id);
        const checksum = try alloc.dupe(u8, data[pos .. pos + checksum_len]);
        pos += checksum_len;

        artifacts[idx] = .{
            .kind = kind,
            .name = name,
            .artifact_id = artifact_id,
            .byte_len = byte_len,
            .checksum = checksum,
        };
        initialized += 1;
    }

    var base_source: ?manifest_types.BaseSourceDescriptor = null;
    errdefer if (base_source) |*descriptor| manifest_base_source.freeOwnedDescriptor(alloc, descriptor);
    if (version >= 12) {
        if (pos + base_source_len > data.len) return error.InvalidManifest;
        if (base_source_len > 0) {
            base_source = try decodeBaseSourceAlloc(alloc, data[pos .. pos + base_source_len]);
        }
        pos += base_source_len;
    }

    if (pos != data.len) return error.InvalidManifest;

    return .{
        .namespace = namespace,
        .version = manifest_version,
        .built_at_ns = built_at_ns,
        .wal_start_lsn = wal_start_lsn,
        .wal_end_lsn = wal_end_lsn,
        .base_source = base_source,
        .stats = .{
            .document_count = document_count,
            .document_base_version = document_base_version,
            .document_publish_mode = document_publish_mode,
            .text_segment_count = text_segment_count,
            .vector_segment_count = vector_segment_count,
            .sparse_segment_count = sparse_segment_count,
            .graph_segment_count = graph_segment_count,
            .published_search_sources = published_search_sources,
            .derived_outputs = derived_outputs,
            .policy = policy,
            .schema_json = schema_json,
            .read_schema_json = read_schema_json,
            .indexes_json = indexes_json,
        },
        .artifacts = artifacts,
    };
}

fn decodeStringAlloc(alloc: Allocator, data: []const u8, pos_ptr: *usize) ![]const u8 {
    var pos = pos_ptr.*;
    if (pos + 4 > data.len) return error.InvalidManifest;
    const len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    if (pos + len > data.len) return error.InvalidManifest;
    const value = try alloc.dupe(u8, data[pos .. pos + len]);
    pos += len;
    pos_ptr.* = pos;
    return value;
}

fn decodeOptionalStringAlloc(alloc: Allocator, data: []const u8, pos_ptr: *usize) !?[]const u8 {
    const value = try decodeStringAlloc(alloc, data, pos_ptr);
    if (value.len == 0) {
        alloc.free(value);
        return null;
    }
    return value;
}

fn decodeStringListAlloc(alloc: Allocator, data: []const u8, pos_ptr: *usize) ![]const []const u8 {
    var pos = pos_ptr.*;
    if (pos + 4 > data.len) return error.InvalidManifest;
    const count = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    pos_ptr.* = pos;
    if (count == 0) return &.{};

    const out = try alloc.alloc([]const u8, count);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
    }
    for (0..count) |idx| {
        out[idx] = try decodeStringAlloc(alloc, data, pos_ptr);
        initialized += 1;
    }
    return out;
}

fn freeStringList(alloc: Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len != 0) alloc.free(values);
}

fn decodeBaseSourceAlloc(alloc: Allocator, data: []const u8) !manifest_types.BaseSourceDescriptor {
    if (data.len == 0) return error.InvalidManifest;
    var pos: usize = 0;
    const kind_value = data[pos];
    pos += 1;

    const descriptor: manifest_types.BaseSourceDescriptor = switch (kind_value) {
        @intFromEnum(manifest_types.BaseSourceKind.antfly_document_segments) => .{ .antfly_document_segments = {} },
        @intFromEnum(manifest_types.BaseSourceKind.antfly_lsm_overlay) => .{ .antfly_lsm_overlay = {} },
        @intFromEnum(manifest_types.BaseSourceKind.antfly_row_fragments) => blk: {
            const snapshot_id = try decodeStringAlloc(alloc, data, &pos);
            errdefer alloc.free(snapshot_id);
            const schema_fingerprint = try decodeStringAlloc(alloc, data, &pos);
            errdefer alloc.free(schema_fingerprint);
            const row_fragment_artifacts = try decodeStringListAlloc(alloc, data, &pos);
            errdefer freeStringList(alloc, row_fragment_artifacts);
            const row_fragment_stats_artifacts = try decodeStringListAlloc(alloc, data, &pos);
            errdefer freeStringList(alloc, row_fragment_stats_artifacts);
            break :blk .{ .antfly_row_fragments = .{
                .snapshot_id = snapshot_id,
                .schema_fingerprint = schema_fingerprint,
                .row_fragment_artifacts = row_fragment_artifacts,
                .row_fragment_stats_artifacts = row_fragment_stats_artifacts,
            } };
        },
        @intFromEnum(manifest_types.BaseSourceKind.external_parquet),
        @intFromEnum(manifest_types.BaseSourceKind.external_iceberg),
        @intFromEnum(manifest_types.BaseSourceKind.external_lance),
        => blk: {
            if (pos + 1 > data.len) return error.InvalidManifest;
            const format: manifest_types.ExternalBaseFormat = switch (data[pos]) {
                @intFromEnum(manifest_types.ExternalBaseFormat.parquet_prefix) => .parquet_prefix,
                @intFromEnum(manifest_types.ExternalBaseFormat.iceberg) => .iceberg,
                @intFromEnum(manifest_types.ExternalBaseFormat.lance) => .lance,
                else => return error.InvalidManifest,
            };
            pos += 1;
            const source_uri = try decodeStringAlloc(alloc, data, &pos);
            errdefer alloc.free(source_uri);
            const snapshot_id = try decodeStringAlloc(alloc, data, &pos);
            errdefer alloc.free(snapshot_id);
            const schema_fingerprint = try decodeStringAlloc(alloc, data, &pos);
            errdefer alloc.free(schema_fingerprint);
            const file_inventory_artifact = try decodeOptionalStringAlloc(alloc, data, &pos);
            errdefer if (file_inventory_artifact) |artifact_id| alloc.free(artifact_id);
            const row_group_metadata_artifact = try decodeOptionalStringAlloc(alloc, data, &pos);
            errdefer if (row_group_metadata_artifact) |artifact_id| alloc.free(artifact_id);
            const delete_metadata_artifact = try decodeOptionalStringAlloc(alloc, data, &pos);
            errdefer if (delete_metadata_artifact) |artifact_id| alloc.free(artifact_id);
            const source = manifest_types.ExternalBaseSource{
                .format = format,
                .source_uri = source_uri,
                .snapshot_id = snapshot_id,
                .schema_fingerprint = schema_fingerprint,
                .file_inventory_artifact = file_inventory_artifact,
                .row_group_metadata_artifact = row_group_metadata_artifact,
                .delete_metadata_artifact = delete_metadata_artifact,
            };
            break :blk switch (kind_value) {
                @intFromEnum(manifest_types.BaseSourceKind.external_parquet) => .{ .external_parquet = source },
                @intFromEnum(manifest_types.BaseSourceKind.external_iceberg) => .{ .external_iceberg = source },
                @intFromEnum(manifest_types.BaseSourceKind.external_lance) => .{ .external_lance = source },
                else => unreachable,
            };
        },
        else => return error.InvalidManifest,
    };

    if (pos != data.len) {
        var cleanup = descriptor;
        manifest_base_source.freeOwnedDescriptor(alloc, &cleanup);
        return error.InvalidManifest;
    }
    descriptor.validate() catch |err| {
        var cleanup = descriptor;
        manifest_base_source.freeOwnedDescriptor(alloc, &cleanup);
        return err;
    };
    return descriptor;
}

test "manifest codec round-trips deterministically" {
    const alloc = std.testing.allocator;
    var manifest = manifest_types.Manifest{
        .namespace = try alloc.dupe(u8, "products"),
        .version = 42,
        .built_at_ns = 123456,
        .wal_start_lsn = 1000,
        .wal_end_lsn = 1050,
        .stats = .{
            .document_count = 99,
            .document_base_version = 42,
            .document_publish_mode = .head_republish,
            .text_segment_count = 2,
            .vector_segment_count = 1,
            .sparse_segment_count = 1,
            .graph_segment_count = 1,
            .published_search_sources = try search_sources.defaultPublishedSearchSourcesAlloc(alloc),
            .derived_outputs = try search_sources.defaultMaterializedDerivedOutputsAlloc(alloc),
        },
        .artifacts = try alloc.alloc(manifest_types.ArtifactRef, 4),
    };
    defer manifest.deinit(alloc);

    manifest.artifacts[0] = .{
        .kind = .text_segment,
        .name = try alloc.dupe(u8, "full_text_index_v0"),
        .artifact_id = try alloc.dupe(u8, "text-0001"),
        .byte_len = 4096,
        .checksum = try alloc.dupe(u8, "sha256:text"),
    };
    manifest.artifacts[1] = .{
        .kind = .vector_segment,
        .artifact_id = try alloc.dupe(u8, "vec-0001"),
        .byte_len = 2048,
        .checksum = try alloc.dupe(u8, "sha256:vec"),
    };
    manifest.artifacts[2] = .{
        .kind = .sparse_segment,
        .artifact_id = try alloc.dupe(u8, "sparse-0001"),
        .byte_len = 1024,
        .checksum = try alloc.dupe(u8, "sha256:sparse"),
    };
    manifest.artifacts[3] = .{
        .kind = .graph_segment,
        .artifact_id = try alloc.dupe(u8, "graph-0001"),
        .byte_len = 512,
        .checksum = try alloc.dupe(u8, "sha256:graph"),
    };

    const encoded_a = try encodeAlloc(alloc, manifest);
    defer alloc.free(encoded_a);
    const encoded_b = try encodeAlloc(alloc, manifest);
    defer alloc.free(encoded_b);

    try std.testing.expectEqualSlices(u8, encoded_a, encoded_b);

    var decoded = try decodeAlloc(alloc, encoded_a);
    defer decoded.deinit(alloc);

    try std.testing.expectEqualStrings("products", decoded.namespace);
    try std.testing.expectEqual(@as(u64, 42), decoded.version);
    try std.testing.expectEqual(@as(u64, 99), decoded.stats.document_count);
    try std.testing.expectEqual(@as(u64, 42), decoded.stats.document_base_version);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.head_republish, decoded.stats.document_publish_mode);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embedding_index_name, decoded.stats.published_search_sources.findVector().?.index_name);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, decoded.stats.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqualStrings(search_sources.default_chunk_preview_output_name, decoded.stats.derived_outputs.findByKind(.chunk_preview).?.name);
    try std.testing.expectEqualStrings(search_sources.default_chunk_embeddings_output_name, decoded.stats.derived_outputs.findByKind(.chunk_embeddings).?.name);
    try std.testing.expectEqualStrings(search_sources.default_rerank_terms_output_name, decoded.stats.derived_outputs.findByKind(.rerank_terms).?.name);
    try std.testing.expectEqual(@as(usize, 4), decoded.artifacts.len);
    try std.testing.expectEqual(manifest_types.ArtifactKind.text_segment, decoded.artifacts[0].kind);
    try std.testing.expectEqualStrings("vec-0001", decoded.artifacts[1].artifact_id);
    try std.testing.expectEqual(manifest_types.ArtifactKind.sparse_segment, decoded.artifacts[2].kind);
    try std.testing.expectEqual(manifest_types.ArtifactKind.graph_segment, decoded.artifacts[3].kind);
}

test "manifest codec round-trips optional lake base source" {
    const alloc = std.testing.allocator;
    var manifest = manifest_types.Manifest{
        .namespace = try alloc.dupe(u8, "events"),
        .version = 12,
        .built_at_ns = 123456,
        .wal_start_lsn = 0,
        .wal_end_lsn = 0,
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
            .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
            .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
            .file_inventory_artifact = try alloc.dupe(u8, "external-files-0001"),
        } },
        .stats = .{
            .document_count = 0,
            .document_base_version = 12,
        },
        .artifacts = try alloc.alloc(manifest_types.ArtifactRef, 1),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .external_base_source,
        .name = try alloc.dupe(u8, "events.files"),
        .artifact_id = try alloc.dupe(u8, "external-files-0001"),
        .byte_len = 4096,
        .checksum = try alloc.dupe(u8, "sha256:files"),
    };

    const encoded = try encodeAlloc(alloc, manifest);
    defer alloc.free(encoded);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expect(decoded.base_source != null);
    try std.testing.expectEqual(manifest_types.BaseSourceKind.external_iceberg, std.meta.activeTag(decoded.base_source.?));
    try std.testing.expectEqualStrings("iceberg-123", decoded.base_source.?.external_iceberg.snapshot_id);
    try std.testing.expectEqualStrings("external-files-0001", decoded.base_source.?.external_iceberg.file_inventory_artifact.?);
    try std.testing.expectEqual(@as(usize, 1), decoded.artifacts.len);
    try std.testing.expectEqual(manifest_types.ArtifactKind.external_base_source, decoded.artifacts[0].kind);
    try std.testing.expectEqualStrings("external-files-0001", decoded.artifacts[0].artifact_id);
}

test "manifest codec rejects bad magic" {
    const alloc = std.testing.allocator;
    const bad = [_]u8{ 'B', 'A', 'D', '!', 1, 0 };
    try std.testing.expectError(error.InvalidManifest, decodeAlloc(alloc, &bad));
}
