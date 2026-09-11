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

//! Local write request and configuration values shared with storage owners.

const std = @import("std");
const backups_api = @import("backups.zig");
const asset_producer_mod = @import("../storage/db/enrichment/asset_producer.zig");
const document_extraction_mod = @import("../storage/db/enrichment/document_extraction.zig");
const db_mod = @import("../storage/db/control_root.zig");
const doc_identity = @import("../storage/db/doc_identity_namespace.zig");

pub const StorageKernelArtifactDocumentRequest = struct {
    doc_key: []const u8,
    artifact_name: []const u8,
};

pub const StorageKernelEmbeddingCorruptionRequest = struct {
    doc_key: []const u8,
    index_name: []const u8,
};

pub const StorageKernelArtifactRangeRequest = struct {
    artifact_name: []const u8,
    request: db_mod.types.DocumentArtifactTableReprocessRequest,
};

pub const StorageKernelArtifactPlacementRequest = struct {
    doc_key: []const u8,
    artifact_name: []const u8,
    update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
};

pub const StorageKernelArtifactChildRangeBatchRequest = struct {
    doc_key: []const u8,
    artifact_name: []const u8,
    batch: db_mod.DocumentArtifactChildRangeApplyBatch,
};

pub fn encodeStorageKernelArtifactChildRangeBatchRequest(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    batch: db_mod.DocumentArtifactChildRangeApplyBatch,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, StorageKernelArtifactChildRangeBatchRequest{
        .doc_key = doc_key,
        .artifact_name = artifact_name,
        .batch = batch,
    }, .{ .emit_null_optional_fields = false });
}

pub const ManagedDbOpenMode = enum {
    default,
    default_async,
    writer_no_replay,
    startup_catch_up,
    restore_repair,
    query_readonly,
    status_only,
};

pub const StartupCatchUpMetadata = struct {
    pub const MetadataSource = enum { supplied, local_persisted };
    pub const IdentityValidation = enum {
        exact,
        reassign_same_table,
    };

    indexes_json: ?[]const u8 = null,
    schema_json: ?[]const u8 = null,
    identity_namespace: ?doc_identity.Namespace = null,
    identity_validation: IdentityValidation = .exact,
    target_index_name: ?[]const u8 = null,
    metadata_source: MetadataSource = .supplied,
    /// Internal owner-side executor mode. Normal startup inspection only
    /// discovers durable repair debt; the bounded repair worker sets this to
    /// advance at most one admitted intent.
    advance_index_repairs: bool = false,
    index_repair_options: db_mod.types.ArtifactRepairRunOptions = .{},
};

pub fn indexesJsonNeedsAssetProducer(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

pub fn indexesJsonHasGeneratedEnrichment(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

pub fn jsonValueHasGeneratedEnrichment(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            // Public embeddings configs are themselves the durable declaration
            // of a generated producer. Translation into the internal
            // `generator` form happens only while opening the DB, so startup
            // ownership planning must recognize the public form directly.
            // `external: true` is the explicit inverse: callers own vectors and
            // no resident enrichment runtime is required.
            if (object.get("type")) |index_type| {
                if (index_type == .string and std.mem.eql(u8, index_type.string, "embeddings")) {
                    const external = if (object.get("external")) |external_value|
                        external_value == .bool and external_value.bool
                    else
                        false;
                    if (!external) return true;
                }
            }
            if (object.get("kind")) |kind| {
                if (kind == .string and (std.mem.eql(u8, kind.string, "asset") or std.mem.eql(u8, kind.string, "chunk"))) return true;
            }
            if (object.get("generator") != null or object.get("chunker") != null) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueHasGeneratedEnrichment(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueHasGeneratedEnrichment(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringHasGeneratedEnrichment(alloc, raw);
        },
        else => return false,
    }
}

pub fn jsonStringHasGeneratedEnrichment(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueHasGeneratedEnrichment(alloc, parsed.value);
}

pub fn jsonValueNeedsAssetProducer(alloc: std.mem.Allocator, value: std.json.Value) anyerror!bool {
    switch (value) {
        .object => |object| {
            if (try objectIsModelBackedAssetEnrichment(alloc, object)) return true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (try jsonValueNeedsAssetProducer(alloc, entry.value_ptr.*)) return true;
            }
            return false;
        },
        .array => |array| {
            for (array.items) |item| {
                if (try jsonValueNeedsAssetProducer(alloc, item)) return true;
            }
            return false;
        },
        .string => |raw| {
            return try jsonStringNeedsAssetProducer(alloc, raw);
        },
        else => return false,
    }
}

pub fn jsonStringNeedsAssetProducer(alloc: std.mem.Allocator, raw: []const u8) anyerror!bool {
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    if (!jsonStringLooksStructured(trimmed)) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch return false;
    defer parsed.deinit();
    return try jsonValueNeedsAssetProducer(alloc, parsed.value);
}

pub fn jsonStringLooksStructured(trimmed: []const u8) bool {
    return trimmed.len >= 2 and
        ((trimmed[0] == '{' and trimmed[trimmed.len - 1] == '}') or
            (trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']'));
}

pub fn objectIsModelBackedAssetEnrichment(alloc: std.mem.Allocator, object: std.json.ObjectMap) !bool {
    const kind = object.get("kind") orelse return false;
    if (kind != .string or !std.mem.eql(u8, kind.string, "asset")) return false;
    const producer_value = object.get("producer_json") orelse return false;
    const producer_json = switch (producer_value) {
        .string => |raw| raw,
        .object, .array => try std.json.Stringify.valueAlloc(alloc, producer_value, .{}),
        else => return false,
    };
    const owns_producer_json = producer_value != .string;
    defer if (owns_producer_json) alloc.free(@constCast(producer_json));
    var producer_cfg = asset_producer_mod.parseProducerConfig(alloc, producer_json) catch return false;
    defer producer_cfg.deinit(alloc);
    if (producer_cfg.type == .document_extraction) {
        var extraction_cfg = document_extraction_mod.parseConfig(alloc, producer_cfg.config_json) catch return false;
        defer extraction_cfg.deinit(alloc);
        return extraction_cfg.ocr_enabled or
            extraction_cfg.ocr_pdf_fallback_enabled or
            extraction_cfg.transcription_enabled;
    }
    return producer_cfg.type != .copy;
}

pub fn freeStorageKernelBackupShards(
    alloc: std.mem.Allocator,
    shards: []const backups_api.ShardSnapshot,
) void {
    freeBackupShards(alloc, shards);
}

pub fn freeBackupShards(alloc: std.mem.Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(alloc);
    alloc.free(@constCast(shards));
}

pub const StorageKernelReconcileState = enum {
    complete,
    repair_pending,
    busy,
    degraded,
    restore_repair_pending,
};

pub const StorageKernelReconcileResult = struct {
    state: StorageKernelReconcileState = .complete,
    indexes_added: usize = 0,
    indexes_removed: usize = 0,
    indexes_pending: usize = 0,
    repair_discovered: usize = 0,
    repair_attempted: usize = 0,
    repair_repaired: usize = 0,
    repair_remaining: usize = 0,
    repair_terminal: usize = 0,
    repair_busy: usize = 0,
    repair_disk_waits: usize = 0,
    next_retry_at_ms: u64 = 0,
    restore_repair_attempted: usize = 0,
    restore_repair_progressed: usize = 0,
    restore_repair_pending: usize = 0,
};
