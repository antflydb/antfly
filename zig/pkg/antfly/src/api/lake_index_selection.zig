// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Authorized native publication selection. Absence or stale coverage is an
//! automatic fallback; explicitly requested indexes fail closed.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const sql = local.sql_catalog;
const Store = @import("lake_index_store.zig").Store;
const coverage = @import("lake_index_coverage.zig");
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;

pub const Policy = enum { automatic, required };
pub const Selected = struct {
    state: std.json.Parsed(catalog.State),
    delete_objects: catalog.Digest,
    pub fn publication(self: Selected) catalog.Publication {
        return self.state.value.published.?;
    }
    pub fn deinit(self: *Selected) void {
        self.state.deinit();
        self.* = undefined;
    }
};

pub fn select(a: A, table: sql.Table, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, context: Context, policy: Policy) !?Selected {
    try context.ensureActive();
    return selectVerified(a, table, source, store, context) catch |err| {
        try context.ensureActive();
        if (err == error.OutOfMemory) return err;
        if (policy == .required) return err;
        return null;
    };
}
fn selectVerified(a: A, table: sql.Table, source: *local.serverless_query_lake_serving.ServingSource, store: *Store, context: Context) !Selected {
    const definitions = table.external_indexes orelse return error.ExternalLakeIndexUnavailable;
    var state = try catalog.parse(a, definitions.catalog_json);
    errdefer state.deinit();
    const published = state.value.published orelse return error.ExternalLakeIndexUnavailable;
    if (published.inventory.byte_len > (local.serverless_external_source_mod.codec.DecodeLimits{}).max_artifact_bytes) return error.ExternalSourceInventoryTooLarge;
    if (!std.mem.eql(u8, &published.signature.desired, &definitions.desired) or !std.mem.eql(u8, &published.signature.store, &store.identity)) return error.ExternalLakeIndexUnavailable;
    const binding = (table.external_base_source orelse return error.InvalidExternalTableBinding).binding;
    if (!std.mem.eql(u8, &published.signature.credentials, &try source.credentialIdentity(binding))) return error.ExternalLakeIndexUnavailable;
    const pinned = try coverage.pin(source, context);
    if (!std.mem.eql(u8, &published.signature.source, &pinned.source)) return error.ExternalLakeIndexUnavailable;
    var artifacts = store.artifactStore();
    artifacts.allocator = a;
    const normalized: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none;
    const bytes = try artifacts.getVerifiedAllocWithCancellation(published.inventory.artifact_id, published.inventory.byte_len, published.inventory.checksum, normalized);
    defer a.free(bytes);
    var inventory = try local.serverless_external_source_mod.decodeInventoryAlloc(a, bytes);
    defer inventory.deinit(a);
    try local.serverless_query_lake_scan_plan.validateBindingInventory(binding, inventory);
    // Manifest binding plus the current complete coverage signature prevents
    // snapshot-label-only reuse. Verified inventory identity checks defend the
    // artifact root before any sidecar lookup or row hydration.
    if (inventory.files.len != source.inventory.files.len) return error.InvalidExternalLakeIndexCoverage;
    var by_id: std.StringHashMapUnmanaged(*const local.serverless_external_source_types.FileEntry) = .empty;
    defer by_id.deinit(a);
    try by_id.ensureTotalCapacity(a, @intCast(inventory.files.len));
    for (inventory.files) |*file| by_id.putAssumeCapacity(file.file_id, file);
    for (source.inventory.files) |current| {
        const stored = by_id.get(current.file_id) orelse return error.InvalidExternalLakeIndexCoverage;
        if (!std.mem.eql(u8, stored.object_uri, current.object_uri) or
            !std.mem.eql(u8, stored.etag, current.etag) or !std.mem.eql(u8, stored.version_id, current.version_id) or
            stored.byte_len != current.byte_len or stored.row_count != current.row_count) return error.InvalidExternalLakeIndexCoverage;
    }
    try context.ensureActive();
    return .{ .state = state, .delete_objects = pinned.delete_objects };
}
