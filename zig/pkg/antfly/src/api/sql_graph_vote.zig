// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Live participant admission only. Ordered replay and decided resolution use
//! the immutable stored intent and never perform metadata I/O or refire a VM.
const std = @import("std");
const local = @import("antfly_local_sources");
const graph = @import("../system_catalog/graph_preparation.zig");
const catalog = @import("table_catalog.zig");
const types = local.storage_db_types;

pub fn require(alloc: std.mem.Allocator, source: catalog.CatalogSource, table: []const u8, request: types.BatchRequest, deadline_ns: ?u64) !void {
    const proof = request.graph_preparation orelse {
        // Preserve the established ordinary atomic graph batch lane. New
        // graph-bearing prepared intents alone require the capability witness.
        if (request.transaction == null and request.trigger_receipts.len == 0) return;
        if (request.graph_writes.len != 0 or request.graph_deletes.len != 0) return error.SqlTriggerCoordinationRequired;
        for (request.transforms) |transform| for (transform.operations) |operation| {
            if (try local.storage_db_transform.graphProjectionPath(operation.path) != null) return error.SqlTriggerCoordinationRequired;
        };
        return;
    };
    try proof.validate();
    if (request.trigger_receipts.len == 0) return error.SqlTriggerCoordinationRequired;
    var region: std.heap.ArenaAllocator = .init(alloc);
    defer region.deinit();
    const selected: graph.Source = .{ .table_id = proof.table_id, .epoch = .{ .incarnation = proof.incarnation, .revision = proof.source_revision }, .schema_digest = proof.schema_digest, .indexes = proof.indexes };
    // This is completed BEFORE acquiring any owner/apply fence. The native
    // vote independently checks the signed body, schema and index generations.
    const prepared = try source.graphPreparationRead(region.allocator(), selected, deadline_ns);
    if (prepared.snapshot.graph_intent_protocol < proof.data_protocol or prepared.snapshot.metadata_group_id != proof.metadata_group_id or !std.mem.eql(u8, prepared.snapshot.record.name, table)) return error.CatalogGenerationChanged;
}

test "graph preparation vote preserves ordinary atomic graph batches while new prepared effects fail closed without a witness" {
    const source = catalog.emptyCatalogSource();
    const request: types.BatchRequest = .{ .transforms = &.{.{ .key = "a", .operations = &.{.{ .op = .push, .path = "_edges.g.R", .value_json = "{\"target\":\"b\"}" }} }} };
    try require(std.testing.allocator, source, "docs", request, null);
    var prepared = request;
    prepared.transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 1 } };
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, require(std.testing.allocator, source, "docs", prepared, null));
    prepared.transforms = &.{};
    prepared.graph_writes = &.{.{ .source = "a", .target = "b", .index_name = "g", .edge_type = "R" }};
    try std.testing.expectError(error.SqlTriggerCoordinationRequired, require(std.testing.allocator, source, "docs", prepared, null));
}
