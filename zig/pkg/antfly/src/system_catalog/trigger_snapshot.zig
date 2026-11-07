// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Statement preparation reads the source and incoming FK directory from one
//! native catalog cut. A partial directory never proves there are no children.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;
pub const max_incoming_tables = 128;
pub const max_snapshot_bytes = 8 * 1024 * 1024;
const Table = @import("antfly_local_sources").common_topology_records.TableRecord;
pub const Request = struct {
    table_id: u64,
    expected_revision: ?u64 = null,
    pub fn validate(self: @This()) !void {
        if (self.table_id == 0) return error.InvalidCatalogMutation;
    }
};
pub const Snapshot = struct {
    metadata_group_id: u64 = 0,
    graph_intent_protocol: u16 = 0,
    graph_outcome_head: u64 = 0,
    revision: u64,
    epoch: std.meta.Child(@FieldType(domain.ResolvedMany, "relation_epoch")),
    source: domain.SqlSource,
    record: Table,
    incoming_records: []const Table,
    incoming_tables: []const u64,
    incoming_stamp: @FieldType(domain.QualifiedIncomingForeignKeys, "stamp"),
    coverage_revision: u64,
};
