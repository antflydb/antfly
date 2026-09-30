// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const tables = @import("table_manager.zig");
const transitions = @import("transition_state.zig");

pub fn coordinated(alloc: std.mem.Allocator, schema: []const u8) !bool {
    const Shape = struct { unique_constraints: ?[]const struct {} = null, foreign_keys: ?[]const struct {} = null };
    var parsed = try std.json.parseFromSlice(Shape, alloc, if (schema.len == 0) "{}" else schema, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return (if (parsed.value.unique_constraints) |v| v.len else 0) != 0 or (if (parsed.value.foreign_keys) |v| v.len else 0) != 0;
}

pub fn requireStores(stores: []const tables.StoreRecord) !void {
    if (stores.len > 4096) return error.RelationalTopologyProtocolUpgradeRequired;
    var found = false;
    for (stores) |store| {
        if (!tables.storeServesTableData(store.role)) continue;
        found = true;
        if (store.reporter_incarnation == 0 or store.relational_topology_protocol_version != tables.relational_topology_protocol_version) return error.RelationalTopologyProtocolUpgradeRequired;
    }
    if (!found) return error.RelationalTopologyProtocolUpgradeRequired;
}

/// Produces a pinned capability contract before any data-group command is sent.
/// The metadata apply transaction repeats this check and activates a durable
/// floor, so future placements cannot introduce an older data runtime.
pub fn prepare(alloc: std.mem.Allocator, contract: *transitions.TransitionTableContract, stores: []const tables.StoreRecord) !void {
    if (!try coordinated(alloc, contract.schema_json) and !try coordinated(alloc, contract.read_schema_json)) {
        contract.integrity_protocol = .none;
        return;
    }
    if (contract.read_schema_json.len != 0) return error.RelationalTopologyMigrationUnsupported;
    try requireStores(stores);
    contract.integrity_protocol = .distributed_quiescent_v1;
}

pub fn requireStoreAtFloor(floor: u16, incoming: tables.StoreRecord) !void {
    if (!tables.storeServesTableData(incoming.role) or floor == 0) return;
    if (incoming.reporter_incarnation == 0 or incoming.relational_topology_protocol_version < floor) return error.RelationalTopologyProtocolUpgradeRequired;
}

test "relational topology admission requires complete distributed rollout and pins read mappings" {
    const alloc = std.testing.allocator;
    var contract: transitions.TransitionTableContract = .{ .schema_json = "{\"unique_constraints\":[{\"name\":\"pk\"}]}" };
    const capable: tables.StoreRecord = .{ .store_id = 1, .node_id = 1, .reporter_incarnation = 9, .relational_topology_protocol_version = tables.relational_topology_protocol_version };
    const old: tables.StoreRecord = .{ .store_id = 2, .node_id = 2 };
    try std.testing.expectError(error.RelationalTopologyProtocolUpgradeRequired, prepare(alloc, &contract, &.{ capable, old }));
    try std.testing.expectEqual(.none, contract.integrity_protocol);
    var metadata = old;
    metadata.role = "metadata";
    try prepare(alloc, &contract, &.{ capable, metadata });
    try std.testing.expectEqual(.distributed_quiescent_v1, contract.integrity_protocol);
    try std.testing.expectError(error.RelationalTopologyProtocolUpgradeRequired, requireStoreAtFloor(1, old));
    try requireStoreAtFloor(1, metadata);
    contract.read_schema_json = "{}";
    try std.testing.expectError(error.RelationalTopologyMigrationUnsupported, prepare(alloc, &contract, &.{capable}));
    var different = contract;
    different.read_schema_json = "{\"version\":1}";
    try std.testing.expect(!contract.eql(different));
}
