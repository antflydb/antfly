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

//! Ordered row effects with a leader-selected commit clock.
const std = @import("std");
const descriptor = @import("../storage/kernel_owner_descriptor.zig");
const batch_api = @import("antfly_local_sources").api_internal_batch;
pub const required_protocol_version = @import("../common/data_raft_protocol.zig").batch_history_clock_protocol_version;
pub const max_bytes = 16 * 1024 * 1024;

pub const Entry = struct {
    table: []const u8,
    batch: ?u8 = null,
    protocol: u16 = required_protocol_version,
    storage_owner: descriptor.Descriptor,
    history_commit_time_ns: i64,
    row_batch_json: []const u8,

    pub fn validate(self: Entry) !void {
        if (self.batch != null or self.protocol != required_protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
        if (self.history_commit_time_ns <= 0 or self.table.len == 0 or self.table.len > 1024 or
            !std.unicode.utf8ValidateSlice(self.table) or std.mem.indexOfScalar(u8, self.table, 0) != null or
            self.row_batch_json.len == 0 or self.row_batch_json.len > max_bytes) return error.InvalidReplicatedBatch;
        const owner = self.storage_owner;
        if (owner.identity.table_id == 0 or owner.identity.shard_id == 0 or owner.identity.range_id == 0 or
            owner.schema_json.len > 1024 * 1024 or owner.indexes_json.len > 1024 * 1024 or
            owner.restore != null or owner.restore_bootstrap_json.len != 0 or owner.initial_child_bootstrap_json.len != 0 or
            owner.restore_cancel_recovery or owner.restore_ha_replay) return error.InvalidReplicatedBatch;
        if (owner.table_storage) |storage| if (storage.engine != .local) return error.InvalidReplicatedBatch;
    }

    pub fn rowBatch(self: Entry, a: std.mem.Allocator) !batch_api.OwnedBatchRequest {
        var parsed = try batch_api.parseInternalBatchRequest(a, self.row_batch_json);
        errdefer parsed.deinit(a);
        if (!eligible(parsed.req)) return error.InvalidReplicatedBatch;
        return parsed;
    }
};

/// Only ordinary primary-row work uses this envelope. Private lifecycle
/// commands retain their existing ordered encoding and projection semantics.
pub fn eligible(req: @import("antfly_local_sources").storage_db_types.BatchRequest) bool {
    return req.source_producer == .ordinary and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0) and
        req.transaction == null and req.resolver_review == null and
        req.online_source == null and req.artifact_catalog == null and req.group_source == null and
        req.artifact_publication == null and req.artifact_publication_transport == null and req.leann_record == null and
        req.restore_staging == null and req.restore_staging_scope == null and req.restore_staging_plan_id == null and
        req.relational_topology == null and req.relational_generation_gc == null and req.row_policy_publication == null and
        !req.activate_range_tracking and !req.graph_endpoint_cleanup and
        req.relational_activation == null and req.relational_retirement == null and req.relational_index_maintenance == null and
        req.split_checkpoint == null and req.split_replication == null and req.split_transition == null and
        req.merge_source_transition == null and req.merge_checkpoint == null and req.merge_replication == null and
        req.merge_page == null and req.merge_proof_adoption == null and req.merge_artifacts.len == 0;
}

pub fn encode(a: std.mem.Allocator, entry: Entry, activated: u16) ![]u8 {
    if (activated < required_protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
    try entry.validate();
    var batch = try entry.rowBatch(a);
    defer batch.deinit(a);
    const bytes = try std.json.Stringify.valueAlloc(a, entry, .{ .emit_null_optional_fields = true });
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidReplicatedBatch;
    return bytes;
}

pub fn decodeValue(a: std.mem.Allocator, value: std.json.Value) !std.json.Parsed(Entry) {
    if (value != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "table", "batch", "protocol", "storage_owner", "history_commit_time_ns", "row_batch_json" }) |name| if (!value.object.contains(name)) return error.InvalidReplicatedBatch;
    const owner = value.object.get("storage_owner").?;
    if (owner != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "lsm_root_generation", "identity", "schema_json", "indexes_json", "initial_range", "table_storage" }) |name| if (!owner.object.contains(name)) return error.InvalidReplicatedBatch;
    var parsed = try std.json.parseFromValue(Entry, a, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

test "history control entry clock requires a new decoder and stays outside generic row work" {
    const a = std.testing.allocator;
    const body = try batch_api.encodeBatchRequest(a, .{ .source_producer = .ordinary, .writes = &.{.{ .key = "x", .value = "{}" }} });
    defer a.free(body);
    const entry: Entry = .{
        .table = "docs",
        .storage_owner = .{ .lsm_root_generation = 0, .identity = .{ .table_id = 7, .shard_id = 2, .range_id = 3 }, .schema_json = "{}", .indexes_json = "{}" },
        .history_commit_time_ns = 1_700_000_000_000_000_000,
        .row_batch_json = body,
    };
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, entry, required_protocol_version - 1));
    const bytes = try encode(a, entry, required_protocol_version);
    defer a.free(bytes);
    const raft = @import("raft_batch.zig");
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, raft.decode(a, bytes));
    var applied = try raft.decodeForApply(a, bytes);
    defer applied.deinit(a);
    try std.testing.expectEqualStrings(entry.table, applied.batch.table_name);
    try std.testing.expectEqual(entry.history_commit_time_ns, applied.batch.history_commit_time_ns.?);
    try std.testing.expectEqualDeep(entry.storage_owner, applied.batch.storage_owner_descriptor.?.view());
    try std.testing.expectEqualStrings("x", applied.batch.batch.req.writes[0].key);
}
