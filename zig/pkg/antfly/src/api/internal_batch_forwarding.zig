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

const std = @import("std");
const data_raft_protocol = @import("../common/data_raft_protocol.zig");
const raft_mutation_forwarding = @import("raft_mutation_forwarding.zig");

pub const raft_batch_protocol_version = data_raft_protocol.batch_protocol_version;
pub const raft_batch_index_owner_control_protocol_version = data_raft_protocol.batch_index_owner_control_protocol_version;
pub const raft_batch_artifact_catalog_protocol_version = data_raft_protocol.batch_artifact_catalog_protocol_version;
pub const raft_batch_timestamp_protocol_version = data_raft_protocol.batch_timestamp_protocol_version;
pub const raft_batch_activation_barrier_protocol_version = data_raft_protocol.batch_activation_barrier_protocol_version;
pub const raft_batch_merge_transition_protocol_version = data_raft_protocol.batch_merge_transition_protocol_version;
pub const raft_batch_split_delta_predecessor_protocol_version = data_raft_protocol.batch_split_delta_predecessor_protocol_version;
pub const raft_batch_merge_artifacts_protocol_version = data_raft_protocol.batch_merge_artifacts_protocol_version;
pub const raft_batch_merge_copy_attempt_protocol_version = data_raft_protocol.batch_merge_copy_attempt_protocol_version;
pub const raft_batch_merge_retirements_protocol_version = data_raft_protocol.batch_merge_retirements_protocol_version;
pub const raft_batch_merge_page_protocol_version = data_raft_protocol.batch_merge_page_protocol_version;
pub const raft_batch_online_source_protocol_version = data_raft_protocol.batch_online_source_protocol_version;
pub const raft_batch_source_pin_protocol_version = data_raft_protocol.batch_source_pin_protocol_version;
pub const raft_batch_relational_transfer_protocol_version = data_raft_protocol.batch_relational_transfer_protocol_version;
pub const raft_batch_source_scope_protocol_version = data_raft_protocol.batch_source_scope_protocol_version;
pub const raft_batch_merge_chunk_protocol_version = data_raft_protocol.batch_merge_chunk_protocol_version;
pub const raft_batch_merge_proof_adoption_protocol_version = data_raft_protocol.batch_merge_proof_adoption_protocol_version;
const http_common = @import("../raft/transport/http_common.zig");

pub const remaining_ms_header = "X-Antfly-Raft-Batch-Remaining-Ms";
pub const forwards_remaining_header = "X-Antfly-Raft-Batch-Forwards-Remaining";
pub const campaign_allowed_header = "X-Antfly-Raft-Batch-Campaign-Allowed";
pub const outcome_header = "X-Antfly-Raft-Batch-Outcome";
pub const outcome_not_proposed_v1 = "not-proposed-v1";
/// Exact internal rejection: the destination was reachable, but validation,
/// catalog, topology, or storage could not admit this write before proposal.
/// This is not evidence that its Raft leader route is unreachable.
pub const admission_unavailable_body = "routed raft batch admission unavailable";
pub const outcome_unknown_v1 = "unknown-v1";
pub const outcome_committed_visibility_pending_v1 = "committed-visibility-pending-v1";
pub const outcome_committed_repair_required_v1 = "committed-repair-required-v1";
pub const committed_visibility_pending_body = "committed_visibility_pending";
pub const committed_repair_required_body = "committed_repair_required";
pub const max_forwards: u8 = 2;
pub const max_remaining_ms: u32 = 5_000;

/// Routing state carried only between trusted internal group-write endpoints.
/// The remaining budget is relative because monotonic clocks are process-local.
/// Each sender reserves response time before publishing the next budget.
pub const Context = raft_mutation_forwarding.Context;

pub fn parse(req: http_common.HttpRequest) !?Context {
    return parseValues(
        req.header(remaining_ms_header),
        req.header(forwards_remaining_header),
        req.header(campaign_allowed_header),
    );
}

pub fn parseValues(
    remaining_raw: ?[]const u8,
    forwards_raw: ?[]const u8,
    campaign_raw: ?[]const u8,
) !?Context {
    return raft_mutation_forwarding.parseValues(
        remaining_raw,
        forwards_raw,
        campaign_raw,
        .{ .max_remaining_ms = max_remaining_ms, .max_forwards = max_forwards },
    ) catch return error.InvalidRaftBatchForwardingHeaders;
}

pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        test "graph batch34 selects protected prepared effects without upgrading ordinary graph deltas" {
            const types = @import("antfly_local_sources").storage_db_types;
            var req: types.BatchRequest = .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }} };
            try std.testing.expectEqual(@as(?u16, null), graphTransactionProtocol(req));
            req.transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 1 } };
            try std.testing.expectEqual(@as(?u16, 34), graphTransactionProtocol(req));
            req.graph_writes = &.{};
            req.transforms = &.{.{ .key = "a", .operations = &.{.{ .op = .push, .path = "_edges.g.R", .value_json = "{\"target\":\"b\"}" }} }};
            try std.testing.expectEqual(@as(?u16, 34), graphTransactionProtocol(req));
            req.transforms = &.{};
            try std.testing.expectEqual(@as(?u16, null), graphTransactionProtocol(req));
            req.graph_preparation = .{ .metadata_group_id = 1, .incarnation = @splat(1), .source_revision = 1, .table_id = 1, .schema_digest = @splat(2), .indexes = &.{.{ .name = "g", .digest = @splat(3) }} };
            try std.testing.expectEqual(@as(?u16, 34), graphTransactionProtocol(req));
        }

        test "internal batch forwarding headers are all-or-none and strictly parsed" {
            const valid_headers = [_]http_common.RequestHeader{
                .{ .name = remaining_ms_header, .value = "425" },
                .{ .name = forwards_remaining_header, .value = "1" },
                .{ .name = campaign_allowed_header, .value = "false" },
            };
            const context = (try parse(.{ .method = .POST, .uri = "/", .headers = &valid_headers })).?;
            try std.testing.expectEqual(@as(u32, 425), context.remaining_ms);
            try std.testing.expectEqual(@as(u8, 1), context.forwards_remaining);
            try std.testing.expect(!context.campaign_allowed);

            try std.testing.expect((try parse(.{ .method = .POST, .uri = "/" })) == null);
            try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
                .method = .POST,
                .uri = "/",
                .headers = valid_headers[0..2],
            }));
            const zero_budget = [_]http_common.RequestHeader{
                .{ .name = remaining_ms_header, .value = "0" },
                .{ .name = forwards_remaining_header, .value = "1" },
                .{ .name = campaign_allowed_header, .value = "true" },
            };
            try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
                .method = .POST,
                .uri = "/",
                .headers = &zero_budget,
            }));
            const excessive_budget = [_]http_common.RequestHeader{
                .{ .name = remaining_ms_header, .value = "5001" },
                .{ .name = forwards_remaining_header, .value = "1" },
                .{ .name = campaign_allowed_header, .value = "true" },
            };
            try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
                .method = .POST,
                .uri = "/",
                .headers = &excessive_budget,
            }));
            const excessive_hops = [_]http_common.RequestHeader{
                .{ .name = remaining_ms_header, .value = "425" },
                .{ .name = forwards_remaining_header, .value = "3" },
                .{ .name = campaign_allowed_header, .value = "true" },
            };
            try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
                .method = .POST,
                .uri = "/",
                .headers = &excessive_hops,
            }));
            const invalid_campaign = [_]http_common.RequestHeader{
                .{ .name = remaining_ms_header, .value = "425" },
                .{ .name = forwards_remaining_header, .value = "1" },
                .{ .name = campaign_allowed_header, .value = "1" },
            };
            try std.testing.expectError(error.InvalidRaftBatchForwardingHeaders, parse(.{
                .method = .POST,
                .uri = "/",
                .headers = &invalid_campaign,
            }));
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}

/// Private source retirement has its own decoder boundary; routing a new
/// source role through the historical SQL-drop floor is insufficient.
pub fn retirementControlProtocol(req: @import("antfly_local_sources").storage_db_types.BatchRequest) ?u16 {
    const command = req.relational_topology orelse return null;
    if (command.fence.role == .document_retirement_source or command.document_schema_digest != null)
        return data_raft_protocol.batch_document_retirement_protocol_version;
    if (command.sql_drop_control) return data_raft_protocol.batch_sql_drop_control_protocol_version;
    return null;
}

/// Only the new prepared/protected lane changes format. Existing ordinary
/// single-owner graph deltas retain their historical protocol and wire bytes.
pub fn graphTransactionProtocol(req: @import("antfly_local_sources").storage_db_types.BatchRequest) ?u16 {
    if (@import("antfly_local_sources").system_catalog_policies.requiresSchemaPairDecoder(req.row_policy_install_bundle))
        return data_raft_protocol.batch_coupled_schema_policy_protocol_version;
    if (req.graph_preparation != null) return data_raft_protocol.batch_graph_transaction_protocol_version;
    if (req.transaction == null and req.trigger_receipts.len == 0) return null;
    if (req.graph_writes.len != 0 or req.graph_deletes.len != 0) return data_raft_protocol.batch_graph_transaction_protocol_version;
    for (req.transforms) |transform| for (transform.operations) |operation| {
        if ((@import("antfly_local_sources").storage_db_transform.graphProjectionPath(operation.path) catch return data_raft_protocol.batch_graph_transaction_protocol_version) != null)
            return data_raft_protocol.batch_graph_transaction_protocol_version;
    };
    return null;
}

pub fn externalTransactionProtocol(req: @import("antfly_local_sources").storage_db_types.BatchRequest) ?u16 {
    if (req.sql_lock != null) return @import("../common/data_raft_protocol.zig").batch_sql_lock_protocol_version;
    if (req.transaction) |transaction| if (transaction == .begin and transaction.begin.sql_locks != null) return @import("../common/data_raft_protocol.zig").batch_sql_lock_protocol_version;
    if (req.transaction) |transaction| switch (transaction) {
        .begin => |begin| if (begin.external_decision_owner != null) {
            return @import("../common/data_raft_protocol.zig").batch_external_transaction_protocol_version;
        },
        .prepare => |prepare| if (prepare.external_decision_owner != null or prepare.external_activation) {
            return @import("../common/data_raft_protocol.zig").batch_external_transaction_protocol_version;
        },
        .resolve => |resolve| if (resolve.external_decision_owner != null) {
            return @import("../common/data_raft_protocol.zig").batch_external_transaction_protocol_version;
        },
        else => {},
    };
    return null;
}

pub fn validateExternalVoteAdmission(req: @import("antfly_local_sources").storage_db_types.BatchRequest, realtime_ns: u64) !void {
    if (req.transaction) |transaction| {
        if (transaction == .begin) {
            if (transaction.begin.external_decision_owner) |owner| try owner.requireFreshVoteAt(@max(1, realtime_ns));
            return;
        }
        if (transaction != .prepare) return;
        const prepare = transaction.prepare;
        if (prepare.external_activation) {
            const owner = prepare.external_decision_owner orelse return error.InvalidExternalDecisionOwner;
            try owner.validate();
        } else if (prepare.external_decision_owner) |owner| try owner.requireFreshVoteAt(@max(1, realtime_ns));
    }
}

pub fn materializeExternalVoteTimestamp(req: *@import("antfly_local_sources").storage_db_types.BatchRequest, realtime_ns: u64) void {
    if (req.transaction) |transaction| switch (transaction) {
        .begin => |begin| if (begin.external_decision_owner != null) {
            req.timestamp_ns = @max(1, realtime_ns);
        },
        .prepare => |prepare| if (prepare.external_decision_owner != null and !prepare.external_activation) {
            req.timestamp_ns = @max(1, realtime_ns);
        },
        else => {},
    };
}

test "row-policy coupled schema batch35 precedes historical policy and graph selectors" {
    const Request = @import("antfly_local_sources").storage_db_types.BatchRequest;
    const ordinary: Request = .{ .row_policy_install_bundle = "{\"schema_pair\":null}" };
    try std.testing.expectEqual(@as(?u16, null), graphTransactionProtocol(ordinary));
    const paired: Request = .{ .row_policy_install_bundle = "{\"schema_pair\":{}}" };
    try std.testing.expectEqual(@as(?u16, 35), graphTransactionProtocol(paired));
}
