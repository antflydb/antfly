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

pub const SqlAdapterClassificationReason = enum {
    aggregate_duplicate_output_name,
    bulk_io_plan,
    cte_mutation_source_plan,
    duplicate_conflict_update_target,
    duplicate_output_name,
    duplicate_row_batch_target,
    duplicate_update_target,
    enforced_unique_conflict_target,
    extension,
    invalid_expression_conflict_target,
    invalid_named_conflict_target,
    multi_output_subquery_delete_selector,
    multi_output_subquery_update_selector,
    multi_table_generation_barrier,
    prepared_transaction_plan,
    recursive_cte_stream_plan,
    role_setting_plan,
    routine_body_plan,
    routine_option_plan,
    row_security_policy_plan,
    row_rewrite_expression_plan,
    row_lock_mode_plan,
    schema_namespace,
    session_setting,
    set_operation_plan,
    system_time_temporal_table,
    temporal_fk_action,
    transaction_control,
};

pub fn classificationReasonFromToken(token: []const u8) ?SqlAdapterClassificationReason {
    return std.meta.stringToEnum(SqlAdapterClassificationReason, token);
}

pub fn classificationReasonTokenIsKnown(token: []const u8) bool {
    return classificationReasonFromToken(token) != null;
}

pub fn classificationReasonToken(reason: SqlAdapterClassificationReason) []const u8 {
    return @tagName(reason);
}

pub fn classificationReasonIsAdapterNoop(reason: SqlAdapterClassificationReason) bool {
    return switch (reason) {
        .extension,
        .schema_namespace,
        .session_setting,
        .transaction_control,
        => true,
        else => false,
    };
}

pub fn classificationReasonIsUnsupportedRequirement(reason: SqlAdapterClassificationReason) bool {
    return !classificationReasonIsAdapterNoop(reason);
}

test "sql adapter diagnostics accept only stable known classification reasons" {
    try std.testing.expect(classificationReasonTokenIsKnown("set_operation_plan"));
    try std.testing.expect(classificationReasonTokenIsKnown("bulk_io_plan"));
    try std.testing.expect(classificationReasonTokenIsKnown("cte_mutation_source_plan"));
    try std.testing.expectEqual(SqlAdapterClassificationReason.prepared_transaction_plan, classificationReasonFromToken("prepared_transaction_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.role_setting_plan, classificationReasonFromToken("role_setting_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_body_plan, classificationReasonFromToken("routine_body_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_option_plan, classificationReasonFromToken("routine_option_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.row_security_policy_plan, classificationReasonFromToken("row_security_policy_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.row_lock_mode_plan, classificationReasonFromToken("row_lock_mode_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.row_rewrite_expression_plan, classificationReasonFromToken("row_rewrite_expression_plan").?);
    try std.testing.expectEqualStrings("multi_table_generation_barrier", classificationReasonToken(.multi_table_generation_barrier));
    try std.testing.expect(classificationReasonIsAdapterNoop(.session_setting));
    try std.testing.expect(!classificationReasonIsAdapterNoop(.set_operation_plan));
    try std.testing.expect(classificationReasonIsUnsupportedRequirement(.set_operation_plan));
    try std.testing.expect(!classificationReasonIsUnsupportedRequirement(.session_setting));
    try std.testing.expect(!classificationReasonTokenIsKnown("set operation plan"));
    try std.testing.expect(!classificationReasonTokenIsKnown("future_reason"));
    try std.testing.expect(!classificationReasonTokenIsKnown(""));
}
