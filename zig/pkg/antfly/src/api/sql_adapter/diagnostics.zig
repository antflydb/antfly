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

const token_mod = @import("token.zig");

pub const SourceSpan = token_mod.SourceSpan;
pub const Token = token_mod.Token;

pub const SqlDiagnostic = struct {
    reason: SqlAdapterClassificationReason,
    span: SourceSpan = .{},

    pub fn fromToken(reason: SqlAdapterClassificationReason, token: Token) SqlDiagnostic {
        return .{ .reason = reason, .span = token.sourceSpan() };
    }
};

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
    set_operation_output_shape,
    row_rewrite_expression_plan,
    row_lock_mode_plan,
    schema_namespace,
    session_setting,
    set_operation_source_schema,
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

pub const NativeRequirementCategory = enum {
    auth_row_filter,
    bulk_io_route,
    catalog_lifecycle,
    conflict_target_validation,
    output_shape_validation,
    prepared_transaction_recovery,
    role_setting_model,
    routine_execution_hooks,
    schema_rewrite_backfill,
    stream_materialization,
    temporal_execution_model,
    transaction_control,
};

pub const NativeExecutionRequirement = struct {
    category: NativeRequirementCategory,
    durable_metadata: bool = false,
    coordinator_recovery: bool = false,
    auth_and_audit: bool = false,
    materialization: bool = false,
    spill: bool = false,
    backpressure: bool = false,
};

pub fn nativeExecutionRequirement(reason: SqlAdapterClassificationReason) NativeExecutionRequirement {
    return switch (reason) {
        .bulk_io_plan => .{ .category = .bulk_io_route, .auth_and_audit = true },
        .cte_mutation_source_plan => .{ .category = .stream_materialization, .materialization = true },
        .duplicate_conflict_update_target,
        .duplicate_row_batch_target,
        .duplicate_update_target,
        .enforced_unique_conflict_target,
        .invalid_expression_conflict_target,
        .invalid_named_conflict_target,
        => .{ .category = .conflict_target_validation },
        .aggregate_duplicate_output_name,
        .duplicate_output_name,
        .multi_output_subquery_delete_selector,
        .multi_output_subquery_update_selector,
        .set_operation_output_shape,
        => .{ .category = .output_shape_validation },
        .extension,
        .schema_namespace,
        .set_operation_source_schema,
        => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .multi_table_generation_barrier => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .prepared_transaction_plan => .{ .category = .prepared_transaction_recovery, .coordinator_recovery = true },
        .recursive_cte_stream_plan,
        .set_operation_plan,
        => .{
            .category = .stream_materialization,
            .materialization = true,
            .spill = true,
            .backpressure = true,
        },
        .role_setting_plan,
        .session_setting,
        => .{ .category = .role_setting_model, .durable_metadata = reason == .role_setting_plan },
        .routine_body_plan,
        .routine_option_plan,
        => .{ .category = .routine_execution_hooks, .durable_metadata = true },
        .row_rewrite_expression_plan => .{ .category = .schema_rewrite_backfill, .durable_metadata = true },
        .row_lock_mode_plan,
        .transaction_control,
        => .{ .category = .transaction_control, .durable_metadata = reason == .row_lock_mode_plan },
        .system_time_temporal_table,
        .temporal_fk_action,
        => .{ .category = .temporal_execution_model, .durable_metadata = true },
    };
}

test "sql adapter diagnostics accept only stable known classification reasons" {
    try std.testing.expect(classificationReasonTokenIsKnown("set_operation_plan"));
    try std.testing.expect(classificationReasonTokenIsKnown("set_operation_output_shape"));
    try std.testing.expect(classificationReasonTokenIsKnown("set_operation_source_schema"));
    try std.testing.expect(classificationReasonTokenIsKnown("bulk_io_plan"));
    try std.testing.expect(classificationReasonTokenIsKnown("cte_mutation_source_plan"));
    try std.testing.expectEqual(SqlAdapterClassificationReason.prepared_transaction_plan, classificationReasonFromToken("prepared_transaction_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.role_setting_plan, classificationReasonFromToken("role_setting_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_body_plan, classificationReasonFromToken("routine_body_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_option_plan, classificationReasonFromToken("routine_option_plan").?);
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

test "sql adapter diagnostics map unsupported classifications to native requirements" {
    const recursive = nativeExecutionRequirement(.recursive_cte_stream_plan);
    try std.testing.expectEqual(NativeRequirementCategory.stream_materialization, recursive.category);
    try std.testing.expect(recursive.materialization);
    try std.testing.expect(recursive.spill);
    try std.testing.expect(recursive.backpressure);

    const set_operation = nativeExecutionRequirement(.set_operation_plan);
    try std.testing.expectEqual(NativeRequirementCategory.stream_materialization, set_operation.category);
    try std.testing.expect(set_operation.materialization);
    try std.testing.expect(set_operation.spill);
    try std.testing.expect(set_operation.backpressure);

    const set_operation_shape = nativeExecutionRequirement(.set_operation_output_shape);
    try std.testing.expectEqual(NativeRequirementCategory.output_shape_validation, set_operation_shape.category);

    const set_operation_schema = nativeExecutionRequirement(.set_operation_source_schema);
    try std.testing.expectEqual(NativeRequirementCategory.catalog_lifecycle, set_operation_schema.category);
    try std.testing.expect(set_operation_schema.durable_metadata);

    const bulk = nativeExecutionRequirement(.bulk_io_plan);
    try std.testing.expectEqual(NativeRequirementCategory.bulk_io_route, bulk.category);
    try std.testing.expect(bulk.auth_and_audit);

    const prepared = nativeExecutionRequirement(.prepared_transaction_plan);
    try std.testing.expectEqual(NativeRequirementCategory.prepared_transaction_recovery, prepared.category);
    try std.testing.expect(prepared.coordinator_recovery);

    const rewrite = nativeExecutionRequirement(.row_rewrite_expression_plan);
    try std.testing.expectEqual(NativeRequirementCategory.schema_rewrite_backfill, rewrite.category);
    try std.testing.expect(rewrite.durable_metadata);

    inline for (std.meta.fields(SqlAdapterClassificationReason)) |field| {
        const reason: SqlAdapterClassificationReason = @enumFromInt(field.value);
        _ = nativeExecutionRequirement(reason);
    }
}
