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

pub const SqlDiagnosticPhase = enum {
    parse,
    bind,
    plan,
    execute,
};

pub const SqlDiagnosticCode = enum {
    current_transaction_aborted,
    document_sql_array_requires_unnest,
    document_sql_bounded_scan_byte_cap_exceeded,
    document_sql_bounded_scan_incomplete_topk,
    document_sql_bounded_scan_missing_exact_producer,
    document_sql_bounded_scan_policy_required,
    document_sql_bounded_scan_row_cap_exceeded,
    document_sql_bounded_scan_unbounded_source,
    document_sql_bounded_scan_unsupported_residual,
    document_sql_requires_bounded_scan,
    document_sql_unnest_requires_array,
    document_sql_unnest_unsupported,
    document_sql_write_unsupported,
    document_sql_view_mapping_unsupported,
    invalid_role_setting,
    invalid_sql_catalog,
    invalid_sql_request,
    invalid_sql_session,
    invalid_sql_write,
    permission_denied,
    prepared_statement_already_exists,
    prepared_statement_argument_mismatch,
    prepared_statement_not_found,
    read_unavailable,
    read_only_transaction,
    role_setting_not_found,
    sql_read_backpressured,
    statement_timeout,
    system_versioned_history_pruned,
    table_not_found,
    topology_changed,
    unique_owner_unavailable,
    unsupported_sql_statement,
};

pub const SqlDiagnosticEnvelope = struct {
    phase: SqlDiagnosticPhase,
    code: SqlDiagnosticCode,
    message: []const u8,
    span: SourceSpan = .{},
    hint: ?[]const u8 = null,
    missing_native_model: ?[]const u8 = null,

    pub fn init(phase: SqlDiagnosticPhase, code: SqlDiagnosticCode) SqlDiagnosticEnvelope {
        return .{
            .phase = phase,
            .code = code,
            .message = diagnosticCodeDefaultMessage(code),
            .missing_native_model = diagnosticCodeMissingNativeModel(code),
        };
    }

    pub fn withMessage(self: SqlDiagnosticEnvelope, message: []const u8) SqlDiagnosticEnvelope {
        var copy = self;
        copy.message = message;
        return copy;
    }

    pub fn withSpan(self: SqlDiagnosticEnvelope, span: SourceSpan) SqlDiagnosticEnvelope {
        var copy = self;
        copy.span = span;
        return copy;
    }

    pub fn withHint(self: SqlDiagnosticEnvelope, hint: []const u8) SqlDiagnosticEnvelope {
        var copy = self;
        copy.hint = hint;
        return copy;
    }

    pub fn withMissingNativeModel(self: SqlDiagnosticEnvelope, missing_native_model: []const u8) SqlDiagnosticEnvelope {
        var copy = self;
        copy.missing_native_model = missing_native_model;
        return copy;
    }
};

pub fn diagnosticPhaseToken(phase: SqlDiagnosticPhase) []const u8 {
    return @tagName(phase);
}

pub fn diagnosticCodeToken(code: SqlDiagnosticCode) []const u8 {
    return @tagName(code);
}

pub fn diagnosticCodeDefaultMessage(code: SqlDiagnosticCode) []const u8 {
    return switch (code) {
        .current_transaction_aborted => "current transaction is aborted",
        .document_sql_array_requires_unnest => "document SQL array predicates require explicit UNNEST",
        .document_sql_bounded_scan_byte_cap_exceeded => "document SQL bounded scan byte cap exceeded",
        .document_sql_bounded_scan_incomplete_topk => "document SQL bounded scan cannot prove ordered top-k completeness",
        .document_sql_bounded_scan_missing_exact_producer => "document SQL requires an exact native producer or bounded scan",
        .document_sql_bounded_scan_policy_required => "document SQL requires an explicit bounded scan policy",
        .document_sql_bounded_scan_row_cap_exceeded => "document SQL bounded scan row cap exceeded",
        .document_sql_bounded_scan_unbounded_source => "document SQL scan source is unbounded",
        .document_sql_bounded_scan_unsupported_residual => "document SQL bounded scan residual predicate is unsupported",
        .document_sql_requires_bounded_scan => "document SQL requires an explicit bounded scan policy",
        .document_sql_unnest_requires_array => "document SQL UNNEST requires an array field",
        .document_sql_unnest_unsupported => "document SQL UNNEST shape is unsupported",
        .document_sql_write_unsupported => "document_sql_write_unsupported",
        .document_sql_view_mapping_unsupported => "document_sql_view_mapping_unsupported",
        .invalid_role_setting => "invalid sql setting",
        .invalid_sql_catalog => "invalid sql catalog",
        .invalid_sql_request => "invalid sql request",
        .invalid_sql_session => "invalid sql session",
        .invalid_sql_write => "invalid sql write",
        .permission_denied => "permission denied",
        .prepared_statement_already_exists => "prepared statement already exists",
        .prepared_statement_argument_mismatch => "prepared statement argument mismatch",
        .prepared_statement_not_found => "prepared statement not found",
        .read_unavailable => "read unavailable",
        .read_only_transaction => "cannot execute statement in a read-only transaction",
        .role_setting_not_found => "role setting not found",
        .sql_read_backpressured => "sql read backpressured",
        .statement_timeout => "sql statement timeout",
        .system_versioned_history_pruned => "system-versioned history pruned",
        .table_not_found => "table not found",
        .topology_changed => "topology changed",
        .unique_owner_unavailable => "unique owner unavailable",
        .unsupported_sql_statement => "unsupported sql statement",
    };
}

pub fn diagnosticCodeMissingNativeModel(code: SqlDiagnosticCode) ?[]const u8 {
    return switch (code) {
        .document_sql_array_requires_unnest,
        .document_sql_unnest_requires_array,
        .document_sql_unnest_unsupported,
        => "document SQL array expansion execution",
        .document_sql_bounded_scan_byte_cap_exceeded,
        .document_sql_bounded_scan_incomplete_topk,
        .document_sql_bounded_scan_missing_exact_producer,
        .document_sql_bounded_scan_policy_required,
        .document_sql_bounded_scan_row_cap_exceeded,
        .document_sql_bounded_scan_unbounded_source,
        .document_sql_bounded_scan_unsupported_residual,
        .document_sql_requires_bounded_scan,
        => "document SQL bounded-scan execution proof",
        .document_sql_write_unsupported => "document SQL write execution",
        .document_sql_view_mapping_unsupported => "document-to-SQL view mapping execution",
        .unsupported_sql_statement => "typed Antfly logical plan for this SQL shape",
        else => null,
    };
}

pub fn knownErrorDiagnostic(phase: SqlDiagnosticPhase, err: anyerror) ?SqlDiagnosticEnvelope {
    const code: SqlDiagnosticCode = switch (err) {
        error.DocumentSqlArrayRequiresUnnest => .document_sql_array_requires_unnest,
        error.DocumentSqlBoundedScanByteCapExceeded => .document_sql_bounded_scan_byte_cap_exceeded,
        error.DocumentSqlBoundedScanIncompleteTopK => .document_sql_bounded_scan_incomplete_topk,
        error.DocumentSqlBoundedScanMissingExactProducer => .document_sql_bounded_scan_missing_exact_producer,
        error.DocumentSqlIndexUnavailable => .document_sql_bounded_scan_missing_exact_producer,
        error.DocumentSqlBoundedScanPolicyRequired => .document_sql_bounded_scan_policy_required,
        error.DocumentSqlBoundedScanRowCapExceeded => .document_sql_bounded_scan_row_cap_exceeded,
        error.DocumentSqlBoundedScanUnboundedSource => .document_sql_bounded_scan_unbounded_source,
        error.DocumentSqlBoundedScanUnsupportedResidual => .document_sql_bounded_scan_unsupported_residual,
        error.DocumentSqlRequiresBoundedScan => .document_sql_requires_bounded_scan,
        error.DocumentSqlUnnestRequiresArray => .document_sql_unnest_requires_array,
        error.DocumentSqlUnnestUnsupported => .document_sql_unnest_unsupported,
        error.DocumentSqlWriteUnsupported => .document_sql_write_unsupported,
        error.DocumentSqlViewMappingUnsupported => .document_sql_view_mapping_unsupported,
        error.InvalidRoleSetting => .invalid_role_setting,
        error.InvalidSqlCatalog => .invalid_sql_catalog,
        error.InvalidSqlSession => .invalid_sql_session,
        error.Forbidden,
        error.PermissionDenied,
        => .permission_denied,
        error.PreparedStatementAlreadyExists => .prepared_statement_already_exists,
        error.PreparedStatementArgumentMismatch => .prepared_statement_argument_mismatch,
        error.PreparedStatementNotFound => .prepared_statement_not_found,
        error.ReadUnavailable => .read_unavailable,
        error.RoleSettingNotFound => .role_setting_not_found,
        error.SavepointNotFound => .invalid_sql_request,
        error.SqlReadOnlyTransaction => .read_only_transaction,
        error.StatementTimeout => .statement_timeout,
        error.SystemVersionedHistoryPruned => .system_versioned_history_pruned,
        error.TableNotFound => .table_not_found,
        error.TopologyChanged => .topology_changed,
        error.UniqueOwnerTopologyUnavailable,
        error.DocIdentityNamespaceMismatch,
        => .unique_owner_unavailable,
        error.UnsupportedOperation,
        error.UnsupportedRowsQuery,
        error.UnsupportedSqlShape,
        => .unsupported_sql_statement,
        else => return null,
    };
    const diagnostic = SqlDiagnosticEnvelope.init(phase, code);
    return switch (code) {
        .document_sql_array_requires_unnest => diagnostic.withHint("Use explicit UNNEST for array fields before applying scalar predicates."),
        .document_sql_bounded_scan_byte_cap_exceeded => diagnostic.withHint("Increase the document SQL bounded-scan byte cap or use an indexed/native producer."),
        .document_sql_bounded_scan_incomplete_topk => diagnostic.withHint("Add an explicit LIMIT with a bounded candidate policy, or use an exact order-preserving native producer."),
        .document_sql_bounded_scan_missing_exact_producer => diagnostic.withHint("Add a matching native index/materialization or provide a bounded scan policy with exact residual execution."),
        .document_sql_bounded_scan_policy_required => diagnostic.withHint("Provide a bounded scan policy with non-zero row and byte caps."),
        .document_sql_bounded_scan_row_cap_exceeded => diagnostic.withHint("Increase the document SQL bounded-scan row cap or use an indexed/native producer."),
        .document_sql_bounded_scan_unbounded_source => diagnostic.withHint("Add an explicit LIMIT or provide a bounded scan policy."),
        .document_sql_bounded_scan_unsupported_residual => diagnostic.withHint("Rewrite the predicate to a supported exact residual shape or add a native indexed producer."),
        .document_sql_requires_bounded_scan => diagnostic.withHint("Provide an explicit document SQL bounded-scan policy or add an exact indexed/native producer."),
        .document_sql_unnest_requires_array => diagnostic.withHint("Use UNNEST only on fields declared with array type metadata."),
        .document_sql_unnest_unsupported => diagnostic.withHint("Use a single top-level UNNEST over one array field with a bounded document producer."),
        else => diagnostic,
    };
}

pub const SqlAdapterClassificationReason = enum {
    aggregate_duplicate_output_name,
    bulk_io_plan,
    cte_mutation_source_plan,
    document_sql_bounded_scan_incomplete_topk,
    document_sql_bounded_scan_missing_exact_producer,
    document_sql_bounded_scan_unbounded_source,
    document_sql_bounded_scan_unsupported_residual,
    document_sql_view_mapping_catalog,
    document_sql_view_mapping_unsupported,
    document_sql_write_unsupported,
    document_table_ddl_invalid_antfly_extension,
    document_table_ddl_invalid_dynamic_template,
    document_table_ddl_malformed_schema_json,
    document_table_ddl_missing_default_type,
    document_table_ddl_mixed_relational_shape,
    document_table_ddl_multi_document_type_unsupported,
    document_table_ddl_shorthand,
    document_table_ddl_unknown_default_type,
    duplicate_conflict_update_target,
    duplicate_output_name,
    duplicate_row_batch_target,
    duplicate_update_target,
    enforced_unique_conflict_target,
    extension,
    insert_overriding_value_plan,
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
    table_access_method_plan,
    table_cluster_plan,
    table_owner_plan,
    table_persistence_plan,
    table_storage_parameters_plan,
    table_tablespace_plan,
    table_trigger_state_plan,
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
    return switch (reason) {
        .aggregate_duplicate_output_name,
        .duplicate_output_name,
        => false,
        else => !classificationReasonIsAdapterNoop(reason),
    };
}

pub const NativeRequirementCategory = enum {
    auth_row_filter,
    bulk_io_route,
    catalog_lifecycle,
    conflict_target_validation,
    identity_override_execution,
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
        .document_sql_bounded_scan_incomplete_topk => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_missing_exact_producer => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_unbounded_source => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_unsupported_residual => .{ .category = .stream_materialization },
        .document_sql_view_mapping_catalog => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .document_sql_view_mapping_unsupported => .{ .category = .stream_materialization },
        .document_sql_write_unsupported => .{ .category = .auth_row_filter, .auth_and_audit = true },
        .document_table_ddl_invalid_antfly_extension,
        .document_table_ddl_invalid_dynamic_template,
        .document_table_ddl_malformed_schema_json,
        .document_table_ddl_missing_default_type,
        .document_table_ddl_mixed_relational_shape,
        .document_table_ddl_multi_document_type_unsupported,
        .document_table_ddl_shorthand,
        .document_table_ddl_unknown_default_type,
        => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .duplicate_conflict_update_target,
        .duplicate_row_batch_target,
        .duplicate_update_target,
        .enforced_unique_conflict_target,
        .invalid_expression_conflict_target,
        .invalid_named_conflict_target,
        => .{ .category = .conflict_target_validation },
        .insert_overriding_value_plan => .{ .category = .identity_override_execution },
        .aggregate_duplicate_output_name,
        .duplicate_output_name,
        .multi_output_subquery_delete_selector,
        .multi_output_subquery_update_selector,
        .set_operation_output_shape,
        => .{ .category = .output_shape_validation },
        .extension,
        .schema_namespace,
        .set_operation_source_schema,
        .table_access_method_plan,
        .table_cluster_plan,
        .table_owner_plan,
        .table_persistence_plan,
        .table_storage_parameters_plan,
        .table_tablespace_plan,
        .table_trigger_state_plan,
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
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_incomplete_topk, classificationReasonFromToken("document_sql_bounded_scan_incomplete_topk").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_missing_exact_producer, classificationReasonFromToken("document_sql_bounded_scan_missing_exact_producer").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_unbounded_source, classificationReasonFromToken("document_sql_bounded_scan_unbounded_source").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_unsupported_residual, classificationReasonFromToken("document_sql_bounded_scan_unsupported_residual").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_view_mapping_catalog, classificationReasonFromToken("document_sql_view_mapping_catalog").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_view_mapping_unsupported, classificationReasonFromToken("document_sql_view_mapping_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_unsupported, classificationReasonFromToken("document_sql_write_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_invalid_antfly_extension, classificationReasonFromToken("document_table_ddl_invalid_antfly_extension").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_invalid_dynamic_template, classificationReasonFromToken("document_table_ddl_invalid_dynamic_template").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_malformed_schema_json, classificationReasonFromToken("document_table_ddl_malformed_schema_json").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_missing_default_type, classificationReasonFromToken("document_table_ddl_missing_default_type").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_mixed_relational_shape, classificationReasonFromToken("document_table_ddl_mixed_relational_shape").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_multi_document_type_unsupported, classificationReasonFromToken("document_table_ddl_multi_document_type_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_shorthand, classificationReasonFromToken("document_table_ddl_shorthand").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_unknown_default_type, classificationReasonFromToken("document_table_ddl_unknown_default_type").?);
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
    try std.testing.expect(!classificationReasonIsUnsupportedRequirement(.aggregate_duplicate_output_name));
    try std.testing.expect(!classificationReasonIsUnsupportedRequirement(.duplicate_output_name));
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

    const document_ddl = nativeExecutionRequirement(.document_table_ddl_missing_default_type);
    try std.testing.expectEqual(NativeRequirementCategory.catalog_lifecycle, document_ddl.category);
    try std.testing.expect(document_ddl.durable_metadata);

    const document_write = nativeExecutionRequirement(.document_sql_write_unsupported);
    try std.testing.expectEqual(NativeRequirementCategory.auth_row_filter, document_write.category);
    try std.testing.expect(document_write.auth_and_audit);

    const rewrite = nativeExecutionRequirement(.row_rewrite_expression_plan);
    try std.testing.expectEqual(NativeRequirementCategory.schema_rewrite_backfill, rewrite.category);
    try std.testing.expect(rewrite.durable_metadata);

    inline for (std.meta.fields(SqlAdapterClassificationReason)) |field| {
        const reason: SqlAdapterClassificationReason = @enumFromInt(field.value);
        _ = nativeExecutionRequirement(reason);
    }
}

test "sql diagnostics expose stable phase code and native model fields" {
    const unsupported = SqlDiagnosticEnvelope.init(.plan, .unsupported_sql_statement).withSpan(.{ .start = 7, .end = 19 });
    try std.testing.expectEqual(SqlDiagnosticPhase.plan, unsupported.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.unsupported_sql_statement, unsupported.code);
    try std.testing.expectEqualStrings("plan", diagnosticPhaseToken(unsupported.phase));
    try std.testing.expectEqualStrings("unsupported_sql_statement", diagnosticCodeToken(unsupported.code));
    try std.testing.expectEqualStrings("unsupported sql statement", unsupported.message);
    try std.testing.expectEqualStrings("typed Antfly logical plan for this SQL shape", unsupported.missing_native_model.?);
    try std.testing.expectEqual(@as(usize, 7), unsupported.span.start);
    try std.testing.expectEqual(@as(usize, 19), unsupported.span.end);

    const readonly = knownErrorDiagnostic(.execute, error.SqlReadOnlyTransaction) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticPhase.execute, readonly.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.read_only_transaction, readonly.code);
    try std.testing.expect(readonly.missing_native_model == null);

    const array_requires_unnest = knownErrorDiagnostic(.bind, error.DocumentSqlArrayRequiresUnnest) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticPhase.bind, array_requires_unnest.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_array_requires_unnest, array_requires_unnest.code);
    try std.testing.expectEqualStrings("document_sql_array_requires_unnest", diagnosticCodeToken(array_requires_unnest.code));
    try std.testing.expectEqualStrings("document SQL array predicates require explicit UNNEST", array_requires_unnest.message);
    try std.testing.expectEqualStrings("document SQL array expansion execution", array_requires_unnest.missing_native_model.?);
    try std.testing.expectEqualStrings("Use explicit UNNEST for array fields before applying scalar predicates.", array_requires_unnest.hint.?);

    const unsupported_unnest = knownErrorDiagnostic(.bind, error.DocumentSqlUnnestUnsupported) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticPhase.bind, unsupported_unnest.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_unnest_unsupported, unsupported_unnest.code);
    try std.testing.expectEqualStrings("document_sql_unnest_unsupported", diagnosticCodeToken(unsupported_unnest.code));
    try std.testing.expectEqualStrings("document SQL UNNEST shape is unsupported", unsupported_unnest.message);
    try std.testing.expectEqualStrings("document SQL array expansion execution", unsupported_unnest.missing_native_model.?);
    try std.testing.expectEqualStrings("Use a single top-level UNNEST over one array field with a bounded document producer.", unsupported_unnest.hint.?);

    const unnest_requires_array = knownErrorDiagnostic(.bind, error.DocumentSqlUnnestRequiresArray) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticPhase.bind, unnest_requires_array.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_unnest_requires_array, unnest_requires_array.code);
    try std.testing.expectEqualStrings("document_sql_unnest_requires_array", diagnosticCodeToken(unnest_requires_array.code));
    try std.testing.expectEqualStrings("document SQL UNNEST requires an array field", unnest_requires_array.message);
    try std.testing.expectEqualStrings("document SQL array expansion execution", unnest_requires_array.missing_native_model.?);
    try std.testing.expectEqualStrings("Use UNNEST only on fields declared with array type metadata.", unnest_requires_array.hint.?);

    const requires_bounded = knownErrorDiagnostic(.plan, error.DocumentSqlRequiresBoundedScan) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticPhase.plan, requires_bounded.phase);
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_requires_bounded_scan, requires_bounded.code);
    try std.testing.expectEqualStrings("document_sql_requires_bounded_scan", diagnosticCodeToken(requires_bounded.code));
    try std.testing.expectEqualStrings("document SQL requires an explicit bounded scan policy", requires_bounded.message);
    try std.testing.expectEqualStrings("document SQL bounded-scan execution proof", requires_bounded.missing_native_model.?);
    try std.testing.expectEqualStrings("Provide an explicit document SQL bounded-scan policy or add an exact indexed/native producer.", requires_bounded.hint.?);

    const policy_required = knownErrorDiagnostic(.plan, error.DocumentSqlBoundedScanPolicyRequired) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_policy_required, policy_required.code);
    try std.testing.expectEqualStrings("document_sql_bounded_scan_policy_required", diagnosticCodeToken(policy_required.code));
    try std.testing.expectEqualStrings("document SQL requires an explicit bounded scan policy", policy_required.message);
    try std.testing.expectEqualStrings("Provide a bounded scan policy with non-zero row and byte caps.", policy_required.hint.?);

    const unbounded_source = knownErrorDiagnostic(.plan, error.DocumentSqlBoundedScanUnboundedSource) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_unbounded_source, unbounded_source.code);
    try std.testing.expectEqualStrings("document SQL scan source is unbounded", unbounded_source.message);
    try std.testing.expectEqualStrings("Add an explicit LIMIT or provide a bounded scan policy.", unbounded_source.hint.?);

    const unsupported_residual = knownErrorDiagnostic(.plan, error.DocumentSqlBoundedScanUnsupportedResidual) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_unsupported_residual, unsupported_residual.code);
    try std.testing.expectEqualStrings("document SQL bounded scan residual predicate is unsupported", unsupported_residual.message);
    try std.testing.expectEqualStrings("Rewrite the predicate to a supported exact residual shape or add a native indexed producer.", unsupported_residual.hint.?);

    const incomplete_topk = knownErrorDiagnostic(.plan, error.DocumentSqlBoundedScanIncompleteTopK) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_incomplete_topk, incomplete_topk.code);
    try std.testing.expectEqualStrings("document SQL bounded scan cannot prove ordered top-k completeness", incomplete_topk.message);
    try std.testing.expectEqualStrings("Add an explicit LIMIT with a bounded candidate policy, or use an exact order-preserving native producer.", incomplete_topk.hint.?);

    const missing_exact = knownErrorDiagnostic(.plan, error.DocumentSqlBoundedScanMissingExactProducer) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_missing_exact_producer, missing_exact.code);
    try std.testing.expectEqualStrings("document SQL requires an exact native producer or bounded scan", missing_exact.message);
    try std.testing.expectEqualStrings("Add a matching native index/materialization or provide a bounded scan policy with exact residual execution.", missing_exact.hint.?);
    const index_unavailable = knownErrorDiagnostic(.plan, error.DocumentSqlIndexUnavailable) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_missing_exact_producer, index_unavailable.code);

    const row_cap = knownErrorDiagnostic(.execute, error.DocumentSqlBoundedScanRowCapExceeded) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_row_cap_exceeded, row_cap.code);
    try std.testing.expectEqualStrings("document SQL bounded scan row cap exceeded", row_cap.message);
    try std.testing.expectEqualStrings("Increase the document SQL bounded-scan row cap or use an indexed/native producer.", row_cap.hint.?);

    const byte_cap = knownErrorDiagnostic(.execute, error.DocumentSqlBoundedScanByteCapExceeded) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_byte_cap_exceeded, byte_cap.code);
    try std.testing.expectEqualStrings("document SQL bounded scan byte cap exceeded", byte_cap.message);
    try std.testing.expectEqualStrings("Increase the document SQL bounded-scan byte cap or use an indexed/native producer.", byte_cap.hint.?);
}
