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
    document_sql_write_duplicate_source,
    document_sql_write_join_missing_cardinality_proof,
    document_sql_write_join_missing_exact_producer,
    document_sql_write_join_missing_index_proof,
    document_sql_write_join_ordered_index_proof,
    document_sql_write_join_partial_index_proof,
    document_sql_write_join_stale_index_proof,
    document_sql_write_merge_requires_native_producer,
    document_sql_write_returning_all_unsupported,
    document_sql_write_returning_duplicate_output,
    document_sql_write_returning_expression_unsupported,
    document_sql_write_returning_generated_field,
    document_sql_write_returning_version_unsupported,
    document_sql_write_returning_virtual_field_unsupported,
    document_sql_write_source_assignment_alias,
    document_sql_write_source_assignment_ambiguous_reference,
    document_sql_write_source_assignment_generated_field,
    document_sql_write_source_assignment_missing_field,
    document_sql_write_source_assignment_reserved_field,
    document_sql_write_source_assignment_target_generated_field,
    document_sql_write_source_assignment_target_reserved_field,
    document_sql_write_source_assignment_type_mismatch,
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
        .document_sql_write_duplicate_source => "document SQL joined write source produced duplicate join keys",
        .document_sql_write_join_missing_cardinality_proof => "document SQL joined write lacks a document cardinality proof",
        .document_sql_write_join_missing_exact_producer => "document SQL joined writes require exact target and source producers",
        .document_sql_write_join_missing_index_proof => "document SQL joined write requires indexed join fields",
        .document_sql_write_join_ordered_index_proof => "document SQL joined write cannot use ordered or composite join indexes",
        .document_sql_write_join_partial_index_proof => "document SQL joined write cannot use partial join indexes",
        .document_sql_write_join_stale_index_proof => "document SQL joined write cannot use stale or rebuilding join indexes",
        .document_sql_write_merge_requires_native_producer => "document SQL MERGE requires a bounded native document producer contract",
        .document_sql_write_returning_all_unsupported => "document SQL write RETURNING * is not supported",
        .document_sql_write_returning_duplicate_output => "document SQL write RETURNING has duplicate output names",
        .document_sql_write_returning_expression_unsupported => "document SQL write RETURNING expression is not supported",
        .document_sql_write_returning_generated_field => "document SQL write RETURNING cannot read a generated field",
        .document_sql_write_returning_version_unsupported => "document SQL write RETURNING _version is not supported",
        .document_sql_write_returning_virtual_field_unsupported => "document SQL write RETURNING virtual field is not supported",
        .document_sql_write_source_assignment_alias => "document SQL source assignment requires the joined source alias",
        .document_sql_write_source_assignment_ambiguous_reference => "document SQL source assignment has an ambiguous target/source reference",
        .document_sql_write_source_assignment_generated_field => "document SQL source assignment cannot read a generated source field",
        .document_sql_write_source_assignment_missing_field => "document SQL source assignment references an unmapped source field",
        .document_sql_write_source_assignment_reserved_field => "document SQL source assignment cannot read reserved document fields",
        .document_sql_write_source_assignment_target_generated_field => "document SQL source assignment cannot write a generated target field",
        .document_sql_write_source_assignment_target_reserved_field => "document SQL source assignment cannot write reserved document fields",
        .document_sql_write_source_assignment_type_mismatch => "document SQL source assignment source and target types differ",
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
        .document_sql_write_source_assignment_alias,
        .document_sql_write_source_assignment_ambiguous_reference,
        .document_sql_write_source_assignment_generated_field,
        .document_sql_write_source_assignment_missing_field,
        .document_sql_write_source_assignment_reserved_field,
        .document_sql_write_source_assignment_target_generated_field,
        .document_sql_write_source_assignment_target_reserved_field,
        .document_sql_write_source_assignment_type_mismatch,
        .document_sql_write_duplicate_source,
        .document_sql_write_join_missing_cardinality_proof,
        .document_sql_write_join_missing_exact_producer,
        .document_sql_write_join_missing_index_proof,
        .document_sql_write_join_ordered_index_proof,
        .document_sql_write_join_partial_index_proof,
        .document_sql_write_join_stale_index_proof,
        .document_sql_write_unsupported,
        .document_sql_write_returning_all_unsupported,
        .document_sql_write_returning_duplicate_output,
        .document_sql_write_returning_expression_unsupported,
        .document_sql_write_returning_generated_field,
        .document_sql_write_returning_version_unsupported,
        .document_sql_write_returning_virtual_field_unsupported,
        => "document SQL write execution",
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
        error.DocumentSqlWriteDuplicateSource => .document_sql_write_duplicate_source,
        error.DocumentSqlWriteJoinMissingCardinalityProof => .document_sql_write_join_missing_cardinality_proof,
        error.DocumentSqlWriteJoinMissingExactProducer => .document_sql_write_join_missing_exact_producer,
        error.DocumentSqlWriteJoinMissingIndexProof => .document_sql_write_join_missing_index_proof,
        error.DocumentSqlWriteJoinOrderedIndexProof => .document_sql_write_join_ordered_index_proof,
        error.DocumentSqlWriteJoinPartialIndexProof => .document_sql_write_join_partial_index_proof,
        error.DocumentSqlWriteJoinStaleIndexProof => .document_sql_write_join_stale_index_proof,
        error.DocumentSqlMergeRequiresNativeProducer => .document_sql_write_merge_requires_native_producer,
        error.DocumentSqlWriteReturningAllUnsupported => .document_sql_write_returning_all_unsupported,
        error.DocumentSqlWriteReturningDuplicateOutput => .document_sql_write_returning_duplicate_output,
        error.DocumentSqlWriteReturningExpressionUnsupported => .document_sql_write_returning_expression_unsupported,
        error.DocumentSqlWriteReturningGeneratedField => .document_sql_write_returning_generated_field,
        error.DocumentSqlWriteReturningVersionUnsupported => .document_sql_write_returning_version_unsupported,
        error.DocumentSqlWriteReturningVirtualFieldUnsupported => .document_sql_write_returning_virtual_field_unsupported,
        error.DocumentSqlWriteSourceAssignmentAlias => .document_sql_write_source_assignment_alias,
        error.DocumentSqlWriteSourceAssignmentAmbiguousReference => .document_sql_write_source_assignment_ambiguous_reference,
        error.DocumentSqlWriteSourceAssignmentGeneratedField => .document_sql_write_source_assignment_generated_field,
        error.DocumentSqlWriteSourceAssignmentMissingField => .document_sql_write_source_assignment_missing_field,
        error.DocumentSqlWriteSourceAssignmentReservedField => .document_sql_write_source_assignment_reserved_field,
        error.DocumentSqlWriteSourceAssignmentTargetGeneratedField => .document_sql_write_source_assignment_target_generated_field,
        error.DocumentSqlWriteSourceAssignmentTargetReservedField => .document_sql_write_source_assignment_target_reserved_field,
        error.DocumentSqlWriteSourceAssignmentTypeMismatch => .document_sql_write_source_assignment_type_mismatch,
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
        .document_sql_write_duplicate_source => diagnostic.withHint("Constrain the source producer so each target join key matches at most one source document."),
        .document_sql_write_join_missing_cardinality_proof => diagnostic.withHint("Add a document-native cardinality proof before admitting this non-identity joined write."),
        .document_sql_write_join_missing_exact_producer => diagnostic.withHint("Use an exact _id join, or add a native producer proof for both joined document inputs."),
        .document_sql_write_join_missing_index_proof => diagnostic.withHint("Add ready single-field indexes for both document join fields before admitting this joined write."),
        .document_sql_write_join_ordered_index_proof => diagnostic.withHint("Use an unordered single-field equality index for each document join field."),
        .document_sql_write_join_partial_index_proof => diagnostic.withHint("Use full join-field indexes or prove the partial predicates cover the joined write."),
        .document_sql_write_join_stale_index_proof => diagnostic.withHint("Wait for join-field indexes to become ready before admitting this joined write."),
        .document_sql_write_merge_requires_native_producer => diagnostic.withHint("Admit MERGE only after the lowerer and runtime prove a bounded deterministic native document producer."),
        .document_sql_write_returning_all_unsupported => diagnostic.withHint("List admitted document projection fields explicitly instead of using RETURNING *."),
        .document_sql_write_returning_duplicate_output => diagnostic.withHint("Give each document RETURNING item a unique output alias."),
        .document_sql_write_returning_expression_unsupported => diagnostic.withHint("Return _id, _doc, or declared document projection fields until expression rows are proven."),
        .document_sql_write_returning_generated_field => diagnostic.withHint("Return the generated field source data, or wait for native generated-field readback parity."),
        .document_sql_write_returning_version_unsupported => diagnostic.withHint("Wait for native document writes to expose deterministic post-write versions before returning _version."),
        .document_sql_write_returning_virtual_field_unsupported => diagnostic.withHint("Return only declared document projection fields with native readback semantics."),
        else => diagnostic,
    };
}

pub const SqlAdapterClassificationReason = enum {
    aggregate_duplicate_output_name,
    bulk_io_plan,
    conversion_catalog_plan,
    cte_body_join_plan,
    cte_mutation_source_plan,
    document_sql_bounded_scan_incomplete_topk,
    document_sql_bounded_scan_missing_exact_producer,
    document_sql_bounded_scan_unbounded_source,
    document_sql_bounded_scan_unsupported_residual,
    document_sql_view_mapping_catalog,
    document_sql_view_mapping_unsupported,
    document_sql_write_join_missing_cardinality_proof,
    document_sql_write_join_missing_exact_producer,
    document_sql_write_join_missing_index_proof,
    document_sql_write_join_ordered_index_proof,
    document_sql_write_join_partial_index_proof,
    document_sql_write_join_stale_index_proof,
    document_sql_write_merge_requires_native_producer,
    document_sql_write_returning_all_unsupported,
    document_sql_write_returning_duplicate_output,
    document_sql_write_returning_expression_unsupported,
    document_sql_write_returning_generated_field,
    document_sql_write_returning_version_unsupported,
    document_sql_write_returning_virtual_field_unsupported,
    document_sql_write_source_assignment_alias,
    document_sql_write_source_assignment_ambiguous_reference,
    document_sql_write_source_assignment_generated_field,
    document_sql_write_source_assignment_missing_field,
    document_sql_write_source_assignment_reserved_field,
    document_sql_write_source_assignment_target_generated_field,
    document_sql_write_source_assignment_target_reserved_field,
    document_sql_write_source_assignment_type_mismatch,
    document_sql_write_unsupported,
    document_table_ddl_duplicate_schema_name,
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
    event_trigger_catalog_plan,
    extension,
    foreign_data_catalog_plan,
    graph_query_plan,
    insert_overriding_value_plan,
    invalid_expression_conflict_target,
    invalid_named_conflict_target,
    multi_output_subquery_delete_selector,
    multi_output_subquery_update_selector,
    multi_table_generation_barrier,
    operator_catalog_plan,
    prepared_transaction_plan,
    recursive_cte_stream_plan,
    role_setting_plan,
    rule_catalog_plan,
    routine_body_plan,
    routine_option_plan,
    set_operation_output_shape,
    row_rewrite_expression_plan,
    row_lock_mode_plan,
    schema_namespace,
    session_setting,
    set_operation_source_schema,
    set_operation_plan,
    statistics_catalog_plan,
    subquery_expression_plan,
    subquery_quantified_plan,
    subquery_scalar_plan,
    subquery_semijoin_plan,
    system_time_temporal_table,
    table_access_method_plan,
    table_cluster_plan,
    table_column_statistics_plan,
    table_column_storage_plan,
    table_owner_plan,
    table_persistence_plan,
    table_storage_parameters_plan,
    table_tablespace_plan,
    trigger_catalog_plan,
    table_trigger_state_plan,
    temporal_fk_action,
    text_search_catalog_plan,
    transaction_control,
    transform_catalog_plan,
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

pub fn classificationReasonIsExpressionStructural(reason: SqlAdapterClassificationReason) bool {
    return switch (reason) {
        .document_sql_bounded_scan_unsupported_residual,
        .document_sql_write_returning_expression_unsupported,
        .invalid_expression_conflict_target,
        .row_rewrite_expression_plan,
        .subquery_expression_plan,
        .subquery_quantified_plan,
        .subquery_scalar_plan,
        .subquery_semijoin_plan,
        => true,
        else => false,
    };
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
        .cte_body_join_plan, .cte_mutation_source_plan => .{ .category = .stream_materialization, .materialization = true },
        .document_sql_bounded_scan_incomplete_topk => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_missing_exact_producer => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_unbounded_source => .{ .category = .stream_materialization },
        .document_sql_bounded_scan_unsupported_residual => .{ .category = .stream_materialization },
        .document_sql_view_mapping_catalog => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .document_sql_view_mapping_unsupported => .{ .category = .stream_materialization },
        .document_sql_write_join_missing_cardinality_proof, .document_sql_write_join_missing_exact_producer => .{ .category = .stream_materialization },
        .document_sql_write_join_missing_index_proof,
        .document_sql_write_join_ordered_index_proof,
        .document_sql_write_join_partial_index_proof,
        .document_sql_write_join_stale_index_proof,
        .document_sql_write_merge_requires_native_producer,
        .document_sql_write_returning_all_unsupported,
        .document_sql_write_returning_duplicate_output,
        .document_sql_write_returning_expression_unsupported,
        .document_sql_write_returning_generated_field,
        .document_sql_write_returning_version_unsupported,
        .document_sql_write_returning_virtual_field_unsupported,
        => .{ .category = .stream_materialization },
        .document_sql_write_source_assignment_alias,
        .document_sql_write_source_assignment_ambiguous_reference,
        .document_sql_write_source_assignment_generated_field,
        .document_sql_write_source_assignment_missing_field,
        .document_sql_write_source_assignment_reserved_field,
        .document_sql_write_source_assignment_target_generated_field,
        .document_sql_write_source_assignment_target_reserved_field,
        .document_sql_write_source_assignment_type_mismatch,
        => .{ .category = .auth_row_filter, .auth_and_audit = true },
        .document_sql_write_unsupported => .{ .category = .auth_row_filter, .auth_and_audit = true },
        .document_table_ddl_duplicate_schema_name,
        .document_table_ddl_invalid_antfly_extension,
        .document_table_ddl_invalid_dynamic_template,
        .document_table_ddl_malformed_schema_json,
        .document_table_ddl_missing_default_type,
        .document_table_ddl_mixed_relational_shape,
        .document_table_ddl_multi_document_type_unsupported,
        .document_table_ddl_shorthand,
        .document_table_ddl_unknown_default_type,
        .conversion_catalog_plan,
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
        .event_trigger_catalog_plan,
        .graph_query_plan,
        .schema_namespace,
        .foreign_data_catalog_plan,
        .operator_catalog_plan,
        .rule_catalog_plan,
        .set_operation_source_schema,
        .statistics_catalog_plan,
        .table_access_method_plan,
        .table_cluster_plan,
        .table_column_statistics_plan,
        .table_column_storage_plan,
        .table_owner_plan,
        .table_persistence_plan,
        .table_storage_parameters_plan,
        .table_tablespace_plan,
        .trigger_catalog_plan,
        .table_trigger_state_plan,
        .text_search_catalog_plan,
        .transform_catalog_plan,
        => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .multi_table_generation_barrier => .{ .category = .catalog_lifecycle, .durable_metadata = true },
        .prepared_transaction_plan => .{ .category = .prepared_transaction_recovery, .coordinator_recovery = true },
        .recursive_cte_stream_plan,
        .set_operation_plan,
        .subquery_expression_plan,
        .subquery_quantified_plan,
        .subquery_scalar_plan,
        .subquery_semijoin_plan,
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
    try std.testing.expectEqual(SqlAdapterClassificationReason.conversion_catalog_plan, classificationReasonFromToken("conversion_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.cte_body_join_plan, classificationReasonFromToken("cte_body_join_plan").?);
    try std.testing.expect(classificationReasonTokenIsKnown("cte_mutation_source_plan"));
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_incomplete_topk, classificationReasonFromToken("document_sql_bounded_scan_incomplete_topk").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_missing_exact_producer, classificationReasonFromToken("document_sql_bounded_scan_missing_exact_producer").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_unbounded_source, classificationReasonFromToken("document_sql_bounded_scan_unbounded_source").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_bounded_scan_unsupported_residual, classificationReasonFromToken("document_sql_bounded_scan_unsupported_residual").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_view_mapping_catalog, classificationReasonFromToken("document_sql_view_mapping_catalog").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_view_mapping_unsupported, classificationReasonFromToken("document_sql_view_mapping_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_missing_cardinality_proof, classificationReasonFromToken("document_sql_write_join_missing_cardinality_proof").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_missing_exact_producer, classificationReasonFromToken("document_sql_write_join_missing_exact_producer").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_missing_index_proof, classificationReasonFromToken("document_sql_write_join_missing_index_proof").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_ordered_index_proof, classificationReasonFromToken("document_sql_write_join_ordered_index_proof").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_partial_index_proof, classificationReasonFromToken("document_sql_write_join_partial_index_proof").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_join_stale_index_proof, classificationReasonFromToken("document_sql_write_join_stale_index_proof").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_merge_requires_native_producer, classificationReasonFromToken("document_sql_write_merge_requires_native_producer").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_all_unsupported, classificationReasonFromToken("document_sql_write_returning_all_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_duplicate_output, classificationReasonFromToken("document_sql_write_returning_duplicate_output").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_expression_unsupported, classificationReasonFromToken("document_sql_write_returning_expression_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_generated_field, classificationReasonFromToken("document_sql_write_returning_generated_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_version_unsupported, classificationReasonFromToken("document_sql_write_returning_version_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_returning_virtual_field_unsupported, classificationReasonFromToken("document_sql_write_returning_virtual_field_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_alias, classificationReasonFromToken("document_sql_write_source_assignment_alias").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_ambiguous_reference, classificationReasonFromToken("document_sql_write_source_assignment_ambiguous_reference").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_generated_field, classificationReasonFromToken("document_sql_write_source_assignment_generated_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_missing_field, classificationReasonFromToken("document_sql_write_source_assignment_missing_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_reserved_field, classificationReasonFromToken("document_sql_write_source_assignment_reserved_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_target_generated_field, classificationReasonFromToken("document_sql_write_source_assignment_target_generated_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_target_reserved_field, classificationReasonFromToken("document_sql_write_source_assignment_target_reserved_field").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_source_assignment_type_mismatch, classificationReasonFromToken("document_sql_write_source_assignment_type_mismatch").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_sql_write_unsupported, classificationReasonFromToken("document_sql_write_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_duplicate_schema_name, classificationReasonFromToken("document_table_ddl_duplicate_schema_name").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_invalid_antfly_extension, classificationReasonFromToken("document_table_ddl_invalid_antfly_extension").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_invalid_dynamic_template, classificationReasonFromToken("document_table_ddl_invalid_dynamic_template").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_malformed_schema_json, classificationReasonFromToken("document_table_ddl_malformed_schema_json").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_missing_default_type, classificationReasonFromToken("document_table_ddl_missing_default_type").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_mixed_relational_shape, classificationReasonFromToken("document_table_ddl_mixed_relational_shape").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_multi_document_type_unsupported, classificationReasonFromToken("document_table_ddl_multi_document_type_unsupported").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_shorthand, classificationReasonFromToken("document_table_ddl_shorthand").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.document_table_ddl_unknown_default_type, classificationReasonFromToken("document_table_ddl_unknown_default_type").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.event_trigger_catalog_plan, classificationReasonFromToken("event_trigger_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.foreign_data_catalog_plan, classificationReasonFromToken("foreign_data_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.graph_query_plan, classificationReasonFromToken("graph_query_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.operator_catalog_plan, classificationReasonFromToken("operator_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.prepared_transaction_plan, classificationReasonFromToken("prepared_transaction_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.role_setting_plan, classificationReasonFromToken("role_setting_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.rule_catalog_plan, classificationReasonFromToken("rule_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_body_plan, classificationReasonFromToken("routine_body_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.routine_option_plan, classificationReasonFromToken("routine_option_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.row_lock_mode_plan, classificationReasonFromToken("row_lock_mode_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.row_rewrite_expression_plan, classificationReasonFromToken("row_rewrite_expression_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.statistics_catalog_plan, classificationReasonFromToken("statistics_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.subquery_expression_plan, classificationReasonFromToken("subquery_expression_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.subquery_quantified_plan, classificationReasonFromToken("subquery_quantified_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.subquery_scalar_plan, classificationReasonFromToken("subquery_scalar_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.subquery_semijoin_plan, classificationReasonFromToken("subquery_semijoin_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.text_search_catalog_plan, classificationReasonFromToken("text_search_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.trigger_catalog_plan, classificationReasonFromToken("trigger_catalog_plan").?);
    try std.testing.expectEqual(SqlAdapterClassificationReason.transform_catalog_plan, classificationReasonFromToken("transform_catalog_plan").?);
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

test "sql adapter diagnostics keep unsupported expression classifications structural" {
    const reasons = [_]SqlAdapterClassificationReason{
        .document_sql_bounded_scan_unsupported_residual,
        .document_sql_write_returning_expression_unsupported,
        .invalid_expression_conflict_target,
        .row_rewrite_expression_plan,
        .subquery_expression_plan,
        .subquery_quantified_plan,
        .subquery_scalar_plan,
        .subquery_semijoin_plan,
    };

    for (reasons) |reason| {
        try std.testing.expect(classificationReasonIsExpressionStructural(reason));
        const token = classificationReasonToken(reason);
        try std.testing.expect(token.len > 0);
        try std.testing.expectEqual(reason, classificationReasonFromToken(token).?);
        const requirement = nativeExecutionRequirement(reason);
        try std.testing.expect(requirement.category == .stream_materialization or
            requirement.category == .conflict_target_validation or
            requirement.category == .schema_rewrite_backfill);
    }

    try std.testing.expect(!classificationReasonIsExpressionStructural(.session_setting));
    try std.testing.expect(!classificationReasonIsExpressionStructural(.duplicate_output_name));
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

    const duplicate_source = knownErrorDiagnostic(.execute, error.DocumentSqlWriteDuplicateSource) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_duplicate_source, duplicate_source.code);
    try std.testing.expectEqualStrings("document SQL joined write source produced duplicate join keys", duplicate_source.message);
    try std.testing.expectEqualStrings("Constrain the source producer so each target join key matches at most one source document.", duplicate_source.hint.?);

    const join_missing_cardinality = knownErrorDiagnostic(.plan, error.DocumentSqlWriteJoinMissingCardinalityProof) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_join_missing_cardinality_proof, join_missing_cardinality.code);
    try std.testing.expectEqualStrings("document SQL joined write lacks a document cardinality proof", join_missing_cardinality.message);
    try std.testing.expectEqualStrings("Add a document-native cardinality proof before admitting this non-identity joined write.", join_missing_cardinality.hint.?);

    const join_missing_index = knownErrorDiagnostic(.plan, error.DocumentSqlWriteJoinMissingIndexProof) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_join_missing_index_proof, join_missing_index.code);
    try std.testing.expectEqualStrings("document SQL joined write requires indexed join fields", join_missing_index.message);

    const join_stale_index = knownErrorDiagnostic(.plan, error.DocumentSqlWriteJoinStaleIndexProof) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_join_stale_index_proof, join_stale_index.code);
    try std.testing.expectEqualStrings("document SQL joined write cannot use stale or rebuilding join indexes", join_stale_index.message);

    const join_partial_index = knownErrorDiagnostic(.plan, error.DocumentSqlWriteJoinPartialIndexProof) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_join_partial_index_proof, join_partial_index.code);
    try std.testing.expectEqualStrings("document SQL joined write cannot use partial join indexes", join_partial_index.message);

    const join_ordered_index = knownErrorDiagnostic(.plan, error.DocumentSqlWriteJoinOrderedIndexProof) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_join_ordered_index_proof, join_ordered_index.code);
    try std.testing.expectEqualStrings("document SQL joined write cannot use ordered or composite join indexes", join_ordered_index.message);

    const document_merge = knownErrorDiagnostic(.plan, error.DocumentSqlMergeRequiresNativeProducer) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_merge_requires_native_producer, document_merge.code);
    try std.testing.expectEqualStrings("document SQL MERGE requires a bounded native document producer contract", document_merge.message);
    try std.testing.expectEqualStrings("Admit MERGE only after the lowerer and runtime prove a bounded deterministic native document producer.", document_merge.hint.?);

    const returning_all = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningAllUnsupported) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_all_unsupported, returning_all.code);
    try std.testing.expectEqualStrings("document SQL write RETURNING * is not supported", returning_all.message);
    try std.testing.expectEqualStrings("List admitted document projection fields explicitly instead of using RETURNING *.", returning_all.hint.?);

    const returning_duplicate = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningDuplicateOutput) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_duplicate_output, returning_duplicate.code);
    try std.testing.expectEqualStrings("Give each document RETURNING item a unique output alias.", returning_duplicate.hint.?);

    const returning_expression = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningExpressionUnsupported) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_expression_unsupported, returning_expression.code);
    try std.testing.expectEqualStrings("Return _id, _doc, or declared document projection fields until expression rows are proven.", returning_expression.hint.?);

    const returning_generated = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningGeneratedField) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_generated_field, returning_generated.code);
    try std.testing.expectEqualStrings("document SQL write RETURNING cannot read a generated field", returning_generated.message);

    const returning_version = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningVersionUnsupported) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_version_unsupported, returning_version.code);
    try std.testing.expectEqualStrings("Wait for native document writes to expose deterministic post-write versions before returning _version.", returning_version.hint.?);

    const returning_virtual = knownErrorDiagnostic(.plan, error.DocumentSqlWriteReturningVirtualFieldUnsupported) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_write_returning_virtual_field_unsupported, returning_virtual.code);
    try std.testing.expectEqualStrings("Return only declared document projection fields with native readback semantics.", returning_virtual.hint.?);

    const byte_cap = knownErrorDiagnostic(.execute, error.DocumentSqlBoundedScanByteCapExceeded) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(SqlDiagnosticCode.document_sql_bounded_scan_byte_cap_exceeded, byte_cap.code);
    try std.testing.expectEqualStrings("document SQL bounded scan byte cap exceeded", byte_cap.message);
    try std.testing.expectEqualStrings("Increase the document SQL bounded-scan byte cap or use an indexed/native producer.", byte_cap.hint.?);
}
