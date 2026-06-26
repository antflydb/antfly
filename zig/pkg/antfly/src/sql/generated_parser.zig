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

const generated = @import("grammar/generated/root.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

const generated_token_id_stack_capacity = 1024;
const generated_parse_stack_capacity = 512;

pub const GeneratedSqlStatementKind = enum {
    session,
    transaction,
    prepared,
    ddl,
    dml,
    read,
    extension_index,
    graph,
    cursor,
    unsupported,
    other,
};

pub const GeneratedSqlSessionKind = enum {
    set,
    reset,
    show,
    discard_all,
};

pub const GeneratedSqlTransactionKind = enum {
    set_transaction,
    start_transaction,
    begin,
    commit,
    rollback,
    savepoint,
    release_savepoint,
    rollback_to_savepoint,
};

pub const GeneratedSqlPreparedKind = enum {
    prepare,
    execute,
    deallocate,
};

pub const GeneratedSqlCursorKind = enum {
    declare,
    fetch,
    close,
};

pub const GeneratedSqlDdlKind = enum {
    create_database,
    create_schema,
    create_table,
    create_view,
    create_materialized_view,
    create_domain,
    create_sequence,
    create_enum_type,
    create_tablespace,
    create_publication,
    create_subscription,
    create_policy,
    create_function,
    create_procedure,
    create_role,
    create_collation,
    create_operator,
    create_aggregate,
    create_cast,
    create_index,
    create_extension,
    alter_table,
    alter_database,
    alter_extension,
    alter_schema,
    alter_tablespace,
    alter_view,
    alter_domain,
    alter_sequence,
    alter_enum_type,
    alter_publication,
    alter_subscription,
    alter_policy,
    alter_role,
    alter_collation,
    drop_table,
    drop_view,
    drop_materialized_view,
    drop_domain,
    drop_sequence,
    drop_enum_type,
    drop_tablespace,
    drop_publication,
    drop_subscription,
    drop_policy,
    drop_function,
    drop_procedure,
    drop_role,
    drop_collation,
    drop_operator,
    drop_aggregate,
    drop_cast,
    drop_index,
    drop_schema,
    drop_database,
    drop_extension,
    refresh_materialized_view,
    relation_population,
    create_graph_index,
    create_graph_metric,
};

pub const GeneratedSqlDmlKind = enum {
    insert_values,
    insert_select,
    update,
    delete,
    truncate,
    merge,
};

pub const GeneratedSqlReadKind = enum {
    query,
    aggregate,
    join,
    lateral,
    window,
    set_operation,
    cte,
};

pub const GeneratedSqlGraphKind = enum {
    create_index,
    create_metric,
    alter_metric,
};

pub const GeneratedSqlGraphTableFunctionKind = enum {
    traverse,
    neighbors,
    shortest_path,
    k_shortest_paths,
    match,
    metric,
    metric_rerank,
};

pub const GeneratedSqlAntflyTableFunctionKind = enum {
    full_text_search,
    semantic_search,
    vector_search,
    graph_traverse,
    graph_neighbors,
    graph_shortest_path,
    graph_k_shortest_paths,
    graph_match,
    graph_metric,
    graph_metric_rerank,
    hybrid_search,
};

pub const GeneratedSqlExtensionIndexKind = enum {
    create_index,
    drop_index,
    create_extension,
    drop_extension,
};

pub const GeneratedSqlUnsupportedKind = enum {
    analyze,
    call,
    checkpoint,
    close,
    cluster,
    comment,
    copy,
    discard,
    insert_overriding_value,
    alter_aggregate,
    alter_index,
    alter_conversion,
    alter_default_privileges,
    alter_event_trigger,
    alter_foreign_table,
    alter_foreign_data_wrapper,
    alter_function,
    alter_large_object,
    alter_language,
    alter_materialized_view,
    alter_operator,
    alter_operator_class,
    alter_operator_family,
    alter_policy,
    alter_procedure,
    alter_routine,
    alter_publication,
    alter_rule,
    alter_server,
    alter_subscription,
    alter_system,
    alter_statistics,
    alter_text_search_configuration,
    alter_text_search_dictionary,
    alter_text_search_parser,
    alter_text_search_template,
    alter_trigger,
    alter_transform,
    alter_user_mapping,
    create_access_method,
    create_conversion,
    create_database_options,
    create_event_trigger,
    create_foreign_table,
    create_foreign_data_wrapper,
    create_language,
    create_materialized_view,
    create_operator_class,
    create_operator_family,
    create_policy,
    create_publication,
    create_rule,
    create_schema_options,
    create_server,
    create_subscription,
    create_statistics,
    create_text_search_configuration,
    create_text_search_dictionary,
    create_text_search_parser,
    create_text_search_template,
    create_transform,
    create_trigger,
    create_user_mapping,
    declare,
    do_block,
    drop_foreign_table,
    drop_foreign_data_wrapper,
    drop_access_method,
    drop_conversion,
    drop_event_trigger,
    drop_language,
    drop_collation_multi,
    drop_domain_multi,
    drop_extension_multi,
    drop_index_multi,
    drop_materialized_view_multi,
    drop_owned,
    drop_operator_class,
    drop_operator_family,
    drop_publication_multi,
    drop_routine,
    drop_role_multi,
    drop_schema_multi,
    drop_sequence_multi,
    drop_statistics,
    drop_table_multi,
    drop_text_search_configuration,
    drop_text_search_dictionary,
    drop_text_search_parser,
    drop_text_search_template,
    drop_transform,
    drop_type_multi,
    drop_user_mapping,
    drop_view_multi,
    explain,
    fetch,
    grant,
    import_foreign_schema,
    listen,
    load,
    lock,
    move,
    notify,
    reassign_owned,
    reindex,
    release,
    revoke,
    role_session_control,
    savepoint,
    security_label,
    drop_materialized_view,
    drop_policy,
    drop_publication,
    drop_rule,
    drop_server,
    drop_subscription,
    drop_trigger,
    unlisten,
    vacuum,
};

pub const GeneratedSqlUnsupportedReason = enum {
    analyze_not_planned_by_generated_parser,
    call_not_planned_by_generated_parser,
    checkpoint_not_planned_by_generated_parser,
    close_not_planned_by_generated_parser,
    cluster_not_planned_by_generated_parser,
    comment_not_planned_by_generated_parser,
    copy_not_planned_by_generated_parser,
    discard_not_planned_by_generated_parser,
    insert_overriding_value_not_planned_by_generated_parser,
    alter_aggregate_not_planned_by_generated_parser,
    alter_index_not_planned_by_generated_parser,
    alter_conversion_not_planned_by_generated_parser,
    alter_default_privileges_not_planned_by_generated_parser,
    alter_event_trigger_not_planned_by_generated_parser,
    alter_foreign_table_not_planned_by_generated_parser,
    alter_foreign_data_wrapper_not_planned_by_generated_parser,
    alter_function_not_planned_by_generated_parser,
    alter_large_object_not_planned_by_generated_parser,
    alter_language_not_planned_by_generated_parser,
    alter_materialized_view_not_planned_by_generated_parser,
    alter_operator_not_planned_by_generated_parser,
    alter_operator_class_not_planned_by_generated_parser,
    alter_operator_family_not_planned_by_generated_parser,
    alter_policy_not_planned_by_generated_parser,
    alter_procedure_not_planned_by_generated_parser,
    alter_routine_not_planned_by_generated_parser,
    alter_publication_not_planned_by_generated_parser,
    alter_rule_not_planned_by_generated_parser,
    alter_server_not_planned_by_generated_parser,
    alter_subscription_not_planned_by_generated_parser,
    alter_system_not_planned_by_generated_parser,
    alter_statistics_not_planned_by_generated_parser,
    alter_text_search_configuration_not_planned_by_generated_parser,
    alter_text_search_dictionary_not_planned_by_generated_parser,
    alter_text_search_parser_not_planned_by_generated_parser,
    alter_text_search_template_not_planned_by_generated_parser,
    alter_trigger_not_planned_by_generated_parser,
    alter_transform_not_planned_by_generated_parser,
    alter_user_mapping_not_planned_by_generated_parser,
    create_access_method_not_planned_by_generated_parser,
    create_conversion_not_planned_by_generated_parser,
    create_database_options_not_planned_by_generated_parser,
    create_event_trigger_not_planned_by_generated_parser,
    create_foreign_table_not_planned_by_generated_parser,
    create_foreign_data_wrapper_not_planned_by_generated_parser,
    create_language_not_planned_by_generated_parser,
    create_materialized_view_not_planned_by_generated_parser,
    create_operator_class_not_planned_by_generated_parser,
    create_operator_family_not_planned_by_generated_parser,
    create_policy_not_planned_by_generated_parser,
    create_publication_not_planned_by_generated_parser,
    create_rule_not_planned_by_generated_parser,
    create_schema_options_not_planned_by_generated_parser,
    create_server_not_planned_by_generated_parser,
    create_subscription_not_planned_by_generated_parser,
    create_statistics_not_planned_by_generated_parser,
    create_text_search_configuration_not_planned_by_generated_parser,
    create_text_search_dictionary_not_planned_by_generated_parser,
    create_text_search_parser_not_planned_by_generated_parser,
    create_text_search_template_not_planned_by_generated_parser,
    create_transform_not_planned_by_generated_parser,
    create_trigger_not_planned_by_generated_parser,
    create_user_mapping_not_planned_by_generated_parser,
    declare_not_planned_by_generated_parser,
    do_block_not_planned_by_generated_parser,
    drop_foreign_table_not_planned_by_generated_parser,
    drop_foreign_data_wrapper_not_planned_by_generated_parser,
    drop_access_method_not_planned_by_generated_parser,
    drop_conversion_not_planned_by_generated_parser,
    drop_event_trigger_not_planned_by_generated_parser,
    drop_language_not_planned_by_generated_parser,
    drop_collation_multi_not_planned_by_generated_parser,
    drop_domain_multi_not_planned_by_generated_parser,
    drop_extension_multi_not_planned_by_generated_parser,
    drop_index_multi_not_planned_by_generated_parser,
    drop_materialized_view_multi_not_planned_by_generated_parser,
    drop_owned_not_planned_by_generated_parser,
    drop_operator_class_not_planned_by_generated_parser,
    drop_operator_family_not_planned_by_generated_parser,
    drop_publication_multi_not_planned_by_generated_parser,
    drop_routine_not_planned_by_generated_parser,
    drop_role_multi_not_planned_by_generated_parser,
    drop_schema_multi_not_planned_by_generated_parser,
    drop_sequence_multi_not_planned_by_generated_parser,
    drop_statistics_not_planned_by_generated_parser,
    drop_table_multi_not_planned_by_generated_parser,
    drop_text_search_configuration_not_planned_by_generated_parser,
    drop_text_search_dictionary_not_planned_by_generated_parser,
    drop_text_search_parser_not_planned_by_generated_parser,
    drop_text_search_template_not_planned_by_generated_parser,
    drop_transform_not_planned_by_generated_parser,
    drop_type_multi_not_planned_by_generated_parser,
    drop_user_mapping_not_planned_by_generated_parser,
    drop_view_multi_not_planned_by_generated_parser,
    explain_not_planned_by_generated_parser,
    fetch_not_planned_by_generated_parser,
    grant_not_planned_by_generated_parser,
    import_foreign_schema_not_planned_by_generated_parser,
    listen_not_planned_by_generated_parser,
    load_not_planned_by_generated_parser,
    lock_not_planned_by_generated_parser,
    move_not_planned_by_generated_parser,
    notify_not_planned_by_generated_parser,
    reassign_owned_not_planned_by_generated_parser,
    reindex_not_planned_by_generated_parser,
    release_not_planned_by_generated_parser,
    revoke_not_planned_by_generated_parser,
    role_session_control_not_planned_by_generated_parser,
    savepoint_not_planned_by_generated_parser,
    security_label_not_planned_by_generated_parser,
    drop_materialized_view_not_planned_by_generated_parser,
    drop_policy_not_planned_by_generated_parser,
    drop_publication_not_planned_by_generated_parser,
    drop_rule_not_planned_by_generated_parser,
    drop_server_not_planned_by_generated_parser,
    drop_subscription_not_planned_by_generated_parser,
    drop_trigger_not_planned_by_generated_parser,
    unlisten_not_planned_by_generated_parser,
    vacuum_not_planned_by_generated_parser,
};

pub const GeneratedSqlStatement = union(GeneratedSqlStatementKind) {
    session: GeneratedSqlSessionKind,
    transaction: GeneratedSqlTransactionKind,
    prepared: GeneratedSqlPreparedKind,
    ddl: GeneratedSqlDdlKind,
    dml: GeneratedSqlDmlKind,
    read: GeneratedSqlReadKind,
    extension_index: GeneratedSqlExtensionIndexKind,
    graph: GeneratedSqlGraphKind,
    cursor: GeneratedSqlCursorKind,
    unsupported: GeneratedSqlUnsupportedKind,
    other: void,
};

pub const GeneratedSqlTokenRange = struct {
    start: usize,
    end: usize,
};

pub const GeneratedSqlOrderDirection = enum {
    asc,
    desc,
};

pub const GeneratedSqlNullsOrder = enum {
    first,
    last,
};

pub const GeneratedSqlListAst = struct {
    first_tokens: ?GeneratedSqlTokenRange = null,
    last_tokens: ?GeneratedSqlTokenRange = null,
    items: []GeneratedSqlTokenRange = &.{},
    expression_items: []GeneratedSqlTokenRange = &.{},
    alias_items: []?GeneratedSqlTokenRange = &.{},
    alias_name_items: []?GeneratedSqlTokenRange = &.{},
    direction_items: []?GeneratedSqlTokenRange = &.{},
    directions: []?GeneratedSqlOrderDirection = &.{},
    order_using_operator_items: []?GeneratedSqlTokenRange = &.{},
    nulls_order_items: []?GeneratedSqlTokenRange = &.{},
    nulls_orders: []?GeneratedSqlNullsOrder = &.{},
    expressions: []GeneratedSqlExpressionAst = &.{},
    count: usize = 0,

    pub fn deinit(self: *GeneratedSqlListAst, alloc: std.mem.Allocator) void {
        for (self.expressions) |*expression| expression.deinit(alloc);
        if (self.expressions.len > 0) alloc.free(self.expressions);
        if (self.nulls_orders.len > 0) alloc.free(self.nulls_orders);
        if (self.nulls_order_items.len > 0) alloc.free(self.nulls_order_items);
        if (self.order_using_operator_items.len > 0) alloc.free(self.order_using_operator_items);
        if (self.directions.len > 0) alloc.free(self.directions);
        if (self.direction_items.len > 0) alloc.free(self.direction_items);
        if (self.alias_name_items.len > 0) alloc.free(self.alias_name_items);
        if (self.alias_items.len > 0) alloc.free(self.alias_items);
        if (self.expression_items.len > 0) alloc.free(self.expression_items);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = .{};
    }
};

pub const GeneratedSqlExpressionKind = enum {
    token_range,
    comparison,
    like,
    ilike,
    in_list,
    between,
    not_like,
    not_ilike,
    not_in_list,
    not_between,
    quantified_comparison,
    exists_subquery,
    not_exists_subquery,
    is_null,
    is_not_null,
    is_true,
    is_false,
    is_unknown,
    is_not_true,
    is_not_false,
    is_not_unknown,
    is_distinct_from,
    is_not_distinct_from,
    logical_or,
    logical_and,
    logical_not,
    grouped,
    subquery,
    unary_positive,
    unary_negative,
    additive,
    subtractive,
    multiplicative,
    divisive,
    modulo,
    contains,
    overlaps,
    json_key_exists,
    json_key_any,
    json_key_all,
    regex_match,
    regex_imatch,
    regex_not_match,
    regex_not_imatch,
    string_concat,
    json_access,
    json_text_access,
    json_path_access,
    json_path_text_access,
    function_call,
    array_constructor,
    cast,
    case_expression,
    interval_literal,
    extract_expression,
    timestamp_literal,
    current_date,
    current_timestamp,
};

pub const GeneratedSqlSubqueryTailAst = struct {
    order_tokens: ?GeneratedSqlTokenRange = null,
    order_items: GeneratedSqlListAst = .{},
    order_first_expression: ?*GeneratedSqlExpressionAst = null,
    order_last_expression: ?*GeneratedSqlExpressionAst = null,
    limit_tokens: ?GeneratedSqlTokenRange = null,
    limit_expression: ?*GeneratedSqlExpressionAst = null,
    limit_all: bool = false,
    offset_tokens: ?GeneratedSqlTokenRange = null,
    offset_expression: ?*GeneratedSqlExpressionAst = null,
    fetch_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_expression: ?*GeneratedSqlExpressionAst = null,

    pub fn deinit(self: *GeneratedSqlSubqueryTailAst, alloc: std.mem.Allocator) void {
        self.order_items.deinit(alloc);
        if (self.order_first_expression) |expr| {
            expr.deinit(alloc);
            alloc.destroy(expr);
        }
        if (self.order_last_expression) |expr| {
            expr.deinit(alloc);
            alloc.destroy(expr);
        }
        if (self.limit_expression) |expr| {
            expr.deinit(alloc);
            alloc.destroy(expr);
        }
        if (self.offset_expression) |expr| {
            expr.deinit(alloc);
            alloc.destroy(expr);
        }
        if (self.fetch_count_expression) |expr| {
            expr.deinit(alloc);
            alloc.destroy(expr);
        }
        self.* = .{};
    }
};

pub const GeneratedSqlBetweenModifier = enum {
    asymmetric,
    symmetric,
};

pub const GeneratedSqlExpressionAst = struct {
    kind: GeneratedSqlExpressionKind = .token_range,
    tokens: ?GeneratedSqlTokenRange = null,
    inner_tokens: ?GeneratedSqlTokenRange = null,
    inner_expression_kind: ?GeneratedSqlExpressionKind = null,
    inner_expression: ?*GeneratedSqlExpressionAst = null,
    subquery_read_kind: ?GeneratedSqlReadKind = null,
    subquery_select_tokens: ?GeneratedSqlTokenRange = null,
    subquery_projection_tokens: ?GeneratedSqlTokenRange = null,
    subquery_projection_items: GeneratedSqlListAst = .{},
    subquery_source_tokens: ?GeneratedSqlTokenRange = null,
    subquery_where_tokens: ?GeneratedSqlTokenRange = null,
    subquery_where_expression_kind: ?GeneratedSqlExpressionKind = null,
    subquery_where_expression: ?*GeneratedSqlExpressionAst = null,
    subquery_set_operation_tokens: ?GeneratedSqlTokenRange = null,
    subquery_set_operation: ?*GeneratedSqlSetOperationAst = null,
    subquery_tail: ?*GeneratedSqlSubqueryTailAst = null,
    function_name_tokens: ?GeneratedSqlTokenRange = null,
    argument_tokens: ?GeneratedSqlTokenRange = null,
    argument_distinct_tokens: ?GeneratedSqlTokenRange = null,
    argument_value_tokens: ?GeneratedSqlTokenRange = null,
    argument_items: GeneratedSqlListAst = .{},
    argument_order_tokens: ?GeneratedSqlTokenRange = null,
    argument_order_items: GeneratedSqlListAst = .{},
    within_group_tokens: ?GeneratedSqlTokenRange = null,
    within_group_order_tokens: ?GeneratedSqlTokenRange = null,
    within_group_order_items: GeneratedSqlListAst = .{},
    filter_tokens: ?GeneratedSqlTokenRange = null,
    filter_predicate_tokens: ?GeneratedSqlTokenRange = null,
    filter_expression_kind: ?GeneratedSqlExpressionKind = null,
    filter_expression: ?*GeneratedSqlExpressionAst = null,
    over_tokens: ?GeneratedSqlTokenRange = null,
    over_name_tokens: ?GeneratedSqlTokenRange = null,
    over_definition_tokens: ?GeneratedSqlTokenRange = null,
    over_partition_tokens: ?GeneratedSqlTokenRange = null,
    over_partition_items: GeneratedSqlListAst = .{},
    over_order_tokens: ?GeneratedSqlTokenRange = null,
    over_order_items: GeneratedSqlListAst = .{},
    over_frame_tokens: ?GeneratedSqlTokenRange = null,
    over_frame_start_expression_tokens: ?GeneratedSqlTokenRange = null,
    over_frame_start_expression_kind: ?GeneratedSqlExpressionKind = null,
    over_frame_start_expression: ?*GeneratedSqlExpressionAst = null,
    over_frame_end_expression_tokens: ?GeneratedSqlTokenRange = null,
    over_frame_end_expression_kind: ?GeneratedSqlExpressionKind = null,
    over_frame_end_expression: ?*GeneratedSqlExpressionAst = null,
    array_tokens: ?GeneratedSqlTokenRange = null,
    array_items: GeneratedSqlListAst = .{},
    cast_expression_tokens: ?GeneratedSqlTokenRange = null,
    cast_expression_kind: ?GeneratedSqlExpressionKind = null,
    cast_expression: ?*GeneratedSqlExpressionAst = null,
    cast_type_tokens: ?GeneratedSqlTokenRange = null,
    case_branch_count: usize = 0,
    case_first_when_tokens: ?GeneratedSqlTokenRange = null,
    case_last_when_tokens: ?GeneratedSqlTokenRange = null,
    case_first_condition_tokens: ?GeneratedSqlTokenRange = null,
    case_first_condition_kind: ?GeneratedSqlExpressionKind = null,
    case_first_condition: ?*GeneratedSqlExpressionAst = null,
    case_first_result_tokens: ?GeneratedSqlTokenRange = null,
    case_first_result_kind: ?GeneratedSqlExpressionKind = null,
    case_first_result: ?*GeneratedSqlExpressionAst = null,
    case_condition_items: GeneratedSqlListAst = .{},
    case_result_items: GeneratedSqlListAst = .{},
    case_else_tokens: ?GeneratedSqlTokenRange = null,
    case_else_expression_tokens: ?GeneratedSqlTokenRange = null,
    case_else_expression_kind: ?GeneratedSqlExpressionKind = null,
    case_else_expression: ?*GeneratedSqlExpressionAst = null,
    boolean_condition_count: usize = 0,
    boolean_first_condition_tokens: ?GeneratedSqlTokenRange = null,
    boolean_first_condition_kind: ?GeneratedSqlExpressionKind = null,
    boolean_first_condition: ?*GeneratedSqlExpressionAst = null,
    boolean_last_condition_tokens: ?GeneratedSqlTokenRange = null,
    boolean_last_condition_kind: ?GeneratedSqlExpressionKind = null,
    boolean_last_condition: ?*GeneratedSqlExpressionAst = null,
    boolean_condition_items: GeneratedSqlListAst = .{},
    interval_value_tokens: ?GeneratedSqlTokenRange = null,
    timestamp_type_tokens: ?GeneratedSqlTokenRange = null,
    timestamp_value_tokens: ?GeneratedSqlTokenRange = null,
    current_timestamp_precision_tokens: ?GeneratedSqlTokenRange = null,
    extract_field_tokens: ?GeneratedSqlTokenRange = null,
    extract_source_tokens: ?GeneratedSqlTokenRange = null,
    extract_source_expression_kind: ?GeneratedSqlExpressionKind = null,
    extract_source_expression: ?*GeneratedSqlExpressionAst = null,
    left_tokens: ?GeneratedSqlTokenRange = null,
    left_expression_kind: ?GeneratedSqlExpressionKind = null,
    left_expression: ?*GeneratedSqlExpressionAst = null,
    negation_tokens: ?GeneratedSqlTokenRange = null,
    operator_tokens: ?GeneratedSqlTokenRange = null,
    between_modifier_tokens: ?GeneratedSqlTokenRange = null,
    between_modifier: ?GeneratedSqlBetweenModifier = null,
    between_lower_tokens: ?GeneratedSqlTokenRange = null,
    between_lower_expression_kind: ?GeneratedSqlExpressionKind = null,
    between_lower_expression: ?*GeneratedSqlExpressionAst = null,
    between_upper_tokens: ?GeneratedSqlTokenRange = null,
    between_upper_expression_kind: ?GeneratedSqlExpressionKind = null,
    between_upper_expression: ?*GeneratedSqlExpressionAst = null,
    quantifier_tokens: ?GeneratedSqlTokenRange = null,
    right_tokens: ?GeneratedSqlTokenRange = null,
    right_expression_kind: ?GeneratedSqlExpressionKind = null,
    right_expression: ?*GeneratedSqlExpressionAst = null,
    escape_tokens: ?GeneratedSqlTokenRange = null,
    escape_expression_kind: ?GeneratedSqlExpressionKind = null,
    escape_expression: ?*GeneratedSqlExpressionAst = null,

    pub fn deinit(self: *GeneratedSqlExpressionAst, alloc: std.mem.Allocator) void {
        if (self.inner_expression) |inner| {
            inner.deinit(alloc);
            alloc.destroy(inner);
        }
        if (self.left_expression) |left| {
            left.deinit(alloc);
            alloc.destroy(left);
        }
        if (self.right_expression) |right| {
            right.deinit(alloc);
            alloc.destroy(right);
        }
        if (self.filter_expression) |filter| {
            filter.deinit(alloc);
            alloc.destroy(filter);
        }
        if (self.escape_expression) |escape| {
            escape.deinit(alloc);
            alloc.destroy(escape);
        }
        if (self.cast_expression) |cast_expression| {
            cast_expression.deinit(alloc);
            alloc.destroy(cast_expression);
        }
        if (self.over_frame_start_expression) |frame_start_expression| {
            frame_start_expression.deinit(alloc);
            alloc.destroy(frame_start_expression);
        }
        if (self.over_frame_end_expression) |frame_end_expression| {
            frame_end_expression.deinit(alloc);
            alloc.destroy(frame_end_expression);
        }
        if (self.between_lower_expression) |between_lower_expression| {
            between_lower_expression.deinit(alloc);
            alloc.destroy(between_lower_expression);
        }
        if (self.between_upper_expression) |between_upper_expression| {
            between_upper_expression.deinit(alloc);
            alloc.destroy(between_upper_expression);
        }
        if (self.case_first_condition) |case_first_condition| {
            case_first_condition.deinit(alloc);
            alloc.destroy(case_first_condition);
        }
        if (self.case_first_result) |case_first_result| {
            case_first_result.deinit(alloc);
            alloc.destroy(case_first_result);
        }
        self.case_condition_items.deinit(alloc);
        self.case_result_items.deinit(alloc);
        if (self.case_else_expression) |case_else_expression| {
            case_else_expression.deinit(alloc);
            alloc.destroy(case_else_expression);
        }
        if (self.boolean_first_condition) |boolean_first_condition| {
            boolean_first_condition.deinit(alloc);
            alloc.destroy(boolean_first_condition);
        }
        if (self.boolean_last_condition) |boolean_last_condition| {
            boolean_last_condition.deinit(alloc);
            alloc.destroy(boolean_last_condition);
        }
        if (self.extract_source_expression) |extract_source_expression| {
            extract_source_expression.deinit(alloc);
            alloc.destroy(extract_source_expression);
        }
        self.subquery_projection_items.deinit(alloc);
        if (self.subquery_where_expression) |subquery_where_expression| {
            subquery_where_expression.deinit(alloc);
            alloc.destroy(subquery_where_expression);
        }
        if (self.subquery_set_operation) |subquery_set_operation| {
            subquery_set_operation.deinit(alloc);
            alloc.destroy(subquery_set_operation);
        }
        if (self.subquery_tail) |tail| {
            tail.deinit(alloc);
            alloc.destroy(tail);
        }
        self.boolean_condition_items.deinit(alloc);
        self.argument_items.deinit(alloc);
        self.argument_order_items.deinit(alloc);
        self.within_group_order_items.deinit(alloc);
        self.over_partition_items.deinit(alloc);
        self.over_order_items.deinit(alloc);
        self.array_items.deinit(alloc);
        self.* = .{};
    }
};

pub const GeneratedSqlJoinKind = enum {
    inner,
    left,
    right,
    full,
};

pub const GeneratedSqlJoinConditionKind = enum {
    on,
    using,
};

pub const GeneratedSqlSetOperationKind = enum {
    @"union",
    intersect,
    except,
};

pub const GeneratedSqlSetOperationAst = struct {
    tokens: ?GeneratedSqlTokenRange = null,
    operator_tokens: ?GeneratedSqlTokenRange = null,
    kind: ?GeneratedSqlSetOperationKind = null,
    all_tokens: ?GeneratedSqlTokenRange = null,
    right_query_tokens: ?GeneratedSqlTokenRange = null,
    right_select_tokens: ?GeneratedSqlTokenRange = null,
    right_distinct_tokens: ?GeneratedSqlTokenRange = null,
    right_distinct_on_items: GeneratedSqlListAst = .{},
    right_projection_tokens: ?GeneratedSqlTokenRange = null,
    right_projection_items: GeneratedSqlListAst = .{},
    right_projection_first_expression: GeneratedSqlExpressionAst = .{},
    right_projection_last_expression: GeneratedSqlExpressionAst = .{},
    right_source_tokens: ?GeneratedSqlTokenRange = null,
    right_where_tokens: ?GeneratedSqlTokenRange = null,
    right_where_expression: GeneratedSqlExpressionAst = .{},

    pub fn deinit(self: *GeneratedSqlSetOperationAst, alloc: std.mem.Allocator) void {
        self.right_distinct_on_items.deinit(alloc);
        self.right_projection_items.deinit(alloc);
        self.right_projection_first_expression.deinit(alloc);
        self.right_projection_last_expression.deinit(alloc);
        self.right_where_expression.deinit(alloc);
        self.* = .{};
    }
};

pub const GeneratedSqlJoinAst = struct {
    tokens: GeneratedSqlTokenRange,
    operator_tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlJoinKind,
    tree_index: usize = 0,
    tree_depth: usize = 1,
    left_child_index: ?usize = null,
    left_tokens: GeneratedSqlTokenRange,
    right_tokens: GeneratedSqlTokenRange,
    condition_kind: GeneratedSqlJoinConditionKind,
    condition_tokens: GeneratedSqlTokenRange,
    predicate_tokens: ?GeneratedSqlTokenRange = null,
    predicate_expression: GeneratedSqlExpressionAst = .{},
    using_tokens: ?GeneratedSqlTokenRange = null,
    using_column_tokens: ?GeneratedSqlTokenRange = null,
    using_columns: GeneratedSqlListAst = .{},

    pub fn deinit(self: *GeneratedSqlJoinAst, alloc: std.mem.Allocator) void {
        self.predicate_expression.deinit(alloc);
        self.using_columns.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlGraphTableFunctionAst = struct {
    tokens: GeneratedSqlTokenRange,
    name_tokens: GeneratedSqlTokenRange,
    argument_tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlGraphTableFunctionKind,
    argument_count: usize = 0,
    table_name_value_tokens: ?GeneratedSqlTokenRange = null,
    index_value_tokens: ?GeneratedSqlTokenRange = null,
    start_value_tokens: ?GeneratedSqlTokenRange = null,
    start_result_ref_value_tokens: ?GeneratedSqlTokenRange = null,
    target_value_tokens: ?GeneratedSqlTokenRange = null,
    target_result_ref_value_tokens: ?GeneratedSqlTokenRange = null,
    pattern_value_tokens: ?GeneratedSqlTokenRange = null,
    return_value_tokens: ?GeneratedSqlTokenRange = null,
    metric_value_tokens: ?GeneratedSqlTokenRange = null,
    query_value_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlNamedArgumentAst = struct {
    tokens: GeneratedSqlTokenRange,
    name_tokens: GeneratedSqlTokenRange,
    operator_tokens: GeneratedSqlTokenRange,
    value_tokens: GeneratedSqlTokenRange,
};

pub const GeneratedSqlAntflyTableFunctionAst = struct {
    tokens: GeneratedSqlTokenRange,
    name_tokens: GeneratedSqlTokenRange,
    argument_tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlAntflyTableFunctionKind,
    argument_items: []GeneratedSqlNamedArgumentAst = &.{},
    argument_count: usize = 0,

    pub fn deinit(self: *GeneratedSqlAntflyTableFunctionAst, alloc: std.mem.Allocator) void {
        if (self.argument_items.len > 0) alloc.free(self.argument_items);
        self.* = undefined;
    }
};

pub const GeneratedSqlWindowAst = struct {
    tokens: GeneratedSqlTokenRange,
    name_tokens: GeneratedSqlTokenRange,
    definition_tokens: GeneratedSqlTokenRange,
    partition_tokens: ?GeneratedSqlTokenRange = null,
    partition_items: GeneratedSqlListAst = .{},
    order_tokens: ?GeneratedSqlTokenRange = null,
    order_items: GeneratedSqlListAst = .{},
    frame_tokens: ?GeneratedSqlTokenRange = null,
    frame_start_expression_tokens: ?GeneratedSqlTokenRange = null,
    frame_start_expression_kind: ?GeneratedSqlExpressionKind = null,
    frame_start_expression: ?*GeneratedSqlExpressionAst = null,
    frame_end_expression_tokens: ?GeneratedSqlTokenRange = null,
    frame_end_expression_kind: ?GeneratedSqlExpressionKind = null,
    frame_end_expression: ?*GeneratedSqlExpressionAst = null,

    pub fn deinit(self: *GeneratedSqlWindowAst, alloc: std.mem.Allocator) void {
        self.partition_items.deinit(alloc);
        self.order_items.deinit(alloc);
        if (self.frame_start_expression) |frame_start_expression| {
            frame_start_expression.deinit(alloc);
            alloc.destroy(frame_start_expression);
        }
        if (self.frame_end_expression) |frame_end_expression| {
            frame_end_expression.deinit(alloc);
            alloc.destroy(frame_end_expression);
        }
        self.* = undefined;
    }
};

pub const GeneratedSqlCteAst = struct {
    name_tokens: GeneratedSqlTokenRange,
    column_tokens: ?GeneratedSqlTokenRange = null,
    column_name_tokens: ?GeneratedSqlTokenRange = null,
    column_names: GeneratedSqlListAst = .{},
    materialization_tokens: ?GeneratedSqlTokenRange = null,
    materialization: ?GeneratedSqlCteMaterialization = null,
    body_tokens: ?GeneratedSqlTokenRange = null,
    body_kind: ?GeneratedSqlReadKind = null,
    body_select_tokens: ?GeneratedSqlTokenRange = null,
    body_distinct_tokens: ?GeneratedSqlTokenRange = null,
    body_distinct_on_items: GeneratedSqlListAst = .{},
    body_projection_tokens: ?GeneratedSqlTokenRange = null,
    body_source_tokens: ?GeneratedSqlTokenRange = null,
    body_source_antfly_function_items: []GeneratedSqlAntflyTableFunctionAst = &.{},
    body_source_antfly_function_count: usize = 0,
    body_source_graph_function_items: []GeneratedSqlGraphTableFunctionAst = &.{},
    body_source_graph_function_count: usize = 0,
    body_join_tokens: ?GeneratedSqlTokenRange = null,
    body_join_operator_tokens: ?GeneratedSqlTokenRange = null,
    body_join_kind: ?GeneratedSqlJoinKind = null,
    body_join_left_tokens: ?GeneratedSqlTokenRange = null,
    body_join_right_tokens: ?GeneratedSqlTokenRange = null,
    body_join_predicate_tokens: ?GeneratedSqlTokenRange = null,
    body_join_predicate_expression: GeneratedSqlExpressionAst = .{},
    body_join_items: []GeneratedSqlJoinAst = &.{},
    body_join_tree_root_index: ?usize = null,
    body_join_tree_depth: usize = 0,
    body_where_tokens: ?GeneratedSqlTokenRange = null,
    body_group_tokens: ?GeneratedSqlTokenRange = null,
    body_having_tokens: ?GeneratedSqlTokenRange = null,
    body_window_tokens: ?GeneratedSqlTokenRange = null,
    body_window_items: []GeneratedSqlWindowAst = &.{},
    body_window_count: usize = 0,
    body_order_tokens: ?GeneratedSqlTokenRange = null,
    body_limit_tokens: ?GeneratedSqlTokenRange = null,
    body_limit_expression: GeneratedSqlExpressionAst = .{},
    body_limit_all: bool = false,
    body_offset_tokens: ?GeneratedSqlTokenRange = null,
    body_offset_expression: GeneratedSqlExpressionAst = .{},
    body_fetch_tokens: ?GeneratedSqlTokenRange = null,
    body_fetch_count_tokens: ?GeneratedSqlTokenRange = null,
    body_fetch_count_expression: GeneratedSqlExpressionAst = .{},
    body_row_lock_tokens: ?GeneratedSqlTokenRange = null,
    body_set_operation_tokens: ?GeneratedSqlTokenRange = null,
    body_set_operation: GeneratedSqlSetOperationAst = .{},
    body_projection_items: GeneratedSqlListAst = .{},
    body_projection_first_expression: GeneratedSqlExpressionAst = .{},
    body_projection_last_expression: GeneratedSqlExpressionAst = .{},
    body_where_expression: GeneratedSqlExpressionAst = .{},
    body_group_items: GeneratedSqlListAst = .{},
    body_group_first_expression: GeneratedSqlExpressionAst = .{},
    body_group_last_expression: GeneratedSqlExpressionAst = .{},
    body_having_expression: GeneratedSqlExpressionAst = .{},
    body_order_items: GeneratedSqlListAst = .{},
    body_order_first_expression: GeneratedSqlExpressionAst = .{},
    body_order_last_expression: GeneratedSqlExpressionAst = .{},

    pub fn deinit(self: *GeneratedSqlCteAst, alloc: std.mem.Allocator) void {
        self.column_names.deinit(alloc);
        self.body_distinct_on_items.deinit(alloc);
        self.body_projection_items.deinit(alloc);
        self.body_projection_first_expression.deinit(alloc);
        self.body_projection_last_expression.deinit(alloc);
        for (self.body_source_antfly_function_items) |*item| item.deinit(alloc);
        if (self.body_source_antfly_function_items.len > 0) alloc.free(self.body_source_antfly_function_items);
        if (self.body_source_graph_function_items.len > 0) alloc.free(self.body_source_graph_function_items);
        self.body_join_predicate_expression.deinit(alloc);
        for (self.body_join_items) |*join| join.deinit(alloc);
        if (self.body_join_items.len > 0) alloc.free(self.body_join_items);
        self.body_where_expression.deinit(alloc);
        self.body_group_items.deinit(alloc);
        self.body_group_first_expression.deinit(alloc);
        self.body_group_last_expression.deinit(alloc);
        self.body_having_expression.deinit(alloc);
        for (self.body_window_items) |*window| window.deinit(alloc);
        if (self.body_window_items.len > 0) alloc.free(self.body_window_items);
        self.body_order_items.deinit(alloc);
        self.body_order_first_expression.deinit(alloc);
        self.body_order_last_expression.deinit(alloc);
        self.body_limit_expression.deinit(alloc);
        self.body_offset_expression.deinit(alloc);
        self.body_fetch_count_expression.deinit(alloc);
        self.body_set_operation.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlCteMaterialization = enum {
    materialized,
    not_materialized,
};

pub const GeneratedSqlSessionAst = struct {
    kind: GeneratedSqlSessionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    name_tokens: ?GeneratedSqlTokenRange = null,
    value_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlTransactionAst = struct {
    kind: GeneratedSqlTransactionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    boundary_tail_tokens: ?GeneratedSqlTokenRange = null,
    mode_tokens: ?GeneratedSqlTokenRange = null,
    name_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlPreparedAst = struct {
    kind: GeneratedSqlPreparedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    name_tokens: ?GeneratedSqlTokenRange = null,
    parameter_tokens: ?GeneratedSqlTokenRange = null,
    argument_tokens: ?GeneratedSqlTokenRange = null,
    inner_statement_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlCursorAst = struct {
    kind: GeneratedSqlCursorKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    tail_tokens: ?GeneratedSqlTokenRange = null,
};

pub const GeneratedSqlDdlAst = struct {
    kind: GeneratedSqlDdlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    object_name_tokens: ?GeneratedSqlTokenRange = null,
    schema_name_tokens: ?GeneratedSqlTokenRange = null,
    version_tokens: ?GeneratedSqlTokenRange = null,
    index_table_tokens: ?GeneratedSqlTokenRange = null,
    index_method_tokens: ?GeneratedSqlTokenRange = null,
    index_elements_tokens: ?GeneratedSqlTokenRange = null,
    index_include_tokens: ?GeneratedSqlTokenRange = null,
    index_options_tokens: ?GeneratedSqlTokenRange = null,
    index_where_tokens: ?GeneratedSqlTokenRange = null,
    alter_table_operation_tokens: ?GeneratedSqlTokenRange = null,
    unique: bool = false,
    if_not_exists: bool = false,
    if_exists: bool = false,
    cascade: bool = false,
    force: bool = false,
    replace_existing: bool = false,
};

pub const GeneratedSqlReadAst = struct {
    kind: GeneratedSqlReadKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    cte_tokens: ?GeneratedSqlTokenRange = null,
    cte_list_tokens: ?GeneratedSqlTokenRange = null,
    cte_name_tokens: ?GeneratedSqlTokenRange = null,
    cte_body_tokens: ?GeneratedSqlTokenRange = null,
    cte_last_name_tokens: ?GeneratedSqlTokenRange = null,
    cte_last_body_tokens: ?GeneratedSqlTokenRange = null,
    cte_items: []GeneratedSqlCteAst = &.{},
    cte_count: usize = 0,
    cte_recursive: bool = false,
    distinct_tokens: ?GeneratedSqlTokenRange = null,
    distinct_on_items: GeneratedSqlListAst = .{},
    projection_tokens: ?GeneratedSqlTokenRange = null,
    projection_items: GeneratedSqlListAst = .{},
    projection_first_expression: GeneratedSqlExpressionAst = .{},
    projection_last_expression: GeneratedSqlExpressionAst = .{},
    source_tokens: ?GeneratedSqlTokenRange = null,
    source_graph_function_tokens: ?GeneratedSqlTokenRange = null,
    source_graph_function_name_tokens: ?GeneratedSqlTokenRange = null,
    source_graph_function_argument_tokens: ?GeneratedSqlTokenRange = null,
    source_graph_function_kind: ?GeneratedSqlGraphTableFunctionKind = null,
    source_antfly_function_items: []GeneratedSqlAntflyTableFunctionAst = &.{},
    source_antfly_function_count: usize = 0,
    source_graph_function_items: []GeneratedSqlGraphTableFunctionAst = &.{},
    source_graph_function_count: usize = 0,
    join_tokens: ?GeneratedSqlTokenRange = null,
    join_operator_tokens: ?GeneratedSqlTokenRange = null,
    join_kind: ?GeneratedSqlJoinKind = null,
    join_left_tokens: ?GeneratedSqlTokenRange = null,
    join_right_tokens: ?GeneratedSqlTokenRange = null,
    join_predicate_tokens: ?GeneratedSqlTokenRange = null,
    join_predicate_expression: GeneratedSqlExpressionAst = .{},
    join_items: []GeneratedSqlJoinAst = &.{},
    join_tree_root_index: ?usize = null,
    join_tree_depth: usize = 0,
    where_tokens: ?GeneratedSqlTokenRange = null,
    where_expression: GeneratedSqlExpressionAst = .{},
    group_tokens: ?GeneratedSqlTokenRange = null,
    group_items: GeneratedSqlListAst = .{},
    group_first_expression: GeneratedSqlExpressionAst = .{},
    group_last_expression: GeneratedSqlExpressionAst = .{},
    having_tokens: ?GeneratedSqlTokenRange = null,
    having_expression: GeneratedSqlExpressionAst = .{},
    window_tokens: ?GeneratedSqlTokenRange = null,
    window_items: []GeneratedSqlWindowAst = &.{},
    window_count: usize = 0,
    order_tokens: ?GeneratedSqlTokenRange = null,
    order_items: GeneratedSqlListAst = .{},
    order_first_expression: GeneratedSqlExpressionAst = .{},
    order_last_expression: GeneratedSqlExpressionAst = .{},
    limit_tokens: ?GeneratedSqlTokenRange = null,
    limit_expression: GeneratedSqlExpressionAst = .{},
    limit_all: bool = false,
    offset_tokens: ?GeneratedSqlTokenRange = null,
    offset_expression: GeneratedSqlExpressionAst = .{},
    fetch_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_tokens: ?GeneratedSqlTokenRange = null,
    fetch_count_expression: GeneratedSqlExpressionAst = .{},
    row_lock_tokens: ?GeneratedSqlTokenRange = null,
    set_operation_tokens: ?GeneratedSqlTokenRange = null,
    set_operation: GeneratedSqlSetOperationAst = .{},

    pub fn deinit(self: *GeneratedSqlReadAst, alloc: std.mem.Allocator) void {
        for (self.cte_items) |*cte| cte.deinit(alloc);
        if (self.cte_items.len > 0) alloc.free(self.cte_items);
        for (self.source_antfly_function_items) |*item| item.deinit(alloc);
        if (self.source_antfly_function_items.len > 0) alloc.free(self.source_antfly_function_items);
        if (self.source_graph_function_items.len > 0) alloc.free(self.source_graph_function_items);
        for (self.join_items) |*join| join.deinit(alloc);
        if (self.join_items.len > 0) alloc.free(self.join_items);
        self.distinct_on_items.deinit(alloc);
        self.projection_items.deinit(alloc);
        self.projection_first_expression.deinit(alloc);
        self.projection_last_expression.deinit(alloc);
        self.join_predicate_expression.deinit(alloc);
        self.where_expression.deinit(alloc);
        self.group_items.deinit(alloc);
        self.group_first_expression.deinit(alloc);
        self.group_last_expression.deinit(alloc);
        self.having_expression.deinit(alloc);
        for (self.window_items) |*window| window.deinit(alloc);
        if (self.window_items.len > 0) alloc.free(self.window_items);
        self.order_items.deinit(alloc);
        self.order_first_expression.deinit(alloc);
        self.order_last_expression.deinit(alloc);
        self.limit_expression.deinit(alloc);
        self.offset_expression.deinit(alloc);
        self.fetch_count_expression.deinit(alloc);
        self.set_operation.deinit(alloc);
        self.* = undefined;
    }
};

fn rebaseGeneratedSqlTokenRange(
    range: GeneratedSqlTokenRange,
    base: usize,
    end: usize,
) !GeneratedSqlTokenRange {
    if (range.start < base or range.end < range.start or range.end > end) return error.UnsupportedSqlShape;
    return .{ .start = range.start - base, .end = range.end - base };
}

fn rebaseGeneratedSqlTokenRangeOptional(
    range: ?GeneratedSqlTokenRange,
    base: usize,
    end: usize,
) !?GeneratedSqlTokenRange {
    return if (range) |value| try rebaseGeneratedSqlTokenRange(value, base, end) else null;
}

fn cloneRebasedGeneratedSqlTokenRangeSliceAlloc(
    alloc: std.mem.Allocator,
    ranges: []const GeneratedSqlTokenRange,
    base: usize,
    end: usize,
) ![]GeneratedSqlTokenRange {
    if (ranges.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlTokenRange, ranges.len);
    errdefer alloc.free(cloned);
    for (ranges, cloned) |range, *out| out.* = try rebaseGeneratedSqlTokenRange(range, base, end);
    return cloned;
}

fn cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(
    alloc: std.mem.Allocator,
    ranges: []const ?GeneratedSqlTokenRange,
    base: usize,
    end: usize,
) ![]?GeneratedSqlTokenRange {
    if (ranges.len == 0) return &.{};
    const cloned = try alloc.alloc(?GeneratedSqlTokenRange, ranges.len);
    errdefer alloc.free(cloned);
    for (ranges, cloned) |range, *out| out.* = try rebaseGeneratedSqlTokenRangeOptional(range, base, end);
    return cloned;
}

fn cloneGeneratedSqlOrderDirectionOptionalSliceAlloc(
    alloc: std.mem.Allocator,
    directions: []const ?GeneratedSqlOrderDirection,
) ![]?GeneratedSqlOrderDirection {
    if (directions.len == 0) return &.{};
    return try alloc.dupe(?GeneratedSqlOrderDirection, directions);
}

fn cloneGeneratedSqlNullsOrderOptionalSliceAlloc(
    alloc: std.mem.Allocator,
    orders: []const ?GeneratedSqlNullsOrder,
) ![]?GeneratedSqlNullsOrder {
    if (orders.len == 0) return &.{};
    return try alloc.dupe(?GeneratedSqlNullsOrder, orders);
}

fn cloneRebasedGeneratedExpressionPtrAlloc(
    alloc: std.mem.Allocator,
    expression: ?*GeneratedSqlExpressionAst,
    base: usize,
    end: usize,
) anyerror!?*GeneratedSqlExpressionAst {
    const input = expression orelse return null;
    const cloned = try alloc.create(GeneratedSqlExpressionAst);
    errdefer alloc.destroy(cloned);
    cloned.* = try cloneRebasedGeneratedExpressionAlloc(alloc, input.*, base, end);
    return cloned;
}

fn cloneRebasedGeneratedSetOperationPtrAlloc(
    alloc: std.mem.Allocator,
    set_operation: ?*GeneratedSqlSetOperationAst,
    base: usize,
    end: usize,
) anyerror!?*GeneratedSqlSetOperationAst {
    const input = set_operation orelse return null;
    const cloned = try alloc.create(GeneratedSqlSetOperationAst);
    errdefer alloc.destroy(cloned);
    cloned.* = try cloneRebasedGeneratedSetOperationAlloc(alloc, input.*, base, end);
    return cloned;
}

fn cloneRebasedGeneratedSubqueryTailPtrAlloc(
    alloc: std.mem.Allocator,
    tail: ?*GeneratedSqlSubqueryTailAst,
    base: usize,
    end: usize,
) anyerror!?*GeneratedSqlSubqueryTailAst {
    const input = tail orelse return null;
    const cloned = try alloc.create(GeneratedSqlSubqueryTailAst);
    cloned.* = .{};
    errdefer {
        cloned.deinit(alloc);
        alloc.destroy(cloned);
    }
    cloned.order_tokens = try rebaseGeneratedSqlTokenRangeOptional(input.order_tokens, base, end);
    cloned.order_items = try cloneRebasedGeneratedListAlloc(alloc, input.order_items, base, end);
    cloned.order_first_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, input.order_first_expression, base, end);
    cloned.order_last_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, input.order_last_expression, base, end);
    cloned.limit_tokens = try rebaseGeneratedSqlTokenRangeOptional(input.limit_tokens, base, end);
    cloned.limit_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, input.limit_expression, base, end);
    cloned.limit_all = input.limit_all;
    cloned.offset_tokens = try rebaseGeneratedSqlTokenRangeOptional(input.offset_tokens, base, end);
    cloned.offset_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, input.offset_expression, base, end);
    cloned.fetch_tokens = try rebaseGeneratedSqlTokenRangeOptional(input.fetch_tokens, base, end);
    cloned.fetch_count_tokens = try rebaseGeneratedSqlTokenRangeOptional(input.fetch_count_tokens, base, end);
    cloned.fetch_count_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, input.fetch_count_expression, base, end);
    return cloned;
}

fn cloneRebasedGeneratedExpressionSliceAlloc(
    alloc: std.mem.Allocator,
    expressions: []const GeneratedSqlExpressionAst,
    base: usize,
    end: usize,
) anyerror![]GeneratedSqlExpressionAst {
    if (expressions.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlExpressionAst, expressions.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*expression| expression.deinit(alloc);
        alloc.free(cloned);
    }
    for (expressions, cloned) |expression, *out| {
        out.* = try cloneRebasedGeneratedExpressionAlloc(alloc, expression, base, end);
        initialized += 1;
    }
    return cloned;
}

fn cloneRebasedGeneratedListAlloc(
    alloc: std.mem.Allocator,
    list: GeneratedSqlListAst,
    base: usize,
    end: usize,
) !GeneratedSqlListAst {
    var cloned = GeneratedSqlListAst{};
    errdefer cloned.deinit(alloc);
    cloned.first_tokens = try rebaseGeneratedSqlTokenRangeOptional(list.first_tokens, base, end);
    cloned.last_tokens = try rebaseGeneratedSqlTokenRangeOptional(list.last_tokens, base, end);
    cloned.items = try cloneRebasedGeneratedSqlTokenRangeSliceAlloc(alloc, list.items, base, end);
    cloned.expression_items = try cloneRebasedGeneratedSqlTokenRangeSliceAlloc(alloc, list.expression_items, base, end);
    cloned.alias_items = try cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(alloc, list.alias_items, base, end);
    cloned.alias_name_items = try cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(alloc, list.alias_name_items, base, end);
    cloned.direction_items = try cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(alloc, list.direction_items, base, end);
    cloned.directions = try cloneGeneratedSqlOrderDirectionOptionalSliceAlloc(alloc, list.directions);
    cloned.order_using_operator_items = try cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(alloc, list.order_using_operator_items, base, end);
    cloned.nulls_order_items = try cloneRebasedGeneratedSqlTokenRangeOptionalSliceAlloc(alloc, list.nulls_order_items, base, end);
    cloned.nulls_orders = try cloneGeneratedSqlNullsOrderOptionalSliceAlloc(alloc, list.nulls_orders);
    cloned.expressions = try cloneRebasedGeneratedExpressionSliceAlloc(alloc, list.expressions, base, end);
    cloned.count = list.count;
    return cloned;
}

fn cloneRebasedGeneratedExpressionAlloc(
    alloc: std.mem.Allocator,
    expression: GeneratedSqlExpressionAst,
    base: usize,
    end: usize,
) anyerror!GeneratedSqlExpressionAst {
    var cloned = expression;
    cloned.inner_expression = null;
    cloned.subquery_projection_items = .{};
    cloned.subquery_where_expression = null;
    cloned.subquery_set_operation = null;
    cloned.subquery_tail = null;
    cloned.argument_items = .{};
    cloned.argument_order_items = .{};
    cloned.within_group_order_items = .{};
    cloned.filter_expression = null;
    cloned.over_partition_items = .{};
    cloned.over_order_items = .{};
    cloned.over_frame_start_expression = null;
    cloned.over_frame_end_expression = null;
    cloned.array_items = .{};
    cloned.cast_expression = null;
    cloned.case_first_condition = null;
    cloned.case_first_result = null;
    cloned.case_condition_items = .{};
    cloned.case_result_items = .{};
    cloned.case_else_expression = null;
    cloned.boolean_first_condition = null;
    cloned.boolean_last_condition = null;
    cloned.boolean_condition_items = .{};
    cloned.extract_source_expression = null;
    cloned.left_expression = null;
    cloned.between_lower_expression = null;
    cloned.between_upper_expression = null;
    cloned.right_expression = null;
    cloned.escape_expression = null;
    errdefer cloned.deinit(alloc);

    cloned.tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.tokens, base, end);
    cloned.inner_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.inner_tokens, base, end);
    cloned.inner_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.inner_expression, base, end);
    cloned.subquery_select_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.subquery_select_tokens, base, end);
    cloned.subquery_projection_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.subquery_projection_tokens, base, end);
    cloned.subquery_projection_items = try cloneRebasedGeneratedListAlloc(alloc, expression.subquery_projection_items, base, end);
    cloned.subquery_source_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.subquery_source_tokens, base, end);
    cloned.subquery_where_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.subquery_where_tokens, base, end);
    cloned.subquery_where_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.subquery_where_expression, base, end);
    cloned.subquery_set_operation_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.subquery_set_operation_tokens, base, end);
    cloned.subquery_set_operation = try cloneRebasedGeneratedSetOperationPtrAlloc(alloc, expression.subquery_set_operation, base, end);
    cloned.subquery_tail = try cloneRebasedGeneratedSubqueryTailPtrAlloc(alloc, expression.subquery_tail, base, end);
    cloned.function_name_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.function_name_tokens, base, end);
    cloned.argument_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.argument_tokens, base, end);
    cloned.argument_distinct_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.argument_distinct_tokens, base, end);
    cloned.argument_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.argument_value_tokens, base, end);
    cloned.argument_items = try cloneRebasedGeneratedListAlloc(alloc, expression.argument_items, base, end);
    cloned.argument_order_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.argument_order_tokens, base, end);
    cloned.argument_order_items = try cloneRebasedGeneratedListAlloc(alloc, expression.argument_order_items, base, end);
    cloned.within_group_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.within_group_tokens, base, end);
    cloned.within_group_order_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.within_group_order_tokens, base, end);
    cloned.within_group_order_items = try cloneRebasedGeneratedListAlloc(alloc, expression.within_group_order_items, base, end);
    cloned.filter_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.filter_tokens, base, end);
    cloned.filter_predicate_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.filter_predicate_tokens, base, end);
    cloned.filter_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.filter_expression, base, end);
    cloned.over_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_tokens, base, end);
    cloned.over_name_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_name_tokens, base, end);
    cloned.over_definition_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_definition_tokens, base, end);
    cloned.over_partition_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_partition_tokens, base, end);
    cloned.over_partition_items = try cloneRebasedGeneratedListAlloc(alloc, expression.over_partition_items, base, end);
    cloned.over_order_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_order_tokens, base, end);
    cloned.over_order_items = try cloneRebasedGeneratedListAlloc(alloc, expression.over_order_items, base, end);
    cloned.over_frame_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_frame_tokens, base, end);
    cloned.over_frame_start_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_frame_start_expression_tokens, base, end);
    cloned.over_frame_start_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.over_frame_start_expression, base, end);
    cloned.over_frame_end_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.over_frame_end_expression_tokens, base, end);
    cloned.over_frame_end_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.over_frame_end_expression, base, end);
    cloned.array_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.array_tokens, base, end);
    cloned.array_items = try cloneRebasedGeneratedListAlloc(alloc, expression.array_items, base, end);
    cloned.cast_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.cast_expression_tokens, base, end);
    cloned.cast_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.cast_expression, base, end);
    cloned.cast_type_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.cast_type_tokens, base, end);
    cloned.case_first_when_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_first_when_tokens, base, end);
    cloned.case_last_when_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_last_when_tokens, base, end);
    cloned.case_first_condition_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_first_condition_tokens, base, end);
    cloned.case_first_condition = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.case_first_condition, base, end);
    cloned.case_first_result_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_first_result_tokens, base, end);
    cloned.case_first_result = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.case_first_result, base, end);
    cloned.case_condition_items = try cloneRebasedGeneratedListAlloc(alloc, expression.case_condition_items, base, end);
    cloned.case_result_items = try cloneRebasedGeneratedListAlloc(alloc, expression.case_result_items, base, end);
    cloned.case_else_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_else_tokens, base, end);
    cloned.case_else_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.case_else_expression_tokens, base, end);
    cloned.case_else_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.case_else_expression, base, end);
    cloned.boolean_first_condition_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.boolean_first_condition_tokens, base, end);
    cloned.boolean_first_condition = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.boolean_first_condition, base, end);
    cloned.boolean_last_condition_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.boolean_last_condition_tokens, base, end);
    cloned.boolean_last_condition = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.boolean_last_condition, base, end);
    cloned.boolean_condition_items = try cloneRebasedGeneratedListAlloc(alloc, expression.boolean_condition_items, base, end);
    cloned.interval_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.interval_value_tokens, base, end);
    cloned.timestamp_type_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.timestamp_type_tokens, base, end);
    cloned.timestamp_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.timestamp_value_tokens, base, end);
    cloned.current_timestamp_precision_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.current_timestamp_precision_tokens, base, end);
    cloned.extract_field_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.extract_field_tokens, base, end);
    cloned.extract_source_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.extract_source_tokens, base, end);
    cloned.extract_source_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.extract_source_expression, base, end);
    cloned.left_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.left_tokens, base, end);
    cloned.left_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.left_expression, base, end);
    cloned.negation_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.negation_tokens, base, end);
    cloned.operator_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.operator_tokens, base, end);
    cloned.between_modifier_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.between_modifier_tokens, base, end);
    cloned.between_lower_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.between_lower_tokens, base, end);
    cloned.between_lower_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.between_lower_expression, base, end);
    cloned.between_upper_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.between_upper_tokens, base, end);
    cloned.between_upper_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.between_upper_expression, base, end);
    cloned.quantifier_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.quantifier_tokens, base, end);
    cloned.right_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.right_tokens, base, end);
    cloned.right_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.right_expression, base, end);
    cloned.escape_tokens = try rebaseGeneratedSqlTokenRangeOptional(expression.escape_tokens, base, end);
    cloned.escape_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, expression.escape_expression, base, end);
    return cloned;
}

fn cloneRebasedGeneratedSetOperationAlloc(
    alloc: std.mem.Allocator,
    set_operation: GeneratedSqlSetOperationAst,
    base: usize,
    end: usize,
) !GeneratedSqlSetOperationAst {
    var cloned = GeneratedSqlSetOperationAst{};
    errdefer cloned.deinit(alloc);
    cloned.tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.tokens, base, end);
    cloned.operator_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.operator_tokens, base, end);
    cloned.kind = set_operation.kind;
    cloned.all_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.all_tokens, base, end);
    cloned.right_query_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_query_tokens, base, end);
    cloned.right_select_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_select_tokens, base, end);
    cloned.right_distinct_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_distinct_tokens, base, end);
    cloned.right_distinct_on_items = try cloneRebasedGeneratedListAlloc(alloc, set_operation.right_distinct_on_items, base, end);
    cloned.right_projection_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_projection_tokens, base, end);
    cloned.right_projection_items = try cloneRebasedGeneratedListAlloc(alloc, set_operation.right_projection_items, base, end);
    cloned.right_projection_first_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, set_operation.right_projection_first_expression, base, end);
    cloned.right_projection_last_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, set_operation.right_projection_last_expression, base, end);
    cloned.right_source_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_source_tokens, base, end);
    cloned.right_where_tokens = try rebaseGeneratedSqlTokenRangeOptional(set_operation.right_where_tokens, base, end);
    cloned.right_where_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, set_operation.right_where_expression, base, end);
    return cloned;
}

fn cloneRebasedGeneratedJoinSliceAlloc(
    alloc: std.mem.Allocator,
    joins: []const GeneratedSqlJoinAst,
    base: usize,
    end: usize,
) ![]GeneratedSqlJoinAst {
    if (joins.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlJoinAst, joins.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*join| join.deinit(alloc);
        alloc.free(cloned);
    }
    for (joins, cloned) |join, *out| {
        var temp = GeneratedSqlJoinAst{
            .tokens = try rebaseGeneratedSqlTokenRange(join.tokens, base, end),
            .operator_tokens = try rebaseGeneratedSqlTokenRange(join.operator_tokens, base, end),
            .kind = join.kind,
            .tree_index = join.tree_index,
            .tree_depth = join.tree_depth,
            .left_child_index = join.left_child_index,
            .left_tokens = try rebaseGeneratedSqlTokenRange(join.left_tokens, base, end),
            .right_tokens = try rebaseGeneratedSqlTokenRange(join.right_tokens, base, end),
            .condition_kind = join.condition_kind,
            .condition_tokens = try rebaseGeneratedSqlTokenRange(join.condition_tokens, base, end),
        };
        errdefer temp.deinit(alloc);
        temp.predicate_tokens = try rebaseGeneratedSqlTokenRangeOptional(join.predicate_tokens, base, end);
        temp.predicate_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, join.predicate_expression, base, end);
        temp.using_tokens = try rebaseGeneratedSqlTokenRangeOptional(join.using_tokens, base, end);
        temp.using_column_tokens = try rebaseGeneratedSqlTokenRangeOptional(join.using_column_tokens, base, end);
        temp.using_columns = try cloneRebasedGeneratedListAlloc(alloc, join.using_columns, base, end);
        out.* = temp;
        initialized += 1;
    }
    return cloned;
}

fn cloneRebasedGeneratedNamedArgumentSliceAlloc(
    alloc: std.mem.Allocator,
    arguments: []const GeneratedSqlNamedArgumentAst,
    base: usize,
    end: usize,
) ![]GeneratedSqlNamedArgumentAst {
    if (arguments.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlNamedArgumentAst, arguments.len);
    errdefer alloc.free(cloned);
    for (arguments, cloned) |argument, *out| {
        out.* = .{
            .tokens = try rebaseGeneratedSqlTokenRange(argument.tokens, base, end),
            .name_tokens = try rebaseGeneratedSqlTokenRange(argument.name_tokens, base, end),
            .operator_tokens = try rebaseGeneratedSqlTokenRange(argument.operator_tokens, base, end),
            .value_tokens = try rebaseGeneratedSqlTokenRange(argument.value_tokens, base, end),
        };
    }
    return cloned;
}

fn cloneRebasedGeneratedAntflyTableFunctionSliceAlloc(
    alloc: std.mem.Allocator,
    functions: []const GeneratedSqlAntflyTableFunctionAst,
    base: usize,
    end: usize,
) ![]GeneratedSqlAntflyTableFunctionAst {
    if (functions.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlAntflyTableFunctionAst, functions.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*function| function.deinit(alloc);
        alloc.free(cloned);
    }
    for (functions, cloned) |function, *out| {
        out.* = .{
            .tokens = try rebaseGeneratedSqlTokenRange(function.tokens, base, end),
            .name_tokens = try rebaseGeneratedSqlTokenRange(function.name_tokens, base, end),
            .argument_tokens = try rebaseGeneratedSqlTokenRange(function.argument_tokens, base, end),
            .kind = function.kind,
            .argument_items = try cloneRebasedGeneratedNamedArgumentSliceAlloc(alloc, function.argument_items, base, end),
            .argument_count = function.argument_count,
        };
        initialized += 1;
    }
    return cloned;
}

fn cloneRebasedGeneratedGraphTableFunctionSliceAlloc(
    alloc: std.mem.Allocator,
    functions: []const GeneratedSqlGraphTableFunctionAst,
    base: usize,
    end: usize,
) ![]GeneratedSqlGraphTableFunctionAst {
    if (functions.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlGraphTableFunctionAst, functions.len);
    errdefer alloc.free(cloned);
    for (functions, cloned) |function, *out| {
        out.* = .{
            .tokens = try rebaseGeneratedSqlTokenRange(function.tokens, base, end),
            .name_tokens = try rebaseGeneratedSqlTokenRange(function.name_tokens, base, end),
            .argument_tokens = try rebaseGeneratedSqlTokenRange(function.argument_tokens, base, end),
            .kind = function.kind,
            .argument_count = function.argument_count,
            .table_name_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.table_name_value_tokens, base, end),
            .index_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.index_value_tokens, base, end),
            .start_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.start_value_tokens, base, end),
            .start_result_ref_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.start_result_ref_value_tokens, base, end),
            .target_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.target_value_tokens, base, end),
            .target_result_ref_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.target_result_ref_value_tokens, base, end),
            .pattern_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.pattern_value_tokens, base, end),
            .return_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.return_value_tokens, base, end),
            .metric_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.metric_value_tokens, base, end),
            .query_value_tokens = try rebaseGeneratedSqlTokenRangeOptional(function.query_value_tokens, base, end),
        };
    }
    return cloned;
}

fn cloneRebasedGeneratedWindowSliceAlloc(
    alloc: std.mem.Allocator,
    windows: []const GeneratedSqlWindowAst,
    base: usize,
    end: usize,
) ![]GeneratedSqlWindowAst {
    if (windows.len == 0) return &.{};
    const cloned = try alloc.alloc(GeneratedSqlWindowAst, windows.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*window| window.deinit(alloc);
        alloc.free(cloned);
    }
    for (windows, cloned) |window, *out| {
        var temp = GeneratedSqlWindowAst{
            .tokens = try rebaseGeneratedSqlTokenRange(window.tokens, base, end),
            .name_tokens = try rebaseGeneratedSqlTokenRange(window.name_tokens, base, end),
            .definition_tokens = try rebaseGeneratedSqlTokenRange(window.definition_tokens, base, end),
            .partition_tokens = try rebaseGeneratedSqlTokenRangeOptional(window.partition_tokens, base, end),
            .order_tokens = try rebaseGeneratedSqlTokenRangeOptional(window.order_tokens, base, end),
            .frame_tokens = try rebaseGeneratedSqlTokenRangeOptional(window.frame_tokens, base, end),
            .frame_start_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(window.frame_start_expression_tokens, base, end),
            .frame_start_expression_kind = window.frame_start_expression_kind,
            .frame_end_expression_tokens = try rebaseGeneratedSqlTokenRangeOptional(window.frame_end_expression_tokens, base, end),
            .frame_end_expression_kind = window.frame_end_expression_kind,
        };
        errdefer temp.deinit(alloc);
        temp.partition_items = try cloneRebasedGeneratedListAlloc(alloc, window.partition_items, base, end);
        temp.order_items = try cloneRebasedGeneratedListAlloc(alloc, window.order_items, base, end);
        temp.frame_start_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, window.frame_start_expression, base, end);
        temp.frame_end_expression = try cloneRebasedGeneratedExpressionPtrAlloc(alloc, window.frame_end_expression, base, end);
        out.* = temp;
        initialized += 1;
    }
    return cloned;
}

pub fn cloneCteBodyReadAstAlloc(
    alloc: std.mem.Allocator,
    source_statement_span: token_mod.SourceSpan,
    cte: GeneratedSqlCteAst,
) !GeneratedSqlReadAst {
    const body = cte.body_tokens orelse return error.UnsupportedSqlShape;
    const body_kind = cte.body_kind orelse return error.UnsupportedSqlShape;
    var cloned = GeneratedSqlReadAst{
        .kind = body_kind,
        .statement_span = source_statement_span,
        .command_span = source_statement_span,
    };
    errdefer cloned.deinit(alloc);
    cloned.distinct_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_distinct_tokens, body.start, body.end);
    cloned.distinct_on_items = try cloneRebasedGeneratedListAlloc(alloc, cte.body_distinct_on_items, body.start, body.end);
    cloned.projection_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_projection_tokens, body.start, body.end);
    cloned.projection_items = try cloneRebasedGeneratedListAlloc(alloc, cte.body_projection_items, body.start, body.end);
    cloned.projection_first_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_projection_first_expression, body.start, body.end);
    cloned.projection_last_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_projection_last_expression, body.start, body.end);
    cloned.source_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_source_tokens, body.start, body.end);
    cloned.source_antfly_function_items = try cloneRebasedGeneratedAntflyTableFunctionSliceAlloc(alloc, cte.body_source_antfly_function_items, body.start, body.end);
    cloned.source_antfly_function_count = cte.body_source_antfly_function_count;
    cloned.source_graph_function_items = try cloneRebasedGeneratedGraphTableFunctionSliceAlloc(alloc, cte.body_source_graph_function_items, body.start, body.end);
    cloned.source_graph_function_count = cte.body_source_graph_function_count;
    if (cloned.source_graph_function_items.len > 0) {
        const first = cloned.source_graph_function_items[0];
        cloned.source_graph_function_tokens = first.tokens;
        cloned.source_graph_function_name_tokens = first.name_tokens;
        cloned.source_graph_function_argument_tokens = first.argument_tokens;
        cloned.source_graph_function_kind = first.kind;
    }
    cloned.join_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_join_tokens, body.start, body.end);
    cloned.join_operator_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_join_operator_tokens, body.start, body.end);
    cloned.join_kind = cte.body_join_kind;
    cloned.join_left_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_join_left_tokens, body.start, body.end);
    cloned.join_right_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_join_right_tokens, body.start, body.end);
    cloned.join_predicate_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_join_predicate_tokens, body.start, body.end);
    cloned.join_predicate_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_join_predicate_expression, body.start, body.end);
    cloned.join_items = try cloneRebasedGeneratedJoinSliceAlloc(alloc, cte.body_join_items, body.start, body.end);
    cloned.join_tree_root_index = cte.body_join_tree_root_index;
    cloned.join_tree_depth = cte.body_join_tree_depth;
    cloned.where_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_where_tokens, body.start, body.end);
    cloned.where_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_where_expression, body.start, body.end);
    cloned.group_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_group_tokens, body.start, body.end);
    cloned.group_items = try cloneRebasedGeneratedListAlloc(alloc, cte.body_group_items, body.start, body.end);
    cloned.group_first_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_group_first_expression, body.start, body.end);
    cloned.group_last_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_group_last_expression, body.start, body.end);
    cloned.having_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_having_tokens, body.start, body.end);
    cloned.having_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_having_expression, body.start, body.end);
    cloned.window_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_window_tokens, body.start, body.end);
    cloned.window_items = try cloneRebasedGeneratedWindowSliceAlloc(alloc, cte.body_window_items, body.start, body.end);
    cloned.window_count = cte.body_window_count;
    cloned.order_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_order_tokens, body.start, body.end);
    cloned.order_items = try cloneRebasedGeneratedListAlloc(alloc, cte.body_order_items, body.start, body.end);
    cloned.order_first_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_order_first_expression, body.start, body.end);
    cloned.order_last_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_order_last_expression, body.start, body.end);
    cloned.limit_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_limit_tokens, body.start, body.end);
    cloned.limit_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_limit_expression, body.start, body.end);
    cloned.limit_all = cte.body_limit_all;
    cloned.offset_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_offset_tokens, body.start, body.end);
    cloned.offset_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_offset_expression, body.start, body.end);
    cloned.fetch_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_fetch_tokens, body.start, body.end);
    cloned.fetch_count_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_fetch_count_tokens, body.start, body.end);
    cloned.fetch_count_expression = try cloneRebasedGeneratedExpressionAlloc(alloc, cte.body_fetch_count_expression, body.start, body.end);
    cloned.row_lock_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_row_lock_tokens, body.start, body.end);
    cloned.set_operation_tokens = try rebaseGeneratedSqlTokenRangeOptional(cte.body_set_operation_tokens, body.start, body.end);
    cloned.set_operation = try cloneRebasedGeneratedSetOperationAlloc(alloc, cte.body_set_operation, body.start, body.end);
    return cloned;
}

pub const GeneratedSqlDmlReadBodyAst = struct {
    tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlReadKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    projection_tokens: ?GeneratedSqlTokenRange = null,
    source_tokens: ?GeneratedSqlTokenRange = null,
    where_tokens: ?GeneratedSqlTokenRange = null,
    set_operation_tokens: ?GeneratedSqlTokenRange = null,
    wrapper_projection_star: bool = false,
};

pub const GeneratedSqlDmlCteItemAst = struct {
    name_tokens: GeneratedSqlTokenRange,
    body_tokens: GeneratedSqlTokenRange,
    body_read: GeneratedSqlDmlReadBodyAst,
};

pub const GeneratedSqlDmlCteAst = struct {
    tokens: GeneratedSqlTokenRange,
    list_tokens: GeneratedSqlTokenRange,
    first_name_tokens: GeneratedSqlTokenRange,
    first_body_tokens: GeneratedSqlTokenRange,
    first_body_read: GeneratedSqlDmlReadBodyAst,
    last_name_tokens: GeneratedSqlTokenRange,
    last_body_tokens: GeneratedSqlTokenRange,
    last_body_read: GeneratedSqlDmlReadBodyAst,
    items: []GeneratedSqlDmlCteItemAst = &.{},
    count: usize = 0,
    recursive: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

pub const GeneratedSqlDmlAst = struct {
    kind: GeneratedSqlDmlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    cte_tokens: ?GeneratedSqlTokenRange = null,
    cte_prefix: ?GeneratedSqlDmlCteAst = null,
    target_table_tokens: ?GeneratedSqlTokenRange = null,
    insert_columns_tokens: ?GeneratedSqlTokenRange = null,
    values_tokens: ?GeneratedSqlTokenRange = null,
    source_tokens: ?GeneratedSqlTokenRange = null,
    source_read: ?GeneratedSqlDmlReadBodyAst = null,
    assignments_tokens: ?GeneratedSqlTokenRange = null,
    where_tokens: ?GeneratedSqlTokenRange = null,
    conflict_tokens: ?GeneratedSqlTokenRange = null,
    returning_tokens: ?GeneratedSqlTokenRange = null,
    additional_target_tokens: ?GeneratedSqlTokenRange = null,
    cte_recursive: bool = false,
    default_values: bool = false,
    restart_identity: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.cte_prefix) |*cte_prefix| cte_prefix.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlGraphAst = struct {
    kind: GeneratedSqlGraphKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
};

pub const GeneratedSqlUnsupportedAst = struct {
    kind: GeneratedSqlUnsupportedKind,
    reason: GeneratedSqlUnsupportedReason,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    subject_tokens: ?GeneratedSqlTokenRange = null,
    explain_options_tokens: ?GeneratedSqlTokenRange = null,
    explain_options_valid: bool = true,
    explain_analyze: bool = false,
    explain_format: GeneratedSqlExplainFormat = .text,
    explain_verbose: bool = false,
    explain_costs: bool = true,
    explain_buffers: bool = false,
    explain_timing: bool = true,
    explain_summary: bool = true,
    explain_settings: bool = false,
    explain_wal: bool = false,
};

pub const GeneratedSqlExplainFormat = enum {
    text,
    json,
};

pub const GeneratedSqlAst = union(enum) {
    session: GeneratedSqlSessionAst,
    transaction: GeneratedSqlTransactionAst,
    prepared: GeneratedSqlPreparedAst,
    ddl: GeneratedSqlDdlAst,
    dml: GeneratedSqlDmlAst,
    read: *GeneratedSqlReadAst,
    extension_index: GeneratedSqlDdlAst,
    graph: GeneratedSqlGraphAst,
    cursor: GeneratedSqlCursorAst,
    unsupported: GeneratedSqlUnsupportedAst,

    pub fn deinit(self: *GeneratedSqlAst, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .dml => |*dml| dml.deinit(alloc),
            .read => |read| {
                read.deinit(alloc);
                alloc.destroy(read);
            },
            else => {},
        }
        self.* = undefined;
    }
};

pub const GeneratedSqlParseResult = struct {
    kind: GeneratedSqlStatementKind,
    statement: GeneratedSqlStatement,
    ast: ?GeneratedSqlAst = null,

    pub fn deinit(self: *GeneratedSqlParseResult, alloc: std.mem.Allocator) void {
        if (self.ast) |*ast| ast.deinit(alloc);
        self.* = undefined;
    }
};

pub const GeneratedSqlAstMode = enum {
    full,
    skip_read,
};

pub const GeneratedSqlDiagnostic = struct {
    state: u16,
    lookahead: u16,
    token_index: usize,
    source_start: usize,
    source_end: usize,
    expected: []const []const u8,
    actual: []const u8,
};

const DiagnosticSpan = struct {
    start: usize,
    end: usize,
    actual: []const u8,
};

pub const GeneratedSqlCorpusCase = struct {
    sql: []const u8,
    kind: GeneratedSqlStatementKind,
};

pub const first_family_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SET antfly.sync_level = 'write'", .kind = .session },
    .{ .sql = "SET search_path public", .kind = .session },
    .{ .sql = "SET search_path TO public", .kind = .session },
    .{ .sql = "SET LOCAL antfly.sync_level = 'propose'", .kind = .session },
    .{ .sql = "SET LOCAL search_path TO tenant_schema, public", .kind = .session },
    .{ .sql = "RESET search_path", .kind = .session },
    .{ .sql = "RESET ALL", .kind = .session },
    .{ .sql = "SHOW search_path", .kind = .session },
    .{ .sql = "SHOW ALL", .kind = .session },
    .{ .sql = "DISCARD ALL", .kind = .session },
    .{ .sql = "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY", .kind = .session },
    .{ .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", .kind = .transaction },
    .{ .sql = "START TRANSACTION", .kind = .transaction },
    .{ .sql = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ", .kind = .transaction },
    .{ .sql = "BEGIN", .kind = .transaction },
    .{ .sql = "BEGIN READ WRITE DEFERRABLE", .kind = .transaction },
    .{ .sql = "BEGIN WORK", .kind = .transaction },
    .{ .sql = "BEGIN TRANSACTION", .kind = .transaction },
    .{ .sql = "BEGIN TRANSACTION READ ONLY", .kind = .transaction },
    .{ .sql = "COMMIT", .kind = .transaction },
    .{ .sql = "COMMIT WORK", .kind = .transaction },
    .{ .sql = "COMMIT TRANSACTION", .kind = .transaction },
    .{ .sql = "END", .kind = .transaction },
    .{ .sql = "END WORK", .kind = .transaction },
    .{ .sql = "END TRANSACTION", .kind = .transaction },
    .{ .sql = "ROLLBACK", .kind = .transaction },
    .{ .sql = "ROLLBACK WORK", .kind = .transaction },
    .{ .sql = "ROLLBACK TRANSACTION", .kind = .transaction },
    .{ .sql = "SAVEPOINT before_retry", .kind = .transaction },
    .{ .sql = "RELEASE SAVEPOINT before_retry", .kind = .transaction },
    .{ .sql = "RELEASE before_retry", .kind = .transaction },
    .{ .sql = "ROLLBACK TO SAVEPOINT before_retry", .kind = .transaction },
    .{ .sql = "ROLLBACK TO before_retry", .kind = .transaction },
    .{ .sql = "PREPARE read_stmt AS SELECT id FROM usage_records", .kind = .prepared },
    .{ .sql = "PREPARE read_stmt(text) AS SELECT id FROM usage_records WHERE status = $1", .kind = .prepared },
    .{ .sql = "EXECUTE read_stmt()", .kind = .prepared },
    .{ .sql = "DEALLOCATE read_stmt", .kind = .prepared },
    .{ .sql = "DEALLOCATE PREPARE read_stmt", .kind = .prepared },
    .{ .sql = "DEALLOCATE ALL", .kind = .prepared },
    .{ .sql = "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records", .kind = .cursor },
    .{ .sql = "FETCH FROM usage_cursor", .kind = .cursor },
    .{ .sql = "CLOSE usage_cursor", .kind = .cursor },
    .{ .sql = "CLOSE ALL", .kind = .cursor },
};

pub const simple_ddl_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "CREATE DATABASE tenant_ops", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA analytics", .kind = .ddl },
    .{ .sql = "CREATE SCHEMA IF NOT EXISTS analytics", .kind = .ddl },
    .{ .sql = "CREATE TABLE usage_records (id text PRIMARY KEY, status text DEFAULT 'open')", .kind = .ddl },
    .{ .sql = "CREATE TABLE IF NOT EXISTS usage_records (id text PRIMARY KEY)", .kind = .ddl },
    .{ .sql = "CREATE TEMP TABLE usage_session_records (id uuid PRIMARY KEY, status text)", .kind = .ddl },
    .{ .sql = "CREATE UNLOGGED TABLE IF NOT EXISTS usage_ingest_records (id uuid PRIMARY KEY, payload jsonb)", .kind = .ddl },
    .{ .sql = "SELECT account_id, total INTO public.usage_archive FROM usage_records WHERE total > 10", .kind = .ddl },
    .{ .sql = "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records WITH NO DATA", .kind = .ddl },
    .{ .sql = "CREATE VIEW active_usage AS SELECT id, status FROM usage_records", .kind = .ddl },
    .{ .sql = "CREATE OR REPLACE VIEW IF NOT EXISTS active_usage AS SELECT id, status FROM usage_records", .kind = .ddl },
    .{ .sql = "CREATE MATERIALIZED VIEW usage_summary AS SELECT status, count(*) FROM usage_records GROUP BY status", .kind = .ddl },
    .{ .sql = "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)", .kind = .ddl },
    .{ .sql = "CREATE PUBLICATION usage_pub FOR TABLE usage_records", .kind = .ddl },
    .{ .sql = "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=example dbname=usage' PUBLICATION usage_pub", .kind = .ddl },
    .{ .sql = "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)", .kind = .ddl },
    .{ .sql = "CREATE FUNCTION audit_changes() RETURNS trigger LANGUAGE plpgsql", .kind = .ddl },
    .{ .sql = "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql", .kind = .ddl },
    .{ .sql = "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql", .kind = .ddl },
    .{ .sql = "CREATE ROLE app_writer", .kind = .ddl },
    .{ .sql = "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2')", .kind = .ddl },
    .{ .sql = "CREATE OPERATOR === (FUNCTION = text_eq, LEFTARG = text, RIGHTARG = text)", .kind = .ddl },
    .{ .sql = "CREATE AGGREGATE first_value_text(text) (SFUNC = first_sfunc, STYPE = text)", .kind = .ddl },
    .{ .sql = "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT", .kind = .ddl },
    .{ .sql = "ALTER TABLE usage_records ADD COLUMN status text", .kind = .ddl },
    .{ .sql = "ALTER TABLE IF EXISTS ONLY usage_records DROP COLUMN IF EXISTS status RESTRICT", .kind = .ddl },
    .{ .sql = "ALTER DATABASE tenant_ops SET timezone TO 'UTC'", .kind = .ddl },
    .{ .sql = "ALTER EXTENSION postgis UPDATE TO '3.5.0'", .kind = .ddl },
    .{ .sql = "ALTER VIEW active_usage RENAME TO active_usage_v2", .kind = .ddl },
    .{ .sql = "ALTER DOMAIN positive_amount SET NOT NULL", .kind = .ddl },
    .{ .sql = "ALTER PUBLICATION usage_pub ADD TABLE usage_records", .kind = .ddl },
    .{ .sql = "ALTER SUBSCRIPTION usage_sub DISABLE", .kind = .ddl },
    .{ .sql = "ALTER POLICY usage_policy ON usage_records RENAME TO usage_policy_v2", .kind = .ddl },
    .{ .sql = "ALTER ROLE app_writer IN DATABASE appdb SET app.tenant_id = current_setting('app.tenant_id')", .kind = .ddl },
    .{ .sql = "ALTER COLLATION case_insensitive RENAME TO ci_text", .kind = .ddl },
    .{ .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status)", .kind = .extension_index },
    .{ .sql = "CREATE INDEX IF NOT EXISTS usage_records_status_idx ON usage_records (status)", .kind = .extension_index },
    .{ .sql = "CREATE UNIQUE INDEX usage_records_status_active_idx ON usage_records (status) INCLUDE (tenant_id, amount) WHERE deleted_at IS NULL", .kind = .extension_index },
    .{ .sql = "CREATE EXTENSION vector", .kind = .extension_index },
    .{ .sql = "DROP TABLE usage_records", .kind = .ddl },
    .{ .sql = "DROP TABLE IF EXISTS usage_records", .kind = .ddl },
    .{ .sql = "DROP VIEW IF EXISTS active_usage CASCADE", .kind = .ddl },
    .{ .sql = "DROP MATERIALIZED VIEW IF EXISTS usage_summary CASCADE", .kind = .ddl },
    .{ .sql = "DROP DOMAIN IF EXISTS positive_amount CASCADE", .kind = .ddl },
    .{ .sql = "DROP PUBLICATION IF EXISTS usage_pub", .kind = .ddl },
    .{ .sql = "DROP SUBSCRIPTION IF EXISTS usage_sub", .kind = .ddl },
    .{ .sql = "DROP POLICY IF EXISTS usage_policy ON usage_records", .kind = .ddl },
    .{ .sql = "DROP FUNCTION IF EXISTS audit_changes(text) CASCADE", .kind = .ddl },
    .{ .sql = "DROP PROCEDURE rotate_usage()", .kind = .ddl },
    .{ .sql = "DROP ROLE IF EXISTS app_writer", .kind = .ddl },
    .{ .sql = "DROP COLLATION IF EXISTS ci_text", .kind = .ddl },
    .{ .sql = "DROP OPERATOR === (text, text)", .kind = .ddl },
    .{ .sql = "DROP AGGREGATE first_value_text(text)", .kind = .ddl },
    .{ .sql = "DROP CAST (jsonb AS text)", .kind = .ddl },
    .{ .sql = "REFRESH MATERIALIZED VIEW usage_summary", .kind = .ddl },
    .{ .sql = "DROP INDEX usage_records_status_idx", .kind = .extension_index },
    .{ .sql = "DROP EXTENSION vector", .kind = .extension_index },
    .{ .sql = "DROP SCHEMA analytics CASCADE", .kind = .ddl },
    .{ .sql = "DROP DATABASE tenant_ops", .kind = .ddl },
};

pub const simple_dml_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open')", .kind = .dml },
    .{ .sql = "INSERT INTO usage_records (id) SELECT id FROM incoming_usage", .kind = .dml },
    .{ .sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "DELETE FROM usage_records WHERE id = 'u1' RETURNING id", .kind = .dml },
    .{ .sql = "TRUNCATE usage_records", .kind = .dml },
    .{ .sql = "MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN UPDATE SET status = source_rows.status", .kind = .dml },
};

pub const simple_read_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", .kind = .read },
    .{ .sql = "SELECT status AS state, id FROM usage_records", .kind = .read },
    .{ .sql = "SELECT status state, id FROM usage_records", .kind = .read },
    .{ .sql = "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'", .kind = .read },
    .{ .sql = "SELECT CAST(id AS text) AS id_text, CAST(active AS bool) AS active_bool, CAST(created_at_ns AS timestamptz) AS created_at FROM usage_records WHERE id = $1", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'", .kind = .read },
    .{ .sql = "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb", .kind = .read },
    .{ .sql = "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = ANY($1::text[])", .kind = .read },
    .{ .sql = "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1", .kind = .read },
    .{ .sql = "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1", .kind = .read },
    .{ .sql = "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1", .kind = .read },
    .{ .sql = "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5", .kind = .read },
    .{ .sql = "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1", .kind = .read },
    .{ .sql = "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1", .kind = .read },
    .{ .sql = "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5", .kind = .read },
    .{ .sql = "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'", .kind = .read },
    .{ .sql = "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status LIKE SOME(ARRAY['op%', 'ready%'])", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE name ILIKE ALL(ARRAY['ada%', 'grace%'])", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY (SELECT pattern FROM active_patterns)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL (SELECT pattern FROM blocked_patterns)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records WHERE archived IS TRUE)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE id NOT IN (SELECT id FROM archived_records WHERE archived IS TRUE)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE NOT EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE metadata ? 'flags'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status ~ 'op.*'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status ~* 'op.*'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status !~ 'closed.*'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status !~* 'closed.*'", .kind = .read },
    .{ .sql = "SELECT first_name || ' ' || last_name FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status ISNULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) NOTNULL", .kind = .read },
    .{ .sql = "SELECT cluster, comment, grant, listen, lock, notify, revoke FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS TRUE", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE (status = 'open')", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score + bonus > 10", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE score * weight > 10", .kind = .read },
    .{ .sql = "SELECT -amount AS neg_amount, +bonus AS plus_bonus FROM usage_records WHERE amount > -10 ORDER BY -amount DESC", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'", .kind = .read },
    .{ .sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id", .kind = .read },
    .{ .sql = "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer", .kind = .read },
    .{ .sql = "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer", .kind = .read },
    .{ .sql = "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer", .kind = .read },
    .{ .sql = "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant", .kind = .read },
    .{ .sql = "SELECT DISTINCT status FROM usage_records ORDER BY status", .kind = .read },
    .{ .sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records FOR NO KEY UPDATE OF usage_records NOWAIT", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records FOR KEY SHARE SKIP LOCKED", .kind = .read },
    .{ .sql = "SELECT status FROM usage_records GROUP BY status HAVING status = 'open'", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts USING (account_id)", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id", .kind = .read },
    .{ .sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id", .kind = .read },
    .{ .sql = "SELECT id FROM LATERAL (SELECT id FROM usage_records) AS source_rows", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)", .kind = .read },
    .{ .sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)", .kind = .read },
    .{ .sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive", .kind = .read },
    .{ .sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
    .{ .sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
    .{ .sql = "WITH source_rows(source_id) AS NOT MATERIALIZED (SELECT id FROM usage_records) SELECT source_id FROM source_rows", .kind = .read },
    .{ .sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows", .kind = .read },
    .{ .sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows", .kind = .read },
};

pub const antfly_extension_read_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "SELECT * FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10)", .kind = .read },
    .{ .sql = "SELECT id, score FROM antfly.semantic_search(index => 'docs_embeddings', query => 'refund', limit => 10)", .kind = .read },
    .{ .sql = "SELECT id FROM antfly.vector_search(index => 'docs_vectors', vector => '[0.1,0.2]', limit => 10)", .kind = .read },
    .{ .sql = "SELECT id FROM antfly.graph_traverse(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', max_depth => 2)", .kind = .read },
    .{ .sql = "SELECT id FROM antfly.graph_neighbors(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', direction => 'out')", .kind = .read },
    .{ .sql = "SELECT id, score FROM antfly.graph_shortest_path(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', target => 'doc:z', max_depth => 4)", .kind = .read },
    .{ .sql = "SELECT id, score FROM antfly.graph_k_shortest_paths(table_name => 'docs', index => 'docs_edge_graph', result_ref => '$starts', target_result_ref => '$targets', k => 3, max_depth => 6)", .kind = .read },
    .{ .sql = "SELECT id, score FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm", .kind = .read },
    .{ .sql = "SELECT gm.id, ranked.score FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm JOIN antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5) AS ranked ON gm.id = ranked.id", .kind = .read },
    .{ .sql = "SELECT * FROM antfly.graph_metric_rerank(full_text_index => 'docs_body_fts', query => 'refund', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', weight => 1.5, base_weight => 0.25)", .kind = .read },
};

pub const simple_graph_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "CREATE GRAPH INDEX docs_edge_graph ON doc_edges", .kind = .graph },
    .{ .sql = "CREATE GRAPH INDEX docs_edge_graph_syntax ON doc_edges EDGE (source_doc -> target_doc) TYPE edge_type WEIGHT confidence WITH (edge_policy = 'all')", .kind = .graph },
    .{ .sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", .kind = .graph },
    .{ .sql = "ALTER GRAPH INDEX docs_edge_graph ADD METRIC pagerank_v1 USING pagerank WITH (damping = 0.85, max_iterations = 40)", .kind = .graph },
};

pub const unsupported_corpus = [_]GeneratedSqlCorpusCase{
    .{ .sql = "ALTER AGGREGATE first_value_text(text) OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER INDEX usage_status_idx RENAME TO usage_status_idx_v2", .kind = .unsupported },
    .{ .sql = "ALTER CONVERSION usage_conv RENAME TO usage_conv_v2", .kind = .unsupported },
    .{ .sql = "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly", .kind = .unsupported },
    .{ .sql = "ALTER EVENT TRIGGER usage_ddl_start DISABLE", .kind = .unsupported },
    .{ .sql = "ALTER FOREIGN TABLE foreign_usage_records RENAME TO foreign_usage_archive", .kind = .unsupported },
    .{ .sql = "ALTER FOREIGN DATA WRAPPER usage_fdw OPTIONS (ADD host 'localhost')", .kind = .unsupported },
    .{ .sql = "ALTER FUNCTION normalize_status(text) OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER LARGE OBJECT 12345 OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER LANGUAGE usage_lang OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER MATERIALIZED VIEW usage_summary RENAME TO usage_summary_v2", .kind = .unsupported },
    .{ .sql = "ALTER OPERATOR === (text, text) OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER OPERATOR CLASS usage_ops USING btree RENAME TO usage_ops_v2", .kind = .unsupported },
    .{ .sql = "ALTER OPERATOR FAMILY usage_family USING btree RENAME TO usage_family_v2", .kind = .unsupported },
    .{ .sql = "ALTER PROCEDURE refresh_usage_records() OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER ROUTINE normalize_status(text) OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER RULE usage_insert ON usage_records RENAME TO usage_insert_v2", .kind = .unsupported },
    .{ .sql = "ALTER SERVER usage_server VERSION '15'", .kind = .unsupported },
    .{ .sql = "ALTER SYSTEM SET work_mem = '64MB'", .kind = .unsupported },
    .{ .sql = "ALTER STATISTICS usage_stats SET STATISTICS 100", .kind = .unsupported },
    .{ .sql = "ALTER TEXT SEARCH CONFIGURATION usage_search RENAME TO usage_search_v2", .kind = .unsupported },
    .{ .sql = "ALTER TEXT SEARCH DICTIONARY usage_dict OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER TEXT SEARCH PARSER usage_parser RENAME TO usage_parser_v2", .kind = .unsupported },
    .{ .sql = "ALTER TEXT SEARCH TEMPLATE usage_template RENAME TO usage_template_v2", .kind = .unsupported },
    .{ .sql = "ALTER TRIGGER usage_audit ON usage_records RENAME TO usage_audit_v2", .kind = .unsupported },
    .{ .sql = "ALTER TRANSFORM FOR jsonb LANGUAGE plpgsql OWNER TO app_role", .kind = .unsupported },
    .{ .sql = "ALTER USER MAPPING FOR usage_user SERVER usage_server OPTIONS (SET user 'remote')", .kind = .unsupported },
    .{ .sql = "ANALYZE", .kind = .unsupported },
    .{ .sql = "CALL refresh_usage_records()", .kind = .unsupported },
    .{ .sql = "CHECKPOINT", .kind = .unsupported },
    .{ .sql = "CLUSTER usage_records USING usage_status_idx", .kind = .unsupported },
    .{ .sql = "COMMENT ON TABLE usage_records IS 'billing rows'", .kind = .unsupported },
    .{ .sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv)", .kind = .unsupported },
    .{ .sql = "DISCARD TEMP", .kind = .unsupported },
    .{ .sql = "DISCARD PLANS", .kind = .unsupported },
    .{ .sql = "CREATE ACCESS METHOD usage_am TYPE INDEX HANDLER usage_handler", .kind = .unsupported },
    .{ .sql = "CREATE CONVERSION usage_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1", .kind = .unsupported },
    .{ .sql = "CREATE DATABASE tenant_ops WITH OWNER app", .kind = .unsupported },
    .{ .sql = "CREATE EVENT TRIGGER usage_ddl_start ON ddl_command_start EXECUTE FUNCTION audit_ddl()", .kind = .unsupported },
    .{ .sql = "CREATE FOREIGN TABLE foreign_usage_records (id text) SERVER usage_fdw", .kind = .unsupported },
    .{ .sql = "CREATE FOREIGN DATA WRAPPER usage_fdw HANDLER usage_fdw_handler", .kind = .unsupported },
    .{ .sql = "CREATE LANGUAGE usage_lang", .kind = .unsupported },
    .{ .sql = "CREATE OPERATOR CLASS usage_ops DEFAULT FOR TYPE text USING btree AS OPERATOR 1 < (text, text)", .kind = .unsupported },
    .{ .sql = "CREATE OPERATOR FAMILY usage_family USING btree", .kind = .unsupported },
    .{ .sql = "CREATE RULE usage_insert AS ON INSERT TO usage_records DO ALSO NOTIFY usage_events", .kind = .unsupported },
    .{ .sql = "CREATE SCHEMA analytics AUTHORIZATION app_user", .kind = .unsupported },
    .{ .sql = "CREATE SERVER usage_server FOREIGN DATA WRAPPER postgres_fdw", .kind = .unsupported },
    .{ .sql = "CREATE STATISTICS usage_stats ON tenant_id, status FROM usage_records", .kind = .unsupported },
    .{ .sql = "CREATE TRIGGER usage_audit BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_usage()", .kind = .unsupported },
    .{ .sql = "CREATE TEXT SEARCH CONFIGURATION usage_search (COPY = pg_catalog.english)", .kind = .unsupported },
    .{ .sql = "CREATE TEXT SEARCH DICTIONARY usage_dict (TEMPLATE = simple)", .kind = .unsupported },
    .{ .sql = "CREATE TEXT SEARCH PARSER usage_parser (START = prsd_start)", .kind = .unsupported },
    .{ .sql = "CREATE TEXT SEARCH TEMPLATE usage_template (LEXIZE = dsimple_lexize)", .kind = .unsupported },
    .{ .sql = "CREATE TRANSFORM FOR jsonb LANGUAGE plpgsql FROM SQL WITH FUNCTION jsonb_to_plpgsql(internal)", .kind = .unsupported },
    .{ .sql = "CREATE USER MAPPING FOR usage_user SERVER usage_server OPTIONS (user 'remote')", .kind = .unsupported },
    .{ .sql = "DO 'BEGIN NULL; END'", .kind = .unsupported },
    .{ .sql = "DROP FOREIGN TABLE IF EXISTS foreign_usage_records", .kind = .unsupported },
    .{ .sql = "DROP FOREIGN DATA WRAPPER IF EXISTS usage_fdw CASCADE", .kind = .unsupported },
    .{ .sql = "IMPORT FOREIGN SCHEMA public FROM SERVER usage_server INTO local_schema", .kind = .unsupported },
    .{ .sql = "DROP ACCESS METHOD IF EXISTS usage_am", .kind = .unsupported },
    .{ .sql = "DROP CONVERSION IF EXISTS usage_conv", .kind = .unsupported },
    .{ .sql = "DROP EVENT TRIGGER IF EXISTS usage_ddl_start", .kind = .unsupported },
    .{ .sql = "DROP COLLATION case_insensitive, accent_insensitive", .kind = .unsupported },
    .{ .sql = "DROP DOMAIN positive_amount, nonempty_text CASCADE", .kind = .unsupported },
    .{ .sql = "DROP EXTENSION vector, postgis", .kind = .unsupported },
    .{ .sql = "DROP INDEX usage_status_idx, usage_tenant_idx CASCADE", .kind = .unsupported },
    .{ .sql = "DROP LANGUAGE IF EXISTS usage_lang CASCADE", .kind = .unsupported },
    .{ .sql = "DROP MATERIALIZED VIEW usage_summary, old_usage_summary CASCADE", .kind = .unsupported },
    .{ .sql = "DROP OWNED BY usage_role CASCADE", .kind = .unsupported },
    .{ .sql = "DROP OPERATOR CLASS IF EXISTS usage_ops USING btree", .kind = .unsupported },
    .{ .sql = "DROP OPERATOR FAMILY IF EXISTS usage_family USING btree", .kind = .unsupported },
    .{ .sql = "DROP PUBLICATION usage_pub, old_usage_pub", .kind = .unsupported },
    .{ .sql = "DROP ROUTINE IF EXISTS normalize_status(text) CASCADE", .kind = .unsupported },
    .{ .sql = "DROP ROLE app_writer, app_reader", .kind = .unsupported },
    .{ .sql = "DROP SCHEMA analytics, reporting", .kind = .unsupported },
    .{ .sql = "DROP SEQUENCE order_id_seq, old_order_id_seq CASCADE", .kind = .unsupported },
    .{ .sql = "DROP STATISTICS IF EXISTS usage_stats", .kind = .unsupported },
    .{ .sql = "DROP TABLE usage_records, archived_usage_records", .kind = .unsupported },
    .{ .sql = "DROP TEXT SEARCH CONFIGURATION IF EXISTS usage_search", .kind = .unsupported },
    .{ .sql = "DROP TEXT SEARCH DICTIONARY IF EXISTS usage_dict", .kind = .unsupported },
    .{ .sql = "DROP TEXT SEARCH PARSER IF EXISTS usage_parser", .kind = .unsupported },
    .{ .sql = "DROP TEXT SEARCH TEMPLATE IF EXISTS usage_template", .kind = .unsupported },
    .{ .sql = "DROP TRANSFORM FOR jsonb LANGUAGE plpgsql", .kind = .unsupported },
    .{ .sql = "DROP TYPE usage_status, archived_status CASCADE", .kind = .unsupported },
    .{ .sql = "DROP VIEW active_usage, archived_usage", .kind = .unsupported },
    .{ .sql = "EXPLAIN", .kind = .unsupported },
    .{ .sql = "EXPLAIN SELECT id FROM usage_records", .kind = .unsupported },
    .{ .sql = "EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')", .kind = .unsupported },
    .{ .sql = "EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON, BUFFERS, TIMING OFF, SUMMARY OFF, SETTINGS ON, WAL) SELECT id FROM usage_records", .kind = .unsupported },
    .{ .sql = "EXPLAIN (FORMAT YAML) SELECT 1", .kind = .unsupported },
    .{ .sql = "GRANT SELECT ON TABLE usage_records TO readonly", .kind = .unsupported },
    .{ .sql = "LISTEN usage_events", .kind = .unsupported },
    .{ .sql = "LOAD 'auto_explain'", .kind = .unsupported },
    .{ .sql = "LOCK TABLE usage_records IN SHARE MODE", .kind = .unsupported },
    .{ .sql = "MOVE FROM usage_cursor", .kind = .unsupported },
    .{ .sql = "NOTIFY usage_events, 'changed'", .kind = .unsupported },
    .{ .sql = "VACUUM (FULL, VERBOSE, ANALYZE) public.usage_records", .kind = .unsupported },
    .{ .sql = "REINDEX INDEX CONCURRENTLY public.usage_status_idx", .kind = .unsupported },
    .{ .sql = "REASSIGN OWNED BY old_role TO new_role", .kind = .unsupported },
    .{ .sql = "REVOKE SELECT ON TABLE usage_records FROM readonly", .kind = .unsupported },
    .{ .sql = "SET ROLE app_user", .kind = .unsupported },
    .{ .sql = "SET ROLE DEFAULT", .kind = .unsupported },
    .{ .sql = "RESET ROLE", .kind = .unsupported },
    .{ .sql = "SECURITY LABEL ON TABLE usage_records IS 'internal'", .kind = .unsupported },
    .{ .sql = "DROP RULE IF EXISTS usage_insert ON usage_records", .kind = .unsupported },
    .{ .sql = "DROP SERVER IF EXISTS usage_server CASCADE", .kind = .unsupported },
    .{ .sql = "DROP TRIGGER IF EXISTS usage_audit ON usage_records", .kind = .unsupported },
    .{ .sql = "DROP USER MAPPING IF EXISTS FOR usage_user SERVER usage_server", .kind = .unsupported },
    .{ .sql = "UNLISTEN *", .kind = .unsupported },
};

pub fn parseSqlAlloc(alloc: std.mem.Allocator, sql: []const u8) !GeneratedSqlParseResult {
    var tokens = try lexer.tokenizeAlloc(alloc, sql);
    defer lexer.freeTokens(alloc, &tokens);
    return try parseTokensAlloc(alloc, tokens.items);
}

pub fn parseTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !GeneratedSqlParseResult {
    return parseTokensAllocWithAstMode(alloc, tokens, .full);
}

pub fn parseTokensAllocWithAstMode(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    ast_mode: GeneratedSqlAstMode,
) !GeneratedSqlParseResult {
    const statement = classifyStatement(tokens);
    var token_id_buffer: [generated_token_id_stack_capacity]u16 = undefined;
    if (tokenIdsIntoBuffer(tokens, &token_id_buffer)) |token_ids| {
        try parseGeneratedTokenIds(alloc, token_ids);
    } else |err| switch (err) {
        error.NoSpaceLeft => {
            const token_ids = try tokenIdsAlloc(alloc, tokens);
            defer alloc.free(token_ids);
            try parseGeneratedTokenIds(alloc, token_ids);
        },
        else => return err,
    }
    return try buildGeneratedParseResult(alloc, tokens, statement, ast_mode);
}

fn parseGeneratedTokenIds(alloc: std.mem.Allocator, token_ids: []const u16) !void {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        return try generated.parseWithStackBuffer(token_ids, &stack_buffer);
    }
    return try generated.parse(alloc, token_ids);
}

fn buildGeneratedParseResult(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    statement: GeneratedSqlStatement,
    ast_mode: GeneratedSqlAstMode,
) !GeneratedSqlParseResult {
    return .{
        .kind = std.meta.activeTag(statement),
        .statement = statement,
        .ast = if (ast_mode == .skip_read and statement == .read) null else try buildGeneratedAst(alloc, tokens, statement),
    };
}

fn generatedTokenEnd(tokens: []const token_mod.Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) end -= 1;
    return end;
}

pub fn parseFirstFamilyTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    if (!isFirstFamilyTokens(tokens)) return null;
    return try parseTokensAlloc(alloc, tokens);
}

pub fn parseGeneratedGateTokensAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    return try parseGeneratedGateTokensAllocWithAstMode(alloc, tokens, .full);
}

pub fn parseGeneratedGateTokensAllocWithAstMode(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    ast_mode: GeneratedSqlAstMode,
) !?GeneratedSqlParseResult {
    return parseGeneratedGateTokensStrictAllocWithAstMode(alloc, tokens, ast_mode) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => return null,
        else => return err,
    };
}

pub fn parseGeneratedGateTokensStrictAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlParseResult {
    return try parseGeneratedGateTokensStrictAllocWithAstMode(alloc, tokens, .full);
}

pub fn parseGeneratedGateTokensStrictAllocWithAstMode(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    ast_mode: GeneratedSqlAstMode,
) !?GeneratedSqlParseResult {
    const kind = classifyTokens(tokens);
    if (kind == .other) return null;
    return try parseTokensAllocWithAstMode(alloc, tokens, ast_mode);
}

pub fn isFirstFamilyTokens(tokens: []const token_mod.Token) bool {
    const kind = classifyTokens(tokens);
    return kind == .session or kind == .transaction or kind == .prepared;
}

pub fn isGeneratedGateTokens(tokens: []const token_mod.Token) bool {
    return classifyTokens(tokens) != .other;
}

pub fn diagnosticAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !?GeneratedSqlDiagnostic {
    var diagnostic_tokens = try diagnosticTokenIdsAlloc(alloc, tokens);
    defer diagnostic_tokens.deinit(alloc);

    const info = try parseGeneratedTokenIdsError(alloc, diagnostic_tokens.token_ids) orelse return null;
    const source_token_index = diagnostic_tokens.sourceTokenIndex(info.token_index);
    const expected_count = generated.expectedTerminalCountForState(info.state);
    const expected = try alloc.alloc([]const u8, expected_count);
    for (expected, 0..) |*name, idx| {
        name.* = generated.expectedTerminalNameForState(info.state, idx) orelse return error.UnsupportedSqlShape;
    }
    const span: DiagnosticSpan = if (source_token_index < tokens.len)
        .{ .start = tokens[source_token_index].source_start, .end = tokens[source_token_index].source_end, .actual = tokens[source_token_index].text }
    else
        .{ .start = diagnostic_tokens.endSourceOffset(tokens), .end = diagnostic_tokens.endSourceOffset(tokens), .actual = "$end" };
    return .{
        .state = info.state,
        .lookahead = info.lookahead,
        .token_index = source_token_index,
        .source_start = span.start,
        .source_end = span.end,
        .expected = expected,
        .actual = span.actual,
    };
}

const DiagnosticTokenIds = struct {
    token_ids: []u16,
    source_indexes: []usize,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.token_ids);
        alloc.free(self.source_indexes);
        self.* = .{ .token_ids = &.{}, .source_indexes = &.{} };
    }

    fn sourceTokenIndex(self: @This(), generated_token_index: usize) usize {
        if (generated_token_index < self.source_indexes.len) return self.source_indexes[generated_token_index];
        return if (self.source_indexes.len == 0) 0 else self.source_indexes[self.source_indexes.len - 1] + 1;
    }

    fn endSourceOffset(self: @This(), tokens: []const token_mod.Token) usize {
        const end_token = self.sourceTokenIndex(self.source_indexes.len);
        if (end_token == 0) return 0;
        if (tokens.len == 0) return 0;
        const previous = @min(end_token - 1, tokens.len - 1);
        return tokens[previous].source_end;
    }
};

fn diagnosticTokenIdsAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) !DiagnosticTokenIds {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    var source_indexes: std.ArrayListUnmanaged(usize) = .empty;
    errdefer ids.deinit(alloc);
    errdefer source_indexes.deinit(alloc);

    var sink = TokenIdSink{ .list = .{ .items = &ids, .alloc = alloc } };
    for (tokens, 0..) |tok, index| {
        if (tok.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        const prev = if (index > 0) tokens[index - 1] else null;
        const next = if (index + 1 < tokens.len) tokens[index + 1] else null;
        const before = ids.items.len;
        try appendTokenIds(&sink, tokens, index, tok, prev, next);
        const added = ids.items.len - before;
        try source_indexes.ensureUnusedCapacity(alloc, added);
        for (0..added) |_| source_indexes.appendAssumeCapacity(index);
    }

    const token_ids = try ids.toOwnedSlice(alloc);
    errdefer alloc.free(token_ids);
    return .{
        .token_ids = token_ids,
        .source_indexes = try source_indexes.toOwnedSlice(alloc),
    };
}

fn parseGeneratedTokenIdsError(alloc: std.mem.Allocator, token_ids: []const u16) !?generated.ParseErrorInfo {
    if (token_ids.len + 1 <= generated_parse_stack_capacity) {
        var stack_buffer: [generated_parse_stack_capacity]u16 = undefined;
        return try generated.parseErrorWithStackBuffer(token_ids, &stack_buffer);
    }
    return try generated.parseError(alloc, token_ids);
}

pub fn tokenIdsAlloc(alloc: std.mem.Allocator, tokens: []const token_mod.Token) ![]u16 {
    var ids: std.ArrayListUnmanaged(u16) = .empty;
    errdefer ids.deinit(alloc);
    var sink = TokenIdSink{ .list = .{ .items = &ids, .alloc = alloc } };
    for (tokens, 0..) |tok, index| {
        if (tok.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        const prev = if (index > 0) tokens[index - 1] else null;
        const next = if (index + 1 < tokens.len) tokens[index + 1] else null;
        try appendTokenIds(&sink, tokens, index, tok, prev, next);
    }
    return try ids.toOwnedSlice(alloc);
}

fn tokenIdsIntoBuffer(tokens: []const token_mod.Token, buffer: []u16) ![]const u16 {
    var sink = TokenIdSink{ .buffer = .{ .items = buffer } };
    for (tokens, 0..) |tok, index| {
        if (tok.kind == .semicolon and trailingSemicolonOnly(tokens, index)) break;
        const prev = if (index > 0) tokens[index - 1] else null;
        const next = if (index + 1 < tokens.len) tokens[index + 1] else null;
        try appendTokenIds(&sink, tokens, index, tok, prev, next);
    }
    return sink.buffer.items[0..sink.buffer.len];
}

const TokenIdSink = union(enum) {
    list: struct {
        items: *std.ArrayListUnmanaged(u16),
        alloc: std.mem.Allocator,
    },
    buffer: struct {
        items: []u16,
        len: usize = 0,
    },

    fn append(self: *TokenIdSink, id: u16) !void {
        switch (self.*) {
            .list => |list| try list.items.append(list.alloc, id),
            .buffer => |*buffer| {
                if (buffer.len == buffer.items.len) return error.NoSpaceLeft;
                buffer.items[buffer.len] = id;
                buffer.len += 1;
            },
        }
    }

    fn appendToken(self: *TokenIdSink, token: generated.Token) !void {
        try self.append(generated.tokenId(token));
    }
};

fn appendTokenIds(
    ids: *TokenIdSink,
    tokens: []const token_mod.Token,
    index: usize,
    tok: token_mod.Token,
    prev: ?token_mod.Token,
    next: ?token_mod.Token,
) !void {
    switch (tok.kind) {
        .identifier => {
            if (try contextualKeywordSymbolId(tokens, index, tok, prev)) |id| {
                try ids.append(id);
                return;
            }
            if (generatedParserTreatsKeywordAsIdentifier(tok, prev, next)) {
                try appendIdentifierIds(ids, tok.text, false);
                return;
            }
            if (try keywordSymbolId(tok)) |id| {
                try ids.append(id);
                return;
            }
            const allow_trailing_dot = next != null and next.?.kind == .star;
            try appendIdentifierIds(ids, tok.text, allow_trailing_dot);
        },
        .string => try ids.appendToken(.STRING),
        .number => try ids.appendToken(.NUMBER),
        .placeholder => try ids.appendToken(.PLACEHOLDER),
        .comma => try ids.appendToken(.COMMA),
        .star => try ids.appendToken(.STAR),
        .eq => try ids.appendToken(.EQ),
        .neq => try ids.appendToken(.NEQ),
        .gt => try ids.appendToken(.GT),
        .gte => try ids.appendToken(.GTE),
        .lt => try ids.appendToken(.LT),
        .lte => try ids.appendToken(.LTE),
        .plus => try ids.appendToken(.PLUS),
        .minus => try ids.appendToken(.MINUS),
        .slash => try ids.appendToken(.SLASH),
        .percent => try ids.appendToken(.PERCENT),
        .pipe_concat => try ids.appendToken(.PIPE_CONCAT),
        .at_contains => try ids.appendToken(.AT_CONTAINS),
        .range_overlap => try ids.appendToken(.RANGE_OVERLAP),
        .question => try ids.appendToken(.QUESTION),
        .question_any => try ids.appendToken(.QUESTION_ANY),
        .question_all => try ids.appendToken(.QUESTION_ALL),
        .regex_match => try ids.appendToken(.REGEX_MATCH),
        .regex_imatch => try ids.appendToken(.REGEX_IMATCH),
        .regex_not_match => try ids.appendToken(.REGEX_NOT_MATCH),
        .regex_not_imatch => try ids.appendToken(.REGEX_NOT_IMATCH),
        .lparen => try ids.appendToken(.LPAREN),
        .rparen => try ids.appendToken(.RPAREN),
        .lbracket => try ids.appendToken(.LBRACKET),
        .rbracket => try ids.appendToken(.RBRACKET),
        .arrow_json => try ids.appendToken(.ARROW_JSON),
        .arrow_text => try ids.appendToken(.ARROW_TEXT),
        .path_arrow_json => try ids.appendToken(.PATH_ARROW_JSON),
        .path_arrow_text => try ids.appendToken(.PATH_ARROW_TEXT),
        .semicolon => try ids.appendToken(.SEMICOLON),
    }
}

fn contextualKeywordSymbolId(tokens: []const token_mod.Token, index: usize, tok: token_mod.Token, prev: ?token_mod.Token) !?u16 {
    _ = prev;
    if (generatedRoutineKeywordContext(tokens, index) and tok.matchesKeyword("routine")) return generated.tokenId(.ROUTINE);
    if (generatedSessionKeywordContext(tokens, index) and tok.matchesKeyword("local")) return generated.tokenId(.LOCAL);
    if (!generatedTransactionControlContext(tokens, index)) return null;
    if (tok.matchesKeyword("characteristics")) return generated.tokenId(.CHARACTERISTICS);
    if (tok.matchesKeyword("committed")) return generated.tokenId(.COMMITTED);
    if (tok.matchesKeyword("deferrable")) return generated.tokenId(.DEFERRABLE);
    if (tok.matchesKeyword("isolation")) return generated.tokenId(.ISOLATION);
    if (tok.matchesKeyword("level")) return generated.tokenId(.LEVEL);
    if (tok.matchesKeyword("read")) return generated.tokenId(.READ);
    if (tok.matchesKeywordTag(.release)) return generated.tokenId(.RELEASE);
    if (tok.matchesKeyword("repeatable")) return generated.tokenId(.REPEATABLE);
    if (tok.matchesKeywordTag(.as)) return generated.tokenId(.AS);
    if (tok.matchesKeywordTag(.savepoint)) return generated.tokenId(.SAVEPOINT);
    if (tok.matchesKeyword("serializable")) return generated.tokenId(.SERIALIZABLE);
    if (tok.matchesKeyword("session")) return generated.tokenId(.SESSION);
    if (tok.matchesKeyword("start")) return generated.tokenId(.START);
    if (tok.matchesKeywordTag(.to)) return generated.tokenId(.TO);
    if (tok.matchesKeyword("transaction")) return generated.tokenId(.TRANSACTION);
    if (tok.matchesKeyword("uncommitted")) return generated.tokenId(.UNCOMMITTED);
    if (tok.matchesKeyword("work")) return generated.tokenId(.WORK);
    if (tok.matchesKeyword("write")) return generated.tokenId(.WRITE);
    return null;
}

fn generatedSessionKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    if (index >= tokens.len or index != 1 or tokens.len == 0) return false;
    return tokens[0].matchesKeywordTag(.set);
}

fn generatedTransactionControlContext(tokens: []const token_mod.Token, index: usize) bool {
    if (index >= tokens.len or tokens.len == 0) return false;
    const first = tokens[0];
    if (first.matchesKeyword("start")) return true;
    if (first.matchesKeywordTag(.begin)) return true;
    if (first.matchesKeywordTag(.savepoint)) return true;
    if (first.matchesKeywordTag(.release)) return true;
    if (first.matchesKeywordTag(.commit)) return index <= 1;
    if (first.matchesKeywordTag(.end)) return index <= 1;
    if (first.matchesKeywordTag(.rollback)) return index <= 3;
    if (!first.matchesKeywordTag(.set)) return false;
    if (tokens.len > 1 and tokens[1].matchesKeyword("transaction")) return true;
    return tokens.len > 4 and
        tokens[1].matchesKeyword("session") and
        tokens[2].matchesKeyword("characteristics") and
        tokens[3].matchesKeywordTag(.as) and
        tokens[4].matchesKeyword("transaction");
}

fn generatedRoutineKeywordContext(tokens: []const token_mod.Token, index: usize) bool {
    if (index >= tokens.len or tokens.len == 0) return false;
    if (index != 1) return false;
    return tokens[0].matchesKeywordTag(.drop);
}

fn generatedParserTreatsKeywordAsIdentifier(tok: token_mod.Token, prev: ?token_mod.Token, next: ?token_mod.Token) bool {
    if (tok.matchesKeywordTag(.offset)) return next != null and next.?.matchesKeywordTag(.limit);
    if (tok.matchesKeywordTag(.fetch)) return prev != null and prev.?.matchesKeywordTag(.as);
    return false;
}

fn appendIdentifierIds(ids: *TokenIdSink, text: []const u8, allow_trailing_dot: bool) !void {
    if (text.len == 0) return error.UnsupportedSqlShape;
    const has_trailing_dot = text[text.len - 1] == '.';
    if (has_trailing_dot and !allow_trailing_dot) return error.UnsupportedSqlShape;
    const body = if (has_trailing_dot) text[0 .. text.len - 1] else text;
    if (body.len == 0) return error.UnsupportedSqlShape;

    var parts = std.mem.splitScalar(u8, body, '.');
    var emitted = false;
    while (parts.next()) |part| {
        if (part.len == 0) return error.UnsupportedSqlShape;
        if (emitted) try ids.appendToken(.DOT);
        try ids.appendToken(.IDENT);
        emitted = true;
    }
    if (has_trailing_dot) try ids.appendToken(.DOT);
}

fn keywordSymbolId(tok: token_mod.Token) !?u16 {
    if (tok.keyword == null) return null;
    if (tok.text.len > 128) return error.UnsupportedSqlShape;
    var name_buffer: [128]u8 = undefined;
    const name = uppercaseKeyword(&name_buffer, tok.text);
    return generated.tokenIdByName(name);
}

fn uppercaseKeyword(buffer: []u8, text: []const u8) []const u8 {
    std.debug.assert(text.len <= buffer.len);
    const out = buffer[0..text.len];
    for (text, 0..) |ch, idx| {
        out[idx] = switch (ch) {
            'a'...'z' => ch - 'a' + 'A',
            else => ch,
        };
    }
    return out;
}

fn trailingSemicolonOnly(tokens: []const token_mod.Token, index: usize) bool {
    if (tokens[index].kind != .semicolon) return false;
    for (tokens[index + 1 ..]) |tok| {
        if (tok.kind != .semicolon) return false;
    }
    return true;
}

fn classifyTokens(tokens: []const token_mod.Token) GeneratedSqlStatementKind {
    return std.meta.activeTag(classifyStatement(tokens));
}

fn classifyStatement(tokens: []const token_mod.Token) GeneratedSqlStatement {
    if (tokens.len == 0) return .other;
    const first = tokens[0];
    if (first.matchesKeywordTag(.set) and tokens.len > 1 and tokens[1].matchesKeyword("transaction")) return .{ .transaction = .set_transaction };
    if (first.matchesKeywordTag(.set) and tokens.len > 1 and tokens[1].matchesKeywordTag(.role)) return .{ .unsupported = .role_session_control };
    if (first.matchesKeywordTag(.set)) return .{ .session = .set };
    if (first.matchesKeywordTag(.reset) and tokens.len > 1 and tokens[1].matchesKeywordTag(.role)) return .{ .unsupported = .role_session_control };
    if (first.matchesKeywordTag(.reset)) return .{ .session = .reset };
    if (first.matchesKeywordTag(.show)) return .{ .session = .show };
    if (first.matchesKeywordTag(.discard)) {
        if (tokens.len > 1 and tokens[1].matchesKeywordTag(.all)) return .{ .session = .discard_all };
        return .{ .unsupported = .discard };
    }
    if (first.matchesKeyword("start")) return .{ .transaction = .start_transaction };
    if (first.matchesKeywordTag(.begin)) return .{ .transaction = .begin };
    if (first.matchesKeywordTag(.commit)) return .{ .transaction = .commit };
    if (first.matchesKeywordTag(.end)) return .{ .transaction = .commit };
    if (first.matchesKeywordTag(.rollback)) {
        if (tokens.len > 1 and tokens[1].matchesKeywordTag(.to)) return .{ .transaction = .rollback_to_savepoint };
        return .{ .transaction = .rollback };
    }
    if (first.matchesKeywordTag(.savepoint)) return .{ .transaction = .savepoint };
    if (first.matchesKeywordTag(.release)) return .{ .transaction = .release_savepoint };
    if (first.matchesKeywordTag(.prepare)) return .{ .prepared = .prepare };
    if (first.matchesKeywordTag(.execute)) return .{ .prepared = .execute };
    if (first.matchesKeywordTag(.deallocate)) return .{ .prepared = .deallocate };
    if (first.matchesKeywordTag(.close)) return .{ .cursor = .close };
    if (first.matchesKeywordTag(.declare)) return .{ .cursor = .declare };
    if (first.matchesKeywordTag(.fetch)) return .{ .cursor = .fetch };
    if (first.matchesKeywordTag(.create) and tokens.len > 1) {
        if (generatedCreateTableAsTargetRange(tokens, 0, statementTokenEnd(tokens)) != null) return .{ .ddl = .relation_population };
        if (generatedCreateTableTargetRange(tokens, 0, statementTokenEnd(tokens)) != null) return .{ .ddl = .create_table };
        const second = tokens[1];
        if (second.matchesKeywordTag(.@"or") and tokens.len > 3 and tokens[2].matchesKeywordTag(.replace) and tokens[3].matchesKeywordTag(.view)) return .{ .ddl = .create_view };
        if (second.matchesKeywordTag(.@"or") and tokens.len > 3 and tokens[2].matchesKeywordTag(.replace) and tokens[3].matchesKeywordTag(.function)) return .{ .ddl = .create_function };
        if (second.matchesKeywordTag(.@"or") and tokens.len > 3 and tokens[2].matchesKeywordTag(.replace) and tokens[3].matchesKeywordTag(.procedure)) return .{ .ddl = .create_procedure };
        if (second.matchesKeywordTag(.database)) {
            if (generatedCreateCatalogHeadHasUnsupportedTail(tokens, 2)) return .{ .unsupported = .create_database_options };
            return .{ .ddl = .create_database };
        }
        if (second.matchesKeywordTag(.schema)) {
            if (generatedCreateCatalogHeadHasUnsupportedTail(tokens, 2)) return .{ .unsupported = .create_schema_options };
            return .{ .ddl = .create_schema };
        }
        if (second.matchesKeywordTag(.table)) return .{ .ddl = .create_table };
        if (second.matchesKeywordTag(.view)) return .{ .ddl = .create_view };
        if (second.matchesKeywordTag(.materialized) and tokens.len > 2 and tokens[2].matchesKeywordTag(.view)) return .{ .ddl = .create_materialized_view };
        if (second.matchesKeywordTag(.domain)) return .{ .ddl = .create_domain };
        if (second.matchesKeywordTag(.sequence)) return .{ .ddl = .create_sequence };
        if (second.matchesKeywordTag(.type)) return .{ .ddl = .create_enum_type };
        if (second.matchesKeywordTag(.tablespace)) return .{ .ddl = .create_tablespace };
        if (second.matchesKeywordTag(.publication)) return .{ .ddl = .create_publication };
        if (second.matchesKeywordTag(.subscription)) return .{ .ddl = .create_subscription };
        if (second.matchesKeywordTag(.policy)) return .{ .ddl = .create_policy };
        if (second.matchesKeywordTag(.function)) return .{ .ddl = .create_function };
        if (second.matchesKeywordTag(.procedure)) return .{ .ddl = .create_procedure };
        if (second.matchesKeyword("user") and tokens.len > 2 and tokens[2].matchesKeyword("mapping")) return .{ .unsupported = .create_user_mapping };
        if (isGeneratedRoleAliasKeyword(second)) return .{ .ddl = .create_role };
        if (second.matchesKeywordTag(.collation)) return .{ .ddl = .create_collation };
        if (second.matchesKeywordTag(.operator)) {
            if (tokens.len > 2 and tokens[2].matchesKeyword("class")) return .{ .unsupported = .create_operator_class };
            if (tokens.len > 2 and tokens[2].matchesKeyword("family")) return .{ .unsupported = .create_operator_family };
            return .{ .ddl = .create_operator };
        }
        if (second.matchesKeywordTag(.aggregate)) return .{ .ddl = .create_aggregate };
        if (second.matchesKeywordTag(.cast)) return .{ .ddl = .create_cast };
        if (second.matchesKeywordTag(.index)) return .{ .extension_index = .create_index };
        if (second.matchesKeywordTag(.unique) and tokens.len > 2 and tokens[2].matchesKeywordTag(.index)) return .{ .extension_index = .create_index };
        if (second.matchesKeywordTag(.access) and tokens.len > 2 and tokens[2].matchesKeywordTag(.method)) return .{ .unsupported = .create_access_method };
        if (second.matchesKeyword("conversion")) return .{ .unsupported = .create_conversion };
        if (second.matchesKeyword("event") and tokens.len > 2 and tokens[2].matchesKeywordTag(.trigger)) return .{ .unsupported = .create_event_trigger };
        if (second.matchesKeywordTag(.foreign) and tokens.len > 2 and tokens[2].matchesKeywordTag(.table)) return .{ .unsupported = .create_foreign_table };
        if (second.matchesKeywordTag(.foreign) and tokens.len > 3 and tokens[2].matchesKeywordTag(.data) and tokens[3].matchesKeyword("wrapper")) return .{ .unsupported = .create_foreign_data_wrapper };
        if (second.matchesKeyword("language")) return .{ .unsupported = .create_language };
        if (second.matchesKeywordTag(.graph) and tokens.len > 2) {
            if (tokens[2].matchesKeywordTag(.index)) return .{ .graph = .create_index };
            if (tokens[2].matchesKeywordTag(.metric)) return .{ .graph = .create_metric };
        }
        if (second.matchesKeywordTag(.rule)) return .{ .unsupported = .create_rule };
        if (second.matchesKeywordTag(.server)) return .{ .unsupported = .create_server };
        if (second.matchesKeyword("statistics")) return .{ .unsupported = .create_statistics };
        if (second.matchesKeywordTag(.text) and tokens.len > 3 and tokens[2].matchesKeyword("search")) {
            const text_search_kind = tokens[3];
            if (text_search_kind.matchesKeyword("configuration")) return .{ .unsupported = .create_text_search_configuration };
            if (text_search_kind.matchesKeyword("dictionary")) return .{ .unsupported = .create_text_search_dictionary };
            if (text_search_kind.matchesKeyword("parser")) return .{ .unsupported = .create_text_search_parser };
            if (text_search_kind.matchesKeyword("template")) return .{ .unsupported = .create_text_search_template };
        }
        if (second.matchesKeyword("transform")) return .{ .unsupported = .create_transform };
        if (second.matchesKeywordTag(.trigger)) return .{ .unsupported = .create_trigger };
        if (second.matchesKeywordTag(.extension)) return .{ .extension_index = .create_extension };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 2 and tokens[1].matchesKeywordTag(.graph) and tokens[2].matchesKeywordTag(.index)) {
        return .{ .graph = .alter_metric };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 2 and tokens[1].matchesKeywordTag(.foreign) and tokens[2].matchesKeywordTag(.table)) {
        return .{ .unsupported = .alter_foreign_table };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 3 and tokens[1].matchesKeywordTag(.foreign) and tokens[2].matchesKeywordTag(.data) and tokens[3].matchesKeyword("wrapper")) {
        return .{ .unsupported = .alter_foreign_data_wrapper };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.table)) {
        return .{ .ddl = .alter_table };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.database)) {
        return .{ .ddl = .alter_database };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.extension)) {
        return .{ .ddl = .alter_extension };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.schema)) {
        return .{ .ddl = .alter_schema };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.tablespace)) {
        return .{ .ddl = .alter_tablespace };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.view)) {
        return .{ .ddl = .alter_view };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.domain)) {
        return .{ .ddl = .alter_domain };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.sequence)) {
        return .{ .ddl = .alter_sequence };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1 and tokens[1].matchesKeywordTag(.type)) {
        return .{ .ddl = .alter_enum_type };
    }
    if (first.matchesKeywordTag(.alter) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.aggregate)) return .{ .unsupported = .alter_aggregate };
        if (second.matchesKeyword("conversion")) return .{ .unsupported = .alter_conversion };
        if (second.matchesKeywordTag(.default) and tokens.len > 2 and tokens[2].matchesKeywordTag(.privileges)) return .{ .unsupported = .alter_default_privileges };
        if (second.matchesKeyword("event") and tokens.len > 2 and tokens[2].matchesKeywordTag(.trigger)) return .{ .unsupported = .alter_event_trigger };
        if (second.matchesKeywordTag(.function)) return .{ .unsupported = .alter_function };
        if (second.matchesKeywordTag(.index)) return .{ .unsupported = .alter_index };
        if (second.matchesKeyword("large") and tokens.len > 2 and tokens[2].matchesKeyword("object")) return .{ .unsupported = .alter_large_object };
        if (second.matchesKeyword("language")) return .{ .unsupported = .alter_language };
        if (second.matchesKeywordTag(.materialized) and tokens.len > 2 and tokens[2].matchesKeywordTag(.view)) return .{ .unsupported = .alter_materialized_view };
        if (second.matchesKeywordTag(.operator)) {
            if (tokens.len > 2 and tokens[2].matchesKeyword("class")) return .{ .unsupported = .alter_operator_class };
            if (tokens.len > 2 and tokens[2].matchesKeyword("family")) return .{ .unsupported = .alter_operator_family };
            return .{ .unsupported = .alter_operator };
        }
        if (second.matchesKeywordTag(.policy)) return .{ .ddl = .alter_policy };
        if (second.matchesKeywordTag(.procedure)) return .{ .unsupported = .alter_procedure };
        if (second.matchesKeyword("routine")) return .{ .unsupported = .alter_routine };
        if (second.matchesKeywordTag(.publication)) return .{ .ddl = .alter_publication };
        if (second.matchesKeywordTag(.rule)) return .{ .unsupported = .alter_rule };
        if (second.matchesKeywordTag(.server)) return .{ .unsupported = .alter_server };
        if (second.matchesKeywordTag(.subscription)) return .{ .ddl = .alter_subscription };
        if (second.matchesKeywordTag(.system)) return .{ .unsupported = .alter_system };
        if (second.matchesKeyword("statistics")) return .{ .unsupported = .alter_statistics };
        if (second.matchesKeywordTag(.text) and tokens.len > 3 and tokens[2].matchesKeyword("search")) {
            const text_search_kind = tokens[3];
            if (text_search_kind.matchesKeyword("configuration")) return .{ .unsupported = .alter_text_search_configuration };
            if (text_search_kind.matchesKeyword("dictionary")) return .{ .unsupported = .alter_text_search_dictionary };
            if (text_search_kind.matchesKeyword("parser")) return .{ .unsupported = .alter_text_search_parser };
            if (text_search_kind.matchesKeyword("template")) return .{ .unsupported = .alter_text_search_template };
        }
        if (second.matchesKeywordTag(.trigger)) return .{ .unsupported = .alter_trigger };
        if (second.matchesKeyword("transform")) return .{ .unsupported = .alter_transform };
        if (second.matchesKeyword("user") and tokens.len > 2 and tokens[2].matchesKeyword("mapping")) return .{ .unsupported = .alter_user_mapping };
        if (isGeneratedRoleAliasKeyword(second)) return .{ .ddl = .alter_role };
        if (second.matchesKeywordTag(.collation)) return .{ .ddl = .alter_collation };
    }
    if (first.matchesKeywordTag(.drop) and tokens.len > 1) {
        const second = tokens[1];
        if (second.matchesKeywordTag(.table)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_table_multi };
            return .{ .ddl = .drop_table };
        }
        if (second.matchesKeywordTag(.view)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_view_multi };
            return .{ .ddl = .drop_view };
        }
        if (second.matchesKeywordTag(.domain)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_domain_multi };
            return .{ .ddl = .drop_domain };
        }
        if (second.matchesKeywordTag(.sequence)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_sequence_multi };
            return .{ .ddl = .drop_sequence };
        }
        if (second.matchesKeywordTag(.type)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_type_multi };
            return .{ .ddl = .drop_enum_type };
        }
        if (second.matchesKeywordTag(.tablespace)) return .{ .ddl = .drop_tablespace };
        if (second.matchesKeywordTag(.publication)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_publication_multi };
            return .{ .ddl = .drop_publication };
        }
        if (second.matchesKeywordTag(.subscription)) return .{ .ddl = .drop_subscription };
        if (second.matchesKeywordTag(.function)) return .{ .ddl = .drop_function };
        if (second.matchesKeywordTag(.procedure)) return .{ .ddl = .drop_procedure };
        if (second.matchesKeywordTag(.index)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_index_multi };
            return .{ .extension_index = .drop_index };
        }
        if (second.matchesKeywordTag(.schema)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_schema_multi };
            return .{ .ddl = .drop_schema };
        }
        if (second.matchesKeywordTag(.database)) return .{ .ddl = .drop_database };
        if (second.matchesKeywordTag(.extension)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_extension_multi };
            return .{ .extension_index = .drop_extension };
        }
        if (second.matchesKeywordTag(.access) and tokens.len > 2 and tokens[2].matchesKeywordTag(.method)) return .{ .unsupported = .drop_access_method };
        if (second.matchesKeyword("conversion")) return .{ .unsupported = .drop_conversion };
        if (second.matchesKeyword("event") and tokens.len > 2 and tokens[2].matchesKeywordTag(.trigger)) return .{ .unsupported = .drop_event_trigger };
        if (second.matchesKeywordTag(.foreign) and tokens.len > 2 and tokens[2].matchesKeywordTag(.table)) return .{ .unsupported = .drop_foreign_table };
        if (second.matchesKeywordTag(.foreign) and tokens.len > 3 and tokens[2].matchesKeywordTag(.data) and tokens[3].matchesKeyword("wrapper")) return .{ .unsupported = .drop_foreign_data_wrapper };
        if (second.matchesKeyword("language")) return .{ .unsupported = .drop_language };
        if (second.matchesKeywordTag(.materialized) and tokens.len > 2 and tokens[2].matchesKeywordTag(.view)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 3)) return .{ .unsupported = .drop_materialized_view_multi };
            return .{ .ddl = .drop_materialized_view };
        }
        if (second.matchesKeywordTag(.policy)) return .{ .ddl = .drop_policy };
        if (second.matchesKeywordTag(.rule)) return .{ .unsupported = .drop_rule };
        if (second.matchesKeyword("user") and tokens.len > 2 and tokens[2].matchesKeyword("mapping")) return .{ .unsupported = .drop_user_mapping };
        if (isGeneratedRoleAliasKeyword(second)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_role_multi };
            return .{ .ddl = .drop_role };
        }
        if (second.matchesKeywordTag(.collation)) {
            if (generatedDropCatalogHeadHasMultipleTargets(tokens, 2)) return .{ .unsupported = .drop_collation_multi };
            return .{ .ddl = .drop_collation };
        }
        if (second.matchesKeywordTag(.operator)) {
            if (tokens.len > 2 and tokens[2].matchesKeyword("class")) return .{ .unsupported = .drop_operator_class };
            if (tokens.len > 2 and tokens[2].matchesKeyword("family")) return .{ .unsupported = .drop_operator_family };
            return .{ .ddl = .drop_operator };
        }
        if (second.matchesKeyword("routine")) return .{ .unsupported = .drop_routine };
        if (second.matchesKeywordTag(.owned)) return .{ .unsupported = .drop_owned };
        if (second.matchesKeywordTag(.aggregate)) return .{ .ddl = .drop_aggregate };
        if (second.matchesKeywordTag(.cast)) return .{ .ddl = .drop_cast };
        if (second.matchesKeywordTag(.server)) return .{ .unsupported = .drop_server };
        if (second.matchesKeyword("statistics")) return .{ .unsupported = .drop_statistics };
        if (second.matchesKeywordTag(.text) and tokens.len > 3 and tokens[2].matchesKeyword("search")) {
            const text_search_kind = tokens[3];
            if (text_search_kind.matchesKeyword("configuration")) return .{ .unsupported = .drop_text_search_configuration };
            if (text_search_kind.matchesKeyword("dictionary")) return .{ .unsupported = .drop_text_search_dictionary };
            if (text_search_kind.matchesKeyword("parser")) return .{ .unsupported = .drop_text_search_parser };
            if (text_search_kind.matchesKeyword("template")) return .{ .unsupported = .drop_text_search_template };
        }
        if (second.matchesKeyword("transform")) return .{ .unsupported = .drop_transform };
        if (second.matchesKeywordTag(.trigger)) return .{ .unsupported = .drop_trigger };
    }
    if (first.matchesKeywordTag(.insert)) {
        if (generatedInsertHasOverridingValue(tokens)) return .{ .unsupported = .insert_overriding_value };
        for (tokens) |token| {
            if (token.matchesKeywordTag(.select)) return .{ .dml = .insert_select };
        }
        return .{ .dml = .insert_values };
    }
    if (first.matchesKeywordTag(.update)) return .{ .dml = .update };
    if (first.matchesKeywordTag(.delete)) return .{ .dml = .delete };
    if (first.matchesKeywordTag(.truncate)) return .{ .dml = .truncate };
    if (first.matchesKeywordTag(.merge)) return .{ .dml = .merge };
    if (first.matchesKeywordTag(.import) and tokens.len > 2 and tokens[1].matchesKeywordTag(.foreign) and tokens[2].matchesKeywordTag(.schema)) return .{ .unsupported = .import_foreign_schema };
    if (first.matchesKeywordTag(.with)) {
        if (generatedWriteKindForWithStatement(tokens)) |kind| return .{ .dml = kind };
        return .{ .read = classifyReadKind(tokens) };
    }
    if (first.matchesKeywordTag(.select)) {
        if (generatedSelectIntoTargetRange(tokens, 0, statementTokenEnd(tokens)) != null) return .{ .ddl = .relation_population };
        return .{ .read = classifyReadKind(tokens) };
    }
    if (first.matchesKeywordTag(.analyze)) return .{ .unsupported = .analyze };
    if (first.matchesKeywordTag(.call)) return .{ .unsupported = .call };
    if (first.matchesKeywordTag(.checkpoint)) return .{ .unsupported = .checkpoint };
    if (first.matchesKeywordTag(.cluster)) return .{ .unsupported = .cluster };
    if (first.matchesKeywordTag(.comment)) return .{ .unsupported = .comment };
    if (first.matchesKeywordTag(.copy)) return .{ .unsupported = .copy };
    if (first.matchesKeywordTag(.do)) return .{ .unsupported = .do_block };
    if (first.matchesKeywordTag(.explain)) return .{ .unsupported = .explain };
    if (first.matchesKeywordTag(.grant)) return .{ .unsupported = .grant };
    if (first.matchesKeywordTag(.listen)) return .{ .unsupported = .listen };
    if (first.matchesKeywordTag(.load)) return .{ .unsupported = .load };
    if (first.matchesKeywordTag(.lock)) return .{ .unsupported = .lock };
    if (first.matchesKeywordTag(.move)) return .{ .unsupported = .move };
    if (first.matchesKeywordTag(.notify)) return .{ .unsupported = .notify };
    if (first.matchesKeywordTag(.refresh) and tokens.len > 2 and tokens[1].matchesKeywordTag(.materialized) and tokens[2].matchesKeywordTag(.view)) {
        return .{ .ddl = .refresh_materialized_view };
    }
    if (first.matchesKeywordTag(.reassign) and tokens.len > 1 and tokens[1].matchesKeywordTag(.owned)) return .{ .unsupported = .reassign_owned };
    if (first.matchesKeywordTag(.reindex)) return .{ .unsupported = .reindex };
    if (first.matchesKeywordTag(.revoke)) return .{ .unsupported = .revoke };
    if (first.matchesKeywordTag(.security)) return .{ .unsupported = .security_label };
    if (first.matchesKeywordTag(.unlisten)) return .{ .unsupported = .unlisten };
    if (first.matchesKeywordTag(.vacuum)) return .{ .unsupported = .vacuum };
    return .other;
}

fn isGeneratedRoleAliasKeyword(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.role) or
        token.matchesKeyword("user") or
        token.matchesKeywordTag(.group);
}

fn classifyReadKind(tokens: []const token_mod.Token) GeneratedSqlReadKind {
    return classifyReadKindInRange(tokens, .{ .start = 0, .end = statementTokenEnd(tokens) });
}

fn generatedReadRowLockStart(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var depth: usize = 0;
    var index = start;
    var candidate: ?usize = null;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth != 0 or !tokens[index].matchesKeywordTag(.@"for")) continue;
        if (!generatedReadRowLockModeStartsAt(tokens, index + 1, end)) continue;
        if (generatedReadHasCompleteSourceBefore(tokens, start, index)) candidate = index;
    }
    return candidate;
}

fn generatedReadRowLockModeStartsAt(tokens: []const token_mod.Token, start: usize, end: usize) bool {
    if (start >= end or end > tokens.len) return false;
    if (tokens[start].matchesKeywordTag(.update) or tokens[start].matchesKeywordTag(.share)) return true;
    if (start + 1 < end and
        tokens[start].matchesKeywordTag(.key) and
        tokens[start + 1].matchesKeywordTag(.share))
    {
        return true;
    }
    if (start + 2 < end and
        tokens[start].matchesKeywordTag(.no) and
        tokens[start + 1].matchesKeywordTag(.key) and
        tokens[start + 2].matchesKeywordTag(.update))
    {
        return true;
    }
    return false;
}

fn generatedReadHasCompleteSourceBefore(tokens: []const token_mod.Token, start: usize, end: usize) bool {
    var depth: usize = 0;
    var index = start;
    while (index < end and index < tokens.len) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth == 0 and tokens[index].matchesKeywordTag(.from) and index + 1 < end) return true;
    }
    return false;
}

fn generatedSelectIntoTargetRange(tokens: []const token_mod.Token, start: usize, end: usize) ?GeneratedSqlTokenRange {
    const into_index = findTopLevelKeyword(tokens, start, end, .into) orelse return null;
    var index = into_index + 1;
    consumeGeneratedRelationLifetime(tokens, &index, end);
    if (index < end and tokens[index].matchesKeywordTag(.table)) index += 1;
    return generatedQualifiedNameRange(tokens, index, end);
}

fn generatedCreateTableAsTargetRange(tokens: []const token_mod.Token, start: usize, end: usize) ?GeneratedSqlTokenRange {
    var index = start + 1;
    consumeGeneratedRelationLifetime(tokens, &index, end);
    if (index >= end or !tokens[index].matchesKeywordTag(.table)) return null;
    index += 1;
    _ = consumeGeneratedIfNotExists(tokens, &index, end);
    const target = generatedQualifiedNameRange(tokens, index, end) orelse return null;
    index = target.end;
    if (index >= end or !tokens[index].matchesKeywordTag(.as)) return null;
    return target;
}

fn generatedCreateTableTargetRange(tokens: []const token_mod.Token, start: usize, end: usize) ?GeneratedSqlTokenRange {
    var index = start + 1;
    consumeGeneratedRelationLifetime(tokens, &index, end);
    if (index >= end or !tokens[index].matchesKeywordTag(.table)) return null;
    index += 1;
    _ = consumeGeneratedIfNotExists(tokens, &index, end);
    return generatedQualifiedNameRange(tokens, index, end);
}

fn generatedCreateCatalogHeadHasUnsupportedTail(tokens: []const token_mod.Token, name_start: usize) bool {
    const end = statementTokenEnd(tokens);
    var index = name_start;
    _ = consumeGeneratedIfNotExists(tokens, &index, end);
    const name = generatedQualifiedNameRange(tokens, index, end) orelse return false;
    return name.end < end;
}

fn generatedDropCatalogHeadHasMultipleTargets(tokens: []const token_mod.Token, name_start: usize) bool {
    const end = statementTokenEnd(tokens);
    var index = name_start;
    _ = consumeGeneratedIfExists(tokens, &index, end);
    const first = generatedQualifiedNameRange(tokens, index, end) orelse return false;
    return first.end < end and tokens[first.end].kind == .comma;
}

fn consumeGeneratedRelationLifetime(tokens: []const token_mod.Token, index: *usize, end: usize) void {
    if (index.* >= end) return;
    if (tokens[index.*].matchesKeywordTag(.temp) or
        tokens[index.*].matchesKeywordTag(.temporary) or
        tokens[index.*].matchesKeywordTag(.unlogged))
    {
        index.* += 1;
    }
}

fn generatedWriteKindForWithStatement(tokens: []const token_mod.Token) ?GeneratedSqlDmlKind {
    const start = generatedWithFinalStatementIndex(tokens, .{ .allow_recursive = true }) orelse return null;
    if (tokens[start].matchesKeywordTag(.insert)) {
        const select_index = findTopLevelKeyword(tokens, start + 1, tokens.len, .select);
        const values_index = findTopLevelKeyword(tokens, start + 1, tokens.len, .values);
        const default_index = findTopLevelKeyword(tokens, start + 1, tokens.len, .default);
        if (select_index) |idx| {
            if ((values_index == null or idx < values_index.?) and
                (default_index == null or idx < default_index.?))
            {
                return .insert_select;
            }
        }
        return .insert_values;
    }
    if (tokens[start].matchesKeywordTag(.update)) return .update;
    if (tokens[start].matchesKeywordTag(.delete)) return .delete;
    if (tokens[start].matchesKeywordTag(.truncate)) return .truncate;
    if (tokens[start].matchesKeywordTag(.merge)) return .merge;
    return null;
}

fn generatedInsertHasOverridingValue(tokens: []const token_mod.Token) bool {
    const end = statementTokenEnd(tokens);
    const overriding_index = findTopLevelKeyword(tokens, 1, end, .overriding) orelse return false;
    if (overriding_index + 2 >= end) return false;
    if (!(tokens[overriding_index + 1].matchesKeywordTag(.system) or
        tokens[overriding_index + 1].matchesKeywordTag(.user)))
    {
        return false;
    }
    return tokens[overriding_index + 2].matchesKeywordTag(.value);
}

fn buildUnsupportedAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlUnsupportedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlUnsupportedAst {
    var ast = GeneratedSqlUnsupportedAst{
        .kind = kind,
        .reason = switch (kind) {
            .analyze => .analyze_not_planned_by_generated_parser,
            .call => .call_not_planned_by_generated_parser,
            .checkpoint => .checkpoint_not_planned_by_generated_parser,
            .close => .close_not_planned_by_generated_parser,
            .cluster => .cluster_not_planned_by_generated_parser,
            .comment => .comment_not_planned_by_generated_parser,
            .copy => .copy_not_planned_by_generated_parser,
            .discard => .discard_not_planned_by_generated_parser,
            .insert_overriding_value => .insert_overriding_value_not_planned_by_generated_parser,
            .alter_aggregate => .alter_aggregate_not_planned_by_generated_parser,
            .alter_index => .alter_index_not_planned_by_generated_parser,
            .alter_conversion => .alter_conversion_not_planned_by_generated_parser,
            .alter_default_privileges => .alter_default_privileges_not_planned_by_generated_parser,
            .alter_event_trigger => .alter_event_trigger_not_planned_by_generated_parser,
            .alter_foreign_table => .alter_foreign_table_not_planned_by_generated_parser,
            .alter_foreign_data_wrapper => .alter_foreign_data_wrapper_not_planned_by_generated_parser,
            .alter_function => .alter_function_not_planned_by_generated_parser,
            .alter_large_object => .alter_large_object_not_planned_by_generated_parser,
            .alter_language => .alter_language_not_planned_by_generated_parser,
            .alter_materialized_view => .alter_materialized_view_not_planned_by_generated_parser,
            .alter_operator => .alter_operator_not_planned_by_generated_parser,
            .alter_operator_class => .alter_operator_class_not_planned_by_generated_parser,
            .alter_operator_family => .alter_operator_family_not_planned_by_generated_parser,
            .alter_policy => .alter_policy_not_planned_by_generated_parser,
            .alter_procedure => .alter_procedure_not_planned_by_generated_parser,
            .alter_routine => .alter_routine_not_planned_by_generated_parser,
            .alter_publication => .alter_publication_not_planned_by_generated_parser,
            .alter_rule => .alter_rule_not_planned_by_generated_parser,
            .alter_server => .alter_server_not_planned_by_generated_parser,
            .alter_subscription => .alter_subscription_not_planned_by_generated_parser,
            .alter_system => .alter_system_not_planned_by_generated_parser,
            .alter_statistics => .alter_statistics_not_planned_by_generated_parser,
            .alter_text_search_configuration => .alter_text_search_configuration_not_planned_by_generated_parser,
            .alter_text_search_dictionary => .alter_text_search_dictionary_not_planned_by_generated_parser,
            .alter_text_search_parser => .alter_text_search_parser_not_planned_by_generated_parser,
            .alter_text_search_template => .alter_text_search_template_not_planned_by_generated_parser,
            .alter_trigger => .alter_trigger_not_planned_by_generated_parser,
            .alter_transform => .alter_transform_not_planned_by_generated_parser,
            .alter_user_mapping => .alter_user_mapping_not_planned_by_generated_parser,
            .create_access_method => .create_access_method_not_planned_by_generated_parser,
            .create_conversion => .create_conversion_not_planned_by_generated_parser,
            .create_database_options => .create_database_options_not_planned_by_generated_parser,
            .create_event_trigger => .create_event_trigger_not_planned_by_generated_parser,
            .create_foreign_table => .create_foreign_table_not_planned_by_generated_parser,
            .create_foreign_data_wrapper => .create_foreign_data_wrapper_not_planned_by_generated_parser,
            .create_language => .create_language_not_planned_by_generated_parser,
            .create_materialized_view => .create_materialized_view_not_planned_by_generated_parser,
            .create_operator_class => .create_operator_class_not_planned_by_generated_parser,
            .create_operator_family => .create_operator_family_not_planned_by_generated_parser,
            .create_policy => .create_policy_not_planned_by_generated_parser,
            .create_publication => .create_publication_not_planned_by_generated_parser,
            .create_rule => .create_rule_not_planned_by_generated_parser,
            .create_schema_options => .create_schema_options_not_planned_by_generated_parser,
            .create_server => .create_server_not_planned_by_generated_parser,
            .create_subscription => .create_subscription_not_planned_by_generated_parser,
            .create_statistics => .create_statistics_not_planned_by_generated_parser,
            .create_text_search_configuration => .create_text_search_configuration_not_planned_by_generated_parser,
            .create_text_search_dictionary => .create_text_search_dictionary_not_planned_by_generated_parser,
            .create_text_search_parser => .create_text_search_parser_not_planned_by_generated_parser,
            .create_text_search_template => .create_text_search_template_not_planned_by_generated_parser,
            .create_transform => .create_transform_not_planned_by_generated_parser,
            .create_trigger => .create_trigger_not_planned_by_generated_parser,
            .create_user_mapping => .create_user_mapping_not_planned_by_generated_parser,
            .declare => .declare_not_planned_by_generated_parser,
            .do_block => .do_block_not_planned_by_generated_parser,
            .drop_foreign_table => .drop_foreign_table_not_planned_by_generated_parser,
            .drop_foreign_data_wrapper => .drop_foreign_data_wrapper_not_planned_by_generated_parser,
            .drop_access_method => .drop_access_method_not_planned_by_generated_parser,
            .drop_conversion => .drop_conversion_not_planned_by_generated_parser,
            .drop_event_trigger => .drop_event_trigger_not_planned_by_generated_parser,
            .drop_language => .drop_language_not_planned_by_generated_parser,
            .drop_collation_multi => .drop_collation_multi_not_planned_by_generated_parser,
            .drop_domain_multi => .drop_domain_multi_not_planned_by_generated_parser,
            .drop_extension_multi => .drop_extension_multi_not_planned_by_generated_parser,
            .drop_index_multi => .drop_index_multi_not_planned_by_generated_parser,
            .drop_materialized_view_multi => .drop_materialized_view_multi_not_planned_by_generated_parser,
            .drop_owned => .drop_owned_not_planned_by_generated_parser,
            .drop_operator_class => .drop_operator_class_not_planned_by_generated_parser,
            .drop_operator_family => .drop_operator_family_not_planned_by_generated_parser,
            .drop_publication_multi => .drop_publication_multi_not_planned_by_generated_parser,
            .drop_routine => .drop_routine_not_planned_by_generated_parser,
            .drop_role_multi => .drop_role_multi_not_planned_by_generated_parser,
            .drop_schema_multi => .drop_schema_multi_not_planned_by_generated_parser,
            .drop_sequence_multi => .drop_sequence_multi_not_planned_by_generated_parser,
            .drop_statistics => .drop_statistics_not_planned_by_generated_parser,
            .drop_table_multi => .drop_table_multi_not_planned_by_generated_parser,
            .drop_text_search_configuration => .drop_text_search_configuration_not_planned_by_generated_parser,
            .drop_text_search_dictionary => .drop_text_search_dictionary_not_planned_by_generated_parser,
            .drop_text_search_parser => .drop_text_search_parser_not_planned_by_generated_parser,
            .drop_text_search_template => .drop_text_search_template_not_planned_by_generated_parser,
            .drop_transform => .drop_transform_not_planned_by_generated_parser,
            .drop_type_multi => .drop_type_multi_not_planned_by_generated_parser,
            .drop_user_mapping => .drop_user_mapping_not_planned_by_generated_parser,
            .drop_view_multi => .drop_view_multi_not_planned_by_generated_parser,
            .explain => .explain_not_planned_by_generated_parser,
            .fetch => .fetch_not_planned_by_generated_parser,
            .grant => .grant_not_planned_by_generated_parser,
            .import_foreign_schema => .import_foreign_schema_not_planned_by_generated_parser,
            .listen => .listen_not_planned_by_generated_parser,
            .load => .load_not_planned_by_generated_parser,
            .lock => .lock_not_planned_by_generated_parser,
            .move => .move_not_planned_by_generated_parser,
            .notify => .notify_not_planned_by_generated_parser,
            .reassign_owned => .reassign_owned_not_planned_by_generated_parser,
            .reindex => .reindex_not_planned_by_generated_parser,
            .release => .release_not_planned_by_generated_parser,
            .revoke => .revoke_not_planned_by_generated_parser,
            .role_session_control => .role_session_control_not_planned_by_generated_parser,
            .savepoint => .savepoint_not_planned_by_generated_parser,
            .security_label => .security_label_not_planned_by_generated_parser,
            .drop_materialized_view => .drop_materialized_view_not_planned_by_generated_parser,
            .drop_policy => .drop_policy_not_planned_by_generated_parser,
            .drop_publication => .drop_publication_not_planned_by_generated_parser,
            .drop_rule => .drop_rule_not_planned_by_generated_parser,
            .drop_server => .drop_server_not_planned_by_generated_parser,
            .drop_subscription => .drop_subscription_not_planned_by_generated_parser,
            .drop_trigger => .drop_trigger_not_planned_by_generated_parser,
            .unlisten => .unlisten_not_planned_by_generated_parser,
            .vacuum => .vacuum_not_planned_by_generated_parser,
        },
        .statement_span = statement_span,
        .command_span = command_span,
    };
    if (kind == .explain) {
        populateGeneratedExplainUnsupportedAst(tokens, end, &ast);
    } else if (generatedUnsupportedSubjectStart(kind, end)) |subject_start| {
        ast.subject_tokens = .{ .start = subject_start, .end = end };
    }
    return ast;
}

fn generatedUnsupportedSubjectStart(kind: GeneratedSqlUnsupportedKind, end: usize) ?usize {
    const start: usize = switch (kind) {
        .create_trigger,
        .drop_trigger,
        => 2,
        else => 1,
    };
    return if (end > start) start else null;
}

fn populateGeneratedExplainUnsupportedAst(
    tokens: []const token_mod.Token,
    end: usize,
    ast: *GeneratedSqlUnsupportedAst,
) void {
    if (end <= 1 or end > tokens.len) return;
    var index: usize = 1;
    if (tokens[index].matchesKeywordTag(.analyze)) {
        ast.explain_analyze = true;
        index += 1;
    } else if (tokens[index].kind == .lparen) {
        const close = findMatchingParen(tokens, index, end) orelse {
            ast.explain_options_valid = false;
            return;
        };
        ast.explain_options_tokens = .{ .start = index, .end = close + 1 };
        parseGeneratedExplainOptionList(tokens, index + 1, close, ast) catch {
            ast.explain_options_valid = false;
        };
        index = close + 1;
    }
    if (index < end) ast.subject_tokens = .{ .start = index, .end = end };
}

fn parseGeneratedExplainOptionList(
    tokens: []const token_mod.Token,
    start: usize,
    end: usize,
    ast: *GeneratedSqlUnsupportedAst,
) !void {
    var index = start;
    while (index < end) {
        try parseGeneratedExplainOption(tokens, &index, end, ast);
        if (index == end) return;
        if (tokens[index].kind != .comma) return error.UnsupportedSqlShape;
        index += 1;
        if (index >= end) return error.UnsupportedSqlShape;
    }
    if (start == end) return error.UnsupportedSqlShape;
}

fn parseGeneratedExplainOption(
    tokens: []const token_mod.Token,
    index: *usize,
    end: usize,
    ast: *GeneratedSqlUnsupportedAst,
) !void {
    if (index.* >= end) return error.UnsupportedSqlShape;
    const name = tokens[index.*];
    index.* += 1;
    if (name.matchesKeywordTag(.format)) {
        if (index.* >= end) return error.UnsupportedSqlShape;
        if (tokens[index.*].matchesKeywordTag(.json)) {
            ast.explain_format = .json;
        } else if (tokens[index.*].matchesKeywordTag(.text)) {
            ast.explain_format = .text;
        } else {
            return error.UnsupportedSqlShape;
        }
        index.* += 1;
        return;
    }

    if (name.matchesKeywordTag(.analyze)) {
        ast.explain_analyze = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.verbose)) {
        ast.explain_verbose = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.costs)) {
        ast.explain_costs = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.buffers)) {
        ast.explain_buffers = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.timing)) {
        ast.explain_timing = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.summary)) {
        ast.explain_summary = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.settings)) {
        ast.explain_settings = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else if (name.matchesKeywordTag(.wal)) {
        ast.explain_wal = parseGeneratedExplainOptionalBool(tokens, index, end, true);
    } else {
        return error.UnsupportedSqlShape;
    }
}

fn parseGeneratedExplainOptionalBool(
    tokens: []const token_mod.Token,
    index: *usize,
    end: usize,
    default_value: bool,
) bool {
    const before = index.*;
    if (index.* < end and
        (tokens[index.*].matchesKeywordTag(.true) or
            tokens[index.*].matchesKeywordTag(.on) or
            tokens[index.*].matchesKeywordTag(.yes)))
    {
        index.* += 1;
        return true;
    }
    index.* = before;
    if (index.* < end and
        (tokens[index.*].matchesKeywordTag(.false) or
            tokens[index.*].matchesKeywordTag(.off) or
            tokens[index.*].matchesKeywordTag(.no)))
    {
        index.* += 1;
        return false;
    }
    index.* = before;
    return default_value;
}

fn buildGeneratedAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, statement: GeneratedSqlStatement) !?GeneratedSqlAst {
    const end = statementTokenEnd(tokens);
    if (end == 0) return null;
    const statement_span = sourceSpanForTokenRange(tokens, .{ .start = 0, .end = end }) orelse return null;
    const command_start = generatedCommandStartIndex(tokens, statement) orelse 0;
    const command_span = tokens[command_start].sourceSpan();
    return switch (statement) {
        .session => |kind| .{ .session = buildSessionAst(tokens, end, kind, statement_span, command_span) },
        .transaction => |kind| .{ .transaction = buildTransactionAst(tokens, end, kind, statement_span, command_span) },
        .prepared => |kind| .{ .prepared = buildPreparedAst(tokens, end, kind, statement_span, command_span) },
        .ddl => |kind| .{ .ddl = buildDdlAst(tokens, end, kind, statement_span, command_span) },
        .dml => |kind| .{ .dml = try buildDmlAst(alloc, tokens, command_start, end, kind, statement_span, command_span) },
        .read => |kind| {
            const read_ast = try alloc.create(GeneratedSqlReadAst);
            errdefer alloc.destroy(read_ast);
            try buildReadAstInPlace(alloc, tokens, end, kind, statement_span, command_span, read_ast);
            return .{ .read = read_ast };
        },
        .extension_index => |kind| .{ .extension_index = buildDdlAst(tokens, end, ddlKindFromExtensionIndexKind(kind), statement_span, command_span) },
        .graph => |kind| .{ .graph = .{
            .kind = kind,
            .statement_span = statement_span,
            .command_span = command_span,
        } },
        .cursor => |kind| .{ .cursor = buildCursorAst(end, kind, statement_span, command_span) },
        .unsupported => |kind| .{ .unsupported = buildUnsupportedAst(tokens, end, kind, statement_span, command_span) },
        else => null,
    };
}

fn buildTransactionAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlTransactionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlTransactionAst {
    var ast = GeneratedSqlTransactionAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .set_transaction => ast.mode_tokens = if (end > 1) .{ .start = 1, .end = end } else null,
        .start_transaction => {
            if (end == 2) {
                ast.boundary_tail_tokens = .{ .start = 1, .end = 2 };
            } else if (end > 2) {
                ast.mode_tokens = .{ .start = 1, .end = end };
            }
        },
        .begin => {
            if (generatedBeginHasModeTail(tokens, end)) {
                ast.mode_tokens = .{ .start = 1, .end = end };
            } else if (end > 1) {
                ast.boundary_tail_tokens = .{ .start = 1, .end = end };
            }
        },
        .commit, .rollback => {
            if (end > 1) ast.boundary_tail_tokens = .{ .start = 1, .end = end };
        },
        .savepoint => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = end };
        },
        .release_savepoint => {
            ast.name_tokens = generatedSavepointNameRange(tokens, end, 1);
        },
        .rollback_to_savepoint => {
            ast.name_tokens = generatedSavepointNameRange(tokens, end, 2);
        },
    }
    return ast;
}

fn generatedSavepointNameRange(tokens: []const token_mod.Token, end: usize, start: usize) ?GeneratedSqlTokenRange {
    if (start >= end or end > tokens.len) return null;
    const name_start = if (tokens[start].matchesKeywordTag(.savepoint)) start + 1 else start;
    return if (name_start < end) .{ .start = name_start, .end = end } else null;
}

fn buildCursorAst(
    end: usize,
    kind: GeneratedSqlCursorKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlCursorAst {
    return .{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
        .tail_tokens = if (end > 1) .{ .start = 1, .end = end } else null,
    };
}

fn generatedBeginHasModeTail(tokens: []const token_mod.Token, end: usize) bool {
    if (end <= 1 or end > tokens.len) return false;
    if (end == 2 and (tokens[1].matchesKeyword("work") or tokens[1].matchesKeyword("transaction"))) return false;
    return true;
}

fn generatedCommandStartIndex(tokens: []const token_mod.Token, statement: GeneratedSqlStatement) ?usize {
    return switch (statement) {
        .dml => if (tokens.len > 0 and tokens[0].matchesKeywordTag(.with))
            generatedWithFinalStatementIndex(tokens, .{ .allow_recursive = true })
        else
            0,
        .read => if (tokens.len > 0 and tokens[0].matchesKeywordTag(.with))
            findTopLevelKeyword(tokens, 0, statementTokenEnd(tokens), .select)
        else
            0,
        else => 0,
    };
}

const GeneratedWithFinalStatementOptions = struct {
    allow_recursive: bool = false,
};

fn generatedWithFinalStatementIndex(tokens: []const token_mod.Token, options: GeneratedWithFinalStatementOptions) ?usize {
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.with)) return null;
    var index: usize = 1;
    if (index < tokens.len and tokens[index].matchesKeywordTag(.recursive)) {
        if (!options.allow_recursive) return null;
        index += 1;
    }

    while (true) {
        if (index >= tokens.len or tokens[index].kind != .identifier) return null;
        index += 1;
        if (index < tokens.len and tokens[index].kind == .lparen) {
            index = (findMatchingParen(tokens, index, tokens.len) orelse return null) + 1;
        }
        if (index >= tokens.len or !tokens[index].matchesKeywordTag(.as)) return null;
        index += 1;
        if (index < tokens.len and tokens[index].matchesKeywordTag(.materialized)) {
            index += 1;
        } else if (index + 1 < tokens.len and tokens[index].matchesKeywordTag(.not) and tokens[index + 1].matchesKeywordTag(.materialized)) {
            index += 2;
        }
        if (index >= tokens.len or tokens[index].kind != .lparen) return null;
        index = (findMatchingParen(tokens, index, tokens.len) orelse return null) + 1;
        if (index < tokens.len and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }

    if (index >= tokens.len or tokens[index].kind != .identifier) return null;
    return index;
}

fn ddlKindFromExtensionIndexKind(kind: GeneratedSqlExtensionIndexKind) GeneratedSqlDdlKind {
    return switch (kind) {
        .create_index => .create_index,
        .drop_index => .drop_index,
        .create_extension => .create_extension,
        .drop_extension => .drop_extension,
    };
}

fn buildSessionAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlSessionKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlSessionAst {
    var ast = GeneratedSqlSessionAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .set => {
            const name_start = findSetNameStart(tokens, end);
            const value_start = findSetValueStart(tokens, end, name_start) orelse end;
            if (value_start > name_start) ast.name_tokens = .{ .start = name_start, .end = value_start - 1 };
            if (value_start < end) ast.value_tokens = .{ .start = value_start, .end = end };
        },
        .reset, .show => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = end };
        },
        .discard_all => {},
    }
    return ast;
}

fn findSetNameStart(tokens: []const token_mod.Token, end: usize) usize {
    if (end > 2 and tokens[1].matchesKeyword("local")) return 2;
    return 1;
}

fn findSetValueStart(tokens: []const token_mod.Token, end: usize, name_start: usize) ?usize {
    var index: usize = name_start;
    while (index < end) : (index += 1) {
        if (tokens[index].kind == .eq or tokens[index].matchesKeywordTag(.to)) return index + 1;
    }
    return if (end > name_start + 1) name_start + 1 else null;
}

fn buildPreparedAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlPreparedKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlPreparedAst {
    var ast = GeneratedSqlPreparedAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    switch (kind) {
        .prepare => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = 2 };
            if (findKeyword(tokens, 2, end, .as)) |as_index| {
                if (as_index > 2) ast.parameter_tokens = .{ .start = 2, .end = as_index };
                if (as_index + 1 < end) ast.inner_statement_tokens = .{ .start = as_index + 1, .end = end };
            }
        },
        .execute => {
            if (end > 1) ast.name_tokens = .{ .start = 1, .end = 2 };
            if (end > 2) ast.argument_tokens = .{ .start = 2, .end = end };
        },
        .deallocate => {
            const name_start: usize = if (end > 2 and tokens[1].matchesKeywordTag(.prepare)) 2 else 1;
            if (end > name_start) ast.name_tokens = .{ .start = name_start, .end = end };
        },
    }
    return ast;
}

fn buildDdlAst(
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlDdlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) GeneratedSqlDdlAst {
    var ast = GeneratedSqlDdlAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    var index: usize = 2;
    switch (kind) {
        .create_database => {
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .create_schema => {
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .create_extension => {
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |_| index += 1;
            if (index < end and tokens[index].matchesKeywordTag(.with)) index += 1;
            if (index + 1 < end and tokens[index].matchesKeywordTag(.schema)) {
                ast.schema_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index + 1, end);
                index += 2;
            }
            if (index + 1 < end and tokens[index].matchesKeyword("version") and tokens[index + 1].kind == .string) {
                ast.version_tokens = .{ .start = index + 1, .end = index + 2 };
            }
        },
        .create_table => {
            index = 1;
            consumeGeneratedRelationLifetime(tokens, &index, end);
            if (index < end and tokens[index].matchesKeywordTag(.table)) index += 1;
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
        },
        .create_view => {
            index = 1;
            if (index + 2 < end and tokens[index].matchesKeywordTag(.@"or") and tokens[index + 1].matchesKeywordTag(.replace)) {
                ast.replace_existing = true;
                index += 2;
            }
            if (index < end and tokens[index].matchesKeywordTag(.view)) index += 1;
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
        },
        .create_materialized_view => {
            if (end > 3 and tokens[1].matchesKeywordTag(.materialized) and tokens[2].matchesKeywordTag(.view)) {
                index = 3;
                ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |view_range| {
                    if (view_range.end < end) ast.alter_table_operation_tokens = .{ .start = view_range.end, .end = end };
                }
            }
        },
        .create_domain => {
            if (end > 2 and tokens[1].matchesKeywordTag(.domain)) {
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, 2, end);
                if (ast.object_name_tokens) |domain_range| {
                    if (domain_range.end < end) ast.alter_table_operation_tokens = .{ .start = domain_range.end, .end = end };
                }
            }
        },
        .create_sequence => {
            if (end > 2 and tokens[1].matchesKeywordTag(.sequence)) {
                index = 2;
                ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |sequence_range| {
                    if (sequence_range.end < end) ast.alter_table_operation_tokens = .{ .start = sequence_range.end, .end = end };
                }
            }
        },
        .create_enum_type => {
            if (end > 2 and tokens[1].matchesKeywordTag(.type)) {
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, 2, end);
                if (ast.object_name_tokens) |type_range| {
                    if (type_range.end < end) ast.alter_table_operation_tokens = .{ .start = type_range.end, .end = end };
                }
            }
        },
        .create_tablespace => {
            if (end > 2 and tokens[1].matchesKeywordTag(.tablespace)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |tablespace_range| {
                    if (tablespace_range.end < end) ast.alter_table_operation_tokens = .{ .start = tablespace_range.end, .end = end };
                }
            }
        },
        .create_publication => {
            if (end > 2 and tokens[1].matchesKeywordTag(.publication)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |publication_range| {
                    if (publication_range.end < end) ast.alter_table_operation_tokens = .{ .start = publication_range.end, .end = end };
                }
            }
        },
        .create_subscription => {
            if (end > 2 and tokens[1].matchesKeywordTag(.subscription)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |subscription_range| {
                    if (subscription_range.end < end) ast.alter_table_operation_tokens = .{ .start = subscription_range.end, .end = end };
                }
            }
        },
        .create_policy => {
            if (end > 4 and tokens[1].matchesKeywordTag(.policy)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |policy_range| {
                    index = policy_range.end;
                    if (index < end and tokens[index].matchesKeywordTag(.on)) {
                        ast.index_table_tokens = generatedQualifiedNameRange(tokens, index + 1, end);
                        if (ast.index_table_tokens) |table_range| {
                            if (table_range.end < end) ast.alter_table_operation_tokens = .{ .start = table_range.end, .end = end };
                        }
                    }
                }
            }
        },
        .create_function, .create_procedure => {
            index = 1;
            if (index + 2 < end and tokens[index].matchesKeywordTag(.@"or") and tokens[index + 1].matchesKeywordTag(.replace)) {
                ast.replace_existing = true;
                index += 2;
            }
            if (index < end and (tokens[index].matchesKeywordTag(.function) or tokens[index].matchesKeywordTag(.procedure))) {
                index += 1;
            }
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            if (ast.object_name_tokens) |routine_range| {
                if (routine_range.end < end) ast.alter_table_operation_tokens = .{ .start = routine_range.end, .end = end };
            }
        },
        .create_role => {
            if (end > 2 and isGeneratedRoleAliasKeyword(tokens[1])) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |role_range| {
                    if (role_range.end < end) ast.alter_table_operation_tokens = .{ .start = role_range.end, .end = end };
                }
            }
        },
        .create_collation => {
            if (end > 2 and tokens[1].matchesKeywordTag(.collation)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |collation_range| {
                    if (collation_range.end < end) ast.alter_table_operation_tokens = .{ .start = collation_range.end, .end = end };
                }
            }
        },
        .create_aggregate => {
            if (end > 2 and tokens[1].matchesKeywordTag(.aggregate)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |aggregate_range| {
                    if (aggregate_range.end < end) ast.alter_table_operation_tokens = .{ .start = aggregate_range.end, .end = end };
                }
            }
        },
        .create_operator, .create_cast => {
            if (end > 2) ast.alter_table_operation_tokens = .{ .start = 2, .end = end };
        },
        .create_index => {
            if (tokens.len > 2 and tokens[1].matchesKeywordTag(.unique) and tokens[2].matchesKeywordTag(.index)) {
                ast.unique = true;
                index = 3;
            }
            ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |name_range| index = name_range.end;
            if (index < end and tokens[index].matchesKeywordTag(.on)) {
                index += 1;
                ast.index_table_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.index_table_tokens) |table_range| index = table_range.end;
                if (index + 1 < end and tokens[index].matchesKeywordTag(.using)) {
                    ast.index_method_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index + 1, end);
                    if (ast.index_method_tokens) |method_range| index = method_range.end;
                }
                if (index < end and tokens[index].kind == .lparen) {
                    if (findMatchingParen(tokens, index, end)) |close_index| {
                        ast.index_elements_tokens = .{ .start = index + 1, .end = close_index };
                        index = close_index + 1;
                    }
                }
                if (index < end and tokens[index].matchesKeywordTag(.include)) {
                    const include_open = index + 1;
                    if (include_open < end and tokens[include_open].kind == .lparen) {
                        if (findMatchingParen(tokens, include_open, end)) |close_index| {
                            ast.index_include_tokens = .{ .start = include_open + 1, .end = close_index };
                            index = close_index + 1;
                        }
                    }
                }
                if (index < end and tokens[index].matchesKeywordTag(.with)) {
                    const options_start = index;
                    if (index + 1 < end and tokens[index + 1].kind == .lparen) {
                        if (findMatchingParen(tokens, index + 1, end)) |close_index| {
                            ast.index_options_tokens = .{ .start = options_start, .end = close_index + 1 };
                            index = close_index + 1;
                        }
                    } else {
                        ast.index_options_tokens = .{ .start = index, .end = end };
                    }
                }
                if (index < end and tokens[index].matchesKeywordTag(.where)) {
                    ast.index_where_tokens = .{ .start = index + 1, .end = end };
                }
            }
        },
        .alter_table => {
            if (end > 2 and tokens[1].matchesKeywordTag(.table)) {
                index = 2;
                ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
                if (index < end and tokens[index].matchesKeywordTag(.only)) index += 1;
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |table_range| {
                    index = table_range.end;
                    if (index < end) ast.alter_table_operation_tokens = .{ .start = index, .end = end };
                }
            }
        },
        .alter_database => {
            if (end > 2 and tokens[1].matchesKeywordTag(.database)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |database_range| {
                    if (database_range.end < end) ast.alter_table_operation_tokens = .{ .start = database_range.end, .end = end };
                }
            }
        },
        .alter_extension => {
            if (end > 2 and tokens[1].matchesKeywordTag(.extension)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |extension_range| {
                    if (extension_range.end < end) ast.alter_table_operation_tokens = .{ .start = extension_range.end, .end = end };
                    if (extension_range.end + 2 < end and
                        tokens[extension_range.end].matchesKeywordTag(.update) and
                        tokens[extension_range.end + 1].matchesKeywordTag(.to) and
                        tokens[extension_range.end + 2].kind == .string)
                    {
                        ast.version_tokens = .{ .start = extension_range.end + 2, .end = extension_range.end + 3 };
                    }
                }
            }
        },
        .alter_schema => {
            if (end > 2 and tokens[1].matchesKeywordTag(.schema)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |schema_range| {
                    if (schema_range.end < end) ast.alter_table_operation_tokens = .{ .start = schema_range.end, .end = end };
                }
            }
        },
        .alter_tablespace => {
            if (end > 2 and tokens[1].matchesKeywordTag(.tablespace)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |tablespace_range| {
                    if (tablespace_range.end < end) ast.alter_table_operation_tokens = .{ .start = tablespace_range.end, .end = end };
                }
            }
        },
        .alter_view => {
            if (end > 2 and tokens[1].matchesKeywordTag(.view)) {
                index = 2;
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |view_range| {
                    index = view_range.end;
                    if (index < end) ast.alter_table_operation_tokens = .{ .start = index, .end = end };
                }
            }
        },
        .alter_domain => {
            if (end > 2 and tokens[1].matchesKeywordTag(.domain)) {
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, 2, end);
                if (ast.object_name_tokens) |domain_range| {
                    if (domain_range.end < end) ast.alter_table_operation_tokens = .{ .start = domain_range.end, .end = end };
                }
            }
        },
        .alter_sequence => {
            if (end > 2 and tokens[1].matchesKeywordTag(.sequence)) {
                index = 2;
                ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |sequence_range| {
                    if (sequence_range.end < end) ast.alter_table_operation_tokens = .{ .start = sequence_range.end, .end = end };
                }
            }
        },
        .alter_enum_type => {
            if (end > 2 and tokens[1].matchesKeywordTag(.type)) {
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, 2, end);
                if (ast.object_name_tokens) |type_range| {
                    if (type_range.end < end) ast.alter_table_operation_tokens = .{ .start = type_range.end, .end = end };
                }
            }
        },
        .alter_publication => {
            if (end > 2 and tokens[1].matchesKeywordTag(.publication)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |publication_range| {
                    if (publication_range.end < end) ast.alter_table_operation_tokens = .{ .start = publication_range.end, .end = end };
                }
            }
        },
        .alter_subscription => {
            if (end > 2 and tokens[1].matchesKeywordTag(.subscription)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |subscription_range| {
                    if (subscription_range.end < end) ast.alter_table_operation_tokens = .{ .start = subscription_range.end, .end = end };
                }
            }
        },
        .alter_policy => {
            if (end > 4 and tokens[1].matchesKeywordTag(.policy)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |policy_range| {
                    index = policy_range.end;
                    if (index < end and tokens[index].matchesKeywordTag(.on)) {
                        ast.index_table_tokens = generatedQualifiedNameRange(tokens, index + 1, end);
                        if (ast.index_table_tokens) |table_range| {
                            if (table_range.end < end) ast.alter_table_operation_tokens = .{ .start = table_range.end, .end = end };
                        }
                    }
                }
            }
        },
        .alter_role => {
            if (end > 2 and isGeneratedRoleAliasKeyword(tokens[1])) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |role_range| {
                    if (role_range.end < end) ast.alter_table_operation_tokens = .{ .start = role_range.end, .end = end };
                }
            }
        },
        .alter_collation => {
            if (end > 2 and tokens[1].matchesKeywordTag(.collation)) {
                ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, 2, end);
                if (ast.object_name_tokens) |collation_range| {
                    if (collation_range.end < end) ast.alter_table_operation_tokens = .{ .start = collation_range.end, .end = end };
                }
            }
        },
        .drop_database => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (findKeywordText(tokens, index + 1, end, "force") != null) ast.force = true;
        },
        .drop_table => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_view => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_materialized_view => {
            index = 3;
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_domain => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_sequence => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_enum_type => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_tablespace => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .drop_publication => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .drop_subscription => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .drop_policy => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |policy_range| {
                index = policy_range.end;
                if (index < end and tokens[index].matchesKeywordTag(.on)) {
                    ast.index_table_tokens = generatedQualifiedNameRange(tokens, index + 1, end);
                    if (ast.index_table_tokens) |table_range| {
                        ast.cascade = findKeyword(tokens, table_range.end, end, .cascade) != null;
                    }
                }
            }
        },
        .drop_function, .drop_procedure => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
            if (ast.object_name_tokens) |routine_range| {
                if (routine_range.end < end) ast.alter_table_operation_tokens = .{ .start = routine_range.end, .end = end };
            }
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_role => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |role_range| {
                if (role_range.end < end) ast.alter_table_operation_tokens = .{ .start = role_range.end, .end = end };
            }
        },
        .drop_collation => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
        },
        .drop_aggregate => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            if (ast.object_name_tokens) |aggregate_range| {
                if (aggregate_range.end < end) ast.alter_table_operation_tokens = .{ .start = aggregate_range.end, .end = end };
            }
        },
        .drop_operator, .drop_cast => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            if (index < end) ast.alter_table_operation_tokens = .{ .start = index, .end = end };
        },
        .drop_index => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_schema => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .drop_extension => {
            ast.if_exists = consumeGeneratedIfExists(tokens, &index, end);
            ast.object_name_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
            ast.cascade = findKeyword(tokens, index + 1, end, .cascade) != null;
        },
        .refresh_materialized_view => {
            if (end > 3 and tokens[1].matchesKeywordTag(.materialized) and tokens[2].matchesKeywordTag(.view)) {
                index = 3;
                if (index < end and tokens[index].matchesKeyword("concurrently")) index += 1;
                ast.object_name_tokens = generatedQualifiedNameRange(tokens, index, end);
                if (ast.object_name_tokens) |view_range| {
                    if (view_range.end < end) ast.alter_table_operation_tokens = .{ .start = view_range.end, .end = end };
                }
            }
        },
        .relation_population => {
            if (tokens.len > 0 and tokens[0].matchesKeywordTag(.select)) {
                ast.object_name_tokens = generatedSelectIntoTargetRange(tokens, 0, end);
            } else {
                ast.object_name_tokens = generatedCreateTableAsTargetRange(tokens, 0, end);
                index = 1;
                consumeGeneratedRelationLifetime(tokens, &index, end);
                if (index < end and tokens[index].matchesKeywordTag(.table)) index += 1;
                ast.if_not_exists = consumeGeneratedIfNotExists(tokens, &index, end);
            }
        },
        else => {},
    }
    return ast;
}

fn buildDmlAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    start: usize,
    end: usize,
    kind: GeneratedSqlDmlKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
) !GeneratedSqlDmlAst {
    var ast = GeneratedSqlDmlAst{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    errdefer ast.deinit(alloc);
    if (start > 0 and tokens.len > 0 and tokens[0].matchesKeywordTag(.with)) {
        ast.cte_tokens = .{ .start = 1, .end = start };
        ast.cte_recursive = tokens.len > 1 and tokens[1].matchesKeywordTag(.recursive);
        try buildDmlCteAstAlloc(alloc, tokens, start, &ast);
    }
    switch (kind) {
        .insert_values, .insert_select => buildInsertDmlAst(tokens, start, end, &ast),
        .update => buildUpdateDmlAst(tokens, start, end, &ast),
        .delete => buildDeleteDmlAst(tokens, start, end, &ast),
        .truncate => buildTruncateDmlAst(tokens, start, end, &ast),
        .merge => buildMergeDmlAst(tokens, start, end, &ast),
    }
    buildDmlChildReadAst(tokens, &ast);
    return ast;
}

fn buildDmlCteAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    final_statement_index: usize,
    ast: *GeneratedSqlDmlAst,
) !void {
    const cte_tokens = ast.cte_tokens orelse return;
    var index: usize = if (ast.cte_recursive) 2 else 1;
    if (index >= final_statement_index) return;
    const list_tokens = GeneratedSqlTokenRange{ .start = index, .end = final_statement_index };
    var count: usize = 0;
    var items = std.ArrayListUnmanaged(GeneratedSqlDmlCteItemAst).empty;
    errdefer items.deinit(alloc);
    var first_name: ?GeneratedSqlTokenRange = null;
    var first_body: ?GeneratedSqlTokenRange = null;
    var first_read: ?GeneratedSqlDmlReadBodyAst = null;
    var last_name: ?GeneratedSqlTokenRange = null;
    var last_body: ?GeneratedSqlTokenRange = null;
    var last_read: ?GeneratedSqlDmlReadBodyAst = null;
    while (index < final_statement_index) {
        if (tokens[index].kind != .identifier) return;
        const name_tokens = GeneratedSqlTokenRange{ .start = index, .end = index + 1 };
        index += 1;
        if (index < final_statement_index and tokens[index].kind == .lparen) {
            index = (findMatchingParen(tokens, index, final_statement_index) orelse return) + 1;
        }
        if (index >= final_statement_index or !tokens[index].matchesKeywordTag(.as)) return;
        index += 1;
        if (index < final_statement_index and tokens[index].matchesKeywordTag(.materialized)) {
            index += 1;
        } else if (index + 1 < final_statement_index and tokens[index].matchesKeywordTag(.not) and tokens[index + 1].matchesKeywordTag(.materialized)) {
            index += 2;
        }
        if (index >= final_statement_index or tokens[index].kind != .lparen) return;
        const close = findMatchingParen(tokens, index, final_statement_index) orelse return;
        if (index + 1 >= close) return;
        const body_tokens = GeneratedSqlTokenRange{ .start = index + 1, .end = close };
        const body_read = buildDmlReadBodyMetadata(tokens, body_tokens) orelse return;
        try items.append(alloc, .{
            .name_tokens = name_tokens,
            .body_tokens = body_tokens,
            .body_read = body_read,
        });
        if (count == 0) {
            first_name = name_tokens;
            first_body = body_tokens;
            first_read = body_read;
        }
        last_name = name_tokens;
        last_body = body_tokens;
        last_read = body_read;
        count += 1;
        index = close + 1;
        if (index < final_statement_index and tokens[index].kind == .comma) {
            index += 1;
            continue;
        }
        break;
    }
    if (count == 0 or index != final_statement_index) return;
    const owned_items = try items.toOwnedSlice(alloc);
    errdefer alloc.free(owned_items);
    ast.cte_prefix = .{
        .tokens = cte_tokens,
        .list_tokens = list_tokens,
        .first_name_tokens = first_name.?,
        .first_body_tokens = first_body.?,
        .first_body_read = first_read.?,
        .last_name_tokens = last_name.?,
        .last_body_tokens = last_body.?,
        .last_body_read = last_read.?,
        .items = owned_items,
        .count = count,
        .recursive = ast.cte_recursive,
    };
}

fn buildDmlChildReadAst(
    tokens: []const token_mod.Token,
    ast: *GeneratedSqlDmlAst,
) void {
    const source_tokens = ast.source_tokens orelse return;
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return;
    switch (ast.kind) {
        .insert_select => buildDmlSelectSourceReadAst(tokens, ast, source_tokens),
        .update, .delete, .merge => buildDmlRelationSourceReadAst(tokens, ast, source_tokens),
        else => {},
    }
}

fn buildDmlSelectSourceReadAst(
    tokens: []const token_mod.Token,
    ast: *GeneratedSqlDmlAst,
    source_tokens: GeneratedSqlTokenRange,
) void {
    ast.source_read = buildDmlReadBodyMetadata(tokens, source_tokens) orelse return;
}

fn buildDmlReadBodyMetadata(
    tokens: []const token_mod.Token,
    source_tokens: GeneratedSqlTokenRange,
) ?GeneratedSqlDmlReadBodyAst {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return null;
    if (!tokens[source_tokens.start].matchesKeywordTag(.select)) return null;
    const statement_span = sourceSpanForTokenRange(tokens, source_tokens) orelse return null;
    var read_body = GeneratedSqlDmlReadBodyAst{
        .tokens = source_tokens,
        .kind = classifyReadKindInRange(tokens, source_tokens),
        .statement_span = statement_span,
        .command_span = tokens[source_tokens.start].sourceSpan(),
    };
    const body_end = firstTopLevelSetOperation(tokens, source_tokens.start + 1, source_tokens.end) orelse source_tokens.end;
    if (body_end < source_tokens.end) {
        const set_operation_tail_start = generatedSetOperationResultTailStart(tokens, .{ .start = body_end, .end = source_tokens.end }) orelse source_tokens.end;
        read_body.set_operation_tokens = .{ .start = body_end, .end = set_operation_tail_start };
    }
    var distinct_tokens: ?GeneratedSqlTokenRange = null;
    const projection_start = generatedReadProjectionStartInRange(tokens, source_tokens.start, body_end, &distinct_tokens);
    const from_index = findTopLevelKeyword(tokens, projection_start, body_end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, body_end, .where);
    const group_index = findTopLevelKeywordSequence(tokens, projection_start, body_end, .group, .by);
    const having_index = findTopLevelKeyword(tokens, projection_start, body_end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, body_end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, body_end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, body_end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, body_end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, body_end, .fetch);
    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
    if (projection_start < projection_end) read_body.projection_tokens = .{ .start = projection_start, .end = projection_end };
    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < source_end) read_body.source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse body_end;
        if (idx + 1 < where_end) read_body.where_tokens = .{ .start = idx + 1, .end = where_end };
    }
    return read_body;
}

fn buildDmlRelationSourceReadAst(
    tokens: []const token_mod.Token,
    ast: *GeneratedSqlDmlAst,
    source_tokens: GeneratedSqlTokenRange,
) void {
    const statement_span = sourceSpanForTokenRange(tokens, source_tokens) orelse return;
    ast.source_read = .{
        .tokens = source_tokens,
        .kind = classifyReadKindInRange(tokens, source_tokens),
        .statement_span = statement_span,
        .command_span = tokens[source_tokens.start].sourceSpan(),
        .source_tokens = source_tokens,
        .wrapper_projection_star = true,
    };
}

fn buildReadAstInPlace(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    end: usize,
    kind: GeneratedSqlReadKind,
    statement_span: token_mod.SourceSpan,
    command_span: token_mod.SourceSpan,
    ast: *GeneratedSqlReadAst,
) !void {
    ast.* = .{
        .kind = kind,
        .statement_span = statement_span,
        .command_span = command_span,
    };
    errdefer ast.deinit(alloc);
    const select_index = findTopLevelKeyword(tokens, 0, end, .select) orelse {
        return;
    };
    if (select_index > 0 and tokens[0].matchesKeywordTag(.with)) {
        ast.cte_tokens = .{ .start = 1, .end = select_index };
        try buildReadCteAst(alloc, tokens, select_index, ast);
    }

    const body_end = firstTopLevelSetOperation(tokens, select_index + 1, end) orelse end;
    if (body_end < end) {
        const set_operation_tail_start = generatedSetOperationResultTailStart(tokens, .{ .start = body_end, .end = end }) orelse end;
        const set_operation_tokens = GeneratedSqlTokenRange{ .start = body_end, .end = set_operation_tail_start };
        ast.set_operation_tokens = set_operation_tokens;
        ast.set_operation = try buildGeneratedSetOperationAst(alloc, tokens, set_operation_tokens);
    }

    const projection_start = generatedReadProjectionStart(tokens, select_index, body_end, ast);
    if (generatedDistinctOnExpressionTokens(tokens, ast.distinct_tokens)) |distinct_on_tokens| {
        ast.distinct_on_items = try buildTopLevelListAst(alloc, tokens, distinct_on_tokens, .{});
    }
    const from_index = findTopLevelKeyword(tokens, projection_start, body_end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, body_end, .where);
    const group_index = findTopLevelKeywordSequence(tokens, projection_start, body_end, .group, .by);
    const having_index = findTopLevelKeyword(tokens, projection_start, body_end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, body_end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, body_end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, body_end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, body_end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, body_end, .fetch);
    const row_lock_index = generatedReadRowLockStart(tokens, projection_start, body_end);

    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
    if (projection_start < projection_end) {
        const projection_tokens = GeneratedSqlTokenRange{ .start = projection_start, .end = projection_end };
        ast.projection_tokens = projection_tokens;
        ast.projection_items = try buildTopLevelListAst(alloc, tokens, projection_tokens, .{ .bare_alias = true });
        if (generatedListExpressionTokens(ast.projection_items, 0)) |first_tokens| {
            ast.projection_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
        }
        if (generatedListExpressionTokens(ast.projection_items, ast.projection_items.count -| 1)) |last_tokens| {
            ast.projection_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
        }
    }

    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < source_end) {
            const source_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = source_end };
            ast.source_tokens = source_tokens;
            try buildGeneratedReadGraphSourceAst(alloc, tokens, source_tokens, ast);
            try buildReadJoinAst(alloc, tokens, source_tokens, ast);
        }
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < where_end) {
            const where_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = where_end };
            ast.where_tokens = where_tokens;
            ast.where_expression = try buildGeneratedExpressionAst(alloc, tokens, where_tokens);
        }
    }
    if (group_index) |idx| {
        const group_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const group_end = firstOptionalIndex(&[_]?usize{ having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (group_start < group_end) {
            const group_tokens = GeneratedSqlTokenRange{ .start = group_start, .end = group_end };
            ast.group_tokens = group_tokens;
            ast.group_items = try buildTopLevelListAst(alloc, tokens, group_tokens, .{});
            if (generatedListExpressionTokens(ast.group_items, 0)) |first_tokens| {
                ast.group_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
            }
            if (generatedListExpressionTokens(ast.group_items, ast.group_items.count -| 1)) |last_tokens| {
                ast.group_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
            }
        }
    }
    if (having_index) |idx| {
        const having_end = firstOptionalIndex(&[_]?usize{ window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < having_end) {
            const having_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = having_end };
            ast.having_tokens = having_tokens;
            ast.having_expression = try buildGeneratedExpressionAst(alloc, tokens, having_tokens);
        }
    }
    if (window_index) |idx| {
        const window_end = firstOptionalIndex(&[_]?usize{ order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < window_end) {
            const window_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = window_end };
            ast.window_tokens = window_tokens;
            ast.window_items = try buildGeneratedWindowAstList(alloc, tokens, window_tokens);
            ast.window_count = ast.window_items.len;
        }
    }
    if (order_index) |idx| {
        const order_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const order_end = firstOptionalIndex(&[_]?usize{ limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (order_start < order_end) {
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            ast.order_tokens = order_tokens;
            ast.order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
            if (generatedListExpressionTokens(ast.order_items, 0)) |first_tokens| {
                ast.order_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
            }
            if (generatedListExpressionTokens(ast.order_items, ast.order_items.count -| 1)) |last_tokens| {
                ast.order_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
            }
        }
    }
    if (limit_index) |idx| {
        const limit_end = firstOptionalIndex(&[_]?usize{ offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < limit_end) {
            const limit_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = limit_end };
            ast.limit_tokens = limit_tokens;
            if (limit_tokens.end == limit_tokens.start + 1 and tokens[limit_tokens.start].matchesKeywordTag(.all)) {
                ast.limit_all = true;
            } else {
                ast.limit_expression = try buildGeneratedExpressionAst(alloc, tokens, limit_tokens);
            }
        }
    }
    if (offset_index) |idx| {
        const offset_end = firstOptionalIndex(&[_]?usize{ fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < offset_end) {
            const offset_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = offset_end };
            ast.offset_tokens = offset_tokens;
            if (generatedOffsetExpressionTokens(tokens, offset_tokens)) |expression_tokens| {
                ast.offset_expression = try buildGeneratedExpressionAst(alloc, tokens, expression_tokens);
            }
        }
    }
    if (fetch_index) |idx| {
        const fetch_end = row_lock_index orelse body_end;
        if (idx + 1 < fetch_end) {
            const fetch_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = fetch_end };
            ast.fetch_tokens = fetch_tokens;
            if (generatedFetchCountTokens(tokens, fetch_tokens)) |count_tokens| {
                ast.fetch_count_tokens = count_tokens;
                ast.fetch_count_expression = try buildGeneratedExpressionAst(alloc, tokens, count_tokens);
            }
        }
    }
    if (row_lock_index) |idx| {
        if (idx + 1 < body_end) ast.row_lock_tokens = .{ .start = idx, .end = body_end };
    }
    if (ast.set_operation_tokens) |set_operation_tokens| {
        try buildGeneratedReadResultTailAst(alloc, tokens, set_operation_tokens.end, end, ast);
    }

    return;
}

fn generatedSetOperationResultTailStart(
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) ?usize {
    if (range.start >= range.end or range.end > tokens.len) return null;

    var right_start = range.start + 1;
    if (right_start < range.end and tokens[right_start].matchesKeywordTag(.all)) {
        right_start += 1;
    }
    if (right_start >= range.end or !tokens[right_start].matchesKeywordTag(.select)) return null;

    var right_distinct_tokens: ?GeneratedSqlTokenRange = null;
    const projection_start = generatedReadProjectionStartInRange(tokens, right_start, range.end, &right_distinct_tokens);
    const order_index = findTopLevelKeyword(tokens, projection_start, range.end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, range.end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, range.end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, range.end, .fetch);
    return firstOptionalIndex(&[_]?usize{ order_index, limit_index, offset_index, fetch_index }) orelse range.end;
}

fn buildGeneratedReadResultTailAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    start: usize,
    end: usize,
    ast: *GeneratedSqlReadAst,
) !void {
    if (start >= end or end > tokens.len) return;

    const order_index = findTopLevelKeyword(tokens, start, end, .order);
    const limit_index = findTopLevelKeyword(tokens, start, end, .limit);
    const offset_index = findTopLevelKeyword(tokens, start, end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, start, end, .fetch);

    if (order_index) |idx| {
        const order_start = if (idx + 1 < end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const order_end = firstOptionalIndex(&[_]?usize{ limit_index, offset_index, fetch_index }) orelse end;
        if (order_start < order_end) {
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            ast.order_tokens = order_tokens;
            ast.order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
            if (generatedListExpressionTokens(ast.order_items, 0)) |first_tokens| {
                ast.order_first_expression = try buildGeneratedExpressionAst(alloc, tokens, first_tokens);
            }
            if (generatedListExpressionTokens(ast.order_items, ast.order_items.count -| 1)) |last_tokens| {
                ast.order_last_expression = try buildGeneratedExpressionAst(alloc, tokens, last_tokens);
            }
        }
    }
    if (limit_index) |idx| {
        const limit_end = firstOptionalIndex(&[_]?usize{ offset_index, fetch_index }) orelse end;
        if (idx + 1 < limit_end) {
            const limit_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = limit_end };
            ast.limit_tokens = limit_tokens;
            if (limit_tokens.end == limit_tokens.start + 1 and tokens[limit_tokens.start].matchesKeywordTag(.all)) {
                ast.limit_all = true;
            } else {
                ast.limit_expression = try buildGeneratedExpressionAst(alloc, tokens, limit_tokens);
            }
        }
    }
    if (offset_index) |idx| {
        const offset_end = firstOptionalIndex(&[_]?usize{fetch_index}) orelse end;
        if (idx + 1 < offset_end) {
            const offset_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = offset_end };
            ast.offset_tokens = offset_tokens;
            if (generatedOffsetExpressionTokens(tokens, offset_tokens)) |expression_tokens| {
                ast.offset_expression = try buildGeneratedExpressionAst(alloc, tokens, expression_tokens);
            }
        }
    }
    if (fetch_index) |idx| {
        if (idx + 1 < end) {
            const fetch_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = end };
            ast.fetch_tokens = fetch_tokens;
            if (generatedFetchCountTokens(tokens, fetch_tokens)) |count_tokens| {
                ast.fetch_count_tokens = count_tokens;
                ast.fetch_count_expression = try buildGeneratedExpressionAst(alloc, tokens, count_tokens);
            }
        }
    }
}

fn buildGeneratedSubqueryExpressionAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    inner_range: GeneratedSqlTokenRange,
    ast: *GeneratedSqlExpressionAst,
) !void {
    if (inner_range.start >= inner_range.end or inner_range.end > tokens.len) return;
    ast.subquery_read_kind = classifyReadKindInRange(tokens, inner_range);
    const select_index = findTopLevelKeyword(tokens, inner_range.start, inner_range.end, .select) orelse return;
    ast.subquery_select_tokens = .{ .start = select_index, .end = select_index + 1 };

    const set_operation_index = firstTopLevelSetOperation(tokens, select_index + 1, inner_range.end);
    const body_end = set_operation_index orelse inner_range.end;
    var set_operation_tail_start: ?usize = null;
    if (body_end < inner_range.end) {
        const tail_start = generatedSetOperationResultTailStart(tokens, .{ .start = body_end, .end = inner_range.end }) orelse inner_range.end;
        const set_operation_tokens = GeneratedSqlTokenRange{ .start = body_end, .end = tail_start };
        ast.subquery_set_operation_tokens = set_operation_tokens;
        ast.subquery_set_operation = try buildGeneratedSetOperationAstAlloc(alloc, tokens, set_operation_tokens);
        set_operation_tail_start = tail_start;
    }

    var distinct_tokens: ?GeneratedSqlTokenRange = null;
    const projection_start = generatedReadProjectionStartInRange(tokens, select_index, body_end, &distinct_tokens);
    const from_index = findTopLevelKeyword(tokens, projection_start, body_end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, body_end, .where);
    const group_index = findTopLevelKeywordSequence(tokens, projection_start, body_end, .group, .by);
    const having_index = findTopLevelKeyword(tokens, projection_start, body_end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, body_end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, body_end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, body_end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, body_end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, body_end, .fetch);
    const row_lock_index = generatedReadRowLockStart(tokens, projection_start, body_end);

    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
    if (projection_start < projection_end) {
        ast.subquery_projection_tokens = .{ .start = projection_start, .end = projection_end };
        ast.subquery_projection_items = try buildTopLevelListAst(alloc, tokens, ast.subquery_projection_tokens.?, .{ .bare_alias = true });
    }
    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < source_end) ast.subquery_source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < where_end) {
            ast.subquery_where_tokens = .{ .start = idx + 1, .end = where_end };
            ast.subquery_where_expression_kind = generatedExpressionKindForRange(tokens, ast.subquery_where_tokens.?);
            ast.subquery_where_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, ast.subquery_where_tokens.?);
        }
    }
    const tail_start = set_operation_tail_start orelse projection_start;
    try buildGeneratedSubqueryResultTailAst(alloc, tokens, tail_start, inner_range.end, ast);
}

fn buildGeneratedSubqueryResultTailAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    start: usize,
    end: usize,
    ast: *GeneratedSqlExpressionAst,
) !void {
    if (start >= end or end > tokens.len) return;

    const order_index = findTopLevelKeyword(tokens, start, end, .order);
    const limit_index = findTopLevelKeyword(tokens, start, end, .limit);
    const offset_index = findTopLevelKeyword(tokens, start, end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, start, end, .fetch);

    if (order_index) |idx| {
        const order_start = if (idx + 1 < end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const order_end = firstOptionalIndex(&[_]?usize{ limit_index, offset_index, fetch_index }) orelse end;
        if (order_start < order_end) {
            const tail = try ensureGeneratedSubqueryTailAstAlloc(alloc, ast);
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            tail.order_tokens = order_tokens;
            tail.order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
            if (generatedListExpressionTokens(tail.order_items, 0)) |first_tokens| {
                tail.order_first_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, first_tokens);
            }
            if (generatedListExpressionTokens(tail.order_items, tail.order_items.count -| 1)) |last_tokens| {
                tail.order_last_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, last_tokens);
            }
        }
    }
    if (limit_index) |idx| {
        const limit_end = firstOptionalIndex(&[_]?usize{ offset_index, fetch_index }) orelse end;
        if (idx + 1 < limit_end) {
            const tail = try ensureGeneratedSubqueryTailAstAlloc(alloc, ast);
            const limit_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = limit_end };
            tail.limit_tokens = limit_tokens;
            if (limit_tokens.end == limit_tokens.start + 1 and tokens[limit_tokens.start].matchesKeywordTag(.all)) {
                tail.limit_all = true;
            } else {
                tail.limit_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, limit_tokens);
            }
        }
    }
    if (offset_index) |idx| {
        const offset_end = firstOptionalIndex(&[_]?usize{fetch_index}) orelse end;
        if (idx + 1 < offset_end) {
            const tail = try ensureGeneratedSubqueryTailAstAlloc(alloc, ast);
            const offset_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = offset_end };
            tail.offset_tokens = offset_tokens;
            if (generatedOffsetExpressionTokens(tokens, offset_tokens)) |expression_tokens| {
                tail.offset_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, expression_tokens);
            }
        }
    }
    if (fetch_index) |idx| {
        if (idx + 1 < end) {
            const tail = try ensureGeneratedSubqueryTailAstAlloc(alloc, ast);
            const fetch_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = end };
            tail.fetch_tokens = fetch_tokens;
            if (generatedFetchCountTokens(tokens, fetch_tokens)) |count_tokens| {
                tail.fetch_count_tokens = count_tokens;
                tail.fetch_count_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, count_tokens);
            }
        }
    }
}

fn ensureGeneratedSubqueryTailAstAlloc(
    alloc: std.mem.Allocator,
    ast: *GeneratedSqlExpressionAst,
) !*GeneratedSqlSubqueryTailAst {
    if (ast.subquery_tail) |tail| return tail;
    const tail = try alloc.create(GeneratedSqlSubqueryTailAst);
    tail.* = .{};
    ast.subquery_tail = tail;
    return tail;
}

fn generatedOffsetExpressionTokens(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start >= range.end or range.end > tokens.len) return null;
    var end = range.end;
    if (end > range.start and (tokens[end - 1].matchesKeywordTag(.row) or tokens[end - 1].matchesKeywordTag(.rows))) {
        end -= 1;
    }
    if (range.start >= end) return null;
    return .{ .start = range.start, .end = end };
}

fn generatedFetchCountTokens(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start + 2 > range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.first) and !tokens[range.start].matchesKeywordTag(.next)) return null;
    var end = range.end;
    if (end > range.start and tokens[end - 1].matchesKeywordTag(.only)) end -= 1;
    if (end > range.start and (tokens[end - 1].matchesKeywordTag(.row) or tokens[end - 1].matchesKeywordTag(.rows))) {
        end -= 1;
    }
    const start = range.start + 1;
    if (start >= end) return null;
    return .{ .start = start, .end = end };
}

fn generatedReadProjectionStart(
    tokens: []const token_mod.Token,
    select_index: usize,
    body_end: usize,
    ast: *GeneratedSqlReadAst,
) usize {
    const distinct_index = select_index + 1;
    if (distinct_index < body_end and tokens[distinct_index].matchesKeywordTag(.all)) return distinct_index + 1;
    if (distinct_index >= body_end or !tokens[distinct_index].matchesKeywordTag(.distinct)) return distinct_index;
    if (distinct_index + 2 < body_end and tokens[distinct_index + 1].matchesKeywordTag(.on) and tokens[distinct_index + 2].kind == .lparen) {
        if (findMatchingParen(tokens, distinct_index + 2, body_end)) |close| {
            ast.distinct_tokens = .{ .start = distinct_index, .end = close + 1 };
            return close + 1;
        }
    }
    ast.distinct_tokens = .{ .start = distinct_index, .end = distinct_index + 1 };
    return distinct_index + 1;
}

fn generatedDistinctOnExpressionTokens(tokens: []const token_mod.Token, distinct_tokens: ?GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    const range = distinct_tokens orelse return null;
    if (range.start + 4 > range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.distinct) or
        !tokens[range.start + 1].matchesKeywordTag(.on) or
        tokens[range.start + 2].kind != .lparen or
        tokens[range.end - 1].kind != .rparen)
    {
        return null;
    }
    if (range.start + 3 >= range.end - 1) return null;
    return .{ .start = range.start + 3, .end = range.end - 1 };
}

fn buildReadCteAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, final_select_index: usize, ast: *GeneratedSqlReadAst) !void {
    if (final_select_index < 5 or !tokens[0].matchesKeywordTag(.with)) return;
    var index: usize = 1;
    if (index < final_select_index and tokens[index].matchesKeywordTag(.recursive)) {
        ast.cte_recursive = true;
        index += 1;
    }
    if (index >= final_select_index) return;
    ast.cte_list_tokens = .{ .start = index, .end = final_select_index };

    var items: std.ArrayListUnmanaged(GeneratedSqlCteAst) = .empty;
    var items_owned = false;
    defer if (!items_owned) {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    };
    var count: usize = 0;
    while (index < final_select_index) {
        if (tokens[index].kind != .identifier) return;
        if (index + 2 >= final_select_index) return;

        const name_tokens = GeneratedSqlTokenRange{ .start = index, .end = index + 1 };
        const cte = try alloc.create(GeneratedSqlCteAst);
        defer alloc.destroy(cte);
        cte.* = .{ .name_tokens = name_tokens };
        var cte_owned = false;
        errdefer if (!cte_owned) cte.deinit(alloc);

        var cursor = index + 1;
        if (cursor < final_select_index and tokens[cursor].kind == .lparen) {
            const column_close = findMatchingParen(tokens, cursor, final_select_index) orelse return;
            if (column_close >= final_select_index) return;
            cte.column_tokens = .{ .start = cursor, .end = column_close + 1 };
            if (cursor + 1 < column_close) {
                cte.column_name_tokens = .{ .start = cursor + 1, .end = column_close };
                cte.column_names = try buildTopLevelListAst(alloc, tokens, cte.column_name_tokens.?, .{});
            }
            cursor = column_close + 1;
        }

        if (cursor >= final_select_index or !tokens[cursor].matchesKeywordTag(.as)) return;
        cursor += 1;

        if (cursor < final_select_index and tokens[cursor].matchesKeywordTag(.materialized)) {
            cte.materialization_tokens = .{ .start = cursor, .end = cursor + 1 };
            cte.materialization = .materialized;
            cursor += 1;
        } else if (cursor + 1 < final_select_index and
            tokens[cursor].matchesKeywordTag(.not) and
            tokens[cursor + 1].matchesKeywordTag(.materialized))
        {
            cte.materialization_tokens = .{ .start = cursor, .end = cursor + 2 };
            cte.materialization = .not_materialized;
            cursor += 2;
        }

        if (cursor >= final_select_index or tokens[cursor].kind != .lparen) return;
        const close = findMatchingParen(tokens, cursor, final_select_index) orelse return;
        if (close >= final_select_index) return;
        cte.body_tokens = if (cursor + 1 < close)
            .{ .start = cursor + 1, .end = close }
        else
            null;
        if (cte.body_tokens) |body_tokens| try buildReadCteBodyMetadata(alloc, tokens, body_tokens, cte);

        try items.append(alloc, cte.*);
        cte_owned = true;
        count += 1;
        if (count == 1) {
            ast.cte_name_tokens = name_tokens;
            ast.cte_body_tokens = cte.body_tokens;
        }
        ast.cte_last_name_tokens = name_tokens;
        ast.cte_last_body_tokens = cte.body_tokens;

        index = close + 1;
        if (index == final_select_index) break;
        if (tokens[index].kind != .comma) return;
        index += 1;
    }
    ast.cte_items = try items.toOwnedSlice(alloc);
    items_owned = true;
    ast.cte_count = count;
}

fn buildReadCteBodyMetadata(alloc: std.mem.Allocator, tokens: []const token_mod.Token, body_tokens: GeneratedSqlTokenRange, cte: *GeneratedSqlCteAst) !void {
    const select_index = findTopLevelKeyword(tokens, body_tokens.start, body_tokens.end, .select) orelse return;
    if (select_index != body_tokens.start) return;
    cte.body_kind = classifyReadKindInRange(tokens, body_tokens);
    cte.body_select_tokens = .{ .start = select_index, .end = select_index + 1 };

    const body_end = firstTopLevelSetOperation(tokens, select_index + 1, body_tokens.end) orelse body_tokens.end;
    if (body_end < body_tokens.end) {
        const set_operation_tokens = GeneratedSqlTokenRange{ .start = body_end, .end = body_tokens.end };
        cte.body_set_operation_tokens = set_operation_tokens;
        try buildGeneratedSetOperationAstInto(alloc, tokens, set_operation_tokens, &cte.body_set_operation);
    }

    const projection_start = generatedReadProjectionStartInRange(tokens, select_index, body_end, &cte.body_distinct_tokens);
    if (generatedDistinctOnExpressionTokens(tokens, cte.body_distinct_tokens)) |distinct_on_tokens| {
        cte.body_distinct_on_items = try buildTopLevelListAst(alloc, tokens, distinct_on_tokens, .{});
    }
    const from_index = findTopLevelKeyword(tokens, projection_start, body_end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, body_end, .where);
    const group_index = findTopLevelKeywordSequence(tokens, projection_start, body_end, .group, .by);
    const having_index = findTopLevelKeyword(tokens, projection_start, body_end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, body_end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, body_end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, body_end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, body_end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, body_end, .fetch);
    const row_lock_index = generatedReadRowLockStart(tokens, projection_start, body_end);

    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
    if (projection_start < projection_end) {
        const projection_tokens = GeneratedSqlTokenRange{ .start = projection_start, .end = projection_end };
        cte.body_projection_tokens = projection_tokens;
        cte.body_projection_items = try buildTopLevelListAst(alloc, tokens, projection_tokens, .{ .bare_alias = true });
        if (generatedListExpressionTokens(cte.body_projection_items, 0)) |first_tokens| {
            try buildGeneratedExpressionAstInto(alloc, tokens, first_tokens, &cte.body_projection_first_expression);
        }
        if (generatedListExpressionTokens(cte.body_projection_items, cte.body_projection_items.count -| 1)) |last_tokens| {
            try buildGeneratedExpressionAstInto(alloc, tokens, last_tokens, &cte.body_projection_last_expression);
        }
    }
    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < source_end) {
            const source_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = source_end };
            cte.body_source_tokens = source_tokens;
            cte.body_source_antfly_function_items = try buildGeneratedAntflyTableFunctionItemsAst(alloc, tokens, source_tokens);
            cte.body_source_antfly_function_count = cte.body_source_antfly_function_items.len;
            cte.body_source_graph_function_items = try buildGeneratedGraphTableFunctionItemsAst(alloc, tokens, cte.body_source_antfly_function_items);
            cte.body_source_graph_function_count = cte.body_source_graph_function_items.len;
            cte.body_join_items = try buildGeneratedJoinItemsAst(alloc, tokens, source_tokens);
            if (cte.body_join_items.len > 0) {
                cte.body_join_tree_root_index = cte.body_join_items.len - 1;
                cte.body_join_tree_depth = cte.body_join_items.len;

                const first = cte.body_join_items[0];
                cte.body_join_tokens = source_tokens;
                cte.body_join_operator_tokens = first.operator_tokens;
                cte.body_join_kind = first.kind;
                cte.body_join_left_tokens = first.left_tokens;
                cte.body_join_right_tokens = first.right_tokens;
                cte.body_join_predicate_tokens = first.predicate_tokens;
                if (first.predicate_tokens) |predicate_tokens| {
                    try buildGeneratedExpressionAstInto(alloc, tokens, predicate_tokens, &cte.body_join_predicate_expression);
                }
            }
        }
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < where_end) {
            const where_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = where_end };
            cte.body_where_tokens = where_tokens;
            try buildGeneratedExpressionAstInto(alloc, tokens, where_tokens, &cte.body_where_expression);
        }
    }
    if (group_index) |idx| {
        const group_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const group_end = firstOptionalIndex(&[_]?usize{ having_index, window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (group_start < group_end) {
            const group_tokens = GeneratedSqlTokenRange{ .start = group_start, .end = group_end };
            cte.body_group_tokens = group_tokens;
            cte.body_group_items = try buildTopLevelListAst(alloc, tokens, group_tokens, .{});
            if (generatedListExpressionTokens(cte.body_group_items, 0)) |first_tokens| {
                try buildGeneratedExpressionAstInto(alloc, tokens, first_tokens, &cte.body_group_first_expression);
            }
            if (generatedListExpressionTokens(cte.body_group_items, cte.body_group_items.count -| 1)) |last_tokens| {
                try buildGeneratedExpressionAstInto(alloc, tokens, last_tokens, &cte.body_group_last_expression);
            }
        }
    }
    if (having_index) |idx| {
        const having_end = firstOptionalIndex(&[_]?usize{ window_index, order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < having_end) {
            const having_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = having_end };
            cte.body_having_tokens = having_tokens;
            try buildGeneratedExpressionAstInto(alloc, tokens, having_tokens, &cte.body_having_expression);
        }
    }
    if (window_index) |idx| {
        const window_end = firstOptionalIndex(&[_]?usize{ order_index, limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < window_end) {
            const window_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = window_end };
            cte.body_window_tokens = window_tokens;
            cte.body_window_items = try buildGeneratedWindowAstList(alloc, tokens, window_tokens);
            cte.body_window_count = cte.body_window_items.len;
        }
    }
    if (order_index) |idx| {
        const order_start = if (idx + 1 < body_end and tokens[idx + 1].matchesKeywordTag(.by)) idx + 2 else idx + 1;
        const order_end = firstOptionalIndex(&[_]?usize{ limit_index, offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (order_start < order_end) {
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            cte.body_order_tokens = order_tokens;
            cte.body_order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
            if (generatedListExpressionTokens(cte.body_order_items, 0)) |first_tokens| {
                try buildGeneratedExpressionAstInto(alloc, tokens, first_tokens, &cte.body_order_first_expression);
            }
            if (generatedListExpressionTokens(cte.body_order_items, cte.body_order_items.count -| 1)) |last_tokens| {
                try buildGeneratedExpressionAstInto(alloc, tokens, last_tokens, &cte.body_order_last_expression);
            }
        }
    }
    if (limit_index) |idx| {
        const limit_end = firstOptionalIndex(&[_]?usize{ offset_index, fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < limit_end) {
            const limit_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = limit_end };
            cte.body_limit_tokens = limit_tokens;
            if (limit_tokens.end == limit_tokens.start + 1 and tokens[limit_tokens.start].matchesKeywordTag(.all)) {
                cte.body_limit_all = true;
            } else {
                try buildGeneratedExpressionAstInto(alloc, tokens, limit_tokens, &cte.body_limit_expression);
            }
        }
    }
    if (offset_index) |idx| {
        const offset_end = firstOptionalIndex(&[_]?usize{ fetch_index, row_lock_index }) orelse body_end;
        if (idx + 1 < offset_end) {
            const offset_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = offset_end };
            cte.body_offset_tokens = offset_tokens;
            if (generatedOffsetExpressionTokens(tokens, offset_tokens)) |expression_tokens| {
                try buildGeneratedExpressionAstInto(alloc, tokens, expression_tokens, &cte.body_offset_expression);
            }
        }
    }
    if (fetch_index) |idx| {
        const fetch_end = row_lock_index orelse body_end;
        if (idx + 1 < fetch_end) {
            const fetch_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = fetch_end };
            cte.body_fetch_tokens = fetch_tokens;
            if (generatedFetchCountTokens(tokens, fetch_tokens)) |count_tokens| {
                cte.body_fetch_count_tokens = count_tokens;
                try buildGeneratedExpressionAstInto(alloc, tokens, count_tokens, &cte.body_fetch_count_expression);
            }
        }
    }
    if (row_lock_index) |idx| {
        if (idx + 1 < body_end) cte.body_row_lock_tokens = .{ .start = idx, .end = body_end };
    }
}

fn buildGeneratedExpressionAstInto(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    out: *GeneratedSqlExpressionAst,
) !void {
    out.* = .{};
    try buildGeneratedExpressionAstInPlace(alloc, tokens, range, out);
}

fn buildGeneratedSetOperationAstInto(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    out: *GeneratedSqlSetOperationAst,
) !void {
    out.* = .{};
    try buildGeneratedSetOperationAstInPlace(alloc, tokens, range, out);
}

fn buildGeneratedWindowAstList(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) ![]GeneratedSqlWindowAst {
    var windows: std.ArrayListUnmanaged(GeneratedSqlWindowAst) = .empty;
    var windows_owned = false;
    defer if (!windows_owned) {
        for (windows.items) |*window| window.deinit(alloc);
        windows.deinit(alloc);
    };

    var depth: usize = 0;
    var item_start = range.start;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) break;
                depth -= 1;
            },
            .comma => if (depth == 0) {
                if (item_start < index) {
                    try windows.append(alloc, try buildGeneratedWindowAst(alloc, tokens, .{ .start = item_start, .end = index }));
                }
                item_start = index + 1;
            },
            else => {},
        }
    }
    if (item_start < range.end) {
        try windows.append(alloc, try buildGeneratedWindowAst(alloc, tokens, .{ .start = item_start, .end = range.end }));
    }
    const owned = try windows.toOwnedSlice(alloc);
    windows_owned = true;
    return owned;
}

fn buildGeneratedWindowAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    item_tokens: GeneratedSqlTokenRange,
) !GeneratedSqlWindowAst {
    var ast = GeneratedSqlWindowAst{
        .tokens = item_tokens,
        .name_tokens = .{ .start = item_tokens.start, .end = @min(item_tokens.start + 1, item_tokens.end) },
        .definition_tokens = .{ .start = item_tokens.end, .end = item_tokens.end },
    };
    errdefer ast.deinit(alloc);

    const as_index = findTopLevelKeyword(tokens, item_tokens.start, item_tokens.end, .as) orelse return ast;
    if (item_tokens.start < as_index) ast.name_tokens = .{ .start = item_tokens.start, .end = as_index };
    if (as_index + 1 >= item_tokens.end or tokens[as_index + 1].kind != .lparen) return ast;
    const close_index = findMatchingParen(tokens, as_index + 1, item_tokens.end) orelse return ast;
    ast.definition_tokens = .{ .start = as_index + 2, .end = close_index };

    const definition = ast.definition_tokens;
    const partition_index = findTopLevelKeywordSequence(tokens, definition.start, definition.end, .partition, .by);
    const order_index = findTopLevelKeywordSequence(tokens, definition.start, definition.end, .order, .by);
    const rows_index = findTopLevelKeyword(tokens, definition.start, definition.end, .rows);
    const range_index = findTopLevelKeyword(tokens, definition.start, definition.end, .range);
    const frame_index = minOptionalIndex(rows_index, range_index);

    if (partition_index) |idx| {
        const partition_start = idx + 2;
        const partition_end = firstOptionalIndex(&[_]?usize{ order_index, frame_index }) orelse definition.end;
        if (partition_start < partition_end) {
            const partition_tokens = GeneratedSqlTokenRange{ .start = partition_start, .end = partition_end };
            ast.partition_tokens = partition_tokens;
            ast.partition_items = try buildTopLevelListAst(alloc, tokens, partition_tokens, .{});
        }
    }
    if (order_index) |idx| {
        const order_start = idx + 2;
        const order_end = frame_index orelse definition.end;
        if (order_start < order_end) {
            const order_tokens = GeneratedSqlTokenRange{ .start = order_start, .end = order_end };
            ast.order_tokens = order_tokens;
            ast.order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
        }
    }
    if (frame_index) |idx| {
        ast.frame_tokens = .{ .start = idx, .end = definition.end };
        if (generatedWindowFrameExpressionRanges(tokens, ast.frame_tokens.?)) |frame_expressions| {
            if (frame_expressions.start_expression_tokens) |expression_tokens| {
                ast.frame_start_expression_tokens = expression_tokens;
                ast.frame_start_expression_kind = generatedExpressionKindForRange(tokens, expression_tokens);
                ast.frame_start_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, expression_tokens);
            }
            if (frame_expressions.end_expression_tokens) |expression_tokens| {
                ast.frame_end_expression_tokens = expression_tokens;
                ast.frame_end_expression_kind = generatedExpressionKindForRange(tokens, expression_tokens);
                ast.frame_end_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, expression_tokens);
            }
        }
    }
    return ast;
}

const GeneratedWindowFrameExpressionRanges = struct {
    start_expression_tokens: ?GeneratedSqlTokenRange = null,
    end_expression_tokens: ?GeneratedSqlTokenRange = null,
};

fn generatedWindowFrameExpressionRanges(
    tokens: []const token_mod.Token,
    frame_tokens: GeneratedSqlTokenRange,
) ?GeneratedWindowFrameExpressionRanges {
    if (frame_tokens.start + 1 >= frame_tokens.end or frame_tokens.end > tokens.len) return null;
    if (!tokens[frame_tokens.start].matchesKeywordTag(.rows) and !tokens[frame_tokens.start].matchesKeywordTag(.range)) return null;
    const body_start = frame_tokens.start + 1;
    if (tokens[body_start].matchesKeywordTag(.between)) {
        const and_index = findTopLevelKeyword(tokens, body_start + 1, frame_tokens.end, .@"and") orelse return null;
        return .{
            .start_expression_tokens = generatedWindowFrameBoundExpressionTokens(tokens, body_start + 1, and_index),
            .end_expression_tokens = generatedWindowFrameBoundExpressionTokens(tokens, and_index + 1, frame_tokens.end),
        };
    }
    return .{
        .start_expression_tokens = generatedWindowFrameBoundExpressionTokens(tokens, body_start, frame_tokens.end),
    };
}

fn generatedWindowFrameBoundExpressionTokens(
    tokens: []const token_mod.Token,
    start: usize,
    end: usize,
) ?GeneratedSqlTokenRange {
    if (start >= end or end > tokens.len) return null;
    if (tokens[start].matchesKeywordTag(.unbounded) or tokens[start].matchesKeywordTag(.current)) return null;
    const preceding_index = findTopLevelKeyword(tokens, start, end, .preceding);
    const following_index = findTopLevelKeyword(tokens, start, end, .following);
    const bound_index = firstOptionalIndex(&[_]?usize{ preceding_index, following_index }) orelse return null;
    if (start >= bound_index) return null;
    return .{ .start = start, .end = bound_index };
}

fn generatedReadProjectionStartInRange(
    tokens: []const token_mod.Token,
    select_index: usize,
    body_end: usize,
    distinct_tokens: *?GeneratedSqlTokenRange,
) usize {
    const distinct_index = select_index + 1;
    if (distinct_index < body_end and tokens[distinct_index].matchesKeywordTag(.all)) return distinct_index + 1;
    if (distinct_index >= body_end or !tokens[distinct_index].matchesKeywordTag(.distinct)) return distinct_index;
    if (distinct_index + 2 < body_end and tokens[distinct_index + 1].matchesKeywordTag(.on) and tokens[distinct_index + 2].kind == .lparen) {
        if (findMatchingParen(tokens, distinct_index + 2, body_end)) |close| {
            distinct_tokens.* = .{ .start = distinct_index, .end = close + 1 };
            return close + 1;
        }
    }
    distinct_tokens.* = .{ .start = distinct_index, .end = distinct_index + 1 };
    return distinct_index + 1;
}

fn buildGeneratedSetOperationAstInPlace(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    out_ast: *GeneratedSqlSetOperationAst,
) !void {
    out_ast.* = GeneratedSqlSetOperationAst{ .tokens = range };
    errdefer out_ast.deinit(alloc);
    if (range.start >= range.end or range.end > tokens.len) return;

    out_ast.operator_tokens = .{ .start = range.start, .end = range.start + 1 };
    if (tokens[range.start].matchesKeywordTag(.@"union")) {
        out_ast.kind = .@"union";
    } else if (tokens[range.start].matchesKeywordTag(.intersect)) {
        out_ast.kind = .intersect;
    } else if (tokens[range.start].matchesKeywordTag(.except)) {
        out_ast.kind = .except;
    } else {
        return;
    }

    var right_start = range.start + 1;
    if (right_start < range.end and tokens[right_start].matchesKeywordTag(.all)) {
        out_ast.all_tokens = .{ .start = right_start, .end = right_start + 1 };
        right_start += 1;
    }
    if (right_start >= range.end or !tokens[right_start].matchesKeywordTag(.select)) return;
    out_ast.right_query_tokens = .{ .start = right_start, .end = range.end };
    out_ast.right_select_tokens = .{ .start = right_start, .end = right_start + 1 };

    const projection_start = generatedReadProjectionStartInRange(tokens, right_start, range.end, &out_ast.right_distinct_tokens);
    if (generatedDistinctOnExpressionTokens(tokens, out_ast.right_distinct_tokens)) |distinct_on_tokens| {
        out_ast.right_distinct_on_items = try buildTopLevelListAst(alloc, tokens, distinct_on_tokens, .{});
    }
    const from_index = findTopLevelKeyword(tokens, projection_start, range.end, .from);
    const where_index = findTopLevelKeyword(tokens, projection_start, range.end, .where);
    const group_index = findTopLevelKeywordSequence(tokens, projection_start, range.end, .group, .by);
    const having_index = findTopLevelKeyword(tokens, projection_start, range.end, .having);
    const window_index = findTopLevelKeyword(tokens, projection_start, range.end, .window);
    const order_index = findTopLevelKeyword(tokens, projection_start, range.end, .order);
    const limit_index = findTopLevelKeyword(tokens, projection_start, range.end, .limit);
    const offset_index = findTopLevelKeyword(tokens, projection_start, range.end, .offset);
    const fetch_index = findTopLevelKeyword(tokens, projection_start, range.end, .fetch);

    const projection_end = firstOptionalIndex(&[_]?usize{ from_index, where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse range.end;
    if (projection_start < projection_end) {
        const projection_tokens = GeneratedSqlTokenRange{ .start = projection_start, .end = projection_end };
        out_ast.right_projection_tokens = projection_tokens;
        out_ast.right_projection_items = try buildTopLevelListAst(alloc, tokens, projection_tokens, .{ .bare_alias = true });
        if (generatedListExpressionTokens(out_ast.right_projection_items, 0)) |first_tokens| {
            try buildGeneratedExpressionAstInPlace(alloc, tokens, first_tokens, &out_ast.right_projection_first_expression);
        }
        if (generatedListExpressionTokens(out_ast.right_projection_items, out_ast.right_projection_items.count -| 1)) |last_tokens| {
            try buildGeneratedExpressionAstInPlace(alloc, tokens, last_tokens, &out_ast.right_projection_last_expression);
        }
    }
    if (from_index) |idx| {
        const source_end = firstOptionalIndex(&[_]?usize{ where_index, group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse range.end;
        if (idx + 1 < source_end) out_ast.right_source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = firstOptionalIndex(&[_]?usize{ group_index, having_index, window_index, order_index, limit_index, offset_index, fetch_index }) orelse range.end;
        if (idx + 1 < where_end) {
            const where_tokens = GeneratedSqlTokenRange{ .start = idx + 1, .end = where_end };
            out_ast.right_where_tokens = where_tokens;
            try buildGeneratedExpressionAstInPlace(alloc, tokens, where_tokens, &out_ast.right_where_expression);
        }
    }
}

fn buildGeneratedSetOperationAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !GeneratedSqlSetOperationAst {
    var ast = GeneratedSqlSetOperationAst{};
    try buildGeneratedSetOperationAstInPlace(alloc, tokens, range, &ast);
    return ast;
}

fn buildGeneratedSetOperationAstAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !*GeneratedSqlSetOperationAst {
    const node = try alloc.create(GeneratedSqlSetOperationAst);
    errdefer alloc.destroy(node);
    try buildGeneratedSetOperationAstInPlace(alloc, tokens, range, node);
    return node;
}

fn classifyReadKindInRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) GeneratedSqlReadKind {
    if (range.start < range.end and tokens[range.start].matchesKeywordTag(.with)) return .cte;
    if (firstTopLevelSetOperation(tokens, range.start + 1, range.end) != null) return .set_operation;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.lateral)) return .lateral;
    }
    index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.over)) return .window;
    }
    if (range.start + 1 < range.end and tokens[range.start].matchesKeywordTag(.select) and tokens[range.start + 1].matchesKeywordTag(.distinct)) {
        if (range.start + 2 < range.end and tokens[range.start + 2].matchesKeywordTag(.on)) return .query;
        return .aggregate;
    }
    if (generatedReadRangeHasAggregateShape(tokens, range)) return .aggregate;
    index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(.join)) return .join;
    }
    return .query;
}

fn generatedReadRangeHasAggregateShape(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        const token = tokens[index];
        switch (token.kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0) {
                if (token.matchesKeywordTag(.group) or token.matchesKeywordTag(.having)) return true;
                if (index + 1 < range.end and tokens[index + 1].kind == .lparen and generatedSqlAggregateFunctionName(token)) return true;
            },
            else => {},
        }
    }
    return false;
}

fn generatedSqlAggregateFunctionName(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.count) or
        token.matchesKeywordTag(.sum) or
        token.matchesKeywordTag(.avg) or
        token.matchesKeywordTag(.min) or
        token.matchesKeywordTag(.max) or
        token.matchesKeywordTag(.bool_or) or
        token.matchesKeywordTag(.bool_and) or
        token.matchesKeywordTag(.array_agg) or
        token.matchesKeywordTag(.string_agg);
}

fn generatedGraphTableFunctionKind(token: token_mod.Token) ?GeneratedSqlGraphTableFunctionKind {
    return generatedGraphTableFunctionKindFromAntfly(generatedAntflyTableFunctionKind(token) orelse return null);
}

fn generatedGraphTableFunctionKindFromAntfly(kind: GeneratedSqlAntflyTableFunctionKind) ?GeneratedSqlGraphTableFunctionKind {
    return switch (kind) {
        .graph_traverse => .traverse,
        .graph_neighbors => .neighbors,
        .graph_shortest_path => .shortest_path,
        .graph_k_shortest_paths => .k_shortest_paths,
        .graph_match => .match,
        .graph_metric => .metric,
        .graph_metric_rerank => .metric_rerank,
        else => null,
    };
}

fn generatedAntflyTableFunctionKind(token: token_mod.Token) ?GeneratedSqlAntflyTableFunctionKind {
    if (token.matchesQualifiedKeywordTag("antfly", .full_text_search)) return .full_text_search;
    if (token.matchesQualifiedKeywordTag("antfly", .semantic_search)) return .semantic_search;
    if (token.matchesQualifiedKeywordTag("antfly", .vector_search)) return .vector_search;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_traverse)) return .graph_traverse;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_neighbors)) return .graph_neighbors;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_shortest_path)) return .graph_shortest_path;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_k_shortest_paths)) return .graph_k_shortest_paths;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_match)) return .graph_match;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric)) return .graph_metric;
    if (token.matchesQualifiedKeywordTag("antfly", .graph_metric_rerank)) return .graph_metric_rerank;
    if (token.matchesQualifiedKeywordTag("antfly", .hybrid_search)) return .hybrid_search;
    return null;
}

fn buildGeneratedReadGraphSourceAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    source_tokens: GeneratedSqlTokenRange,
    ast: *GeneratedSqlReadAst,
) !void {
    ast.source_antfly_function_items = try buildGeneratedAntflyTableFunctionItemsAst(alloc, tokens, source_tokens);
    ast.source_antfly_function_count = ast.source_antfly_function_items.len;
    ast.source_graph_function_items = try buildGeneratedGraphTableFunctionItemsAst(alloc, tokens, ast.source_antfly_function_items);
    ast.source_graph_function_count = ast.source_graph_function_items.len;
    if (ast.source_graph_function_items.len == 0) return;

    const first = ast.source_graph_function_items[0];
    ast.source_graph_function_tokens = first.tokens;
    ast.source_graph_function_name_tokens = first.name_tokens;
    ast.source_graph_function_argument_tokens = first.argument_tokens;
    ast.source_graph_function_kind = first.kind;
}

fn buildGeneratedAntflyTableFunctionItemsAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    source_tokens: GeneratedSqlTokenRange,
) ![]GeneratedSqlAntflyTableFunctionAst {
    if (source_tokens.start >= source_tokens.end or source_tokens.end > tokens.len) return &.{};
    var items: std.ArrayListUnmanaged(GeneratedSqlAntflyTableFunctionAst) = .empty;
    var items_owned = false;
    defer if (!items_owned) {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    };

    var index = source_tokens.start;
    var depth: usize = 0;
    while (index < source_tokens.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => {
                depth += 1;
                continue;
            },
            .rparen => {
                depth -|= 1;
                continue;
            },
            else => {},
        }
        if (depth != 0) continue;
        var function_start = index;
        if (tokens[function_start].matchesKeywordTag(.lateral)) {
            if (function_start + 1 >= source_tokens.end) continue;
            function_start += 1;
        }
        const kind = generatedAntflyTableFunctionKind(tokens[function_start]) orelse continue;
        if (function_start + 1 >= source_tokens.end or tokens[function_start + 1].kind != .lparen) continue;
        const close_index = findMatchingParen(tokens, function_start + 1, source_tokens.end) orelse continue;
        const argument_tokens = GeneratedSqlTokenRange{ .start = function_start + 2, .end = close_index };
        try items.ensureUnusedCapacity(alloc, 1);
        var item = GeneratedSqlAntflyTableFunctionAst{
            .tokens = .{ .start = function_start, .end = close_index + 1 },
            .name_tokens = .{ .start = function_start, .end = function_start + 1 },
            .argument_tokens = argument_tokens,
            .kind = kind,
            .argument_items = try buildGeneratedNamedArgumentItemsAst(alloc, tokens, argument_tokens),
        };
        item.argument_count = item.argument_items.len;
        items.appendAssumeCapacity(item);
        index = close_index;
    }

    if (items.items.len == 0) return &.{};
    const owned = try items.toOwnedSlice(alloc);
    items_owned = true;
    return owned;
}

fn buildGeneratedNamedArgumentItemsAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) ![]GeneratedSqlNamedArgumentAst {
    if (range.start >= range.end) return &.{};
    var items: std.ArrayListUnmanaged(GeneratedSqlNamedArgumentAst) = .empty;
    errdefer items.deinit(alloc);

    var item_start = range.start;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return error.UnsupportedSqlShape;
                depth -= 1;
            },
            .comma => if (depth == 0) {
                try appendGeneratedNamedArgumentItemAst(alloc, &items, tokens, .{ .start = item_start, .end = index });
                item_start = index + 1;
            },
            else => {},
        }
    }
    if (depth != 0) return error.UnsupportedSqlShape;
    try appendGeneratedNamedArgumentItemAst(alloc, &items, tokens, .{ .start = item_start, .end = range.end });

    if (items.items.len == 0) return &.{};
    return try items.toOwnedSlice(alloc);
}

fn appendGeneratedNamedArgumentItemAst(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(GeneratedSqlNamedArgumentAst),
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end) return error.UnsupportedSqlShape;
    var depth: usize = 0;
    var operator_start: ?usize = null;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return error.UnsupportedSqlShape;
                depth -= 1;
            },
            .eq => if (depth == 0) {
                operator_start = index;
                break;
            },
            else => {},
        }
    }
    const eq_index = operator_start orelse return error.UnsupportedSqlShape;
    if (eq_index != range.start + 1 or tokens[range.start].kind != .identifier) return error.UnsupportedSqlShape;
    var value_start = eq_index + 1;
    if (value_start < range.end and tokens[value_start].kind == .gt) value_start += 1;
    if (value_start >= range.end) return error.UnsupportedSqlShape;
    try items.append(alloc, .{
        .tokens = range,
        .name_tokens = .{ .start = range.start, .end = eq_index },
        .operator_tokens = .{ .start = eq_index, .end = value_start },
        .value_tokens = .{ .start = value_start, .end = range.end },
    });
}

fn buildGeneratedGraphTableFunctionItemsAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    antfly_items: []const GeneratedSqlAntflyTableFunctionAst,
) ![]GeneratedSqlGraphTableFunctionAst {
    var items: std.ArrayListUnmanaged(GeneratedSqlGraphTableFunctionAst) = .empty;
    errdefer items.deinit(alloc);
    for (antfly_items) |item| {
        const kind = generatedGraphTableFunctionKindFromAntfly(item.kind) orelse continue;
        try items.append(alloc, buildGeneratedGraphTableFunctionItemAst(tokens, item, kind));
    }
    if (items.items.len == 0) return &.{};
    return try items.toOwnedSlice(alloc);
}

fn buildGeneratedGraphTableFunctionItemAst(
    tokens: []const token_mod.Token,
    item: GeneratedSqlAntflyTableFunctionAst,
    kind: GeneratedSqlGraphTableFunctionKind,
) GeneratedSqlGraphTableFunctionAst {
    return .{
        .tokens = item.tokens,
        .name_tokens = item.name_tokens,
        .argument_tokens = item.argument_tokens,
        .kind = kind,
        .argument_count = item.argument_count,
        .table_name_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "table_name", "table" }),
        .index_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "index", "graph_index" }),
        .start_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "start", "start_node" }),
        .start_result_ref_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "start_result_ref", "result_ref" }),
        .target_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "target", "target_node" }),
        .target_result_ref_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{"target_result_ref"}),
        .pattern_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{"pattern"}),
        .return_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "return", "return_aliases" }),
        .metric_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "metric", "graph_metric" }),
        .query_value_tokens = generatedNamedArgumentValueByNames(tokens, item, &.{ "query", "text" }),
    };
}

fn generatedNamedArgumentValueByNames(
    tokens: []const token_mod.Token,
    item: GeneratedSqlAntflyTableFunctionAst,
    names: []const []const u8,
) ?GeneratedSqlTokenRange {
    for (item.argument_items) |argument| {
        if (argument.name_tokens.end != argument.name_tokens.start + 1) continue;
        for (names) |name| {
            if (std.ascii.eqlIgnoreCase(tokens[argument.name_tokens.start].text, name)) return argument.value_tokens;
        }
    }
    return null;
}

fn buildReadJoinAst(alloc: std.mem.Allocator, tokens: []const token_mod.Token, source_tokens: GeneratedSqlTokenRange, ast: *GeneratedSqlReadAst) !void {
    ast.join_items = try buildGeneratedJoinItemsAst(alloc, tokens, source_tokens);
    if (ast.join_items.len == 0) return;

    ast.join_tree_root_index = ast.join_items.len - 1;
    ast.join_tree_depth = ast.join_items.len;

    const first = ast.join_items[0];
    ast.join_tokens = source_tokens;
    ast.join_operator_tokens = first.operator_tokens;
    ast.join_kind = first.kind;
    ast.join_left_tokens = first.left_tokens;
    ast.join_right_tokens = first.right_tokens;
    ast.join_predicate_tokens = first.predicate_tokens;
    if (first.predicate_tokens) |predicate_tokens| {
        try buildGeneratedExpressionAstInto(alloc, tokens, predicate_tokens, &ast.join_predicate_expression);
    }
}

fn buildGeneratedJoinItemsAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    source_tokens: GeneratedSqlTokenRange,
) ![]GeneratedSqlJoinAst {
    var items: std.ArrayListUnmanaged(GeneratedSqlJoinAst) = .empty;
    var items_owned = false;
    defer if (!items_owned) {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    };

    var scan = source_tokens.start;
    while (findTopLevelKeyword(tokens, scan, source_tokens.end, .join)) |join_index| {
        const condition = generatedJoinCondition(tokens, join_index + 1, source_tokens.end) orelse return &.{};
        const operator = generatedJoinOperator(tokens, source_tokens, join_index) orelse return &.{};
        if (source_tokens.start >= operator.tokens.start or join_index + 1 >= condition.keyword_index) return &.{};

        const next_join_index = findTopLevelKeyword(tokens, condition.end, source_tokens.end, .join);
        const item_end = if (next_join_index) |next_join|
            (generatedJoinOperator(tokens, source_tokens, next_join) orelse return &.{}).tokens.start
        else
            condition.end;

        try items.ensureUnusedCapacity(alloc, 1);
        const tree_index = items.items.len;
        var item = GeneratedSqlJoinAst{
            .tokens = .{ .start = source_tokens.start, .end = item_end },
            .operator_tokens = operator.tokens,
            .kind = operator.kind,
            .tree_index = tree_index,
            .tree_depth = tree_index + 1,
            .left_child_index = if (tree_index == 0) null else tree_index - 1,
            .left_tokens = .{ .start = source_tokens.start, .end = operator.tokens.start },
            .right_tokens = .{ .start = join_index + 1, .end = condition.keyword_index },
            .condition_kind = condition.kind,
            .condition_tokens = condition.tokens,
        };
        switch (condition.kind) {
            .on => {
                item.predicate_tokens = condition.body_tokens;
                try buildGeneratedExpressionAstInto(alloc, tokens, condition.body_tokens, &item.predicate_expression);
            },
            .using => {
                item.using_tokens = condition.tokens;
                item.using_column_tokens = condition.body_tokens;
                item.using_columns = try buildTopLevelListAst(alloc, tokens, condition.body_tokens, .{});
            },
        }
        items.appendAssumeCapacity(item);

        scan = item_end;
        if (scan >= source_tokens.end) break;
    }

    if (items.items.len == 0) return &.{};
    const owned = try items.toOwnedSlice(alloc);
    items_owned = true;
    return owned;
}

const GeneratedJoinCondition = struct {
    kind: GeneratedSqlJoinConditionKind,
    keyword_index: usize,
    tokens: GeneratedSqlTokenRange,
    body_tokens: GeneratedSqlTokenRange,
    end: usize,
};

fn generatedJoinCondition(tokens: []const token_mod.Token, start: usize, end: usize) ?GeneratedJoinCondition {
    const on_index = findTopLevelKeyword(tokens, start, end, .on);
    const using_index = findTopLevelKeyword(tokens, start, end, .using);
    const condition_index = blk: {
        if (on_index) |on| {
            if (using_index) |using| break :blk if (on < using) on else using;
            break :blk on;
        }
        break :blk using_index orelse return null;
    };
    if (tokens[condition_index].matchesKeywordTag(.on)) {
        const next_join_index = findTopLevelKeyword(tokens, condition_index + 1, end, .join);
        const condition_end = if (next_join_index) |next_join| next_join else end;
        if (condition_index + 1 >= condition_end) return null;
        return .{
            .kind = .on,
            .keyword_index = condition_index,
            .tokens = .{ .start = condition_index, .end = condition_end },
            .body_tokens = .{ .start = condition_index + 1, .end = condition_end },
            .end = condition_end,
        };
    }
    if (condition_index + 2 >= end or tokens[condition_index + 1].kind != .lparen) return null;
    const close = findMatchingParen(tokens, condition_index + 1, end) orelse return null;
    if (condition_index + 2 >= close) return null;
    return .{
        .kind = .using,
        .keyword_index = condition_index,
        .tokens = .{ .start = condition_index, .end = close + 1 },
        .body_tokens = .{ .start = condition_index + 2, .end = close },
        .end = close + 1,
    };
}

const GeneratedJoinOperator = struct {
    tokens: GeneratedSqlTokenRange,
    kind: GeneratedSqlJoinKind,
};

fn generatedJoinOperator(tokens: []const token_mod.Token, source_tokens: GeneratedSqlTokenRange, join_index: usize) ?GeneratedJoinOperator {
    if (join_index >= source_tokens.end or !tokens[join_index].matchesKeywordTag(.join)) return null;
    if (join_index >= source_tokens.start + 2 and tokens[join_index - 1].matchesKeywordTag(.outer)) {
        if (tokens[join_index - 2].matchesKeywordTag(.left)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .left,
        };
        if (tokens[join_index - 2].matchesKeywordTag(.right)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .right,
        };
        if (tokens[join_index - 2].matchesKeywordTag(.full)) return .{
            .tokens = .{ .start = join_index - 2, .end = join_index + 1 },
            .kind = .full,
        };
    }
    if (join_index >= source_tokens.start + 1) {
        if (tokens[join_index - 1].matchesKeywordTag(.inner)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .inner,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.left)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .left,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.right)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .right,
        };
        if (tokens[join_index - 1].matchesKeywordTag(.full)) return .{
            .tokens = .{ .start = join_index - 1, .end = join_index + 1 },
            .kind = .full,
        };
    }
    return .{
        .tokens = .{ .start = join_index, .end = join_index + 1 },
        .kind = .inner,
    };
}

fn buildInsertDmlAst(tokens: []const token_mod.Token, start: usize, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (start + 3 >= end or !tokens[start + 1].matchesKeywordTag(.into)) return;
    var target_index: usize = start + 2;
    if (target_index < end and tokens[target_index].matchesKeywordTag(.only)) target_index += 1;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, target_index, end);
    var index: usize = if (ast.target_table_tokens) |target| target.end else start + 3;
    if (index + 1 < end and tokens[index].matchesKeywordTag(.as) and tokens[index + 1].kind == .identifier) {
        index += 2;
    }
    if (index + 1 < end and tokens[index].matchesKeywordTag(.default) and tokens[index + 1].matchesKeywordTag(.values)) {
        ast.default_values = true;
        const conflict_index = findTopLevelKeywordSequence(tokens, index + 2, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, index + 2, end, .returning) orelse end;
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
        return;
    }
    if (index < end and tokens[index].kind == .lparen) {
        if (findMatchingParen(tokens, index, end)) |close| {
            ast.insert_columns_tokens = .{ .start = index, .end = close + 1 };
            index = close + 1;
        }
    }
    if (findTopLevelKeyword(tokens, index, end, .values)) |values_index| {
        const conflict_index = findTopLevelKeywordSequence(tokens, values_index + 1, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, values_index + 1, end, .returning) orelse end;
        const values_end = conflict_index orelse returning_index;
        ast.values_tokens = .{ .start = values_index + 1, .end = values_end };
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
    } else if (findTopLevelKeyword(tokens, index, end, .select)) |select_index| {
        const conflict_index = findTopLevelKeywordSequence(tokens, select_index + 1, end, .on, .conflict);
        const returning_index = findTopLevelKeyword(tokens, select_index + 1, end, .returning) orelse end;
        const source_end = conflict_index orelse returning_index;
        ast.source_tokens = .{ .start = select_index, .end = source_end };
        if (conflict_index) |idx| {
            const conflict_end = if (returning_index < end) returning_index else end;
            if (idx + 1 < conflict_end) ast.conflict_tokens = .{ .start = idx + 1, .end = conflict_end };
        }
        if (returning_index < end) ast.returning_tokens = .{ .start = returning_index + 1, .end = end };
    }
}

fn buildUpdateDmlAst(tokens: []const token_mod.Token, start: usize, end: usize, ast: *GeneratedSqlDmlAst) void {
    var target_index: usize = start + 1;
    if (target_index < end and tokens[target_index].matchesKeywordTag(.only)) target_index += 1;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, target_index, end);
    var search_start: usize = if (ast.target_table_tokens) |target| target.end else start + 2;
    if (search_start + 1 < end and tokens[search_start].matchesKeywordTag(.as) and tokens[search_start + 1].kind == .identifier) {
        search_start += 2;
    }
    const set_index = findTopLevelKeyword(tokens, search_start, end, .set) orelse return;
    const from_index = findTopLevelKeyword(tokens, set_index + 1, end, .from);
    const where_index = findTopLevelKeyword(tokens, set_index + 1, end, .where);
    const returning_index = findTopLevelKeyword(tokens, set_index + 1, end, .returning);
    const assignments_end = minOptionalIndex(from_index, minOptionalIndex(where_index, returning_index) orelse end) orelse end;
    if (set_index + 1 < assignments_end) ast.assignments_tokens = .{ .start = set_index + 1, .end = assignments_end };
    if (from_index) |idx| {
        const source_end = minOptionalIndex(where_index, returning_index) orelse end;
        if (idx + 1 < source_end) ast.source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = returning_index orelse end;
        if (idx + 1 < where_end) ast.where_tokens = .{ .start = idx + 1, .end = where_end };
    }
    if (returning_index) |idx| {
        if (idx + 1 < end) ast.returning_tokens = .{ .start = idx + 1, .end = end };
    }
}

fn buildDeleteDmlAst(tokens: []const token_mod.Token, start: usize, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (start + 2 >= end or !tokens[start + 1].matchesKeywordTag(.from)) return;
    var target_index = start + 2;
    if (target_index < end and tokens[target_index].matchesKeywordTag(.only)) target_index += 1;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, target_index, end);
    const scan_start = if (ast.target_table_tokens) |target| target.end else target_index;
    const using_index = findTopLevelKeyword(tokens, scan_start, end, .using);
    const where_index = findTopLevelKeyword(tokens, scan_start, end, .where);
    const returning_index = findTopLevelKeyword(tokens, scan_start, end, .returning);
    if (using_index) |idx| {
        const source_end = minOptionalIndex(where_index, returning_index) orelse end;
        if (idx + 1 < source_end) ast.source_tokens = .{ .start = idx + 1, .end = source_end };
    }
    if (where_index) |idx| {
        const where_end = returning_index orelse end;
        if (idx + 1 < where_end) ast.where_tokens = .{ .start = idx + 1, .end = where_end };
    }
    if (returning_index) |idx| {
        if (idx + 1 < end) ast.returning_tokens = .{ .start = idx + 1, .end = end };
    }
}

fn buildTruncateDmlAst(tokens: []const token_mod.Token, start: usize, end: usize, ast: *GeneratedSqlDmlAst) void {
    var index: usize = start + 1;
    if (index < end and tokens[index].matchesKeywordTag(.table)) index += 1;
    if (index < end and tokens[index].matchesKeywordTag(.only)) index += 1;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, index, end);
    if (ast.target_table_tokens) |target| index = target.end;

    const option_index = firstTopLevelTruncateOption(tokens, index, end) orelse end;
    if (index < option_index and tokens[index].kind == .comma) {
        ast.additional_target_tokens = .{ .start = index, .end = option_index };
    }
    ast.restart_identity = generatedTruncateHasRestartIdentity(tokens, option_index, end);
    ast.cascade = findTopLevelKeyword(tokens, option_index, end, .cascade) != null;
}

fn buildMergeDmlAst(tokens: []const token_mod.Token, start: usize, end: usize, ast: *GeneratedSqlDmlAst) void {
    if (start + 3 >= end or !tokens[start + 1].matchesKeywordTag(.into)) return;
    ast.target_table_tokens = generatedSingleTokenRangeIfIdentifier(tokens, start + 2, end);
    if (findTopLevelKeyword(tokens, start + 3, end, .using)) |using_index| {
        const on_index = findTopLevelKeyword(tokens, using_index + 1, end, .on) orelse end;
        if (using_index + 1 < on_index) ast.source_tokens = .{ .start = using_index + 1, .end = on_index };
        if (on_index + 1 < end) ast.where_tokens = .{ .start = on_index + 1, .end = end };
    }
}

fn firstTopLevelTruncateOption(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var best: ?usize = null;
    const candidates = [_]token_mod.TokenKeyword{ .restart, .@"continue", .identity, .cascade, .restrict };
    for (candidates) |keyword| {
        if (findTopLevelKeyword(tokens, start, end, keyword)) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn generatedTruncateHasRestartIdentity(tokens: []const token_mod.Token, start: usize, end: usize) bool {
    return start + 1 < end and
        tokens[start].matchesKeywordTag(.restart) and
        tokens[start + 1].matchesKeywordTag(.identity);
}

fn firstTopLevelSetOperation(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var best: ?usize = null;
    const candidates = [_]token_mod.TokenKeyword{ .@"union", .intersect, .except };
    for (candidates) |keyword| {
        if (findTopLevelKeyword(tokens, start, end, keyword)) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn consumeGeneratedIfNotExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 2 >= end) return false;
    if (!tokens[index.*].matchesKeywordTag(.@"if") or
        !tokens[index.* + 1].matchesKeywordTag(.not) or
        !tokens[index.* + 2].matchesKeywordTag(.exists))
    {
        return false;
    }
    index.* += 3;
    return true;
}

fn consumeGeneratedIfExists(tokens: []const token_mod.Token, index: *usize, end: usize) bool {
    if (index.* + 1 >= end) return false;
    if (!tokens[index.*].matchesKeywordTag(.@"if") or
        !tokens[index.* + 1].matchesKeywordTag(.exists))
    {
        return false;
    }
    index.* += 2;
    return true;
}

fn generatedSingleTokenRangeIfIdentifier(tokens: []const token_mod.Token, index: usize, end: usize) ?GeneratedSqlTokenRange {
    if (index >= end or tokens[index].kind != .identifier) return null;
    return .{ .start = index, .end = index + 1 };
}

fn generatedQualifiedNameRange(tokens: []const token_mod.Token, index: usize, end: usize) ?GeneratedSqlTokenRange {
    return generatedSingleTokenRangeIfIdentifier(tokens, index, end);
}

const GeneratedSqlListOptions = struct {
    bare_alias: bool = false,
    order_modifiers: bool = false,
};

fn buildTopLevelListAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    options: GeneratedSqlListOptions,
) !GeneratedSqlListAst {
    var ast = GeneratedSqlListAst{};
    if (range.start >= range.end or range.end > tokens.len) return ast;

    var items: std.ArrayListUnmanaged(GeneratedSqlTokenRange) = .empty;
    errdefer items.deinit(alloc);
    var item_start = range.start;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) {
                    items.deinit(alloc);
                    return ast;
                }
                depth -= 1;
            },
            .comma => if (depth == 0) {
                try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = index });
                item_start = index + 1;
            },
            else => {},
        }
    }
    try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = range.end });
    ast.items = try items.toOwnedSlice(alloc);
    errdefer {
        alloc.free(ast.items);
        ast.items = &.{};
    }
    if (ast.items.len > 0) {
        ast.expression_items = try alloc.alloc(GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.expression_items);
            ast.expression_items = &.{};
        }
        ast.alias_items = try alloc.alloc(?GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.alias_items);
            ast.alias_items = &.{};
        }
        ast.alias_name_items = try alloc.alloc(?GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.alias_name_items);
            ast.alias_name_items = &.{};
        }
        ast.direction_items = try alloc.alloc(?GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.direction_items);
            ast.direction_items = &.{};
        }
        ast.directions = try alloc.alloc(?GeneratedSqlOrderDirection, ast.items.len);
        errdefer {
            alloc.free(ast.directions);
            ast.directions = &.{};
        }
        ast.order_using_operator_items = try alloc.alloc(?GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.order_using_operator_items);
            ast.order_using_operator_items = &.{};
        }
        ast.nulls_order_items = try alloc.alloc(?GeneratedSqlTokenRange, ast.items.len);
        errdefer {
            alloc.free(ast.nulls_order_items);
            ast.nulls_order_items = &.{};
        }
        ast.nulls_orders = try alloc.alloc(?GeneratedSqlNullsOrder, ast.items.len);
        errdefer {
            alloc.free(ast.nulls_orders);
            ast.nulls_orders = &.{};
        }
        for (ast.items, 0..) |item, item_index| {
            const aliased = generatedAliasedListItem(tokens, item, options);
            ast.expression_items[item_index] = aliased.expression_tokens;
            ast.alias_items[item_index] = aliased.alias_tokens;
            ast.alias_name_items[item_index] = aliased.alias_name_tokens;
            ast.direction_items[item_index] = aliased.direction_tokens;
            ast.directions[item_index] = aliased.direction;
            ast.order_using_operator_items[item_index] = aliased.order_using_operator_tokens;
            ast.nulls_order_items[item_index] = aliased.nulls_order_tokens;
            ast.nulls_orders[item_index] = aliased.nulls_order;
        }
        ast.expressions = try alloc.alloc(GeneratedSqlExpressionAst, ast.items.len);
        var expression_count: usize = 0;
        errdefer {
            for (ast.expressions[0..expression_count]) |*expression| expression.deinit(alloc);
            alloc.free(ast.expressions);
            ast.expressions = &.{};
        }
        for (ast.expression_items) |item| {
            try buildGeneratedExpressionAstInPlace(alloc, tokens, item, &ast.expressions[expression_count]);
            expression_count += 1;
        }
    }
    return ast;
}

const GeneratedAliasedListItem = struct {
    expression_tokens: GeneratedSqlTokenRange,
    alias_tokens: ?GeneratedSqlTokenRange = null,
    alias_name_tokens: ?GeneratedSqlTokenRange = null,
    direction_tokens: ?GeneratedSqlTokenRange = null,
    direction: ?GeneratedSqlOrderDirection = null,
    order_using_operator_tokens: ?GeneratedSqlTokenRange = null,
    nulls_order_tokens: ?GeneratedSqlTokenRange = null,
    nulls_order: ?GeneratedSqlNullsOrder = null,
};

fn generatedAliasedListItem(
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    options: GeneratedSqlListOptions,
) GeneratedAliasedListItem {
    var result = GeneratedAliasedListItem{ .expression_tokens = range };
    if (range.end <= range.start + 1 or range.end > tokens.len) return result;

    if (options.order_modifiers) {
        var expression_end = range.end;
        if (expression_end >= range.start + 2 and
            tokens[expression_end - 2].matchesKeywordTag(.nulls))
        {
            if (tokens[expression_end - 1].matchesKeywordTag(.first)) {
                result.nulls_order_tokens = .{ .start = expression_end - 2, .end = expression_end };
                result.nulls_order = .first;
                expression_end -= 2;
            } else if (tokens[expression_end - 1].matchesKeywordTag(.last)) {
                result.nulls_order_tokens = .{ .start = expression_end - 2, .end = expression_end };
                result.nulls_order = .last;
                expression_end -= 2;
            }
        }
        if (expression_end > range.start and tokens[expression_end - 1].matchesKeywordTag(.asc)) {
            result.direction_tokens = .{ .start = expression_end - 1, .end = expression_end };
            result.direction = .asc;
            expression_end -= 1;
        } else if (expression_end > range.start and tokens[expression_end - 1].matchesKeywordTag(.desc)) {
            result.direction_tokens = .{ .start = expression_end - 1, .end = expression_end };
            result.direction = .desc;
            expression_end -= 1;
        } else if (expression_end >= range.start + 2 and tokens[expression_end - 2].matchesKeywordTag(.using)) {
            switch (tokens[expression_end - 1].kind) {
                .eq, .neq, .lt, .lte, .gt, .gte => {
                    result.direction_tokens = .{ .start = expression_end - 2, .end = expression_end };
                    result.order_using_operator_tokens = .{ .start = expression_end - 1, .end = expression_end };
                    expression_end -= 2;
                },
                else => {},
            }
        }
        if (expression_end > range.start) result.expression_tokens = .{ .start = range.start, .end = expression_end };
        return result;
    }

    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return result;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(.as) and index > range.start and index + 1 < range.end) {
                result.expression_tokens = .{ .start = range.start, .end = index };
                result.alias_tokens = .{ .start = index, .end = range.end };
                result.alias_name_tokens = .{ .start = index + 1, .end = range.end };
                return result;
            },
        }
    }
    if (options.bare_alias and generatedBareAliasCandidate(tokens, range)) {
        result.expression_tokens = .{ .start = range.start, .end = range.end - 1 };
        result.alias_tokens = .{ .start = range.end - 1, .end = range.end };
        result.alias_name_tokens = result.alias_tokens;
    }
    return result;
}

fn generatedBareAliasCandidate(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    if (range.end <= range.start + 1 or range.end > tokens.len) return false;
    if (tokens[range.end - 1].kind != .identifier) return false;
    return switch (tokens[range.end - 2].kind) {
        .comma,
        .lparen,
        .lbracket,
        .eq,
        .neq,
        .lt,
        .lte,
        .gt,
        .gte,
        .plus,
        .minus,
        .slash,
        .percent,
        .pipe_concat,
        .at_contains,
        .range_overlap,
        .question,
        .question_any,
        .question_all,
        .arrow_json,
        .arrow_text,
        .path_arrow_json,
        .path_arrow_text,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        => false,
        else => true,
    };
}

fn generatedListExpressionTokens(list: GeneratedSqlListAst, index: usize) ?GeneratedSqlTokenRange {
    if (index < list.expression_items.len) return list.expression_items[index];
    if (index < list.items.len) return list.items[index];
    return null;
}

fn recordGeneratedListItem(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(GeneratedSqlTokenRange),
    ast: *GeneratedSqlListAst,
    range: GeneratedSqlTokenRange,
) !void {
    if (range.start >= range.end) return;
    try items.append(alloc, range);
    if (ast.count == 0) ast.first_tokens = range;
    ast.last_tokens = range;
    ast.count += 1;
}

fn buildGeneratedExpressionAstInPlace(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    ast: *GeneratedSqlExpressionAst,
) anyerror!void {
    ast.* = GeneratedSqlExpressionAst{ .tokens = range };
    errdefer ast.deinit(alloc);
    if (generatedQualifiedStarExpression(tokens, range)) return;
    if (generatedSubqueryExpressionInnerRange(tokens, range)) |inner_range| {
        ast.kind = .subquery;
        ast.inner_tokens = inner_range;
        try buildGeneratedSubqueryExpressionAst(alloc, tokens, inner_range, ast);
        return;
    }
    if (generatedWrappedExpressionInnerRange(tokens, range)) |inner_range| {
        ast.kind = .grouped;
        ast.inner_tokens = inner_range;
        ast.inner_expression_kind = generatedExpressionKindForRange(tokens, inner_range);
        ast.inner_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, inner_range);
        return;
    }
    if (generatedCastExpression(tokens, range)) |cast_expression| {
        ast.kind = .cast;
        ast.cast_expression_tokens = cast_expression.expression_tokens;
        ast.cast_expression_kind = generatedExpressionKindForRange(tokens, cast_expression.expression_tokens);
        ast.cast_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, cast_expression.expression_tokens);
        ast.cast_type_tokens = cast_expression.type_tokens;
        return;
    }
    if (generatedCaseExpression(tokens, range)) |case_expression| {
        ast.kind = .case_expression;
        ast.case_branch_count = case_expression.branch_count;
        ast.case_first_when_tokens = case_expression.first_when_tokens;
        ast.case_last_when_tokens = case_expression.last_when_tokens;
        ast.case_first_condition_tokens = case_expression.first_condition_tokens;
        ast.case_first_condition_kind = generatedExpressionKindForRange(tokens, case_expression.first_condition_tokens);
        ast.case_first_condition = try buildGeneratedExpressionNodeAlloc(alloc, tokens, case_expression.first_condition_tokens);
        ast.case_first_result_tokens = case_expression.first_result_tokens;
        ast.case_first_result_kind = generatedExpressionKindForRange(tokens, case_expression.first_result_tokens);
        ast.case_first_result = try buildGeneratedExpressionNodeAlloc(alloc, tokens, case_expression.first_result_tokens);
        const branch_lists = try buildGeneratedCaseBranchLists(alloc, tokens, range);
        ast.case_condition_items = branch_lists.conditions;
        ast.case_result_items = branch_lists.results;
        ast.case_else_tokens = case_expression.else_tokens;
        ast.case_else_expression_tokens = case_expression.else_expression_tokens;
        if (case_expression.else_expression_tokens) |else_expression_tokens| {
            ast.case_else_expression_kind = generatedExpressionKindForRange(tokens, else_expression_tokens);
            ast.case_else_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, else_expression_tokens);
        }
        return;
    }
    if (generatedIntervalLiteralExpression(tokens, range)) |value_tokens| {
        ast.kind = .interval_literal;
        ast.interval_value_tokens = value_tokens;
        return;
    }
    if (generatedTimestampLiteralExpression(tokens, range)) |timestamp_literal| {
        ast.kind = .timestamp_literal;
        ast.timestamp_type_tokens = timestamp_literal.type_tokens;
        ast.timestamp_value_tokens = timestamp_literal.value_tokens;
        return;
    }
    if (generatedCurrentTimestampExpression(tokens, range)) |precision_tokens| {
        ast.kind = .current_timestamp;
        ast.current_timestamp_precision_tokens = precision_tokens;
        return;
    }
    if (generatedCurrentDateExpression(tokens, range)) {
        ast.kind = .current_date;
        return;
    }
    if (generatedExtractExpression(tokens, range)) |extract_expression| {
        ast.kind = .extract_expression;
        ast.extract_field_tokens = extract_expression.field_tokens;
        ast.extract_source_tokens = extract_expression.source_tokens;
        ast.extract_source_expression_kind = generatedExpressionKindForRange(tokens, extract_expression.source_tokens);
        ast.extract_source_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, extract_expression.source_tokens);
        return;
    }
    if (findTopLevelExpressionOperator(tokens, range)) |operator| {
        ast.kind = operator.kind;
        if (!operator.prefix) {
            if (range.start >= operator.index) return;
            if (!operator.postfix and operator.index + 1 >= range.end) return;
            const left_end = if (operator.negation_index) |negation_index|
                if (negation_index < operator.index) negation_index else operator.index
            else
                operator.index;
            if (range.start >= left_end) return;
            ast.left_tokens = .{ .start = range.start, .end = left_end };
            ast.left_expression_kind = generatedExpressionKindForRange(tokens, ast.left_tokens.?);
            ast.left_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, ast.left_tokens.?);
        } else if (operator.index + 1 >= range.end) {
            return;
        }
        if (operator.negation_index) |negation_index| ast.negation_tokens = .{ .start = negation_index, .end = negation_index + 1 };
        const operator_end = operator.operator_end_index orelse operator.index + 1;
        ast.operator_tokens = .{ .start = operator.index, .end = operator_end };
        const right_start = if (operator.quantifier_index) |quantifier_index| blk: {
            ast.quantifier_tokens = .{ .start = quantifier_index, .end = quantifier_index + 1 };
            break :blk quantifier_index + 1;
        } else operator_end;
        if (right_start >= range.end) return;
        var operand_start = right_start;
        if (isGeneratedBetweenExpressionKind(operator.kind)) {
            if (generatedBetweenModifier(tokens[right_start])) |modifier| {
                if (right_start + 1 >= range.end) return;
                ast.between_modifier_tokens = .{ .start = right_start, .end = right_start + 1 };
                ast.between_modifier = modifier;
                operand_start = right_start + 1;
            }
        }
        var right_end = range.end;
        if (isGeneratedLikeExpressionKind(operator.kind)) {
            if (findTopLevelKeyword(tokens, operand_start, range.end, .escape)) |escape_index| {
                if (operand_start >= escape_index or escape_index + 1 >= range.end) return;
                right_end = escape_index;
                ast.escape_tokens = .{ .start = escape_index, .end = range.end };
                const escape_expression_tokens = GeneratedSqlTokenRange{ .start = escape_index + 1, .end = range.end };
                ast.escape_expression_kind = generatedExpressionKindForRange(tokens, escape_expression_tokens);
                ast.escape_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, escape_expression_tokens);
            }
        }
        ast.right_tokens = .{ .start = operand_start, .end = right_end };
        ast.right_expression_kind = generatedExpressionKindForRange(tokens, ast.right_tokens.?);
        ast.right_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, ast.right_tokens.?);
        if (isGeneratedBetweenExpressionKind(operator.kind)) {
            if (generatedBetweenBoundExpressionRanges(tokens, .{ .start = operand_start, .end = right_end })) |bounds| {
                ast.between_lower_tokens = bounds.lower_tokens;
                ast.between_lower_expression_kind = generatedExpressionKindForRange(tokens, bounds.lower_tokens);
                ast.between_lower_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, bounds.lower_tokens);
                ast.between_upper_tokens = bounds.upper_tokens;
                ast.between_upper_expression_kind = generatedExpressionKindForRange(tokens, bounds.upper_tokens);
                ast.between_upper_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, bounds.upper_tokens);
            }
        }
        if (try generatedBooleanChainMetadata(alloc, tokens, range, operator.kind)) |boolean_chain| {
            ast.boolean_condition_count = boolean_chain.condition_count;
            ast.boolean_first_condition_tokens = boolean_chain.first_condition_tokens;
            ast.boolean_first_condition_kind = boolean_chain.first_condition_kind;
            ast.boolean_first_condition = boolean_chain.first_condition;
            ast.boolean_last_condition_tokens = boolean_chain.last_condition_tokens;
            ast.boolean_last_condition_kind = boolean_chain.last_condition_kind;
            ast.boolean_last_condition = boolean_chain.last_condition;
            ast.boolean_condition_items = boolean_chain.condition_items;
        }
        return;
    }
    if (generatedFunctionCallExpression(tokens, range)) |function_call| {
        ast.kind = .function_call;
        ast.function_name_tokens = function_call.name_tokens;
        ast.argument_tokens = function_call.argument_tokens;
        ast.argument_distinct_tokens = function_call.argument_distinct_tokens;
        ast.argument_value_tokens = function_call.argument_value_tokens;
        if (function_call.argument_value_tokens) |argument_value_tokens| {
            ast.argument_items = try buildTopLevelListAst(alloc, tokens, argument_value_tokens, .{});
        }
        ast.argument_order_tokens = function_call.argument_order_tokens;
        if (function_call.argument_order_tokens) |argument_order_tokens| {
            ast.argument_order_items = try buildTopLevelListAst(alloc, tokens, argument_order_tokens, .{ .order_modifiers = true });
        }
        ast.within_group_tokens = function_call.within_group_tokens;
        ast.within_group_order_tokens = function_call.within_group_order_tokens;
        if (function_call.within_group_order_tokens) |order_tokens| {
            ast.within_group_order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
        }
        ast.filter_tokens = function_call.filter_tokens;
        ast.filter_predicate_tokens = function_call.filter_predicate_tokens;
        if (function_call.filter_predicate_tokens) |predicate_tokens| {
            ast.filter_expression_kind = generatedExpressionKindForRange(tokens, predicate_tokens);
            ast.filter_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, predicate_tokens);
        }
        ast.over_tokens = function_call.over_tokens;
        ast.over_name_tokens = function_call.over_name_tokens;
        ast.over_definition_tokens = function_call.over_definition_tokens;
        ast.over_partition_tokens = function_call.over_partition_tokens;
        if (function_call.over_partition_tokens) |partition_tokens| {
            ast.over_partition_items = try buildTopLevelListAst(alloc, tokens, partition_tokens, .{});
        }
        ast.over_order_tokens = function_call.over_order_tokens;
        if (function_call.over_order_tokens) |order_tokens| {
            ast.over_order_items = try buildTopLevelListAst(alloc, tokens, order_tokens, .{ .order_modifiers = true });
        }
        ast.over_frame_tokens = function_call.over_frame_tokens;
        if (function_call.over_frame_tokens) |frame_tokens| {
            if (generatedWindowFrameExpressionRanges(tokens, frame_tokens)) |frame_expressions| {
                if (frame_expressions.start_expression_tokens) |expression_tokens| {
                    ast.over_frame_start_expression_tokens = expression_tokens;
                    ast.over_frame_start_expression_kind = generatedExpressionKindForRange(tokens, expression_tokens);
                    ast.over_frame_start_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, expression_tokens);
                }
                if (frame_expressions.end_expression_tokens) |expression_tokens| {
                    ast.over_frame_end_expression_tokens = expression_tokens;
                    ast.over_frame_end_expression_kind = generatedExpressionKindForRange(tokens, expression_tokens);
                    ast.over_frame_end_expression = try buildGeneratedExpressionNodeAlloc(alloc, tokens, expression_tokens);
                }
            }
        }
        return;
    }
    if (generatedArrayConstructorExpression(tokens, range)) |array_constructor| {
        ast.kind = .array_constructor;
        ast.array_tokens = array_constructor.element_tokens;
        if (array_constructor.element_tokens) |element_tokens| {
            ast.array_items = try buildTopLevelListAst(alloc, tokens, element_tokens, .{});
        }
        return;
    }
    return;
}

fn buildGeneratedExpressionAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) anyerror!GeneratedSqlExpressionAst {
    var ast = GeneratedSqlExpressionAst{};
    try buildGeneratedExpressionAstInPlace(alloc, tokens, range, &ast);
    return ast;
}

fn buildGeneratedExpressionNodeAlloc(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !*GeneratedSqlExpressionAst {
    const node = try alloc.create(GeneratedSqlExpressionAst);
    errdefer alloc.destroy(node);
    try buildGeneratedExpressionAstInPlace(alloc, tokens, range, node);
    return node;
}

const GeneratedCaseBranchLists = struct {
    conditions: GeneratedSqlListAst = .{},
    results: GeneratedSqlListAst = .{},

    fn deinit(self: *GeneratedCaseBranchLists, alloc: std.mem.Allocator) void {
        self.conditions.deinit(alloc);
        self.results.deinit(alloc);
        self.* = .{};
    }
};

fn buildGeneratedExpressionListAstFromRanges(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    ranges: []const GeneratedSqlTokenRange,
) !GeneratedSqlListAst {
    var ast = GeneratedSqlListAst{};
    if (ranges.len == 0) return ast;
    errdefer ast.deinit(alloc);

    ast.count = ranges.len;
    ast.first_tokens = ranges[0];
    ast.last_tokens = ranges[ranges.len - 1];
    ast.items = try alloc.dupe(GeneratedSqlTokenRange, ranges);
    ast.expression_items = try alloc.dupe(GeneratedSqlTokenRange, ranges);
    ast.expressions = try alloc.alloc(GeneratedSqlExpressionAst, ranges.len);
    @memset(ast.expressions, .{});
    for (ranges, 0..) |range, index| {
        try buildGeneratedExpressionAstInPlace(alloc, tokens, range, &ast.expressions[index]);
    }
    return ast;
}

fn buildGeneratedCaseBranchLists(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) !GeneratedCaseBranchLists {
    var condition_ranges: std.ArrayListUnmanaged(GeneratedSqlTokenRange) = .empty;
    defer condition_ranges.deinit(alloc);
    var result_ranges: std.ArrayListUnmanaged(GeneratedSqlTokenRange) = .empty;
    defer result_ranges.deinit(alloc);
    var lists = GeneratedCaseBranchLists{};
    errdefer lists.deinit(alloc);

    if (range.start + 5 > range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[range.start].matchesKeywordTag(.case)) return error.UnsupportedSqlShape;
    if (!tokens[range.end - 1].matchesKeywordTag(.end)) return error.UnsupportedSqlShape;
    const body_end = range.end - 1;
    var cursor = range.start + 1;
    while (cursor < body_end) {
        if (tokens[cursor].matchesKeywordTag(.@"else")) break;
        if (!tokens[cursor].matchesKeywordTag(.when)) return error.UnsupportedSqlShape;
        const then_index = findTopLevelCaseKeyword(tokens, cursor + 1, body_end, .then) orelse return error.UnsupportedSqlShape;
        if (cursor + 1 >= then_index) return error.UnsupportedSqlShape;
        const next_index = findNextTopLevelCaseBoundary(tokens, then_index + 1, body_end) orelse body_end;
        if (then_index + 1 >= next_index) return error.UnsupportedSqlShape;
        try condition_ranges.append(alloc, .{ .start = cursor + 1, .end = then_index });
        try result_ranges.append(alloc, .{ .start = then_index + 1, .end = next_index });
        cursor = next_index;
    }
    if (condition_ranges.items.len == 0 or condition_ranges.items.len != result_ranges.items.len) {
        return error.UnsupportedSqlShape;
    }
    lists.conditions = try buildGeneratedExpressionListAstFromRanges(alloc, tokens, condition_ranges.items);
    lists.results = try buildGeneratedExpressionListAstFromRanges(alloc, tokens, result_ranges.items);
    return lists;
}

fn generatedExpressionKindForRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlExpressionKind {
    if (generatedQualifiedStarExpression(tokens, range)) return .token_range;
    if (generatedSubqueryExpressionInnerRange(tokens, range) != null) return .subquery;
    if (generatedWrappedExpressionInnerRange(tokens, range) != null) return .grouped;
    if (generatedCastExpression(tokens, range) != null) return .cast;
    if (generatedCaseExpression(tokens, range) != null) return .case_expression;
    if (generatedIntervalLiteralExpression(tokens, range) != null) return .interval_literal;
    if (generatedTimestampLiteralExpression(tokens, range) != null) return .timestamp_literal;
    if (generatedCurrentTimestampExpression(tokens, range) != null) return .current_timestamp;
    if (generatedCurrentDateExpression(tokens, range)) return .current_date;
    if (generatedExtractExpression(tokens, range) != null) return .extract_expression;
    if (findTopLevelExpressionOperator(tokens, range)) |operator| return operator.kind;
    if (generatedFunctionCallExpression(tokens, range) != null) return .function_call;
    if (generatedArrayConstructorExpression(tokens, range) != null) return .array_constructor;
    return null;
}

fn generatedQualifiedStarExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    if (range.start >= range.end or range.end > tokens.len) return false;
    if (range.end == range.start + 2) {
        return tokens[range.start].kind == .identifier and
            tokens[range.start + 1].kind == .star;
    }
    return false;
}

const GeneratedBetweenBoundExpressionRanges = struct {
    lower_tokens: GeneratedSqlTokenRange,
    upper_tokens: GeneratedSqlTokenRange,
};

fn generatedBetweenBoundExpressionRanges(
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
) ?GeneratedBetweenBoundExpressionRanges {
    const and_index = findTopLevelBetweenBoundAnd(tokens, range) orelse return null;
    if (range.start >= and_index or and_index + 1 >= range.end) return null;
    return .{
        .lower_tokens = .{ .start = range.start, .end = and_index },
        .upper_tokens = .{ .start = and_index + 1, .end = range.end },
    };
}

fn findTopLevelBetweenBoundAnd(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?usize {
    if (range.start >= range.end or range.end > tokens.len) return null;
    var depth: usize = 0;
    var case_depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.case)) {
                    case_depth += 1;
                } else if (tokens[index].matchesKeywordTag(.end)) {
                    if (case_depth == 0) return null;
                    case_depth -= 1;
                } else if (case_depth == 0 and tokens[index].matchesKeywordTag(.@"and")) {
                    return index;
                }
            },
        }
    }
    if (depth != 0 or case_depth != 0) return null;
    return null;
}

fn generatedSubqueryExpressionInnerRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    const inner_range = generatedWrappedExpressionInnerRange(tokens, range) orelse return null;
    if (inner_range.start >= inner_range.end or inner_range.end > tokens.len) return null;
    if (tokens[inner_range.start].matchesKeywordTag(.select) or tokens[inner_range.start].matchesKeywordTag(.with)) return inner_range;
    return null;
}

fn generatedIntervalLiteralExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start >= range.end or range.end > tokens.len or range.end - range.start != 2) return null;
    if (!tokens[range.start].matchesKeywordTag(.interval)) return null;
    if (tokens[range.start + 1].kind != .string) return null;
    return .{ .start = range.start + 1, .end = range.end };
}

const GeneratedTimestampLiteralExpression = struct {
    type_tokens: GeneratedSqlTokenRange,
    value_tokens: GeneratedSqlTokenRange,
};

fn generatedTimestampLiteralExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedTimestampLiteralExpression {
    if (range.start >= range.end or range.end > tokens.len or range.end - range.start != 2) return null;
    if (!tokens[range.start].matchesKeywordTag(.timestamp) and !tokens[range.start].matchesKeywordTag(.timestamptz)) return null;
    if (tokens[range.start + 1].kind != .string) return null;
    return .{
        .type_tokens = .{ .start = range.start, .end = range.start + 1 },
        .value_tokens = .{ .start = range.start + 1, .end = range.end },
    };
}

fn generatedCurrentDateExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    return range.start < range.end and range.end <= tokens.len and range.end - range.start == 1 and tokens[range.start].matchesKeywordTag(.current_date);
}

fn generatedCurrentTimestampExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start >= range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.current_timestamp)) return null;
    if (range.end - range.start == 1) return .{ .start = range.start + 1, .end = range.start + 1 };
    if (range.end - range.start != 4) return null;
    if (tokens[range.start + 1].kind != .lparen) return null;
    if (tokens[range.start + 2].kind != .number) return null;
    if (tokens[range.start + 3].kind != .rparen) return null;
    return .{ .start = range.start + 2, .end = range.start + 3 };
}

const GeneratedExtractExpression = struct {
    field_tokens: GeneratedSqlTokenRange,
    source_tokens: GeneratedSqlTokenRange,
};

fn generatedExtractExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedExtractExpression {
    if (range.start >= range.end or range.end > tokens.len or range.end - range.start < 6) return null;
    if (!tokens[range.start].matchesKeywordTag(.extract)) return null;
    if (tokens[range.start + 1].kind != .lparen) return null;
    if (tokens[range.start + 2].kind != .identifier) return null;
    if (!tokens[range.start + 3].matchesKeywordTag(.from)) return null;
    if (tokens[range.end - 1].kind != .rparen) return null;
    const source_tokens = GeneratedSqlTokenRange{ .start = range.start + 4, .end = range.end - 1 };
    if (source_tokens.start >= source_tokens.end) return null;
    return .{
        .field_tokens = .{ .start = range.start + 2, .end = range.start + 3 },
        .source_tokens = source_tokens,
    };
}

const GeneratedFunctionCallExpression = struct {
    name_tokens: GeneratedSqlTokenRange,
    argument_tokens: ?GeneratedSqlTokenRange = null,
    argument_distinct_tokens: ?GeneratedSqlTokenRange = null,
    argument_value_tokens: ?GeneratedSqlTokenRange = null,
    argument_order_tokens: ?GeneratedSqlTokenRange = null,
    within_group_tokens: ?GeneratedSqlTokenRange = null,
    within_group_order_tokens: ?GeneratedSqlTokenRange = null,
    filter_tokens: ?GeneratedSqlTokenRange = null,
    filter_predicate_tokens: ?GeneratedSqlTokenRange = null,
    over_tokens: ?GeneratedSqlTokenRange = null,
    over_name_tokens: ?GeneratedSqlTokenRange = null,
    over_definition_tokens: ?GeneratedSqlTokenRange = null,
    over_partition_tokens: ?GeneratedSqlTokenRange = null,
    over_order_tokens: ?GeneratedSqlTokenRange = null,
    over_frame_tokens: ?GeneratedSqlTokenRange = null,
};

const GeneratedFunctionArgumentMetadata = struct {
    argument_tokens: ?GeneratedSqlTokenRange = null,
    distinct_tokens: ?GeneratedSqlTokenRange = null,
    value_tokens: ?GeneratedSqlTokenRange = null,
    order_tokens: ?GeneratedSqlTokenRange = null,
};

fn generatedFunctionCallExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedFunctionCallExpression {
    if (range.start + 2 > range.end or range.end > tokens.len) return null;

    var depth: usize = 0;
    var lparen_index: ?usize = null;
    var function_close_index: ?usize = null;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => {
                if (depth == 0 and lparen_index == null) lparen_index = index;
                depth += 1;
            },
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0 and function_close_index == null) {
                    function_close_index = index;
                    break;
                }
            },
            else => {},
        }
    }
    if (depth != 0) return null;
    const open_index = lparen_index orelse return null;
    const close_index = function_close_index orelse return null;
    if (open_index <= range.start) return null;
    if (isGeneratedFunctionCallBlockedName(tokens[open_index - 1])) return null;
    if (!isGeneratedQualifiedNameRange(tokens, .{ .start = range.start, .end = open_index })) return null;
    const argument_metadata = generatedFunctionArgumentMetadata(tokens, open_index, close_index) orelse return null;
    var cursor = close_index + 1;
    var within_group_tokens: ?GeneratedSqlTokenRange = null;
    var within_group_order_tokens: ?GeneratedSqlTokenRange = null;
    if (cursor < range.end and tokens[cursor].matchesKeywordTag(.within)) {
        if (cursor + 5 > range.end) return null;
        if (!tokens[cursor + 1].matchesKeywordTag(.group)) return null;
        if (tokens[cursor + 2].kind != .lparen) return null;
        if (!tokens[cursor + 3].matchesKeywordTag(.order)) return null;
        if (cursor + 4 >= range.end or !tokens[cursor + 4].matchesKeywordTag(.by)) return null;
        const within_close = findMatchingParen(tokens, cursor + 2, range.end) orelse return null;
        if (within_close >= range.end) return null;
        within_group_tokens = .{ .start = cursor, .end = within_close + 1 };
        within_group_order_tokens = .{ .start = cursor + 5, .end = within_close };
        if (within_group_order_tokens.?.start >= within_group_order_tokens.?.end) return null;
        cursor = within_close + 1;
    }
    var filter_tokens: ?GeneratedSqlTokenRange = null;
    var filter_predicate_tokens: ?GeneratedSqlTokenRange = null;
    if (cursor < range.end and tokens[cursor].matchesKeywordTag(.filter)) {
        if (cursor + 4 > range.end) return null;
        if (!tokens[cursor].matchesKeywordTag(.filter)) return null;
        if (tokens[cursor + 1].kind != .lparen) return null;
        if (!tokens[cursor + 2].matchesKeywordTag(.where)) return null;
        const filter_close = findMatchingParen(tokens, cursor + 1, range.end) orelse return null;
        filter_tokens = .{ .start = cursor, .end = filter_close + 1 };
        filter_predicate_tokens = .{ .start = cursor + 3, .end = filter_close };
        if (filter_predicate_tokens.?.start >= filter_predicate_tokens.?.end) return null;
        cursor = filter_close + 1;
    }
    var over_tokens: ?GeneratedSqlTokenRange = null;
    var over_name_tokens: ?GeneratedSqlTokenRange = null;
    var over_definition_tokens: ?GeneratedSqlTokenRange = null;
    var over_partition_tokens: ?GeneratedSqlTokenRange = null;
    var over_order_tokens: ?GeneratedSqlTokenRange = null;
    var over_frame_tokens: ?GeneratedSqlTokenRange = null;
    if (cursor < range.end and tokens[cursor].matchesKeywordTag(.over)) {
        if (cursor + 1 >= range.end) return null;
        if (tokens[cursor + 1].kind == .identifier) {
            if (cursor + 2 != range.end) return null;
            over_tokens = .{ .start = cursor, .end = range.end };
            over_name_tokens = .{ .start = cursor + 1, .end = range.end };
        } else if (tokens[cursor + 1].kind == .lparen) {
            const over_close = findMatchingParen(tokens, cursor + 1, range.end) orelse return null;
            if (over_close + 1 != range.end) return null;
            over_tokens = .{ .start = cursor, .end = range.end };
            over_definition_tokens = .{ .start = cursor + 2, .end = over_close };
            const definition = over_definition_tokens.?;
            const partition_index = findTopLevelKeywordSequence(tokens, definition.start, definition.end, .partition, .by);
            const order_index = findTopLevelKeywordSequence(tokens, definition.start, definition.end, .order, .by);
            const rows_index = findTopLevelKeyword(tokens, definition.start, definition.end, .rows);
            const range_index = findTopLevelKeyword(tokens, definition.start, definition.end, .range);
            const frame_index = minOptionalIndex(rows_index, range_index);
            if (partition_index) |idx| {
                const partition_start = idx + 2;
                const partition_end = firstOptionalIndex(&[_]?usize{ order_index, frame_index }) orelse definition.end;
                if (partition_start < partition_end) over_partition_tokens = .{ .start = partition_start, .end = partition_end };
            }
            if (order_index) |idx| {
                const order_start = idx + 2;
                const order_end = frame_index orelse definition.end;
                if (order_start < order_end) over_order_tokens = .{ .start = order_start, .end = order_end };
            }
            if (frame_index) |idx| over_frame_tokens = .{ .start = idx, .end = definition.end };
        } else {
            return null;
        }
        cursor = range.end;
    }
    if (cursor != range.end) return null;
    return .{
        .name_tokens = .{ .start = range.start, .end = open_index },
        .argument_tokens = argument_metadata.argument_tokens,
        .argument_distinct_tokens = argument_metadata.distinct_tokens,
        .argument_value_tokens = argument_metadata.value_tokens,
        .argument_order_tokens = argument_metadata.order_tokens,
        .within_group_tokens = within_group_tokens,
        .within_group_order_tokens = within_group_order_tokens,
        .filter_tokens = filter_tokens,
        .filter_predicate_tokens = filter_predicate_tokens,
        .over_tokens = over_tokens,
        .over_name_tokens = over_name_tokens,
        .over_definition_tokens = over_definition_tokens,
        .over_partition_tokens = over_partition_tokens,
        .over_order_tokens = over_order_tokens,
        .over_frame_tokens = over_frame_tokens,
    };
}

fn generatedFunctionArgumentMetadata(tokens: []const token_mod.Token, open_index: usize, close_index: usize) ?GeneratedFunctionArgumentMetadata {
    if (open_index + 1 >= close_index) return .{};
    var metadata = GeneratedFunctionArgumentMetadata{
        .argument_tokens = .{ .start = open_index + 1, .end = close_index },
        .value_tokens = .{ .start = open_index + 1, .end = close_index },
    };
    var value_start = open_index + 1;
    if (tokens[value_start].matchesKeywordTag(.distinct)) {
        metadata.distinct_tokens = .{ .start = value_start, .end = value_start + 1 };
        value_start += 1;
        if (value_start >= close_index) return null;
    }
    const order_index = findTopLevelKeywordSequence(tokens, value_start, close_index, .order, .by);
    const value_end = order_index orelse close_index;
    if (value_start >= value_end) return null;
    metadata.value_tokens = .{ .start = value_start, .end = value_end };
    if (order_index) |idx| {
        if (idx + 2 >= close_index) return null;
        metadata.order_tokens = .{ .start = idx + 2, .end = close_index };
    }
    return metadata;
}

fn isGeneratedFunctionCallBlockedName(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.in) or
        token.matchesKeywordTag(.not) or
        token.matchesKeywordTag(.any) or
        token.matchesKeywordTag(.all) or
        token.matchesKeywordTag(.some);
}

fn isGeneratedQualifiedNameRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    if (range.start >= range.end or range.end > tokens.len) return false;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        if (tokens[index].kind != .identifier) return false;
    }
    return true;
}

const GeneratedArrayConstructorExpression = struct {
    element_tokens: ?GeneratedSqlTokenRange = null,
};

fn generatedArrayConstructorExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedArrayConstructorExpression {
    if (range.start + 3 > range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.array)) return null;
    if (tokens[range.start + 1].kind != .lbracket or tokens[range.end - 1].kind != .rbracket) return null;
    var depth: usize = 0;
    var index = range.start + 1;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lbracket, .lparen => depth += 1,
            .rbracket, .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0 and index + 1 != range.end) return null;
            },
            else => {},
        }
    }
    if (depth != 0) return null;
    return .{
        .element_tokens = if (range.start + 2 < range.end - 1)
            .{ .start = range.start + 2, .end = range.end - 1 }
        else
            null,
    };
}

const GeneratedCastExpression = struct {
    expression_tokens: GeneratedSqlTokenRange,
    type_tokens: GeneratedSqlTokenRange,
};

fn generatedCastExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedCastExpression {
    if (range.start >= range.end or range.end > tokens.len) return null;
    if (range.start + 6 <= range.end and tokens[range.start].matchesKeywordTag(.cast)) {
        if (tokens[range.start + 1].kind != .lparen or tokens[range.end - 1].kind != .rparen) return null;
        const inner_start = range.start + 2;
        const inner_end = range.end - 1;
        const as_index = findTopLevelKeyword(tokens, inner_start, inner_end, .as) orelse return null;
        if (inner_start >= as_index or as_index + 1 >= inner_end) return null;
        const expression_tokens = GeneratedSqlTokenRange{ .start = inner_start, .end = as_index };
        const type_tokens = GeneratedSqlTokenRange{ .start = as_index + 1, .end = inner_end };
        if (!isGeneratedTypeNameRange(tokens, type_tokens)) return null;
        return .{
            .expression_tokens = expression_tokens,
            .type_tokens = type_tokens,
        };
    }
    return null;
}

fn isGeneratedTypeNameRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) bool {
    if (range.start >= range.end or range.end > tokens.len) return false;
    var name_end = range.end;
    if (range.end >= range.start + 2 and tokens[range.end - 2].kind == .lbracket and tokens[range.end - 1].kind == .rbracket) {
        name_end = range.end - 2;
    }
    return isGeneratedQualifiedNameRange(tokens, .{ .start = range.start, .end = name_end });
}

const GeneratedCaseExpression = struct {
    branch_count: usize,
    first_when_tokens: GeneratedSqlTokenRange,
    last_when_tokens: GeneratedSqlTokenRange,
    first_condition_tokens: GeneratedSqlTokenRange,
    first_result_tokens: GeneratedSqlTokenRange,
    else_tokens: ?GeneratedSqlTokenRange = null,
    else_expression_tokens: ?GeneratedSqlTokenRange = null,
};

fn generatedCaseExpression(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedCaseExpression {
    if (range.start + 5 > range.end or range.end > tokens.len) return null;
    if (!tokens[range.start].matchesKeywordTag(.case)) return null;
    if (!tokens[range.end - 1].matchesKeywordTag(.end)) return null;
    var cursor = range.start + 1;
    var branch_count: usize = 0;
    var first_when_tokens: ?GeneratedSqlTokenRange = null;
    var last_when_tokens: ?GeneratedSqlTokenRange = null;
    var first_condition_tokens: ?GeneratedSqlTokenRange = null;
    var first_result_tokens: ?GeneratedSqlTokenRange = null;
    var else_tokens: ?GeneratedSqlTokenRange = null;
    var else_expression_tokens: ?GeneratedSqlTokenRange = null;
    const body_end = range.end - 1;
    while (cursor < body_end) {
        if (tokens[cursor].matchesKeywordTag(.@"else")) {
            const expression_tokens = GeneratedSqlTokenRange{ .start = cursor + 1, .end = body_end };
            if (expression_tokens.start >= expression_tokens.end) return null;
            else_tokens = .{ .start = cursor, .end = body_end };
            else_expression_tokens = expression_tokens;
            cursor = body_end;
            break;
        }
        if (!tokens[cursor].matchesKeywordTag(.when)) return null;
        const then_index = findTopLevelCaseKeyword(tokens, cursor + 1, body_end, .then) orelse return null;
        if (cursor + 1 >= then_index) return null;
        const next_index = findNextTopLevelCaseBoundary(tokens, then_index + 1, body_end) orelse body_end;
        if (then_index + 1 >= next_index) return null;
        const when_tokens = GeneratedSqlTokenRange{ .start = cursor, .end = next_index };
        if (branch_count == 0) {
            first_when_tokens = when_tokens;
            first_condition_tokens = .{ .start = cursor + 1, .end = then_index };
            first_result_tokens = .{ .start = then_index + 1, .end = next_index };
        }
        last_when_tokens = when_tokens;
        branch_count += 1;
        cursor = next_index;
    }
    if (branch_count == 0 or cursor != body_end) return null;
    return .{
        .branch_count = branch_count,
        .first_when_tokens = first_when_tokens.?,
        .last_when_tokens = last_when_tokens.?,
        .first_condition_tokens = first_condition_tokens.?,
        .first_result_tokens = first_result_tokens.?,
        .else_tokens = else_tokens,
        .else_expression_tokens = else_expression_tokens,
    };
}

fn findNextTopLevelCaseBoundary(tokens: []const token_mod.Token, start: usize, end: usize) ?usize {
    var depth: usize = 0;
    var case_depth: usize = 0;
    var index = start;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.case)) {
                    case_depth += 1;
                } else if (tokens[index].matchesKeywordTag(.end)) {
                    if (case_depth == 0) return null;
                    case_depth -= 1;
                } else if (case_depth == 0 and (tokens[index].matchesKeywordTag(.when) or tokens[index].matchesKeywordTag(.@"else"))) {
                    return index;
                }
            },
        }
    }
    return null;
}

fn findTopLevelCaseKeyword(tokens: []const token_mod.Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var case_depth: usize = 0;
    var index = start;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.case)) {
                    case_depth += 1;
                } else if (tokens[index].matchesKeywordTag(.end)) {
                    if (case_depth == 0) return null;
                    case_depth -= 1;
                } else if (case_depth == 0 and tokens[index].matchesKeywordTag(keyword)) {
                    return index;
                }
            },
        }
    }
    return null;
}

fn generatedWrappedExpressionInnerRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlTokenRange {
    if (range.start >= range.end or range.end > tokens.len or range.end - range.start < 2) return null;
    if (tokens[range.start].kind != .lparen or tokens[range.end - 1].kind != .rparen) return null;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
                if (depth == 0 and index + 1 != range.end) return null;
            },
            else => {},
        }
    }
    if (depth != 0) return null;
    return .{ .start = range.start + 1, .end = range.end - 1 };
}

const GeneratedSqlExpressionOperator = struct {
    kind: GeneratedSqlExpressionKind,
    index: usize,
    operator_end_index: ?usize = null,
    negation_index: ?usize = null,
    quantifier_index: ?usize = null,
    prefix: bool = false,
    postfix: bool = false,
};

const GeneratedSqlBooleanChainMetadata = struct {
    condition_count: usize,
    first_condition_tokens: GeneratedSqlTokenRange,
    first_condition_kind: ?GeneratedSqlExpressionKind,
    first_condition: *GeneratedSqlExpressionAst,
    last_condition_tokens: GeneratedSqlTokenRange,
    last_condition_kind: ?GeneratedSqlExpressionKind,
    last_condition: *GeneratedSqlExpressionAst,
    condition_items: GeneratedSqlListAst,
};

fn generatedBooleanChainMetadata(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    kind: GeneratedSqlExpressionKind,
) !?GeneratedSqlBooleanChainMetadata {
    if (range.start >= range.end or range.end > tokens.len) return null;

    var condition_items = try buildGeneratedBooleanConditionListAst(alloc, tokens, range, kind);
    errdefer condition_items.deinit(alloc);
    if (condition_items.count < 2) return null;

    const first_tokens = condition_items.first_tokens orelse return null;
    const last_tokens = condition_items.last_tokens orelse return null;
    const first_condition = try buildGeneratedExpressionNodeAlloc(alloc, tokens, first_tokens);
    errdefer {
        first_condition.deinit(alloc);
        alloc.destroy(first_condition);
    }
    const last_condition = try buildGeneratedExpressionNodeAlloc(alloc, tokens, last_tokens);
    return .{
        .condition_count = condition_items.count,
        .first_condition_tokens = first_tokens,
        .first_condition_kind = generatedExpressionKindForRange(tokens, first_tokens),
        .first_condition = first_condition,
        .last_condition_tokens = last_tokens,
        .last_condition_kind = generatedExpressionKindForRange(tokens, last_tokens),
        .last_condition = last_condition,
        .condition_items = condition_items,
    };
}

fn buildGeneratedBooleanConditionListAst(
    alloc: std.mem.Allocator,
    tokens: []const token_mod.Token,
    range: GeneratedSqlTokenRange,
    kind: GeneratedSqlExpressionKind,
) !GeneratedSqlListAst {
    const keyword: token_mod.TokenKeyword = switch (kind) {
        .logical_or => .@"or",
        .logical_and => .@"and",
        else => return .{},
    };
    var ast = GeneratedSqlListAst{};
    if (range.start >= range.end or range.end > tokens.len) return ast;

    var items: std.ArrayListUnmanaged(GeneratedSqlTokenRange) = .empty;
    errdefer items.deinit(alloc);
    var depth: usize = 0;
    var case_depth: usize = 0;
    var item_start = range.start;
    var skip_next_between_and = false;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) {
                    items.deinit(alloc);
                    return .{};
                }
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.case)) {
                    case_depth += 1;
                } else if (tokens[index].matchesKeywordTag(.end)) {
                    if (case_depth == 0) {
                        items.deinit(alloc);
                        return .{};
                    }
                    case_depth -= 1;
                } else if (case_depth == 0) {
                    if (tokens[index].matchesKeywordTag(.between) and kind == .logical_and) {
                        skip_next_between_and = true;
                    } else if (tokens[index].matchesKeywordTag(keyword)) {
                        if (kind == .logical_and and skip_next_between_and) {
                            skip_next_between_and = false;
                        } else {
                            if (item_start >= index) {
                                items.deinit(alloc);
                                return .{};
                            }
                            try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = index });
                            item_start = index + 1;
                        }
                    }
                }
            },
        }
    }
    if (depth != 0 or case_depth != 0 or item_start >= range.end or ast.count == 0) {
        items.deinit(alloc);
        return .{};
    }

    try recordGeneratedListItem(alloc, &items, &ast, .{ .start = item_start, .end = range.end });
    ast.items = try items.toOwnedSlice(alloc);
    errdefer {
        alloc.free(ast.items);
        ast.items = &.{};
    }
    ast.expression_items = try alloc.alloc(GeneratedSqlTokenRange, ast.items.len);
    errdefer {
        alloc.free(ast.expression_items);
        ast.expression_items = &.{};
    }
    for (ast.items, 0..) |item, item_index| {
        ast.expression_items[item_index] = item;
    }
    ast.expressions = try alloc.alloc(GeneratedSqlExpressionAst, ast.items.len);
    var expression_count: usize = 0;
    errdefer {
        for (ast.expressions[0..expression_count]) |*expression| expression.deinit(alloc);
        alloc.free(ast.expressions);
        ast.expressions = &.{};
    }
    for (ast.expression_items) |item| {
        try buildGeneratedExpressionAstInPlace(alloc, tokens, item, &ast.expressions[expression_count]);
        expression_count += 1;
    }
    return ast;
}

fn findTopLevelExpressionOperator(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?GeneratedSqlExpressionOperator {
    if (range.start >= range.end or range.end > tokens.len) return null;
    var depth: usize = 0;
    var index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(.@"or")) return .{ .kind = .logical_or, .index = index },
        }
    }
    depth = 0;
    index = range.start;
    var skip_next_between_and = false;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.between)) {
                    skip_next_between_and = true;
                } else if (tokens[index].matchesKeywordTag(.@"and")) {
                    if (skip_next_between_and) {
                        skip_next_between_and = false;
                    } else {
                        return .{ .kind = .logical_and, .index = index };
                    }
                }
            },
        }
    }
    depth = 0;
    index = range.start;
    if (tokens[index].matchesKeywordTag(.not)) {
        if (index + 1 < range.end and tokens[index + 1].matchesKeywordTag(.exists)) {
            return .{ .kind = .not_exists_subquery, .index = index + 1, .negation_index = index, .prefix = true };
        }
        return .{ .kind = .logical_not, .index = index, .prefix = true };
    }
    if (tokens[index].matchesKeywordTag(.exists)) return .{ .kind = .exists_subquery, .index = index, .prefix = true };
    if (tokens[index].kind == .plus and index + 1 < range.end) return .{ .kind = .unary_positive, .index = index, .prefix = true };
    if (tokens[index].kind == .minus and index + 1 < range.end) return .{ .kind = .unary_negative, .index = index, .prefix = true };
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .eq, .neq, .lt, .lte, .gt, .gte => if (depth == 0) {
                if (index + 1 < range.end and isGeneratedQuantifiedOperator(tokens[index + 1])) {
                    return .{ .kind = .quantified_comparison, .index = index, .quantifier_index = index + 1 };
                }
                return .{ .kind = .comparison, .index = index };
            },
            .at_contains => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .contains, .index = index },
            .range_overlap => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .overlaps, .index = index },
            .question => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .json_key_exists, .index = index },
            .question_any => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .json_key_any, .index = index },
            .question_all => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .json_key_all, .index = index },
            .regex_match => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .regex_match, .index = index },
            .regex_imatch => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .regex_imatch, .index = index },
            .regex_not_match => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .regex_not_match, .index = index },
            .regex_not_imatch => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .regex_not_imatch, .index = index },
            else => if (depth == 0) {
                if (tokens[index].matchesKeywordTag(.not) and index + 1 < range.end) {
                    if (tokens[index + 1].matchesKeywordTag(.like)) {
                        if (index + 2 < range.end and isGeneratedQuantifiedOperator(tokens[index + 2])) {
                            return .{ .kind = .not_like, .index = index + 1, .negation_index = index, .quantifier_index = index + 2 };
                        }
                        return .{ .kind = .not_like, .index = index + 1, .negation_index = index };
                    }
                    if (tokens[index + 1].matchesKeywordTag(.ilike)) {
                        if (index + 2 < range.end and isGeneratedQuantifiedOperator(tokens[index + 2])) {
                            return .{ .kind = .not_ilike, .index = index + 1, .negation_index = index, .quantifier_index = index + 2 };
                        }
                        return .{ .kind = .not_ilike, .index = index + 1, .negation_index = index };
                    }
                    if (tokens[index + 1].matchesKeywordTag(.in)) return .{ .kind = .not_in_list, .index = index + 1, .negation_index = index };
                    if (tokens[index + 1].matchesKeywordTag(.between)) return .{ .kind = .not_between, .index = index + 1, .negation_index = index };
                }
                if (tokens[index].matchesKeywordTag(.like)) {
                    if (index + 1 < range.end and isGeneratedQuantifiedOperator(tokens[index + 1])) {
                        return .{ .kind = .like, .index = index, .quantifier_index = index + 1 };
                    }
                    return .{ .kind = .like, .index = index };
                }
                if (tokens[index].matchesKeywordTag(.ilike)) {
                    if (index + 1 < range.end and isGeneratedQuantifiedOperator(tokens[index + 1])) {
                        return .{ .kind = .ilike, .index = index, .quantifier_index = index + 1 };
                    }
                    return .{ .kind = .ilike, .index = index };
                }
                if (tokens[index].matchesKeywordTag(.in)) return .{ .kind = .in_list, .index = index };
                if (tokens[index].matchesKeywordTag(.between)) return .{ .kind = .between, .index = index };
                if (tokens[index].matchesKeywordTag(.isnull)) return .{ .kind = .is_null, .index = index, .postfix = true };
                if (tokens[index].matchesKeywordTag(.notnull)) return .{ .kind = .is_not_null, .index = index, .postfix = true };
                if (tokens[index].matchesKeywordTag(.is) and index + 1 < range.end) {
                    if (tokens[index + 1].matchesKeywordTag(.null)) return .{ .kind = .is_null, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.true)) return .{ .kind = .is_true, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.false)) return .{ .kind = .is_false, .index = index };
                    if (tokens[index + 1].matchesKeywordTag(.unknown)) return .{ .kind = .is_unknown, .index = index };
                    if (index + 3 < range.end and tokens[index + 1].matchesKeywordTag(.distinct) and tokens[index + 2].matchesKeywordTag(.from)) {
                        return .{ .kind = .is_distinct_from, .index = index, .operator_end_index = index + 3 };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.null)) {
                        return .{ .kind = .is_not_null, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.true)) {
                        return .{ .kind = .is_not_true, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.false)) {
                        return .{ .kind = .is_not_false, .index = index };
                    }
                    if (index + 2 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.unknown)) {
                        return .{ .kind = .is_not_unknown, .index = index };
                    }
                    if (index + 4 < range.end and tokens[index + 1].matchesKeywordTag(.not) and tokens[index + 2].matchesKeywordTag(.distinct) and tokens[index + 3].matchesKeywordTag(.from)) {
                        return .{ .kind = .is_not_distinct_from, .index = index, .operator_end_index = index + 4, .negation_index = index + 1 };
                    }
                }
            },
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .pipe_concat => if (depth == 0 and index > range.start and index + 1 < range.end) return .{ .kind = .string_concat, .index = index },
            else => {},
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .plus => if (depth == 0 and index > range.start) return .{ .kind = .additive, .index = index },
            .minus => if (depth == 0 and index > range.start) return .{ .kind = .subtractive, .index = index },
            else => {},
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .star => if (depth == 0 and index > range.start) return .{ .kind = .multiplicative, .index = index },
            .slash => if (depth == 0 and index > range.start) return .{ .kind = .divisive, .index = index },
            .percent => if (depth == 0 and index > range.start) return .{ .kind = .modulo, .index = index },
            else => {},
        }
    }
    depth = 0;
    index = range.start;
    while (index < range.end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .arrow_json => if (depth == 0 and index > range.start) return .{ .kind = .json_access, .index = index },
            .arrow_text => if (depth == 0 and index > range.start) return .{ .kind = .json_text_access, .index = index },
            .path_arrow_json => if (depth == 0 and index > range.start) return .{ .kind = .json_path_access, .index = index },
            .path_arrow_text => if (depth == 0 and index > range.start) return .{ .kind = .json_path_text_access, .index = index },
            else => {},
        }
    }
    return null;
}

fn isGeneratedQuantifiedOperator(token: token_mod.Token) bool {
    return token.matchesKeywordTag(.any) or token.matchesKeywordTag(.all) or token.matchesKeywordTag(.some);
}

fn isGeneratedLikeExpressionKind(kind: GeneratedSqlExpressionKind) bool {
    return kind == .like or kind == .ilike or kind == .not_like or kind == .not_ilike;
}

fn isGeneratedBetweenExpressionKind(kind: GeneratedSqlExpressionKind) bool {
    return kind == .between or kind == .not_between;
}

fn generatedBetweenModifier(token: token_mod.Token) ?GeneratedSqlBetweenModifier {
    if (token.matchesKeywordTag(.asymmetric)) return .asymmetric;
    if (token.matchesKeywordTag(.symmetric)) return .symmetric;
    return null;
}

fn findKeyword(tokens: []const token_mod.Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var index = start;
    while (index < end) : (index += 1) {
        if (tokens[index].matchesKeywordTag(keyword)) return index;
    }
    return null;
}

fn findTopLevelKeyword(tokens: []const token_mod.Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var index = start;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(keyword)) return index,
        }
    }
    return null;
}

fn findTopLevelKeywordSequence(tokens: []const token_mod.Token, start: usize, end: usize, first: token_mod.TokenKeyword, second: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var index = start;
    while (index + 1 < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen, .lbracket => depth += 1,
            .rparen, .rbracket => {
                if (depth == 0) return null;
                depth -= 1;
            },
            else => if (depth == 0 and tokens[index].matchesKeywordTag(first) and tokens[index + 1].matchesKeywordTag(second)) return index,
        }
    }
    return null;
}

fn findKeywordText(tokens: []const token_mod.Token, start: usize, end: usize, keyword: []const u8) ?usize {
    var index = start;
    while (index < end) : (index += 1) {
        if (tokens[index].matchesKeyword(keyword)) return index;
    }
    return null;
}

fn findMatchingParen(tokens: []const token_mod.Token, open_index: usize, end: usize) ?usize {
    if (open_index >= end or tokens[open_index].kind != .lparen) return null;
    var depth: usize = 1;
    var index = open_index + 1;
    while (index < end) : (index += 1) {
        switch (tokens[index].kind) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) return index;
            },
            else => {},
        }
    }
    return null;
}

fn minOptionalIndex(a: ?usize, b: ?usize) ?usize {
    if (a == null) return b;
    if (b == null) return a;
    return @min(a.?, b.?);
}

fn firstOptionalIndex(indices: []const ?usize) ?usize {
    var best: ?usize = null;
    for (indices) |index| {
        if (index) |idx| {
            if (best == null or idx < best.?) best = idx;
        }
    }
    return best;
}

fn statementTokenEnd(tokens: []const token_mod.Token) usize {
    var end = tokens.len;
    while (end > 0 and tokens[end - 1].kind == .semicolon) end -= 1;
    return end;
}

fn sourceSpanForTokenRange(tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) ?token_mod.SourceSpan {
    if (range.start >= range.end or range.end > tokens.len) return null;
    return .{
        .start = tokens[range.start].source_start,
        .end = tokens[range.end - 1].source_end,
    };
}

test "generated SQL parser accepts session and control statements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    for (first_family_corpus) |case| {
        const result = try parseSqlAlloc(alloc, case.sql);
        try std.testing.expectEqual(case.kind, result.kind);
    }
}

test "generated SQL parser facade classifies gated corpus" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const corpus = first_family_corpus ++ simple_ddl_corpus ++ simple_dml_corpus ++ simple_read_corpus ++ antfly_extension_read_corpus ++ simple_graph_corpus ++ unsupported_corpus;
    for (corpus) |case| {
        const generated_result = parseSqlAlloc(alloc, case.sql) catch |err| {
            std.debug.print("generated parser rejected corpus SQL: {s}\n", .{case.sql});
            return err;
        };
        try std.testing.expectEqual(case.kind, generated_result.kind);
    }
}

test "generated SQL parser facade exposes typed statement nodes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    try std.testing.expectEqual(GeneratedSqlStatement{ .session = .set }, (try parseSqlAlloc(alloc, "SET search_path TO public")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .commit }, (try parseSqlAlloc(alloc, "END WORK")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .rollback }, (try parseSqlAlloc(alloc, "ROLLBACK")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .prepared = .execute }, (try parseSqlAlloc(alloc, "EXECUTE read_stmt()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .prepared = .deallocate }, (try parseSqlAlloc(alloc, "DEALLOCATE PREPARE read_stmt")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .prepared = .deallocate }, (try parseSqlAlloc(alloc, "DEALLOCATE ALL")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_table }, (try parseSqlAlloc(alloc, "CREATE TABLE usage_records (id text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_table }, (try parseSqlAlloc(alloc, "CREATE TEMP TABLE usage_session_records (id uuid)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_table }, (try parseSqlAlloc(alloc, "CREATE UNLOGGED TABLE IF NOT EXISTS usage_ingest_records (id uuid)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_view }, (try parseSqlAlloc(alloc, "CREATE VIEW active_usage AS SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_materialized_view }, (try parseSqlAlloc(alloc, "CREATE MATERIALIZED VIEW usage_summary AS SELECT status FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_view }, (try parseSqlAlloc(alloc, "ALTER VIEW active_usage RENAME TO active_usage_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_view }, (try parseSqlAlloc(alloc, "DROP VIEW active_usage")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_materialized_view }, (try parseSqlAlloc(alloc, "DROP MATERIALIZED VIEW usage_summary")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .refresh_materialized_view }, (try parseSqlAlloc(alloc, "REFRESH MATERIALIZED VIEW usage_summary")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_domain }, (try parseSqlAlloc(alloc, "CREATE DOMAIN positive_amount AS numeric")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_domain }, (try parseSqlAlloc(alloc, "ALTER DOMAIN positive_amount SET NOT NULL")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_domain }, (try parseSqlAlloc(alloc, "DROP DOMAIN positive_amount")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_sequence }, (try parseSqlAlloc(alloc, "CREATE SEQUENCE order_id_seq AS bigint START WITH 10")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_sequence }, (try parseSqlAlloc(alloc, "ALTER SEQUENCE IF EXISTS order_id_seq RESTART WITH 1000")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_sequence }, (try parseSqlAlloc(alloc, "DROP SEQUENCE IF EXISTS order_id_seq CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_enum_type }, (try parseSqlAlloc(alloc, "CREATE TYPE usage_status AS ENUM ('open', 'done')")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_enum_type }, (try parseSqlAlloc(alloc, "ALTER TYPE usage_status ADD VALUE IF NOT EXISTS 'archived' AFTER 'done'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_enum_type }, (try parseSqlAlloc(alloc, "DROP TYPE IF EXISTS usage_status CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_tablespace }, (try parseSqlAlloc(alloc, "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_tablespace }, (try parseSqlAlloc(alloc, "ALTER TABLESPACE fastspace RENAME TO fastspace_archive")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_tablespace }, (try parseSqlAlloc(alloc, "DROP TABLESPACE IF EXISTS fastspace_archive")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_publication }, (try parseSqlAlloc(alloc, "CREATE PUBLICATION usage_pub FOR TABLE usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_publication }, (try parseSqlAlloc(alloc, "ALTER PUBLICATION usage_pub ADD TABLE usage_events")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_publication }, (try parseSqlAlloc(alloc, "DROP PUBLICATION IF EXISTS usage_pub")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_subscription }, (try parseSqlAlloc(alloc, "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=db' PUBLICATION usage_pub")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_subscription }, (try parseSqlAlloc(alloc, "ALTER SUBSCRIPTION usage_sub DISABLE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_subscription }, (try parseSqlAlloc(alloc, "DROP SUBSCRIPTION IF EXISTS usage_sub")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_policy }, (try parseSqlAlloc(alloc, "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_policy }, (try parseSqlAlloc(alloc, "ALTER POLICY usage_policy ON usage_records RENAME TO usage_policy_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_policy }, (try parseSqlAlloc(alloc, "DROP POLICY IF EXISTS usage_policy ON usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_function }, (try parseSqlAlloc(alloc, "CREATE FUNCTION audit_changes() RETURNS trigger LANGUAGE plpgsql")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_function }, (try parseSqlAlloc(alloc, "CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_procedure }, (try parseSqlAlloc(alloc, "CREATE PROCEDURE rotate_usage() LANGUAGE plpgsql")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_function }, (try parseSqlAlloc(alloc, "DROP FUNCTION IF EXISTS audit_changes(text) CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_procedure }, (try parseSqlAlloc(alloc, "DROP PROCEDURE rotate_usage()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_role }, (try parseSqlAlloc(alloc, "CREATE ROLE app_writer")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_role }, (try parseSqlAlloc(alloc, "CREATE USER app_writer")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_role }, (try parseSqlAlloc(alloc, "CREATE GROUP app_readers")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_role }, (try parseSqlAlloc(alloc, "ALTER ROLE app_writer RESET statement_timeout")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_role }, (try parseSqlAlloc(alloc, "ALTER USER app_writer RESET statement_timeout")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_role }, (try parseSqlAlloc(alloc, "ALTER GROUP app_readers RESET statement_timeout")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_role }, (try parseSqlAlloc(alloc, "DROP ROLE IF EXISTS app_writer")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_role }, (try parseSqlAlloc(alloc, "DROP USER IF EXISTS app_writer")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_role }, (try parseSqlAlloc(alloc, "DROP GROUP IF EXISTS app_readers")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_collation }, (try parseSqlAlloc(alloc, "CREATE COLLATION case_insensitive (provider = icu, locale = 'und-u-ks-level2')")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_collation }, (try parseSqlAlloc(alloc, "ALTER COLLATION case_insensitive RENAME TO ci_text")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_collation }, (try parseSqlAlloc(alloc, "DROP COLLATION IF EXISTS ci_text")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_operator }, (try parseSqlAlloc(alloc, "CREATE OPERATOR === (FUNCTION = text_eq, LEFTARG = text, RIGHTARG = text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_operator }, (try parseSqlAlloc(alloc, "DROP OPERATOR === (text, text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_aggregate }, (try parseSqlAlloc(alloc, "ALTER AGGREGATE first_value_text(text) OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_operator }, (try parseSqlAlloc(alloc, "ALTER OPERATOR === (text, text) OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_operator_class }, (try parseSqlAlloc(alloc, "ALTER OPERATOR CLASS usage_ops USING btree RENAME TO usage_ops_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_operator_family }, (try parseSqlAlloc(alloc, "ALTER OPERATOR FAMILY usage_family USING btree RENAME TO usage_family_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_operator_class }, (try parseSqlAlloc(alloc, "CREATE OPERATOR CLASS usage_ops DEFAULT FOR TYPE text USING btree AS OPERATOR 1 < (text, text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_operator_family }, (try parseSqlAlloc(alloc, "CREATE OPERATOR FAMILY usage_family USING btree")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_operator_class }, (try parseSqlAlloc(alloc, "DROP OPERATOR CLASS IF EXISTS usage_ops USING btree")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_operator_family }, (try parseSqlAlloc(alloc, "DROP OPERATOR FAMILY IF EXISTS usage_family USING btree")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_aggregate }, (try parseSqlAlloc(alloc, "CREATE AGGREGATE first_value_text(text) (SFUNC = first_sfunc, STYPE = text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_aggregate }, (try parseSqlAlloc(alloc, "DROP AGGREGATE first_value_text(text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .create_cast }, (try parseSqlAlloc(alloc, "CREATE CAST (jsonb AS text) WITH FUNCTION jsonb_to_text(jsonb) AS ASSIGNMENT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_cast }, (try parseSqlAlloc(alloc, "DROP CAST (jsonb AS text)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .relation_population }, (try parseSqlAlloc(alloc, "SELECT account_id, total INTO usage_archive FROM usage_records WHERE total > 10")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .relation_population }, (try parseSqlAlloc(alloc, "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .alter_schema }, (try parseSqlAlloc(alloc, "ALTER SCHEMA analytics RENAME TO reporting")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .ddl = .drop_schema }, (try parseSqlAlloc(alloc, "DROP SCHEMA analytics CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .extension_index = .create_index }, (try parseSqlAlloc(alloc, "CREATE INDEX usage_status_idx ON usage_records (status)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .extension_index = .drop_index }, (try parseSqlAlloc(alloc, "DROP INDEX usage_status_idx")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .extension_index = .create_extension }, (try parseSqlAlloc(alloc, "CREATE EXTENSION vector")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .extension_index = .drop_extension }, (try parseSqlAlloc(alloc, "DROP EXTENSION vector")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records (id) VALUES ('u1')")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records DEFAULT VALUES")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records AS u (id, status) VALUES ('u8', 'ready') RETURNING u.id, u.status AS returned_status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO products (product_id, product_name, price, valid_at) VALUES (4, 'constructor', 26.5, daterange(DATE '2025-02-01', DATE '2025-03-01')) RETURNING *")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO users (id, email, name) VALUES ('u2', 'A@EXAMPLE.TEST', 'new') ON CONFLICT (lower(email)) DO UPDATE SET name = excluded.name RETURNING id, name")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO users (id, tenant_id, email, status, name) VALUES ('u2', 't1', 'A@EXAMPLE.TEST', 'active', 'new') ON CONFLICT (tenant_id, (lower(email))) DO UPDATE SET name = excluded.name RETURNING id, name")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_select }, (try parseSqlAlloc(alloc, "INSERT INTO ONLY public.usage_records (id, status, amount) SELECT archive_id, archive_status, archive_amount FROM ONLY public.archived_records WHERE archive_status = 'ready' RETURNING id, status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET status = 'done' WHERE id = 'u1'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET status = 'processing' WHERE status = 'queued' ORDER BY id ASC OFFSET 1 ROW FETCH FIRST ROW ONLY FOR UPDATE RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE prices FOR PORTION OF valid_time FROM 3 TO 7 SET price = 99 WHERE sku = 'sku:a' RETURNING *")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET (quantity, status) = ROW(source.source_quantity, DEFAULT) FROM source_records AS source WHERE usage_records.source_id = source.source_pk FOR UPDATE RETURNING id, quantity, status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET (status, email) = ($1, lower(email)) WHERE id = $2 RETURNING status, email")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_values }, (try parseSqlAlloc(alloc, "INSERT INTO usage_records (id, email, next_status) VALUES ('u2', 'a@example.test', 'ACTIVE') ON CONFLICT (email) DO UPDATE SET status = overlay(status placing excluded.next_status from 2 for 1) RETURNING id, status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM usage_records AS u WHERE status = 'expired' FOR UPDATE RETURNING u.*")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM ONLY public.usage_records WHERE id = $1 RETURNING id, LOWER(status) AS status_key")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM usage_records WHERE status = 'expired' ORDER BY expires_at ASC LIMIT 10 RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM prices FOR PORTION OF valid_time FROM 2 TO 8 WHERE sku = 'sku:b' RETURNING *")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .truncate }, (try parseSqlAlloc(alloc, "TRUNCATE TABLE ONLY usage_records RESTRICT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .truncate }, (try parseSqlAlloc(alloc, "TRUNCATE TABLE ONLY public.usage_records CONTINUE IDENTITY RESTRICT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .insert_select }, (try parseSqlAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "WITH source_rows AS NOT MATERIALIZED (SELECT id FROM usage_records) DELETE FROM usage_records USING source_rows WHERE usage_records.id = source_rows.id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .merge }, (try parseSqlAlloc(alloc, "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) MERGE INTO usage_records USING source_rows ON usage_records.id = source_rows.id WHEN MATCHED THEN DELETE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "WITH ready_sources AS (SELECT source_pk, source_status FROM source_records WHERE source_status = 'ready') UPDATE usage_records SET status = source.source_status FROM ready_sources AS source WHERE usage_records.source_id = source.source_pk FOR UPDATE RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .merge }, (try parseSqlAlloc(alloc, "WITH source_rows AS (SELECT id, status, amount FROM usage_records WHERE id IN ('cte-merge-source-update', 'cte-merge-source-insert')) MERGE INTO usage_records AS target USING source_rows AS source ON target.id = source.status WHEN MATCHED THEN UPDATE SET status = 'cte-merged', amount = source.amount WHEN NOT MATCHED AND source.id = 'cte-merge-source-insert' THEN INSERT (id, status, amount) VALUES (source.status, 'cte-inserted', source.amount) RETURNING target.id, target.status, target.amount")).statement);
}

test "generated SQL parser facade exposes typed read and unsupported statement nodes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .aggregate }, (try parseSqlAlloc(alloc, "SELECT COUNT(DISTINCT *) AS row_count FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .aggregate }, (try parseSqlAlloc(alloc, "SELECT percentile_cont([0.25, 0.75]) WITHIN GROUP (ORDER BY amount) AS bad FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT substring(status FROM 2 FOR 3) AS status_mid FROM usage_records WHERE substr(status, 1, 2) = $1 ORDER BY substring(status FROM 2) ASC LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT overlay(status placing 'X' from 2 for 3) AS status_overlay FROM usage_records WHERE overlay(status placing 'X' from 2) = $1 ORDER BY overlay(status placing 'Y' from 2 for 1) ASC LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT strpos(status, '-') AS dash_pos FROM usage_records WHERE position('-' in status) > $1 ORDER BY strpos(status, '-') ASC LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT _id FROM docs offset LIMIT 10")).statement);
    try std.testing.expectError(error.UnexpectedToken, parseSqlAlloc(alloc, "SELECT count(*) AS row_count FROM docs offset"));
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT _id FROM docs FETCH FIRST 10 ROWS ONLY")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT _id FROM docs AS fetch LIMIT 10")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .join }, (try parseSqlAlloc(alloc, "SELECT d._id FROM docs d JOIN other o ON d._id = o.doc_id WHERE d.status = 'active' LIMIT 10")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .aggregate }, (try parseSqlAlloc(alloc, "SELECT count(*) AS row_count FROM docs d WHERE d.status = 'active' GROUP BY d.status LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .window }, (try parseSqlAlloc(alloc, "SELECT tenant, id, bool_or(enabled) FILTER (WHERE status = 'open') OVER (PARTITION BY tenant ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS any_enabled FROM usage_records WHERE status = 'open' ORDER BY any_enabled DESC LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .cte }, (try parseSqlAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .query }, (try parseSqlAlloc(alloc, "SELECT id FROM usage_records FOR UPDATE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .cte }, (try parseSqlAlloc(alloc, "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows FOR SHARE NOWAIT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .graph = .create_index }, (try parseSqlAlloc(alloc, "CREATE GRAPH INDEX docs_edge_graph ON doc_edges")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .graph = .create_metric }, (try parseSqlAlloc(alloc, "CREATE GRAPH METRIC docs_pagerank ON doc_edges")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .graph = .alter_metric }, (try parseSqlAlloc(alloc, "ALTER GRAPH INDEX docs_edge_graph ADD METRIC pagerank_v1 USING pagerank")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .analyze }, (try parseSqlAlloc(alloc, "ANALYZE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_conversion }, (try parseSqlAlloc(alloc, "ALTER CONVERSION usage_conv RENAME TO usage_conv_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_default_privileges }, (try parseSqlAlloc(alloc, "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_event_trigger }, (try parseSqlAlloc(alloc, "ALTER EVENT TRIGGER usage_ddl_start DISABLE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_index }, (try parseSqlAlloc(alloc, "ALTER INDEX usage_status_idx RENAME TO usage_status_idx_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_materialized_view }, (try parseSqlAlloc(alloc, "ALTER MATERIALIZED VIEW usage_summary RENAME TO usage_summary_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_rule }, (try parseSqlAlloc(alloc, "ALTER RULE usage_insert ON usage_records RENAME TO usage_insert_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_statistics }, (try parseSqlAlloc(alloc, "ALTER STATISTICS usage_stats SET STATISTICS 100")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_system }, (try parseSqlAlloc(alloc, "ALTER SYSTEM SET work_mem = '64MB'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_text_search_configuration }, (try parseSqlAlloc(alloc, "ALTER TEXT SEARCH CONFIGURATION usage_search RENAME TO usage_search_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_text_search_dictionary }, (try parseSqlAlloc(alloc, "ALTER TEXT SEARCH DICTIONARY usage_dict OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_text_search_parser }, (try parseSqlAlloc(alloc, "ALTER TEXT SEARCH PARSER usage_parser RENAME TO usage_parser_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_text_search_template }, (try parseSqlAlloc(alloc, "ALTER TEXT SEARCH TEMPLATE usage_template RENAME TO usage_template_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_trigger }, (try parseSqlAlloc(alloc, "ALTER TRIGGER usage_audit ON usage_records RENAME TO usage_audit_v2")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_large_object }, (try parseSqlAlloc(alloc, "ALTER LARGE OBJECT 12345 OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .call }, (try parseSqlAlloc(alloc, "CALL refresh_usage_records()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .checkpoint }, (try parseSqlAlloc(alloc, "CHECKPOINT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .cursor = .close }, (try parseSqlAlloc(alloc, "CLOSE usage_cursor")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .cluster }, (try parseSqlAlloc(alloc, "CLUSTER usage_records USING usage_status_idx")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .comment }, (try parseSqlAlloc(alloc, "COMMENT ON TABLE usage_records IS 'billing rows'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .copy }, (try parseSqlAlloc(alloc, "COPY usage_records FROM STDIN")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_access_method }, (try parseSqlAlloc(alloc, "CREATE ACCESS METHOD usage_am TYPE INDEX HANDLER usage_handler")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_conversion }, (try parseSqlAlloc(alloc, "CREATE CONVERSION usage_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_database_options }, (try parseSqlAlloc(alloc, "CREATE DATABASE tenant_ops WITH OWNER app")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_schema_options }, (try parseSqlAlloc(alloc, "CREATE SCHEMA analytics AUTHORIZATION app_user")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_event_trigger }, (try parseSqlAlloc(alloc, "CREATE EVENT TRIGGER usage_ddl_start ON ddl_command_start EXECUTE FUNCTION audit_ddl()")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .cursor = .declare }, (try parseSqlAlloc(alloc, "DECLARE usage_cursor CURSOR FOR SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_access_method }, (try parseSqlAlloc(alloc, "DROP ACCESS METHOD IF EXISTS usage_am")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_conversion }, (try parseSqlAlloc(alloc, "DROP CONVERSION IF EXISTS usage_conv")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_event_trigger }, (try parseSqlAlloc(alloc, "DROP EVENT TRIGGER IF EXISTS usage_ddl_start")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_collation_multi }, (try parseSqlAlloc(alloc, "DROP COLLATION case_insensitive, accent_insensitive")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_domain_multi }, (try parseSqlAlloc(alloc, "DROP DOMAIN positive_amount, nonempty_text CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_extension_multi }, (try parseSqlAlloc(alloc, "DROP EXTENSION vector, postgis")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_index_multi }, (try parseSqlAlloc(alloc, "DROP INDEX usage_status_idx, usage_tenant_idx CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_materialized_view_multi }, (try parseSqlAlloc(alloc, "DROP MATERIALIZED VIEW usage_summary, old_usage_summary CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_owned }, (try parseSqlAlloc(alloc, "DROP OWNED BY usage_role CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_publication_multi }, (try parseSqlAlloc(alloc, "DROP PUBLICATION usage_pub, old_usage_pub")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_routine }, (try parseSqlAlloc(alloc, "DROP ROUTINE IF EXISTS normalize_status(text) CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_role_multi }, (try parseSqlAlloc(alloc, "DROP ROLE app_writer, app_reader")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_schema_multi }, (try parseSqlAlloc(alloc, "DROP SCHEMA analytics, reporting")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_sequence_multi }, (try parseSqlAlloc(alloc, "DROP SEQUENCE order_id_seq, old_order_id_seq CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_statistics }, (try parseSqlAlloc(alloc, "DROP STATISTICS IF EXISTS usage_stats")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_table_multi }, (try parseSqlAlloc(alloc, "DROP TABLE usage_records, archived_usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_type_multi }, (try parseSqlAlloc(alloc, "DROP TYPE usage_status, archived_status CASCADE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_text_search_configuration }, (try parseSqlAlloc(alloc, "DROP TEXT SEARCH CONFIGURATION IF EXISTS usage_search")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .explain }, (try parseSqlAlloc(alloc, "EXPLAIN SELECT id FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .cursor = .fetch }, (try parseSqlAlloc(alloc, "FETCH FROM usage_cursor")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .grant }, (try parseSqlAlloc(alloc, "GRANT SELECT ON TABLE usage_records TO readonly")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .listen }, (try parseSqlAlloc(alloc, "LISTEN usage_events")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .load }, (try parseSqlAlloc(alloc, "LOAD 'auto_explain'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .lock }, (try parseSqlAlloc(alloc, "LOCK TABLE usage_records IN SHARE MODE")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .move }, (try parseSqlAlloc(alloc, "MOVE FROM usage_cursor")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .notify }, (try parseSqlAlloc(alloc, "NOTIFY usage_events, 'changed'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_statistics }, (try parseSqlAlloc(alloc, "CREATE STATISTICS usage_stats ON tenant_id, status FROM usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_text_search_configuration }, (try parseSqlAlloc(alloc, "CREATE TEXT SEARCH CONFIGURATION usage_search (COPY = pg_catalog.english)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_text_search_dictionary }, (try parseSqlAlloc(alloc, "CREATE TEXT SEARCH DICTIONARY usage_dict (TEMPLATE = simple)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_text_search_parser }, (try parseSqlAlloc(alloc, "CREATE TEXT SEARCH PARSER usage_parser (START = prsd_start)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_text_search_template }, (try parseSqlAlloc(alloc, "CREATE TEXT SEARCH TEMPLATE usage_template (LEXIZE = dsimple_lexize)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .create_transform }, (try parseSqlAlloc(alloc, "CREATE TRANSFORM FOR jsonb LANGUAGE plpgsql FROM SQL WITH FUNCTION jsonb_to_plpgsql(internal)")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_transform }, (try parseSqlAlloc(alloc, "ALTER TRANSFORM FOR jsonb LANGUAGE plpgsql OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .alter_routine }, (try parseSqlAlloc(alloc, "ALTER ROUTINE normalize_status(text) OWNER TO app_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_text_search_dictionary }, (try parseSqlAlloc(alloc, "DROP TEXT SEARCH DICTIONARY IF EXISTS usage_dict")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_text_search_parser }, (try parseSqlAlloc(alloc, "DROP TEXT SEARCH PARSER IF EXISTS usage_parser")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_text_search_template }, (try parseSqlAlloc(alloc, "DROP TEXT SEARCH TEMPLATE IF EXISTS usage_template")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_transform }, (try parseSqlAlloc(alloc, "DROP TRANSFORM FOR jsonb LANGUAGE plpgsql")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .drop_view_multi }, (try parseSqlAlloc(alloc, "DROP VIEW active_usage, archived_usage")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .vacuum }, (try parseSqlAlloc(alloc, "VACUUM FULL usage_records")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .reindex }, (try parseSqlAlloc(alloc, "REINDEX INDEX usage_records_status_idx")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .release_savepoint }, (try parseSqlAlloc(alloc, "RELEASE SAVEPOINT usage_batch")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .reassign_owned }, (try parseSqlAlloc(alloc, "REASSIGN OWNED BY old_role TO new_role")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .revoke }, (try parseSqlAlloc(alloc, "REVOKE SELECT ON TABLE usage_records FROM readonly")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .transaction = .savepoint }, (try parseSqlAlloc(alloc, "SAVEPOINT usage_batch")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .security_label }, (try parseSqlAlloc(alloc, "SECURITY LABEL ON TABLE usage_records IS 'internal'")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .unsupported = .unlisten }, (try parseSqlAlloc(alloc, "UNLISTEN *")).statement);
}

test "generated SQL parser facade accepts promoted DML and read edge shapes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET status = 'processing' WHERE status = 'queued' ORDER BY id ASC OFFSET 1 ROW FETCH FIRST ROW ONLY FOR UPDATE RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE prices FOR PORTION OF valid_time FROM 3 TO 7 SET price = 99 WHERE sku = 'sku:a' RETURNING *")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET (quantity, status) = ROW(source.source_quantity, DEFAULT) FROM source_records AS source WHERE usage_records.source_id = source.source_pk FOR UPDATE RETURNING id, quantity, status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM usage_records AS u WHERE status = 'expired' FOR UPDATE RETURNING u.*")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM usage_records WHERE status = 'expired' ORDER BY expires_at ASC LIMIT 10 RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .delete }, (try parseSqlAlloc(alloc, "DELETE FROM ONLY public.usage_records WHERE id = $1 RETURNING id, LOWER(status) AS status_key")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .update }, (try parseSqlAlloc(alloc, "UPDATE usage_records SET status = 'archived' WHERE (usage_records.id, usage_records.status) IN (SELECT archived_records.organization_id, archived_records.status FROM archived_records) FOR UPDATE SKIP LOCKED RETURNING id")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .merge }, (try parseSqlAlloc(alloc, "MERGE INTO ONLY public.usage_records AS target USING ONLY public.archived_records AS source ON target.id = source.archive_id WHEN MATCHED THEN UPDATE SET status = source.archive_status WHEN NOT MATCHED THEN INSERT (id, status, amount) VALUES (source.archive_id, source.archive_status, source.archive_amount) RETURNING target.id, target.status")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .dml = .truncate }, (try parseSqlAlloc(alloc, "TRUNCATE TABLE ONLY usage_records RESTRICT")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .join }, (try parseSqlAlloc(alloc, "SELECT d._id FROM docs d JOIN other o ON d._id = o.doc_id WHERE d.status = 'active' LIMIT 10")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .aggregate }, (try parseSqlAlloc(alloc, "SELECT count(*) AS row_count FROM docs d WHERE d.status = 'active' GROUP BY d.status LIMIT 5")).statement);
    try std.testing.expectEqual(GeneratedSqlStatement{ .read = .window }, (try parseSqlAlloc(alloc, "SELECT tenant, id, bool_or(enabled) FILTER (WHERE status = 'open') OVER (PARTITION BY tenant ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS any_enabled FROM usage_records WHERE status = 'open' ORDER BY any_enabled DESC LIMIT 5")).statement);
}

test "generated SQL parser facade builds control AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const set_sql = "  SET antfly.sync_level = 'write';";
    var set_tokens = try lexer.tokenizeAlloc(alloc, set_sql);
    defer lexer.freeTokens(alloc, &set_tokens);
    const set_result = try parseTokensAlloc(alloc, set_tokens.items);
    switch (set_result.ast.?) {
        .session => |session| {
            try std.testing.expectEqual(GeneratedSqlSessionKind.set, session.kind);
            try std.testing.expectEqualStrings("SET antfly.sync_level = 'write'", spanText(set_sql, session.statement_span));
            try std.testing.expectEqualStrings("SET", spanText(set_sql, session.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, session.name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const set_local_sql = "SET LOCAL antfly.sync_level = 'propose';";
    const set_local_result = try parseSqlAlloc(alloc, set_local_sql);
    switch (set_local_result.ast.?) {
        .session => |session| {
            try std.testing.expectEqual(GeneratedSqlSessionKind.set, session.kind);
            try std.testing.expectEqualStrings("SET LOCAL antfly.sync_level = 'propose'", spanText(set_local_sql, session.statement_span));
            try std.testing.expectEqualStrings("SET", spanText(set_local_sql, session.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, session.name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, session.value_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const reset_all_sql = "RESET ALL;";
    const reset_all_result = try parseSqlAlloc(alloc, reset_all_sql);
    switch (reset_all_result.ast.?) {
        .session => |session| {
            try std.testing.expectEqual(GeneratedSqlSessionKind.reset, session.kind);
            try std.testing.expectEqualStrings("RESET ALL", spanText(reset_all_sql, session.statement_span));
            try std.testing.expectEqualStrings("RESET", spanText(reset_all_sql, session.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, session.name_tokens.?);
            try std.testing.expect(session.value_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const show_all_sql = "SHOW ALL;";
    const show_all_result = try parseSqlAlloc(alloc, show_all_sql);
    switch (show_all_result.ast.?) {
        .session => |session| {
            try std.testing.expectEqual(GeneratedSqlSessionKind.show, session.kind);
            try std.testing.expectEqualStrings("SHOW ALL", spanText(show_all_sql, session.statement_span));
            try std.testing.expectEqualStrings("SHOW", spanText(show_all_sql, session.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, session.name_tokens.?);
            try std.testing.expect(session.value_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const transaction_sql = "ROLLBACK TRANSACTION;";
    const transaction_result = try parseSqlAlloc(alloc, transaction_sql);
    switch (transaction_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.rollback, transaction.kind);
            try std.testing.expectEqualStrings("ROLLBACK TRANSACTION", spanText(transaction_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("ROLLBACK", spanText(transaction_sql, transaction.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, transaction.boundary_tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const end_transaction_sql = "END TRANSACTION;";
    const end_transaction_result = try parseSqlAlloc(alloc, end_transaction_sql);
    switch (end_transaction_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.commit, transaction.kind);
            try std.testing.expectEqualStrings("END TRANSACTION", spanText(end_transaction_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("END", spanText(end_transaction_sql, transaction.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, transaction.boundary_tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const transaction_mode_sql = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ;";
    const transaction_mode_result = try parseSqlAlloc(alloc, transaction_mode_sql);
    switch (transaction_mode_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.start_transaction, transaction.kind);
            try std.testing.expectEqualStrings("START TRANSACTION ISOLATION LEVEL REPEATABLE READ", spanText(transaction_mode_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("START", spanText(transaction_mode_sql, transaction.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, transaction.mode_tokens.?);
            try std.testing.expect(transaction.boundary_tail_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const release_savepoint_sql = "RELEASE SAVEPOINT before_retry;";
    const release_savepoint_result = try parseSqlAlloc(alloc, release_savepoint_sql);
    switch (release_savepoint_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.release_savepoint, transaction.kind);
            try std.testing.expectEqualStrings("RELEASE SAVEPOINT before_retry", spanText(release_savepoint_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("RELEASE", spanText(release_savepoint_sql, transaction.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, transaction.name_tokens.?);
            try std.testing.expect(transaction.boundary_tail_tokens == null);
            try std.testing.expect(transaction.mode_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const rollback_to_savepoint_sql = "ROLLBACK TO before_retry;";
    const rollback_to_savepoint_result = try parseSqlAlloc(alloc, rollback_to_savepoint_sql);
    switch (rollback_to_savepoint_result.ast.?) {
        .transaction => |transaction| {
            try std.testing.expectEqual(GeneratedSqlTransactionKind.rollback_to_savepoint, transaction.kind);
            try std.testing.expectEqualStrings("ROLLBACK TO before_retry", spanText(rollback_to_savepoint_sql, transaction.statement_span));
            try std.testing.expectEqualStrings("ROLLBACK", spanText(rollback_to_savepoint_sql, transaction.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, transaction.name_tokens.?);
            try std.testing.expect(transaction.boundary_tail_tokens == null);
            try std.testing.expect(transaction.mode_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const prepare_sql = "PREPARE read_stmt(text) AS SELECT id FROM usage_records WHERE status = $1";
    var prepare_tokens = try lexer.tokenizeAlloc(alloc, prepare_sql);
    defer lexer.freeTokens(alloc, &prepare_tokens);
    const prepare_result = try parseTokensAlloc(alloc, prepare_tokens.items);
    switch (prepare_result.ast.?) {
        .prepared => |prepared| {
            try std.testing.expectEqual(GeneratedSqlPreparedKind.prepare, prepared.kind);
            try std.testing.expectEqualStrings("PREPARE", spanText(prepare_sql, prepared.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, prepared.name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 5 }, prepared.parameter_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 14 }, prepared.inner_statement_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const deallocate_all_sql = "DEALLOCATE ALL";
    var deallocate_all_tokens = try lexer.tokenizeAlloc(alloc, deallocate_all_sql);
    defer lexer.freeTokens(alloc, &deallocate_all_tokens);
    const deallocate_all_result = try parseTokensAlloc(alloc, deallocate_all_tokens.items);
    switch (deallocate_all_result.ast.?) {
        .prepared => |prepared| {
            try std.testing.expectEqual(GeneratedSqlPreparedKind.deallocate, prepared.kind);
            try std.testing.expectEqualStrings("DEALLOCATE", spanText(deallocate_all_sql, prepared.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, prepared.name_tokens.?);
            try std.testing.expect(prepared.parameter_tokens == null);
            try std.testing.expect(prepared.argument_tokens == null);
            try std.testing.expect(prepared.inner_statement_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const deallocate_prepare_sql = "DEALLOCATE PREPARE read_stmt";
    var deallocate_prepare_tokens = try lexer.tokenizeAlloc(alloc, deallocate_prepare_sql);
    defer lexer.freeTokens(alloc, &deallocate_prepare_tokens);
    const deallocate_prepare_result = try parseTokensAlloc(alloc, deallocate_prepare_tokens.items);
    switch (deallocate_prepare_result.ast.?) {
        .prepared => |prepared| {
            try std.testing.expectEqual(GeneratedSqlPreparedKind.deallocate, prepared.kind);
            try std.testing.expectEqualStrings("DEALLOCATE", spanText(deallocate_prepare_sql, prepared.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, prepared.name_tokens.?);
            try std.testing.expect(prepared.parameter_tokens == null);
            try std.testing.expect(prepared.argument_tokens == null);
            try std.testing.expect(prepared.inner_statement_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const cursor_sql = "FETCH FORWARD 10 IN usage_cursor;";
    const cursor_result = try parseSqlAlloc(alloc, cursor_sql);
    switch (cursor_result.ast.?) {
        .cursor => |cursor| {
            try std.testing.expectEqual(GeneratedSqlCursorKind.fetch, cursor.kind);
            try std.testing.expectEqualStrings("FETCH FORWARD 10 IN usage_cursor", spanText(cursor_sql, cursor.statement_span));
            try std.testing.expectEqualStrings("FETCH", spanText(cursor_sql, cursor.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 5 }, cursor.tail_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ddl_sql = "CREATE SCHEMA IF NOT EXISTS analytics";
    const ddl_result = try parseSqlAlloc(alloc, ddl_sql);
    switch (ddl_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_schema, ddl.kind);
            try std.testing.expectEqualStrings("CREATE SCHEMA IF NOT EXISTS analytics", spanText(ddl_sql, ddl.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(ddl_sql, ddl.command_span));
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_unlogged_table_sql = "CREATE UNLOGGED TABLE IF NOT EXISTS usage_ingest_records (id uuid)";
    const create_unlogged_table_result = try parseSqlAlloc(alloc, create_unlogged_table_sql);
    switch (create_unlogged_table_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_table, ddl.kind);
            try std.testing.expectEqualStrings(create_unlogged_table_sql, spanText(create_unlogged_table_sql, ddl.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(create_unlogged_table_sql, ddl.command_span));
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_view_sql = "CREATE OR REPLACE VIEW IF NOT EXISTS active_usage AS SELECT id, status FROM usage_records";
    const create_view_result = try parseSqlAlloc(alloc, create_view_sql);
    switch (create_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_view, ddl.kind);
            try std.testing.expect(ddl.replace_existing);
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_materialized_view_sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS usage_summary AS SELECT status FROM usage_records WITH NO DATA";
    const create_materialized_view_result = try parseSqlAlloc(alloc, create_materialized_view_sql);
    switch (create_materialized_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_materialized_view, ddl.kind);
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 15 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_domain_sql = "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)";
    const create_domain_result = try parseSqlAlloc(alloc, create_domain_sql);
    switch (create_domain_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_domain, ddl.kind);
            try std.testing.expectEqualStrings("CREATE", spanText(create_domain_sql, ddl.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 11 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_sequence_sql = "CREATE SEQUENCE IF NOT EXISTS order_id_seq AS bigint START WITH 10";
    const create_sequence_result = try parseSqlAlloc(alloc, create_sequence_sql);
    switch (create_sequence_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_sequence, ddl.kind);
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 11 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_enum_type_sql = "CREATE TYPE usage_status AS ENUM ('open', 'done')";
    const create_enum_type_result = try parseSqlAlloc(alloc, create_enum_type_sql);
    switch (create_enum_type_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_enum_type, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_tablespace_sql = "CREATE TABLESPACE fastspace LOCATION '/var/lib/antfly/fastspace'";
    const create_tablespace_result = try parseSqlAlloc(alloc, create_tablespace_sql);
    switch (create_tablespace_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_tablespace, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 5 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_publication_sql = "CREATE PUBLICATION usage_pub FOR TABLE usage_records";
    const create_publication_result = try parseSqlAlloc(alloc, create_publication_sql);
    switch (create_publication_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_publication, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_subscription_sql = "CREATE SUBSCRIPTION usage_sub CONNECTION 'host=db' PUBLICATION usage_pub";
    const create_subscription_result = try parseSqlAlloc(alloc, create_subscription_sql);
    switch (create_subscription_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_subscription, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 7 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_policy_sql = "CREATE POLICY usage_policy ON usage_records USING (tenant_id = current_user)";
    const create_policy_result = try parseSqlAlloc(alloc, create_policy_sql);
    switch (create_policy_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_policy, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.index_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_policy_sql = "ALTER POLICY usage_policy ON usage_records WITH CHECK (status = 'ready')";
    const alter_policy_result = try parseSqlAlloc(alloc, alter_policy_sql);
    switch (alter_policy_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_policy, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.index_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_policy_sql = "DROP POLICY IF EXISTS usage_policy ON usage_records CASCADE";
    const drop_policy_result = try parseSqlAlloc(alloc, drop_policy_sql);
    switch (drop_policy_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_policy, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl.index_table_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const extension_sql = "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public VERSION '1.3'";
    const extension_result = try parseSqlAlloc(alloc, extension_sql);
    switch (extension_result.ast.?) {
        .extension_index => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_extension, ddl.kind);
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, ddl.schema_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, ddl.version_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const select_into_sql = "SELECT account_id, total INTO public.usage_archive FROM usage_records WHERE total > 10";
    const select_into_result = try parseSqlAlloc(alloc, select_into_sql);
    switch (select_into_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.relation_population, ddl.kind);
            try std.testing.expectEqualStrings(select_into_sql, spanText(select_into_sql, ddl.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(select_into_sql, ddl.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_table_as_sql = "CREATE TEMP TABLE IF NOT EXISTS usage_session_archive AS SELECT account_id FROM usage_records WITH NO DATA";
    const create_table_as_result = try parseSqlAlloc(alloc, create_table_as_sql);
    switch (create_table_as_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.relation_population, ddl.kind);
            try std.testing.expectEqualStrings(create_table_as_sql, spanText(create_table_as_sql, ddl.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(create_table_as_sql, ddl.command_span));
            try std.testing.expect(ddl.if_not_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const create_index_sql = "CREATE INDEX docs_body_fts ON docs USING antfly_full_text (body) WITH (analyzer = 'standard')";
    const create_index_result = try parseSqlAlloc(alloc, create_index_sql);
    switch (create_index_result.ast.?) {
        .extension_index => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_index, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.index_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ddl.index_method_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, ddl.index_elements_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 16 }, ddl.index_options_tokens.?);
            try std.testing.expect(!ddl.unique);
            try std.testing.expect(ddl.index_include_tokens == null);
            try std.testing.expect(ddl.index_where_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const covering_partial_index_sql = "CREATE UNIQUE INDEX docs_status_active_idx ON docs (status) INCLUDE (tenant_id, amount) WHERE deleted_at IS NULL";
    const covering_partial_index_result = try parseSqlAlloc(alloc, covering_partial_index_sql);
    switch (covering_partial_index_result.ast.?) {
        .extension_index => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.create_index, ddl.kind);
            try std.testing.expect(ddl.unique);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.index_table_tokens.?);
            try std.testing.expect(ddl.index_method_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, ddl.index_elements_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 14 }, ddl.index_include_tokens.?);
            try std.testing.expect(ddl.index_options_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 19 }, ddl.index_where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_table_sql = "ALTER TABLE IF EXISTS ONLY docs ADD COLUMN status text";
    const alter_table_result = try parseSqlAlloc(alloc, alter_table_sql);
    switch (alter_table_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_table, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_database_sql = "ALTER DATABASE tenant_ops SET timezone TO 'UTC'";
    const alter_database_result = try parseSqlAlloc(alloc, alter_database_sql);
    switch (alter_database_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_database, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 7 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_extension_sql = "ALTER EXTENSION postgis UPDATE TO '3.5.0'";
    const alter_extension_result = try parseSqlAlloc(alloc, alter_extension_sql);
    switch (alter_extension_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_extension, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.version_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_schema_sql = "ALTER SCHEMA analytics RENAME TO reporting";
    const alter_schema_result = try parseSqlAlloc(alloc, alter_schema_sql);
    switch (alter_schema_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_schema, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_tablespace_sql = "ALTER TABLESPACE fastspace RENAME TO fastspace_archive";
    const alter_tablespace_result = try parseSqlAlloc(alloc, alter_tablespace_sql);
    switch (alter_tablespace_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_tablespace, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_publication_sql = "ALTER PUBLICATION usage_pub ADD TABLE usage_events";
    const alter_publication_result = try parseSqlAlloc(alloc, alter_publication_sql);
    switch (alter_publication_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_publication, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_subscription_sql = "ALTER SUBSCRIPTION usage_sub DISABLE";
    const alter_subscription_result = try parseSqlAlloc(alloc, alter_subscription_sql);
    switch (alter_subscription_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_subscription, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_view_sql = "ALTER VIEW active_usage RENAME TO active_usage_v2";
    const alter_view_result = try parseSqlAlloc(alloc, alter_view_sql);
    switch (alter_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_view, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_domain_sql = "ALTER DOMAIN positive_amount SET NOT NULL";
    const alter_domain_result = try parseSqlAlloc(alloc, alter_domain_sql);
    switch (alter_domain_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_domain, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_sequence_sql = "ALTER SEQUENCE IF EXISTS order_id_seq RESTART WITH 1000";
    const alter_sequence_result = try parseSqlAlloc(alloc, alter_sequence_sql);
    switch (alter_sequence_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_sequence, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_enum_type_sql = "ALTER TYPE usage_status ADD VALUE IF NOT EXISTS 'archived' AFTER 'done'";
    const alter_enum_type_result = try parseSqlAlloc(alloc, alter_enum_type_sql);
    switch (alter_enum_type_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.alter_enum_type, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 11 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_index_sql = "DROP INDEX IF EXISTS usage_status_idx";
    const drop_index_result = try parseSqlAlloc(alloc, drop_index_sql);
    switch (drop_index_result.ast.?) {
        .extension_index => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_index, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_table_sql = "DROP TABLE IF EXISTS usage_records CASCADE";
    const drop_table_result = try parseSqlAlloc(alloc, drop_table_sql);
    switch (drop_table_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_table, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_view_sql = "DROP VIEW IF EXISTS active_usage CASCADE";
    const drop_view_result = try parseSqlAlloc(alloc, drop_view_sql);
    switch (drop_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_view, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_materialized_view_sql = "DROP MATERIALIZED VIEW IF EXISTS usage_summary CASCADE";
    const drop_materialized_view_result = try parseSqlAlloc(alloc, drop_materialized_view_sql);
    switch (drop_materialized_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_materialized_view, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const refresh_materialized_view_sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY usage_summary WITH NO DATA";
    const refresh_materialized_view_result = try parseSqlAlloc(alloc, refresh_materialized_view_sql);
    switch (refresh_materialized_view_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.refresh_materialized_view, ddl.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, ddl.alter_table_operation_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_domain_sql = "DROP DOMAIN IF EXISTS positive_amount CASCADE";
    const drop_domain_result = try parseSqlAlloc(alloc, drop_domain_sql);
    switch (drop_domain_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_domain, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_sequence_sql = "DROP SEQUENCE IF EXISTS order_id_seq CASCADE";
    const drop_sequence_result = try parseSqlAlloc(alloc, drop_sequence_sql);
    switch (drop_sequence_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_sequence, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_enum_type_sql = "DROP TYPE IF EXISTS usage_status CASCADE";
    const drop_enum_type_result = try parseSqlAlloc(alloc, drop_enum_type_sql);
    switch (drop_enum_type_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_enum_type, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.cascade);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_tablespace_sql = "DROP TABLESPACE IF EXISTS fastspace_archive";
    const drop_tablespace_result = try parseSqlAlloc(alloc, drop_tablespace_sql);
    switch (drop_tablespace_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_tablespace, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_publication_sql = "DROP PUBLICATION IF EXISTS usage_pub";
    const drop_publication_result = try parseSqlAlloc(alloc, drop_publication_sql);
    switch (drop_publication_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_publication, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_subscription_sql = "DROP SUBSCRIPTION IF EXISTS usage_sub";
    const drop_subscription_result = try parseSqlAlloc(alloc, drop_subscription_sql);
    switch (drop_subscription_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_subscription, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const drop_sql = "DROP DATABASE IF EXISTS tenant_ops WITH (FORCE)";
    const drop_result = try parseSqlAlloc(alloc, drop_sql);
    switch (drop_result.ast.?) {
        .ddl => |ddl| {
            try std.testing.expectEqual(GeneratedSqlDdlKind.drop_database, ddl.kind);
            try std.testing.expect(ddl.if_exists);
            try std.testing.expect(ddl.force);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, ddl.object_name_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const dml_sql = "UPDATE usage_records SET status = 'done' WHERE id = 'u1'";
    const dml_result = try parseSqlAlloc(alloc, dml_sql);
    switch (dml_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.update, dml.kind);
            try std.testing.expectEqualStrings("UPDATE usage_records SET status = 'done' WHERE id = 'u1'", spanText(dml_sql, dml.statement_span));
            try std.testing.expectEqualStrings("UPDATE", spanText(dml_sql, dml.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, dml.assignments_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, dml.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_update_sql = "WITH source_rows AS (SELECT id FROM usage_records) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows) RETURNING id";
    const cte_update_result = try parseSqlAlloc(alloc, cte_update_sql);
    switch (cte_update_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.update, dml.kind);
            try std.testing.expectEqualStrings("UPDATE", spanText(cte_update_sql, dml.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, dml.cte_tokens.?);
            const cte_prefix = dml.cte_prefix orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, cte_prefix.list_tokens);
            try std.testing.expectEqual(@as(usize, 1), cte_prefix.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, cte_prefix.first_name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, cte_prefix.first_body_tokens);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, cte_prefix.first_body_read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, cte_prefix.first_body_read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, cte_prefix.first_body_read.source_tokens.?);
            try std.testing.expect(!dml.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 15 }, dml.assignments_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 24 }, dml.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, dml.returning_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const joined_update_sql = "UPDATE usage_records SET status = source.status FROM archived_records AS source WHERE usage_records.source_id = source.archive_id";
    const joined_update_result = try parseSqlAlloc(alloc, joined_update_sql);
    switch (joined_update_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.update, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, dml.source_tokens.?);
            const source_read = dml.source_read orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, source_read.tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, source_read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, source_read.kind);
            try std.testing.expect(source_read.wrapper_projection_star);
        },
        else => return error.TestUnexpectedResult,
    }

    const joined_delete_sql = "DELETE FROM usage_records USING archived_records AS source WHERE usage_records.source_id = source.archive_id";
    const joined_delete_result = try parseSqlAlloc(alloc, joined_delete_sql);
    switch (joined_delete_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.delete, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, dml.source_tokens.?);
            const source_read = dml.source_read orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, source_read.tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, source_read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, source_read.kind);
            try std.testing.expect(source_read.wrapper_projection_star);
        },
        else => return error.TestUnexpectedResult,
    }

    const merge_sql = "MERGE INTO usage_records USING archived_records AS source ON usage_records.source_id = source.archive_id WHEN MATCHED THEN DELETE";
    const merge_result = try parseSqlAlloc(alloc, merge_sql);
    switch (merge_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.merge, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, dml.source_tokens.?);
            const source_read = dml.source_read orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, source_read.tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, source_read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, source_read.kind);
            try std.testing.expect(source_read.wrapper_projection_star);
        },
        else => return error.TestUnexpectedResult,
    }

    const recursive_insert_sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) INSERT INTO archive(id) SELECT id FROM source_rows";
    const recursive_insert_result = try parseSqlAlloc(alloc, recursive_insert_sql);
    switch (recursive_insert_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_select, dml.kind);
            try std.testing.expectEqualStrings("INSERT", spanText(recursive_insert_sql, dml.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, dml.cte_tokens.?);
            const cte_prefix = dml.cte_prefix orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 10 }, cte_prefix.list_tokens);
            try std.testing.expectEqual(@as(usize, 1), cte_prefix.count);
            try std.testing.expect(cte_prefix.recursive);
            try std.testing.expectEqual(@as(usize, 1), cte_prefix.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, cte_prefix.first_name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, cte_prefix.first_body_tokens);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, cte_prefix.first_body_read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, cte_prefix.first_body_read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, cte_prefix.first_body_read.source_tokens.?);
            try std.testing.expectEqual(cte_prefix.first_name_tokens, cte_prefix.items[0].name_tokens);
            try std.testing.expectEqual(cte_prefix.first_body_tokens, cte_prefix.items[0].body_tokens);
            try std.testing.expectEqual(cte_prefix.first_body_read.tokens, cte_prefix.items[0].body_read.tokens);
            try std.testing.expect(dml.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, dml.insert_columns_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 20 }, dml.source_tokens.?);
            const source_read = dml.source_read orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 20 }, source_read.tokens);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, source_read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, source_read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, source_read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const recursive_multi_cte_update_sql = "WITH RECURSIVE seed_rows AS (SELECT id FROM usage_records), source_rows AS (SELECT id FROM seed_rows), final_rows AS (SELECT id FROM source_rows) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM final_rows)";
    const recursive_multi_cte_update_result = try parseSqlAlloc(alloc, recursive_multi_cte_update_sql);
    switch (recursive_multi_cte_update_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.update, dml.kind);
            try std.testing.expectEqualStrings("UPDATE", spanText(recursive_multi_cte_update_sql, dml.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 28 }, dml.cte_tokens.?);
            const cte_prefix = dml.cte_prefix orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 28 }, cte_prefix.list_tokens);
            try std.testing.expectEqual(@as(usize, 3), cte_prefix.count);
            try std.testing.expectEqual(@as(usize, 3), cte_prefix.items.len);
            try std.testing.expect(cte_prefix.recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, cte_prefix.items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, cte_prefix.items[0].body_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, cte_prefix.items[1].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 18 }, cte_prefix.items[1].body_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, cte_prefix.items[2].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 27 }, cte_prefix.items[2].body_tokens);
            try std.testing.expectEqual(cte_prefix.first_name_tokens, cte_prefix.items[0].name_tokens);
            try std.testing.expectEqual(cte_prefix.first_body_tokens, cte_prefix.items[0].body_tokens);
            try std.testing.expectEqual(cte_prefix.last_name_tokens, cte_prefix.items[2].name_tokens);
            try std.testing.expectEqual(cte_prefix.last_body_tokens, cte_prefix.items[2].body_tokens);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, cte_prefix.items[1].body_read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, cte_prefix.items[1].body_read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, cte_prefix.items[1].body_read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 29, .end = 30 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 31, .end = 34 }, dml.assignments_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 35, .end = 43 }, dml.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const default_insert_sql = "INSERT INTO usage_records DEFAULT VALUES";
    const default_insert_result = try parseSqlAlloc(alloc, default_insert_sql);
    switch (default_insert_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.default_values);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, dml.target_table_tokens.?);
            try std.testing.expect(dml.insert_columns_tokens == null);
            try std.testing.expect(dml.values_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const conflict_insert_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT (id) DO NOTHING RETURNING id";
    const conflict_insert_result = try parseSqlAlloc(alloc, conflict_insert_sql);
    switch (conflict_insert_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 8 }, dml.insert_columns_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 14 }, dml.values_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 21 }, dml.conflict_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, dml.returning_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const partial_conflict_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT (id) WHERE status = 'ready' DO NOTHING";
    const partial_conflict_result = try parseSqlAlloc(alloc, partial_conflict_sql);
    switch (partial_conflict_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.conflict_tokens != null);
            try std.testing.expect(dml.returning_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const named_conflict_sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'ready') ON CONFLICT ON CONSTRAINT usage_records_pkey DO NOTHING";
    const named_conflict_result = try parseSqlAlloc(alloc, named_conflict_sql);
    switch (named_conflict_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.insert_values, dml.kind);
            try std.testing.expect(dml.conflict_tokens != null);
            try std.testing.expect(dml.returning_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const truncate_sql = "TRUNCATE TABLE public.usage_records, usage_archive RESTART IDENTITY CASCADE";
    const truncate_result = try parseSqlAlloc(alloc, truncate_sql);
    switch (truncate_result.ast.?) {
        .dml => |dml| {
            try std.testing.expectEqual(GeneratedSqlDmlKind.truncate, dml.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, dml.target_table_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 5 }, dml.additional_target_tokens.?);
            try std.testing.expect(dml.restart_identity);
            try std.testing.expect(dml.cascade);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds read AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const read_sql = "SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10";
    const read_result = try parseSqlAlloc(alloc, read_sql);
    switch (read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqualStrings("SELECT id, status FROM usage_records WHERE status = 'open' ORDER BY id LIMIT 10", spanText(read_sql, read.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(read_sql, read.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[1].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.items[0]);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.limit_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.limit_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.limit_expression.tokens.?);
            try std.testing.expect(read.group_tokens == null);
            try std.testing.expect(read.having_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const alias_projection_read_sql = "SELECT status AS state, id FROM usage_records";
    const alias_projection_read_result = try parseSqlAlloc(alloc, alias_projection_read_sql);
    switch (alias_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expression_items.len);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.alias_items.len);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.alias_name_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 4 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.alias_name_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expression_items[1]);
            try std.testing.expect(read.projection_items.alias_items[1] == null);
            try std.testing.expect(read.projection_items.alias_name_items[1] == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const bare_alias_projection_read_sql = "SELECT status state, id FROM usage_records";
    const bare_alias_projection_read_result = try parseSqlAlloc(alloc, bare_alias_projection_read_sql);
    switch (bare_alias_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 5 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_items.alias_name_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.projection_items.expression_items[1]);
            try std.testing.expect(read.projection_items.alias_items[1] == null);
            try std.testing.expect(read.projection_items.alias_name_items[1] == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const qualified_star_projection_read_sql = "SELECT d.* FROM docs AS d WHERE d._id = 'doc:a'";
    const qualified_star_projection_read_result = try parseSqlAlloc(alloc, qualified_star_projection_read_sql);
    switch (qualified_star_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const table_qualified_star_projection_read_sql = "SELECT docs.* FROM docs WHERE _id = 'doc:a'";
    const table_qualified_star_projection_read_result = try parseSqlAlloc(alloc, table_qualified_star_projection_read_sql);
    switch (table_qualified_star_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 3 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlAlloc(alloc, "SELECT docs. FROM docs"));

    const star_with_extra_projection_read_sql = "SELECT *, lower(status) AS status_key, metadata->>'source' AS source FROM usage_records ORDER BY id ASC LIMIT 5";
    const star_with_extra_projection_read_result = try parseSqlAlloc(alloc, star_with_extra_projection_read_sql);
    switch (star_with_extra_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 15 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 3), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 9 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 15 }, read.projection_items.items[2]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_text_access, read.projection_items.expressions[2].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 21 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, read.limit_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const select_all_modifier_read_sql = "SELECT ALL _id FROM docs WHERE _id = 'doc:a'";
    const select_all_modifier_read_result = try parseSqlAlloc(alloc, select_all_modifier_read_sql);
    switch (select_all_modifier_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expect(read.distinct_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_source_read_sql = "SELECT * FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm";
    var graph_source_tokens = try lexer.tokenizeAlloc(alloc, graph_source_read_sql);
    defer lexer.freeTokens(alloc, &graph_source_tokens);
    const graph_source_read_result = try parseTokensAlloc(alloc, graph_source_tokens.items);
    switch (graph_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_items.len);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.match, read.source_graph_function_kind.?);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.match, read.source_graph_function_items[0].kind);
            try std.testing.expectEqual(@as(usize, 5), read.source_graph_function_items[0].argument_count);
            try std.testing.expect(std.meta.eql(read.source_graph_function_tokens.?, read.source_graph_function_items[0].tokens));
            try std.testing.expectEqualStrings(
                "antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b')",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "antfly.graph_match",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_name_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_argument_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'docs'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_items[0].table_name_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'docs_edge_graph'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_items[0].index_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'doc:root'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_items[0].start_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'(a)-[:cites]->(b)'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_items[0].pattern_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'b'",
                tokenRangeText(graph_source_read_sql, graph_source_tokens.items, read.source_graph_function_items[0].return_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_neighbors_source_read_sql = "SELECT * FROM antfly.graph_neighbors(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', direction => 'out') AS neighbors";
    var graph_neighbors_source_tokens = try lexer.tokenizeAlloc(alloc, graph_neighbors_source_read_sql);
    defer lexer.freeTokens(alloc, &graph_neighbors_source_tokens);
    const graph_neighbors_source_read_result = try parseTokensAlloc(alloc, graph_neighbors_source_tokens.items);
    switch (graph_neighbors_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_count);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_neighbors, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.neighbors, read.source_graph_function_items[0].kind);
            try std.testing.expectEqualStrings(
                "'doc:root'",
                tokenRangeText(graph_neighbors_source_read_sql, graph_neighbors_source_tokens.items, read.source_graph_function_items[0].start_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_shortest_path_source_read_sql = "SELECT * FROM antfly.graph_shortest_path(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', target => 'doc:z', max_depth => 4) AS path";
    var graph_shortest_path_source_tokens = try lexer.tokenizeAlloc(alloc, graph_shortest_path_source_read_sql);
    defer lexer.freeTokens(alloc, &graph_shortest_path_source_tokens);
    const graph_shortest_path_source_read_result = try parseTokensAlloc(alloc, graph_shortest_path_source_tokens.items);
    switch (graph_shortest_path_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_count);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_shortest_path, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.shortest_path, read.source_graph_function_items[0].kind);
            try std.testing.expectEqualStrings(
                "'doc:a'",
                tokenRangeText(graph_shortest_path_source_read_sql, graph_shortest_path_source_tokens.items, read.source_graph_function_items[0].start_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'doc:z'",
                tokenRangeText(graph_shortest_path_source_read_sql, graph_shortest_path_source_tokens.items, read.source_graph_function_items[0].target_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_k_shortest_paths_source_read_sql = "SELECT * FROM antfly.graph_k_shortest_paths(table_name => 'docs', index => 'docs_edge_graph', result_ref => '$starts', target_result_ref => '$targets', k => 3, max_depth => 6) AS paths";
    var graph_k_shortest_paths_source_tokens = try lexer.tokenizeAlloc(alloc, graph_k_shortest_paths_source_read_sql);
    defer lexer.freeTokens(alloc, &graph_k_shortest_paths_source_tokens);
    const graph_k_shortest_paths_source_read_result = try parseTokensAlloc(alloc, graph_k_shortest_paths_source_tokens.items);
    switch (graph_k_shortest_paths_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_count);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_k_shortest_paths, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.k_shortest_paths, read.source_graph_function_items[0].kind);
            try std.testing.expectEqualStrings(
                "'$starts'",
                tokenRangeText(graph_k_shortest_paths_source_read_sql, graph_k_shortest_paths_source_tokens.items, read.source_graph_function_items[0].start_result_ref_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'$targets'",
                tokenRangeText(graph_k_shortest_paths_source_read_sql, graph_k_shortest_paths_source_tokens.items, read.source_graph_function_items[0].target_result_ref_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const full_text_source_read_sql = "SELECT * FROM antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10) AS hits";
    var full_text_source_tokens = try lexer.tokenizeAlloc(alloc, full_text_source_read_sql);
    defer lexer.freeTokens(alloc, &full_text_source_tokens);
    const full_text_source_read_result = try parseTokensAlloc(alloc, full_text_source_tokens.items);
    switch (full_text_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_items.len);
            try std.testing.expectEqual(@as(usize, 0), read.source_graph_function_count);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.full_text_search, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(@as(usize, 3), read.source_antfly_function_items[0].argument_count);
            try std.testing.expectEqual(@as(usize, 3), read.source_antfly_function_items[0].argument_items.len);
            try std.testing.expectEqualStrings(
                "index",
                tokenRangeText(full_text_source_read_sql, full_text_source_tokens.items, read.source_antfly_function_items[0].argument_items[0].name_tokens),
            );
            try std.testing.expectEqualStrings(
                "=>",
                tokenRangeText(full_text_source_read_sql, full_text_source_tokens.items, read.source_antfly_function_items[0].argument_items[0].operator_tokens),
            );
            try std.testing.expectEqualStrings(
                "'docs_body_fts'",
                tokenRangeText(full_text_source_read_sql, full_text_source_tokens.items, read.source_antfly_function_items[0].argument_items[0].value_tokens),
            );
            try std.testing.expectEqualStrings(
                "antfly.full_text_search(index => 'docs_body_fts', query => 'refund', limit => 10)",
                tokenRangeText(full_text_source_read_sql, full_text_source_tokens.items, read.source_antfly_function_items[0].tokens),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_metric_rerank_source_read_sql = "SELECT * FROM antfly.graph_metric_rerank(full_text_index => 'docs_body_fts', query => 'refund', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', weight => 1.5, base_weight => 0.25) AS ranked";
    var graph_metric_rerank_source_tokens = try lexer.tokenizeAlloc(alloc, graph_metric_rerank_source_read_sql);
    defer lexer.freeTokens(alloc, &graph_metric_rerank_source_tokens);
    const graph_metric_rerank_source_read_result = try parseTokensAlloc(alloc, graph_metric_rerank_source_tokens.items);
    switch (graph_metric_rerank_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.source_graph_function_count);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_metric_rerank, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.metric_rerank, read.source_graph_function_items[0].kind);
            try std.testing.expectEqual(@as(usize, 6), read.source_graph_function_items[0].argument_count);
            try std.testing.expectEqualStrings(
                "antfly.graph_metric_rerank",
                tokenRangeText(graph_metric_rerank_source_read_sql, graph_metric_rerank_source_tokens.items, read.source_graph_function_items[0].name_tokens),
            );
            try std.testing.expectEqualStrings(
                "'docs_edge_graph'",
                tokenRangeText(graph_metric_rerank_source_read_sql, graph_metric_rerank_source_tokens.items, read.source_graph_function_items[0].index_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'pagerank'",
                tokenRangeText(graph_metric_rerank_source_read_sql, graph_metric_rerank_source_tokens.items, read.source_graph_function_items[0].metric_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'refund'",
                tokenRangeText(graph_metric_rerank_source_read_sql, graph_metric_rerank_source_tokens.items, read.source_graph_function_items[0].query_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const joined_graph_source_read_sql = "SELECT gm.id, ranked.score FROM antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b') AS gm JOIN antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5) AS ranked ON gm.id = ranked.id";
    var joined_graph_source_tokens = try lexer.tokenizeAlloc(alloc, joined_graph_source_read_sql);
    defer lexer.freeTokens(alloc, &joined_graph_source_tokens);
    const joined_graph_source_read_result = try parseTokensAlloc(alloc, joined_graph_source_tokens.items);
    switch (joined_graph_source_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(@as(usize, 2), read.source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 2), read.source_antfly_function_items.len);
            try std.testing.expectEqual(@as(usize, 2), read.source_graph_function_count);
            try std.testing.expectEqual(@as(usize, 2), read.source_graph_function_items.len);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_match, read.source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_metric, read.source_antfly_function_items[1].kind);
            try std.testing.expectEqual(@as(usize, 5), read.source_antfly_function_items[0].argument_count);
            try std.testing.expectEqual(@as(usize, 4), read.source_antfly_function_items[1].argument_count);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.match, read.source_graph_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.metric, read.source_graph_function_items[1].kind);
            try std.testing.expectEqual(@as(usize, 5), read.source_graph_function_items[0].argument_count);
            try std.testing.expectEqual(@as(usize, 4), read.source_graph_function_items[1].argument_count);
            try std.testing.expect(std.meta.eql(read.source_graph_function_tokens.?, read.source_graph_function_items[0].tokens));
            try std.testing.expectEqualStrings(
                "antfly.graph_match(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:root', pattern => '(a)-[:cites]->(b)', return => 'b')",
                tokenRangeText(joined_graph_source_read_sql, joined_graph_source_tokens.items, read.source_graph_function_items[0].tokens),
            );
            try std.testing.expectEqualStrings(
                "antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5)",
                tokenRangeText(joined_graph_source_read_sql, joined_graph_source_tokens.items, read.source_graph_function_items[1].tokens),
            );
            try std.testing.expectEqualStrings(
                "'docs_edge_graph'",
                tokenRangeText(joined_graph_source_read_sql, joined_graph_source_tokens.items, read.source_graph_function_items[1].index_value_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "'pagerank'",
                tokenRangeText(joined_graph_source_read_sql, joined_graph_source_tokens.items, read.source_graph_function_items[1].metric_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const cast_projection_read_sql = "SELECT CAST(id AS text) AS id_text FROM usage_records WHERE id = 'u1'";
    const cast_projection_read_result = try parseSqlAlloc(alloc, cast_projection_read_sql);
    switch (cast_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.alias_name_items[0].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[0].cast_expression_tokens.?);
            try std.testing.expect(read.projection_items.expressions[0].cast_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].cast_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[0].cast_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_first_expression.cast_expression_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_first_expression.cast_type_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const typed_cast_projection_read_sql = "SELECT CAST(id AS text) AS id_text, CAST(active AS bool) AS active_bool, CAST(created_at_ns AS timestamptz) AS created_at FROM usage_records WHERE id = $1";
    const typed_cast_projection_read_result = try parseSqlAlloc(alloc, typed_cast_projection_read_sql);
    switch (typed_cast_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 27 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 3), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[0].cast_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.projection_items.expressions[1].cast_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.projection_items.expressions[2].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.projection_items.expressions[2].cast_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 28, .end = 29 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 30, .end = 33 }, read.where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cast_predicate_read_sql = "SELECT id FROM usage_records WHERE CAST(amount + 1 AS text) = '2'";
    const cast_predicate_read_result = try parseSqlAlloc(alloc, cast_predicate_read_sql);
    switch (cast_predicate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 15 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, read.where_expression.left_expression_kind.?);
            const cast_expression = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.cast, cast_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, cast_expression.cast_expression_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.additive, cast_expression.cast_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, cast_expression.cast_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const postfix_cast_read_sql = "SELECT id::text AS id_text FROM usage_records WHERE id::text = 'u1'";
    const postfix_cast_read_result = try parseSqlAlloc(alloc, postfix_cast_read_sql);
    switch (postfix_cast_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expect(read.where_expression.left_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const jsonb_postfix_cast_read_sql = "SELECT id FROM usage_records WHERE metadata->'flags' = $1::jsonb";
    const jsonb_postfix_cast_read_result = try parseSqlAlloc(alloc, jsonb_postfix_cast_read_sql);
    switch (jsonb_postfix_cast_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_access, read.where_expression.left_expression_kind.?);
            try std.testing.expect(read.where_expression.right_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.right_expression.?.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const array_suffix_postfix_cast_read_sql = "SELECT id FROM usage_records WHERE status = ANY($1::text[])";
    const array_suffix_postfix_cast_read_result = try parseSqlAlloc(alloc, array_suffix_postfix_cast_read_sql);
    switch (array_suffix_postfix_cast_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, read.where_expression.right_expression_kind.?);
            const grouped = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, grouped.inner_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, grouped.inner_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds predicate read AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const case_projection_read_sql = "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = 'u1'";
    const case_projection_read_result = try parseSqlAlloc(alloc, case_projection_read_sql);
    switch (case_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 22 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 22 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 20 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 22 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.projection_items.alias_name_items[0].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.case_expression, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions[0].case_branch_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 8 }, read.projection_items.expressions[0].case_first_when_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 14 }, read.projection_items.expressions[0].case_last_when_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].case_first_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.projection_items.expressions[0].case_first_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_items.expressions[0].case_first_result_tokens.?);
            try std.testing.expect(read.projection_items.expressions[0].case_first_result_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].case_first_result.?.kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions[0].case_condition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].case_condition_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.projection_items.expressions[0].case_condition_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.projection_items.expressions[0].case_condition_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.projection_items.expressions[0].case_condition_items.expressions[1].kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions[0].case_result_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_items.expressions[0].case_result_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.projection_items.expressions[0].case_result_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].case_result_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[0].case_result_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 19 }, read.projection_items.expressions[0].case_else_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 19 }, read.projection_items.expressions[0].case_else_expression_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].case_else_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].case_else_expression.?.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const null_case_projection_read_sql = "SELECT CASE WHEN email IS NULL THEN NULL ELSE email END AS maybe_email FROM usage_records WHERE id = 'u1'";
    const null_case_projection_read_result = try parseSqlAlloc(alloc, null_case_projection_read_sql);
    switch (null_case_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.case_expression, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[0].case_branch_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 8 }, read.projection_items.expressions[0].case_first_when_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].case_first_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_items.expressions[0].case_first_result_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read.projection_items.expressions[0].case_else_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.projection_items.expressions[0].case_else_expression_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[0].case_condition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].case_condition_items.items[0]);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[0].case_result_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_items.expressions[0].case_result_items.items[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    const like_read_sql = "SELECT id FROM usage_records WHERE status LIKE 'open%'";
    const like_read_result = try parseSqlAlloc(alloc, like_read_sql);
    switch (like_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ilike_read_sql = "SELECT id FROM usage_records WHERE status ILIKE 'open%'";
    const ilike_read_result = try parseSqlAlloc(alloc, ilike_read_sql);
    switch (ilike_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const like_escape_read_sql = "SELECT id FROM usage_records WHERE status LIKE 'op!_%' ESCAPE '!'";
    const like_escape_read_result = try parseSqlAlloc(alloc, like_escape_read_sql);
    switch (like_escape_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read.where_expression.escape_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.escape_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.escape_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ilike_escape_read_sql = "SELECT id FROM usage_records WHERE lower(status) ILIKE 'op!_%' ESCAPE '!'";
    const ilike_escape_read_result = try parseSqlAlloc(alloc, ilike_escape_read_sql);
    switch (ilike_escape_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 13 }, read.where_expression.escape_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.escape_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.where_expression.escape_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const like_any_read_sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])";
    const like_any_read_result = try parseSqlAlloc(alloc, like_any_read_sql);
    switch (like_any_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 19 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 19 }, read.where_expression.right_tokens.?);
            const grouped = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, grouped.kind);
            const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const like_any_subquery_read_sql = "SELECT id FROM usage_records WHERE lower(status) LIKE ANY (SELECT pattern FROM active_patterns)";
    const like_any_subquery_read_result = try parseSqlAlloc(alloc, like_any_subquery_read_sql);
    switch (like_any_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 17 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 17 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 16 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, subquery.subquery_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const in_list_read_sql = "SELECT id FROM usage_records WHERE id IN ('u1', 'u2')";
    const in_list_read_result = try parseSqlAlloc(alloc, in_list_read_sql);
    switch (in_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 12 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const in_subquery_read_sql = "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records WHERE archived IS TRUE)";
    const in_subquery_read_result = try parseSqlAlloc(alloc, in_subquery_read_sql);
    switch (in_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 17 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 17 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 16 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, subquery.subquery_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, subquery.subquery_where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const in_subquery_tail_read_sql = "SELECT id FROM usage_records WHERE id IN (SELECT id FROM archived_records WHERE archived IS TRUE ORDER BY id DESC LIMIT 5 OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY)";
    var in_subquery_tail_tokens = try lexer.tokenizeAlloc(alloc, in_subquery_tail_read_sql);
    defer in_subquery_tail_tokens.deinit(alloc);
    const in_subquery_tail_read_result = try parseTokensAlloc(alloc, in_subquery_tail_tokens.items);
    switch (in_subquery_tail_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlExpressionKind.in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            const tail = subquery.subquery_tail orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings(
                "id DESC",
                tokenRangeText(in_subquery_tail_read_sql, in_subquery_tail_tokens.items, tail.order_tokens.?),
            );
            try std.testing.expectEqual(@as(usize, 1), tail.order_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, tail.order_first_expression.?.kind);
            try std.testing.expectEqualStrings(
                "5",
                tokenRangeText(in_subquery_tail_read_sql, in_subquery_tail_tokens.items, tail.limit_tokens.?),
            );
            try std.testing.expect(!tail.limit_all);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, tail.limit_expression.?.kind);
            try std.testing.expectEqualStrings(
                "1 ROWS",
                tokenRangeText(in_subquery_tail_read_sql, in_subquery_tail_tokens.items, tail.offset_tokens.?),
            );
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, tail.offset_expression.?.kind);
            try std.testing.expectEqualStrings(
                "NEXT 2 ROWS ONLY",
                tokenRangeText(in_subquery_tail_read_sql, in_subquery_tail_tokens.items, tail.fetch_tokens.?),
            );
            try std.testing.expectEqualStrings(
                "2",
                tokenRangeText(in_subquery_tail_read_sql, in_subquery_tail_tokens.items, tail.fetch_count_tokens.?),
            );
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, tail.fetch_count_expression.?.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const between_read_sql = "SELECT id FROM usage_records WHERE score BETWEEN 1 AND 10";
    const between_read_result = try parseSqlAlloc(alloc, between_read_sql);
    switch (between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.between_lower_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.between_lower_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.between_lower_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.between_upper_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.between_upper_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.between_upper_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const symmetric_between_read_sql = "SELECT id FROM usage_records WHERE priority BETWEEN SYMMETRIC 20 AND 10";
    const symmetric_between_read_result = try parseSqlAlloc(alloc, symmetric_between_read_sql);
    switch (symmetric_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.between_modifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlBetweenModifier.symmetric, read.where_expression.between_modifier.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.between_lower_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.between_upper_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const function_symmetric_between_read_sql = "SELECT id FROM usage_records WHERE array_length(tags, 1) BETWEEN SYMMETRIC 3 AND 1";
    const function_symmetric_between_read_result = try parseSqlAlloc(alloc, function_symmetric_between_read_sql);
    switch (function_symmetric_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 16 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.where_expression.between_modifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlBetweenModifier.symmetric, read.where_expression.between_modifier.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.where_expression.between_lower_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.where_expression.between_upper_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_like_read_sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'closed%'";
    const not_like_read_result = try parseSqlAlloc(alloc, not_like_read_sql);
    switch (not_like_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_ilike_read_sql = "SELECT id FROM usage_records WHERE status NOT ILIKE 'closed%'";
    const not_ilike_read_result = try parseSqlAlloc(alloc, not_ilike_read_sql);
    switch (not_ilike_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_like_escape_read_sql = "SELECT id FROM usage_records WHERE status NOT LIKE 'cl!_%' ESCAPE '!'";
    const not_like_escape_read_result = try parseSqlAlloc(alloc, not_like_escape_read_sql);
    switch (not_like_escape_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_like, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read.where_expression.escape_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.escape_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.escape_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_ilike_escape_read_sql = "SELECT id FROM usage_records WHERE lower(status) NOT ILIKE 'cl!_%' ESCAPE '!'";
    const not_ilike_escape_read_result = try parseSqlAlloc(alloc, not_ilike_escape_read_sql);
    switch (not_ilike_escape_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 14 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 14 }, read.where_expression.escape_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.escape_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.where_expression.escape_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_ilike_all_read_sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL(ARRAY['bot%', 'sys%'])";
    const not_ilike_all_read_result = try parseSqlAlloc(alloc, not_ilike_all_read_sql);
    switch (not_ilike_all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 20 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 20 }, read.where_expression.right_tokens.?);
            const grouped = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, grouped.kind);
            const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_ilike_all_subquery_read_sql = "SELECT id FROM usage_records WHERE lower(name) NOT ILIKE ALL (SELECT pattern FROM blocked_patterns)";
    const not_ilike_all_subquery_read_result = try parseSqlAlloc(alloc, not_ilike_all_subquery_read_sql);
    switch (not_ilike_all_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 18 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_ilike, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 18 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 17 }, subquery.inner_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_in_list_read_sql = "SELECT id FROM usage_records WHERE id NOT IN ('u1', 'u2')";
    const not_in_list_read_result = try parseSqlAlloc(alloc, not_in_list_read_sql);
    switch (not_in_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_in_subquery_read_sql = "SELECT id FROM usage_records WHERE id NOT IN (SELECT id FROM archived_records WHERE archived IS TRUE)";
    const not_in_subquery_read_result = try parseSqlAlloc(alloc, not_in_subquery_read_sql);
    switch (not_in_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 18 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_in_list, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 18 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 17 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, subquery.subquery_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 17 }, subquery.subquery_where_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_between_read_sql = "SELECT id FROM usage_records WHERE score NOT BETWEEN 1 AND 10";
    const not_between_read_result = try parseSqlAlloc(alloc, not_between_read_sql);
    switch (not_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.between_lower_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.between_upper_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_asymmetric_between_read_sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN ASYMMETRIC 10 AND 20";
    const not_asymmetric_between_read_result = try parseSqlAlloc(alloc, not_asymmetric_between_read_sql);
    switch (not_asymmetric_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.between_modifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlBetweenModifier.asymmetric, read.where_expression.between_modifier.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_symmetric_between_read_sql = "SELECT id FROM usage_records WHERE priority NOT BETWEEN SYMMETRIC 20 AND 10";
    const not_symmetric_between_read_result = try parseSqlAlloc(alloc, not_symmetric_between_read_sql);
    switch (not_symmetric_between_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_between, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.between_modifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlBetweenModifier.symmetric, read.where_expression.between_modifier.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds quantified predicate AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const any_read_sql = "SELECT id FROM usage_records WHERE score = ANY (1, 2)";
    const any_read_result = try parseSqlAlloc(alloc, any_read_sql);
    switch (any_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const all_read_sql = "SELECT id FROM usage_records WHERE score <> ALL (1, 2)";
    const all_read_result = try parseSqlAlloc(alloc, all_read_sql);
    switch (all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const some_read_sql = "SELECT id FROM usage_records WHERE score > SOME (1, 2)";
    const some_read_result = try parseSqlAlloc(alloc, some_read_sql);
    switch (some_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 13 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const quantified_array_read_sql = "SELECT id FROM usage_records WHERE status = ANY(ARRAY['active','pending']::text[])";
    const quantified_array_read_result = try parseSqlAlloc(alloc, quantified_array_read_sql);
    switch (quantified_array_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 16 }, read.where_expression.right_tokens.?);
            const grouped = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, grouped.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 15 }, grouped.inner_tokens.?);
            const array_constructor = grouped.inner_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 14 }, array_constructor.array_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, array_constructor.array_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, array_constructor.array_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.expressions.len);
        },
        else => return error.TestUnexpectedResult,
    }

    const any_subquery_read_sql = "SELECT id FROM usage_records WHERE score = ANY (SELECT score FROM thresholds WHERE active IS TRUE)";
    const any_subquery_read_result = try parseSqlAlloc(alloc, any_subquery_read_sql);
    switch (any_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 18 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 18 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 17 }, subquery.inner_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const all_subquery_read_sql = "SELECT id FROM usage_records WHERE score <> ALL (SELECT score FROM archived_thresholds)";
    const all_subquery_read_result = try parseSqlAlloc(alloc, all_subquery_read_sql);
    switch (all_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 14 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.quantified_comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.quantifier_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 14 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), subquery.subquery_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, subquery.subquery_projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, subquery.subquery_projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, subquery.subquery_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const exists_subquery_read_sql = "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)";
    const exists_subquery_read_result = try parseSqlAlloc(alloc, exists_subquery_read_sql);
    switch (exists_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 16 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.exists_subquery, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expect(read.where_expression.negation_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 16 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 15 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), subquery.subquery_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, subquery.subquery_projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, subquery.subquery_projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, subquery.subquery_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 15 }, subquery.subquery_where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, subquery.subquery_where_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, subquery.subquery_where_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 15 }, subquery.subquery_where_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const not_exists_subquery_read_sql = "SELECT id FROM usage_records WHERE NOT EXISTS (SELECT 1 FROM thresholds WHERE active IS TRUE)";
    const not_exists_subquery_read_result = try parseSqlAlloc(alloc, not_exists_subquery_read_sql);
    switch (not_exists_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 17 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.not_exists_subquery, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 17 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, read.where_expression.right_expression_kind.?);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 16 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, subquery.subquery_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), subquery.subquery_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, subquery.subquery_projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, subquery.subquery_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, subquery.subquery_where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, subquery.subquery_where_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, subquery.subquery_where_expression.?.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const exists_set_operation_subquery_read_sql = "SELECT id FROM usage_records WHERE EXISTS (SELECT id FROM thresholds UNION SELECT id FROM archived_thresholds)";
    const exists_set_operation_subquery_read_result = try parseSqlAlloc(alloc, exists_set_operation_subquery_read_sql);
    switch (exists_set_operation_subquery_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 17 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.exists_subquery, read.where_expression.kind);
            const subquery = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.subquery, subquery.kind);
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, subquery.subquery_read_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 16 }, subquery.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, subquery.subquery_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), subquery.subquery_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, subquery.subquery_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 16 }, subquery.subquery_set_operation_tokens.?);
            const set_operation = subquery.subquery_set_operation orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 16 }, set_operation.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, set_operation.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlSetOperationKind.@"union", set_operation.kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 16 }, set_operation.right_query_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, set_operation.right_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, set_operation.right_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), set_operation.right_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, set_operation.right_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const array_contains_read_sql = "SELECT id FROM usage_records WHERE tags @> ARRAY['hot','new']";
    const array_contains_read_result = try parseSqlAlloc(alloc, array_contains_read_sql);
    switch (array_contains_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.contains, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read.where_expression.right_tokens.?);
            const array_constructor = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, array_constructor.array_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.expressions.len);
        },
        else => return error.TestUnexpectedResult,
    }

    const array_overlap_read_sql = "SELECT id FROM usage_records WHERE tags && ARRAY['hot','new']";
    const array_overlap_read_result = try parseSqlAlloc(alloc, array_overlap_read_sql);
    switch (array_overlap_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.overlaps, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read.where_expression.right_tokens.?);
            const array_constructor = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, array_constructor.array_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.expressions.len);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_key_read_sql = "SELECT id FROM usage_records WHERE metadata ? 'flags'";
    const json_key_read_result = try parseSqlAlloc(alloc, json_key_read_sql);
    switch (json_key_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_key_exists, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_key_any_read_sql = "SELECT id FROM usage_records WHERE metadata ?| ARRAY['flags','billing']";
    const json_key_any_read_result = try parseSqlAlloc(alloc, json_key_any_read_sql);
    switch (json_key_any_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_key_any, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read.where_expression.right_tokens.?);
            const array_constructor = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, array_constructor.array_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_key_all_read_sql = "SELECT id FROM usage_records WHERE metadata ?& ARRAY['flags','billing']";
    const json_key_all_read_result = try parseSqlAlloc(alloc, json_key_all_read_sql);
    switch (json_key_all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_key_all, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 13 }, read.where_expression.right_tokens.?);
            const array_constructor = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.array_constructor, array_constructor.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, array_constructor.array_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), array_constructor.array_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const regex_cases = [_]struct {
        sql: []const u8,
        kind: GeneratedSqlExpressionKind,
    }{
        .{ .sql = "SELECT id FROM usage_records WHERE status ~ 'op.*'", .kind = .regex_match },
        .{ .sql = "SELECT id FROM usage_records WHERE status ~* 'op.*'", .kind = .regex_imatch },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~ 'closed.*'", .kind = .regex_not_match },
        .{ .sql = "SELECT id FROM usage_records WHERE status !~* 'closed.*'", .kind = .regex_not_imatch },
    };
    for (regex_cases) |case| {
        const regex_read_result = try parseSqlAlloc(alloc, case.sql);
        switch (regex_read_result.ast.?) {
            .read => |read| {
                try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
                try std.testing.expectEqual(case.kind, read.where_expression.kind);
                try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
                try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
                try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    const concat_projection_read_sql = "SELECT first_name || ' ' || last_name FROM usage_records";
    const concat_projection_read_result = try parseSqlAlloc(alloc, concat_projection_read_sql);
    switch (concat_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.string_concat, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_items.expressions[0].operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.string_concat, read.projection_items.expressions[0].right_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.string_concat, read.projection_first_expression.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const concat_predicate_read_sql = "SELECT id FROM usage_records WHERE status || ':' || id = 'open:u1'";
    const concat_predicate_read_result = try parseSqlAlloc(alloc, concat_predicate_read_sql);
    switch (concat_predicate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.string_concat, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.string_concat, read.where_expression.left_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds null logical and join AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const is_null_read_sql = "SELECT id FROM usage_records WHERE deleted_at IS NULL";
    const is_null_read_result = try parseSqlAlloc(alloc, is_null_read_sql);
    switch (is_null_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_null_read_sql = "SELECT id FROM usage_records WHERE deleted_at IS NOT NULL";
    const is_not_null_read_result = try parseSqlAlloc(alloc, is_not_null_read_sql);
    switch (is_not_null_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const postfix_isnull_read_sql = "SELECT id FROM usage_records WHERE status ISNULL";
    const postfix_isnull_read_result = try parseSqlAlloc(alloc, postfix_isnull_read_sql);
    switch (postfix_isnull_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expect(read.where_expression.right_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const postfix_notnull_read_sql = "SELECT id FROM usage_records WHERE lower(status) NOTNULL";
    const postfix_notnull_read_result = try parseSqlAlloc(alloc, postfix_notnull_read_sql);
    switch (postfix_notnull_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_null, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expect(read.where_expression.right_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_true_read_sql = "SELECT id FROM usage_records WHERE active IS TRUE";
    const is_true_read_result = try parseSqlAlloc(alloc, is_true_read_sql);
    switch (is_true_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_true, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_false_read_sql = "SELECT id FROM usage_records WHERE active IS NOT FALSE";
    const is_not_false_read_result = try parseSqlAlloc(alloc, is_not_false_read_sql);
    switch (is_not_false_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_false, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_unknown_read_sql = "SELECT id FROM usage_records WHERE active IS UNKNOWN";
    const is_unknown_read_result = try parseSqlAlloc(alloc, is_unknown_read_sql);
    switch (is_unknown_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_unknown, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_unknown_read_sql = "SELECT id FROM usage_records WHERE active IS NOT UNKNOWN";
    const is_not_unknown_read_result = try parseSqlAlloc(alloc, is_not_unknown_read_sql);
    switch (is_not_unknown_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_unknown, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_distinct_read_sql = "SELECT id FROM usage_records WHERE status IS DISTINCT FROM previous_status";
    const is_distinct_read_result = try parseSqlAlloc(alloc, is_distinct_read_sql);
    switch (is_distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_distinct_from, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const is_not_distinct_read_sql = "SELECT id FROM usage_records WHERE status IS NOT DISTINCT FROM previous_status";
    const is_not_distinct_read_result = try parseSqlAlloc(alloc, is_not_distinct_read_sql);
    switch (is_not_distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_distinct_from, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.where_expression.negation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_or_read_sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL";
    const logical_or_read_result = try parseSqlAlloc(alloc, logical_or_read_sql);
    switch (logical_or_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_or, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_expression.?.tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.where_expression.boolean_condition_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_first_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_first_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_first_condition.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_first_condition.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_last_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_last_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_last_condition.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_last_condition.?.tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.where_expression.boolean_condition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_condition_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_condition_items.last_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_condition_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_condition_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_condition_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_condition_items.expressions[1].kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_or_chain_read_sql = "SELECT id FROM usage_records WHERE status = 'open' OR deleted_at IS NULL OR amount > 10";
    const logical_or_chain_read_result = try parseSqlAlloc(alloc, logical_or_chain_read_sql);
    switch (logical_or_chain_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 16 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_or, read.where_expression.kind);
            try std.testing.expectEqual(@as(usize, 3), read.where_expression.boolean_condition_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_first_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_first_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_first_condition.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.where_expression.boolean_last_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_last_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_last_condition.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.where_expression.boolean_last_condition.?.tokens.?);
            try std.testing.expectEqual(@as(usize, 3), read.where_expression.boolean_condition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_condition_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.where_expression.boolean_condition_items.last_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_condition_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_condition_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.where_expression.boolean_condition_items.items[2]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_condition_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_condition_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_condition_items.expressions[2].kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_and_read_sql = "SELECT id FROM usage_records WHERE status = 'open' AND deleted_at IS NULL";
    const logical_and_read_result = try parseSqlAlloc(alloc, logical_and_read_sql);
    switch (logical_and_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 12 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_and, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
            try std.testing.expectEqual(@as(usize, 2), read.where_expression.boolean_condition_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.boolean_first_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_first_condition_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.boolean_last_condition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_last_condition_kind.?);
            try std.testing.expectEqual(@as(usize, 2), read.where_expression.boolean_condition_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.boolean_condition_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.boolean_condition_items.expressions[1].kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_not_read_sql = "SELECT id FROM usage_records WHERE NOT deleted_at IS NULL";
    const logical_not_read_result = try parseSqlAlloc(alloc, logical_not_read_sql);
    switch (logical_not_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_not, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression_kind.?);
            try std.testing.expect(read.where_expression.left_expression == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_null, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.right_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const grouped_read_sql = "SELECT id FROM usage_records WHERE (status = 'open')";
    const grouped_read_result = try parseSqlAlloc(alloc, grouped_read_sql);
    switch (grouped_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.inner_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.inner_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.inner_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.where_expression.inner_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const logical_not_grouped_read_sql = "SELECT id FROM usage_records WHERE NOT (deleted_at IS NULL)";
    const logical_not_grouped_read_result = try parseSqlAlloc(alloc, logical_not_grouped_read_sql);
    switch (logical_not_grouped_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_not, read.where_expression.kind);
            try std.testing.expect(read.where_expression.left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 11 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.grouped, read.where_expression.right_expression_kind.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const additive_comparison_read_sql = "SELECT id FROM usage_records WHERE score + bonus > 10";
    const additive_comparison_read_result = try parseSqlAlloc(alloc, additive_comparison_read_sql);
    switch (additive_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.additive, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.additive, read.where_expression.left_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.where_expression.right_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const multiplicative_comparison_read_sql = "SELECT id FROM usage_records WHERE score * weight > 10";
    const multiplicative_comparison_read_result = try parseSqlAlloc(alloc, multiplicative_comparison_read_sql);
    switch (multiplicative_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.multiplicative, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_text_comparison_read_sql = "SELECT id FROM usage_records WHERE payload ->> 'status' = 'open'";
    const json_text_comparison_read_result = try parseSqlAlloc(alloc, json_text_comparison_read_sql);
    switch (json_text_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_text_access, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const function_comparison_read_sql = "SELECT id FROM usage_records WHERE lower(status) = 'open'";
    const function_comparison_read_result = try parseSqlAlloc(alloc, function_comparison_read_sql);
    switch (function_comparison_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.where_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    var function_tokens = try lexer.tokenizeAlloc(alloc, "lower(status, fallback)");
    defer lexer.freeTokens(alloc, &function_tokens);
    var function_expression = try buildGeneratedExpressionAst(alloc, function_tokens.items, .{ .start = 0, .end = function_tokens.items.len });
    defer function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 0, .end = 1 }, function_expression.function_name_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 5 }, function_expression.argument_tokens.?);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.first_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.last_tokens.?);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.items.len);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.items[1]);
    try std.testing.expectEqual(@as(usize, 2), function_expression.argument_items.expressions.len);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, function_expression.argument_items.expressions[0].kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, function_expression.argument_items.expressions[0].tokens.?);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, function_expression.argument_items.expressions[1].kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, function_expression.argument_items.expressions[1].tokens.?);

    var distinct_function_tokens = try lexer.tokenizeAlloc(alloc, "count(DISTINCT status)");
    defer lexer.freeTokens(alloc, &distinct_function_tokens);
    var distinct_function_expression = try buildGeneratedExpressionAst(alloc, distinct_function_tokens.items, .{ .start = 0, .end = distinct_function_tokens.items.len });
    defer distinct_function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, distinct_function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 4 }, distinct_function_expression.argument_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, distinct_function_expression.argument_distinct_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, distinct_function_expression.argument_value_tokens.?);
    try std.testing.expectEqual(@as(usize, 1), distinct_function_expression.argument_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, distinct_function_expression.argument_items.items[0]);

    var ordered_argument_function_tokens = try lexer.tokenizeAlloc(alloc, "array_agg(DISTINCT status ORDER BY amount DESC)");
    defer lexer.freeTokens(alloc, &ordered_argument_function_tokens);
    var ordered_argument_function_expression = try buildGeneratedExpressionAst(alloc, ordered_argument_function_tokens.items, .{ .start = 0, .end = ordered_argument_function_tokens.items.len });
    defer ordered_argument_function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, ordered_argument_function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 8 }, ordered_argument_function_expression.argument_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, ordered_argument_function_expression.argument_distinct_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, ordered_argument_function_expression.argument_value_tokens.?);
    try std.testing.expectEqual(@as(usize, 1), ordered_argument_function_expression.argument_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 8 }, ordered_argument_function_expression.argument_order_tokens.?);
    try std.testing.expectEqual(@as(usize, 1), ordered_argument_function_expression.argument_order_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 8 }, ordered_argument_function_expression.argument_order_items.items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, ordered_argument_function_expression.argument_order_items.expression_items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, ordered_argument_function_expression.argument_order_items.direction_items[0].?);
    try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, ordered_argument_function_expression.argument_order_items.directions[0].?);

    var filter_function_tokens = try lexer.tokenizeAlloc(alloc, "count(*) FILTER (WHERE status = 'open')");
    defer lexer.freeTokens(alloc, &filter_function_tokens);
    var filter_function_expression = try buildGeneratedExpressionAst(alloc, filter_function_tokens.items, .{ .start = 0, .end = filter_function_tokens.items.len });
    defer filter_function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, filter_function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 0, .end = 1 }, filter_function_expression.function_name_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, filter_function_expression.argument_tokens.?);
    try std.testing.expectEqual(@as(usize, 1), filter_function_expression.argument_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, filter_function_expression.argument_items.items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 11 }, filter_function_expression.filter_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, filter_function_expression.filter_predicate_tokens.?);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, filter_function_expression.filter_expression_kind.?);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, filter_function_expression.filter_expression.?.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, filter_function_expression.filter_expression.?.left_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, filter_function_expression.filter_expression.?.operator_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, filter_function_expression.filter_expression.?.right_tokens.?);

    var within_group_function_tokens = try lexer.tokenizeAlloc(alloc, "percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST)");
    defer lexer.freeTokens(alloc, &within_group_function_tokens);
    var within_group_function_expression = try buildGeneratedExpressionAst(alloc, within_group_function_tokens.items, .{ .start = 0, .end = within_group_function_tokens.items.len });
    defer within_group_function_expression.deinit(alloc);
    try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, within_group_function_expression.kind);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 0, .end = 1 }, within_group_function_expression.function_name_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, within_group_function_expression.argument_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 14 }, within_group_function_expression.within_group_tokens.?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, within_group_function_expression.within_group_order_tokens.?);
    try std.testing.expectEqual(@as(usize, 1), within_group_function_expression.within_group_order_items.count);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, within_group_function_expression.within_group_order_items.items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, within_group_function_expression.within_group_order_items.expression_items[0]);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, within_group_function_expression.within_group_order_items.direction_items[0].?);
    try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, within_group_function_expression.within_group_order_items.directions[0].?);
    try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 13 }, within_group_function_expression.within_group_order_items.nulls_order_items[0].?);
    try std.testing.expectEqual(GeneratedSqlNullsOrder.last, within_group_function_expression.within_group_order_items.nulls_orders[0].?);

    const nested_list_read_sql = "SELECT id, row_number() OVER (PARTITION BY tenant, account ORDER BY id) AS rn FROM usage_records ORDER BY id, tenant";
    const nested_list_read_result = try parseSqlAlloc(alloc, nested_list_read_sql);
    switch (nested_list_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 19 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read.projection_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expression_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read.projection_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 19 }, read.projection_items.alias_items[1].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.projection_items.alias_name_items[1].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 26 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read.order_items.items[1]);
        },
        else => return error.TestUnexpectedResult,
    }

    const function_call_read_sql = "SELECT concat_ws(',', status), id FROM usage_records ORDER BY status, id";
    const function_call_read_result = try parseSqlAlloc(alloc, function_call_read_sql);
    switch (function_call_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_first_expression.argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_first_expression.argument_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_first_expression.argument_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.projection_first_expression.argument_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.argument_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.argument_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.count);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_items.items[1]);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.order_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.order_last_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const aggregate_filter_read_sql = "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count FROM usage_records GROUP BY customer";
    const aggregate_filter_read_result = try parseSqlAlloc(alloc, aggregate_filter_read_sql);
    switch (aggregate_filter_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 16 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read.projection_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 16 }, read.projection_items.alias_items[1].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.projection_items.alias_name_items[1].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[1].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[1].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 14 }, read.projection_items.expressions[1].filter_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 13 }, read.projection_items.expressions[1].filter_predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.projection_items.expressions[1].filter_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.projection_items.expressions[1].filter_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.group_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.group_items.items[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    const global_count_read_sql = "SELECT COUNT(*) AS total FROM usage_records";
    const global_count_read_result = try parseSqlAlloc(alloc, global_count_read_sql);
    switch (global_count_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 7 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 5 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[0].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const distinct_aggregate_read_sql = "SELECT customer, COUNT(DISTINCT status) AS status_count FROM usage_records GROUP BY customer";
    const distinct_aggregate_read_result = try parseSqlAlloc(alloc, distinct_aggregate_read_sql);
    switch (distinct_aggregate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 8 }, read.projection_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read.projection_items.expressions[1].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[1].argument_distinct_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.projection_items.expressions[1].argument_value_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[1].argument_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.group_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ordered_argument_aggregate_read_sql = "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) AS statuses FROM usage_records GROUP BY customer";
    const ordered_argument_aggregate_read_result = try parseSqlAlloc(alloc, ordered_argument_aggregate_read_sql);
    switch (ordered_argument_aggregate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 14 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 14 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.projection_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.projection_items.expressions[1].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[1].argument_distinct_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.projection_items.expressions[1].argument_value_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 11 }, read.projection_items.expressions[1].argument_order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[1].argument_order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.projection_items.expressions[1].argument_order_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, read.projection_items.expressions[1].argument_order_items.directions[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.group_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const ordered_set_aggregate_read_sql = "SELECT customer, percentile_cont(0.5) WITHIN GROUP (ORDER BY amount DESC NULLS LAST) AS median_amount FROM usage_records GROUP BY customer";
    const ordered_set_aggregate_read_result = try parseSqlAlloc(alloc, ordered_set_aggregate_read_sql);
    switch (ordered_set_aggregate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 19 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 19 }, read.projection_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 17 }, read.projection_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 19 }, read.projection_items.alias_items[1].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[1].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_items.expressions[1].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 17 }, read.projection_items.expressions[1].within_group_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read.projection_items.expressions[1].within_group_order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.expressions[1].within_group_order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read.projection_items.expressions[1].within_group_order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.projection_items.expressions[1].within_group_order_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.projection_items.expressions[1].within_group_order_items.direction_items[0].?);
            try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, read.projection_items.expressions[1].within_group_order_items.directions[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 16 }, read.projection_items.expressions[1].within_group_order_items.nulls_order_items[0].?);
            try std.testing.expectEqual(GeneratedSqlNullsOrder.last, read.projection_items.expressions[1].within_group_order_items.nulls_orders[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.group_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, read.group_items.items[0]);
        },
        else => return error.TestUnexpectedResult,
    }

    const aggregate_read_sql = "SELECT status FROM usage_records GROUP BY status HAVING count > 1";
    const aggregate_read_result = try parseSqlAlloc(alloc, aggregate_read_sql);
    switch (aggregate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.last_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.count);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.items[0]);
            try std.testing.expectEqual(@as(usize, 1), read.group_items.expressions.len);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.group_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.group_last_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, read.having_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.having_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.having_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.having_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.having_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const joined_read_sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id";
    const joined_read_result = try parseSqlAlloc(alloc, joined_read_sql);
    switch (joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
            try std.testing.expectEqual(@as(?usize, 0), read.join_tree_root_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_tree_depth);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[0].kind);
            try std.testing.expectEqual(@as(usize, 0), read.join_items[0].tree_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_items[0].tree_depth);
            try std.testing.expect(read.join_items[0].left_child_index == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinConditionKind.on, read.join_items[0].condition_kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.join_items[0].condition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_items[0].predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_predicate_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.join_predicate_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.join_predicate_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const using_joined_read_sql = "SELECT usage_records.id FROM usage_records JOIN accounts USING (account_id)";
    const using_joined_read_result = try parseSqlAlloc(alloc, using_joined_read_sql);
    switch (using_joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_right_tokens.?);
            try std.testing.expect(read.join_predicate_tokens == null);
            try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
            try std.testing.expectEqual(@as(?usize, 0), read.join_tree_root_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_tree_depth);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlJoinConditionKind.using, read.join_items[0].condition_kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.join_items[0].condition_tokens);
            try std.testing.expect(read.join_items[0].predicate_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.join_items[0].using_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.join_items[0].using_column_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.join_items[0].using_columns.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.join_items[0].using_columns.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.join_items[0].using_columns.expressions[0].kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const multi_joined_read_sql = "SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id";
    const multi_joined_read_result = try parseSqlAlloc(alloc, multi_joined_read_sql);
    switch (multi_joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(@as(usize, 2), read.join_items.len);
            try std.testing.expectEqual(@as(?usize, 1), read.join_tree_root_index);
            try std.testing.expectEqual(@as(usize, 2), read.join_tree_depth);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[0].kind);
            try std.testing.expectEqual(@as(usize, 0), read.join_items[0].tree_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_items[0].tree_depth);
            try std.testing.expect(read.join_items[0].left_child_index == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinConditionKind.on, read.join_items[0].condition_kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.join_items[0].condition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 10 }, read.join_items[0].predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 16 }, read.join_items[1].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.join_items[1].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.join_items[1].kind);
            try std.testing.expectEqual(@as(usize, 1), read.join_items[1].tree_index);
            try std.testing.expectEqual(@as(usize, 2), read.join_items[1].tree_depth);
            try std.testing.expectEqual(@as(?usize, 0), read.join_items[1].left_child_index);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 10 }, read.join_items[1].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.join_items[1].right_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinConditionKind.on, read.join_items[1].condition_kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 16 }, read.join_items[1].condition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 16 }, read.join_items[1].predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[1].predicate_expression.kind);
        },
        else => return error.TestUnexpectedResult,
    }

    const left_outer_joined_read_sql = "SELECT usage_records.id FROM usage_records LEFT OUTER JOIN accounts ON usage_records.account_id = accounts.id";
    const left_outer_joined_read_result = try parseSqlAlloc(alloc, left_outer_joined_read_sql);
    switch (left_outer_joined_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read.join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.left, read.join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.join_predicate_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.join_items.len);
            try std.testing.expectEqual(@as(?usize, 0), read.join_tree_root_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_tree_depth);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 12 }, read.join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 7 }, read.join_items[0].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinKind.left, read.join_items[0].kind);
            try std.testing.expectEqual(@as(usize, 0), read.join_items[0].tree_index);
            try std.testing.expectEqual(@as(usize, 1), read.join_items[0].tree_depth);
            try std.testing.expect(read.join_items[0].left_child_index == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.join_items[0].left_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.join_items[0].right_tokens);
            try std.testing.expectEqual(GeneratedSqlJoinConditionKind.on, read.join_items[0].condition_kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read.join_items[0].condition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.join_items[0].predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_items[0].predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.join_predicate_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.join_predicate_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.join_predicate_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.join_predicate_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const distinct_read_sql = "SELECT DISTINCT status FROM usage_records ORDER BY status";
    const distinct_read_result = try parseSqlAlloc(alloc, distinct_read_sql);
    switch (distinct_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.distinct_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.order_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const distinct_on_read_sql = "SELECT DISTINCT ON (organization_id) organization_id, id FROM usage_records ORDER BY organization_id ASC, created_at DESC";
    const distinct_on_read_result = try parseSqlAlloc(alloc, distinct_on_read_sql);
    switch (distinct_on_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.distinct_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.distinct_on_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.distinct_on_items.first_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.distinct_on_items.last_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.distinct_on_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.distinct_on_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.distinct_on_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds extended read AST spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const order_nulls_read_sql = "SELECT id FROM usage_records ORDER BY created_at DESC NULLS LAST, score ASC NULLS FIRST";
    const order_nulls_read_result = try parseSqlAlloc(alloc, order_nulls_read_sql);
    switch (order_nulls_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 15 }, read.order_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.order_items.direction_items[0].?);
            try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, read.order_items.directions[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 10 }, read.order_items.nulls_order_items[0].?);
            try std.testing.expectEqual(GeneratedSqlNullsOrder.last, read.order_items.nulls_orders[0].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 15 }, read.order_items.items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.order_items.expression_items[1]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.order_items.direction_items[1].?);
            try std.testing.expectEqual(GeneratedSqlOrderDirection.asc, read.order_items.directions[1].?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 15 }, read.order_items.nulls_order_items[1].?);
            try std.testing.expectEqual(GeneratedSqlNullsOrder.first, read.order_items.nulls_orders[1].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_last_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.order_last_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const order_using_read_sql = "SELECT id FROM usage_records ORDER BY 1 USING > LIMIT 5";
    const order_using_read_result = try parseSqlAlloc(alloc, order_using_read_sql);
    switch (order_using_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 9 }, read.order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 9 }, read.order_items.direction_items[0].?);
            try std.testing.expect(read.order_items.directions[0] == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.order_items.order_using_operator_items[0].?);
            try std.testing.expect(read.order_items.nulls_order_items[0] == null);
            try std.testing.expect(read.order_items.nulls_orders[0] == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_first_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.limit_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const paginated_read_sql = "SELECT id FROM usage_records OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY";
    const paginated_read_result = try parseSqlAlloc(alloc, paginated_read_sql);
    switch (paginated_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, read.offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.offset_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read.fetch_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.fetch_count_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.fetch_count_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.fetch_count_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const limit_all_read_sql = "SELECT id FROM usage_records ORDER BY id LIMIT ALL OFFSET 2 ROWS";
    const limit_all_read_result = try parseSqlAlloc(alloc, limit_all_read_sql);
    switch (limit_all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.limit_tokens.?);
            try std.testing.expect(read.limit_all);
            try std.testing.expect(read.limit_expression.tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 12 }, read.offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.offset_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const fetch_default_read_sql = "SELECT id FROM usage_records FETCH FIRST ROWS ONLY";
    const fetch_default_read_result = try parseSqlAlloc(alloc, fetch_default_read_sql);
    switch (fetch_default_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.fetch_tokens.?);
            try std.testing.expect(read.fetch_count_tokens == null);
            try std.testing.expect(read.fetch_count_expression.tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const window_read_sql = "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM usage_records";
    const window_read_result = try parseSqlAlloc(alloc, window_read_sql);
    switch (window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 14 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.source_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            const window_expression = read.projection_items.expressions[1];
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, window_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 12 }, window_expression.over_tokens.?);
            try std.testing.expect(window_expression.over_name_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 11 }, window_expression.over_definition_tokens.?);
            try std.testing.expect(window_expression.over_partition_tokens == null);
            try std.testing.expectEqual(@as(usize, 0), window_expression.over_partition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), window_expression.over_order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_order_items.items[0]);
            try std.testing.expect(window_expression.over_frame_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)";
    const named_window_read_result = try parseSqlAlloc(alloc, named_window_read_sql);
    switch (named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 20 }, read.window_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_count);
            try std.testing.expectEqual(@as(usize, 1), read.window_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 20 }, read.window_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.window_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 19 }, read.window_items[0].definition_tokens);
            try std.testing.expect(read.window_items[0].partition_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.window_items[0].order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_items[0].order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.window_items[0].order_items.items[0]);
            try std.testing.expect(read.window_items[0].frame_tokens == null);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            const window_expression = read.projection_items.expressions[1];
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, window_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 8 }, window_expression.over_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, window_expression.over_name_tokens.?);
            try std.testing.expect(window_expression.over_definition_tokens == null);
            try std.testing.expect(window_expression.over_partition_tokens == null);
            try std.testing.expectEqual(@as(usize, 0), window_expression.over_partition_items.count);
            try std.testing.expect(window_expression.over_order_tokens == null);
            try std.testing.expectEqual(@as(usize, 0), window_expression.over_order_items.count);
            try std.testing.expect(window_expression.over_frame_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const partitioned_window_read_sql = "SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY id) AS rn FROM usage_records";
    const partitioned_window_read_result = try parseSqlAlloc(alloc, partitioned_window_read_sql);
    switch (partitioned_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 17 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.source_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            const window_expression = read.projection_items.expressions[1];
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, window_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 15 }, window_expression.over_tokens.?);
            try std.testing.expect(window_expression.over_name_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 14 }, window_expression.over_definition_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_partition_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), window_expression.over_partition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_partition_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, window_expression.over_order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), window_expression.over_order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, window_expression.over_order_items.items[0]);
            try std.testing.expect(window_expression.over_frame_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const partitioned_named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (PARTITION BY tenant ORDER BY id)";
    const partitioned_named_window_read_result = try parseSqlAlloc(alloc, partitioned_named_window_read_sql);
    switch (partitioned_named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 23 }, read.window_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 22 }, read.window_items[0].definition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.window_items[0].partition_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_items[0].partition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.window_items[0].partition_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.window_items[0].order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_items[0].order_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const framed_window_read_sql = "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records";
    const framed_window_read_result = try parseSqlAlloc(alloc, framed_window_read_sql);
    switch (framed_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 21 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, read.source_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            const window_expression = read.projection_items.expressions[1];
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, window_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 19 }, window_expression.over_tokens.?);
            try std.testing.expect(window_expression.over_name_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 18 }, window_expression.over_definition_tokens.?);
            try std.testing.expect(window_expression.over_partition_tokens == null);
            try std.testing.expectEqual(@as(usize, 0), window_expression.over_partition_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), window_expression.over_order_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, window_expression.over_order_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 18 }, window_expression.over_frame_tokens.?);
            try std.testing.expect(window_expression.over_frame_start_expression_tokens == null);
            try std.testing.expect(window_expression.over_frame_end_expression_tokens == null);
            try std.testing.expect(window_expression.over_frame_start_expression == null);
            try std.testing.expect(window_expression.over_frame_end_expression == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const offset_framed_window_read_sql = "SELECT id, count(*) OVER (ORDER BY amount ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS current_and_next FROM usage_records";
    const offset_framed_window_read_result = try parseSqlAlloc(alloc, offset_framed_window_read_sql);
    switch (offset_framed_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            const window_expression = read.projection_items.expressions[1];
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, window_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 19 }, window_expression.over_frame_tokens.?);
            try std.testing.expect(window_expression.over_frame_start_expression_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, window_expression.over_frame_end_expression_tokens.?);
            try std.testing.expect(window_expression.over_frame_end_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, window_expression.over_frame_end_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, window_expression.over_frame_end_expression.?.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const framed_named_window_read_sql = "SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)";
    const framed_named_window_read_result = try parseSqlAlloc(alloc, framed_named_window_read_sql);
    switch (framed_named_window_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 27 }, read.window_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.window_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 26 }, read.window_items[0].definition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.window_items[0].order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 26 }, read.window_items[0].frame_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.window_items[0].frame_start_expression_tokens.?);
            try std.testing.expect(read.window_items[0].frame_start_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.window_items[0].frame_start_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.window_items[0].frame_start_expression.?.tokens.?);
            try std.testing.expect(read.window_items[0].frame_end_expression_tokens == null);
            try std.testing.expect(read.window_items[0].frame_end_expression == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_named_window_body_read_sql = "WITH source_rows AS (SELECT id, row_number() OVER usage_window AS rn FROM usage_records WINDOW usage_window AS (ORDER BY id)) SELECT rn FROM source_rows";
    const cte_named_window_body_read_result = try parseSqlAlloc(alloc, cte_named_window_body_read_sql);
    switch (cte_named_window_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlReadKind.window, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 24 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 24 }, read.cte_items[0].body_window_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_window_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_window_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 24 }, read.cte_items[0].body_window_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, read.cte_items[0].body_window_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 23 }, read.cte_items[0].body_window_items[0].definition_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 22, .end = 23 }, read.cte_items[0].body_window_items[0].order_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_window_items[0].order_items.count);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_distinct_on_body_read_sql = "WITH source_rows AS (SELECT DISTINCT ON (organization_id) organization_id FROM usage_records ORDER BY organization_id) SELECT organization_id FROM source_rows";
    const cte_distinct_on_body_read_result = try parseSqlAlloc(alloc, cte_distinct_on_body_read_sql);
    switch (cte_distinct_on_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 16 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 10 }, read.cte_items[0].body_distinct_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_distinct_on_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.cte_items[0].body_distinct_on_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_distinct_on_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.cte_items[0].body_distinct_on_items.expressions[0].tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[0].body_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.cte_items[0].body_order_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_join_body_read_sql = "WITH joined_rows AS (SELECT usage_records.id FROM usage_records JOIN accounts ON usage_records.account_id = accounts.id JOIN tenants ON accounts.tenant_id = tenants.id) SELECT id FROM joined_rows";
    const cte_join_body_read_result = try parseSqlAlloc(alloc, cte_join_body_read_sql);
    switch (cte_join_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlReadKind.join, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 20 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 20 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 20 }, read.cte_items[0].body_join_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.cte_items[0].body_join_operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlJoinKind.inner, read.cte_items[0].body_join_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.cte_items[0].body_join_left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.cte_items[0].body_join_right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 14 }, read.cte_items[0].body_join_predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.cte_items[0].body_join_predicate_expression.kind);
            try std.testing.expectEqual(@as(usize, 2), read.cte_items[0].body_join_items.len);
            try std.testing.expectEqual(@as(?usize, 1), read.cte_items[0].body_join_tree_root_index);
            try std.testing.expectEqual(@as(usize, 2), read.cte_items[0].body_join_tree_depth);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 14 }, read.cte_items[0].body_join_items[0].tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 14 }, read.cte_items[0].body_join_items[0].predicate_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 20 }, read.cte_items[0].body_join_items[1].tokens);
            try std.testing.expectEqual(@as(?usize, 0), read.cte_items[0].body_join_items[1].left_child_index);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.cte_items[0].body_join_items[1].operator_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 20 }, read.cte_items[0].body_join_items[1].predicate_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_read_sql = "WITH source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows";
    const cte_read_result = try parseSqlAlloc(alloc, cte_read_sql);
    switch (cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.cte_items[0].body_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.cte_items[0].body_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.cte_items[0].body_projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expect(!read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const materialized_cte_read_sql = "WITH source_rows AS MATERIALIZED (SELECT id FROM usage_records) SELECT id FROM source_rows";
    const materialized_cte_read_result = try parseSqlAlloc(alloc, materialized_cte_read_sql);
    switch (materialized_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.cte_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.cte_items[0].materialization_tokens.?);
            try std.testing.expectEqual(GeneratedSqlCteMaterialization.materialized, read.cte_items[0].materialization.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const aliased_cte_read_sql = "WITH source_rows(source_id) AS NOT MATERIALIZED (SELECT id FROM usage_records) SELECT source_id FROM source_rows";
    const aliased_cte_read_result = try parseSqlAlloc(alloc, aliased_cte_read_sql);
    switch (aliased_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 14 }, read.cte_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 5 }, read.cte_items[0].column_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.cte_items[0].column_name_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].column_names.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.cte_items[0].column_names.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 8 }, read.cte_items[0].materialization_tokens.?);
            try std.testing.expectEqual(GeneratedSqlCteMaterialization.not_materialized, read.cte_items[0].materialization.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 18 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const multi_cte_read_sql = "WITH first_rows AS (SELECT id FROM usage_records), second_rows AS (SELECT id FROM first_rows) SELECT id FROM second_rows";
    const multi_cte_read_result = try parseSqlAlloc(alloc, multi_cte_read_sql);
    switch (multi_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 18 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 18 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 17 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.cte_count);
            try std.testing.expectEqual(@as(usize, 2), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[1].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 17 }, read.cte_items[1].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[1].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.cte_items[1].body_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.cte_items[1].body_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, read.cte_items[1].body_source_tokens.?);
            try std.testing.expect(!read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 21, .end = 22 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const recursive_cte_read_sql = "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records) SELECT id FROM source_rows";
    const recursive_cte_read_result = try parseSqlAlloc(alloc, recursive_cte_read_sql);
    switch (recursive_cte_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 10 }, read.cte_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 10 }, read.cte_list_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_last_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_last_body_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.cte_items[0].name_tokens);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.cte_items[0].body_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.cte_items[0].body_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expect(read.cte_recursive);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_set_operation_read_sql = "WITH source_rows AS (SELECT id FROM usage_records UNION SELECT id FROM usage_archive) SELECT id FROM source_rows";
    const cte_set_operation_read_result = try parseSqlAlloc(alloc, cte_set_operation_read_sql);
    switch (cte_set_operation_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 13 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.cte_items[0].body_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.cte_items[0].body_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.cte_items[0].body_set_operation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 13 }, read.cte_items[0].body_set_operation.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.cte_items[0].body_set_operation.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlSetOperationKind.@"union", read.cte_items[0].body_set_operation.kind.?);
            try std.testing.expect(read.cte_items[0].body_set_operation.all_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, read.cte_items[0].body_set_operation.right_query_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.cte_items[0].body_set_operation.right_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[0].body_set_operation.right_projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_set_operation.right_projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.cte_items[0].body_set_operation.right_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_aggregate_body_read_sql = "WITH totals AS (SELECT COUNT(*) AS total FROM usage_records) SELECT total FROM totals";
    const cte_aggregate_body_read_result = try parseSqlAlloc(alloc, cte_aggregate_body_read_sql);
    switch (cte_aggregate_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items.len);
            try std.testing.expectEqual(GeneratedSqlReadKind.aggregate, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 13 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 11 }, read.cte_items[0].body_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.cte_items[0].body_projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.cte_items[0].body_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_paginated_body_read_sql = "WITH source_rows AS (SELECT id FROM usage_records ORDER BY id LIMIT 5 OFFSET 2 ROWS) SELECT id FROM source_rows";
    const cte_paginated_body_read_result = try parseSqlAlloc(alloc, cte_paginated_body_read_sql);
    switch (cte_paginated_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 16 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[0].body_order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.cte_items[0].body_limit_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_limit_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.cte_items[0].body_limit_expression.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 16 }, read.cte_items[0].body_offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.cte_items[0].body_offset_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_fetch_body_read_sql = "WITH source_rows AS (SELECT id FROM usage_records FETCH FIRST 3 ROWS ONLY) SELECT id FROM source_rows";
    const cte_fetch_body_read_result = try parseSqlAlloc(alloc, cte_fetch_body_read_sql);
    switch (cte_fetch_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 13 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 13 }, read.cte_items[0].body_fetch_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[0].body_fetch_count_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.cte_items[0].body_fetch_count_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.cte_items[0].body_fetch_count_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_row_lock_body_read_sql = "WITH source_rows AS (SELECT id FROM usage_records FOR UPDATE SKIP LOCKED) SELECT id FROM source_rows";
    const cte_row_lock_body_read_result = try parseSqlAlloc(alloc, cte_row_lock_body_read_sql);
    switch (cte_row_lock_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 12 }, read.cte_items[0].body_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.cte_items[0].body_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 12 }, read.cte_items[0].body_row_lock_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const cte_graph_source_body_read_sql = "WITH ranked AS (SELECT * FROM antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5) AS gm) SELECT id FROM ranked";
    var cte_graph_source_body_tokens = try lexer.tokenizeAlloc(alloc, cte_graph_source_body_read_sql);
    defer lexer.freeTokens(alloc, &cte_graph_source_body_tokens);
    const cte_graph_source_body_read_result = try parseTokensAlloc(alloc, cte_graph_source_body_tokens.items);
    switch (cte_graph_source_body_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.cte, read.kind);
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.cte_items[0].body_kind.?);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_source_antfly_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_source_antfly_function_items.len);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_source_graph_function_count);
            try std.testing.expectEqual(@as(usize, 1), read.cte_items[0].body_source_graph_function_items.len);
            try std.testing.expectEqual(GeneratedSqlAntflyTableFunctionKind.graph_metric, read.cte_items[0].body_source_antfly_function_items[0].kind);
            try std.testing.expectEqual(GeneratedSqlGraphTableFunctionKind.metric, read.cte_items[0].body_source_graph_function_items[0].kind);
            try std.testing.expectEqualStrings(
                "antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', top_k => 5)",
                tokenRangeText(cte_graph_source_body_read_sql, cte_graph_source_body_tokens.items, read.cte_items[0].body_source_graph_function_items[0].tokens),
            );
            try std.testing.expectEqualStrings(
                "'pagerank'",
                tokenRangeText(cte_graph_source_body_read_sql, cte_graph_source_body_tokens.items, read.cte_items[0].body_source_graph_function_items[0].metric_value_tokens.?),
            );
        },
        else => return error.TestUnexpectedResult,
    }

    const set_operation_read_sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive";
    const set_operation_read_result = try parseSqlAlloc(alloc, set_operation_read_sql);
    switch (set_operation_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 9 }, read.set_operation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 9 }, read.set_operation.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.set_operation.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlSetOperationKind.@"union", read.set_operation.kind.?);
            try std.testing.expect(read.set_operation.all_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.set_operation.right_query_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.set_operation.right_select_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.set_operation.right_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.set_operation.right_projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.set_operation.right_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const set_operation_all_read_sql = "SELECT id FROM usage_records UNION ALL SELECT id FROM usage_archive";
    const set_operation_all_read_result = try parseSqlAlloc(alloc, set_operation_all_read_sql);
    switch (set_operation_all_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 10 }, read.set_operation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.set_operation.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlSetOperationKind.@"union", read.set_operation.kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.set_operation.all_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 10 }, read.set_operation.right_query_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.set_operation.right_projection_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.set_operation.right_source_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const set_operation_tail_read_sql = "SELECT id FROM usage_records UNION SELECT id FROM usage_archive ORDER BY id ASC LIMIT ALL OFFSET 2 ROWS";
    const set_operation_tail_read_result = try parseSqlAlloc(alloc, set_operation_tail_read_sql);
    switch (set_operation_tail_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.set_operation, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 9 }, read.set_operation_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 9 }, read.set_operation.right_query_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 13 }, read.order_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.limit_tokens.?);
            try std.testing.expect(read.limit_all);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 18 }, read.offset_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.offset_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, read.offset_expression.tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const graph_sql = "CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')";
    const graph_result = try parseSqlAlloc(alloc, graph_sql);
    switch (graph_result.ast.?) {
        .graph => |graph| {
            try std.testing.expectEqual(GeneratedSqlGraphKind.create_metric, graph.kind);
            try std.testing.expectEqualStrings("CREATE GRAPH METRIC docs_pagerank ON doc_edges WITH (metric = 'pagerank')", spanText(graph_sql, graph.statement_span));
            try std.testing.expectEqualStrings("CREATE", spanText(graph_sql, graph.command_span));
        },
        else => return error.TestUnexpectedResult,
    }

    const alter_graph_sql = "ALTER GRAPH INDEX docs_edge_graph ADD METRIC pagerank_v1 USING pagerank WITH (damping = 0.85, max_iterations = 40)";
    const alter_graph_result = try parseSqlAlloc(alloc, alter_graph_sql);
    switch (alter_graph_result.ast.?) {
        .graph => |graph| {
            try std.testing.expectEqual(GeneratedSqlGraphKind.alter_metric, graph.kind);
            try std.testing.expectEqualStrings(alter_graph_sql, spanText(alter_graph_sql, graph.statement_span));
            try std.testing.expectEqualStrings("ALTER", spanText(alter_graph_sql, graph.command_span));
        },
        else => return error.TestUnexpectedResult,
    }

    const analyze_sql = "ANALYZE";
    const analyze_result = try parseSqlAlloc(alloc, analyze_sql);
    switch (analyze_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.analyze, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.analyze_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("ANALYZE", spanText(analyze_sql, unsupported.statement_span));
            try std.testing.expectEqualStrings("ANALYZE", spanText(analyze_sql, unsupported.command_span));
            try std.testing.expect(unsupported.subject_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const row_lock_sql = "SELECT id FROM usage_records FOR UPDATE SKIP LOCKED";
    const row_lock_result = try parseSqlAlloc(alloc, row_lock_sql);
    switch (row_lock_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqualStrings(row_lock_sql, spanText(row_lock_sql, read.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(row_lock_sql, read.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 8 }, read.row_lock_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const no_key_update_row_lock_sql = "SELECT id FROM usage_records FOR NO KEY UPDATE OF usage_records NOWAIT";
    const no_key_update_row_lock_result = try parseSqlAlloc(alloc, no_key_update_row_lock_sql);
    switch (no_key_update_row_lock_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqualStrings(no_key_update_row_lock_sql, spanText(no_key_update_row_lock_sql, read.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(no_key_update_row_lock_sql, read.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 11 }, read.row_lock_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const key_share_row_lock_sql = "SELECT id FROM usage_records FOR KEY SHARE SKIP LOCKED";
    const key_share_row_lock_result = try parseSqlAlloc(alloc, key_share_row_lock_sql);
    switch (key_share_row_lock_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqualStrings(key_share_row_lock_sql, spanText(key_share_row_lock_sql, read.statement_span));
            try std.testing.expectEqualStrings("SELECT", spanText(key_share_row_lock_sql, read.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 9 }, read.row_lock_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const checkpoint_sql = "CHECKPOINT";
    const checkpoint_result = try parseSqlAlloc(alloc, checkpoint_sql);
    switch (checkpoint_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.checkpoint, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.checkpoint_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("CHECKPOINT", spanText(checkpoint_sql, unsupported.statement_span));
            try std.testing.expectEqualStrings("CHECKPOINT", spanText(checkpoint_sql, unsupported.command_span));
            try std.testing.expect(unsupported.subject_tokens == null);
        },
        else => return error.TestUnexpectedResult,
    }

    const copy_sql = "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv)";
    const copy_result = try parseSqlAlloc(alloc, copy_sql);
    switch (copy_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.copy, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.copy_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("COPY", spanText(copy_sql, unsupported.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 14 }, unsupported.subject_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const vacuum_sql = "VACUUM (FULL, VERBOSE, ANALYZE) public.usage_records";
    const vacuum_result = try parseSqlAlloc(alloc, vacuum_sql);
    switch (vacuum_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.vacuum, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.vacuum_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("VACUUM", spanText(vacuum_sql, unsupported.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 9 }, unsupported.subject_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const reindex_sql = "REINDEX INDEX CONCURRENTLY public.usage_status_idx";
    const reindex_result = try parseSqlAlloc(alloc, reindex_sql);
    switch (reindex_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.reindex, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.reindex_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("REINDEX", spanText(reindex_sql, unsupported.command_span));
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, unsupported.subject_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const utility_cases = [_]struct {
        sql: []const u8,
        kind: GeneratedSqlUnsupportedKind,
        reason: GeneratedSqlUnsupportedReason,
        subject_tokens: GeneratedSqlTokenRange,
    }{
        .{
            .sql = "ALTER AGGREGATE first_value_text(text) OWNER TO app_role",
            .kind = .alter_aggregate,
            .reason = .alter_aggregate_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER CONVERSION usage_conv RENAME TO usage_conv_v2",
            .kind = .alter_conversion,
            .reason = .alter_conversion_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly",
            .kind = .alter_default_privileges,
            .reason = .alter_default_privileges_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 12 },
        },
        .{
            .sql = "ALTER EVENT TRIGGER usage_ddl_start DISABLE",
            .kind = .alter_event_trigger,
            .reason = .alter_event_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "ALTER INDEX usage_status_idx RENAME TO usage_status_idx_v2",
            .kind = .alter_index,
            .reason = .alter_index_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "ALTER FOREIGN TABLE foreign_usage_records RENAME TO foreign_usage_archive",
            .kind = .alter_foreign_table,
            .reason = .alter_foreign_table_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "ALTER FOREIGN DATA WRAPPER usage_fdw OPTIONS (ADD host 'localhost')",
            .kind = .alter_foreign_data_wrapper,
            .reason = .alter_foreign_data_wrapper_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 11 },
        },
        .{
            .sql = "ALTER FUNCTION normalize_status(text) OWNER TO app_role",
            .kind = .alter_function,
            .reason = .alter_function_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER LANGUAGE usage_lang OWNER TO app_role",
            .kind = .alter_language,
            .reason = .alter_language_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "ALTER MATERIALIZED VIEW usage_summary RENAME TO usage_summary_v2",
            .kind = .alter_materialized_view,
            .reason = .alter_materialized_view_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "ALTER OPERATOR === (text, text) OWNER TO app_role",
            .kind = .alter_operator,
            .reason = .alter_operator_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 13 },
        },
        .{
            .sql = "ALTER OPERATOR CLASS usage_ops USING btree RENAME TO usage_ops_v2",
            .kind = .alter_operator_class,
            .reason = .alter_operator_class_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER OPERATOR FAMILY usage_family USING btree RENAME TO usage_family_v2",
            .kind = .alter_operator_family,
            .reason = .alter_operator_family_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER PROCEDURE refresh_usage_records() OWNER TO app_role",
            .kind = .alter_procedure,
            .reason = .alter_procedure_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER LARGE OBJECT 12345 OWNER TO app_role",
            .kind = .alter_large_object,
            .reason = .alter_large_object_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "ALTER ROUTINE normalize_status(text) OWNER TO app_role",
            .kind = .alter_routine,
            .reason = .alter_routine_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER RULE usage_insert ON usage_records RENAME TO usage_insert_v2",
            .kind = .alter_rule,
            .reason = .alter_rule_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER SERVER usage_server VERSION '15'",
            .kind = .alter_server,
            .reason = .alter_server_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "ALTER SYSTEM SET work_mem = '64MB'",
            .kind = .alter_system,
            .reason = .alter_system_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "ALTER STATISTICS usage_stats SET STATISTICS 100",
            .kind = .alter_statistics,
            .reason = .alter_statistics_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "ALTER TEXT SEARCH CONFIGURATION usage_search RENAME TO usage_search_v2",
            .kind = .alter_text_search_configuration,
            .reason = .alter_text_search_configuration_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER TEXT SEARCH DICTIONARY usage_dict OWNER TO app_role",
            .kind = .alter_text_search_dictionary,
            .reason = .alter_text_search_dictionary_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER TEXT SEARCH PARSER usage_parser RENAME TO usage_parser_v2",
            .kind = .alter_text_search_parser,
            .reason = .alter_text_search_parser_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER TEXT SEARCH TEMPLATE usage_template RENAME TO usage_template_v2",
            .kind = .alter_text_search_template,
            .reason = .alter_text_search_template_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER TRIGGER usage_audit ON usage_records RENAME TO usage_audit_v2",
            .kind = .alter_trigger,
            .reason = .alter_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "ALTER TRANSFORM FOR jsonb LANGUAGE plpgsql OWNER TO app_role",
            .kind = .alter_transform,
            .reason = .alter_transform_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "ALTER USER MAPPING FOR usage_user SERVER usage_server OPTIONS (SET user 'remote')",
            .kind = .alter_user_mapping,
            .reason = .alter_user_mapping_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 13 },
        },
        .{
            .sql = "CALL refresh_usage_records()",
            .kind = .call,
            .reason = .call_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 4 },
        },
        .{
            .sql = "CLUSTER usage_records USING usage_status_idx",
            .kind = .cluster,
            .reason = .cluster_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 4 },
        },
        .{
            .sql = "COMMENT ON TABLE usage_records IS 'billing rows'",
            .kind = .comment,
            .reason = .comment_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "CREATE ACCESS METHOD usage_am TYPE INDEX HANDLER usage_handler",
            .kind = .create_access_method,
            .reason = .create_access_method_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "CREATE CONVERSION usage_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1",
            .kind = .create_conversion,
            .reason = .create_conversion_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "CREATE DATABASE tenant_ops WITH OWNER app",
            .kind = .create_database_options,
            .reason = .create_database_options_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "CREATE EVENT TRIGGER usage_ddl_start ON ddl_command_start EXECUTE FUNCTION audit_ddl()",
            .kind = .create_event_trigger,
            .reason = .create_event_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 11 },
        },
        .{
            .sql = "CREATE FOREIGN TABLE foreign_usage_records (id text) SERVER usage_fdw",
            .kind = .create_foreign_table,
            .reason = .create_foreign_table_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 10 },
        },
        .{
            .sql = "CREATE FOREIGN DATA WRAPPER usage_fdw HANDLER usage_fdw_handler",
            .kind = .create_foreign_data_wrapper,
            .reason = .create_foreign_data_wrapper_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "CREATE LANGUAGE usage_lang",
            .kind = .create_language,
            .reason = .create_language_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 3 },
        },
        .{
            .sql = "CREATE OPERATOR CLASS usage_ops DEFAULT FOR TYPE text USING btree AS OPERATOR 1 < (text, text)",
            .kind = .create_operator_class,
            .reason = .create_operator_class_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 19 },
        },
        .{
            .sql = "CREATE OPERATOR FAMILY usage_family USING btree",
            .kind = .create_operator_family,
            .reason = .create_operator_family_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "CREATE RULE usage_insert AS ON INSERT TO usage_records DO ALSO NOTIFY usage_events",
            .kind = .create_rule,
            .reason = .create_rule_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 12 },
        },
        .{
            .sql = "CREATE SCHEMA analytics AUTHORIZATION app_user",
            .kind = .create_schema_options,
            .reason = .create_schema_options_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "CREATE SERVER usage_server FOREIGN DATA WRAPPER postgres_fdw",
            .kind = .create_server,
            .reason = .create_server_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "CREATE STATISTICS usage_stats ON tenant_id, status FROM usage_records",
            .kind = .create_statistics,
            .reason = .create_statistics_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "CREATE TEXT SEARCH CONFIGURATION usage_search (COPY = pg_catalog.english)",
            .kind = .create_text_search_configuration,
            .reason = .create_text_search_configuration_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 10 },
        },
        .{
            .sql = "CREATE TEXT SEARCH DICTIONARY usage_dict (TEMPLATE = simple)",
            .kind = .create_text_search_dictionary,
            .reason = .create_text_search_dictionary_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 10 },
        },
        .{
            .sql = "CREATE TEXT SEARCH PARSER usage_parser (START = prsd_start)",
            .kind = .create_text_search_parser,
            .reason = .create_text_search_parser_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 10 },
        },
        .{
            .sql = "CREATE TEXT SEARCH TEMPLATE usage_template (LEXIZE = dsimple_lexize)",
            .kind = .create_text_search_template,
            .reason = .create_text_search_template_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 10 },
        },
        .{
            .sql = "CREATE TRANSFORM FOR jsonb LANGUAGE plpgsql FROM SQL WITH FUNCTION jsonb_to_plpgsql(internal)",
            .kind = .create_transform,
            .reason = .create_transform_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 14 },
        },
        .{
            .sql = "CREATE TRIGGER usage_audit BEFORE INSERT ON usage_records FOR EACH ROW EXECUTE FUNCTION audit_usage()",
            .kind = .create_trigger,
            .reason = .create_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 2, .end = 15 },
        },
        .{
            .sql = "CREATE USER MAPPING FOR usage_user SERVER usage_server OPTIONS (user 'remote')",
            .kind = .create_user_mapping,
            .reason = .create_user_mapping_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 12 },
        },
        .{
            .sql = "DO 'BEGIN NULL; END'",
            .kind = .do_block,
            .reason = .do_block_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 2 },
        },
        .{
            .sql = "DROP FOREIGN TABLE IF EXISTS foreign_usage_records",
            .kind = .drop_foreign_table,
            .reason = .drop_foreign_table_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP FOREIGN DATA WRAPPER IF EXISTS usage_fdw CASCADE",
            .kind = .drop_foreign_data_wrapper,
            .reason = .drop_foreign_data_wrapper_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "IMPORT FOREIGN SCHEMA public FROM SERVER usage_server INTO local_schema",
            .kind = .import_foreign_schema,
            .reason = .import_foreign_schema_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "DROP ACCESS METHOD IF EXISTS usage_am",
            .kind = .drop_access_method,
            .reason = .drop_access_method_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP CONVERSION IF EXISTS usage_conv",
            .kind = .drop_conversion,
            .reason = .drop_conversion_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP EVENT TRIGGER IF EXISTS usage_ddl_start",
            .kind = .drop_event_trigger,
            .reason = .drop_event_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP COLLATION case_insensitive, accent_insensitive",
            .kind = .drop_collation_multi,
            .reason = .drop_collation_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP DOMAIN positive_amount, nonempty_text CASCADE",
            .kind = .drop_domain_multi,
            .reason = .drop_domain_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP EXTENSION vector, postgis",
            .kind = .drop_extension_multi,
            .reason = .drop_extension_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP INDEX usage_status_idx, usage_tenant_idx CASCADE",
            .kind = .drop_index_multi,
            .reason = .drop_index_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP LANGUAGE IF EXISTS usage_lang CASCADE",
            .kind = .drop_language,
            .reason = .drop_language_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP MATERIALIZED VIEW usage_summary, old_usage_summary CASCADE",
            .kind = .drop_materialized_view_multi,
            .reason = .drop_materialized_view_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP OWNED BY usage_role CASCADE",
            .kind = .drop_owned,
            .reason = .drop_owned_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP OPERATOR CLASS IF EXISTS usage_ops USING btree",
            .kind = .drop_operator_class,
            .reason = .drop_operator_class_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "DROP OPERATOR FAMILY IF EXISTS usage_family USING btree",
            .kind = .drop_operator_family,
            .reason = .drop_operator_family_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 8 },
        },
        .{
            .sql = "DROP PUBLICATION usage_pub, old_usage_pub",
            .kind = .drop_publication_multi,
            .reason = .drop_publication_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP ROUTINE IF EXISTS normalize_status(text) CASCADE",
            .kind = .drop_routine,
            .reason = .drop_routine_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "DROP ROLE app_writer, app_reader",
            .kind = .drop_role_multi,
            .reason = .drop_role_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP SCHEMA analytics, reporting",
            .kind = .drop_schema_multi,
            .reason = .drop_schema_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP SEQUENCE order_id_seq, old_order_id_seq CASCADE",
            .kind = .drop_sequence_multi,
            .reason = .drop_sequence_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP STATISTICS IF EXISTS usage_stats",
            .kind = .drop_statistics,
            .reason = .drop_statistics_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP TABLE usage_records, archived_usage_records",
            .kind = .drop_table_multi,
            .reason = .drop_table_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "DROP TEXT SEARCH CONFIGURATION IF EXISTS usage_search",
            .kind = .drop_text_search_configuration,
            .reason = .drop_text_search_configuration_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP TEXT SEARCH DICTIONARY IF EXISTS usage_dict",
            .kind = .drop_text_search_dictionary,
            .reason = .drop_text_search_dictionary_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP TEXT SEARCH PARSER IF EXISTS usage_parser",
            .kind = .drop_text_search_parser,
            .reason = .drop_text_search_parser_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP TEXT SEARCH TEMPLATE IF EXISTS usage_template",
            .kind = .drop_text_search_template,
            .reason = .drop_text_search_template_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP TRANSFORM FOR jsonb LANGUAGE plpgsql",
            .kind = .drop_transform,
            .reason = .drop_transform_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP TYPE usage_status, archived_status CASCADE",
            .kind = .drop_type_multi,
            .reason = .drop_type_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP VIEW active_usage, archived_usage",
            .kind = .drop_view_multi,
            .reason = .drop_view_multi_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 5 },
        },
        .{
            .sql = "GRANT SELECT ON TABLE usage_records TO readonly",
            .kind = .grant,
            .reason = .grant_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "LISTEN usage_events",
            .kind = .listen,
            .reason = .listen_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 2 },
        },
        .{
            .sql = "LOAD 'auto_explain'",
            .kind = .load,
            .reason = .load_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 2 },
        },
        .{
            .sql = "LOCK TABLE usage_records IN SHARE MODE",
            .kind = .lock,
            .reason = .lock_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "NOTIFY usage_events, 'changed'",
            .kind = .notify,
            .reason = .notify_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 4 },
        },
        .{
            .sql = "REVOKE SELECT ON TABLE usage_records FROM readonly",
            .kind = .revoke,
            .reason = .revoke_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "REASSIGN OWNED BY old_role TO new_role",
            .kind = .reassign_owned,
            .reason = .reassign_owned_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "SECURITY LABEL ON TABLE usage_records IS 'internal'",
            .kind = .security_label,
            .reason = .security_label_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP RULE IF EXISTS usage_insert ON usage_records",
            .kind = .drop_rule,
            .reason = .drop_rule_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 7 },
        },
        .{
            .sql = "DROP SERVER IF EXISTS usage_server CASCADE",
            .kind = .drop_server,
            .reason = .drop_server_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 6 },
        },
        .{
            .sql = "DROP TRIGGER IF EXISTS usage_audit ON usage_records",
            .kind = .drop_trigger,
            .reason = .drop_trigger_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 2, .end = 7 },
        },
        .{
            .sql = "DROP USER MAPPING IF EXISTS FOR usage_user SERVER usage_server",
            .kind = .drop_user_mapping,
            .reason = .drop_user_mapping_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 9 },
        },
        .{
            .sql = "UNLISTEN *",
            .kind = .unlisten,
            .reason = .unlisten_not_planned_by_generated_parser,
            .subject_tokens = .{ .start = 1, .end = 2 },
        },
    };
    for (utility_cases) |case| {
        const result = try parseSqlAlloc(alloc, case.sql);
        switch (result.ast.?) {
            .unsupported => |unsupported| {
                try std.testing.expectEqual(case.kind, unsupported.kind);
                try std.testing.expectEqual(case.reason, unsupported.reason);
                try std.testing.expectEqualStrings(case.sql[0..std.mem.indexOfScalar(u8, case.sql, ' ').?], spanText(case.sql, unsupported.command_span));
                try std.testing.expectEqual(case.subject_tokens, unsupported.subject_tokens.?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    const explain_sql = "EXPLAIN SELECT id FROM usage_records";
    const explain_result = try parseSqlAlloc(alloc, explain_sql);
    switch (explain_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expect(unsupported.reason == .explain_not_planned_by_generated_parser);
            try std.testing.expectEqualStrings("EXPLAIN SELECT id FROM usage_records", spanText(explain_sql, unsupported.statement_span));
            try std.testing.expectEqualStrings("EXPLAIN", spanText(explain_sql, unsupported.command_span));
            try std.testing.expect(unsupported.subject_tokens.?.start == 1);
            try std.testing.expect(unsupported.subject_tokens.?.end == 5);
            try std.testing.expect(unsupported.explain_options_tokens == null);
            try std.testing.expect(unsupported.explain_options_valid);
            try std.testing.expect(!unsupported.explain_analyze);
            try std.testing.expectEqual(GeneratedSqlExplainFormat.text, unsupported.explain_format);
        },
        else => return error.TestUnexpectedResult,
    }

    const empty_explain_sql = "EXPLAIN";
    const empty_explain_result = try parseSqlAlloc(alloc, empty_explain_sql);
    switch (empty_explain_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqualStrings("EXPLAIN", spanText(empty_explain_sql, unsupported.statement_span));
            try std.testing.expect(unsupported.subject_tokens == null);
            try std.testing.expect(unsupported.explain_options_tokens == null);
            try std.testing.expect(unsupported.explain_options_valid);
        },
        else => return error.TestUnexpectedResult,
    }

    const explain_options_sql = "EXPLAIN (FORMAT JSON, VERBOSE, COSTS OFF, ANALYZE ON, BUFFERS, TIMING OFF, SUMMARY OFF, SETTINGS ON, WAL) SELECT id FROM usage_records";
    const explain_options_result = try parseSqlAlloc(alloc, explain_options_sql);
    switch (explain_options_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlUnsupportedReason.explain_not_planned_by_generated_parser, unsupported.reason);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 26, .end = 30 }, unsupported.subject_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 26 }, unsupported.explain_options_tokens.?);
            try std.testing.expect(unsupported.explain_options_valid);
            try std.testing.expect(unsupported.explain_analyze);
            try std.testing.expectEqual(GeneratedSqlExplainFormat.json, unsupported.explain_format);
            try std.testing.expect(unsupported.explain_verbose);
            try std.testing.expect(!unsupported.explain_costs);
            try std.testing.expect(unsupported.explain_buffers);
            try std.testing.expect(!unsupported.explain_timing);
            try std.testing.expect(!unsupported.explain_summary);
            try std.testing.expect(unsupported.explain_settings);
            try std.testing.expect(unsupported.explain_wal);
        },
        else => return error.TestUnexpectedResult,
    }

    const explain_analyze_sql = "EXPLAIN ANALYZE INSERT INTO usage_records (id) VALUES ('u1')";
    const explain_analyze_result = try parseSqlAlloc(alloc, explain_analyze_sql);
    switch (explain_analyze_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 12 }, unsupported.subject_tokens.?);
            try std.testing.expect(unsupported.explain_options_tokens == null);
            try std.testing.expect(unsupported.explain_options_valid);
            try std.testing.expect(unsupported.explain_analyze);
        },
        else => return error.TestUnexpectedResult,
    }

    const invalid_explain_options_sql = "EXPLAIN (FORMAT YAML) SELECT 1";
    const invalid_explain_options_result = try parseSqlAlloc(alloc, invalid_explain_options_sql);
    switch (invalid_explain_options_result.ast.?) {
        .unsupported => |unsupported| {
            try std.testing.expectEqual(GeneratedSqlUnsupportedKind.explain, unsupported.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 7 }, unsupported.subject_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 5 }, unsupported.explain_options_tokens.?);
            try std.testing.expect(!unsupported.explain_options_valid);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser facade builds unary arithmetic expression spans" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const unary_arithmetic_read_sql = "SELECT -amount AS neg_amount, +bonus AS plus_bonus FROM usage_records WHERE amount > -10 ORDER BY -amount DESC";
    const unary_arithmetic_read_result = try parseSqlAlloc(alloc, unary_arithmetic_read_sql);
    switch (unary_arithmetic_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.unary_negative, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].operator_tokens.?);
            try std.testing.expect(read.projection_items.expressions[0].left_tokens == null);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_items.expressions[0].right_tokens.?);
            const negative_projection_right = read.projection_items.expressions[0].right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, negative_projection_right.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, negative_projection_right.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.unary_positive, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.projection_items.expressions[1].operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_items.expressions[1].right_tokens.?);
            const positive_projection_right = read.projection_items.expressions[1].right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, positive_projection_right.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, positive_projection_right.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 17 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.unary_negative, read.where_expression.right_expression_kind.?);
            const right = read.where_expression.right_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.unary_negative, right.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 16 }, right.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, right.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.unary_negative, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read.order_first_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 20, .end = 21 }, read.order_first_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes JSON path operator AST metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const json_path_projection_read_sql = "SELECT metadata #>> '{billing,plan}' AS plan FROM usage_records WHERE metadata #> '{flags}' = $1::jsonb";
    const json_path_projection_read_result = try parseSqlAlloc(alloc, json_path_projection_read_sql);
    switch (json_path_projection_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.projection_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 6 }, read.projection_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 4 }, read.projection_items.expression_items[0]);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 6 }, read.projection_items.alias_items[0].?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_text_access, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 2, .end = 3 }, read.projection_items.expressions[0].operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[0].right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_text_access, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 12 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_access, read.where_expression.left_expression_kind.?);
            const path_left = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_access, path_left.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, path_left.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, path_left.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, path_left.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 13, .end = 14 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const json_path_text_predicate_read_sql = "SELECT id FROM usage_records WHERE metadata #>> '{billing,plan}' = 'pro'";
    const json_path_text_predicate_read_result = try parseSqlAlloc(alloc, json_path_text_predicate_read_sql);
    switch (json_path_text_predicate_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 8 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_text_access, read.where_expression.left_expression_kind.?);
            const path_left = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.json_path_text_access, path_left.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, path_left.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, path_left.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, path_left.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes interval literal AST metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const interval_read_sql = "SELECT date_bin(INTERVAL '1 hour', amount, 0) AS amount_bucket FROM usage_records WHERE date_bin(INTERVAL '1 day', amount, 0) = $1";
    const interval_read_result = try parseSqlAlloc(alloc, interval_read_sql);
    switch (interval_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_first_expression.function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 9 }, read.projection_first_expression.argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 3), read.projection_first_expression.argument_items.count);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 5 }, read.projection_first_expression.argument_items.items[0]);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.interval_literal, read.projection_first_expression.argument_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 4, .end = 5 }, read.projection_first_expression.argument_items.expressions[0].interval_value_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.where_expression.left_expression_kind.?);
            const predicate_call = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 17, .end = 23 }, predicate_call.argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 3), predicate_call.argument_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.interval_literal, predicate_call.argument_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, predicate_call.argument_items.expressions[0].interval_value_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 24, .end = 25 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes timestamp literal AST metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const timestamp_read_sql = "SELECT date_bin(INTERVAL '1 hour', TIMESTAMPTZ '2025-01-01T01:30:00+01:30', TIMESTAMP '2025-01-01T00:00:00') AS planned_bucket FROM usage_records WHERE id = $1";
    const timestamp_read_result = try parseSqlAlloc(alloc, timestamp_read_sql);
    switch (timestamp_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 11 }, read.projection_first_expression.argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 3), read.projection_first_expression.argument_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.interval_literal, read.projection_first_expression.argument_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.timestamp_literal, read.projection_first_expression.argument_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 6, .end = 7 }, read.projection_first_expression.argument_items.expressions[1].timestamp_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 7, .end = 8 }, read.projection_first_expression.argument_items.expressions[1].timestamp_value_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.timestamp_literal, read.projection_first_expression.argument_items.expressions[2].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.projection_first_expression.argument_items.expressions[2].timestamp_type_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.projection_first_expression.argument_items.expressions[2].timestamp_value_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes current temporal keyword AST metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const current_timestamp_sql = "SELECT CURRENT_TIMESTAMP(6) AS planned_at_ns FROM users WHERE id = $1";
    const current_timestamp_result = try parseSqlAlloc(alloc, current_timestamp_sql);
    switch (current_timestamp_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.current_timestamp, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_first_expression.current_timestamp_precision_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 11, .end = 12 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }

    const current_date_sql = "SELECT CURRENT_DATE AS planned_day_ns FROM users WHERE id = $1";
    const current_date_result = try parseSqlAlloc(alloc, current_date_sql);
    switch (current_date_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.current_date, read.projection_first_expression.kind);
            try std.testing.expect(read.projection_first_expression.current_timestamp_precision_tokens == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 9, .end = 10 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes extract expression AST metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const extract_read_sql = "SELECT EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(hour FROM amount) = $1";
    const extract_read_result = try parseSqlAlloc(alloc, extract_read_sql);
    switch (extract_read_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.extract_expression, read.projection_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_first_expression.extract_field_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_first_expression.extract_source_tokens.?);
            try std.testing.expect(read.projection_first_expression.extract_source_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, read.projection_first_expression.extract_source_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 5, .end = 6 }, read.projection_first_expression.extract_source_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.extract_expression, read.where_expression.left_expression_kind.?);
            const predicate_extract = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlExpressionKind.extract_expression, predicate_extract.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, predicate_extract.extract_field_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, predicate_extract.extract_source_tokens.?);
            try std.testing.expect(predicate_extract.extract_source_expression_kind == null);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.token_range, predicate_extract.extract_source_expression.?.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 16, .end = 17 }, predicate_extract.extract_source_expression.?.tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 18, .end = 19 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 20 }, read.where_expression.right_tokens.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes temporal function read metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const temporal_function_sql = "SELECT date_part('hour', amount) AS amount_hour, EXTRACT(dow FROM amount) AS amount_dow FROM usage_records WHERE EXTRACT(dow FROM amount) = $1 ORDER BY date_part('month', amount) ASC LIMIT 5";
    const temporal_function_result = try parseSqlAlloc(alloc, temporal_function_sql);
    switch (temporal_function_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 6 }, read.projection_items.expressions[0].argument_tokens.?);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.expressions[0].argument_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.extract_expression, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 12, .end = 13 }, read.projection_items.expressions[1].extract_field_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 14, .end = 15 }, read.projection_items.expressions[1].extract_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.extract_expression, read.where_expression.left_expression_kind.?);
            const predicate_extract = read.where_expression.left_expression orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 23, .end = 24 }, predicate_extract.extract_field_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 25, .end = 26 }, predicate_extract.extract_source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 27, .end = 28 }, read.where_expression.operator_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 28, .end = 29 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(@as(usize, 1), read.order_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 31, .end = 32 }, read.order_first_expression.function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 33, .end = 36 }, read.order_first_expression.argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read.order_items.direction_items[0].?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser exposes range helper function metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const range_helper_sql = "SELECT lower(p.valid_at) AS valid_start, upper(p.valid_at) AS valid_end FROM price_intervals AS p WHERE lower(p.valid_at) >= 1 AND upper(p.valid_at) IS NOT NULL ORDER BY upper(p.valid_at) DESC LIMIT 5";
    const range_helper_result = try parseSqlAlloc(alloc, range_helper_sql);
    switch (range_helper_result.ast.?) {
        .read => |read| {
            try std.testing.expectEqual(GeneratedSqlReadKind.query, read.kind);
            try std.testing.expectEqual(@as(usize, 2), read.projection_items.count);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[0].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 1, .end = 2 }, read.projection_items.expressions[0].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 3, .end = 4 }, read.projection_items.expressions[0].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.projection_items.expressions[1].kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 8, .end = 9 }, read.projection_items.expressions[1].function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 10, .end = 11 }, read.projection_items.expressions[1].argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 15, .end = 18 }, read.source_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.logical_and, read.where_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 19, .end = 25 }, read.where_expression.left_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.comparison, read.where_expression.left_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 26, .end = 33 }, read.where_expression.right_tokens.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.is_not_null, read.where_expression.right_expression_kind.?);
            try std.testing.expectEqual(GeneratedSqlExpressionKind.function_call, read.order_first_expression.kind);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 35, .end = 36 }, read.order_first_expression.function_name_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 37, .end = 38 }, read.order_first_expression.argument_tokens.?);
            try std.testing.expectEqual(GeneratedSqlTokenRange{ .start = 39, .end = 40 }, read.order_items.direction_items[0].?);
            try std.testing.expectEqual(GeneratedSqlOrderDirection.desc, read.order_items.directions[0].?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "generated SQL parser reports source-aware diagnostics" {
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, "CREATE TABLE usage_records (id)");
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
    defer std.testing.allocator.free(diagnostic.expected);
    try std.testing.expect(diagnostic.expected.len > 0);
    try std.testing.expectEqualStrings(")", diagnostic.actual);
    try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
}

test "generated SQL parser diagnostics map generated token indexes to source tokens" {
    const sql = "SELECT public.docs. FROM public.docs";
    var tokens = try lexer.tokenizeAlloc(std.testing.allocator, sql);
    defer lexer.freeTokens(std.testing.allocator, &tokens);
    const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
    defer std.testing.allocator.free(diagnostic.expected);
    try std.testing.expect(diagnostic.token_index <= tokens.items.len);
    try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
    try std.testing.expect(diagnostic.source_end <= sql.len);
    try std.testing.expect(diagnostic.expected.len > 0);
}

test "generated SQL parser reports bounded diagnostics for malformed corpus" {
    const cases = [_][]const u8{
        "SELECT id FROM",
        "SELECT id FROM usage_records WHERE",
        "WITH source_rows AS (SELECT id FROM usage_records SELECT id FROM source_rows",
        "CREATE TABLE usage_records (id text",
        "INSERT INTO usage_records (id VALUES ('u1')",
        "EXPLAIN (FORMAT",
    };

    for (cases) |sql| {
        var tokens = try lexer.tokenizeAlloc(std.testing.allocator, sql);
        defer lexer.freeTokens(std.testing.allocator, &tokens);
        const diagnostic = try diagnosticAlloc(std.testing.allocator, tokens.items) orelse return error.ExpectedDiagnostic;
        defer std.testing.allocator.free(diagnostic.expected);
        try std.testing.expect(diagnostic.token_index <= tokens.items.len);
        try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
        try std.testing.expect(diagnostic.expected.len > 0);
    }
}

fn appendFuzzSqlPart(buffer: []u8, len: *usize, part: []const u8) void {
    if (len.* + part.len > buffer.len) return;
    @memcpy(buffer[len.* .. len.* + part.len], part);
    len.* += part.len;
}

fn appendFuzzSqlByte(buffer: []u8, len: *usize, byte: u8) void {
    if (len.* == buffer.len) return;
    buffer[len.*] = byte;
    len.* += 1;
}

fn generatedParserRandomFuzzSql(random: std.Random, buffer: []u8) []const u8 {
    const parts = [_][]const u8{
        "SELECT",
        "WITH",
        "RECURSIVE",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "EXPLAIN",
        "FROM",
        "WHERE",
        "GROUP",
        "ORDER",
        "BY",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "FIRST",
        "ROWS",
        "ONLY",
        "AS",
        "JOIN",
        "LEFT",
        "ON",
        "UNION",
        "ALL",
        "VALUES",
        "SET",
        "INTO",
        "TABLE",
        "INDEX",
        "GRAPH",
        "METRIC",
        "id",
        "status",
        "tenant",
        "usage_records",
        "source_rows",
        "1",
        "42",
        "'open'",
        "$1",
        "(",
        ")",
        ",",
        ".",
        "*",
        "=",
        "<>",
        "::",
        "+",
        "-",
    };
    var len: usize = 0;
    const part_count = random.intRangeLessThan(usize, 1, 36);
    for (0..part_count) |idx| {
        if (idx != 0 and random.boolean()) appendFuzzSqlByte(buffer, &len, ' ');
        appendFuzzSqlPart(buffer, &len, parts[random.intRangeLessThan(usize, 0, parts.len)]);
    }
    return buffer[0..len];
}

fn generatedParserMutatedFuzzSql(random: std.Random, seed: []const u8, buffer: []u8) []const u8 {
    const replacement = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_(),.*=<>+-' ";
    var len: usize = 0;
    for (seed) |byte| {
        const action = random.intRangeLessThan(u8, 0, 16);
        switch (action) {
            0 => {},
            1 => {
                appendFuzzSqlByte(buffer, &len, replacement[random.intRangeLessThan(usize, 0, replacement.len)]);
            },
            2 => {
                appendFuzzSqlByte(buffer, &len, byte);
                appendFuzzSqlByte(buffer, &len, byte);
            },
            3 => {
                appendFuzzSqlByte(buffer, &len, byte);
                appendFuzzSqlByte(buffer, &len, replacement[random.intRangeLessThan(usize, 0, replacement.len)]);
            },
            else => appendFuzzSqlByte(buffer, &len, byte),
        }
    }
    return buffer[0..len];
}

fn exerciseGeneratedParserFuzzSql(alloc: std.mem.Allocator, sql: []const u8) !void {
    var tokens = lexer.tokenizeAlloc(alloc, sql) catch |err| switch (err) {
        error.UnsupportedSqlShape => return,
        else => return err,
    };
    defer lexer.freeTokens(alloc, &tokens);

    var parsed = parseTokensAlloc(alloc, tokens.items) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => {
            const diagnostic = diagnosticAlloc(alloc, tokens.items) catch |diagnostic_err| switch (diagnostic_err) {
                error.UnsupportedSqlShape => return,
                else => return diagnostic_err,
            } orelse return error.ExpectedDiagnostic;
            defer alloc.free(diagnostic.expected);
            try std.testing.expect(diagnostic.token_index <= tokens.items.len);
            try std.testing.expect(diagnostic.source_end >= diagnostic.source_start);
            try std.testing.expect(diagnostic.source_end <= sql.len);
            try std.testing.expect(diagnostic.expected.len > 0);
            return;
        },
        else => return err,
    };
    defer parsed.deinit(alloc);
}

test "generated SQL parser deterministic fuzz exercises scanner parser and diagnostics" {
    const seeds = [_][]const u8{
        "SELECT id, status FROM usage_records WHERE kind = 'order' ORDER BY id LIMIT 5",
        "WITH source_rows AS (SELECT id FROM usage_records WHERE status = 'open') SELECT id FROM source_rows",
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'open') ON CONFLICT (id) DO UPDATE SET status = excluded.status",
        "CREATE TABLE usage_records (id text PRIMARY KEY, status text)",
        "CREATE GRAPH METRIC docs_pagerank ON doc_edges",
        "EXPLAIN (FORMAT JSON, ANALYZE ON) SELECT id FROM usage_records",
    };

    var prng = std.Random.DefaultPrng.init(0x514c_f077);
    const random = prng.random();
    for (0..384) |case_index| {
        var buffer: [256]u8 = undefined;
        const sql = if (case_index % 3 == 0)
            generatedParserRandomFuzzSql(random, &buffer)
        else
            generatedParserMutatedFuzzSql(random, seeds[random.intRangeLessThan(usize, 0, seeds.len)], &buffer);
        try exerciseGeneratedParserFuzzSql(std.testing.allocator, sql);
    }
}

test "generated SQL parser rejects unsupported token shapes" {
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlAlloc(std.testing.allocator, "SELECT a ! b"));
}

fn spanText(sql: []const u8, span: token_mod.SourceSpan) []const u8 {
    return sql[span.start..span.end];
}

fn tokenRangeText(sql: []const u8, tokens: []const token_mod.Token, range: GeneratedSqlTokenRange) []const u8 {
    return sql[tokens[range.start].source_start..tokens[range.end - 1].source_end];
}
