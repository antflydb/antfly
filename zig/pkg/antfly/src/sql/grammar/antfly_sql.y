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
//
// This is an Antfly-owned grammar seed. PostgreSQL and CockroachDB grammar
// behavior are compatibility references, but this file is not a vendored copy
// of PostgreSQL gram.y.

%reference postgres_major 19
%reference postgres_branch master
%reference postgres_commit 4cc02b80774ecdc4cf2a2d5df09c07df36d68ca5
%reference postgres_commit_date 2026-06-23
%reference postgres_gram_y https://github.com/postgres/postgres/blob/4cc02b80774ecdc4cf2a2d5df09c07df36d68ca5/src/backend/parser/gram.y
%reference postgres_scan_l https://github.com/postgres/postgres/blob/4cc02b80774ecdc4cf2a2d5df09c07df36d68ca5/src/backend/parser/scan.l
%reference cockroach_sql_y https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/parser/sql.y

%expect 10719

%start statement

%token IDENT STRING NUMBER PLACEHOLDER
%token COMMA DOT STAR SEMICOLON LPAREN RPAREN LBRACKET RBRACKET
%token EQ NEQ LT LTE GT GTE PLUS MINUS SLASH PERCENT PIPE_CONCAT COLON COLON_COLON
%token AT_CONTAINS RANGE_OVERLAP QUESTION QUESTION_ANY QUESTION_ALL
%token ARROW_JSON ARROW_TEXT PATH_ARROW_JSON PATH_ARROW_TEXT
%token REGEX_MATCH REGEX_IMATCH REGEX_NOT_MATCH REGEX_NOT_IMATCH
%token ACCESS ADD AGGREGATE ALL ALTER ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC AUTHORIZATION BEGIN BETWEEN BY CASCADE CALL CASE CAST CHECK CHECKPOINT CLOSE CLUSTER COLLATION COMMIT COMMENT CONCURRENTLY CONFLICT CONSTRAINT CONTINUE CURRENT CURRENT_DATE CURRENT_TIMESTAMP
%token CREATE COPY CROSS DATA DATABASE DATE DEALLOCATE DECLARE DEFAULT DELETE DESC DISCARD DISTINCT DO DOMAIN DROP EXECUTE
%token ELSE END ESCAPE EXPLAIN EXISTS EXTENSION EXTRACT FALSE FETCH FILTER FIRST FOLLOWING FOR FOREIGN FROM FULL FUNCTION GENERATED GRANT GRAPH GROUP HASH HAVING IDENTITY IF ILIKE INCLUDE IN INDEX INNER INSERT INTERVAL INTO IS
%token ISNULL JOIN KEY LABEL LAST LATERAL LEFT LIKE LIMIT LIST LISTEN LOAD LOCAL LOCK LOCKED MATCH MATCHED MATERIALIZED MERGE METHOD METRIC MOVE NATURAL NO NOT NULL NOTIFY NOTNULL NOWAIT NULLS OF ON OR ORDER OUTER OVER OVERLAY OWNED PARTITION PLACING POLICY POSITION PRECEDING PREPARE PRIMARY PUBLIC PUBLICATION IMPORT
%token NEXT NOTHING OFFSET ONLY OPERATOR OVERRIDING OVERLAPS PERIOD PORTION PREPARED PRIVILEGES PROCEDURE QUERY RANGE REASSIGN RECURSIVE REFRESH REINDEX RELEASE RENAME REPLACE RESET RESTART RESTRICT RETURN RETURNING REVOKE RIGHT ROLLBACK ROLE ROUTINE ROW ROWS RULE SAVEPOINT SCHEMA SECURITY SELECT SERVER SET SHARE SHOW SKIP SOME SUBSCRIPTION SYSTEM TABLE TEMP TEMPORARY TIMESTAMP TIMESTAMPTZ TO TRUNCATE
%token SEQUENCE STORED SUBSTRING SYMMETRIC TABLESPACE THEN TRUE TRIGGER UNION UNIQUE UNKNOWN UNLISTEN UNLOGGED UPDATE USER USING VACUUM VALUE VALUES VIEW VIRTUAL WHEN WHERE WINDOW WITH WITHIN WITHOUT
%token BINARY CURSOR HOLD SCROLL
%token ANALYZE_VERBOSE SYSTEM_TIME_FOR
%token CHARACTERISTICS COMMITTED CONSTRAINTS DEFERRABLE DEFERRED IMMEDIATE ISOLATION LEVEL READ REPEATABLE SERIALIZABLE SESSION START TRANSACTION UNCOMMITTED WORK WRITE
%token EXCEPT INTERSECT UNBOUNDED
%token ALWAYS BASE_WEIGHT FIELD FRESHNESS GRAPH_METRIC KEY KIND METRIC METRIC_FRESHNESS MISSING_SCORE NAME SOURCE SOURCES TYPE WEIGHT

statement:
    session_statement
  | transaction_statement
  | prepared_statement
  | prepared_transaction_statement
  | ddl_statement
  | dml_statement
  | read_statement
  | graph_statement
  | cursor_statement
  | unsupported_statement
  ;

session_statement:
    SET qualified_name EQ session_value
  | SET qualified_name TO session_value_list
  | SET qualified_name session_value_list
  | SET LOCAL qualified_name EQ session_value
  | SET LOCAL qualified_name TO session_value_list
  | SET LOCAL qualified_name session_value_list
  | SET SESSION qualified_name EQ session_value
  | SET SESSION qualified_name TO session_value_list
  | SET SESSION qualified_name session_value_list
  | SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
  | RESET qualified_name
  | RESET ALL
  | SHOW qualified_name
  | SHOW ALL
  | DISCARD ALL
  ;

session_value_list:
    session_value
  | session_value_list COMMA session_value
  ;

session_value:
    expression
  | ON
  ;

transaction_statement:
    SET TRANSACTION transaction_mode_list
  | SET CONSTRAINTS constraint_mode_target_list constraint_check_mode
  | START TRANSACTION start_transaction_tail_opt
  | BEGIN begin_transaction_tail_opt
  | COMMIT transaction_boundary_tail_opt
  | END transaction_boundary_tail_opt
  | ROLLBACK transaction_boundary_tail_opt
  | ROLLBACK TO savepoint_keyword_opt identifier_name
  | SAVEPOINT identifier_name
  | RELEASE savepoint_keyword_opt identifier_name
  ;

transaction_boundary_tail_opt:
    /* empty */
  | WORK
  | TRANSACTION
  ;

start_transaction_tail_opt:
    /* empty */
  | transaction_mode_list
  ;

begin_transaction_tail_opt:
    /* empty */
  | WORK
  | TRANSACTION
  | TRANSACTION transaction_mode_list
  | transaction_mode_list
  ;

transaction_mode_list:
    transaction_mode_item
  | transaction_mode_list COMMA transaction_mode_item
  | transaction_mode_list transaction_mode_item
  ;

transaction_mode_item:
    ISOLATION LEVEL transaction_isolation_level
  | READ ONLY
  | READ WRITE
  | DEFERRABLE
  | NOT DEFERRABLE
  ;

transaction_isolation_level:
    SERIALIZABLE
  | REPEATABLE READ
  | READ COMMITTED
  | READ UNCOMMITTED
  ;

constraint_mode_target_list:
    ALL
  | qualified_name_list
  ;

constraint_check_mode:
    DEFERRED
  | IMMEDIATE
  ;

savepoint_keyword_opt:
    /* empty */
  | SAVEPOINT
  ;

prepared_statement:
    PREPARE identifier_name prepare_parameter_types_opt AS statement
  | EXECUTE identifier_name execute_argument_list_opt
  | DEALLOCATE identifier_name
  | DEALLOCATE PREPARE identifier_name
  | DEALLOCATE ALL
  ;

prepared_transaction_statement:
    PREPARE TRANSACTION STRING
  | COMMIT PREPARED STRING
  | ROLLBACK PREPARED STRING
  ;

prepare_parameter_types_opt:
    /* empty */
  | LPAREN type_name_list RPAREN
  ;

type_name_list:
    type_name
  | type_name_list COMMA type_name
  ;

ddl_statement:
    create_database_statement
  | create_schema_statement
  | create_table_statement
  | create_view_statement
  | create_materialized_view_statement
  | create_domain_statement
  | create_sequence_statement
  | create_type_statement
  | create_tablespace_statement
  | create_publication_statement
  | create_subscription_statement
  | create_policy_statement
  | create_routine_statement
  | create_role_statement
  | create_type_system_statement
  | create_index_statement
  | create_extension_statement
  | alter_table_statement
  | alter_database_statement
  | alter_extension_statement
  | alter_view_statement
  | alter_domain_statement
  | alter_sequence_statement
  | alter_type_statement
  | alter_schema_statement
  | alter_tablespace_statement
  | alter_publication_statement
  | alter_subscription_statement
  | alter_policy_statement
  | alter_role_statement
  | alter_type_system_statement
  | drop_statement
  | refresh_materialized_view_statement
  | relation_population_statement
  ;

create_database_statement:
    CREATE DATABASE if_not_exists_opt qualified_name
  ;

create_schema_statement:
    CREATE SCHEMA if_not_exists_opt qualified_name
  ;

create_table_statement:
    CREATE relation_lifetime_opt TABLE index_target_relation_prefix_opt if_not_exists_opt qualified_name create_table_body create_table_partition_opt create_table_option_list_opt
  | CREATE OR REPLACE relation_lifetime_opt TABLE index_target_relation_prefix_opt if_not_exists_opt qualified_name create_table_body create_table_partition_opt create_table_option_list_opt
  ;

create_table_body:
    LPAREN column_definition_list RPAREN
  | LPAREN RPAREN
  | PARTITION OF qualified_name table_partition_bound
  | AS read_statement create_table_as_data_opt
  ;

create_table_partition_opt:
    /* empty */
  | PARTITION BY table_partition_strategy LPAREN expression_list RPAREN
  ;

table_partition_strategy:
    RANGE
  | LIST
  | HASH
  ;

table_partition_bound:
    FOR VALUES FROM LPAREN expression_list RPAREN TO LPAREN expression_list RPAREN
  | FOR VALUES IN LPAREN expression_list RPAREN
  | DEFAULT
  ;

create_table_option_list_opt:
    /* empty */
  | create_table_option_list
  ;

create_table_option_list:
    create_table_option
  | create_table_option_list create_table_option
  ;

create_table_option:
    WITH SYSTEM IDENT
  | WITH LPAREN create_table_storage_parameter_list RPAREN
  | IDENT SCHEMA identifier_name AS IDENT STRING
  ;

create_table_storage_parameter_list:
    create_table_storage_parameter
  | create_table_storage_parameter_list COMMA create_table_storage_parameter
  ;

create_table_storage_parameter:
    qualified_name EQ STRING
  ;

create_view_statement:
    CREATE create_view_replace_opt VIEW if_not_exists_opt qualified_name view_column_list_opt AS read_statement
  ;

create_materialized_view_statement:
    CREATE create_view_replace_opt MATERIALIZED VIEW if_not_exists_opt qualified_name diagnostic_tail
  ;

create_view_replace_opt:
    /* empty */
  | OR REPLACE
  ;

view_column_list_opt:
    /* empty */
  | LPAREN identifier_list RPAREN
  ;

create_domain_statement:
    CREATE DOMAIN qualified_name AS diagnostic_tail
  ;

create_sequence_statement:
    CREATE SEQUENCE if_not_exists_opt qualified_name sequence_options_opt
  ;

create_type_statement:
    CREATE TYPE qualified_name AS diagnostic_tail
  ;

create_tablespace_statement:
    CREATE TABLESPACE qualified_name diagnostic_tail
  ;

create_publication_statement:
    CREATE PUBLICATION qualified_name diagnostic_tail
  ;

create_subscription_statement:
    CREATE SUBSCRIPTION qualified_name diagnostic_tail
  ;

create_policy_statement:
    CREATE POLICY identifier_name ON qualified_name policy_tail_opt
  ;

policy_tail_opt:
    /* empty */
  | policy_tail
  ;

policy_tail:
    policy_to_opt policy_using_opt policy_with_check_opt
  ;

policy_to_opt:
    /* empty */
  | TO qualified_name_list
  ;

policy_using_opt:
    /* empty */
  | USING LPAREN expression RPAREN
  ;

policy_with_check_opt:
    /* empty */
  | WITH CHECK LPAREN expression RPAREN
  ;

create_routine_statement:
    CREATE create_routine_replace_opt routine_kind qualified_name LPAREN routine_parameter_list_opt RPAREN diagnostic_tail_opt
  ;

create_role_statement:
    CREATE role_keyword identifier_name diagnostic_tail_opt
  ;

create_type_system_statement:
    CREATE COLLATION identifier_name diagnostic_tail_opt
  | CREATE OPERATOR operator_symbol diagnostic_tail_opt
  | CREATE AGGREGATE identifier_name diagnostic_tail
  | CREATE CAST diagnostic_tail
  ;

operator_symbol:
    EQ
  | NEQ
  | LT
  | LTE
  | GT
  | GTE
  | PLUS
  | MINUS
  | STAR
  | SLASH
  | PERCENT
  | PIPE_CONCAT
  | AT_CONTAINS
  | RANGE_OVERLAP
  | QUESTION
  | QUESTION_ANY
  | QUESTION_ALL
  | REGEX_MATCH
  | REGEX_IMATCH
  | REGEX_NOT_MATCH
  | REGEX_NOT_IMATCH
  ;

create_routine_replace_opt:
    /* empty */
  | OR REPLACE
  ;

routine_kind:
    FUNCTION
  | PROCEDURE
  | ROUTINE
  ;

drop_routine_kind:
    FUNCTION
  | PROCEDURE
  ;

routine_parameter_list_opt:
    /* empty */
  | routine_parameter_list
  ;

routine_parameter_list:
    routine_parameter
  | routine_parameter_list COMMA routine_parameter
  ;

routine_parameter:
    type_name
  | identifier_name type_name
  ;

relation_population_statement:
    SELECT distinct_clause_opt select_list INTO relation_lifetime_opt table_keyword_opt qualified_name from_clause_opt where_clause_opt group_by_clause_opt having_clause_opt window_clause_opt order_by_clause_opt pagination_clause_list_opt
  ;

relation_lifetime_opt:
    /* empty */
  | TEMP
  | TEMPORARY
  | UNLOGGED
  ;

create_table_as_data_opt:
    /* empty */
  | WITH DATA
  | WITH NO DATA
  ;

create_index_statement:
    CREATE create_index_unique_opt INDEX if_not_exists_opt qualified_name ON index_target_relation_prefix_opt qualified_name index_method_opt LPAREN index_element_list_opt RPAREN index_nulls_distinct_opt index_include_opt index_options_opt index_where_opt
  | CREATE create_index_unique_opt INDEX CONCURRENTLY if_not_exists_opt qualified_name ON index_target_relation_prefix_opt qualified_name index_method_opt LPAREN index_element_list_opt RPAREN index_nulls_distinct_opt index_include_opt index_options_opt index_where_opt
  ;

create_index_unique_opt:
    /* empty */
  | UNIQUE
  ;

index_nulls_distinct_opt:
    /* empty */
  | NULLS DISTINCT
  | NULLS NOT DISTINCT
  ;

index_target_relation_prefix_opt:
    /* empty */
  | ONLY
  ;

create_extension_statement:
    CREATE EXTENSION if_not_exists_opt qualified_name extension_schema_opt extension_version_opt
  ;

extension_schema_opt:
    /* empty */
  | WITH SCHEMA qualified_name
  | SCHEMA qualified_name
  ;

extension_version_opt:
    /* empty */
  | identifier_name STRING
  ;

alter_table_statement:
    ALTER TABLE alter_table_relation_prefix_opt qualified_name alter_table_partition_action
  | ALTER TABLE alter_table_relation_prefix_opt qualified_name ADD table_constraint table_constraint_attribute_list_opt
  | ALTER TABLE alter_table_relation_prefix_opt qualified_name diagnostic_tail_opt
  ;

alter_table_partition_action:
    IDENT PARTITION qualified_name table_partition_bound
  | IDENT PARTITION qualified_name
  ;

alter_database_statement:
    ALTER DATABASE qualified_name diagnostic_tail
  ;

alter_extension_statement:
    ALTER EXTENSION qualified_name diagnostic_tail
  ;

alter_view_statement:
    ALTER VIEW qualified_name RENAME TO qualified_name
  ;

alter_domain_statement:
    ALTER DOMAIN qualified_name diagnostic_tail
  ;

alter_sequence_statement:
    ALTER SEQUENCE if_exists_opt qualified_name diagnostic_tail
  ;

sequence_options_opt:
    /* empty */
  | sequence_option_list
  ;

sequence_option_list:
    sequence_option
  | sequence_option_list sequence_option
  ;

sequence_option:
    AS type_name
  | START WITH expression
  | identifier_name BY expression
  | identifier_name expression
  | NO identifier_name
  | OWNED BY qualified_name
  ;

alter_type_statement:
    ALTER TYPE qualified_name diagnostic_tail
  ;

alter_schema_statement:
    ALTER SCHEMA qualified_name RENAME TO qualified_name
  ;

alter_tablespace_statement:
    ALTER TABLESPACE qualified_name RENAME TO qualified_name
  ;

alter_publication_statement:
    ALTER PUBLICATION qualified_name diagnostic_tail
  ;

alter_subscription_statement:
    ALTER SUBSCRIPTION qualified_name diagnostic_tail
  ;

alter_policy_statement:
    ALTER POLICY identifier_name ON qualified_name diagnostic_tail_opt
  ;

alter_role_statement:
    ALTER role_keyword identifier_name alter_role_database_opt diagnostic_tail
  ;

alter_role_database_opt:
    /* empty */
  | IN DATABASE identifier_name
  ;

alter_type_system_statement:
    ALTER COLLATION identifier_name RENAME TO identifier_name
  ;

alter_table_relation_prefix_opt:
    /* empty */
  | IF EXISTS
  | ONLY
  | IF EXISTS ONLY
  ;

drop_statement:
    DROP TABLE if_exists_opt qualified_name drop_behavior_opt
  | DROP INDEX if_exists_opt qualified_name drop_behavior_opt
  | DROP INDEX CONCURRENTLY if_exists_opt qualified_name drop_behavior_opt
  | DROP SCHEMA if_exists_opt qualified_name drop_behavior_opt
  | DROP DATABASE if_exists_opt qualified_name drop_database_force_opt
  | DROP EXTENSION if_exists_opt qualified_name drop_behavior_opt
  | DROP VIEW if_exists_opt qualified_name drop_behavior_opt
  | DROP MATERIALIZED VIEW if_exists_opt qualified_name drop_behavior_opt
  | DROP DOMAIN if_exists_opt qualified_name drop_behavior_opt
  | DROP SEQUENCE if_exists_opt qualified_name drop_behavior_opt
  | DROP TYPE if_exists_opt qualified_name drop_behavior_opt
  | DROP TABLESPACE if_exists_opt qualified_name
  | DROP PUBLICATION if_exists_opt qualified_name
  | DROP SUBSCRIPTION if_exists_opt qualified_name
  | DROP POLICY if_exists_opt identifier_name ON qualified_name drop_behavior_opt
  | DROP drop_routine_kind if_exists_opt qualified_name LPAREN routine_type_list_opt RPAREN drop_behavior_opt
  | DROP role_keyword if_exists_opt identifier_name drop_behavior_opt
  | DROP COLLATION if_exists_opt identifier_name
  | DROP OPERATOR operator_symbol diagnostic_tail_opt
  | DROP OPERATOR IF EXISTS operator_symbol diagnostic_tail_opt
  | DROP AGGREGATE identifier_name diagnostic_tail
  | DROP AGGREGATE IF EXISTS identifier_name diagnostic_tail
  | DROP CAST diagnostic_tail
  | DROP CAST IF EXISTS diagnostic_tail
  ;

role_keyword:
    ROLE
  | GROUP
  ;

refresh_materialized_view_statement:
    REFRESH MATERIALIZED VIEW diagnostic_tail
  ;

drop_database_force_opt:
    /* empty */
  | WITH LPAREN identifier_name RPAREN
  ;

routine_type_list_opt:
    /* empty */
  | type_name_list
  ;

dml_statement:
    insert_statement
  | with_clause insert_statement
  | update_statement
  | with_clause update_statement
  | delete_statement
  | with_clause delete_statement
  | truncate_statement
  | merge_statement
  | with_clause merge_statement
  ;

insert_statement:
    INSERT INTO insert_target insert_columns_opt VALUES value_tuple_list conflict_clause_opt returning_clause_opt
  | INSERT INTO insert_target DEFAULT VALUES conflict_clause_opt returning_clause_opt
  | INSERT INTO insert_target insert_columns_opt read_statement conflict_clause_opt returning_clause_opt
  ;

insert_target:
    qualified_name
  | qualified_name AS identifier_name
  | ONLY qualified_name
  | ONLY qualified_name AS identifier_name
  ;

update_statement:
    UPDATE update_target update_for_portion_opt SET assignment_list update_from_clause_opt where_clause_opt order_by_clause_opt pagination_clause_list_opt row_lock_clause_opt returning_clause_opt
  ;

update_target:
    qualified_name table_alias_opt
  | ONLY qualified_name table_alias_opt
  ;

delete_statement:
    DELETE FROM delete_target delete_for_portion_opt using_clause_opt where_clause_opt order_by_clause_opt pagination_clause_list_opt row_lock_clause_opt returning_clause_opt
  ;

delete_target:
    qualified_name table_alias_opt
  | ONLY qualified_name table_alias_opt
  ;

update_from_clause_opt:
    /* empty */
  | FROM table_reference_list
  ;

update_for_portion_opt:
    /* empty */
  | FOR PORTION OF identifier_name FROM expression TO expression
  ;

delete_for_portion_opt:
    /* empty */
  | FOR PORTION OF identifier_name FROM expression TO expression
  ;

truncate_statement:
    TRUNCATE table_keyword_opt truncate_table_list truncate_identity_opt drop_behavior_opt
  ;

table_keyword_opt:
    /* empty */
  | TABLE
  ;

truncate_table_list:
    truncate_table_item
  | truncate_table_list COMMA truncate_table_item
  ;

truncate_table_item:
    qualified_name
  | ONLY qualified_name
  ;

truncate_identity_opt:
    /* empty */
  | RESTART IDENTITY
  | CONTINUE IDENTITY
  ;

merge_statement:
    MERGE INTO merge_target table_alias_opt USING table_reference ON expression merge_action_list returning_clause_opt
  ;

merge_target:
    qualified_name
  | ONLY qualified_name
  ;

table_alias_opt:
    /* empty */
  | AS identifier_name
  | identifier_name
  ;

read_statement:
    select_set_statement
  | with_clause select_set_statement
  ;

select_set_statement:
    select_statement
  | select_statement set_operation select_statement
  | select_set_statement set_operation select_statement
  ;

set_operation:
    UNION all_opt
  | INTERSECT
  | EXCEPT
  ;

all_opt:
    /* empty */
  | ALL
  ;

select_statement:
    SELECT distinct_clause_opt select_list from_clause_opt where_clause_opt group_by_clause_opt having_clause_opt window_clause_opt order_by_clause_opt pagination_clause_list_opt row_lock_clause_opt
  ;

distinct_clause_opt:
    /* empty */
  | ALL
  | DISTINCT
  | DISTINCT ON LPAREN expression_list RPAREN
  ;

row_lock_clause_opt:
    /* empty */
  | row_lock_clause
  ;

row_lock_clause:
    FOR row_lock_strength row_lock_target_opt row_lock_wait_policy_opt
  ;

row_lock_strength:
    UPDATE
  | SHARE
  | NO KEY UPDATE
  | KEY SHARE
  ;

row_lock_target_opt:
    /* empty */
  | OF qualified_name_list
  ;

row_lock_wait_policy_opt:
    /* empty */
  | NOWAIT
  | SKIP LOCKED
  ;

graph_statement:
    CREATE GRAPH INDEX if_not_exists_opt identifier_name ON qualified_name diagnostic_tail_opt
  | CREATE GRAPH METRIC identifier_name ON qualified_name diagnostic_tail_opt
  | ALTER GRAPH INDEX identifier_name diagnostic_tail_opt
  ;

cursor_statement:
    CLOSE diagnostic_tail
  | DECLARE identifier_name declare_cursor_options_opt CURSOR declare_cursor_hold_opt FOR cursor_subject_statement
  | DECLARE diagnostic_tail
  | FETCH diagnostic_tail
  | MOVE diagnostic_tail
  ;

cursor_subject_statement:
    read_statement
  | dml_statement
  ;

declare_cursor_options_opt:
    /* empty */
  | declare_cursor_options
  ;

declare_cursor_options:
    declare_cursor_option
  | declare_cursor_options declare_cursor_option
  ;

declare_cursor_option:
    BINARY
  | SCROLL
  | NO SCROLL
  ;

declare_cursor_hold_opt:
    /* empty */
  | WITH HOLD
  | WITHOUT HOLD
  ;

unsupported_statement:
    ANALYZE
  | ANALYZE ANALYZE_VERBOSE
  | ANALYZE qualified_name analyze_column_list_opt
  | ANALYZE ANALYZE_VERBOSE qualified_name analyze_column_list_opt
  | EXPLAIN explain_options_opt explain_subject_opt
  | DO diagnostic_tail_opt
  | INSERT INTO insert_target insert_columns_opt OVERRIDING SYSTEM VALUE insert_overriding_body
  | INSERT INTO insert_target insert_columns_opt OVERRIDING USER VALUE insert_overriding_body
  | CREATE DATABASE if_not_exists_opt qualified_name diagnostic_tail
  | CREATE SCHEMA if_not_exists_opt qualified_name diagnostic_tail
  | CREATE ACCESS METHOD diagnostic_tail_opt
  | CREATE FOREIGN DATA identifier_name diagnostic_tail_opt
  | CREATE FOREIGN TABLE diagnostic_tail_opt
  | CREATE IDENT TRIGGER diagnostic_tail_opt
  | CREATE IDENT diagnostic_tail_opt
  | CREATE IDENT IDENT diagnostic_tail_opt
  | CREATE OPERATOR IDENT diagnostic_tail_opt
  | CREATE RULE diagnostic_tail_opt
  | CREATE SERVER diagnostic_tail_opt
  | CREATE OR REPLACE TRIGGER trigger_diagnostic_tail_opt
  | CREATE TRIGGER trigger_diagnostic_tail_opt
  | CREATE USER diagnostic_tail_opt
  | ALTER FOREIGN DATA identifier_name diagnostic_tail_opt
  | ALTER DEFAULT PRIVILEGES diagnostic_tail_opt
  | ALTER FOREIGN TABLE diagnostic_tail_opt
  | ALTER AGGREGATE diagnostic_tail
  | ALTER identifier_name FOR diagnostic_tail
  | ALTER INDEX diagnostic_tail_opt
  | ALTER identifier_name TRIGGER diagnostic_tail_opt
  | ALTER identifier_name identifier_name diagnostic_tail_opt
  | ALTER MATERIALIZED VIEW diagnostic_tail_opt
  | ALTER OPERATOR diagnostic_tail
  | ALTER RULE diagnostic_tail_opt
  | ALTER SERVER diagnostic_tail_opt
  | ALTER SYSTEM diagnostic_tail_opt
  | ALTER TRIGGER diagnostic_tail_opt
  | DROP ACCESS METHOD diagnostic_tail_opt
  | DROP FOREIGN DATA identifier_name diagnostic_tail_opt
  | DROP FOREIGN TABLE diagnostic_tail_opt
  | DROP TABLE if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP INDEX if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP INDEX CONCURRENTLY if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP EXTENSION if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP VIEW if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP MATERIALIZED VIEW if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP DOMAIN if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP SEQUENCE if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP TYPE if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP PUBLICATION if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP role_keyword if_exists_opt identifier_name COMMA diagnostic_tail
  | DROP COLLATION if_exists_opt identifier_name COMMA diagnostic_tail
  | DROP OPERATOR IDENT diagnostic_tail_opt
  | DROP identifier_name TRIGGER diagnostic_tail_opt
  | DROP identifier_name diagnostic_tail_opt
  | DROP identifier_name identifier_name diagnostic_tail_opt
  | DROP OWNED diagnostic_tail_opt
  | DROP SCHEMA if_exists_opt qualified_name COMMA diagnostic_tail
  | DROP RULE diagnostic_tail_opt
  | DROP ROUTINE diagnostic_tail_opt
  | DROP SERVER diagnostic_tail_opt
  | DROP TRIGGER diagnostic_tail_opt
  | IMPORT FOREIGN SCHEMA identifier_name FROM SERVER identifier_name INTO identifier_name diagnostic_tail_opt
  | CALL diagnostic_tail_opt
  | CHECKPOINT diagnostic_tail_opt
  | COPY
  | COPY diagnostic_tail
  | CLUSTER diagnostic_tail_opt
  | COMMENT diagnostic_tail_opt
  | DISCARD discard_unsupported_target
  | GRANT diagnostic_tail_opt
  | LISTEN diagnostic_tail_opt
  | LOAD diagnostic_tail_opt
  | LOCK diagnostic_tail_opt
  | MATCH graph_path graph_where_opt RETURN graph_match_tail
  | MATCH graph_path graph_source_binding graph_where_opt RETURN graph_match_tail
  | NOTIFY diagnostic_tail_opt
  | SET ROLE diagnostic_tail_opt
  | SET SESSION AUTHORIZATION diagnostic_tail_opt
  | VACUUM diagnostic_tail_opt
  | REINDEX diagnostic_tail_opt
  | REASSIGN OWNED diagnostic_tail_opt
  | RESET ROLE
  | RESET SESSION AUTHORIZATION
  | REVOKE diagnostic_tail_opt
  | SECURITY diagnostic_tail_opt
  | UNLISTEN diagnostic_tail_opt
  ;

graph_match_tail:
    graph_match_token
  | graph_match_tail graph_match_token
  ;

graph_path:
    graph_node
  | graph_node graph_out_relationship graph_node
  | graph_node graph_in_relationship graph_node
  ;

graph_node:
    LPAREN identifier_name RPAREN
  ;

graph_out_relationship:
    MINUS LBRACKET graph_relationship_label_opt RBRACKET ARROW_JSON
  ;

graph_in_relationship:
    LT MINUS LBRACKET graph_relationship_label_opt RBRACKET MINUS
  ;

graph_relationship_label_opt:
    /* empty */
  | COLON identifier_name
  ;

graph_source_binding:
    WITH GRAPH graph_binding_value ON graph_binding_value START graph_binding_value
  ;

graph_binding_value:
    IDENT
  | STRING
  ;

graph_where_opt:
    /* empty */
  | WHERE graph_match_tail
  ;

graph_match_token:
    IDENT
  | STRING
  | NUMBER
  | PLACEHOLDER
  | COMMA
  | DOT
  | STAR
  | EQ
  | NEQ
  | LT
  | LTE
  | GT
  | GTE
  | PLUS
  | MINUS
  | SLASH
  | PERCENT
  | COLON
  | COLON_COLON
  | LPAREN
  | RPAREN
  | LBRACKET
  | RBRACKET
  | ARROW_JSON
  | ARROW_TEXT
  | PATH_ARROW_JSON
  | PATH_ARROW_TEXT
  | PIPE_CONCAT
  | AT_CONTAINS
  | RANGE_OVERLAP
  | QUESTION
  | QUESTION_ANY
  | QUESTION_ALL
  | REGEX_MATCH
  | REGEX_IMATCH
  | REGEX_NOT_MATCH
  | REGEX_NOT_IMATCH
  | ACCESS
  | ADD
  | AGGREGATE
  | ALL
  | ALTER
  | ANALYZE
  | AND
  | AS
  | ASC
  | BY
  | CASCADE
  | CALL
  | CHECK
  | CHECKPOINT
  | CLOSE
  | CLUSTER
  | COLLATION
  | COMMENT
  | CONSTRAINT
  | COPY
  | DATA
  | DATABASE
  | DATE
  | DECLARE
  | DEFAULT
  | DELETE
  | DESC
  | DO
  | DOMAIN
  | DROP
  | EXECUTE
  | EXISTS
  | FALSE
  | EXTENSION
  | FETCH
  | FIRST
  | FOR
  | FOREIGN
  | FROM
  | FULL
  | FUNCTION
  | GENERATED
  | GRAPH
  | GRANT
  | GROUP
  | IF
  | INCLUDE
  | IN
  | INDEX
  | INSERT
  | IS
  | LABEL
  | LAST
  | LEFT
  | LIMIT
  | LISTEN
  | LOAD
  | LOCK
  | MATCH
  | METHOD
  | MOVE
  | NAME
  | NEXT
  | NO
  | NOT
  | NOTIFY
  | NULL
  | OF
  | OFFSET
  | ON
  | OR
  | ORDER
  | POLICY
  | PREPARE
  | PRIVILEGES
  | PROCEDURE
  | PUBLICATION
  | QUERY
  | RANGE
  | REASSIGN
  | REINDEX
  | RELEASE
  | RENAME
  | RESET
  | RESTRICT
  | REVOKE
  | ROLE
  | ROUTINE
  | RULE
  | SAVEPOINT
  | SCHEMA
  | SECURITY
  | SELECT
  | SEQUENCE
  | SERVER
  | SET
  | SHARE
  | SHOW
  | SKIP
  | SUBSCRIPTION
  | TABLE
  | TABLESPACE
  | TEMP
  | TEMPORARY
  | TIMESTAMP
  | TIMESTAMPTZ
  | TO
  | TRIGGER
  | TRUE
  | TYPE
  | UNLISTEN
  | UPDATE
  | USER
  | VACUUM
  | VALUE
  | VIEW
  | WHERE
  | WITH
  ;

insert_overriding_body:
    VALUES value_tuple_list conflict_clause_opt returning_clause_opt
  | DEFAULT VALUES conflict_clause_opt returning_clause_opt
  | read_statement conflict_clause_opt returning_clause_opt
  ;

explain_options_opt:
    /* empty */
  | ANALYZE
  | LPAREN explain_option_list RPAREN
  ;

discard_unsupported_target:
    identifier_name
  | TEMP
  | TEMPORARY
  ;

explain_option_list:
    explain_option
  | explain_option_list COMMA explain_option
  ;

explain_option:
    explain_option_name explain_option_value_opt
  ;

explain_option_name:
    identifier_name
  | ANALYZE
  ;

explain_option_value_opt:
    /* empty */
  | explain_option_value
  ;

explain_option_value:
    IDENT
  | ON
  | TRUE
  | FALSE
  | NUMBER
  | STRING
  ;

explain_subject_opt:
    /* empty */
  | statement
  ;

column_definition_list:
    table_definition
  | column_definition_list COMMA table_definition
  ;

table_definition:
    column_definition
  | table_constraint
  | table_constraint table_constraint_attribute_list
  | period_table_constraint
  | LIKE qualified_name table_like_options_opt
  ;

table_like_options_opt:
    /* empty */
  | table_like_options
  ;

table_like_options:
    table_like_option
  | table_like_options table_like_option
  ;

table_like_option:
    IDENT ALL
  | IDENT IDENT
  | IDENT UPDATE IDENT
  ;

column_definition:
    identifier_name type_name column_constraint_list_opt
  ;

table_constraint:
    PRIMARY KEY LPAREN constraint_key_list RPAREN index_include_opt
  | UNIQUE index_nulls_distinct_opt LPAREN constraint_key_list RPAREN index_include_opt
  | FOREIGN KEY LPAREN referential_key_list RPAREN IDENT qualified_name LPAREN referential_key_list RPAREN foreign_key_match_opt foreign_key_action_list_opt
  | CHECK LPAREN expression RPAREN
  | CONSTRAINT identifier_name table_constraint
  ;

table_constraint_attribute_list_opt:
    /* empty */
  | table_constraint_attribute_list
  ;

table_constraint_attribute_list:
    table_constraint_attribute
  | table_constraint_attribute_list table_constraint_attribute
  ;

table_constraint_attribute:
    NOT IDENT
  | IDENT
  | IDENT IDENT
  ;

constraint_key_list:
    constraint_key
  | constraint_key_list COMMA constraint_key
  ;

constraint_key:
    identifier_name constraint_key_temporal_opt
  ;

constraint_key_temporal_opt:
    /* empty */
  | WITHOUT OVERLAPS
  ;

referential_key_list:
    referential_key
  | referential_key_list COMMA referential_key
  ;

referential_key:
    identifier_name
  | PERIOD identifier_name
  ;

foreign_key_match_opt:
    /* empty */
  | MATCH FULL
  | MATCH IDENT
  ;

foreign_key_action_list_opt:
    /* empty */
  | foreign_key_action_list
  ;

foreign_key_action_list:
    foreign_key_action
  | foreign_key_action_list foreign_key_action
  ;

foreign_key_action:
    ON DELETE foreign_key_action_kind
  | ON UPDATE foreign_key_action_kind
  ;

foreign_key_action_kind:
    CASCADE
  | RESTRICT
  | NO IDENT
  | SET NULL
  | SET DEFAULT
  ;

period_table_constraint:
    PERIOD FOR identifier_name LPAREN identifier_list RPAREN
  ;

column_constraint_list_opt:
    /* empty */
  | column_constraint_list
  ;

column_constraint_list:
    column_constraint
  | column_constraint_list column_constraint
  ;

column_constraint:
    PRIMARY KEY
  | UNIQUE
  | NULLS DISTINCT
  | NULLS NOT DISTINCT
  | INCLUDE LPAREN identifier_list RPAREN
  | table_constraint_attribute
  | NOT NULL
  | DEFAULT expression
  | CHECK LPAREN expression RPAREN
  | IDENT qualified_name LPAREN identifier_list RPAREN foreign_key_match_opt foreign_key_action_list_opt
  | CONSTRAINT identifier_name column_constraint
  | IDENT identifier_name
  | generated_column_constraint
  ;

generated_column_constraint:
    GENERATED ALWAYS AS LPAREN expression RPAREN generated_column_storage_opt
  | GENERATED ALWAYS AS IDENTITY identity_options_opt
  | GENERATED BY DEFAULT AS IDENTITY identity_options_opt
  ;

generated_column_storage_opt:
    /* empty */
  | STORED
  | VIRTUAL
  ;

identity_options_opt:
    /* empty */
  | LPAREN sequence_option_list RPAREN
  ;

insert_columns_opt:
    /* empty */
  | LPAREN qualified_name_list RPAREN
  ;

value_tuple_list:
    value_tuple
  | value_tuple_list COMMA value_tuple
  ;

value_tuple:
    LPAREN expression_list RPAREN
  ;

assignment_list:
    assignment
  | assignment_list COMMA assignment
  ;

assignment:
    qualified_name EQ expression
  | LPAREN identifier_list RPAREN EQ expression
  | LPAREN identifier_list RPAREN EQ LPAREN expression_list RPAREN
  | LPAREN identifier_list RPAREN EQ ROW LPAREN expression_list RPAREN
  ;

select_list:
    select_item_list
  ;

select_item_list:
    select_item
  | select_item_list COMMA select_item
  ;

select_item:
    expression
  | expression AS identifier_name
  | expression identifier_name
  | STAR
  | qualified_name DOT STAR
  | window_function_expression
  | window_function_expression AS identifier_name
  | window_function_expression identifier_name
  ;

window_function_expression:
    qualified_name LPAREN function_argument_list_opt RPAREN within_group_clause_opt filter_clause_opt OVER LPAREN window_definition RPAREN
  | qualified_name LPAREN function_argument_list_opt RPAREN within_group_clause_opt filter_clause_opt OVER identifier_name
  ;

window_definition:
    partition_by_clause_opt order_by_clause_opt window_frame_opt
  ;

partition_by_clause_opt:
    /* empty */
  | PARTITION BY expression_list
  ;

window_clause_opt:
    /* empty */
  | WINDOW named_window_list
  ;

named_window_list:
    named_window
  | named_window_list COMMA named_window
  ;

named_window:
    identifier_name AS LPAREN window_definition RPAREN
  ;

window_frame_opt:
    /* empty */
  | window_frame_unit window_frame_extent
  ;

window_frame_unit:
    ROWS
  | RANGE
  ;

window_frame_extent:
    window_frame_bound
  | BETWEEN window_frame_bound AND window_frame_bound
  ;

window_frame_bound:
    UNBOUNDED PRECEDING
  | UNBOUNDED FOLLOWING
  | CURRENT ROW
  | expression PRECEDING
  | expression FOLLOWING
  ;

from_clause_opt:
    /* empty */
  | FROM table_reference_list
  ;

table_reference_list:
    table_reference
  | table_reference_list COMMA table_reference
  ;

table_reference:
    table_relation_source system_time_as_of_opt
  | qualified_name LPAREN function_argument_list_opt RPAREN table_function_alias_opt
  | LATERAL qualified_name LPAREN function_argument_list_opt RPAREN table_function_alias_opt
  | table_reference qualified_join_operator table_reference join_condition
  | table_reference conditionless_join_operator table_reference
  | LATERAL LPAREN read_statement RPAREN AS identifier_name
  ;

table_relation_source:
    qualified_name table_alias_opt
  | ONLY qualified_name table_alias_opt
  ;

system_time_as_of_opt:
    /* empty */
  | SYSTEM_TIME_FOR IDENT AS OF system_time_as_of_value
  ;

system_time_as_of_value:
    NUMBER
  | STRING
  | DATE STRING
  | TIMESTAMP STRING
  | TIMESTAMPTZ STRING
  ;

table_function_alias_opt:
    /* empty */
  | AS identifier_name
  | identifier_name
  ;

join_condition:
    ON expression
  | USING LPAREN identifier_list RPAREN
  ;

qualified_join_operator:
    JOIN
  | INNER JOIN
  | LEFT JOIN
  | LEFT OUTER JOIN
  | RIGHT JOIN
  | RIGHT OUTER JOIN
  | FULL JOIN
  | FULL OUTER JOIN
  ;

conditionless_join_operator:
    CROSS JOIN
  | NATURAL JOIN
  ;

using_clause_opt:
    /* empty */
  | USING table_reference_list
  ;

where_clause_opt:
    /* empty */
  | WHERE expression
  ;

group_by_clause_opt:
    /* empty */
  | GROUP BY expression_list
  ;

having_clause_opt:
    /* empty */
  | HAVING expression
  ;

order_by_clause_opt:
    /* empty */
  | ORDER BY order_by_list
  ;

order_by_list:
    order_by_item
  | order_by_list COMMA order_by_item
  ;

order_by_item:
    expression order_modifier_opt nulls_order_opt
  ;

order_modifier_opt:
    /* empty */
  | ASC
  | DESC
  | USING comparison_operator
  ;

nulls_order_opt:
    /* empty */
  | NULLS FIRST
  | NULLS LAST
  ;

pagination_clause_list_opt:
    /* empty */
  | pagination_clause_list
  ;

pagination_clause_list:
    pagination_clause
  | pagination_clause_list pagination_clause
  ;

pagination_clause:
    LIMIT expression
  | LIMIT ALL
  | OFFSET NUMBER row_rows_opt
  | OFFSET expression row_rows_opt
  | OFFSET NUMBER row_rows_opt FETCH fetch_first_next fetch_count_opt row_rows ONLY
  | FETCH fetch_first_next fetch_count_opt row_rows ONLY
  ;

fetch_first_next:
    FIRST
  | NEXT
  ;

fetch_count_opt:
    /* empty */
  | expression
  ;

row_rows_opt:
    /* empty */
  | ROW
  | ROWS
  ;

row_rows:
    ROW
  | ROWS
  ;

returning_clause_opt:
    /* empty */
  | RETURNING select_list
  ;

conflict_clause_opt:
    /* empty */
  | ON CONFLICT conflict_target DO conflict_action
  | ON CONFLICT DO NOTHING
  ;

conflict_target:
    LPAREN expression_list RPAREN conflict_target_where_opt
  | ON CONSTRAINT identifier_name
  ;

conflict_target_where_opt:
    /* empty */
  | WHERE expression
  ;

conflict_action:
    NOTHING
  | UPDATE SET assignment_list where_clause_opt
  ;

with_clause:
    WITH recursive_opt cte_list
  ;

recursive_opt:
    /* empty */
  | RECURSIVE
  ;

cte_list:
    cte
  | cte_list COMMA cte
  ;

cte:
    identifier_name cte_column_aliases_opt AS cte_materialization_opt LPAREN statement RPAREN
  ;

cte_column_aliases_opt:
    /* empty */
  | LPAREN identifier_list RPAREN
  ;

cte_materialization_opt:
    /* empty */
  | MATERIALIZED
  | NOT MATERIALIZED
  ;

merge_action_list:
    merge_action
  | merge_action_list merge_action
  ;

merge_action:
    WHEN MATCHED merge_condition_opt THEN UPDATE SET assignment_list
  | WHEN MATCHED merge_condition_opt THEN DELETE
  | WHEN MATCHED merge_condition_opt THEN DO NOTHING
  | WHEN NOT MATCHED merge_condition_opt THEN INSERT insert_columns_opt VALUES value_tuple
  | WHEN NOT MATCHED merge_condition_opt THEN DO NOTHING
  ;

merge_condition_opt:
    /* empty */
  | AND expression
  ;

index_method_opt:
    /* empty */
  | USING identifier_name
  ;

index_element_list_opt:
    /* empty */
  | index_element_list
  ;

index_element_list:
    index_element
  | index_element_list COMMA index_element
  ;

index_element:
    expression index_element_opclass_opt index_element_temporal_opt order_modifier_opt nulls_order_opt
  ;

index_element_opclass_opt:
    /* empty */
  | identifier_name
  ;

index_element_temporal_opt:
    /* empty */
  | WITHOUT OVERLAPS
  ;

index_include_opt:
    /* empty */
  | INCLUDE LPAREN identifier_list RPAREN
  ;

index_options_opt:
    /* empty */
  | WITH LPAREN option_list RPAREN
  ;

index_where_opt:
    /* empty */
  | WHERE expression
  ;

graph_options_opt:
    /* empty */
  | WITH LPAREN option_list RPAREN
  ;

option_list:
    option
  | option_list COMMA option
  ;

option:
    option_name EQ expression
  ;

option_name:
    identifier_name
  | METRIC
  ;

drop_behavior_opt:
    /* empty */
  | CASCADE
  | RESTRICT
  ;

if_not_exists_opt:
    /* empty */
  | IF NOT EXISTS
  ;

if_exists_opt:
    /* empty */
  | IF EXISTS
  ;

execute_argument_list_opt:
    /* empty */
  | LPAREN RPAREN
  | LPAREN expression_list RPAREN
  ;

function_argument_list_opt:
    /* empty */
  | STAR
  | DISTINCT STAR
  | function_argument_list
  ;

function_argument_list:
    distinct_opt function_argument_item_list function_argument_order_opt
  ;

function_argument_item_list:
    function_argument_item
  | function_argument_item_list COMMA function_argument_item
  ;

function_argument_item:
    expression
  | function_argument_name function_named_arg_operator expression
  ;

function_argument_name:
    identifier_name
  | BASE_WEIGHT
  | FIELD
  | FRESHNESS
  | GRAPH
  | GRAPH_METRIC
  | INDEX
  | KEY
  | KIND
  | LIMIT
  | METRIC
  | METRIC_FRESHNESS
  | MISSING_SCORE
  | NAME
  | OFFSET
  | QUERY
  | RETURN
  | SOURCE
  | SOURCES
  | START
  | TABLE
  | TYPE
  | WEIGHT
  ;

function_named_arg_operator:
    EQ
  | EQ GT
  ;

distinct_opt:
    /* empty */
  | DISTINCT
  ;

function_argument_order_opt:
    /* empty */
  | ORDER BY order_by_list
  ;

array_element_list_opt:
    /* empty */
  | expression_list
  ;

expression_list:
    expression
  | expression_list COMMA expression
  ;

expression:
    or_expression
  ;

or_expression:
    and_expression
  | or_expression OR and_expression
  ;

and_expression:
    not_expression
  | and_expression AND not_expression
  ;

not_expression:
    comparison_expression
  | NOT not_expression
  ;

comparison_expression:
    concat_expression
  | concat_expression EQ concat_expression
  | concat_expression NEQ concat_expression
  | concat_expression LT concat_expression
  | concat_expression LTE concat_expression
  | concat_expression GT concat_expression
  | concat_expression GTE concat_expression
  | concat_expression AT_CONTAINS concat_expression
  | concat_expression RANGE_OVERLAP concat_expression
  | concat_expression QUESTION concat_expression
  | concat_expression QUESTION_ANY concat_expression
  | concat_expression QUESTION_ALL concat_expression
  | concat_expression REGEX_MATCH concat_expression
  | concat_expression REGEX_IMATCH concat_expression
  | concat_expression REGEX_NOT_MATCH concat_expression
  | concat_expression REGEX_NOT_IMATCH concat_expression
  | concat_expression LIKE concat_expression like_escape_opt
  | concat_expression ILIKE concat_expression like_escape_opt
  | concat_expression LIKE quantified_operator quantified_rhs
  | concat_expression ILIKE quantified_operator quantified_rhs
  | concat_expression IN in_rhs
  | concat_expression BETWEEN between_modifier_opt concat_expression AND concat_expression
  | concat_expression NOT LIKE concat_expression like_escape_opt
  | concat_expression NOT ILIKE concat_expression like_escape_opt
  | concat_expression NOT LIKE quantified_operator quantified_rhs
  | concat_expression NOT ILIKE quantified_operator quantified_rhs
  | concat_expression NOT IN in_rhs
  | concat_expression NOT BETWEEN between_modifier_opt concat_expression AND concat_expression
  | concat_expression comparison_operator quantified_operator quantified_rhs
  | concat_expression IS NULL
  | concat_expression IS NOT NULL
  | concat_expression ISNULL
  | concat_expression NOTNULL
  | concat_expression IS TRUE
  | concat_expression IS FALSE
  | concat_expression IS UNKNOWN
  | concat_expression IS NOT TRUE
  | concat_expression IS NOT FALSE
  | concat_expression IS NOT UNKNOWN
  | concat_expression IS DISTINCT FROM concat_expression
  | concat_expression IS NOT DISTINCT FROM concat_expression
  ;

like_escape_opt:
    /* empty */
  | ESCAPE concat_expression
  ;

between_modifier_opt:
    /* empty */
  | ASYMMETRIC
  | SYMMETRIC
  ;

comparison_operator:
    EQ
  | NEQ
  | LT
  | LTE
  | GT
  | GTE
  ;

quantified_operator:
    ANY
  | ALL
  | SOME
  ;

in_rhs:
    LPAREN expression_list RPAREN
  | LPAREN read_statement RPAREN
  ;

quantified_rhs:
    LPAREN expression_list RPAREN
  | LPAREN read_statement RPAREN
  ;

concat_expression:
    additive_expression
  | concat_expression PIPE_CONCAT additive_expression
  ;

additive_expression:
    multiplicative_expression
  | additive_expression PLUS multiplicative_expression
  | additive_expression MINUS multiplicative_expression
  ;

multiplicative_expression:
    unary_expression
  | multiplicative_expression STAR unary_expression
  | multiplicative_expression SLASH unary_expression
  | multiplicative_expression PERCENT unary_expression
  ;

unary_expression:
    postfix_expression
  | PLUS unary_expression
  | MINUS unary_expression
  ;

postfix_expression:
    primary_expression
  | postfix_expression ARROW_JSON primary_expression
  | postfix_expression ARROW_TEXT primary_expression
  | postfix_expression PATH_ARROW_JSON primary_expression
  | postfix_expression PATH_ARROW_TEXT primary_expression
  | postfix_expression COLON_COLON type_name
  ;

primary_expression:
    literal
  | qualified_name
  | DEFAULT
  | PLACEHOLDER
  | LPAREN expression COMMA expression_list RPAREN
  | LPAREN expression RPAREN
  | LPAREN read_statement RPAREN
  | ARRAY LBRACKET array_element_list_opt RBRACKET
  | LBRACKET array_element_list_opt RBRACKET
  | CAST LPAREN expression AS type_name RPAREN
  | CASE case_when_list case_else_opt END
  | EXISTS LPAREN read_statement RPAREN
  | EXTRACT LPAREN identifier_name FROM expression RPAREN
  | CURRENT_DATE
  | CURRENT_TIMESTAMP current_timestamp_precision_opt
  | SUBSTRING LPAREN expression FROM expression substring_for_clause_opt RPAREN
  | OVERLAY LPAREN expression PLACING expression FROM expression overlay_for_clause_opt RPAREN
  | POSITION LPAREN concat_expression IN expression RPAREN
  | LEFT LPAREN function_argument_list_opt RPAREN within_group_clause_opt filter_clause_opt
  | RIGHT LPAREN function_argument_list_opt RPAREN within_group_clause_opt filter_clause_opt
  | qualified_name LPAREN function_argument_list_opt RPAREN within_group_clause_opt filter_clause_opt
  ;

current_timestamp_precision_opt:
    /* empty */
  | LPAREN NUMBER RPAREN
  ;

overlay_for_clause_opt:
    /* empty */
  | FOR expression
  ;

substring_for_clause_opt:
    /* empty */
  | FOR expression
  ;

case_when_list:
    WHEN expression THEN expression
  | case_when_list WHEN expression THEN expression
  ;

case_else_opt:
    /* empty */
  | ELSE expression
  ;

within_group_clause_opt:
    /* empty */
  | WITHIN GROUP LPAREN ORDER BY order_by_list RPAREN
  ;

filter_clause_opt:
    /* empty */
  | FILTER LPAREN WHERE expression RPAREN
  ;

literal:
    STRING
  | NUMBER
  | INTERVAL STRING
  | DATE STRING
  | TIMESTAMP STRING
  | TIMESTAMPTZ STRING
  | TRUE
  | FALSE
  | NULL
  ;

qualified_name:
    identifier_name
  | qualified_name DOT identifier_name
  ;

qualified_name_list:
    qualified_name
  | qualified_name_list COMMA qualified_name
  ;

identifier_list:
    identifier_name
  | identifier_list COMMA identifier_name
  ;

identifier_name:
    IDENT
  | ACCESS
  | AGGREGATE
  | BASE_WEIGHT
  | CALL
  | CHECKPOINT
  | CLOSE
  | CLUSTER
  | COLLATION
  | COMMENT
  | DECLARE
  | FIELD
  | DOMAIN
  | FRESHNESS
  | FUNCTION
  | GRAPH_METRIC
  | GRANT
  | INCLUDE
  | KEY
  | KIND
  | LABEL
  | LISTEN
  | LOAD
  | LOCK
  | MATERIALIZED
  | METHOD
  | METRIC
  | METRIC_FRESHNESS
  | MISSING_SCORE
  | MOVE
  | NAME
  | NEXT
  | NOTIFY
  | OPERATOR
  | OVERRIDING
  | OWNED
  | PROCEDURE
  | REASSIGN
  | REFRESH
  | RELEASE
  | RENAME
  | REPLACE
  | RESET
  | RESTART
  | REVOKE
  | SAVEPOINT
  | SECURITY
  | SEQUENCE
  | TABLESPACE
  | SOURCE
  | SOURCES
  | SYSTEM
  | TYPE
  | UNLISTEN
  | USER
  | VALUE
  | VIEW
  | WEIGHT
  ;

type_name:
    qualified_name type_modifier_opt array_type_suffix_opt
  | type_keyword_name type_modifier_opt array_type_suffix_opt
  ;

type_modifier_opt:
    /* empty */
  | LPAREN expression_list RPAREN
  ;

type_keyword_name:
    DATE
  | TIMESTAMP timestamp_time_zone_opt
  | TIMESTAMPTZ
  ;

timestamp_time_zone_opt:
    /* empty */
  | WITH identifier_name identifier_name
  | WITHOUT identifier_name identifier_name
  ;

array_type_suffix_opt:
    /* empty */
  | LBRACKET RBRACKET
  ;

analyze_column_list_opt:
    /* empty */
  | LPAREN identifier_list RPAREN
  ;

diagnostic_tail_opt:
    /* empty */
  | diagnostic_tail
  ;

trigger_diagnostic_tail_opt:
    /* empty */
  | trigger_diagnostic_tail
  ;

trigger_diagnostic_tail:
    trigger_diagnostic_token
  | trigger_diagnostic_tail trigger_diagnostic_token
  ;

trigger_diagnostic_token:
    diagnostic_token
  | TRUNCATE
  ;

diagnostic_tail:
    diagnostic_token
  | diagnostic_tail diagnostic_token
  ;

diagnostic_token:
    IDENT
  | STRING
  | NUMBER
  | PLACEHOLDER
  | COMMA
  | DOT
  | STAR
  | EQ
  | NEQ
  | LT
  | LTE
  | GT
  | GTE
  | PLUS
  | MINUS
  | SLASH
  | PERCENT
  | COLON
  | COLON_COLON
  | LPAREN
  | RPAREN
  | LBRACKET
  | RBRACKET
  | ARROW_JSON
  | ARROW_TEXT
  | PATH_ARROW_JSON
  | PATH_ARROW_TEXT
  | PIPE_CONCAT
  | AT_CONTAINS
  | RANGE_OVERLAP
  | QUESTION
  | QUESTION_ANY
  | QUESTION_ALL
  | REGEX_MATCH
  | REGEX_IMATCH
  | REGEX_NOT_MATCH
  | REGEX_NOT_IMATCH
  | ACCESS
  | ADD
  | AGGREGATE
  | ALL
  | ALTER
  | ANALYZE
  | AS
  | BY
  | CASCADE
  | CALL
  | CHECK
  | CHECKPOINT
  | CLOSE
  | CLUSTER
  | COLLATION
  | COMMENT
  | CONSTRAINT
  | COPY
  | DATA
  | DATABASE
  | DATE
  | DECLARE
  | DEFAULT
  | DELETE
  | DISTINCT
  | DO
  | DOMAIN
  | DROP
  | EXECUTE
  | EXISTS
  | EXTENSION
  | FALSE
  | FETCH
  | FIRST
  | FOR
  | FOREIGN
  | FROM
  | FULL
  | FUNCTION
  | GENERATED
  | GRAPH
  | GRANT
  | GROUP
  | IF
  | INCLUDE
  | IN
  | INDEX
  | INSERT
  | IS
  | LABEL
  | LAST
  | KEY
  | LISTEN
  | LIMIT
  | LOAD
  | LOCK
  | LOCKED
  | MATERIALIZED
  | METHOD
  | METRIC
  | MOVE
  | NEXT
  | NO
  | NOT
  | NOWAIT
  | NULL
  | NOTIFY
  | OF
  | ON
  | ONLY
  | OR
  | OPERATOR
  | OVERRIDING
  | OVERLAPS
  | OWNED
  | PERIOD
  | POLICY
  | PRIMARY
  | PRIVILEGES
  | PROCEDURE
  | PUBLICATION
  | REASSIGN
  | REFRESH
  | RELEASE
  | RENAME
  | REPLACE
  | RESET
  | RESTRICT
  | RESTART
  | REVOKE
  | ROLE
  | ROW
  | RULE
  | SAVEPOINT
  | SCHEMA
  | SECURITY
  | SEQUENCE
  | SELECT
  | SERVER
  | SET
  | SHARE
  | SKIP
  | SOURCE
  | STORED
  | SUBSCRIPTION
  | SYSTEM
  | TABLE
  | TABLESPACE
  | TEMP
  | TEMPORARY
  | TIMESTAMP
  | TIMESTAMPTZ
  | TO
  | TRIGGER
  | TRUE
  | TYPE
  | UNIQUE
  | UNLISTEN
  | UNLOGGED
  | UPDATE
  | USER
  | USING
  | VACUUM
  | VALUE
  | VIEW
  | VIRTUAL
  | WEIGHT
  | WHERE
  | WITH
  | WITHOUT
  | ALWAYS
  ;
