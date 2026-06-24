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

%start statement

%token IDENT STRING NUMBER PLACEHOLDER
%token COMMA DOT STAR SEMICOLON LPAREN RPAREN LBRACKET RBRACKET
%token EQ NEQ LT LTE GT GTE PLUS MINUS SLASH PERCENT COLON COLON_COLON
%token ARROW_JSON ARROW_TEXT PATH_ARROW_JSON PATH_ARROW_TEXT
%token ALL ALTER ANALYZE AND AS ASC BEGIN BY CASCADE COMMIT CONFLICT CONTINUE
%token CREATE DATABASE DEALLOCATE DEFAULT DELETE DESC DISCARD DO DROP EXECUTE
%token EXPLAIN EXISTS EXTENSION FALSE FROM FULL GRAPH GROUP HAVING IDENTITY IF INDEX INSERT INTO
%token JOIN KEY LATERAL LIMIT MATCHED MERGE METRIC NOT NULL ON OR ORDER PREPARE PRIMARY PUBLIC
%token NOTHING QUERY RESET RESTART RESTRICT RETURNING ROLLBACK SCHEMA SELECT SET SHOW TABLE TO TRUNCATE
%token THEN TRUE UPDATE USING VALUES WHEN WHERE WITH

statement:
    session_statement
  | transaction_statement
  | prepared_statement
  | ddl_statement
  | dml_statement
  | read_statement
  | graph_statement
  | unsupported_statement
  ;

session_statement:
    SET qualified_name EQ expression
  | SET qualified_name TO expression
  | SET qualified_name expression
  | RESET qualified_name
  | SHOW qualified_name
  | DISCARD ALL
  ;

transaction_statement:
    BEGIN
  | COMMIT
  | ROLLBACK
  ;

prepared_statement:
    PREPARE IDENT prepare_parameter_types_opt AS statement
  | EXECUTE IDENT argument_list_opt
  | DEALLOCATE IDENT
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
  | create_index_statement
  | create_extension_statement
  | alter_table_statement
  | drop_statement
  ;

create_database_statement:
    CREATE DATABASE if_not_exists_opt qualified_name
  ;

create_schema_statement:
    CREATE SCHEMA if_not_exists_opt qualified_name
  ;

create_table_statement:
    CREATE TABLE if_not_exists_opt qualified_name LPAREN column_definition_list RPAREN
  ;

create_index_statement:
    CREATE INDEX if_not_exists_opt IDENT ON qualified_name index_method_opt LPAREN index_element_list RPAREN index_options_opt
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
  | IDENT STRING
  ;

alter_table_statement:
    ALTER TABLE qualified_name unsupported_tail_opt
  ;

drop_statement:
    DROP TABLE if_exists_opt qualified_name drop_behavior_opt
  | DROP INDEX if_exists_opt qualified_name drop_behavior_opt
  | DROP SCHEMA if_exists_opt qualified_name drop_behavior_opt
  | DROP DATABASE if_exists_opt qualified_name drop_database_force_opt
  | DROP EXTENSION if_exists_opt qualified_name drop_behavior_opt
  ;

drop_database_force_opt:
    /* empty */
  | WITH LPAREN IDENT RPAREN
  ;

dml_statement:
    insert_statement
  | update_statement
  | delete_statement
  | truncate_statement
  | merge_statement
  ;

insert_statement:
    INSERT INTO qualified_name insert_columns_opt VALUES value_tuple_list conflict_clause_opt returning_clause_opt
  | INSERT INTO qualified_name DEFAULT VALUES conflict_clause_opt returning_clause_opt
  | INSERT INTO qualified_name insert_columns_opt read_statement conflict_clause_opt returning_clause_opt
  ;

update_statement:
    UPDATE qualified_name SET assignment_list where_clause_opt returning_clause_opt
  | UPDATE qualified_name SET assignment_list FROM table_reference_list where_clause_opt returning_clause_opt
  ;

delete_statement:
    DELETE FROM qualified_name using_clause_opt where_clause_opt returning_clause_opt
  ;

truncate_statement:
    TRUNCATE table_keyword_opt truncate_table_list truncate_identity_opt drop_behavior_opt
  ;

table_keyword_opt:
    /* empty */
  | TABLE
  ;

truncate_table_list:
    qualified_name
  | truncate_table_list COMMA qualified_name
  ;

truncate_identity_opt:
    /* empty */
  | RESTART IDENTITY
  | CONTINUE IDENTITY
  ;

merge_statement:
    MERGE INTO qualified_name USING table_reference ON expression merge_action_list
  ;

read_statement:
    select_statement
  | with_clause select_statement
  ;

select_statement:
    SELECT select_list from_clause_opt where_clause_opt group_by_clause_opt having_clause_opt order_by_clause_opt limit_clause_opt
  ;

graph_statement:
    CREATE GRAPH INDEX IDENT ON qualified_name graph_options_opt
  | CREATE GRAPH METRIC IDENT ON qualified_name graph_options_opt
  ;

unsupported_statement:
    ANALYZE unsupported_tail_opt
  | EXPLAIN statement
  ;

column_definition_list:
    column_definition
  | column_definition_list COMMA column_definition
  ;

column_definition:
    IDENT type_name column_constraint_list_opt
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
  | NOT NULL
  | DEFAULT expression
  ;

insert_columns_opt:
    /* empty */
  | LPAREN identifier_list RPAREN
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
  ;

select_list:
    STAR
  | select_item_list
  ;

select_item_list:
    select_item
  | select_item_list COMMA select_item
  ;

select_item:
    expression
  | expression AS IDENT
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
    qualified_name
  | qualified_name AS IDENT
  | table_reference JOIN table_reference ON expression
  | LATERAL LPAREN read_statement RPAREN AS IDENT
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
    expression
  | expression ASC
  | expression DESC
  ;

limit_clause_opt:
    /* empty */
  | LIMIT expression
  ;

returning_clause_opt:
    /* empty */
  | RETURNING select_list
  ;

conflict_clause_opt:
    /* empty */
  | ON CONFLICT conflict_target DO conflict_action
  ;

conflict_target:
    LPAREN identifier_list RPAREN
  ;

conflict_action:
    NOTHING
  | UPDATE SET assignment_list where_clause_opt
  ;

with_clause:
    WITH cte_list
  ;

cte_list:
    cte
  | cte_list COMMA cte
  ;

cte:
    IDENT AS LPAREN statement RPAREN
  ;

merge_action_list:
    merge_action
  | merge_action_list merge_action
  ;

merge_action:
    WHEN MATCHED THEN UPDATE SET assignment_list
  | WHEN MATCHED THEN DELETE
  | WHEN NOT MATCHED THEN INSERT insert_columns_opt VALUES value_tuple
  ;

index_method_opt:
    /* empty */
  | USING IDENT
  ;

index_element_list:
    index_element
  | index_element_list COMMA index_element
  ;

index_element:
    expression
  ;

index_options_opt:
    /* empty */
  | WITH LPAREN option_list RPAREN
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
    IDENT
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

argument_list_opt:
    /* empty */
  | LPAREN RPAREN
  | LPAREN expression_list RPAREN
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
    comparison_expression
  | and_expression AND comparison_expression
  ;

comparison_expression:
    additive_expression
  | additive_expression EQ additive_expression
  | additive_expression NEQ additive_expression
  | additive_expression LT additive_expression
  | additive_expression LTE additive_expression
  | additive_expression GT additive_expression
  | additive_expression GTE additive_expression
  ;

additive_expression:
    multiplicative_expression
  | additive_expression PLUS multiplicative_expression
  | additive_expression MINUS multiplicative_expression
  ;

multiplicative_expression:
    postfix_expression
  | multiplicative_expression STAR postfix_expression
  | multiplicative_expression SLASH postfix_expression
  | multiplicative_expression PERCENT postfix_expression
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
  | PLACEHOLDER
  | LPAREN expression RPAREN
  | qualified_name LPAREN argument_list_opt RPAREN
  ;

literal:
    STRING
  | NUMBER
  | TRUE
  | FALSE
  | NULL
  ;

qualified_name:
    IDENT
  | qualified_name DOT IDENT
  ;

identifier_list:
    IDENT
  | identifier_list COMMA IDENT
  ;

type_name:
    qualified_name
  ;

unsupported_tail_opt:
    /* empty */
  ;
