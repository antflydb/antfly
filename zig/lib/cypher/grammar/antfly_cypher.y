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

%start query
%expect 0
%token IDENT STRING INTEGER NUMBER PARAMETER PATTERN_LBRACKET
%token LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE COMMA COLON DOT RANGE
%token PLUS MINUS STAR SLASH PERCENT CARET EQ NEQ LT LTE GT GTE REGEX PIPE SEMICOLON
%token MATCH OPTIONAL WHERE RETURN WITH DISTINCT AS ORDER BY ASC DESC SKIP LIMIT
%token AND OR XOR NOT IN IS NULL TRUE FALSE STARTS ENDS CONTAINS CASE WHEN THEN ELSE END
%token UNWIND CALL YIELD UNION ALL CREATE MERGE ON SET REMOVE DELETE DETACH
%token EXPLAIN PROFILE EXISTS REPEATABLE ELEMENTS DIFFERENT RELATIONSHIPS REDUCE

query: explain_opt clauses union_tail semicolon_opt ;
explain_opt: /* empty */ | EXPLAIN | PROFILE ;
semicolon_opt: /* empty */ | SEMICOLON ;
union_tail: /* empty */ | union_tail UNION all_opt clauses ;
all_opt: /* empty */ | ALL ;
clauses: clause | clauses clause ;
clause:
    optional_opt MATCH mode_opt paths where_opt
  | RETURN projection
  | WITH projection
  | UNWIND expression AS IDENT
  | CALL qualified_name LPAREN arguments_opt RPAREN yield_opt where_opt
  | CALL LBRACE clauses RBRACE
  | CREATE paths
  | MERGE path merge_actions
  | SET assignments
  | REMOVE expressions
  | detach_opt DELETE expressions
  ;
optional_opt: /* empty */ | OPTIONAL ;
mode_opt: /* empty */ | REPEATABLE ELEMENTS | DIFFERENT RELATIONSHIPS ;
detach_opt: /* empty */ | DETACH ;
where_opt: /* empty */ | WHERE expression ;
paths: path | paths COMMA path ;
path: node chain | IDENT EQ node chain | IDENT LPAREN node chain RPAREN | IDENT EQ IDENT LPAREN node chain RPAREN ;
chain: /* empty */ | chain relationship node ;
node: LPAREN alias_opt labels properties_opt RPAREN ;
alias_opt: /* empty */ | IDENT ;
labels: /* empty */ | labels COLON IDENT ;
properties_opt: /* empty */ | map ;
relationship: MINUS detail_opt MINUS | MINUS detail_opt MINUS GT | LT MINUS detail_opt MINUS ;
detail_opt: /* empty */ | LBRACKET alias_opt edge_types quantifier_opt properties_opt RBRACKET ;
edge_types: /* empty */ | COLON types ;
types: IDENT | types PIPE IDENT | types PIPE COLON IDENT ;
quantifier_opt: /* empty */ | STAR | STAR INTEGER | STAR INTEGER RANGE INTEGER | STAR RANGE INTEGER | STAR INTEGER RANGE ;
projection: distinct_opt projection_items order_opt skip_opt limit_opt where_opt ;
distinct_opt: /* empty */ | DISTINCT ;
projection_items: STAR | items | STAR COMMA items ;
items: item | items COMMA item ;
item: expression alias_projection_opt ;
alias_projection_opt: /* empty */ | AS IDENT ;
order_opt: /* empty */ | ORDER BY orders ;
orders: order | orders COMMA order ;
order: expression direction_opt ;
direction_opt: /* empty */ | ASC | DESC ;
skip_opt: /* empty */ | SKIP expression ;
limit_opt: /* empty */ | LIMIT expression ;
yield_opt: /* empty */ | YIELD yields ;
yields: IDENT alias_projection_opt | yields COMMA IDENT alias_projection_opt ;
merge_actions: /* empty */ | merge_actions ON CREATE SET assignments | merge_actions ON MATCH SET assignments ;
assignments: assignment | assignments COMMA assignment ;
assignment: postfix EQ expression | postfix PLUS EQ expression ;
arguments_opt: /* empty */ | expressions ;
expressions: expression | expressions COMMA expression ;
qualified_name: IDENT | qualified_name DOT IDENT ;
expression: disjunction ;
disjunction: exclusive | disjunction OR exclusive ;
exclusive: conjunction | exclusive XOR conjunction ;
conjunction: negation | conjunction AND negation ;
negation: comparison | NOT negation ;
comparison:
    scalar_comparison
  | additive IN additive | additive REGEX additive
  | additive STARTS WITH additive | additive ENDS WITH additive | additive CONTAINS additive
  | scalar_comparison IS NULL | scalar_comparison IS NOT NULL
  ;
scalar_comparison: additive | scalar_comparison EQ additive | scalar_comparison NEQ additive
  | scalar_comparison LT additive | scalar_comparison LTE additive
  | scalar_comparison GT additive | scalar_comparison GTE additive ;
additive: multiplicative | additive PLUS multiplicative | additive MINUS multiplicative ;
multiplicative: power | multiplicative STAR power | multiplicative SLASH power | multiplicative PERCENT power ;
power: unary | unary CARET power ;
unary: postfix | PLUS unary | MINUS unary ;
postfix: primary | postfix DOT IDENT | postfix LBRACKET subscript RBRACKET | postfix LBRACE map_selectors_opt RBRACE | postfix LPAREN call_arguments RPAREN ;
subscript: expression | expression RANGE expression_opt | RANGE expression ;
expression_opt: /* empty */ | expression ;
map_selectors_opt: /* empty */ | map_selectors ;
map_selectors: map_selector | map_selectors COMMA map_selector ;
map_selector: DOT IDENT | DOT STAR | IDENT | IDENT COLON expression ;
primary:
    IDENT | STRING | INTEGER | NUMBER | PARAMETER | TRUE | FALSE | NULL
  | LPAREN expression RPAREN
  | ALL LPAREN expression WHERE expression RPAREN
  | REDUCE LPAREN IDENT EQ expression COMMA IDENT IN expression PIPE expression RPAREN
  | PATTERN_LBRACKET path where_opt PIPE expression RBRACKET
  | LBRACKET arguments_opt RBRACKET
  | LBRACKET expression WHERE expression comprehension_projection RBRACKET
  | LBRACKET expression PIPE expression RBRACKET
  | map
  | CASE case_operand_opt branches else_opt END
  | EXISTS LBRACE clauses RBRACE
  ;
call_arguments: /* empty */ | STAR | expressions | DISTINCT expressions | expression WHERE expression ;
map: LBRACE entries_opt RBRACE ;
entries_opt: /* empty */ | entries ;
entries: entry | entries COMMA entry ;
entry: IDENT COLON expression | STRING COLON expression ;
case_operand_opt: /* empty */ | expression ;
branches: WHEN expression THEN expression | branches WHEN expression THEN expression ;
else_opt: /* empty */ | ELSE expression ;
comprehension_projection: /* empty */ | PIPE expression ;
