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

const generated_parser = @import("generated_parser.zig");
const token_mod = @import("token.zig");

const TokenKind = token_mod.TokenKind;

pub fn generatedTokenRangeEqual(
    a: generated_parser.GeneratedSqlTokenRange,
    b: generated_parser.GeneratedSqlTokenRange,
) bool {
    return a.start == b.start and a.end == b.end;
}

pub fn generatedExpressionKindUsesOperatorPayload(kind: generated_parser.GeneratedSqlExpressionKind) bool {
    return switch (kind) {
        .comparison,
        .like,
        .ilike,
        .in_list,
        .between,
        .not_like,
        .not_ilike,
        .not_in_list,
        .not_between,
        .quantified_comparison,
        .exists_subquery,
        .not_exists_subquery,
        .is_null,
        .is_not_null,
        .is_true,
        .is_false,
        .is_unknown,
        .is_not_true,
        .is_not_false,
        .is_not_unknown,
        .is_distinct_from,
        .is_not_distinct_from,
        .logical_or,
        .logical_and,
        .logical_not,
        .unary_positive,
        .unary_negative,
        .additive,
        .subtractive,
        .multiplicative,
        .divisive,
        .modulo,
        .contains,
        .overlaps,
        .json_key_exists,
        .json_key_any,
        .json_key_all,
        .regex_match,
        .regex_imatch,
        .regex_not_match,
        .regex_not_imatch,
        .string_concat,
        .json_access,
        .json_text_access,
        .json_path_access,
        .json_path_text_access,
        => true,
        else => false,
    };
}

pub fn generatedExpressionKindAllowsEscapePayload(kind: generated_parser.GeneratedSqlExpressionKind) bool {
    return switch (kind) {
        .like, .ilike, .not_like, .not_ilike => true,
        else => false,
    };
}

pub fn generatedTokenKindIsComparisonOperator(kind: TokenKind) bool {
    return kind == .eq or kind == .neq or kind == .lt or kind == .lte or kind == .gt or kind == .gte;
}
