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

const db_mod = @import("../../storage/db/mod.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_operator = @import("operator.zig");
const generated_parser = @import("../generated_parser.zig");
const token_mod = @import("../token.zig");

const Token = token_mod.Token;

pub fn generatedPredicateAt(
    tokens: []const Token,
    pos: usize,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const expression = generated_expression_ast orelse return null;
    const range = expression.tokens orelse return null;
    if (range.start != pos) return null;
    switch (expression.kind) {
        .exists_subquery,
        .not_exists_subquery,
        => {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
            return expression;
        },
        .comparison,
        .in_list,
        .not_in_list,
        .quantified_comparison,
        .like,
        .ilike,
        .not_like,
        .not_ilike,
        => {
            if (expression.right_expression_kind != .subquery) return null;
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
            return expression;
        },
        else => return null,
    }
}

pub fn predicateKind(expression: *const generated_parser.GeneratedSqlExpressionAst) !db_mod.types.RelationalRowsSubqueryPredicateKind {
    return switch (expression.kind) {
        .exists_subquery, .not_exists_subquery => .exists,
        .comparison => .scalar,
        .in_list, .not_in_list => .in,
        .quantified_comparison, .like, .ilike, .not_like, .not_ilike => .quantified,
        else => error.UnsupportedSqlShape,
    };
}

pub fn predicateNegated(expression: *const generated_parser.GeneratedSqlExpressionAst) bool {
    return switch (expression.kind) {
        .not_exists_subquery, .not_in_list, .not_like, .not_ilike => true,
        else => false,
    };
}

pub fn comparisonOp(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !db_mod.types.RelationalRowsSubqueryPredicateOp {
    return switch (expression.kind) {
        .exists_subquery, .not_exists_subquery => .eq,
        .in_list, .not_in_list => .eq,
        .like, .not_like => .like,
        .ilike, .not_ilike => .ilike,
        .comparison, .quantified_comparison => blk: {
            const operator_tokens = expression.operator_tokens orelse return error.UnsupportedSqlShape;
            if (operator_tokens.end != operator_tokens.start + 1) return error.UnsupportedSqlShape;
            var pos = operator_tokens.start;
            const op = try expr_operator.parseComparisonOp(tokens, &pos);
            if (pos != operator_tokens.end) return error.UnsupportedSqlShape;
            break :blk switch (op) {
                .eq => .eq,
                .ne => .ne,
                .gt => .gt,
                .gte => .gte,
                .lt => .lt,
                .lte => .lte,
                .is_null, .is_not_null, .is_distinct, .is_not_distinct => error.UnsupportedSqlShape,
            };
        },
        else => error.UnsupportedSqlShape,
    };
}

pub fn quantifier(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !?db_mod.types.RelationalRowsSubqueryQuantifier {
    return switch (expression.kind) {
        .quantified_comparison, .like, .ilike, .not_like, .not_ilike => blk: {
            const quantifier_tokens = expression.quantifier_tokens orelse return error.UnsupportedSqlShape;
            if (quantifier_tokens.end != quantifier_tokens.start + 1) return error.UnsupportedSqlShape;
            break :blk if (tokens[quantifier_tokens.start].matchesKeywordTag(.all)) .all else .any;
        },
        else => null,
    };
}
