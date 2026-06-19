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

const ddl_plan = @import("ddl_plan.zig");
const parser = @import("parser.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const ExtensionFunctionBinding = struct {
    sql_name: []const u8,
    native_expression_kind: runtime_schema.RelationalRowsExpressionKind,
    arity: u16,
};

pub const RoutineExpressionBinding = struct {
    sql_name: []const u8,
    arity: u16,
    expression: runtime_schema.RelationalRowsExpression,
    null_input: ?ddl_plan.RoutineNullInput = null,
};

pub const SqlFunctionBindings = struct {
    extension_functions: []const ExtensionFunctionBinding = &.{},
    routine_expressions: []const RoutineExpressionBinding = &.{},
};

pub fn sqlKeywordIsAnyOrSome(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "any") or std.ascii.eqlIgnoreCase(text, "some");
}

pub fn sqlKeywordStartsScalarPredicate(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "is") or
        std.ascii.eqlIgnoreCase(text, "isnull") or
        std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "between") or
        std.ascii.eqlIgnoreCase(text, "like") or
        std.ascii.eqlIgnoreCase(text, "ilike") or
        std.ascii.eqlIgnoreCase(text, "notnull") or
        sqlKeywordIsAnyOrSome(text) or
        std.ascii.eqlIgnoreCase(text, "all");
}

pub fn peekExtensionFunctionCall(tokens: []const Token, pos: usize, bindings: []const ExtensionFunctionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn extensionFunctionBinding(bindings: []const ExtensionFunctionBinding, name: []const u8) !?ExtensionFunctionBinding {
    var found: ?ExtensionFunctionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn peekRoutineExpressionCall(tokens: []const Token, pos: usize, bindings: []const RoutineExpressionBinding) bool {
    if (bindings.len == 0) return false;
    if (pos + 1 >= tokens.len or tokens[pos].kind != .identifier or tokens[pos + 1].kind != .lparen) return false;
    for (bindings) |binding| {
        if (std.ascii.eqlIgnoreCase(binding.sql_name, tokens[pos].text)) return true;
    }
    return false;
}

pub fn routineExpressionBinding(
    bindings: []const RoutineExpressionBinding,
    name: []const u8,
    arity: usize,
) !?RoutineExpressionBinding {
    var found: ?RoutineExpressionBinding = null;
    for (bindings) |binding| {
        if (!std.ascii.eqlIgnoreCase(binding.sql_name, name)) continue;
        if (@as(usize, binding.arity) != arity) continue;
        if (found != null) return error.UnsupportedSqlShape;
        found = binding;
    }
    return found;
}

pub fn routineArgumentExpressionIsNullLiteral(value: runtime_schema.RelationalRowsExpression) bool {
    return value.kind == .value and std.mem.eql(u8, value.value_json, "null");
}

fn routineArgIndex(field: []const u8) ?usize {
    if (!std.mem.startsWith(u8, field, "arg") or field.len <= 3) return null;
    var index: usize = 0;
    for (field[3..]) |ch| {
        if (ch < '0' or ch > '9') return null;
        index = index * 10 + @as(usize, ch - '0');
    }
    if (index == 0) return null;
    return index - 1;
}

pub fn cloneExpressionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpression,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpression {
    if (value.kind == .field and value.field_source == .row) {
        if (routineArgIndex(value.field)) |index| {
            if (index >= args.len) return error.UnsupportedSqlShape;
            return try runtime_schema.cloneRelationalRowsExpressionAlloc(alloc, args[index]);
        }
    }

    var cloned: runtime_schema.RelationalRowsExpression = .{
        .kind = value.kind,
        .field_source = value.field_source,
        .cast_type = value.cast_type,
        .json_as_text = value.json_as_text,
    };
    errdefer runtime_schema.freeRelationalRowsExpression(alloc, cloned);

    if (value.field.len > 0) cloned.field = try alloc.dupe(u8, value.field);
    if (value.value_json.len > 0) cloned.value_json = try alloc.dupe(u8, value.value_json);
    if (value.json_path.len > 0) cloned.json_path = try alloc.dupe(u8, value.json_path);

    if (value.operands.len > 0) {
        const operands = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.operands.len);
        var initialized: usize = 0;
        errdefer {
            for (operands[0..initialized]) |operand| runtime_schema.freeRelationalRowsExpression(alloc, operand);
            alloc.free(operands);
        }
        for (value.operands, 0..) |operand, i| {
            operands[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, operand, args);
            initialized += 1;
        }
        cloned.operands = operands;
    }

    if (value.case_branches.len > 0) {
        const branches = try alloc.alloc(runtime_schema.RelationalRowsExpressionCaseBranch, value.case_branches.len);
        var initialized: usize = 0;
        errdefer {
            for (branches[0..initialized]) |branch| freeRoutineExpressionCaseBranch(alloc, branch);
            alloc.free(branches);
        }
        for (value.case_branches, 0..) |branch, i| {
            branches[i] = try cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(alloc, branch, args);
            initialized += 1;
        }
        cloned.case_branches = branches;
    }

    if (value.case_else.len > 0) {
        const fallback = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.case_else.len);
        var initialized: usize = 0;
        errdefer {
            for (fallback[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(fallback);
        }
        for (value.case_else, 0..) |expression, i| {
            fallback[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        cloned.case_else = fallback;
    }

    return cloned;
}

fn cloneExpressionCaseBranchSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCaseBranch {
    const when = try cloneExpressionConditionSubstitutingRoutineArgsAlloc(alloc, value.when, args);
    var when_transferred = false;
    errdefer if (!when_transferred) runtime_schema.freeRelationalRowsExpressionCondition(alloc, when);
    const then_expression = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.then, args);
    when_transferred = true;
    return .{
        .when = when,
        .then = then_expression,
    };
}

fn cloneExpressionConditionSubstitutingRoutineArgsAlloc(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCondition,
    args: []const runtime_schema.RelationalRowsExpression,
) anyerror!runtime_schema.RelationalRowsExpressionCondition {
    const lhs = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, value.lhs, args);
    var lhs_transferred = false;
    errdefer if (!lhs_transferred) runtime_schema.freeRelationalRowsExpression(alloc, lhs);

    const rhs = if (value.rhs.len > 0) blk: {
        const out = try alloc.alloc(runtime_schema.RelationalRowsExpression, value.rhs.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |expression| runtime_schema.freeRelationalRowsExpression(alloc, expression);
            alloc.free(out);
        }
        for (value.rhs, 0..) |expression, i| {
            out[i] = try cloneExpressionSubstitutingRoutineArgsAlloc(alloc, expression, args);
            initialized += 1;
        }
        break :blk out;
    } else &.{};

    lhs_transferred = true;
    return .{
        .lhs = lhs,
        .op = value.op,
        .rhs = rhs,
    };
}

fn freeRoutineExpressionCaseBranch(
    alloc: std.mem.Allocator,
    value: runtime_schema.RelationalRowsExpressionCaseBranch,
) void {
    runtime_schema.freeRelationalRowsExpressionCondition(alloc, value.when);
    runtime_schema.freeRelationalRowsExpression(alloc, value.then);
}

pub fn validateExtensionFunctionArity(
    kind: runtime_schema.RelationalRowsExpressionKind,
    declared_arity: u16,
    operand_count: usize,
) !void {
    if (operand_count != @as(usize, declared_arity)) return error.UnsupportedSqlShape;
    switch (kind) {
        .uuid_v4 => if (operand_count != 0) return error.UnsupportedSqlShape,
        .lower, .upper, .md5 => if (operand_count != 1) return error.UnsupportedSqlShape,
        .concat => if (operand_count == 0) return error.UnsupportedSqlShape,
        .concat_ws => if (operand_count < 2) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
}

pub fn sqlJoinedSourceAliasTerminator(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlKeywordIsLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "length") or
        std.ascii.eqlIgnoreCase(text, "char_length") or
        std.ascii.eqlIgnoreCase(text, "character_length");
}

pub fn sqlKeywordIsOctetLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "octet_length");
}

pub fn sqlKeywordIsBitLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "bit_length");
}

pub fn sqlKeywordIsJsonArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_array_length") or
        std.ascii.eqlIgnoreCase(text, "jsonb_array_length");
}

pub fn sqlKeywordIsCardinalityFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "cardinality");
}

pub fn sqlKeywordIsArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_length") or
        sqlKeywordIsCardinalityFunction(text);
}

pub fn sqlKeywordIsArrayPositionFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_position") or
        std.ascii.eqlIgnoreCase(text, "array_positions");
}

pub fn sqlKeywordIsArrayToStringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_to_string");
}

pub fn arrayLengthDefaultOutput(keyword: []const u8) []const u8 {
    if (sqlKeywordIsCardinalityFunction(keyword)) return "cardinality";
    return "array_length";
}

pub fn sqlKeywordIsJsonTypeofFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_typeof") or
        std.ascii.eqlIgnoreCase(text, "jsonb_typeof");
}

pub fn sqlKeywordIsJsonExtractPathFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path") or
        std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsJsonBuildObjectFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_build_object") or
        std.ascii.eqlIgnoreCase(text, "jsonb_build_object");
}

pub fn sqlJsonExtractPathFunctionAsText(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsAsciiFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ascii");
}

pub fn sqlKeywordIsChrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "chr");
}

pub fn sqlKeywordIsSubstringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "substring") or
        std.ascii.eqlIgnoreCase(text, "substr");
}

pub fn sqlKeywordIsOverlayFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "overlay");
}

pub fn sqlKeywordIsTranslateFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "translate");
}

pub fn sqlKeywordIsSplitPartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "split_part");
}

pub fn sqlKeywordIsStrposFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "strpos");
}

pub fn sqlKeywordIsLeftRightFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "right");
}

pub fn sqlKeywordIsPadFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lpad") or
        std.ascii.eqlIgnoreCase(text, "rpad");
}

pub fn sqlKeywordIsRepeatFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "repeat");
}

pub fn sqlKeywordIsReverseFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "reverse");
}

pub fn sqlKeywordIsInitcapFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "initcap");
}

pub fn sqlKeywordIsMd5Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "md5");
}

pub fn sqlKeywordIsStartsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "starts_with");
}

pub fn sqlKeywordIsEndsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ends_with");
}

pub fn sqlKeywordIsDateTruncFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_trunc");
}

pub fn sqlKeywordIsDateBinFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_bin");
}

pub fn sqlKeywordIsDatePartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_part") or
        std.ascii.eqlIgnoreCase(text, "extract");
}

pub fn sqlKeywordIsTrimVariantFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "btrim") or
        std.ascii.eqlIgnoreCase(text, "ltrim") or
        std.ascii.eqlIgnoreCase(text, "rtrim");
}

pub fn sqlKeywordIsUuidV4Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "gen_random_uuid") or
        std.ascii.eqlIgnoreCase(text, "uuid_generate_v4");
}

pub fn sqlKeywordIsRegexpMatchFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_match") or
        std.ascii.eqlIgnoreCase(text, "regexp_like");
}

pub fn sqlKeywordIsRegexpCountFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_count");
}

pub fn sqlKeywordIsRegexpSubstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_substr");
}

pub fn sqlKeywordIsRegexpInstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_instr");
}

pub fn rowExpressionBoundaryKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "as") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "and") or
        std.ascii.eqlIgnoreCase(text, "or") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "asc") or
        std.ascii.eqlIgnoreCase(text, "desc") or
        std.ascii.eqlIgnoreCase(text, "nulls") or
        std.ascii.eqlIgnoreCase(text, "then") or
        std.ascii.eqlIgnoreCase(text, "else") or
        std.ascii.eqlIgnoreCase(text, "end") or
        std.ascii.eqlIgnoreCase(text, "when") or
        std.ascii.eqlIgnoreCase(text, "filter") or
        std.ascii.eqlIgnoreCase(text, "over");
}

pub fn sqlWhereTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "join") or
        std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "inner") or
        std.ascii.eqlIgnoreCase(text, "with") or
        std.ascii.eqlIgnoreCase(text, "over") or
        std.ascii.eqlIgnoreCase(text, "lateral");
}

pub fn sqlWindowTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn parenthesizedPredicateGroupCanStartAt(tokens: []const Token, index: usize) bool {
    if (index >= tokens.len or tokens[index].kind != .lparen) return false;
    const close = parser.findMatchingRParenIndex(tokens, index) orelse return false;
    if (close + 1 >= tokens.len) return true;
    return !tokenContinuesScalarExpressionPredicate(tokens, close + 1);
}

fn tokenContinuesScalarExpressionPredicate(tokens: []const Token, index: usize) bool {
    const token = tokens[index];
    switch (token.kind) {
        .eq, .neq, .gt, .gte, .lt, .lte, .arrow_text, .arrow_json, .path_arrow_text, .path_arrow_json => return true,
        .identifier => {
            if (std.ascii.eqlIgnoreCase(token.text, "is") or
                std.ascii.eqlIgnoreCase(token.text, "like") or
                std.ascii.eqlIgnoreCase(token.text, "ilike") or
                std.ascii.eqlIgnoreCase(token.text, "in") or
                std.ascii.eqlIgnoreCase(token.text, "between"))
            {
                return true;
            }
            if (std.ascii.eqlIgnoreCase(token.text, "not") and index + 1 < tokens.len and tokens[index + 1].kind == .identifier) {
                const after_not = tokens[index + 1].text;
                if (std.ascii.eqlIgnoreCase(after_not, "like") or
                    std.ascii.eqlIgnoreCase(after_not, "ilike") or
                    std.ascii.eqlIgnoreCase(after_not, "in") or
                    std.ascii.eqlIgnoreCase(after_not, "between"))
                {
                    return true;
                }
            }
        },
        else => {},
    }
    return false;
}

test "sql adapter expression keyword predicates classify function and tail tokens" {
    try std.testing.expect(sqlKeywordIsAnyOrSome("SOME"));
    try std.testing.expect(sqlKeywordStartsScalarPredicate("between"));
    try std.testing.expect(sqlKeywordIsArrayLengthFunction("cardinality"));
    try std.testing.expectEqualStrings("cardinality", arrayLengthDefaultOutput("cardinality"));
    try std.testing.expect(sqlKeywordIsJsonExtractPathFunction("jsonb_extract_path_text"));
    try std.testing.expect(sqlJsonExtractPathFunctionAsText("json_extract_path_text"));
    try std.testing.expect(sqlKeywordIsRegexpMatchFunction("regexp_like"));
    try std.testing.expect(rowExpressionBoundaryKeyword("returning") == false);
    try std.testing.expect(sqlWhereTailClauseKeyword("returning"));
    try std.testing.expect(sqlWindowTailClauseKeyword("fetch"));
}

test "sql adapter expression grammar distinguishes grouped predicates from scalar expression predicates" {
    const grouped = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .eq, .text = "=", .source_start = 3, .source_end = 4 },
        .{ .kind = .number, .text = "1", .source_start = 5, .source_end = 6 },
        .{ .kind = .rparen, .text = ")", .source_start = 6, .source_end = 7 },
        .{ .kind = .identifier, .text = "and", .source_start = 8, .source_end = 11 },
    };
    try std.testing.expect(parenthesizedPredicateGroupCanStartAt(grouped[0..], 0));

    const compared_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "a", .source_start = 1, .source_end = 2 },
        .{ .kind = .rparen, .text = ")", .source_start = 2, .source_end = 3 },
        .{ .kind = .eq, .text = "=", .source_start = 4, .source_end = 5 },
        .{ .kind = .number, .text = "1", .source_start = 6, .source_end = 7 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(compared_expression[0..], 0));

    const not_like_expression = [_]Token{
        .{ .kind = .lparen, .text = "(", .source_start = 0, .source_end = 1 },
        .{ .kind = .identifier, .text = "name", .source_start = 1, .source_end = 5 },
        .{ .kind = .rparen, .text = ")", .source_start = 5, .source_end = 6 },
        .{ .kind = .identifier, .text = "not", .source_start = 7, .source_end = 10 },
        .{ .kind = .identifier, .text = "ilike", .source_start = 11, .source_end = 16 },
        .{ .kind = .string, .text = "%bot%", .source_start = 17, .source_end = 24 },
    };
    try std.testing.expect(!parenthesizedPredicateGroupCanStartAt(not_like_expression[0..], 0));
}
