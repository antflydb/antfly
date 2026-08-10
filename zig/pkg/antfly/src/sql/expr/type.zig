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

const binder = @import("../binder.zig");
const db_mod = struct {
    pub const types = @import("../../storage/db/types.zig");
};
const ddl_plan = @import("../ddl_plan.zig");
const expr_equal = @import("equal.zig");
const runtime_schema = @import("../../storage/schema.zig");
const strings = @import("../strings.zig");
const value_mod = @import("../value.zig");

const stringSlicesContains = strings.stringSlicesContains;
const stringSlicesIntersect = strings.stringSlicesIntersect;

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

pub fn windowOutputType(
    function: db_mod.types.RelationalRowsWindowFunction,
    value_type: ?runtime_schema.AntflyType,
) !runtime_schema.AntflyType {
    return switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .count, .sum, .avg => .numeric,
        .lag, .lead, .first_value, .last_value, .nth_value, .min, .max => value_type orelse error.UnsupportedSqlShape,
        .bool_or, .bool_and => .boolean,
    };
}

pub fn sqlExpressionIsInterval(expression: db_mod.types.RelationalRowsExpression) bool {
    return expression.kind == .interval_ns or expression.kind == .interval_months;
}

pub fn sqlExpressionContainsInterval(expression: db_mod.types.RelationalRowsExpression) bool {
    if (sqlExpressionIsInterval(expression)) return true;
    for (expression.operands) |operand| {
        if (sqlExpressionContainsInterval(operand)) return true;
    }
    return false;
}

pub fn rowExpressionDeterministic(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.field_source != .row) return false;
    if (expression.kind == .now or expression.kind == .uuid_v4) return false;
    for (expression.operands) |operand| {
        if (!rowExpressionDeterministic(operand)) return false;
    }
    for (expression.case_branches) |branch| {
        if (!rowExpressionConditionDeterministic(branch.when)) return false;
        if (!rowExpressionDeterministic(branch.then)) return false;
    }
    for (expression.case_else) |case_else| {
        if (!rowExpressionDeterministic(case_else)) return false;
    }
    return true;
}

pub fn rowExpressionConditionDeterministic(condition: runtime_schema.RelationalRowsExpressionCondition) bool {
    if (!rowExpressionDeterministic(condition.lhs)) return false;
    for (condition.rhs) |rhs| {
        if (!rowExpressionDeterministic(rhs)) return false;
    }
    return true;
}

pub fn writeRowExpressionJson(writer: *std.Io.Writer, expression: db_mod.types.RelationalRowsExpression) !void {
    switch (expression.kind) {
        .field => {
            try writer.print("{{\"field\":{f}", .{std.json.fmt(expression.field, .{})});
            if (expression.field_source != .row) {
                try writer.print(",\"source\":{f}", .{std.json.fmt(rowExpressionFieldSourceName(expression.field_source), .{})});
            }
            try writer.writeByte('}');
        },
        .value => {
            try writer.writeAll("{\"value\":");
            try writer.writeAll(expression.value_json);
            try writer.writeByte('}');
        },
        .now => {
            try writer.writeAll("{\"op\":\"now\",\"args\":[]}");
        },
        .case => {
            try writer.writeAll("{\"op\":\"case\",\"cases\":[");
            for (expression.case_branches, 0..) |branch, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.writeAll("{\"when\":{\"lhs\":");
                try writeRowExpressionJson(writer, branch.when.lhs);
                try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(branch.when.op), .{})});
                if (branch.when.rhs.len == 1) {
                    try writer.writeAll(",\"rhs\":");
                    try writeRowExpressionJson(writer, branch.when.rhs[0]);
                }
                try writer.writeAll("},\"then\":");
                try writeRowExpressionJson(writer, branch.then);
                try writer.writeByte('}');
            }
            try writer.writeAll("],\"else\":");
            if (expression.case_else.len != 1) return error.UnsupportedSqlShape;
            try writeRowExpressionJson(writer, expression.case_else[0]);
            try writer.writeByte('}');
        },
        .cast => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const cast_type = expression.cast_type orelse return error.UnsupportedSqlShape;
            try writer.print("{{\"op\":\"cast\",\"to\":{f},\"args\":[", .{std.json.fmt(rowExpressionCastTypeName(cast_type), .{})});
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.writeAll("]}");
        },
        .json_extract => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
            try writer.writeAll("{\"op\":\"json_extract\",\"args\":[");
            try writeRowExpressionJson(writer, expression.operands[0]);
            try writer.print("],\"path\":{f}", .{std.json.fmt(expression.json_path, .{})});
            if (expression.json_as_text) try writer.writeAll(",\"as_text\":true");
            try writer.writeByte('}');
        },
        else => {
            try writer.print("{{\"op\":{f},\"args\":[", .{std.json.fmt(rowExpressionOpName(expression.kind), .{})});
            for (expression.operands, 0..) |operand, i| {
                if (i != 0) try writer.writeByte(',');
                try writeRowExpressionJson(writer, operand);
            }
            try writer.writeAll("]}");
        },
    }
}

pub fn writeRowExpressionConditionJson(writer: *std.Io.Writer, condition: db_mod.types.RelationalRowsExpressionCondition) !void {
    try writer.writeAll("{\"lhs\":");
    try writeRowExpressionJson(writer, condition.lhs);
    try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(condition.op), .{})});
    if (condition.rhs.len == 1) {
        try writer.writeAll(",\"rhs\":");
        try writeRowExpressionJson(writer, condition.rhs[0]);
    }
    try writer.writeByte('}');
}

pub fn rowRewriteExpressionFingerprintAlloc(
    alloc: std.mem.Allocator,
    expression: db_mod.types.RelationalRowsExpression,
) ![]u8 {
    switch (expression.kind) {
        .field => return try std.fmt.allocPrint(
            alloc,
            "field[{s}:{s}]",
            .{ @tagName(expression.field_source), expression.field },
        ),
        .value => return try std.fmt.allocPrint(
            alloc,
            "value[{x}]",
            .{expression.value_json},
        ),
        .cast => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const operand = try rowRewriteExpressionFingerprintAlloc(alloc, expression.operands[0]);
            defer alloc.free(operand);
            return try std.fmt.allocPrint(
                alloc,
                "cast[{s}|{s}]",
                .{ if (expression.cast_type) |cast_type| @tagName(cast_type) else "none", operand },
            );
        },
        .json_extract => {
            if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            const operand = try rowRewriteExpressionFingerprintAlloc(alloc, expression.operands[0]);
            defer alloc.free(operand);
            return try std.fmt.allocPrint(
                alloc,
                "json_extract[{x}|text={}|{s}]",
                .{ expression.json_path, expression.json_as_text, operand },
            );
        },
        .case => {
            if (expression.case_branches.len == 0) return error.UnsupportedSqlShape;
            return try std.fmt.allocPrint(
                alloc,
                "case[branches={d}:else={d}]",
                .{ expression.case_branches.len, expression.case_else.len },
            );
        },
        else => {
            var out: std.Io.Writer.Allocating = .init(alloc);
            errdefer out.deinit();
            const writer = &out.writer;
            try writer.print("{s}[", .{@tagName(expression.kind)});
            for (expression.operands, 0..) |operand, i| {
                const operand_fingerprint = try rowRewriteExpressionFingerprintAlloc(alloc, operand);
                defer alloc.free(operand_fingerprint);
                if (i != 0) try writer.writeByte('+');
                try writer.writeAll(operand_fingerprint);
            }
            try writer.writeByte(']');
            return try out.toOwnedSlice();
        },
    }
}

pub fn rowSecurityPredicateFingerprintSuffixAlloc(
    alloc: std.mem.Allocator,
    predicate: ddl_plan.RowSecurityPolicyPredicate,
) ![]u8 {
    return switch (predicate) {
        .current_setting_equals => |current_setting| try std.fmt.allocPrint(
            alloc,
            "kind=current_setting_eq:field={s}:setting={s}",
            .{ current_setting.field, current_setting.setting_name },
        ),
        .literal_equals => |literal| blk: {
            const value_json_hex = try fingerprintStringOptionHexAlloc(alloc, literal.value_json);
            defer alloc.free(value_json_hex);
            break :blk try std.fmt.allocPrint(
                alloc,
                "kind=literal_eq:field={s}:value_json_hex={s}",
                .{ literal.field, value_json_hex },
            );
        },
        .expression => |expression| blk: {
            const expression_json = try rowSecurityExpressionConditionJsonAlloc(alloc, expression);
            defer alloc.free(expression_json);
            const expression_json_hex = try fingerprintStringOptionHexAlloc(alloc, expression_json);
            defer alloc.free(expression_json_hex);
            break :blk try std.fmt.allocPrint(alloc, "kind=expression:json_hex={s}", .{expression_json_hex});
        },
        .conjunction => |conjunction| blk: {
            var out = try std.fmt.allocPrint(alloc, "kind=and:terms={d}", .{conjunction.predicates.len});
            errdefer alloc.free(out);
            for (conjunction.predicates) |term| {
                const term_suffix = try rowSecurityPredicateFingerprintSuffixAlloc(alloc, term);
                defer alloc.free(term_suffix);
                const next = try std.fmt.allocPrint(alloc, "{s}:term={s}", .{ out, term_suffix });
                alloc.free(out);
                out = next;
            }
            break :blk out;
        },
        .disjunction => |disjunction| blk: {
            var out = try std.fmt.allocPrint(alloc, "kind=or:terms={d}", .{disjunction.predicates.len});
            errdefer alloc.free(out);
            for (disjunction.predicates) |term| {
                const term_suffix = try rowSecurityPredicateFingerprintSuffixAlloc(alloc, term);
                defer alloc.free(term_suffix);
                const next = try std.fmt.allocPrint(alloc, "{s}:term={s}", .{ out, term_suffix });
                alloc.free(out);
                out = next;
            }
            break :blk out;
        },
    };
}

fn rowSecurityExpressionConditionJsonAlloc(
    alloc: std.mem.Allocator,
    condition: db_mod.types.RelationalRowsExpressionCondition,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try writeRowExpressionConditionJson(&out.writer, condition);
    return try out.toOwnedSlice();
}

fn fingerprintStringOptionHexAlloc(alloc: std.mem.Allocator, option: ?[]const u8) ![]const u8 {
    const value = option orelse return try alloc.dupe(u8, "default");
    if (value.len == 0) return try alloc.dupe(u8, "empty");
    const out = try alloc.alloc(u8, value.len * 2);
    const hex = "0123456789abcdef";
    for (value, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

pub fn writeRowExpressionPredicateGroupsJson(
    writer: *std.Io.Writer,
    field_name: []const u8,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !void {
    try writer.print("{f}:[", .{std.json.fmt(field_name, .{})});
    for (groups, 0..) |group, group_i| {
        if (group_i != 0) try writer.writeByte(',');
        try writer.writeAll("{\"all\":[");
        for (group.conditions, 0..) |condition, condition_i| {
            if (condition_i != 0) try writer.writeByte(',');
            try writeRowExpressionConditionJson(writer, condition);
        }
        try writer.writeAll("]}");
    }
    try writer.writeByte(']');
}

pub fn rowExpressionOpName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field => "field",
        .value => "value",
        .now => "now",
        .uuid_v4 => "uuid_v4",
        .coalesce => "coalesce",
        .lower => "lower",
        .upper => "upper",
        .initcap => "initcap",
        .trim => "trim",
        .ltrim => "ltrim",
        .rtrim => "rtrim",
        .replace => "replace",
        .regexp_replace => "regexp_replace",
        .regexp_substr => "regexp_substr",
        .regexp_count => "regexp_count",
        .regexp_instr => "regexp_instr",
        .translate => "translate",
        .substring => "substring",
        .overlay => "overlay",
        .split_part => "split_part",
        .strpos => "strpos",
        .ascii => "ascii",
        .left => "left",
        .right => "right",
        .lpad => "lpad",
        .rpad => "rpad",
        .repeat => "repeat",
        .reverse => "reverse",
        .md5 => "md5",
        .soundex => "soundex",
        .starts_with => "starts_with",
        .ends_with => "ends_with",
        .chr => "chr",
        .like => "like",
        .ilike => "ilike",
        .regexp_match => "regexp_match",
        .bool_and => "and",
        .bool_or => "or",
        .bool_not => "not",
        .date_trunc => "date_trunc",
        .date_bin => "date_bin",
        .date_part => "date_part",
        .concat => "concat",
        .concat_ws => "concat_ws",
        .length => "length",
        .octet_length => "octet_length",
        .bit_length => "bit_length",
        .nullif => "nullif",
        .greatest => "greatest",
        .least => "least",
        .abs => "abs",
        .round => "round",
        .trunc => "trunc",
        .floor => "floor",
        .ceil => "ceil",
        .sqrt => "sqrt",
        .sign => "sign",
        .power => "power",
        .add => "add",
        .sub => "sub",
        .mul => "mul",
        .div => "div",
        .mod => "mod",
        .case => "case",
        .cast => "cast",
        .json_extract => "json_extract",
        .json_path_exists => "json_path_exists",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        .array_length => "array_length",
        .array_position => "array_position",
        .array_positions => "array_positions",
        .array_append => "array_append",
        .array_prepend => "array_prepend",
        .array_cat => "array_cat",
        .array_remove => "array_remove",
        .array_replace => "array_replace",
        .array_to_string => "array_to_string",
        .string_to_array => "string_to_array",
        .interval_ns => "interval_ns",
        .interval_months => "interval_months",
    };
}

pub fn rowExpressionDefaultOutputName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
    return switch (kind) {
        .field => "field",
        .value => "value",
        .case => "case",
        .cast => "cast",
        .json_extract => "json_extract",
        .json_typeof => "json_typeof",
        .json_array_length => "json_array_length",
        .json_build_object => "jsonb_build_object",
        .to_jsonb => "to_jsonb",
        else => rowExpressionOpName(kind),
    };
}

pub fn rowExpressionFieldSourceName(source: db_mod.types.RelationalRowsExpressionFieldSource) []const u8 {
    return switch (source) {
        .row => "row",
        .existing => "existing",
        .proposed => "proposed",
        .source => "source",
    };
}

pub fn rowExpressionCastTypeName(cast_type: db_mod.types.RelationalRowsExpressionCastType) []const u8 {
    return switch (cast_type) {
        .text => "text",
        .numeric => "numeric",
        .bool => "bool",
        .datetime => "datetime",
    };
}

pub fn rowExpressionConditionOpName(op: runtime_schema.RelationalCheckOp) []const u8 {
    return switch (op) {
        .eq => "eq",
        .ne => "ne",
        .gt => "gt",
        .gte => "gte",
        .lt => "lt",
        .lte => "lte",
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .is_distinct => "is_distinct",
        .is_not_distinct => "is_not_distinct",
    };
}

pub fn writeRelationalCheckAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const runtime_schema.RelationalCheck,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writeRelationalCheckAtomJson(writer, predicate);
        wrote_atom.* = true;
    }
}

pub fn writeRelationalCheckAtomJson(writer: *std.Io.Writer, predicate: runtime_schema.RelationalCheck) !void {
    try writer.print("{{\"field\":{f},\"op\":{f}", .{
        std.json.fmt(predicate.field, .{}),
        std.json.fmt(ddl_plan.relationalCheckOpToken(predicate.op), .{}),
    });
    if (predicate.value_json) |value_json| {
        try writer.writeAll(",\"value\":");
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

pub fn writeInPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(if (predicate.negated) "not_in" else "in", .{}),
        });
        try writer.writeAll(predicate.values_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeTextPatternPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsTextPatternPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"text_pattern\",\"pattern\":{f}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.pattern, .{}),
        });
        if (predicate.case_insensitive) try writer.writeAll(",\"case_insensitive\":true");
        if (predicate.negated) try writer.writeAll(",\"negated\":true");
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeStructuredValuePredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    op_name: []const u8,
    predicates: anytype,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(op_name, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathEqPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_eq\",\"path\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

pub fn writeJsonPathExistsPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_exists\",\"path\":{f}}}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        wrote_atom.* = true;
    }
}

pub fn validateCheckExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) anyerror!void {
    switch (condition.op) {
        .is_null, .is_not_null => if (condition.rhs.len != 0) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => if (condition.rhs.len != 1) return error.InvalidSqlCatalog,
    }
    const lhs_type = try checkExpressionTypeForColumns(columns, condition.lhs);
    if (condition.rhs.len == 0) return;
    const rhs_type = try checkExpressionTypeForColumns(columns, condition.rhs[0]);
    if (!checkExpressionTypesComparable(lhs_type, rhs_type)) return error.InvalidSqlCatalog;
    switch (condition.op) {
        .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(lhs_type) or !checkExpressionTypeOrderable(rhs_type)) return error.InvalidSqlCatalog,
        else => {},
    }
}

pub fn validateCheckExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    _ = try checkExpressionTypeForColumns(columns, expression);
}

pub const CheckExpressionType = union(enum) {
    type: runtime_schema.AntflyType,
    null,
};

fn checkExpressionContainsInterval(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.kind == .interval_ns or expression.kind == .interval_months) return true;
    for (expression.operands) |operand| {
        if (checkExpressionContainsInterval(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (checkExpressionContainsInterval(branch.then)) return true;
        if (checkExpressionContainsInterval(branch.when.lhs)) return true;
        for (branch.when.rhs) |rhs| {
            if (checkExpressionContainsInterval(rhs)) return true;
        }
    }
    for (expression.case_else) |case_else| {
        if (checkExpressionContainsInterval(case_else)) return true;
    }
    return false;
}

fn validateDateBinStrideExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!void {
    switch (expression.kind) {
        .interval_ns => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (checkExpressionContainsInterval(expression.operands[0])) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .numeric)) return error.InvalidSqlCatalog;
        },
        .interval_months => return error.InvalidSqlCatalog,
        else => {
            if (checkExpressionContainsInterval(expression)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression), .numeric)) return error.InvalidSqlCatalog;
        },
    }
}

pub fn checkExpressionTypeForColumns(
    columns: []const runtime_schema.RelationalColumn,
    expression: runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidSqlCatalog;
        const column = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
        return .{ .type = column.field_type };
    }
    if (expression.kind == .value) return checkExpressionLiteralType(expression.value_json);

    for (expression.case_branches) |branch| try validateCheckExpressionConditionForColumns(columns, branch.when);

    switch (expression.kind) {
        .field, .value => unreachable,
        .now => return .{ .type = .datetime },
        .uuid_v4 => return .{ .type = .text },
        .regexp_replace, .regexp_substr => {
            if (expression.kind == .regexp_replace and expression.operands.len != 3 and expression.operands.len != 4) return error.InvalidSqlCatalog;
            if (expression.kind == .regexp_substr and expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .translate, .substring, .overlay, .split_part, .left, .right, .lpad, .rpad, .repeat, .reverse, .chr, .md5, .soundex, .concat, .concat_ws => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .text };
        },
        .length, .octet_length, .bit_length, .ascii, .strpos => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .regexp_count, .regexp_instr => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .starts_with, .ends_with, .like, .ilike => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .regexp_match => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            for (expression.operands[0..2]) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            if (expression.operands.len == 3 and !checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[2]), .boolean)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .bool_and, .bool_or, .bool_not => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .boolean)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .boolean };
        },
        .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .interval_ns, .interval_months => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, operand), .numeric)) return error.InvalidSqlCatalog;
            }
            return .{ .type = .numeric };
        },
        .add, .sub => {
            var saw_datetime = false;
            for (expression.operands) |operand| {
                const operand_type = try checkExpressionTypeForColumns(columns, operand);
                switch (operand_type) {
                    .type => |field_type| {
                        if (field_type == .datetime) saw_datetime = true else if (field_type != .numeric) return error.InvalidSqlCatalog;
                    },
                    .null => return error.InvalidSqlCatalog,
                }
            }
            return .{ .type = if (saw_datetime) .datetime else .numeric };
        },
        .date_trunc => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_bin => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            try validateDateBinStrideExpressionForColumns(columns, expression.operands[0]);
            const source_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(source_type, .datetime) and !checkExpressionTypeEquals(source_type, .numeric)) return error.InvalidSqlCatalog;
            const origin_type = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            if (!checkExpressionTypeEquals(origin_type, .datetime) and !checkExpressionTypeEquals(origin_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .datetime };
        },
        .date_part => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[0]))) return error.InvalidSqlCatalog;
            const value_type = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            if (!checkExpressionTypeEquals(value_type, .datetime) and !checkExpressionTypeEquals(value_type, .numeric)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .cast => return .{ .type = switch (expression.cast_type orelse return error.InvalidSqlCatalog) {
            .text => .text,
            .numeric => .numeric,
            .bool => .boolean,
            .datetime => .datetime,
        } },
        .json_extract => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = if (expression.json_as_text) .text else .json };
        },
        .json_path_exists => {
            if (expression.operands.len != 1 or expression.json_path.len == 0) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .boolean };
        },
        .json_typeof => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .json_array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .json)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .json_build_object => {
            if (expression.operands.len % 2 != 0) return error.InvalidSqlCatalog;
            var index: usize = 0;
            while (index < expression.operands.len) : (index += 2) {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[index]))) return error.InvalidSqlCatalog;
                _ = try checkExpressionTypeForColumns(columns, expression.operands[index + 1]);
            }
            return .{ .type = .json };
        },
        .to_jsonb => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[0]);
            return .{ .type = .json };
        },
        .array_length => {
            if (expression.operands.len != 1) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .numeric };
        },
        .array_position => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .numeric };
        },
        .array_positions => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_append, .array_prepend, .array_remove => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            return .{ .type = .array };
        },
        .array_replace => {
            if (expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            _ = try checkExpressionTypeForColumns(columns, expression.operands[1]);
            _ = try checkExpressionTypeForColumns(columns, expression.operands[2]);
            return .{ .type = .array };
        },
        .array_cat => {
            if (expression.operands.len != 2) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[1]), .array)) return error.InvalidSqlCatalog;
            return .{ .type = .array };
        },
        .array_to_string => {
            if (expression.operands.len != 2 and expression.operands.len != 3) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeEquals(try checkExpressionTypeForColumns(columns, expression.operands[0]), .array)) return error.InvalidSqlCatalog;
            if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[1]))) return error.InvalidSqlCatalog;
            if (expression.operands.len == 3 and !checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, expression.operands[2]))) return error.InvalidSqlCatalog;
            return .{ .type = .text };
        },
        .string_to_array => {
            for (expression.operands) |operand| {
                if (!checkExpressionTypeTextLike(try checkExpressionTypeForColumns(columns, operand))) return error.InvalidSqlCatalog;
            }
            return .{ .type = .array };
        },
        .coalesce, .nullif, .greatest, .least => return try checkExpressionCommonType(columns, expression.operands),
        .case => {
            if (expression.case_else.len != 1) return error.InvalidSqlCatalog;
            var candidate = try checkExpressionTypeForColumns(columns, expression.case_else[0]);
            for (expression.case_branches) |branch| {
                const branch_type = try checkExpressionTypeForColumns(columns, branch.then);
                if (!checkExpressionTypesComparable(candidate, branch_type)) return error.InvalidSqlCatalog;
                if (candidate == .null) candidate = branch_type;
            }
            return candidate;
        },
    }
}

fn checkExpressionCommonType(
    columns: []const runtime_schema.RelationalColumn,
    expressions: []const runtime_schema.RelationalRowsExpression,
) anyerror!CheckExpressionType {
    if (expressions.len == 0) return error.InvalidSqlCatalog;
    var candidate = try checkExpressionTypeForColumns(columns, expressions[0]);
    for (expressions[1..]) |expression| {
        const expression_type = try checkExpressionTypeForColumns(columns, expression);
        if (!checkExpressionTypesComparable(candidate, expression_type)) return error.InvalidSqlCatalog;
        if (candidate == .null) candidate = expression_type;
    }
    return candidate;
}

pub fn checkExpressionLiteralType(value_json: []const u8) !CheckExpressionType {
    if (std.mem.eql(u8, value_json, "null")) return .null;
    if (std.mem.eql(u8, value_json, "true") or std.mem.eql(u8, value_json, "false")) return .{ .type = .boolean };
    if (value_json.len == 0) return error.InvalidSqlCatalog;
    return switch (value_json[0]) {
        '"' => .{ .type = .text },
        '[' => .{ .type = .array },
        '{' => .{ .type = .json },
        '-', '0'...'9' => .{ .type = .numeric },
        else => error.InvalidSqlCatalog,
    };
}

pub fn checkExpressionTypesComparable(lhs: CheckExpressionType, rhs: CheckExpressionType) bool {
    if (lhs == .null or rhs == .null) return true;
    if (checkExpressionTypeTextLike(lhs) and checkExpressionTypeTextLike(rhs)) return true;
    return switch (lhs) {
        .null => true,
        .type => |lhs_type| switch (rhs) {
            .null => true,
            .type => |rhs_type| (lhs_type == .datetime and rhs_type == .numeric) or
                (lhs_type == .numeric and rhs_type == .datetime) or
                lhs_type == rhs_type,
        },
    };
}

pub fn checkExpressionTypeEquals(value: CheckExpressionType, expected: runtime_schema.AntflyType) bool {
    return switch (value) {
        .null => false,
        .type => |actual| actual == expected,
    };
}

pub fn checkExpressionTypeTextLike(value: CheckExpressionType) bool {
    return switch (value) {
        .null => false,
        .type => |field_type| switch (field_type) {
            .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
            else => false,
        },
    };
}

pub fn checkExpressionTypeOrderable(value: CheckExpressionType) bool {
    return checkExpressionTypeTextLike(value) or
        checkExpressionTypeEquals(value, .numeric) or
        checkExpressionTypeEquals(value, .datetime) or
        checkExpressionTypeEquals(value, .boolean);
}

pub fn sqlExpressionTypesComparable(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    if (sqlExpressionTypeIsTextLike(lhs) and sqlExpressionTypeIsTextLike(rhs)) return true;
    if ((lhs == .datetime and rhs == .numeric) or (lhs == .numeric and rhs == .datetime)) return true;
    return lhs == rhs;
}

pub fn sqlExpressionTypeIsTextLike(field_type: runtime_schema.AntflyType) bool {
    return switch (field_type) {
        .text, .keyword, .link, .html, .search_as_you_type, .blob, .geoshape => true,
        else => false,
    };
}

pub fn sqlExpressionTypeIsOrderable(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime or field_type == .boolean;
}

pub fn sqlAggregateMinMaxTypeAllowed(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime;
}

pub fn sqlAggregateModeTypeAllowed(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsTextLike(field_type) or field_type == .numeric or field_type == .datetime or field_type == .boolean;
}

pub fn validateAggregateInputExpression(
    type_context: RowExpressionTypeContext,
    op: db_mod.types.RelationalRowsAggregateOp,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    switch (op) {
        .count, .array_agg => {},
        .string_agg => try type_context.validateTextRowExpression(expression),
        .sum, .avg, .percentile_cont, .percentile_disc => try type_context.validateNumericRowExpression(expression),
        .mode => try validateAggregateModeRowExpression(type_context, expression),
        .min, .max => try validateAggregateMinMaxRowExpression(type_context, expression),
        .bool_or, .bool_and => try type_context.validateBooleanRowExpression(expression),
    }
}

pub fn validateAggregateModeRowExpression(
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const output_type = try type_context.rowExpressionOutputType(expression);
    if (!sqlAggregateModeTypeAllowed(output_type)) return error.UnsupportedSqlShape;
}

pub fn validateAggregateMinMaxRowExpression(
    type_context: RowExpressionTypeContext,
    expression: db_mod.types.RelationalRowsExpression,
) !void {
    const output_type = try type_context.rowExpressionOutputType(expression);
    if (!sqlAggregateMinMaxTypeAllowed(output_type)) return error.UnsupportedSqlShape;
}

pub fn sqlExpressionTypeIsOrderKey(field_type: runtime_schema.AntflyType) bool {
    return sqlExpressionTypeIsOrderable(field_type) or field_type == .json or field_type == .array;
}

pub fn sqlExpressionResultTypesCompatible(lhs: runtime_schema.AntflyType, rhs: runtime_schema.AntflyType) bool {
    return sqlExpressionTypesComparable(lhs, rhs);
}

pub fn rowExpressionTypeContext(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema,
    defer_row_expression_field_validation: bool,
) RowExpressionTypeContext {
    return .{
        .alloc = alloc,
        .schema = schema,
        .joined_source_schema = joined_source_schema,
        .defer_row_expression_field_validation = defer_row_expression_field_validation,
    };
}

pub fn rowExpressionFieldSourceOrDefault(
    override: ?db_mod.types.RelationalRowsExpressionFieldSource,
) db_mod.types.RelationalRowsExpressionFieldSource {
    return override orelse .row;
}

pub const RowExpressionTypeContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    joined_source_schema: ?runtime_schema.TableSchema = null,
    defer_row_expression_field_validation: bool = false,

    pub fn rowExpressionIsNullLiteral(self: @This(), expression: db_mod.types.RelationalRowsExpression) !bool {
        if (expression.kind != .value) return false;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        return parsed.value == .null;
    }

    pub fn validateNumericRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .integer, .float, .number_string => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .now => {},
            .date_trunc => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .length, .octet_length, .bit_length, .ascii => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .strpos => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .left, .right => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression.operands[0]);
            },
            .add => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) {
                    if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                    const lhs_interval = sqlExpressionIsInterval(expression.operands[0]);
                    const rhs_interval = sqlExpressionIsInterval(expression.operands[1]);
                    if (lhs_interval == rhs_interval) return error.UnsupportedSqlShape;
                    if (lhs_interval) {
                        try self.validateIntervalRowExpression(expression.operands[0]);
                        try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
                    } else {
                        try self.validateNumericOrDatetimeRowExpression(expression.operands[0]);
                        try self.validateIntervalRowExpression(expression.operands[1]);
                    }
                    return;
                }
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .mul => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .sub => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) {
                    if (sqlExpressionIsInterval(expression.operands[0]) or !sqlExpressionIsInterval(expression.operands[1])) return error.UnsupportedSqlShape;
                    try self.validateNumericOrDatetimeRowExpression(expression.operands[0]);
                    try self.validateIntervalRowExpression(expression.operands[1]);
                    return;
                }
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .div, .mod, .power => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateNumericRowExpression(branch.then);
                try self.validateNumericRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .numeric) return error.UnsupportedSqlShape;
            },
            .array_length => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
            },
            .array_position, .array_positions => try self.validateArrayPositionExpression(expression),
            .json_array_length => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .regexp_count, .regexp_instr => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .interval_ns, .interval_months => try self.validateIntervalRowExpression(expression),
            .date_part => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateBooleanRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .boolean) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .bool => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .starts_with => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .ends_with => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .like, .ilike => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .regexp_match => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateBooleanRowExpression(expression.operands[2]);
            },
            .json_path_exists => {
                if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .bool_and, .bool_or => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .bool_not => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateBooleanRowExpression(expression.operands[0]);
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateBooleanRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateBooleanRowExpression(branch.then);
                try self.validateBooleanRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .bool) return error.UnsupportedSqlShape;
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn schemaForRowExpressionField(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
    ) runtime_schema.TableSchema {
        if (expression.field_source == .source) return self.joined_source_schema orelse self.schema;
        return self.schema;
    }

    pub fn validateIntervalRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (!sqlExpressionIsInterval(expression) or expression.operands.len != 1) return error.UnsupportedSqlShape;
        if (sqlExpressionContainsInterval(expression.operands[0])) return error.UnsupportedSqlShape;
        try self.validateNumericRowExpression(expression.operands[0]);
    }

    pub fn validateDateBinStrideRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .interval_ns => try self.validateIntervalRowExpression(expression),
            .interval_months => return error.UnsupportedSqlShape,
            else => {
                if (sqlExpressionContainsInterval(expression)) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression);
            },
        }
    }

    pub fn validateNumericOrDatetimeRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
            },
            .now => {},
            .date_trunc => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .date_bin => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateDateBinStrideRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[2]);
            },
            .date_part => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericOrDatetimeRowExpression(expression.operands[1]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .datetime) return error.UnsupportedSqlShape;
            },
            else => try self.validateNumericRowExpression(expression),
        }
    }

    pub fn validateOrderableRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        const output_type = try self.rowExpressionOutputType(expression);
        if (!sqlExpressionTypeIsOrderKey(output_type)) return error.UnsupportedSqlShape;
    }

    pub fn rowExpressionOutputType(self: @This(), expression: db_mod.types.RelationalRowsExpression) !runtime_schema.AntflyType {
        switch (expression.kind) {
            .field => {
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse {
                    if (self.defer_row_expression_field_validation) return .json;
                    return error.InvalidSqlCatalog;
                };
                return column.field_type;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                return switch (parsed.value) {
                    .integer, .float, .number_string => .numeric,
                    .string => .keyword,
                    .bool => .boolean,
                    .array => .array,
                    .object, .null => .json,
                };
            },
            .now, .date_trunc => return .datetime,
            .date_bin => return .datetime,
            .uuid_v4, .lower, .upper, .initcap, .trim, .ltrim, .rtrim, .replace, .regexp_replace, .regexp_substr, .translate, .substring, .overlay, .split_part, .left, .right, .lpad, .rpad, .repeat, .reverse, .chr, .md5, .soundex, .json_typeof, .array_to_string => return .keyword,
            .concat, .concat_ws => return .text,
            .starts_with, .ends_with, .like, .ilike, .regexp_match, .bool_and, .bool_or, .bool_not, .json_path_exists => return .boolean,
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                return try self.rowExpressionOutputType(expression.operands[0]);
            },
            .length, .octet_length, .bit_length, .strpos, .ascii, .regexp_count, .regexp_instr, .abs, .round, .trunc, .floor, .ceil, .sqrt, .sign, .power, .mul, .div, .mod, .json_array_length, .array_length, .array_position, .interval_ns, .interval_months, .date_part => return .numeric,
            .add, .sub => {
                if (expression.operands.len > 0 and sqlExpressionContainsInterval(expression)) {
                    return try self.rowExpressionOutputType(expression.operands[0]);
                }
                return .numeric;
            },
            .coalesce, .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                return try self.expressionOperandOutputType(expression.operands);
            },
            .case => {
                return try self.caseExpressionOutputType(expression.case_branches, expression.case_else);
            },
            .cast => return switch (expression.cast_type orelse return error.UnsupportedSqlShape) {
                .text => .text,
                .numeric => .numeric,
                .bool => .boolean,
                .datetime => .datetime,
            },
            .json_extract => return if (expression.json_as_text) .keyword else .json,
            .json_build_object, .to_jsonb => return .json,
            .array_positions, .array_append, .array_prepend, .array_cat, .array_remove, .array_replace, .string_to_array => return .array,
        }
    }

    pub fn rowExpressionOutputArrayItemType(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!?runtime_schema.AntflyType {
        const output_type = try self.rowExpressionOutputType(expression);
        if (output_type != .array) return null;
        switch (expression.kind) {
            .field => {
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, .array) orelse {
                    if (self.defer_row_expression_field_validation) return .json;
                    return error.InvalidSqlCatalog;
                };
                return column.array_item_type orelse .json;
            },
            .array_positions => return .numeric,
            .array_append, .array_prepend, .array_remove, .array_replace => {
                if (expression.operands.len > 0 and (try self.rowExpressionOutputType(expression.operands[0])) == .array) {
                    return (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse .json;
                }
                return .json;
            },
            .array_cat => {
                for (expression.operands) |operand| {
                    if ((try self.rowExpressionOutputType(operand)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(operand)) orelse .json;
                    }
                }
                return .json;
            },
            .string_to_array => return .keyword,
            .value => return .json,
            .coalesce, .greatest, .least => {
                for (expression.operands) |operand| {
                    if (try self.rowExpressionIsNullLiteral(operand)) continue;
                    if ((try self.rowExpressionOutputType(operand)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(operand)) orelse .json;
                    }
                }
                return .json;
            },
            .case => {
                for (expression.case_branches) |branch| {
                    if ((try self.rowExpressionOutputType(branch.then)) == .array) {
                        return (try self.rowExpressionOutputArrayItemType(branch.then)) orelse .json;
                    }
                }
                if (expression.case_else.len == 1 and (try self.rowExpressionOutputType(expression.case_else[0])) == .array) {
                    return (try self.rowExpressionOutputArrayItemType(expression.case_else[0])) orelse .json;
                }
                return .json;
            },
            else => return .json,
        }
    }

    pub fn expressionOperandOutputType(
        self: @This(),
        operands: []const db_mod.types.RelationalRowsExpression,
    ) anyerror!runtime_schema.AntflyType {
        if (operands.len == 0) return error.UnsupportedSqlShape;
        for (operands) |operand| {
            if (try self.rowExpressionIsNullLiteral(operand)) continue;
            return try self.rowExpressionOutputType(operand);
        }
        return .json;
    }

    pub fn caseExpressionOutputType(
        self: @This(),
        branches: []const db_mod.types.RelationalRowsExpressionCaseBranch,
        fallback: []const db_mod.types.RelationalRowsExpression,
    ) anyerror!runtime_schema.AntflyType {
        if (branches.len == 0 or fallback.len != 1) return error.UnsupportedSqlShape;
        var result_type: ?runtime_schema.AntflyType = null;
        for (branches) |branch| {
            try self.mergeCaseExpressionArmType(branch.then, &result_type);
        }
        try self.mergeCaseExpressionArmType(fallback[0], &result_type);
        return result_type orelse .json;
    }

    pub fn mergeCaseExpressionArmType(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
        result_type: *?runtime_schema.AntflyType,
    ) anyerror!void {
        if (try self.rowExpressionIsNullLiteral(expression)) return;
        const arm_type = try self.rowExpressionOutputType(expression);
        if (result_type.*) |existing| {
            if (!sqlExpressionResultTypesCompatible(existing, arm_type)) return error.UnsupportedSqlShape;
        } else {
            result_type.* = arm_type;
        }
    }

    pub fn validateExpressionOperandDomains(
        self: @This(),
        expression: db_mod.types.RelationalRowsExpression,
    ) anyerror!void {
        switch (expression.kind) {
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, false);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, false);
            },
            .greatest, .least => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                try self.validateExpressionSameDomainOperands(expression.operands, true);
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateExpressionSameDomainOperands(
        self: @This(),
        operands: []const db_mod.types.RelationalRowsExpression,
        require_orderable: bool,
    ) anyerror!void {
        var result_type: ?runtime_schema.AntflyType = null;
        for (operands) |operand| {
            if (try self.rowExpressionIsNullLiteral(operand)) continue;
            const operand_type = try self.rowExpressionOutputType(operand);
            if (result_type) |existing| {
                if (!sqlExpressionResultTypesCompatible(existing, operand_type)) return error.UnsupportedSqlShape;
            } else {
                result_type = operand_type;
            }
        }
        if (require_orderable) {
            const operand_type = result_type orelse return;
            if (!sqlExpressionTypeIsOrderable(operand_type)) return error.UnsupportedSqlShape;
        }
    }

    pub fn validateTextRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) !void {
        switch (expression.kind) {
            .field => {
                if (self.defer_row_expression_field_validation) return;
                const column = binder.relationalColumnForField(self.schemaForRowExpressionField(expression), expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .string => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .uuid_v4 => {
                if (expression.operands.len != 0) return error.UnsupportedSqlShape;
            },
            .coalesce => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .lower, .upper, .initcap, .md5, .soundex => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .chr => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateNumericRowExpression(expression.operands[0]);
            },
            .trim, .ltrim, .rtrim => {
                if (expression.operands.len != 1 and expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .replace, .translate => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .regexp_replace => {
                if (expression.operands.len != 3 and expression.operands.len != 4) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .regexp_substr => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
            },
            .substring => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateNumericRowExpression(expression.operands[2]);
            },
            .overlay => {
                if (expression.operands.len != 3 and expression.operands.len != 4) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                try self.validateNumericRowExpression(expression.operands[2]);
                if (expression.operands.len == 4) try self.validateNumericRowExpression(expression.operands[3]);
            },
            .split_part => {
                if (expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateTextRowExpression(expression.operands[1]);
                try self.validateNumericRowExpression(expression.operands[2]);
            },
            .left, .right => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .lpad, .rpad => {
                if (expression.operands.len != 2 and expression.operands.len != 3) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
                if (expression.operands.len == 3) try self.validateTextRowExpression(expression.operands[2]);
            },
            .repeat => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
                try self.validateNumericRowExpression(expression.operands[1]);
            },
            .reverse => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateTextRowExpression(expression.operands[0]);
            },
            .starts_with, .ends_with => return error.UnsupportedSqlShape,
            .concat => {
                if (expression.operands.len == 0) return error.UnsupportedSqlShape;
            },
            .concat_ws => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateTextRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateTextRowExpression(branch.then);
                try self.validateTextRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .text) return error.UnsupportedSqlShape;
            },
            .json_extract => {
                if (expression.operands.len != 1 or !expression.json_as_text) return error.UnsupportedSqlShape;
            },
            .json_typeof => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                try self.validateJsonRowExpression(expression.operands[0]);
            },
            .array_append, .array_prepend, .array_remove => try self.validateArrayElementTransformExpression(expression),
            .array_replace => try self.validateArrayReplaceExpression(expression),
            .array_cat => try self.validateArrayCatExpression(expression),
            .array_to_string => try self.validateArrayToStringExpression(expression),
            else => return error.UnsupportedSqlShape,
        }
    }

    pub fn validateJsonRowExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        const output_type = try self.rowExpressionOutputType(expression);
        if (output_type != .json) return error.UnsupportedSqlShape;
    }

    pub fn validateJsonBuildObjectExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .json_build_object or expression.operands.len % 2 != 0) return error.UnsupportedSqlShape;
        var index: usize = 0;
        while (index < expression.operands.len) : (index += 2) {
            try self.validateTextRowExpression(expression.operands[index]);
            _ = try self.rowExpressionOutputType(expression.operands[index + 1]);
        }
    }

    pub fn validateStringToArrayExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .string_to_array or expression.operands.len != 2) return error.UnsupportedSqlShape;
        try self.validateTextRowExpression(expression.operands[0]);
        try self.validateTextRowExpression(expression.operands[1]);
        try self.validateStringToArrayDelimiterLiteral(expression.operands[1]);
    }

    pub fn validateArrayPositionExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if ((expression.kind != .array_position and expression.kind != .array_positions) or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (try self.rowExpressionIsNullLiteral(expression.operands[1])) return;
        const needle_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (!sqlExpressionTypesComparable(item_type, needle_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayElementTransformExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if ((expression.kind != .array_append and expression.kind != .array_prepend and expression.kind != .array_remove) or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (try self.rowExpressionIsNullLiteral(expression.operands[1])) return;
        const element_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (!sqlExpressionTypesComparable(item_type, element_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayReplaceExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_replace or expression.operands.len != 3) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        const item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        if (!try self.rowExpressionIsNullLiteral(expression.operands[1])) {
            const old_type = try self.rowExpressionOutputType(expression.operands[1]);
            if (!sqlExpressionTypesComparable(item_type, old_type)) return error.UnsupportedSqlShape;
        }
        if (!try self.rowExpressionIsNullLiteral(expression.operands[2])) {
            const new_type = try self.rowExpressionOutputType(expression.operands[2]);
            if (!sqlExpressionTypesComparable(item_type, new_type)) return error.UnsupportedSqlShape;
        }
    }

    pub fn validateArrayCatExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_cat or expression.operands.len != 2) return error.UnsupportedSqlShape;
        const left_type = try self.rowExpressionOutputType(expression.operands[0]);
        const right_type = try self.rowExpressionOutputType(expression.operands[1]);
        if (left_type != .array or right_type != .array) return error.UnsupportedSqlShape;
        const left_item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[0])) orelse return error.UnsupportedSqlShape;
        const right_item_type = (try self.rowExpressionOutputArrayItemType(expression.operands[1])) orelse return error.UnsupportedSqlShape;
        if (!sqlExpressionTypesComparable(left_item_type, right_item_type)) return error.UnsupportedSqlShape;
    }

    pub fn validateArrayToStringExpression(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .array_to_string or (expression.operands.len != 2 and expression.operands.len != 3)) return error.UnsupportedSqlShape;
        const array_type = try self.rowExpressionOutputType(expression.operands[0]);
        if (array_type != .array) return error.UnsupportedSqlShape;
        try self.validateTextRowExpression(expression.operands[1]);
        if (expression.operands.len == 3) try self.validateTextRowExpression(expression.operands[2]);
    }

    pub fn validateStringToArrayDelimiterLiteral(self: @This(), expression: db_mod.types.RelationalRowsExpression) anyerror!void {
        if (expression.kind != .value) return;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        switch (parsed.value) {
            .null => {},
            .string => |delimiter| if (delimiter.len == 0) return error.UnsupportedSqlShape,
            else => return error.UnsupportedSqlShape,
        }
    }
};

pub fn validateExpressionScalarMembershipValues(
    type_context: RowExpressionTypeContext,
    lhs: db_mod.types.RelationalRowsExpression,
    values: std.json.Value,
) !void {
    if (values != .array) return error.UnsupportedSqlShape;
    const lhs_type = try type_context.rowExpressionOutputType(lhs);
    if (lhs_type == .array or lhs_type == .json or lhs_type == .embedding) return error.UnsupportedSqlShape;
    for (values.array.items) |value| {
        if (!value_mod.sqlScalarValueMatches(lhs_type, value)) return error.UnsupportedSqlShape;
    }
}

pub fn validateExpressionConditionTypes(
    type_context: RowExpressionTypeContext,
    defer_row_expression_field_validation: bool,
    lhs: db_mod.types.RelationalRowsExpression,
    op: runtime_schema.RelationalCheckOp,
    rhs: []const db_mod.types.RelationalRowsExpression,
) !void {
    if (defer_row_expression_field_validation) {
        switch (op) {
            .is_null, .is_not_null => if (rhs.len != 0) return error.UnsupportedSqlShape,
            .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => if (rhs.len != 1) return error.UnsupportedSqlShape,
        }
        return;
    }
    switch (op) {
        .is_null, .is_not_null => {
            if (rhs.len != 0) return error.UnsupportedSqlShape;
            return;
        },
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            if (rhs.len != 1) return error.UnsupportedSqlShape;
        },
    }
    const lhs_type = try type_context.rowExpressionOutputType(lhs);
    const rhs_expression = rhs[0];
    if (try type_context.rowExpressionIsNullLiteral(rhs_expression)) return;
    const rhs_type = try type_context.rowExpressionOutputType(rhs_expression);
    if (!sqlExpressionTypesComparable(lhs_type, rhs_type)) return error.UnsupportedSqlShape;
    switch (op) {
        .gt, .gte, .lt, .lte => if (!sqlExpressionTypeIsOrderable(lhs_type) or !sqlExpressionTypeIsOrderable(rhs_type)) return error.UnsupportedSqlShape,
        else => {},
    }
}

pub fn validateGeneratedColumnExpressionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    expression: runtime_schema.RelationalRowsExpression,
) error{InvalidSqlCatalog}!void {
    if (expression.kind == .field) {
        if (std.mem.eql(u8, expression.field, generated_column_name)) return error.InvalidSqlCatalog;
        _ = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
    }
    for (expression.operands) |operand| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedColumnExpressionConditionForColumns(columns, generated_column_name, branch.when);
        try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, case_else);
}

fn validateGeneratedColumnExpressionConditionForColumns(
    columns: []const runtime_schema.RelationalColumn,
    generated_column_name: []const u8,
    condition: runtime_schema.RelationalRowsExpressionCondition,
) error{InvalidSqlCatalog}!void {
    try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedColumnExpressionForColumns(columns, generated_column_name, rhs);
}

pub fn validateUniquePredicateExpressionsForColumns(
    columns: []const runtime_schema.RelationalColumn,
    conditions: []const runtime_schema.RelationalRowsExpressionCondition,
) !void {
    for (conditions) |condition| {
        try validateCheckExpressionConditionForColumns(columns, condition);
        if (!rowExpressionDeterministic(condition.lhs)) return error.InvalidSqlCatalog;
        for (condition.rhs) |rhs| {
            if (!rowExpressionDeterministic(rhs)) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateSqlUniqueExpressionListUnique(expressions: []const runtime_schema.UniqueExpression) !void {
    for (expressions, 0..) |lhs, i| {
        for (expressions[i + 1 ..]) |rhs| {
            if (lhs.op != rhs.op) continue;
            if (lhs.op == .expression) {
                if (lhs.expression != null and rhs.expression != null and expr_equal.relationalRowsExpressionEqual(lhs.expression.?, rhs.expression.?)) return error.UnsupportedSqlShape;
                continue;
            }
            if (std.ascii.eqlIgnoreCase(lhs.field, rhs.field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn validateCheckForColumns(columns: []const runtime_schema.RelationalColumn, check: runtime_schema.RelationalCheck) !void {
    if (check.expression) |condition| {
        if (check.field.len != 0 or check.value_json != null) return error.InvalidSqlCatalog;
        return validateCheckExpressionConditionForColumns(columns, condition);
    }
    const column = binder.relationalColumnForDdl(columns, check.field) orelse return error.InvalidSqlCatalog;
    switch (check.op) {
        .is_null, .is_not_null => if (check.value_json != null) return error.InvalidSqlCatalog,
        .eq, .ne, .is_distinct, .is_not_distinct, .gt, .gte, .lt, .lte => {
            const value_json = check.value_json orelse return error.InvalidSqlCatalog;
            const field_type: CheckExpressionType = .{ .type = column.field_type };
            const value_type = try checkExpressionLiteralType(value_json);
            if (!checkExpressionTypesComparable(field_type, value_type)) return error.InvalidSqlCatalog;
            switch (check.op) {
                .gt, .gte, .lt, .lte => if (!checkExpressionTypeOrderable(field_type) or !checkExpressionTypeOrderable(value_type)) return error.InvalidSqlCatalog,
                else => {},
            }
        },
    }
}

pub fn validateGeneratedColumnForColumns(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    const generated = column.generated orelse return;
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSqlCatalog;
            if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
            const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
            if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
        },
        .concat => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
            }
        },
        .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (!checkExpressionTypeTextLike(.{ .type = source.field_type })) return error.InvalidSqlCatalog;
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSqlCatalog;
            try validateGeneratedColumnExpressionForColumns(columns, column.name, expression);
            if (!rowExpressionDeterministic(expression)) return error.InvalidSqlCatalog;
            const expression_type = try checkExpressionTypeForColumns(columns, expression);
            if (!checkExpressionTypesComparable(.{ .type = column.field_type }, expression_type)) return error.InvalidSqlCatalog;
        },
    }
}

pub fn validateCreateIndexIncludeColumns(
    columns: []const runtime_schema.RelationalColumn,
    key_columns: []const []const u8,
    include_columns: []const []const u8,
) !void {
    for (include_columns) |column| {
        if (stringSlicesContains(key_columns, column)) return error.InvalidSqlCatalog;
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniquePredicatesForColumns(columns: []const runtime_schema.RelationalColumn, predicates: []const runtime_schema.UniquePredicate) !void {
    for (predicates) |predicate| {
        const found = binder.relationalColumnForDdl(columns, predicate.field) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintForColumns(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraint: runtime_schema.UniqueConstraint) !void {
    if (constraint.columns.len == 0 and constraint.expressions.len == 0) return error.InvalidSqlCatalog;
    if (constraint.without_overlaps_period) |period| {
        _ = binder.relationalPeriodForDdl(periods, period) orelse return error.InvalidSqlCatalog;
    }
    for (constraint.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => {
                const found = binder.relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
                if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
            },
            .expression => {
                const row_expression = expression.expression orelse return error.InvalidSqlCatalog;
                try validateCheckExpressionForColumns(columns, row_expression);
                if (!rowExpressionDeterministic(row_expression)) return error.InvalidSqlCatalog;
                if (!checkExpressionTypeOrderable(try checkExpressionTypeForColumns(columns, row_expression))) return error.InvalidSqlCatalog;
            },
        }
    }
    try validateCreateIndexIncludeColumns(columns, constraint.columns, constraint.include_columns);
    try validateUniquePredicatesForColumns(columns, constraint.where);
    try validateUniquePredicateExpressionsForColumns(columns, constraint.where_expressions);
}

pub fn validateRelationalColumnCatalog(columns: []const runtime_schema.RelationalColumn) !void {
    for (columns, 0..) |column, i| {
        if (binder.relationalColumnIndex(columns[0..i], column.name) != null) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, column.name, column.path)) return error.InvalidSqlCatalog;
        if (column.collation != null and !binder.relationalColumnTypeSupportsCollation(column.field_type, column.array_item_type)) return error.InvalidSqlCatalog;
        if (column.generated) |_| try validateGeneratedColumnForColumns(columns, column);
        try validateUniquePredicatesForColumns(columns, column.index_where);
        try validateUniquePredicateExpressionsForColumns(columns, column.index_where_expressions);
        try validateRelationalColumnIndexIncludes(columns, column);
        if (column.on_update_value) |_| {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        }
    }
}

pub fn validateRelationalColumnIndexIncludes(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    if (column.index_include_columns.len == 0) return;
    if (!column.indexed or column.index_name == null) return error.InvalidSqlCatalog;
    for (column.index_include_columns) |field| {
        _ = binder.relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
    }
    const index_name = column.index_name.?;
    for (columns) |peer| {
        if (!binder.relationalColumnHasDeclaredIndexName(peer, index_name)) continue;
        if (!stringSlicesEqual(peer.index_include_columns, column.index_include_columns)) return error.InvalidSqlCatalog;
        if (stringSlicesContains(column.index_include_columns, peer.name)) return error.InvalidSqlCatalog;
    }
}

pub fn validateUniqueConstraintCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, constraints: []const runtime_schema.UniqueConstraint) !void {
    for (constraints, 0..) |constraint, i| {
        if (binder.uniqueConstraintNameExists(constraints[0..i], constraint.name)) return error.InvalidSqlCatalog;
        try validateUniqueConstraintForColumns(columns, periods, constraint);
    }
}

pub fn validateForeignKeyCatalog(columns: []const runtime_schema.RelationalColumn, periods: []const runtime_schema.RelationalPeriod, foreign_keys: []const runtime_schema.ForeignKey) !void {
    for (foreign_keys, 0..) |foreign_key, i| {
        if (binder.foreignKeyNameExists(foreign_keys[0..i], foreign_key.name)) return error.InvalidSqlCatalog;
        try binder.validateForeignKeyForColumns(columns, periods, foreign_key);
    }
}

pub fn validateRelationalCheckCatalog(columns: []const runtime_schema.RelationalColumn, checks: []const runtime_schema.RelationalCheck) !void {
    for (checks, 0..) |check, i| {
        if (binder.relationalCheckNameExists(checks[0..i], check.name)) return error.InvalidSqlCatalog;
        try validateCheckForColumns(columns, check);
    }
}

pub fn validatePrimaryKeyColumns(columns: []const runtime_schema.RelationalColumn, primary_key: runtime_schema.PrimaryKey) !void {
    if (primary_key.columns.len == 0) return error.InvalidSqlCatalog;
    for (primary_key.columns) |column| {
        const found = binder.relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.nullable) return error.InvalidSqlCatalog;
    }
    try validateCreateIndexIncludeColumns(columns, primary_key.columns, primary_key.include_columns);
}

pub fn generatedColumnReferencesAny(column: runtime_schema.RelationalColumn, fields: []const []const u8) bool {
    const generated = column.generated orelse return false;
    if (generated.field) |field| {
        if (stringSlicesContains(fields, field)) return true;
    }
    if (generated.expression) |expression| {
        if (expressionReferencesAny(expression, fields)) return true;
    }
    return stringSlicesIntersect(generated.fields, fields);
}

pub fn uniqueConstraintReferencesAny(
    constraint: runtime_schema.UniqueConstraint,
    fields: []const []const u8,
) bool {
    if (stringSlicesIntersect(constraint.columns, fields)) return true;
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (stringSlicesContains(fields, expression.field)) return true,
            .expression => if (expression.expression) |row_expression| {
                if (expressionReferencesAny(row_expression, fields)) return true;
            },
        }
    }
    for (constraint.where) |predicate| {
        if (stringSlicesContains(fields, predicate.field)) return true;
    }
    for (constraint.where_expressions) |condition| {
        if (expressionConditionReferencesAny(condition, fields)) return true;
    }
    return false;
}

pub fn isCaseFoldExpressionOp(op: runtime_schema.UniqueExpressionOp) bool {
    return switch (op) {
        .lower, .upper, .md5 => true,
        .expression => false,
    };
}

pub fn relationalGeneratedOpForUniqueExpressionOp(op: runtime_schema.UniqueExpressionOp) runtime_schema.RelationalGeneratedOp {
    return switch (op) {
        .lower => .lower,
        .upper => .upper,
        .md5 => .md5,
        .expression => unreachable,
    };
}

pub fn uniqueExpressionOpToken(op: runtime_schema.UniqueExpressionOp) []const u8 {
    return switch (op) {
        .lower => "lower",
        .upper => "upper",
        .md5 => "md5",
        .expression => "expression",
    };
}

pub fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

pub fn uniquePredicateAsRelationalCheckOp(op: runtime_schema.UniquePredicateOp) runtime_schema.RelationalCheckOp {
    return switch (op) {
        .is_null => .is_null,
        .is_not_null => .is_not_null,
        .eq => .eq,
        .ne => .ne,
    };
}

pub fn relationalCheckOpFromUniquePredicateToken(token: []const u8) ?runtime_schema.RelationalCheckOp {
    if (std.mem.eql(u8, token, "is_null")) return .is_null;
    if (std.mem.eql(u8, token, "is_not_null")) return .is_not_null;
    if (std.mem.eql(u8, token, "eq")) return .eq;
    if (std.mem.eql(u8, token, "ne")) return .ne;
    return null;
}

pub fn expressionReferencesAny(expression: runtime_schema.RelationalRowsExpression, fields: []const []const u8) bool {
    if (expression.kind == .field and stringSlicesContains(fields, expression.field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesAny(operand, fields)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesAny(branch.when, fields)) return true;
        if (expressionReferencesAny(branch.then, fields)) return true;
    }
    for (expression.case_else) |case_else| {
        if (expressionReferencesAny(case_else, fields)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesAny(condition: runtime_schema.RelationalRowsExpressionCondition, fields: []const []const u8) bool {
    if (expressionReferencesAny(condition.lhs, fields)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesAny(rhs, fields)) return true;
    }
    return false;
}

pub fn expressionReferencesField(expression: runtime_schema.RelationalRowsExpression, field: []const u8) bool {
    if (expression.kind == .field and expression.field_source == .row and std.mem.eql(u8, expression.field, field)) return true;
    for (expression.operands) |operand| {
        if (expressionReferencesField(operand, field)) return true;
    }
    for (expression.case_branches) |branch| {
        if (expressionConditionReferencesField(branch.when, field)) return true;
        if (expressionReferencesField(branch.then, field)) return true;
    }
    for (expression.case_else) |fallback| {
        if (expressionReferencesField(fallback, field)) return true;
    }
    return false;
}

pub fn expressionConditionReferencesField(condition: runtime_schema.RelationalRowsExpressionCondition, field: []const u8) bool {
    if (expressionReferencesField(condition.lhs, field)) return true;
    for (condition.rhs) |rhs| {
        if (expressionReferencesField(rhs, field)) return true;
    }
    return false;
}

test "sql expr_type validates unique expression lists" {
    try validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .upper, .field = "status" },
    });
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .lower, .field = "status" },
        .{ .op = .lower, .field = "STATUS" },
    }));

    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };
    try std.testing.expectError(error.UnsupportedSqlShape, validateSqlUniqueExpressionListUnique(&.{
        .{ .op = .expression, .expression = lower_status },
        .{ .op = .expression, .expression = lower_status },
    }));
}

test "sql expr_type names every row expression kind" {
    inline for (std.meta.fields(runtime_schema.RelationalRowsExpressionKind)) |field| {
        const kind: runtime_schema.RelationalRowsExpressionKind = @field(runtime_schema.RelationalRowsExpressionKind, field.name);
        try std.testing.expect(rowExpressionOpName(kind).len > 0);
        try std.testing.expect(rowExpressionDefaultOutputName(kind).len > 0);
    }
}

test "sql expr_type validates expression output and interval helpers" {
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, try windowOutputType(.row_number, null));
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, try windowOutputType(.lag, .keyword));
    try std.testing.expectError(error.UnsupportedSqlShape, windowOutputType(.lag, null));

    const interval_expression: runtime_schema.RelationalRowsExpression = .{
        .kind = .interval_ns,
        .operands = &.{.{ .kind = .value, .value_json = "1000" }},
    };
    const nested_interval_expression: runtime_schema.RelationalRowsExpression = .{
        .kind = .add,
        .operands = &.{
            .{ .kind = .field, .field = "created_at" },
            interval_expression,
        },
    };
    try std.testing.expect(sqlExpressionIsInterval(interval_expression));
    try std.testing.expect(sqlExpressionContainsInterval(nested_interval_expression));
    try std.testing.expect(!sqlExpressionContainsInterval(.{ .kind = .field, .field = "created_at" }));
}

test "sql expr_type detects deterministic row expressions" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"open\"" };
    const source_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status", .field_source = .source };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try std.testing.expect(rowExpressionDeterministic(status_field));
    try std.testing.expect(!rowExpressionDeterministic(source_field));
    try std.testing.expect(!rowExpressionDeterministic(now_expression));
    try std.testing.expect(rowExpressionConditionDeterministic(condition));
    try std.testing.expect(!rowExpressionConditionDeterministic(nondeterministic_condition));
}

test "sql expr_type detects catalog expression references" {
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const tenant_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "tenant_id" };
    const literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "\"active\"" };
    const condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{literal},
    };
    const generated_column: runtime_schema.RelationalColumn = .{
        .name = "tenant_status",
        .path = "tenant_status",
        .field_type = .keyword,
        .generated = .{ .op = .concat_ws, .fields = &.{ "tenant_id", "status" }, .separator = ":" },
    };
    const expression_generated: runtime_schema.RelationalColumn = .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = .{ .kind = .lower, .operands = &.{status_field} } },
    };
    const unique_constraint: runtime_schema.UniqueConstraint = .{
        .name = "tenant_status_key",
        .columns = &.{"tenant_id"},
        .expressions = &.{.{ .op = .expression, .expression = .{ .kind = .concat, .operands = &.{ tenant_field, status_field } } }},
        .where_expressions = &.{condition},
    };

    try std.testing.expect(generatedColumnReferencesAny(generated_column, &.{"tenant_id"}));
    try std.testing.expect(generatedColumnReferencesAny(expression_generated, &.{"status"}));
    try std.testing.expect(expressionConditionReferencesAny(condition, &.{"status"}));
    try std.testing.expect(expressionConditionReferencesField(condition, "status"));
    try std.testing.expect(!expressionConditionReferencesField(condition, "tenant_id"));
    try std.testing.expect(uniqueConstraintReferencesAny(unique_constraint, &.{"status"}));
    try std.testing.expect(!uniqueConstraintReferencesAny(unique_constraint, &.{"missing"}));
}

test "sql expr_type maps unique expression and predicate operators" {
    try std.testing.expect(isCaseFoldExpressionOp(.lower));
    try std.testing.expect(isCaseFoldExpressionOp(.upper));
    try std.testing.expect(isCaseFoldExpressionOp(.md5));
    try std.testing.expect(!isCaseFoldExpressionOp(.expression));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, relationalGeneratedOpForUniqueExpressionOp(.lower));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.upper, relationalGeneratedOpForUniqueExpressionOp(.upper));
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.md5, relationalGeneratedOpForUniqueExpressionOp(.md5));
    try std.testing.expectEqualStrings("expression", uniqueExpressionOpToken(.expression));
    try std.testing.expectEqualStrings("eq", uniquePredicateOpToken(.eq));
    try std.testing.expectEqualStrings("is_not_null", uniquePredicateOpToken(.is_not_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.ne, uniquePredicateAsRelationalCheckOp(.ne));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, uniquePredicateAsRelationalCheckOp(.is_null));
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, relationalCheckOpFromUniquePredicateToken("eq").?);
    try std.testing.expect(relationalCheckOpFromUniquePredicateToken("missing") == null);
}

test "sql expr_type validates catalog check expression types" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const status_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "status" };
    const amount_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "amount" };
    const metadata_field: runtime_schema.RelationalRowsExpression = .{ .kind = .field, .field = "metadata" };
    const numeric_literal: runtime_schema.RelationalRowsExpression = .{ .kind = .value, .value_json = "10" };
    const now_expression: runtime_schema.RelationalRowsExpression = .{ .kind = .now };
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{status_field},
    };
    const valid_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .gt,
        .rhs = &.{numeric_literal},
    };
    const incomparable_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = status_field,
        .op = .eq,
        .rhs = &.{metadata_field},
    };
    const nondeterministic_condition: runtime_schema.RelationalRowsExpressionCondition = .{
        .lhs = amount_field,
        .op = .eq,
        .rhs = &.{now_expression},
    };

    try validateCheckExpressionForColumns(&columns, lower_status);
    try validateCheckExpressionConditionForColumns(&columns, valid_condition);
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckExpressionConditionForColumns(&columns, incomparable_condition));
    try validateGeneratedColumnExpressionForColumns(&columns, "status_lower", lower_status);
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnExpressionForColumns(&columns, "status", status_field));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicateExpressionsForColumns(&columns, &.{nondeterministic_condition}));

    try std.testing.expect(sqlExpressionTypeIsTextLike(.keyword));
    try std.testing.expect(!sqlExpressionTypeIsTextLike(.json));
    try std.testing.expect(sqlExpressionTypesComparable(.keyword, .text));
    try std.testing.expect(sqlExpressionTypesComparable(.datetime, .numeric));
    try std.testing.expect(!sqlExpressionTypesComparable(.json, .text));
    try std.testing.expect(sqlExpressionTypeIsOrderable(.boolean));
    try std.testing.expect(!sqlExpressionTypeIsOrderable(.json));
    try std.testing.expect(sqlAggregateMinMaxTypeAllowed(.datetime));
    try std.testing.expect(!sqlAggregateMinMaxTypeAllowed(.boolean));
    try std.testing.expect(sqlAggregateModeTypeAllowed(.boolean));
    try std.testing.expect(sqlExpressionTypeIsOrderKey(.json));
    try std.testing.expect(sqlExpressionResultTypesCompatible(.keyword, .text));

    const aggregate_schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
            .{ .name = "enabled", .path = "enabled", .field_type = .boolean },
        },
    };
    const aggregate_type_context = RowExpressionTypeContext{ .alloc = std.testing.allocator, .schema = aggregate_schema };
    try validateAggregateInputExpression(aggregate_type_context, .sum, .{ .kind = .field, .field = "amount" });
    try std.testing.expectError(error.InvalidSqlCatalog, validateAggregateInputExpression(aggregate_type_context, .sum, .{ .kind = .field, .field = "status" }));
    try validateAggregateInputExpression(aggregate_type_context, .mode, .{ .kind = .field, .field = "enabled" });
    try validateAggregateMinMaxRowExpression(aggregate_type_context, .{ .kind = .field, .field = "status" });
    try std.testing.expectError(error.UnsupportedSqlShape, validateAggregateMinMaxRowExpression(aggregate_type_context, .{ .kind = .field, .field = "enabled" }));
}

test "sql expr_type validates DDL expression catalog constraints" {
    const columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "status", .path = "status", .field_type = .keyword },
        .{ .name = "amount", .path = "amount", .field_type = .numeric },
        .{ .name = "created_at", .path = "created_at", .field_type = .datetime },
        .{ .name = "updated_at", .path = "updated_at", .field_type = .datetime },
        .{ .name = "metadata", .path = "metadata", .field_type = .json },
    };
    const pk_columns = [_]runtime_schema.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "status", .path = "status", .field_type = .keyword },
    };
    const periods = [_]runtime_schema.RelationalPeriod{.{ .name = "valid_at", .start_column = "created_at", .end_column = "updated_at" }};
    const lower_status: runtime_schema.RelationalRowsExpression = .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    };

    try validateRelationalColumnCatalog(&columns);
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalColumnCatalog(&.{.{
        .name = "amount",
        .path = "amount",
        .field_type = .numeric,
        .collation = "C",
    }}));
    try validatePrimaryKeyColumns(&pk_columns, .{ .columns = &.{"id"}, .include_columns = &.{"status"} });
    try std.testing.expectError(error.InvalidSqlCatalog, validatePrimaryKeyColumns(&columns, .{ .columns = &.{"status"} }));

    try validateCheckForColumns(&columns, .{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" });
    try std.testing.expectError(error.InvalidSqlCatalog, validateCheckForColumns(&columns, .{ .name = "bad_json_order", .field = "metadata", .op = .gt, .value_json = "{}" }));
    try validateRelationalCheckCatalog(&columns, &.{.{ .name = "amount_positive", .field = "amount", .op = .gt, .value_json = "0" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateRelationalCheckCatalog(&columns, &.{
        .{ .name = "dup_check", .field = "amount", .op = .gt, .value_json = "0" },
        .{ .name = "dup_check", .field = "amount", .op = .lt, .value_json = "10" },
    }));

    try validateGeneratedColumnForColumns(&columns, .{
        .name = "status_lower",
        .path = "status_lower",
        .field_type = .keyword,
        .generated = .{ .op = .expression, .expression = lower_status },
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateGeneratedColumnForColumns(&columns, .{
        .name = "status",
        .path = "status",
        .field_type = .keyword,
        .generated = .{ .op = .lower, .field = "status" },
    }));

    try validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"amount"});
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"status"}));
    try std.testing.expectError(error.InvalidSqlCatalog, validateCreateIndexIncludeColumns(&columns, &.{"status"}, &.{"metadata"}));

    try validateUniquePredicatesForColumns(&columns, &.{.{ .field = "status", .op = .eq, .value_json = "\"open\"" }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniquePredicatesForColumns(&columns, &.{.{ .field = "metadata", .op = .is_not_null }}));

    try validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "status_key",
        .columns = &.{"status"},
        .expressions = &.{.{ .op = .expression, .expression = lower_status }},
        .include_columns = &.{"amount"},
        .without_overlaps_period = "valid_at",
    });
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "metadata_key",
        .columns = &.{"metadata"},
    }));
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintForColumns(&columns, &periods, .{
        .name = "empty_key",
    }));
    try validateUniqueConstraintCatalog(&columns, &periods, &.{.{ .name = "status_key", .columns = &.{"status"} }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateUniqueConstraintCatalog(&columns, &periods, &.{
        .{ .name = "dup_key", .columns = &.{"status"} },
        .{ .name = "dup_key", .columns = &.{"amount"} },
    }));

    try validateForeignKeyCatalog(&columns, &periods, &.{.{
        .name = "status_parent_fkey",
        .parent_table = "parent_statuses",
        .child_columns = &.{"status"},
        .parent_columns = &.{"status"},
    }});
    try std.testing.expectError(error.InvalidSqlCatalog, validateForeignKeyCatalog(&columns, &periods, &.{
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"status"}, .parent_columns = &.{"status"} },
        .{ .name = "dup_fkey", .parent_table = "parent_statuses", .child_columns = &.{"amount"}, .parent_columns = &.{"amount"} },
    }));
}
